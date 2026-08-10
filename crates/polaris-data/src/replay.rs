use std::{
    fs::File,
    io::{BufRead, BufReader},
    path::{Path, PathBuf},
    pin::Pin,
    vec::IntoIter,
};

use async_stream::try_stream;
use futures_core::Stream;
use futures_util::StreamExt;
use tokio::sync::mpsc;

use crate::{
    OrderbookBuilder,
    errors::PolarisError,
    models::{ReplayStream, StandardEvent},
};

const EVENT_CHANNEL_CAPACITY: usize = 16;

pub(crate) enum ReplayRecord {
    Event(StandardEvent),
    Reset,
}

pub(crate) type ReplayRecordStream =
    Pin<Box<dyn Stream<Item = Result<ReplayRecord, PolarisError>> + Send>>;

pub(crate) fn replay_record_stream(
    paths: Vec<PathBuf>,
    from_us: i64,
    to_us: i64,
    mut gaps: Vec<(i64, i64)>,
) -> ReplayRecordStream {
    Box::pin(try_stream! {
        gaps.sort_unstable();
        let mut gaps = gaps.into_iter().peekable();
        for path in paths {
            let (sender, mut receiver) = mpsc::channel(EVENT_CHANNEL_CAPACITY);
            let reader_path = path.clone();
            let reader = tokio::task::spawn_blocking(move || {
                read_snapshot_events(&reader_path, from_us, to_us, |event| {
                    sender.blocking_send(event).is_ok()
                })
            });

            while let Some(event) = receiver.recv().await {
                while gaps
                    .peek()
                    .is_some_and(|(_, end_us)| event.timestamp.saturating_mul(1_000) >= *end_us)
                {
                    yield ReplayRecord::Reset;
                    gaps.next();
                }
                yield ReplayRecord::Event(event);
            }

            reader
                .await
                .map_err(|err| PolarisError::Request(format!("snapshot reader join failed: {err}")))??;
        }
    })
}

pub(crate) fn replay_stream(
    records: ReplayRecordStream,
    materialize_orderbooks: bool,
) -> ReplayStream {
    Box::pin(try_stream! {
        let mut records = records;
        let mut orderbooks = OrderbookBuilder::new();
        while let Some(record) = records.next().await {
            match record? {
                ReplayRecord::Reset => orderbooks.clear(),
                ReplayRecord::Event(event) if materialize_orderbooks => {
                    if let Some(event) = orderbooks.apply(event)? {
                        yield event;
                    }
                }
                ReplayRecord::Event(event) => yield event,
            }
        }
    })
}

pub(crate) struct LocalReplayIterator {
    paths: IntoIter<PathBuf>,
    current: Option<SnapshotEventReader>,
    from_us: i64,
    to_us: i64,
    gaps: std::iter::Peekable<IntoIter<(i64, i64)>>,
    orderbooks: OrderbookBuilder,
    materialize_orderbooks: bool,
    finished: bool,
}

impl LocalReplayIterator {
    pub(crate) fn new(
        paths: Vec<PathBuf>,
        from_us: i64,
        to_us: i64,
        mut gaps: Vec<(i64, i64)>,
        materialize_orderbooks: bool,
    ) -> Self {
        gaps.sort_unstable();
        Self {
            paths: paths.into_iter(),
            current: None,
            from_us,
            to_us,
            gaps: gaps.into_iter().peekable(),
            orderbooks: OrderbookBuilder::new(),
            materialize_orderbooks,
            finished: false,
        }
    }

    fn fail(&mut self, error: PolarisError) -> Option<Result<StandardEvent, PolarisError>> {
        self.finished = true;
        self.current = None;
        self.paths = Vec::new().into_iter();
        Some(Err(error))
    }
}

impl Iterator for LocalReplayIterator {
    type Item = Result<StandardEvent, PolarisError>;

    fn next(&mut self) -> Option<Self::Item> {
        if self.finished {
            return None;
        }
        loop {
            if self.current.is_none() {
                let Some(path) = self.paths.next() else {
                    self.finished = true;
                    return None;
                };
                match SnapshotEventReader::open(path, self.from_us, self.to_us) {
                    Ok(reader) => self.current = Some(reader),
                    Err(error) => return self.fail(error),
                }
            }

            let next = self.current.as_mut().expect("reader initialized").next();
            let event = match next {
                Some(Ok(event)) => event,
                Some(Err(error)) => return self.fail(error),
                None => {
                    self.current = None;
                    continue;
                }
            };
            let timestamp_us = event.timestamp.saturating_mul(1_000);
            while self
                .gaps
                .peek()
                .is_some_and(|(_, end_us)| timestamp_us >= *end_us)
            {
                self.orderbooks.clear();
                self.gaps.next();
            }
            if self.materialize_orderbooks {
                match self.orderbooks.apply(event) {
                    Ok(Some(event)) => return Some(Ok(event)),
                    Ok(None) => continue,
                    Err(error) => return self.fail(error),
                }
            }
            return Some(Ok(event));
        }
    }
}

struct SnapshotEventReader {
    reader: Box<dyn BufRead + Send>,
    path: PathBuf,
    compressed: bool,
    buffer: Vec<u8>,
    from_us: i64,
    to_us: i64,
    finished: bool,
}

impl SnapshotEventReader {
    fn open(path: PathBuf, from_us: i64, to_us: i64) -> Result<Self, PolarisError> {
        let file = File::open(&path)?;
        let compressed = path.extension().and_then(|value| value.to_str()) == Some("zst");
        let reader: Box<dyn BufRead + Send> = if compressed {
            let decoder = zstd::stream::read::Decoder::new(file).map_err(|err| {
                PolarisError::Decode(format!("invalid zstd stream in {}: {err}", path.display()))
            })?;
            Box::new(BufReader::new(decoder))
        } else {
            Box::new(BufReader::new(file))
        };
        Ok(Self {
            reader,
            path,
            compressed,
            buffer: Vec::with_capacity(8 * 1024),
            from_us,
            to_us,
            finished: false,
        })
    }

    fn map_io_error(&self, error: std::io::Error) -> PolarisError {
        if self.compressed {
            PolarisError::Decode(format!(
                "invalid zstd stream in {}: {error}",
                self.path.display()
            ))
        } else {
            PolarisError::Io(error)
        }
    }
}

impl Iterator for SnapshotEventReader {
    type Item = Result<StandardEvent, PolarisError>;

    fn next(&mut self) -> Option<Self::Item> {
        if self.finished {
            return None;
        }
        loop {
            self.buffer.clear();
            match self.reader.read_until(b'\n', &mut self.buffer) {
                Ok(0) => {
                    self.finished = true;
                    return None;
                }
                Ok(_) => {}
                Err(error) => {
                    self.finished = true;
                    return Some(Err(self.map_io_error(error)));
                }
            }
            while self
                .buffer
                .last()
                .is_some_and(|byte| matches!(byte, b'\n' | b'\r'))
            {
                self.buffer.pop();
            }
            if self.buffer.iter().all(u8::is_ascii_whitespace) {
                continue;
            }
            let mut event: StandardEvent = match serde_json::from_slice(&self.buffer) {
                Ok(event) => event,
                Err(error) => {
                    self.finished = true;
                    return Some(Err(PolarisError::Decode(format!(
                        "invalid ndjson line in {}: {error}",
                        self.path.display()
                    ))));
                }
            };
            let timestamp_us = if event.timestamp.unsigned_abs() >= 100_000_000_000_000 {
                let timestamp_us = event.timestamp;
                event.timestamp /= 1_000;
                timestamp_us
            } else {
                event.timestamp.saturating_mul(1_000)
            };
            if timestamp_us >= self.to_us {
                self.finished = true;
                return None;
            }
            if timestamp_us < self.from_us {
                continue;
            }
            return Some(Ok(event));
        }
    }
}

fn read_snapshot_events(
    path: &Path,
    from_us: i64,
    to_us: i64,
    mut emit: impl FnMut(StandardEvent) -> bool,
) -> Result<(), PolarisError> {
    let reader = SnapshotEventReader::open(path.to_path_buf(), from_us, to_us)?;
    for event in reader {
        let event = event?;
        if !emit(event) {
            break;
        }
    }
    Ok(())
}

#[cfg(test)]
mod tests {
    use futures_util::StreamExt;
    use serde_json::json;
    use tempfile::TempDir;

    use super::*;

    #[tokio::test]
    async fn materialization_clears_across_known_coverage_gaps() {
        let root = TempDir::new().unwrap();
        let first = root.path().join("first.jsonl");
        let second = root.path().join("second.jsonl");
        std::fs::write(
            &first,
            format!(
                "{}\n",
                json!({"timestamp":1000,"source":"s","market":"m","type":"orderbook","data":{"bids":[[100,1]],"asks":[[101,1]]}})
            ),
        )
        .unwrap();
        std::fs::write(
            &second,
            format!(
                "{}\n{}\n{}\n",
                json!({"timestamp":3000,"source":"s","market":"m","type":"orderbook_delta","data":{"bids":[[100,9]]}}),
                json!({"timestamp":3500,"source":"s","market":"m","type":"trade","data":{"price":95,"quantity":1}}),
                json!({"timestamp":4000,"source":"s","market":"m","type":"orderbook","data":{"bids":[[90,2]],"asks":[[91,3]]}})
            ),
        )
        .unwrap();

        let records = replay_record_stream(
            vec![first, second],
            0,
            10_000_000,
            vec![(2_000_000, 3_000_000)],
        );
        let rows = replay_stream(records, true)
            .collect::<Vec<_>>()
            .await
            .into_iter()
            .collect::<Result<Vec<_>, _>>()
            .unwrap();

        assert_eq!(rows.len(), 3);
        assert_eq!(rows[0].event_type, "orderbook");
        assert_eq!(rows[1].event_type, "trade");
        assert_eq!(rows[2].data["bids"], json!([{"price":90.0,"quantity":2.0}]));
    }

    #[tokio::test]
    async fn decoding_is_lazy_across_lines() {
        let root = TempDir::new().unwrap();
        let path = root.path().join("lazy.jsonl");
        std::fs::write(
            &path,
            format!(
                "{}\nnot json\n",
                json!({"timestamp":1000,"source":"s","market":"m","type":"trade","data":{"price":1,"quantity":1}})
            ),
        )
        .unwrap();

        let mut records = replay_record_stream(vec![path], 0, 10_000_000, vec![]);
        assert!(matches!(
            records.next().await.unwrap().unwrap(),
            ReplayRecord::Event(_)
        ));
        assert!(matches!(
            records.next().await.unwrap(),
            Err(PolarisError::Decode(_))
        ));
    }

    #[test]
    fn direct_reader_preserves_half_open_microsecond_filtering_and_crlf() {
        let root = TempDir::new().unwrap();
        let path = root.path().join("range.jsonl");
        std::fs::write(
            &path,
            format!(
                "  \r\n{}\r\n{}\r\n",
                json!({"timestamp":1_704_067_200_000_500_i64,"type":"trade","data":{"price":1}}),
                json!({"timestamp":1_704_067_200_001_000_i64,"type":"trade","data":{"price":2}}),
            ),
        )
        .unwrap();

        let rows = LocalReplayIterator::new(
            vec![path],
            1_704_067_200_000_250,
            1_704_067_200_001_000,
            vec![],
            false,
        )
        .collect::<Result<Vec<_>, _>>()
        .unwrap();

        assert_eq!(rows.len(), 1);
        assert_eq!(rows[0].timestamp, 1_704_067_200_000_i64);
        assert_eq!(rows[0].data["price"], 1);
    }

    #[tokio::test]
    async fn direct_and_async_replay_paths_are_event_for_event_equal() {
        let root = TempDir::new().unwrap();
        let path = root.path().join("parity.jsonl");
        std::fs::write(
            &path,
            format!(
                "{}\n{}\n{}\n",
                json!({"timestamp":1000,"source":"s","market":"m","type":"orderbook","data":{"bids":[[100,1]],"asks":[[101,2]]},"sequence":1}),
                json!({"timestamp":2000,"source":"s","market":"m","type":"orderbook_delta","data":{"bids":[[100,3]]},"sequence":2}),
                json!({"timestamp":3000,"source":"s","market":"m","type":"trade","data":{"price":100,"quantity":1},"sequence":3}),
            ),
        )
        .unwrap();

        for materialize in [false, true] {
            let direct =
                LocalReplayIterator::new(vec![path.clone()], 0, 10_000_000, vec![], materialize)
                    .collect::<Result<Vec<_>, _>>()
                    .unwrap();
            let async_rows = replay_stream(
                replay_record_stream(vec![path.clone()], 0, 10_000_000, vec![]),
                materialize,
            )
            .collect::<Vec<_>>()
            .await
            .into_iter()
            .collect::<Result<Vec<_>, _>>()
            .unwrap();
            assert_eq!(direct, async_rows);
        }
    }
}
