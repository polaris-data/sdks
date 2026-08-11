use std::{
    fs::File,
    io::{BufRead, BufReader},
    path::PathBuf,
    pin::Pin,
    sync::mpsc::{Receiver, sync_channel},
    thread::{self, JoinHandle},
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
const PREFETCH_CHANNEL_CAPACITY: usize = 256;

#[doc(hidden)]
#[derive(Clone, Debug, PartialEq)]
pub struct ExactReplayEvent {
    pub event: StandardEvent,
    pub event_json: String,
    pub timestamp_us: i64,
    pub replay_ordinal: u64,
    pub source_file_ordinal: u32,
    pub source_row_ordinal: u64,
}

struct DecodedEvent {
    event: StandardEvent,
    event_json: Option<String>,
    timestamp_us: i64,
    source_file_ordinal: u32,
    source_row_ordinal: u64,
}

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
        let (sender, mut receiver) = mpsc::channel(EVENT_CHANNEL_CAPACITY);
        let reader = tokio::task::spawn_blocking(move || {
            for decoded in ReplayFileIterator::new(paths, from_us, to_us, false) {
                let decoded = decoded?;
                if sender
                    .blocking_send((decoded.event, decoded.timestamp_us))
                    .is_err()
                {
                    break;
                }
            }
            Ok::<(), PolarisError>(())
        });

        while let Some((event, timestamp_us)) = receiver.recv().await {
            while gaps
                .peek()
                .is_some_and(|(_, end_us)| timestamp_us >= *end_us)
            {
                yield ReplayRecord::Reset;
                gaps.next();
            }
            yield ReplayRecord::Event(event);
        }

        reader
            .await
            .map_err(|err| PolarisError::Request(format!("snapshot reader join failed: {err}")))??;
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
    files: ReplayFileIterator,
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
            files: ReplayFileIterator::new(paths, from_us, to_us, false),
            gaps: gaps.into_iter().peekable(),
            orderbooks: OrderbookBuilder::new(),
            materialize_orderbooks,
            finished: false,
        }
    }

    fn fail(&mut self, error: PolarisError) -> Option<Result<StandardEvent, PolarisError>> {
        self.finished = true;
        self.files.close();
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
            let decoded = match self.files.next() {
                Some(Ok(decoded)) => decoded,
                Some(Err(error)) => return self.fail(error),
                None => {
                    self.finished = true;
                    return None;
                }
            };
            while self
                .gaps
                .peek()
                .is_some_and(|(_, end_us)| decoded.timestamp_us >= *end_us)
            {
                self.orderbooks.clear();
                self.gaps.next();
            }
            if self.materialize_orderbooks {
                match self.orderbooks.apply(decoded.event) {
                    Ok(Some(event)) => return Some(Ok(event)),
                    Ok(None) => continue,
                    Err(error) => return self.fail(error),
                }
            }
            return Some(Ok(decoded.event));
        }
    }
}

#[doc(hidden)]
pub struct LocalExactReplayIterator {
    files: ReplayFileIterator,
    gaps: std::iter::Peekable<IntoIter<(i64, i64)>>,
    orderbooks: OrderbookBuilder,
    materialize_orderbooks: bool,
    replay_ordinal: u64,
    finished: bool,
}

impl LocalExactReplayIterator {
    pub(crate) fn new(
        paths: Vec<PathBuf>,
        from_us: i64,
        to_us: i64,
        mut gaps: Vec<(i64, i64)>,
        materialize_orderbooks: bool,
    ) -> Self {
        gaps.sort_unstable();
        Self {
            files: ReplayFileIterator::new(paths, from_us, to_us, true),
            gaps: gaps.into_iter().peekable(),
            orderbooks: OrderbookBuilder::new(),
            materialize_orderbooks,
            replay_ordinal: 0,
            finished: false,
        }
    }
}

impl Iterator for LocalExactReplayIterator {
    type Item = Result<ExactReplayEvent, PolarisError>;

    fn next(&mut self) -> Option<Self::Item> {
        if self.finished {
            return None;
        }
        loop {
            let decoded = match self.files.next() {
                Some(Ok(decoded)) => decoded,
                Some(Err(error)) => {
                    self.finished = true;
                    self.files.close();
                    return Some(Err(error));
                }
                None => {
                    self.finished = true;
                    return None;
                }
            };
            while self
                .gaps
                .peek()
                .is_some_and(|(_, end_us)| decoded.timestamp_us >= *end_us)
            {
                self.orderbooks.clear();
                self.gaps.next();
            }
            let replay_ordinal = self.replay_ordinal;
            self.replay_ordinal = self.replay_ordinal.saturating_add(1);
            let (event, event_json) = if self.materialize_orderbooks {
                match self.orderbooks.apply(decoded.event) {
                    Ok(Some(event)) => {
                        let mut exact_event = event.clone();
                        exact_event.timestamp = decoded.timestamp_us;
                        let event_json = match serde_json::to_string(&exact_event) {
                            Ok(event_json) => event_json,
                            Err(error) => {
                                self.finished = true;
                                self.files.close();
                                return Some(Err(PolarisError::Decode(error.to_string())));
                            }
                        };
                        (event, event_json)
                    }
                    Ok(None) => continue,
                    Err(error) => {
                        self.finished = true;
                        self.files.close();
                        return Some(Err(error));
                    }
                }
            } else {
                (
                    decoded.event,
                    decoded
                        .event_json
                        .expect("exact replay reader retains source JSON"),
                )
            };
            return Some(Ok(ExactReplayEvent {
                event,
                event_json,
                timestamp_us: decoded.timestamp_us,
                replay_ordinal,
                source_file_ordinal: decoded.source_file_ordinal,
                source_row_ordinal: decoded.source_row_ordinal,
            }));
        }
    }
}

struct ReplayFileIterator {
    paths: std::iter::Enumerate<IntoIter<PathBuf>>,
    current: Option<SnapshotReader>,
    prefetched: Option<SnapshotReader>,
    from_us: i64,
    to_us: i64,
    capture_json: bool,
    finished: bool,
}

impl ReplayFileIterator {
    fn new(paths: Vec<PathBuf>, from_us: i64, to_us: i64, capture_json: bool) -> Self {
        Self {
            paths: paths.into_iter().enumerate(),
            current: None,
            prefetched: None,
            from_us,
            to_us,
            capture_json,
            finished: false,
        }
    }

    fn initialize(&mut self) -> Result<bool, PolarisError> {
        let Some((ordinal, path)) = self.paths.next() else {
            self.finished = true;
            return Ok(false);
        };
        self.current = Some(SnapshotReader::Direct(SnapshotEventReader::open(
            path,
            self.from_us,
            self.to_us,
            ordinal as u32,
            self.capture_json,
        )?));
        self.start_prefetch();
        Ok(true)
    }

    fn start_prefetch(&mut self) {
        if self.prefetched.is_none() {
            if let Some((ordinal, path)) = self.paths.next() {
                self.prefetched = Some(SnapshotReader::prefetched(
                    path,
                    self.from_us,
                    self.to_us,
                    ordinal as u32,
                    self.capture_json,
                ));
            }
        }
    }

    fn close(&mut self) {
        self.finished = true;
        self.current = None;
        self.prefetched = None;
        self.paths = Vec::new().into_iter().enumerate();
    }
}

impl Iterator for ReplayFileIterator {
    type Item = Result<DecodedEvent, PolarisError>;

    fn next(&mut self) -> Option<Self::Item> {
        if self.finished {
            return None;
        }
        if self.current.is_none() {
            match self.initialize() {
                Ok(true) => {}
                Ok(false) => return None,
                Err(error) => {
                    self.close();
                    return Some(Err(error));
                }
            }
        }
        loop {
            if let Some(item) = self.current.as_mut().expect("reader initialized").next() {
                return Some(item);
            }
            self.current = self.prefetched.take();
            if self.current.is_none() {
                self.finished = true;
                return None;
            }
            self.start_prefetch();
        }
    }
}

enum SnapshotReader {
    Direct(SnapshotEventReader),
    Prefetched {
        receiver: Receiver<Result<DecodedEvent, PolarisError>>,
        worker: Option<JoinHandle<()>>,
    },
}

impl SnapshotReader {
    fn prefetched(
        path: PathBuf,
        from_us: i64,
        to_us: i64,
        file_ordinal: u32,
        capture_json: bool,
    ) -> Self {
        let (sender, receiver) = sync_channel(PREFETCH_CHANNEL_CAPACITY);
        let worker = thread::spawn(move || {
            match SnapshotEventReader::open(path, from_us, to_us, file_ordinal, capture_json) {
                Ok(reader) => {
                    for item in reader {
                        if sender.send(item).is_err() {
                            break;
                        }
                    }
                }
                Err(error) => {
                    let _ = sender.send(Err(error));
                }
            }
        });
        Self::Prefetched {
            receiver,
            worker: Some(worker),
        }
    }
}

impl Iterator for SnapshotReader {
    type Item = Result<DecodedEvent, PolarisError>;

    fn next(&mut self) -> Option<Self::Item> {
        match self {
            Self::Direct(reader) => reader.next(),
            Self::Prefetched { receiver, worker } => match receiver.recv() {
                Ok(item) => Some(item),
                Err(_) => {
                    let joined = worker.take().is_none_or(|worker| worker.join().is_ok());
                    (!joined).then(|| {
                        Err(PolarisError::Request(
                            "snapshot prefetch worker panicked".to_owned(),
                        ))
                    })
                }
            },
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
    source_file_ordinal: u32,
    next_row_ordinal: u64,
    capture_json: bool,
    finished: bool,
}

impl SnapshotEventReader {
    fn open(
        path: PathBuf,
        from_us: i64,
        to_us: i64,
        source_file_ordinal: u32,
        capture_json: bool,
    ) -> Result<Self, PolarisError> {
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
            source_file_ordinal,
            next_row_ordinal: 0,
            capture_json,
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
    type Item = Result<DecodedEvent, PolarisError>;

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
            let source_row_ordinal = self.next_row_ordinal;
            self.next_row_ordinal = self.next_row_ordinal.saturating_add(1);
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
            let event_json = if self.capture_json {
                match String::from_utf8(self.buffer.clone()) {
                    Ok(value) => Some(value),
                    Err(error) => {
                        self.finished = true;
                        return Some(Err(PolarisError::Decode(format!(
                            "invalid utf-8 in {}: {error}",
                            self.path.display()
                        ))));
                    }
                }
            } else {
                None
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
            return Some(Ok(DecodedEvent {
                event,
                event_json,
                timestamp_us,
                source_file_ordinal: self.source_file_ordinal,
                source_row_ordinal,
            }));
        }
    }
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

    #[test]
    fn exact_reader_preserves_capture_order_precision_and_ordinals_across_files() {
        let root = TempDir::new().unwrap();
        let first = root.path().join("first.jsonl");
        let second = root.path().join("second.jsonl");
        std::fs::write(
            &first,
            format!(
                "{}\n{}\n",
                json!({"timestamp":1_704_067_200_000_999_i64,"type":"trade","data":{"price":1}}),
                json!({"timestamp":1_704_067_200_000_500_i64,"type":"trade","data":{"price":2}}),
            ),
        )
        .unwrap();
        std::fs::write(
            &second,
            format!(
                "{}\n",
                json!({"timestamp":1_704_067_200_001_001_i64,"type":"trade","data":{"price":3}}),
            ),
        )
        .unwrap();

        let rows = LocalExactReplayIterator::new(
            vec![first, second],
            1_704_067_200_000_000,
            1_704_067_200_002_000,
            vec![],
            false,
        )
        .collect::<Result<Vec<_>, _>>()
        .unwrap();

        assert_eq!(
            rows.iter().map(|row| row.timestamp_us).collect::<Vec<_>>(),
            vec![
                1_704_067_200_000_999,
                1_704_067_200_000_500,
                1_704_067_200_001_001,
            ]
        );
        assert_eq!(
            rows.iter()
                .map(|row| row.replay_ordinal)
                .collect::<Vec<_>>(),
            vec![0, 1, 2]
        );
        assert_eq!(
            rows.iter()
                .map(|row| (row.source_file_ordinal, row.source_row_ordinal))
                .collect::<Vec<_>>(),
            vec![(0, 0), (0, 1), (1, 0)]
        );
        assert_eq!(
            rows.iter()
                .map(|row| row.event.data["price"].as_i64().unwrap())
                .collect::<Vec<_>>(),
            vec![1, 2, 3]
        );
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
