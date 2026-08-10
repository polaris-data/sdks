use std::{
    fs::File,
    io::{BufRead, BufReader, Read},
    path::{Path, PathBuf},
    pin::Pin,
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

fn read_snapshot_events(
    path: &Path,
    from_us: i64,
    to_us: i64,
    emit: impl FnMut(StandardEvent) -> bool,
) -> Result<(), PolarisError> {
    let file = File::open(path)?;

    if path.extension().and_then(|value| value.to_str()) == Some("zst") {
        let decoder = zstd::stream::read::Decoder::new(file).map_err(|err| {
            PolarisError::Decode(format!("invalid zstd stream in {}: {err}", path.display()))
        })?;
        read_lines(BufReader::new(decoder), from_us, to_us, emit).map_err(|err| match err {
            PolarisError::Io(io) => {
                PolarisError::Decode(format!("invalid zstd stream in {}: {io}", path.display()))
            }
            other => other,
        })
    } else {
        read_lines(BufReader::new(file), from_us, to_us, emit)
    }
}

fn read_lines<R: Read>(
    reader: BufReader<R>,
    from_us: i64,
    to_us: i64,
    mut emit: impl FnMut(StandardEvent) -> bool,
) -> Result<(), PolarisError> {
    let from_ms = from_us / 1_000;
    let to_ms = to_us / 1_000;
    for line in reader.lines() {
        let line = line?;
        if line.trim().is_empty() {
            continue;
        }
        let mut event: StandardEvent = serde_json::from_str(&line)
            .map_err(|err| PolarisError::Decode(format!("invalid ndjson line: {err}")))?;
        if event.timestamp.unsigned_abs() >= 100_000_000_000_000 {
            event.timestamp /= 1_000;
        }
        if event.timestamp >= to_ms {
            break;
        }
        if event.timestamp < from_ms {
            continue;
        }
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
}
