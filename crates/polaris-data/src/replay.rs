use std::{
    fs::File,
    io::{BufRead, BufReader, Read},
    path::Path,
};

use async_stream::try_stream;

use crate::{
    OrderbookBuilder,
    errors::PolarisError,
    models::{ReplayStream, StandardEvent},
};

pub(crate) fn replay_stream(
    paths: Vec<std::path::PathBuf>,
    from_us: i64,
    to_us: i64,
    source: String,
    market: String,
    materialize_orderbooks: bool,
    mut gaps: Vec<(i64, i64)>,
) -> ReplayStream {
    Box::pin(try_stream! {
        let mut orderbooks = OrderbookBuilder::new();
        gaps.sort_unstable();
        let mut gaps = gaps.into_iter().peekable();
        for path in paths {
            let events = tokio::task::spawn_blocking({
                let path = path.clone();
                let source = source.clone();
                let market = market.clone();
                move || read_snapshot_events(&path, from_us, to_us, &source, &market)
            })
            .await
            .map_err(|err| PolarisError::Request(format!("snapshot reader join failed: {err}")))??;

            for event in events {
                while gaps
                    .peek()
                    .is_some_and(|(_, end_us)| event.timestamp.saturating_mul(1_000) >= *end_us)
                {
                    orderbooks.clear();
                    gaps.next();
                }
                if materialize_orderbooks {
                    if let Some(event) = orderbooks.apply(event)? {
                        yield event;
                    }
                } else {
                    yield event;
                }
            }
        }
    })
}

pub(crate) fn read_snapshot_events(
    path: &Path,
    from_us: i64,
    to_us: i64,
    _source: &str,
    _market: &str,
) -> Result<Vec<StandardEvent>, PolarisError> {
    let file = File::open(path)?;
    let mut events = Vec::new();

    if path.extension().and_then(|value| value.to_str()) == Some("zst") {
        let decoder = zstd::stream::read::Decoder::new(file).map_err(|err| {
            PolarisError::Decode(format!("invalid zstd stream in {}: {err}", path.display()))
        })?;
        read_lines(BufReader::new(decoder), from_us, to_us, &mut events).map_err(
            |err| match err {
                PolarisError::Io(io) => {
                    PolarisError::Decode(format!("invalid zstd stream in {}: {io}", path.display()))
                }
                other => other,
            },
        )?;
    } else {
        read_lines(BufReader::new(file), from_us, to_us, &mut events)?;
    }

    Ok(events)
}

fn read_lines<R: Read>(
    reader: BufReader<R>,
    from_us: i64,
    to_us: i64,
    out: &mut Vec<StandardEvent>,
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
        out.push(event);
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

        let rows = replay_stream(
            vec![first, second],
            0,
            10_000_000,
            "s".to_owned(),
            "m".to_owned(),
            true,
            vec![(2_000_000, 3_000_000)],
        )
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
}
