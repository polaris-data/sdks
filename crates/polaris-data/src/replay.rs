use std::{
    fs::File,
    io::{BufRead, BufReader, Read},
    path::Path,
};

use async_stream::try_stream;

use crate::{
    errors::PolarisError,
    models::{ReplayStream, StandardEvent},
};

pub(crate) fn replay_stream(
    paths: Vec<std::path::PathBuf>,
    from_us: i64,
    to_us: i64,
    source: String,
    market: String,
) -> ReplayStream {
    Box::pin(try_stream! {
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
                yield event;
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
