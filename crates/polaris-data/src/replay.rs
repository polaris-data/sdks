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
    models::{LegacyStandardEvent, ReplayStream, StandardEvent, StandardEventV2},
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
                        exact_event.set_legacy_timestamp(decoded.timestamp_us);
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
    pending: Option<Vec<u8>>,
    schema: SnapshotSchema,
    metadata_source: Option<String>,
    metadata_market: Option<String>,
    from_us: i64,
    to_us: i64,
    source_file_ordinal: u32,
    next_row_ordinal: u64,
    capture_json: bool,
    finished: bool,
}

#[derive(Clone, Copy)]
enum SnapshotSchema {
    Legacy,
    V2,
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
        let mut snapshot = Self {
            reader,
            path,
            compressed,
            buffer: Vec::with_capacity(8 * 1024),
            pending: None,
            schema: SnapshotSchema::Legacy,
            metadata_source: None,
            metadata_market: None,
            from_us,
            to_us,
            source_file_ordinal,
            next_row_ordinal: 0,
            capture_json,
            finished: false,
        };
        snapshot.initialize_schema()?;
        Ok(snapshot)
    }

    fn initialize_schema(&mut self) -> Result<(), PolarisError> {
        loop {
            self.buffer.clear();
            match self.reader.read_until(b'\n', &mut self.buffer) {
                Ok(0) => {
                    self.finished = true;
                    return Ok(());
                }
                Ok(_) => {}
                Err(error) => return Err(self.map_io_error(error)),
            }
            trim_line_ending(&mut self.buffer);
            if self.buffer.iter().all(u8::is_ascii_whitespace) {
                continue;
            }
            let value: serde_json::Value =
                serde_json::from_slice(&self.buffer).map_err(|error| {
                    PolarisError::Decode(format!(
                        "invalid ndjson line in {}: {error}",
                        self.path.display()
                    ))
                })?;
            if value.get("type").and_then(serde_json::Value::as_str) == Some("metadata") {
                let version = value
                    .get("data")
                    .and_then(serde_json::Value::as_object)
                    .and_then(|data| data.get("schema_version"))
                    .and_then(serde_json::Value::as_str)
                    .ok_or_else(|| {
                        PolarisError::Decode(format!(
                            "metadata in {} is missing data.schema_version",
                            self.path.display()
                        ))
                    })?;
                if version != "v2" {
                    return Err(PolarisError::Decode(format!(
                        "unsupported standard event schema version '{version}' in {}",
                        self.path.display()
                    )));
                }
                self.schema = SnapshotSchema::V2;
                self.metadata_source = value
                    .get("source")
                    .and_then(serde_json::Value::as_str)
                    .filter(|value| !value.is_empty())
                    .map(ToOwned::to_owned);
                self.metadata_market = value
                    .get("market")
                    .and_then(serde_json::Value::as_str)
                    .filter(|value| !value.is_empty())
                    .map(ToOwned::to_owned);
                self.next_row_ordinal = 1;
            } else {
                self.pending = Some(self.buffer.clone());
            }
            return Ok(());
        }
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
            if let Some(pending) = self.pending.take() {
                self.buffer = pending;
            } else {
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
                trim_line_ending(&mut self.buffer);
            }
            if self.buffer.iter().all(u8::is_ascii_whitespace) {
                continue;
            }
            let source_row_ordinal = self.next_row_ordinal;
            self.next_row_ordinal = self.next_row_ordinal.saturating_add(1);
            let value: serde_json::Value = match serde_json::from_slice(&self.buffer) {
                Ok(value) => value,
                Err(error) => {
                    self.finished = true;
                    return Some(Err(PolarisError::Decode(format!(
                        "invalid ndjson line in {}: {error}",
                        self.path.display()
                    ))));
                }
            };
            let event = match self.schema {
                SnapshotSchema::Legacy => {
                    serde_json::from_value::<LegacyStandardEvent>(value).map(StandardEvent::Legacy)
                }
                SnapshotSchema::V2 => {
                    let Some(object) = value.as_object() else {
                        self.finished = true;
                        return Some(Err(PolarisError::Decode(format!(
                            "expected each v2 NDJSON row in {} to be an object",
                            self.path.display()
                        ))));
                    };
                    if object.get("type").and_then(serde_json::Value::as_str) == Some("metadata") {
                        self.finished = true;
                        return Some(Err(PolarisError::Decode(format!(
                            "standard snapshot {} contains non-leading metadata",
                            self.path.display()
                        ))));
                    }
                    let missing = [
                        "collector_timestamp",
                        "collector_sequence",
                        "exchange_timestamp",
                        "exchange_sequence",
                    ]
                    .into_iter()
                    .find(|key| !object.contains_key(*key));
                    if let Some(field) = missing {
                        self.finished = true;
                        return Some(Err(PolarisError::Decode(format!(
                            "v2 standard event in {} is missing {field}",
                            self.path.display()
                        ))));
                    }
                    serde_json::from_value::<StandardEventV2>(value).and_then(|mut event| {
                        if event.source.is_empty() {
                            if let Some(source) = &self.metadata_source {
                                event.source.clone_from(source);
                            }
                        }
                        if event.market.is_empty() {
                            if let Some(market) = &self.metadata_market {
                                event.market.clone_from(market);
                            }
                        }
                        validate_v2_event(&event).map_err(serde::de::Error::custom)?;
                        Ok(StandardEvent::V2(event))
                    })
                }
            };
            let mut event = match event {
                Ok(event) => event,
                Err(error) => {
                    self.finished = true;
                    return Some(Err(PolarisError::Decode(format!(
                        "invalid {:?} standard event in {}: {error}",
                        match self.schema {
                            SnapshotSchema::Legacy => "legacy",
                            SnapshotSchema::V2 => "v2",
                        },
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
            let timestamp_us = match &mut event {
                StandardEvent::Legacy(event) => {
                    if event.timestamp.unsigned_abs() >= 100_000_000_000_000 {
                        let timestamp_us = event.timestamp;
                        event.timestamp /= 1_000;
                        timestamp_us
                    } else {
                        event.timestamp.saturating_mul(1_000)
                    }
                }
                StandardEvent::V2(event) => event.collector_timestamp.saturating_mul(1_000),
            };
            if timestamp_us >= self.to_us {
                continue;
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

fn trim_line_ending(buffer: &mut Vec<u8>) {
    while buffer
        .last()
        .is_some_and(|byte| matches!(byte, b'\n' | b'\r'))
    {
        buffer.pop();
    }
}

fn validate_v2_event(event: &StandardEventV2) -> Result<(), String> {
    if event.source.is_empty() || event.market.is_empty() || event.event_type.is_empty() {
        return Err("v2 standard event requires source, market, and type".to_owned());
    }
    let data = event
        .data
        .as_object()
        .ok_or_else(|| "v2 standard event data must be an object".to_owned())?;
    match event.event_type.as_str() {
        "trade" => {
            if !data.contains_key("order_id")
                || !data.get("price").is_some_and(serde_json::Value::is_number)
                || !data
                    .get("quantity")
                    .is_some_and(serde_json::Value::is_number)
                || !data.contains_key("side")
            {
                return Err("invalid v2 trade payload".to_owned());
            }
            if data
                .get("order_id")
                .is_some_and(|value| !value.is_null() && !value.is_string())
                || data.get("side").is_some_and(|value| {
                    !value.is_null() && !matches!(value.as_str(), Some("buy" | "sell"))
                })
            {
                return Err("invalid v2 trade nullable fields".to_owned());
            }
        }
        "orderbook" => {
            if !data
                .get("is_snapshot")
                .is_some_and(serde_json::Value::is_boolean)
                || !data.get("bids").is_some_and(serde_json::Value::is_array)
                || !data.get("asks").is_some_and(serde_json::Value::is_array)
            {
                return Err("invalid v2 orderbook payload".to_owned());
            }
        }
        "point" => {
            if !data.get("series").is_some_and(serde_json::Value::is_string)
                || !data.get("value").is_some_and(serde_json::Value::is_string)
            {
                return Err("invalid v2 point payload".to_owned());
            }
        }
        "record" => {
            if !data.get("series").is_some_and(serde_json::Value::is_string)
                || !data.get("values").is_some_and(serde_json::Value::is_object)
            {
                return Err("invalid v2 record payload".to_owned());
            }
        }
        "option_ticker" => {
            if event
                .extra
                .get("instrument")
                .and_then(serde_json::Value::as_str)
                .is_none_or(|instrument| instrument.is_empty())
            {
                return Err("option_ticker instrument must be non-empty".to_owned());
            }
        }
        "intent" => {}
        other => return Err(format!("unsupported v2 standard event type '{other}'")),
    }
    Ok(())
}

#[cfg(test)]
mod tests {
    use futures_util::StreamExt;
    use serde_json::json;
    use tempfile::TempDir;

    use super::*;

    const LEGACY_FIXTURE: &str = include_str!("../../../tests/fixtures/events/legacy-v1.jsonl");
    const V2_FIXTURE: &str = include_str!("../../../tests/fixtures/events/schema-v2.jsonl");
    const PROPAMM_FIXTURE: &str =
        include_str!("../../../tests/fixtures/events/propamm-fermiswap-v2.jsonl");

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
        assert_eq!(rows[0].event_type(), "orderbook");
        assert_eq!(rows[1].event_type(), "trade");
        assert_eq!(
            rows[2].data()["bids"],
            json!([{"price":90.0,"quantity":2.0}])
        );
    }

    #[test]
    fn v2_reader_consumes_metadata_preserves_order_and_scans_after_to_regression() {
        let root = TempDir::new().unwrap();
        let path = root.path().join("v2.jsonl");
        std::fs::write(&path, V2_FIXTURE).unwrap();

        let all = ReplayFileIterator::new(
            vec![path.clone()],
            1_704_067_200_000_000,
            1_704_067_201_000_000,
            true,
        )
        .collect::<Result<Vec<_>, _>>()
        .unwrap();
        assert_eq!(all.len(), 7);
        assert_eq!(
            all.iter()
                .map(|row| row.source_row_ordinal)
                .collect::<Vec<_>>(),
            vec![1, 2, 3, 4, 5, 6, 8]
        );
        assert_eq!(
            all.iter()
                .map(|row| row.event.timestamp())
                .collect::<Vec<_>>(),
            vec![
                1_704_067_200_100,
                1_704_067_200_300,
                1_704_067_200_200,
                1_704_067_200_400,
                1_704_067_200_500,
                1_704_067_200_450,
                1_704_067_200_550,
            ]
        );
        assert!(
            all.iter()
                .all(|row| matches!(row.event, StandardEvent::V2(_)))
        );
        let point = crate::PolarisClient::try_parse_point_series(
            all[3].event.clone(),
            &["mark_price", "mark_px"],
        )
        .expect("numeric-string point payload should decode");
        assert_eq!(point.data().value, 100.75);

        let filtered = ReplayFileIterator::new(
            vec![path.clone()],
            1_704_067_200_150_000,
            1_704_067_200_250_000,
            false,
        )
        .collect::<Result<Vec<_>, _>>()
        .unwrap();
        assert_eq!(filtered.len(), 1);
        assert_eq!(filtered[0].event.event_type(), "trade");

        let after_out_of_range = ReplayFileIterator::new(
            vec![path],
            1_704_067_200_525_000,
            1_704_067_200_575_000,
            false,
        )
        .collect::<Result<Vec<_>, _>>()
        .unwrap();
        assert_eq!(after_out_of_range.len(), 1);
        assert_eq!(after_out_of_range[0].source_row_ordinal, 8);
    }

    #[test]
    fn v2_reader_inherits_market_from_metadata_without_changing_exact_json() {
        let root = TempDir::new().unwrap();
        let path = root.path().join("propamm.jsonl");
        std::fs::write(&path, PROPAMM_FIXTURE).unwrap();

        let rows = ReplayFileIterator::new(vec![path], i64::MIN, i64::MAX, true)
            .collect::<Result<Vec<_>, _>>()
            .unwrap();

        assert_eq!(rows.len(), 2);
        assert!(rows.iter().all(|row| row.event.market() == "ethereum"));
        assert!(
            !rows[1]
                .event_json
                .as_deref()
                .unwrap()
                .contains("\"market\"")
        );
    }

    #[test]
    fn shared_legacy_and_v2_fixtures_replay_as_one_mixed_range() {
        let root = TempDir::new().unwrap();
        let legacy = root.path().join("legacy.jsonl");
        let v2 = root.path().join("v2.jsonl");
        std::fs::write(&legacy, LEGACY_FIXTURE).unwrap();
        std::fs::write(&v2, V2_FIXTURE).unwrap();

        let rows = ReplayFileIterator::new(
            vec![legacy, v2],
            1_704_067_200_000_000,
            1_704_067_201_000_000,
            false,
        )
        .collect::<Result<Vec<_>, _>>()
        .unwrap();

        assert_eq!(rows.len(), 11);
        assert!(
            rows[..4]
                .iter()
                .all(|row| matches!(row.event, StandardEvent::Legacy(_)))
        );
        assert!(
            rows[4..]
                .iter()
                .all(|row| matches!(row.event, StandardEvent::V2(_)))
        );
        assert_eq!(rows[4].source_row_ordinal, 1);
    }

    #[test]
    fn v2_materialized_delta_keeps_delta_identity_and_complete_book() {
        let root = TempDir::new().unwrap();
        let path = root.path().join("v2.jsonl");
        std::fs::write(&path, V2_FIXTURE).unwrap();
        let rows = LocalReplayIterator::new(
            vec![path],
            1_704_067_200_000_000,
            1_704_067_201_000_000,
            Vec::new(),
            true,
        )
        .collect::<Result<Vec<_>, _>>()
        .unwrap();
        assert_eq!(rows.len(), 7);
        assert_eq!(rows[1].data()["is_snapshot"], false);
        assert_eq!(
            rows[1].data()["bids"],
            json!([{"price":100.0,"quantity":4.0}])
        );
        assert_eq!(
            rows[1].data()["asks"],
            json!([{"price":102.0,"quantity":5.0}])
        );
    }

    #[test]
    fn unsupported_metadata_version_fails_before_events() {
        let root = TempDir::new().unwrap();
        let path = root.path().join("unknown.jsonl");
        std::fs::write(
            &path,
            "{\"type\":\"metadata\",\"data\":{\"schema_version\":\"v3\"}}\n{\"timestamp\":1}\n",
        )
        .unwrap();
        let error = match ReplayFileIterator::new(vec![path], i64::MIN, i64::MAX, false).next() {
            Some(Err(error)) => error,
            _ => panic!("expected unsupported version error"),
        };
        assert!(
            error
                .to_string()
                .contains("unsupported standard event schema version 'v3'")
        );
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
        assert_eq!(rows[0].timestamp(), 1_704_067_200_000_i64);
        assert_eq!(rows[0].data()["price"], 1);
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
                .map(|row| row.event.data()["price"].as_i64().unwrap())
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
