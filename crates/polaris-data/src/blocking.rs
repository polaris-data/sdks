//! Blocking facade over the canonical async Polaris client.

use std::{
    collections::VecDeque,
    fs::{self, File},
    future::Future,
    io::{BufWriter, Write},
    path::PathBuf,
    sync::Arc,
    time::Duration,
};

use chrono::Duration as ChronoDuration;
use futures_util::StreamExt;
use serde_json::Value;
use tokio::runtime::{Handle, Runtime};

use crate::{
    BboQuote, CatalogQuery, CatalogResponse, DepthMetricsRow, Diagnostic, DownloadManifestQuery,
    DownloadManifestResponse, HistoricalQuery, ListSnapshotsQuery, OhlcvOutput, OhlcvQuery,
    OrderbookEvent, PointSeriesEvent, PolarisError, RawQuery, RawReplayQuery, RawReplayStream,
    ReplayQuery, ReplayStream, SnapshotEntry, StandardEvent, TimeInput, TradeEvent, VolatilityBar,
    VolumeBar, VwapBar,
};

const DEFAULT_REPLAY_CHUNK_HOURS: i64 = 24;

/// Configuration for persistent raw-replay caching.
#[derive(Clone, Debug)]
pub struct RawReplayCacheConfig {
    pub enabled: bool,
    pub directory: PathBuf,
}

#[derive(Clone, Debug)]
pub struct PolarisClientBuilder {
    api_key: Option<String>,
    base_url: String,
    timeout: Duration,
    dataset_root: Option<PathBuf>,
}

impl Default for PolarisClientBuilder {
    fn default() -> Self {
        Self {
            api_key: None,
            base_url: "https://api.polaris.supply".to_owned(),
            timeout: Duration::from_secs(30),
            dataset_root: None,
        }
    }
}

impl PolarisClientBuilder {
    pub fn new() -> Self {
        Self::default()
    }

    pub fn api_key(mut self, value: impl Into<String>) -> Self {
        self.api_key = Some(value.into());
        self
    }

    pub fn base_url(mut self, value: impl Into<String>) -> Self {
        self.base_url = value.into();
        self
    }

    pub fn timeout(mut self, value: Duration) -> Self {
        self.timeout = value;
        self
    }

    pub fn dataset_root(mut self, value: impl Into<PathBuf>) -> Self {
        self.dataset_root = Some(value.into());
        self
    }

    pub fn build(self) -> Result<PolarisClient, PolarisError> {
        let mut builder = crate::PolarisClient::builder()
            .base_url(self.base_url)
            .timeout(self.timeout);
        if let Some(api_key) = self.api_key {
            builder = builder.api_key(api_key);
        }
        if let Some(root) = self.dataset_root {
            builder = builder.dataset_root(root);
        }
        PolarisClient::from_async(builder.build()?)
    }
}

#[derive(Clone)]
pub struct PolarisClient {
    inner: crate::PolarisClient,
    runtime: Arc<Runtime>,
}

impl PolarisClient {
    pub fn builder() -> PolarisClientBuilder {
        PolarisClientBuilder::default()
    }

    pub fn from_async(inner: crate::PolarisClient) -> Result<Self, PolarisError> {
        let runtime = Runtime::new().map_err(|err| {
            PolarisError::Request(format!("failed to create Tokio runtime: {err}"))
        })?;
        Ok(Self {
            inner,
            runtime: Arc::new(runtime),
        })
    }

    pub fn as_async(&self) -> &crate::PolarisClient {
        &self.inner
    }

    pub fn dataset_root(&self) -> &std::path::Path {
        self.inner.dataset_root()
    }

    pub fn cache_dir(&self) -> &std::path::Path {
        self.inner.cache_dir()
    }

    pub fn daily_dir(&self) -> &std::path::Path {
        self.inner.daily_dir()
    }

    pub fn take_diagnostics(&self) -> Vec<Diagnostic> {
        self.inner.take_diagnostics()
    }

    fn run<T>(
        &self,
        future: impl Future<Output = Result<T, PolarisError>>,
    ) -> Result<T, PolarisError> {
        if Handle::try_current().is_ok() {
            return Err(PolarisError::BlockingInAsyncRuntime);
        }
        self.runtime.block_on(future)
    }

    pub fn health(&self) -> Result<Value, PolarisError> {
        self.run(self.inner.health())
    }

    pub fn catalog(&self, query: CatalogQuery) -> Result<CatalogResponse, PolarisError> {
        self.run(self.inner.catalog(query))
    }

    pub fn download_manifest(
        &self,
        query: DownloadManifestQuery,
    ) -> Result<DownloadManifestResponse, PolarisError> {
        self.run(self.inner.download_manifest(query))
    }

    pub fn list_snapshots(
        &self,
        query: ListSnapshotsQuery,
    ) -> Result<Vec<SnapshotEntry>, PolarisError> {
        self.run(self.inner.list_snapshots(query))
    }

    pub fn events(&self, query: HistoricalQuery) -> Result<Vec<StandardEvent>, PolarisError> {
        self.run(self.inner.events(query))
    }

    pub fn trades(&self, query: HistoricalQuery) -> Result<Vec<TradeEvent>, PolarisError> {
        self.run(self.inner.trades(query))
    }

    pub fn replay(&self, query: ReplayQuery) -> Result<ReplayIterator, PolarisError> {
        let stream = self.run(self.inner.replay(query))?;
        Ok(ReplayIterator {
            runtime: Arc::clone(&self.runtime),
            stream,
            failed_runtime_check: false,
        })
    }

    pub fn raw(&self, query: RawQuery) -> Result<Vec<Value>, PolarisError> {
        self.run(self.inner.raw(query))
    }

    pub fn raw_replay(&self, query: RawReplayQuery) -> Result<RawReplayIterator, PolarisError> {
        let stream = self.run(self.inner.raw_replay(query))?;
        Ok(RawReplayIterator {
            runtime: Arc::clone(&self.runtime),
            stream,
            failed_runtime_check: false,
        })
    }

    /// Replay raw rows while reusing a completed on-disk cache when configured.
    pub fn raw_replay_cached(
        &self,
        query: RawReplayQuery,
        cache: RawReplayCacheConfig,
    ) -> Result<CachedRawReplayIterator, PolarisError> {
        if !cache.enabled || query.from.is_none() || query.to.is_none() {
            return Ok(CachedRawReplayIterator::Passthrough(
                self.raw_replay(query)?,
            ));
        }

        let final_path = raw_replay_cache_path(&cache.directory, &query)?;
        let compressed_path = final_path.with_extension("jsonl.zst");
        for candidate in [&final_path, &compressed_path] {
            if candidate.exists() {
                return Ok(CachedRawReplayIterator::Cached(
                    crate::decode_ndjson_file(candidate)?.into(),
                ));
            }
        }

        fs::create_dir_all(&cache.directory)?;
        let temp_path = final_path.with_file_name(format!(
            ".{}.part",
            final_path
                .file_name()
                .and_then(|name| name.to_str())
                .unwrap_or("replay.jsonl")
        ));
        let writer = BufWriter::new(File::create(&temp_path)?);
        Ok(CachedRawReplayIterator::Writing {
            inner: self.raw_replay(query)?,
            writer: Some(writer),
            temp_path,
            final_path,
        })
    }

    /// Preserve the Python SDK's historical, ordered 24-hour replay chunking.
    pub fn replay_chunked(
        &self,
        query: ReplayQuery,
    ) -> Result<ChunkedReplayIterator, PolarisError> {
        ChunkedReplayIterator::new(self.clone(), query)
    }

    /// Preserve the Python SDK's historical, ordered 24-hour raw replay chunking.
    pub fn raw_replay_chunked(
        &self,
        query: RawReplayQuery,
        cache: RawReplayCacheConfig,
    ) -> Result<ChunkedRawReplayIterator, PolarisError> {
        ChunkedRawReplayIterator::new(self.clone(), query, cache)
    }

    pub fn ohlcv(&self, query: OhlcvQuery) -> Result<OhlcvOutput, PolarisError> {
        self.run(self.inner.ohlcv(query))
    }

    pub fn l2_snapshots(
        &self,
        query: HistoricalQuery,
    ) -> Result<Vec<OrderbookEvent>, PolarisError> {
        self.run(self.inner.l2_snapshots(query))
    }

    pub fn bbo(&self, query: HistoricalQuery) -> Result<Vec<BboQuote>, PolarisError> {
        self.run(self.inner.bbo(query))
    }

    pub fn funding_rates(
        &self,
        query: HistoricalQuery,
    ) -> Result<Vec<PointSeriesEvent>, PolarisError> {
        self.run(self.inner.funding_rates(query))
    }

    pub fn mark_prices(
        &self,
        query: HistoricalQuery,
    ) -> Result<Vec<PointSeriesEvent>, PolarisError> {
        self.run(self.inner.mark_prices(query))
    }

    pub fn volume(&self, query: OhlcvQuery) -> Result<Vec<VolumeBar>, PolarisError> {
        self.run(self.inner.volume(query))
    }

    pub fn vwap(&self, query: OhlcvQuery) -> Result<Vec<VwapBar>, PolarisError> {
        self.run(self.inner.vwap(query))
    }

    pub fn volatility(&self, query: OhlcvQuery) -> Result<Vec<VolatilityBar>, PolarisError> {
        self.run(self.inner.volatility(query))
    }

    pub fn depth_metrics(
        &self,
        query: HistoricalQuery,
        depth_pct: Option<f64>,
        slippage_notional: Option<f64>,
    ) -> Result<Vec<DepthMetricsRow>, PolarisError> {
        self.run(
            self.inner
                .depth_metrics(query, depth_pct, slippage_notional),
        )
    }
}

pub struct ReplayIterator {
    runtime: Arc<Runtime>,
    stream: ReplayStream,
    failed_runtime_check: bool,
}

impl Iterator for ReplayIterator {
    type Item = Result<StandardEvent, PolarisError>;

    fn next(&mut self) -> Option<Self::Item> {
        if Handle::try_current().is_ok() {
            if self.failed_runtime_check {
                return None;
            }
            self.failed_runtime_check = true;
            return Some(Err(PolarisError::BlockingInAsyncRuntime));
        }
        self.runtime.block_on(self.stream.next())
    }
}

pub struct RawReplayIterator {
    runtime: Arc<Runtime>,
    stream: RawReplayStream,
    failed_runtime_check: bool,
}

pub enum CachedRawReplayIterator {
    Passthrough(RawReplayIterator),
    Cached(VecDeque<Value>),
    Writing {
        inner: RawReplayIterator,
        writer: Option<BufWriter<File>>,
        temp_path: PathBuf,
        final_path: PathBuf,
    },
}

impl Iterator for CachedRawReplayIterator {
    type Item = Result<Value, PolarisError>;

    fn next(&mut self) -> Option<Self::Item> {
        match self {
            Self::Passthrough(inner) => inner.next(),
            Self::Cached(rows) => rows.pop_front().map(Ok),
            Self::Writing {
                inner,
                writer,
                temp_path,
                final_path,
            } => match inner.next() {
                Some(Ok(value)) => {
                    let writer = writer
                        .as_mut()
                        .expect("cache writer exists until completion");
                    if let Err(error) = write_cache_row(writer, &value) {
                        let _ = fs::remove_file(temp_path);
                        return Some(Err(error));
                    }
                    Some(Ok(value))
                }
                Some(Err(error)) => {
                    let _ = fs::remove_file(temp_path);
                    Some(Err(error))
                }
                None => {
                    let mut writer = writer.take()?;
                    if let Err(error) = writer.flush().map_err(PolarisError::Io) {
                        let _ = fs::remove_file(temp_path);
                        return Some(Err(error));
                    }
                    drop(writer);
                    if let Err(error) =
                        fs::rename(&*temp_path, &*final_path).map_err(PolarisError::Io)
                    {
                        let _ = fs::remove_file(temp_path);
                        return Some(Err(error));
                    }
                    *self = Self::Cached(VecDeque::new());
                    None
                }
            },
        }
    }
}

impl Drop for CachedRawReplayIterator {
    fn drop(&mut self) {
        if let Self::Writing { temp_path, .. } = self {
            let _ = fs::remove_file(temp_path);
        }
    }
}

pub struct ChunkedReplayIterator {
    client: PolarisClient,
    source: String,
    market: String,
    next_start_us: i64,
    end_us: i64,
    allow_gaps: bool,
    current: Option<ReplayIterator>,
}

impl ChunkedReplayIterator {
    fn new(client: PolarisClient, query: ReplayQuery) -> Result<Self, PolarisError> {
        let (next_start_us, end_us) = required_range(query.from.as_ref(), query.to.as_ref())?;
        Ok(Self {
            client,
            source: query.source,
            market: query.market,
            next_start_us,
            end_us,
            allow_gaps: query.allow_gaps,
            current: None,
        })
    }
}

impl Iterator for ChunkedReplayIterator {
    type Item = Result<StandardEvent, PolarisError>;

    fn next(&mut self) -> Option<Self::Item> {
        loop {
            if let Some(current) = self.current.as_mut() {
                if let Some(row) = current.next() {
                    return Some(row);
                }
                self.current = None;
            }
            if self.next_start_us >= self.end_us {
                return None;
            }
            let chunk_end_us = (self.next_start_us + chunk_micros()).min(self.end_us);
            let query = ReplayQuery {
                source: self.source.clone(),
                market: self.market.clone(),
                from: Some(TimeInput::EpochMicros(self.next_start_us)),
                to: Some(TimeInput::EpochMicros(chunk_end_us)),
                allow_gaps: self.allow_gaps,
            };
            self.next_start_us = chunk_end_us;
            match self.client.replay(query) {
                Ok(iterator) => self.current = Some(iterator),
                Err(error) => return Some(Err(error)),
            }
        }
    }
}

pub struct ChunkedRawReplayIterator {
    client: PolarisClient,
    source: String,
    market: String,
    next_start_us: i64,
    end_us: i64,
    limit: usize,
    cache: RawReplayCacheConfig,
    current: Option<CachedRawReplayIterator>,
}

impl ChunkedRawReplayIterator {
    fn new(
        client: PolarisClient,
        query: RawReplayQuery,
        cache: RawReplayCacheConfig,
    ) -> Result<Self, PolarisError> {
        let (next_start_us, end_us) = required_range(query.from.as_ref(), query.to.as_ref())?;
        Ok(Self {
            client,
            source: query.source,
            market: query.market,
            next_start_us,
            end_us,
            limit: query.limit,
            cache,
            current: None,
        })
    }
}

impl Iterator for ChunkedRawReplayIterator {
    type Item = Result<Value, PolarisError>;

    fn next(&mut self) -> Option<Self::Item> {
        loop {
            if let Some(current) = self.current.as_mut() {
                if let Some(row) = current.next() {
                    return Some(row);
                }
                self.current = None;
            }
            if self.next_start_us >= self.end_us {
                return None;
            }
            let chunk_end_us = (self.next_start_us + chunk_micros()).min(self.end_us);
            let query = RawReplayQuery {
                source: self.source.clone(),
                market: self.market.clone(),
                from: Some(TimeInput::EpochMicros(self.next_start_us)),
                to: Some(TimeInput::EpochMicros(chunk_end_us)),
                limit: self.limit,
            };
            self.next_start_us = chunk_end_us;
            match self.client.raw_replay_cached(query, self.cache.clone()) {
                Ok(iterator) => self.current = Some(iterator),
                Err(error) => return Some(Err(error)),
            }
        }
    }
}

fn required_range(
    from: Option<&TimeInput>,
    to: Option<&TimeInput>,
) -> Result<(i64, i64), PolarisError> {
    let from = from.ok_or_else(|| {
        PolarisError::InvalidResponse("from and to are required for chunked replay".to_owned())
    })?;
    let to = to.ok_or_else(|| {
        PolarisError::InvalidResponse("from and to are required for chunked replay".to_owned())
    })?;
    let from_us = crate::time::to_epoch_micros(from)?;
    let to_us = crate::time::to_epoch_micros(to)?;
    if from_us >= to_us {
        return Err(PolarisError::InvalidResponse(
            "from must be before to".to_owned(),
        ));
    }
    Ok((from_us, to_us))
}

fn chunk_micros() -> i64 {
    ChronoDuration::hours(DEFAULT_REPLAY_CHUNK_HOURS)
        .num_microseconds()
        .expect("24 hours fits in microseconds")
}

fn raw_replay_cache_path(
    directory: &std::path::Path,
    query: &RawReplayQuery,
) -> Result<PathBuf, PolarisError> {
    let from = crate::time::to_iso8601(query.from.as_ref().expect("cache requires from"))?;
    let to = crate::time::to_iso8601(query.to.as_ref().expect("cache requires to"))?;
    Ok(directory.join(format!(
        "{}_{}_{}_{}_raw.jsonl",
        safe_filename_fragment(&query.source),
        safe_filename_fragment(&query.market),
        safe_filename_fragment(&from.replace(':', "-")),
        safe_filename_fragment(&to.replace(':', "-")),
    )))
}

fn safe_filename_fragment(value: &str) -> String {
    let cleaned = value
        .chars()
        .map(|character| match character {
            '-' | '_' | '.' => character,
            value if value.is_alphanumeric() => value,
            _ => '_',
        })
        .collect::<String>();
    let trimmed = cleaned.trim_matches('_');
    if trimmed.is_empty() {
        "dataset".to_owned()
    } else {
        trimmed.to_owned()
    }
}

fn write_cache_row(writer: &mut BufWriter<File>, row: &Value) -> Result<(), PolarisError> {
    serde_json::to_writer(&mut *writer, row).map_err(|error| {
        PolarisError::Request(format!("failed to serialize replay cache row: {error}"))
    })?;
    writer.write_all(b"\n").map_err(PolarisError::Io)
}

impl Iterator for RawReplayIterator {
    type Item = Result<Value, PolarisError>;

    fn next(&mut self) -> Option<Self::Item> {
        if Handle::try_current().is_ok() {
            if self.failed_runtime_check {
                return None;
            }
            self.failed_runtime_check = true;
            return Some(Err(PolarisError::BlockingInAsyncRuntime));
        }
        self.runtime.block_on(self.stream.next())
    }
}
