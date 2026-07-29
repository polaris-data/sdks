//! Blocking facade over the canonical async Polaris client.

use std::{future::Future, path::PathBuf, sync::Arc, time::Duration};

use futures_util::StreamExt;
use serde_json::Value;
use tokio::runtime::{Handle, Runtime};

use crate::{
    BboQuote, CatalogQuery, CatalogResponse, DepthMetricsRow, Diagnostic, DownloadManifestQuery,
    DownloadManifestResponse, HistoricalQuery, ListSnapshotsQuery, OhlcvOutput, OhlcvQuery,
    OrderbookEvent, PointSeriesEvent, PolarisError, RawQuery, RawReplayQuery, RawReplayStream,
    ReplayQuery, ReplayStream, SnapshotEntry, StandardEvent, TradeEvent, VolatilityBar, VolumeBar,
    VwapBar,
};

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
