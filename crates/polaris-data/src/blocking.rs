//! Blocking facade over the canonical async Polaris client.

use std::{future::Future, path::PathBuf, sync::Arc, time::Duration};

use futures_util::StreamExt;
use serde_json::Value;
use tokio::runtime::{Handle, Runtime};

use crate::{
    CatalogCount, CatalogQuery, CatalogResponse, Diagnostic, EventsQuery, FundingRateRow,
    HistoricalRowsQuery, HistoricalStream, IntentRow, IntentRowsQuery, L2OrderbooksQuery,
    L2UpdatesQuery, MixedEventRow, OhlcvRow, OhlcvRowsQuery, OptionTickerRow,
    OptionTickerRowsQuery, OrderbookL2Row, PolarisError, RawCaptureRow, RawChannelQuery, RawQuery,
    RealtimeStream, StandardEvent, StreamQuery, TradeRow,
};

const HISTORICAL_CHANNEL_CAPACITY: usize = 16;

#[derive(Clone, Debug)]
pub struct PolarisClientBuilder {
    api_key: Option<String>,
    base_url: String,
    stream_url: Option<String>,
    timeout: Duration,
    dataset_root: Option<PathBuf>,
}

impl Default for PolarisClientBuilder {
    fn default() -> Self {
        Self {
            api_key: None,
            base_url: "https://api.polaris.supply".to_owned(),
            stream_url: None,
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

    pub fn stream_url(mut self, value: impl Into<String>) -> Self {
        self.stream_url = Some(value.into());
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
        if let Some(stream_url) = self.stream_url {
            builder = builder.stream_url(stream_url);
        }
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

    pub fn instruments(
        &self,
        query: crate::InstrumentsQuery,
    ) -> Result<crate::InstrumentsResponse, PolarisError> {
        self.run(self.inner.instruments(query))
    }

    pub fn count(&self) -> Result<CatalogCount, PolarisError> {
        self.run(self.inner.count())
    }

    pub fn trades(
        &self,
        query: HistoricalRowsQuery,
    ) -> Result<HistoricalIterator<TradeRow>, PolarisError> {
        let stream = self.run(self.inner.trades(query))?;
        Ok(self.historical_iterator(stream, HISTORICAL_CHANNEL_CAPACITY))
    }

    pub fn events(
        &self,
        query: EventsQuery,
    ) -> Result<HistoricalIterator<MixedEventRow>, PolarisError> {
        let stream = self.run(self.inner.events(query))?;
        Ok(self.historical_iterator(stream, HISTORICAL_CHANNEL_CAPACITY))
    }

    pub fn intents(
        &self,
        query: IntentRowsQuery,
    ) -> Result<HistoricalIterator<IntentRow>, PolarisError> {
        let stream = self.run(self.inner.intents(query))?;
        Ok(self.historical_iterator(stream, HISTORICAL_CHANNEL_CAPACITY))
    }

    pub fn option_tickers(
        &self,
        query: OptionTickerRowsQuery,
    ) -> Result<HistoricalIterator<OptionTickerRow>, PolarisError> {
        let stream = self.run(self.inner.option_tickers(query))?;
        Ok(self.historical_iterator(stream, HISTORICAL_CHANNEL_CAPACITY))
    }

    pub fn perpetual_tickers(
        &self,
        query: HistoricalRowsQuery,
    ) -> Result<HistoricalIterator<FundingRateRow>, PolarisError> {
        let stream = self.run(self.inner.perpetual_tickers(query))?;
        Ok(self.historical_iterator(stream, HISTORICAL_CHANNEL_CAPACITY))
    }

    pub fn stream(&self, query: StreamQuery) -> Result<RealtimeIterator, PolarisError> {
        let stream = self.run(self.inner.stream(query))?;
        Ok(RealtimeIterator {
            runtime: Arc::clone(&self.runtime),
            stream,
            failed_runtime_check: false,
        })
    }

    pub fn raw(&self, query: RawQuery) -> Result<Vec<Value>, PolarisError> {
        self.run(self.inner.raw(query))
    }

    pub fn raw_channel(
        &self,
        query: RawChannelQuery,
    ) -> Result<HistoricalIterator<RawCaptureRow>, PolarisError> {
        let stream = self.run(self.inner.raw_channel(query))?;
        Ok(self.historical_iterator(stream, HISTORICAL_CHANNEL_CAPACITY))
    }

    pub fn ohlcv(
        &self,
        query: OhlcvRowsQuery,
    ) -> Result<HistoricalIterator<OhlcvRow>, PolarisError> {
        let stream = self.run(self.inner.ohlcv(query))?;
        Ok(self.historical_iterator(stream, HISTORICAL_CHANNEL_CAPACITY))
    }

    pub fn l2_snapshots(
        &self,
        query: L2OrderbooksQuery,
    ) -> Result<HistoricalIterator<OrderbookL2Row>, PolarisError> {
        let stream = self.run(self.inner.l2_snapshots(query))?;
        Ok(self.historical_iterator(stream, HISTORICAL_CHANNEL_CAPACITY))
    }

    /// Return flat stateless source snapshots and deltas from the direct API.
    pub fn l2_updates(
        &self,
        query: L2UpdatesQuery,
    ) -> Result<HistoricalIterator<OrderbookL2Row>, PolarisError> {
        let stream = self.run(self.inner.l2_updates(query))?;
        Ok(self.historical_iterator(stream, HISTORICAL_CHANNEL_CAPACITY))
    }

    pub fn funding_rates(
        &self,
        query: HistoricalRowsQuery,
    ) -> Result<HistoricalIterator<FundingRateRow>, PolarisError> {
        let stream = self.run(self.inner.funding_rates(query))?;
        Ok(self.historical_iterator(stream, HISTORICAL_CHANNEL_CAPACITY))
    }

    fn historical_iterator<T: Send + 'static>(
        &self,
        mut stream: HistoricalStream<T>,
        capacity: usize,
    ) -> HistoricalIterator<T> {
        let (sender, receiver) = tokio::sync::mpsc::channel(capacity);
        self.runtime.spawn(async move {
            while let Some(item) = stream.next().await {
                if sender.send(item).await.is_err() {
                    break;
                }
            }
        });
        HistoricalIterator {
            backend: HistoricalIteratorBackend::Channel {
                _runtime: Arc::clone(&self.runtime),
                receiver,
            },
            failed_runtime_check: false,
        }
    }
}

pub struct HistoricalIterator<T> {
    backend: HistoricalIteratorBackend<T>,
    failed_runtime_check: bool,
}

enum HistoricalIteratorBackend<T> {
    Channel {
        _runtime: Arc<Runtime>,
        receiver: tokio::sync::mpsc::Receiver<Result<T, PolarisError>>,
    },
}

pub struct RealtimeIterator {
    runtime: Arc<Runtime>,
    stream: RealtimeStream,
    failed_runtime_check: bool,
}

pub enum RealtimePoll {
    Event(Result<StandardEvent, PolarisError>),
    Pending,
    Closed,
}

impl RealtimeIterator {
    pub fn next_timeout(&mut self, timeout: Duration) -> RealtimePoll {
        if Handle::try_current().is_ok() {
            if self.failed_runtime_check {
                return RealtimePoll::Closed;
            }
            self.failed_runtime_check = true;
            return RealtimePoll::Event(Err(PolarisError::BlockingInAsyncRuntime));
        }
        match self
            .runtime
            .block_on(tokio::time::timeout(timeout, self.stream.next()))
        {
            Ok(Some(event)) => RealtimePoll::Event(event),
            Ok(None) => RealtimePoll::Closed,
            Err(_) => RealtimePoll::Pending,
        }
    }
}

impl Iterator for RealtimeIterator {
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

impl<T> Iterator for HistoricalIterator<T> {
    type Item = Result<T, PolarisError>;

    fn next(&mut self) -> Option<Self::Item> {
        if Handle::try_current().is_ok() {
            if self.failed_runtime_check {
                return None;
            }
            self.failed_runtime_check = true;
            return Some(Err(PolarisError::BlockingInAsyncRuntime));
        }
        match &mut self.backend {
            HistoricalIteratorBackend::Channel { receiver, .. } => receiver.blocking_recv(),
        }
    }
}
