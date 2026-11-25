use std::{collections::BTreeMap, sync::Arc, time::Duration};

use serde::{Deserialize, Serialize, de::DeserializeOwned};
use serde_json::Value;

use crate::{DataQuery, DataResponse, PolarisError, SubscriptionRequest, TableDataMessage, ffi};

pub type Row = BTreeMap<String, Value>;
pub type HistoricalStream<T> =
    std::pin::Pin<Box<dyn futures_core::Stream<Item = Result<T, PolarisError>> + Send>>;
pub type RealtimeStream = HistoricalStream<TableDataMessage>;

#[derive(Clone, Debug, Serialize, Deserialize)]
pub struct StreamQuery {
    pub subscriptions: Vec<SubscriptionRequest>,
}

#[derive(Clone)]
pub struct Client {
    pub(crate) options: Options,
}

#[derive(Clone, Serialize)]
pub(crate) struct Options {
    api_key: Option<String>,
    base_url: String,
    stream_url: Option<String>,
    timeout_ms: u32,
}

impl Default for Options {
    fn default() -> Self {
        Self {
            api_key: None,
            base_url: "https://api.polaris.supply".to_owned(),
            stream_url: None,
            timeout_ms: 30_000,
        }
    }
}

#[derive(Clone, Default)]
pub struct PolarisClientBuilder(Options);

impl PolarisClientBuilder {
    pub fn new() -> Self {
        Self::default()
    }

    pub fn api_key(mut self, value: impl Into<String>) -> Self {
        self.0.api_key = Some(value.into());
        self
    }

    pub fn base_url(mut self, value: impl Into<String>) -> Self {
        self.0.base_url = value.into();
        self
    }

    pub fn stream_url(mut self, value: impl Into<String>) -> Self {
        self.0.stream_url = Some(value.into());
        self
    }

    pub fn timeout(mut self, value: Duration) -> Self {
        self.0.timeout_ms = value.as_millis().clamp(1, u128::from(u32::MAX)) as u32;
        self
    }

    pub fn build(mut self) -> Result<Client, PolarisError> {
        if self.0.api_key.is_none() {
            self.0.api_key = std::env::var("POLARIS_API_KEY").ok();
        }
        let client = Client { options: self.0 };
        ffi::validate(&client.input("validate", ())?)?;
        Ok(client)
    }
}

impl Client {
    pub fn builder() -> PolarisClientBuilder {
        PolarisClientBuilder::default()
    }

    pub(crate) fn input(
        &self,
        operation: &str,
        query: impl Serialize,
    ) -> Result<Vec<u8>, PolarisError> {
        #[derive(Serialize)]
        struct Input<'a, Q> {
            #[serde(flatten)]
            options: &'a Options,
            operation: &'a str,
            query: Q,
        }
        serde_json::to_vec(&Input {
            options: &self.options,
            operation,
            query,
        })
        .map_err(|error| PolarisError::Request(error.to_string()))
    }

    pub(crate) async fn request<T: DeserializeOwned + Send + 'static>(
        &self,
        operation: &str,
        query: impl Serialize,
    ) -> Result<T, PolarisError> {
        let input = self.input(operation, query)?;
        tokio::task::spawn_blocking(move || ffi::request(&input))
            .await
            .map_err(task_error)?
    }

    pub fn data_rows(&self, mut query: DataQuery) -> HistoricalStream<Row> {
        let client = self.clone();
        Box::pin(async_stream::try_stream! {
            let mut tracker = ffi::CursorSet::new(query.cursor.as_deref())?;
            loop {
                let page: DataResponse = client.data(query.clone()).await?;
                if page.has_more {
                    tracker.remember(page.next_cursor.as_deref().expect("validated native data page"))?;
                }
                for row in page.data {
                    yield row;
                }
                if !page.has_more {
                    break;
                }
                query.cursor = page.next_cursor;
            }
        })
    }

    pub async fn stream(&self, query: StreamQuery) -> Result<RealtimeStream, PolarisError> {
        let input = self.input("stream", query)?;
        let stream = AsyncStream::new(input)?;
        Ok(Box::pin(async_stream::try_stream! {
            while let Some(page) = stream.next().await? {
                yield page;
            }
        }))
    }
}

struct AsyncStream(Arc<ffi::Stream>);

impl AsyncStream {
    fn new(input: Vec<u8>) -> Result<Self, PolarisError> {
        Ok(Self(Arc::new(ffi::Stream::new(&input)?)))
    }

    async fn next(&self) -> Result<Option<TableDataMessage>, PolarisError> {
        let stream = Arc::clone(&self.0);
        tokio::task::spawn_blocking(move || stream.next())
            .await
            .map_err(task_error)?
    }
}

impl Drop for AsyncStream {
    fn drop(&mut self) {
        self.0.cancel();
    }
}

fn task_error(error: tokio::task::JoinError) -> PolarisError {
    PolarisError::Request(error.to_string())
}
