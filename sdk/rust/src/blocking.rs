//! Blocking interfaces to the Polaris SDK.

use std::{sync::Arc, time::Duration};

use serde::{Serialize, de::DeserializeOwned};

use crate::{
    Client, DataQuery, DataResponse, PolarisError, Row, StreamQuery, TableDataMessage, ffi,
};

#[derive(Clone, Default)]
pub struct PolarisClientBuilder(crate::PolarisClientBuilder);

impl PolarisClientBuilder {
    pub fn new() -> Self {
        Self::default()
    }

    pub fn api_key(self, value: impl Into<String>) -> Self {
        Self(self.0.api_key(value))
    }

    pub fn base_url(self, value: impl Into<String>) -> Self {
        Self(self.0.base_url(value))
    }

    pub fn stream_url(self, value: impl Into<String>) -> Self {
        Self(self.0.stream_url(value))
    }

    pub fn timeout(self, value: Duration) -> Self {
        Self(self.0.timeout(value))
    }

    pub fn build(self) -> Result<PolarisClient, PolarisError> {
        PolarisClient::from_async(self.0.build()?)
    }
}

#[derive(Clone)]
pub struct PolarisClient(Client);

fn ensure_blocking() -> Result<(), PolarisError> {
    if tokio::runtime::Handle::try_current().is_ok() {
        return Err(PolarisError::BlockingInAsyncRuntime);
    }
    Ok(())
}

impl PolarisClient {
    pub fn builder() -> PolarisClientBuilder {
        PolarisClientBuilder::default()
    }

    pub fn from_async(inner: Client) -> Result<Self, PolarisError> {
        ensure_blocking()?;
        Ok(Self(inner))
    }

    pub fn as_async(&self) -> &Client {
        &self.0
    }

    #[doc(hidden)]
    pub fn request<T: DeserializeOwned>(
        &self,
        operation: &str,
        query: impl Serialize,
    ) -> Result<T, PolarisError> {
        ensure_blocking()?;
        ffi::request(&self.0.input(operation, query)?)
    }

    pub fn data_rows(&self, query: DataQuery) -> Result<HistoricalIterator, PolarisError> {
        ensure_blocking()?;
        let tracker = ffi::CursorSet::new(query.cursor.as_deref())?;
        Ok(HistoricalIterator {
            client: self.clone(),
            query,
            tracker,
            rows: Vec::new().into_iter(),
            final_page: false,
        })
    }

    pub fn stream(&self, query: StreamQuery) -> Result<RealtimeIterator, PolarisError> {
        ensure_blocking()?;
        let input = self.0.input("stream", query)?;
        Ok(RealtimeIterator(Arc::new(ffi::Stream::new(&input)?)))
    }
}

pub struct HistoricalIterator {
    client: PolarisClient,
    query: DataQuery,
    tracker: ffi::CursorSet,
    rows: std::vec::IntoIter<Row>,
    final_page: bool,
}

impl Iterator for HistoricalIterator {
    type Item = Result<Row, PolarisError>;

    fn next(&mut self) -> Option<Self::Item> {
        loop {
            if let Some(row) = self.rows.next() {
                return Some(Ok(row));
            }
            if self.final_page {
                return None;
            }
            let result = self
                .client
                .data(self.query.clone())
                .and_then(|page: DataResponse| {
                    if page.has_more {
                        self.tracker.remember(
                            page.next_cursor
                                .as_deref()
                                .expect("validated native data page"),
                        )?;
                    }
                    Ok(page)
                });
            match result {
                Ok(page) => {
                    self.query.cursor = page.next_cursor;
                    self.final_page = !page.has_more;
                    self.rows = page.data.into_iter();
                }
                Err(error) => {
                    self.final_page = true;
                    return Some(Err(error));
                }
            }
        }
    }
}

pub struct RealtimeIterator(Arc<ffi::Stream>);

impl RealtimeIterator {
    pub fn close(&self) {
        self.0.cancel();
    }

    pub fn read(&self) -> Result<Option<TableDataMessage>, PolarisError> {
        ensure_blocking()?;
        self.0.next()
    }
}

impl Iterator for RealtimeIterator {
    type Item = Result<TableDataMessage, PolarisError>;

    fn next(&mut self) -> Option<Self::Item> {
        self.read().transpose()
    }
}

impl Drop for RealtimeIterator {
    fn drop(&mut self) {
        self.close();
    }
}
