use std::time::Duration;

use crate::{client::PolarisClient, errors::PolarisError, http::HttpClient};

#[derive(Clone, Debug)]
pub struct PolarisClientBuilder {
    api_key: Option<String>,
    base_url: String,
    stream_url: Option<String>,
    timeout: Duration,
}

impl Default for PolarisClientBuilder {
    fn default() -> Self {
        Self {
            api_key: None,
            base_url: "https://api.polaris.supply".to_owned(),
            stream_url: None,
            timeout: Duration::from_secs(30),
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

    pub fn build(self) -> Result<PolarisClient, PolarisError> {
        let api_key = self
            .api_key
            .or_else(|| std::env::var("POLARIS_API_KEY").ok());
        let stream_url = crate::realtime::resolve_stream_url(&self.base_url, self.stream_url)?;
        let http = HttpClient::new(self.base_url, self.timeout, api_key.clone())?;
        Ok(PolarisClient::from_parts(api_key, http, stream_url))
    }
}
