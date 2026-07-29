use std::{path::PathBuf, time::Duration};

use crate::{
    client::PolarisClient,
    errors::PolarisError,
    http::HttpClient,
    storage::{ensure_layout, resolve_root},
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
        let api_key = self
            .api_key
            .or_else(|| std::env::var("POLARIS_API_KEY").ok());
        let root = resolve_root(self.dataset_root)?;
        let layout = ensure_layout(root)?;
        let http = HttpClient::new(self.base_url, self.timeout, api_key.clone())?;
        Ok(PolarisClient::from_parts(api_key, layout, http))
    }
}
