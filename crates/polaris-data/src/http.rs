use reqwest::{Client, StatusCode, Url};
use serde_json::Value;

use crate::errors::PolarisError;

#[derive(Clone, Copy)]
pub(crate) enum AuthMode {
    None,
    IfAvailable,
    Required,
}

#[derive(Clone)]
pub(crate) struct HttpClient {
    client: Client,
    base_url: Url,
    api_key: Option<String>,
}

impl HttpClient {
    pub(crate) fn new(
        base_url: String,
        timeout: std::time::Duration,
        api_key: Option<String>,
    ) -> Result<Self, PolarisError> {
        let base_url = Url::parse(&base_url)
            .map_err(|err| PolarisError::InvalidResponse(format!("invalid base url: {err}")))?;
        let client = Client::builder()
            .redirect(reqwest::redirect::Policy::limited(10))
            .timeout(timeout)
            .user_agent(format!("polaris-rs/{}", env!("CARGO_PKG_VERSION")))
            .build()?;
        Ok(Self {
            client,
            base_url,
            api_key,
        })
    }

    pub(crate) async fn get_json(
        &self,
        path: &str,
        params: &[(String, String)],
        auth_mode: AuthMode,
    ) -> Result<Value, PolarisError> {
        let response = self.request(path, params, auth_mode).await?;
        let status = response.status();
        let body = response.text().await?;
        if !status.is_success() {
            return Err(self.map_error(status, body));
        }
        serde_json::from_str(&body).map_err(|err| {
            PolarisError::InvalidResponse(format!("response was not valid JSON: {err}"))
        })
    }

    pub(crate) async fn download_absolute_bytes(&self, url: &str) -> Result<Vec<u8>, PolarisError> {
        let url = Url::parse(url).map_err(|err| {
            PolarisError::InvalidResponse(format!("invalid download url '{url}': {err}"))
        })?;
        let response = self.client.get(url).send().await?;
        let status = response.status();
        let body = response.bytes().await?;
        if !status.is_success() {
            let text = String::from_utf8_lossy(&body).to_string();
            return Err(self.map_error(status, text));
        }
        Ok(body.to_vec())
    }

    pub(crate) async fn get_bytes(
        &self,
        path: &str,
        params: &[(String, String)],
        auth_mode: AuthMode,
    ) -> Result<(Option<String>, Vec<u8>), PolarisError> {
        let response = self.request(path, params, auth_mode).await?;
        let status = response.status();
        let content_type = response
            .headers()
            .get(reqwest::header::CONTENT_TYPE)
            .and_then(|value| value.to_str().ok())
            .map(ToOwned::to_owned);
        let body = response.bytes().await?.to_vec();
        if !status.is_success() {
            return Err(self.map_error(status, String::from_utf8_lossy(&body).to_string()));
        }
        Ok((content_type, body))
    }

    async fn request(
        &self,
        path: &str,
        params: &[(String, String)],
        auth_mode: AuthMode,
    ) -> Result<reqwest::Response, PolarisError> {
        let url = self.base_url.join(path).map_err(|err| {
            PolarisError::InvalidResponse(format!("failed to join URL '{path}': {err}"))
        })?;
        let mut request = self.client.get(url).query(params);

        match auth_mode {
            AuthMode::None => {}
            AuthMode::IfAvailable => {
                if let Some(api_key) = &self.api_key {
                    request = request.bearer_auth(api_key);
                }
            }
            AuthMode::Required => {
                let api_key = self
                    .api_key
                    .as_ref()
                    .ok_or_else(|| PolarisError::Unauthorized {
                        message: "API key is required for this endpoint".to_owned(),
                        status_code: None,
                        body: None,
                    })?;
                request = request.bearer_auth(api_key);
            }
        }

        Ok(request.send().await?)
    }

    fn map_error(&self, status: StatusCode, body: String) -> PolarisError {
        let mut message = format!("HTTP {}", status.as_u16());
        let mut reset_at = None;

        if let Ok(value) = serde_json::from_str::<Value>(&body) {
            if let Some(found) = value.get("error").and_then(Value::as_str) {
                message = found.to_owned();
            } else if let Some(found) = value.get("message").and_then(Value::as_str) {
                message = found.to_owned();
            }
            reset_at = value
                .get("reset_at")
                .and_then(Value::as_str)
                .map(ToOwned::to_owned);
        } else if !body.is_empty() {
            message = body.clone();
        }

        PolarisError::from_status(status.as_u16(), message, body, reset_at)
    }
}
