use thiserror::Error;

#[derive(Debug, Error)]
pub enum PolarisError {
    #[error("unauthorized: {message}")]
    Unauthorized {
        message: String,
        status_code: Option<u16>,
        body: Option<String>,
    },
    #[error("access denied: {message}")]
    AccessDenied {
        message: String,
        status_code: Option<u16>,
        body: Option<String>,
    },
    #[error("not found: {message}")]
    NotFound {
        message: String,
        status_code: Option<u16>,
        body: Option<String>,
    },
    #[error("rate limited: {message}")]
    RateLimited {
        message: String,
        reset_at: Option<String>,
        status_code: Option<u16>,
        body: Option<String>,
    },
    #[error("invalid response: {0}")]
    InvalidResponse(String),
    #[error("snapshot coverage gap for {dataset_source}/{market}: {intervals:?}")]
    CoverageGap {
        dataset_source: String,
        market: String,
        intervals: Vec<String>,
    },
    #[error("i/o error: {0}")]
    Io(#[from] std::io::Error),
    #[error("decode error: {0}")]
    Decode(String),
    #[error("request failed: {0}")]
    Request(String),
    #[error("realtime stream connection failed: {0}")]
    StreamConnection(String),
    #[error("realtime stream protocol error{code_suffix}: {message}", code_suffix = code.as_ref().map(|value| format!(" ({value})")).unwrap_or_default())]
    StreamProtocol {
        code: Option<String>,
        message: String,
    },
    #[error(
        "blocking Polaris client cannot run inside an active Tokio runtime; use the async client"
    )]
    BlockingInAsyncRuntime,
}

impl PolarisError {
    pub(crate) fn from_status(
        status: u16,
        message: String,
        body: String,
        reset_at: Option<String>,
    ) -> Self {
        match status {
            401 => Self::Unauthorized {
                message,
                status_code: Some(status),
                body: Some(body),
            },
            402 => Self::AccessDenied {
                message,
                status_code: Some(status),
                body: Some(body),
            },
            404 => Self::NotFound {
                message,
                status_code: Some(status),
                body: Some(body),
            },
            429 => Self::RateLimited {
                message,
                reset_at,
                status_code: Some(status),
                body: Some(body),
            },
            _ => Self::Request(format!("HTTP {status}: {message}")),
        }
    }
}

impl From<reqwest::Error> for PolarisError {
    fn from(value: reqwest::Error) -> Self {
        Self::Request(value.to_string())
    }
}
