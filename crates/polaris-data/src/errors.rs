use serde::{Deserialize, Serialize};
use thiserror::Error;

#[derive(Debug, Error)]
pub enum PolarisError {
    #[error("unauthorized: {0}")]
    Unauthorized(Box<Fault>),
    #[error("access denied: {0}")]
    AccessDenied(Box<Fault>),
    #[error("not found: {0}")]
    NotFound(Box<Fault>),
    #[error("rate limited: {0}")]
    RateLimited(Box<Fault>),
    #[error("invalid response: {0}")]
    InvalidResponse(String),
    #[error("decode error: {0}")]
    Decode(String),
    #[error("request failed: {0}")]
    Request(String),
    #[error("request failed: {0}")]
    Api(Box<Fault>),
    #[error("realtime stream connection failed: {0}")]
    StreamConnection(String),
    #[error("realtime stream protocol error: {message}")]
    StreamProtocol {
        code: Option<String>,
        message: String,
    },
    #[error(
        "blocking Polaris client cannot run inside an active Tokio runtime; use the async client"
    )]
    BlockingInAsyncRuntime,
}

#[derive(Clone, Debug, Serialize, Deserialize)]
pub struct Fault {
    pub kind: String,
    pub message: String,
    pub status_code: Option<u16>,
    pub body: Option<String>,
    pub code: Option<String>,
    pub resolution: Option<String>,
    pub reset_at: Option<String>,
}

impl std::fmt::Display for Fault {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.write_str(&self.message)?;
        if let Some(status) = self.status_code {
            write!(f, " (HTTP {status})")?;
        }
        Ok(())
    }
}

impl From<Fault> for PolarisError {
    fn from(fault: Fault) -> Self {
        match fault.kind.as_str() {
            "unauthorized" => Self::Unauthorized(Box::new(fault)),
            "access_denied" => Self::AccessDenied(Box::new(fault)),
            "not_found" => Self::NotFound(Box::new(fault)),
            "rate_limited" => Self::RateLimited(Box::new(fault)),
            "invalid_response" => Self::InvalidResponse(fault.message),
            "stream_decode" => Self::Decode(fault.message),
            "stream_connection" => Self::StreamConnection(fault.message),
            "stream_protocol" => Self::StreamProtocol {
                code: fault.code,
                message: fault.message,
            },
            _ if fault.status_code.is_some() => Self::Api(Box::new(fault)),
            _ => Self::Request(fault.message),
        }
    }
}

impl PolarisError {
    pub fn into_fault(self) -> Fault {
        let (kind, message, code) = match self {
            Self::Unauthorized(f)
            | Self::AccessDenied(f)
            | Self::NotFound(f)
            | Self::RateLimited(f)
            | Self::Api(f) => return *f,
            Self::InvalidResponse(message) => ("invalid_response", message, None),
            Self::Decode(message) => ("stream_decode", message, None),
            Self::Request(message) => ("request", message, None),
            Self::StreamConnection(message) => ("stream_connection", message, None),
            Self::StreamProtocol { message, code } => ("stream_protocol", message, code),
            error @ Self::BlockingInAsyncRuntime => {
                ("blocking_in_async_runtime", error.to_string(), None)
            }
        };
        Fault {
            kind: kind.to_owned(),
            message,
            status_code: None,
            body: None,
            code,
            resolution: None,
            reset_at: None,
        }
    }
}
