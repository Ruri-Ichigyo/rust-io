use std::time::Duration;

use thiserror::Error;

pub type IoResult<T> = Result<T, IoError>;

/// Typed error for every I/O operation.
#[derive(Debug, Error)]
pub enum IoError {
    #[error("invalid URI: {0}")]
    InvalidUri(String),

    #[error("invalid configuration: {0}")]
    InvalidConfig(String),

    #[error("not found: {0}")]
    NotFound(String),

    #[error("permission denied: {0}")]
    PermissionDenied(String),

    #[error("operation timed out after {0:?}")]
    Timeout(Duration),

    #[error("unsupported operation: {0}")]
    Unsupported(String),

    #[error("I/O error: {0}")]
    Io(#[from] std::io::Error),

    #[error("HTTP error: {message}")]
    Http {
        status: Option<u16>,
        message: String,
    },

    #[error("S3 error: {message}")]
    S3 { retryable: bool, message: String },

    #[error("all candidates failed")]
    AllFailed(Vec<(String, IoError)>),

    #[error("{0}")]
    Other(String),
}

impl IoError {
    /// Whether retrying the same operation can plausibly succeed.
    pub fn is_retryable(&self) -> bool {
        match self {
            IoError::Timeout(_) => true,
            IoError::Http { status, .. } => match status {
                None => true,
                Some(status) => *status == 429 || (500..600).contains(status),
            },
            IoError::S3 { retryable, .. } => *retryable,
            IoError::Io(error) => matches!(
                error.kind(),
                std::io::ErrorKind::Interrupted
                    | std::io::ErrorKind::WouldBlock
                    | std::io::ErrorKind::TimedOut
                    | std::io::ErrorKind::ConnectionReset
                    | std::io::ErrorKind::ConnectionAborted
                    | std::io::ErrorKind::NotConnected
            ),
            _ => false,
        }
    }

    pub(crate) fn from_io(path: &str, error: std::io::Error) -> Self {
        match error.kind() {
            std::io::ErrorKind::NotFound => IoError::NotFound(path.to_string()),
            std::io::ErrorKind::PermissionDenied => IoError::PermissionDenied(path.to_string()),
            _ => IoError::Io(error),
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::io::ErrorKind;
    use std::time::Duration;

    fn http(status: Option<u16>) -> IoError {
        IoError::Http {
            status,
            message: String::new(),
        }
    }

    #[test]
    fn timeout_is_retryable() {
        assert!(IoError::Timeout(Duration::from_secs(1)).is_retryable());
    }

    #[test]
    fn http_retryability_depends_on_status() {
        assert!(http(None).is_retryable());
        assert!(http(Some(429)).is_retryable());
        assert!(http(Some(500)).is_retryable());
        assert!(http(Some(503)).is_retryable());
        assert!(!http(Some(400)).is_retryable());
        assert!(!http(Some(404)).is_retryable());
    }

    #[test]
    fn s3_retryability_comes_from_the_backend() {
        assert!(
            IoError::S3 {
                retryable: true,
                message: String::new()
            }
            .is_retryable()
        );
        assert!(
            !IoError::S3 {
                retryable: false,
                message: String::new()
            }
            .is_retryable()
        );
    }

    #[test]
    fn only_transient_io_kinds_are_retryable() {
        assert!(
            IoError::Io(std::io::Error::new(ErrorKind::ConnectionReset, "reset")).is_retryable()
        );
        assert!(IoError::Io(std::io::Error::new(ErrorKind::WouldBlock, "busy")).is_retryable());
        assert!(!IoError::Io(std::io::Error::new(ErrorKind::InvalidData, "bad")).is_retryable());
    }

    #[test]
    fn from_io_maps_permission_denied() {
        let error = IoError::from_io(
            "/protected",
            std::io::Error::new(ErrorKind::PermissionDenied, "denied"),
        );
        assert!(matches!(error, IoError::PermissionDenied(path) if path == "/protected"));
    }

    #[test]
    fn from_io_maps_not_found() {
        let error = IoError::from_io(
            "/missing",
            std::io::Error::new(ErrorKind::NotFound, "missing"),
        );
        assert!(matches!(error, IoError::NotFound(path) if path == "/missing"));
    }
}
