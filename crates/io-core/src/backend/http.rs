use std::path::Path;
use std::time::UNIX_EPOCH;

use async_trait::async_trait;
use bytes::{Bytes, BytesMut};
use futures::StreamExt;
use reqwest::header::{CONTENT_LENGTH, CONTENT_RANGE, CONTENT_TYPE, ETAG, LAST_MODIFIED, RANGE};
use reqwest::{Client, StatusCode};
use tokio::io::{AsyncRead, AsyncReadExt};
use tokio_util::io::{ReaderStream, StreamReader};

use super::{IoBackend, Metadata};
use crate::error::{IoError, IoResult};
use crate::uri::{Scheme, Uri};

/// HTTP(S) backend for pre-signed URLs, public objects, and REST endpoints.
#[derive(Clone)]
pub struct HttpBackend {
    client: Client,
}

impl HttpBackend {
    pub fn new() -> IoResult<Self> {
        let client = Client::builder()
            .connect_timeout(std::time::Duration::from_secs(15))
            .user_agent("rust-io/0.3")
            .redirect(reqwest::redirect::Policy::limited(10))
            .gzip(true)
            .build()
            .map_err(|error| IoError::Other(format!("failed to build HTTP client: {error}")))?;
        Ok(Self { client })
    }

    /// Reuse a caller-provided client so connection pools can be shared.
    pub fn from_client(client: Client) -> Self {
        Self { client }
    }
}

fn http_uri(uri: &Uri) -> IoResult<&url::Url> {
    match uri {
        Uri::Http(url) => Ok(url),
        other => Err(IoError::Unsupported(format!(
            "HTTP backend cannot handle {other}"
        ))),
    }
}

fn status_error(uri: &Uri, status: StatusCode) -> IoError {
    match status.as_u16() {
        404 => IoError::NotFound(uri.to_string()),
        401 | 403 => IoError::PermissionDenied(uri.to_string()),
        code => IoError::Http {
            status: Some(code),
            message: format!(
                "{code} {} for {uri}",
                status.canonical_reason().unwrap_or("HTTP error")
            ),
        },
    }
}

fn transport_error(uri: &Uri, error: reqwest::Error) -> IoError {
    if let Some(status) = error.status() {
        return status_error(uri, status);
    }
    IoError::Http {
        status: None,
        message: format!("{uri}: {error}"),
    }
}

async fn ensure_success(uri: &Uri, response: reqwest::Response) -> IoResult<reqwest::Response> {
    let status = response.status();
    if status.is_success() {
        Ok(response)
    } else {
        Err(status_error(uri, status))
    }
}

#[async_trait]
impl IoBackend for HttpBackend {
    fn scheme(&self) -> Scheme {
        Scheme::Http
    }

    async fn read_range(&self, uri: &Uri, offset: u64, length: u64) -> IoResult<Bytes> {
        let url = http_uri(uri)?;
        if length == 0 {
            return Ok(Bytes::new());
        }
        let end = offset
            .checked_add(length - 1)
            .ok_or_else(|| IoError::InvalidConfig("range overflows u64".into()))?;

        let response = self
            .client
            .get(url.clone())
            .header(RANGE, format!("bytes={offset}-{end}"))
            .send()
            .await
            .map_err(|error| transport_error(uri, error))?;
        let status = response.status();
        if status == StatusCode::RANGE_NOT_SATISFIABLE {
            return Ok(Bytes::new());
        }
        let response = ensure_success(uri, response).await?;

        if status == StatusCode::PARTIAL_CONTENT {
            let bytes = response
                .bytes()
                .await
                .map_err(|error| transport_error(uri, error))?;
            let take = usize::try_from(length)
                .unwrap_or(usize::MAX)
                .min(bytes.len());
            return Ok(bytes.slice(..take));
        }

        // The server ignored Range. Stream and discard `offset` bytes instead
        // of buffering the whole object before slicing.
        let mut stream = response.bytes_stream();
        let mut skip = offset;
        let mut collected = BytesMut::with_capacity(
            usize::try_from(length)
                .unwrap_or(usize::MAX)
                .min(1024 * 1024),
        );
        while let Some(chunk) = stream.next().await {
            let chunk = chunk.map_err(|error| transport_error(uri, error))?;
            let chunk_len = chunk.len() as u64;
            if skip >= chunk_len {
                skip -= chunk_len;
                continue;
            }
            let start = usize::try_from(skip).unwrap_or(usize::MAX);
            skip = 0;
            let needed = usize::try_from(length)
                .unwrap_or(usize::MAX)
                .saturating_sub(collected.len());
            let take = needed.min(chunk.len() - start);
            collected.extend_from_slice(&chunk[start..start + take]);
            if collected.len() as u64 >= length {
                break;
            }
        }
        Ok(collected.freeze())
    }

    async fn write(&self, uri: &Uri, data: Bytes) -> IoResult<()> {
        let url = http_uri(uri)?;
        let response = self
            .client
            .put(url.clone())
            .body(data)
            .send()
            .await
            .map_err(|error| transport_error(uri, error))?;
        ensure_success(uri, response).await?;
        Ok(())
    }

    async fn open_read(&self, uri: &Uri) -> IoResult<Box<dyn AsyncRead + Send + Unpin>> {
        let url = http_uri(uri)?;
        let response = self
            .client
            .get(url.clone())
            .send()
            .await
            .map_err(|error| transport_error(uri, error))?;
        let response = ensure_success(uri, response).await?;
        let stream = response.bytes_stream().map(|chunk| {
            chunk.map_err(|error| std::io::Error::new(std::io::ErrorKind::ConnectionReset, error))
        });
        Ok(Box::new(StreamReader::new(stream)))
    }

    async fn open_range(
        &self,
        uri: &Uri,
        offset: u64,
        length: u64,
    ) -> IoResult<Box<dyn AsyncRead + Send + Unpin>> {
        let url = http_uri(uri)?;
        if length == 0 {
            return Ok(Box::new(tokio::io::empty()));
        }
        let end = offset
            .checked_add(length - 1)
            .ok_or_else(|| IoError::InvalidConfig("range overflows u64".into()))?;

        let response = self
            .client
            .get(url.clone())
            .header(RANGE, format!("bytes={offset}-{end}"))
            .send()
            .await
            .map_err(|error| transport_error(uri, error))?;
        let status = response.status();
        if status == StatusCode::RANGE_NOT_SATISFIABLE {
            return Ok(Box::new(tokio::io::empty()));
        }
        if status != StatusCode::PARTIAL_CONTENT {
            if status.is_success() {
                return Err(IoError::Unsupported(format!(
                    "{uri}: server ignored the range request"
                )));
            }
            return Err(status_error(uri, status));
        }
        if let Some(value) = response
            .headers()
            .get(CONTENT_RANGE)
            .and_then(|value| value.to_str().ok())
        {
            let start = value
                .strip_prefix("bytes ")
                .and_then(|value| value.split_once('-'))
                .and_then(|(start, _)| start.parse::<u64>().ok());
            if start != Some(offset) {
                return Err(IoError::Http {
                    status: Some(206),
                    message: format!(
                        "{uri}: unexpected Content-Range '{value}' for offset {offset}"
                    ),
                });
            }
        }

        let stream = response.bytes_stream().map(|chunk| {
            chunk.map_err(|error| std::io::Error::new(std::io::ErrorKind::ConnectionReset, error))
        });
        Ok(Box::new(StreamReader::new(stream).take(length)))
    }

    async fn write_from_path(&self, uri: &Uri, path: &Path, len: u64) -> IoResult<()> {
        let url = http_uri(uri)?;
        let file = tokio::fs::File::open(path)
            .await
            .map_err(|error| IoError::from_io(&path.display().to_string(), error))?;
        let body = reqwest::Body::wrap_stream(ReaderStream::new(file));
        let response = self
            .client
            .put(url.clone())
            .header(CONTENT_LENGTH, len)
            .body(body)
            .send()
            .await
            .map_err(|error| transport_error(uri, error))?;
        ensure_success(uri, response).await?;
        Ok(())
    }

    async fn stat(&self, uri: &Uri) -> IoResult<Metadata> {
        let url = http_uri(uri)?;
        let response = self
            .client
            .head(url.clone())
            .send()
            .await
            .map_err(|error| transport_error(uri, error))?;
        let response = ensure_success(uri, response).await?;
        let headers = response.headers();

        let len = headers
            .get(CONTENT_LENGTH)
            .and_then(|value| value.to_str().ok())
            .and_then(|value| value.parse::<u64>().ok())
            .unwrap_or(0);
        let modified = headers
            .get(LAST_MODIFIED)
            .and_then(|value| value.to_str().ok())
            .and_then(|value| httpdate::parse_http_date(value).ok())
            .and_then(|time| time.duration_since(UNIX_EPOCH).ok())
            .map(|duration| duration.as_secs());

        Ok(Metadata {
            uri: uri.clone(),
            len,
            is_file: true,
            is_dir: false,
            modified,
            content_type: headers
                .get(CONTENT_TYPE)
                .and_then(|value| value.to_str().ok())
                .map(str::to_string),
            etag: headers
                .get(ETAG)
                .and_then(|value| value.to_str().ok())
                .map(str::to_string),
        })
    }

    async fn list(&self, uri: &Uri) -> IoResult<Vec<String>> {
        let _ = http_uri(uri)?;
        Err(IoError::Unsupported("HTTP has no directory listing".into()))
    }

    async fn delete(&self, uri: &Uri) -> IoResult<()> {
        let url = http_uri(uri)?;
        let response = self
            .client
            .delete(url.clone())
            .send()
            .await
            .map_err(|error| transport_error(uri, error))?;
        ensure_success(uri, response).await?;
        Ok(())
    }

    async fn copy(&self, _from: &Uri, _to: &Uri) -> IoResult<()> {
        Err(IoError::Unsupported(
            "HTTP has no server-side copy; use IoEngine::copy".into(),
        ))
    }

    async fn supports_parallel_ranges(&self, uri: &Uri) -> IoResult<bool> {
        let url = http_uri(uri)?;
        let response = self
            .client
            .get(url.clone())
            .header(RANGE, "bytes=0-0")
            .send()
            .await
            .map_err(|error| transport_error(uri, error))?;
        let status = response.status();
        if !status.is_success() {
            return Err(status_error(uri, status));
        }
        Ok(status == StatusCode::PARTIAL_CONTENT)
    }
}
