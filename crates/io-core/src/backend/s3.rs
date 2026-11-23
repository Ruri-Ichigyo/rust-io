//! S3 backend: one lazily built AWS client shared by every operation.

use std::path::Path;

use async_trait::async_trait;
use aws_credential_types::Credentials;
use aws_sdk_s3::Client;
use aws_sdk_s3::config::Region;
use aws_sdk_s3::error::SdkError;
use aws_sdk_s3::primitives::ByteStream;
use bytes::Bytes;
use tokio::io::AsyncRead;
use tokio::sync::OnceCell;

use super::{IoBackend, Metadata};
use crate::error::{IoError, IoResult};
use crate::uri::{Scheme, Uri};

/// Connection settings for the S3 backend.
#[derive(Debug, Clone, Default)]
pub struct S3Config {
    pub access_key: Option<String>,
    pub secret_key: Option<String>,
    pub session_token: Option<String>,
    pub endpoint: Option<String>,
    pub region: Option<String>,
    pub force_path_style: bool,
}

impl S3Config {
    /// Read `IO_S3_*` variables; unset values fall back to the AWS default chain.
    pub fn from_env() -> Self {
        let env = |name: &str| std::env::var(name).ok().filter(|value| !value.is_empty());
        let endpoint = env("IO_S3_ENDPOINT");
        let force_path_style = std::env::var("IO_S3_PATH_STYLE")
            .map(|value| matches!(value.as_str(), "1" | "true" | "TRUE"))
            .unwrap_or(endpoint.is_some());
        Self {
            access_key: env("IO_S3_ACCESS_KEY"),
            secret_key: env("IO_S3_SECRET_KEY"),
            session_token: env("IO_S3_SESSION_TOKEN"),
            endpoint,
            region: env("IO_S3_REGION"),
            force_path_style,
        }
    }
}

/// S3 backend with lazy client initialization.
pub struct S3Backend {
    config: S3Config,
    client: OnceCell<Client>,
}

impl Default for S3Backend {
    fn default() -> Self {
        Self::new()
    }
}

impl S3Backend {
    pub fn new() -> Self {
        Self::with_config(S3Config::from_env())
    }

    pub fn with_config(config: S3Config) -> Self {
        Self {
            config,
            client: OnceCell::new(),
        }
    }

    pub fn config(&self) -> &S3Config {
        &self.config
    }

    async fn client(&self) -> IoResult<&Client> {
        self.client
            .get_or_try_init(|| async {
                let mut loader = aws_config::defaults(aws_config::BehaviorVersion::latest());
                if let (Some(access_key), Some(secret_key)) =
                    (&self.config.access_key, &self.config.secret_key)
                {
                    loader = loader.credentials_provider(Credentials::new(
                        access_key.clone(),
                        secret_key.clone(),
                        self.config.session_token.clone(),
                        None,
                        "io-core",
                    ));
                }
                if let Some(region) = &self.config.region {
                    loader = loader.region(Region::new(region.clone()));
                }
                if let Some(endpoint) = &self.config.endpoint {
                    loader = loader.endpoint_url(endpoint.clone());
                }

                let sdk = loader.load().await;
                let mut builder = aws_sdk_s3::config::Builder::from(&sdk);
                if self.config.force_path_style {
                    builder = builder.force_path_style(true);
                }
                Ok::<Client, IoError>(Client::from_conf(builder.build()))
            })
            .await
    }
}

fn s3_parts(uri: &Uri) -> IoResult<(&str, &str)> {
    match uri {
        Uri::S3 { bucket, key } => Ok((bucket, key)),
        other => Err(IoError::Unsupported(format!(
            "S3 backend cannot handle {other}"
        ))),
    }
}

fn s3_status<E>(error: &SdkError<E>) -> Option<u16> {
    match error {
        SdkError::ServiceError(service) => Some(service.raw().status().as_u16()),
        _ => None,
    }
}

fn s3_error<E: std::fmt::Display>(uri: &Uri, error: SdkError<E>) -> IoError {
    match &error {
        SdkError::ServiceError(service) => {
            let status = service.raw().status().as_u16();
            match status {
                404 => IoError::NotFound(uri.to_string()),
                401 | 403 => IoError::PermissionDenied(uri.to_string()),
                _ => IoError::S3 {
                    retryable: status == 429 || (500..600).contains(&status),
                    message: format!("{uri}: HTTP {status}: {}", service.err()),
                },
            }
        }
        SdkError::TimeoutError(_) => IoError::S3 {
            retryable: true,
            message: format!("{uri}: request timed out"),
        },
        other => IoError::S3 {
            retryable: true,
            message: format!("{uri}: {other}"),
        },
    }
}

fn stream_error(uri: &Uri, error: impl std::fmt::Display) -> IoError {
    IoError::S3 {
        retryable: true,
        message: format!("{uri}: {error}"),
    }
}

fn copy_source(bucket: &str, key: &str) -> String {
    let mut source = String::with_capacity(bucket.len() + key.len() + 1);
    source.push_str(bucket);
    source.push('/');
    for byte in key.bytes() {
        match byte {
            b'A'..=b'Z' | b'a'..=b'z' | b'0'..=b'9' | b'-' | b'.' | b'_' | b'~' | b'/' => {
                source.push(byte as char);
            }
            _ => source.push_str(&format!("%{byte:02X}")),
        }
    }
    source
}

#[async_trait]
impl IoBackend for S3Backend {
    fn scheme(&self) -> Scheme {
        Scheme::S3
    }

    async fn read_range(&self, uri: &Uri, offset: u64, length: u64) -> IoResult<Bytes> {
        let (bucket, key) = s3_parts(uri)?;
        if length == 0 {
            return Ok(Bytes::new());
        }
        let end = offset
            .checked_add(length - 1)
            .ok_or_else(|| IoError::InvalidConfig("range overflows u64".into()))?;
        let client = self.client().await?;
        let response = match client
            .get_object()
            .bucket(bucket)
            .key(key)
            .range(format!("bytes={offset}-{end}"))
            .send()
            .await
        {
            Ok(response) => response,
            Err(error) if s3_status(&error) == Some(416) => return Ok(Bytes::new()),
            Err(error) => return Err(s3_error(uri, error)),
        };
        let content_range = response.content_range().map(str::to_string);
        let aggregated = response
            .body
            .collect()
            .await
            .map_err(|error| stream_error(uri, error))?;
        let bytes = aggregated.into_bytes();

        if let Some(range) = content_range {
            let start = range
                .strip_prefix("bytes ")
                .and_then(|value| value.split_once('-'))
                .and_then(|(start, _)| start.parse::<u64>().ok());
            if start != Some(offset) {
                return Err(IoError::S3 {
                    retryable: false,
                    message: format!(
                        "{uri}: unexpected Content-Range '{range}' for offset {offset}"
                    ),
                });
            }
        } else if bytes.len() as u64 > length {
            return Err(IoError::S3 {
                retryable: false,
                message: format!(
                    "{uri}: range request returned {} bytes for length {length}",
                    bytes.len()
                ),
            });
        }
        Ok(bytes)
    }

    async fn write(&self, uri: &Uri, data: Bytes) -> IoResult<()> {
        let (bucket, key) = s3_parts(uri)?;
        let client = self.client().await?;
        client
            .put_object()
            .bucket(bucket)
            .key(key)
            .body(ByteStream::from(data))
            .send()
            .await
            .map_err(|error| s3_error(uri, error))?;
        Ok(())
    }

    async fn open_read(&self, uri: &Uri) -> IoResult<Box<dyn AsyncRead + Send + Unpin>> {
        let (bucket, key) = s3_parts(uri)?;
        let client = self.client().await?;
        let response = client
            .get_object()
            .bucket(bucket)
            .key(key)
            .send()
            .await
            .map_err(|error| s3_error(uri, error))?;
        Ok(Box::new(response.body.into_async_read()))
    }

    async fn write_from_path(&self, uri: &Uri, path: &Path, _len: u64) -> IoResult<()> {
        let (bucket, key) = s3_parts(uri)?;
        let client = self.client().await?;
        let body = ByteStream::from_path(path)
            .await
            .map_err(|error| stream_error(uri, error))?;
        client
            .put_object()
            .bucket(bucket)
            .key(key)
            .body(body)
            .send()
            .await
            .map_err(|error| s3_error(uri, error))?;
        Ok(())
    }

    async fn stat(&self, uri: &Uri) -> IoResult<Metadata> {
        let (bucket, key) = s3_parts(uri)?;
        let client = self.client().await?;
        let response = client
            .head_object()
            .bucket(bucket)
            .key(key)
            .send()
            .await
            .map_err(|error| s3_error(uri, error))?;
        Ok(Metadata {
            uri: uri.clone(),
            len: response.content_length().unwrap_or(0).max(0) as u64,
            is_file: true,
            is_dir: false,
            modified: response
                .last_modified()
                .map(|modified| modified.secs().max(0) as u64),
            content_type: response.content_type().map(str::to_string),
            etag: response.e_tag().map(str::to_string),
        })
    }

    async fn list(&self, uri: &Uri) -> IoResult<Vec<String>> {
        let (bucket, key) = s3_parts(uri)?;
        let client = self.client().await?;
        let prefix = if key.ends_with('/') {
            key.to_string()
        } else {
            format!("{key}/")
        };
        let mut entries = Vec::new();
        let mut continuation: Option<String> = None;

        loop {
            let mut request = client
                .list_objects_v2()
                .bucket(bucket)
                .prefix(&prefix)
                .delimiter("/");
            if let Some(token) = &continuation {
                request = request.continuation_token(token);
            }
            let response = request.send().await.map_err(|error| s3_error(uri, error))?;

            for common in response.common_prefixes() {
                if let Some(value) = common.prefix() {
                    let name = value.strip_prefix(&prefix).unwrap_or(value);
                    if !name.is_empty() {
                        entries.push(name.to_string());
                    }
                }
            }
            for object in response.contents() {
                if let Some(value) = object.key() {
                    let name = value.strip_prefix(&prefix).unwrap_or(value);
                    if !name.is_empty() {
                        entries.push(name.to_string());
                    }
                }
            }

            if response.is_truncated().unwrap_or(false) {
                continuation = response.next_continuation_token().map(str::to_string);
            } else {
                break;
            }
        }

        entries.sort();
        entries.dedup();
        Ok(entries)
    }

    async fn delete(&self, uri: &Uri) -> IoResult<()> {
        let (bucket, key) = s3_parts(uri)?;
        let client = self.client().await?;
        client
            .delete_object()
            .bucket(bucket)
            .key(key)
            .send()
            .await
            .map_err(|error| s3_error(uri, error))?;
        Ok(())
    }

    async fn copy(&self, from: &Uri, to: &Uri) -> IoResult<()> {
        let (from_bucket, from_key) = s3_parts(from)?;
        let (to_bucket, to_key) = s3_parts(to)?;
        let client = self.client().await?;
        let source = copy_source(from_bucket, from_key);
        client
            .copy_object()
            .bucket(to_bucket)
            .key(to_key)
            .copy_source(source)
            .send()
            .await
            .map_err(|error| s3_error(to, error))?;
        Ok(())
    }

    async fn supports_parallel_ranges(&self, _uri: &Uri) -> IoResult<bool> {
        Ok(true)
    }
}

#[cfg(test)]
mod tests {
    use super::copy_source;

    #[test]
    fn copy_source_keeps_slashes_and_encodes_spaces() {
        assert_eq!(
            copy_source("bucket", "dir name/file.bin"),
            "bucket/dir%20name/file.bin"
        );
        assert_eq!(copy_source("bucket", "a/b"), "bucket/a/b");
    }
}
