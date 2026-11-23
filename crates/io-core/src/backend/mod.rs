pub mod http;
pub mod local;
pub mod s3;

use std::path::Path;

use async_trait::async_trait;
use bytes::Bytes;
use tokio::io::AsyncRead;

use crate::error::{IoError, IoResult};
use crate::uri::{Scheme, Uri};

/// Chunk size used by streaming reads, hashing, and cross-backend copies.
pub const IO_CHUNK: usize = 64 * 1024;

/// Backend-neutral object metadata.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct Metadata {
    pub uri: Uri,
    pub len: u64,
    pub is_file: bool,
    pub is_dir: bool,
    /// Unix seconds, when the backend exposes it.
    pub modified: Option<u64>,
    pub content_type: Option<String>,
    pub etag: Option<String>,
}

/// Async, object-safe transport used by [`crate::engine::IoEngine`].
#[async_trait]
pub trait IoBackend: Send + Sync {
    fn scheme(&self) -> Scheme;

    async fn read_all(&self, uri: &Uri) -> IoResult<Bytes> {
        use tokio::io::AsyncReadExt;

        let mut reader = self.open_read(uri).await?;
        let mut buffer = Vec::new();
        reader
            .read_to_end(&mut buffer)
            .await
            .map_err(|error| IoError::from_io(&uri.to_string(), error))?;
        Ok(Bytes::from(buffer))
    }

    async fn read_range(&self, uri: &Uri, offset: u64, length: u64) -> IoResult<Bytes>;

    async fn write(&self, uri: &Uri, data: Bytes) -> IoResult<()>;

    async fn open_read(&self, uri: &Uri) -> IoResult<Box<dyn AsyncRead + Send + Unpin>>;

    /// Stream `path` into `uri`. `len` is the file length at validation time.
    async fn write_from_path(&self, uri: &Uri, path: &Path, len: u64) -> IoResult<()>;

    async fn stat(&self, uri: &Uri) -> IoResult<Metadata>;

    async fn list(&self, uri: &Uri) -> IoResult<Vec<String>>;

    async fn delete(&self, uri: &Uri) -> IoResult<()>;

    async fn copy(&self, from: &Uri, to: &Uri) -> IoResult<()>;

    /// Whether the backend can serve independent byte ranges concurrently.
    async fn supports_parallel_ranges(&self, _uri: &Uri) -> IoResult<bool> {
        Ok(false)
    }
}
