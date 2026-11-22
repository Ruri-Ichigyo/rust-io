use std::future::Future;
use std::path::{Path, PathBuf};
use std::sync::Arc;
use std::time::{Duration, SystemTime, UNIX_EPOCH};

use bytes::Bytes;
use futures::{StreamExt, stream};
use sha2::{Digest, Sha256};
use tokio::io::{AsyncReadExt, AsyncWriteExt};

use crate::backend::http::HttpBackend;
use crate::backend::local::LocalBackend;
use crate::backend::s3::S3Backend;
use crate::backend::{IO_CHUNK, IoBackend, Metadata};
use crate::error::{IoError, IoResult};
use crate::temp::TempPath;
use crate::uri::{Scheme, Uri};

pub const DEFAULT_CONCURRENCY: usize = 8;

/// Bounded retry policy for transient backend errors.
#[derive(Debug, Clone)]
pub struct RetryPolicy {
    pub max_attempts: u32,
    pub base_delay: Duration,
    pub max_delay: Duration,
}

impl Default for RetryPolicy {
    fn default() -> Self {
        Self {
            max_attempts: 3,
            base_delay: Duration::from_millis(50),
            max_delay: Duration::from_secs(2),
        }
    }
}

impl RetryPolicy {
    pub fn none() -> Self {
        Self {
            max_attempts: 1,
            base_delay: Duration::ZERO,
            max_delay: Duration::ZERO,
        }
    }

    pub fn with_max_attempts(mut self, attempts: u32) -> Self {
        self.max_attempts = attempts;
        self
    }

    pub fn with_delay(mut self, base: Duration, max: Duration) -> Self {
        self.base_delay = base;
        self.max_delay = max;
        self
    }
}

/// Builder for a shared [`IoEngine`].
#[derive(Default)]
pub struct IoEngineBuilder {
    retry: RetryPolicy,
    default_timeout: Option<Duration>,
    http: Option<HttpBackend>,
    s3: Option<S3Backend>,
}

impl IoEngineBuilder {
    pub fn retry(mut self, retry: RetryPolicy) -> Self {
        self.retry = retry;
        self
    }

    pub fn default_timeout(mut self, timeout: Duration) -> Self {
        self.default_timeout = Some(timeout);
        self
    }

    pub fn http(mut self, backend: HttpBackend) -> Self {
        self.http = Some(backend);
        self
    }

    pub fn s3(mut self, backend: S3Backend) -> Self {
        self.s3 = Some(backend);
        self
    }

    pub fn build(self) -> IoResult<IoEngine> {
        let http = match self.http {
            Some(backend) => backend,
            None => HttpBackend::new()?,
        };
        Ok(IoEngine {
            local: Arc::new(LocalBackend::new()),
            http: Arc::new(http),
            s3: Arc::new(self.s3.unwrap_or_default()),
            retry: self.retry,
            default_timeout: self.default_timeout,
        })
    }
}

/// Facade over all backends: routing, deadlines, retries, multiplexing, streaming.
#[derive(Clone)]
pub struct IoEngine {
    local: Arc<dyn IoBackend>,
    http: Arc<dyn IoBackend>,
    s3: Arc<dyn IoBackend>,
    retry: RetryPolicy,
    default_timeout: Option<Duration>,
}

fn path_error(path: &Path, error: std::io::Error) -> IoError {
    IoError::from_io(&path.display().to_string(), error)
}

fn hash_local(path: &Path) -> IoResult<String> {
    use std::io::Read;

    let mut file = std::fs::File::open(path).map_err(|error| path_error(path, error))?;
    let mut hasher = Sha256::new();
    let mut buffer = vec![0u8; IO_CHUNK];
    loop {
        let read = file
            .read(&mut buffer)
            .map_err(|error| path_error(path, error))?;
        if read == 0 {
            break;
        }
        hasher.update(&buffer[..read]);
    }
    Ok(hex::encode(hasher.finalize()))
}

impl IoEngine {
    pub fn builder() -> IoEngineBuilder {
        IoEngineBuilder::default()
    }

    pub fn new() -> IoResult<Self> {
        Self::builder().build()
    }

    pub fn retry_policy(&self) -> &RetryPolicy {
        &self.retry
    }

    fn backend(&self, uri: &Uri) -> &Arc<dyn IoBackend> {
        match uri.scheme() {
            Scheme::Local => &self.local,
            Scheme::Http => &self.http,
            Scheme::S3 => &self.s3,
        }
    }

    fn backoff(&self, attempt: u32) -> Duration {
        let factor = 1u32 << attempt.saturating_sub(1).min(16);
        let delay = self
            .retry
            .base_delay
            .checked_mul(factor)
            .unwrap_or(self.retry.max_delay)
            .min(self.retry.max_delay);
        let jitter = SystemTime::now()
            .duration_since(UNIX_EPOCH)
            .map(|elapsed| elapsed.subsec_nanos() % 50)
            .unwrap_or(0);
        delay + Duration::from_millis(u64::from(jitter))
    }

    async fn with_retry<T, F, Fut>(
        &self,
        timeout: Option<Duration>,
        mut operation: F,
    ) -> IoResult<T>
    where
        F: FnMut() -> Fut,
        Fut: Future<Output = IoResult<T>>,
    {
        let attempts = self.retry.max_attempts.max(1);
        let deadline = timeout.or(self.default_timeout);
        let run = async {
            let mut last_error = None;

            for attempt in 1..=attempts {
                match operation().await {
                    Ok(value) => return Ok(value),
                    Err(error) => {
                        let retry = error.is_retryable() && attempt < attempts;
                        last_error = Some(error);
                        if !retry {
                            break;
                        }
                        tokio::time::sleep(self.backoff(attempt)).await;
                    }
                }
            }

            Err(last_error
                .unwrap_or_else(|| IoError::Other("retry loop produced no result".into())))
        };

        match deadline {
            Some(limit) => tokio::time::timeout(limit, run)
                .await
                .map_err(|_| IoError::Timeout(limit))?,
            None => run.await,
        }
    }

    pub async fn read(&self, uri: &Uri, timeout: Option<Duration>) -> IoResult<Bytes> {
        let backend = self.backend(uri).clone();
        let uri = uri.clone();
        self.with_retry(timeout, || {
            let backend = backend.clone();
            let uri = uri.clone();
            async move { backend.read_all(&uri).await }
        })
        .await
    }

    pub async fn read_range(
        &self,
        uri: &Uri,
        offset: u64,
        length: u64,
        timeout: Option<Duration>,
    ) -> IoResult<Bytes> {
        if length == 0 {
            return Ok(Bytes::new());
        }
        let backend = self.backend(uri).clone();
        let uri = uri.clone();
        self.with_retry(timeout, || {
            let backend = backend.clone();
            let uri = uri.clone();
            async move { backend.read_range(&uri, offset, length).await }
        })
        .await
    }

    pub async fn write(&self, uri: &Uri, data: Bytes, timeout: Option<Duration>) -> IoResult<()> {
        let backend = self.backend(uri).clone();
        let uri = uri.clone();
        self.with_retry(timeout, || {
            let backend = backend.clone();
            let uri = uri.clone();
            let data = data.clone();
            async move { backend.write(&uri, data).await }
        })
        .await
    }

    pub async fn stat(&self, uri: &Uri, timeout: Option<Duration>) -> IoResult<Metadata> {
        let backend = self.backend(uri).clone();
        let uri = uri.clone();
        self.with_retry(timeout, || {
            let backend = backend.clone();
            let uri = uri.clone();
            async move { backend.stat(&uri).await }
        })
        .await
    }

    pub async fn list(&self, uri: &Uri, timeout: Option<Duration>) -> IoResult<Vec<String>> {
        let backend = self.backend(uri).clone();
        let uri = uri.clone();
        self.with_retry(timeout, || {
            let backend = backend.clone();
            let uri = uri.clone();
            async move { backend.list(&uri).await }
        })
        .await
    }

    pub async fn delete(&self, uri: &Uri, timeout: Option<Duration>) -> IoResult<()> {
        let backend = self.backend(uri).clone();
        let uri = uri.clone();
        self.with_retry(timeout, || {
            let backend = backend.clone();
            let uri = uri.clone();
            async move { backend.delete(&uri).await }
        })
        .await
    }

    pub async fn copy(&self, from: &Uri, to: &Uri, timeout: Option<Duration>) -> IoResult<()> {
        if from.scheme() == to.scheme() {
            let backend = self.backend(from).clone();
            let from = from.clone();
            let to = to.clone();
            self.with_retry(timeout, || {
                let backend = backend.clone();
                let from = from.clone();
                let to = to.clone();
                async move { backend.copy(&from, &to).await }
            })
            .await
        } else {
            let from = from.clone();
            let to = to.clone();
            self.with_retry(timeout, || {
                let engine = self.clone();
                let from = from.clone();
                let to = to.clone();
                async move { engine.copy_cross(&from, &to).await }
            })
            .await
        }
    }

    async fn copy_cross(&self, from: &Uri, to: &Uri) -> IoResult<()> {
        let spill = std::env::temp_dir().join(format!("io-core-copy-{}", std::process::id()));
        let temp = TempPath::for_target(&spill);
        let path = temp.path().to_path_buf();
        self.download_once(from, &path).await?;
        self.upload_once(&path, to).await?;
        Ok(())
    }

    async fn download_once(&self, uri: &Uri, path: &Path) -> IoResult<u64> {
        let backend = self.backend(uri).clone();
        let uri = uri.clone();
        let path = path.to_path_buf();
        let mut reader = backend.open_read(&uri).await?;

        if let Some(parent) = path
            .parent()
            .filter(|parent| !parent.as_os_str().is_empty())
        {
            tokio::fs::create_dir_all(parent)
                .await
                .map_err(|error| path_error(parent, error))?;
        }

        let mut temp = TempPath::for_target(&path);
        let temp_path = temp.path().to_path_buf();
        let mut file = temp
            .create(true)
            .await
            .map_err(|error| path_error(&temp_path, error))?;
        let mut buffer = vec![0u8; IO_CHUNK];
        let mut copied = 0u64;
        loop {
            let read = reader
                .read(&mut buffer)
                .await
                .map_err(|error| IoError::from_io(&uri.to_string(), error))?;
            if read == 0 {
                break;
            }
            file.write_all(&buffer[..read])
                .await
                .map_err(|error| path_error(&temp_path, error))?;
            copied += read as u64;
        }
        file.sync_all()
            .await
            .map_err(|error| path_error(&temp_path, error))?;
        drop(file);

        tokio::fs::rename(temp.path(), &path)
            .await
            .map_err(|error| path_error(&path, error))?;
        temp.disarm();
        Ok(copied)
    }

    async fn upload_once(&self, path: &Path, uri: &Uri) -> IoResult<()> {
        let metadata = tokio::fs::metadata(path)
            .await
            .map_err(|error| path_error(path, error))?;
        if !metadata.is_file() {
            return Err(IoError::InvalidConfig(format!(
                "upload source is not a file: {}",
                path.display()
            )));
        }
        let backend = self.backend(uri).clone();
        let uri = uri.clone();
        let path = path.to_path_buf();
        backend.write_from_path(&uri, &path, metadata.len()).await
    }

    pub async fn download(
        &self,
        uri: &Uri,
        path: &Path,
        timeout: Option<Duration>,
    ) -> IoResult<u64> {
        let uri = uri.clone();
        let path = path.to_path_buf();
        self.with_retry(timeout, || {
            let engine = self.clone();
            let uri = uri.clone();
            let path = path.clone();
            async move { engine.download_once(&uri, &path).await }
        })
        .await
    }

    pub async fn upload(&self, path: &Path, uri: &Uri, timeout: Option<Duration>) -> IoResult<()> {
        let uri = uri.clone();
        let path = path.to_path_buf();
        self.with_retry(timeout, || {
            let engine = self.clone();
            let uri = uri.clone();
            let path = path.clone();
            async move { engine.upload_once(&path, &uri).await }
        })
        .await
    }

    async fn hash_once(&self, uri: &Uri) -> IoResult<String> {
        let backend = self.backend(uri).clone();
        let uri = uri.clone();
        let mut reader = backend.open_read(&uri).await?;
        let mut hasher = Sha256::new();
        let mut buffer = vec![0u8; IO_CHUNK];
        loop {
            let read = reader
                .read(&mut buffer)
                .await
                .map_err(|error| IoError::from_io(&uri.to_string(), error))?;
            if read == 0 {
                break;
            }
            hasher.update(&buffer[..read]);
        }
        Ok(hex::encode(hasher.finalize()))
    }

    pub async fn hash(&self, uri: &Uri, timeout: Option<Duration>) -> IoResult<String> {
        let uri = uri.clone();
        self.with_retry(timeout, || {
            let engine = self.clone();
            let uri = uri.clone();
            async move { engine.hash_once(&uri).await }
        })
        .await
    }

    pub async fn hash_many(
        &self,
        uris: &[Uri],
        limit: usize,
        timeout: Option<Duration>,
    ) -> IoResult<Vec<(Uri, IoResult<String>)>> {
        if limit == 0 {
            return Err(IoError::InvalidConfig(
                "concurrency limit must be greater than zero".into(),
            ));
        }

        if uris.iter().all(|uri| matches!(uri, Uri::Local(_))) {
            let paths: Vec<PathBuf> = uris
                .iter()
                .map(|uri| match uri {
                    Uri::Local(path) => path.clone(),
                    _ => unreachable!("checked above"),
                })
                .collect();
            let threads = limit.min(
                std::thread::available_parallelism()
                    .map(|count| count.get())
                    .unwrap_or(limit),
            );
            let pool = rayon::ThreadPoolBuilder::new()
                .num_threads(threads)
                .build()
                .map_err(|error| IoError::Other(format!("failed to build hash pool: {error}")))?;
            let task = tokio::task::spawn_blocking(move || {
                use rayon::prelude::*;
                pool.install(|| {
                    paths
                        .into_par_iter()
                        .map(|path| hash_local(&path))
                        .collect::<Vec<_>>()
                })
            });
            let hashes = match timeout.or(self.default_timeout) {
                Some(deadline) => tokio::time::timeout(deadline, task)
                    .await
                    .map_err(|_| IoError::Timeout(deadline))?
                    .map_err(|error| IoError::Other(format!("hashing task failed: {error}")))?,
                None => task
                    .await
                    .map_err(|error| IoError::Other(format!("hashing task failed: {error}")))?,
            };
            return Ok(uris.iter().cloned().zip(hashes).collect());
        }

        self.run_bounded(uris, limit, timeout, |engine, uri, timeout| async move {
            engine.hash(&uri, timeout).await
        })
        .await
    }

    async fn run_bounded<T, R, F, Fut>(
        &self,
        items: &[T],
        limit: usize,
        timeout: Option<Duration>,
        operation: F,
    ) -> IoResult<Vec<(T, IoResult<R>)>>
    where
        T: Clone + Send,
        R: Send,
        F: Fn(IoEngine, T, Option<Duration>) -> Fut + Send + Sync,
        Fut: Future<Output = IoResult<R>> + Send,
    {
        if limit == 0 {
            return Err(IoError::InvalidConfig(
                "concurrency limit must be greater than zero".into(),
            ));
        }
        let tasks = items.iter().cloned().enumerate().map(|(index, item)| {
            let engine = self.clone();
            let future = operation(engine, item.clone(), timeout);
            async move { (index, item, future.await) }
        });
        let mut buffered = stream::iter(tasks).buffer_unordered(limit);
        let mut collected = Vec::with_capacity(items.len());
        while let Some(item) = buffered.next().await {
            collected.push(item);
        }
        collected.sort_by_key(|(index, _, _)| *index);
        Ok(collected
            .into_iter()
            .map(|(_, item, result)| (item, result))
            .collect())
    }

    pub async fn read_many(
        &self,
        uris: &[Uri],
        limit: usize,
        timeout: Option<Duration>,
    ) -> IoResult<Vec<(Uri, IoResult<Bytes>)>> {
        self.run_bounded(uris, limit, timeout, |engine, uri, timeout| async move {
            engine.read(&uri, timeout).await
        })
        .await
    }

    pub async fn write_many(
        &self,
        items: &[(Uri, Bytes)],
        limit: usize,
        timeout: Option<Duration>,
    ) -> IoResult<Vec<(Uri, IoResult<()>)>> {
        let results = self
            .run_bounded(items, limit, timeout, |engine, (uri, data), timeout| async move {
                engine.write(&uri, data, timeout).await
            })
            .await?;
        Ok(results
            .into_iter()
            .map(|((uri, _), result)| (uri, result))
            .collect())
    }

    pub async fn copy_many(
        &self,
        pairs: &[(Uri, Uri)],
        limit: usize,
        timeout: Option<Duration>,
    ) -> IoResult<Vec<(Uri, IoResult<()>)>> {
        let results = self
            .run_bounded(
                pairs,
                limit,
                timeout,
                |engine, (from, to), timeout| async move { engine.copy(&from, &to, timeout).await },
            )
            .await?;
        Ok(results
            .into_iter()
            .map(|((from, _), result)| (from, result))
            .collect())
    }

    pub async fn read_any(
        &self,
        uris: &[Uri],
        timeout: Option<Duration>,
    ) -> IoResult<(Uri, Bytes)> {
        if uris.is_empty() {
            return Err(IoError::AllFailed(Vec::new()));
        }
        let tasks = uris.iter().cloned().map(|uri| {
            let engine = self.clone();
            async move {
                let result = engine.read(&uri, timeout).await;
                (uri, result)
            }
        });
        let mut buffered = stream::iter(tasks).buffer_unordered(uris.len());
        let mut failures = Vec::new();
        while let Some((uri, result)) = buffered.next().await {
            match result {
                Ok(bytes) => return Ok((uri, bytes)),
                Err(error) => failures.push((uri.to_string(), error)),
            }
        }
        Err(IoError::AllFailed(failures))
    }
}
