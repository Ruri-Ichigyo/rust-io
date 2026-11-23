use std::path::Path;
use std::time::UNIX_EPOCH;

use async_trait::async_trait;
use bytes::Bytes;
use tokio::io::{AsyncRead, AsyncReadExt, AsyncSeekExt, AsyncWriteExt};

use super::{IoBackend, Metadata};
use crate::error::{IoError, IoResult};
use crate::temp::TempPath;
use crate::uri::{Scheme, Uri};

#[derive(Debug, Default, Clone)]
pub struct LocalBackend;

impl LocalBackend {
    pub fn new() -> Self {
        Self
    }
}

fn local_path(uri: &Uri) -> IoResult<&Path> {
    match uri {
        Uri::Local(path) => Ok(path),
        other => Err(IoError::Unsupported(format!(
            "local backend cannot handle {other}"
        ))),
    }
}

fn path_error(path: &Path, error: std::io::Error) -> IoError {
    IoError::from_io(&path.display().to_string(), error)
}

fn temp_path(target: &Path) -> TempPath {
    TempPath::for_target(target)
}

async fn ensure_parent(target: &Path) -> IoResult<()> {
    if let Some(parent) = target
        .parent()
        .filter(|parent| !parent.as_os_str().is_empty())
    {
        tokio::fs::create_dir_all(parent)
            .await
            .map_err(|error| path_error(parent, error))?;
    }
    Ok(())
}

async fn finish_atomic(temp: &mut TempPath, target: &Path) -> IoResult<()> {
    // Replacing an existing file must not widen its mode (the temp file uses
    // the process umask). Best effort: a metadata failure just keeps defaults.
    if let Ok(existing) = tokio::fs::metadata(target).await {
        let _ = tokio::fs::set_permissions(temp.path(), existing.permissions()).await;
    }
    tokio::fs::rename(temp.path(), target)
        .await
        .map_err(|error| path_error(target, error))?;
    temp.disarm();

    if let Some(parent) = target
        .parent()
        .filter(|parent| !parent.as_os_str().is_empty())
        && let Ok(directory) = tokio::fs::File::open(parent).await
    {
        let _ = directory.sync_all().await;
    }
    Ok(())
}

async fn write_atomic(target: &Path, data: &[u8]) -> IoResult<()> {
    ensure_parent(target).await?;
    let mut temp = temp_path(target);
    let path = temp.path().to_path_buf();

    let mut file = temp
        .create(false)
        .await
        .map_err(|error| path_error(&path, error))?;
    file.write_all(data)
        .await
        .map_err(|error| path_error(&path, error))?;
    file.sync_all()
        .await
        .map_err(|error| path_error(&path, error))?;
    drop(file);

    finish_atomic(&mut temp, target).await
}

async fn copy_atomic(source: &Path, target: &Path) -> IoResult<u64> {
    ensure_parent(target).await?;
    let mut temp = temp_path(target);
    let temp_path = temp.path().to_path_buf();
    let source = source.to_path_buf();
    let destination = temp_path.clone();

    let copied = tokio::task::spawn_blocking(move || copy_file(&source, &destination))
        .await
        .map_err(|error| IoError::Other(format!("copy task failed: {error}")))?
        .map_err(|error| path_error(&temp_path, error))?;

    finish_atomic(&mut temp, target).await?;
    Ok(copied)
}

fn copy_file(source: &Path, target: &Path) -> std::io::Result<u64> {
    // Linux: try a copy-on-write reflink first; fall back to the kernel's
    // copy_file_range path via std::fs::copy.
    #[cfg(target_os = "linux")]
    {
        use std::os::unix::fs::OpenOptionsExt;
        use std::os::unix::fs::PermissionsExt;

        let src = std::fs::File::open(source)?;
        let dst = std::fs::OpenOptions::new()
            .write(true)
            .create_new(true)
            .mode(0o600)
            .open(target)?;
        if rustix::fs::ioctl_ficlone(&dst, &src).is_ok() {
            if let Ok(metadata) = src.metadata() {
                let mode = metadata.permissions().mode() & 0o777;
                let _ = dst.set_permissions(std::fs::Permissions::from_mode(mode));
            }
            dst.sync_all()?;
            return Ok(src.metadata()?.len());
        }
        drop(dst);
        std::fs::remove_file(target)?;
    }
    std::fs::copy(source, target)
}

#[async_trait]
impl IoBackend for LocalBackend {
    fn scheme(&self) -> Scheme {
        Scheme::Local
    }

    async fn read_all(&self, uri: &Uri) -> IoResult<Bytes> {
        let path = local_path(uri)?;
        tokio::fs::read(path)
            .await
            .map(Bytes::from)
            .map_err(|error| path_error(path, error))
    }

    async fn read_range(&self, uri: &Uri, offset: u64, length: u64) -> IoResult<Bytes> {
        if length == 0 {
            return Ok(Bytes::new());
        }
        let path = local_path(uri)?;
        let mut file = tokio::fs::File::open(path)
            .await
            .map_err(|error| path_error(path, error))?;

        file.seek(std::io::SeekFrom::Start(offset))
            .await
            .map_err(|error| path_error(path, error))?;
        let mut buffer = Vec::with_capacity(length.min(1024 * 1024) as usize);
        file.take(length)
            .read_to_end(&mut buffer)
            .await
            .map_err(|error| path_error(path, error))?;
        Ok(Bytes::from(buffer))
    }

    async fn write(&self, uri: &Uri, data: Bytes) -> IoResult<()> {
        write_atomic(local_path(uri)?, &data).await
    }

    async fn open_read(&self, uri: &Uri) -> IoResult<Box<dyn AsyncRead + Send + Unpin>> {
        let path = local_path(uri)?;
        let file = tokio::fs::File::open(path)
            .await
            .map_err(|error| path_error(path, error))?;
        Ok(Box::new(file))
    }

    async fn write_from_path(&self, uri: &Uri, path: &Path, _len: u64) -> IoResult<()> {
        let target = local_path(uri)?;
        copy_atomic(path, target).await?;
        Ok(())
    }

    async fn stat(&self, uri: &Uri) -> IoResult<Metadata> {
        let path = local_path(uri)?;
        let metadata = tokio::fs::metadata(path)
            .await
            .map_err(|error| path_error(path, error))?;
        Ok(Metadata {
            uri: uri.clone(),
            len: metadata.len(),
            is_file: metadata.is_file(),
            is_dir: metadata.is_dir(),
            modified: metadata
                .modified()
                .ok()
                .and_then(|time| time.duration_since(UNIX_EPOCH).ok())
                .map(|duration| duration.as_secs()),
            content_type: None,
            etag: None,
        })
    }

    async fn list(&self, uri: &Uri) -> IoResult<Vec<String>> {
        let path = local_path(uri)?;
        let mut entries = Vec::new();
        let mut reader = tokio::fs::read_dir(path)
            .await
            .map_err(|error| path_error(path, error))?;
        while let Some(entry) = reader
            .next_entry()
            .await
            .map_err(|error| path_error(path, error))?
        {
            let file_type = entry
                .file_type()
                .await
                .map_err(|error| path_error(path, error))?;
            let name = entry.file_name().to_string_lossy().into_owned();
            entries.push(if file_type.is_dir() {
                format!("{name}/")
            } else {
                name
            });
        }
        entries.sort();
        Ok(entries)
    }

    async fn delete(&self, uri: &Uri) -> IoResult<()> {
        let path = local_path(uri)?;
        tokio::fs::remove_file(path)
            .await
            .map_err(|error| path_error(path, error))
    }

    async fn copy(&self, from: &Uri, to: &Uri) -> IoResult<()> {
        let source = local_path(from)?;
        let target = local_path(to)?;
        if source == target {
            return Ok(());
        }
        copy_atomic(source, target).await?;
        Ok(())
    }
}
