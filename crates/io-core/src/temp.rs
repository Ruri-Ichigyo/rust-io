use std::path::{Path, PathBuf};
use std::sync::atomic::{AtomicU64, Ordering};
use std::time::{SystemTime, UNIX_EPOCH};

use tokio::fs::{File, OpenOptions};

static TEMP_COUNTER: AtomicU64 = AtomicU64::new(0);

/// A unique sibling path for atomic replacement, removed on drop unless disarmed.
pub(crate) struct TempPath {
    path: PathBuf,
    armed: bool,
}

impl TempPath {
    pub(crate) fn for_target(target: &Path) -> Self {
        let name = target
            .file_name()
            .map(|name| name.to_string_lossy().into_owned())
            .unwrap_or_else(|| "io".to_string());
        // Keep the derived name within common NAME_MAX limits.
        let name: String = name.chars().take(100).collect();
        let suffix = TEMP_COUNTER.fetch_add(1, Ordering::Relaxed);
        let nonce = SystemTime::now()
            .duration_since(UNIX_EPOCH)
            .map(|elapsed| elapsed.subsec_nanos())
            .unwrap_or(0);
        let path = target.with_file_name(format!(
            ".{name}.io-{}-{suffix}-{nonce}.tmp",
            std::process::id()
        ));
        Self { path, armed: true }
    }

    pub(crate) fn path(&self) -> &Path {
        &self.path
    }

    /// Create the temp file exclusively (`O_EXCL`) so a pre-planted symlink
    /// cannot redirect the write. `private` forces mode 0600 on Unix.
    pub(crate) async fn create(&self, private: bool) -> std::io::Result<File> {
        let mut options = OpenOptions::new();
        options.write(true).create_new(true);
        #[cfg(unix)]
        {
            options.mode(if private { 0o600 } else { 0o666 });
        }
        #[cfg(not(unix))]
        let _ = private;
        options.open(&self.path).await
    }

    pub(crate) fn disarm(&mut self) {
        self.armed = false;
    }
}

impl Drop for TempPath {
    fn drop(&mut self) {
        if self.armed {
            let _ = std::fs::remove_file(&self.path);
        }
    }
}
