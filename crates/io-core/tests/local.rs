use std::sync::Arc;

use bytes::Bytes;
use io_core::backend::{IoBackend, local::LocalBackend};
use io_core::{IoError, Uri};
use tempfile::tempdir;
use tokio::io::AsyncReadExt;
use tokio::sync::Barrier;

fn uri(path: impl AsRef<std::path::Path>) -> Uri {
    Uri::Local(path.as_ref().to_path_buf())
}

#[tokio::test]
async fn local_write_read_roundtrip_and_stat() {
    let dir = tempdir().unwrap();
    let target = dir.path().join("nested/deeper/sample.bin");
    let backend = LocalBackend::new();

    backend
        .write(&uri(&target), Bytes::from_static(b"hello world"))
        .await
        .unwrap();

    assert_eq!(
        backend.read_all(&uri(&target)).await.unwrap(),
        Bytes::from_static(b"hello world")
    );
    let metadata = backend.stat(&uri(&target)).await.unwrap();
    assert_eq!(metadata.len, 11);
    assert!(metadata.is_file);
    assert!(!metadata.is_dir);
    assert_eq!(metadata.uri, uri(&target));
    assert!(metadata.modified.is_some());
}

#[tokio::test]
async fn local_read_range_clamps_and_reports_missing() {
    let dir = tempdir().unwrap();
    let target = dir.path().join("range.txt");
    let backend = LocalBackend::new();
    backend
        .write(&uri(&target), Bytes::from_static(b"0123456789"))
        .await
        .unwrap();

    assert_eq!(
        backend.read_range(&uri(&target), 2, 3).await.unwrap(),
        Bytes::from_static(b"234")
    );
    assert_eq!(
        backend.read_range(&uri(&target), 9, 5).await.unwrap(),
        Bytes::from_static(b"9")
    );
    assert!(
        backend
            .read_range(&uri(&target), 20, 5)
            .await
            .unwrap()
            .is_empty()
    );
    assert!(matches!(
        backend
            .read_range(&uri(dir.path().join("missing")), 0, 1)
            .await,
        Err(IoError::NotFound(_))
    ));
}

#[tokio::test]
async fn local_list_copy_delete() {
    let dir = tempdir().unwrap();
    let backend = LocalBackend::new();
    let source = dir.path().join("source.txt");
    let nested = dir.path().join("sub");
    std::fs::create_dir(&nested).unwrap();
    std::fs::write(&source, b"copy me").unwrap();

    let mut names = backend.list(&uri(dir.path())).await.unwrap();
    names.sort();
    assert_eq!(names, vec!["source.txt", "sub/"]);

    let destination = dir.path().join("copied.txt");
    backend
        .copy(&uri(&source), &uri(&destination))
        .await
        .unwrap();
    assert_eq!(std::fs::read(&destination).unwrap(), b"copy me");

    backend.delete(&uri(&destination)).await.unwrap();
    assert!(!destination.exists());
    assert!(matches!(
        backend.delete(&uri(&destination)).await,
        Err(IoError::NotFound(_))
    ));
}

#[tokio::test]
async fn local_write_from_path_and_open_read() {
    let dir = tempdir().unwrap();
    let backend = LocalBackend::new();
    let source = dir.path().join("from.bin");
    std::fs::write(&source, b"streamed").unwrap();
    let target = dir.path().join("to.bin");

    backend
        .write_from_path(&uri(&target), &source, 8)
        .await
        .unwrap();
    assert_eq!(std::fs::read(&target).unwrap(), b"streamed");

    let mut reader = backend.open_read(&uri(&target)).await.unwrap();
    let mut buffer = Vec::new();
    reader.read_to_end(&mut buffer).await.unwrap();
    assert_eq!(buffer, b"streamed");
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn local_atomic_rewrite_never_exposes_partial_payload() {
    let dir = tempdir().unwrap();
    let target = Arc::new(dir.path().join("atomic.bin"));
    let backend = Arc::new(LocalBackend::new());
    let payload_a = Bytes::from(vec![b'A'; 64 * 1024]);
    let payload_b = Bytes::from(vec![b'B'; 64 * 1024]);

    backend
        .write(&uri(target.as_ref()), payload_a.clone())
        .await
        .unwrap();

    let barrier = Arc::new(Barrier::new(2));
    let writer = {
        let backend = backend.clone();
        let target = target.clone();
        let barrier = barrier.clone();
        tokio::spawn(async move {
            barrier.wait().await;
            for iteration in 0..40 {
                let payload = if iteration % 2 == 0 {
                    payload_b.clone()
                } else {
                    payload_a.clone()
                };
                backend.write(&uri(target.as_ref()), payload).await.unwrap();
            }
        })
    };

    let reader = {
        let backend = backend.clone();
        let target = target.clone();
        let barrier = barrier.clone();
        tokio::spawn(async move {
            barrier.wait().await;
            for _ in 0..100 {
                let bytes = backend.read_all(&uri(target.as_ref())).await.unwrap();
                assert_eq!(bytes.len(), 64 * 1024);
                assert!(
                    bytes.iter().all(|byte| *byte == b'A')
                        || bytes.iter().all(|byte| *byte == b'B'),
                    "observed a partially written payload"
                );
            }
        })
    };

    writer.await.unwrap();
    reader.await.unwrap();
}

#[tokio::test]
async fn local_failed_write_leaves_no_temp_file() {
    let dir = tempdir().unwrap();
    let backend = LocalBackend::new();
    let target = dir.path().join("occupied");
    std::fs::create_dir(&target).unwrap();

    let error = backend
        .write(&uri(&target), Bytes::from_static(b"nope"))
        .await
        .unwrap_err();
    assert!(
        !matches!(error, IoError::Unsupported(_)),
        "write must reach the atomic rename path, got {error:?}"
    );

    let names: Vec<String> = std::fs::read_dir(dir.path())
        .unwrap()
        .map(|entry| entry.unwrap().file_name().to_string_lossy().into_owned())
        .collect();
    assert_eq!(names, vec!["occupied".to_string()]);
}

#[cfg(unix)]
#[tokio::test]
async fn local_atomic_write_preserves_existing_permissions() {
    use std::os::unix::fs::PermissionsExt;

    let dir = tempdir().unwrap();
    let backend = LocalBackend::new();
    let target = dir.path().join("secret.txt");
    std::fs::write(&target, b"old").unwrap();
    std::fs::set_permissions(&target, std::fs::Permissions::from_mode(0o600)).unwrap();

    backend
        .write(&uri(&target), Bytes::from_static(b"new"))
        .await
        .unwrap();

    let mode = std::fs::metadata(&target).unwrap().permissions().mode() & 0o777;
    assert_eq!(mode, 0o600);
}

#[tokio::test]
async fn local_copy_large_file() {
    let dir = tempdir().unwrap();
    let backend = LocalBackend::new();
    let source = dir.path().join("source-large.bin");
    let target = dir.path().join("target-large.bin");
    let payload: Vec<u8> = (0..4 * 1024 * 1024)
        .map(|index| (index % 251) as u8)
        .collect();
    std::fs::write(&source, &payload).unwrap();

    backend.copy(&uri(&source), &uri(&target)).await.unwrap();

    assert_eq!(std::fs::read(&target).unwrap(), payload);
}

#[tokio::test]
async fn local_write_supports_long_basenames() {
    let dir = tempdir().unwrap();
    let backend = LocalBackend::new();
    let target = dir.path().join("a".repeat(240));

    backend
        .write(&uri(&target), Bytes::from_static(b"long name"))
        .await
        .unwrap();
    assert_eq!(
        backend.read_all(&uri(&target)).await.unwrap(),
        Bytes::from_static(b"long name")
    );
}
