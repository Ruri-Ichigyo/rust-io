//! S3 backend tests.
//!
//! The in-memory server runs unconditionally. The real-endpoint test runs only
//! when `IO_S3_BUCKET` and `IO_S3_ENDPOINT` are set; otherwise it records a skip.

use std::sync::Arc;
use std::time::{SystemTime, UNIX_EPOCH};

use bytes::Bytes;
use io_core::backend::IoBackend;
use io_core::backend::s3::{S3Backend, S3Config};
use io_core::{IoError, Uri};

mod common;
use common::{S3Store, TestServer, s3_handler};

fn local_backend(server: &TestServer) -> S3Backend {
    S3Backend::with_config(S3Config {
        access_key: Some("test".into()),
        secret_key: Some("test".into()),
        session_token: None,
        endpoint: Some(server.base().to_string()),
        region: Some("us-east-1".into()),
        force_path_style: true,
    })
}

#[tokio::test]
async fn s3_backend_roundtrip_against_in_memory_server() {
    let store = Arc::new(S3Store::new());
    let server = TestServer::start(s3_handler(store)).await;
    let backend = local_backend(&server);
    let suffix = SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .unwrap()
        .as_nanos();
    let prefix = format!("io-core-test/{suffix}");
    let uri = Uri::parse(&format!("s3://bucket/{prefix}/object.bin")).unwrap();

    backend
        .write(&uri, Bytes::from_static(b"hello s3"))
        .await
        .unwrap();
    assert_eq!(
        backend.read_all(&uri).await.unwrap(),
        Bytes::from_static(b"hello s3")
    );
    assert_eq!(
        backend.read_range(&uri, 1, 4).await.unwrap(),
        Bytes::from_static(b"ello")
    );
    assert!(backend.read_range(&uri, 100, 1).await.unwrap().is_empty());

    let metadata = backend.stat(&uri).await.unwrap();
    assert_eq!(metadata.len, 8);
    assert_eq!(
        metadata.content_type.as_deref(),
        Some("application/octet-stream")
    );

    let nested = Uri::parse(&format!("s3://bucket/{prefix}/nested/deeper.bin")).unwrap();
    backend
        .write(&nested, Bytes::from_static(b"nested"))
        .await
        .unwrap();
    let parent = Uri::parse(&format!("s3://bucket/{prefix}/")).unwrap();
    let mut names = backend.list(&parent).await.unwrap();
    names.sort();
    assert_eq!(names, vec!["nested/", "object.bin"], "{names:?}");

    let copy = Uri::parse(&format!("s3://bucket/{prefix}/copy.bin")).unwrap();
    backend.copy(&uri, &copy).await.unwrap();
    assert_eq!(
        backend.read_all(&copy).await.unwrap(),
        Bytes::from_static(b"hello s3")
    );

    let spaced = Uri::parse(&format!("s3://bucket/{prefix}/dir name/file.bin")).unwrap();
    let spaced_copy = Uri::parse(&format!("s3://bucket/{prefix}/dir name/copied.bin")).unwrap();
    backend
        .write(&spaced, Bytes::from_static(b"spaced key"))
        .await
        .unwrap();
    backend.copy(&spaced, &spaced_copy).await.unwrap();
    assert_eq!(
        backend.read_all(&spaced_copy).await.unwrap(),
        Bytes::from_static(b"spaced key")
    );

    backend.delete(&uri).await.unwrap();
    backend.delete(&copy).await.unwrap();
    backend.delete(&spaced).await.unwrap();
    backend.delete(&spaced_copy).await.unwrap();
    assert!(matches!(
        backend.stat(&uri).await,
        Err(IoError::NotFound(_))
    ));
}

#[tokio::test]
async fn s3_roundtrip_when_configured() {
    let Ok(bucket) = std::env::var("IO_S3_BUCKET") else {
        eprintln!("skipped: IO_S3_BUCKET is not set");
        return;
    };
    let config = S3Config::from_env();
    if config.endpoint.is_none() {
        eprintln!("skipped: IO_S3_ENDPOINT is not set");
        return;
    }

    let backend = S3Backend::with_config(config);
    let suffix = SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .unwrap()
        .as_nanos();
    let prefix = format!("io-core-test/{suffix}");
    let uri = Uri::parse(&format!("s3://{bucket}/{prefix}/object.bin")).unwrap();

    backend
        .write(&uri, Bytes::from_static(b"hello s3"))
        .await
        .unwrap();
    assert_eq!(
        backend.read_all(&uri).await.unwrap(),
        Bytes::from_static(b"hello s3")
    );
    assert_eq!(
        backend.read_range(&uri, 1, 4).await.unwrap(),
        Bytes::from_static(b"ello")
    );

    let metadata = backend.stat(&uri).await.unwrap();
    assert_eq!(metadata.len, 8);

    let parent = Uri::parse(&format!("s3://{bucket}/{prefix}/")).unwrap();
    let names = backend.list(&parent).await.unwrap();
    assert!(names.contains(&"object.bin".to_string()), "{names:?}");

    let copy = Uri::parse(&format!("s3://{bucket}/{prefix}/copy.bin")).unwrap();
    backend.copy(&uri, &copy).await.unwrap();
    assert_eq!(
        backend.read_all(&copy).await.unwrap(),
        Bytes::from_static(b"hello s3")
    );

    backend.delete(&uri).await.unwrap();
    backend.delete(&copy).await.unwrap();
    assert!(matches!(
        backend.stat(&uri).await,
        Err(IoError::NotFound(_))
    ));
}
