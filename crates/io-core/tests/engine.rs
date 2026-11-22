use std::collections::HashMap;
use std::sync::Arc;
use std::sync::atomic::{AtomicUsize, Ordering};
use std::time::Duration;

use bytes::Bytes;
use io_core::backend::http::HttpBackend;
use io_core::{IoEngine, IoError, IoResult, MuxOp, MuxOutcome, RetryPolicy, Uri};

mod common;
use common::{Response, TestServer};

const SHA256_ABC: &str = "ba7816bf8f01cfea414140de5dae2223b00361a396177a9cb410ff61f20015ad";

fn uri(path: impl AsRef<std::path::Path>) -> Uri {
    Uri::Local(path.as_ref().to_path_buf())
}

fn http_uri(server: &TestServer, path: &str) -> Uri {
    Uri::parse(&server.url(path)).unwrap()
}

#[tokio::test]
async fn engine_routes_local_and_http_and_enforces_timeout() {
    let server = TestServer::start(|request| match request.path.as_str() {
        "/slow" => Response::ok(b"late".to_vec()).with_delay(Duration::from_millis(250)),
        "/ok" => Response::ok(b"http".to_vec()),
        _ => Response::status(404),
    })
    .await;

    let engine = IoEngine::builder()
        .retry(RetryPolicy::none())
        .build()
        .unwrap();
    let dir = tempfile::tempdir().unwrap();
    let local = dir.path().join("local.txt");
    std::fs::write(&local, b"local").unwrap();

    assert_eq!(
        engine.read(&uri(&local), None).await.unwrap(),
        Bytes::from_static(b"local")
    );
    assert_eq!(
        engine.read(&http_uri(&server, "/ok"), None).await.unwrap(),
        Bytes::from_static(b"http")
    );

    let error = engine
        .read(&http_uri(&server, "/slow"), Some(Duration::from_millis(40)))
        .await
        .unwrap_err();
    assert!(matches!(error, IoError::Timeout(_)), "got {error:?}");
}

#[tokio::test]
async fn engine_retries_transient_errors_only() {
    let flaky_hits = Arc::new(AtomicUsize::new(0));
    let hits = flaky_hits.clone();
    let flaky = TestServer::start(move |_| {
        let attempt = hits.fetch_add(1, Ordering::SeqCst);
        if attempt == 0 {
            Response::status(500)
        } else {
            Response::ok(b"recovered".to_vec())
        }
    })
    .await;

    let engine = IoEngine::builder()
        .retry(
            RetryPolicy::default()
                .with_max_attempts(3)
                .with_delay(Duration::from_millis(1), Duration::from_millis(5)),
        )
        .build()
        .unwrap();

    assert_eq!(
        engine.read(&http_uri(&flaky, "/x"), None).await.unwrap(),
        Bytes::from_static(b"recovered")
    );
    assert_eq!(flaky_hits.load(Ordering::SeqCst), 2);

    let permanent_hits = Arc::new(AtomicUsize::new(0));
    let hits = permanent_hits.clone();
    let permanent = TestServer::start(move |_| {
        hits.fetch_add(1, Ordering::SeqCst);
        Response::status(404)
    })
    .await;

    assert!(matches!(
        engine.read(&http_uri(&permanent, "/x"), None).await,
        Err(IoError::NotFound(_))
    ));
    assert_eq!(permanent_hits.load(Ordering::SeqCst), 1);

    let zero_hits = Arc::new(AtomicUsize::new(0));
    let hits = zero_hits.clone();
    let zero = TestServer::start(move |_| {
        hits.fetch_add(1, Ordering::SeqCst);
        Response::status(500)
    })
    .await;
    let no_attempts = IoEngine::builder()
        .retry(RetryPolicy::none().with_max_attempts(0))
        .build()
        .unwrap();
    assert!(
        no_attempts
            .read(&http_uri(&zero, "/x"), None)
            .await
            .is_err()
    );
    assert_eq!(zero_hits.load(Ordering::SeqCst), 1);
}

#[tokio::test]
async fn engine_zero_length_range_is_empty_for_every_scheme() {
    let server = TestServer::start(|_| Response::status(500)).await;
    let engine = IoEngine::new().unwrap();
    let dir = tempfile::tempdir().unwrap();

    assert!(
        engine
            .read_range(&uri(dir.path().join("missing")), 0, 0, None)
            .await
            .unwrap()
            .is_empty()
    );
    assert!(
        engine
            .read_range(&http_uri(&server, "/x"), 0, 0, None)
            .await
            .unwrap()
            .is_empty()
    );
    assert_eq!(server.stats().requests(), 0);
}

#[tokio::test]
async fn engine_retries_truncated_download_and_cleans_temp_files() {
    let hits = Arc::new(AtomicUsize::new(0));
    let counter = hits.clone();
    let server = TestServer::start(move |_| {
        let attempt = counter.fetch_add(1, Ordering::SeqCst);
        if attempt == 0 {
            Response::ok(b"short".to_vec()).with_header("Content-Length", "100")
        } else {
            Response::ok(b"recovered body".to_vec())
        }
    })
    .await;

    let engine = IoEngine::builder()
        .retry(
            RetryPolicy::default()
                .with_max_attempts(3)
                .with_delay(Duration::from_millis(1), Duration::from_millis(5)),
        )
        .build()
        .unwrap();
    let dir = tempfile::tempdir().unwrap();
    let destination = dir.path().join("nested").join("out.bin");
    let written = engine
        .download(
            &http_uri(&server, "/flaky"),
            &destination,
            Some(Duration::from_secs(5)),
        )
        .await
        .unwrap();

    assert_eq!(written, 14);
    assert_eq!(std::fs::read(&destination).unwrap(), b"recovered body");
    assert_eq!(hits.load(Ordering::SeqCst), 2);

    let names: Vec<String> = std::fs::read_dir(destination.parent().unwrap())
        .unwrap()
        .map(|entry| entry.unwrap().file_name().to_string_lossy().into_owned())
        .collect();
    assert_eq!(names, vec!["out.bin".to_string()]);
}

#[tokio::test]
async fn engine_mux_runs_heterogeneous_ops_in_completion_order() {
    let server = TestServer::start(|_| {
        Response::ok(b"slow".to_vec()).with_delay(Duration::from_millis(300))
    })
    .await;
    let dir = tempfile::tempdir().unwrap();
    let file = dir.path().join("local.txt");
    std::fs::write(&file, b"local").unwrap();
    let missing = dir.path().join("missing.txt");
    let created = dir.path().join("created.txt");
    let engine = IoEngine::new().unwrap();

    let ops = vec![
        MuxOp::Read(http_uri(&server, "/slow")),
        MuxOp::Read(uri(&file)),
        MuxOp::Stat(uri(&file)),
        MuxOp::Hash(uri(&file)),
        MuxOp::Read(uri(&missing)),
        MuxOp::Write(uri(&created), Bytes::from_static(b"new")),
    ];
    let results = engine.mux(&ops, 4, None).await.unwrap();

    assert_eq!(results.len(), 6);
    assert_ne!(
        results[0].0, 0,
        "the 300ms HTTP read must not complete first"
    );
    let by_index: HashMap<usize, IoResult<MuxOutcome>> = results.into_iter().collect();
    match &by_index[&1] {
        Ok(MuxOutcome::Bytes(bytes)) => assert_eq!(bytes, &Bytes::from_static(b"local")),
        other => panic!("unexpected read outcome: {other:?}"),
    }
    assert!(matches!(&by_index[&2], Ok(MuxOutcome::Metadata(_))));
    assert!(matches!(&by_index[&3], Ok(MuxOutcome::Hash(_))));
    assert!(matches!(&by_index[&4], Err(IoError::NotFound(_))));
    assert!(matches!(&by_index[&5], Ok(MuxOutcome::Unit)));
    assert_eq!(std::fs::read(&created).unwrap(), b"new");

    assert!(matches!(
        engine.mux(&ops, 0, None).await,
        Err(IoError::InvalidConfig(_))
    ));
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn engine_bounds_concurrency_and_preserves_order() {
    let hits = Arc::new(AtomicUsize::new(0));
    let counter = hits.clone();
    let server = TestServer::start(move |_| {
        counter.fetch_add(1, Ordering::SeqCst);
        Response::ok(b"x".to_vec()).with_delay(Duration::from_millis(30))
    })
    .await;

    let engine = IoEngine::builder()
        .retry(RetryPolicy::none())
        .build()
        .unwrap();
    let uris: Vec<Uri> = (0..12)
        .map(|index| http_uri(&server, &format!("/item/{index}")))
        .collect();

    let results = engine.read_many(&uris, 4, None).await.unwrap();
    assert_eq!(results.len(), uris.len());
    for (index, (result_uri, result)) in results.iter().enumerate() {
        assert_eq!(result_uri, &uris[index]);
        assert_eq!(result.as_ref().unwrap(), &Bytes::from_static(b"x"));
    }

    let max_active = server.stats().max_active();
    assert!(
        max_active <= 4,
        "limit not enforced: {max_active} concurrent requests"
    );
    assert!(
        max_active >= 2,
        "expected real concurrency, got {max_active}"
    );

    let before = server.stats().requests();
    assert!(matches!(
        engine.read_many(&uris, 0, None).await,
        Err(IoError::InvalidConfig(_))
    ));
    assert_eq!(server.stats().requests(), before);
}

#[tokio::test]
async fn engine_read_any_returns_first_success() {
    let server = TestServer::start(|request| match request.path.as_str() {
        "/slow" => Response::status(500).with_delay(Duration::from_millis(200)),
        "/fast" => Response::ok(b"fast".to_vec()).with_delay(Duration::from_millis(5)),
        _ => Response::status(404),
    })
    .await;

    let engine = IoEngine::new().unwrap();
    let uris = vec![http_uri(&server, "/slow"), http_uri(&server, "/fast")];
    let (winner, bytes) = engine.read_any(&uris, None).await.unwrap();
    assert_eq!(winner, uris[1]);
    assert_eq!(bytes, Bytes::from_static(b"fast"));

    assert!(matches!(
        engine.read_any(&[], None).await,
        Err(IoError::AllFailed(_))
    ));
}

#[tokio::test]
async fn engine_streams_downloads_uploads_and_hashes() {
    let captured: Arc<std::sync::Mutex<Vec<u8>>> = Arc::new(std::sync::Mutex::new(Vec::new()));
    let stored = captured.clone();
    let server =
        TestServer::start(
            move |request| match (request.method.as_str(), request.path.as_str()) {
                ("GET", "/obj") => Response::ok(b"http payload".to_vec()),
                ("PUT", "/upload") => {
                    *stored.lock().unwrap() = request.body;
                    Response::status(204)
                }
                _ => Response::status(404),
            },
        )
        .await;

    let engine = IoEngine::builder()
        .retry(RetryPolicy::none())
        .http(HttpBackend::new().unwrap())
        .build()
        .unwrap();
    let dir = tempfile::tempdir().unwrap();
    let downloaded = dir.path().join("downloaded.bin");
    let written = engine
        .download(&http_uri(&server, "/obj"), &downloaded, None)
        .await
        .unwrap();
    assert_eq!(written, 12);
    assert_eq!(std::fs::read(&downloaded).unwrap(), b"http payload");

    let source = dir.path().join("abc.bin");
    std::fs::write(&source, b"abc").unwrap();
    engine
        .upload(&source, &http_uri(&server, "/upload"), None)
        .await
        .unwrap();
    assert_eq!(captured.lock().unwrap().as_slice(), b"abc");

    assert_eq!(engine.hash(&uri(&source), None).await.unwrap(), SHA256_ABC);
    let missing = dir.path().join("missing.bin");
    let hashes = engine
        .hash_many(&[uri(&source), uri(&missing)], 2, None)
        .await
        .unwrap();
    assert_eq!(hashes[0].1.as_ref().unwrap(), SHA256_ABC);
    assert!(matches!(hashes[1].1, Err(IoError::NotFound(_))));

    let target = dir.path().join("nested/copied.bin");
    engine
        .copy(&uri(&source), &uri(&target), None)
        .await
        .unwrap();
    assert_eq!(std::fs::read(&target).unwrap(), b"abc");

    let http_copy = http_uri(&server, "/upload");
    engine.copy(&uri(&source), &http_copy, None).await.unwrap();
    assert_eq!(captured.lock().unwrap().as_slice(), b"abc");
}

#[tokio::test]
async fn engine_batches_isolate_item_errors() {
    let engine = IoEngine::new().unwrap();
    let dir = tempfile::tempdir().unwrap();
    let blocker = dir.path().join("blocker");
    std::fs::write(&blocker, b"file").unwrap();
    let good = dir.path().join("good.txt");
    let bad = blocker.join("bad.txt");

    let writes = engine
        .write_many(
            &[
                (uri(&good), Bytes::from_static(b"ok")),
                (uri(&bad), Bytes::from_static(b"no")),
            ],
            2,
            None,
        )
        .await
        .unwrap();
    assert_eq!(std::fs::read(&good).unwrap(), b"ok");
    assert_eq!(writes[0].0, uri(&good));
    assert!(writes[0].1.is_ok());
    assert_eq!(writes[1].0, uri(&bad));
    assert!(writes[1].1.is_err());

    let source = dir.path().join("source.txt");
    std::fs::write(&source, b"copy").unwrap();
    let destination = dir.path().join("destination.txt");
    let missing = dir.path().join("missing.txt");
    let copies = engine
        .copy_many(
            &[
                (uri(&source), uri(&destination)),
                (uri(&missing), uri(&destination)),
            ],
            2,
            None,
        )
        .await
        .unwrap();
    assert!(copies[0].1.is_ok());
    assert!(matches!(copies[1].1, Err(IoError::NotFound(_))));
    assert_eq!(std::fs::read(&destination).unwrap(), b"copy");
}

#[tokio::test]
async fn engine_read_range_uses_backend_and_streams_reader() {
    let server = TestServer::start(|request| match request.path.as_str() {
        "/obj" => {
            if let Some(range) = request.header("range") {
                assert_eq!(range, "bytes=2-4");
                Response::ok(b"234".to_vec()).with_status(206)
            } else {
                Response::ok(b"0123456789".to_vec())
            }
        }
        _ => Response::status(404),
    })
    .await;

    let engine = IoEngine::new().unwrap();
    assert_eq!(
        engine
            .read_range(&http_uri(&server, "/obj"), 2, 3, None)
            .await
            .unwrap(),
        Bytes::from_static(b"234")
    );
}
