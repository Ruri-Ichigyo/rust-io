use std::sync::{Arc, Mutex};

use bytes::Bytes;
use io_core::backend::{IoBackend, http::HttpBackend};
use io_core::{IoError, Uri};
use tokio::io::AsyncReadExt;

mod common;
use common::{Request, Response, TestServer};

fn server_uri(server: &TestServer, path: &str) -> Uri {
    Uri::parse(&server.url(path)).unwrap()
}

fn range_slice(request: &Request) -> Response {
    let range = request.header("range").unwrap_or_default();
    let Some(spec) = range.strip_prefix("bytes=") else {
        return Response::status(400);
    };
    let Some((start, end)) = spec.split_once('-') else {
        return Response::status(400);
    };
    let Ok(start) = start.parse::<usize>() else {
        return Response::status(400);
    };
    let Ok(end) = end.parse::<usize>() else {
        return Response::status(400);
    };
    let body = b"0123456789";
    let end = end.min(body.len() - 1);
    if start > end {
        return Response::status(400);
    }
    Response::ok(body[start..=end].to_vec())
        .with_header(
            "Content-Range",
            &format!("bytes {start}-{end}/{}", body.len()),
        )
        .with_status(206)
}

#[tokio::test]
async fn http_read_range_stat_and_error_mapping() {
    let server =
        TestServer::start(
            |request| match (request.method.as_str(), request.path.as_str()) {
                ("GET", "/obj") => {
                    if request.header("range").is_some() {
                        range_slice(&request)
                    } else {
                        Response::ok(b"0123456789".to_vec())
                            .with_header("Content-Type", "text/plain")
                            .with_header("Last-Modified", "Wed, 21 Oct 2015 07:28:00 GMT")
                    }
                }
                ("HEAD", "/obj") => Response::ok(Vec::new())
                    .with_header("Content-Length", "10")
                    .with_header("Content-Type", "text/plain")
                    .with_header("Last-Modified", "Wed, 21 Oct 2015 07:28:00 GMT"),
                ("GET", "/missing") => Response::status(404),
                ("GET", "/denied") => Response::status(403),
                ("GET", "/boom") => Response::status(500),
                ("GET", "/ignore-range") => Response::ok(b"0123456789".to_vec()),
                ("GET", "/range416") => Response::status(416),
                _ => Response::status(404),
            },
        )
        .await;

    let backend = HttpBackend::new().unwrap();
    let uri = server_uri(&server, "/obj");

    assert_eq!(
        backend.read_all(&uri).await.unwrap(),
        Bytes::from_static(b"0123456789")
    );
    assert_eq!(
        backend.read_range(&uri, 2, 3).await.unwrap(),
        Bytes::from_static(b"234")
    );
    assert_eq!(
        backend
            .read_range(&server_uri(&server, "/ignore-range"), 2, 3)
            .await
            .unwrap(),
        Bytes::from_static(b"234")
    );
    assert!(
        backend
            .read_range(&server_uri(&server, "/range416"), 100, 1)
            .await
            .unwrap()
            .is_empty()
    );

    let metadata = backend.stat(&uri).await.unwrap();
    assert_eq!(metadata.len, 10);
    assert_eq!(metadata.content_type.as_deref(), Some("text/plain"));
    assert_eq!(metadata.modified, Some(1_445_412_480));

    assert!(matches!(
        backend.read_all(&server_uri(&server, "/missing")).await,
        Err(IoError::NotFound(_))
    ));
    assert!(matches!(
        backend.read_all(&server_uri(&server, "/denied")).await,
        Err(IoError::PermissionDenied(_))
    ));
    let boom = backend
        .read_all(&server_uri(&server, "/boom"))
        .await
        .unwrap_err();
    match &boom {
        IoError::Http { status, .. } => assert_eq!(*status, Some(500)),
        other => panic!("expected HTTP error, got {other:?}"),
    }
    assert!(boom.is_retryable());
}

#[tokio::test]
async fn http_write_and_streaming_upload() {
    let captured: Arc<Mutex<Vec<u8>>> = Arc::new(Mutex::new(Vec::new()));
    let stored = captured.clone();
    let content_lengths: Arc<Mutex<Vec<String>>> = Arc::new(Mutex::new(Vec::new()));
    let lengths = content_lengths.clone();

    let server = TestServer::start(move |request| {
        if request.method == "PUT" && request.path == "/upload" {
            lengths.lock().unwrap().push(
                request
                    .header("content-length")
                    .unwrap_or_default()
                    .to_string(),
            );
            *stored.lock().unwrap() = request.body;
            Response::status(204)
        } else {
            Response::status(404)
        }
    })
    .await;

    let backend = HttpBackend::new().unwrap();
    let uri = server_uri(&server, "/upload");
    backend
        .write(&uri, Bytes::from_static(b"direct"))
        .await
        .unwrap();
    assert_eq!(captured.lock().unwrap().as_slice(), b"direct");

    let dir = tempfile::tempdir().unwrap();
    let file = dir.path().join("payload.bin");
    std::fs::write(&file, b"streamed body").unwrap();
    backend.write_from_path(&uri, &file, 13).await.unwrap();

    assert_eq!(captured.lock().unwrap().as_slice(), b"streamed body");
    assert_eq!(content_lengths.lock().unwrap().last().unwrap(), "13");
}

#[tokio::test]
async fn http_open_read_streams_body() {
    let server = TestServer::start(|_| Response::ok(b"stream me".to_vec())).await;
    let backend = HttpBackend::new().unwrap();
    let mut reader = backend
        .open_read(&server_uri(&server, "/obj"))
        .await
        .unwrap();
    let mut body = Vec::new();
    reader.read_to_end(&mut body).await.unwrap();
    assert_eq!(body, b"stream me");
}
