//! Minimal HTTP/1.1 test server: one request per connection, explicit
//! Content-Length, optional delay, and in-flight counters for concurrency tests.

#![allow(dead_code)]

use std::collections::{HashMap, HashSet};
use std::sync::atomic::{AtomicUsize, Ordering};
use std::sync::{Arc, Mutex};
use std::time::Duration;

use tokio::io::{AsyncReadExt, AsyncWriteExt};
use tokio::net::TcpListener;

pub struct Request {
    pub method: String,
    pub path: String,
    pub headers: Vec<(String, String)>,
    pub body: Vec<u8>,
}

impl Request {
    pub fn header(&self, name: &str) -> Option<&str> {
        self.headers
            .iter()
            .find(|(key, _)| key.eq_ignore_ascii_case(name))
            .map(|(_, value)| value.as_str())
    }
}

pub struct Response {
    pub status: u16,
    pub headers: Vec<(String, String)>,
    pub body: Vec<u8>,
    pub delay: Duration,
}

impl Response {
    pub fn ok(body: impl Into<Vec<u8>>) -> Self {
        Self {
            status: 200,
            headers: Vec::new(),
            body: body.into(),
            delay: Duration::ZERO,
        }
    }

    pub fn status(status: u16) -> Self {
        Self {
            status,
            headers: Vec::new(),
            body: Vec::new(),
            delay: Duration::ZERO,
        }
    }

    pub fn with_header(mut self, name: &str, value: &str) -> Self {
        self.headers.push((name.to_string(), value.to_string()));
        self
    }

    pub fn with_status(mut self, status: u16) -> Self {
        self.status = status;
        self
    }

    pub fn with_delay(mut self, delay: Duration) -> Self {
        self.delay = delay;
        self
    }
}

#[derive(Default)]
pub struct Stats {
    requests: AtomicUsize,
    active: AtomicUsize,
    max_active: AtomicUsize,
}

impl Stats {
    pub fn requests(&self) -> usize {
        self.requests.load(Ordering::SeqCst)
    }

    pub fn max_active(&self) -> usize {
        self.max_active.load(Ordering::SeqCst)
    }
}

type Handler = Arc<dyn Fn(Request) -> Response + Send + Sync>;

pub struct TestServer {
    base: String,
    stats: Arc<Stats>,
    task: tokio::task::JoinHandle<()>,
}

impl TestServer {
    pub async fn start<F>(handler: F) -> Self
    where
        F: Fn(Request) -> Response + Send + Sync + 'static,
    {
        let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
        let address = listener.local_addr().unwrap();
        let stats = Arc::new(Stats::default());
        let handler: Handler = Arc::new(handler);
        let task_stats = stats.clone();

        let task = tokio::spawn(async move {
            loop {
                let Ok((mut socket, _)) = listener.accept().await else {
                    break;
                };
                let handler = handler.clone();
                let stats = task_stats.clone();
                tokio::spawn(async move {
                    let Some(request) = read_request(&mut socket).await else {
                        return;
                    };
                    stats.requests.fetch_add(1, Ordering::SeqCst);
                    let response = handler(request);

                    let active = stats.active.fetch_add(1, Ordering::SeqCst) + 1;
                    stats.max_active.fetch_max(active, Ordering::SeqCst);
                    if !response.delay.is_zero() {
                        tokio::time::sleep(response.delay).await;
                    }
                    stats.active.fetch_sub(1, Ordering::SeqCst);

                    write_response(&mut socket, response).await;
                });
            }
        });

        Self {
            base: format!("http://{address}"),
            stats,
            task,
        }
    }

    pub fn base(&self) -> &str {
        &self.base
    }

    pub fn stats(&self) -> &Stats {
        &self.stats
    }

    pub fn url(&self, path: &str) -> String {
        format!("{}{path}", self.base)
    }

    pub fn captured(&self) -> Arc<Mutex<Vec<u8>>> {
        Arc::new(Mutex::new(Vec::new()))
    }
}

impl Drop for TestServer {
    fn drop(&mut self) {
        self.task.abort();
    }
}

async fn read_request(socket: &mut tokio::net::TcpStream) -> Option<Request> {
    let mut buffer = Vec::new();
    let mut chunk = [0u8; 4096];

    let head_end = loop {
        let read = socket.read(&mut chunk).await.ok()?;
        if read == 0 {
            return None;
        }
        buffer.extend_from_slice(&chunk[..read]);
        if let Some(position) = buffer.windows(4).position(|window| window == b"\r\n\r\n") {
            break position;
        }
    };

    let head = String::from_utf8_lossy(&buffer[..head_end]).into_owned();
    let mut lines = head.split("\r\n");
    let mut request_line = lines.next()?.split_whitespace();
    let method = request_line.next()?.to_string();
    let path = request_line.next()?.to_string();
    let headers: Vec<(String, String)> = lines
        .filter_map(|line| line.split_once(':'))
        .map(|(name, value)| (name.trim().to_string(), value.trim().to_string()))
        .collect();

    let content_length = headers
        .iter()
        .find(|(name, _)| name.eq_ignore_ascii_case("content-length"))
        .and_then(|(_, value)| value.parse::<usize>().ok())
        .unwrap_or(0);

    let mut body = buffer[head_end + 4..].to_vec();
    while body.len() < content_length {
        let read = socket.read(&mut chunk).await.ok()?;
        if read == 0 {
            break;
        }
        body.extend_from_slice(&chunk[..read]);
    }
    body.truncate(content_length);

    Some(Request {
        method,
        path,
        headers,
        body,
    })
}

async fn write_response(socket: &mut tokio::net::TcpStream, response: Response) {
    let reason = match response.status {
        200 => "OK",
        204 => "No Content",
        206 => "Partial Content",
        400 => "Bad Request",
        403 => "Forbidden",
        404 => "Not Found",
        429 => "Too Many Requests",
        500 => "Internal Server Error",
        _ => "Unknown",
    };
    let mut head = format!(
        "HTTP/1.1 {} {reason}\r\nConnection: close\r\n",
        response.status
    );
    let mut has_length = false;
    for (name, value) in &response.headers {
        if name.eq_ignore_ascii_case("content-length") {
            has_length = true;
        }
        head.push_str(&format!("{name}: {value}\r\n"));
    }
    if !has_length {
        head.push_str(&format!("Content-Length: {}\r\n", response.body.len()));
    }
    head.push_str("\r\n");

    let _ = socket.write_all(head.as_bytes()).await;
    let _ = socket.write_all(&response.body).await;
    let _ = socket.shutdown().await;
}

const S3_LAST_MODIFIED: &str = "2015-10-21T07:28:00.000Z";
const S3_LAST_MODIFIED_HEADER: &str = "Wed, 21 Oct 2015 07:28:00 GMT";

#[derive(Default)]
pub struct S3Store {
    objects: Mutex<HashMap<String, Vec<u8>>>,
}

impl S3Store {
    pub fn new() -> Self {
        Self::default()
    }
}

pub fn s3_handler(store: Arc<S3Store>) -> impl Fn(Request) -> Response + Send + Sync + 'static {
    move |request| handle_s3(&store, request)
}

fn percent_decode(input: &str) -> String {
    let bytes = input.as_bytes();
    let mut output = Vec::with_capacity(bytes.len());
    let mut index = 0;
    while index < bytes.len() {
        if bytes[index] == b'%' && index + 2 < bytes.len() {
            let hex = &input[index + 1..index + 3];
            if let Ok(value) = u8::from_str_radix(hex, 16) {
                output.push(value);
                index += 3;
                continue;
            }
        }
        output.push(bytes[index]);
        index += 1;
    }
    String::from_utf8_lossy(&output).into_owned()
}

fn xml_escape(value: &str) -> String {
    value
        .replace('&', "&amp;")
        .replace('<', "&lt;")
        .replace('>', "&gt;")
}

fn s3_error_response(status: u16, code: &str) -> Response {
    let body = format!(
        "<?xml version=\"1.0\" encoding=\"UTF-8\"?>\
         <Error><Code>{code}</Code><Message>{code}</Message></Error>"
    );
    Response::status(status)
        .with_header("Content-Type", "application/xml")
        .with_body(body.into_bytes())
}

impl Response {
    pub fn with_body(mut self, body: Vec<u8>) -> Self {
        self.body = body;
        self
    }
}

fn split_path(path: &str) -> (&str, HashMap<String, String>) {
    let (path, query) = path.split_once('?').unwrap_or((path, ""));
    let mut params = HashMap::new();
    for pair in query.split('&').filter(|pair| !pair.is_empty()) {
        let (key, value) = pair.split_once('=').unwrap_or((pair, ""));
        params.insert(percent_decode(key), percent_decode(value));
    }
    (path, params)
}

fn handle_s3(store: &S3Store, request: Request) -> Response {
    let (path, params) = split_path(&request.path);
    let path = percent_decode(path);
    let trimmed = path.trim_start_matches('/');
    let (bucket, key) = match trimmed.split_once('/') {
        Some((bucket, key)) => (bucket, key.to_string()),
        None => (trimmed, String::new()),
    };
    let method = request.method.as_str();
    let object_name = format!("{bucket}/{key}");

    if key.is_empty() && method == "GET" {
        return list_objects(store, bucket, &params);
    }

    if method == "PUT" {
        if let Some(source) = request.header("x-amz-copy-source") {
            let source = source.trim_start_matches('/');
            let (source_bucket, source_key) = source.split_once('/').unwrap_or((source, ""));
            let source = format!(
                "{}/{}",
                percent_decode(source_bucket),
                percent_decode(source_key)
            );
            let mut objects = store.objects.lock().unwrap();
            let Some(data) = objects.get(&source).cloned() else {
                return s3_error_response(404, "NoSuchKey");
            };
            objects.insert(object_name, data);
            let body = format!(
                "<?xml version=\"1.0\" encoding=\"UTF-8\"?>\
                 <CopyObjectResult><ETag>\"etag\"</ETag>\
                 <LastModified>{S3_LAST_MODIFIED}</LastModified></CopyObjectResult>"
            );
            return Response::ok(body.into_bytes()).with_header("Content-Type", "application/xml");
        }
        store
            .objects
            .lock()
            .unwrap()
            .insert(object_name, request.body);
        return Response::ok(Vec::new()).with_header("ETag", "\"etag\"");
    }

    if method == "HEAD" || method == "GET" {
        let data = store.objects.lock().unwrap().get(&object_name).cloned();
        let Some(data) = data else {
            return s3_error_response(404, "NoSuchKey");
        };
        if method == "HEAD" {
            return Response::ok(Vec::new())
                .with_header("Content-Length", &data.len().to_string())
                .with_header("ETag", "\"etag\"")
                .with_header("Content-Type", "application/octet-stream")
                .with_header("Last-Modified", S3_LAST_MODIFIED_HEADER);
        }
        if let Some(range) = request.header("range")
            && let Some(spec) = range.strip_prefix("bytes=")
            && let Some((start, end)) = spec.split_once('-')
        {
            let start = start.parse::<usize>().unwrap_or(0);
            let end = end.parse::<usize>().unwrap_or(data.len().saturating_sub(1));
            let end = end.min(data.len().saturating_sub(1));
            if start >= data.len() {
                return Response::status(416);
            }
            if start <= end {
                return Response::ok(data[start..=end].to_vec())
                    .with_status(206)
                    .with_header(
                        "Content-Range",
                        &format!("bytes {start}-{end}/{}", data.len()),
                    );
            }
        }
        return Response::ok(data).with_header("ETag", "\"etag\"");
    }

    if method == "DELETE" {
        store.objects.lock().unwrap().remove(&object_name);
        return Response::status(204);
    }

    s3_error_response(405, "MethodNotAllowed")
}

fn list_objects(store: &S3Store, bucket: &str, params: &HashMap<String, String>) -> Response {
    let prefix = params.get("prefix").cloned().unwrap_or_default();
    let delimiter = params.get("delimiter").cloned().unwrap_or_default();
    let full_prefix = format!("{bucket}/{prefix}");
    let objects = store.objects.lock().unwrap();

    let mut contents = Vec::new();
    let mut common = HashSet::new();
    for (key, data) in objects.iter() {
        let Some(rest) = key.strip_prefix(&full_prefix) else {
            continue;
        };
        if !delimiter.is_empty()
            && let Some(position) = rest.find(&delimiter)
        {
            common.insert(format!("{prefix}{}", &rest[..position + delimiter.len()]));
            continue;
        }
        let object_key = key.strip_prefix(&format!("{bucket}/")).unwrap_or(key);
        contents.push((object_key.to_string(), data.len()));
    }
    contents.sort();

    let mut body = format!(
        "<?xml version=\"1.0\" encoding=\"UTF-8\"?>\
         <ListBucketResult xmlns=\"http://s3.amazonaws.com/doc/2006-03-01/\">\
         <Name>{}</Name><Prefix>{}</Prefix><KeyCount>{}</KeyCount>\
         <MaxKeys>1000</MaxKeys><IsTruncated>false</IsTruncated>",
        xml_escape(bucket),
        xml_escape(&prefix),
        contents.len() + common.len()
    );
    for (key, size) in contents {
        body.push_str(&format!(
            "<Contents><Key>{}</Key><LastModified>{S3_LAST_MODIFIED}</LastModified>\
             <ETag>\"etag\"</ETag><Size>{size}</Size><StorageClass>STANDARD</StorageClass></Contents>",
            xml_escape(&key)
        ));
    }
    let mut common: Vec<String> = common.into_iter().collect();
    common.sort();
    for prefix in common {
        body.push_str(&format!(
            "<CommonPrefixes><Prefix>{}</Prefix></CommonPrefixes>",
            xml_escape(&prefix)
        ));
    }
    body.push_str("</ListBucketResult>");

    Response::ok(body.into_bytes()).with_header("Content-Type", "application/xml")
}
