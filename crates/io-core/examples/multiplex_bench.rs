//! Sequential vs multiplexed local reads.
//!
//! Run with:
//! `cargo run --release -p io-core --example multiplex_bench`

use std::time::{Duration, Instant};

use io_core::{IoEngine, Uri};
use tokio::io::{AsyncReadExt, AsyncWriteExt};
use tokio::net::TcpListener;

const FILES: usize = 256;
const SIZE: usize = 256 * 1024;
const LIMIT: usize = 32;
const REMOTE_REQUESTS: usize = 64;
const REMOTE_DELAY_MS: u64 = 30;

async fn start_slow_server(delay: Duration) -> (String, tokio::task::JoinHandle<()>) {
    let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
    let address = listener.local_addr().unwrap();
    let task = tokio::spawn(async move {
        loop {
            let Ok((mut socket, _)) = listener.accept().await else {
                break;
            };
            tokio::spawn(async move {
                let mut buffer = [0u8; 2048];
                let _ = socket.read(&mut buffer).await;
                tokio::time::sleep(delay).await;
                let body = b"ok";
                let response = format!(
                    "HTTP/1.1 200 OK\r\nContent-Length: {}\r\nConnection: close\r\n\r\n",
                    body.len()
                );
                let _ = socket.write_all(response.as_bytes()).await;
                let _ = socket.write_all(body).await;
                let _ = socket.shutdown().await;
            });
        }
    });
    (format!("http://{address}/slow"), task)
}

#[tokio::main]
async fn main() -> Result<(), Box<dyn std::error::Error>> {
    let dir = std::env::temp_dir().join(format!("io-core-bench-{}", std::process::id()));
    tokio::fs::create_dir_all(&dir).await?;
    let payload = vec![0xABu8; SIZE];
    let mut uris = Vec::with_capacity(FILES);
    for index in 0..FILES {
        let path = dir.join(format!("file-{index}.bin"));
        std::fs::write(&path, &payload)?;
        uris.push(Uri::Local(path));
    }

    let engine = IoEngine::new()?;
    let total_bytes = (FILES * SIZE) as f64;

    let start = Instant::now();
    for uri in &uris {
        engine.read(uri, None).await?;
    }
    let sequential = start.elapsed();

    let start = Instant::now();
    let results = engine.read_many(&uris, LIMIT, None).await?;
    let multiplexed = start.elapsed();
    for (_, result) in &results {
        result.as_ref().map_err(|error| error.to_string())?;
    }

    println!("files={FILES} bytes={} limit={LIMIT}", FILES * SIZE);
    println!("sequential_ms={:.2}", sequential.as_secs_f64() * 1000.0);
    println!("multiplex_ms={:.2}", multiplexed.as_secs_f64() * 1000.0);
    println!(
        "speedup={:.2}x",
        sequential.as_secs_f64() / multiplexed.as_secs_f64()
    );
    println!(
        "throughput_MiB_s={:.1}",
        total_bytes / multiplexed.as_secs_f64() / 1024.0 / 1024.0
    );

    let (base, server) = start_slow_server(Duration::from_millis(REMOTE_DELAY_MS)).await;
    let remote_uris: Vec<Uri> = (0..REMOTE_REQUESTS)
        .map(|index| Uri::parse(&format!("{base}?request={index}")).unwrap())
        .collect();

    let start = Instant::now();
    for uri in &remote_uris {
        engine.read(uri, None).await?;
    }
    let remote_sequential = start.elapsed();

    let start = Instant::now();
    let results = engine.read_many(&remote_uris, 8, None).await?;
    let remote_multiplexed = start.elapsed();
    for (_, result) in &results {
        result.as_ref().map_err(|error| error.to_string())?;
    }
    server.abort();

    println!("remote_requests={REMOTE_REQUESTS} remote_delay_ms={REMOTE_DELAY_MS} limit=8");
    println!(
        "remote_sequential_ms={:.2}",
        remote_sequential.as_secs_f64() * 1000.0
    );
    println!(
        "remote_multiplex_ms={:.2}",
        remote_multiplexed.as_secs_f64() * 1000.0
    );
    println!(
        "remote_speedup={:.2}x",
        remote_sequential.as_secs_f64() / remote_multiplexed.as_secs_f64()
    );

    tokio::fs::remove_dir_all(&dir).await?;
    Ok(())
}
