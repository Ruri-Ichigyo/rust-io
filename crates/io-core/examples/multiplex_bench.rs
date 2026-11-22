//! Sequential vs multiplexed local reads.
//!
//! Run with:
//! `cargo run --release -p io-core --example multiplex_bench`

use std::time::Instant;

use io_core::{IoEngine, Uri};

const FILES: usize = 256;
const SIZE: usize = 256 * 1024;
const LIMIT: usize = 32;

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

    tokio::fs::remove_dir_all(&dir).await?;
    Ok(())
}
