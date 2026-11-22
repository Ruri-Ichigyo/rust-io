# rust-io

> High-performance Rust native extensions for Python: one async I/O engine for local files,
> HTTP(S), and S3, plus literature search, MinerU document parsing, archives, and SHA-256 dedup.

## Crates

| Crate | Python module | Kind | Purpose |
|---|---|---|---|
| [`rust-io`](crates/rust-io) | `rust_io` | `cdylib` | PyO3 facade that mounts the submodules |
| [`io-core`](crates/io-core) | — | `rlib` | Generic async I/O engine (routing, retries, deadlines, multiplexing, streaming) |
| [`io-py`](crates/io-py) | `rust_io.io` | `rlib` | PyO3 bindings for `io-core` |
| [`files-io`](crates/files-io) | `rust_io.files` | `rlib` | `File` (local + S3), archives, dedup |
| [`net-io`](crates/net-io) | `rust_io.net` | `rlib` | 15 literature sources, MinerU API, web scraping |

## `rust_io.io` — generic async I/O

```python
import asyncio
import rust_io.io as io

async def main():
    await io.write("/tmp/report.bin", b"payload")

    # Local, HTTP(S), and S3 behind one URI model.
    results = await io.read_many(
        ["/tmp/a.bin", "s3://bucket/b.bin", "https://example.com/c.bin"],
        limit=3,
    )

    # Streaming transfer: no whole-object buffering.
    await io.download("https://example.com/paper.pdf", "/tmp/paper.pdf")
    digest = await io.hash("/tmp/paper.pdf")

asyncio.run(main())
```

Capabilities: bounded concurrent batches, heterogeneous multiplexing (`mux`), first-success
mirror races (`read_any`), per-call deadlines, retry only for transient errors, atomic local
writes, streamed upload/download, and parallel streaming SHA-256.

From Rust, the same engine exposes a heterogeneous multiplexer that returns results in
completion order:

```rust
use io_core::{IoEngine, MuxOp, Uri};

let engine = IoEngine::new()?;
let ops = [
    MuxOp::Read(Uri::parse("/tmp/a.bin")?),
    MuxOp::Stat(Uri::parse("https://example.com/b.bin")?),
    MuxOp::Hash(Uri::parse("s3://bucket/c.bin")?),
];
let completed = engine.mux(&ops, 8, None).await?;
```

## Optimization harness

`scripts/rust-anything` is the repo-local Rust optimization gauntlet: format, clippy
(`-D warnings`), workspace tests, io-core coverage gate, dependency audit, a
sequential-vs-multiplexed benchmark, and optional mutation testing.

```bash
./scripts/rust-anything all
./scripts/rust-anything bench
./scripts/rust-anything mutants
```

The benchmark reports both numbers rather than assuming a win: on cached local files
(tmpfs) scheduling overhead can make bounded concurrency slower than sequential reads;
the multiplexer targets latency-bound and heterogeneous workloads.

## Existing modules

`rust_io.files` and `rust_io.net` keep their previous APIs.

```python
import rust_io.net as net
import rust_io.files as files

result = await net.fetch_one("crossref", "search", {"query": "CRISPR"})
files.File("/tmp/output.txt").write("hello")
```

## Build

Requires Rust 1.85+ (edition 2024) and Python 3.12+.

```bash
python -m venv .venv && . .venv/bin/activate
pip install maturin pytest pytest-asyncio

# development install
maturin develop --release -m crates/rust-io/Cargo.toml

# wheel / sdist
maturin build --release -m crates/rust-io/Cargo.toml
maturin sdist -m crates/rust-io/Cargo.toml
```

## Test

```bash
cargo fmt --all -- --check
cargo clippy --workspace --all-targets -- -D warnings
cargo test --workspace
python -m pytest -q
```

`io-core` also has a SPEC and architecture note:
[SPEC.md](crates/io-core/SPEC.md), [ARCHITECTURE.md](crates/io-core/ARCHITECTURE.md).

## License

Apache-2.0.
