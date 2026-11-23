# rust-io

> High-performance Rust native extensions for Python: one async I/O engine for local files,
> HTTP(S), and S3 — plus literature search, MinerU parsing, archives, and SHA-256 dedup.

```bash
pip install rust-io
```

Requires Python 3.12+ and a CPython/PyPy build for your platform. Wheels are published for
Linux (glibc and musl, x86_64/aarch64), macOS (x86_64/arm64), and Windows (x64).

## Generic async I/O: `rust_io.io`

One URI model (`/path`, `file://`, `http(s)://`, `s3://`) with a single async engine:

```python
import asyncio
import rust_io.io as io

async def main():
    await io.write("/tmp/report.bin", b"payload")

    # Bounded concurrent batch; results in input order, one failure does not abort.
    results = await io.read_many(
        ["/tmp/a.bin", "s3://bucket/b.bin", "https://example.com/c.bin"],
        limit=8,
    )

    # First-success race across mirrors.
    winner = await io.read_any([mirror_1, mirror_2])

    # Streamed transfer; the whole object is never buffered in memory.
    await io.download("https://example.com/paper.pdf", "/tmp/paper.pdf")

    # Segmented download: probe 206 range support, then 8 concurrent ranges.
    await io.download("https://example.com/big.bin", "/tmp/big.bin", connections=8)
    digest = await io.hash("/tmp/paper.pdf")

asyncio.run(main())
```

| Function | Purpose |
|---|---|
| `read` / `read_range` | Read an object or a byte range |
| `write` | Write bytes, bytearray, memoryview, or str |
| `stat` / `list` / `delete` / `copy` | Metadata, listing, deletion, copy |
| `read_many` / `write_many` / `copy_many` | Bounded concurrent batches, input order |
| `read_any` | Race mirrors, return the first success |
| `download` / `upload` | Streaming transfers with atomic local replacement |
| `hash` / `hash_many` | Streaming SHA-256, parallel batch hashing |

Under the hood (`io-core`): retries only for transient errors, whole-call deadlines,
bounded multiplexing, atomic local writes, cross-backend copy via a private spill file,
and a heterogeneous `IoEngine::mux` in the Rust API.

## Modules

| Python module | Description |
|---|---|
| `rust_io.io` | Generic async I/O for local files, HTTP(S), and S3 (above) |
| `rust_io.files` | `File` class, batch copy/compress, archives, SHA-256 dedup |
| `rust_io.net` | 15 literature sources, MinerU API, web scraping |

### `rust_io.files`

```python
import rust_io.files as files

handle = files.File("/tmp/output.txt")
handle.write("hello")
print(handle.read(as_text=True))
print(handle.content_hash())
```

### `rust_io.net`

```python
import rust_io.net as net

result = await net.fetch_one("crossref", "search", {"query": "CRISPR", "limit": 5})
results = await net.fetch_multi(["crossref", "openalex"], "search", {"query": "BRCA1"})
parsed = await net.mineru_create_task("https://example.com/paper.pdf", token="...")
```

## S3 configuration

`unset` values fall back to the AWS default credential chain.

| Variable | Meaning |
|---|---|
| `IO_S3_ENDPOINT` | Custom endpoint (MinIO/LocalStack); enables path-style addressing |
| `IO_S3_REGION` | Region |
| `IO_S3_ACCESS_KEY` / `IO_S3_SECRET_KEY` / `IO_S3_SESSION_TOKEN` | Explicit credentials |
| `IO_S3_PATH_STYLE` | `1`/`true` forces path-style |

## Build from source

Requires Rust 1.85+ (edition 2024).

```bash
python -m venv .venv && . .venv/bin/activate
pip install maturin
maturin develop --release -m crates/rust-io/Cargo.toml
```

## Verification

- 82 Rust tests and 11 Python tests
- `io-core` line coverage 85.19%, mutation score 16/16 non-equivalent mutants
- `cargo audit` clean
- Multiplex benchmark: 64 HTTP requests at 30 ms latency → 2005 ms sequential vs 252 ms at `limit=8`

Source, issues, and docs: <https://github.com/Ruri-Ichigyo/rust-io>

## License

Apache-2.0.
