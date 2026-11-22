# io-core

> Backend-agnostic async I/O engine: local files, HTTP(S), and S3 behind one URI model,
> with bounded multiplexing, deadlines, retries, streaming, and parallel hashing.

## What it provides

| Capability | API |
|---|---|
| URI routing (`file`, plain path, `s3://`, `http(s)://`) | `Uri::parse` |
| Read / range read / write / stat / list / delete / copy | `IoEngine::{read,read_range,write,stat,list,delete,copy}` |
| Streaming transfer | `IoEngine::{download,upload}` |
| SHA-256 (streaming) / parallel batch | `IoEngine::{hash,hash_many}` |
| Bounded concurrency | `IoEngine::{read_many,write_many,copy_many}` |
| First-success race (mirrors) | `IoEngine::read_any` |
| Retry + jitter + per-call deadline | `RetryPolicy`, `timeout` argument |

## Quick start

```rust
use io_core::{IoEngine, Uri};

let engine = IoEngine::new()?;
let uri = Uri::parse("/tmp/report.txt")?;
engine.write(&uri, bytes::Bytes::from_static(b"hello")).await?;
assert_eq!(engine.read(&uri, None).await?, "hello");
```

Bounded multiplexing:

```rust
let results = engine.read_many(&uris, 4, Some(Duration::from_secs(10))).await?;
for (uri, result) in results {
    // input order is preserved; one failure does not abort the batch
}
```

Streaming upload/download never buffers the whole object:

```rust
engine.download(&uri, Path::new("/tmp/paper.pdf"), None).await?;
engine.upload(Path::new("/tmp/paper.pdf"), &uri, None).await?;
```

## Design

```
IoEngine (facade)
 ├─ Uri router ── LocalBackend  (tokio::fs, atomic rename, dir fsync)
 │               HttpBackend   (reqwest pool, Range/HEAD/PUT, streamed body)
 │               S3Backend     (aws-sdk-s3, lazy shared client, ByteStream)
 ├─ RetryPolicy + deadline      (exponential backoff, jitter, typed retryability)
 ├─ Bounded multiplexer         (futures::buffer_unordered, ordered results)
 └─ Blocking pool               (spawn_blocking + rayon for CPU hashing)
```

- `IoBackend` is object-safe (`async-trait`) and `Send + Sync`; backends are `Arc`d and
  shared by every call, so clients and connection pools are reused.
- Local writes go to a sibling temp file, are fsynced, then atomically renamed; readers
  never observe partial payloads.
- `download`/`upload` stream in 64 KiB chunks (upload uses `ReaderStream` /
  `ByteStream::from_path`), so large objects do not live in memory.
- Every operation clones a cheap `Arc` and reruns through the retry loop; the deadline
  covers the whole call including retries.
- Cross-backend `copy` spills through a unique temp file, keeping memory bounded.
- `hash_many` hashes local files with rayon inside one `spawn_blocking` task; mixed
  local/remote batches use bounded async hashing.

See [ARCHITECTURE.md](ARCHITECTURE.md) for the patterns and invariants, and
[SPEC.md](SPEC.md) for the executable contract.

## S3 configuration

Unset values fall back to the AWS default credential chain.

| Variable | Meaning |
|---|---|
| `IO_S3_ENDPOINT` | Custom endpoint (MinIO, LocalStack); enables path-style addressing |
| `IO_S3_REGION` | Region (default `us-east-1` via AWS config) |
| `IO_S3_ACCESS_KEY` / `IO_S3_SECRET_KEY` / `IO_S3_SESSION_TOKEN` | Explicit credentials |
| `IO_S3_PATH_STYLE` | `1`/`true` forces path-style; defaults to on when an endpoint is set |

## Limits

- HTTP has no directory listing or server-side copy (`Unsupported`).
- S3 integration tests run only when `IO_S3_ENDPOINT` and `IO_S3_BUCKET` are set.
- Multiplexing is Tokio readiness plus bounded future polling (no io_uring backend).
