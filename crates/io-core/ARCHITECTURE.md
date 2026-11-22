# io-core architecture

## Goals

1. One call model for every location (`file`, `http(s)`, `s3`).
2. Async-first, with CPU work and blocking syscalls moved off the async workers.
3. Reuse expensive resources (TLS/HTTP connection pools, S3 client, buffers) across calls.
4. Bound concurrency and memory; make cancellation and deadlines predictable.
5. Keep backend-specific behavior behind a single trait.

## Patterns

| Pattern | Where | Why |
|---|---|---|
| Facade | `IoEngine` | Callers never branch on scheme; one retry/deadline policy. |
| Strategy | `IoBackend` + `LocalBackend`/`HttpBackend`/`S3Backend` | Swap transports without changing the engine. |
| Router | `IoEngine::backend` | O(1) routing by `Scheme`. |
| Adapter | backends implement the trait over `tokio::fs`, `reqwest`, `aws-sdk-s3` | Uniform async surface. |
| Builder | `IoEngineBuilder` | Optional client injection, retry policy, default timeout. |
| Typed errors | `IoError` | Callers can distinguish retryable vs permanent without string matching. |
| RAII | `TempPath` | Temp files are removed on error without async cleanup code. |
| Bounded pipeline | `buffer_unordered(limit)` | Real multiplexing with ordered results and per-item isolation. |
| Bulkhead | per-call timeout + bounded attempts | One slow backend cannot pin the runtime indefinitely. |
| Lazy singleton | `S3Backend::client` (`tokio::sync::OnceCell`) | Client is built once, on first use, never at import. |

## Data flow

```
caller
  └─ IoEngine::<op>(uri, timeout)
       ├─ backend(uri): Arc<dyn IoBackend>
       ├─ with_retry (deadline around the entire attempt loop)
       │    └─ backend.<op>
       │         ├─ Local: tokio::fs, temp+fsync+rename, dir fsync
       │         ├─ Http: pooled reqwest, Range/HEAD/PUT, streamed body
       │         └─ S3: lazy aws-sdk-s3 client, ByteStream (zero-copy writes)
       └─ bounded batches: buffer_unordered(limit) → sort by input index
```

## Invariants

- Local writes are atomic: temp file in the target directory, `fsync`, `rename`, then
  best-effort directory `fsync`. A failed write leaves no temp file (`TempPath` drop).
- Streaming transfers copy in 64 KiB chunks; `download` writes to a temp file and renames.
- `read_many` / `write_many` / `copy_many` return results in input order and never abort
  the batch for one failed item.
- `read_any` returns the first success and drops the remaining futures (cancellation).
- `limit == 0` and invalid URIs are configuration errors (`InvalidConfig`/`InvalidUri`).
- Retries only run for retryable errors; the deadline covers retries.
- No `unsafe` in `io-core` or `io-py`.
- Existing `rust_io.files` / `rust_io.net` APIs are untouched.

## Async / threading split

- Network and disk I/O: Tokio async primitives (`tokio::fs` uses its blocking pool for
  syscalls).
- CPU-bound hashing of many local files: one `spawn_blocking` task running a rayon
  parallel iterator (no blocking of async workers, uses all cores).
- Python: `pyo3-async-runtimes` polls the futures on the Tokio runtime; the GIL is not
  held while I/O or hashing runs.

## Failure model

| Failure | Handling |
|---|---|
| Partial local write | temp + fsync + atomic rename; guard removes temp on any error |
| Partial download | temp file + rename; guard removes temp |
| Oversized object | streaming chunks; `read` is the explicit whole-object API |
| Deadline | whole call wrapped in `tokio::time::timeout` |
| Retry storm | bounded attempts, exponential backoff, jitter, retryable-only |
| Concurrency overshoot | `buffer_unordered(limit)`; verified by in-flight probe test |
| HTTP/S3 404 vs 5xx | mapped to `NotFound` / `PermissionDenied` / retryable `Http`/`S3` |
| Panic across FFI | no `unwrap` on user input; PyO3 converts unexpected panics to exceptions |
