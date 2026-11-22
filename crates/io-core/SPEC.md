# SPEC — Generic async I/O engine (`io-core` + `rust_io.io`)

Status: autonomous run; **spec approval not obtained** (old-coder autonomous mode).
Change under test: commit on branch `feat/generic-io`; parent = `363ae08` (rust-io 0.2.1).
Isolation: feature branch in the existing worktree (no separate worktree; the crate tree is
self-contained and the gauntlet rebuilds from it).

## Goal

Evolve `rust-io` from a Lingua Seeker-specific helper into a general-purpose I/O SDK:

1. One async, backend-agnostic engine for local files, HTTP(S), and S3.
2. I/O multiplexing: bounded-concurrency batches, first-success races, per-call deadlines,
   cancellation by future drop, and reusable connection pools.
3. Both async and blocking/parallel execution: async I/O for network/disk, rayon for
   CPU-bound hashing, Tokio's blocking pool for filesystem syscalls that cannot be async.
4. Modern structure: URI routing, a `Send + Sync` backend trait, an engine facade,
   typed errors, builder configuration, retry policy, and streaming transfer APIs.
5. Python exposure as a new `rust_io.io` submodule. Existing `rust_io.files` and
   `rust_io.net` APIs keep their signatures and behavior.

## Behaviors (acceptance criteria)

### B1 — URI model

- `Uri::parse("file:///tmp/a b")` → Local `/tmp/a b` (percent-decoded).
- `Uri::parse("/tmp/a.txt")` and `Uri::parse("relative/a.txt")` → Local path.
- `Uri::parse("s3://bucket/key/path")` → S3 bucket `bucket`, key `key/path`.
- `Uri::parse("https://example.com/a.pdf")` → Http.
- Empty string, `s3://`, `s3://bucket` (no key), and `ftp://...` return
  `IoError::InvalidUri` with the offending input in the message.
- `Uri::to_string()` round-trips each accepted form.

### B2 — Local backend

- `write` creates missing parents, then `read` returns exactly the written bytes.
- `write` is atomic: a reader never observes a partially written payload; no temporary
  file remains after success or failure.
- `read_range(offset, len)` returns the requested slice; `offset >= len` → empty bytes;
  a range extending past EOF is clamped; a missing file → `NotFound`.
  `len == 0` returns empty bytes without an existence check on every backend (revision
  after review: HTTP/S3 and local now agree; 416 responses become empty bytes).
- `stat` reports `len`, `is_file`, `is_dir`, `modified` (seconds), and scheme.
- `list` returns direct child names, directories marked with a trailing `/`.
- `copy` preserves bytes; `delete` removes a file; deleting a missing file → `NotFound`.
- Concurrent readers during repeated atomic rewrites always see one complete payload.

### B3 — HTTP backend

- `read` issues GET and returns the body bytes; 200 → bytes.
- `read_range` sends `Range: bytes=start-end` and returns the partial body.
- `stat` issues HEAD and maps `Content-Length`/`Last-Modified`.
- `write` issues PUT with the body (used for pre-signed URLs).
- 404 → `NotFound`; 401/403 → `PermissionDenied`; 5xx → `Backend`; transport failure →
  `Backend` with the underlying message.
- `download` streams response chunks to a file without buffering the whole object in memory
  (verified with an object larger than the 64 KiB stream chunk and a peak-buffer probe).
- The HTTP backend reuses one `reqwest::Client` per engine instance (connection pooling).

### B4 — S3 backend

- `read`, `read_range`, `write`, `stat`, `list`, `delete`, and `copy` map to the
  corresponding S3 operations.
- Missing object → `NotFound`.
- Explicit credentials and the default AWS credential chain are both supported.
- Network integration is env-gated (`IO_S3_ENDPOINT`, `IO_S3_BUCKET`); when unset the test
  records `skipped` in EVIDENCE and only construction/routing is exercised.

### B5 — Engine: routing, timeout, retry

- An `IoEngine` routes each URI to Local, Http, or S3 by scheme.
- Every operation accepts a per-call timeout; exceeding it → `IoError::Timeout`.
  The deadline covers the whole call including retries (revision after RED: a
  retry cannot run past the caller's deadline).
- Transient errors (`Timeout`, 429/5xx, connection reset, S3 5xx) are retried up to
  `RetryPolicy::max_attempts` with exponential backoff and jitter; permanent errors
  (`NotFound`, `PermissionDenied`, `InvalidUri`) are not retried.
- `max_attempts == 0` is normalized to 1 (never a silent no-op).
- Dropping the future cancels async operations (no detached task, no data written after drop
  for atomic local writes). Blocking rayon hashing cannot be interrupted mid-file: a
  `hash_many` deadline returns `Timeout`, but the worker may finish in the background
  (revision after review; recorded as a known limit).

### B6 — Multiplexing

- `read_many(uris, limit)` / `write_many(items, limit)` / `copy_many(pairs, limit)`:
  - results are in input order;
  - at most `limit` operations run concurrently (probe backend tracks max in-flight);
  - one failing item does not abort the others;
  - `limit == 0` → `IoError::InvalidConfig`.
- `read_any(uris)` returns the first successful `(Uri, Bytes)` and cancels the remaining
  futures; all failures → `IoError::AllFailed` containing every cause.
- `limit` and timeouts are enforced across mixed backends.

### B7 — Multithreaded hashing

- `hash_many(uris)` computes SHA-256 with rayon inside one blocking task; results are in
  input order, missing files yield per-item `NotFound` errors, and the event loop stays
  responsive (the Python binding releases the GIL).
- `hash` streams the file in chunks (no whole-file buffering); verified by hashing a file
  larger than the chunk size and comparing with `sha256sum`.

### B8 — Python `rust_io.io`

Async functions (all return awaitables, never block the event loop):

- `read(uri, timeout_ms=None) -> bytes`
- `read_range(uri, offset, length, timeout_ms=None) -> bytes`
- `write(uri, data, timeout_ms=None) -> None` (accepts `bytes`, `bytearray`, `memoryview`, `str`)
- `stat(uri, timeout_ms=None) -> dict`
- `list(uri, timeout_ms=None) -> list[str]`
- `delete(uri, timeout_ms=None) -> None`
- `copy(src, dst, timeout_ms=None) -> None`
- `read_many(uris, limit=8, timeout_ms=None) -> list[dict]` where each item is
  `{"ok": bool, "uri": str, "bytes": bytes | None, "error": str | None}`
- `write_many(items, limit=8, timeout_ms=None) -> list[dict]` where items are
  `(uri, data)` pairs and each result has `ok`, `uri`, `error`
- `copy_many(pairs, limit=8, timeout_ms=None) -> list[dict]`
- `read_any(uris, timeout_ms=None) -> dict` with `uri` and `bytes`
- `download(uri, path, timeout_ms=None) -> int` (bytes written, streaming)
- `upload(path, uri, timeout_ms=None) -> None` (streaming)
- `hash(uri, timeout_ms=None) -> str`
- `hash_many(uris, limit=8) -> list[dict]`

Errors map to: `ValueError` (invalid URI/config), `FileNotFoundError` (not found),
`PermissionError`, `TimeoutError`, `ConnectionError` (HTTP failure), `OSError`
(filesystem I/O), `RuntimeError` (backend/all-failed).

`rust_io.io.__name__ == "rust_io.io"`; module is importable as `import rust_io.io`.

### B9 — Invariants

- Existing `rust_io.files` / `rust_io.net` signatures and behavior are unchanged.
- Existing Rust tests (net-io 31, files-io 11) and the Python facade test
  `tests/core/agents/acquisition/test_rust_io_facade.py` (8) keep passing.
- No `unsafe` in `io-core` or `io-py`.
- No whole-object buffering in `download`/`upload`.
- All new dependencies are listed in the setup plan; no new dependency is added silently.

## Failure model (Tier 3)

| Mode | Mitigation | Check |
|---|---|---|
| Partial/truncated local write | write temp file → fsync → atomic rename → fsync dir | concurrent reader stress test; temp file absence |
| Unbounded memory on large transfer | streaming `download`/`upload`, chunked hashing | large-file test + peak-buffer probe |
| Reader/writer race | atomic rename; only complete payloads visible | repeat-rewrite reader test |
| Timeout / cancellation leak | `tokio::time::timeout`; futures dropped, no `spawn` for user ops | timeout + abort test |
| Retry storm / duplicate writes | retry only transient errors, bounded attempts, jitter | retry-count probe |
| Concurrency overshoot | semaphore/buffer_unordered bound proven by in-flight probe | B6 tests |
| Silent wrong bytes | byte-exact roundtrip and SHA-256 vs `sha256sum` | B2/B7 |
| Panic across FFI | no `unwrap` on user input; errors returned | clippy + tests |
| SSRF / hostile URL | client SDK intentionally accepts caller URLs; documented | known limit |
| S3 signing/config errors | typed backend errors | env-gated integration |

## Setup plan

New crates:

- `backend/libs/io-core` (`rlib`) — engine, backends, multiplexing, streaming.
- `backend/libs/io-py` (`rlib`) — PyO3 bindings for `io-core`; exported as `rust_io.io`.

New dependencies (one-line justification each):

| Crate | Why |
|---|---|
| `async-trait` | object-safe async backend trait used behind `Arc<dyn IoBackend>` |
| `bytes` | cheap shared buffers across backends and Python |
| `futures` | `buffer_unordered` bounded multiplexing |
| `reqwest` | HTTP(S) backend (rustls, stream, gzip, socks) |
| `aws-config`, `aws-sdk-s3`, `aws-credential-types` | S3 backend and credential chain |
| `tokio` (rt, fs, io-util, time, sync, net) | async I/O, timeout, semaphore, test server |
| `tokio-util` (io) | streaming upload from files |
| `url` | URI parsing and HTTP range URLs |
| `sha2`, `hex` | streaming SHA-256 |
| `httpdate` | parse HTTP `Last-Modified` for `stat` |
| `rayon` | parallel CPU hashing inside the blocking pool |
| `thiserror` | typed error enum |
| `pyo3`, `pyo3-async-runtimes` | async Python bindings |
| dev: `tempfile` | isolated filesystem tests |

Git: branch `feat/generic-io`, SPEC commit before code, GREEN checkpoints allowed.
Tooling: `cargo fmt`, `cargo clippy -- -D warnings`, `cargo test`, `cargo llvm-cov`
(changed-line coverage), `cargo mutants` (io-core), `cargo audit`, wheel build + pytest.
`rust-anything` is not installed in this environment; the Rust-native gauntlet above
(clippy, llvm-cov, mutants, audit) plus old-coder is used instead. Recorded as a deviation.

## Known limits

- HTTP PUT semantics depend on the server; no multipart upload.
- S3 integration requires an endpoint/bucket; otherwise only unit-level routing is verified.
- No WASM/io_uring backend; multiplexing is Tokio readiness + futures polling.
