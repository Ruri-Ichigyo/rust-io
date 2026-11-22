# EVIDENCE — generic async I/O engine

Source state: commit `520c026` (this file is updated in the following commit).
Spec: [SPEC.md](SPEC.md). Architecture: [ARCHITECTURE.md](ARCHITECTURE.md).
Spec approval: **not obtained** (autonomous run; old-coder autonomous mode).
Tier: 3 (public API, concurrency, data I/O).

## Behavior → test map

| Spec | Evidence |
|---|---|
| B1 URI model | `src/uri.rs` unit tests: parse/round-trip/case/insensitive/hostile inputs |
| B2 local backend | `tests/local.rs`: roundtrip, range clamp/NotFound, list/copy/delete, atomic rewrite stress, no temp on failure, permission preservation, long basenames |
| B3 HTTP backend | `tests/http.rs`: GET/HEAD/Range/PUT, 404/403/500 mapping, streamed upload, ignored-Range fallback, 416 |
| B4 S3 backend | `tests/s3.rs` in-memory S3 server: read/range/stat/list/copy (incl. spaces)/delete/404/416; env-gated real endpoint test |
| B5 routing/timeout/retry | `tests/engine.rs`: routing, timeout, 500→200 retry, 404 no-retry, `max_attempts=0` still attempts once, truncated-body retry |
| B6 multiplexing | `tests/engine.rs`: in-flight probe (`max_active <= 4`), input order, zero-limit config error, `read_any` first success, zero-length range; `mux` heterogeneous ops, completion order, per-op error isolation |
| B7 hashing | `tests/engine.rs` + `tests/local.rs`: SHA-256 known vector, missing file error, streaming hash, local rayon pool bounded by `limit` |
| B8 Python `rust_io.io` | `tests/test_rust_io.py`: exports, local roundtrip, read_range, stat/list, batches, read_any, download/upload, hash/hash_many, exception mapping, zero-limit |
| B9 invariants | `tests/test_rust_io.py` (existing net/files tests) plus `cargo test --workspace` |

## Gauntlet results

| Layer | Command | Result |
|---|---|---|
| Format | `cargo fmt --all -- --check` | exit 0 |
| Lint | `cargo clippy --workspace --all-targets -- -D warnings` | exit 0, zero warnings |
| Tests | `cargo test --workspace` | 79 passed, 0 failed (io-core 37, files-io 11, net-io 31) |
| Coverage | `cargo llvm-cov -p io-core --tests --summary-only --fail-under-lines 80` | **85.19% lines**, gate exit 0 |
| Mutation | `cargo mutants -p io-core --file crates/io-core/src/uri.rs --file crates/io-core/src/error.rs --timeout 60 --jobs 4` | 20 mutants: 16 caught, 4 unviable, **0 missed** |
| Supply chain | `cargo audit` | exit 0, zero advisories |
| Python wheel | `maturin develop --release -m crates/rust-io/Cargo.toml` + `pytest -q` | 11 passed |
| Real execution | same wheel imported as `rust_io`; `rust_io.io` local/HTTP/S3 contract tests | passed |

All numbers above were produced by `./scripts/rust-anything` (or its exact commands) in
this workspace, with `CARGO_TARGET_DIR` on a large filesystem to avoid `/tmp` exhaustion.

## Optimization harness (`rust-anything`)

No external `rust-anything` binary/skill exists in this environment, so the repository
ships its own fail-closed gauntlet: `scripts/rust-anything {all|fmt|clippy|test|coverage|audit|bench|mutants}`.
`all` runs format, clippy `-D warnings`, workspace tests, the io-core 80% line-coverage
gate, `cargo audit`, and `examples/multiplex_bench.rs`.

Benchmark (256 × 256 KiB local files plus 64 HTTP requests with 30 ms latency):

```text
files=256 bytes=67108864 limit=32
sequential_ms=7.17
multiplex_ms=6.76
speedup=1.06x
throughput_MiB_s=9460.5
remote_requests=64 remote_delay_ms=30 limit=8
remote_sequential_ms=2005.23
remote_multiplex_ms=252.40
remote_speedup=7.94x
```

This is reported honestly: cached local reads are within run-to-run noise (0.67x–1.06x
across runs), while latency-bound HTTP requests show ~7.9x. Multiplexing targets the
latter; the harness prints both numbers instead of asserting a win.

## Independent review (old-coder verification pass)

A fresh-context reviewer audited `io-core`, `io-py`, the facade, and tests. Findings and
disposition:

| Severity | Finding | Fix |
|---|---|---|
| High | S3 `copy` used form encoding (`%2F`, `+`) for `x-amz-copy-source`; spaces targeted the wrong key | Dedicated encoder keeps `/` literal and emits `%20`; mock now splits like S3; space-key test |
| Medium | HTTP streamed-body errors were `ErrorKind::Other` (non-retryable); download blamed the temp path | Chunk errors map to `ConnectionReset`; download loop separates reader (source URI) and writer (temp path) errors; truncated-body retry test |
| Medium | Local `hash_many` ignored `limit`/`timeout` | Rayon pool `num_threads(limit)` plus whole-call deadline; cancellation limit documented |
| Medium | `read_range(len=0)` differed across backends; 416 was an error | Zero length returns empty before any I/O; HTTP/S3 416 → empty; tests |
| Low | Predictable temp path, `File::create` followed symlinks; long basenames overflowed temp name | `O_EXCL` + nonce, 0600 for private spills, basename truncated; long-name test |
| Low | HTTP Range fallback buffered the whole object | Streams and discards skipped bytes; ignored-Range test |
| Low | S3 range response not validated | Checks `Content-Range` start / body length; 416 test |
| Low | HEAD cannot work on GET-presigned URLs | Recorded as a known limit (SPEC sanctions HEAD) |
| Low | Test gaps (scope encoding, ignored Range, zero length, cleanup, race) | Added; atomic race test now uses a barrier and a fixed number of reads |

## Known limits

- Real S3 integration is env-gated (`IO_S3_ENDPOINT`, `IO_S3_BUCKET`); without it the
  in-memory S3 contract server is the coverage.
- Blocking rayon hashing cannot be interrupted mid-file: a deadline returns `Timeout`
  while the worker may finish in the background.
- `stat` uses HEAD; a GET-only pre-signed URL returns `PermissionDenied` (possible
  follow-up: fallback to a 1-byte GET).
- HTTP has no directory listing or server-side copy (`Unsupported`).
- HTTP/S3 URLs are caller-controlled; the SDK is a client, not an SSRF boundary.
- `mux` yields completion order, not input order; the returned index is the ordering key.
- Local cached reads show no speedup from concurrency (benchmark is printed, not hidden).
- `rust-anything` is a repository-local harness because no external tool of that name is
  installed; it composes clippy, llvm-cov, cargo-mutants, cargo-audit, and the benchmark.
