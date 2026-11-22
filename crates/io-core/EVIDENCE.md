# EVIDENCE — generic async I/O engine

Source state: commit `fd396fc56c` (this file is added in the following commit).
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
| B6 multiplexing | `tests/engine.rs`: in-flight probe (`max_active <= 4`), input order, zero-limit config error, `read_any` first success, zero-length range |
| B7 hashing | `tests/engine.rs` + `tests/local.rs`: SHA-256 known vector, missing file error, streaming hash, local rayon pool bounded by `limit` |
| B8 Python `rust_io.io` | `tests/test_rust_io.py`: exports, local roundtrip, read_range, stat/list, batches, read_any, download/upload, hash/hash_many, exception mapping, zero-limit |
| B9 invariants | `tests/test_rust_io.py` (existing net/files tests) plus `cargo test --workspace` |

## Gauntlet results

| Layer | Command | Result |
|---|---|---|
| Format | `cargo fmt --all -- --check` | exit 0 |
| Lint | `cargo clippy --workspace --all-targets -- -D warnings` | exit 0, zero warnings |
| Tests | `cargo test --workspace` | 78 passed, 0 failed (io-core 36, files-io 11, net-io 31) |
| Coverage | `cargo llvm-cov -p io-core --tests --summary-only --fail-under-lines 80` | **83.93% lines**, gate exit 0 |
| Mutation | `cargo mutants -p io-core --file src/uri.rs --file src/error.rs --timeout 60 --jobs 4` | 20 mutants: 14 caught, 6 unviable, **0 missed** |
| Supply chain | `cargo audit` (io-core and files-io lockfiles) | exit 0, zero advisories |
| Python wheel | `maturin develop --release -m crates/rust-io/Cargo.toml` + `pytest -q` | 11 passed |
| Real execution | same wheel imported as `rust_io`; `rust_io.io` local/HTTP/S3 contract tests | passed |

Coverage and mutation were measured on the identical source tree before the crates were
moved into this repository; the workspace suites were rerun here. The coverage rerun in
this checkout was blocked by `/tmp` disk exhaustion (ENOSPC), recorded verbatim; no
source changed between the two states apart from crate descriptions and version 0.3.0.

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
- `rust-anything` is not installed in this environment. The Rust-native gauntlet
  (clippy, llvm-cov, cargo-mutants, cargo-audit) plus old-coder was used instead.
