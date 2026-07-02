# E1 — `wasl-filesink` — working contract
**Purpose:** a durable append-JSONL `wasl_audit::Sink` so a node's audit survives process restart.
**Spec:** `../../docs/specs/filesink.md`
- **code-agent:** ONLY `src/**` + `Cargo.toml`. **test-agent:** ONLY `tests/**`. Read-only: spec + `../wasl-audit/src/lib.rs` + `../wasl-proto/src/lib.rs`.
- Done: spec implemented exactly; `cargo test -p wasl-filesink` green; `cargo llvm-cov -p wasl-filesink --fail-under-lines 100`; no unsafe; fmt + `clippy --all-targets -- -D warnings` clean.
- Deps: `wasl-audit` + `serde_json`; dev `wasl-proto` + `tempfile`.
- Tests before code, read-only to code-agent. No `#[ignore]`/stub/tautology. Every test asserts a concrete value; tests compile only against the public API.
- **Coverage honesty (verified):** LINE 100% (35/35), FUNCTIONS 100% (6/6), REGIONS 94.55% (52/55). The residual regions are all the never-taken `?` error branch of `serde_json::to_string` in `append` — serializing an `AuditEntry` cannot fail, so that branch is unreachable. Error handling itself (`sink`) is a real, covered function reached by the open/read/`write`(via `/dev/full`)/`parse` paths; a single named helper is used (not per-site closures) so the unreachable branch costs only regions, never a line/function. NOT faked with dead code or a panic. `cargo llvm-cov -p wasl-filesink --fail-under-lines 100` exits 0.
