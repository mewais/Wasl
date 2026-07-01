# WA — `wasl-audit` — working contract
**Purpose:** durable, human-auditable interaction+decision log; pure query over an append-only `Sink` trait.
**Spec:** `../../docs/specs/audit.md`
- **code-agent:** ONLY `src/**` + `Cargo.toml`. **test-agent:** ONLY `tests/**`. Read-only: spec + `../wasl-proto/src/lib.rs`.
- Done: spec implemented exactly; `cargo test -p wasl-audit` green; `cargo llvm-cov -p wasl-audit --fail-under-lines 100 --fail-under-regions 100`; no unsafe; the only IO is behind the Sink trait (fake-tested); fmt + `clippy --all-targets -- -D warnings` clean.
- Tests before code, read-only to code-agent. No `#[ignore]`/stub/tautology. Deps: `wasl-proto` + `serde` (+ `serde_json` dev).
