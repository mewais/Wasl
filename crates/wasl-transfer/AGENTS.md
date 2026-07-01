# WX — `wasl-transfer` — working contract
**Purpose:** transfer tool — pure `scp -3`/`rsync` argv builders (direct A->B, A->many) + `Runner` trait
(exec behind a mockable seam). **Spec:** `../../docs/specs/transfer.md`
- **code-agent:** ONLY `src/**` + `Cargo.toml`. **test-agent:** ONLY `tests/**`. Read-only: spec + `../wasl-proto/src/lib.rs`.
- Done: spec implemented exactly; `cargo test -p wasl-transfer` green; `cargo llvm-cov -p wasl-transfer --fail-under-lines 100 --fail-under-regions 100`; no unsafe; the only "IO" is behind the Runner trait (unit-tested via a fake, never real exec); fmt + `clippy --all-targets -- -D warnings` clean.
- Tests before code, read-only to code-agent. No `#[ignore]`/stub/tautology. Deps: `wasl-proto` + `serde` (+ `serde_json` dev).
