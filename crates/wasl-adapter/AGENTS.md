# C1 — `wasl-adapter` — working contract
**Purpose:** the last-inch seam: `AgentAdapter` trait + `Delivery` extraction + `AdapterExecutor`
(implements `wasl_a2a_server::TaskExecutor`). **Spec:** `../../docs/specs/adapter.md`
- **code-agent:** ONLY `src/**` + `Cargo.toml`. **test-agent:** ONLY `tests/**`. Read-only: spec + `../wasl-a2a-types/src/lib.rs` + `../wasl-a2a-server/src/lib.rs`.
- Done: `cargo test -p wasl-adapter` green; `cargo llvm-cov -p wasl-adapter --fail-under-lines 100 --fail-under-regions 100`; no unsafe; fmt + `clippy --all-targets -- -D warnings` clean. The real CLI adapters (claude/codex/gemini) are NOT in this crate — they are a separate UNTESTED-HERE block.
- Tests before code, read-only to code-agent. No `#[ignore]`/stub/tautology.
