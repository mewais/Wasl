# WM — `wasl-mailbox` — working contract
**Purpose:** message routing — inbox/outbox, multicast fan-out + reply fan-in correlation. Pure (no IO).
**Spec:** `../../docs/specs/mailbox.md`
- **code-agent:** ONLY `src/**` + `Cargo.toml`. **test-agent:** ONLY `tests/**`. Read-only: spec + `../wasl-proto/src/lib.rs`.
- Done: spec implemented exactly; `cargo test -p wasl-mailbox` green; `cargo llvm-cov -p wasl-mailbox --fail-under-lines 100 --fail-under-regions 100`; no unsafe/IO; fmt + `clippy --all-targets -- -D warnings` clean.
- Tests before code, read-only to code-agent. No `#[ignore]`/stub/tautology. Deps: `wasl-proto` + `serde` (+ `serde_json` dev).
