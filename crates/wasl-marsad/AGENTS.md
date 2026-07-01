# WC — `wasl-marsad` — working contract
**Purpose:** optional Tier-3 Marsad connector — surface HITL gates to Marsad and get a human decision back,
notify Marsad of auto-approved interactions, and emit an observation feed. Pure logic over a mockable
`MarsadLink` trait (the daemon/server supplies the real HTTP/IPC link). **Spec:** `../../docs/specs/marsad-connector.md`
- **code-agent:** ONLY `src/**` + `Cargo.toml`. **test-agent:** ONLY `tests/**`. Read-only: spec + `../wasl-proto/src/lib.rs` + `../wasl-gate/src/lib.rs`.
- Done: spec implemented exactly; `cargo test -p wasl-marsad` green; `cargo llvm-cov -p wasl-marsad --fail-under-lines 100 --fail-under-regions 100`; no unsafe; the only IO is behind the `MarsadLink` trait (fake-tested, never real network); fmt + `clippy --all-targets -- -D warnings` clean.
- Tests before code, read-only to code-agent. No `#[ignore]`/stub/tautology. Deps: `wasl-proto` + `wasl-gate` + `serde` (+ `serde_json` dev).
