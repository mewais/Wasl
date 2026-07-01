# WD — `wasl-daemon` (routing core) — working contract
**Purpose:** the per-host daemon's routing/dispatch core — resolve peers against the registry and dispatch a
(already-gated) tool call to them: consult/command/message fan out envelopes and collect replies; transfer
delegates to the transfer tool. Local vs cross-host delivery is decided here; the actual socket IO is behind
the `Mesh` trait (and `wasl_transfer::Runner`). The real networking event loop / binary is a later thin
shell. **Spec:** `../../docs/specs/daemon.md`
- **code-agent:** ONLY `src/**` + `Cargo.toml`. **test-agent:** ONLY `tests/**`. Read-only: spec + `../wasl-proto/src/lib.rs` + `../wasl-registry/src/lib.rs` + `../wasl-mailbox/src/lib.rs` + `../wasl-transfer/src/lib.rs`.
- Done: spec implemented exactly; `cargo test -p wasl-daemon` green; `cargo llvm-cov -p wasl-daemon --fail-under-lines 100 --fail-under-regions 100`; no unsafe; the only IO is behind the `Mesh` + `Runner` traits (fake-tested); fmt + `clippy --all-targets -- -D warnings` clean.
- Tests before code, read-only to code-agent. No `#[ignore]`/stub/tautology. Deps: `wasl-proto` + `wasl-registry` + `wasl-mailbox` + `wasl-transfer` + `serde` (+ `serde_json` dev).
