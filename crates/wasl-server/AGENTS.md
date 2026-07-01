# WS — `wasl-server` (gate+audit spine) — working contract
**Purpose:** the server request spine — run the three-tier permission-before-send gate for a tool call
(Tier-1 elicitation / Tier-3 marsad synchronous via the `Surfacer` trait; Tier-2 prepare→commit via the
gate ledger) and record every decision to the audit log. Pure orchestration over the `Surfacer` seam +
`wasl_audit::Sink`. Routing/transport (registry/mailbox/transfer dispatch) is the daemon's job (WD), NOT
this block. **Spec:** `../../docs/specs/server.md`
- **code-agent:** ONLY `src/**` + `Cargo.toml`. **test-agent:** ONLY `tests/**`. Read-only: spec + `../wasl-proto/src/lib.rs` + `../wasl-gate/src/lib.rs` + `../wasl-audit/src/lib.rs`.
- Done: spec implemented exactly; `cargo test -p wasl-server` green; `cargo llvm-cov -p wasl-server --fail-under-lines 100 --fail-under-regions 100`; no unsafe; the only IO is behind the `Surfacer` + `Sink` traits (fake-tested); fmt + `clippy --all-targets -- -D warnings` clean.
- Tests before code, read-only to code-agent. No `#[ignore]`/stub/tautology. Deps: `wasl-proto` + `wasl-gate` + `wasl-audit` + `serde` (+ `serde_json` dev).
