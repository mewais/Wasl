# A1 — `wasl-a2a-types` — working contract
**Purpose:** faithful A2A (Agent2Agent) wire types — Message/Part/Task/Artifact/AgentCard + JSON-RPC
params/results + SSE events. Pure serde, no IO. **Spec:** `../../docs/specs/a2a-types.md`
- **code-agent:** ONLY `src/**` + `Cargo.toml`. **test-agent:** ONLY `tests/**`. Read-only: the spec.
- Done: spec implemented exactly (camelCase, `kind` discriminators, kebab `input-required`); `cargo test -p wasl-a2a-types` green; `cargo llvm-cov -p wasl-a2a-types --fail-under-lines 100 --fail-under-regions 100`; no unsafe; fmt + `clippy --all-targets -- -D warnings` clean.
- Tests before code, read-only to code-agent. No `#[ignore]`/stub/tautology. Deps: `serde` + `serde_json`.
