# B2 — `wasl-a2a-client` — working contract
**Purpose:** A2A client (send side). reqwest JSON-RPC to a peer's A2A endpoint. **Spec:** `../../docs/specs/a2a-client.md`
- **code-agent:** ONLY `src/**` + `Cargo.toml`. **test-agent:** ONLY `tests/**`. Read-only: spec + `../wasl-a2a-types/src/lib.rs` + `../wasl-a2a-server/src/lib.rs` (for the loopback test server + TaskExecutor).
- Done: `cargo test -p wasl-a2a-client` green (loopback against a real wasl-a2a-server + fake executor); `cargo llvm-cov -p wasl-a2a-client --fail-under-lines 100 --fail-under-regions 100` (every arm reachable — Ok/Rpc-err/Http-err/Decode + streaming + failed-task + agent-card); no unsafe; fmt + `clippy --all-targets -- -D warnings` clean.
- Tests before code, read-only to code-agent. No `#[ignore]`/stub/tautology.
