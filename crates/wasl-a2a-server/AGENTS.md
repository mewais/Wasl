# B1 — `wasl-a2a-server` — working contract
**Purpose:** A2A HTTP server (receive side). axum JSON-RPC + Task lifecycle + SSE, over a `TaskExecutor` seam.
**Spec:** `../../docs/specs/a2a-server.md`
- **code-agent:** ONLY `src/**` + `Cargo.toml`. **test-agent:** ONLY `tests/**`. Read-only: spec + `../wasl-a2a-types/src/lib.rs`.
- Done: spec implemented; `cargo test -p wasl-a2a-server` green (core unit tests + loopback HTTP tests); the request-handling CORE (`handle_send`/`handle_get`/`handle_cancel`/`stream_events`/id/lifecycle) at 100% line+region via `cargo llvm-cov`; the axum `serve()` bind loop is the IO shell (loopback-tested, may be the only uncovered line — note it); no unsafe; fmt + `clippy --all-targets -- -D warnings` clean.
- Tests before code, read-only to code-agent. No `#[ignore]`/stub/tautology. IO only via axum/http; the agent-driving is behind `TaskExecutor` (fake in tests).
