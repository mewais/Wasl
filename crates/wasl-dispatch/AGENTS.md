# D1 — `wasl-dispatch` — working contract
**Purpose:** async send-side dispatch — resolve peers (registry → A2A endpoint), fan out one A2A message per
peer (concurrently), collect replies/delivered + unknown/failed. **Spec:** `../../docs/specs/dispatch.md`
- **code-agent:** ONLY `src/**` + `Cargo.toml`. **test-agent:** ONLY `tests/**`. Read-only: spec + the dep crates' `src/lib.rs`.
- Done: `cargo test -p wasl-dispatch` green (loopback against REAL wasl-a2a-server + wasl-adapter fake executor); `cargo llvm-cov -p wasl-dispatch --fail-under-lines 100` (region residual from generic HTTP/serde monomorphization documented if any); no unsafe; fmt + `clippy --all-targets -- -D warnings` clean.
- Tests before code, read-only to code-agent. No `#[ignore]`/stub/tautology.
