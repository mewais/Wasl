# E2 — `wasl-procrunner` — working contract
**Purpose:** a real `wasl_transfer::Runner` that runs a transfer argv (scp/rsync) as a child process.
**Spec:** `../../docs/specs/procrunner.md`
- **code-agent:** ONLY `src/**` + `Cargo.toml`. **test-agent:** ONLY `tests/**`. Read-only: spec + `../wasl-transfer/src/lib.rs`.
- Done: spec implemented exactly; `cargo test -p wasl-procrunner` green; `cargo llvm-cov -p wasl-procrunner --fail-under-lines 100 --fail-under-regions 100`; no unsafe; fmt + `clippy --all-targets -- -D warnings` clean.
- Deps: `wasl-transfer` only. Tests use stand-in commands (`sh -c ...`, a nonexistent program) — no network, no real scp.
- Tests before code, read-only to code-agent. No `#[ignore]`/stub/tautology. Every test asserts a concrete value; tests compile only against the public API.
- **Testability honesty:** the PROCESS PLUMBING (spawn/wait/exit→Ok/Err, stderr capture, spawn failure, empty-argv guard) is genuinely 100% tested. A REAL `scp -3`/`rsync` transfer between TWO hosts is **UNTESTED HERE** (no 2nd host / SSH); the argv is exactly what `wasl_transfer` builds (already unit-tested), so running it against real hosts is the same code path, unverifiable here only for lack of a second machine.
