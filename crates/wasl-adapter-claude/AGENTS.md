# E3 — `wasl-adapter-claude` — working contract
**Purpose:** a concrete `wasl_adapter::AgentAdapter` that drives the Claude Code CLI (the last inch).
**Spec:** `../../docs/specs/adapter-claude.md`
- **code-agent:** ONLY `src/**` + `Cargo.toml`. **test-agent:** ONLY `tests/**`. Read-only: spec + `../wasl-adapter/src/lib.rs`.
- Done: spec implemented exactly; `cargo test -p wasl-adapter-claude` green; `cargo llvm-cov -p wasl-adapter-claude --fail-under-lines 100`; no unsafe; fmt + `clippy --all-targets -- -D warnings` clean.
- Deps: `wasl-adapter`, `serde`, `serde_json`, `async-trait`, `tokio`. Tests use a fake `CliInvoker` + stand-in commands (`cat`, `sh -c`) — NO real `claude`, NO network.
- Tests before code, read-only to code-agent. No `#[ignore]`/stub/tautology. Every test asserts a concrete value; tests compile only against the public API.
- **Testability honesty (non-negotiable):**
  - ✅ genuine 100%: `compose_prompt`, `build_argv`, `parse_reply` (pure).
  - ✅ tested: `ClaudeAdapter::deliver` wiring (fake `CliInvoker`) + `ProcessCliInvoker` process plumbing (stand-in commands).
  - ❌ **UNTESTED HERE (ASSUMPTION):** that the REAL `claude` CLI accepts these flags, reads the prompt on stdin, emits the assumed JSON, and that `--session-id` persists context. Must be verified on a host with a real `claude` binary. If the real contract differs, fix ONLY `build_argv`/`parse_reply`/the stdin convention.
  - **Coverage (verified):** genuine 100% — LINE 77/77, FUNCTIONS 13/13, REGIONS 63/63; `--fail-under-lines 100 --fail-under-regions 100` exits 0; zero uncovered regions. Achieved by passing a single named error helper `ad` *bare* (`.map_err(ad)?`) at the unreachable IO sites (stdin write, `wait`) so those `?` branches cost neither lines nor regions — same technique as `wasl-filesink`'s `sink`. No dead code / `#[allow]`-for-coverage / panics. (The `[lints.clippy]` allow-table only silences two pure-STYLE lints tripped by the frozen tests — `type_complexity`, `default_constructed_unit_structs`; src has neither pattern.)
