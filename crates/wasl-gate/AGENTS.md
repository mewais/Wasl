# WG — `wasl-gate` — working contract

**Purpose:** the three-tier permission-before-send gate — pure decision logic: choose the gate mode per
client capabilities (elicitation / prepare-commit / marsad), build the human-facing gate request +
elicitation message (never secrets), map an elicitation response to an outcome, and track pending
prepare→commit approvals in a ledger. IO (sending elicitations, calling Marsad) lives in the server block.
**Tier:** L0/L1 (pure state + logic, no IO, no `unsafe`).
**Full spec (read first):** `../../docs/specs/gate.md`
**Architecture:** `../../ARCHITECTURE.md`

## What you may edit
- **code-agent:** write ONLY `crates/wasl-gate/src/**` and `crates/wasl-gate/Cargo.toml`. Read-only:
  `crates/wasl-gate/tests/**`, `../../docs/specs/gate.md`, `../wasl-proto/src/lib.rs`. No other blocks.
- **test-agent:** write ONLY `crates/wasl-gate/tests/**`. Read-only: the spec + `../wasl-proto/src/lib.rs`.

## Definition of done
- Public API in `docs/specs/gate.md` implemented exactly.
- `cargo test -p wasl-gate` green; `cargo llvm-cov -p wasl-gate --fail-under-lines 100 --fail-under-regions 100` passes.
- No `unsafe`, no IO. `cargo fmt` clean; `cargo clippy -p wasl-gate --all-targets -- -D warnings` clean.

## Rules
- Tests written BEFORE code, read-only to the code-agent. Do not edit tests to pass. No `#[ignore]`/`#[cfg]`
  -disable/stub/tautology. Unreachable branches: delete, don't leave uncovered.
- Deps: `wasl-proto` + `serde` (+ `serde_json` dev). No other internal deps.
