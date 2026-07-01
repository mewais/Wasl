# WP — `wasl-proto` — working contract

**Purpose:** Wasl wire + domain types — agent cards, the four tool params/results (consult / command /
message / transfer, each with one-or-many peers for multicast), reply envelopes, gate request/decision/
token types, and the MCP `elicitation/create` request/response shapes.
**Tier:** L0 (pure, serde-only, no IO, no `unsafe`).
**Full spec (read first):** `../../docs/specs/proto.md`
**Architecture:** `../../ARCHITECTURE.md`

## What you may edit
- **code-agent:** write ONLY `crates/wasl-proto/src/**` and `crates/wasl-proto/Cargo.toml`. Read-only:
  `crates/wasl-proto/tests/**` and `../../docs/specs/proto.md`. No other blocks.
- **test-agent:** write ONLY `crates/wasl-proto/tests/**`. Read-only: `../../docs/specs/proto.md`.

## Definition of done
- The public API in `docs/specs/proto.md` is implemented exactly (names, signatures, serde forms).
- `cargo test -p wasl-proto` fully green.
- `cargo llvm-cov -p wasl-proto --fail-under-lines 100 --fail-under-regions 100` passes.
- No `unsafe`, no IO. `cargo fmt` clean; `cargo clippy -p wasl-proto --all-targets -- -D warnings` clean.

## Rules
- Tests are written BEFORE code and are read-only to the code-agent. Do not edit tests to pass.
- No test may be `#[ignore]`d, `#[cfg]`-disabled, stubbed, or a tautology. Every test runs and asserts.
- If a branch is unreachable from the public API, delete it — do not leave it uncovered.
- Only deps: `serde` (+ `serde_json` dev). No other internal deps.
