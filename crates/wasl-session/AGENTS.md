# R3 — `wasl-session` (bin) — working contract

**Purpose:** Wasl's minimal local control surface so "live" is demonstrable standalone — a human observes a
Wasl-owned managed session via `attach` and injects via `send`, both mediated by the single-writer actor
(Model A only). A thin CLI over a versioned control-plane (R4) client.
**Tier:** 🔩 binary (thin CLI over the R4 client).
**Full spec (read first):** `../../docs/specs/session.md` (+ the wire it speaks: `../../docs/specs/runtime-control-api.md`).
**Architecture:** `../../ARCHITECTURE.md`

## What you may edit
- **code-agent:** write ONLY `crates/wasl-session/src/**` and `crates/wasl-session/Cargo.toml`. Read-only:
  `crates/wasl-session/tests/**`, the specs above, and the public API of `../wasl-runtime`.
- **test-agent:** write ONLY `crates/wasl-session/tests/**`. Read-only: the specs above.

## Phase-1 scope (THIS phase) — SKELETON + spec-pinned mapping ONLY
- Crate/bin skeleton + the dependency edge to R1. `main` prints usage and exits (no command behavior).
- The CLI renders ONLY the PUBLIC vocabulary R1 owns (`TurnEvent`/`TargetStatus`/`TurnReceipt`/`TargetId`/
  `TurnSource`/`CancelReason`/`ShutdownMode`) plus `wasl_proto::GateDecision`; it NEVER names the internal
  `ProtocolEvent`.
- The one spec-pinned Phase-1 mapping provided: `gate <target> approve|deny [--reason]` → `GateDecision`
  (`GateArg::to_decision`), per session.md §gate. NO control-plane client logic (that is Phase-2, against a
  fake R4 client).

## Definition of done (Phase-1)
- `cargo test -p wasl-session` green (skeleton_smoke). `cargo build -p wasl-session` compiles (bin builds).
- No `unsafe`. `cargo fmt` clean; `cargo clippy -p wasl-session --all-targets -- -D warnings` clean.

## Rules
- Tests are written BEFORE code and are read-only to the code-agent. Do not edit tests to pass.
- Implement the spec EXACTLY; invent no type. If a test references something not in the spec, STOP and report.
- No test may be `#[ignore]`d/`#[cfg]`-disabled/stubbed/a tautology.
- Deps: `wasl-runtime`, `wasl-proto` (later: the R4 control-plane client, `tokio`, `clap` for the real CLI).

## Coverage honesty (Phase-1 — skeleton)
Phase-1 is a bin skeleton + one pure spec-pinned mapping; the `stub` `main` is runtime-bind glue. The pure
CLI/mapping/render LINE-100% bar (spec §Testability) applies from Phase-2 when the command semantics are wired
and unit-tested against a fake R4 client. Report honestly; the `main` usage-print glue is REGION residual by
nature (enumerated here) and is not gamed with fake tests.
