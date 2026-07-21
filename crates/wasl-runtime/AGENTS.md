# R1 — `wasl-runtime` — working contract

**Purpose:** the generic managed-runtime core — the frozen, object-safe type/trait surface every generic
caller (dispatch, the R3 control surface, a future external provider) talks to: the id newtypes,
`RuntimeCapabilities`, the public `DeliveryTarget` seam, the `RuntimeProvider` get-or-create seam, the driver
seam (`InteractiveProtocol`/`InteractiveTransport`/the internal `ProtocolEvent`), the public `TurnEvent`
vocabulary, admission/status/receipt value types, `Causation`, and the `RuntimeError` taxonomy.
**Tier:** L1 (traits + pure state machine + fakes fully testable).
**Full spec (read first):** `../../docs/specs/runtime.md` (+ wire pins in `../../docs/specs/runtime-control-api.md`).
**Architecture:** `../../ARCHITECTURE.md`

## What you may edit
- **code-agent:** write ONLY `crates/wasl-runtime/src/**` and `crates/wasl-runtime/Cargo.toml`. Read-only:
  `crates/wasl-runtime/tests/**`, the specs above, and the public APIs of `../wasl-proto`, `../wasl-audit`,
  `../wasl-adapter`.
- **test-agent:** write ONLY `crates/wasl-runtime/tests/**`. Read-only: the specs above.

## Phase-1 scope (THIS phase) — TYPE SURFACE ONLY
- Emit the full type/trait/enum surface exactly as the spec pins it (names, fields, variants, derives, serde
  tags/casing). Traits are DECLARED (object-safe) with NO bodies. NO scheduler/actor/queue/driver LOGIC.
- The wire types that cross the R4 control API (`TurnEvent`, `TargetStatus`, `TurnReceipt`,
  `RuntimeCapabilities`, `Causation`, `TurnSource`, `BusyPolicy`, `BusyPhase`, `NativeGateDetail`,
  `ShutdownMode`, `CancelReason`, `EventSeq`) derive `Serialize`/`Deserialize` with the EXACT tags/casing the
  control-api spec pins (internally-tagged `type` + `snake_case`; durations as integer `*_ms`; `EventSeq` a
  bare `u64`; `Causation` request-ids bare strings via `wasl_proto::RequestId` transparency).
- `ProtocolEvent` is INTERNAL: `Debug/Clone/PartialEq/Eq` only, deliberately NOT serde (boundary invariant).
- `DeliveryCertainty` is re-USED from `wasl_proto` (re-exported here), never re-defined.
- The concrete providers (`EphemeralRuntimeProvider`/`WaslManagedRuntimeProvider`) and the per-target actor
  are Phase-2/Phase-4 LOGIC and are intentionally NOT in the Phase-1 surface (no test references them; adding
  empty logic-holders would be inventing dead code).

## Definition of done (Phase-1)
- Every Phase-1 test green: `cargo test -p wasl-runtime` (object_safety, wire_forms, types_smoke,
  protocol_event_internal, control_api_wire).
- All four `dyn` seams object-safe. `cargo build -p wasl-runtime` compiles.
- No `unsafe`. `cargo fmt` clean; `cargo clippy -p wasl-runtime --all-targets -- -D warnings` clean.

## Rules
- Tests are written BEFORE code and are read-only to the code-agent. Do not edit tests to pass.
- Implement the spec EXACTLY; invent no type. If a test references something not in the spec, STOP and report.
- No test may be `#[ignore]`d/`#[cfg]`-disabled/stubbed/a tautology.
- Deps: `wasl-proto`, `wasl-audit`, `wasl-adapter`, `serde`, `serde_json`, `async-trait`, `tokio`, `futures`.

## Coverage honesty (Phase-1 — type-only surface)
Phase-1 is a pure type/enum/trait-declaration surface. It is exercised by the tester's serde round-trip,
object-safety, and taxonomy tests — that is the correct Phase-1 bar (compiles, object-safe, tests green,
clippy/fmt clean). **Behavioral LINE-100% coverage (the repo's `--fail-under-lines 100` gate) applies from
Phase-2**, when the actor/state-machine/providers land and can be driven against the in-repo scripted
transport. For Phase-1, trait declarations have no bodies and several enum variants are constructed but not
behaviorally exercised — this is expected and is NOT to be gamed with fake tests. Report the numbers honestly.

**Measured Phase-1 numbers (`cargo llvm-cov -p wasl-runtime --summary-only`):** LINE 92.31% (36/39),
REGION 91.49% (43/47), FUNCTIONS 75% (3/4). The single uncovered function / 3 uncovered lines are
`MonotonicClock::now` (src/lib.rs:85-87) — the injected-clock seam the Phase-2 actor uses for deterministic
deadlines under `tokio::time::pause`; no Phase-1 type-surface test constructs a clock, so it is legitimately
unexercised until Phase-2. The other missed regions are unexercised serde `deserialize` arms / a couple of
`RuntimeError` Display arms reached only along paths a behavioral test drives. NOT gamed — the repo's
`--fail-under-lines 100` gate applies from Phase-2 when the actor/providers/clock are driven against the
in-repo scripted transport.
