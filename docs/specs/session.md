# R3 — `wasl-session` (bin) — the local control surface — spec (authoritative)

**Tier:** 🔩 binary (thin CLI over a versioned control-plane client). **Deps:** `wasl-runtime` (R1: the
`TurnEvent`/`TargetStatus`/`TurnReceipt`/`TargetId`/`TurnSource`/`CancelReason`/`ShutdownMode` PUBLIC types +
`GateDecision`), the control-plane client for `runtime-control-api.md` (R4), `serde`, `serde_json`,
`tokio`, `clap` (arg parsing, per repo bin convention). Dev: `tokio` (macros, rt). No `unsafe`.

`wasl-session` is Wasl's own minimal **local control surface** so "live" is demonstrable **standalone** — a
human interacts with a Wasl-owned managed session without any external tool (design §4.3; the standalone
constraint). It is **Model A only** (design §1.1): a human **observes** via `attach` and **injects** via
`send`, both mediated by the single-writer actor; a human NEVER types into the underlying process directly.

**Key architectural fact (impl-plan Phase 3.5):** a `wasl-session` process CANNOT reach an actor + child
owned by a *different* `wasl-mcpd` process by linking the same crate. It therefore talks to the runtime owner
over the **inter-process control plane** specified in `docs/specs/runtime-control-api.md` (R4) — a versioned
local API (UDS on unix / loopback-HTTP elsewhere). Without that, the Phase-4 demo (`wasl session send` then an
A2A `consult`) is false. This spec covers the CLI + command semantics; R4 covers the wire.

---

## Commands (design §4.3, amended for the locked native-gate policy)

```text
wasl session start     <target-key> [--protocol claude|opencode-acp|opencode-serve]
wasl session list
wasl session attach    <target-id>                 # observe the live transcript + status stream
wasl session send      <target-id> <text>          # human-originated turn — SAME scheduler/actor path
wasl session interrupt <target-id>
wasl session stop      <target-id> [--drain]
wasl session gate      <target-id> approve|deny    # resolve a native AwaitingGate (R1 §9 policy (i))
```

Global options: `--endpoint <uds-path|url>` (the runtime owner's control-plane address; defaults to the
node's configured local socket) and `--json` (machine-readable output). All commands reach the runtime owner
via the R4 client; none link R1's actor directly.

### `start <target-key> [--protocol …]`

Get-or-create a managed target for `target-key` (== a `context_id`; R1 §10 `TargetKey`). `--protocol` selects
the R2 driver (default: the node's configured managed protocol). Calls the R4 create endpoint; prints the
resulting `TargetId` and initial `TargetStatus`. Subject to R1 admission control (target explosion / per-sender
cap / rate limit) — a refusal surfaces as a clear error (`RuntimeError::AdmissionRefused`), non-zero exit.

### `list`

List currently-known targets with their `TargetId`, `TargetStatus` (incl. phase/queued depth), capabilities,
and last-event seq. Backed by the R4 targets endpoint (`RuntimeProvider::list_targets` + per-target status).

### `attach <target-id>` — OBSERVE ONLY

Subscribe to the target's event stream via **`events(after_seq)`** (R1 §4.3) over the R4 sequenced stream.
Starts from the current snapshot (`snapshot()` → `(status, last_seq)`), then follows live `TurnEvent`s. It is
**read-only**: `attach` NEVER writes to the session (that is `send`/`gate`). Renders the transcript
(`AssistantDelta`/`Tool*`/`TurnCompleted`) + status changes. On a `Truncated{earliest, latest}` item it
re-snapshots and resumes from `latest` (the operator-api reconnect discipline). Multiple `attach`es to one
target hold **independent cursors** (multi-observer) and see identical `(seq, event)` pairs. Ctrl-C detaches;
it never affects the session.

### `send <target-id> <text>` — THE ONLY HUMAN INPUT PATH

`send` is a `TurnRequest` with **`source = TurnSource::LocalHuman`**, routed through the **same** `submit_turn`
/ actor path (R4 `submit_turn` endpoint) as an agent-originated turn. This is the *only* way a human puts input
into a Wasl-owned session (Model A). Human input **never** touches a raw shared stdin — it funnels through the
single-writer actor exactly like an `Agent` turn, preserving the single-writer invariant. `send` prints the
`TurnReceipt` (`Started{turn_id, first_seq}` / `Queued{position}` / `RejectedBusy` / `RejectedUnavailable` /
`Duplicate`); with `--json` it emits the receipt as JSON. To watch the reply, the human uses `attach` (or
`send` may optionally follow the stream from `first_seq` until this `turn_id`'s terminal event when run
interactively — but the receipt is the authoritative admission result).

### `interrupt <target-id>`

Request cancellation of the active turn: R4 `cancel_turn` with `CancelReason::OperatorRequest`. **Best-effort;
meaningful only if the target's `capabilities().interruptible` is true** — which, per the R1/R2 contract-status
tables, is **⛔ UNKNOWN on every real substrate** today, so on a real driver this typically returns
`Unsupported` (a clear, honest error), not a silent no-op. Reflected in the event stream + status when it does
take effect.

### `stop <target-id> [--drain]`

`--drain` ⇒ `ShutdownMode::Drain` (finish the active turn, reject new submits, drop the queue as `TurnExpired`,
then `Stopped`); without it ⇒ `ShutdownMode::Immediate` (stop now; an injected-but-unfinished `command`
becomes `OutcomeUnknown` per R1 §7.3/§8). Prints the final status.

### `gate <target-id> approve|deny` — resolve a native `AwaitingGate`

Provides the human approve/deny path for R1's locked native-gate policy (i) (§9). Routes to the actor's
`resolve_native_gate(turn_id, GateDecision)` **through the single-writer actor** via R4 — never a raw
side-channel to stdin. `approve` → `GateDecision::Approve`; `deny` → `GateDecision::Deny{reason}` (an optional
`--reason` string; defaults to a node-standard reason). Only valid when the target is in `AwaitingGate`; on any
other status it returns `GateUnresolvable`/`NotFound`. This native (receive-side) gate is **DISTINCT from**
Wasl's send-side gate (`wasl-gate`/`wasl-operator`); `wasl session gate` answers the CLI's own prompt, not
Wasl admission gating.

---

## Behavior contract (normative)

- Every command maps to exactly one (or, for `attach`, one streaming) R4 call; the CLI holds NO runtime state
  of its own beyond the current stream cursor.
- The CLI only ever observes/expresses the PUBLIC vocabulary — `TurnEvent`, `TargetStatus`, `TurnReceipt`,
  `GateDecision`. It never sees `ProtocolEvent` (internal to R1; R4 does not expose it).
- Errors surface as the R4 error body's `RuntimeError` mapped to a clear message + non-zero exit; `--json`
  emits the structured error.
- **Local authorization:** the control plane is local (UDS/loopback) and carries send/interrupt/stop/gate
  authority; the CLI relies on R4's local-authorization posture (default loopback/trusted-socket bind;
  `runtime-control-api.md` §Security). The CLI itself adds no auth — that is R4/deployment's concern.

---

## Standalone demo (design §5.3; impl-plan Phase 4)

`wasl session send` is exactly what the phase-4/5 standalone demo drives to establish a fact in a live session
before an A2A `consult` retrieves it (`scripts/demo-live-consult.sh`, sibling to `scripts/live-verify.sh`):
`wasl session send <t> "remember token VIOLET-7F29"` (a `LocalHuman` turn), then a real A2A `consult` from
another node asserts the **SAME native process/session** answered (pid/native-session-id unchanged — not a
fresh headless spawn) and the reply contains the token. A separate busy/queueing test submits a second turn
concurrently and asserts it queues without interleaving.

---

## Testability honesty + coverage

**Deterministic (no CLI, no LLM, no real daemon):** arg parsing (each subcommand + option), the mapping of
each command to its R4 request, and the rendering of each `TurnReceipt`/`TurnEvent`/`TargetStatus`/error — all
unit-tested against a **fake R4 client** (records the request, returns scriptable responses/streams). This
proves the control-surface wiring to 100% line without a running runtime owner.

**UNTESTED HERE (needs `--features live-cli` + a real `wasl-mcpd`):** the end-to-end path against a real
runtime owner over a real UDS/loopback socket — covered by the R4 loopback tests (`runtime-control-api.md` /
its serving crate) and the Phase-4/5 standalone demo. The two UNKNOWN facts (interruptibility, streaming
granularity) surface here as `interrupt` returning `Unsupported` and `attach` rendering whole-blob deltas on
claude/serve — the CLI reports them honestly rather than pretending.

`cargo llvm-cov -p wasl-session --fail-under-lines 100` on the pure CLI/mapping/render core; REGION residuals
only on the `main`/runtime-bind glue, enumerated in `AGENTS.md`. No `#[ignore]`/`todo!`/tautology.
