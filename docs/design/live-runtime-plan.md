# Live / Non-Headless Agent Delivery — design plan (authoritative for this feature)

**Status:** 🟡 **Planned — not started.** 100% unimplemented; greenfield for this capability. No crate,
type, test, or script described below exists yet. This document is the design contract to be turned into
per-crate specs (`docs/specs/*.md`) and code, test-first, once accepted. Nothing here claims to work; where a
capability is only a hypothesis pending a spike, it is labelled so explicitly. **Interaction model is locked
to Model A** (Wasl-owned session; human observes via `attach` and injects via `send`); Model B
(terminal-attach) is foreclosed by the single-writer invariant — see §1.1.

**Scope:** add genuine support for delivering a consult/command/message into a **Wasl-owned, persistent,
multi-turn interactive** coding-agent session that a human can **observe** (`wasl session attach`) and
**inject into via Wasl** (`wasl session send`) — as an *alternative* runtime to Wasl's current universal
behavior of spawning a fresh headless CLI process per delivery. This is **Model A** (see the "Interaction
model" note in §1): Wasl spawns and owns the session and holds the sole input handle; "human-visible" means
*observable + Wasl-mediated input*, **not** a terminal a human types into directly. The current behavior
stays the default and unchanged.

**Hard constraint (locked):** this capability is designed, implemented, and proven **entirely within the
Wasl repository**, using **only Wasl's own existing CLI adapter targets** (claude / codex / antigravity /
opencode), with **zero dependency on, or knowledge of, any external supervisor/consumer.** No
project-specific code, type, field name, or assumption from any external tool may enter Wasl's core. An
external supervisor MAY later plug in — but only as an optional, pluggable, generic extension, designed and
proven as a *separate follow-on effort* after this plan's phases ship. See §8 (Future work) and §9
(Non-goals).

**Relationship to `ARCHITECTURE.md`:** this feature is additive to the existing block map. It does not modify
any existing crate's contract. It is expected to introduce new crates — provisionally `wasl-runtime`
(generic scheduler/state-machine core, L1) and `wasl-runtime-protocol` (one driver per structured CLI
protocol, L2) — plus a small local-control-surface binary. `ARCHITECTURE.md` is **not** edited by this
document; the block-table entries are proposed in §7 for a later, separate edit once specs land.

---

## Table of contents

1. [Context / motivation](#1-context--motivation)
2. [Core design: a capability model, not a binary "live" flag](#2-core-design-a-capability-model-not-a-binary-live-flag)
3. [New abstraction layers](#3-new-abstraction-layers)
4. [Turn/session state machine (Wasl-owned)](#4-turnsession-state-machine-wasl-owned)
5. [Standalone validation strategy](#5-standalone-validation-strategy)
6. [Build sequencing (strict order)](#6-build-sequencing-strict-order)
7. [Proposed block-map delta](#7-proposed-block-map-delta-not-applied-here)
8. [Future work — external runtime seam (out of scope for this plan)](#8-future-work--external-runtime-seam-out-of-scope-for-this-plan)
9. [Non-goals](#9-non-goals-for-this-plan)

---

## 1. Context / motivation

Wasl's "last inch" is the `C1 wasl-adapter` seam: an incoming A2A Task is turned into a `Delivery`, and

```rust
#[async_trait::async_trait]
pub trait AgentAdapter: Send + Sync {
    async fn deliver(&self, d: &Delivery) -> Result<String, AdapterError>;
}
```

is called to inject it into the local agent and return the reply. This is `request -> reply`: one shot, no
lifecycle, no observability, no notion of "the session is busy."

**The reality across all four concrete adapters today** (`E3 claude`, `E4 codex`, `E5 antigravity`, `W9
opencode`) is that every `deliver` **spawns a brand-new headless CLI process**:

- claude: `claude -p --output-format json --session-id|--resume <uuid>`, prompt on stdin, process exits.
- codex: `codex exec` / `exec resume`, per-turn process.
- antigravity: `agy -p` / `--conversation`, per-turn process.
- opencode: `opencode run --format json [-s <id>]`, per-turn process that self-terminates on `step_finish`.

Continuity, where it exists, is **resume-based** (claude's `--resume`, opencode's `-s`, codex/agy's thread
ledger) — context is *recovered* by re-attaching to a stored session on each new process, not *held* in a
live one. Consequences:

- Every consult/command/message answer comes from a **disposable, invisible, parallel** conversation.
- There is **no way today** to inject into an **already-running, observable, multi-turn interactive**
  session — a persistent session Wasl holds open and a human can watch live, rather than a fresh headless
  process per turn.
- There is no concept of the target being **busy**, no queueing, no live streaming, no attach/observe, no
  interrupt, no mid-turn correlation.

**Goal:** deliver into a genuinely live, persistent, multi-turn session — while keeping Wasl fully
self-contained and honest about which adapters can actually do this and which cannot.

### 1.1 Interaction model (Model A — locked)

**Wasl owns the session.** Wasl spawns and owns the underlying process/protocol connection and holds the
**sole** input handle to it. A human never types into that process directly; instead the human:

- **observes** the live transcript + status via `wasl session attach` (a read-only subscriber to the event
  stream), and
- **injects** input via `wasl session send`, which is a `TurnRequest` with `source = TurnSource::LocalHuman`
  routed through **the same single-writer actor** as an agent-originated turn.

Under this model the **single-writer invariant is PRESERVED, not lost**: there is exactly **one physical
writer** — the per-target actor (§4) — and **two logical turn sources** — `Agent` and `LocalHuman` — which
the actor serializes. See the single-writer language in §4; it is not duplicated here.

**Model B — attaching to a session a human is personally driving in their own terminal / native TUI — is
OUT OF SCOPE and foreclosed.** That model would require a shared raw input path (PTY / multi-writer stdin),
which directly contradicts the locked single-writer invariant ("no shared raw handles, ever") that §4 and §8
forbid. It is not a feasibility question a spike could settle; it contradicts the design's own safety rule.
See §9 (Non-goals).

---

## 2. Core design: a capability model, not a binary "live" flag

A single `live: bool` would lie. The four adapters have **genuinely different** properties, and forcing
symmetry across them is exactly the dishonesty the block regime forbids ("no block claims end-to-end 'it
works' on the strength of fakes alone"). Instead, a target reports an explicit **capability set**, and every
test and every scheduler decision is **gated on the specific capability it needs.**

```rust
/// What a delivery target can honestly do. All fields are INDEPENDENT booleans — an adapter reports the
/// exact subset it has proven, never a rounded-up "live: true". A generic caller (dispatch, the local
/// control surface, a future external provider) reads these to decide behavior; it MUST NOT assume any
/// field implies another.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
pub struct RuntimeCapabilities {
    /// The SAME OS process serves multiple turns (no per-turn spawn). The strongest "live" signal.
    pub persistent_process: bool,
    /// Conversation context persists across turns even if via resume (NOT necessarily the same process).
    /// Every current adapter can already claim this; `persistent_process` is the strictly stronger claim.
    pub persistent_conversation: bool,
    /// The protocol yields reliable turn START/END boundaries (not a screen-scrape guess).
    pub structured_turns: bool,
    /// Assistant output can be streamed incrementally (deltas), not only as a final blob.
    pub streaming_output: bool,
    /// A human/viewer can observe the live session (the "non-headless" property).
    pub attachable: bool,
    /// An in-flight turn can be cancelled/interrupted cleanly.
    pub interruptible: bool,
    /// Turn boundaries + native ids give stable, machine-usable provenance (correlation ids), not just text.
    pub structured_provenance: bool,
    /// A dropped/crashed session can be resumed with context intact.
    pub resumable: bool,
}
```

**Expected honest reports (hypotheses, to be confirmed by the §6 phase-0 spikes — do NOT hard-code these
until verified):**

| Target | `persistent_process` | `persistent_conversation` | `structured_turns` | `attachable` | Notes |
|---|---|---|---|---|---|
| claude (stream-json stdin) | **candidate: true** | true | likely true | candidate | Strongest near-term candidate — a resumable multi-turn stdin protocol already exists. |
| opencode (ACP / JSON-RPC) | **candidate: true** | true | likely true | candidate | Second target; structurally different from claude → proves the abstraction is generic. |
| codex | likely **false** | true | resume-based | — | May only ever honestly claim `persistent_conversation` unless a spike finds a real long-lived protocol. |
| antigravity | likely **false** | true | resume-based | — | Same as codex. Do not force `persistent_process` on it. |

The `EphemeralRuntimeProvider` (§3.2) — today's spawn-per-turn behavior — reports
`persistent_process: false, persistent_conversation: true, structured_turns: false` and so on: it is a
**valid, first-class, permanently-supported** target that simply does not claim liveness. No adapter is
forced to grow a capability it does not have.

---

## 3. New abstraction layers

**Do not bolt this onto `AgentAdapter::deliver`.** Growing that trait a pile of optional /
`unsupported_operation()` methods would produce a weak, partially-implemented interface where every caller
must defensively probe what actually works. Instead, three cleanly-separated layers sit *above* the existing
adapter, and the existing adapter is *wrapped* (never modified) as one provider.

```text
      dispatch / local control surface / (future) external provider
                              │  talks only to ──►  DeliveryTarget   (§3.1)
                              ▼
   RuntimeProvider  (§3.2)  ── constructs / recovers ──►  DeliveryTarget
     ├─ EphemeralRuntimeProvider   → wraps today's AgentAdapter::deliver, UNCHANGED
     └─ WaslManagedRuntimeProvider → Wasl owns a long-lived process/protocol
                              │  drives, per protocol ──►
                              ▼
   InteractiveProtocol + InteractiveTransport  (§3.3)  — driver seam, one per CLI protocol
     ├─ ClaudeStreamProtocol      (phase 4)
     └─ OpenCodeAcpProtocol       (phase 5)
```

### 3.1 `DeliveryTarget` — the seam dispatch/routing talks to

The one interface the generic parts of Wasl address. It is deliberately small, async, and **admission-safe**
(see `submit_turn`).

```rust
/// A live-or-ephemeral thing Wasl can deliver turns into. Generic dispatch/routing knows ONLY this trait —
/// it never sees a Claude/ACP/process detail. Every method is advisory-or-atomic as noted.
#[async_trait::async_trait]
pub trait DeliveryTarget: Send + Sync {
    /// Stable id for this target (a Wasl-side handle, not a CLI-native session id).
    fn target_id(&self) -> &TargetId;

    /// What this target can honestly do (§2). Constant for the target's lifetime.
    fn capabilities(&self) -> RuntimeCapabilities;

    /// A best-effort snapshot of state (§4). ADVISORY ONLY — never gate an admission on this; it can be
    /// stale the instant it returns. The authoritative admission decision is `submit_turn`.
    async fn status(&self) -> TargetStatus;

    /// THE ATOMIC ADMISSION DECISION. Submits a turn and returns immediately with a receipt describing
    /// whether it Started, Queued (with position), or was Rejected. This MUST NOT race with `status()` and
    /// MUST NOT block waiting for the full reply — the reply arrives via `events`. The `busy_policy`
    /// governs what happens if the target is busy (§4).
    async fn submit_turn(&self, req: TurnRequest, busy_policy: BusyPolicy) -> TurnReceipt;

    /// A replayable, sequenced event stream for this target. `after_seq` lets a (re)subscriber resume
    /// without missing or duplicating events (event-journal semantics, §4). Carries turn lifecycle,
    /// assistant deltas, tool activity, gate activity, completion/failure.
    async fn events(&self, after_seq: EventSeq) -> TurnEventStream;

    /// Request cancellation of an in-flight turn. Best-effort; only meaningful if
    /// `capabilities().interruptible`. Reflected in the event stream + status.
    async fn cancel_turn(&self, turn_id: TurnId, reason: CancelReason) -> Result<(), RuntimeError>;

    /// Shut the target down. `Drain` finishes the active turn then stops; `Immediate` stops now.
    async fn shutdown(&self, mode: ShutdownMode) -> Result<(), RuntimeError>;
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub enum TurnReceipt {
    /// Admitted and now the active turn.
    Started { turn_id: TurnId, first_seq: EventSeq },
    /// Admitted into the FIFO queue at `position` (0 = next after the active turn).
    Queued { turn_id: TurnId, position: usize },
    /// Refused: the target is busy and `busy_policy` said not to queue.
    RejectedBusy { active_turn: TurnId },
    /// Refused: the target is Offline/Faulted/Stopped (not a busy condition).
    RejectedUnavailable { status: TargetStatus },
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub enum ShutdownMode { Drain, Immediate }
```

`TurnRequest` carries the `Delivery` payload plus the routing/causation metadata the scheduler needs
(idempotency key, causation chain, provenance) — see §4.

### 3.2 `RuntimeProvider` — constructs / recovers targets

```rust
/// Constructs (or recovers, after a restart) `DeliveryTarget`s. Config selects which provider a node uses,
/// e.g. `runtime = "ephemeral" | "managed"`.
#[async_trait::async_trait]
pub trait RuntimeProvider: Send + Sync {
    /// Get-or-create the target for a given context/agent identity. Idempotent: the same key returns the
    /// same live target (for managed) or a fresh ephemeral wrapper (for ephemeral).
    async fn target_for(&self, key: &TargetKey) -> Result<Arc<dyn DeliveryTarget>, RuntimeError>;

    /// List currently-known targets (for the local control surface `list`).
    async fn list_targets(&self) -> Vec<TargetId>;
}
```

Two initial implementations:

```rust
/// Backward-compatibility path: wraps today's spawn-per-turn `AgentAdapter::deliver` UNCHANGED. Every
/// `submit_turn` maps to exactly one `deliver` call; the "turn" starts, runs, and completes as one unit;
/// no live process is held. Reports `persistent_process: false`. THIS MUST KEEP WORKING EXACTLY AS TODAY —
/// it is the default and the permanent fallback.
pub struct EphemeralRuntimeProvider<A: wasl_adapter::AgentAdapter> { /* adapter, ... */ }

/// The new capability: Wasl itself spawns and OWNS a long-lived process/protocol connection that serves
/// multiple turns. Delegates the wire details to an `InteractiveProtocol` driver (§3.3); owns the
/// scheduler/actor/state-machine (§4) generically.
pub struct WaslManagedRuntimeProvider<P: InteractiveProtocol> { /* protocol, ... */ }
```

`EphemeralRuntimeProvider` yields an `EphemeralTarget: DeliveryTarget` (§6 phase 3). Its `submit_turn` can
only ever return `Started` (a fresh spawn is never "busy" from Wasl's view — the OS handles concurrency) and
its `events` stream emits a synthetic `TurnAccepted → (final) AssistantDelta → TurnCompleted` sequence so the
generic caller sees one uniform event model regardless of provider.

### 3.3 `InteractiveProtocol` + `InteractiveTransport` — the driver seam

Used **only** by `WaslManagedRuntimeProvider`. One implementation per structured CLI protocol. This is the
**only** place Claude-specific or ACP-specific knowledge is allowed. The generic scheduler in §4 MUST NOT
know any of it — exactly as the existing adapters keep CLI specifics behind the `CliInvoker` seam.

```rust
/// A structured, multi-turn CLI protocol Wasl can drive as a live session. Analogous in spirit to the
/// per-adapter `CliInvoker` seam, but for a LONG-LIVED connection instead of a one-shot process.
#[async_trait::async_trait]
pub trait InteractiveProtocol: Send + Sync {
    fn capabilities(&self) -> RuntimeCapabilities;

    /// Start a fresh live session, returning a live transport.
    async fn spawn(&self, opts: &SpawnOptions) -> Result<Box<dyn InteractiveTransport>, RuntimeError>;

    /// Re-attach to an existing native session (only if `capabilities().resumable`).
    async fn resume(&self, native: &NativeSessionId, opts: &SpawnOptions)
        -> Result<Box<dyn InteractiveTransport>, RuntimeError>;
}

/// A live connection to one running agent session. The scheduler is the SOLE writer (§4): nothing else may
/// call `send_turn`, and nothing else may hold the underlying stdin/socket handle.
#[async_trait::async_trait]
pub trait InteractiveTransport: Send {
    /// Push one turn's prompt into the live session. Returns once the turn is ACCEPTED by the CLI (not once
    /// it completes — completion arrives via `next_event`).
    async fn send_turn(&mut self, prompt: &ComposedPrompt) -> Result<(), RuntimeError>;

    /// Pull the next normalized protocol event. Ends with `ProcessExited` when the session dies.
    async fn next_event(&mut self) -> Option<ProtocolEvent>;

    /// Best-effort interrupt of the active turn (only if `capabilities().interruptible`).
    async fn interrupt(&mut self) -> Result<(), RuntimeError>;
}

/// The NORMALIZED event vocabulary every protocol driver maps its native stream onto. Driver-specific
/// wire shapes (claude stream-json objects, ACP JSON-RPC notifications) are translated HERE and never leak
/// upward. This is the contract the generic scheduler consumes.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum ProtocolEvent {
    /// The session is up and ready to accept the first/next turn (carries the native session id if any).
    SessionReady { native: Option<NativeSessionId> },
    /// The CLI acknowledged the submitted turn and is now working on it.
    TurnAccepted { native_turn: Option<String> },
    /// An incremental chunk of assistant output.
    AssistantDelta { text: String },
    /// A tool call began / finished (name only; args/results stay in provenance, not model-trusted text).
    ToolStarted { name: String },
    ToolFinished { name: String, ok: bool },
    /// The CLI is asking for a NATIVE permission (its own approval gate). Maps to `AwaitingGate` (§4).
    NativeGateRequested { detail: NativeGateDetail },
    /// The turn finished successfully; carries the final reply text (concatenated deltas or a final blob).
    TurnCompleted { reply: String },
    /// The turn failed inside the session (distinct from the process dying).
    TurnFailed { error: String },
    /// The underlying process/connection exited. Terminal for this transport.
    ProcessExited { code: Option<i32> },
}
```

**Explicitly avoided:**

- **(a) Growing `AgentAdapter` with optional methods.** The existing trait stays `deliver -> reply`. Liveness
  lives in the new layers; ephemeral adapters are *wrapped*, not amended. This keeps every interface fully
  implemented by everything that implements it.
- **(b) PTY / terminal screen-scraping as the primary mechanism.** Raw PTY automation is fragile: ANSI escape
  sequences, ambiguous completion detection, no correlation ids, and human-keystroke races against Wasl's
  writes. It is permitted **only** as an explicitly-weaker, clearly-labelled fallback for a CLI with no
  structured protocol — such a driver MUST report `structured_turns: false` (and almost certainly
  `structured_provenance: false`) so no test or caller mistakes it for a first-class live target. It is
  **never** the default design and never the target of phases 4–5.

---

## 4. Turn/session state machine (Wasl-owned)

For a **managed** target, Wasl owns the whole lifecycle. There is **one actor/task per managed target**. That
actor is the **single writer** to the live transport and the single owner of:

- the active turn and the pending **FIFO** queue,
- the live `InteractiveTransport`,
- the monotonic **event sequence numbers** (for replay/resubscribe),
- an **idempotency-key map** (dedupe re-sends),
- the **transcript projection** (what a human attach/observe sees),
- **gate state** (native CLI gates surfaced as `AwaitingGate`).

**Single-writer invariant (locked):** all inputs — A2A-originated turns AND any local/human input Wasl
exposes (§4, control surface) — funnel through this one actor. **Nothing else may write to the underlying
process's stdin/socket directly.** No shared raw handles, ever. This is the property that makes concurrent
human + agent input on the same live session safe. (This is exactly why Model B — a human driving the native
TUI as a second writer — is foreclosed; see §1.1 and §9.)

### 4.0 State durability: in-memory only (one write-ahead exception)

**All authoritative runtime state for a managed target is IN-MEMORY only and does NOT survive a node
restart.** The list above — managed targets, the active turn and FIFO queue, the event journal / sequence
numbers, the idempotency-key map, the transcript projection, and per-turn delivery-certainty — lives solely
in the actor's process. **On a node restart, managed targets are abandoned** (the owned child process is
gone with its parent); there is no reconstruction of the live session, its queue, or its in-flight turns.
Only the `wasl-audit` log persists across a restart.

This makes managed sessions **the first Wasl subsystem whose authoritative live state is NOT the append-only
audit log** — a deliberate, documented departure from Wasl's usual "audit = durable truth" convention, called
out here so no reader assumes the audit log can rebuild a managed session.

Two clarifications so the trait/type language above is not read as promising more than this:

- The `RuntimeProvider` **"constructs / recovers"** wording (§3.2) and the **`resumable`** capability (§2)
  refer to **re-attaching to a CLI-native session WITHIN a single node-process lifetime** (and only where the
  concrete CLI actually supports resume) — e.g. `InteractiveProtocol::resume` re-hydrating context after a
  dropped transport. They do **NOT** mean surviving a Wasl node restart, and must not be relied on for that.
- **THE ONE EXCEPTION (a safety carve-out, not general durability):** for a **`command`** delivery, its
  delivery-certainty + idempotency record is **write-ahead durable in `wasl-audit` BEFORE injection**. A
  crash therefore cannot erase the evidence needed to detect an `OutcomeUnknown` command and prevent an
  unsafe blind retry (§4.4). This is the *only* piece of runtime state that is durable; everything else is
  in-memory and lost on restart.

### 4.1 Status

```rust
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum TargetStatus {
    Offline,
    Starting,
    Idle { since: Timestamp },
    Busy { active_turn: TurnId, source: TurnSource, phase: BusyPhase, since: Timestamp, queued: usize },
    /// Blocked on a NATIVE CLI permission gate. This is BUSY, not idle — the session cannot take a new turn.
    AwaitingGate { active_turn: TurnId, detail: NativeGateDetail, since: Timestamp, queued: usize },
    Interrupting { active_turn: TurnId, since: Timestamp },
    Draining { active_turn: TurnId, since: Timestamp },
    Faulted { error: String, recoverable: bool },
    Stopped,
}

/// The fine-grained phase of an active turn (for observability + smarter busy policies).
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum BusyPhase {
    Dispatching,      // prompt handed to the transport, awaiting TurnAccepted
    Generating,       // assistant is producing output
    RunningTool,      // a tool is executing
    WaitingForTool,   // awaiting a tool result
    WaitingForInput,  // the session is blocked awaiting some input
    Completing,       // finalizing the turn
}

/// Who originated the active turn (for status/observability and QueueRelated policy).
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum TurnSource { Agent { from: AgentIdentity }, LocalHuman }
```

**`AwaitingGate` counts as busy.** A session blocked on the CLI's own permission prompt is not free to accept
a new turn; reporting it as idle would let the scheduler inject into a wedged session.

**`AwaitingGate` MUST be resolvable (locked requirement).** A session may enter `AwaitingGate` when the CLI
raises its **own native** permission prompt (`ProtocolEvent::NativeGateRequested`). The design **requires** a
resolution path so a session cannot wedge in `AwaitingGate` forever — **"observe but never resolve" is NOT
acceptable.** The *exact* policy is chosen at the spec phase from one of:

1. **A generic native-gate response operation** exposed through the control surface for explicit human
   approve/deny — i.e. `wasl session gate <target-id> approve|deny` (added to §4.3), routed like any other
   input through the single-writer actor (never a side-channel to raw stdin).
2. **Managed CLI profiles configured to disable native interactive gates**, so that any
   `NativeGateRequested` from a managed session is treated as a **terminal configuration fault** (the session
   is Faulted, not left blocked).
3. **A proven out-of-band native resolution** that does **not** violate single-writer ownership of the
   session's input handle.

This native (receive-side) gate remains **DISTINCT from and does not merge with** Wasl's own send-side
three-tier gate — resolving an `AwaitingGate` is about answering the CLI's prompt, not about Wasl's admission
gating.

### 4.2 Busy policy — per delivery, admission-atomic

```rust
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum BusyPolicy {
    /// Queue behind the active turn, up to a bounded wait; expire if not started in time.
    Queue { max_wait: Duration },
    /// Refuse immediately if busy (→ TurnReceipt::RejectedBusy).
    Reject,
    /// Queue ONLY if the incoming turn shares a causation chain with the active turn (§4.4); else reject.
    QueueRelated { max_wait: Duration },
}
```

Suggested defaults (a delivery may override):

- `consult` → `Queue { max_wait }` with a bounded deadline (a late answer is still useful, but not forever).
- `command` → `Reject` unless a node explicitly opts a command into queueing (a *delayed* command may go
  stale and cause a wrong-time side effect — see delivery certainty, §4.3).
- `message` → `Queue` with **expiry** (a stale notification should drop, not pile up).

`submit_turn` evaluates the policy against the *authoritative* internal state inside the actor — **not**
against a prior `status()` snapshot — so admission never races. This is why `status()` is documented as
advisory-only.

### 4.3 Local control surface (so "live" is demonstrable standalone)

Because Wasl now owns a live, human-visible-capable session, Wasl needs its **own minimal local control
surface** for a human to interact with the **same** managed session — otherwise "non-headless" cannot be
demonstrated without an external tool, violating the standalone constraint. A small CLI:

```text
wasl session start   <target-key> [--protocol claude|opencode]
wasl session list
wasl session attach  <target-id>          # observe the live transcript + status stream
wasl session send    <target-id> <text>   # human-originated turn — SAME scheduler/actor path
wasl session gate    <target-id> approve|deny   # resolve a native AwaitingGate (if policy (i), §4.1)
wasl session interrupt <target-id>
wasl session stop    <target-id> [--drain]
```

`send` is a `TurnRequest` with `source = LocalHuman`, routed through the **same** `submit_turn`/actor as an
agent turn — this is the *only* way a human puts input into a Wasl-owned session (Model A, §1.1). Human input
**never** touches a raw shared stdin. `attach` subscribes via `events(after_seq)` (observe-only). `gate`
provides the human approve/deny path when the native-gate resolution policy chosen in §4.1 is option (i);
like `send`, it is mediated by the actor and never a raw side-channel. This surface is what phase-4/5 demos
drive to establish a fact in a live session before an A2A `consult` retrieves it (§5.3).

### 4.4 Delivery certainty, recursion protection, provenance

**Delivery certainty** is tracked explicitly and included in receipts/audit:

```rust
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum DeliveryCertainty {
    NotInjected,           // never reached the session
    Injected,              // handed to the session, outcome pending
    Completed,             // turn completed, reply captured
    FailedBeforeInjection, // failed before any side effect could occur — safe to retry
    OutcomeUnknown,        // crash/disconnect AFTER injection — may have caused a side effect
}
```

Retry rules (locked):

- **Never auto-retry a `command` whose certainty is `OutcomeUnknown`** after a crash — it may already have
  caused a side effect. This is a hard rule (also a §9 non-goal restated).
- A `consult` retry MUST be a **new, explicitly-linked attempt** (a fresh turn id, linked to the original in
  the causation chain), never a blind resend on the same turn id.

**Recursion / deadlock protection.** The static peer-config graph does not prevent a *runtime* deadlock
(A consults B, B synchronously consults A back on the same live, now-busy session). Every turn carries a
causation chain:

```rust
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct Causation {
    pub root_request: RequestId,
    pub parent_request: Option<RequestId>,
    pub path: Vec<AgentIdentity>,  // agents already on this synchronous call path
    pub hop: u32,                  // distance from root
}
```

Default protections: a **max-hop limit**, and **reject a synchronous consult if the target already appears in
`path`** (would-be cycle). `QueueRelated` uses `root_request` to decide relatedness.

**Provenance — two distinct kinds, never conflated:**

- **Audit provenance** (durable, in the `wasl-audit` log): source agent/node, task id, gate result,
  timestamps, delivery certainty. This is the machine-trustworthy record.
- **Model-visible provenance** (a stable text wrapper handed to the target CLI marking the content as
  *agent-originated / untrusted task content, not system authority*). This is **context, not a security
  control** — an LLM can be argued out of any wrapper. The wrapper mirrors the existing `compose_prompt`
  header convention (`[Consult from {from}]\n\n{text}`) so a live turn reads the same as a headless one, but
  the plan states plainly: **do not rely on the wrapper for isolation** — the real controls are the CLI's own
  permission config and the deployment's model/config isolation (as already documented for the opencode
  receive side).

---

## 5. Standalone validation strategy

Everything must be proven with **zero external consumer** and no real-LLM nondeterminism in the core tests —
matching the existing regime (fake-in-repo cores at 100%, real-CLI behind `--features live-cli`, honesty
about what is `UNTESTED HERE`). Three layers.

### 5.1 Deterministic contract tests against an in-repo fake structured agent

Build a small **fake structured "agent" test binary INTO the Wasl repo** (a dev-only target, like the
stand-in commands the existing `process_invoker` tests use, but *long-lived*). Properties:

- **Remains running**, accepts structured turns over a simple **line-oriented JSON** protocol (one request
  per line, events back one per line) — so `WaslManagedRuntimeProvider` + a trivial `FakeLineProtocol`
  driver can exercise the full generic scheduler without any real CLI.
- Maintains an **in-memory fact map** across turns (`remember K=V` / `recall K`) to **prove context
  retention in the same live process** — the property no headless spawn can have.
- Can be told to **`hold`** deterministically (block a turn until released) to test queueing without timing
  flakiness.
- Can be told to **simulate a crash** (exit mid-turn) to test recovery and `OutcomeUnknown`.

Against this fake, deterministic `#[tokio::test]`s prove: FIFO queueing; reject-vs-queue-on-busy;
**no input interleaving** (concurrent submits never corrupt the single-writer stream); event
**replay/sequencing** (`events(after_seq)` resumes exactly); attach/detach; cancellation; crash + recovery;
**duplicate idempotency-key** handling (a resend with the same key does not double-inject); and native-gate
interaction (`AwaitingGate` blocks admission as busy). This is where the state machine is genuinely 100%
tested — no LLM, no flakiness.

### 5.2 Adapter conformance suite (capability-gated)

Every interactive driver runs the **same** shared suite, and each assertion is **gated on the capability it
tests** — so a driver is only held to what it claims:

```rust
async fn conformance(target: &dyn DeliveryTarget) {
    let caps = target.capabilities();

    if caps.persistent_process {
        let p1 = pid_after_turn(target, "turn 1").await;
        let p2 = pid_after_turn(target, "turn 2").await;
        assert_eq!(p1, p2, "persistent_process claimed → same OS process across turns");
    }
    if caps.persistent_conversation {
        submit_and_wait(target, "remember VIOLET-7F29").await;
        let r = submit_and_wait(target, "recall the token").await;
        assert!(r.contains("VIOLET-7F29"), "persistent_conversation claimed → context retained");
    }
    if caps.structured_turns { /* assert clean start/end boundaries */ }
    if caps.interruptible    { /* assert cancel_turn actually interrupts */ }
    // ...one gated block per capability. Never assert a capability the target did not claim.
}
```

`EphemeralTarget` runs this same suite and passes the `persistent_conversation` block (via resume) while
**correctly not** being asserted for `persistent_process` — proving the gating is honest, not a loophole.

### 5.3 Real-CLI opt-in live tests + a standalone demo

Behind `--features live-cli` (a cargo feature, **not** `#[ignore]` — the orchestrator asserts it RAN), with
a **pinned, tested CLI version**, and a standalone demo script (e.g. `scripts/demo-live-consult.sh`, sibling
to `scripts/live-verify.sh`). The end-to-end proof, entirely standalone:

1. Two Wasl **nodes** run standalone (loopback = two "hosts", same code path — the existing testability-honesty
   convention).
2. Node **B** holds a **genuinely live managed session** (real claude first; real opencode in phase 5).
3. A **human-originated** turn (via `wasl session send`, §4.3) establishes a fact:
   `remember token VIOLET-7F29`.
4. A real A2A **`consult`** from node **A** asks node B for that fact.
5. Assert: the **SAME native process/session** answered (assert the pid/native-session-id is unchanged across
   the human turn and the consult — **not** a fresh headless spawn), the answer contains `VIOLET-7F29`, and it
   round-trips back to node A's `ConsultResult.replies`.
6. A separate **busy/queueing** live test: start a long-held turn, submit a second concurrently, assert it
   **queues and does not interleave**, and both complete in order.

**No external-project name, type, or config appears anywhere in these tests or the demo.**

---

## 6. Build sequencing (strict order)

Do not skip ahead. Each phase is provable before the next begins.

- **Phase 0 — Feasibility spikes (per existing adapter).** For claude / codex / antigravity / opencode:
  does it support multi-turn *same-process* delivery? What is the **exact** ready signal, turn-boundary
  signal, and completion signal? Is there a stable **native session id**? Is it interruptible? Output: an
  **honest capability matrix** (fills in the §2 table with verified values). **Do not force symmetry** — it
  is an acceptable and expected result that codex/antigravity report only `persistent_conversation`.

- **Phase 1 — Contracts + spec.** Freeze the generic traits/types (§3) and the full state-machine spec (§4)
  in per-crate specs + inline rustdoc **before** any scheduler code exists. (This document is the input to
  that phase.)

- **Phase 2 — Deterministic managed-runtime manager.** Build the generic scheduler/actor, FIFO queue, event
  journal, idempotency map, and transcript projection **against the in-repo fake agent only** (§5.1). No real
  CLI, no protocol driver beyond the trivial `FakeLineProtocol`. This is the 100%-tested core.

- **Phase 3 — Wrap today's headless adapters as `EphemeralTarget: DeliveryTarget`.** **Zero behavior
  change.** This is the always-available compatibility path, selected via `runtime = "ephemeral" | "managed"`
  config. Proven by re-running the existing adapter behavior through the new `DeliveryTarget` surface with
  identical results, plus the capability-gated conformance suite (§5.2).

- **Phase 4 — First real managed protocol end-to-end: `ClaudeStreamProtocol`.** Recommended first because
  claude already exposes a resumable multi-turn stdin protocol (stream-json). Proven via layers §5.1–§5.3,
  standalone, against a **pinned tested claude version**.

- **Phase 5 — Second, structurally-different managed protocol: `OpenCodeAcpProtocol` (ACP / JSON-RPC).**
  Chosen specifically to **prove the abstraction is genuinely generic** — that `ProtocolEvent` and the
  scheduler are not just renamed Claude concepts. If the abstraction leaks, this is where it shows.

- **Phase 6 — (future) sketch the external seam.** ONLY after phases 0–5 ship and are green. See §8. This is
  a *sketch in a follow-up doc*, not implementation.

---

## 7. Proposed block-map delta (NOT applied here)

For a **later, separate** `ARCHITECTURE.md` edit once specs land — listed here for planning only:

| # | Crate | Tier | Status | Purpose |
|---|---|---|---|---|
| R1 | `wasl-runtime` | L1 | 🟡 planned | Generic runtime core: `DeliveryTarget`, `RuntimeProvider`, `RuntimeCapabilities`, the per-target actor/state-machine (§4), FIFO queue, event journal, idempotency map, transcript projection, `EphemeralRuntimeProvider`/`WaslManagedRuntimeProvider`. Tested 100% against the in-repo fake structured agent. |
| R2 | `wasl-runtime-protocol` | L2 | 🟡 planned | `InteractiveProtocol`/`InteractiveTransport` drivers: `FakeLineProtocol` (dev), `ClaudeStreamProtocol` (phase 4), `OpenCodeAcpProtocol` (phase 5). IO shell over the tested R1 core; real CLIs behind `--features live-cli`. |
| R3 | `wasl-session` (bin) | 🔩 | 🟡 planned | The local control surface (`wasl session start\|list\|attach\|send\|gate\|interrupt\|stop`) — human input (incl. native-gate approve/deny, §4.1) through the SAME scheduler; makes "live" demonstrable standalone. |

Dependency additions (acyclic): `adapter, proto ← wasl-runtime`; `wasl-runtime ← wasl-runtime-protocol`;
`wasl-runtime, node ← wasl-session`; `wasl-runtime-protocol ← mcp` (managed provider wiring, config toggle).
No existing edge changes; `AgentAdapter` (C1) is unmodified.

---

## 8. Future work — external runtime seam (OUT OF SCOPE for this plan)

**Explicitly out of scope. Designing/building this — and any specific external integration — is a separate
follow-on document, written only once phases 0–5 ship and are green.** Sketched here solely to show the
*shape* the current abstraction leaves room for, so nothing in phases 0–5 forecloses it.

The correct seam is **"same target contract, different provider implementation"** — an external supervisor
would implement a generic provider, e.g.:

```rust
/// FUTURE / NOT PART OF THIS PLAN. A provider that fronts a target OWNED and RUN by an external supervisor,
/// spoken to over a GENERIC transport (HTTP / JSON-RPC) using only GENERIC terms — `target_id`,
/// `agent_identity`, capability flags. NO consumer-specific field names, types, or assumptions. It returns a
/// `DeliveryTarget` like any other; the generic scheduler cannot tell the difference.
pub struct ExternalManagedRuntimeProvider { /* generic endpoint config only */ }
```

**Hard rule (locked, even for the future seam):** Wasl must **NEVER** be handed a raw process / PTY / OS
handle by an external party. Handle-passing creates ownership/ordering ambiguity, dueling stdin writers, and
invisible human-input races — the exact failure modes the §4 single-writer invariant exists to prevent. The
seam is a **contract boundary** (`DeliveryTarget` over a generic wire), never a handle boundary. An external
supervisor registers/owns the underlying session **on its side** and exposes it to Wasl only through the
generic target contract.

This mirrors Wasl's existing external-integration philosophy exactly (the `Surfacer` / audit `Sink` seams:
generic, consumer-named-by-nobody, optional). Nothing in this plan builds it.

---

## 9. Non-goals for this plan

- **No "Model B" terminal-attach — attaching to a session a human is personally driving in their own
  terminal / native TUI is OUT OF SCOPE and foreclosed.** It would require a shared raw input path
  (PTY / multi-writer stdin), which contradicts the locked single-writer invariant (§4) and the §8 hard rule.
  This feature delivers **Model A** only: a Wasl-owned session the human **observes** (`attach`) and
  **injects into via Wasl** (`send`). See §1.1.
- **No cross-restart durability of managed runtime state.** Managed targets, the FIFO queue, the event
  journal, the idempotency map, the transcript projection, and (non-`command`) delivery-certainty are
  in-memory only and are abandoned on a node restart (§4.0). The single documented exception is a `command`'s
  write-ahead-durable certainty/idempotency record in `wasl-audit` — a safety carve-out, not general
  persistence.
- **No external supervisor/consumer integration work of any kind.** (The seam sketch in §8 is a future,
  separate effort.)
- **No claim of `persistent_process` for adapters that do not genuinely have it.** codex/antigravity may
  honestly ship with only `persistent_conversation` — that is a correct outcome, not a shortfall.
- **No PTY-based screen-scraping as a primary mechanism.** Only ever an explicitly-weaker, clearly-labelled
  fallback (`structured_turns: false`), if ever — never the default and never a phase-4/5 target.
- **No automatic retry of a `command` with uncertain delivery outcome** (`OutcomeUnknown`) — it may have
  already caused a side effect.
- **No modification of the existing `AgentAdapter::deliver` contract** and **no behavior change to the
  ephemeral path** — which remains the default and permanent fallback.
