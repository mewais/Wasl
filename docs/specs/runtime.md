# R1 — `wasl-runtime` — generic managed-runtime core — spec (authoritative)

**Tier:** L1 (traits + pure state machine + fakes fully testable). **Deps:** `wasl-adapter` (the
`AgentAdapter` trait + `Delivery`/`DeliveryKind`/`AdapterError` — the ephemeral path wraps it),
`wasl-audit` (the `Sink`/`AuditEntry` observability + write-ahead durability seam, incl. `EntryKind::
RuntimeTurn`, `RuntimeTurnPhase`, and the re-exported `DeliveryCertainty`), `wasl-proto` (`AgentId`,
`RequestId`, `GateDecision`, and the L0-owned `DeliveryCertainty`), `serde`, `serde_json`, `async-trait`,
`tokio` (sync/time). Dev: `tokio` (macros, rt, `time::pause`), `serde_json`. No `unsafe`.

This is the **generic core** of the live / managed-runtime feature (design contract:
`docs/design/live-runtime-plan.md`; execution plan: `docs/design/live-runtime-impl-plan.md`). It owns the
type/trait surface every generic caller (dispatch, the R3 control surface, a future external provider) talks
to, the **per-target actor / state-machine**, the bounded event journal, the idempotency map, the transcript
projection, the delivery-certainty machine, causation cycle-rejection, and the two initial providers
(`EphemeralRuntimeProvider`, `WaslManagedRuntimeProvider`). **Per the plan's cycle-fix, the driver seam
traits (`InteractiveProtocol`/`InteractiveTransport`) and the internal `ProtocolEvent` enum live HERE (R1) as
the generic contract; only CONCRETE drivers live in R2** (`docs/specs/runtime-protocol.md`) — this keeps R2
depending on R1 and never the reverse.

**This spec is the frozen Phase-1 contract.** It is the input to the Phase-1 type-surface tester/coder and the
Phase-2 build. Everything below is object-safe, type-only, and normative. No scheduler code exists yet.

---

## ⚠️ CONTRACT STATUS — what is VERIFIED vs STATIC vs ASSUMED vs UNKNOWN

This crate is **substrate-independent** — its 100%-tested core is proven against an **in-repo fake structured
agent** (`FakeLineProtocol`, R2), never a real CLI. The rows below record where each externally-observable
FACT this contract relies on comes from. Tiers: **VERIFIED** = captured from the (re-run) Phase-0′ spike
against a real installed CLI (cite the specific `out/` artifact — a row is VERIFIED **only** for the exact
shape the probe actually exercised); **STATIC** = from `--help`/documented interface; **ASSUMED** = a design
choice or a wire shape the probes did NOT exercise (needs a follow-up capture before a driver commits an API
around it); **UNKNOWN** = a fact a real driver needs that the spike did NOT settle and that MUST be probed
before the Phase-4/5 driver commits an API around it.

The spike was **re-run** and `spikes/live-substrate/out/` now holds real, redacted, secret-free artifacts;
the gate PASSED (`out/capability-matrix.md`: claude=`verified-true`, opencode serve/acp=`verified-true`,
codex=`unsupported-by-documented-interface`). **The token-only probe prompts
("remember … / recall …") never invoked a tool and never tripped a native permission gate**, so any
tool-call or gate wire shape is **ASSUMED, not VERIFIED** — see the tool/gate rows below and §6/§7.4.

| Claim (generic-core-relevant) | Status |
|---|---|
| ≥1 CLI can be driven **same-process, multi-turn** (the go/no-go the whole core rests on) | ✅ VERIFIED — Phase-0′ gate PASSED (`out/capability-matrix.md`; `spikes/live-substrate/README.md` §"How to read the result"; claude `out/claude/verdict.json` `pass=true`, `pid_turn1==pid_turn2==338507`) |
| claude yields a machine-detectable **turn boundary** (`type=="result"` w/ `result`,`stop_reason`,`terminal_reason`) so `structured_turns` is real | ✅ VERIFIED — `out/claude/events.jsonl` lines 91,103 `type=="result"` per turn |
| A stable **native session id** exists to map a Wasl target onto (claude: client-supplied byte-stable `--session-id` UUID) | ✅ VERIFIED — `out/claude/argv.txt` (`--session-id 405bb438-…`) + `out/claude/verdict.json` `native_session_id`; the same UUID recurs on every `system/init` in `out/claude/events.jsonl` |
| claude `system`/`init` = session-ready + carries top-level `session_id` | ✅ VERIFIED — `out/claude/events.jsonl` lines 1,92 (`type=="system"`,`subtype=="init"`,`session_id`) |
| opencode `acp` yields **real streaming deltas** (`session/update` `agent_message_chunk`) → `streaming_output` achievable | ✅ VERIFIED — `out/opencode/acp/messages.jsonl` lines 117-131,162-176 (`agent_message_chunk`) |
| opencode `acp` `initialize`→`session/new(sessionId)`→`session/prompt`→JSON-RPC `result` w/ `stopReason` turn boundary | ✅ VERIFIED — `out/opencode/acp/messages.jsonl` lines 1-35 (`initialize`), 36-76 (`session/new` `sessionId`), 148-161,193-205 (`result` `stopReason`) |
| opencode `serve` HTTP turn is **synchronous, no deltas** (`info.finish=="stop"` + trailing `step-finish`) | ✅ VERIFIED — `out/opencode/serve/turn_2_message.txt` (`info.finish=="stop"`, `step-finish` `reason:"stop"`) |
| the `sessionID` (serve, uppercase-D) vs `sessionId` (acp, camelCase) native-id casing divergence | ✅ VERIFIED — `out/opencode/serve/turn_*_message.txt` (`"sessionID":"ses_…"`) vs `out/opencode/acp/messages.jsonl` line 40 (`"sessionId":"ses_…"`) |
| claude `tool_use`/`tool_result` content-block shapes (→ `ToolStarted`/`ToolFinished` mapping) | 🅰 ASSUMED — **NOT captured**: the token-only prompt invoked no tool; `out/claude/events.jsonl` contains zero `tool_use`/`tool_result` blocks (`permission_denials: []`). Needs a follow-up capture that exercises a tool before a driver commits the mapping |
| opencode `acp`/`serve` tool-activity + permission/native-gate wire shapes (→ `ToolStarted`/`ToolFinished`/`NativeGateRequested`) | 🅰 ASSUMED — **NOT captured**: `out/opencode/acp/messages.jsonl` shows only `session/update` (`available_commands_update`/`agent_message_chunk`/`usage_update`), no tool call and no permission request. Needs a follow-up capture that exercises a tool/gate |
| ephemeral spawn-per-turn adapters retain conversation via resume (`persistent_conversation`) | ✅ VERIFIED (elsewhere) — `docs/specs/adapter-{claude,codex,opencode}.md` live suites |
| **Interruptibility** (`cancel_turn` actually aborts an in-flight turn on a real substrate) | ⛔ **UNKNOWN** — UNTESTED on every substrate (`out/capability-matrix.md` `interruptible=unknown` for all rows; Notes: "not probed in Phase 0'"); the core's cancel state-machine is fully specified & fake-tested, but no real driver may claim `interruptible: true` until a **Phase-1-follow-up interrupt capture** confirms it |
| **Streaming granularity** for claude + opencode-`serve` (whether/when partial deltas arrive vs whole blocks) | ⛔ **UNKNOWN** — claude `assistant` events carried WHOLE content blocks, NO token deltas observed (`out/claude/events.jsonl` lines 89-90,101-102); opencode-`serve` returned a final message. Only opencode-`acp` showed deltas. A driver on claude/serve MUST report `streaming_output: false` (or run a **Phase-1-follow-up streaming capture**) — do NOT assume deltas |
| Cross-node-restart durability of live state | 🅰 ASSUMED-NONE — in-memory only by contract (§State durability); the one exception is command write-ahead durability (below) |

**No `candidate` cells.** A capability a driver has not proven is `false`, never rounded up. The two UNKNOWN
rows (interruptibility; streaming granularity for claude/serve) AND the two ASSUMED tool/gate rows are carried
forward verbatim into `runtime-protocol.md`'s per-driver tables — a driver that would set `interruptible`/
`streaming_output` true, or that commits a `Tool*`/`NativeGateRequested` mapping around a specific captured
shape, MUST cite a **Phase-1-follow-up capture**, or the flag stays false and the mapping stays ASSUMED.

---

## Single-writer invariant (locked — Model A)

**There is exactly ONE physical writer to any managed session: the per-target actor** (below). All input —
A2A-originated turns (`TurnSource::Agent`) AND local/human input (`TurnSource::LocalHuman`, arriving via R3's
`send`) AND native-gate resolutions (R3's `gate approve|deny`) — funnels through that one actor via
`submit_turn` (or the actor's internal gate-resolution command). **Nothing else may hold or write the
underlying stdin/socket handle, ever.** The two *logical* turn sources are serialized by the single actor;
there is never a second raw writer.

This is **Model A** (design §1.1): Wasl owns the session; a human **observes** via `attach`
(`events(after_seq)`, read-only) and **injects** via `send` (a `TurnRequest{source: LocalHuman}` through the
same `submit_turn`). **Model B (a human driving a native TUI as a second stdin writer) is FORECLOSED** — it
requires a shared raw handle, which this invariant forbids (design §1.1, §9). The invariant is what makes
concurrent agent + human input on one live session safe, and it is the reason `InteractiveTransport` is owned
solely by the actor.

---

## State durability: in-memory only (one write-ahead exception) — LOCKS §4-decision 7

**All authoritative runtime state for a managed target is IN-MEMORY only and is ABANDONED on a node
restart.** This is Wasl's **first subsystem whose source of truth is NOT the append-only audit log** — stated
loudly so no reader assumes the audit log can rebuild a live session. In-memory (lost on restart): the target
registry, the active turn + FIFO queue, the event journal + sequence numbers, the idempotency-key map, the
receipt cache, the transcript projection, gate state, and per-turn delivery-certainty for non-`command`
deliveries.

**THE ONE EXCEPTION (safety carve-out, not general durability):** for a **`command`** delivery, its
delivery-certainty + idempotency record is **write-ahead durable in `wasl-audit` BEFORE injection** (via the
injected `Sink`, below). A crash after injection therefore cannot erase the evidence needed to detect an
`OutcomeUnknown` command and prevent an unsafe blind retry (§Delivery certainty). Everything else is
in-memory. `RuntimeProvider::target_for`'s "get-or-create/recover" and the `resumable` capability refer ONLY
to re-attaching to a CLI-native session **within one node-process lifetime**, never to surviving a Wasl
restart.

---

## 1. Identifiers and small value types

```rust
/// A Wasl-side handle for a target (NOT a CLI-native id). Opaque, stable for the target's lifetime.
#[derive(Debug, Clone, PartialEq, Eq, Hash, Serialize, Deserialize)]
pub struct TargetId(pub String);

/// The get-or-create key for a managed target. LOCKS §4-decision 6: TargetKey = context_id.
/// One live target per conversation context. (Admission control below bounds explosion.)
#[derive(Debug, Clone, PartialEq, Eq, Hash, Serialize, Deserialize)]
pub struct TargetKey(pub String);   // == the Delivery.context_id

/// A CLI-native session id learned from a driver (claude UUID / opencode `ses_…` / acp `sessionId`).
/// Opaque to the generic core; only R2 drivers mint/parse it. Kept for provenance + resume WITHIN a
/// node-process lifetime.
#[derive(Debug, Clone, PartialEq, Eq, Hash, Serialize, Deserialize)]
pub struct NativeSessionId(pub String);

/// A Wasl-side per-turn id, unique for the target's lifetime. Correlate by this, NEVER by EventSeq.
#[derive(Debug, Clone, PartialEq, Eq, Hash, Serialize, Deserialize)]
pub struct TurnId(pub String);

/// Per-target monotonic event-journal sequence (LOCKS §6-open-question "EventSeq scope": PER-TARGET
/// monotonic, starts at 1, gap-free while retained). NOT node-global.
#[derive(Debug, Clone, Copy, PartialEq, Eq, PartialOrd, Ord, Hash, Serialize, Deserialize)]
pub struct EventSeq(pub u64);

/// A caller-supplied idempotency key scoped under a TargetKey. Deduped OUTSIDE the mailbox (§4-decision 2).
#[derive(Debug, Clone, PartialEq, Eq, Hash, Serialize, Deserialize)]
pub struct IdempotencyKey(pub String);

/// Injected monotonic time, so ALL deadlines are deterministic under `tokio::time::pause`
/// (LOCKS impl-plan Phase-1 "injected monotonic clock/timer seam"). Never `SystemTime` in the core.
pub trait RuntimeClock: Send + Sync { fn now(&self) -> std::time::Instant; }
pub struct MonotonicClock;   // Instant::now(); impl Default
/// Wire/audit timestamps (informational only; the ACTUAL waits use RuntimeClock durations).
pub type Timestamp = String; // RFC 3339 UTC millis, rendered at emit time
```

`EventSeq` is documented **per-target** so a client re-subscribing to a target reasons about one gap-free
sequence; correlation across turns is by `TurnId`, which `TurnCompleted` ALWAYS carries (§4-decision 1). A
`TurnReceipt` carries a `TurnId` and a `first_seq: EventSeq`, so a subscriber can `events(first_seq - 1)`
and be guaranteed to see the turn's first event (atomic subscribe-then-admit).

---

## 2. `RuntimeCapabilities` — 8 independent honest booleans (design §2)

```rust
/// What a target can HONESTLY do. All 8 fields are INDEPENDENT — a caller MUST NOT assume any implies
/// another. A driver reports the exact proven subset; a false is honest, a rounded-up true is a bug.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
pub struct RuntimeCapabilities {
    pub persistent_process: bool,        // same OS process serves multiple turns (no per-turn spawn)
    pub persistent_conversation: bool,   // context persists across turns (possibly via resume)
    pub structured_turns: bool,          // reliable turn START/END boundaries (not screen-scrape)
    pub streaming_output: bool,          // assistant output streamable as deltas (⛔ UNKNOWN for claude/serve)
    pub attachable: bool,                // a viewer can observe the live session
    pub interruptible: bool,             // an in-flight turn can be cancelled cleanly (⛔ UNKNOWN — untested)
    pub structured_provenance: bool,     // turn boundaries + native ids give machine-usable correlation ids
    pub resumable: bool,                 // a dropped session can be resumed with context (within one node life)
}
```

**Locked expected reports (per concrete driver — a driver setting all-false to pass conformance is itself a
FAIL; impl-plan Phase-1 "fixed expected-capability report per concrete driver"):** the exact per-driver
matrix is normative in `runtime-protocol.md`. The generic core NEVER hard-codes a capability; every scheduler
decision and every conformance assertion is **gated on the specific capability it needs** (design §5.2).

---

## 3. The public `DeliveryTarget` trait — the ONLY seam generic callers address

```rust
/// A live-or-ephemeral thing Wasl can deliver turns into. Generic dispatch/routing/R3 knows ONLY this trait;
/// it never sees a claude/ACP/process detail (that lives behind InteractiveProtocol, §6). Object-safe.
#[async_trait::async_trait]
pub trait DeliveryTarget: Send + Sync {
    /// Stable Wasl-side handle (not a CLI-native id). Constant for the target's lifetime.
    fn target_id(&self) -> &TargetId;

    /// What this target can honestly do (§2). Constant for the target's lifetime.
    fn capabilities(&self) -> RuntimeCapabilities;

    /// Best-effort state snapshot. ADVISORY ONLY — never gate admission on it; it can be stale the instant
    /// it returns. The authoritative admission decision is `submit_turn` (§4.2).
    async fn status(&self) -> TargetStatus;

    /// THE ATOMIC ADMISSION DECISION. Returns a receipt describing Started / Queued(position) / Rejected
    /// WITHOUT waiting for the reply (the reply arrives via `events`). MUST NOT race with `status()`.
    /// `submit_turn` sends a command + a receipt oneshot into the bounded mailbox and awaits ONLY the
    /// receipt (§4-decision 5). A full mailbox MUST NOT spuriously reject an otherwise-idle target.
    async fn submit_turn(&self, req: TurnRequest, busy_policy: BusyPolicy) -> TurnReceipt;

    /// Replayable, sequenced event stream. `after_seq` lets a (re)subscriber resume with no gap and no
    /// duplicate (§4-decision 1). Multiple observers hold INDEPENDENT cursors. Yields TurnEvent (§5), the
    /// PUBLIC vocabulary — never the internal ProtocolEvent.
    async fn events(&self, after_seq: EventSeq) -> TurnEventStream;

    /// Request cancellation of an in-flight turn. Best-effort; only meaningful if `capabilities().
    /// interruptible`. Reflected in events + status. Cancelling a wrong/completed/queued turn is defined
    /// in the transition matrix (§7), never a panic.
    async fn cancel_turn(&self, turn_id: TurnId, reason: CancelReason) -> Result<(), RuntimeError>;

    /// Shut the target down. Drain finishes the active turn then stops; Immediate stops now (§7).
    async fn shutdown(&self, mode: ShutdownMode) -> Result<(), RuntimeError>;
}

/// `events` returns this. Concretely a `Pin<Box<dyn Stream<Item = SeqEvent> + Send>>`; each item pairs the
/// per-target EventSeq with a public TurnEvent, or a control item on truncation (§4-decision 1).
pub type TurnEventStream = std::pin::Pin<Box<dyn futures::Stream<Item = StreamItem> + Send>>;

#[derive(Debug, Clone, PartialEq, Eq)]
pub enum StreamItem {
    /// A journalled event at its sequence.
    Event { seq: EventSeq, event: TurnEvent },
    /// `after_seq` was below the retained floor: the client MUST re-`snapshot()` then re-subscribe
    /// from `latest`. Carries the retained window so the client knows exactly what it lost.
    Truncated { earliest: EventSeq, latest: EventSeq },
}
```

```rust
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum TurnReceipt {
    /// Admitted and now the active turn. `first_seq` = the EventSeq of this turn's first journalled event,
    /// enabling atomic subscribe-then-admit (subscribe at first_seq-1, never miss the first event).
    Started { turn_id: TurnId, first_seq: EventSeq },
    /// Admitted into the FIFO queue at `position` (0 = next after the active turn).
    Queued { turn_id: TurnId, position: usize },
    /// Refused: target busy and `busy_policy` said not to queue (a BUSY condition).
    RejectedBusy { active_turn: TurnId },
    /// Refused: target Offline/Faulted/Stopped, OR admission control refused (not a busy condition).
    RejectedUnavailable { status: TargetStatus },
    /// A resend of an already-seen (TargetKey, IdempotencyKey): the ORIGINAL receipt is returned verbatim,
    /// NO second injection occurs (§4-decision 2). Wraps the original outcome.
    Duplicate { original: Box<TurnReceipt> },
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum ShutdownMode { Drain, Immediate }

#[derive(Debug, Clone, PartialEq, Eq)]
pub enum CancelReason { OperatorRequest, A2aCancel, Superseded, Shutdown }
```

`RejectedUnavailable` is for **genuine** unavailability (Offline/Faulted/Stopped) or an admission-control
refusal — **never** for transient mailbox pressure on an idle target (§4-decision 5).

### 3.1 `TurnRequest` — the admission payload

```rust
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct TurnRequest {
    /// The delivery payload (wasl-adapter's Delivery: context_id, from, kind, text, data).
    pub delivery: wasl_adapter::Delivery,
    /// Who originated this turn — serialized by the single actor (§Single-writer invariant).
    pub source: TurnSource,
    /// Optional dedupe key scoped under the target's TargetKey (§4-decision 2). None ⇒ no dedupe.
    pub idempotency_key: Option<IdempotencyKey>,
    /// The synchronous/queued call path this turn is on (cycle rejection, §8). Root turns carry hop 0.
    pub causation: Causation,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub enum TurnSource {
    Agent { from: wasl_proto::AgentId },
    LocalHuman,
}
```

---

## 4. The per-target actor / state machine (Wasl-owned)

One actor task per managed target. It is the single writer (§Single-writer invariant) and the single owner
of: the active turn + FIFO queue, the live `InteractiveTransport`, the per-target `EventSeq`, the
idempotency-key map, the receipt cache, the transcript projection, gate state, and per-turn certainty. It
receives commands over a **bounded mpsc mailbox** and drives the transport, translating each
`ProtocolEvent` (internal, §6) into a public `TurnEvent` (§5) it journals and broadcasts.

### 4.1 `TargetStatus` and phases (design §4.1)

```rust
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum TargetStatus {
    Offline,
    Starting,
    Idle { since: Timestamp },
    Busy { active_turn: TurnId, source: TurnSource, phase: BusyPhase, since: Timestamp, queued: usize },
    /// Blocked on a NATIVE CLI permission gate. BUSY, not idle — cannot take a new turn. MUST be resolvable
    /// (§9). Distinct from and NOT merged with Wasl's send-side gate.
    AwaitingGate { active_turn: TurnId, detail: NativeGateDetail, since: Timestamp, queued: usize },
    Interrupting { active_turn: TurnId, since: Timestamp },
    Draining { active_turn: TurnId, since: Timestamp },
    Faulted { error: String, recoverable: bool },
    Stopped,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum BusyPhase {
    Dispatching,     // prompt handed to transport, awaiting TurnAccepted
    Generating,      // assistant is producing output
    RunningTool,     // a tool is executing
    WaitingForTool,  // awaiting a tool result
    WaitingForInput, // blocked awaiting some input
    Completing,      // finalizing the turn
}

/// Opaque, secret-free description of a native CLI gate (its prompt text/preview). Model-untrusted.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct NativeGateDetail { pub summary: String, pub preview: String }
```

**`AwaitingGate` counts as busy** — reporting it idle would let the scheduler inject into a wedged session.

### 4.2 `BusyPolicy` — per-delivery, admission-atomic (design §4.2)

```rust
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum BusyPolicy {
    /// Queue behind the active turn up to a bounded wait; expire if not started in time.
    Queue { max_wait: std::time::Duration },
    /// Refuse immediately if busy (→ RejectedBusy).
    Reject,
    /// Queue ONLY if the incoming turn shares a causation root with the active turn (§8); else reject.
    QueueRelated { max_wait: std::time::Duration },
}
```

**Locked defaults + units + override precedence (LOCKS §6-open-question "BusyPolicy defaults+units+override
precedence"):** durations are `std::time::Duration` (millisecond precision on any wire). Defaults per
`DeliveryKind`: `Consult → Queue{max_wait: 120_000ms}`; `Command → Reject` (a delayed command may cause a
wrong-time side effect — §Delivery certainty); `Message → Queue{max_wait: 30_000ms}` with expiry. Precedence:
an explicit per-call `BusyPolicy` OVERRIDES the kind default; the kind default OVERRIDES a global config
default. `submit_turn` evaluates the policy against the **authoritative internal actor state**, never against
a prior `status()` — this is why `status()` is advisory-only and admission never races.

### 4.3 Event journal — bounded ring + snapshot + atomic replay-to-live handoff (LOCKS §4-decision 1)

The actor keeps a **bounded ring** of `(EventSeq, TurnEvent)`, per-target monotonic from `EventSeq(1)`. It
exposes internally:

- `snapshot() -> (TargetStatus, EventSeq)` — the current status and `last_seq` (the highest journalled seq),
  captured atomically under the actor lock.
- `events(after_seq)` semantics (public via `DeliveryTarget::events`): for `after_seq >= floor` (the lowest
  retained seq minus one), deliver **exactly-once, in ascending seq, with NO gap**, then continue live. For
  `after_seq < floor` (the client fell below the retained window), emit `StreamItem::Truncated{earliest,
  latest}` and STOP — the client re-`snapshot()`s and re-subscribes from `latest`.
- **Atomic replay-to-live handoff (the exact, race-free ordering — o3/gpt must-fix):** the live path is a
  `tokio::sync::broadcast` per target; the retained tail is the bounded ring. Subscribing performs these
  steps in THIS order so no event is missed and none is double-emitted:

  1. **Take the actor lock ONCE** (the whole subscribe critical section is under one acquisition, so the ring
     and `current_seq` cannot advance mid-subscribe).
  2. **Subscribe a live broadcast receiver FIRST** — call `broadcast::Sender::subscribe()` while holding the
     lock, so every event published *after this instant* is captured by this receiver (nothing published
     between subscribe and tail-copy can slip through the crack).
  3. **THEN copy the retained journal tail** `after_seq+1..=current_seq` out of the ring (a `Vec` snapshot),
     where `current_seq` is read under the same lock as the subscribe in step 2.
  4. **Release the lock.**
  5. **Yield the copied tail first** (ascending seq), THEN drain the live receiver — and **de-duplicate the
     boundary** by dropping any live item whose `seq <= last_tail_seq` (the highest seq already emitted from
     the tail). Because step 2 preceded step 3, the live receiver's earliest item has `seq <= current_seq+1`,
     so the ranges overlap-or-abut and the drop-`<=` rule closes the seam with neither gap nor duplicate.

  **Journal-ring floor vs broadcast lag (two independent truncation sources):** if `after_seq < floor` at
  step 3 (below the retained ring), emit `StreamItem::Truncated{earliest, latest}` and STOP (client
  re-`snapshot()`s). Separately, the `tokio::broadcast` channel has its OWN bounded capacity: if the live
  receiver drained in step 5 lags and yields `RecvError::Lagged(n)`, that ALSO surfaces as
  `StreamItem::Truncated{earliest, latest}` (`earliest`/`latest` = the current retained ring window) — the
  client re-snapshots and re-subscribes from `latest`, identical recovery to a below-floor cursor. Both
  truncation sources therefore reduce to one client discipline.

This handoff is exercised by the **Phase-2 (2A) multi-observer/replay tests** (independent cursors, exactly-
once/no-gap across the tail→live boundary, and both truncation paths). `TurnCompleted` ALWAYS carries its
`TurnId`, so a client correlates a completion by id even across a truncation gap (never by seq).

### 4.4 Idempotency — deduped OUTSIDE the mailbox (LOCKS §4-decision 2)

`(TargetKey, IdempotencyKey)` is deduped by a **shared per-key map with a per-key lock, consulted BEFORE the
command enters the mailbox**, so two concurrent same-key submits can NEVER both enqueue. The first submit
records its `TurnReceipt` under the key; any later same-key submit returns `TurnReceipt::Duplicate{ original }`
carrying the recorded receipt — **no second injection**. The map is bounded (TTL + cap, §11). For a
`command`, the idempotency record is part of the write-ahead-durable audit entry (§State durability, §Delivery
certainty) so a resend after a crash is still detected.

### 4.5 Transcript projection — bounded, byte-bounded (LOCKS §4-decision 4)

The actor maintains a **byte-bounded** summary an observer sees: the last-N completed replies plus the current
turn's accumulated deltas, capped by BOTH a count AND a total byte budget (not count alone) so a days-long
session cannot grow unbounded (§11). It is a projection over the journal, never a second source of truth.

### 4.6 Actor mailbox — bounded, receipt-only await (LOCKS §4-decision 5)

The mailbox is a **bounded `tokio::mpsc`**. `submit_turn` sends `(Command, oneshot<TurnReceipt>)` and awaits
ONLY the receipt oneshot. **Mailbox-full MUST NOT spuriously reject an idle target:** on transient mailbox
pressure the caller either awaits space up to a bounded timeout, or the pressure is reflected as
queue-pressure in `status()` — `RejectedUnavailable` is reserved for genuine unavailability (§3). This is the
o3 back-pressure must-fix.

### 4.7 Two §4 locks DEFERRED to Phase-3 node-config wiring (D8, D10) — NOT dropped

Two of the Phase-1 LOCKS-list decisions are node-wiring / config concerns, not this generic core's type
surface. They are recorded here so they are **not read as silently dropped** from the §4 locks — both are
**DEFERRED to the Phase-3 node-config wiring step** (the same delta that carries §11's config bounds and §10's
A2A-sharing threat model), NOT to this crate:

- **D8 — `wasl-mcp` wiring / `RuntimeKind` toggle (DEFERRED to Phase-3).** The `runtime = "ephemeral" |
  "managed"` toggle, the **exhaustive `match` on `RuntimeKind`** (ephemeral → today's `AdapterExecutor`
  **literally unchanged**; managed → the runtime-backed executor), and the `WASL_RUNTIME` env override are
  Phase-3 node-config wiring — they select between the two providers this crate already defines (§10) but add
  no type here. (The control-plane half, D8b, is its own frozen spec now: `runtime-control-api.md`.) This
  core neither reads config nor performs the toggle; it only exposes `EphemeralRuntimeProvider` and
  `WaslManagedRuntimeProvider` for that wiring to choose between.
- **D10 — phase-aware liveness timeouts (DEFERRED to Phase-3).** The startup / acceptance / no-progress /
  gate / idle / shutdown-grace timeouts — **phase-aware** (a long `RunningTool` phase is NOT a dead session,
  §4.1 `BusyPhase`) with heartbeat where the protocol supports it — are config-tunable liveness bounds with
  locked defaults, wired in the Phase-3 `node-config` delta exactly like §11's other bounds (idle-eviction
  already appears in §10/§11). This core validates against the named fields but does NOT own their defaults or
  wiring; deferring keeps the timeout defaults in one config surface rather than hard-coded per crate.

---

## 5. The PUBLIC `TurnEvent` vocabulary (distinct from internal `ProtocolEvent`) — LOCKS impl-plan Phase-1 gap

`TurnEvent` is what `DeliveryTarget::events` and the R3 control plane expose. It is a **generic, driver-free**
public contract — the internal `ProtocolEvent` (§6) is translated INTO it by the actor and **never leaks
onto any public surface** (LOCKS §6-open-question "ProtocolEvent stays internal while only TurnEvent is
public/versioned": RESOLVED — recommended option adopted).

```rust
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(tag = "type", rename_all = "snake_case")]
pub enum TurnEvent {
    /// The target is up and idle (carries the native session id if the driver has one). First-turn only.
    SessionReady { native: Option<NativeSessionId> },
    /// A turn was admitted and started (mirrors Started receipt for late/replay subscribers).
    TurnAccepted { turn_id: TurnId, source: TurnSource },
    /// A turn was admitted into the queue.
    TurnQueued { turn_id: TurnId, position: usize },
    /// An incremental chunk of assistant output. On a non-streaming substrate a single final chunk is
    /// emitted (⛔ streaming granularity UNKNOWN for claude/serve — a whole-blob substrate emits ONE).
    AssistantDelta { turn_id: TurnId, text: String },
    /// A tool call began / finished (name only; args/results are audit provenance, not model-trusted text).
    ToolStarted { turn_id: TurnId, name: String },
    ToolFinished { turn_id: TurnId, name: String, ok: bool },
    /// The session entered a native gate (AwaitingGate). Resolvable via §9.
    NativeGateRequested { turn_id: TurnId, detail: NativeGateDetail },
    /// The native gate was resolved (how, by whom).
    NativeGateResolved { turn_id: TurnId, decision: wasl_proto::GateDecision },
    /// The turn completed successfully. ALWAYS carries TurnId (§4-decision 1) and its final certainty
    /// (the L0-owned `wasl_proto::DeliveryCertainty`, re-exported by `wasl-audit`; §8.1).
    TurnCompleted { turn_id: TurnId, reply: String, certainty: wasl_proto::DeliveryCertainty },
    /// The turn failed inside the session (distinct from the process dying).
    TurnFailed { turn_id: TurnId, error: String, certainty: wasl_proto::DeliveryCertainty },
    /// A turn was cancelled (interrupt result).
    TurnCancelled { turn_id: TurnId, reason: CancelReason },
    /// A turn expired in the queue before starting (BusyPolicy max_wait elapsed).
    TurnExpired { turn_id: TurnId },
    /// The target status changed (observability; carries the new status).
    StatusChanged { status: TargetStatus },
    /// The underlying process/connection exited. Terminal for the current transport.
    ProcessExited { code: Option<i32> },
}
```

**Ordering / replay / truncation guarantees (normative):**

1. On ONE subscription, events arrive in strictly ascending `EventSeq`, gap-free, exactly-once, for any
   `after_seq >= floor`.
2. Per turn, `TurnAccepted` (or `TurnQueued` then `TurnAccepted`) precedes any `AssistantDelta`/`Tool*`/gate
   event for that `turn_id`, which precede exactly one terminal event for that turn
   (`TurnCompleted`/`TurnFailed`/`TurnCancelled`/`TurnExpired`). **Queued-turn admission vs activation timing
   (relative to the prior turn's terminal event):** a turn admitted while the target is busy emits its public
   **`TurnQueued{position}` at ADMISSION** (the moment `submit_turn` returns `Queued`), and its receipt is
   `TurnReceipt::Queued` — which carries NO `first_seq`, because the turn has not started and owns no journal
   events yet. It emits **`TurnAccepted{turn_id}` only later, when it becomes the ACTIVE turn** — i.e. strictly
   AFTER the prior turn's single terminal event (`pop Q → next turn`, §7.4) — and it is at THAT activation
   that the turn's first journalled event is minted. So this turn's **`first_seq` is the `EventSeq` of its
   `TurnAccepted`**, always `> ` the prior turn's terminal-event seq (never interleaved, single-writer
   invariant). A subscriber that only holds a `Queued` receipt therefore correlates the eventual start **by
   `turn_id`** on the stream; a subscriber that wants atomic subscribe-then-admit for a turn that went
   `Started` (not `Queued`) uses `Started.first_seq` per §1.
3. `TurnCompleted` always carries `TurnId`; correlate by id, never by seq.
4. Below-floor resubscription yields `StreamItem::Truncated{earliest, latest}` and the client re-snapshots.
5. Multiple observers each get identical `(seq, event)` pairs with independent cursors (multi-observer).
6. The public wire (R4, `runtime-control-api.md`) exposes ONLY `TurnEvent` + `TargetStatus` + `TurnReceipt`;
   `ProtocolEvent` is never serialized onto it.

---

## 6. The driver seam — `InteractiveProtocol` / `InteractiveTransport` / `ProtocolEvent` (LIVE IN R1)

Used **only** by `WaslManagedRuntimeProvider`. This is the only place CLI-specific knowledge is *allowed* —
but the TRAITS and the internal `ProtocolEvent` enum are the **generic contract** and live in R1 (the plan's
cycle-fix: R2 depends on R1, never the reverse). Concrete drivers (`FakeLineProtocol`, `ClaudeStreamProtocol`,
`OpenCodeAcpProtocol`) are R2.

```rust
#[async_trait::async_trait]
pub trait InteractiveProtocol: Send + Sync {
    fn capabilities(&self) -> RuntimeCapabilities;
    /// Start a fresh live session.
    async fn spawn(&self, opts: &SpawnOptions) -> Result<Box<dyn InteractiveTransport>, RuntimeError>;
    /// Re-attach to an existing native session (only if `capabilities().resumable`; within one node life).
    async fn resume(&self, native: &NativeSessionId, opts: &SpawnOptions)
        -> Result<Box<dyn InteractiveTransport>, RuntimeError>;
}

/// A live connection to ONE running agent session. The actor is the SOLE holder — nothing else may call
/// `send_turn` or touch the underlying handle (§Single-writer invariant).
#[async_trait::async_trait]
pub trait InteractiveTransport: Send {
    /// Push one turn's prompt. Returns once the CLI ACCEPTS the turn (completion arrives via `next_event`).
    async fn send_turn(&mut self, prompt: &ComposedPrompt) -> Result<(), RuntimeError>;
    /// Pull the next NORMALIZED protocol event. Ends with `ProcessExited` when the session dies.
    async fn next_event(&mut self) -> Option<ProtocolEvent>;
    /// Best-effort interrupt of the active turn (only if `capabilities().interruptible`).
    /// ⛔ interruptibility is UNKNOWN on every real substrate (contract-status table); the fake proves the
    /// state machine, a real driver must NOT claim it until a follow-up capture confirms it.
    async fn interrupt(&mut self) -> Result<(), RuntimeError>;
}

/// The NORMALIZED event vocabulary every driver maps its native wire onto. INTERNAL: consumed by the actor,
/// translated into the PUBLIC TurnEvent (§5), NEVER exposed on a public surface. Driver-specific shapes
/// (claude stream-json objects, ACP JSON-RPC notifications, opencode JSONL) are translated in R2 and never
/// leak upward.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum ProtocolEvent {
    SessionReady { native: Option<NativeSessionId> },
    TurnAccepted { native_turn: Option<String> },
    AssistantDelta { text: String },
    ToolStarted { name: String },
    ToolFinished { name: String, ok: bool },
    NativeGateRequested { detail: NativeGateDetail },
    TurnCompleted { reply: String },
    TurnFailed { error: String },
    ProcessExited { code: Option<i32> },
}

/// Options a provider passes a driver at spawn/resume (bin, model, isolated env, target key).
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct SpawnOptions { /* private: bin, model, env pairs, target_key, native (for resume) */ }

/// The prompt the actor hands the transport — the model-visible provenance wrapper (design §4.4) applied,
/// mirroring wasl-adapter's compose_prompt header ("[Consult from {from}]\n\n{text}"). CONTEXT, not a
/// security control (an LLM can be argued out of any wrapper — the real controls are the CLI's own
/// permission config + deployment isolation).
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct ComposedPrompt(pub String);
```

**2C bidirectional genericity note (impl-plan Phase 2C):** the fake extends `ProtocolEvent` usage to a
mid-turn callback (a `ToolStarted` that blocks the turn until the actor sends a value back). If `ProtocolEvent`
+ the single-writer actor carry that WITHOUT a new variant or a second writer, genericity is real. If they
cannot, the spec's claim is scoped to **unidirectional turn loops** and the limitation is documented in
`runtime-protocol.md` — the core does NOT silently grow a second writer.

---

## 7. Full state × command transition matrix (LOCKS impl-plan Phase-1 "full state×command transition matrix")

Every `TargetStatus` × every `DeliveryTarget` method + every `ProtocolEvent`, defining resulting status,
emitted `TurnEvent`(s), certainty change, and queue disposition. This is normative and exhaustive; a Phase-2
match on any (status, input) pair not listed is a bug.

**Legend:** `active` = the current active turn; `Q` = FIFO queue; `→` = resulting status; events are public
`TurnEvent`s.

### 7.1 `submit_turn(req, policy)` by current status

| Status | Result |
|---|---|
| `Offline`/`Faulted`/`Stopped` | `RejectedUnavailable{status}`; no events; certainty `NotInjected`; Q unchanged |
| admission-control refuses (§10) | `RejectedUnavailable{status}`; no events; `NotInjected` |
| dedupe hit (§4.4) | `Duplicate{original}`; no events; no injection |
| `Idle` | `Started{turn_id, first_seq}`; → `Busy{Dispatching}`; emit `TurnAccepted`, `StatusChanged`; certainty `NotInjected`→`Injected` at transport `send_turn` |
| `Starting` | policy `Queue`/`QueueRelated`(related): `Queued{pos}`; emit `TurnQueued`; else `RejectedBusy{active}` |
| `Busy`/`AwaitingGate` + policy `Queue` | `Queued{pos}` (pos = Q length); emit `TurnQueued`; Q grows (bounded, §11 — over cap ⇒ `RejectedBusy`) |
| `Busy`/`AwaitingGate` + policy `Reject` | `RejectedBusy{active}`; no events |
| `Busy`/`AwaitingGate` + policy `QueueRelated` | related (shared causation root, §8) ⇒ `Queued`; else `RejectedBusy{active}` |
| `Interrupting`/`Draining` | policy-dependent as `Busy`, but a NEW turn during `Draining` is `RejectedBusy` (drain admits nothing new) |

### 7.2 `cancel_turn(turn_id, reason)`

| Case | Result |
|---|---|
| `turn_id` is the active turn AND `!interruptible` | `Err(RuntimeError::Unsupported)`; status unchanged |
| active AND `interruptible` | → `Interrupting`; drive `transport.interrupt()`; on ack emit `TurnCancelled`; → `Idle` (or next queued) |
| `turn_id` is a QUEUED turn | remove from Q; emit `TurnCancelled`; status unchanged (queue disposition: dropped) |
| `turn_id` already completed/unknown | `Ok(())` no-op (idempotent) OR `Err(RuntimeError::NotFound)` — LOCKED as **`Ok(())` no-op** (cancelling a finished turn is benign) |
| status `Offline`/`Stopped` | `Err(RuntimeError::Unavailable)` |

### 7.3 `shutdown(mode)` in every phase

| Mode × status | Result |
|---|---|
| `Immediate`, any | → `Draining`(0) then `Stopped`; active turn (if any) ⇒ `TurnCancelled{Shutdown}`; queued ⇒ `TurnExpired`; kill transport; emit `ProcessExited`, `StatusChanged`; certainty of an injected-but-unfinished `command` ⇒ `OutcomeUnknown` |
| `Drain`, `Idle` | → `Stopped` immediately; emit `ProcessExited` |
| `Drain`, `Busy`/`AwaitingGate` | → `Draining{active}`; finish active turn (its terminal event), REJECT new submits (`RejectedBusy`), then drop queue as `TurnExpired`, then `Stopped` |
| `Drain`, `Interrupting` | complete the interrupt, then `Stopped` |
| `Drain`, `Draining` | no-op (already draining) |

### 7.4 Inbound `ProtocolEvent` (from `transport.next_event`) by phase

| ProtocolEvent | In-phase result |
|---|---|
| `SessionReady{native}` | record `native`; if `Starting` → `Idle`; emit `SessionReady`, `StatusChanged` |
| `TurnAccepted` | `Busy{Dispatching}` → `Busy{Generating}`; (public `TurnAccepted` already emitted at admission) |
| `AssistantDelta{text}` | `Busy` → `Busy{Generating}`; emit `AssistantDelta`; append to transcript (byte-bounded) |
| `ToolStarted{name}` | → `Busy{RunningTool}`; emit `ToolStarted` |
| `ToolFinished{name,ok}` | → `Busy{Generating}`; emit `ToolFinished` |
| `NativeGateRequested{detail}` | → `AwaitingGate{detail}`; emit `NativeGateRequested`, `StatusChanged`; admission now BUSY (§9) |
| `TurnCompleted{reply}` | certainty `Injected`→`Completed`; emit `TurnCompleted{turn_id, reply, Completed}`; pop Q → next turn or → `Idle`; emit `StatusChanged` |
| `TurnFailed{error}` | certainty → `Completed` (a definite in-session failure is a known outcome, safe); emit `TurnFailed`; pop Q or → `Idle` |
| `ProcessExited{code}` while `Busy`/`AwaitingGate` with an injected turn | certainty: `command` `Injected`→`OutcomeUnknown`; non-command → surface `TurnFailed`; emit `ProcessExited`; → `Faulted{recoverable}` or `Stopped` per policy; queued turns → `TurnExpired` |
| `ProcessExited{code}` while `Idle`/before injection | certainty `NotInjected`→`FailedBeforeInjection`; → `Faulted`/`Stopped`; no side-effect risk |
| any event referencing a completed/unknown turn (`delta-after-completion`, `duplicate completion`) | IGNORED (logged); no status change; NEVER re-emit a terminal event (hostile-input hardening, R2 2B scripts) |

---

## 8. Delivery certainty, causation cycle-rejection, provenance (design §4.4; LOCKS §4-decision 7 + Phase-1)

### 8.1 Delivery-certainty state machine (exact transition points)

The certainty vocabulary is the **real** `wasl_proto::DeliveryCertainty` — the core does NOT define its own
enum. It is owned in **`wasl-proto` (L0)** and **re-exported by `wasl-audit`** (`pub use
wasl_proto::DeliveryCertainty;`), so both the durable record (a `wasl_audit::AuditEntry` field) and the live
scheduler reference ONE type with **no dependency cycle** (`wasl-runtime` → `wasl-audit` → `wasl-proto`; the
shared type sits below both). Its snake_case wire form is fixed by `wasl-proto`:

```rust
// wasl_proto::DeliveryCertainty — #[serde(rename_all = "snake_case")]
NotInjected            // never reached the session
Injected               // handed to the session, outcome pending
Completed              // turn completed (reply captured) OR a definite in-session TurnFailed
FailedBeforeInjection  // failed before any side effect could occur — SAFE to retry
OutcomeUnknown         // crash/disconnect AFTER injection — MAY have caused a side effect
```

**Transition points (normative):** a turn starts `NotInjected` at admission. It becomes `Injected` the moment
`transport.send_turn` returns Ok (the actual injection). It becomes `Completed` on `ProtocolEvent::
TurnCompleted` or `TurnFailed`. It becomes `FailedBeforeInjection` if the transport fails BEFORE `send_turn`
succeeds (spawn error, accept timeout with no injection). It becomes `OutcomeUnknown` if the process exits /
disconnects while `Injected` and unfinished.

**Which events/receipts carry it:** `TurnCompleted`/`TurnFailed` `TurnEvent`s carry the final certainty;
`TurnReceipt` at admission implies `NotInjected`; the R4 wire status carries the current certainty.

**How it enters `wasl-audit` (LOCKS §4-decision 7 + 12) — the CONCRETE emission contract:** every managed
turn-lifecycle + certainty transition is appended to the injected `Sink` (§12) as a first-class
`wasl_audit::AuditEntry` with `kind: EntryKind::RuntimeTurn` (wire `"runtimeturn"`), the lifecycle `phase:
Some(RuntimeTurnPhase::…)` (wire lowercase — `started`/`injected`/`completed`/`failed`), and the `certainty:
Some(DeliveryCertainty::…)` — **both carried in their own structural, machine-queryable fields, NEVER stuffed
into the free-text `summary`** (`summary` stays human-readable + secret-free). Concretely per non-`command`
turn:

- at admission (turn accepted): `AuditEntry { kind: RuntimeTurn, phase: Some(Started), certainty:
  Some(NotInjected), .. }`;
- on injection (`send_turn` Ok): `phase: Some(Injected), certainty: Some(Injected)`;
- on completion: `phase: Some(Completed), certainty: Some(Completed)` (or `phase: Some(Failed), certainty:
  Some(Completed)` for a definite in-session `TurnFailed`; `certainty: Some(FailedBeforeInjection)` if it
  failed before injection).

**The write-ahead-durable `command` carve-out (§State durability) — exact ordering:** for a `command`
delivery the durable record is written to the `Sink` **BEFORE `transport.send_turn` is called**:

1. **Before injecting**, append `AuditEntry { kind: RuntimeTurn, phase: Some(Injected), certainty:
   Some(Injected), .. }` (the `NotInjected`→`Injected` transition) carrying the `(TargetKey, IdempotencyKey)`
   record — and only after the `Sink::append` returns Ok do we call `transport.send_turn`.
2. **On completion**, append `phase: Some(Completed), certainty: Some(Completed)`.
3. **On crash/disconnect after injection**, the durable record already reads `certainty: Some(Injected)`, so
   recovery reading the log classifies the turn **`OutcomeUnknown`** and NEVER performs a blind retry (a
   delayed/duplicate command may cause a wrong-time side effect). The audit write ordering is what guarantees
   the evidence that a command MAY have run cannot be erased by the crash.

**Retry rules (locked, safety):** NEVER auto-retry a `command` at `OutcomeUnknown` (design §9 non-goal; may
have already caused a side effect). A `consult` retry MUST be a NEW, causation-linked turn (fresh `TurnId`),
never a blind resend on the same id.

### 8.2 Causation + cycle-rejection (LOCKS §4-decision Phase-1: sync AND queued paths + max-hop)

```rust
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct Causation {
    pub root_request: wasl_proto::RequestId,
    pub parent_request: Option<wasl_proto::RequestId>,
    pub path: Vec<wasl_proto::AgentId>,  // agents already on this call path
    pub hop: u32,                         // distance from root
}
```

Protections, **covering BOTH synchronous and queued→queued paths** (o3 must-fix): reject admission if the
target's own agent identity already appears in `causation.path` (would-be cycle) — evaluated at admission for
a SYNC turn AND re-evaluated when a QUEUED turn is about to start (a queued turn can close a cycle later);
reject if `hop > MAX_HOP` (locked default `MAX_HOP = 8`, bounded per §11). `QueueRelated` uses `root_request`
for relatedness. A rejected cycle/hop ⇒ `RejectedUnavailable` (with a distinct `RuntimeError` in audit).

**Provenance (two kinds, never conflated):** audit provenance (durable, in the Sink: source/target,
certainty, causation, timestamps — machine-trustworthy) and model-visible provenance (the `ComposedPrompt`
wrapper — context, NOT a security control; §6).

---

## 9. Native-gate resolution policy — LOCKS §4-decision 3/9 ("observe but never resolve" is BANNED)

`AwaitingGate` (a native CLI permission prompt surfaced via `ProtocolEvent::NativeGateRequested`) MUST be
resolvable. **Locked Phase-1 policy: option (i) — a generic native-gate response operation through R3**, plus
option (ii) as a configurable fallback:

- **(i) generic native-gate response (default):** the actor exposes an internal `resolve_native_gate(turn_id,
  decision: GateDecision)` command; R3's `wasl session gate <target> approve|deny` routes to it **through the
  single-writer actor** (never a raw side-channel). On resolution the actor answers the CLI's prompt over the
  transport, emits `NativeGateResolved`, and returns to `Busy`/`Idle`.
- **(ii) managed-profile: native gates disabled (config):** a managed CLI profile may be configured so any
  `NativeGateRequested` is a **terminal configuration fault** — the actor moves the target to
  `Faulted{recoverable:false}` rather than blocking. Chosen per-deployment.

This native (receive-side) gate is **DISTINCT from and never merged with** Wasl's send-side three-tier gate
(`wasl-gate`, `wasl-operator`): resolving `AwaitingGate` answers the CLI's prompt; it is not Wasl admission
gating. A gate-then-exit sequence (R2 2B hostile script) resolves to `ProcessExited` handling (§7.4).

---

## 10. `TargetKey`, providers, admission control (LOCKS §4-decision 6)

```rust
#[async_trait::async_trait]
pub trait RuntimeProvider: Send + Sync {
    /// Get-or-create the target for a context/agent identity (TargetKey = context_id). Idempotent: the same
    /// key returns the same live managed target, or a fresh ephemeral wrapper. Subject to admission control.
    async fn target_for(&self, key: &TargetKey) -> Result<std::sync::Arc<dyn DeliveryTarget>, RuntimeError>;
    /// List currently-known targets (R3 `list`).
    async fn list_targets(&self) -> Vec<TargetId>;
}

/// Backward-compat path: wraps today's spawn-per-turn AgentAdapter::deliver UNCHANGED. One submit_turn ⇒ one
/// deliver; the turn starts/runs/completes as one unit; no live process held. Reports
/// persistent_process:false. Its events() emits a synthetic TurnAccepted → (final) AssistantDelta →
/// TurnCompleted so a generic caller sees one uniform model. submit_turn can only return Started (a fresh
/// spawn is never "busy"). MUST keep working exactly as today (default + permanent fallback).
pub struct EphemeralRuntimeProvider<A: wasl_adapter::AgentAdapter> { /* adapter, per-driver caps report */ }

/// The new capability: Wasl spawns/OWNS a long-lived process via an InteractiveProtocol driver (§6); owns
/// the actor/state-machine generically. Applies admission control.
pub struct WaslManagedRuntimeProvider<P: InteractiveProtocol> { /* protocol, actors, admission, clock, sink */ }
```

**Admission control (LOCKS §4-decision 6; untrusted `context_id` must not DoS via target explosion):** the
managed provider enforces bounded limits: `max_targets_global`, `max_targets_per_sender`, a per-sender **rate
limit** on new-target creation, and **idle eviction** (an `Idle` target unused past an idle timeout is
shut down `Drain` and removed). Exceeding a limit ⇒ `target_for` returns `RuntimeError::AdmissionRefused` (⇒
`RejectedUnavailable` at submit). These bounds are config-tunable and enumerated in §11.

**Whether different source agents may SHARE one target (LOCKS §6-open-question, security):** DEFAULT = **NO
implicit sharing** — a `TargetKey` (context_id) is bound to the sender identity that created it; a turn from a
different `from` for that key is refused unless the deployment explicitly opts a context into shared use. The
full threat model (a peer steering another peer's live session) is DEFERRED to Phase-3's A2A wiring spec
(`node`/`dispatch` delta) and noted there; the core provides the enforcement hook (admission checks `from`
against the key's owner).

---

## 11. Bounds on ALL retained state (LOCKS §4-decision 11)

Every retained structure is bounded by BOTH count AND bytes where it holds text (days-long sessions):

| State | Bound |
|---|---|
| target registry | `max_targets_global`, `max_targets_per_sender`, idle-eviction timeout (§10) |
| event journal (ring) | `journal_capacity` events (per target); below-floor ⇒ `Truncated` (§4.3) |
| idempotency map | `idempotency_cap` entries + `idempotency_ttl`; LRU/TTL eviction |
| receipt cache | bounded by idempotency map (receipts stored with their key) |
| FIFO queue | `queue_max_len` AND `queue_max_bytes` (over ⇒ `RejectedBusy`) |
| transcript projection | last-N replies AND `transcript_max_bytes` (§4.5) |
| `AssistantDelta` text | `delta_max_bytes` per delta (over ⇒ truncated + a flag) |
| causation path | `MAX_HOP = 8` (§8.2); path length bounded by max-hop |

All bounds are config-tunable with locked defaults; the config surface is Phase-3 wiring (`node-config`
delta), NOT this crate's concern, but the fields are named here so the core validates against them.

---

## 12. Observability into the existing audit `Sink` (LOCKS §4-decision 12)

Managed turn-lifecycle + certainty transitions emit `wasl_audit::AuditEntry` into the **injected `Sink`** (the
same append-only durability seam every other subsystem uses; `docs/specs/audit.md`) — **not** a private
parallel system. Each such entry is `AuditEntry { kind: EntryKind::RuntimeTurn, phase:
Some(RuntimeTurnPhase::…), certainty: Some(DeliveryCertainty::…), actor, targets, summary, .. }` — the phase
and certainty are the real first-class **structural** `AuditEntry` fields (`wasl-audit` extended these; they
are `Option`-typed and `skip_serializing_if` so legacy entries stay byte-compatible), **never** encoded into
the free-text `summary`. The exact per-transition/per-`command` emission ordering is normative in §8.1. The
actor is constructed with an `Arc<Mutex<dyn Sink>>` (or the node's `TeeSink`, so operator observers see
managed activity too — `docs/specs/operator.md`). The write-ahead-durable `command` entry (§8.1) uses this
same Sink. Status/queue-depth/awaiting-gate/last-event/restart-count/journal-floor are exposed via `status()`
and the R4 wire (`runtime-control-api.md`), which draws from the same actor state.

---

## 13. `RuntimeError` — a DEFINED taxonomy (LOCKS impl-plan Phase-1 "RuntimeError taxonomy")

Distinct, testable variants (like `ConfigError`/`AdapterError`), `impl Display + std::error::Error`:

```rust
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum RuntimeError {
    /// A driver spawn/resume failed (process could not start, resume rejected).
    SpawnFailed { detail: String },
    /// The transport IO failed (write/read on the live handle).
    Transport { detail: String },
    /// The target is Offline/Faulted/Stopped for the requested operation.
    Unavailable { status: TargetStatus },
    /// The operation is not supported by this target's capabilities (e.g. cancel when !interruptible).
    Unsupported { operation: String },
    /// Admission control refused (target explosion / rate limit / per-sender cap).
    AdmissionRefused { detail: String },
    /// A causation cycle or max-hop was detected (§8.2).
    CausationRejected { detail: String },
    /// A referenced turn/target id was not found.
    NotFound { detail: String },
    /// A native gate could not be resolved (policy (ii) terminal fault, or malformed resolution).
    GateUnresolvable { detail: String },
    /// A bound was exceeded that is not admission (queue full by bytes, etc.).
    Exhausted { detail: String },
    /// The actor mailbox/target has shut down.
    Shutdown,
}
```

Each variant is independently constructed and asserted in the 2A tests (distinct `Display`, distinct match
arms). No catch-all string error.

---

## 14. Testability honesty — what the fake proves vs what needs `--features live-cli`

**Proven 100% here, deterministically, NO real CLI, NO LLM (impl-plan Phase 2A):** the entire generic core —
single-writer actor; FIFO queue; reject-vs-queue; **no input interleaving** (concurrent submits never corrupt
the single-writer stream); event **replay/sequencing** exactly-once/no-gap incl. truncation; multi-observer
independent cursors; snapshot + atomic replay-to-live handoff; attach/detach; cancel (state machine);
crash→`OutcomeUnknown`; **duplicate idempotency-key** → no double-inject (concurrent same-key can't both
enqueue); `AwaitingGate` blocks admission as busy; native-gate resolution (§9); causation cycle (**sync AND
queued**) rejected; max-hop rejected; every `RuntimeError` variant; the FULL §7 transition matrix. Plus the
**concurrency-hardening** the plan requires before 2A is done: a `Barrier`-driven adversarial concurrent-submit
stress test AND `loom`-or-`proptest` interleaving exploration (line coverage alone will not catch ordering
bugs). The core runs against the in-repo scripted `InteractiveTransport` (R1's own `tests/` support, NOT the
R2 fake — avoids the reverse dev-dep cycle).

**UNTESTED HERE — needs `--features live-cli` (R2/Phase 4–5):** real spawn/resume, real turn-boundary
detection, real native-session-id stability, real gate appearance, and — critically — the two UNKNOWN facts:

- **Interruptibility** ⛔ UNKNOWN — the cancel state machine is fully fake-tested, but NO real substrate has
  demonstrated a real interrupt; no driver may set `interruptible: true` until a Phase-1-follow-up interrupt
  capture confirms it.
- **Streaming granularity** ⛔ UNKNOWN for claude + opencode-`serve` — the delta path is fake-tested, but only
  opencode-`acp` showed real deltas; a claude/serve driver reports `streaming_output: false` (emitting one
  final `AssistantDelta`) until a Phase-1-follow-up streaming capture proves otherwise.

Model B (terminal-attach) is FORECLOSED and never tested (§Single-writer invariant).

---

## 15. Coverage bar

`cargo llvm-cov -p wasl-runtime --fail-under-lines 100` MUST exit 0 (genuine LINE 100%), matching the repo
regime. **REGION** may carry documented residuals ONLY on genuinely-unreachable arms, each enumerated in
`AGENTS.md` with a verified-unreachable note (the `filesink`/`operator` idiom) — e.g. a defensively-matched
`ProtocolEvent` arm that the scripted transport cannot reach is either exercised via a dedicated hostile
script (R2 2B) or deleted, never left as dead code. No `#[ignore]`/`todo!`/tautology; the concurrency stress +
loom/proptest gate is part of DoD. Tests compile only against the public API + `wasl_adapter`/`wasl_audit`/
`wasl_proto` public types. The `--features live-cli` real-CLI tests live in R2 and are asserted to have RUN in
explicit verification passes, never in default CI.
