//! wasl-runtime — R1 — the generic managed-runtime core (Phase-1 type surface).
//!
//! This crate owns the **frozen, object-safe, type-only** contract every generic
//! caller of the live / managed-runtime feature talks to: the identifier
//! newtypes, [`RuntimeCapabilities`], the public [`DeliveryTarget`] seam, the
//! [`RuntimeProvider`] get-or-create seam, the driver seam
//! ([`InteractiveProtocol`] / [`InteractiveTransport`] / the internal
//! [`ProtocolEvent`]), the PUBLIC [`TurnEvent`] vocabulary, the admission /
//! status / receipt value types, [`Causation`], and the [`RuntimeError`]
//! taxonomy.
//!
//! **Phase-1 is TYPE-ONLY.** Traits are DECLARED with no bodies; there is NO
//! scheduler / actor / queue / driver logic here (that is Phase-2). Everything
//! below is normative per `docs/specs/runtime.md` and the wire pins in
//! `docs/specs/runtime-control-api.md`.
//!
//! ## The internal-vs-public boundary
//!
//! [`ProtocolEvent`] (§6) is the NORMALIZED driver vocabulary the actor consumes
//! and translates INTO the public [`TurnEvent`] (§5). It is deliberately NOT
//! `Serialize`/`Deserialize` — a type-level guarantee it can never be placed on
//! the control wire (`runtime-control-api.md` §Boundary invariant). Only the
//! public vocabulary — [`TurnEvent`], [`TargetStatus`], [`TurnReceipt`],
//! [`RuntimeCapabilities`], [`TurnSource`], [`BusyPolicy`], [`BusyPhase`],
//! [`NativeGateDetail`], [`Causation`] (and `wasl_proto::DeliveryCertainty`) —
//! crosses the wire.

#![forbid(unsafe_code)]

use serde::{Deserialize, Serialize};

// Phase-2A logic modules (the actor / journal / config / providers).
mod actor;
mod config;
mod ephemeral;
mod journal;
mod provider;
mod routing;

pub use config::ManagedConfig;
pub use ephemeral::EphemeralRuntimeProvider;
pub use provider::WaslManagedRuntimeProvider;
pub use routing::ProtocolRoutingProvider;

// `wasl_proto` owns the shared vocabulary (`AgentId`/`RequestId`/`GateDecision`
// and, critically, `DeliveryCertainty`), re-exported by `wasl-audit`; this crate
// re-USES those types verbatim rather than defining its own (no dependency
// cycle: wasl-runtime → wasl-audit → wasl-proto).
pub use wasl_proto::DeliveryCertainty;

// ---------------------------------------------------------------------
// 1. Identifiers and small value types (runtime.md §1)
// ---------------------------------------------------------------------

/// A Wasl-side handle for a target (NOT a CLI-native id). Opaque, stable for the
/// target's lifetime.
#[derive(Debug, Clone, PartialEq, Eq, Hash, Serialize, Deserialize)]
pub struct TargetId(pub String);

/// The get-or-create key for a managed target (`TargetKey == context_id`). One
/// live target per conversation context.
#[derive(Debug, Clone, PartialEq, Eq, Hash, Serialize, Deserialize)]
pub struct TargetKey(pub String);

/// A CLI-native session id learned from a driver (claude UUID / opencode `ses_…`
/// / acp `sessionId`). Opaque to the generic core; only R2 drivers mint/parse it.
#[derive(Debug, Clone, PartialEq, Eq, Hash, Serialize, Deserialize)]
pub struct NativeSessionId(pub String);

/// A Wasl-side per-turn id, unique for the target's lifetime. Correlate by this,
/// NEVER by [`EventSeq`].
#[derive(Debug, Clone, PartialEq, Eq, Hash, Serialize, Deserialize)]
pub struct TurnId(pub String);

/// Per-target monotonic event-journal sequence: starts at 1, gap-free while
/// retained, NOT node-global. `Copy` + `Ord`; serializes as a bare `u64`.
#[derive(Debug, Clone, Copy, PartialEq, Eq, PartialOrd, Ord, Hash, Serialize, Deserialize)]
#[serde(transparent)]
pub struct EventSeq(pub u64);

/// A caller-supplied idempotency key scoped under a [`TargetKey`]. Deduped
/// OUTSIDE the mailbox (§4.4).
#[derive(Debug, Clone, PartialEq, Eq, Hash, Serialize, Deserialize)]
pub struct IdempotencyKey(pub String);

/// Injected monotonic time, so ALL deadlines are deterministic under
/// `tokio::time::pause`. Never `SystemTime` in the core.
pub trait RuntimeClock: Send + Sync {
    /// The current monotonic instant.
    fn now(&self) -> std::time::Instant;
}

/// The default [`RuntimeClock`] backed by `Instant::now()`.
#[derive(Debug, Clone, Copy, Default)]
pub struct MonotonicClock;

impl RuntimeClock for MonotonicClock {
    fn now(&self) -> std::time::Instant {
        std::time::Instant::now()
    }
}

/// Wire/audit timestamps (informational only; the ACTUAL waits use
/// [`RuntimeClock`] durations). RFC 3339 UTC millis, rendered at emit time.
pub type Timestamp = String;

// ---------------------------------------------------------------------
// 2. RuntimeCapabilities — 8 independent honest booleans (runtime.md §2)
// ---------------------------------------------------------------------

/// What a target can HONESTLY do. All 8 fields are INDEPENDENT — a caller MUST
/// NOT assume any implies another. A driver reports the exact proven subset; a
/// `false` is honest, a rounded-up `true` is a bug.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
pub struct RuntimeCapabilities {
    /// The same OS process serves multiple turns (no per-turn spawn).
    pub persistent_process: bool,
    /// Context persists across turns (possibly via resume).
    pub persistent_conversation: bool,
    /// Reliable turn START/END boundaries (not screen-scrape).
    pub structured_turns: bool,
    /// Assistant output streamable as deltas.
    pub streaming_output: bool,
    /// A viewer can observe the live session.
    pub attachable: bool,
    /// An in-flight turn can be cancelled cleanly.
    pub interruptible: bool,
    /// Turn boundaries + native ids give machine-usable correlation ids.
    pub structured_provenance: bool,
    /// A dropped session can be resumed with context (within one node life).
    pub resumable: bool,
}

// ---------------------------------------------------------------------
// 3. The public DeliveryTarget trait (runtime.md §3)
// ---------------------------------------------------------------------

/// A live-or-ephemeral thing Wasl can deliver turns into. Generic
/// dispatch/routing/R3 knows ONLY this trait; it never sees a claude/ACP/process
/// detail (that lives behind [`InteractiveProtocol`], §6). Object-safe; used as
/// `Arc<dyn DeliveryTarget>` (§10).
#[async_trait::async_trait]
pub trait DeliveryTarget:
    NativeGateResolver + ToolCallAnswerer + std::fmt::Debug + Send + Sync
{
    /// Stable Wasl-side handle (not a CLI-native id). Constant for the target's
    /// lifetime.
    fn target_id(&self) -> &TargetId;

    /// What this target can honestly do (§2). Constant for the target's lifetime.
    fn capabilities(&self) -> RuntimeCapabilities;

    /// Best-effort state snapshot. ADVISORY ONLY — never gate admission on it.
    /// The authoritative admission decision is [`DeliveryTarget::submit_turn`].
    async fn status(&self) -> TargetStatus;

    /// THE ATOMIC ADMISSION DECISION. Returns a receipt describing Started /
    /// Queued(position) / Rejected WITHOUT waiting for the reply (the reply
    /// arrives via [`DeliveryTarget::events`]). MUST NOT race with `status()`.
    async fn submit_turn(&self, req: TurnRequest, busy_policy: BusyPolicy) -> TurnReceipt;

    /// Replayable, sequenced event stream. `after_seq` lets a (re)subscriber
    /// resume with no gap and no duplicate. Yields the PUBLIC [`TurnEvent`]
    /// vocabulary — never the internal [`ProtocolEvent`].
    async fn events(&self, after_seq: EventSeq) -> TurnEventStream;

    /// Request cancellation of an in-flight turn. Best-effort; only meaningful if
    /// `capabilities().interruptible`.
    async fn cancel_turn(&self, turn_id: TurnId, reason: CancelReason) -> Result<(), RuntimeError>;

    /// Shut the target down. Drain finishes the active turn then stops;
    /// Immediate stops now (§7).
    async fn shutdown(&self, mode: ShutdownMode) -> Result<(), RuntimeError>;
}

/// Native-gate resolution (runtime.md §9 option (i)): a managed target answers a
/// pending native CLI permission gate through the single-writer actor (never a
/// raw side-channel). Exposed as an extra trait the managed target implements so
/// callers can `resolve_native_gate` on the `Arc<dyn DeliveryTarget>` a managed
/// provider returns via a downcast-free trait-upcast on the concrete handle.
#[async_trait::async_trait]
pub trait NativeGateResolver: Send + Sync {
    /// Resolve the native gate held by `turn_id` with `decision`: the actor
    /// answers the CLI's prompt over the transport, emits `NativeGateResolved`,
    /// and returns to `Busy`/`Idle`.
    async fn resolve_native_gate(
        &self,
        turn_id: TurnId,
        decision: wasl_proto::GateDecision,
    ) -> Result<(), RuntimeError>;
}

/// Tool-call answer-back (runtime.md §9 — the tool-channel counterpart of
/// [`NativeGateResolver`]): a managed target answers a mid-turn
/// `ProtocolEvent::ToolStarted`-shaped bidirectional callback through the
/// single-writer actor (never a raw side-channel). Phase-2C additive extension,
/// exposed as an extra supertrait the managed target implements so callers can
/// `answer_tool_call` on the `Arc<dyn DeliveryTarget>` a managed provider
/// returns via a downcast-free trait-upcast on the concrete handle — mirroring
/// exactly how [`NativeGateResolver::resolve_native_gate`] is reached.
#[async_trait::async_trait]
pub trait ToolCallAnswerer: Send + Sync {
    /// Answer the tool call blocking `turn_id` with `value`: the actor sends the
    /// answer over the transport (via [`InteractiveTransport::answer_tool_call`])
    /// from within its own serialized mailbox task — the SAME single writer that
    /// owns the `Box<dyn InteractiveTransport>` — so the blocked turn can proceed.
    /// NEVER hands the caller a second concurrent writer.
    async fn answer_tool_call(&self, turn_id: TurnId, value: String) -> Result<(), RuntimeError>;
}

/// [`DeliveryTarget::events`] returns this: a `Pin<Box<dyn Stream<Item =
/// StreamItem> + Send>>` — each item pairs the per-target [`EventSeq`] with a
/// public [`TurnEvent`], or a control item on truncation.
pub type TurnEventStream = std::pin::Pin<Box<dyn futures::Stream<Item = StreamItem> + Send>>;

/// An item yielded by [`TurnEventStream`].
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum StreamItem {
    /// A journalled event at its sequence.
    Event {
        /// The per-target monotonic sequence.
        seq: EventSeq,
        /// The public event at that sequence.
        event: TurnEvent,
    },
    /// `after_seq` was below the retained floor: the client MUST re-`snapshot()`
    /// then re-subscribe from `latest`. Carries the retained window.
    Truncated {
        /// The lowest retained sequence still available.
        earliest: EventSeq,
        /// The highest retained sequence at truncation time.
        latest: EventSeq,
    },
}

/// The outcome of an admission decision ([`DeliveryTarget::submit_turn`]).
///
/// Internally tagged on the wire (`{"type":"started",...}`) per
/// `runtime-control-api.md`.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(tag = "type", rename_all = "snake_case")]
pub enum TurnReceipt {
    /// Admitted and now the active turn. `first_seq` = the [`EventSeq`] of this
    /// turn's first journalled event (atomic subscribe-then-admit).
    Started {
        /// The new turn's id.
        turn_id: TurnId,
        /// The sequence of this turn's first journalled event.
        first_seq: EventSeq,
    },
    /// Admitted into the FIFO queue at `position` (0 = next after the active turn).
    Queued {
        /// The queued turn's id.
        turn_id: TurnId,
        /// The queue position (0-based).
        position: usize,
    },
    /// Refused: target busy and `busy_policy` said not to queue (a BUSY condition).
    RejectedBusy {
        /// The turn currently holding the target.
        active_turn: TurnId,
    },
    /// Refused: target Offline/Faulted/Stopped, OR admission control refused
    /// (not a busy condition).
    RejectedUnavailable {
        /// The status that caused the refusal.
        status: TargetStatus,
    },
    /// A resend of an already-seen `(TargetKey, IdempotencyKey)`: the ORIGINAL
    /// receipt is returned verbatim, NO second injection occurs.
    Duplicate {
        /// The recorded original receipt, wrapped verbatim.
        original: Box<TurnReceipt>,
    },
}

/// How a target is shut down (§3).
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum ShutdownMode {
    /// Finish the active turn, then stop.
    Drain,
    /// Stop now.
    Immediate,
}

/// Why a turn was cancelled (§3).
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum CancelReason {
    /// A human operator requested cancellation.
    OperatorRequest,
    /// An A2A cancel propagated in.
    A2aCancel,
    /// The turn was superseded by a newer one.
    Superseded,
    /// The target is shutting down.
    Shutdown,
}

// --- 3.1 TurnRequest — the admission payload (runtime.md §3.1) ---

/// The admission payload handed to [`DeliveryTarget::submit_turn`].
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct TurnRequest {
    /// The delivery payload (`wasl_adapter::Delivery`: context_id, from, kind,
    /// text, data).
    pub delivery: wasl_adapter::Delivery,
    /// Who originated this turn — serialized by the single actor.
    pub source: TurnSource,
    /// Optional dedupe key scoped under the target's [`TargetKey`]. `None` ⇒ no
    /// dedupe.
    pub idempotency_key: Option<IdempotencyKey>,
    /// The synchronous/queued call path this turn is on (cycle rejection, §8).
    /// Root turns carry hop 0.
    pub causation: Causation,
}

/// Who originated a turn.
///
/// On the wire: `{"type":"agent","from":"…"}` | `{"type":"local_human"}`.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(tag = "type", rename_all = "snake_case")]
pub enum TurnSource {
    /// An A2A-originated turn from a peer agent.
    Agent {
        /// The originating peer's id.
        from: wasl_proto::AgentId,
    },
    /// A local/human-originated turn (R3 `send`).
    LocalHuman,
}

// ---------------------------------------------------------------------
// 4.1 TargetStatus and phases (runtime.md §4.1)
// ---------------------------------------------------------------------

/// The best-effort, advisory state snapshot of a target.
///
/// Internally tagged on the wire (`{"type":"idle","since":"…"}`) per
/// `runtime-control-api.md`.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(tag = "type", rename_all = "snake_case")]
pub enum TargetStatus {
    /// The target has no live session.
    Offline,
    /// The target is starting up.
    Starting,
    /// The target is up and idle.
    Idle {
        /// When the target became idle.
        since: Timestamp,
    },
    /// The target is running a turn.
    Busy {
        /// The active turn.
        active_turn: TurnId,
        /// Who originated the active turn.
        source: TurnSource,
        /// The current phase of the active turn.
        phase: BusyPhase,
        /// When the active turn started.
        since: Timestamp,
        /// How many turns are queued behind it.
        queued: usize,
    },
    /// Blocked on a NATIVE CLI permission gate. BUSY, not idle — cannot take a
    /// new turn. MUST be resolvable (§9).
    AwaitingGate {
        /// The active turn blocked on the gate.
        active_turn: TurnId,
        /// The secret-free description of the native gate.
        detail: NativeGateDetail,
        /// When the gate was entered.
        since: Timestamp,
        /// How many turns are queued behind it.
        queued: usize,
    },
    /// An interrupt of the active turn is in flight.
    Interrupting {
        /// The turn being interrupted.
        active_turn: TurnId,
        /// When interruption began.
        since: Timestamp,
    },
    /// The target is draining (finishing its active turn, admitting nothing new).
    Draining {
        /// The active turn being finished.
        active_turn: TurnId,
        /// When draining began.
        since: Timestamp,
    },
    /// The target has faulted.
    Faulted {
        /// The failure detail.
        error: String,
        /// Whether the fault is recoverable.
        recoverable: bool,
    },
    /// The target has stopped.
    Stopped,
}

/// The fine-grained phase of a busy target (§4.1).
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum BusyPhase {
    /// Prompt handed to transport, awaiting `TurnAccepted`.
    Dispatching,
    /// The assistant is producing output.
    Generating,
    /// A tool is executing.
    RunningTool,
    /// Awaiting a tool result.
    WaitingForTool,
    /// Blocked awaiting some input.
    WaitingForInput,
    /// Finalizing the turn.
    Completing,
}

/// Opaque, secret-free description of a native CLI gate (its prompt
/// text/preview). Model-untrusted.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct NativeGateDetail {
    /// A short human-readable summary of what the gate is asking.
    pub summary: String,
    /// A preview of the operation the gate guards.
    pub preview: String,
}

// ---------------------------------------------------------------------
// 4.2 BusyPolicy — per-delivery, admission-atomic (runtime.md §4.2)
// ---------------------------------------------------------------------

/// The per-delivery policy applied when the target is busy.
///
/// On the wire, durations are integer milliseconds (`max_wait_ms`) per
/// `runtime-control-api.md` §Conventions.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(tag = "type", rename_all = "snake_case")]
pub enum BusyPolicy {
    /// Queue behind the active turn up to a bounded wait; expire if not started
    /// in time.
    Queue {
        /// The bounded wait before the queued turn expires.
        #[serde(rename = "max_wait_ms", with = "duration_millis")]
        max_wait: std::time::Duration,
    },
    /// Refuse immediately if busy (→ `RejectedBusy`).
    Reject,
    /// Queue ONLY if the incoming turn shares a causation root with the active
    /// turn (§8); else reject.
    QueueRelated {
        /// The bounded wait before the queued turn expires.
        #[serde(rename = "max_wait_ms", with = "duration_millis")]
        max_wait: std::time::Duration,
    },
}

/// serde helper: represent a [`std::time::Duration`] as integer milliseconds on
/// the wire (`runtime-control-api.md` §Conventions — durations are `*_ms`,
/// non-negative integers).
mod duration_millis {
    use serde::{Deserialize, Deserializer, Serializer};

    pub(super) fn serialize<S>(
        value: &std::time::Duration,
        serializer: S,
    ) -> Result<S::Ok, S::Error>
    where
        S: Serializer,
    {
        serializer.serialize_u64(value.as_millis() as u64)
    }

    pub(super) fn deserialize<'de, D>(deserializer: D) -> Result<std::time::Duration, D::Error>
    where
        D: Deserializer<'de>,
    {
        let ms = u64::deserialize(deserializer)?;
        Ok(std::time::Duration::from_millis(ms))
    }
}

// ---------------------------------------------------------------------
// 5. The PUBLIC TurnEvent vocabulary (runtime.md §5)
// ---------------------------------------------------------------------

/// The PUBLIC, driver-free event vocabulary [`DeliveryTarget::events`] and the R4
/// control plane expose. The internal [`ProtocolEvent`] (§6) is translated INTO
/// it by the actor and never leaks onto any public surface.
///
/// Internally tagged, `snake_case` (`{"type":"assistant_delta",...}`).
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(tag = "type", rename_all = "snake_case")]
pub enum TurnEvent {
    /// The target is up and idle (carries the native session id if the driver
    /// has one). First-turn only.
    SessionReady {
        /// The CLI-native session id, if the driver has one.
        native: Option<NativeSessionId>,
    },
    /// A turn was admitted and started (mirrors the `Started` receipt for
    /// late/replay subscribers).
    TurnAccepted {
        /// The accepted turn's id.
        turn_id: TurnId,
        /// Who originated the turn.
        source: TurnSource,
    },
    /// A turn was admitted into the queue.
    TurnQueued {
        /// The queued turn's id.
        turn_id: TurnId,
        /// The queue position.
        position: usize,
    },
    /// An incremental chunk of assistant output. On a non-streaming substrate a
    /// single final chunk is emitted.
    AssistantDelta {
        /// The turn this delta belongs to.
        turn_id: TurnId,
        /// The delta text.
        text: String,
    },
    /// A tool call began (name only; args/results are audit provenance).
    ToolStarted {
        /// The turn this tool call belongs to.
        turn_id: TurnId,
        /// The tool name.
        name: String,
    },
    /// A tool call finished.
    ToolFinished {
        /// The turn this tool call belongs to.
        turn_id: TurnId,
        /// The tool name.
        name: String,
        /// Whether the tool succeeded.
        ok: bool,
    },
    /// The session entered a native gate (`AwaitingGate`). Resolvable via §9.
    NativeGateRequested {
        /// The turn blocked on the gate.
        turn_id: TurnId,
        /// The secret-free gate description.
        detail: NativeGateDetail,
    },
    /// The native gate was resolved (how, by whom).
    NativeGateResolved {
        /// The turn whose gate was resolved.
        turn_id: TurnId,
        /// The resolution decision.
        decision: wasl_proto::GateDecision,
    },
    /// The turn completed successfully. ALWAYS carries `turn_id` and its final
    /// certainty.
    TurnCompleted {
        /// The completed turn's id.
        turn_id: TurnId,
        /// The final reply text.
        reply: String,
        /// The final delivery certainty.
        certainty: wasl_proto::DeliveryCertainty,
    },
    /// The turn failed inside the session (distinct from the process dying).
    TurnFailed {
        /// The failed turn's id.
        turn_id: TurnId,
        /// The failure detail.
        error: String,
        /// The final delivery certainty.
        certainty: wasl_proto::DeliveryCertainty,
    },
    /// A turn was cancelled (interrupt result).
    TurnCancelled {
        /// The cancelled turn's id.
        turn_id: TurnId,
        /// Why it was cancelled.
        reason: CancelReason,
    },
    /// A turn expired in the queue before starting (`BusyPolicy` max_wait
    /// elapsed).
    TurnExpired {
        /// The expired turn's id.
        turn_id: TurnId,
    },
    /// The target status changed (observability; carries the new status).
    StatusChanged {
        /// The new status.
        status: TargetStatus,
    },
    /// The underlying process/connection exited. Terminal for the current
    /// transport.
    ProcessExited {
        /// The exit code, if any.
        code: Option<i32>,
    },
}

// ---------------------------------------------------------------------
// 6. The driver seam — InteractiveProtocol / InteractiveTransport /
//    ProtocolEvent (runtime.md §6). These LIVE IN R1; concrete drivers are R2.
// ---------------------------------------------------------------------

/// A driver that can spawn/resume a live agent session. Used ONLY by the managed
/// provider. Object-safe: `spawn`/`resume` return `Box<dyn InteractiveTransport>`.
#[async_trait::async_trait]
pub trait InteractiveProtocol: Send + Sync {
    /// The fixed, honest capability report for this driver (§2).
    fn capabilities(&self) -> RuntimeCapabilities;

    /// Start a fresh live session.
    async fn spawn(
        &self,
        opts: &SpawnOptions,
    ) -> Result<Box<dyn InteractiveTransport>, RuntimeError>;

    /// Re-attach to an existing native session (only if `capabilities().
    /// resumable`; within one node life).
    async fn resume(
        &self,
        native: &NativeSessionId,
        opts: &SpawnOptions,
    ) -> Result<Box<dyn InteractiveTransport>, RuntimeError>;
}

/// Blanket forwarding impl so an `Arc<P>` is itself an [`InteractiveProtocol`]
/// (the managed provider is generic over `P: InteractiveProtocol`; a shared
/// driver is naturally passed as `Arc<P>`).
#[async_trait::async_trait]
impl<T: InteractiveProtocol + ?Sized> InteractiveProtocol for std::sync::Arc<T> {
    fn capabilities(&self) -> RuntimeCapabilities {
        (**self).capabilities()
    }

    async fn spawn(
        &self,
        opts: &SpawnOptions,
    ) -> Result<Box<dyn InteractiveTransport>, RuntimeError> {
        (**self).spawn(opts).await
    }

    async fn resume(
        &self,
        native: &NativeSessionId,
        opts: &SpawnOptions,
    ) -> Result<Box<dyn InteractiveTransport>, RuntimeError> {
        (**self).resume(native, opts).await
    }
}

/// A live connection to ONE running agent session. The actor is the SOLE holder —
/// nothing else may call `send_turn` or touch the underlying handle
/// (single-writer invariant). Object-safe; held as `Box<dyn InteractiveTransport>`.
#[async_trait::async_trait]
pub trait InteractiveTransport: Send {
    /// Push one turn's prompt. Returns once the CLI ACCEPTS the turn (completion
    /// arrives via [`InteractiveTransport::next_event`]).
    async fn send_turn(&mut self, prompt: &ComposedPrompt) -> Result<(), RuntimeError>;

    /// Pull the next NORMALIZED protocol event. Ends with `ProcessExited` when
    /// the session dies.
    ///
    /// # Cancellation-safety requirement (MUST)
    ///
    /// Actor pumping races `next_event()` against mailbox commands via
    /// `tokio::select!`; therefore this future may be dropped at an await point
    /// and polled again by a subsequent call. Every implementor MUST guarantee
    /// that such cancellation never loses a normalized event and never corrupts
    /// driver-internal framing state. In particular: once an internal queue read
    /// resolves to an event, no further `.await` may occur before that event is
    /// returned from `next_event()`.
    async fn next_event(&mut self) -> Option<ProtocolEvent>;

    /// Best-effort interrupt of the active turn (only if `capabilities().
    /// interruptible`).
    async fn interrupt(&mut self) -> Result<(), RuntimeError>;

    /// Answer a mid-turn `ProtocolEvent::ToolStarted`-shaped bidirectional
    /// callback: send `value` back to the session over the SAME transport the
    /// actor is reading from, so a turn blocked behind a tool-shaped callback can
    /// proceed. Phase-2C additive extension (runtime.md §9 — the tool-channel
    /// counterpart of the gate-channel answer-back `resolve_native_gate` funnels
    /// through `send_turn`).
    ///
    /// DEFAULT NO-OP: every existing implementor (the unidirectional
    /// claude-stdin / opencode-ACP drivers) compiles unchanged and stays
    /// object-safe; only a driver whose native wire actually maps a callback onto
    /// `ToolStarted` and blocks awaiting an answer overrides this. The
    /// single-writer invariant is preserved — the ACTOR is still the sole caller,
    /// reached ONLY through its serialized mailbox (never a raw second handle).
    async fn answer_tool_call(&mut self, _name: &str, _value: &str) -> Result<(), RuntimeError> {
        Ok(())
    }
}

/// The NORMALIZED event vocabulary every driver maps its native wire onto.
///
/// INTERNAL: consumed by the actor, translated into the PUBLIC [`TurnEvent`]
/// (§5), NEVER exposed on a public surface. Deliberately NOT `Serialize`/
/// `Deserialize` — a type-level guarantee it cannot be placed on the wire.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum ProtocolEvent {
    /// The session is ready (carries the native id if the driver has one).
    SessionReady {
        /// The CLI-native session id, if any.
        native: Option<NativeSessionId>,
    },
    /// The CLI accepted the turn (optionally carrying a native turn id).
    TurnAccepted {
        /// The driver-native turn id, if any.
        native_turn: Option<String>,
    },
    /// A chunk of assistant output.
    AssistantDelta {
        /// The delta text.
        text: String,
    },
    /// A tool call began.
    ToolStarted {
        /// The tool name.
        name: String,
    },
    /// A tool call finished.
    ToolFinished {
        /// The tool name.
        name: String,
        /// Whether the tool succeeded.
        ok: bool,
    },
    /// The session entered a native permission gate.
    NativeGateRequested {
        /// The secret-free gate description.
        detail: NativeGateDetail,
    },
    /// The turn completed.
    TurnCompleted {
        /// The reply text.
        reply: String,
    },
    /// The turn failed inside the session.
    TurnFailed {
        /// The failure detail.
        error: String,
    },
    /// The underlying process exited.
    ProcessExited {
        /// The exit code, if any.
        code: Option<i32>,
    },
}

/// Caller-supplied spawn context: per-target protocol routing, cwd, model,
/// posture, env overrides, setup script, binary override, and disallowed tools.
/// All fields are optional; absent fields mean "use the driver/provider default".
/// `Default` yields an all-`None`/empty context (fully backward-compatible).
#[derive(Debug, Clone, PartialEq, Eq, Default, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub struct SpawnContext {
    /// Which protocol to use (e.g. `"claude"`, `"opencode-acp"`). `None` means
    /// the provider picks its default.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub protocol: Option<String>,
    /// Working directory for the spawned process. `None` means inherit.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub cwd: Option<String>,
    /// Model name to pass to the driver. `None` means use the driver default.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub model: Option<String>,
    /// Permission posture (e.g. `"default"`, `"bypassPermissions"`). `None`
    /// means use the driver default.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub posture: Option<String>,
    /// Extra environment variables merged into the spawned process env.
    #[serde(default, skip_serializing_if = "std::collections::BTreeMap::is_empty")]
    pub env: std::collections::BTreeMap<String, String>,
    /// Setup script run before the agent is started. `None` means no setup.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub setup: Option<String>,
    /// Override the agent binary path. `None` means use the driver's configured
    /// default.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub bin: Option<String>,
    /// Tool names the agent is not permitted to call.
    #[serde(default, skip_serializing_if = "Vec::is_empty")]
    pub disallowed_tools: Vec<String>,
    /// D2 (send-capability): an MCP-server config the driver passes to the CLI
    /// verbatim via `--mcp-config` (a JSON blob or a file path). This is how the
    /// managed session gets Wasl's peer-send tools (consult/command/message) so it
    /// can reach peers. `None` = no extra MCP servers. The caller (Marsad, or
    /// wasl-mcpd node config) populates it with the node's send-tools endpoint;
    /// the runtime applies it without interpreting the contents.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub mcp_config: Option<String>,
}

/// Options a provider passes a driver at spawn/resume (bin, model, isolated env,
/// target key, native-for-resume). Fields are private to the crate; only R2
/// drivers construct/read them.
#[derive(Debug, Clone, PartialEq, Eq, Default)]
pub struct SpawnOptions {
    pub(crate) target_key: Option<TargetKey>,
    pub(crate) context: SpawnContext,
}

impl SpawnOptions {
    /// The target key (`TargetKey == context_id`) this spawn/resume is for, if
    /// the provider populated one. R2 drivers that need to derive a
    /// context-scoped native identity (e.g. `ClaudeStreamProtocol`'s UUIDv5
    /// minting) read this; drivers that mint their own arbitrary native id
    /// (e.g. `FakeLineProtocol`) may ignore it.
    pub fn target_key(&self) -> Option<&TargetKey> {
        self.target_key.as_ref()
    }

    /// The spawn context (cwd, model, posture, env, bin, disallowed_tools,
    /// protocol) carried from the create-target request down to the driver.
    pub fn context(&self) -> &SpawnContext {
        &self.context
    }
}

/// The prompt the actor hands the transport — the model-visible provenance
/// wrapper applied. CONTEXT, not a security control.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct ComposedPrompt(pub String);

// ---------------------------------------------------------------------
// 8.2 Causation + cycle-rejection (runtime.md §8.2)
// ---------------------------------------------------------------------

/// The synchronous/queued call path a turn is on, used for cycle rejection and
/// `QueueRelated` relatedness.
///
/// On the wire, `root_request`/`parent_request` are bare JSON strings
/// (`wasl_proto::RequestId` is `#[serde(transparent)]`) and `path` is an array of
/// `wasl_proto::AgentId` strings (`runtime-control-api.md`).
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct Causation {
    /// The root request that started this call path.
    pub root_request: wasl_proto::RequestId,
    /// The immediate parent request, if any.
    pub parent_request: Option<wasl_proto::RequestId>,
    /// Agents already on this call path.
    pub path: Vec<wasl_proto::AgentId>,
    /// Distance from the root.
    pub hop: u32,
}

// ---------------------------------------------------------------------
// 10. RuntimeProvider — the get-or-create seam (runtime.md §10)
// ---------------------------------------------------------------------

/// Get-or-create the target for a context/agent identity. Object-safe: node
/// config wiring (deferred to Phase-3) selects between the two providers behind
/// this trait.
#[async_trait::async_trait]
pub trait RuntimeProvider: Send + Sync {
    /// Get-or-create the target for a `key` (`TargetKey == context_id`).
    /// Idempotent: the same key returns the same live managed target, or a fresh
    /// ephemeral wrapper. Subject to admission control.
    async fn target_for(
        &self,
        key: &TargetKey,
    ) -> Result<std::sync::Arc<dyn DeliveryTarget>, RuntimeError>;

    /// List currently-known targets (R3 `list`).
    async fn list_targets(&self) -> Vec<TargetId>;

    /// Get-or-create the target for a `key`, threading a [`SpawnContext`] to the
    /// driver so per-target protocol routing, cwd, model, posture, env overrides,
    /// and binary choice take effect at spawn time.
    ///
    /// **Default impl**: falls back to [`RuntimeProvider::target_for`] ignoring
    /// the context. Providers that understand the context (e.g.
    /// `WaslManagedRuntimeProvider`, `ProtocolRoutingProvider`) override this.
    /// Ephemeral and external-seam providers need no change — the default keeps
    /// them compiling with zero edits.
    async fn target_for_with_context(
        &self,
        key: &TargetKey,
        _context: SpawnContext,
    ) -> Result<std::sync::Arc<dyn DeliveryTarget>, RuntimeError> {
        self.target_for(key).await
    }
}

// ---------------------------------------------------------------------
// 13. RuntimeError — a DEFINED taxonomy (runtime.md §13)
// ---------------------------------------------------------------------

/// The defined runtime error taxonomy. Each variant has a distinct [`Display`]
/// and distinct match arms; there is no catch-all string error.
///
/// [`Display`]: std::fmt::Display
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum RuntimeError {
    /// A driver spawn/resume failed (process could not start, resume rejected).
    SpawnFailed {
        /// The failure detail.
        detail: String,
    },
    /// The transport IO failed (write/read on the live handle).
    Transport {
        /// The failure detail.
        detail: String,
    },
    /// The transport failed after bytes MAY already have been injected (for
    /// example a timed-out `write_all` where a partial frame may have reached
    /// the child). Callers must treat delivery certainty as `OutcomeUnknown`.
    TransportOutcomeUnknown {
        /// The failure detail.
        detail: String,
    },
    /// The target is Offline/Faulted/Stopped for the requested operation.
    Unavailable {
        /// The status that made the target unavailable.
        status: TargetStatus,
    },
    /// The operation is not supported by this target's capabilities (e.g. cancel
    /// when `!interruptible`).
    Unsupported {
        /// The unsupported operation.
        operation: String,
    },
    /// Admission control refused (target explosion / rate limit / per-sender cap).
    AdmissionRefused {
        /// The refusal detail.
        detail: String,
    },
    /// A causation cycle or max-hop was detected (§8.2).
    CausationRejected {
        /// The rejection detail.
        detail: String,
    },
    /// A referenced turn/target id was not found.
    NotFound {
        /// What was not found.
        detail: String,
    },
    /// A native gate could not be resolved (policy (ii) terminal fault, or
    /// malformed resolution).
    GateUnresolvable {
        /// The reason it could not be resolved.
        detail: String,
    },
    /// A bound was exceeded that is not admission (queue full by bytes, etc.).
    Exhausted {
        /// The exhausted-bound detail.
        detail: String,
    },
    /// The actor mailbox/target has shut down.
    Shutdown,
}

impl std::fmt::Display for RuntimeError {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            RuntimeError::SpawnFailed { detail } => {
                write!(f, "spawn failed: {detail}")
            }
            RuntimeError::Transport { detail } => {
                write!(f, "transport error: {detail}")
            }
            RuntimeError::TransportOutcomeUnknown { detail } => {
                write!(f, "transport error (outcome unknown): {detail}")
            }
            RuntimeError::Unavailable { status } => {
                write!(f, "target unavailable: {status:?}")
            }
            RuntimeError::Unsupported { operation } => {
                write!(f, "operation not supported: {operation}")
            }
            RuntimeError::AdmissionRefused { detail } => {
                write!(f, "admission refused: {detail}")
            }
            RuntimeError::CausationRejected { detail } => {
                write!(f, "causation rejected: {detail}")
            }
            RuntimeError::NotFound { detail } => {
                write!(f, "not found: {detail}")
            }
            RuntimeError::GateUnresolvable { detail } => {
                write!(f, "native gate unresolvable: {detail}")
            }
            RuntimeError::Exhausted { detail } => {
                write!(f, "resource exhausted: {detail}")
            }
            RuntimeError::Shutdown => {
                write!(f, "runtime has shut down")
            }
        }
    }
}

impl std::error::Error for RuntimeError {}

#[cfg(test)]
mod tests {
    use super::{SpawnContext, SpawnOptions};

    #[test]
    fn spawn_options_default_has_no_target_key() {
        assert!(SpawnOptions::default().target_key().is_none());
    }

    #[test]
    fn spawn_context_default_is_all_none() {
        let ctx = SpawnContext::default();
        assert!(ctx.protocol.is_none());
        assert!(ctx.cwd.is_none());
        assert!(ctx.model.is_none());
        assert!(ctx.posture.is_none());
        assert!(ctx.env.is_empty());
        assert!(ctx.setup.is_none());
        assert!(ctx.bin.is_none());
        assert!(ctx.disallowed_tools.is_empty());
    }
}
