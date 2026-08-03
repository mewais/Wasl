//! Phase-2A: the per-target actor / state machine (runtime.md §4, §7, §8).
//!
//! One tokio task per managed target. It is the single physical writer to the
//! session (§Single-writer invariant): the sole owner of the
//! `Box<dyn InteractiveTransport>`, the FIFO queue, the event journal, gate
//! state, delivery-certainty, and the transcript. It receives [`Command`]s over
//! a bounded mpsc mailbox and translates each `ProtocolEvent` into a public
//! `TurnEvent` it journals + broadcasts.
//!
//! `submit_turn`/`cancel_turn`/`shutdown`/`resolve_native_gate` go through the
//! mailbox (serialized, authoritative). `events()`/`status()` read the SHARED
//! journal/status state directly (advisory / lock-based §4.3 handoff), never the
//! mailbox — so they work even while the actor is mid-pump.

use std::collections::VecDeque;
use std::sync::atomic::{AtomicBool, AtomicU64, Ordering};
use std::sync::{Arc, Mutex};
use std::time::Duration;

use tokio::sync::{mpsc, oneshot};

use wasl_adapter::DeliveryKind;
use wasl_audit::{AuditEntry, EntryKind, RuntimeTurnPhase, Sink};
use wasl_proto::{AgentId, GateDecision};

use crate::config::ManagedConfig;
use crate::journal::Journal;
use crate::{
    BusyPhase, BusyPolicy, CancelReason, Causation, ComposedPrompt, DeliveryCertainty, EventSeq,
    ProtocolEvent, RuntimeClock, RuntimeError, ShutdownMode, TargetStatus, TurnEvent, TurnId,
    TurnRequest, TurnSource,
};

/// The shared audit sink handle the actor writes lifecycle/certainty entries to.
pub(crate) type SinkHandle = Arc<Mutex<dyn Sink + Send>>;

/// A command sent into the actor mailbox, paired with a reply oneshot.
pub(crate) enum Command {
    /// Admit a turn; reply with the admission receipt (Started/Queued/Rejected*).
    /// `req` is boxed to keep the mpsc message small (the `TurnRequest` carries a
    /// full `Delivery` + causation path).
    Submit {
        req: Box<TurnRequest>,
        policy: BusyPolicy,
        turn_id: TurnId,
        reply: oneshot::Sender<crate::TurnReceipt>,
    },
    /// Cancel a turn by id.
    Cancel {
        turn_id: TurnId,
        reason: CancelReason,
        reply: oneshot::Sender<Result<(), RuntimeError>>,
    },
    /// Shut the target down.
    Shutdown {
        mode: ShutdownMode,
        reply: oneshot::Sender<Result<(), RuntimeError>>,
    },
    /// Resolve a native gate (§9 option (i)).
    ResolveGate {
        turn_id: TurnId,
        decision: GateDecision,
        reply: oneshot::Sender<Result<(), RuntimeError>>,
    },
    /// Answer a mid-turn `ToolStarted`-shaped bidirectional callback (Phase-2C):
    /// the actor sends `value` over the transport via
    /// `InteractiveTransport::answer_tool_call` — the tool-channel counterpart of
    /// `ResolveGate`, reached ONLY through this mailbox (single writer).
    AnswerToolCall {
        turn_id: TurnId,
        value: String,
        reply: oneshot::Sender<Result<(), RuntimeError>>,
    },
    /// A no-reply nudge (sent by `events()` on subscribe) asking the actor to
    /// re-poll the transport now — the scripted transport cannot signal a late
    /// `push_event`, so a fresh subscriber pokes the actor to pick it up promptly
    /// without the actor busy-polling.
    Poke,
}

/// A turn tracked by the actor (active or queued).
struct TrackedTurn {
    id: TurnId,
    source: TurnSource,
    kind: DeliveryKind,
    prompt: ComposedPrompt,
    causation: Causation,
    certainty: DeliveryCertainty,
}

/// Cancellation state for the currently active turn.
///
/// `cancel_turn(active)` is intentionally split into two phases:
/// 1) acknowledge the caller promptly and request transport interruption, then
/// 2) WAIT for the cancelled turn's own terminal transport event before clearing
///    `active` and promoting the queue.
///
/// This avoids misattributing a late terminal from the old turn to a newly
/// activated queued turn on the same transport.
struct PendingCancellation {
    turn_id: TurnId,
    reason: CancelReason,
    /// Runtime-clock instant when we entered "interrupting and awaiting
    /// terminal confirmation".
    since: std::time::Instant,
    /// Some callers/tests rely on `cancel_turn` surfacing a cancellation event
    /// promptly even though interruption is best-effort. We emit once and track
    /// whether finalization still needs to emit it.
    emitted_turn_cancelled: bool,
}

/// The shared, lock-free-ish state `events()`/`status()` read without the mailbox.
pub(crate) struct Shared {
    pub(crate) journal: Mutex<Journal>,
    pub(crate) status: Mutex<TargetStatus>,
    /// True while the actor is parked on mailbox receive and a `Poke` can wake
    /// it to re-poll transport events.
    pub(crate) poke_needed: AtomicBool,
}

impl Shared {
    fn set_status(&self, status: TargetStatus) {
        *self.status.lock().unwrap() = status;
    }
}

/// The actor's owned (single-writer) state.
pub(crate) struct Actor {
    key: String,
    self_identity: AgentId,
    capabilities: crate::RuntimeCapabilities,
    epoch: std::time::Instant,
    /// Live transport owned by the actor. This is wrapped in `Option` so `stop()`
    /// can release it immediately without requiring the actor task itself to
    /// terminate first.
    transport: Option<Box<dyn crate::InteractiveTransport>>,
    config: ManagedConfig,
    clock: Arc<dyn RuntimeClock>,
    sink: SinkHandle,
    shared: Arc<Shared>,
    mailbox: mpsc::Receiver<Command>,
    audit_seq: Arc<AtomicU64>,

    /// The active turn, if any.
    active: Option<TrackedTurn>,
    /// The FIFO queue behind the active turn.
    queue: VecDeque<TrackedTurn>,
    /// Session is ready (SessionReady seen).
    ready: bool,
    /// The native session id, if learned.
    native: Option<crate::NativeSessionId>,
    /// The transport is dead / target terminal — stop pumping.
    terminal: bool,
    /// True once termination has been initiated for the current transport and a
    /// terminal event is pending. While set, command handling rejects ordinary
    /// mailbox mutations instead of racing teardown.
    transport_terminating: bool,
    /// Draining (finish active, admit nothing).
    draining: bool,
    /// Awaiting a native gate.
    awaiting_gate: bool,
    /// The name of the most-recent still-running tool call (set on `ToolStarted`,
    /// cleared on `ToolFinished`/terminal). Used by `answer_tool_call` (Phase-2C)
    /// to answer a mid-turn `ToolStarted`-shaped bidirectional callback with the
    /// tool's own name.
    running_tool: Option<String>,
    /// Active-turn cancellation is in progress and we are waiting for that SAME
    /// turn's terminal transport event before queue promotion.
    cancelling: Option<PendingCancellation>,
    /// Turn-lifecycle events pumped while idle (before the owning turn was
    /// submitted), replayed once the turn becomes active (§7.4).
    pending_events: VecDeque<ProtocolEvent>,
    /// Parked: a turn's events are queued but no turn is submitted yet.
    idle_parked: bool,
    /// The transport has been drained to `None` while idle (no active turn): an
    /// idle session is quiescent, so wait on the mailbox only — do not spin.
    idle_drained: bool,
    /// Last observed transport progress (event pump or successful injection), in
    /// runtime-clock time so deterministic tests can drive watchdog behavior.
    last_transport_progress: std::time::Instant,
}

/// Which pump outcome the loop should take next.
enum PumpNext {
    /// Keep pumping (an active turn is in flight).
    Pump,
    /// Idle / terminal — only service the mailbox.
    Wait,
}

const WATCHDOG_GUARD_TIMEOUT: Duration = Duration::from_secs(60);

impl Actor {
    fn transport_mut(&mut self) -> &mut dyn crate::InteractiveTransport {
        self.transport
            .as_deref_mut()
            .expect("transport must exist until stop/process-exit transitions")
    }

    fn release_transport(&mut self) {
        self.transport = None;
    }

    #[allow(clippy::too_many_arguments)]
    pub(crate) fn new(
        key: String,
        self_identity: AgentId,
        capabilities: crate::RuntimeCapabilities,
        transport: Box<dyn crate::InteractiveTransport>,
        config: ManagedConfig,
        clock: Arc<dyn RuntimeClock>,
        sink: SinkHandle,
        shared: Arc<Shared>,
        mailbox: mpsc::Receiver<Command>,
        audit_seq: Arc<AtomicU64>,
    ) -> Self {
        let epoch = clock.now();
        Actor {
            key,
            self_identity,
            capabilities,
            epoch,
            transport: Some(transport),
            config,
            clock,
            sink,
            shared,
            mailbox,
            audit_seq,
            active: None,
            queue: VecDeque::new(),
            ready: false,
            native: None,
            terminal: false,
            transport_terminating: false,
            draining: false,
            awaiting_gate: false,
            running_tool: None,
            cancelling: None,
            pending_events: VecDeque::new(),
            idle_parked: false,
            idle_drained: false,
            last_transport_progress: epoch,
        }
    }

    /// The actor's run loop.
    pub(crate) async fn run(mut self) {
        loop {
            match self.pump_mode() {
                PumpNext::Pump if !self.ready => {
                    self.shared.poke_needed.store(false, Ordering::Release);
                    // Still `Starting` (§7.1 row 582 / §7.4 row 612): bias
                    // TOWARD the transport when both branches are
                    // simultaneously ready. This restores a deterministic
                    // "SessionReady is always observed before an admission
                    // decision that races it" ordering (which every existing
                    // caller/test implicitly depends on) WITHOUT sacrificing
                    // mailbox responsiveness: `biased` only affects which
                    // branch wins when BOTH are ready at once — it does not
                    // stop `mailbox.recv()` from being polled/serviced whenever
                    // `next_event()` is genuinely still pending (the normal
                    // case for a real driver's handshake/startup). This is
                    // safe against Issue B's own busy-loop failure mode (a
                    // biased branch that is `Ready` FOREVER, e.g. a scripted
                    // transport whose steps are exhausted) because that
                    // precondition requires an ACTIVE turn to persist
                    // indefinitely; `Starting` has no active turn by
                    // definition and is always a brief, one-time window that
                    // ends the moment `SessionReady`/`ProcessExited` is
                    // observed (`self.ready` flips, or `self.terminal` does).
                    tokio::select! {
                        biased;
                        ev = self
                            .transport
                            .as_mut()
                            .expect("transport must exist while pumping")
                            .next_event() => {
                            match ev {
                                Some(ev) => {
                                    self.idle_drained = false;
                                    self.transport_terminating = false;
                                    self.last_transport_progress = self.clock.now();
                                    self.on_pumped_event(ev).await;
                                }
                                None => {
                                    // A real driver's own idle-poll timeout (NOT
                                    // "nothing is ever coming" — see
                                    // `pump_mode`'s doc comment). Deliberately do
                                    // NOT set `idle_drained` here: that flag may
                                    // only ever park the actor once `self.ready`,
                                    // never before.
                                    //
                                    // Explicitly drain the mailbox here rather
                                    // than relying on `select!`'s branch
                                    // fairness: under `biased`, `next_event()`
                                    // is ALWAYS tried first, so if it keeps
                                    // resolving `Ready` (even `None`) with no
                                    // internal await point — e.g. a scripted
                                    // test double whose script is exhausted
                                    // before ever reaching `.ready()` — the
                                    // mailbox branch would NEVER be polled at
                                    // all, starving every command
                                    // indefinitely. A direct, non-blocking
                                    // `try_recv()` here cannot itself be
                                    // cancelled mid-flight and guarantees a
                                    // pending command is always found.
                                    if self.drain_one_ready_command().await {
                                        break;
                                    }
                                }
                            }
                        }
                        cmd = self.mailbox.recv() => {
                            match cmd {
                                Some(c) => self.on_command(c).await,
                                None => break,
                            }
                        }
                    }
                }
                PumpNext::Pump => {
                    self.shared.poke_needed.store(false, Ordering::Release);
                    // Race transport pumping against mailbox commands so control
                    // paths are serviced even while a slow event wait is in
                    // flight.
                    tokio::select! {
                        ev = self
                            .transport
                            .as_mut()
                            .expect("transport must exist while pumping")
                            .next_event() => {
                            match ev {
                                Some(ev) => {
                                    // Real activity: keep pumping.
                                    self.idle_drained = false;
                                    self.transport_terminating = false;
                                    self.last_transport_progress = self.clock.now();
                                    self.on_pumped_event(ev).await;
                                }
                                None if self.active.is_some() => {
                                    // A turn is in flight but the transport produced
                                    // nothing this poll. Do NOT block here on a nested,
                                    // un-raced `mailbox.recv().await` — that would
                                    // reintroduce Issue B one level deeper.
                                    //
                                    // Also do NOT simply "loop back and let the next
                                    // `select!` iteration's own fairness sort it
                                    // out": VERIFIED EMPIRICALLY (a real bug found
                                    // implementing this fix, via
                                    // `cancel_and_crash.rs`'s `busy_forever_script()`
                                    // tests) that `tokio::select!`'s per-invocation
                                    // "random" branch choice does NOT reliably give
                                    // the mailbox branch a fair chance when
                                    // `next_event()` keeps resolving `Ready` (even
                                    // `None`) with no internal await point — 85,000+
                                    // iterations were observed with ZERO mailbox
                                    // commands ever serviced. This is a real,
                                    // legitimate scenario (a scripted test double
                                    // whose script is exhausted while a turn stays
                                    // active — "busy forever" — not just a
                                    // hypothetical). Explicitly, unconditionally
                                    // drain one ready mailbox command here via a
                                    // direct, non-blocking `try_recv()` (which cannot
                                    // itself be cancelled mid-flight) so a pending
                                    // command is NEVER missed regardless of
                                    // `select!`'s internal fairness heuristics.
                                    if self.drain_one_ready_command().await {
                                        break;
                                    }
                                }
                                None => {
                                    // Idle and the transport is quiescent: an idle session
                                    // produces nothing until a turn is submitted, so PARK
                                    // on the mailbox (no spin) rather than re-poll.
                                    self.idle_drained = true;
                                }
                            }
                        }
                        cmd = self.mailbox.recv() => {
                            match cmd {
                                Some(c) => self.on_command(c).await,
                                None => break,
                            }
                        }
                    }
                }
                PumpNext::Wait => {
                    // Idle-parked / idle-drained / terminal: only the mailbox makes
                    // progress. A submit resets `idle_drained` so the actor pumps
                    // the newly-injected turn's events.
                    self.shared.poke_needed.store(true, Ordering::Release);
                    match self.mailbox.recv().await {
                        Some(c) => self.on_command(c).await,
                        None => break,
                    }
                }
            }
        }
    }

    /// Drain at most one already-ready mailbox command via a non-blocking
    /// `try_recv()`. Used from the `next_event() == None` arms of the pump
    /// `select!`s as a GUARANTEED safety net that does not depend on
    /// `tokio::select!`'s branch-fairness heuristics (verified empirically to
    /// be unreliable here — see the call sites' doc comments): a plain
    /// `try_recv()` cannot itself be cancelled mid-flight and always finds an
    /// already-buffered command regardless of how `select!` would have
    /// scheduled the two branches. Returns `true` if the mailbox is closed
    /// (all `Sender`s dropped) and the actor's run loop must exit.
    async fn drain_one_ready_command(&mut self) -> bool {
        match self.mailbox.try_recv() {
            Ok(cmd) => {
                self.on_command(cmd).await;
                false
            }
            Err(mpsc::error::TryRecvError::Empty) => {
                // Genuinely nothing to do yet. Yield so a transport that
                // keeps saying `None` (a scripted "busy forever" test double,
                // or in principle a slow real driver) cooperates with the
                // scheduler instead of pinning the executor at 100% CPU and
                // starving sibling tasks — notably whichever task is about to
                // send the very command this loop exists to catch.
                tokio::task::yield_now().await;
                false
            }
            Err(mpsc::error::TryRecvError::Disconnected) => true,
        }
    }

    /// Decide whether to pump the transport or wait on the mailbox.
    ///
    /// Terminal ⇒ only serve the mailbox (submits get `RejectedUnavailable`).
    /// Idle-parked ⇒ a turn's lifecycle events are queued in the transport but no
    /// turn has been submitted yet; wait for the submit (do NOT eat the turn's
    /// events, §7.4). Otherwise pump: observe `SessionReady`, drive the active
    /// turn, and notice an idle-time `ProcessExited` (§7.4).
    fn pump_mode(&mut self) -> PumpNext {
        if self.terminal {
            return PumpNext::Wait;
        }
        if self.active.is_some() {
            return PumpNext::Pump;
        }
        if !self.ready {
            // Still `Starting`: NEVER park here. In practice `idle_drained`
            // cannot yet be true at this point (the `run()` loop's biased
            // `!self.ready` branch deliberately never sets it — see its doc
            // comment), so this is belt-and-suspenders defense-in-depth
            // against the same real bug that motivated that change: a `None`
            // from `next_event()` before `SessionReady` is a real driver's own
            // internal idle-poll timeout (see `claude_stream.rs`/
            // `opencode_acp.rs`'s `pull_one` doc comments: a real
            // `system/init` can legitimately land several seconds after that
            // poll interval, due to the CLI's own MCP-server connection
            // time), NOT a signal that the transport will never produce
            // anything else. Treating it the same as the post-ready idle case
            // would park the actor on the mailbox FOREVER, structurally
            // unable to ever observe the real `SessionReady`/`ProcessExited`
            // that eventually arrives (nothing resets `idle_drained` while
            // `!self.ready`, since a submit during `Starting` is queued, not
            // started — it never calls `start_turn`, the only other place
            // that clears the flag). Verified by
            // `tests/starting_phase_idle_timeout.rs`.
            return PumpNext::Pump;
        }
        // Idle: park (wait on the mailbox) once the transport is drained or a
        // turn's events are queued awaiting submission — no idle spin.
        if self.idle_parked || self.idle_drained {
            return PumpNext::Wait;
        }
        PumpNext::Pump
    }

    /// Dispatch a freshly-pumped transport event. While idle (no active turn) a
    /// turn-lifecycle event belongs to a not-yet-submitted turn: buffer it and
    /// PARK (§7.4) rather than consume it against a nonexistent turn. `ready`/
    /// exit events are always processed.
    async fn on_pumped_event(&mut self, ev: ProtocolEvent) {
        let is_lifecycle = matches!(
            ev,
            ProtocolEvent::SessionReady { .. } | ProtocolEvent::ProcessExited { .. }
        );
        if self.active.is_none() && self.ready && !is_lifecycle {
            // A turn event with no active turn and a ready session: it is the
            // pre-queued script for the NEXT turn. Stash + park until a submit.
            self.pending_events.push_back(ev);
            self.idle_parked = true;
            return;
        }
        self.on_protocol_event(ev).await;
    }

    /// Replay any events buffered while idle-parked into the now-active turn
    /// (called right after a turn is started/activated).
    async fn drain_pending(&mut self) {
        self.idle_parked = false;
        while let Some(ev) = self.pending_events.pop_front() {
            // `Box::pin` breaks the `advance → drain → on_protocol_event`
            // async-fn cycle (a replayed terminal event can advance the queue,
            // which may drain again); the recursion depth is bounded by the
            // pending buffer + queue length.
            Box::pin(self.on_protocol_event(ev)).await;
            if self.active.is_none() {
                // The replayed event terminated the turn; stop replaying into a
                // gone turn.
                break;
            }
        }
    }

    // ---------------------------------------------------------------
    // Command handling (mailbox path).
    // ---------------------------------------------------------------

    async fn on_command(&mut self, cmd: Command) {
        self.shared.poke_needed.store(false, Ordering::Release);
        self.maybe_mark_stalled_transport_fault();
        if self.transport_terminating {
            self.reject_during_termination(cmd);
            return;
        }
        match cmd {
            Command::Submit {
                req,
                policy,
                turn_id,
                reply,
            } => {
                let receipt = self.admit(*req, policy, turn_id).await;
                let _ = reply.send(receipt);
            }
            Command::Cancel {
                turn_id,
                reason,
                reply,
            } => {
                let res = self.cancel(turn_id, reason).await;
                let _ = reply.send(res);
            }
            Command::Shutdown { mode, reply } => {
                let res = self.shutdown(mode).await;
                let _ = reply.send(res);
            }
            Command::Poke => {
                // No-op: returning from the mailbox wait makes the run loop
                // re-poll the transport (picking up a late `push_event`).
            }
            Command::ResolveGate {
                turn_id,
                decision,
                reply,
            } => {
                let res = self.resolve_gate(turn_id, decision).await;
                let _ = reply.send(res);
            }
            Command::AnswerToolCall {
                turn_id,
                value,
                reply,
            } => {
                let res = self.answer_tool_call(turn_id, value).await;
                let _ = reply.send(res);
            }
        }
    }

    fn maybe_mark_stalled_transport_fault(&mut self) {
        if self.terminal || self.active.is_none() {
            return;
        }
        if let Some(cancel) = self.cancelling.as_ref() {
            if self.clock.now().saturating_duration_since(cancel.since) >= WATCHDOG_GUARD_TIMEOUT {
                // While waiting for cancel confirmation, never silently activate
                // queued work on the same transport after a timeout. Without
                // protocol-level turn ids, a late terminal from the old turn is
                // indistinguishable from the next turn's terminal signal.
                self.transport_terminating = true;
                self.on_process_exited(None);
                return;
            }
        }
        if self
            .clock
            .now()
            .saturating_duration_since(self.last_transport_progress)
            >= WATCHDOG_GUARD_TIMEOUT
        {
            self.transport_terminating = true;
            self.on_process_exited(None);
        }
    }

    fn reject_during_termination(&mut self, cmd: Command) {
        let status = self.shared.status.lock().unwrap().clone();
        match cmd {
            Command::Submit { reply, .. } => {
                let _ = reply.send(crate::TurnReceipt::RejectedUnavailable {
                    status: status.clone(),
                });
            }
            Command::Cancel { reply, .. }
            | Command::Shutdown { reply, .. }
            | Command::ResolveGate { reply, .. }
            | Command::AnswerToolCall { reply, .. } => {
                let _ = reply.send(Err(RuntimeError::Unavailable {
                    status: status.clone(),
                }));
            }
            Command::Poke => {}
        }
    }

    /// The atomic admission decision (§7.1) evaluated against authoritative state.
    async fn admit(
        &mut self,
        req: TurnRequest,
        policy: BusyPolicy,
        turn_id: TurnId,
    ) -> crate::TurnReceipt {
        use crate::TurnReceipt;

        // Unavailable states refuse (§7.1 row 1).
        if self.terminal {
            return TurnReceipt::RejectedUnavailable {
                status: self.shared.status.lock().unwrap().clone(),
            };
        }

        // Causation cycle / max-hop (§8.2) — a cycle-rejected turn never injects.
        if let Err(_e) = self.check_causation(&req.causation) {
            return TurnReceipt::RejectedUnavailable {
                status: self.shared.status.lock().unwrap().clone(),
            };
        }

        // Starting (session not yet ready, §7.1 row 582): the target has no
        // active turn to be "busy" with yet — a raw session isn't `Idle` until
        // its `SessionReady` is observed. Hold the submit in the queue
        // unconditionally (regardless of `policy`; there is nothing to
        // Reject/relate-to yet) and let it be promoted the moment `SessionReady`
        // arrives (`on_protocol_event`'s `SessionReady` arm pops the queue via
        // `advance_after_terminal`, mirroring row 612's "if Starting → Idle").
        // `self.ready` only ever flips false→true once (never resets), so
        // `!self.ready` implies `self.active.is_none()` by construction.
        if !self.ready {
            debug_assert!(
                self.active.is_none(),
                "admit: !ready implies no active turn (ready never resets)"
            );
            return self.enqueue(req, turn_id, None);
        }

        // Busy? (active turn present). Extract the identity + root up front so the
        // immutable borrow does not outlive the `&mut self` enqueue call.
        let active_info = self
            .active
            .as_ref()
            .map(|a| (a.id.clone(), a.causation.root_request.clone()));
        if let Some((active_turn, active_root)) = active_info {
            if self.draining {
                // Drain admits nothing new (§7.3).
                return TurnReceipt::RejectedBusy { active_turn };
            }
            match &policy {
                BusyPolicy::Reject => {
                    return TurnReceipt::RejectedBusy { active_turn };
                }
                BusyPolicy::Queue { .. } => {
                    return self.enqueue(req, turn_id, Some(active_turn));
                }
                BusyPolicy::QueueRelated { .. } => {
                    if active_root == req.causation.root_request {
                        return self.enqueue(req, turn_id, Some(active_turn));
                    }
                    return TurnReceipt::RejectedBusy { active_turn };
                }
            }
        }

        // Idle: start the turn now.
        self.start_turn(req, turn_id).await
    }

    /// Enqueue a submit that cannot start immediately. `active_turn` is the
    /// currently-running turn to report in a bounds-exceeded `RejectedBusy`, or
    /// `None` when the target is still `Starting` (no active turn exists yet) —
    /// in which case bounds-exceeded instead reports the current (advisory)
    /// status via `RejectedUnavailable`, since there is no active turn id to
    /// attach to `RejectedBusy`.
    fn enqueue(
        &mut self,
        req: TurnRequest,
        turn_id: TurnId,
        active_turn: Option<TurnId>,
    ) -> crate::TurnReceipt {
        use crate::TurnReceipt;
        // Bounds: queue length + bytes (§11).
        let prompt = self.compose(&req);
        let bytes: usize =
            self.queue.iter().map(|t| t.prompt.0.len()).sum::<usize>() + prompt.0.len();
        if self.queue.len() >= self.config.queue_max_len || bytes > self.config.queue_max_bytes {
            return match active_turn {
                Some(active_turn) => TurnReceipt::RejectedBusy { active_turn },
                None => TurnReceipt::RejectedUnavailable {
                    status: self.shared.status.lock().unwrap().clone(),
                },
            };
        }
        let position = self.queue.len();
        let tracked = TrackedTurn {
            id: turn_id.clone(),
            source: req.source.clone(),
            kind: req.delivery.kind,
            prompt,
            causation: req.causation.clone(),
            certainty: DeliveryCertainty::NotInjected,
        };
        self.queue.push_back(tracked);
        self.emit(TurnEvent::TurnQueued {
            turn_id: turn_id.clone(),
            position,
        });
        self.refresh_status();
        TurnReceipt::Queued { turn_id, position }
    }

    /// Start `req` as the active turn (Idle → Busy): mint first event, inject.
    async fn start_turn(&mut self, req: TurnRequest, turn_id: TurnId) -> crate::TurnReceipt {
        use crate::TurnReceipt;
        let prompt = self.compose(&req);
        let tracked = TrackedTurn {
            id: turn_id.clone(),
            source: req.source.clone(),
            kind: req.delivery.kind,
            prompt: prompt.clone(),
            causation: req.causation.clone(),
            certainty: DeliveryCertainty::NotInjected,
        };
        self.active = Some(tracked);
        self.idle_drained = false;

        // TurnAccepted is this turn's first journalled event → first_seq.
        let first_seq = self.emit(TurnEvent::TurnAccepted {
            turn_id: turn_id.clone(),
            source: req.source.clone(),
        });
        // Audit the admission `Started` phase. For a `command` this is DEFERRED
        // so the FIRST durable append for a command is the write-ahead `Injected`
        // one that gates injection (§8.1) — a failing durable append must prevent
        // injection, not be spent on an earlier `Started` write.
        if req.delivery.kind != DeliveryKind::Command {
            self.audit(
                RuntimeTurnPhase::Started,
                DeliveryCertainty::NotInjected,
                &turn_id,
            );
        }
        self.set_busy(BusyPhase::Dispatching);

        // Inject (with the write-ahead-durable command carve-out, §8.1).
        match self.inject_active().await {
            Ok(()) => {
                // Replay any transport events pre-queued (and parked) before this
                // turn was submitted (§7.4).
                self.drain_pending().await;
            }
            Err((certainty, error)) => {
                // Injection failed: emit the turn's terminal failure and advance.
                self.emit_failed_terminal(certainty, error);
                self.idle_parked = false;
                if certainty == DeliveryCertainty::OutcomeUnknown {
                    self.transport_terminating = true;
                    self.on_process_exited(None);
                } else {
                    self.advance_after_terminal().await;
                }
            }
        }

        TurnReceipt::Started { turn_id, first_seq }
    }

    /// Inject the active turn into the transport (§8.1 certainty transitions).
    /// Returns `Err((certainty, error))` if the turn failed before/at injection
    /// (the caller emits the terminal failure + advances — no recursion here).
    async fn inject_active(&mut self) -> Result<(), (DeliveryCertainty, String)> {
        // INVARIANT: an active turn is always present. Both callers set
        // `self.active = Some(..)` immediately before awaiting this
        // (`start_turn` line ~425, `advance_after_terminal` line ~678) and never
        // take it in between. `.expect()` documents the invariant and keeps the
        // LINE covered with no dead closure/arm (the `Option::expect` call runs on
        // every reachable path; the panic-on-`None` lives in std, not this crate),
        // so no uncovered line/function/region is introduced — the compile-time
        // structural equivalent of a documented `unreachable!`.
        let (kind, prompt, turn_id) = {
            let a = self
                .active
                .as_ref()
                .expect("inject_active: callers set an active turn first");
            (a.kind, a.prompt.clone(), a.id.clone())
        };

        if kind == DeliveryKind::Command {
            // WRITE-AHEAD DURABLE (§8.1): append Injected BEFORE send_turn; only
            // inject if the durable append succeeds.
            if self
                .audit_try(
                    RuntimeTurnPhase::Injected,
                    DeliveryCertainty::Injected,
                    &turn_id,
                )
                .is_err()
            {
                // No injection without durability (FailedBeforeInjection — safe).
                return Err((
                    DeliveryCertainty::FailedBeforeInjection,
                    "write-ahead durable append failed".to_string(),
                ));
            }
        }

        // Actually inject.
        match self.transport_mut().send_turn(&prompt).await {
            Ok(()) => {
                self.last_transport_progress = self.clock.now();
                if let Some(a) = self.active.as_mut() {
                    a.certainty = DeliveryCertainty::Injected;
                }
                if kind != DeliveryKind::Command {
                    // Non-command: audit Injected AFTER send (no write-ahead
                    // ordering required, §8.1 / §11e).
                    self.audit(
                        RuntimeTurnPhase::Injected,
                        DeliveryCertainty::Injected,
                        &turn_id,
                    );
                }
                Ok(())
            }
            Err(e) => {
                let certainty = if matches!(e, RuntimeError::TransportOutcomeUnknown { .. }) {
                    DeliveryCertainty::OutcomeUnknown
                } else {
                    DeliveryCertainty::FailedBeforeInjection
                };
                Err((certainty, format!("transport error before injection: {e}")))
            }
        }
    }

    // ---------------------------------------------------------------
    // Protocol-event handling (pump path, §7.4).
    // ---------------------------------------------------------------

    async fn on_protocol_event(&mut self, ev: ProtocolEvent) {
        match ev {
            ProtocolEvent::SessionReady { native } => {
                self.native = native.clone();
                self.ready = true;
                self.emit(TurnEvent::SessionReady { native });
                if self.active.is_none() {
                    // §7.1 row 582 / §7.4 row 612: "if Starting → Idle". Any
                    // turn submitted while still `Starting` was held in the
                    // queue (`admit`'s `!self.ready` branch); promote it now.
                    // `advance_after_terminal` also handles the empty-queue
                    // case correctly (→ `go_idle`, emitting the required
                    // `StatusChanged`), so this is the single call needed
                    // either way.
                    self.advance_after_terminal().await;
                } else {
                    self.refresh_status();
                }
            }
            ProtocolEvent::TurnAccepted { .. } => {
                if self.is_cancelling_active() {
                    return;
                }
                // The public TurnAccepted was emitted at admission; move phase.
                if self.active.is_some() {
                    self.set_busy(BusyPhase::Generating);
                }
            }
            ProtocolEvent::AssistantDelta { text } => {
                if self.is_cancelling_active() {
                    return;
                }
                if let Some(a) = self.active.as_ref() {
                    let turn_id = a.id.clone();
                    let text = self.cap_delta(text);
                    self.set_busy(BusyPhase::Generating);
                    self.emit(TurnEvent::AssistantDelta { turn_id, text });
                }
            }
            ProtocolEvent::ToolStarted { name } => {
                if self.is_cancelling_active() {
                    return;
                }
                if let Some(a) = self.active.as_ref() {
                    let turn_id = a.id.clone();
                    // Track the running tool so a mid-turn `ToolStarted`-shaped
                    // bidirectional callback can be answered by name (Phase-2C).
                    self.running_tool = Some(name.clone());
                    self.set_busy(BusyPhase::RunningTool);
                    self.emit(TurnEvent::ToolStarted { turn_id, name });
                }
            }
            ProtocolEvent::ToolFinished { name, ok } => {
                if self.is_cancelling_active() {
                    return;
                }
                if let Some(a) = self.active.as_ref() {
                    let turn_id = a.id.clone();
                    self.running_tool = None;
                    self.set_busy(BusyPhase::Generating);
                    self.emit(TurnEvent::ToolFinished { turn_id, name, ok });
                }
            }
            ProtocolEvent::NativeGateRequested { detail } => {
                if self.is_cancelling_active() {
                    return;
                }
                if let Some(a) = self.active.as_ref() {
                    let turn_id = a.id.clone();
                    self.awaiting_gate = true;
                    self.emit(TurnEvent::NativeGateRequested {
                        turn_id,
                        detail: detail.clone(),
                    });
                    self.set_awaiting_gate(detail);
                }
            }
            ProtocolEvent::TurnCompleted { reply } => {
                if self.is_cancelling_active() {
                    self.finalize_cancelled_active().await;
                    return;
                }
                self.complete_active(reply).await;
            }
            ProtocolEvent::TurnFailed { error } => {
                if self.is_cancelling_active() {
                    self.finalize_cancelled_active().await;
                    return;
                }
                self.fail_active(DeliveryCertainty::Completed, error).await;
            }
            ProtocolEvent::ProcessExited { code } => {
                self.on_process_exited(code);
            }
            // ── Informational / observability events ──────────────────
            // These are non-turn-completing, non-gate events: emit them
            // sequenced into the journal without changing turn/gate state,
            // mirroring how AssistantDelta/ToolStarted are emitted.
            ProtocolEvent::ThinkingDelta { text } => {
                if self.is_cancelling_active() {
                    return;
                }
                if self.active.is_some() {
                    self.emit(TurnEvent::ThinkingDelta { text });
                }
            }
            ProtocolEvent::UsageUpdate { usage, context_pct } => {
                if self.is_cancelling_active() {
                    return;
                }
                self.emit(TurnEvent::UsageUpdate { usage, context_pct });
            }
            ProtocolEvent::PlanUpdated { entries } => {
                if self.is_cancelling_active() {
                    return;
                }
                self.emit(TurnEvent::PlanUpdated { entries });
            }
            ProtocolEvent::ModesUpdated {
                available_modes,
                current_mode,
            } => {
                if self.is_cancelling_active() {
                    return;
                }
                self.emit(TurnEvent::ModesUpdated {
                    available_modes,
                    current_mode,
                });
            }
            ProtocolEvent::ModelChanged { model } => {
                if self.is_cancelling_active() {
                    return;
                }
                self.emit(TurnEvent::ModelChanged { model });
            }
            ProtocolEvent::SlashCommandsUpdated { commands } => {
                if self.is_cancelling_active() {
                    return;
                }
                self.emit(TurnEvent::SlashCommandsUpdated { commands });
            }
            ProtocolEvent::Compaction { info } => {
                if self.is_cancelling_active() {
                    return;
                }
                self.emit(TurnEvent::Compaction { info });
            }
        }
    }

    async fn complete_active(&mut self, reply: String) {
        // DEFENSIVE — KEPT LIVE (do NOT convert to `unreachable!`): §7.4 line 622
        // mandates that a stray/duplicate `TurnCompleted` referencing no active
        // turn be IGNORED ("hostile-input hardening, R2 2B scripts"). A
        // misbehaving transport can emit a completion before `SessionReady`/before
        // any submit (reaching here with `active == None`, since `on_pumped_event`
        // only parks non-lifecycle events once `ready`). Panicking here would turn
        // a spec-required ignore into an actor-killing crash. Unreachable by the
        // current frozen suite → documented allowed residual (see AGENTS.md).
        let turn = match self.active.take() {
            Some(t) => t,
            None => return, // stray completion (§7.4 line 622) — ignored.
        };
        self.audit(
            RuntimeTurnPhase::Completed,
            DeliveryCertainty::Completed,
            &turn.id,
        );
        self.emit(TurnEvent::TurnCompleted {
            turn_id: turn.id,
            reply,
            certainty: DeliveryCertainty::Completed,
        });
        self.awaiting_gate = false;
        self.running_tool = None;
        self.cancelling = None;
        self.advance_after_terminal().await;
    }

    /// Emit a turn's terminal failure (audit + `TurnFailed`) WITHOUT advancing —
    /// the caller controls the queue-advance loop (avoids async recursion).
    ///
    /// INVARIANT: an active turn is always present. Every caller establishes it
    /// immediately before: `start_turn` (`self.active = Some(..)` then the inject
    /// `Err` arm) / `advance_after_terminal` (`self.active = Some(next)` then the
    /// inject `Err` arm) / `fail_active` (its own `is_none()` stray-guard returns
    /// first). The `None` arm is therefore structurally unreachable — a
    /// control-flow invariant, NOT a hostile-transport guard (that lives in the
    /// callers `complete_active`/`fail_active`, kept live per §7.4 line 622).
    fn emit_failed_terminal(&mut self, certainty: DeliveryCertainty, error: String) {
        // `.expect()`: LINE stays covered (the take runs on every reachable call);
        // the panic-on-`None` lives in std — no dead arm/closure in this crate.
        let turn = self
            .active
            .take()
            .expect("emit_failed_terminal: every caller holds an active turn");
        self.audit(RuntimeTurnPhase::Failed, certainty, &turn.id);
        self.emit(TurnEvent::TurnFailed {
            turn_id: turn.id,
            error,
            certainty,
        });
        self.awaiting_gate = false;
        self.running_tool = None;
        self.cancelling = None;
    }

    async fn fail_active(&mut self, certainty: DeliveryCertainty, error: String) {
        // DEFENSIVE — KEPT LIVE (do NOT convert to `unreachable!`): the stray
        // `TurnFailed` counterpart of `complete_active`'s guard. §7.4 line 622
        // requires a `TurnFailed` referencing no active turn to be IGNORED
        // (hostile-input hardening). A misbehaving transport can emit one before
        // any turn is active; panicking would violate the spec. This `is_none`
        // guard — NOT `emit_failed_terminal`'s (which is invariant-guaranteed
        // Some) — is the real hostile-input drop. Unreachable by the current
        // frozen suite → documented allowed residual (see AGENTS.md).
        if self.active.is_none() {
            return;
        }
        self.emit_failed_terminal(certainty, error);
        self.advance_after_terminal().await;
    }

    fn on_process_exited(&mut self, code: Option<i32>) {
        // Certainty of an injected-but-unfinished turn (§7.4 / §8.1).
        if let Some(turn) = self.active.take() {
            // INVARIANT: a turn that is *active* while the pump delivers events is
            // always `Injected`. Injection is synchronous with activation on this
            // single-writer actor: `start_turn`/`advance_after_terminal` `.await`
            // `inject_active` to completion before the run loop pumps the next
            // event — on Ok the turn is `Injected`, on Err the turn was already
            // taken + terminated (`emit_failed_terminal`), so it is never left
            // active-but-`NotInjected` for a later pumped `ProcessExited` to see.
            // (The spec's §7.4 "before injection" row (line 621) is the *idle*
            // case — no active turn — handled by the queue-drain + terminal below,
            // reached via the `if let` being false.) The invariant is asserted in
            // debug/test builds (the `debug_assert_eq!` LINE is covered — it
            // evaluates on every reachable exit-during-turn); the certainty is then
            // `OutcomeUnknown` unconditionally. Even a hypothetical future
            // invariant break degrades SAFELY (OutcomeUnknown = "may have run, do
            // not retry"), so this is strictly safer than an `unreachable!` panic.
            debug_assert_eq!(
                turn.certainty,
                DeliveryCertainty::Injected,
                "on_process_exited: an active pumped turn is always Injected \
                 (injection is synchronous with activation, §7.4/§8.1)"
            );
            let certainty = DeliveryCertainty::OutcomeUnknown;
            self.audit(RuntimeTurnPhase::Failed, certainty, &turn.id);
            self.emit(TurnEvent::TurnFailed {
                turn_id: turn.id,
                error: "process exited during turn".to_string(),
                certainty,
            });
        }
        // Queued turns expire.
        let queued: Vec<TrackedTurn> = self.queue.drain(..).collect();
        for t in queued {
            self.emit(TurnEvent::TurnExpired { turn_id: t.id });
        }
        self.terminal = true;
        self.release_transport();
        self.transport_terminating = false;
        self.awaiting_gate = false;
        self.running_tool = None;
        self.cancelling = None;
        self.shared.set_status(TargetStatus::Faulted {
            error: "process exited".to_string(),
            recoverable: false,
        });
        self.emit_status();
        self.emit(TurnEvent::ProcessExited { code });
        self.close_broadcast();
    }

    /// After a terminal event for the active turn: pop the queue → next turn or
    /// → Idle. Iterative (no async recursion): a queued turn that fails to inject
    /// emits its terminal + loops to the next queued turn.
    async fn advance_after_terminal(&mut self) {
        if self.draining {
            // Drain: finish the active turn, expire the queue, then stop (§7.3).
            let queued: Vec<TrackedTurn> = self.queue.drain(..).collect();
            for t in queued {
                self.emit(TurnEvent::TurnExpired { turn_id: t.id });
            }
            self.stop();
            return;
        }
        while let Some(next) = self.queue.pop_front() {
            // Re-evaluate causation on pop (§8.2 line 699 — an o3 "must-fix":
            // "re-evaluated when a QUEUED turn is about to start (a queued turn
            // can close a cycle later)"). SPEC-MANDATED and KEPT LIVE in ALL
            // builds (NOT a `debug_assert`/`unreachable!`): the spec requires the
            // re-check to actually run. Given the current data model (per-turn
            // `causation` is immutable and this actor's `self_identity` is fixed
            // at construction, and admission `check_causation`s EVERY submit
            // including queued ones), this re-check cannot newly-fail — so the
            // `TurnExpired`-on-pop arm is unreachable by construction today. It is
            // deliberately retained as spec-required defense-in-depth against a
            // future in which causation/identity could change between admission
            // and pop. Documented allowed residual (see AGENTS.md).
            if self.check_causation(&next.causation).is_err() {
                self.emit(TurnEvent::TurnExpired { turn_id: next.id });
                continue;
            }
            let req_source = next.source.clone();
            let turn_id = next.id.clone();
            let kind = next.kind;
            self.active = Some(next);
            self.emit(TurnEvent::TurnAccepted {
                turn_id: turn_id.clone(),
                source: req_source,
            });
            if kind != DeliveryKind::Command {
                self.audit(
                    RuntimeTurnPhase::Started,
                    DeliveryCertainty::NotInjected,
                    &turn_id,
                );
            }
            self.set_busy(BusyPhase::Dispatching);
            match self.inject_active().await {
                Ok(()) => {
                    self.drain_pending().await;
                    self.refresh_status();
                    return;
                }
                Err((certainty, error)) => {
                    // Injection failed: emit terminal + loop to the next queued.
                    self.emit_failed_terminal(certainty, error);
                    if certainty == DeliveryCertainty::OutcomeUnknown {
                        self.transport_terminating = true;
                        self.on_process_exited(None);
                        return;
                    }
                    continue;
                }
            }
        }
        // Queue empty → Idle.
        self.go_idle();
    }

    // ---------------------------------------------------------------
    // cancel_turn (§7.2).
    // ---------------------------------------------------------------

    async fn cancel(&mut self, turn_id: TurnId, reason: CancelReason) -> Result<(), RuntimeError> {
        if self.terminal {
            return Err(RuntimeError::Unavailable {
                status: self.shared.status.lock().unwrap().clone(),
            });
        }
        // Active turn?
        if let Some(active) = self.active.as_ref() {
            if active.id == turn_id {
                if self.is_cancelling_active() {
                    // Idempotent re-cancel while already interrupting this same
                    // active turn.
                    return Ok(());
                }
                if !self.caps().interruptible {
                    return Err(RuntimeError::Unsupported {
                        operation: "cancel_turn (target is not interruptible)".to_string(),
                    });
                }
                // → Interrupting; drive interrupt; emit TurnCancelled.
                self.shared.set_status(TargetStatus::Interrupting {
                    active_turn: turn_id.clone(),
                    since: self.now_ts(),
                });
                self.emit_status();
                let _ = self.transport_mut().interrupt().await;
                self.cancelling = Some(PendingCancellation {
                    turn_id: turn_id.clone(),
                    reason,
                    since: self.clock.now(),
                    emitted_turn_cancelled: false,
                });
                // Emit cancellation promptly (optimistic) for API responsiveness,
                // but keep the active turn pinned until its REAL terminal event
                // arrives from the transport.
                self.emit(TurnEvent::TurnCancelled {
                    turn_id: turn_id.clone(),
                    reason,
                });
                if let Some(cancel) = self.cancelling.as_mut() {
                    cancel.emitted_turn_cancelled = true;
                }
                return Ok(());
            }
        }
        // Queued turn? remove it (dequeue only), emit TurnCancelled.
        if let Some(pos) = self.queue.iter().position(|t| t.id == turn_id) {
            self.queue.remove(pos);
            self.emit(TurnEvent::TurnCancelled { turn_id, reason });
            self.refresh_status();
            return Ok(());
        }
        // Completed / unknown turn → Ok(()) no-op (§7.2 LOCKED).
        Ok(())
    }

    // ---------------------------------------------------------------
    // shutdown (§7.3).
    // ---------------------------------------------------------------

    async fn shutdown(&mut self, mode: ShutdownMode) -> Result<(), RuntimeError> {
        if self.terminal {
            return Ok(());
        }
        match mode {
            ShutdownMode::Immediate => {
                // Already-cancelling-and-awaiting-confirmation: `cancel()`
                // already emitted a `TurnCancelled` (with its own reason) for
                // this exact turn; do not emit a second one with `Shutdown` as
                // the reason for the same turn.
                let already_cancelled = self.is_cancelling_active();
                if let Some(turn) = self.active.take() {
                    // An injected-but-unfinished `command` at shutdown is
                    // OutcomeUnknown (§7.3); audit it so the durable evidence is
                    // preserved, then surface the cancellation.
                    let certainty = if turn.kind == DeliveryKind::Command
                        && turn.certainty == DeliveryCertainty::Injected
                    {
                        DeliveryCertainty::OutcomeUnknown
                    } else {
                        turn.certainty
                    };
                    self.audit(RuntimeTurnPhase::Failed, certainty, &turn.id);
                    if !already_cancelled {
                        self.emit(TurnEvent::TurnCancelled {
                            turn_id: turn.id,
                            reason: CancelReason::Shutdown,
                        });
                    }
                }
                self.cancelling = None;
                let queued: Vec<TrackedTurn> = self.queue.drain(..).collect();
                for t in queued {
                    self.emit(TurnEvent::TurnExpired { turn_id: t.id });
                }
                self.stop();
                Ok(())
            }
            ShutdownMode::Drain => {
                if let Some(active) = self.active.as_ref() {
                    let active_turn = active.id.clone();
                    self.draining = true;
                    self.shared.set_status(TargetStatus::Draining {
                        active_turn,
                        since: self.now_ts(),
                    });
                    self.emit_status();
                    Ok(())
                } else {
                    self.stop();
                    Ok(())
                }
            }
        }
    }

    fn stop(&mut self) {
        self.terminal = true;
        self.release_transport();
        self.transport_terminating = false;
        self.draining = false;
        self.awaiting_gate = false;
        self.running_tool = None;
        self.cancelling = None;
        self.emit(TurnEvent::ProcessExited { code: None });
        self.shared.set_status(TargetStatus::Stopped);
        self.emit_status();
        self.close_broadcast();
    }

    // ---------------------------------------------------------------
    // resolve_native_gate (§9 option (i)).
    // ---------------------------------------------------------------

    async fn resolve_gate(
        &mut self,
        turn_id: TurnId,
        decision: GateDecision,
    ) -> Result<(), RuntimeError> {
        if !self.awaiting_gate {
            return Err(RuntimeError::GateUnresolvable {
                detail: "no native gate is pending".to_string(),
            });
        }
        // INVARIANT: `awaiting_gate == true` implies `active == Some`. It is set
        // true ONLY inside the `NativeGateRequested` handler's `if let Some(a) =
        // self.active` (so active was present at set), and EVERY path that clears
        // the active turn (`complete_active`, `emit_failed_terminal`,
        // `on_process_exited`, `stop`) also clears `awaiting_gate`. Having passed
        // the `!self.awaiting_gate` guard above, `active` is therefore Some — the
        // `None` arm is a structurally-unreachable control-flow invariant.
        // `.expect()` documents it and keeps the LINE covered with no dead
        // arm/closure (panic-on-`None` lives in std).
        let active_id = self
            .active
            .as_ref()
            .expect("resolve_gate: awaiting_gate implies an active turn (set/cleared together)")
            .id
            .clone();
        if active_id != turn_id {
            return Err(RuntimeError::NotFound {
                detail: format!("turn {turn_id:?} does not hold the pending gate"),
            });
        }
        // Answer the CLI's prompt over the transport (single writer, §9 line 718).
        let answer = ComposedPrompt(match &decision {
            GateDecision::Approve => "approve".to_string(),
            GateDecision::Deny { reason } => format!("deny: {reason}"),
        });
        if let Err(err) = self.transport_mut().send_turn(&answer).await {
            if matches!(err, RuntimeError::TransportOutcomeUnknown { .. }) {
                self.transport_terminating = true;
                self.on_process_exited(None);
            }
            return Err(err);
        }
        self.awaiting_gate = false;
        self.emit(TurnEvent::NativeGateResolved { turn_id, decision });
        // Back to Busy (the turn proceeds).
        self.set_busy(BusyPhase::Generating);
        Ok(())
    }

    // ---------------------------------------------------------------
    // answer_tool_call (Phase-2C — the tool-channel counterpart of §9's gate
    // answer-back; mirrors `resolve_gate` exactly but funnels the answer through
    // `InteractiveTransport::answer_tool_call` instead of `send_turn`).
    // ---------------------------------------------------------------

    async fn answer_tool_call(
        &mut self,
        turn_id: TurnId,
        value: String,
    ) -> Result<(), RuntimeError> {
        // No tool call is blocking the turn — nothing to answer (the tool-channel
        // analogue of `resolve_gate`'s `!awaiting_gate` guard).
        let Some(name) = self.running_tool.clone() else {
            return Err(RuntimeError::NotFound {
                detail: "no tool call is awaiting an answer".to_string(),
            });
        };
        // Correlate by turn id: the answer must target the turn that holds the
        // running tool call. `running_tool == Some` implies an active turn (it is
        // set ONLY inside the `ToolStarted` handler's `if let Some(a) =
        // self.active`, and every active-clearing path also clears it), so the
        // active turn is present here.
        let active_id = self
            .active
            .as_ref()
            .expect("answer_tool_call: running_tool implies an active turn (set/cleared together)")
            .id
            .clone();
        if active_id != turn_id {
            return Err(RuntimeError::NotFound {
                detail: format!("turn {turn_id:?} does not hold the running tool call"),
            });
        }
        // Answer the tool call over the transport (single writer): the actor is
        // the sole caller, reached only through its mailbox — never a raw second
        // handle. The unidirectional drivers' default no-op simply returns Ok.
        self.transport_mut().answer_tool_call(&name, &value).await?;
        Ok(())
    }

    // ---------------------------------------------------------------
    // Causation checks (§8.2).
    // ---------------------------------------------------------------

    fn check_causation(&self, causation: &Causation) -> Result<(), RuntimeError> {
        if causation.hop > self.config.max_hop {
            return Err(RuntimeError::CausationRejected {
                detail: format!(
                    "hop {} exceeds MAX_HOP {}",
                    causation.hop, self.config.max_hop
                ),
            });
        }
        if causation.path.contains(&self.self_identity) {
            return Err(RuntimeError::CausationRejected {
                detail: format!(
                    "cycle: target identity {:?} already on causation path",
                    self.self_identity
                ),
            });
        }
        Ok(())
    }

    // ---------------------------------------------------------------
    // Status / journal / audit helpers.
    // ---------------------------------------------------------------

    fn caps(&self) -> crate::RuntimeCapabilities {
        self.capabilities
    }

    fn emit(&self, event: TurnEvent) -> EventSeq {
        self.shared.journal.lock().unwrap().append(event)
    }

    fn emit_status(&self) {
        let status = self.shared.status.lock().unwrap().clone();
        self.emit(TurnEvent::StatusChanged { status });
    }

    fn set_busy(&mut self, phase: BusyPhase) {
        // INVARIANT: only ever called with an active turn. Every call site holds
        // one: `start_turn`/`advance_after_terminal` set `active = Some(..)`
        // first; the `on_protocol_event` arms are inside `if let Some(a) =
        // self.active`/`if self.active.is_some()`; `resolve_gate` has confirmed
        // `active` Some. The `None` arm is a structurally-unreachable control-flow
        // invariant; `.expect()` documents it and keeps the LINE covered with no
        // dead arm/closure (panic-on-`None` lives in std).
        let active = self
            .active
            .as_ref()
            .expect("set_busy: only called while a turn is active");
        let (active_turn, source) = (active.id.clone(), active.source.clone());
        let status = TargetStatus::Busy {
            active_turn,
            source,
            phase,
            since: self.now_ts(),
            queued: self.queue.len(),
        };
        let changed = *self.shared.status.lock().unwrap() != status;
        self.shared.set_status(status);
        if changed {
            self.emit_status();
        }
    }

    fn set_awaiting_gate(&mut self, detail: crate::NativeGateDetail) {
        if let Some(a) = self.active.as_ref() {
            self.shared.set_status(TargetStatus::AwaitingGate {
                active_turn: a.id.clone(),
                detail,
                since: self.now_ts(),
                queued: self.queue.len(),
            });
            self.emit_status();
        }
    }

    fn refresh_status(&mut self) {
        if let Some(a) = self.active.as_ref() {
            let phase = match &*self.shared.status.lock().unwrap() {
                TargetStatus::Busy { phase, .. } => *phase,
                _ => BusyPhase::Generating,
            };
            let status = TargetStatus::Busy {
                active_turn: a.id.clone(),
                source: a.source.clone(),
                phase,
                since: self.now_ts(),
                queued: self.queue.len(),
            };
            self.shared.set_status(status);
        }
    }

    fn go_idle(&mut self) {
        self.shared.set_status(TargetStatus::Idle {
            since: self.now_ts(),
        });
        self.emit_status();
    }

    fn now_ts(&self) -> crate::Timestamp {
        // Informational; the ACTUAL waits use RuntimeClock durations. Render a
        // stable, monotonic-derived label (not wall clock — deterministic).
        let d = self.clock.now().saturating_duration_since(self.epoch);
        format!("t+{}ms", d.as_millis())
    }

    fn cap_delta(&self, mut text: String) -> String {
        if text.len() > self.config.delta_max_bytes {
            let mut boundary = self.config.delta_max_bytes;
            while boundary > 0 && !text.is_char_boundary(boundary) {
                boundary -= 1;
            }
            text.truncate(boundary);
            text.push_str("…[truncated]");
        }
        text
    }

    fn compose(&self, req: &TurnRequest) -> ComposedPrompt {
        // Mirror wasl-adapter's compose_prompt header (§6 ComposedPrompt).
        let header = match &req.source {
            TurnSource::Agent { from } => format!("[{:?} from {}]", req.delivery.kind, from.0),
            TurnSource::LocalHuman => format!("[{:?} from local]", req.delivery.kind),
        };
        ComposedPrompt(format!("{header}\n\n{}", req.delivery.text))
    }

    fn audit(&self, phase: RuntimeTurnPhase, certainty: DeliveryCertainty, turn_id: &TurnId) {
        let _ = self.audit_try(phase, certainty, turn_id);
    }

    fn audit_try(
        &self,
        phase: RuntimeTurnPhase,
        certainty: DeliveryCertainty,
        turn_id: &TurnId,
    ) -> Result<(), RuntimeError> {
        let seq = self.audit_seq.fetch_add(1, Ordering::SeqCst);
        let entry = AuditEntry {
            seq,
            kind: EntryKind::RuntimeTurn,
            actor: self.self_identity.clone(),
            targets: vec![AgentId(self.key.clone())],
            summary: format!("managed turn {} phase change", turn_id.0),
            decision: None,
            certainty: Some(certainty),
            phase: Some(phase),
        };
        self.sink
            .lock()
            .unwrap()
            .append(&entry)
            .map_err(|e| RuntimeError::Transport {
                detail: format!("audit sink: {e}"),
            })
    }

    fn close_broadcast(&self) {
        self.shared.journal.lock().unwrap().close();
    }

    fn is_cancelling_active(&self) -> bool {
        let Some(cancel) = self.cancelling.as_ref() else {
            return false;
        };
        matches!(self.active.as_ref(), Some(a) if a.id == cancel.turn_id)
    }

    async fn finalize_cancelled_active(&mut self) {
        let Some(cancel) = self.cancelling.take() else {
            return;
        };
        let Some(active) = self.active.as_ref() else {
            return;
        };
        if active.id != cancel.turn_id {
            self.cancelling = Some(cancel);
            return;
        }

        let _ = self.active.take();
        self.awaiting_gate = false;
        // Hardening-3 §0.1 item 1: clear tool-running belief when cancellation is
        // confirmed (same transition where active is cleared) so a later
        // `answer_tool_call` cannot route to the wrong turn.
        self.running_tool = None;
        if !cancel.emitted_turn_cancelled {
            self.emit(TurnEvent::TurnCancelled {
                turn_id: cancel.turn_id,
                reason: cancel.reason,
            });
        }
        self.advance_after_terminal().await;
    }
}
