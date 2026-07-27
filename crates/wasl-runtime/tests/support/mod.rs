#![allow(dead_code)]
//! Shared Phase-2A behavioral test support for `wasl-runtime` (R1).
//!
//! Per `docs/specs/runtime.md` §14, R1's OWN behavioral tests drive the generic
//! core against an **in-`tests/`-support scripted `InteractiveTransport`** — NOT
//! the R2 `FakeLineProtocol` (that is a separate crate, Phase-2B, and depending
//! on it here would create the reverse dev-dependency the plan's cycle-fix
//! forbids). Everything in this module is test-local and touches only the PUBLIC
//! R1 surface (`crates/wasl-runtime/src/lib.rs`) plus the public types of
//! `wasl-proto` / `wasl-audit` / `wasl-adapter`.
//!
//! It provides:
//!   * [`ScriptedProtocol`] — an [`InteractiveProtocol`] whose `spawn`/`resume`
//!     hand out a [`ScriptedTransport`] scripted per target.
//!   * [`ScriptedTransport`] — an [`InteractiveTransport`] that (a) RECORDS every
//!     `send_turn`/`interrupt` call in a shared, inspectable log, and (b) yields
//!     scripted [`ProtocolEvent`]s from `next_event`, gated on a deterministic
//!     synchronization primitive (`tokio::sync::Notify` / channels — **never real
//!     sleeps**) so tests force exact interleavings.
//!   * [`RecordingSink`] / [`OrderCheckingSink`] — fake `wasl_audit::Sink`s that
//!     record `AuditEntry`s (and, for the order-checking variant, assert the
//!     write-ahead ordering of a `command` — the durable append happens strictly
//!     BEFORE `send_turn` on the transport, §8.1).
//!   * [`TestClock`] — a settable [`wasl_runtime::RuntimeClock`] so all deadlines
//!     (`BusyPolicy::Queue{max_wait}`, idle eviction) are deterministic (§1).
//!   * builder helpers for [`TurnRequest`] / [`Causation`] / [`Delivery`].
//!
//! ## FROZEN-SURFACE GAP the coder MUST close for Phase-2A (flagged, not assumed)
//!
//! The Phase-1 surface froze `WaslManagedRuntimeProvider<P: InteractiveProtocol>`
//! (src/lib.rs:750) with PRIVATE fields (`protocol, actors, admission, clock,
//! sink`) and **no constructor** — correct for a type-only phase. Phase-2A
//! behavioral tests cannot construct it without a public constructor that injects
//! the spec-mandated seams. Per the spec these seams ALL exist and are required:
//!
//!   * the injected `RuntimeClock` (runtime.md §1 lines 135-138; §4.7-D10;
//!     impl-plan "injected monotonic clock/timer seam");
//!   * the injected audit `Sink` as `Arc<Mutex<dyn Sink>>` (runtime.md §12
//!     line 797: "The actor is constructed with an `Arc<Mutex<dyn Sink>>`");
//!   * admission-control + bounds config (runtime.md §10 lines 753-757 &
//!     §11 lines 772-782: `max_targets_global`, `max_targets_per_sender`,
//!     `journal_capacity`, `idempotency_cap`/`_ttl`, `queue_max_len`/`_bytes`,
//!     idle-eviction timeout, `MAX_HOP`).
//!
//! This module therefore expects the coder to add (Phase-2A, in `src/`), and the
//! tests below reference, the following PUBLIC items. Their NAMES/shape are
//! chosen to be minimal and are cited to the spec section that requires the seam;
//! if the coder names them differently, this is the single place to re-map:
//!
//! ```ignore
//! // runtime.md §10/§11 — config with locked defaults; Phase-3 only WIRES the
//! // values, the TYPE + `Default` live here so the core validates against them.
//! pub struct ManagedConfig {
//!     pub max_targets_global: usize,
//!     pub max_targets_per_sender: usize,
//!     pub journal_capacity: usize,
//!     pub idempotency_cap: usize,
//!     pub idempotency_ttl: std::time::Duration,
//!     pub queue_max_len: usize,
//!     pub queue_max_bytes: usize,
//!     pub transcript_max_bytes: usize,
//!     pub delta_max_bytes: usize,
//!     pub idle_eviction: std::time::Duration,
//!     pub mailbox_capacity: usize,
//!     pub max_hop: u32, // MAX_HOP default 8 (§8.2)
//! }
//! impl Default for ManagedConfig { /* locked defaults */ }
//!
//! impl<P: InteractiveProtocol> WaslManagedRuntimeProvider<P> {
//!     // §12 injected Sink + §1 injected clock + §10/§11 config.
//!     pub fn with_seams(
//!         protocol: P,
//!         config: ManagedConfig,
//!         clock: std::sync::Arc<dyn RuntimeClock>,
//!         sink: std::sync::Arc<std::sync::Mutex<dyn wasl_audit::Sink + Send>>,
//!     ) -> Self;
//! }
//!
//! // §9 native-gate resolution "through the single-writer actor" (option (i)).
//! // Exposed on the concrete target (downcast-free: an inherent method on the
//! // Arc<dyn DeliveryTarget> the managed provider returns is awkward, so the
//! // spec's `resolve_native_gate(turn_id, decision)` is exposed as an EXTRA
//! // async method the managed target offers). The tests reach it via a public
//! // trait the managed target implements:
//! #[async_trait::async_trait]
//! pub trait NativeGateResolver {
//!     async fn resolve_native_gate(
//!         &self, turn_id: TurnId, decision: wasl_proto::GateDecision,
//!     ) -> Result<(), RuntimeError>;
//! }
//! ```
//!
//! If the coder disagrees with any NAME above, only this doc block + the small
//! `provider()` / `resolve_gate()` shims below need re-mapping — the assertions
//! are all spec-behavioral and name-independent.

use std::collections::VecDeque;
use std::sync::atomic::{AtomicU64, Ordering};
use std::sync::{Arc, Mutex};
use std::time::{Duration, Instant};

use futures::StreamExt;

use wasl_adapter::{Delivery, DeliveryKind};
use wasl_audit::{AuditEntry, AuditError, EntryKind, Sink};
use wasl_proto::{AgentId, RequestId};

use wasl_runtime::{
    Causation, ComposedPrompt, EventSeq, IdempotencyKey, InteractiveProtocol, InteractiveTransport,
    NativeSessionId, ProtocolEvent, RuntimeCapabilities, RuntimeClock, RuntimeError, SpawnOptions,
    StreamItem, TargetKey, TurnEvent, TurnRequest, TurnSource,
};

// =====================================================================
// TestClock — a settable RuntimeClock so every deadline is deterministic.
// (runtime.md §1 lines 135-138 — never SystemTime in the core.)
// =====================================================================

/// A settable monotonic clock. Starts at a fixed base `Instant` and advances
/// ONLY when a test calls [`TestClock::advance`], so `BusyPolicy::Queue`
/// expiry / idle-eviction fire on demand, never on a wall-clock sleep.
pub struct TestClock {
    base: Instant,
    offset: Mutex<Duration>,
}

impl TestClock {
    pub fn new() -> Arc<Self> {
        Arc::new(TestClock {
            base: Instant::now(),
            offset: Mutex::new(Duration::ZERO),
        })
    }

    /// Advance the clock by `by`; any deadline now in the past becomes due.
    pub fn advance(&self, by: Duration) {
        *self.offset.lock().unwrap() += by;
    }
}

impl RuntimeClock for TestClock {
    fn now(&self) -> Instant {
        self.base + *self.offset.lock().unwrap()
    }
}

// =====================================================================
// Fake audit Sinks (mirrors wasl-operator/tests/common/mod.rs conventions).
// =====================================================================

/// Records every entry it is given (in order); always succeeds. The recorded
/// entries carry a globally-monotonic `recv_order` so a test can assert an audit
/// append happened before/after a transport call recorded in the same clock.
#[derive(Default)]
pub struct RecordingSink {
    pub entries: Mutex<Vec<AuditEntry>>,
    pub orders: Mutex<Vec<u64>>,
    counter: Arc<AtomicU64>,
}

impl RecordingSink {
    /// Build a recording sink sharing `counter` with a transport, so the
    /// relative order of audit-appends vs. transport-calls is observable.
    pub fn with_counter(counter: Arc<AtomicU64>) -> Self {
        RecordingSink {
            entries: Mutex::new(Vec::new()),
            orders: Mutex::new(Vec::new()),
            counter,
        }
    }

    pub fn snapshot(&self) -> Vec<AuditEntry> {
        self.entries.lock().unwrap().clone()
    }

    pub fn orders(&self) -> Vec<u64> {
        self.orders.lock().unwrap().clone()
    }

    pub fn runtime_turn_entries(&self) -> Vec<AuditEntry> {
        self.entries
            .lock()
            .unwrap()
            .iter()
            .filter(|e| e.kind == EntryKind::RuntimeTurn)
            .cloned()
            .collect()
    }
}

impl Sink for RecordingSink {
    fn append(&mut self, entry: &AuditEntry) -> Result<(), AuditError> {
        let order = self.counter.fetch_add(1, Ordering::SeqCst);
        self.entries.lock().unwrap().push(entry.clone());
        self.orders.lock().unwrap().push(order);
        Ok(())
    }
}

/// A sink that FAILS the first `fail_first` appends, then succeeds. Used to
/// prove the write-ahead-durable `command` flow (§8.1): if the durable append
/// fails, `transport.send_turn` MUST NOT be called (no injection without
/// durability).
pub struct FailingSink {
    remaining_failures: Mutex<usize>,
    pub appended: Mutex<Vec<AuditEntry>>,
}

impl FailingSink {
    pub fn new(fail_first: usize) -> Self {
        FailingSink {
            remaining_failures: Mutex::new(fail_first),
            appended: Mutex::new(Vec::new()),
        }
    }

    pub fn appended(&self) -> Vec<AuditEntry> {
        self.appended.lock().unwrap().clone()
    }
}

impl Sink for FailingSink {
    fn append(&mut self, entry: &AuditEntry) -> Result<(), AuditError> {
        let mut left = self.remaining_failures.lock().unwrap();
        if *left > 0 {
            *left -= 1;
            return Err(AuditError::Sink("scripted durable-append failure".into()));
        }
        self.appended.lock().unwrap().push(entry.clone());
        Ok(())
    }
}

/// Convenience: wrap any sink as the `Arc<Mutex<dyn Sink + Send>>` the actor is
/// constructed with (runtime.md §12 line 797).
pub fn sink_arc<S: Sink + Send + 'static>(s: S) -> Arc<Mutex<dyn Sink + Send>> {
    Arc::new(Mutex::new(s))
}

// =====================================================================
// The scripted InteractiveTransport (§14, §6).
// =====================================================================

/// One recorded call the actor made on a [`ScriptedTransport`].
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum TransportCall {
    /// `send_turn(prompt)` — carries the exact `ComposedPrompt` text + a global
    /// order stamp so interleaving against audit appends is observable.
    SendTurn { prompt: String, order: u64 },
    /// `interrupt()` was called.
    Interrupt { order: u64 },
}

/// A step in a transport's script: either a canned event to yield from
/// `next_event`, or a GATE the actor must wait on before the next event is
/// released (deterministic — no sleeps).
enum ScriptStep {
    /// Yield this event immediately from the next `next_event` call.
    Emit(ProtocolEvent),
    /// Block `next_event` until `gate` is notified by the test, THEN yield
    /// `event`. This is how a test forces "an event arrives WHILE a subscriber
    /// is mid-snapshot" (§4.3 race window) without a sleep.
    Gated {
        gate: Arc<tokio::sync::Notify>,
        event: ProtocolEvent,
    },
    /// Make the NEXT `send_turn` return this error (transport failure BEFORE
    /// injection → FailedBeforeInjection, §8.1).
    FailNextSend(RuntimeError),
}

/// The shared, inspectable state behind a [`ScriptedTransport`] — kept behind an
/// `Arc<Mutex<_>>` so the test holds a handle after the actor takes ownership of
/// the transport (mirrors the FakeInvoker pattern in
/// `wasl-adapter-codex/tests/adapter.rs`).
#[derive(Default)]
pub struct TransportShared {
    pub calls: Mutex<Vec<TransportCall>>,
    fail_next_send: Mutex<Option<RuntimeError>>,
}

impl TransportShared {
    /// Every `send_turn`/`interrupt` the actor made, in order.
    pub fn calls(&self) -> Vec<TransportCall> {
        self.calls.lock().unwrap().clone()
    }

    /// How many `send_turn` calls occurred (the injection count — the key
    /// idempotency / no-double-inject assertion, §4.4).
    pub fn send_turn_count(&self) -> usize {
        self.calls
            .lock()
            .unwrap()
            .iter()
            .filter(|c| matches!(c, TransportCall::SendTurn { .. }))
            .count()
    }
}

/// A scriptable `InteractiveTransport`. The actor is its SOLE holder (§6 /
/// single-writer invariant); the test drives/inspects it through the shared
/// handle it kept before handing ownership to the provider.
pub struct ScriptedTransport {
    shared: Arc<TransportShared>,
    steps: Arc<Mutex<VecDeque<ScriptStep>>>,
    order_counter: Arc<AtomicU64>,
    /// `interrupt()` resolves the active turn: when the test scripts an
    /// `interrupt_ack`, the transport yields the acknowledging event on the next
    /// `next_event`. A `Notify` released by `interrupt()`.
    interrupt_ack: Arc<tokio::sync::Notify>,
}

#[async_trait::async_trait]
impl InteractiveTransport for ScriptedTransport {
    async fn send_turn(&mut self, prompt: &ComposedPrompt) -> Result<(), RuntimeError> {
        if let Some(err) = self.shared.fail_next_send.lock().unwrap().take() {
            return Err(err);
        }
        let order = self.order_counter.fetch_add(1, Ordering::SeqCst);
        self.shared
            .calls
            .lock()
            .unwrap()
            .push(TransportCall::SendTurn {
                prompt: prompt.0.clone(),
                order,
            });
        Ok(())
    }

    async fn next_event(&mut self) -> Option<ProtocolEvent> {
        loop {
            let step = self.steps.lock().unwrap().pop_front();
            match step {
                Some(ScriptStep::Emit(ev)) => return Some(ev),
                Some(ScriptStep::Gated { gate, event }) => {
                    gate.notified().await;
                    return Some(event);
                }
                Some(ScriptStep::FailNextSend(err)) => {
                    *self.shared.fail_next_send.lock().unwrap() = Some(err);
                    continue;
                }
                None => return None,
            }
        }
    }

    async fn interrupt(&mut self) -> Result<(), RuntimeError> {
        let order = self.order_counter.fetch_add(1, Ordering::SeqCst);
        self.shared
            .calls
            .lock()
            .unwrap()
            .push(TransportCall::Interrupt { order });
        self.interrupt_ack.notify_waiters();
        Ok(())
    }
}

// =====================================================================
// The scripted InteractiveProtocol — hands out per-target scripted transports.
// =====================================================================

/// Builder + provider driver for scripted transports. A test constructs one
/// script per `TargetKey` (via [`ProtocolScript`]) and installs it; `spawn`
/// hands the matching [`ScriptedTransport`] to the actor and returns its shared
/// handle for inspection.
pub struct ScriptedProtocol {
    caps: RuntimeCapabilities,
    /// Pending scripts keyed by target; consumed on `spawn`.
    scripts: Mutex<std::collections::HashMap<String, PreparedTransport>>,
    /// The order counter shared with any [`RecordingSink`] built from
    /// [`ScriptedProtocol::shared_counter`], so audit-vs-transport ordering is
    /// globally comparable.
    order_counter: Arc<AtomicU64>,
    /// Fail `spawn` for these keys (drive `SpawnFailed`, §8.1
    /// FailedBeforeInjection / RejectedUnavailable).
    spawn_failures: Mutex<std::collections::HashSet<String>>,
    /// When true, `spawn` yields to the scheduler BEFORE consuming a script, so
    /// two concurrent `target_for(SAME key)` creators can both pass the
    /// provider's initial "not present" check before either inserts — forcing the
    /// get-or-create re-check-under-lock branch (§10). A real async driver's
    /// `spawn` naturally has such a yield point; the scripted double is otherwise
    /// synchronous, which would close the race window.
    slow_spawn: std::sync::atomic::AtomicBool,
    /// The `SpawnOptions::target_key()` seen on the MOST RECENT `spawn`/`resume`
    /// call (Phase-4 wiring proof: `runtime.md` §6 line 547 documents
    /// `target_key` as a `SpawnOptions` field; `create_target` must populate it
    /// so a real driver like `ClaudeStreamProtocol` can derive a context-scoped
    /// native identity). `None` until the first `spawn`/`resume`.
    last_spawn_target_key: Mutex<Option<TargetKey>>,
}

struct PreparedTransport {
    shared: Arc<TransportShared>,
    steps: Arc<Mutex<VecDeque<ScriptStep>>>,
    interrupt_ack: Arc<tokio::sync::Notify>,
}

impl ScriptedProtocol {
    /// A driver reporting a full-featured managed capability set: persistent
    /// process + conversation, structured turns, attachable, interruptible,
    /// structured provenance, resumable. (The fake is allowed to claim
    /// `interruptible`/`streaming_output` — §14: the fake proves the state
    /// machine; only a REAL driver may not claim them until captured.)
    pub fn new() -> Arc<Self> {
        Self::with_caps(RuntimeCapabilities {
            persistent_process: true,
            persistent_conversation: true,
            structured_turns: true,
            streaming_output: true,
            attachable: true,
            interruptible: true,
            structured_provenance: true,
            resumable: true,
        })
    }

    /// A driver that reports `interruptible: false` — to exercise the §7.2
    /// `cancel` on a `!interruptible` target (`Err(Unsupported)`).
    pub fn non_interruptible() -> Arc<Self> {
        Self::with_caps(RuntimeCapabilities {
            interruptible: false,
            ..Self::new().caps
        })
    }

    /// Build a driver with an explicit capability report.
    pub fn with_caps(caps: RuntimeCapabilities) -> Arc<Self> {
        Arc::new(ScriptedProtocol {
            caps,
            scripts: Mutex::new(std::collections::HashMap::new()),
            order_counter: Arc::new(AtomicU64::new(0)),
            spawn_failures: Mutex::new(std::collections::HashSet::new()),
            slow_spawn: std::sync::atomic::AtomicBool::new(false),
            last_spawn_target_key: Mutex::new(None),
        })
    }

    /// Make `spawn` yield to the scheduler before consuming a script, opening the
    /// concurrent get-or-create race window (§10 re-check-under-lock). Used by the
    /// concurrent-`target_for` test; harmless (a single-op no-op) otherwise.
    pub fn enable_slow_spawn(&self) {
        self.slow_spawn.store(true, Ordering::SeqCst);
    }

    /// The `SpawnOptions::target_key()` observed on the most recent `spawn`/
    /// `resume` call, if any — the Phase-4 wiring proof (see the struct field
    /// doc). `None` before any call.
    pub fn last_spawn_target_key(&self) -> Option<TargetKey> {
        self.last_spawn_target_key.lock().unwrap().clone()
    }

    /// The global order counter shared with a [`RecordingSink`] so
    /// audit-append vs. `send_turn` order is directly comparable (§8.1
    /// write-ahead ordering).
    pub fn shared_counter(&self) -> Arc<AtomicU64> {
        self.order_counter.clone()
    }

    /// Install a script for `key`, returning the shared transport handle the
    /// test inspects after `spawn`.
    pub fn install(&self, key: &str, script: ProtocolScript) -> ScriptHandle {
        let shared = Arc::new(TransportShared::default());
        let steps = Arc::new(Mutex::new(script.steps));
        let interrupt_ack = Arc::new(tokio::sync::Notify::new());
        self.scripts.lock().unwrap().insert(
            key.to_string(),
            PreparedTransport {
                shared: shared.clone(),
                steps: steps.clone(),
                interrupt_ack: interrupt_ack.clone(),
            },
        );
        ScriptHandle {
            shared,
            steps,
            gates: script.gates,
        }
    }

    /// Make `spawn` fail for `key` (→ `SpawnFailed`).
    pub fn fail_spawn(&self, key: &str) {
        self.spawn_failures.lock().unwrap().insert(key.to_string());
    }
}

#[async_trait::async_trait]
impl InteractiveProtocol for ScriptedProtocol {
    fn capabilities(&self) -> RuntimeCapabilities {
        self.caps
    }

    async fn spawn(
        &self,
        opts: &SpawnOptions,
    ) -> Result<Box<dyn InteractiveTransport>, RuntimeError> {
        // Optionally yield first so concurrent same-key creators overlap the
        // provider's initial check → forces the get-or-create re-check-under-lock
        // branch (§10). A real async driver's spawn has such a yield naturally.
        if self.slow_spawn.load(Ordering::SeqCst) {
            tokio::task::yield_now().await;
        }
        // Phase-4 closed the FROZEN-SURFACE GAP this comment used to describe:
        // `SpawnOptions` now exposes `target_key()` (runtime.md §6 line 547),
        // populated by `create_target`. Record it (the wiring proof
        // `spawn_options_target_key.rs` inspects via `last_spawn_target_key()`),
        // then prefer the EXACT keyed script when one was installed under that
        // key; fall back to install-order FIFO otherwise (every pre-Phase-4 test
        // installs one script per call and never relies on ordering across
        // distinct keys, so this fallback keeps them unaffected).
        *self.last_spawn_target_key.lock().unwrap() = opts.target_key().cloned();
        let key = {
            let scripts = self.scripts.lock().unwrap();
            match opts.target_key() {
                Some(k) if scripts.contains_key(&k.0) => k.0.clone(),
                _ => scripts
                    .keys()
                    .next()
                    .cloned()
                    .ok_or_else(|| RuntimeError::SpawnFailed {
                        detail: "no scripted transport installed".into(),
                    })?,
            }
        };
        if self.spawn_failures.lock().unwrap().contains(&key) {
            return Err(RuntimeError::SpawnFailed {
                detail: format!("scripted spawn failure for {key}"),
            });
        }
        let prepared = self.scripts.lock().unwrap().remove(&key).unwrap();
        Ok(Box::new(ScriptedTransport {
            shared: prepared.shared,
            steps: prepared.steps,
            order_counter: self.order_counter.clone(),
            interrupt_ack: prepared.interrupt_ack,
        }))
    }

    async fn resume(
        &self,
        _native: &NativeSessionId,
        opts: &SpawnOptions,
    ) -> Result<Box<dyn InteractiveTransport>, RuntimeError> {
        self.spawn(opts).await
    }
}

/// A test's inspectable handle onto one scripted transport + the gates it can
/// release to force deterministic interleavings.
pub struct ScriptHandle {
    pub shared: Arc<TransportShared>,
    steps: Arc<Mutex<VecDeque<ScriptStep>>>,
    gates: Vec<(String, Arc<tokio::sync::Notify>)>,
}

impl ScriptHandle {
    /// Release a named gate installed via [`ProtocolScript::gated`], unblocking
    /// the transport's `next_event` so the gated event is delivered now.
    pub fn release(&self, gate_name: &str) {
        for (name, notify) in &self.gates {
            if name == gate_name {
                notify.notify_one();
                return;
            }
        }
        panic!("no gate named {gate_name:?} in this script");
    }

    /// Append more steps to a live script (e.g. to complete a turn after
    /// asserting an intermediate state).
    pub fn push_event(&self, event: ProtocolEvent) {
        self.steps
            .lock()
            .unwrap()
            .push_back(ScriptStep::Emit(event));
    }
}

/// A fluent builder for a per-target [`ProtocolEvent`] script.
pub struct ProtocolScript {
    steps: VecDeque<ScriptStep>,
    gates: Vec<(String, Arc<tokio::sync::Notify>)>,
}

impl Default for ProtocolScript {
    fn default() -> Self {
        ProtocolScript {
            steps: VecDeque::new(),
            gates: Vec::new(),
        }
    }
}

impl ProtocolScript {
    pub fn new() -> Self {
        Self::default()
    }

    /// Emit `event` immediately when the actor next polls.
    pub fn emit(mut self, event: ProtocolEvent) -> Self {
        self.steps.push_back(ScriptStep::Emit(event));
        self
    }

    /// A convenience: `SessionReady { native }` then nothing else — a bare
    /// ready session.
    pub fn ready(self, native: Option<&str>) -> Self {
        self.emit(ProtocolEvent::SessionReady {
            native: native.map(|s| NativeSessionId(s.to_string())),
        })
    }

    /// Emit `event` only AFTER the test releases the gate named `gate_name`
    /// (deterministic interleave forcing — §4.3 race window).
    pub fn gated(mut self, gate_name: &str, event: ProtocolEvent) -> Self {
        let notify = Arc::new(tokio::sync::Notify::new());
        self.gates.push((gate_name.to_string(), notify.clone()));
        self.steps.push_back(ScriptStep::Gated {
            gate: notify,
            event,
        });
        self
    }

    /// Make the actor's next `send_turn` on this transport fail (transport
    /// error before injection → FailedBeforeInjection, §8.1).
    pub fn fail_next_send(mut self, err: RuntimeError) -> Self {
        self.steps.push_back(ScriptStep::FailNextSend(err));
        self
    }

    /// A whole happy turn: TurnAccepted → one AssistantDelta → TurnCompleted.
    pub fn happy_turn(self, delta: &str, reply: &str) -> Self {
        self.emit(ProtocolEvent::TurnAccepted { native_turn: None })
            .emit(ProtocolEvent::AssistantDelta {
                text: delta.to_string(),
            })
            .emit(ProtocolEvent::TurnCompleted {
                reply: reply.to_string(),
            })
    }
}

// =====================================================================
// Request builders.
// =====================================================================

pub fn delivery(context_id: &str, kind: DeliveryKind, text: &str) -> Delivery {
    Delivery {
        context_id: context_id.to_string(),
        from: "wasl-b".to_string(),
        kind,
        text: text.to_string(),
        data: vec![],
    }
}

pub fn root_causation(root: &str) -> Causation {
    Causation {
        root_request: RequestId(root.to_string()),
        parent_request: None,
        path: vec![],
        hop: 0,
    }
}

/// A causation on a call path that already contains `path_agents`, at `hop`.
pub fn causation_with_path(root: &str, path_agents: &[&str], hop: u32) -> Causation {
    Causation {
        root_request: RequestId(root.to_string()),
        parent_request: Some(RequestId(root.to_string())),
        path: path_agents.iter().map(|a| AgentId(a.to_string())).collect(),
        hop,
    }
}

pub fn agent_request(
    context_id: &str,
    kind: DeliveryKind,
    text: &str,
    idem: Option<&str>,
    causation: Causation,
) -> TurnRequest {
    TurnRequest {
        delivery: delivery(context_id, kind, text),
        source: TurnSource::Agent {
            from: AgentId("wasl-b".to_string()),
        },
        idempotency_key: idem.map(|k| IdempotencyKey(k.to_string())),
        causation,
    }
}

/// A simple consult root turn with no dedupe.
pub fn consult(context_id: &str, text: &str) -> TurnRequest {
    agent_request(
        context_id,
        DeliveryKind::Consult,
        text,
        None,
        root_causation("req-root"),
    )
}

/// A simple command root turn with no dedupe (`command` → write-ahead durable).
pub fn command(context_id: &str, text: &str) -> TurnRequest {
    agent_request(
        context_id,
        DeliveryKind::Command,
        text,
        None,
        root_causation("req-root"),
    )
}

// =====================================================================
// Stream draining helpers (deterministic, bounded — no sleeps).
// =====================================================================

/// Drain up to `n` [`StreamItem`]s from a stream, stopping early if the stream
/// ends. Bounded by a generous `tokio::time::timeout` ONLY as a hang-guard
/// (a correct impl never hits it); the ordering itself is driven by the script,
/// not by time.
pub async fn drain_items(mut stream: wasl_runtime::TurnEventStream, n: usize) -> Vec<StreamItem> {
    let mut out = Vec::new();
    for _ in 0..n {
        match tokio::time::timeout(Duration::from_secs(5), stream.next()).await {
            Ok(Some(item)) => out.push(item),
            Ok(None) => break,
            Err(_) => break,
        }
    }
    out
}

/// Extract just the public `TurnEvent`s (dropping seq wrappers) from stream
/// items, panicking if a `Truncated` control item is encountered (use
/// [`drain_items`] directly when a test EXPECTS truncation).
pub fn events_of(items: &[StreamItem]) -> Vec<TurnEvent> {
    items
        .iter()
        .map(|it| match it {
            StreamItem::Event { event, .. } => event.clone(),
            StreamItem::Truncated { earliest, latest } => {
                panic!("unexpected Truncated{{{earliest:?},{latest:?}}} in stream")
            }
        })
        .collect()
}

/// The `(seq, event)` pairs, for gap/dup assertions.
pub fn seq_events(items: &[StreamItem]) -> Vec<(EventSeq, TurnEvent)> {
    items
        .iter()
        .filter_map(|it| match it {
            StreamItem::Event { seq, event } => Some((*seq, event.clone())),
            StreamItem::Truncated { .. } => None,
        })
        .collect()
}

/// Assert a `(seq, event)` list is strictly ascending in seq, gap-free relative
/// to its own first element, and duplicate-free (ordering guarantee §5.1).
pub fn assert_ascending_gapfree_unique(pairs: &[(EventSeq, TurnEvent)]) {
    let mut seen = std::collections::BTreeSet::new();
    let mut prev: Option<u64> = None;
    for (seq, _ev) in pairs {
        assert!(
            seen.insert(seq.0),
            "duplicate EventSeq {} in one subscription (violates exactly-once §5.1)",
            seq.0
        );
        if let Some(p) = prev {
            assert!(
                seq.0 > p,
                "EventSeq not strictly ascending: {} after {} (§5.1)",
                seq.0,
                p
            );
        }
        prev = Some(seq.0);
    }
}

/// Find the first `turn_id` carried by a `TurnAccepted` event in a list.
pub fn first_accepted_turn(events: &[TurnEvent]) -> Option<&wasl_runtime::TurnId> {
    events.iter().find_map(|e| match e {
        TurnEvent::TurnAccepted { turn_id, .. } => Some(turn_id),
        _ => None,
    })
}

/// A `TargetKey` from a context id.
pub fn key(context_id: &str) -> TargetKey {
    TargetKey(context_id.to_string())
}

// =====================================================================
// The ONE place the flagged Phase-2A constructor is called (see the module
// doc block's FROZEN-SURFACE GAP). If the coder names the constructor/config
// differently, re-map ONLY the two functions below — every suite routes
// through them.
// =====================================================================

/// Build a managed provider with the scripted driver, the given config, an
/// injected [`TestClock`], and an injected recording [`Sink`] (§10/§11/§1/§12).
///
/// COODER MUST ADD (Phase-2A, per module doc): `ManagedConfig`,
/// `WaslManagedRuntimeProvider::with_seams(protocol, config, clock, sink)`.
pub fn provider(
    protocol: Arc<ScriptedProtocol>,
    config: wasl_runtime::ManagedConfig,
    clock: Arc<TestClock>,
    sink: Arc<Mutex<dyn Sink + Send>>,
) -> wasl_runtime::WaslManagedRuntimeProvider<Arc<ScriptedProtocol>> {
    wasl_runtime::WaslManagedRuntimeProvider::with_seams(protocol, config, clock, sink)
}

/// A tiny config with tight bounds for bounds/admission tests. Uses the coder's
/// `ManagedConfig` (locked-default) as a base and overrides the fields a test
/// needs (§11 names).
pub fn tight_config() -> wasl_runtime::ManagedConfig {
    wasl_runtime::ManagedConfig {
        max_targets_global: 2,
        max_targets_per_sender: 2,
        journal_capacity: 8,
        queue_max_len: 3,
        ..wasl_runtime::ManagedConfig::default()
    }
}

/// Resolve a native gate through the single-writer actor (§9 option (i)).
///
/// CODER MUST ADD: the managed target implements
/// `wasl_runtime::NativeGateResolver::resolve_native_gate(turn_id, decision)`.
pub async fn resolve_gate(
    target: &std::sync::Arc<dyn wasl_runtime::DeliveryTarget>,
    turn_id: wasl_runtime::TurnId,
    decision: wasl_proto::GateDecision,
) -> Result<(), RuntimeError> {
    use wasl_runtime::NativeGateResolver;
    target.resolve_native_gate(turn_id, decision).await
}
