//! Phase-2A: the managed provider + per-target `DeliveryTarget` (runtime.md §10).
//!
//! [`WaslManagedRuntimeProvider`] owns/creates per-`TargetKey` actors (get-or-
//! create, `TargetKey == context_id`), applies global admission control, and
//! injects the spec-mandated seams (`RuntimeClock`, `Arc<Mutex<dyn Sink>>`,
//! [`ManagedConfig`]). Each [`ManagedTarget`] is the public `DeliveryTarget`
//! handle; `submit_turn` does idempotency dedup OUTSIDE the mailbox (§4.4) then
//! sends an admission command through the actor.

use std::collections::HashMap;
use std::sync::atomic::{AtomicBool, AtomicU64, Ordering};
use std::sync::{Arc, Mutex};

use tokio::sync::{mpsc, oneshot};
use tokio::task::JoinHandle;

use wasl_proto::{AgentId, GateDecision};

use crate::actor::{Actor, Command, Shared, SinkHandle};
use crate::config::ManagedConfig;
use crate::journal::Journal;
use crate::{
    BusyPolicy, CancelReason, DeliveryTarget, EventSeq, IdempotencyKey, InteractiveProtocol,
    NativeGateResolver, NativeSessionId, RuntimeCapabilities, RuntimeClock, RuntimeError,
    ShutdownMode, SpawnContext, SpawnOptions, TargetId, TargetKey, TargetStatus, ToolCallAnswerer,
    TurnEventStream, TurnId, TurnReceipt, TurnRequest,
};

/// One live managed target: the public `DeliveryTarget` handle plus a handle onto
/// the actor mailbox and the shared journal/status state.
pub(crate) struct ManagedTarget {
    target_id: TargetId,
    key: TargetKey,
    caps: RuntimeCapabilities,
    mailbox: mpsc::Sender<Command>,
    shared: Arc<Shared>,
    /// Idempotency dedup map, consulted BEFORE the mailbox (§4.4).
    idem: Mutex<HashMap<IdempotencyKey, Arc<IdemSlot>>>,
    turn_counter: AtomicU64,
    /// Aborts the actor task when the last handle drops.
    _actor: AbortOnDrop,
}

/// A per-key idempotency slot: the first (winning) submit fills `receipt`, later
/// same-key submits await `notify` then read it (§4.4 — dedup outside the mailbox
/// so two concurrent same-key submits can never both enqueue).
struct IdemSlot {
    receipt: Mutex<Option<TurnReceipt>>,
    notify: tokio::sync::Notify,
}

/// Aborts a spawned actor task on drop so a dropped target does not leak a
/// forever-polling task (the pump re-polls the scripted transport).
struct AbortOnDrop(JoinHandle<()>);

impl Drop for AbortOnDrop {
    fn drop(&mut self) {
        self.0.abort();
    }
}

impl std::fmt::Debug for ManagedTarget {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("ManagedTarget")
            .field("target_id", &self.target_id)
            .field("key", &self.key)
            .finish_non_exhaustive()
    }
}

impl ManagedTarget {
    fn mint_turn_id(&self) -> TurnId {
        let n = self.turn_counter.fetch_add(1, Ordering::SeqCst);
        TurnId(format!("{}#turn-{}", self.key.0, n))
    }
}

#[async_trait::async_trait]
impl DeliveryTarget for ManagedTarget {
    fn target_id(&self) -> &TargetId {
        &self.target_id
    }

    fn capabilities(&self) -> RuntimeCapabilities {
        self.caps
    }

    async fn status(&self) -> TargetStatus {
        self.shared.status.lock().unwrap().clone()
    }

    async fn submit_turn(&self, req: TurnRequest, busy_policy: BusyPolicy) -> TurnReceipt {
        // --- Idempotency dedup OUTSIDE the mailbox (§4.4) ---
        if let Some(idem_key) = req.idempotency_key.clone() {
            enum Role {
                Winner(Arc<IdemSlot>),
                Loser(Arc<IdemSlot>),
            }
            let role = {
                let mut map = self.idem.lock().unwrap();
                match map.get(&idem_key) {
                    Some(slot) => Role::Loser(slot.clone()),
                    None => {
                        let slot = Arc::new(IdemSlot {
                            receipt: Mutex::new(None),
                            notify: tokio::sync::Notify::new(),
                        });
                        map.insert(idem_key.clone(), slot.clone());
                        Role::Winner(slot)
                    }
                }
            };
            match role {
                Role::Winner(slot) => {
                    let receipt = self.admit_via_mailbox(req, busy_policy).await;
                    *slot.receipt.lock().unwrap() = Some(receipt.clone());
                    slot.notify.notify_waiters();
                    receipt
                }
                Role::Loser(slot) => {
                    // Wait for the winner to record its receipt, then wrap it.
                    loop {
                        let notified = slot.notify.notified();
                        if let Some(original) = slot.receipt.lock().unwrap().clone() {
                            return TurnReceipt::Duplicate {
                                original: Box::new(original),
                            };
                        }
                        notified.await;
                    }
                }
            }
        } else {
            self.admit_via_mailbox(req, busy_policy).await
        }
    }

    async fn events(&self, after_seq: EventSeq) -> TurnEventStream {
        // §4.3 handoff: under ONE journal-lock acquisition, subscribe the live
        // receiver FIRST then copy the retained tail; the window (ring floor →
        // last seq) is captured under the same lock for a lag `Truncated`.
        let (handoff, window) = {
            let journal = self.shared.journal.lock().unwrap();
            let handoff = journal.subscribe(after_seq);
            let window = (journal.floor(), journal.last_seq());
            (handoff, window)
        };
        // Nudge the actor to re-poll the transport now: a subscriber may be
        // waiting on a step the scripted transport was `push_event`ed but cannot
        // signal. The live receiver is already subscribed, so any resulting event
        // reaches this stream. Non-blocking: a full mailbox / stopped actor is
        // harmless (a busy actor will re-poll on its backstop anyway).
        if self.shared.poke_needed.load(Ordering::Acquire) {
            let _ = self.mailbox.try_send(Command::Poke);
        }
        handoff.into_stream(window)
    }

    async fn cancel_turn(&self, turn_id: TurnId, reason: CancelReason) -> Result<(), RuntimeError> {
        let (tx, rx) = oneshot::channel();
        self.mailbox
            .send(Command::Cancel {
                turn_id,
                reason,
                reply: tx,
            })
            .await
            .map_err(|_| RuntimeError::Shutdown)?;
        rx.await.map_err(|_| RuntimeError::Shutdown)?
    }

    async fn shutdown(&self, mode: ShutdownMode) -> Result<(), RuntimeError> {
        let (tx, rx) = oneshot::channel();
        self.mailbox
            .send(Command::Shutdown { mode, reply: tx })
            .await
            .map_err(|_| RuntimeError::Shutdown)?;
        rx.await.map_err(|_| RuntimeError::Shutdown)?
    }
}

#[async_trait::async_trait]
impl NativeGateResolver for ManagedTarget {
    async fn resolve_native_gate(
        &self,
        turn_id: TurnId,
        decision: GateDecision,
    ) -> Result<(), RuntimeError> {
        let (tx, rx) = oneshot::channel();
        self.mailbox
            .send(Command::ResolveGate {
                turn_id,
                decision,
                reply: tx,
            })
            .await
            .map_err(|_| RuntimeError::Shutdown)?;
        rx.await.map_err(|_| RuntimeError::Shutdown)?
    }
}

#[async_trait::async_trait]
impl ToolCallAnswerer for ManagedTarget {
    async fn answer_tool_call(&self, turn_id: TurnId, value: String) -> Result<(), RuntimeError> {
        // Route through the SAME serialized mailbox as `resolve_native_gate`: the
        // single actor task that owns the transport is the sole writer; this
        // handle never exposes a raw second writer to the caller (Phase-2C).
        let (tx, rx) = oneshot::channel();
        self.mailbox
            .send(Command::AnswerToolCall {
                turn_id,
                value,
                reply: tx,
            })
            .await
            .map_err(|_| RuntimeError::Shutdown)?;
        rx.await.map_err(|_| RuntimeError::Shutdown)?
    }
}

impl ManagedTarget {
    async fn admit_via_mailbox(&self, req: TurnRequest, policy: BusyPolicy) -> TurnReceipt {
        // Status is ADVISORY ONLY (§4.3) and must never gate admission — the
        // actor's own `active`/`terminal`/`draining` state (checked inside
        // `Actor::admit`, serialized through the single mailbox) is the sole
        // authority for Busy/Unavailable rejection. A status-snapshot
        // shortcut here would race the authoritative decision and could
        // double-admit or wrongly reject. Always round-trip through the
        // mailbox.
        let turn_id = self.mint_turn_id();
        let (tx, rx) = oneshot::channel();
        // The two error arms below (mailbox `send` err / reply `recv` err) are the
        // ACTOR-DIED / crash-recovery paths and are KEPT LIVE (do NOT convert to
        // `unreachable!`). Under normal operation the actor task outlives this
        // handle (the `ManagedTarget` holds the mailbox `Sender`, so `recv()`
        // never returns `None`; even a terminal actor still replies
        // `RejectedUnavailable` from `admit`), so they are unreachable by the
        // frozen suite. But a tokio task CAN panic — including on one of the
        // `unreachable!()` control-flow guards elsewhere in this crate, or a
        // future bug — which drops `mrx`/the reply `tx`. These arms are the
        // correct place that surfaces "the actor died" as `RejectedUnavailable`
        // rather than propagating a panic to the caller. Removing them would
        // delete real crash-handling. Documented allowed residual (see AGENTS.md).
        if self
            .mailbox
            .send(Command::Submit {
                req: Box::new(req),
                policy,
                turn_id,
                reply: tx,
            })
            .await
            .is_err()
        {
            return TurnReceipt::RejectedUnavailable {
                status: TargetStatus::Stopped,
            };
        }
        match rx.await {
            Ok(receipt) => receipt,
            Err(_) => TurnReceipt::RejectedUnavailable {
                status: TargetStatus::Stopped,
            },
        }
    }
}

/// The managed provider: Wasl spawns/owns a long-lived process per target via an
/// [`InteractiveProtocol`] driver, owns the actor generically, applies admission
/// control (runtime.md §10).
pub struct WaslManagedRuntimeProvider<P: InteractiveProtocol> {
    protocol: Arc<P>,
    config: ManagedConfig,
    clock: Arc<dyn RuntimeClock>,
    sink: SinkHandle,
    /// Registry of live targets (get-or-create; idempotent per key, §10).
    targets: Mutex<HashMap<TargetKey, Arc<ManagedTarget>>>,
    /// Shared audit-seq source so every entry across targets is monotonic.
    audit_seq: Arc<AtomicU64>,
}

impl<P: InteractiveProtocol + 'static> WaslManagedRuntimeProvider<P> {
    /// Construct with the spec-mandated seams (runtime.md §1/§10/§11/§12): the
    /// driver, the bounds/admission [`ManagedConfig`], an injected
    /// [`RuntimeClock`], and the injected audit `Sink`.
    pub fn with_seams(
        protocol: P,
        config: ManagedConfig,
        clock: Arc<dyn RuntimeClock>,
        sink: SinkHandle,
    ) -> Self {
        WaslManagedRuntimeProvider {
            protocol: Arc::new(protocol),
            config,
            clock,
            sink,
            targets: Mutex::new(HashMap::new()),
            audit_seq: Arc::new(AtomicU64::new(0)),
        }
    }

    /// Get-or-create the target for `key`, threading a [`SpawnContext`] to the
    /// driver at spawn time. The real get-or-create logic lives here; the
    /// context-less [`WaslManagedRuntimeProvider::target_for`] delegates here
    /// with a default context so all callers share one code path.
    pub async fn target_for_with_context(
        &self,
        key: &TargetKey,
        context: SpawnContext,
    ) -> Result<Arc<dyn DeliveryTarget>, RuntimeError> {
        // Idempotent get-or-create (§10): same key ⇒ same live target.
        {
            let map = self.targets.lock().unwrap();
            if let Some(existing) = map.get(key) {
                return Ok(existing.clone() as Arc<dyn DeliveryTarget>);
            }
            // Global admission control (§10): a brand-new key past the cap refuses.
            if map.len() >= self.config.max_targets_global {
                return Err(RuntimeError::AdmissionRefused {
                    detail: format!(
                        "max_targets_global {} reached",
                        self.config.max_targets_global
                    ),
                });
            }
        }
        let target = self.create_target(key, context).await?;
        // Re-check under the lock (another task may have created it concurrently).
        let mut map = self.targets.lock().unwrap();
        if let Some(existing) = map.get(key) {
            return Ok(existing.clone() as Arc<dyn DeliveryTarget>);
        }
        map.insert(key.clone(), target.clone());
        Ok(target as Arc<dyn DeliveryTarget>)
    }

    /// Get-or-create the target for `key` (`TargetKey == context_id`), applying
    /// global admission control (§10). Inherent method so callers use it without
    /// importing [`crate::RuntimeProvider`]; the trait impl forwards here.
    /// Delegates to [`Self::target_for_with_context`] with a default context.
    pub async fn target_for(
        &self,
        key: &TargetKey,
    ) -> Result<Arc<dyn DeliveryTarget>, RuntimeError> {
        self.target_for_with_context(key, SpawnContext::default()).await
    }

    /// List currently-known targets (§10, R3 `list`). Inherent; the trait impl
    /// forwards here.
    pub async fn list_targets(&self) -> Vec<TargetId> {
        self.targets
            .lock()
            .unwrap()
            .values()
            .map(|t| t.target_id.clone())
            .collect()
    }

    async fn create_target(
        &self,
        key: &TargetKey,
        context: SpawnContext,
    ) -> Result<Arc<ManagedTarget>, RuntimeError> {
        let caps = self.protocol.capabilities();
        // When `context.resume` is Some(native_id), re-attach to the existing native
        // session; when None, start fresh.  The `opts` carry all other context fields
        // (model, cwd, env, …) in both cases — the driver reads them from
        // `opts.context()` regardless of which path is taken.
        let resume_native = context.resume.clone().map(NativeSessionId);
        let opts = SpawnOptions {
            target_key: Some(key.clone()),
            context,
        };
        let transport = match resume_native {
            Some(native) => self.protocol.resume(&native, &opts).await?,
            None => self.protocol.spawn(&opts).await?,
        };

        let shared = Arc::new(Shared {
            journal: Mutex::new(Journal::new(self.config.journal_capacity)),
            status: Mutex::new(TargetStatus::Starting),
            poke_needed: AtomicBool::new(false),
        });
        let (mtx, mrx) = mpsc::channel(self.config.mailbox_capacity.max(1));

        let self_identity = AgentId(key.0.clone());
        let actor = Actor::new(
            key.0.clone(),
            self_identity,
            caps,
            transport,
            self.config.clone(),
            self.clock.clone(),
            self.sink.clone(),
            shared.clone(),
            mrx,
            self.audit_seq.clone(),
        );
        let handle = tokio::spawn(actor.run());

        let target = Arc::new(ManagedTarget {
            target_id: TargetId(key.0.clone()),
            key: key.clone(),
            caps,
            mailbox: mtx,
            shared,
            idem: Mutex::new(HashMap::new()),
            turn_counter: AtomicU64::new(0),
            _actor: AbortOnDrop(handle),
        });
        Ok(target)
    }
}

#[async_trait::async_trait]
impl<P: InteractiveProtocol + 'static> crate::RuntimeProvider for WaslManagedRuntimeProvider<P> {
    async fn target_for(&self, key: &TargetKey) -> Result<Arc<dyn DeliveryTarget>, RuntimeError> {
        WaslManagedRuntimeProvider::target_for(self, key).await
    }

    async fn target_for_with_context(
        &self,
        key: &TargetKey,
        context: SpawnContext,
    ) -> Result<Arc<dyn DeliveryTarget>, RuntimeError> {
        WaslManagedRuntimeProvider::target_for_with_context(self, key, context).await
    }

    async fn list_targets(&self) -> Vec<TargetId> {
        WaslManagedRuntimeProvider::list_targets(self).await
    }
}
