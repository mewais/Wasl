//! Agents spawning workers on demand (see [`wasl_proto::spawn`]).
//!
//! The node never launches anything: it asks its [`SpawnAuthority`] (the supervisor) to plan
//! and to launch. Between the two it raises the spawn gate on the calling session and waits for
//! the human, like a handoff. Every step is journaled under the request id, so a retried
//! `wasl.spawn` (same id) waits for the same request, and after a restart an asking request
//! keeps watching its gate and an approved one is launched (the authority's launch is
//! idempotent by request id, so it is never started twice).

use std::sync::Arc;
use std::time::Duration;

use async_trait::async_trait;
use serde::de::DeserializeOwned;
use serde::Serialize;
use tokio::sync::watch;
use wasit_control::{codes as ctl_codes, RequestOpts, RpcError, Uplink};
use wasit_core::GateReason;
use wasit_diag::{Ident, Ms};
use wasit_runtime::wire::SessionId;
use wasl_proto::diag::{emit, JournalSite, WaslEvent};
use wasl_proto::{
    codes, methods, spawn_gate, spawn_request_id, Ack, DelegateRequest, DismissAsk, DismissReply,
    DismissRequest, End, EndCode, SpawnApproval, SpawnAsk, SpawnCancel, SpawnDecision, SpawnLaunch,
    SpawnLaunched, SpawnPlan, SpawnReply, SpawnRequest, Spawnable, SpawnableAsk, TaskMode,
};

use crate::diag::rpc_error;
use crate::gate::Answer;
use crate::journal::{durable, Rec};
use crate::node::{err, now_ms, Node};
use crate::state::{GateInfo, SpawnPersist, SpawnPhase, SpawnRec};

/// A `wasl.spawn` caller waiting for a request (see [`Node::spawn_outcome`]).
struct Waiting {
    node: Node,
    id: String,
}

impl Waiting {
    fn new(node: Node, id: String) -> Self {
        if let Some(r) = node.lock().spawns.get(&id) {
            r.waiters.send_modify(|n| *n += 1);
        }
        Waiting { node, id }
    }
}

impl Drop for Waiting {
    fn drop(&mut self) {
        let abandoned = {
            let st = self.node.lock();
            match st.spawns.get(&self.id) {
                Some(r) => {
                    r.waiters.send_modify(|n| *n = n.saturating_sub(1));
                    (*r.waiters.borrow() == 0 && matches!(r.p.phase, SpawnPhase::Asking { .. }))
                        .then(|| r.waiters.subscribe())
                }
                None => None,
            }
        };
        let Some(mut waiters) = abandoned else { return };
        let node = self.node.clone();
        let id = std::mem::take(&mut self.id);
        let grace = node.inner.cfg.spawn_reattach_grace;
        tokio::spawn(async move {
            // A caller that lost its connection re-sends the call with the same request id.
            if let Ok(Ok(_)) = tokio::time::timeout(grace, waiters.wait_for(|n| *n > 0)).await {
                return;
            }
            node.withdraw_spawn(
                &id,
                End::new(EndCode::CallerGone),
                "the spawn request was withdrawn: its caller stopped waiting (the tool call was \
                 cancelled or its client disconnected)"
                    .into(),
            )
            .await
        });
    }
}

/// Who decides about and performs spawns: the supervisor. Implementations must make
/// [`SpawnAuthority::launch`] idempotent by [`SpawnLaunch::request_id`].
#[async_trait]
pub trait SpawnAuthority: Send + Sync + 'static {
    /// Check `ask` against policy and limits; resolve defaults; say whether a human must
    /// approve. Errors are final answers for the agent (`spawn_refused`, `spawn_limit`, …),
    /// except retryable ones (the supervisor is not reachable right now).
    async fn plan(&self, ask: SpawnAsk) -> Result<SpawnPlan, RpcError>;
    /// Launch an approved request (or return what an earlier launch of it produced).
    async fn launch(&self, launch: SpawnLaunch) -> Result<SpawnLaunched, RpcError>;
    /// Stop an agent the caller spawned.
    async fn dismiss(&self, ask: DismissAsk) -> Result<DismissReply, RpcError>;
    /// What the caller may spawn.
    async fn spawnable(&self, ask: SpawnableAsk) -> Result<Spawnable, RpcError>;
}

/// The authority of a daemon that is not the hub: the `wasl.supervisor.*` methods sent to the
/// hub over the connection it opened into this daemon (served there by the hub's node, which
/// has the supervisor's own authority). No hub connection ⇒ `not_connected` (retryable).
pub struct UplinkSpawnAuthority {
    uplink: Uplink,
    timeout: Duration,
}

impl UplinkSpawnAuthority {
    pub fn new(uplink: Uplink, timeout: Duration) -> Self {
        UplinkSpawnAuthority { uplink, timeout }
    }

    async fn call<P: Serialize, T: DeserializeOwned>(
        &self,
        method: &str,
        p: P,
    ) -> Result<T, RpcError> {
        // A server-side uplink routes every host to the hub connection; `host: None` makes the
        // hub serve the request itself.
        let conn = self
            .uplink
            .route(&wasit_control::HostId::from(""))
            .ok_or_else(|| {
                RpcError::new(
                    ctl_codes::NOT_CONNECTED,
                    "no supervisor is connected to this host",
                )
            })?;
        let params = serde_json::to_value(p).map_err(|e| RpcError::internal(e.to_string()))?;
        let opts = RequestOpts::new(self.timeout);
        let v = conn
            .call(method, params, opts)
            .await
            .map_err(|e| e.into_rpc())?;
        serde_json::from_value(v).map_err(|e| RpcError::internal(e.to_string()))
    }
}

#[async_trait]
impl SpawnAuthority for UplinkSpawnAuthority {
    async fn plan(&self, ask: SpawnAsk) -> Result<SpawnPlan, RpcError> {
        self.call(methods::SUPERVISOR_SPAWN_PLAN, ask).await
    }
    async fn launch(&self, launch: SpawnLaunch) -> Result<SpawnLaunched, RpcError> {
        self.call(methods::SUPERVISOR_SPAWN_LAUNCH, launch).await
    }
    async fn dismiss(&self, ask: DismissAsk) -> Result<DismissReply, RpcError> {
        self.call(methods::SUPERVISOR_DISMISS, ask).await
    }
    async fn spawnable(&self, ask: SpawnableAsk) -> Result<Spawnable, RpcError> {
        self.call(methods::SUPERVISOR_SPAWNABLE, ask).await
    }
}

fn unavailable(why: impl std::fmt::Display) -> RpcError {
    err(
        codes::SPAWN_UNAVAILABLE,
        format!("spawning is not available: {why}"),
    )
}

const NO_SUPERVISOR: &str = "no supervisor (such as Marsad) is attached to this host";

/// Longest a launch is retried while the supervisor cannot be reached.
const LAUNCH_PATIENCE: Duration = Duration::from_secs(600);

impl Node {
    /// Let `authority` decide about and perform spawns for this node's sessions (and, on the
    /// hub, for the nodes that forward `wasl.supervisor.*` to it).
    pub fn set_spawn_authority(&self, authority: Arc<dyn SpawnAuthority>) {
        *self
            .inner
            .spawner
            .write()
            .unwrap_or_else(|e| e.into_inner()) = Some(authority);
    }

    pub(crate) fn spawn_authority(&self) -> Option<Arc<dyn SpawnAuthority>> {
        self.inner
            .spawner
            .read()
            .unwrap_or_else(|e| e.into_inner())
            .clone()
    }

    fn authority(&self) -> Result<Arc<dyn SpawnAuthority>, RpcError> {
        self.spawn_authority()
            .ok_or_else(|| unavailable(NO_SUPERVISOR))
    }

    /// `wasl.supervisor.*` served by this node's own authority.
    pub(crate) fn serving_authority(&self) -> Result<Arc<dyn SpawnAuthority>, RpcError> {
        self.authority()
    }

    /// `wasl.spawn`: plan, ask the human (unless the policy allows), launch, delegate the first
    /// task. Returns when the request is finished; a retry with the same request id waits for
    /// the same request.
    pub async fn spawn(&self, req: SpawnRequest) -> Result<SpawnReply, RpcError> {
        if req.request_id.trim().is_empty() {
            return Err(RpcError::invalid_params("a request id is required"));
        }
        if req.spec.name.trim().is_empty() || req.spec.role.trim().is_empty() {
            return Err(RpcError::invalid_params(
                "a name and a role (what the new agent is for) are required",
            ));
        }
        self.local_session(&req.from)?;
        let me = self.me(&req.from);
        if let Some(rx) = self.spawn_watch(&req.request_id, &me)? {
            return self.spawn_outcome(&req.request_id, rx).await;
        }
        let (for_task, deadline_ms) = match self.spawn_deadline(&req) {
            Ok(d) => d,
            Err(why) => {
                // Final: the request would be dead on arrival.
                let end = End::new(EndCode::DeadlinePassed);
                return Err(self.refuse_spawn(&req, &me, end, why).await);
            }
        };
        let auth = self.authority()?;
        let ask = SpawnAsk {
            request_id: req.request_id.clone(),
            parent: me.clone(),
            spec: req.spec.clone(),
        };
        let plan = match self
            .patiently(self.inner.cfg.spawn_connect_wait, || auth.plan(ask.clone()))
            .await
        {
            Ok(plan) => plan,
            // The supervisor cannot be reached: nothing is recorded (nobody could be told, and
            // the agent may ask again).
            Err(e) if e.is_retryable() => {
                return Err(unavailable(format!(
                    "the supervisor cannot be reached ({})",
                    e.message
                )))
            }
            // The supervisor's final answer: recorded as the request's ending, and returned
            // to the agent as it came.
            Err(e) => {
                let end = End::caused_by(EndCode::Refused, e.code.clone());
                self.refuse_spawn(&req, &me, end, e.message.clone()).await;
                return Err(e);
            }
        };
        let phase = match plan.approval {
            SpawnApproval::Allowed => SpawnPhase::Launching {
                decision: SpawnDecision::Policy,
            },
            SpawnApproval::Ask => SpawnPhase::Asking {
                gate: GateInfo {
                    request_id: spawn_request_id(&req.request_id),
                    raised: false,
                    cursor: 0,
                    turn: false,
                },
            },
        };
        let (rx, ack) = {
            let mut st = self.lock();
            if let Some(r) = st.spawns.get(&req.request_id) {
                // A concurrent retry got here first.
                (r.watch.subscribe(), None)
            } else {
                let p = SpawnPersist {
                    request_id: req.request_id.clone(),
                    from: me,
                    spec: req.spec,
                    plan: Some(plan),
                    phase: phase.clone(),
                    updated_ms: now_ms(),
                    for_task,
                    deadline_ms,
                };
                let tx = watch::channel(phase.clone()).0;
                let rx = tx.subscribe();
                st.spawns.insert(
                    req.request_id.clone(),
                    SpawnRec {
                        p,
                        watch: tx,
                        waiters: watch::channel(0).0,
                    },
                );
                (rx, Some(self.persist_spawn(&mut st, &req.request_id)))
            }
        };
        if let Some(ack) = ack {
            self.settle(ack).await;
            self.advance_spawn(req.request_id.clone());
        }
        self.spawn_outcome(&req.request_id, rx).await
    }

    /// The task the caller works on and the spawn's deadline; the reason (in words) when it
    /// already passed.
    fn spawn_deadline(
        &self,
        req: &SpawnRequest,
    ) -> Result<(Option<wasl_proto::TaskId>, Option<u64>), String> {
        let now = now_ms();
        let st = self.lock();
        let parent = st
            .running_inbound(&req.from)
            .map(|r| (r.p.task.id.clone(), r.p.task.deadline_ms));
        // Without `deadline_secs` the first task gets the workstream's default deadline when
        // it is delegated (never already passed).
        let first = req
            .spec
            .instruction
            .as_ref()
            .and(req.spec.deadline_secs)
            .map(|secs| now.saturating_add(secs.saturating_mul(1000)));
        let deadline = match (first, parent.as_ref().map(|p| p.1)) {
            (Some(a), Some(b)) => Some(a.min(b)),
            (a, b) => a.or(b),
        };
        if let Some(d) = deadline.filter(|d| *d <= now) {
            let why = match &parent {
                Some((id, pd)) if *pd == d => format!("the task you are working on ({id})"),
                _ => "the first task".to_string(),
            };
            return Err(format!(
                "not spawned: the deadline of {why} has already passed"
            ));
        }
        Ok((parent.map(|p| p.0), deadline))
    }

    /// Record a request that ended before it had a plan or a gate (a final refusal) and
    /// return what the agent's call fails with. A record of this id that is already there (a
    /// concurrent retry got here first) is left as it is.
    async fn refuse_spawn(
        &self,
        req: &SpawnRequest,
        me: &wasl_proto::AgentId,
        end: End,
        message: String,
    ) -> RpcError {
        let phase = SpawnPhase::failed(end, message);
        let error = match &phase {
            SpawnPhase::Failed { code, message, .. } => err(code, message.clone()),
            _ => unreachable!("failed() builds a failure"),
        };
        let ack = {
            let mut st = self.lock();
            if st.spawns.contains_key(&req.request_id) {
                None
            } else {
                let p = SpawnPersist {
                    request_id: req.request_id.clone(),
                    from: me.clone(),
                    spec: req.spec.clone(),
                    plan: None,
                    phase: phase.clone(),
                    updated_ms: now_ms(),
                    for_task: None,
                    deadline_ms: None,
                };
                st.spawns.insert(
                    req.request_id.clone(),
                    SpawnRec {
                        watch: watch::channel(phase).0,
                        p,
                        waiters: watch::channel(0).0,
                    },
                );
                self.persist_spawn(&mut st, &req.request_id)
            }
        };
        if let Some(a) = ack {
            if let Err(e) = durable(a).await {
                emit(WaslEvent::JournalNotDurable {
                    site: JournalSite::Spawn,
                    error: e.diag(),
                });
            }
        }
        error
    }

    /// Why an asking request may no longer be approved (its deadline passed, or the task it
    /// was for is no longer being worked on), as its ending and in words.
    fn spawn_expired(&self, id: &str) -> Option<(End, String)> {
        let st = self.lock();
        let r = st.spawns.get(id)?;
        if let Some(t) = &r.p.for_task {
            let running = st.inbound.get(t).is_some_and(|i| !i.p.state.is_terminal());
            if !running {
                return Some((
                    End::new(EndCode::TaskEnded),
                    format!(
                        "not spawned: the task it was for ({t}) ended before the spawn was approved"
                    ),
                ));
            }
        }
        match r.p.deadline_ms {
            Some(d) if d <= now_ms() => Some((
                End::new(EndCode::DeadlinePassed),
                "not spawned: its deadline passed before the spawn was approved".into(),
            )),
            _ => None,
        }
    }

    /// `wasl.spawn.cancel`: the caller stopped waiting.
    pub async fn cancel_spawn(&self, req: SpawnCancel) -> Result<Ack, RpcError> {
        self.local_session(&req.from)?;
        let me = self.me(&req.from);
        // Validates the owner; an unknown id is fine (nothing to withdraw).
        if self.spawn_watch(&req.request_id, &me)?.is_some() {
            self.withdraw_spawn(
                &req.request_id,
                End::new(EndCode::Cancelled),
                "the spawn request was withdrawn: the tool call was cancelled".into(),
            )
            .await;
        }
        Ok(Ack {})
    }

    /// Withdraw a request that still asks the human: it ends as `end` (`message` is its
    /// detail, the same for the agent, the record and the gate's resolution) and its gate is
    /// taken down (a late answer is then refused by the runtime). No-op once the request was
    /// decided.
    ///
    /// Runs to the end even when the caller is dropped (an RPC handler whose client went away
    /// right after asking): a settled request must not keep its gate up.
    pub(crate) async fn withdraw_spawn(&self, id: &str, end: End, message: String) {
        let node = self.clone();
        let id = id.to_string();
        let task = tokio::spawn(async move { node.withdraw_spawn_now(&id, end, message).await });
        // Only a panic in the task (already reported) can fail the join.
        let _ = task.await;
    }

    async fn withdraw_spawn_now(&self, id: &str, end: End, message: String) {
        let reason = end.gate_reason(&message);
        let failed = SpawnPhase::failed(end, message);
        let Some((SpawnPhase::Asking { gate }, from)) = self
            .replace_spawn_phase(id, |p| matches!(p, SpawnPhase::Asking { .. }), failed)
            .await
        else {
            return;
        };
        // Raised as of the transition: take it down. Not raised yet: the raiser finds the
        // request no longer asking and takes its gate down itself.
        if let (true, Some(sid)) = (gate.raised, SessionId::parse(&from.session)) {
            self.withdraw_gate(&sid, &gate.request_id, reason).await;
        }
    }

    /// Move a request to `phase` if its current phase satisfies `from`; true if it moved.
    async fn transition_spawn(
        &self,
        id: &str,
        from: impl Fn(&SpawnPhase) -> bool,
        phase: SpawnPhase,
    ) -> bool {
        self.replace_spawn_phase(id, from, phase).await.is_some()
    }

    /// [`Node::transition_spawn`] returning the phase it replaced and the request's sender.
    async fn replace_spawn_phase(
        &self,
        id: &str,
        from: impl Fn(&SpawnPhase) -> bool,
        phase: SpawnPhase,
    ) -> Option<(SpawnPhase, wasl_proto::AgentId)> {
        let (old, sender, ack) = {
            let mut st = self.lock();
            let r = st.spawns.get_mut(id)?;
            if !from(&r.p.phase) {
                return None;
            }
            let old = std::mem::replace(&mut r.p.phase, phase);
            let sender = r.p.from.clone();
            (old, sender, self.persist_spawn(&mut st, id))
        };
        if let Some(a) = ack {
            if let Err(e) = durable(a).await {
                emit(WaslEvent::JournalNotDurable {
                    site: JournalSite::Spawn,
                    error: e.diag(),
                });
            }
        }
        Some((old, sender))
    }

    /// The watch of an existing request with this id (the caller must be its sender).
    fn spawn_watch(
        &self,
        request_id: &str,
        me: &wasl_proto::AgentId,
    ) -> Result<Option<watch::Receiver<SpawnPhase>>, RpcError> {
        let st = self.lock();
        match st.spawns.get(request_id) {
            Some(r) if r.p.from != *me => Err(RpcError::invalid_params(format!(
                "spawn request {request_id} belongs to another session"
            ))),
            Some(r) => Ok(Some(r.watch.subscribe())),
            None => Ok(None),
        }
    }

    /// Wait for the request's outcome. While this waits it counts as a caller of the request;
    /// when the last caller stops waiting (cancelled call, disconnected client) before the
    /// human answered, the request is withdrawn.
    async fn spawn_outcome(
        &self,
        id: &str,
        mut rx: watch::Receiver<SpawnPhase>,
    ) -> Result<SpawnReply, RpcError> {
        let _waiting = Waiting::new(self.clone(), id.to_string());
        let phase = match rx.wait_for(|p| p.is_terminal()).await {
            Ok(p) => p.clone(),
            Err(_) => return Err(err(codes::IO, "the node is shutting down")),
        };
        match phase {
            SpawnPhase::Done { reply } => Ok(reply),
            SpawnPhase::Failed { code, message, .. } => Err(err(&code, message)),
            _ => Err(err(codes::IO, "spawn request in an unexpected state")),
        }
    }

    /// Journal the request's current record (call under the lock).
    fn persist_spawn(&self, st: &mut crate::state::State, id: &str) -> Option<crate::journal::Ack> {
        let rev = st.next_rev();
        let r = st.spawns.get_mut(id)?;
        r.p.updated_ms = now_ms();
        if let Some(ended) = r.p.ended() {
            // The final state is journaled once: this is where the ending is recorded.
            emit(WaslEvent::SpawnEnded {
                request: Ident::new(id),
                code: wasit_diag::Code::new(ended.end.code.as_str()),
                cause: ended.end.cause.as_deref().map(wasit_diag::Code::new),
            });
        }
        r.watch.send_replace(r.p.phase.clone());
        Some(
            self.inner
                .journal
                .submit(rev, r.p.updated_ms, Rec::Spawn(Box::new(r.p.clone()))),
        )
    }

    async fn set_spawn_phase(&self, id: &str, phase: SpawnPhase) {
        let ack = {
            let mut st = self.lock();
            let Some(r) = st.spawns.get_mut(id) else {
                return;
            };
            if r.p.phase.is_terminal() {
                return;
            }
            r.p.phase = phase;
            self.persist_spawn(&mut st, id)
        };
        if let Some(a) = ack {
            if let Err(e) = durable(a).await {
                emit(WaslEvent::JournalNotDurable {
                    site: JournalSite::Spawn,
                    error: e.diag(),
                });
            }
        }
    }

    async fn fail_spawn(&self, id: &str, end: End, message: String) {
        self.set_spawn_phase(id, SpawnPhase::failed(end, message))
            .await;
    }

    /// Why the (already ended) request ended, as a gate resolution: for a gate that was raised
    /// after its request was withdrawn.
    fn spawn_gate_reason(&self, id: &str) -> GateReason {
        match self.lock().spawns.get(id).map(|r| &r.p.phase) {
            Some(SpawnPhase::Failed { end, message, .. }) => end.gate_reason(message),
            _ => GateReason::new(GateReason::WITHDRAWN),
        }
    }

    /// Drive a request from wherever it stands (also used on recovery).
    pub(crate) fn advance_spawn(&self, id: String) {
        let node = self.clone();
        tokio::spawn(async move { node.run_spawn(id).await });
    }

    async fn run_spawn(self, id: String) {
        let phase = {
            let st = self.lock();
            match st.spawns.get(&id) {
                Some(r) => r.p.phase.clone(),
                None => return,
            }
        };
        match phase {
            SpawnPhase::Asking { gate } if gate.raised => self.watch_spawn_gate(id, gate).await,
            SpawnPhase::Asking { .. } => self.raise_spawn_gate(id).await,
            SpawnPhase::Launching { decision } => self.launch_spawn(id, decision).await,
            SpawnPhase::Launched { launched, .. } => self.finish_spawn(id, launched).await,
            SpawnPhase::Done { .. } | SpawnPhase::Failed { .. } => {}
        }
    }

    async fn raise_spawn_gate(self, id: String) {
        let (sid, gate, cursor, turn) = {
            let st = self.lock();
            let Some(r) = st.spawns.get(&id) else { return };
            let Some(sid) = SessionId::parse(&r.p.from.session) else {
                return;
            };
            let Some(plan) = &r.p.plan else { return };
            let gate = spawn_gate(&id, plan, r.p.spec.instruction.clone());
            let (cursor, turn) = self
                .sessions()
                .snapshot(&sid)
                .map(|s| (s.cursor, s.turn.is_some()))
                .unwrap_or((0, false));
            (sid, gate, cursor, turn)
        };
        let request_id = gate.request_id().to_string();
        match self.raise_or_adopt(&sid, gate, cursor).await {
            Ok(cursor) => {
                let info = GateInfo {
                    request_id,
                    raised: true,
                    cursor,
                    turn,
                };
                let asking = |p: &SpawnPhase| matches!(p, SpawnPhase::Asking { .. });
                if !self
                    .transition_spawn(&id, asking, SpawnPhase::Asking { gate: info.clone() })
                    .await
                {
                    // Withdrawn while the gate was being raised: take it back down, saying
                    // what the request's ending was.
                    let reason = self.spawn_gate_reason(&id);
                    self.withdraw_gate(&sid, &info.request_id, reason).await;
                    return;
                }
                self.watch_spawn_gate(id, info).await;
            }
            Err(e) => {
                self.fail_spawn(
                    &id,
                    End::new(EndCode::GateUnavailable),
                    format!("the spawn gate could not be raised on the calling session: {e}"),
                )
                .await
            }
        }
    }

    fn still_asking(&self, id: &str) -> bool {
        self.lock()
            .spawns
            .get(id)
            .is_some_and(|r| matches!(r.p.phase, SpawnPhase::Asking { .. }))
    }

    async fn watch_spawn_gate(self, id: String, gate: GateInfo) {
        let (sid, name) = {
            let st = self.lock();
            let Some(r) = st.spawns.get(&id) else { return };
            (
                SessionId::parse(&r.p.from.session),
                r.p.plan
                    .as_ref()
                    .map(|p| p.name.clone())
                    .unwrap_or_default(),
            )
        };
        let Some(sid) = sid else { return };
        // The blocked `spawn_agent` call cannot receive a result once its turn is over.
        let deadline = self.lock().spawns.get(&id).and_then(|r| r.p.deadline_ms);
        let answer = self.await_answer(&sid, &gate, || self.still_asking(&id), |_| true);
        let answer = match deadline {
            None => answer.await,
            Some(d) => {
                let left = Duration::from_millis(d.saturating_sub(now_ms()));
                match tokio::time::timeout(left, answer).await {
                    Ok(a) => a,
                    Err(_) => {
                        self.withdraw_spawn(
                            &id,
                            End::new(EndCode::DeadlinePassed),
                            "the spawn request was withdrawn: its deadline passed before anyone \
                             answered"
                                .into(),
                        )
                        .await;
                        return;
                    }
                }
            }
        };
        let decision = match answer {
            Answer::Decided(d) => SpawnDecision::from_gate(&d).unwrap_or(SpawnDecision::Deny {
                reason: Some(format!("unknown spawn choice {:?}", d.choice)),
            }),
            Answer::AsQuestion => SpawnDecision::Deny {
                reason: Some("answered as a question; spawning needs allow or deny".into()),
            },
            Answer::Gone => {
                // Taken down by `withdraw_spawn` (already failed), or by someone else.
                self.fail_asking(
                    &id,
                    End::new(EndCode::GateGone),
                    "the spawn request was withdrawn before anyone answered".into(),
                )
                .await;
                return;
            }
            Answer::TurnEnded { interrupted } => {
                let (code, how) = if interrupted {
                    (EndCode::TurnInterrupted, "was interrupted")
                } else {
                    (EndCode::TurnEnded, "ended")
                };
                self.withdraw_spawn(
                    &id,
                    End::new(code),
                    format!(
                        "the spawn request was withdrawn: the asking turn {how} before anyone answered"
                    ),
                )
                .await;
                return;
            }
            Answer::SessionEnded(reason) => {
                // The runtime withdrew the gate with the process.
                self.fail_asking(
                    &id,
                    End::new(EndCode::SessionEnded),
                    format!("the spawn request was withdrawn: {reason}"),
                )
                .await;
                return;
            }
            Answer::SessionGone(reason) => {
                self.fail_asking(
                    &id,
                    End::new(EndCode::SessionGone),
                    format!("the spawn request was withdrawn: {reason}"),
                )
                .await;
                return;
            }
        };
        if let SpawnDecision::Deny { reason } = &decision {
            let message = match reason {
                Some(r) => format!("the human denied spawning {name}: {r}"),
                None => format!("the human denied spawning {name}"),
            };
            self.fail_asking(&id, End::new(EndCode::Denied), message)
                .await;
            return;
        }
        if let Some((end, why)) = self.spawn_expired(&id) {
            self.fail_asking(&id, end, why).await;
            return;
        }
        let launching = SpawnPhase::Launching {
            decision: decision.clone(),
        };
        // Only an asking request launches: a withdrawal that got here first wins.
        if self
            .transition_spawn(&id, |p| matches!(p, SpawnPhase::Asking { .. }), launching)
            .await
        {
            self.launch_spawn(id, decision).await;
        }
    }

    async fn fail_asking(&self, id: &str, end: End, message: String) {
        let failed = SpawnPhase::failed(end, message);
        self.transition_spawn(id, |p| matches!(p, SpawnPhase::Asking { .. }), failed)
            .await;
    }

    async fn launch_spawn(self, id: String, decision: SpawnDecision) {
        let launch = {
            let st = self.lock();
            let Some(r) = st.spawns.get(&id) else { return };
            SpawnLaunch {
                request_id: id.clone(),
                parent: r.p.from.clone(),
                spec: r.p.spec.clone(),
                decision: decision.clone(),
            }
        };
        let Some(auth) = self.spawn_authority() else {
            let end = End::caused_by(EndCode::LaunchFailed, codes::SPAWN_UNAVAILABLE);
            self.fail_spawn(&id, end, NO_SUPERVISOR.into()).await;
            return;
        };
        match self
            .patiently(LAUNCH_PATIENCE, || auth.launch(launch.clone()))
            .await
        {
            Ok(launched) => {
                self.set_spawn_phase(
                    &id,
                    SpawnPhase::Launched {
                        decision,
                        launched: launched.clone(),
                    },
                )
                .await;
                self.finish_spawn(id, launched).await;
            }
            Err(e) => {
                // A supervisor that stayed unreachable ends it as `spawn_unavailable`.
                let cause = if e.is_retryable() {
                    codes::SPAWN_UNAVAILABLE.to_string()
                } else {
                    e.code.clone()
                };
                self.fail_spawn(&id, End::caused_by(EndCode::LaunchFailed, cause), e.message)
                    .await;
            }
        }
    }

    /// Apply the new topology and delegate the first task.
    async fn finish_spawn(self, id: String, launched: SpawnLaunched) {
        if let Err(e) = self.set_topology(launched.topology.clone()).await {
            // A newer version already arrived from the supervisor: fine.
            if e.code != codes::STALE_TOPOLOGY {
                emit(WaslEvent::SpawnTopologyFailed {
                    request: Ident::new(&id),
                    error: rpc_error(&e),
                });
            }
        }
        let (from, instruction, deadline_ms) = {
            let st = self.lock();
            let Some(r) = st.spawns.get(&id) else { return };
            (
                r.p.from.clone(),
                r.p.spec.instruction.clone(),
                r.p.deadline_ms,
            )
        };
        let mut reply = SpawnReply {
            agent: launched.agent.clone(),
            name: launched.name.clone(),
            workstream: launched.workstream.clone(),
            task: None,
            task_error: None,
        };
        if let Some(text) = instruction.filter(|t| !t.trim().is_empty()) {
            let req = DelegateRequest {
                from: from.session.clone(),
                to: vec![launched.agent.to_string()],
                mode: TaskMode::Command,
                instruction: text,
                // What is left of the spawn's deadline (at least a second: the task then
                // expires through the normal path).
                deadline_secs: deadline_ms
                    .map(|d| (d.saturating_sub(now_ms()).div_ceil(1000)).max(1)),
                attachments: Vec::new(),
            };
            match self.delegate(req).await {
                Ok(r) => {
                    if let Some(o) = r.results.into_iter().next() {
                        reply.task = o.task;
                        reply.task_error = o.error;
                    }
                }
                Err(e) => {
                    reply.task_error = Some(wasl_proto::DelegateError {
                        code: e.code,
                        message: e.message,
                    })
                }
            }
        }
        self.set_spawn_phase(&id, SpawnPhase::Done { reply }).await;
    }

    /// Retry `f` on retryable errors (backoff) for up to `patience`.
    async fn patiently<T, F, Fut>(&self, patience: Duration, f: F) -> Result<T, RpcError>
    where
        F: Fn() -> Fut,
        Fut: std::future::Future<Output = Result<T, RpcError>>,
    {
        let give_up = tokio::time::Instant::now() + patience;
        let mut delay = self.inner.cfg.retry_initial;
        loop {
            match f().await {
                Err(e) if e.is_retryable() && tokio::time::Instant::now() + delay < give_up => {
                    emit(WaslEvent::SpawnAuthorityRetry {
                        delay: Ms::of(delay),
                        error: rpc_error(&e),
                    });
                    // Backoff schedule (the only sleep).
                    tokio::time::sleep(delay).await;
                    delay = (delay * 2).min(self.inner.cfg.retry_max);
                }
                other => return other,
            }
        }
    }

    /// `wasl.dismiss`: stop an agent the caller spawned.
    pub async fn dismiss(&self, req: DismissRequest) -> Result<DismissReply, RpcError> {
        self.local_session(&req.from)?;
        let auth = self.authority()?;
        let peer = req.peer.trim().to_string();
        if peer.is_empty() {
            return Err(RpcError::invalid_params("name the agent to dismiss"));
        }
        auth.dismiss(DismissAsk {
            parent: self.me(&req.from),
            peer,
        })
        .await
        .map_err(|e| {
            if e.is_retryable() {
                unavailable(format!("the supervisor cannot be reached ({})", e.message))
            } else {
                e
            }
        })
    }

    /// What `from` may spawn (for `list_peers`); never fails.
    pub(crate) async fn spawnable_for(&self, from: &wasl_proto::AgentId) -> Spawnable {
        let Some(auth) = self.spawn_authority() else {
            return Spawnable::unavailable(NO_SUPERVISOR);
        };
        let ask = SpawnableAsk {
            parent: from.clone(),
        };
        match tokio::time::timeout(Duration::from_secs(5), auth.spawnable(ask)).await {
            Ok(Ok(s)) => s,
            Ok(Err(e)) if e.code == ctl_codes::NOT_CONNECTED => {
                Spawnable::unavailable(NO_SUPERVISOR)
            }
            Ok(Err(e)) => Spawnable::unavailable(e.message),
            Err(_) => Spawnable::unavailable("the supervisor did not answer in time"),
        }
    }

    /// Resume spawn requests after a restart.
    pub(crate) fn recover_spawns(&self) {
        let ids: Vec<String> = self
            .lock()
            .spawns
            .values()
            .filter(|r| !r.p.phase.is_terminal())
            .map(|r| r.p.request_id.clone())
            .collect();
        for id in ids {
            self.advance_spawn(id);
        }
    }
}
