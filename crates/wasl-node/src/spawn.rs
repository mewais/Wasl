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
use wasit_runtime::wire::SessionId;
use wasl_proto::{
    codes, methods, spawn_gate, spawn_request_id, DelegateRequest, DismissAsk, DismissReply,
    DismissRequest, SpawnApproval, SpawnAsk, SpawnDecision, SpawnLaunch, SpawnLaunched, SpawnPlan,
    SpawnReply, SpawnRequest, Spawnable, SpawnableAsk, TaskMode,
};

use crate::gate::Answer;
use crate::journal::{durable, Rec};
use crate::node::{err, now_ms, Node};
use crate::state::{GateInfo, SpawnPersist, SpawnPhase, SpawnRec};

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
        let opts = RequestOpts {
            host: None,
            hops: 0,
            deadline: self.timeout,
        };
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
            return self.spawn_outcome(rx).await;
        }
        let auth = self.authority()?;
        let ask = SpawnAsk {
            request_id: req.request_id.clone(),
            parent: me.clone(),
            spec: req.spec.clone(),
        };
        let plan = self
            .patiently(self.inner.cfg.spawn_connect_wait, || auth.plan(ask.clone()))
            .await
            .map_err(|e| {
                if e.is_retryable() {
                    unavailable(format!("the supervisor cannot be reached ({})", e.message))
                } else {
                    e
                }
            })?;
        let phase = match plan.approval {
            SpawnApproval::Allowed => SpawnPhase::Launching {
                decision: SpawnDecision::Policy,
            },
            SpawnApproval::Ask => SpawnPhase::Asking {
                gate: GateInfo {
                    request_id: spawn_request_id(&req.request_id),
                    raised: false,
                    cursor: 0,
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
                    plan,
                    phase: phase.clone(),
                    updated_ms: now_ms(),
                };
                let tx = watch::channel(phase.clone()).0;
                let rx = tx.subscribe();
                st.spawns
                    .insert(req.request_id.clone(), SpawnRec { p, watch: tx });
                (rx, Some(self.persist_spawn(&mut st, &req.request_id)))
            }
        };
        if let Some(ack) = ack {
            self.settle(ack).await;
            self.advance_spawn(req.request_id.clone());
        }
        self.spawn_outcome(rx).await
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

    async fn spawn_outcome(
        &self,
        mut rx: watch::Receiver<SpawnPhase>,
    ) -> Result<SpawnReply, RpcError> {
        let phase = match rx.wait_for(|p| p.is_terminal()).await {
            Ok(p) => p.clone(),
            Err(_) => return Err(err(codes::IO, "the node is shutting down")),
        };
        match phase {
            SpawnPhase::Done { reply } => Ok(reply),
            SpawnPhase::Failed { code, message } => Err(err(&code, message)),
            _ => Err(err(codes::IO, "spawn request in an unexpected state")),
        }
    }

    /// Journal the request's current record (call under the lock).
    fn persist_spawn(&self, st: &mut crate::state::State, id: &str) -> Option<crate::journal::Ack> {
        let rev = st.next_rev();
        let r = st.spawns.get_mut(id)?;
        r.p.updated_ms = now_ms();
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
                tracing::error!(error = %e, "a spawn record was not journaled");
            }
        }
    }

    async fn fail_spawn(&self, id: &str, code: &str, message: String) {
        self.set_spawn_phase(
            id,
            SpawnPhase::Failed {
                code: code.into(),
                message,
            },
        )
        .await;
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
        let (sid, gate, cursor) = {
            let st = self.lock();
            let Some(r) = st.spawns.get(&id) else { return };
            let Some(sid) = SessionId::parse(&r.p.from.session) else {
                return;
            };
            let gate = spawn_gate(&id, &r.p.plan, r.p.spec.instruction.clone());
            let cursor = self
                .sessions()
                .snapshot(&sid)
                .map(|s| s.cursor)
                .unwrap_or(0);
            (sid, gate, cursor)
        };
        let request_id = gate.request_id().to_string();
        match self.sessions().raise_gate(&sid, gate).await {
            Ok(()) => {
                let info = GateInfo {
                    request_id,
                    raised: true,
                    cursor,
                };
                self.set_spawn_phase(&id, SpawnPhase::Asking { gate: info.clone() })
                    .await;
                self.watch_spawn_gate(id, info).await;
            }
            Err(e) => {
                self.fail_spawn(
                    &id,
                    codes::SPAWN_UNAVAILABLE,
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
            (SessionId::parse(&r.p.from.session), r.p.plan.name.clone())
        };
        let Some(sid) = sid else { return };
        let answer = self
            .await_answer(&sid, &gate, || self.still_asking(&id))
            .await;
        let decision = match answer {
            Answer::Decided(d) => SpawnDecision::from_gate(&d).unwrap_or(SpawnDecision::Deny {
                reason: Some(format!("unknown spawn choice {:?}", d.choice)),
            }),
            Answer::AsQuestion => SpawnDecision::Deny {
                reason: Some("answered as a question; spawning needs allow or deny".into()),
            },
            Answer::Gone => {
                if self.still_asking(&id) {
                    self.fail_spawn(
                        &id,
                        codes::SPAWN_DENIED,
                        "the spawn request was withdrawn before anyone answered".into(),
                    )
                    .await;
                }
                return;
            }
            Answer::SessionEnded(reason) => {
                self.fail_spawn(&id, codes::SPAWN_DENIED, reason).await;
                return;
            }
        };
        if let SpawnDecision::Deny { reason } = &decision {
            let message = match reason {
                Some(r) => format!("the human denied spawning {name}: {r}"),
                None => format!("the human denied spawning {name}"),
            };
            self.fail_spawn(&id, codes::SPAWN_DENIED, message).await;
            return;
        }
        self.set_spawn_phase(
            &id,
            SpawnPhase::Launching {
                decision: decision.clone(),
            },
        )
        .await;
        self.launch_spawn(id, decision).await;
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
            self.fail_spawn(&id, codes::SPAWN_UNAVAILABLE, NO_SUPERVISOR.into())
                .await;
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
                let code = if e.is_retryable() {
                    codes::SPAWN_UNAVAILABLE.to_string()
                } else {
                    e.code.clone()
                };
                self.fail_spawn(&id, &code, e.message).await;
            }
        }
    }

    /// Apply the new topology and delegate the first task.
    async fn finish_spawn(self, id: String, launched: SpawnLaunched) {
        if let Err(e) = self.set_topology(launched.topology.clone()).await {
            // A newer version already arrived from the supervisor: fine.
            if e.code != codes::STALE_TOPOLOGY {
                tracing::warn!(error = %e, "applying the topology of a spawn");
            }
        }
        let (from, instruction) = {
            let st = self.lock();
            let Some(r) = st.spawns.get(&id) else { return };
            (r.p.from.clone(), r.p.spec.instruction.clone())
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
                deadline_secs: None,
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
                    tracing::debug!(error = %e, "spawn authority not reachable; retrying");
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
