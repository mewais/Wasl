//! [`Node`]: topology, task engine API and recovery. See the crate docs.

use std::collections::HashMap;
use std::path::PathBuf;
use std::sync::{Arc, Mutex, MutexGuard, RwLock};
use std::time::Duration;

use tokio::sync::{watch, Notify};
use wasit_control::{codes as ctl_codes, ControlServer, RpcError, Uplink};
use wasit_core::GateRequest;
use wasit_runtime::wire::{Phase, SessionId};
use wasit_runtime::SessionManager;
use wasl_audit::{AuditEntry, AuditKind, AuditLog};
use wasl_proto::{
    codes, handoff_request_id, AgentId, ApprovalVia, CancelRemoteRequest, DelegateError,
    DelegateOutcome, DelegateReply, DelegateRequest, EdgeChangeCause, EdgeEvent, EdgePolicy,
    HandoffDecision, MessageRequest, PeerCard, PeerList, PeerStatus, PeerView, Task, TaskBrief,
    TaskId, TaskList, TaskMode, TaskState, TaskStates, Topology, TopologyApplied, TopologyList,
};

use crate::journal::{durable, Ack, Journal, Rec};
use crate::state::{GateInfo, InRec, OutPersist, OutRec, State};
use crate::transport::{PeerTransport, UplinkTransport};

/// Longest `task_wait` honoured.
pub const MAX_WAIT: Duration = Duration::from_secs(600);

/// Node settings.
#[derive(Debug, Clone)]
pub struct NodeConfig {
    /// This host's id (as the hub knows it).
    pub host_id: String,
    /// Where the node keeps its journal and audit trail (e.g. `<data>/wasl`).
    pub dir: PathBuf,
    /// Peer roles (in `Hello.role`) that count as an attached supervisor: while one is
    /// connected, handoffs raise gates on the sending session; otherwise the MCP front-end
    /// asks its client (elicitation) or the handoff is denied.
    pub supervisor_roles: Vec<String>,
    /// Bound on one node-to-node request.
    pub call_timeout: Duration,
    /// Retry schedule of node-to-node work that failed retryably.
    pub retry_initial: Duration,
    pub retry_max: Duration,
    /// Minimum interval between progress reports of one running task.
    pub progress_interval: Duration,
    /// How long a spawn request waits for an unreachable supervisor before it is refused.
    pub spawn_connect_wait: Duration,
    /// How long a spawn request still asking the human waits for a caller to come back after
    /// its last `wasl.spawn` caller went away (a dropped connection is re-sent with the same
    /// request id) before it is withdrawn.
    pub spawn_reattach_grace: Duration,
}

impl NodeConfig {
    pub fn new(host_id: impl Into<String>, dir: impl Into<PathBuf>) -> Self {
        NodeConfig {
            host_id: host_id.into(),
            dir: dir.into(),
            supervisor_roles: vec!["hub".into(), "gui".into(), "supervisor".into()],
            call_timeout: Duration::from_secs(20),
            retry_initial: Duration::from_millis(200),
            retry_max: Duration::from_secs(5),
            progress_interval: Duration::from_millis(500),
            spawn_connect_wait: Duration::from_secs(15),
            spawn_reattach_grace: Duration::from_secs(30),
        }
    }
}

type SupervisorProbe = Arc<dyn Fn() -> bool + Send + Sync>;

pub(crate) struct Inner {
    pub cfg: NodeConfig,
    pub sessions: SessionManager,
    pub journal: Journal,
    pub audit: AuditLog,
    pub state: Mutex<State>,
    pub transport: RwLock<Option<Arc<dyn PeerTransport>>>,
    pub supervisor: RwLock<Option<SupervisorProbe>>,
    pub spawner: RwLock<Option<Arc<dyn crate::spawn::SpawnAuthority>>>,
    /// Wakes the outbox pump.
    pub kick: Notify,
    /// Wakes the deadline timer.
    pub deadlines: Notify,
    /// Bumped when a transport is attached (the pump re-reads it).
    pub transport_gen: watch::Sender<u64>,
}

/// One Wasl node per host daemon. Cheap to clone.
#[derive(Clone)]
pub struct Node {
    pub(crate) inner: Arc<Inner>,
}

pub(crate) fn now_ms() -> u64 {
    std::time::SystemTime::now()
        .duration_since(std::time::UNIX_EPOCH)
        .map(|d| d.as_millis() as u64)
        .unwrap_or(0)
}

pub(crate) fn err(code: &str, msg: impl Into<String>) -> RpcError {
    RpcError::new(code, msg)
}

fn journal_err(e: crate::journal::JournalError) -> RpcError {
    err(codes::IO, e.to_string())
}

pub(crate) fn truncate(s: &str, max: usize) -> String {
    match s.char_indices().nth(max) {
        Some((i, _)) => format!("{}…", &s[..i]),
        None => s.to_string(),
    }
}

pub(crate) fn peer_status(p: &Phase) -> PeerStatus {
    match p {
        Phase::Starting => PeerStatus::Starting,
        Phase::Idle => PeerStatus::Idle,
        Phase::Working => PeerStatus::Working,
        Phase::NeedsYou => PeerStatus::NeedsYou,
        Phase::Exited { .. } | Phase::Lost { .. } => PeerStatus::Stopped,
        Phase::Failed { .. } => PeerStatus::Failed,
    }
}

impl Node {
    /// Open the node over `sessions` (recovering its journal) and start its background tasks.
    /// Attach a route to other hosts with [`Node::attach_server`] / [`Node::attach_uplink`].
    pub async fn open(cfg: NodeConfig, sessions: SessionManager) -> Result<Node, RpcError> {
        let (journal, entries) = Journal::open(&cfg.dir).await.map_err(journal_err)?;
        let audit = AuditLog::open(cfg.dir.join("audit.ndjson"))
            .await
            .map_err(|e| err(codes::IO, e.to_string()))?;
        let mut st = State::default();
        for e in entries {
            st.rev = st.rev.max(e.rev);
            match e.rec {
                Rec::Topology(t) => {
                    st.topo.insert(t.workstream.clone(), *t);
                }
                Rec::TopologyRemoved { workstream } => {
                    st.topo.remove(&workstream);
                }
                Rec::Edge(_) => {}
                Rec::Spawn(sp) => {
                    let sp = *sp;
                    st.spawns.insert(
                        sp.request_id.clone(),
                        crate::state::SpawnRec {
                            watch: watch::channel(sp.phase.clone()).0,
                            p: sp,
                            waiters: watch::channel(0).0,
                        },
                    );
                }
                Rec::Out(o) => {
                    let o = *o;
                    if let TaskState::Completed { result } = &o.state {
                        if let Some(u) = &result.usage {
                            *st.spent.entry(o.task.workstream.clone()).or_default() += u.cost_usd;
                        }
                    }
                    st.out.insert(
                        o.task.id.clone(),
                        OutRec {
                            watch: watch::channel(o.state.clone()).0,
                            p: o,
                            rev: e.rev,
                        },
                    );
                }
                Rec::In(i) => {
                    let i = *i;
                    st.inbound.insert(
                        i.task.id.clone(),
                        InRec {
                            cancel: watch::channel(i.cancel.clone()).0,
                            p: i,
                            rev: e.rev,
                        },
                    );
                }
            }
        }
        // Rebuild the per-session queues: the task that was running first, then the queued
        // ones in creation order.
        let mut pending: Vec<(&TaskId, &InRec)> = st
            .inbound
            .iter()
            .filter(|(_, r)| !r.p.state.is_terminal())
            .collect();
        pending.sort_by_key(|(_, r)| (r.p.task.created_ms, r.p.task.id.clone()));
        let mut queues: HashMap<String, crate::state::SessionQueue> = HashMap::new();
        for (id, r) in pending {
            let q = queues.entry(r.p.task.to.session.clone()).or_default();
            if r.p.turn.is_some() && q.running.is_none() {
                q.running = Some(id.clone());
            } else {
                q.fifo.push_back(id.clone());
            }
        }
        st.queues = queues;
        let node = Node {
            inner: Arc::new(Inner {
                cfg,
                sessions,
                journal,
                audit,
                state: Mutex::new(st),
                transport: RwLock::new(None),
                supervisor: RwLock::new(None),
                spawner: RwLock::new(None),
                kick: Notify::new(),
                deadlines: Notify::new(),
                transport_gen: watch::channel(0).0,
            }),
        };
        node.recover();
        tokio::spawn(crate::pump::run(Arc::downgrade(&node.inner)));
        tokio::spawn(crate::pump::deadlines(Arc::downgrade(&node.inner)));
        Ok(node)
    }

    /// Resume watchers and workers after [`Node::open`].
    fn recover(&self) {
        let (gates, sessions) = {
            let st = self.lock();
            let gates: Vec<(TaskId, Option<GateInfo>)> = st
                .out
                .values()
                .filter(|r| {
                    matches!(
                        r.p.state,
                        TaskState::PendingApproval {
                            via: ApprovalVia::Gate
                        }
                    )
                })
                .map(|r| (r.p.task.id.clone(), r.p.gate.clone()))
                .collect();
            let sessions: Vec<String> = st.queues.keys().cloned().collect();
            (gates, sessions)
        };
        for (id, gate) in gates {
            match gate {
                Some(g) if g.raised => self.spawn_gate_watcher(id, g),
                _ => {
                    let n = self.clone();
                    tokio::spawn(async move { n.raise_handoff(id).await });
                }
            }
        }
        for s in sessions {
            self.ensure_worker(&s);
        }
        self.recover_spawns();
    }

    pub(crate) fn lock(&self) -> MutexGuard<'_, State> {
        // The state is replaced field by field under the lock; a poisoned lock still holds
        // consistent data.
        self.inner.state.lock().unwrap_or_else(|e| e.into_inner())
    }

    pub fn host_id(&self) -> &str {
        &self.inner.cfg.host_id
    }

    pub fn sessions(&self) -> &SessionManager {
        &self.inner.sessions
    }

    pub fn audit(&self) -> &AuditLog {
        &self.inner.audit
    }

    /// Route node-to-node messages for other hosts through `transport`.
    pub fn attach_transport(&self, transport: Arc<dyn PeerTransport>) {
        *self
            .inner
            .transport
            .write()
            .unwrap_or_else(|e| e.into_inner()) = Some(transport);
        self.inner.transport_gen.send_modify(|n| *n += 1);
        self.inner.kick.notify_one();
    }

    /// Relay through `uplink` (see [`wasit_control::Uplink`]).
    pub fn attach_uplink(&self, uplink: Uplink) {
        let t = UplinkTransport::new(uplink, self.inner.cfg.call_timeout);
        self.attach_transport(Arc::new(t));
    }

    /// Decide "is a supervisor attached" with `probe`.
    pub fn set_supervisor_probe(&self, probe: Arc<dyn Fn() -> bool + Send + Sync>) {
        *self
            .inner
            .supervisor
            .write()
            .unwrap_or_else(|e| e.into_inner()) = Some(probe);
    }

    /// The usual wiring for a daemon serving `server`: relay over the connection the hub opened
    /// (role `hub`), and count any connection with a supervisor role as an attached supervisor.
    /// Spawn requests go to the hub over the same connection
    /// ([`crate::UplinkSpawnAuthority`]); a hub sets its own authority instead.
    pub fn attach_server(&self, server: &ControlServer) {
        self.attach_uplink(Uplink::via_server(server.clone(), "hub"));
        self.set_spawn_authority(Arc::new(crate::spawn::UplinkSpawnAuthority::new(
            Uplink::via_server(server.clone(), "hub"),
            self.inner.cfg.call_timeout * 6,
        )));
        let s = server.clone();
        let roles = self.inner.cfg.supervisor_roles.clone();
        self.set_supervisor_probe(Arc::new(move || {
            s.connections()
                .iter()
                .any(|c| !c.is_closed() && roles.contains(&c.remote_hello().role))
        }));
    }

    pub(crate) fn supervised(&self) -> bool {
        self.inner
            .supervisor
            .read()
            .unwrap_or_else(|e| e.into_inner())
            .as_ref()
            .is_some_and(|p| p())
    }

    pub(crate) fn transport(&self) -> Option<Arc<dyn PeerTransport>> {
        self.inner
            .transport
            .read()
            .unwrap_or_else(|e| e.into_inner())
            .clone()
    }

    pub(crate) fn me(&self, session: &str) -> AgentId {
        AgentId::new(self.inner.cfg.host_id.clone(), session)
    }

    /// Append to the audit trail; resolves once the entry is durable. Callers record an entry
    /// *before* the state change it describes becomes visible (durability first).
    pub(crate) async fn record_audit(&self, entry: AuditEntry) -> Result<(), RpcError> {
        self.inner.audit.record(entry).await.map_err(|e| {
            tracing::error!(error = %e, "audit entry not recorded");
            err(codes::IO, e.to_string())
        })
    }

    // ── journal helpers (call under the lock) ───────────────────────────────

    pub(crate) fn persist_out(&self, st: &mut State, id: &TaskId) -> Option<Ack> {
        let rev = st.next_rev();
        let r = st.out.get_mut(id)?;
        r.rev = rev;
        r.p.updated_ms = now_ms();
        r.watch.send_replace(r.p.state.clone());
        Some(
            self.inner
                .journal
                .submit(rev, r.p.updated_ms, Rec::Out(Box::new(r.p.clone()))),
        )
    }

    pub(crate) fn persist_in(&self, st: &mut State, id: &TaskId) -> Option<Ack> {
        let rev = st.next_rev();
        let r = st.inbound.get_mut(id)?;
        r.rev = rev;
        r.p.updated_ms = now_ms();
        Some(
            self.inner
                .journal
                .submit(rev, r.p.updated_ms, Rec::In(Box::new(r.p.clone()))),
        )
    }

    pub(crate) async fn settle(&self, ack: Option<Ack>) {
        if let Some(a) = ack {
            if let Err(e) = durable(a).await {
                tracing::error!(error = %e, "a task record was not journaled");
            }
        }
    }

    // ── topology ────────────────────────────────────────────────────────────

    /// `wasl.topology.set`.
    pub async fn set_topology(&self, t: Topology) -> Result<TopologyApplied, RpcError> {
        if t.workstream.is_empty() {
            return Err(RpcError::invalid_params("workstream must not be empty"));
        }
        let ack = {
            let mut st = self.lock();
            if let Some(cur) = st.topo.get(&t.workstream) {
                if t.version < cur.version {
                    return Err(err(
                        codes::STALE_TOPOLOGY,
                        format!(
                            "workstream {} is at version {}; refusing version {}",
                            t.workstream, cur.version, t.version
                        ),
                    ));
                }
                if t.version == cur.version {
                    return Ok(TopologyApplied {
                        workstream: t.workstream,
                        version: t.version,
                        changed: false,
                    });
                }
            }
            let rev = st.next_rev();
            let ws = t.workstream.clone();
            let rec = if t.peers.is_empty() && t.edges.is_empty() {
                st.topo.remove(&ws);
                Rec::TopologyRemoved {
                    workstream: ws.clone(),
                }
            } else {
                st.topo.insert(ws.clone(), t.clone());
                Rec::Topology(Box::new(t.clone()))
            };
            self.inner.journal.submit(rev, now_ms(), rec)
        };
        durable(ack).await.map_err(journal_err)?;
        Ok(TopologyApplied {
            workstream: t.workstream,
            version: t.version,
            changed: true,
        })
    }

    /// `wasl.topology.get`.
    pub fn topologies(&self) -> TopologyList {
        TopologyList {
            workstreams: self.lock().topo.values().cloned().collect(),
        }
    }

    // ── peers ───────────────────────────────────────────────────────────────

    pub(crate) fn local_session(&self, s: &str) -> Result<SessionId, RpcError> {
        let id = SessionId::parse(s)
            .ok_or_else(|| err(codes::UNKNOWN_SESSION, format!("not a session id: {s:?}")))?;
        self.inner.sessions.snapshot(&id).map_err(|_| {
            err(
                codes::UNKNOWN_SESSION,
                format!("no session {s} on host {}", self.host_id()),
            )
        })?;
        Ok(id)
    }

    /// Live status of local sessions.
    pub fn peer_status_local(&self, sessions: &[String]) -> Vec<(String, PeerStatus)> {
        sessions
            .iter()
            .map(|s| {
                let st = SessionId::parse(s)
                    .and_then(|id| self.inner.sessions.snapshot(&id).ok())
                    .map(|snap| peer_status(&snap.phase))
                    .unwrap_or(PeerStatus::Unknown);
                (s.clone(), st)
            })
            .collect()
    }

    /// `wasl.peers.list`: the peers `from` has an outgoing edge to, with live status.
    pub async fn peers(&self, from: &str) -> Result<PeerList, RpcError> {
        self.local_session(from)?;
        let me = self.me(from);
        let mut peers: Vec<PeerView> = {
            let st = self.lock();
            st.edges_from(&me)
                .into_iter()
                .map(|(ws, to, policy)| PeerView {
                    card: st.card(&to).cloned().unwrap_or(PeerCard {
                        agent: to.clone(),
                        name: String::new(),
                        driver: String::new(),
                        role: String::new(),
                        description: String::new(),
                        status: PeerStatus::Unknown,
                    }),
                    policy,
                    workstream: ws,
                })
                .collect()
        };
        // Group by host; local status directly, remote hosts asked in parallel (bounded).
        let mut by_host: HashMap<String, Vec<String>> = HashMap::new();
        for p in &peers {
            by_host
                .entry(p.card.agent.host.clone())
                .or_default()
                .push(p.card.agent.session.clone());
        }
        let transport = self.transport();
        let futs = by_host.into_iter().map(|(host, sessions)| {
            let node = self.clone();
            let transport = transport.clone();
            async move {
                if host == node.host_id() {
                    return (host, node.peer_status_local(&sessions));
                }
                let unreachable = |s: &[String]| {
                    s.iter()
                        .map(|x| (x.clone(), PeerStatus::Unreachable))
                        .collect::<Vec<_>>()
                };
                let Some(t) = transport else {
                    return (host, unreachable(&sessions));
                };
                let req = wasl_proto::PeerStatusRequest {
                    sessions: sessions.clone(),
                };
                let params = serde_json::to_value(&req).unwrap_or_default();
                let r = tokio::time::timeout(
                    Duration::from_secs(3),
                    t.call(&host, wasl_proto::methods::NODE_PEER_STATUS, params),
                )
                .await;
                match r {
                    Ok(Ok(v)) => match serde_json::from_value::<wasl_proto::PeerStatusReply>(v) {
                        Ok(r) => (host, r.statuses),
                        Err(_) => (host, unreachable(&sessions)),
                    },
                    _ => (host, unreachable(&sessions)),
                }
            }
        });
        let results = futures::future::join_all(futs).await;
        let mut status: HashMap<(String, String), PeerStatus> = HashMap::new();
        for (host, list) in results {
            for (s, st) in list {
                status.insert((host.clone(), s), st);
            }
        }
        for p in &mut peers {
            p.card.status = status
                .get(&(p.card.agent.host.clone(), p.card.agent.session.clone()))
                .copied()
                .unwrap_or(PeerStatus::Unknown);
        }
        let spawnable = Some(self.spawnable_for(&me).await);
        Ok(PeerList { peers, spawnable })
    }

    // ── delegation ──────────────────────────────────────────────────────────

    /// `wasl.message`: a fire-and-forget delegation of mode `message`.
    pub async fn message(&self, req: MessageRequest) -> Result<DelegateReply, RpcError> {
        self.delegate(DelegateRequest {
            from: req.from,
            to: req.to,
            mode: TaskMode::Message,
            instruction: req.text,
            deadline_secs: None,
            attachments: Vec::new(),
        })
        .await
    }

    /// `wasl.delegate`: create one task per target and return at once.
    pub async fn delegate(&self, req: DelegateRequest) -> Result<DelegateReply, RpcError> {
        if req.to.is_empty() {
            return Err(RpcError::invalid_params("no target peer given"));
        }
        if req.instruction.trim().is_empty() {
            return Err(RpcError::invalid_params("the instruction is empty"));
        }
        self.local_session(&req.from)?;
        let from = self.me(&req.from);
        let supervised = self.supervised();
        let mut results = Vec::new();
        let mut acks = Vec::new();
        let mut gates = Vec::new();
        let mut audits = Vec::new();
        let mut created = Vec::new();
        {
            let st = self.lock();
            let edges = st.edges_from(&from);
            let (parent, mut chain) = match st.running_inbound(&req.from) {
                Some(r) => (Some(r.p.task.id.clone()), r.p.task.chain.clone()),
                None => (None, Vec::new()),
            };
            chain.push(from.clone());
            let hop = chain.len() as u32;
            for target in &req.to {
                let reject = |code: &str, message: String| DelegateOutcome {
                    to: target.clone(),
                    task: None,
                    error: Some(DelegateError {
                        code: code.into(),
                        message,
                    }),
                };
                if edges.is_empty() {
                    results.push(reject(
                        codes::NO_EDGE,
                        "no connections configured for this session: it has no peers to delegate to"
                            .into(),
                    ));
                    continue;
                }
                let found = edges.iter().find(|(_, to, _)| {
                    to.to_string() == *target
                        || st
                            .card(to)
                            .is_some_and(|c| c.name.eq_ignore_ascii_case(target))
                });
                let Some((ws, to, policy)) = found.cloned() else {
                    let names: Vec<String> = edges
                        .iter()
                        .map(|(_, to, _)| {
                            let n = st.name_of(to);
                            if n.is_empty() {
                                to.to_string()
                            } else {
                                n
                            }
                        })
                        .collect();
                    results.push(reject(
                        codes::NO_EDGE,
                        format!(
                            "no connection from this session to {target:?}; reachable peers: {}",
                            names.join(", ")
                        ),
                    ));
                    continue;
                };
                let (limits, paused) = st
                    .topo
                    .get(&ws)
                    .map(|t| (t.limits.clone(), t.paused))
                    .unwrap_or_default();
                let to_name = st.name_of(&to);
                let audit_reject = |code: &str, message: &str| AuditEntry {
                    t_ms: now_ms(),
                    workstream: ws.clone(),
                    actor: from.clone(),
                    target: to.clone(),
                    task: None,
                    kind: AuditKind::Rejected {
                        code: code.into(),
                        message: message.into(),
                    },
                };
                if paused {
                    let msg = format!(
                        "rejected: workstream {ws} is paused by the human; no new delegations until it is resumed. Continue on your own or wait."
                    );
                    audits.push(audit_reject(codes::PAUSED, &msg));
                    results.push(reject(codes::PAUSED, msg));
                    continue;
                }
                if to == from || chain.contains(&to) {
                    let path: Vec<String> = chain
                        .iter()
                        .map(|a| {
                            let n = st.name_of(a);
                            if n.is_empty() {
                                a.to_string()
                            } else {
                                n
                            }
                        })
                        .collect();
                    let msg = format!(
                        "rejected: {target} is already part of this delegation chain ({}); delegating to it would create a cycle. Answer from what you have instead.",
                        path.join(" → ")
                    );
                    audits.push(audit_reject(codes::CYCLE, &msg));
                    results.push(reject(codes::CYCLE, msg));
                    continue;
                }
                if hop > limits.max_depth {
                    let msg = format!(
                        "rejected: this delegation would be {hop} hops deep; the workstream allows at most {}",
                        limits.max_depth
                    );
                    audits.push(audit_reject(codes::MAX_DEPTH, &msg));
                    results.push(reject(codes::MAX_DEPTH, msg));
                    continue;
                }
                if let Some(b) = limits.budget_usd {
                    let spent = st.spent.get(&ws).copied().unwrap_or(0.0);
                    if spent >= b {
                        let msg = format!(
                            "rejected: the workstream budget of ${b:.2} is used up (${spent:.2} reported)"
                        );
                        audits.push(audit_reject(codes::BUDGET, &msg));
                        results.push(reject(codes::BUDGET, msg));
                        continue;
                    }
                }
                if policy == EdgePolicy::Deny {
                    let msg = format!(
                        "rejected: delegation to {target} is not allowed (edge policy: deny)"
                    );
                    audits.push(audit_reject(codes::DENIED, &msg));
                    results.push(reject(codes::DENIED, msg));
                    continue;
                }
                let now = now_ms();
                let deadline_secs = req.deadline_secs.unwrap_or(limits.default_deadline_secs);
                let task = Task {
                    id: TaskId::random(),
                    workstream: ws.clone(),
                    from: from.clone(),
                    to: to.clone(),
                    mode: req.mode,
                    instruction: req.instruction.clone(),
                    attachments: req.attachments.clone(),
                    deadline_ms: now.saturating_add(deadline_secs.saturating_mul(1000)),
                    created_ms: now,
                    chain: chain.clone(),
                    hop,
                    parent: parent.clone(),
                };
                let state = match policy {
                    EdgePolicy::AlwaysAllow => TaskState::Queued,
                    _ if supervised => TaskState::PendingApproval {
                        via: ApprovalVia::Gate,
                    },
                    _ => TaskState::PendingApproval {
                        via: ApprovalVia::Elicitation,
                    },
                };
                audits.push(AuditEntry {
                    t_ms: now,
                    workstream: ws.clone(),
                    actor: from.clone(),
                    target: to.clone(),
                    task: Some(task.id.clone()),
                    kind: AuditKind::Delegated {
                        mode: req.mode,
                        policy,
                        instruction: truncate(&req.instruction, 300),
                    },
                });
                if policy == EdgePolicy::AlwaysAllow {
                    audits.push(AuditEntry {
                        t_ms: now,
                        workstream: ws.clone(),
                        actor: from.clone(),
                        target: to.clone(),
                        task: Some(task.id.clone()),
                        kind: AuditKind::Decided {
                            decision: HandoffDecision::AllowOnce,
                            by: "policy".into(),
                        },
                    });
                }
                if matches!(
                    state,
                    TaskState::PendingApproval {
                        via: ApprovalVia::Gate
                    }
                ) {
                    gates.push(task.id.clone());
                }
                let rec = OutRec {
                    watch: watch::channel(state.clone()).0,
                    p: OutPersist {
                        task,
                        state,
                        updated_ms: now,
                        to_name,
                        delivered: false,
                        gate: None,
                        cancel: None,
                    },
                    rev: 0,
                };
                results.push(DelegateOutcome {
                    to: target.clone(),
                    task: Some(rec.brief()),
                    error: None,
                });
                created.push(rec);
            }
        }
        // Durability first: the trail holds the requests (and the policy's decisions) before
        // any task exists to be delivered.
        for a in audits {
            // Logged by `record_audit`.
            let _ = self.record_audit(a).await;
        }
        {
            let mut st = self.lock();
            for rec in created {
                let id = rec.p.task.id.clone();
                st.out.insert(id.clone(), rec);
                acks.push(self.persist_out(&mut st, &id));
            }
        }
        for a in acks.into_iter().flatten() {
            durable(a).await.map_err(journal_err)?;
        }
        self.inner.deadlines.notify_one();
        self.inner.kick.notify_one();
        // Raise the gates concurrently; the reply does not wait for a human, only for the
        // gates to be journaled (so the caller can rely on them being visible).
        futures::future::join_all(gates.into_iter().map(|id| self.raise_handoff(id))).await;
        // Refresh the briefs (a gate may have failed to raise).
        {
            let st = self.lock();
            for r in &mut results {
                if let Some(t) = &mut r.task {
                    if let Some(rec) = st.out.get(&t.id) {
                        *t = rec.brief();
                    }
                }
            }
        }
        Ok(DelegateReply { results })
    }

    /// Raise the handoff gate of a pending task on its sending session.
    pub(crate) async fn raise_handoff(&self, id: TaskId) {
        let (sid, gate, cursor, turn) = {
            let st = self.lock();
            let Some(r) = st.out.get(&id) else { return };
            if !matches!(r.p.state, TaskState::PendingApproval { .. }) {
                return;
            }
            let t = &r.p.task;
            let from_name = st.name_of(&t.from);
            let to_name = r.p.to_name.clone();
            // People see the agent's name; its id (host/session) is in `details`.
            let label = |a: &AgentId, n: &str| {
                if n.is_empty() {
                    a.to_string()
                } else {
                    n.to_string()
                }
            };
            let gate = GateRequest::Handoff {
                request_id: handoff_request_id(&t.id),
                task_id: t.id.to_string(),
                from: label(&t.from, &from_name),
                to: label(&t.to, &to_name),
                mode: t.mode.as_str().into(),
                instruction: t.instruction.clone(),
                details: serde_json::json!({
                    "workstream": t.workstream,
                    "hop": t.hop,
                    "deadline_ms": t.deadline_ms,
                    "from_agent": t.from,
                    "to_agent": t.to,
                }),
                choices: wasl_proto::handoff_choices(),
            };
            let Some(sid) = SessionId::parse(&t.from.session) else {
                return;
            };
            let (cursor, turn) = self
                .inner
                .sessions
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
                let ack = {
                    let mut st = self.lock();
                    match st.out.get_mut(&id) {
                        Some(r) if matches!(r.p.state, TaskState::PendingApproval { .. }) => {
                            r.p.gate = Some(info.clone());
                            Some(self.persist_out(&mut st, &id))
                        }
                        _ => None,
                    }
                };
                let Some(ack) = ack else {
                    // Decided or withdrawn meanwhile: take the gate back down.
                    self.withdraw_gate(&sid, &info.request_id).await;
                    return;
                };
                self.settle(ack).await;
                self.spawn_gate_watcher(id, info);
            }
            Err(e) => {
                let reason =
                    format!("the handoff gate could not be raised on the sending session: {e}");
                self.finish_out(&id, TaskState::Denied { reason }).await;
            }
        }
    }

    /// Raise `gate` on `sid` and return the cursor to watch it from (`cursor`, taken right
    /// before). A gate with its request id already pending was raised by an earlier run of
    /// this node that ended before recording it: it is adopted, watched from just before it.
    pub(crate) async fn raise_or_adopt(
        &self,
        sid: &SessionId,
        gate: GateRequest,
        cursor: u64,
    ) -> Result<u64, wasit_runtime::wire::CommandError> {
        let rid = gate.request_id().to_string();
        let pending = || {
            self.inner.sessions.snapshot(sid).ok().and_then(|s| {
                s.pending_gates
                    .iter()
                    .find(|g| g.gate.request_id() == rid)
                    .map(|g| g.cursor.saturating_sub(1))
            })
        };
        if let Some(c) = pending() {
            return Ok(c);
        }
        match self.inner.sessions.raise_gate(sid, gate).await {
            Ok(()) => Ok(cursor),
            // Refused as a duplicate: the session (still loading when we looked) has it.
            Err(e @ wasit_runtime::wire::CommandError::Rejected { .. }) => pending().ok_or(e),
            Err(e) => Err(e),
        }
    }

    /// Take a gate down on its session. Runs to the end even when the caller is dropped (an
    /// RPC handler whose client disconnected right after asking for the withdrawal): the
    /// request is already settled, so its gate must not stay up.
    pub(crate) async fn withdraw_gate(&self, sid: &SessionId, request_id: &str) {
        let sessions = self.inner.sessions.clone();
        let (sid, request_id) = (sid.clone(), request_id.to_string());
        let task = tokio::spawn(async move {
            if let Err(e) = sessions.resolve_gate(&sid, &request_id).await {
                // Already answered / the session ended: nothing is pending any more.
                tracing::debug!(error = %e, request_id, "withdrawing a gate");
            }
        });
        // Only a panic in the task (already reported) can fail the join.
        let _ = task.await;
    }

    /// Apply a handoff decision (from the gate, an elicitation, or a supervisor).
    ///
    /// Durability first: the decision is in the audit trail before it takes effect. If the
    /// trail cannot be written the decision is refused. A task settled by someone else while
    /// the entry was being written (cancelled, withdrawn, decided) keeps that outcome.
    pub async fn decide(
        &self,
        id: &TaskId,
        decision: HandoffDecision,
        by: &str,
    ) -> Result<TaskBrief, RpcError> {
        let audits = {
            let st = self.lock();
            let r = Self::awaiting_approval(&st, id)?;
            let task = &r.p.task;
            let mut audits = vec![AuditEntry {
                t_ms: now_ms(),
                workstream: task.workstream.clone(),
                actor: task.from.clone(),
                target: task.to.clone(),
                task: Some(task.id.clone()),
                kind: AuditKind::Decided {
                    decision: decision.clone(),
                    by: by.into(),
                },
            }];
            let edge_changes = decision == HandoffDecision::AlwaysAllow
                && st.topo.get(&task.workstream).is_some_and(|t| {
                    t.edges
                        .iter()
                        .any(|e| e.from == task.from && e.to == task.to)
                });
            if edge_changes {
                audits.push(AuditEntry {
                    t_ms: now_ms(),
                    workstream: task.workstream.clone(),
                    actor: task.from.clone(),
                    target: task.to.clone(),
                    task: Some(task.id.clone()),
                    kind: AuditKind::PolicyChanged {
                        policy: EdgePolicy::AlwaysAllow,
                        cause: format!("always allow chosen ({by})"),
                    },
                });
            }
            let new_state = decided_state(&decision, &r.p);
            if new_state.is_terminal() {
                audits.push(finished_audit(task, &new_state));
            }
            audits
        };
        for a in audits {
            self.record_audit(a).await?;
        }
        let mut acks = Vec::new();
        let (brief, gate_to_withdraw) = {
            let mut st = self.lock();
            let r = Self::awaiting_approval(&st, id)?;
            let task = r.p.task.clone();
            let gate = r.p.gate.clone();
            let new_state = decided_state(&decision, &r.p);
            if decision == HandoffDecision::AlwaysAllow {
                if let Some(t) = st.topo.get_mut(&task.workstream) {
                    let mut changed = false;
                    for e in t
                        .edges
                        .iter_mut()
                        .filter(|e| e.from == task.from && e.to == task.to)
                    {
                        e.policy = EdgePolicy::AlwaysAllow;
                        changed = true;
                    }
                    if changed {
                        let t = t.clone();
                        let ev = EdgeEvent {
                            workstream: t.workstream.clone(),
                            version: t.version,
                            from: task.from.clone(),
                            to: task.to.clone(),
                            policy: EdgePolicy::AlwaysAllow,
                            cause: EdgeChangeCause::Decision,
                            task: Some(task.id.clone()),
                        };
                        let rev = st.next_rev();
                        acks.push(Some(self.inner.journal.submit(
                            rev,
                            now_ms(),
                            Rec::Topology(Box::new(t)),
                        )));
                        let rev = st.next_rev();
                        acks.push(Some(self.inner.journal.submit(
                            rev,
                            now_ms(),
                            Rec::Edge(Box::new(ev)),
                        )));
                    }
                }
            }
            if let Some(r) = st.out.get_mut(id) {
                r.p.state = new_state;
            }
            acks.push(self.persist_out(&mut st, id));
            let brief = st.out.get(id).map(OutRec::brief);
            // A decision that did not come through the gate takes the gate down.
            let withdraw = match (by, gate) {
                ("gate", _) | (_, None) => None,
                (_, Some(g)) => SessionId::parse(&task.from.session).map(|s| (s, g.request_id)),
            };
            (brief, withdraw)
        };
        for a in acks {
            self.settle(a).await;
        }
        if let Some((sid, rid)) = gate_to_withdraw {
            self.withdraw_gate(&sid, &rid).await;
        }
        self.inner.kick.notify_one();
        brief.ok_or_else(|| err(codes::UNKNOWN_TASK, format!("no task {id}")))
    }

    fn awaiting_approval<'a>(st: &'a State, id: &TaskId) -> Result<&'a OutRec, RpcError> {
        let Some(r) = st.out.get(id) else {
            return Err(err(codes::UNKNOWN_TASK, format!("no task {id}")));
        };
        if !matches!(r.p.state, TaskState::PendingApproval { .. }) {
            return Err(err(
                codes::INVALID_STATE,
                format!("task {id} is {}, not awaiting approval", r.p.state.name()),
            ));
        }
        Ok(r)
    }

    /// Move an outbound task to a terminal state (no-op if it already is terminal).
    pub(crate) async fn finish_out(&self, id: &TaskId, state: TaskState) {
        // Audited before it becomes visible (durability first).
        let audit = {
            let st = self.lock();
            let Some(r) = st.out.get(id) else { return };
            if r.p.state.is_terminal() {
                return;
            }
            finished_audit(&r.p.task, &state)
        };
        // Logged by `record_audit`; the outcome is applied regardless (it already happened).
        let _ = self.record_audit(audit).await;
        let ack = {
            let mut st = self.lock();
            let Some(r) = st.out.get_mut(id) else { return };
            if r.p.state.is_terminal() {
                return;
            }
            r.p.state = state;
            self.persist_out(&mut st, id)
        };
        self.settle(ack).await;
    }

    // ── status, wait, cancel ────────────────────────────────────────────────

    /// `wasl.task.status`.
    pub fn status(&self, ids: &[TaskId]) -> TaskStates {
        let st = self.lock();
        let mut tasks = Vec::new();
        let mut unknown = Vec::new();
        for id in ids {
            match st.out.get(id) {
                Some(r) => tasks.push(r.brief()),
                None => unknown.push(id.clone()),
            }
        }
        TaskStates {
            tasks,
            unknown,
            timed_out: false,
        }
    }

    /// `wasl.task.wait`: until all (or, with `any`, one) of `ids` are terminal, or `timeout`.
    /// Holds no lock while waiting; each call waits on its own per-task watches.
    pub async fn wait(&self, ids: &[TaskId], timeout: Duration, any: bool) -> TaskStates {
        let watches: Vec<watch::Receiver<TaskState>> = {
            let st = self.lock();
            ids.iter()
                .filter_map(|id| st.out.get(id).map(|r| r.watch.subscribe()))
                .collect()
        };
        let timeout = timeout.min(MAX_WAIT);
        let done = async {
            if watches.is_empty() {
                return;
            }
            let futs = watches.into_iter().map(|mut w| {
                Box::pin(async move {
                    // Ends when the state is terminal (or the task record is gone).
                    let _ = w.wait_for(|s| s.is_terminal()).await;
                })
            });
            if any {
                futures::future::select_all(futs).await;
            } else {
                futures::future::join_all(futs).await;
            }
        };
        let timed_out = tokio::time::timeout(timeout, done).await.is_err();
        let mut r = self.status(ids);
        r.timed_out = timed_out;
        r
    }

    /// `wasl.task.cancel`.
    pub async fn cancel(&self, id: &TaskId, reason: Option<String>) -> Result<TaskBrief, RpcError> {
        self.withdraw(
            id,
            reason.unwrap_or_else(|| "cancelled by the sender".into()),
            false,
        )
        .await?;
        self.lock()
            .out
            .get(id)
            .map(OutRec::brief)
            .ok_or_else(|| err(codes::UNKNOWN_TASK, format!("no task {id}")))
    }

    /// Cancel or expire an outbound task, forwarding the withdrawal if it was delivered.
    ///
    /// Runs to the end even when the caller is dropped (an RPC handler whose client went away
    /// right after asking): a settled task must not keep its gate up.
    pub(crate) async fn withdraw(
        &self,
        id: &TaskId,
        reason: String,
        expired: bool,
    ) -> Result<(), RpcError> {
        let node = self.clone();
        let id = id.clone();
        tokio::spawn(async move { node.withdraw_now(&id, reason, expired).await })
            .await
            .map_err(|e| err(codes::IO, format!("withdrawing the task failed: {e}")))?
    }

    async fn withdraw_now(
        &self,
        id: &TaskId,
        reason: String,
        expired: bool,
    ) -> Result<(), RpcError> {
        let new_state = if expired {
            TaskState::Expired
        } else {
            TaskState::Cancelled {
                reason: reason.clone(),
            }
        };
        // Audited before it becomes visible (durability first).
        let audit = {
            let st = self.lock();
            let Some(r) = st.out.get(id) else {
                return Err(err(codes::UNKNOWN_TASK, format!("no task {id}")));
            };
            if r.p.state.is_terminal() {
                return Ok(());
            }
            finished_audit(&r.p.task, &new_state)
        };
        // Logged by `record_audit`; a cancellation is applied regardless.
        let _ = self.record_audit(audit).await;
        let (ack, gate) = {
            let mut st = self.lock();
            let Some(r) = st.out.get_mut(id) else {
                return Err(err(codes::UNKNOWN_TASK, format!("no task {id}")));
            };
            if r.p.state.is_terminal() {
                return Ok(());
            }
            let gate = match (&r.p.state, &r.p.gate) {
                (TaskState::PendingApproval { .. }, Some(g)) => {
                    SessionId::parse(&r.p.task.from.session).map(|s| (s, g.request_id.clone()))
                }
                _ => None,
            };
            if r.p.delivered {
                r.p.cancel = Some(CancelRemoteRequest {
                    id: id.clone(),
                    reason,
                    expired,
                });
            }
            r.p.state = new_state;
            (self.persist_out(&mut st, id), gate)
        };
        self.settle(ack).await;
        if let Some((sid, rid)) = gate {
            self.withdraw_gate(&sid, &rid).await;
        }
        self.inner.kick.notify_one();
        Ok(())
    }

    /// `wasl.tasks.list`.
    pub fn tasks(&self) -> TaskList {
        let st = self.lock();
        let mut outbound: Vec<_> = st.out.values().map(OutRec::view).collect();
        let mut inbound: Vec<_> = st.inbound.values().map(InRec::view).collect();
        outbound.sort_by_key(|v| v.task.created_ms);
        inbound.sort_by_key(|v| v.task.created_ms);
        TaskList { outbound, inbound }
    }

    pub(crate) fn not_local(host: &str) -> RpcError {
        err(
            ctl_codes::HOST_UNKNOWN,
            format!("task addressed to host {host}, not this one"),
        )
    }
}

/// The state a handoff decision moves a pending task to.
fn decided_state(decision: &HandoffDecision, p: &crate::state::OutPersist) -> TaskState {
    let to_label = if p.to_name.is_empty() {
        p.task.to.to_string()
    } else {
        p.to_name.clone()
    };
    match decision {
        HandoffDecision::AllowOnce | HandoffDecision::AlwaysAllow => TaskState::Queued,
        HandoffDecision::Deny { reason } => TaskState::Denied {
            reason: match reason {
                Some(r) if !r.is_empty() => {
                    format!("the human denied the handoff to {to_label}: {r}")
                }
                _ => format!("the human denied the handoff to {to_label}"),
            },
        },
    }
}

pub(crate) fn finished_audit(task: &Task, state: &TaskState) -> AuditEntry {
    let detail = match state {
        TaskState::Completed { result } => Some(truncate(&result.summary, 300)),
        TaskState::Failed { error } => Some(error.clone()),
        TaskState::Cancelled { reason } | TaskState::Denied { reason } => Some(reason.clone()),
        _ => None,
    };
    AuditEntry {
        t_ms: now_ms(),
        workstream: task.workstream.clone(),
        actor: task.from.clone(),
        target: task.to.clone(),
        task: Some(task.id.clone()),
        kind: AuditKind::Finished {
            state: state.name().into(),
            detail,
        },
    }
}
