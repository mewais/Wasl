//! wasl-node — the per-host send/receive unit (D2).
//!
//! Pure composition of already-tested cores: the permission-before-send gate
//! (`wasl_server::Gatekeeper` over a `Surfacer` + audit `Sink`), the send-side
//! fan-out (`wasl_dispatch`) over the peer registry (`wasl_registry`), and the
//! A2A receive server (`wasl_a2a_server` + `wasl_adapter`). It adds NO new
//! decision or routing logic — every send goes through the real gate, and only a
//! `Proceed` verdict dispatches. See `docs/specs/node.md`.

use std::future::Future;

use wasl_audit::{AuditEntry, Sink};
use wasl_dispatch::DispatchReport;
use wasl_proto::{
    AgentCard, AgentId, ApprovalToken, CommandParams, CommandResult, ConsultParams, ConsultResult,
    GateDecision, GateKind, HostId, MessageParams, MessageResult,
};
use wasl_registry::PeerRegistry;
use wasl_server::{Gatekeeper, ServerError, Surfacer, Verdict};

// The gate-facade types live in `wasl-gate`; the node re-exports the three that
// reach a consumer of its public API (via `Node::new(caps)`, `mode()`, and the
// `Surfacer` signature the tests implement).
pub use wasl_gate::{ClientCaps, GateMode, GateOutcome};

// ── Types ─────────────────────────────────────────────────────────────────────

/// The outcome of a gated send.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum Sent<R> {
    /// The gate approved and the call was dispatched over A2A.
    Delivered { result: R, report: DispatchReport },
    /// The gate denied/dismissed the call — it was NOT dispatched.
    Blocked { reason: String },
    /// Tier-2: the gate deferred; awaiting a later `commit`.
    Pending { token: ApprovalToken },
}

/// A gate/audit failure (a `wasl_server::ServerError`).
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum NodeError {
    Gate(String),
}

impl std::fmt::Display for NodeError {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            NodeError::Gate(detail) => write!(f, "gate error: {detail}"),
        }
    }
}

impl std::error::Error for NodeError {}

/// Map a gate/audit `ServerError` into a `NodeError`. Kept in one place so its
/// (error-only) body is exercised once and shared by every gate call.
fn gate_err(result: Result<Verdict, ServerError>) -> Result<Verdict, NodeError> {
    result.map_err(|e| NodeError::Gate(e.to_string()))
}

// ── Node ────────────────────────────────────────────────────────────────────

/// The per-host send/receive unit: gate a call, then (on approval) dispatch it.
pub struct Node<S: Surfacer, K: Sink> {
    self_id: AgentId,
    /// This node's host identity (part of the node's identity; routing over the
    /// mesh is the daemon's concern, so it is not read on the loopback send path).
    #[allow(dead_code)]
    self_host: HostId,
    gate: Gatekeeper<S, K>,
    registry: PeerRegistry,
    http: reqwest::Client,
    counter: u64,
}

impl<S: Surfacer, K: Sink> Node<S, K> {
    /// Build a node with a fresh peer registry and HTTP client.
    pub fn new(
        self_id: AgentId,
        self_host: HostId,
        caps: ClientCaps,
        surfacer: S,
        sink: K,
    ) -> Self {
        Self {
            self_id,
            self_host,
            gate: Gatekeeper::new(caps, surfacer, sink),
            registry: PeerRegistry::new(),
            http: reqwest::Client::new(),
            counter: 0,
        }
    }

    /// Register a peer (its `AgentCard.endpoint` is the peer's A2A base URL).
    pub fn register(&mut self, card: AgentCard) {
        self.registry.register(card);
    }

    /// The recorded gate-decision log.
    pub fn audit_entries(&self) -> &[AuditEntry] {
        self.gate.audit_entries()
    }

    /// The number of deferred (Tier-2) gates awaiting a commit.
    pub fn pending_count(&self) -> usize {
        self.gate.pending_count()
    }

    /// The gate mode selected for this node's capabilities.
    pub fn mode(&self) -> GateMode {
        self.gate.mode()
    }

    /// The next deterministic per-node sequence number.
    fn next_seq(&mut self) -> u64 {
        let n = self.counter;
        self.counter += 1;
        n
    }

    /// SEND: authorize through the gate, then (only on `Proceed`) dispatch the
    /// verb via `dispatch` and return the replies. Verdict/error handling lives in
    /// `finish`, shared with `commit_consult`.
    async fn send<R, F, Fut>(
        &mut self,
        id: String,
        kind: GateKind,
        peers: Vec<AgentId>,
        preview: String,
        token: ApprovalToken,
        dispatch: F,
    ) -> Result<Sent<R>, NodeError>
    where
        F: FnOnce(PeerRegistry, AgentId, String, reqwest::Client) -> Fut,
        Fut: Future<Output = (R, DispatchReport)>,
    {
        let base_id = id.clone();
        let seq = self.next_seq();
        let gated = self
            .gate
            .authorize(id, kind, self.self_id.clone(), peers, preview, token, seq);
        self.finish(gated, base_id, dispatch).await
    }

    /// Map a gate result to a `Sent`: propagate a gate/audit failure, otherwise
    /// dispatch the verb only on `Proceed`. The single `?` and the verdict match
    /// are shared by every send and by `commit_consult`.
    async fn finish<R, F, Fut>(
        &self,
        gated: Result<Verdict, ServerError>,
        base_id: String,
        dispatch: F,
    ) -> Result<Sent<R>, NodeError>
    where
        F: FnOnce(PeerRegistry, AgentId, String, reqwest::Client) -> Fut,
        Fut: Future<Output = (R, DispatchReport)>,
    {
        let verdict = gate_err(gated)?;
        match verdict {
            Verdict::Proceed => {
                let (result, report) = dispatch(
                    self.registry.clone(),
                    self.self_id.clone(),
                    base_id,
                    self.http.clone(),
                )
                .await;
                Ok(Sent::Delivered { result, report })
            }
            Verdict::Denied { reason } => Ok(Sent::Blocked { reason }),
            Verdict::Dismissed => Ok(Sent::Blocked {
                reason: "dismissed".to_string(),
            }),
            Verdict::Pending { token } => Ok(Sent::Pending { token }),
        }
    }

    /// SEND consult: gate it; on `Proceed` dispatch over A2A and return the replies.
    pub async fn consult(
        &mut self,
        id: String,
        token: ApprovalToken,
        params: ConsultParams,
    ) -> Result<Sent<ConsultResult>, NodeError> {
        let peers = params.peers.clone();
        let preview = params.question.clone();
        self.send(
            id,
            GateKind::Consult,
            peers,
            preview,
            token,
            move |registry, from, base_id, http| async move {
                wasl_dispatch::consult(&registry, &from, &base_id, &params, &http).await
            },
        )
        .await
    }

    /// SEND command: gate it; on `Proceed` dispatch over A2A and return the replies.
    pub async fn command(
        &mut self,
        id: String,
        token: ApprovalToken,
        params: CommandParams,
    ) -> Result<Sent<CommandResult>, NodeError> {
        let peers = params.peers.clone();
        let preview = params.task.clone();
        self.send(
            id,
            GateKind::Command,
            peers,
            preview,
            token,
            move |registry, from, base_id, http| async move {
                wasl_dispatch::command(&registry, &from, &base_id, &params, &http).await
            },
        )
        .await
    }

    /// SEND message: gate it; on `Proceed` dispatch over A2A and return delivery.
    pub async fn message(
        &mut self,
        id: String,
        token: ApprovalToken,
        params: MessageParams,
    ) -> Result<Sent<MessageResult>, NodeError> {
        let peers = params.peers.clone();
        let preview = params.payload.clone();
        self.send(
            id,
            GateKind::Message,
            peers,
            preview,
            token,
            move |registry, from, base_id, http| async move {
                wasl_dispatch::message(&registry, &from, &base_id, &params, &http).await
            },
        )
        .await
    }

    /// Resolve a Tier-2 pending gate, then (on approve) dispatch. `params`
    /// re-supplies the call to dispatch (the node does not buffer the original).
    pub async fn commit_consult(
        &mut self,
        token: &ApprovalToken,
        decision: GateDecision,
        params: ConsultParams,
    ) -> Result<Sent<ConsultResult>, NodeError> {
        let seq = self.next_seq();
        let base_id = format!("commit-{seq}");
        let gated = self.gate.commit(token, decision, seq);
        self.finish(
            gated,
            base_id,
            move |registry, from, base_id, http| async move {
                wasl_dispatch::consult(&registry, &from, &base_id, &params, &http).await
            },
        )
        .await
    }
}

// ── RECEIVE ─────────────────────────────────────────────────────────────────

/// Build the A2A server for this node's local agent, backed by `adapter` (which
/// injects into the live agent). A thin composer over `A2aServer::new`.
pub fn receiver<A: wasl_adapter::AgentAdapter + 'static>(
    card: wasl_a2a_types::AgentCard,
    adapter: A,
) -> wasl_a2a_server::A2aServer {
    wasl_a2a_server::A2aServer::new(
        card,
        std::sync::Arc::new(wasl_adapter::AdapterExecutor::new(adapter)),
    )
}
