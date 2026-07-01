//! wasl-daemon — the per-host daemon routing core (WD).
//!
//! Resolves peers against the registry and dispatches an already-gated tool call:
//! consult/command/message fan out one envelope per peer (multicast) and collect
//! replies; transfer delegates to the transfer tool. Local vs cross-host delivery
//! is decided here and performed behind the `Mesh` trait (`wasl_transfer::Runner`
//! for transfers) — the real socket event loop is a later thin binary shell.
//! See `docs/specs/daemon.md`.

use serde::{Deserialize, Serialize};

use wasl_mailbox::fan_out;
use wasl_proto::{
    AgentId, CommandParams, CommandResult, ConsultParams, ConsultResult, Envelope, EnvelopeKind,
    HostId, MessageParams, MessageResult, Reply, TransferParams, TransferResult,
};
use wasl_registry::{PeerRegistry, Resolved};
use wasl_transfer::{Runner, Tool, TransferError};

// ---------------------------------------------------------------------
// 1. Delivery seam
// ---------------------------------------------------------------------

/// Delivery seam: hand one envelope to a resolved peer (local in-process or
/// remote over the network) and return the peer's reply body. The impl decides
/// transport from the [`Resolved`] route; the routing core stays IO-free.
pub trait Mesh {
    fn deliver(&mut self, resolved: &Resolved, env: &Envelope) -> Result<String, String>;
}

// ---------------------------------------------------------------------
// 2. Dispatch types
// ---------------------------------------------------------------------

/// What happened to the addressed peers on a dispatch (beyond the replies).
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize, Default)]
pub struct DispatchReport {
    /// Targets not in the registry (never delivered).
    pub unknown: Vec<AgentId>,
    /// Resolved peers whose delivery errored.
    pub failed: Vec<AgentId>,
}

/// The outcome of a dispatch: replies from successfully-delivered peers, the
/// delivered peers themselves, and the report of unknown/failed peers.
pub struct Dispatch {
    /// One per successfully-delivered peer (`from` = that peer), in delivery order.
    pub replies: Vec<Reply>,
    /// Successfully-delivered peers, in order.
    pub delivered: Vec<AgentId>,
    pub report: DispatchReport,
}

/// The resolved route's peer (works for both `Local` and `Remote`).
fn peer_of(resolved: &Resolved) -> AgentId {
    match resolved {
        Resolved::Local { peer } => peer.clone(),
        Resolved::Remote { peer, .. } => peer.clone(),
    }
}

// ---------------------------------------------------------------------
// 3. Core dispatch
// ---------------------------------------------------------------------

/// Resolve `targets`, fan out one envelope per resolved peer (ids `base_id:0..`),
/// deliver each via `mesh`, and collect the results. Order follows the resolved
/// order (which follows `targets` order). Unknown peers go to `report.unknown`;
/// per-peer delivery errors go to `report.failed` and do NOT abort the rest
/// (partial multicast).
#[allow(clippy::too_many_arguments)]
pub fn dispatch(
    registry: &PeerRegistry,
    self_host: &HostId,
    base_id: &str,
    from: &AgentId,
    kind: EnvelopeKind,
    body: &str,
    targets: &[AgentId],
    mesh: &mut dyn Mesh,
) -> Dispatch {
    let (resolved, unknown) = registry.resolve_many(self_host, targets);
    let peers: Vec<AgentId> = resolved.iter().map(peer_of).collect();
    let envs = fan_out(base_id, from, kind, body, &peers);

    let mut replies = Vec::new();
    let mut delivered = Vec::new();
    let mut report = DispatchReport::default();

    for (env, res) in envs.iter().zip(resolved.iter()) {
        match mesh.deliver(res, env) {
            Ok(body) => {
                delivered.push(peer_of(res));
                replies.push(Reply {
                    from: peer_of(res),
                    body,
                });
            }
            Err(_) => report.failed.push(peer_of(res)),
        }
    }

    report.unknown = unknown;
    Dispatch {
        replies,
        delivered,
        report,
    }
}

// ---------------------------------------------------------------------
// 4. Typed tool wrappers
// ---------------------------------------------------------------------

/// Consult: replies are the point. `kind = EnvelopeKind::Consult`, body = question.
pub fn consult(
    registry: &PeerRegistry,
    self_host: &HostId,
    base_id: &str,
    from: &AgentId,
    params: &ConsultParams,
    mesh: &mut dyn Mesh,
) -> (ConsultResult, DispatchReport) {
    let out = dispatch(
        registry,
        self_host,
        base_id,
        from,
        EnvelopeKind::Consult,
        &params.question,
        &params.peers,
        mesh,
    );
    (
        ConsultResult {
            replies: out.replies,
        },
        out.report,
    )
}

/// Command: same shape, body = task, `EnvelopeKind::Command`.
pub fn command(
    registry: &PeerRegistry,
    self_host: &HostId,
    base_id: &str,
    from: &AgentId,
    params: &CommandParams,
    mesh: &mut dyn Mesh,
) -> (CommandResult, DispatchReport) {
    let out = dispatch(
        registry,
        self_host,
        base_id,
        from,
        EnvelopeKind::Command,
        &params.task,
        &params.peers,
        mesh,
    );
    (
        CommandResult {
            replies: out.replies,
        },
        out.report,
    )
}

/// Message: delivery is the point (no replies used). body = payload,
/// `EnvelopeKind::Message`.
pub fn message(
    registry: &PeerRegistry,
    self_host: &HostId,
    base_id: &str,
    from: &AgentId,
    params: &MessageParams,
    mesh: &mut dyn Mesh,
) -> (MessageResult, DispatchReport) {
    let out = dispatch(
        registry,
        self_host,
        base_id,
        from,
        EnvelopeKind::Message,
        &params.payload,
        &params.peers,
        mesh,
    );
    (
        MessageResult {
            delivered: out.delivered,
        },
        out.report,
    )
}

// ---------------------------------------------------------------------
// 5. Transfer (delegates to the transfer tool)
// ---------------------------------------------------------------------

/// Cross-host transfer is host-addressed, not peer-addressed — delegate to the
/// transfer tool over the `Runner` seam.
pub fn transfer(
    tool: Tool,
    params: &TransferParams,
    runner: &mut dyn Runner,
) -> Result<(TransferResult, Vec<TransferError>), TransferError> {
    wasl_transfer::execute(tool, params, runner)
}
