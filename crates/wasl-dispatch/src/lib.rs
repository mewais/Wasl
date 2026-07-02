//! wasl-dispatch — the send-side fan-out: resolve peers and dispatch a gated
//! consult/command/message to them over A2A, concurrently, collecting replies.
//!
//! For each target peer it looks up the peer's A2A endpoint in the registry,
//! builds an A2A `Message` carrying the Wasl verb + sender in `metadata.wasl`
//! (the convention `wasl-adapter` reads on the receive side), sends it with the
//! A2A client, and extracts the reply from the returned Task. Unknown peers and
//! per-peer failures are partitioned into a report (partial multicast never
//! aborts the rest). Async; the A2A HTTP is the client's concern.
//! See `docs/specs/dispatch.md`.

use wasl_a2a_client::A2aClient;
use wasl_a2a_types::{part_text, Message, TaskState};
use wasl_proto::{
    AgentId, CommandParams, CommandResult, ConsultParams, ConsultResult, MessageParams,
    MessageResult, Reply,
};
use wasl_registry::PeerRegistry;

/// Per-dispatch partition of targets that never produced a reply: peers absent
/// from the registry (or with no A2A endpoint), and resolved peers whose send
/// errored or came back as a failed Task.
#[derive(Debug, Clone, PartialEq, Eq, Default)]
pub struct DispatchReport {
    /// Targets absent from the registry OR with no A2A endpoint.
    pub unknown: Vec<AgentId>,
    /// Resolved peers whose send/receive errored or returned a failed Task.
    pub failed: Vec<AgentId>,
}

/// A successfully delivered peer and the reply text it returned.
struct Delivered {
    peer: AgentId,
    body: String,
}

/// Shared fan-out over `(verb, body, peers)`. Returns the delivered peers (in
/// target order, each with reply text) plus the unknown/failed report. The three
/// public verbs are thin wrappers over this.
async fn dispatch(
    registry: &PeerRegistry,
    from: &AgentId,
    base_id: &str,
    verb: &str,
    body: &str,
    peers: &[AgentId],
    http: &reqwest::Client,
) -> (Vec<Delivered>, DispatchReport) {
    // 1. Partition targets in order into resolved `(peer, url)` and unknown.
    let mut resolved: Vec<(AgentId, String)> = Vec::new();
    let mut unknown: Vec<AgentId> = Vec::new();
    for peer in peers {
        match registry.get(peer).and_then(|card| card.endpoint.clone()) {
            Some(url) => resolved.push((peer.clone(), url)),
            None => unknown.push(peer.clone()),
        }
    }

    // 2. Build one A2A message per resolved peer, carrying the Wasl verb.
    let sends = resolved.iter().enumerate().map(|(i, (_, url))| {
        let mut msg = Message::user_text(body, format!("{base_id}:{i}"));
        msg.context_id = Some(base_id.to_string());
        msg.metadata = Some(serde_json::json!({
            "wasl": { "kind": verb, "from": from.as_str() }
        }));
        let client = A2aClient::with_client(url.clone(), http.clone());
        async move { client.send_message(msg).await }
    });

    // 3. Send all concurrently.
    let results = futures::future::join_all(sends).await;

    // 4. Walk results in peer order.
    let mut delivered = Vec::new();
    let mut failed = Vec::new();
    for ((peer, _), result) in resolved.into_iter().zip(results) {
        match result {
            Ok(task) if task.status.state == TaskState::Completed => {
                let body = task
                    .artifacts
                    .first()
                    .and_then(|a| a.parts.iter().find_map(part_text))
                    .unwrap_or("")
                    .to_string();
                delivered.push(Delivered { peer, body });
            }
            _ => failed.push(peer),
        }
    }

    (delivered, DispatchReport { unknown, failed })
}

/// Consult peers (reply text matters): `kind = consult`, `body = question`.
pub async fn consult(
    registry: &PeerRegistry,
    from: &AgentId,
    base_id: &str,
    params: &ConsultParams,
    http: &reqwest::Client,
) -> (ConsultResult, DispatchReport) {
    let (delivered, report) = dispatch(
        registry,
        from,
        base_id,
        "consult",
        &params.question,
        &params.peers,
        http,
    )
    .await;
    let replies = delivered
        .into_iter()
        .map(|d| Reply {
            from: d.peer,
            body: d.body,
        })
        .collect();
    (ConsultResult { replies }, report)
}

/// Command peers (reply text matters): `kind = command`, `body = task`.
pub async fn command(
    registry: &PeerRegistry,
    from: &AgentId,
    base_id: &str,
    params: &CommandParams,
    http: &reqwest::Client,
) -> (CommandResult, DispatchReport) {
    let (delivered, report) = dispatch(
        registry,
        from,
        base_id,
        "command",
        &params.task,
        &params.peers,
        http,
    )
    .await;
    let replies = delivered
        .into_iter()
        .map(|d| Reply {
            from: d.peer,
            body: d.body,
        })
        .collect();
    (CommandResult { replies }, report)
}

/// Message peers (delivery matters, no reply used): `kind = message`,
/// `body = payload`.
pub async fn message(
    registry: &PeerRegistry,
    from: &AgentId,
    base_id: &str,
    params: &MessageParams,
    http: &reqwest::Client,
) -> (MessageResult, DispatchReport) {
    let (delivered, report) = dispatch(
        registry,
        from,
        base_id,
        "message",
        &params.payload,
        &params.peers,
        http,
    )
    .await;
    let delivered = delivered.into_iter().map(|d| d.peer).collect();
    (MessageResult { delivered }, report)
}
