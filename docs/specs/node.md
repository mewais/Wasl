# D2 — `wasl-node` — the per-host send/receive unit — spec (authoritative)

**Tier:** L2 (async composition; two-node loopback-testable). **Deps:** `wasl-proto`, `wasl-server`,
`wasl-audit`, `wasl-registry`, `wasl-dispatch`, `wasl-a2a-types`, `wasl-a2a-server`, `wasl-adapter`,
`reqwest`, `tokio`. Dev: `async-trait`, `axum`. No `unsafe`. This composes the already-tested cores into one
node; it adds NO new decision or routing logic.

**Testability (honesty):** the SEND path (gate → dispatch) and the RECEIVE composer are verified END-TO-END
by a **two-node loopback e2e**: two real `Node`s on `127.0.0.1` (different ports = different "hosts", same
code path), agent A gates + dispatches a real consult/command/message to agent B whose real A2A server runs
a real `AdapterExecutor` over a FAKE `AgentAdapter`. Only the innermost real-CLI adapter is faked
(UNTESTED HERE — `wasl-adapter`'s concern). Real cross-host over SSH/WAN and a real CLI are UNTESTED HERE.

## Types
```rust
/// The outcome of a gated send.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum Sent<R> {
    Delivered { result: R, report: wasl_dispatch::DispatchReport }, // gate approved → dispatched
    Blocked { reason: String },                                     // gate denied / dismissed → NOT dispatched
    Pending { token: ApprovalToken },                              // Tier-2 → awaiting commit()
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub enum NodeError { Gate(String) }   // a gate/audit failure (ServerError). Display + std::error::Error.
```

## Node
```rust
pub struct Node<S: wasl_server::Surfacer, K: wasl_audit::Sink> {
    /* self_id, self_host, gate: Gatekeeper<S,K>, registry: PeerRegistry, http: reqwest::Client, seq counter */
}

impl<S: Surfacer, K: Sink> Node<S, K> {
    pub fn new(self_id: AgentId, self_host: HostId, caps: ClientCaps, surfacer: S, sink: K) -> Self;
    /// Register a peer (its `AgentCard.endpoint` is the peer's A2A base URL).
    pub fn register(&mut self, card: AgentCard);
    /// The recorded gate-decision log (delegates to the Gatekeeper's audit).
    pub fn audit_entries(&self) -> &[AuditEntry];
    pub fn pending_count(&self) -> usize;
    pub fn mode(&self) -> GateMode;

    /// SEND consult: gate it; on Proceed dispatch over A2A and return the replies. Denied/Dismissed →
    /// Blocked (no dispatch). Pending → Pending{token}. A gate/audit failure → Err(NodeError::Gate).
    pub async fn consult(&mut self, id: String, token: ApprovalToken, params: ConsultParams)
        -> Result<Sent<ConsultResult>, NodeError>;
    pub async fn command(&mut self, id: String, token: ApprovalToken, params: CommandParams)
        -> Result<Sent<CommandResult>, NodeError>;
    pub async fn message(&mut self, id: String, token: ApprovalToken, params: MessageParams)
        -> Result<Sent<MessageResult>, NodeError>;
    /// Resolve a Tier-2 pending gate, then (on approve) dispatch. `params` re-supplies the call to dispatch.
    /// (Kept explicit: the node doesn't buffer the original params.)
    pub async fn commit_consult(&mut self, token: &ApprovalToken, decision: GateDecision,
        params: ConsultParams) -> Result<Sent<ConsultResult>, NodeError>;
}

/// RECEIVE: build the A2A server for this node's local agent, backed by `adapter` (which injects into the
/// live agent). Thin composer over `wasl_a2a_server::A2aServer::new(card, AdapterExecutor::new(adapter))`.
pub fn receiver<A: wasl_adapter::AgentAdapter + 'static>(
    card: wasl_a2a_types::AgentCard, adapter: A) -> wasl_a2a_server::A2aServer;
```

### SEND flow (shared across consult/command/message)
1. `let verdict = self.gate.authorize(id, kind, self_id.clone(), params.peers.clone(), preview, token, seq)?;`
   (map `ServerError` → `NodeError::Gate`). `preview` = a secret-free summary (e.g. the question/task/payload).
2. Match:
   - `Verdict::Proceed` → `wasl_dispatch::{consult|command|message}(&registry, &self_id, base_id, &params, &http).await`
     → `Sent::Delivered { result, report }`.
   - `Verdict::Denied { reason }` → `Sent::Blocked { reason }` (NO dispatch).
   - `Verdict::Dismissed` → `Sent::Blocked { reason: "dismissed" }` (NO dispatch).
   - `Verdict::Pending { token }` → `Sent::Pending { token }`.
   The gate is never bypassed: only `Proceed` dispatches.
`base_id`/`seq` come from a per-node monotonic counter (deterministic within a node).

## Test plan (`tests/e2e.rs`; async `#[tokio::test]`; line-100). Build the receive side with a REAL
`receiver(card, fake_adapter)` served on `127.0.0.1:0`; register that peer in the sender node with
`endpoint = "http://127.0.0.1:{port}/"`. Fake `AgentAdapter` records deliveries + scriptable reply/err.
Fake `Surfacer` (approve/deny/dismiss) + a recording/failing audit `Sink` in the tests.
- N1  **approved consult, two nodes** — node A (surfacer approves) `consult([B])` → `Sent::Delivered`, replies
      carry B's echo, `report` empty; A's `audit_entries()` has one `Decision(Approve)`; B's fake adapter saw
      a `Delivery{kind:Consult, from:A}`. (The end-to-end proof: gate → A2A send → receive → reply → back.)
- N2  **command** two nodes → `Delivered`; B's adapter saw `kind:Command`.
- N3  **message** two nodes → `Delivered` with `MessageResult.delivered=[B]`; adapter saw `kind:Message`.
- N4  **multicast** consult([B,C]) across two servers → replies from both in order; report empty; both
      adapters saw the delivery.
- N5  **denied → no dispatch** — surfacer denies → `Sent::Blocked{reason}`; audit has `Some(Deny)`; assert the
      peer's adapter recorded NOTHING (nothing was sent).
- N6  **dismissed → Blocked** ("dismissed"), no dispatch, audit `decision: None`.
- N7  **Tier-2 pending → commit → dispatch** — caps prepare-commit → `consult` → `Sent::Pending{token}`,
      `pending_count()==1`, adapter untouched; `commit_consult(token, Approve, params)` → `Delivered` + audit
      `Some(Approve)` + the adapter now saw the delivery.
- N8  **partial multicast** — consult([B, ghost]) where ghost is unregistered → `report.unknown=[ghost]`,
      B still replies.
- N9  **gate/audit failure** — failing sink → `consult` → `Err(NodeError::Gate)`; the peer's adapter recorded
      NOTHING (aborted before dispatch — durability-first protects the send).
- N10 `Sent`/`NodeError` basics: `NodeError` Display + std::error::Error; `Sent` equality where useful.

Every send goes through the REAL gate + REAL A2A transport + REAL receive executor; only the innermost
`AgentAdapter` is a fake. Bind-before-spawn, short timeouts, abort/join server tasks so tests never hang.
Do NOT weaken to unit-mocks of the transport.
