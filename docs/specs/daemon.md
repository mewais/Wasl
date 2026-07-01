# WD — `wasl-daemon` (routing core) — spec (authoritative)

**Tier:** L1 (pure routing + trait-mocked delivery). **Deps:** `wasl-proto`, `wasl-registry`,
`wasl-mailbox`, `wasl-transfer`, `serde` (+ `serde_json` dev). No `unsafe`. The ONLY IO is behind the
`Mesh` trait (envelope delivery, local or cross-host) and `wasl_transfer::Runner` (transfer exec) — unit
tests use fakes. The real socket event loop / daemon binary is a later thin shell, NOT this block.

This is the routing core the daemon runs AFTER the gate has approved a call. It resolves peers against the
registry, fans out one envelope per peer (multicast/broadcast), delivers each along its route (local
in-process or remote to the peer's host), and collects the replies — reusing `wasl_registry::resolve_many`
and `wasl_mailbox::fan_out`. Transfer delegates to `wasl_transfer::execute`.

Re-use: `wasl_proto::{AgentId, HostId, EnvelopeKind, Envelope, Reply, ConsultParams, CommandParams,
MessageParams, ConsultResult, CommandResult, MessageResult, TransferParams, TransferResult}`,
`wasl_registry::{PeerRegistry, Resolved}`, `wasl_mailbox::fan_out`,
`wasl_transfer::{Tool, Runner, TransferError, execute}`.

## Types
```rust
/// Delivery seam: hand one envelope to a resolved peer (local in-process or
/// remote over the network) and return the peer's reply body. The impl decides
/// transport from the `Resolved` route; the routing core stays IO-free.
pub trait Mesh {
    fn deliver(&mut self, resolved: &Resolved, env: &Envelope) -> Result<String, String>;
}

/// What happened to the addressed peers on a dispatch (beyond the replies).
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize, Default)]
pub struct DispatchReport {
    pub unknown: Vec<AgentId>,  // targets not in the registry (never delivered)
    pub failed: Vec<AgentId>,   // resolved peers whose delivery errored
}
```

## Core dispatch
```rust
/// Resolve `targets`, fan out one envelope per resolved peer (ids `base_id:0..`),
/// deliver each via `mesh`, and collect the results. Order follows the resolved
/// order (which follows `targets` order). Unknown peers go to `report.unknown`;
/// per-peer delivery errors go to `report.failed` and do NOT abort the rest
/// (partial multicast). Successful peers appear in `delivered`, their replies in
/// `replies` (in delivery order).
pub struct Dispatch {
    pub replies: Vec<Reply>,      // one per successfully-delivered peer (from = that peer)
    pub delivered: Vec<AgentId>,  // successfully-delivered peers, in order
    pub report: DispatchReport,
}

pub fn dispatch(
    registry: &PeerRegistry,
    self_host: &HostId,
    base_id: &str,
    from: &AgentId,
    kind: EnvelopeKind,
    body: &str,
    targets: &[AgentId],
    mesh: &mut dyn Mesh,
) -> Dispatch;
```
Algorithm:
1. `let (resolved, unknown) = registry.resolve_many(self_host, targets);`
2. `let peers: Vec<AgentId> = resolved.iter().map(peer_of).collect();` where `peer_of` matches
   `Resolved::Local{peer} | Resolved::Remote{peer,..}` (a private helper).
3. `let envs = fan_out(base_id, from, kind, body, &peers);` (ids `base_id:i`, one per peer).
4. For each `(env, res)` in `envs.iter().zip(resolved.iter())`: `match mesh.deliver(res, env)` →
   `Ok(reply_body)` push `delivered = peer`, `replies.push(Reply{ from: peer, body: reply_body })`;
   `Err(_)` push `report.failed = peer`.
5. `report.unknown = unknown`. Return `Dispatch{ replies, delivered, report }`.

## Typed tool wrappers (thin — build the proto result from a `Dispatch`)
```rust
/// Consult: replies are the point. `kind = EnvelopeKind::Consult`, body = question.
pub fn consult(registry:&PeerRegistry, self_host:&HostId, base_id:&str, from:&AgentId,
               params:&ConsultParams, mesh:&mut dyn Mesh) -> (ConsultResult, DispatchReport);
/// Command: same shape, body = task, `EnvelopeKind::Command`.
pub fn command(registry:&PeerRegistry, self_host:&HostId, base_id:&str, from:&AgentId,
               params:&CommandParams, mesh:&mut dyn Mesh) -> (CommandResult, DispatchReport);
/// Message: delivery is the point (no replies used). body = payload, `EnvelopeKind::Message`.
pub fn message(registry:&PeerRegistry, self_host:&HostId, base_id:&str, from:&AgentId,
               params:&MessageParams, mesh:&mut dyn Mesh) -> (MessageResult, DispatchReport);
```
- `consult`/`command` → `ConsultResult{ replies }` / `CommandResult{ replies }` from `Dispatch.replies`.
- `message` → `MessageResult{ delivered }` from `Dispatch.delivered`.
- All three return the `Dispatch.report` alongside so the caller sees unknown/failed peers.

## Transfer (delegates to the transfer tool)
```rust
/// Cross-host transfer is host-addressed, not peer-addressed — delegate to the
/// transfer tool over the `Runner` seam.
pub fn transfer(tool: Tool, params: &TransferParams, runner: &mut dyn Runner)
    -> Result<(TransferResult, Vec<TransferError>), TransferError>;
```
(= `wasl_transfer::execute(tool, params, runner)`.)

## Test plan (`tests/happy.rs`, `tests/errors.rs`; 100% line+region). Real asserting `#[test]`s. Provide a
**fake Mesh** (records each `(Resolved, Envelope)` it's asked to deliver; scriptable to return a reply body
per peer or to fail specific peers) and reuse a **fake Runner** for transfer.
**happy.rs**
- D1  `dispatch` all-local consult: 2 registered same-host peers → `delivered` = both in order, `replies`
      carry each peer's `from` + the fake's reply body, `report` empty; the fake Mesh saw envelope ids
      `base:0`/`base:1` with `kind == Consult` and the right `to`.
- D2  cross-host: a remote peer (different host) → the fake Mesh receives a `Resolved::Remote{host}` and the
      reply is collected; assert the resolved route was Remote.
- D3  `command`: builds `CommandResult{replies}` (kind Command, body = task); report empty.
- D4  `message`: builds `MessageResult{delivered}` from delivered peers (kind Message, body = payload); it
      does NOT depend on reply bodies.
- D5  multicast ordering: 3 peers → 3 envelopes ids `base:0..2` in target order; replies in the same order.
- D6  unknown peer: a target not registered → `report.unknown` contains it, it is NOT delivered, and the
      other peers still succeed.
- D7  partial failure: fake Mesh fails the 2nd of 3 peers → `delivered` = [1st,3rd], `report.failed` =
      [2nd], replies only for the delivered (proves a failure doesn't abort the rest).
- D8  empty targets → empty `Dispatch` (no delivery, empty results); the Mesh is never called.
- D9  `transfer` all-success via fake Runner → `TransferResult.delivered` = all dst hosts, no failures.
- D10 `DispatchReport` serde round-trip (default empty + populated).
**errors.rs**
- E1  `transfer` empty dst_hosts → `Err(TransferError::NoDestinations)` (runner never called).
- E2  `transfer` partial: fake Runner fails one dst → `Ok((TransferResult, [Failed{that host}]))`.
- E3  all-unknown: every target unregistered → `delivered` empty, `report.unknown` = all, Mesh never called.

Coverage: hit local + remote + unknown + failed in dispatch; each typed wrapper; transfer ok/partial/empty.
Delete unreachable branches rather than leaving them uncovered.
