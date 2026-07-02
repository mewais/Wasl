# D1 — `wasl-dispatch` — send-side fan-out over A2A — spec (authoritative)

**Tier:** L2 (async; loopback-testable end-to-end). **Deps:** `wasl-proto`, `wasl-a2a-types`,
`wasl-a2a-client`, `wasl-registry`, `serde_json`, `reqwest`, `futures`. Dev: `wasl-a2a-server`,
`wasl-adapter`, `tokio`, `async-trait`, `axum`. No `unsafe`. This is the SEND side after the gate has
approved: resolve each target peer to its A2A endpoint, build an A2A `Message` carrying the Wasl verb, send
concurrently over the A2A client, and collect the replies.

**Testability (honesty):** the dispatch is exercised END-TO-END over loopback against a **real**
`wasl-a2a-server` running a **real** `wasl-adapter::AdapterExecutor` over a FAKE `AgentAdapter` — so the
whole send→receive→reply path (incl. the `metadata.wasl` verb convention) is verified on 127.0.0.1
(different ports = different "hosts", same code path). Only the last inch to a real CLI is a fake here (that
is `wasl-adapter`'s concern, UNTESTED HERE). True cross-host over SSH/WAN is the same code path, UNTESTED
HERE only for lack of a 2nd machine.

## Wire convention (must match `wasl-adapter`'s receive-side `delivery_from_message`)
Each outgoing A2A `Message` carries the Wasl verb + sender in `metadata`:
`{"wasl": {"kind": "consult"|"command"|"message", "from": "<sender agent id>"}}`, the body as a single
`Part::Text`, and a `contextId` (use `base_id`). The `messageId` is `"{base_id}:{i}"` for the i-th peer
(deterministic multicast ids). The loopback tests use a real `AdapterExecutor`, so a wrong shape here makes
the fake adapter observe the wrong `Delivery` and the test fails — the integration guards the convention.

## Types
```rust
#[derive(Debug, Clone, PartialEq, Eq, Default)]
pub struct DispatchReport {
    pub unknown: Vec<AgentId>, // targets absent from the registry OR with no A2A endpoint
    pub failed: Vec<AgentId>,  // resolved peers whose send/receive errored or returned a failed Task
}
```

## Peer resolution (private)
`resolve(registry, peer) -> Option<String>`: `registry.get(peer)` → its `AgentCard.endpoint` (the peer's
A2A base URL). `None` (unknown peer) or an endpoint of `None` → the peer is unroutable (goes to
`report.unknown`).

## Dispatch functions (async; concurrent fan-out)
```rust
/// Consult: reply text matters. kind = consult, body = params.question.
pub async fn consult(registry: &PeerRegistry, from: &AgentId, base_id: &str,
                     params: &ConsultParams, http: &reqwest::Client) -> (ConsultResult, DispatchReport);
/// Command: reply text matters. kind = command, body = params.task.
pub async fn command(registry: &PeerRegistry, from: &AgentId, base_id: &str,
                     params: &CommandParams, http: &reqwest::Client) -> (CommandResult, DispatchReport);
/// Message: delivery matters (no reply used). kind = message, body = params.payload.
pub async fn message(registry: &PeerRegistry, from: &AgentId, base_id: &str,
                     params: &MessageParams, http: &reqwest::Client) -> (MessageResult, DispatchReport);
```
Algorithm (shared, generic over the verb + body):
1. Partition targets: resolved `(peer, url)` in order + `unknown` (unresolvable).
2. For each resolved peer i, build `Message` (role user, one `Part::Text{body}`, `message_id = "{base_id}:{i}"`,
   `context_id = base_id`, `metadata = {"wasl":{"kind":k,"from":from}}`) and send via
   `A2aClient::with_client(url, http.clone()).send_message(msg)` — run all concurrently (`futures::join_all`).
3. For each result in peer order:
   - `Ok(task)` with `task.status.state == Completed` → success: `delivered.push(peer)`; reply text =
     the first `Part::Text` of the first artifact (via `wasl_a2a_types::part_text`) or `""` if none →
     `replies.push(Reply{ from: peer, body: reply })`.
   - `Ok(task)` with a non-Completed (e.g. `Failed`) state, OR `Err(_)` → `report.failed.push(peer)`
     (no reply, not delivered).
4. `consult`/`command` → `{Consult,Command}Result{ replies }`; `message` → `MessageResult{ delivered }`.
   Return `(result, DispatchReport{ unknown, failed })`.

## Test plan (`tests/loopback.rs`; async `#[tokio::test]`; line-100, region residual from HTTP/serde
monomorphization documented if any). Spin REAL `wasl-a2a-server`s on `127.0.0.1:0` with a real
`wasl_adapter::AdapterExecutor` over a FAKE `AgentAdapter` (records each `Delivery`; scriptable reply /
error). Register peers in a `PeerRegistry` with `AgentCard.endpoint = "http://127.0.0.1:{port}/"`. Share one
`reqwest::Client`. Bind listeners before spawn (no readiness race); short timeouts.
- D1  `consult([p1,p2])` (2 servers, adapters echo `"reply:{text}"`) → `ConsultResult.replies` =
      `[Reply{p1,"reply:q"}, Reply{p2,"reply:q"}]` IN ORDER; `report` empty. Assert BOTH fake adapters saw a
      `Delivery{ kind: Consult, from: caller, text: q }` — proving the verb + sender crossed the wire.
- D2  `command([p1])` → the adapter saw `kind: Command`; reply collected.
- D3  `message([p1,p2])` → `MessageResult.delivered` = `[p1,p2]`; adapters saw `kind: Message` (delivery,
      not reply-dependent).
- D4  unknown peer (not registered) mixed with a known one → `report.unknown` has it, it is NOT delivered,
      the known peer still succeeds.
- D5  peer registered but `endpoint: None` → `report.unknown` (unroutable), not sent.
- D6  transport failure: a peer whose endpoint points at a dead port (bind+drop) → `report.failed`; a
      co-addressed healthy peer still succeeds (partial multicast survives).
- D7  executor-failure: a peer whose fake adapter returns an `AdapterError` → the A2A Task comes back
      `Failed` → `report.failed` (distinct from transport failure, same bucket).
- D8  multicast ordering: 3 healthy peers → replies in target order; `message_id`s are `base:0/1/2` (assert
      via the fake adapter observing them, or that ordering holds).
- D9  `DispatchReport` Default + equality (empty vs populated).

Every test drives a REAL A2A server + real AdapterExecutor — no faking of the transport or the receive
executor; only the innermost `AgentAdapter` (the CLI last inch) is a fake. Do not weaken to unit-mocks.
