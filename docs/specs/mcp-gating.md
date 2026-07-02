# W4 — `wasl-mcp` gating v2 — real caps, gate authority, operator wiring, Tier-2 — spec (authoritative)

**Tier:** L2 (changes inside the existing IO shell `wasl-mcp`; loopback-testable). **Deps added to
`wasl-mcp`:** `wasl-operator` (hub/surfacer/tee/router), `wasl-server` (the `Surfacer` trait, needed to
call `OperatorSurfacer::surface`). No new external crates; `axum`/`tokio`/`reqwest` are already in the
tree. Dev: none new (`rmcp` client features and `reqwest` are already dev-deps). No `unsafe`. Read first:
`docs/specs/{gate.md,operator.md,operator-api.md,node-config.md,mcp-a2a.md}`.

## Why — the four defects this block retires

All in `crates/wasl-mcp/src/{server,handle}.rs` today:

1. **Fake capability detection.** `surface_gate` (server.rs) hard-codes the caps and throws away the
   mode the tested core chose:

   ```rust
   let caps = ClientCaps { elicitation: true, external: false };
   let _mode = choose_mode(&caps);
   ```

   The real client capability is only consulted afterwards, ad hoc, via `peer.supports_elicitation()`.
   There is exactly one tier wired (elicitation-or-deny); `GateMode::External` and
   `GateMode::PrepareCommit` are unreachable from the MCP facade.

2. **Duplicated decision mapping.** server.rs re-implements the `ElicitationAction → GateOutcome`
   table that `wasl_gate::outcome_of` already owns (and tests):

   ```rust
   Ok(result) => match result.action {
       ElicitationAction::Accept => GateOutcome::Approved,
       ElicitationAction::Decline => GateOutcome::Denied { reason: "declined".to_string() },
       ElicitationAction::Cancel => GateOutcome::Dismissed,
   },
   ```

3. **`NoDestinations` swallowed as success.** handle.rs (`dispatch_approved`, ~line 219) carries a
   comment promising a blocked reply and then returns an EMPTY SUCCESS:

   ```rust
   // A transfer with no destinations is a caller error, not a gate
   // block — surface it as a blocked reply carrying the reason (no
   // silent success, no dispatch of anything else).
   Err(TransferError::NoDestinations) => ToolReply::Transfer(TransferResult {
       delivered: Vec::new(),
   }),
   ```

   The agent is told the transfer "succeeded to zero hosts". (mcp-a2a.md pinned this; THIS spec
   supersedes that line.)

4. **`operator.bind` parsed but dead.** W2 validates `[operator] bind` (config.rs
   `OperatorConfig { pub bind: Option<String> }`, doc: "RESERVED FOR W3") and W3 shipped the whole
   `wasl-operator` crate — but `run()` still "deliberately ignores" it. No operator surface ever
   starts; `OperatorSurfacer`/`TeeSink`/`announce` have no production caller.

This block makes the MCP facade run the REAL three-tier gate: detect the client's elicitation
capability from the rmcp handshake, pick the tier by a configured authority policy, serve the operator
router when bound, mirror Tier-1 gates to the operator feed, and make Tier-2 prepare→commit reachable
through the tools.

## 1. Config — the `[gate]` section + consuming `operator.bind`

```toml
[gate]
authority = "elicitation"   # "elicitation" (default) | "elicitation-first" | "external-first" | "prepare-commit"

[operator]                  # W2 already parses/validates this; W4 CONSUMES it
bind = "127.0.0.1:4243"     # socket address; default absent = operator surface not started
```

```rust
// crates/wasl-mcp/src/gating.rs (new module; re-exported from the crate root)
/// Which authority may decide a gate, in priority order. Chosen ONCE per call
/// from config + detected caps (see select_mode).
#[derive(Debug, Clone, Copy, PartialEq, Eq, Default)]
pub enum GateAuthority {
    /// Tier-1 or nothing: elicitation-capable client → Elicitation; otherwise
    /// auto-deny. EXACTLY today's behavior; the default.
    #[default]
    Elicitation,
    /// Tier-1 → Tier-3 → Tier-2: delegate to `wasl_gate::choose_mode` verbatim
    /// (elicitation > external > prepare_commit).
    ElicitationFirst,
    /// Tier-3 → Tier-1 → Tier-2: prefer the operator even for elicitation-capable
    /// clients (central approval desks).
    ExternalFirst,
    /// Always Tier-2: every call defers via prepare→commit regardless of caps.
    PrepareCommit,
}
```

- Config shape: `Config` gains `pub gate: GateConfig` with
  `pub struct GateConfig { pub authority: GateAuthority }`; `Default` = `Elicitation`.
- TOML: `[gate]` is `#[serde(deny_unknown_fields, default)]` like every other section; `authority`
  deserializes as a plain `String` and is validated in the explicit second step (the `adapter.kind`
  pattern): exact, case-sensitive match on the four strings above; anything else →
  `ConfigError::UnknownGateAuthority(String)`, Display `"unknown gate authority: {value}"`.
  Validation order becomes: TOML syntax/shape → binds → adapter kind → **gate authority** → peer urls
  → duplicate peers.
- Precedence table (node-config.md) gains one row: `gate.authority` — no env var, file key
  `gate.authority`, built-in default `"elicitation"`. There are NO new env vars in this block.
- `operator.bind`: unchanged shape; `run()` now consumes it (§6). No `[operator]` timeout key in this
  block — the hub runs `HubConfig::default()` (120 s); a later block may add
  `operator.decision_timeout_ms`.

## 2. Real `ClientCaps` detection (the rmcp 0.9 surface)

The rmcp handshake already carries the client's declared capabilities; the server-side peer exposes
them. Actual rmcp 0.9.1 API (verified in the vendored source):

```rust
// rmcp-0.9.1/src/service.rs
impl<R: ServiceRole> Peer<R> { pub fn peer_info(&self) -> Option<&R::PeerInfo> }
// rmcp-0.9.1/src/service/server.rs — RoleServer::PeerInfo = ClientInfo
// rmcp-0.9.1/src/model.rs
pub type ClientInfo = InitializeRequestParam;   // { protocol_version, capabilities: ClientCapabilities, client_info }
// rmcp-0.9.1/src/model/capabilities.rs
pub struct ClientCapabilities { ..., pub elicitation: Option<ElicitationCapability> }
// rmcp-0.9.1/src/service/server.rs (feature "elicitation", already enabled)
impl Peer<RoleServer> {
    pub fn supports_elicitation(&self) -> bool {
        if let Some(client_info) = self.peer_info() {
            client_info.capabilities.elicitation.is_some()
        } else { false }
    }
}
```

New pure seam in `gating.rs` (unit-testable without a transport — `InitializeRequestParam` and the
`ClientCapabilities::builder().enable_elicitation().build()` builder are plain public types):

```rust
/// The REAL ClientCaps for this call: elicitation from the client's declared
/// MCP capabilities (None peer_info — pre-initialize — counts as false, matching
/// rmcp's supports_elicitation), external = an OperatorHub is attached.
pub fn detect_caps(client: Option<&rmcp::model::ClientInfo>, operator_attached: bool) -> ClientCaps {
    ClientCaps {
        elicitation: client.map(|c| c.capabilities.elicitation.is_some()).unwrap_or(false),
        external: operator_attached,
    }
}
```

The rmcp layer calls it as `detect_caps(peer.peer_info(), self.hub.is_some())`. The hard-coded
`ClientCaps { elicitation: true, external: false }` and the discarded `let _mode = choose_mode(&caps)`
are DELETED.

## 3. Mode selection

```rust
/// Select the tier for this call, or None = auto-deny (no authority can decide).
/// ElicitationFirst is wasl_gate::choose_mode verbatim — do not re-derive it.
pub fn select_mode(authority: GateAuthority, caps: &ClientCaps) -> Option<GateMode> {
    match authority {
        GateAuthority::Elicitation =>
            caps.elicitation.then_some(GateMode::Elicitation),
        GateAuthority::ElicitationFirst => Some(wasl_gate::choose_mode(caps)),
        GateAuthority::ExternalFirst => Some(if caps.external { GateMode::External }
            else if caps.elicitation { GateMode::Elicitation }
            else { GateMode::PrepareCommit }),
        GateAuthority::PrepareCommit => Some(GateMode::PrepareCommit),
    }
}
```

Truth table (normative):

| authority          | elicitation, external | mode           |
|--------------------|-----------------------|----------------|
| elicitation        | true, *               | Elicitation    |
| elicitation        | false, *              | **None (deny)** — reason pinned: `"client cannot be prompted (no elicitation capability)"` (today's exact string) |
| elicitation-first  | true, *               | Elicitation    |
| elicitation-first  | false, true           | External       |
| elicitation-first  | false, false          | PrepareCommit  |
| external-first     | *, true               | External       |
| external-first     | true, false           | Elicitation    |
| external-first     | false, false          | PrepareCommit  |
| prepare-commit     | *, *                  | PrepareCommit  |

The `None` deny flows through `resolve_and_dispatch` exactly like today's no-elicitation deny (audited
`Deny`, `ToolReply::Blocked`, nothing dispatched).

## 4. The per-call flow (server.rs) — one path per tier

`WaslMcp` gains two fields and one constructor:

```rust
pub struct WaslMcp<K: Sink, R: Runner> { /* existing fields, plus: */
    authority: GateAuthority,
    hub: Option<OperatorHub>,
}
/// Existing signature preserved: new(..) == with_gating(.., GateAuthority::Elicitation, None).
pub fn new(self_id, self_host, registry, state) -> Self;
pub fn with_gating(self_id, self_host, registry, state,
                   authority: GateAuthority, hub: Option<OperatorHub>) -> Self;
```

`WaslState` gains the Tier-2 tables (constructed empty by the existing `WaslState::new` — its
signature does not change):

```rust
pub struct WaslState<K: Sink, R: Runner> { /* existing fields, plus: */
    pub ledger: GateLedger,                                   // wasl_gate — token validity
    pub pending_calls: HashMap<ApprovalToken, ToolCall>,      // in lockstep with ledger.pending
    pub decisions: HashMap<ApprovalToken, GateOutcome>,       // arrived human decisions, first wins
}
```

`gated_call` becomes: detect caps → `select_mode` → branch:

- **`None` (deny):** `outcome = Denied { reason: "client cannot be prompted (no elicitation
  capability)" }` → `resolve_and_dispatch` as today.

- **`Some(Elicitation)` — Tier-1, mirrored observe-only when the operator is on:**
  1. `req = build_request("gate".into(), kind, from, targets, preview)` (id unchanged — the wire id
     for mirrored frames is the hub's, not the request's).
  2. If `hub` is `Some`: `let gate_id = hub.announce(GateMode::Elicitation, &req);` — broadcast
     `GateRequested{decidable: false}` BEFORE the elicitation goes out; never enters `/v1/gates`;
     `POST …/decision` on it is `404 unknown_gate` (all per operator.md §announce).
  3. `peer.create_elicitation(..)` with `wasl_gate::elicitation_for(&req).message` and the empty
     schema, exactly as today. Map the result via §5 (the deduplicated path). A transport `Err` stays
     `Denied { reason: format!("elicitation transport error: {err}") }`.
  4. If mirrored: `hub.resolve_announced(&gate_id, &outcome, DecidedBy::Elicitation);` — for EVERY
     outcome, including the transport-error deny.
  5. `resolve_and_dispatch(..)` as today.

- **`Some(External)` — Tier-3 via `OperatorSurfacer`:** the surfacer blocks its thread for up to
  `decision_timeout` (operator.md's normative embedder contract), so it must run on the blocking
  pool, never a plain async worker. `OperatorHub` is `Clone` (Arc-cheap), so a fresh surfacer is built
  per gate inside `spawn_blocking` (which is legal on ANY tokio runtime flavor, unlike
  `block_in_place`):

  ```rust
  let hub = hub.clone();
  let outcome = tokio::task::spawn_blocking(move || {
      let mut s = OperatorSurfacer::new(hub);
      wasl_server::Surfacer::surface(&mut s, GateMode::External, &req)
  }).await.unwrap_or(GateOutcome::Dismissed);   // a panicked/cancelled waiter never approves
  ```

  `open_gate`/`wait`/`decide`/timeout semantics and all `GateRequested{decidable:
  true}`/`GateDecided{operator|timeout}` frames come from the hub (operator.md) — this crate adds
  nothing. Then `resolve_and_dispatch(..)`. Note `External` with `hub == None` is unreachable by
  construction (`caps.external = hub.is_some()` is the only source of `external: true`).

- **`Some(PrepareCommit)` — Tier-2:** see §5. Returns a `Pending` reply WITHOUT calling
  `resolve_and_dispatch`; nothing is audited at prepare (mirrors `wasl-server`'s Gatekeeper: "nothing
  is recorded until `commit`").

## 5. Tier-2 prepare→commit through the MCP facade

The deferral state lives in the already-tested `wasl_gate::GateLedger` (quoted, gate.md §4):

```rust
pub struct GateLedger { /* private */ }
impl GateLedger {
    pub fn new() -> Self;                                              // also Default
    pub fn prepare(&mut self, token: ApprovalToken, req: GateRequest); // duplicate token overwrites
    pub fn is_pending(&self, token: &ApprovalToken) -> bool;
    pub fn pending_count(&self) -> usize;
    pub fn peek(&self, token: &ApprovalToken) -> Option<&GateRequest>;
    pub fn commit(&mut self, token: &ApprovalToken, decision: GateDecision)
        -> Result<(GateRequest, GateDecision), GateError>;
        // unknown → Err(UnknownToken); already committed → Err(AlreadyResolved)
}
pub enum GateError { UnknownToken, AlreadyResolved }
```

### Prepare (inside the four tools, when mode = PrepareCommit)

1. `seq = state.next_seq()`; `token = ApprovalToken(format!("tok-mcp-{seq}"))` (deterministic, the
   `base_id` idiom); `req = build_request(token.0.clone(), kind, from, targets, preview)` — Tier-2
   requests carry the token as their id so the operator sees which token to tell the agent about.
2. `state.ledger.prepare(token.clone(), req.clone());`
   `state.pending_calls.insert(token.clone(), call);` — ledger and `pending_calls` are inserted (and
   later removed) together: LOCKSTEP is the invariant §Commit relies on.
3. **If the operator is bound**, the gate is DECIDABLE on the operator surface:
   `let w = hub.open_gate(GateMode::PrepareCommit, &req);` then spawn the decision waiter:

   ```rust
   let (hub2, state2, tok2) = (hub.clone(), self.state.clone(), token.clone());
   tokio::spawn(async move {
       let outcome = tokio::task::spawn_blocking(move || hub2.wait(w))
           .await.unwrap_or(GateOutcome::Dismissed);
       state2.lock().await.decisions.entry(tok2).or_insert(outcome);  // first decision wins
   });
   ```

   The human approval lands via `POST /v1/gates/{gate_id}/decision` (operator-api.md); the hub
   broadcasts `GateDecided{decided_by: operator}` (or `{dismissed, timeout}` after
   `decision_timeout`, which also bounds Tier-2 waits whenever the operator is on).
4. **If the operator is not bound**, approval lands via the embedder API (below); Tier-2 waits are
   then unbounded (a prepared token simply awaits its commit).
5. Reply: `ToolReply::Pending { token }`, rendered as a SUCCESS content block (it is not an error —
   the agent's next move is defined). Pinned JSON (serde struct, this key order):

   ```json
   {"status":"pending","token":"tok-mcp-0","hint":"awaiting human approval; call the commit tool with this token"}
   ```

   `ToolReply` gains the `Pending { token: ApprovalToken }` variant; `is_blocked()` stays
   `Blocked`-only.

### The embedder API (operator not bound)

```rust
impl<K, R> WaslMcp<K, R> {
    /// Deliver a human decision for a prepared token (the embedder's decision
    /// inlet when no operator surface is bound). First decision wins.
    pub async fn resolve_pending(&self, token: &ApprovalToken, outcome: GateOutcome)
        -> Result<(), GateError>;
    // pending in ledger + no decision yet → insert into decisions, Ok(())
    // pending in ledger + decision already present → Err(AlreadyResolved)
    // not pending → classify_not_pending (UnknownToken | AlreadyResolved)
}
```

### The fifth tool: `commit`

```rust
/// `commit` tool arguments.
pub struct CommitArgs { /// The approval token returned by a pending tool call.
                        pub token: String }
#[tool(description = "Redeem a prepare token: report the human decision for a pending \
                      gated call, dispatching it if approved")]
async fn commit(&self, Parameters(args): Parameters<CommitArgs>, peer: Peer<RoleServer>) -> …;
```

Behavior (under the state lock; `token = ApprovalToken(args.token)`):

1. `decisions.remove(&token)` is `Some(outcome)` → resolve:
   - `ledger.commit(&token, ledger_decision_of(&outcome)).expect("ledger in lockstep with decisions")`
     — infallible here by the lockstep invariant; `ledger_decision_of` (pure, in `gating.rs`):
     `Approved → Approve`, `Denied{reason} → Deny{reason}`, `Dismissed → Deny{reason: "dismissed"}`
     (the ledger needs SOME decision to retire the token; the AUDIT decision is `outcome.to_decision()`
     inside `resolve_and_dispatch`, which correctly records `None` for a dismissal).
   - `let call = pending_calls.remove(&token).expect(..)` (lockstep), then
     `resolve_and_dispatch(&ctx, &outcome, call, &base_id, seq, ..)` with a fresh `seq`/`base_id` —
     the ONE audit record for this gate is written here, then the approved call dispatches / the
     denial blocks, exactly like a Tier-1 outcome. No `resolve_announced`: when the operator is on the
     hub already broadcast `GateDecided` at POST/timeout time.
2. `None` and `ledger.is_pending(&token)` → the human has not decided: return the pinned `pending`
   JSON again (idempotent poll; the agent retries later).
3. Neither → `classify_not_pending(&mut ledger, &token) -> GateError` and reply
   `ToolReply::Blocked { reason: err.to_string() }` → `"blocked: unknown approval token"` /
   `"blocked: approval token already resolved"` (GateError's pinned Displays). `classify_not_pending`
   probes `ledger.commit(&token, GateDecision::Approve)` — side-effect-free on a non-pending token —
   and returns its error; the `Ok` arm is unreachable there (guarded by `is_pending == false`) and
   carries the verified-unreachable comment, house style.

The `commit` tool is always advertised (rmcp's `#[tool_router]` is static); with nothing prepared it
is a self-describing no-op (`"blocked: unknown approval token"`).

## 6. Dedup — `wasl_gate::outcome_of` is the only action→outcome mapping

rmcp's `ElicitationAction` is a different type from `wasl_proto::ElicitationAction`; the transport
edge converts the TYPE, the tested core owns the SEMANTICS:

```rust
// gating.rs — type conversion only, no decision logic
pub fn proto_action(a: rmcp::model::ElicitationAction) -> wasl_proto::ElicitationAction {
    match a { Accept => Accept, Decline => Decline, Cancel => Cancel }
}
// server.rs elicitation arm:
Ok(result) => wasl_gate::outcome_of(&ElicitationResponse { action: proto_action(result.action) }),
```

The hand-rolled `match result.action { Accept => GateOutcome::Approved, .. }` block quoted in §Why is
DELETED.

## 7. Fix — `NoDestinations` is a blocked reply

`wasl_transfer::execute`'s only `Err` is `NoDestinations` (`Failed` goes into the partial-failure
vector, never the outer `Err`), so the two `Err` arms in `dispatch_approved` collapse into one:

```rust
// handle.rs — replaces the empty-success arm quoted in §Why
ToolCall::Transfer { tool, params } => match wasl_transfer::execute(tool, &params, runner) {
    Ok((result, _failures)) => ToolReply::Transfer(result),
    // A transfer that cannot address anyone is a caller error, surfaced like
    // every other transfer failure — never an empty success.
    Err(err) => ToolReply::Blocked { reason: err.to_string() },
},
```

Pinned MCP-level reply (via the unchanged `reply_to_result`): an error-content `CallToolResult` with
text exactly `"blocked: transfer has no destination hosts"` (`TransferError::NoDestinations`'s
Display). The gate decision (Approve) is still audited — the failure is post-gate.

## 8. `run()` wiring — operator surface + `TeeSink`

New sink wrapper in lib.rs so `run()` keeps one handler type:

```rust
pub enum RunSink { Plain(DefaultSink), Teed(TeeSink<DefaultSink>) }
impl Sink for RunSink { /* delegate append */ }
```

`run()`, after `Config::load()` and the (unchanged) A2A receive shell — IO shell, UNTESTED HERE like
the A2A block, same labeling:

1. If `cfg.operator.bind` is `Some(bind)`: parse as `SocketAddr` with the same lenient warn-and-skip
   as `a2a.bind` (`"wasl-mcpd: WARNING: operator.bind={bind:?} is not a valid socket address ({err});
   operator surface not started."` — defensive only: `operator.bind` has no env override, so the
   effective string was already validated at load). On success: build
   `NodeInfo { agent_id, host_id, version: env!("CARGO_PKG_VERSION").to_string() }`, `hub =
   OperatorHub::new(info, HubConfig::default())`, and `tokio::spawn(wasl_operator::serve(hub.clone(),
   addr))` (stderr line on termination, the A2A idiom).
2. `build_handler(cfg: &Config, hub: Option<OperatorHub>) -> WaslMcp<RunSink, ProcessRunner>`: open
   the audit sink exactly as today (`open_audit`, including the loud MemSink fallback), then wrap:
   `hub` present → `RunSink::Teed(TeeSink::new(sink, hub.clone()))` (durable append first, then the
   `Audit` frame fan-out — every gated call's decision entry now reaches `/v1/events`), else
   `RunSink::Plain(sink)`. Construct via `WaslMcp::with_gating(.., cfg.gate.authority, hub)`.
3. Everything else in `run()` is byte-for-byte today's flow.

## 9. Backward compatibility

With **no config file and no new keys**: `gate.authority = Elicitation`, `operator.bind = None` →
no hub, `RunSink::Plain`, `caps.external = false`, and `select_mode` reproduces today's two behaviors
exactly — elicitation-capable client → Tier-1 elicitation with the identical message and outcome
mapping; non-capable client → deny with the identical pinned reason string. Audit entries, reply
rendering, config errors, env-var semantics, A2A wiring: unchanged. The ONLY externally visible
deviations without new config, both deliberate and pinned:

- (a) `NoDestinations` now yields the blocked reply of §7 instead of `{"delivered":[]}` (bug fix;
  supersedes mcp-a2a.md's pinned arm).
- (b) The tool list gains `commit` (five tools); calling it with any token while nothing is prepared
  is `"blocked: unknown approval token"`. `tests/smoke.rs`'s advertised-tools assertion is updated to
  the five names — the ONE permitted edit to the existing suites; `tests/handle.rs` and the config
  suites stay green unmodified except handle.rs's `NoDestinations` expectation, which flips to the
  §7 reply.

## 10. Test plan

### `tests/gating_unit.rs` — the pure seams (no transport, no tokio)

- U1 `detect_caps`: `None` client info → `elicitation: false`; a real
  `InitializeRequestParam` with `ClientCapabilities::builder().enable_elicitation().build()` → true;
  default (no elicitation) capabilities → false; `operator_attached` passes through to `external`
  in all four combinations. Assert equivalence with rmcp's rule (capabilities.elicitation.is_some()).
- U2 `select_mode`: the FULL truth table of §3 (4 authorities × the caps quadrant), including
  `ElicitationFirst ≡ wasl_gate::choose_mode` asserted for all four caps values.
- U3 `proto_action` (all three variants) and the composition
  `outcome_of(&ElicitationResponse{action: proto_action(a)})`: Accept→Approved,
  Decline→Denied{"declined"}, Cancel→Dismissed.
- U4 `ledger_decision_of`: Approved→Approve, Denied{r}→Deny{r}, Dismissed→Deny{"dismissed"}.
- U5 config: `[gate] authority = "…"` parses each of the four values; absent section/key →
  `GateAuthority::Elicitation`; `"Elicitation"`/`"bogus"` → `Err(UnknownGateAuthority(v))` with `v`
  preserved; Display contains `"unknown gate authority: bogus"`; unknown key under `[gate]` →
  `Err(Toml)`; full-example round-trip with the new section; `Config::default().gate` asserted.
- U6 `RunSink`: `Plain` appends to the inner sink; `Teed` appends AND broadcasts the equal `Audit`
  frame (hub subscriber `try_recv`), inner-sink `Err` propagates with no frame (the TeeSink contract,
  re-asserted through the wrapper).

### `tests/gating_loopback.rs` — end-to-end over in-memory rmcp + loopback HTTP

Harness: the smoke.rs duplex idiom for MCP (a scriptable `ClientHandler` whose `get_info()` declares
elicitation capability when the case needs it and whose `create_elicitation` returns a scripted
`ElicitationAction`; the bare `()` client is the no-elicitation client), the handle.rs loopback A2A
peer (real `A2aServer` + recording fake adapter on `127.0.0.1:0`) for dispatch proof, and — when the
operator is in play — a hub with test seams (`with_seams`, small `decision_timeout` where the case
needs it) served via `wasl_operator::router` on `127.0.0.1:0` with a real `reqwest` client for
POST/SSE. Bind-before-spawn, short timeouts, abort/join tasks.

- L1 **operator-decided send (approve):** `()` client, `with_gating(.., ExternalFirst, Some(hub))`;
  call `consult` → SSE/subscriber sees `GateRequested{decidable: true, mode: external}`; `GET
  /v1/gates` shows it; `POST …/decision {"outcome":"approved"}` → 204 → the tool returns the peer's
  real reply; the fake adapter SAW the delivery; audit holds one `Approve`; `GateDecided{approved,
  operator}` observed.
- L2 **operator deny with reason** → error-content `"blocked: {reason}"`; adapter recorded NOTHING;
  audit `Deny{reason}`.
- L3 **operator timeout:** tiny `decision_timeout`, no POST → `"blocked: dismissed"`; audit decision
  `None`; `GateDecided{dismissed, timeout}`; a late POST → 409 `already_decided`.
- L4 **elicitation mirrored observe-only:** elicitation-capable client scripted Accept,
  `ElicitationFirst` + hub; `consult` → frames `GateRequested{decidable: false, mode: elicitation}`
  then `GateDecided{approved, decided_by: elicitation}`; `/v1/gates` NEVER contains it; `POST` on its
  gate_id → 404 `unknown_gate`; the call dispatched normally. A scripted Decline variant asserts the
  mirrored `GateDecided{denied, elicitation, reason: "declined"}` and no dispatch.
- L5 **Tier-2 roundtrip via operator POST:** `PrepareCommit` authority + hub; `consult` → success
  content equal to the pinned pending JSON (token `tok-mcp-0`); frame `GateRequested{decidable: true,
  mode: prepare_commit}`; `commit(token)` BEFORE any decision → the pending JSON again; `POST
  approved` → `commit(token)` returns the peer's real consult reply; audit has exactly ONE entry for
  the gate (`Approve`, recorded at commit); re-`commit` → `"blocked: approval token already
  resolved"`.
- L6 **Tier-2 embedder path (no hub):** `PrepareCommit`, `hub: None`; pending token;
  `resolve_pending(&token, Approved)` → `Ok`; second `resolve_pending` → `Err(AlreadyResolved)`;
  `commit` → dispatched reply; `resolve_pending` on a made-up token → `Err(UnknownToken)`. A
  `Denied{reason}` variant yields `"blocked: {reason}"` and audit `Deny`; a `Dismissed` variant yields
  `"blocked: dismissed"` and audit decision `None`.
- L7 **commit on an unknown token** (nothing ever prepared) → `"blocked: unknown approval token"`.
- L8 **default authority = today:** `new(..)` (no hub), `()` client → every tool blocks with exactly
  `"blocked: client cannot be prompted (no elicitation capability)"`, audit `Deny`, adapter untouched;
  the elicitation-capable Accept path still dispatches (the v1 behaviors, now proven at tool level
  with REAL caps detection).
- L9 **NoDestinations pinned reply:** approved `transfer` with empty `dst_hosts` → error-content
  `"blocked: transfer has no destination hosts"`; audit still holds the `Approve`; runner untouched.
- Smoke: `tests/smoke.rs` asserts the FIVE advertised tools (`command`, `commit`, `consult`,
  `message`, `transfer`).

### Coverage bar

`gating.rs` (detect_caps / select_mode / proto_action / ledger_decision_of / GateAuthority parsing)
and the config additions: **100% line+region** (`cargo llvm-cov`) — pure code, no excuses, with ONE
permitted verified-unreachable region: `classify_not_pending`'s `Ok` arm (guarded by
`is_pending == false`; documented in-code, filesink.md style). The handle/server tier logic
(prepare/commit tables, mirror hooks, deny path, §7 arm) is covered by the loopback suites. The
`run()` operator wiring (bind parse, hub construction, `serve` spawn, `RunSink` selection in
`build_handler`) is IO shell: `build_handler`'s sink/authority selection is covered via `RunSink`/U6
and constructor tests; the live serve loops stay UNTESTED HERE, labeled in code + AGENTS.md exactly
like the A2A receive shell. No `#[ignore]`, no `todo!`, no tautologies; `cargo test -p wasl-mcp`
green; `clippy --all-targets -- -D warnings` and `fmt` clean; overall `wasl-mcp` coverage must not
regress.

## Honesty

- ✅ verified here: real caps detection against rmcp's declared client capabilities (unit + real
  handshake over duplex), the full authority×caps mode table, operator-decided sends
  (approve/deny/timeout) over real loopback HTTP+SSE, Tier-1 mirroring (observe-only, non-decidable),
  Tier-2 prepare→commit through the tools via both the operator POST and the embedder API, the
  deduplicated outcome mapping, the `NoDestinations` blocked reply, byte-for-byte default behavior.
- ❌ UNTESTED HERE (unchanged stance): the rmcp stdio serve loop, the live A2A receive loop, the live
  operator `serve()` bind in `run()` (its router IS loopback-tested), driving a real `claude` CLI.
