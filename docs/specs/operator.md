# W3 — `wasl-operator` — spec (authoritative)

**Tier:** L2 (HTTP IO; loopback-testable). **Deps:** `wasl-proto`, `wasl-gate`, `wasl-audit`,
`wasl-server` (the `Surfacer` trait), `axum` 0.7, `tokio` (sync/time/net/macros), `tokio-stream`
(sync feature, for `BroadcastStream`), `serde`, `serde_json`, `futures`. Dev: `reqwest` 0.12
(json+stream), `tokio` (rt-multi-thread). No `unsafe`. New workspace member + workspace-dep entry.

This is the node's **operator surface**: the serving side of the versioned wire contract in
`docs/specs/operator-api.md` (read that first — it is the consumer-facing half and pins every JSON
byte via `fixtures/operator-v1/`). Three pieces: an **`OperatorHub`** broker (pending decidable-gate
table + broadcast fan-out + a blocking decision rendezvous), an **`OperatorSurfacer`** implementing
`wasl_server::Surfacer` (Tier-3: publish the gate, block until an authority POSTs a decision or the
timeout dismisses it), and a **`TeeSink`** wrapping any inner `wasl_audit::Sink` (durable append
first, then broadcast — the observation feed). Consumer-agnostic by construction: nothing in this
crate names, assumes, or links any consumer.

**Testability (honesty):** the hub core (register/decide/timeout/announce/snapshot/tee + frame
serde + error mapping) is unit-tested to **100% line+region** with a fake clock, counter gate ids,
and a scriptable inner sink — with ONE documented exception, the post-timeout race arm inside
`wait` (§Surfacer), which cannot be reached deterministically; its constituent operations are each
covered via the public two-phase API and the arm carries a filesink-style verified-unreachable
note. The axum router/handlers/SSE writer are the IO shell, verified by loopback integration tests
(127.0.0.1, real `reqwest`), NOT held to 100%; only the `serve()` bind loop may be otherwise
uncovered. Wiring into `wasl-mcpd` (config `operator.bind`, W2/W4) is OUT of scope here.

## Types

```rust
/// The node identity served at /v1/info and inside `hello` frames.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct NodeInfo { pub agent_id: AgentId, pub host_id: HostId, pub version: String }

/// Hub tuning. Defaults: decision_timeout = 120 s (a human answers in minutes, not ms; W2 config
/// overrides), channel_capacity = 256 broadcast frames (see the lag policy).
#[derive(Debug, Clone, Copy)]
pub struct HubConfig { pub decision_timeout: Duration, pub channel_capacity: usize } // impl Default

/// Injectable wall clock (deterministic tests). Default `SystemClock` = `SystemTime::now()`.
pub trait Clock: Send + Sync { fn now(&self) -> SystemTime; }
pub struct SystemClock;

/// Injectable gate-id source (deterministic tests). Default `CounterGateIds` → "gate-1", "gate-2", …
/// (the `CounterIds` idiom from `wasl-a2a-server`, single prefix).
pub trait GateIds: Send + Sync { fn next(&self) -> String; }
pub struct CounterGateIds; // AtomicU64, impl Default

/// Who/how a gate resolved (wire: "operator" | "elicitation" | "timeout").
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "lowercase")]
pub enum DecidedBy { Operator, Elicitation, Timeout }

/// POST /v1/gates/{id}/decision body. `reason` only meaningful with Denied
/// (a Denied with no reason records "denied by operator").
#[derive(Debug, Clone, Deserialize)]
pub struct DecisionBody { pub outcome: DecisionOutcome, #[serde(default)] pub reason: Option<String> }
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "lowercase")]
pub enum DecisionOutcome { Approved, Denied, Dismissed }
// DecisionBody → GateOutcome: Approved→Approved; Denied→Denied{reason.unwrap_or("denied by operator")};
// Dismissed→Dismissed.

#[derive(Debug, Clone, PartialEq, Eq)]
pub enum DecideError { UnknownGate, AlreadyDecided } // impl Display + std::error::Error
// HTTP mapping: UnknownGate→404 {"error":"unknown_gate"}, AlreadyDecided→409 {"error":"already_decided"}.

/// One broadcastable frame, paired with its node-global sequence. `Frame` variants serialize to
/// EXACTLY the `data:` payloads in operator-api.md (the SSE `event:` name comes from
/// `Frame::event_name()` — "gate_requested" | "gate_decided" | "audit"; the payload does not
/// repeat it). `hello` is connection-local, built per subscription, NOT broadcast.
#[derive(Debug, Clone, PartialEq, Serialize)]
pub enum Frame {
    GateRequested { gate_id: String, mode: GateMode, decidable: bool, request: GateRequest,
                    requested_at: String, timeout_ms: u64, deadline: String },
    GateDecided   { gate_id: String, outcome: DecisionOutcome, decided_by: DecidedBy,
                    reason: Option<String>, decided_at: String },
    Audit         { entry: AuditEntry },
}
#[derive(Debug, Clone, PartialEq, Serialize)]
pub struct SeqFrame { pub seq: u64, pub frame: Frame }
```

Timestamps are RFC 3339 UTC millis rendered from `Clock::now()`; the deadline used for the ACTUAL
wait is `mpsc::recv_timeout` (monotonic) — the wire timestamps are informational.

## `OperatorHub`

```rust
#[derive(Clone)] // cheap: Arc<Inner>
pub struct OperatorHub { /* Arc<Inner{ info, cfg, clock, ids,
    seq: AtomicU64,                                            // frame sequence, starts at 1
    tx: tokio::sync::broadcast::Sender<SeqFrame>,              // capacity = cfg.channel_capacity
    pending: Mutex<HashMap<String, PendingGate>>,              // decidable gates only
    resolved: Mutex<HashSet<String>> }> */ }
// PendingGate { request: GateRequest, requested_at: SystemTime, deadline: SystemTime,
//               waiter: std::sync::mpsc::SyncSender<GateOutcome> }
```

- `new(info: NodeInfo, cfg: HubConfig) -> Self` (SystemClock + CounterGateIds);
  `with_seams(info, cfg, clock: Arc<dyn Clock>, ids: Arc<dyn GateIds>) -> Self`.
- `info(&self) -> NodeInfo`.
- `subscribe(&self) -> (u64, broadcast::Receiver<SeqFrame>)` — `(last_seq, rx)`; last_seq feeds the
  `hello` frame.
- **`open_gate(&self, mode: GateMode, req: &GateRequest) -> PendingWait`** — mint a gate id, create
  a `std::sync::mpsc::sync_channel::<GateOutcome>(1)`, insert the `PendingGate` (deadline =
  `clock.now() + cfg.decision_timeout`), broadcast `GateRequested{decidable: true, ..}`. Returns
  `PendingWait { gate_id: String, /* private */ rx }`.
- **`wait(&self, w: PendingWait) -> GateOutcome`** — block on `rx.recv_timeout(decision_timeout)`:
  - `Ok(outcome)` → return it (a `decide` already removed the gate, marked it resolved, and
    broadcast `GateDecided{decided_by: Operator}`).
  - `Err(Timeout)` → `self.timeout_gate(&w.gate_id)`:
    - `Ok(())` (it was still pending: removed, marked resolved, `GateDecided{outcome: Dismissed,
      decided_by: Timeout}` broadcast) → return `GateOutcome::Dismissed`.
    - `Err(AlreadyDecided)` — the race arm: a decision landed between the timeout firing and the
      table lock. **Invariant that makes this safe:** `decide` sends on the waiter channel and
      broadcasts *while holding the `pending` lock*, so "gate absent" ⇒ the outcome is already
      buffered ⇒ `rx.recv()` returns immediately; return that outcome. (This arm is the one
      permitted-uncovered core region — not deterministically reachable; `timeout_gate`'s
      AlreadyDecided arm itself IS covered directly, see H8.)
- **`decide(&self, gate_id: &str, body: DecisionBody) -> Result<(), DecideError>`** — under the
  `pending` lock: remove the gate (absent → `AlreadyDecided` if in `resolved`, else `UnknownGate`);
  insert into `resolved`; `waiter.try_send(outcome)` (capacity-1 buffer + single-decider-by-removal
  ⇒ never full; a `Disconnected` error is ignored — the waiter died, the frame still tells
  observers); broadcast `GateDecided{decided_by: Operator, reason: deny-reason or None}`. Exactly
  one concurrent caller wins.
- **`timeout_gate(&self, gate_id: &str) -> Result<(), DecideError>`** — `decide`'s twin for the
  timeout path (same lock discipline; no waiter send — the waiter IS the caller); broadcasts
  `GateDecided{outcome: Dismissed, decided_by: Timeout, reason: None}`.
- **`announce(&self, mode: GateMode, req: &GateRequest) -> String`** — observe-only mirror hook
  (W4 uses it for Tier-1 elicitation gates): mint an id, broadcast `GateRequested{decidable:
  false}`, do NOT touch `pending` (so it never appears in `/v1/gates` and `decide` on it →
  `UnknownGate`/404). Returns the gate id.
- **`resolve_announced(&self, gate_id: &str, outcome: &GateOutcome, decided_by: DecidedBy)`** —
  broadcast the matching `GateDecided` (per `wasl_gate::outcome_of` mapping: Approved/
  Denied{reason}/Dismissed); infallible, no table.
- **`snapshot(&self) -> GatesSnapshot`** — the `/v1/gates` body: pending decidable gates with
  `age_ms = now − requested_at` (saturating), ascending `requested_at` then `gate_id`; plus
  `last_seq`.
- **`publish_audit(&self, entry: &AuditEntry)`** — broadcast `Frame::Audit`; infallible
  (`broadcast::Sender::send`'s no-subscriber error is ignored).

Every broadcast assigns `seq = seq.fetch_add(1)+1` and sends `SeqFrame` — pure ring-buffer write,
sync and non-blocking, safe from any thread. `resolved` grows with gate count for the process
lifetime (same accepted tradeoff as `GateLedger`'s resolved set) — that memory buys the honest
404-vs-409 distinction.

## `OperatorSurfacer` — the sync↔async bridge, explicitly

```rust
pub struct OperatorSurfacer { hub: OperatorHub }
impl wasl_server::Surfacer for OperatorSurfacer {
    fn surface(&mut self, mode: GateMode, req: &GateRequest) -> GateOutcome {
        let w = self.hub.open_gate(mode, req);
        self.hub.wait(w)
    }
}
```

`Surfacer::surface` is **sync `&mut`** and this crate mounts inside `wasl-mcp`'s multi-thread
tokio runtime — the bridge is therefore designed to need NO runtime handle at all:

- The **wait path uses only `std::sync::mpsc`** (`recv_timeout`), never a tokio primitive — so
  `surface()` is runtime-agnostic and cannot hit `blocking_recv`-in-async panics.
- The **async→sync handoff** (POST handler → blocked surfacer thread) is
  `SyncSender::try_send` on the capacity-1 channel: never blocks the axum task.
- The **sync→async handoff** (surfacer thread → SSE subscribers) is
  `tokio::sync::broadcast::Sender::send`: a sync, non-blocking call.
- **Embedder contract (normative, document loudly on the impl):** `surface()` blocks its thread
  for up to `decision_timeout`. Callers on the tokio runtime MUST invoke the gatekeeper's
  authorize path inside `tokio::task::block_in_place` (fine on `wasl-mcp`'s multi-thread runtime)
  or from `spawn_blocking`; never on a plain async worker. This crate cannot detect the violation
  (block_in_place is invisible) — the contract lives here and in W4's wiring spec.

## `TeeSink`

```rust
pub struct TeeSink<K: Sink> { inner: K, hub: OperatorHub }
impl<K: Sink> TeeSink<K> { pub fn new(inner: K, hub: OperatorHub) -> Self; pub fn into_inner(self) -> K; }
impl<K: Sink> Sink for TeeSink<K> {
    fn append(&mut self, entry: &AuditEntry) -> Result<(), AuditError> {
        self.inner.append(entry)?;        // durable FIRST; on Err nothing is broadcast, error propagates
        self.hub.publish_audit(entry);    // infallible best-effort fan-out
        Ok(())
    }
}
```

**Never lose audit on slow subscribers:** durability is the inner sink's alone; the broadcast is a
lossy-by-design observation feed. **Lag/drop policy (normative):** the broadcast channel holds
`channel_capacity` frames; when a subscriber falls behind, tokio's broadcast drops its OLDEST
unseen frames (`RecvError::Lagged(n)`). The SSE writer **skips and continues** (no stream close,
no error frame): the dropped frames simply create a gap in the `id:` sequence, which is the
consumer's documented signal to re-snapshot `/v1/gates` (operator-api.md §Reconnect). Closing on
lag was rejected — it turns one slow reader into a reconnect storm, and reconnecting recovers no
more information than the gap already conveys (replay does not exist; the durable trail is the
file). A `Closed` recv error (hub dropped) ends the stream.

## HTTP shell (axum, the `wasl-a2a-server` idioms)

```rust
pub fn router(hub: OperatorHub) -> axum::Router;                       // hub is the State (Clone)
pub async fn serve(hub: OperatorHub, addr: SocketAddr) -> std::io::Result<()>; // bind + axum::serve
```

- `GET /v1/info` → `200 Json(hub.info())`.
- `GET /v1/gates` → `200 Json(hub.snapshot())`.
- `GET /v1/events` → `Sse` over: one synthesized `hello` `Event` (`event("hello")`, NO `.id()`,
  data = `{api_version:1, agent_id, host_id, version, last_seq, resume:"snapshot"}`), then
  `BroadcastStream(rx)` mapped `SeqFrame → Event::default().event(frame.event_name())
  .id(seq.to_string()).data(compact_json)`; `Err(Lagged(_))` items are filtered out (skip-and-
  continue per the policy above); keep-alive via `Sse::keep_alive` (~15 s, `: ka`). The
  `Last-Event-ID` header is not read.
- `POST /v1/gates/{gate_id}/decision` → body `Json<DecisionBody>` (axum rejection → `400
  {"error":"malformed", "detail": ...}` via a custom extractor-rejection mapping so the body shape
  is OURS, matching `error_malformed.json`); then `hub.decide()` → `204` empty, or the
  `DecideError` mapping (§Types). Unknown-outcome strings and missing `outcome` are serde
  rejections → 400.

## Test plan

**`tests/hub.rs` (unit — no HTTP — 100% line+region on the core, minus the ONE noted race arm).**
Fake `Clock` (settable), `CounterGateIds` via `with_seams`, a scriptable fake inner `Sink`
(records / fails on demand), and `hub.subscribe()` receivers drained with `try_recv`.
- H1 open→decide approved: thread runs `open_gate`+`wait`; main thread sees `GateRequested`
  (seq 1, gate-1, decidable, mode, request echoed, requested_at/deadline from the fake clock,
  timeout_ms = cfg) then `decide("gate-1", approved)` → Ok; `wait` returns `Approved`;
  `GateDecided{approved, operator, reason: None}` at seq 2; snapshot now empty.
- H2 deny with reason → `Denied{reason}` + frame reason `Some`; H2b deny WITHOUT reason →
  `Denied{"denied by operator"}`.
- H3 POST-dismiss (`outcome: dismissed`) → `wait` returns `Dismissed`, frame decided_by
  `operator`.
- H4 timeout: `decision_timeout = 0` (or a few ms), no decide → `wait` returns `Dismissed`;
  `GateDecided{dismissed, timeout}`; gate resolved (in the resolved set).
- H5 decide after timeout → `Err(AlreadyDecided)`; H6 decide unknown id → `Err(UnknownGate)`;
  H7 double decide → second `Err(AlreadyDecided)`.
- H8 the race constituents, deterministically via the two-phase API: `open_gate` → `decide` →
  `timeout_gate(id)` → `Err(AlreadyDecided)` AND the buffered outcome is recv-able — i.e. every
  operation the `wait` race arm composes, each covered; the arm itself carries the documented
  verified-unreachable note (mirrors `filesink.md`).
- H9 announce/resolve_announced: `decidable: false` frame; not in `snapshot()`; `decide` on it →
  `UnknownGate`; `resolve_announced(.., Approved, Elicitation)` → `GateDecided{approved,
  elicitation}`; same for Denied{reason}/Dismissed mapping.
- H10 snapshot: two pending gates, fake clock advanced → correct `age_ms`, ascending order,
  `last_seq` equals frames published.
- H11 TeeSink: inner Ok → entry in inner AND an `Audit` frame with the equal entry, in that order
  (script the inner sink to assert broadcast hadn't happened yet at append time — e.g. the fake
  sink holds a subscriber and asserts `try_recv` is Empty inside `append`); inner Err → error
  propagates and NO frame; zero subscribers → still Ok; `into_inner` returns the sink.
- H12 serde/wire: each `Frame` variant + `SeqFrame` + `DecisionBody` (both directions where
  deserialized) + `DecidedBy`/`DecisionOutcome` casing; `DecideError` Display non-empty +
  distinct; `HubConfig::default()` values.
- H13 multi-subscriber at hub level: two receivers get identical `SeqFrame`s (same seq).

**`tests/loopback.rs` (integration — real HTTP+SSE on `127.0.0.1:0` with `reqwest`, the
`a2a-server` loopback idioms: bind the listener synchronously, spawn `axum::serve`, short client
timeouts).**
- L1 `GET /v1/info` → 200, exactly the `NodeInfo` JSON.
- L2 subscribe `/v1/events` → first frame is `event: hello` with no `id:` and the schema fields
  (`api_version: 1`, identity, `last_seq`, `resume: "snapshot"`).
- L3 the full rendezvous: subscribe → `spawn_blocking(|| surfacer.surface(External, &req))` → SSE
  `gate_requested` (`id: 1`, gate-1) → `GET /v1/gates` shows it (age/deadline present) → POST
  approved → 204 empty body → SSE `gate_decided{approved, operator}` → the spawned `surface`
  returns `Approved` → `/v1/gates` empty.
- L4 timeout over HTTP: tiny `decision_timeout`, no POST → `surface` returns `Dismissed`; SSE
  `gate_decided{dismissed, timeout}`; a late POST → 409 `already_decided`.
- L5 multi-subscriber: two SSE clients, one gate flow → both see identical frames with equal
  `id:`s.
- L6 slow subscriber: `channel_capacity: 2`; open a reader, don't poll it while publishing many
  audit entries via `TeeSink`, then read → the stream is still open and the received `id:`s show
  a gap (skip-and-continue), while a concurrently-draining fast reader saw every frame; the inner
  sink has ALL entries.
- L7 errors: POST unknown id → 404 `{"error":"unknown_gate",...}`; malformed body (bad JSON,
  missing outcome, `"outcome":"maybe"`) → 400 `{"error":"malformed",...}`; decided id → 409.
- L8 audit tee over HTTP: `TeeSink::append` → SSE `audit` frame whose `entry` equals what the
  inner sink recorded (including a `decision: null` send entry — the auto-approved case).

**`tests/fixtures.rs` (conformance — the producer half of the golden-fixture policy).** For every
file in `fixtures/operator-v1/` (the table in operator-api.md, and the test FAILS if a directory
entry is unlisted or vice versa): build the corresponding live value (frames via a
fake-clock/counter-id hub, bodies via the handlers where reachable, else the serde types) and
assert `serde_json::Value` equality with the file; for request-shaped fixtures
(`decision_request_*.json`) additionally assert they DESERIALIZE into the expected `DecisionBody`.
Fixtures are committed with this crate (append-only; see operator-api.md §Fixture policy).

**Coverage bar:** hub core + surfacer + tee + frames/serde/errors at **100% line+region**
(`cargo llvm-cov`), with exactly two permitted exceptions, each carrying a verified justification
comment: (1) the `wait` post-timeout race arm (H8 note), (2) the `serve()` bind loop (IO shell,
exercised by every loopback test via `router()` except the bind itself). No
`#[ignore]`/`todo!`/tautologies; loopback tests hermetic (ephemeral ports, spawned servers, short
timeouts). Delete unreachable branches rather than leaving them uncovered.
