//! `tests/gating_loopback.rs` — END-TO-END tests for W4 gating over in-memory
//! rmcp (a scriptable `ClientHandler`, the `smoke.rs` duplex idiom) plus real
//! HTTP+SSE for the operator surface (`wasl_operator::router`) and a real
//! loopback A2A peer for dispatch proof (the `tests/handle.rs` idiom,
//! duplicated here since every `tests/*.rs` file is its own separate crate).
//! Mirrors the spec's §Test plan L1–L9 (`docs/specs/mcp-gating.md`), plus a
//! NEW smoke test asserting the five advertised tools — the ONE place this
//! block's fifth `commit` tool is smoke-tested; `tests/smoke.rs` itself is
//! FROZEN and stays untouched.
//!
//! Test-first: this MUST fail to compile until `src/` defines the public API
//! (`WaslMcp::with_gating`, `GateAuthority`, `WaslMcp::resolve_pending`, the
//! `commit` tool, ...).

use std::collections::VecDeque;
use std::net::SocketAddr;
use std::sync::{Arc, Mutex};
use std::time::Duration;

use async_trait::async_trait;
use serde_json::{json, Value};

use rmcp::model::{
    CallToolRequestParam, CallToolResult, ClientCapabilities, ClientInfo,
    CreateElicitationRequestParam, CreateElicitationResult, ElicitationAction,
};
use rmcp::service::{RequestContext, RunningService};
use rmcp::{ClientHandler, ErrorData as McpError, RoleClient, RoleServer, ServiceExt};

use wasl_a2a_server::A2aServer;
use wasl_a2a_types::{AgentCapabilities, AgentCard as A2aCard};
use wasl_adapter::{AdapterError, AdapterExecutor, AgentAdapter, Delivery};
use wasl_audit::{AuditEntry, AuditError, AuditLog, Sink};
use wasl_gate::{GateError, GateOutcome};
use wasl_mcp::{CounterTokenIds, GateAuthority, WaslMcp, WaslState};
use wasl_operator::{HubConfig, NodeInfo, OperatorHub};
use wasl_proto::{AgentCard, AgentId, ApprovalToken, ConsultResult, GateDecision, HostId};
use wasl_registry::PeerRegistry;
use wasl_transfer::Runner;

// =======================================================================
// 0. Shared harness
// =======================================================================

fn agent(id: &str) -> AgentId {
    AgentId(id.to_string())
}

fn host(id: &str) -> HostId {
    HostId(id.to_string())
}

fn token(s: &str) -> ApprovalToken {
    ApprovalToken(s.to_string())
}

/// A short-timeout HTTP client so a dead port fails fast instead of hanging.
fn http_client() -> reqwest::Client {
    reqwest::Client::builder()
        .timeout(Duration::from_secs(5))
        .build()
        .expect("build reqwest client")
}

// ---------------------------------------------------------------------
// A shared, introspectable audit sink. `WaslMcp` owns its `WaslState` (and
// therefore the audit log) behind a private `Arc<Mutex<_>>`, so the only way
// a black-box test can inspect what was recorded is to hand the state an
// `Arc`-backed `Sink` and keep a clone of the `Arc` in the test.
// ---------------------------------------------------------------------

#[derive(Clone, Default)]
struct SharedSink(Arc<Mutex<Vec<AuditEntry>>>);

impl SharedSink {
    fn entries(&self) -> Vec<AuditEntry> {
        self.0.lock().expect("shared sink lock").clone()
    }
}

impl Sink for SharedSink {
    fn append(&mut self, entry: &AuditEntry) -> Result<(), AuditError> {
        self.0.lock().expect("shared sink lock").push(entry.clone());
        Ok(())
    }
}

/// A sink that can be switched to fail on demand (and back), so a test can
/// simulate a TRANSIENT audit-sink outage across a `commit` retry — proves
/// the Tier-2 `commit` path is durability-first (regression test for the
/// review finding that `commit` used to consume the ledger token BEFORE the
/// audit write, permanently losing the human's decision on a write failure).
#[derive(Clone, Default)]
struct SwitchableSink {
    entries: Arc<Mutex<Vec<AuditEntry>>>,
    failing: Arc<std::sync::atomic::AtomicBool>,
}

impl SwitchableSink {
    fn set_failing(&self, failing: bool) {
        self.failing
            .store(failing, std::sync::atomic::Ordering::SeqCst);
    }

    fn entries(&self) -> Vec<AuditEntry> {
        self.entries.lock().expect("switchable sink lock").clone()
    }
}

impl Sink for SwitchableSink {
    fn append(&mut self, entry: &AuditEntry) -> Result<(), AuditError> {
        if self.failing.load(std::sync::atomic::Ordering::SeqCst) {
            return Err(AuditError::Sink("disk full".to_string()));
        }
        self.entries
            .lock()
            .expect("switchable sink lock")
            .push(entry.clone());
        Ok(())
    }
}

/// A `Runner` that must never be invoked: every loopback test here either
/// never reaches the transfer seam (consult/command/message) or reaches it
/// only via the `NoDestinations` short-circuit (L9), which never calls
/// `Runner::run`.
#[derive(Clone)]
struct PanicRunner;

impl Runner for PanicRunner {
    fn run(&mut self, _argv: &[String]) -> Result<(), String> {
        panic!("no test in gating_loopback.rs may invoke the transfer runner");
    }
}

type Handler = WaslMcp<SharedSink, PanicRunner>;

/// Production (`WaslMcp::new`/`with_gating`) mints unguessable Tier-2 tokens
/// (`RandomTokenIds`, see `server.rs`'s `TokenIds` seam) — deliberately NOT
/// the pinned `"tok-mcp-{n}"` literals this suite's L5-L7 cases assert on.
/// These loopback tests only need DETERMINISTIC tokens to pin those literals
/// (they are not testing token unguessability — that is
/// `prepared_tokens_are_not_sequential_or_guessable`'s job, below), so the
/// harness opts into `CounterTokenIds` via `with_seams` instead of weakening
/// the production default.
fn build_handler(
    authority: GateAuthority,
    hub: Option<OperatorHub>,
    registry: PeerRegistry,
    sink: SharedSink,
) -> Handler {
    let state = WaslState::new(AuditLog::new(sink), http_client(), PanicRunner);
    WaslMcp::with_seams(
        agent("me"),
        host("h1"),
        registry,
        state,
        authority,
        hub,
        Arc::new(CounterTokenIds::default()),
    )
}

/// The default-authority equivalent of `build_handler` (still deterministic
/// tokens, still `GateAuthority::Elicitation`/no hub — `WaslMcp::new`'s
/// behavior — just via `with_seams` so L5-L7's pinned token literals hold).
fn build_default_handler(registry: PeerRegistry, sink: SharedSink) -> Handler {
    build_handler(GateAuthority::default(), None, registry, sink)
}

// ---------------------------------------------------------------------
// A scriptable rmcp client: declares elicitation capability (or not) at
// `get_info()`, and answers `create_elicitation` with a scripted queue of
// actions (mirrors the smoke.rs `()` bare-client idiom, extended to script
// responses).
// ---------------------------------------------------------------------

#[derive(Clone)]
struct ScriptedClient {
    elicitation_capable: bool,
    actions: Arc<Mutex<VecDeque<ElicitationAction>>>,
}

impl ScriptedClient {
    fn new(elicitation_capable: bool, actions: Vec<ElicitationAction>) -> Self {
        ScriptedClient {
            elicitation_capable,
            actions: Arc::new(Mutex::new(actions.into())),
        }
    }

    fn accepting() -> Self {
        Self::new(true, vec![ElicitationAction::Accept])
    }

    fn declining() -> Self {
        Self::new(true, vec![ElicitationAction::Decline])
    }
}

impl ClientHandler for ScriptedClient {
    fn get_info(&self) -> ClientInfo {
        let mut info = ClientInfo::default();
        if self.elicitation_capable {
            info.capabilities = ClientCapabilities::builder().enable_elicitation().build();
        }
        info
    }

    async fn create_elicitation(
        &self,
        _request: CreateElicitationRequestParam,
        _context: RequestContext<RoleClient>,
    ) -> Result<CreateElicitationResult, McpError> {
        let action = self
            .actions
            .lock()
            .expect("scripted actions lock")
            .pop_front()
            .expect("ScriptedClient: no more scripted elicitation actions");
        Ok(CreateElicitationResult {
            action,
            content: None,
        })
    }
}

/// Wire `handler` and `client` together over an in-memory duplex pipe (the
/// `tests/smoke.rs` idiom) and run both handshakes concurrently. Generic
/// over the handler's `Sink` (not just `Handler = WaslMcp<SharedSink, _>`) so
/// the audit-failure regression test can plug in `SwitchableSink`.
async fn connect<C, H>(
    handler: H,
    client: C,
) -> (RunningService<RoleClient, C>, RunningService<RoleServer, H>)
where
    C: ClientHandler + Send + Sync + 'static,
    H: rmcp::ServerHandler + Send + Sync + 'static,
{
    let (client_tx, server_rx) = tokio::io::duplex(4096);
    let (server_tx, client_rx) = tokio::io::duplex(4096);

    let server_task = tokio::spawn(async move { handler.serve((server_rx, server_tx)).await });
    let client_running = client
        .serve((client_rx, client_tx))
        .await
        .expect("client must initialize");
    let server_running = server_task
        .await
        .expect("server task join")
        .expect("server must initialize");

    (client_running, server_running)
}

// `&Peer<RoleClient>` rather than `&RunningService<RoleClient, C>`: the
// latter isn't `Clone` (only its `Deref` target, `Peer`, is — the cheap
// Arc-backed handle these tests pass into spawned tasks), and `Peer` is the
// single concrete type every `RunningService<RoleClient, _>` derefs to.
async fn call(client: &rmcp::Peer<RoleClient>, name: &str, args: Value) -> CallToolResult {
    client
        .call_tool(CallToolRequestParam {
            name: name.to_string().into(),
            arguments: args.as_object().cloned(),
        })
        .await
        .expect("call_tool transport must succeed")
}

fn text_of(res: &CallToolResult) -> String {
    res.content
        .first()
        .and_then(|c| c.as_text())
        .map(|t| t.text.clone())
        .unwrap_or_default()
}

fn is_error(res: &CallToolResult) -> bool {
    res.is_error.unwrap_or(false)
}

/// Decode a successful tool reply's JSON content into `T`.
fn decode<T: serde::de::DeserializeOwned>(res: &CallToolResult) -> T {
    assert!(!is_error(res), "expected success content, got: {res:?}");
    serde_json::from_str(&text_of(res)).unwrap_or_else(|e| panic!("decode {res:?}: {e}"))
}

// ---------------------------------------------------------------------
// A real loopback A2A peer: a genuine `A2aServer` over a real
// `AdapterExecutor` wrapping a FAKE, recording innermost `AgentAdapter` —
// the `tests/handle.rs` idiom, duplicated (separate test-crate).
// ---------------------------------------------------------------------

struct FakeAdapter {
    recorded: Arc<Mutex<Vec<Delivery>>>,
}

#[async_trait]
impl AgentAdapter for FakeAdapter {
    async fn deliver(&self, d: &Delivery) -> Result<String, AdapterError> {
        self.recorded.lock().expect("recorded lock").push(d.clone());
        Ok(format!("reply:{}", d.text))
    }
}

struct A2aPeer {
    url: String,
    recorded: Arc<Mutex<Vec<Delivery>>>,
    handle: tokio::task::JoinHandle<()>,
}

impl A2aPeer {
    fn deliveries(&self) -> Vec<Delivery> {
        self.recorded.lock().expect("recorded lock").clone()
    }
}

impl Drop for A2aPeer {
    fn drop(&mut self) {
        self.handle.abort();
    }
}

fn a2a_card() -> A2aCard {
    A2aCard {
        name: "loopback-peer".to_string(),
        description: "wasl-mcp gating loopback peer".to_string(),
        url: "http://127.0.0.1/".to_string(),
        version: "0.1.0".to_string(),
        capabilities: AgentCapabilities {
            streaming: false,
            push_notifications: false,
        },
        default_input_modes: vec!["text/plain".to_string()],
        default_output_modes: vec!["text/plain".to_string()],
        skills: vec![],
    }
}

/// Spin a REAL `A2aServer` over a REAL `AdapterExecutor` wrapping a
/// `FakeAdapter`, bound before `axum::serve` spawns (no readiness race).
async fn spawn_a2a_echo() -> A2aPeer {
    let recorded = Arc::new(Mutex::new(Vec::new()));
    let adapter = FakeAdapter {
        recorded: recorded.clone(),
    };
    let executor = Arc::new(AdapterExecutor::new(adapter));
    let server = Arc::new(A2aServer::new(a2a_card(), executor));
    let app = server.router();

    let listener = tokio::net::TcpListener::bind("127.0.0.1:0")
        .await
        .expect("bind ephemeral port");
    let port = listener.local_addr().expect("local addr").port();
    let handle = tokio::spawn(async move {
        axum::serve(listener, app).await.expect("serve");
    });

    A2aPeer {
        url: format!("http://127.0.0.1:{port}/"),
        recorded,
        handle,
    }
}

fn registry_with_peer_a(peer_url: &str) -> PeerRegistry {
    let mut registry = PeerRegistry::new();
    registry.register(AgentCard {
        id: agent("peerA"),
        host: host("loopback"),
        endpoint: Some(peer_url.to_string()),
    });
    registry
}

// ---------------------------------------------------------------------
// The operator's real HTTP+SSE surface: `wasl_operator::router` bound on
// `127.0.0.1:0`, plus a tiny hand-rolled SSE reader over
// `reqwest::Response::chunk()` (no SSE-client crate in the workspace,
// mirroring `wasl-operator`'s own `tests/common::SseReader`).
// ---------------------------------------------------------------------

fn node_info() -> NodeInfo {
    NodeInfo {
        agent_id: agent("me"),
        host_id: host("h1"),
        version: "0.1.0".to_string(),
    }
}

fn fresh_hub() -> OperatorHub {
    OperatorHub::new(node_info(), HubConfig::default())
}

fn hub_with_timeout(ms: u64) -> OperatorHub {
    OperatorHub::new(
        node_info(),
        HubConfig {
            decision_timeout: Duration::from_millis(ms),
            channel_capacity: 256,
        },
    )
}

/// Bind an ephemeral `127.0.0.1` port, mount `wasl_operator::router(hub)`,
/// and spawn `axum::serve`. Binding happens synchronously before the task is
/// spawned so connections queue immediately (no readiness race).
async fn spawn_operator_router(hub: OperatorHub) -> SocketAddr {
    let app = wasl_operator::router(hub);
    let listener = tokio::net::TcpListener::bind("127.0.0.1:0")
        .await
        .expect("bind ephemeral port");
    let addr = listener.local_addr().expect("local addr");
    tokio::spawn(async move {
        axum::serve(listener, app).await.expect("serve");
    });
    addr
}

const SSE_TIMEOUT: Duration = Duration::from_secs(5);

#[derive(Debug, Clone)]
struct SseEvent {
    event: String,
    data: Value,
}

struct SseReader {
    resp: reqwest::Response,
    buf: String,
}

impl SseReader {
    fn new(resp: reqwest::Response) -> Self {
        SseReader {
            resp,
            buf: String::new(),
        }
    }

    async fn next_event(&mut self, timeout: Duration) -> Option<SseEvent> {
        loop {
            if let Some(idx) = self.buf.find("\n\n") {
                let block: String = self.buf.drain(..idx + 2).collect();
                if let Some(ev) = parse_sse_block(&block) {
                    return Some(ev);
                }
                continue;
            }
            match tokio::time::timeout(timeout, self.resp.chunk()).await {
                Ok(Ok(Some(bytes))) => self.buf.push_str(&String::from_utf8_lossy(&bytes)),
                _ => return None,
            }
        }
    }
}

fn parse_sse_block(block: &str) -> Option<SseEvent> {
    let mut event: Option<String> = None;
    let mut data_lines: Vec<String> = Vec::new();
    for raw_line in block.split('\n') {
        let line = raw_line.trim_end_matches('\r');
        if line.is_empty() || line.starts_with(':') {
            continue;
        }
        if let Some(rest) = line.strip_prefix("event:") {
            event = Some(rest.trim().to_string());
        } else if let Some(rest) = line.strip_prefix("data:") {
            data_lines.push(rest.trim().to_string());
        }
        // `id:` is not needed by any assertion below; skipped.
    }
    if data_lines.is_empty() {
        return None; // pure comment/keep-alive block: keep reading.
    }
    let data_str = data_lines.join("\n");
    let data: Value = serde_json::from_str(&data_str)
        .unwrap_or_else(|e| panic!("SSE data was not JSON: {e}: {data_str:?}"));
    Some(SseEvent {
        event: event.unwrap_or_default(),
        data,
    })
}

/// Subscribe to `/v1/events` and consume the `hello` frame.
async fn subscribe(addr: SocketAddr) -> SseReader {
    let resp = http_client()
        .get(format!("http://{addr}/v1/events"))
        .send()
        .await
        .expect("GET /v1/events");
    let mut sse = SseReader::new(resp);
    let hello = sse.next_event(SSE_TIMEOUT).await.expect("hello frame");
    assert_eq!(hello.event, "hello");
    sse
}

async fn post_decision(addr: SocketAddr, gate_id: &str, body: Value) -> reqwest::Response {
    http_client()
        .post(format!("http://{addr}/v1/gates/{gate_id}/decision"))
        .json(&body)
        .send()
        .await
        .expect("POST decision")
}

async fn get_gates(addr: SocketAddr) -> Value {
    http_client()
        .get(format!("http://{addr}/v1/gates"))
        .send()
        .await
        .expect("GET /v1/gates")
        .json()
        .await
        .expect("gates json")
}

// =======================================================================
// L1 — operator-decided send (approve)
// =======================================================================

#[tokio::test]
async fn l1_operator_decided_approve_dispatches_over_a2a() {
    let peer = spawn_a2a_echo().await;
    let hub = fresh_hub();
    let addr = spawn_operator_router(hub.clone()).await;
    let sink = SharedSink::default();

    let handler = build_handler(
        GateAuthority::ExternalFirst,
        Some(hub),
        registry_with_peer_a(&peer.url),
        sink.clone(),
    );
    let (client, server) = connect(handler, ()).await;

    let mut sse = subscribe(addr).await;

    let call_task = tokio::spawn({
        let client = client.clone();
        async move {
            call(
                &client,
                "consult",
                json!({"peers": ["peerA"], "question": "hi"}),
            )
            .await
        }
    });

    let requested = sse
        .next_event(SSE_TIMEOUT)
        .await
        .expect("gate_requested frame");
    assert_eq!(requested.event, "gate_requested");
    assert_eq!(requested.data["decidable"], json!(true));
    assert_eq!(requested.data["mode"], json!("external"));
    assert_eq!(requested.data["request"]["kind"], json!("consult"));
    let gate_id = requested.data["gate_id"]
        .as_str()
        .expect("gate_id string")
        .to_string();

    let snapshot = get_gates(addr).await;
    let gates = snapshot["gates"].as_array().expect("gates array");
    assert_eq!(gates.len(), 1);
    assert_eq!(gates[0]["gate_id"], json!(gate_id));

    let post = post_decision(addr, &gate_id, json!({"outcome": "approved"})).await;
    assert_eq!(post.status(), reqwest::StatusCode::NO_CONTENT);

    let decided = sse
        .next_event(SSE_TIMEOUT)
        .await
        .expect("gate_decided frame");
    assert_eq!(decided.event, "gate_decided");
    assert_eq!(decided.data["outcome"], json!("approved"));
    assert_eq!(decided.data["decided_by"], json!("operator"));

    let result = call_task.await.expect("call task join");
    let consult: ConsultResult = decode(&result);
    assert_eq!(consult.replies.len(), 1);
    assert_eq!(consult.replies[0].from, agent("peerA"));
    assert_eq!(consult.replies[0].body, "reply:hi");

    let deliveries = peer.deliveries();
    assert_eq!(deliveries.len(), 1, "the fake adapter must have SEEN it");
    assert_eq!(deliveries[0].from, "me");

    let entries = sink.entries();
    assert_eq!(entries.len(), 1);
    assert_eq!(entries[0].decision, Some(GateDecision::Approve));

    client.cancel().await.expect("client shutdown");
    server.cancel().await.expect("server shutdown");
}

// =======================================================================
// L2 — operator deny with reason
// =======================================================================

#[tokio::test]
async fn l2_operator_deny_with_reason_blocks_and_records_nothing_on_the_wire() {
    let peer = spawn_a2a_echo().await;
    let hub = fresh_hub();
    let addr = spawn_operator_router(hub.clone()).await;
    let sink = SharedSink::default();

    let handler = build_handler(
        GateAuthority::ExternalFirst,
        Some(hub),
        registry_with_peer_a(&peer.url),
        sink.clone(),
    );
    let (client, server) = connect(handler, ()).await;

    let mut sse = subscribe(addr).await;

    let call_task = tokio::spawn({
        let client = client.clone();
        async move {
            call(
                &client,
                "consult",
                json!({"peers": ["peerA"], "question": "hi"}),
            )
            .await
        }
    });

    let requested = sse
        .next_event(SSE_TIMEOUT)
        .await
        .expect("gate_requested frame");
    let gate_id = requested.data["gate_id"].as_str().unwrap().to_string();

    let post = post_decision(
        addr,
        &gate_id,
        json!({"outcome": "denied", "reason": "no thanks"}),
    )
    .await;
    assert_eq!(post.status(), reqwest::StatusCode::NO_CONTENT);

    let decided = sse
        .next_event(SSE_TIMEOUT)
        .await
        .expect("gate_decided frame");
    assert_eq!(decided.data["outcome"], json!("denied"));
    assert_eq!(decided.data["reason"], json!("no thanks"));

    let result = call_task.await.expect("call task join");
    assert!(is_error(&result));
    assert_eq!(text_of(&result), "blocked: no thanks");

    assert!(
        peer.deliveries().is_empty(),
        "a denied call must never dispatch"
    );

    let entries = sink.entries();
    assert_eq!(entries.len(), 1);
    assert_eq!(
        entries[0].decision,
        Some(GateDecision::Deny {
            reason: "no thanks".to_string()
        })
    );

    client.cancel().await.expect("client shutdown");
    server.cancel().await.expect("server shutdown");
}

// =======================================================================
// L3 — operator timeout
// =======================================================================

#[tokio::test]
async fn l3_operator_timeout_dismisses_and_late_post_conflicts() {
    let peer = spawn_a2a_echo().await;
    let hub = hub_with_timeout(50);
    let addr = spawn_operator_router(hub.clone()).await;
    let sink = SharedSink::default();

    let handler = build_handler(
        GateAuthority::ExternalFirst,
        Some(hub),
        registry_with_peer_a(&peer.url),
        sink.clone(),
    );
    let (client, server) = connect(handler, ()).await;

    let mut sse = subscribe(addr).await;

    let call_task = tokio::spawn({
        let client = client.clone();
        async move {
            call(
                &client,
                "consult",
                json!({"peers": ["peerA"], "question": "hi"}),
            )
            .await
        }
    });

    let requested = sse
        .next_event(SSE_TIMEOUT)
        .await
        .expect("gate_requested frame");
    let gate_id = requested.data["gate_id"].as_str().unwrap().to_string();

    // No POST — let the tiny decision_timeout dismiss it.
    let result = call_task.await.expect("call task join");
    assert!(is_error(&result));
    assert_eq!(text_of(&result), "blocked: dismissed");

    let decided = sse
        .next_event(SSE_TIMEOUT)
        .await
        .expect("gate_decided frame");
    assert_eq!(decided.data["outcome"], json!("dismissed"));
    assert_eq!(decided.data["decided_by"], json!("timeout"));

    let entries = sink.entries();
    assert_eq!(entries.len(), 1);
    assert_eq!(
        entries[0].decision, None,
        "a dismissal carries no gate decision"
    );

    // A late POST conflicts.
    let late = post_decision(addr, &gate_id, json!({"outcome": "approved"})).await;
    assert_eq!(late.status(), reqwest::StatusCode::CONFLICT);
    let body: Value = late.json().await.expect("409 body");
    assert_eq!(body["error"], json!("already_decided"));

    assert!(peer.deliveries().is_empty());

    client.cancel().await.expect("client shutdown");
    server.cancel().await.expect("server shutdown");
}

// =======================================================================
// L4 — elicitation mirrored observe-only
// =======================================================================

#[tokio::test]
async fn l4_elicitation_accept_is_mirrored_observe_only_and_dispatches() {
    let peer = spawn_a2a_echo().await;
    let hub = fresh_hub();
    let addr = spawn_operator_router(hub.clone()).await;
    let sink = SharedSink::default();

    let handler = build_handler(
        GateAuthority::ElicitationFirst,
        Some(hub),
        registry_with_peer_a(&peer.url),
        sink.clone(),
    );
    let (client, server) = connect(handler, ScriptedClient::accepting()).await;

    let mut sse = subscribe(addr).await;

    let result = call(
        &client,
        "consult",
        json!({"peers": ["peerA"], "question": "hi"}),
    )
    .await;
    let consult: ConsultResult = decode(&result);
    assert_eq!(consult.replies[0].body, "reply:hi");
    assert_eq!(
        peer.deliveries().len(),
        1,
        "elicitation approval dispatches"
    );

    let requested = sse
        .next_event(SSE_TIMEOUT)
        .await
        .expect("mirrored gate_requested frame");
    assert_eq!(requested.data["decidable"], json!(false));
    assert_eq!(requested.data["mode"], json!("elicitation"));
    let gate_id = requested.data["gate_id"].as_str().unwrap().to_string();

    let decided = sse
        .next_event(SSE_TIMEOUT)
        .await
        .expect("mirrored gate_decided frame");
    assert_eq!(decided.data["outcome"], json!("approved"));
    assert_eq!(decided.data["decided_by"], json!("elicitation"));

    // Never decidable: absent from /v1/gates, and a POST on it 404s.
    let snapshot = get_gates(addr).await;
    assert!(snapshot["gates"].as_array().unwrap().is_empty());
    let post = post_decision(addr, &gate_id, json!({"outcome": "approved"})).await;
    assert_eq!(post.status(), reqwest::StatusCode::NOT_FOUND);
    let body: Value = post.json().await.expect("404 body");
    assert_eq!(body["error"], json!("unknown_gate"));

    client.cancel().await.expect("client shutdown");
    server.cancel().await.expect("server shutdown");
}

#[tokio::test]
async fn l4_elicitation_decline_is_mirrored_denied_and_never_dispatches() {
    let peer = spawn_a2a_echo().await;
    let hub = fresh_hub();
    let addr = spawn_operator_router(hub.clone()).await;
    let sink = SharedSink::default();

    let handler = build_handler(
        GateAuthority::ElicitationFirst,
        Some(hub),
        registry_with_peer_a(&peer.url),
        sink.clone(),
    );
    let (client, server) = connect(handler, ScriptedClient::declining()).await;

    let mut sse = subscribe(addr).await;

    let result = call(
        &client,
        "consult",
        json!({"peers": ["peerA"], "question": "hi"}),
    )
    .await;
    assert!(is_error(&result));
    assert_eq!(text_of(&result), "blocked: declined");
    assert!(
        peer.deliveries().is_empty(),
        "a decline must never dispatch"
    );

    sse.next_event(SSE_TIMEOUT)
        .await
        .expect("mirrored gate_requested frame");
    let decided = sse
        .next_event(SSE_TIMEOUT)
        .await
        .expect("mirrored gate_decided frame");
    assert_eq!(decided.data["outcome"], json!("denied"));
    assert_eq!(decided.data["decided_by"], json!("elicitation"));
    assert_eq!(decided.data["reason"], json!("declined"));

    client.cancel().await.expect("client shutdown");
    server.cancel().await.expect("server shutdown");
}

// =======================================================================
// L5 — Tier-2 roundtrip via operator POST
// =======================================================================

#[tokio::test]
async fn l5_prepare_commit_roundtrip_via_operator_post() {
    let peer = spawn_a2a_echo().await;
    let hub = fresh_hub();
    let addr = spawn_operator_router(hub.clone()).await;
    let sink = SharedSink::default();

    let handler = build_handler(
        GateAuthority::PrepareCommit,
        Some(hub),
        registry_with_peer_a(&peer.url),
        sink.clone(),
    );
    let (client, server) = connect(handler, ()).await;

    let mut sse = subscribe(addr).await;

    let pending_json = "{\"status\":\"pending\",\"token\":\"tok-mcp-0\",\"hint\":\"awaiting human \
                         approval; call the commit tool with this token\"}";

    let first = call(
        &client,
        "consult",
        json!({"peers": ["peerA"], "question": "hi"}),
    )
    .await;
    assert!(!is_error(&first), "pending is a SUCCESS content block");
    assert_eq!(text_of(&first), pending_json);

    let requested = sse
        .next_event(SSE_TIMEOUT)
        .await
        .expect("gate_requested frame");
    assert_eq!(requested.data["decidable"], json!(true));
    assert_eq!(requested.data["mode"], json!("prepare_commit"));
    assert_eq!(requested.data["request"]["id"], json!("tok-mcp-0"));
    let gate_id = requested.data["gate_id"].as_str().unwrap().to_string();

    // commit BEFORE any decision -> the pending JSON again (idempotent poll).
    let poll = call(&client, "commit", json!({"token": "tok-mcp-0"})).await;
    assert!(!is_error(&poll));
    assert_eq!(text_of(&poll), pending_json);

    let post = post_decision(addr, &gate_id, json!({"outcome": "approved"})).await;
    assert_eq!(post.status(), reqwest::StatusCode::NO_CONTENT);

    sse.next_event(SSE_TIMEOUT)
        .await
        .expect("gate_decided frame");

    // The decision reaches `decisions` via a spawned background task; poll
    // `commit` until it settles (short bound, no arbitrary sleep-and-hope).
    let mut committed = None;
    for _ in 0..100 {
        let res = call(&client, "commit", json!({"token": "tok-mcp-0"})).await;
        if text_of(&res) != pending_json {
            committed = Some(res);
            break;
        }
        tokio::time::sleep(Duration::from_millis(20)).await;
    }
    let committed = committed.expect("commit must eventually settle");
    let consult: ConsultResult = decode(&committed);
    assert_eq!(consult.replies[0].from, agent("peerA"));
    assert_eq!(consult.replies[0].body, "reply:hi");

    let entries = sink.entries();
    assert_eq!(
        entries.len(),
        1,
        "exactly ONE audit entry, recorded at commit, not at prepare"
    );
    assert_eq!(entries[0].decision, Some(GateDecision::Approve));

    let re_commit = call(&client, "commit", json!({"token": "tok-mcp-0"})).await;
    assert!(is_error(&re_commit));
    assert_eq!(
        text_of(&re_commit),
        "blocked: approval token already resolved"
    );

    client.cancel().await.expect("client shutdown");
    server.cancel().await.expect("server shutdown");
}

// =======================================================================
// L6 — Tier-2 embedder path (no hub)
// =======================================================================

#[tokio::test]
async fn l6_prepare_commit_embedder_path_approved() {
    let peer = spawn_a2a_echo().await;
    let sink = SharedSink::default();
    let handler = build_handler(
        GateAuthority::PrepareCommit,
        None,
        registry_with_peer_a(&peer.url),
        sink.clone(),
    );
    let embedder = handler.clone();
    let (client, server) = connect(handler, ()).await;

    let pending = call(
        &client,
        "consult",
        json!({"peers": ["peerA"], "question": "hi"}),
    )
    .await;
    assert!(!is_error(&pending));

    embedder
        .resolve_pending(&token("tok-mcp-0"), GateOutcome::Approved)
        .await
        .expect("first resolve_pending must succeed");

    assert_eq!(
        embedder
            .resolve_pending(&token("tok-mcp-0"), GateOutcome::Approved)
            .await,
        Err(GateError::AlreadyResolved)
    );

    let committed = call(&client, "commit", json!({"token": "tok-mcp-0"})).await;
    let consult: ConsultResult = decode(&committed);
    assert_eq!(consult.replies[0].body, "reply:hi");

    assert_eq!(
        embedder
            .resolve_pending(&token("tok-mcp-999"), GateOutcome::Approved)
            .await,
        Err(GateError::UnknownToken)
    );

    client.cancel().await.expect("client shutdown");
    server.cancel().await.expect("server shutdown");
}

#[tokio::test]
async fn l6_prepare_commit_embedder_path_denied() {
    let peer = spawn_a2a_echo().await;
    let sink = SharedSink::default();
    let handler = build_handler(
        GateAuthority::PrepareCommit,
        None,
        registry_with_peer_a(&peer.url),
        sink.clone(),
    );
    let embedder = handler.clone();
    let (client, server) = connect(handler, ()).await;

    call(
        &client,
        "consult",
        json!({"peers": ["peerA"], "question": "hi"}),
    )
    .await;

    embedder
        .resolve_pending(
            &token("tok-mcp-0"),
            GateOutcome::Denied {
                reason: "no".to_string(),
            },
        )
        .await
        .expect("resolve_pending Denied must succeed");

    let committed = call(&client, "commit", json!({"token": "tok-mcp-0"})).await;
    assert!(is_error(&committed));
    assert_eq!(text_of(&committed), "blocked: no");

    let entries = sink.entries();
    assert_eq!(entries.len(), 1);
    assert_eq!(
        entries[0].decision,
        Some(GateDecision::Deny {
            reason: "no".to_string()
        })
    );
    assert!(peer.deliveries().is_empty());

    client.cancel().await.expect("client shutdown");
    server.cancel().await.expect("server shutdown");
}

#[tokio::test]
async fn l6_prepare_commit_embedder_path_dismissed() {
    let peer = spawn_a2a_echo().await;
    let sink = SharedSink::default();
    let handler = build_handler(
        GateAuthority::PrepareCommit,
        None,
        registry_with_peer_a(&peer.url),
        sink.clone(),
    );
    let embedder = handler.clone();
    let (client, server) = connect(handler, ()).await;

    call(
        &client,
        "consult",
        json!({"peers": ["peerA"], "question": "hi"}),
    )
    .await;

    embedder
        .resolve_pending(&token("tok-mcp-0"), GateOutcome::Dismissed)
        .await
        .expect("resolve_pending Dismissed must succeed");

    let committed = call(&client, "commit", json!({"token": "tok-mcp-0"})).await;
    assert!(is_error(&committed));
    assert_eq!(text_of(&committed), "blocked: dismissed");

    let entries = sink.entries();
    assert_eq!(entries.len(), 1);
    assert_eq!(entries[0].decision, None);
    assert!(peer.deliveries().is_empty());

    client.cancel().await.expect("client shutdown");
    server.cancel().await.expect("server shutdown");
}

// =======================================================================
// L7 — commit on an unknown token
// =======================================================================

#[tokio::test]
async fn l7_commit_on_unknown_token_when_nothing_was_ever_prepared() {
    let sink = SharedSink::default();
    let handler = build_default_handler(PeerRegistry::new(), sink.clone());
    let (client, server) = connect(handler, ()).await;

    let res = call(&client, "commit", json!({"token": "tok-mcp-0"})).await;
    assert!(is_error(&res));
    assert_eq!(text_of(&res), "blocked: unknown approval token");
    assert!(
        sink.entries().is_empty(),
        "an unknown commit audits nothing"
    );

    client.cancel().await.expect("client shutdown");
    server.cancel().await.expect("server shutdown");
}

// =======================================================================
// L8 — default authority = today's behavior
// =======================================================================

#[tokio::test]
async fn l8_default_authority_denies_a_non_elicitation_capable_client_on_every_tool() {
    let peer = spawn_a2a_echo().await;
    let sink = SharedSink::default();
    let mut registry = registry_with_peer_a(&peer.url);
    registry.register(AgentCard {
        id: agent("hostPeer"),
        host: host("h2"),
        endpoint: Some(peer.url.clone()),
    });
    let handler = build_default_handler(registry, sink.clone());
    let (client, server) = connect(handler, ()).await;

    const PINNED_DENY: &str = "blocked: client cannot be prompted (no elicitation capability)";

    let c = call(
        &client,
        "consult",
        json!({"peers": ["peerA"], "question": "q"}),
    )
    .await;
    assert!(is_error(&c));
    assert_eq!(text_of(&c), PINNED_DENY);

    let cmd = call(&client, "command", json!({"peers": ["peerA"], "task": "t"})).await;
    assert!(is_error(&cmd));
    assert_eq!(text_of(&cmd), PINNED_DENY);

    let m = call(
        &client,
        "message",
        json!({"peers": ["peerA"], "payload": "p"}),
    )
    .await;
    assert!(is_error(&m));
    assert_eq!(text_of(&m), PINNED_DENY);

    let t = call(
        &client,
        "transfer",
        json!({"src_host": "h1", "dst_hosts": ["h2"], "path": "/data/f"}),
    )
    .await;
    assert!(is_error(&t));
    assert_eq!(text_of(&t), PINNED_DENY);

    assert!(
        peer.deliveries().is_empty(),
        "every denied call must never dispatch"
    );
    let entries = sink.entries();
    assert_eq!(entries.len(), 4);
    for e in &entries {
        assert_eq!(
            e.decision,
            Some(GateDecision::Deny {
                reason: "client cannot be prompted (no elicitation capability)".to_string()
            })
        );
    }

    client.cancel().await.expect("client shutdown");
    server.cancel().await.expect("server shutdown");
}

#[tokio::test]
async fn l8_default_authority_elicitation_capable_accept_still_dispatches() {
    let peer = spawn_a2a_echo().await;
    let sink = SharedSink::default();
    let handler = build_default_handler(registry_with_peer_a(&peer.url), sink.clone());
    let (client, server) = connect(handler, ScriptedClient::accepting()).await;

    let result = call(
        &client,
        "consult",
        json!({"peers": ["peerA"], "question": "hi"}),
    )
    .await;
    let consult: ConsultResult = decode(&result);
    assert_eq!(consult.replies[0].body, "reply:hi");
    assert_eq!(peer.deliveries().len(), 1);
    assert_eq!(sink.entries()[0].decision, Some(GateDecision::Approve));

    client.cancel().await.expect("client shutdown");
    server.cancel().await.expect("server shutdown");
}

// =======================================================================
// L9 — NoDestinations pinned reply
// =======================================================================

#[tokio::test]
async fn l9_approved_transfer_with_no_destinations_is_a_pinned_blocked_reply() {
    let sink = SharedSink::default();
    let handler = build_default_handler(PeerRegistry::new(), sink.clone());
    let (client, server) = connect(handler, ScriptedClient::accepting()).await;

    let result = call(
        &client,
        "transfer",
        json!({"src_host": "h1", "dst_hosts": [], "path": "/data/f"}),
    )
    .await;
    assert!(is_error(&result));
    assert_eq!(
        text_of(&result),
        "blocked: transfer has no destination hosts"
    );

    let entries = sink.entries();
    assert_eq!(entries.len(), 1);
    assert_eq!(
        entries[0].decision,
        Some(GateDecision::Approve),
        "the gate decision is audited even though the transfer itself failed post-gate"
    );

    client.cancel().await.expect("client shutdown");
    server.cancel().await.expect("server shutdown");
}

// =======================================================================
// Bug 1 (durability-first commit) — regression: a Tier-2 `commit` audit-sink
// failure must not permanently strand an approved decision. The old code
// consumed the ledger token (and dropped the decision out of `decisions`)
// BEFORE the audit write ran, so a transient sink failure lost the human's
// approval with no audit trace and made the token "already resolved"
// forever on every retry.
// =======================================================================

#[tokio::test]
async fn commit_survives_a_transient_audit_failure_and_redeems_exactly_once_on_retry() {
    let peer = spawn_a2a_echo().await;
    let sink = SwitchableSink::default();
    let state = WaslState::new(AuditLog::new(sink.clone()), http_client(), PanicRunner);
    let handler: WaslMcp<SwitchableSink, PanicRunner> = WaslMcp::with_seams(
        agent("me"),
        host("h1"),
        registry_with_peer_a(&peer.url),
        state,
        GateAuthority::PrepareCommit,
        None,
        Arc::new(CounterTokenIds::default()),
    );
    let embedder = handler.clone();
    let (client, server) = connect(handler, ()).await;

    let pending = call(
        &client,
        "consult",
        json!({"peers": ["peerA"], "question": "hi"}),
    )
    .await;
    assert!(!is_error(&pending), "pending is a SUCCESS content block");

    embedder
        .resolve_pending(&token("tok-mcp-0"), GateOutcome::Approved)
        .await
        .expect("resolve_pending must succeed");

    // The sink is failing: `commit` must fail the whole call
    // (durability-first, handle.rs's own contract) rather than silently
    // dropping the decision or dispatching anyway.
    sink.set_failing(true);
    let failing_attempt = client
        .call_tool(CallToolRequestParam {
            name: "commit".to_string().into(),
            arguments: json!({"token": "tok-mcp-0"}).as_object().cloned(),
        })
        .await;
    assert!(
        failing_attempt.is_err(),
        "an audit-sink failure must propagate as a call error, not a silent success: {failing_attempt:?}"
    );
    assert!(
        sink.entries().is_empty(),
        "the failed write must not be recorded"
    );
    assert!(
        peer.deliveries().is_empty(),
        "nothing may dispatch while the audit write is failing"
    );

    // THE BUG: the old code committed the ledger (and removed the decision
    // from `decisions`) BEFORE the audit write above ran, so at this point
    // the token would already be irrecoverably "resolved" with the approval
    // gone. Prove the fix: the SAME token is still redeemable once the sink
    // recovers.
    sink.set_failing(false);
    let retried = call(&client, "commit", json!({"token": "tok-mcp-0"})).await;
    assert!(
        !is_error(&retried),
        "the same token must still redeem once the sink recovers: {retried:?}"
    );
    let consult: ConsultResult = decode(&retried);
    assert_eq!(consult.replies[0].from, agent("peerA"));
    assert_eq!(consult.replies[0].body, "reply:hi");
    assert_eq!(
        peer.deliveries().len(),
        1,
        "the retry must dispatch exactly once"
    );
    assert_eq!(
        sink.entries().len(),
        1,
        "exactly ONE audit entry — from the successful retry, none from the failed attempt"
    );
    assert_eq!(sink.entries()[0].decision, Some(GateDecision::Approve));

    // Now that it is truly committed, a further commit is a terminal
    // already-resolved block, never a re-dispatch.
    let re_commit = call(&client, "commit", json!({"token": "tok-mcp-0"})).await;
    assert!(is_error(&re_commit));
    assert_eq!(
        text_of(&re_commit),
        "blocked: approval token already resolved"
    );
    assert_eq!(
        peer.deliveries().len(),
        1,
        "a terminal re-commit must never dispatch again"
    );

    client.cancel().await.expect("client shutdown");
    server.cancel().await.expect("server shutdown");
}

// =======================================================================
// Bug 3 (security) — Tier-2 approval tokens must be unguessable, not the
// old sequential `tok-mcp-{seq}` counter. `WaslMcp` is `Clone` over a shared
// `Arc<Mutex<WaslState>>`, so nothing else stops a second session from
// calling `commit` with a token it merely guessed; only the token's own
// unguessability protects it. Deliberately uses the PRODUCTION
// `with_gating` constructor (not `with_seams` + `CounterTokenIds`, unlike
// every other test in this file) so this proves what actually ships.
// =======================================================================

#[tokio::test]
async fn prepared_tokens_are_not_sequential_or_guessable() {
    let peer = spawn_a2a_echo().await;
    let sink = SharedSink::default();
    let state = WaslState::new(AuditLog::new(sink), http_client(), PanicRunner);
    let handler: Handler = WaslMcp::with_gating(
        agent("me"),
        host("h1"),
        registry_with_peer_a(&peer.url),
        state,
        GateAuthority::PrepareCommit,
        None,
    );
    let (client, server) = connect(handler, ()).await;

    let first = call(
        &client,
        "consult",
        json!({"peers": ["peerA"], "question": "hi"}),
    )
    .await;
    let second = call(
        &client,
        "consult",
        json!({"peers": ["peerA"], "question": "hi"}),
    )
    .await;

    let tok1 = decode::<Value>(&first)["token"]
        .as_str()
        .expect("token string")
        .to_string();
    let tok2 = decode::<Value>(&second)["token"]
        .as_str()
        .expect("token string")
        .to_string();

    assert_ne!(
        tok1, tok2,
        "two distinct prepared gates get distinct tokens"
    );
    assert_ne!(tok1, "tok-mcp-0", "must not be the old sequential scheme");
    assert_ne!(tok2, "tok-mcp-1", "must not be the old sequential scheme");

    // Unguessable: 128 bits of hex payload after the fixed prefix, and the
    // two draws must not merely be adjacent integers (the whole point of
    // the fix — a guessable/enumerable scheme, even a non-decimal one,
    // would still defeat the purpose).
    let mut values = Vec::new();
    for tok in [&tok1, &tok2] {
        let payload = tok.strip_prefix("tok-mcp-").expect("tok-mcp- prefix");
        assert_eq!(payload.len(), 32, "128 bits of hex payload: {tok}");
        assert!(
            payload.chars().all(|c| c.is_ascii_hexdigit()),
            "hex payload: {tok}"
        );
        values.push(u128::from_str_radix(payload, 16).expect("valid hex"));
    }
    assert!(
        values[0].abs_diff(values[1]) > 1,
        "tokens must not be sequential/adjacent integers"
    );

    client.cancel().await.expect("client shutdown");
    server.cancel().await.expect("server shutdown");
}

// =======================================================================
// Smoke — the FIVE advertised tools (the one permitted edit-adjacent change:
// a NEW test here, tests/smoke.rs itself stays frozen and untouched).
// =======================================================================

#[tokio::test]
async fn smoke_lists_the_five_tools_including_commit() {
    let sink = SharedSink::default();
    let handler = build_default_handler(PeerRegistry::new(), sink);
    let (client, server) = connect(handler, ()).await;

    let tools = client
        .list_all_tools()
        .await
        .expect("list_tools must succeed");
    let mut names: Vec<String> = tools.iter().map(|t| t.name.to_string()).collect();
    names.sort();
    assert_eq!(
        names,
        vec![
            "command".to_string(),
            "commit".to_string(),
            "consult".to_string(),
            // Feature A: the roster-discovery tool is now advertised alongside
            // the send/commit tools so the inner agent can map names -> agent_ids.
            "list_peers".to_string(),
            "message".to_string(),
            "transfer".to_string(),
        ],
        "the Wasl tools (post-W4: + commit; Feature A: + list_peers) must be advertised"
    );

    client.cancel().await.expect("client shutdown");
    server.cancel().await.expect("server shutdown");
}

// =======================================================================
// Feature A — list_peers returns the roster by NAME + self identity, and the
// send tools accept a name alias.
// =======================================================================

/// `list_peers` joins the routing registry (agent_id/host/url) with the
/// config-delivered name side-table (name/host_name) and reports this node's own
/// identity by name — the mapping the manager uses to address "Writer" instead
/// of an opaque `draft-node-<uuid>`.
#[tokio::test]
async fn list_peers_reports_roster_by_name_and_self_identity() {
    use std::collections::BTreeMap;
    use wasl_mcp::config::PeerName;

    // A registry with one reachable peer (routing data), plus a name side-table
    // giving it a human name — exactly what `build_handler` wires from config.
    let mut registry = PeerRegistry::new();
    registry.register(AgentCard {
        id: AgentId("draft-node-writer".to_string()),
        host: HostId("host-17".to_string()),
        endpoint: Some("http://10.0.0.7:48000".to_string()),
    });
    let mut names = BTreeMap::new();
    names.insert(
        "draft-node-writer".to_string(),
        PeerName {
            name: "Writer".to_string(),
            host_name: "cpu-box".to_string(),
        },
    );

    let state = WaslState::new(AuditLog::new(SharedSink::default()), http_client(), PanicRunner);
    let handler = WaslMcp::with_seams(
        agent("draft-node-self"),
        host("host-16"),
        registry,
        state,
        GateAuthority::default(),
        None,
        Arc::new(CounterTokenIds::default()),
    )
    .with_names("gpu-box", names);

    let (client, server) = connect(handler, ()).await;
    let result = call(&client, "list_peers", json!({})).await;
    let text = text_of(&result);
    assert!(!text.is_empty(), "list_peers returns a text block");
    let reply: Value = serde_json::from_str(&text).expect("list_peers returns JSON");

    let peers = reply["peers"].as_array().expect("peers array");
    assert_eq!(peers.len(), 1, "the one reachable peer is listed");
    assert_eq!(peers[0]["name"], "Writer");
    assert_eq!(peers[0]["agent_id"], "draft-node-writer");
    assert_eq!(peers[0]["host"], "host-17");
    assert_eq!(peers[0]["host_name"], "cpu-box");
    assert_eq!(peers[0]["url"], "http://10.0.0.7:48000");

    // Self identity carries the node's own host display name.
    assert_eq!(reply["self"]["agent_id"], "draft-node-self");
    assert_eq!(reply["self"]["host"], "host-16");
    assert_eq!(reply["self"]["host_name"], "gpu-box");

    client.cancel().await.expect("client shutdown");
    server.cancel().await.expect("server shutdown");
}
