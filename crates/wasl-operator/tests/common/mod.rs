#![allow(dead_code)]
//! Shared black-box test fixtures for `wasl_operator`.
//!
//! Nothing here touches `src/` — these helpers only build wire fixtures (from
//! `wasl_proto`/`wasl_audit`), a settable fake `Clock`, scriptable fake
//! `wasl_audit::Sink`s, and a tiny hand-rolled SSE client over `reqwest`'s byte
//! stream (there is no SSE client crate in the workspace). `hub.rs`,
//! `loopback.rs`, `serve.rs` and `fixtures.rs` all `mod common;` this.

use std::collections::VecDeque;
use std::net::SocketAddr;
use std::pin::Pin;
use std::sync::{Arc, Mutex};
use std::time::{Duration, SystemTime, UNIX_EPOCH};

use bytes::Bytes;
use futures::{Stream, StreamExt};
use serde_json::Value;

use wasl_audit::{AuditEntry, AuditError, EntryKind, Sink};
use wasl_proto::{AgentId, GateKind, GateRequest, HostId};

use wasl_operator::{Clock, HubConfig, NodeInfo, OperatorHub};

// ---------------------------------------------------------------------
// Deterministic instants — shared with the golden fixtures under
// fixtures/operator-v1/. `T0` is exactly "2026-07-02T12:00:00.000Z".
// ---------------------------------------------------------------------

pub const T0_MS: u64 = 1_782_993_600_000;
pub const T_DECIDE_MS: u64 = 1_782_993_603_500; // T0 + 3.5s -> "...12:00:03.500Z"
pub const T_ELICIT_REQ_MS: u64 = 1_782_993_605_000; // T0 + 5s -> "...12:00:05.000Z"
pub const T_ELICIT_DECIDE_MS: u64 = 1_782_993_606_000; // T0 + 6s -> "...12:00:06.000Z"
pub const T_DEADLINE_MS: u64 = 1_782_993_720_000; // T0 + 120s -> "...12:02:00.000Z"

/// Build a `SystemTime` from a UNIX-epoch millisecond count.
pub fn at(ms: u64) -> SystemTime {
    UNIX_EPOCH + Duration::from_millis(ms)
}

// ---------------------------------------------------------------------
// Fake clock (settable)
// ---------------------------------------------------------------------

pub struct FakeClock(Mutex<SystemTime>);

impl FakeClock {
    pub fn new(t: SystemTime) -> Self {
        FakeClock(Mutex::new(t))
    }

    pub fn set(&self, t: SystemTime) {
        *self.0.lock().unwrap() = t;
    }
}

impl Clock for FakeClock {
    fn now(&self) -> SystemTime {
        *self.0.lock().unwrap()
    }
}

// ---------------------------------------------------------------------
// Common node identity / gate requests
// ---------------------------------------------------------------------

/// The `NodeInfo` used across the golden fixtures: `{"agent_id":"wasl-a","host_id":"host-1","version":"0.1.0"}`.
pub fn node_info() -> NodeInfo {
    NodeInfo {
        agent_id: AgentId("wasl-a".to_string()),
        host_id: HostId("host-1".to_string()),
        version: "0.1.0".to_string(),
    }
}

/// A hub with `node_info()` identity, `HubConfig::default()` tuning, the given
/// (fake) clock, and the default `CounterGateIds` (so gate ids are
/// deterministic: "gate-1", "gate-2", ...).
pub fn fresh_hub_with_clock(clock: Arc<dyn Clock>) -> OperatorHub {
    OperatorHub::with_seams(
        node_info(),
        HubConfig::default(),
        clock,
        Arc::new(wasl_operator::CounterGateIds::default()),
    )
}

/// A hub with an explicit `HubConfig` (e.g. a short `decision_timeout` for
/// timeout tests) atop the shared identity/clock/ids conventions.
pub fn hub_with_cfg(clock: Arc<dyn Clock>, cfg: HubConfig) -> OperatorHub {
    OperatorHub::with_seams(
        node_info(),
        cfg,
        clock,
        Arc::new(wasl_operator::CounterGateIds::default()),
    )
}

/// The `GateRequest` used verbatim in `gate_requested_external.json` /
/// `gates_snapshot.json`: `call-9`, a message to `wasl-a`, "hi bob".
pub fn req_call9() -> GateRequest {
    GateRequest {
        id: "call-9".to_string(),
        kind: GateKind::Message,
        from: AgentId("wasl-a".to_string()),
        summary: "message to 1 peer(s)".to_string(),
        preview: "hi bob".to_string(),
    }
}

/// The `GateRequest` used verbatim in `gate_requested_elicitation.json`:
/// `call-10`, a consult from `wasl-b`.
pub fn req_call10() -> GateRequest {
    GateRequest {
        id: "call-10".to_string(),
        kind: GateKind::Consult,
        from: AgentId("wasl-b".to_string()),
        summary: "consult 1 peer(s)".to_string(),
        preview: "what's the status?".to_string(),
    }
}

/// A minimal, deterministic filler `AuditEntry` used only to advance the
/// hub's frame sequence counter to a known value.
pub fn filler_entry(seq: u64) -> AuditEntry {
    AuditEntry {
        seq,
        kind: EntryKind::Message,
        actor: AgentId("filler".to_string()),
        targets: vec![],
        summary: "filler".to_string(),
        decision: None,
    }
}

/// Publish `n` filler audit frames through the hub (advances the SSE
/// sequence counter by exactly `n`, independent of gate ids).
pub fn publish_n_filler(hub: &OperatorHub, n: u64) {
    for seq in 1..=n {
        hub.publish_audit(&filler_entry(seq));
    }
}

// ---------------------------------------------------------------------
// Scriptable fake `wasl_audit::Sink`s
// ---------------------------------------------------------------------

/// Records every entry it's given; always succeeds.
#[derive(Default)]
pub struct RecordingSink {
    pub entries: Mutex<Vec<AuditEntry>>,
}

impl RecordingSink {
    pub fn len(&self) -> usize {
        self.entries.lock().unwrap().len()
    }

    pub fn snapshot(&self) -> Vec<AuditEntry> {
        self.entries.lock().unwrap().clone()
    }
}

impl Sink for RecordingSink {
    fn append(&mut self, entry: &AuditEntry) -> Result<(), AuditError> {
        self.entries.lock().unwrap().push(entry.clone());
        Ok(())
    }
}

/// Always fails with a fixed message; records nothing.
pub struct FailingSink {
    pub message: String,
}

impl FailingSink {
    pub fn new(message: &str) -> Self {
        FailingSink {
            message: message.to_string(),
        }
    }
}

impl Sink for FailingSink {
    fn append(&mut self, _entry: &AuditEntry) -> Result<(), AuditError> {
        Err(AuditError::Sink(self.message.clone()))
    }
}

/// Wraps a `RecordingSink` and, on every `append`, asserts that a shared
/// broadcast receiver (subscribed to the same hub `TeeSink` will publish to)
/// has NOTHING pending yet — proving durable-append happens strictly before
/// the observation-feed broadcast.
pub struct OrderCheckingSink {
    pub inner: RecordingSink,
    rx: Arc<Mutex<tokio::sync::broadcast::Receiver<wasl_operator::SeqFrame>>>,
}

impl OrderCheckingSink {
    pub fn new(rx: Arc<Mutex<tokio::sync::broadcast::Receiver<wasl_operator::SeqFrame>>>) -> Self {
        OrderCheckingSink {
            inner: RecordingSink::default(),
            rx,
        }
    }
}

impl Sink for OrderCheckingSink {
    fn append(&mut self, entry: &AuditEntry) -> Result<(), AuditError> {
        {
            let mut rx = self.rx.lock().unwrap();
            match rx.try_recv() {
                Err(tokio::sync::broadcast::error::TryRecvError::Empty) => {}
                other => panic!(
                    "TeeSink broadcast a frame before the inner sink's append returned: {other:?}"
                ),
            }
        }
        self.inner.append(entry)
    }
}

// ---------------------------------------------------------------------
// HTTP loopback plumbing
// ---------------------------------------------------------------------

pub fn client() -> reqwest::Client {
    reqwest::Client::builder()
        .timeout(Duration::from_secs(5))
        .build()
        .expect("reqwest client")
}

/// Bind an ephemeral `127.0.0.1` port, mount `wasl_operator::router(hub)`, and
/// spawn `axum::serve`. Binding happens synchronously before the task is
/// spawned so connections queue immediately (no readiness race).
pub async fn spawn_router(hub: OperatorHub) -> SocketAddr {
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

// ---------------------------------------------------------------------
// A tiny hand-rolled SSE reader over `reqwest::Response::bytes_stream()`.
// There is no SSE client crate in this workspace; this parses the same
// `event:`/`id:`/`data:`/comment framing the spec describes in prose.
// ---------------------------------------------------------------------

#[derive(Debug, Clone)]
pub struct SseEvent {
    pub event: String,
    pub id: Option<u64>,
    pub data: Value,
}

pub struct SseReader {
    stream: Pin<Box<dyn Stream<Item = Result<Bytes, reqwest::Error>> + Send>>,
    buf: String,
}

impl SseReader {
    pub fn new(resp: reqwest::Response) -> Self {
        SseReader {
            stream: Box::pin(resp.bytes_stream()),
            buf: String::new(),
        }
    }

    /// Read the next complete SSE event (blank-line-terminated block),
    /// silently skipping pure comment/keep-alive blocks (`: ka`), up to
    /// `timeout`. Returns `None` on timeout, stream error, or stream end.
    pub async fn next_event(&mut self, timeout: Duration) -> Option<SseEvent> {
        loop {
            if let Some(idx) = self.buf.find("\n\n") {
                let block: String = self.buf.drain(..idx + 2).collect();
                if let Some(ev) = parse_sse_block(&block) {
                    return Some(ev);
                }
                continue;
            }
            match tokio::time::timeout(timeout, self.stream.next()).await {
                Ok(Some(Ok(bytes))) => self.buf.push_str(&String::from_utf8_lossy(&bytes)),
                Ok(Some(Err(_))) | Ok(None) | Err(_) => return None,
            }
        }
    }

    /// Read up to `n` events (see `next_event`), stopping early on a `None`.
    pub async fn next_events(&mut self, n: usize, timeout: Duration) -> Vec<SseEvent> {
        let mut out = Vec::new();
        for _ in 0..n {
            match self.next_event(timeout).await {
                Some(ev) => out.push(ev),
                None => break,
            }
        }
        out
    }
}

fn parse_sse_block(block: &str) -> Option<SseEvent> {
    let mut event: Option<String> = None;
    let mut id: Option<u64> = None;
    let mut data_lines: VecDeque<String> = VecDeque::new();

    for raw_line in block.split('\n') {
        let line = raw_line.trim_end_matches('\r');
        if line.is_empty() {
            continue;
        }
        if line.starts_with(':') {
            // Comment / keep-alive line — ignored per the SSE spec.
            continue;
        }
        if let Some(rest) = line.strip_prefix("event:") {
            event = Some(rest.trim().to_string());
        } else if let Some(rest) = line.strip_prefix("id:") {
            id = rest.trim().parse::<u64>().ok();
        } else if let Some(rest) = line.strip_prefix("data:") {
            data_lines.push_back(rest.trim().to_string());
        }
    }

    if data_lines.is_empty() {
        return None; // pure comment/keep-alive block: keep reading.
    }
    let data_str: String = data_lines.into_iter().collect::<Vec<_>>().join("\n");
    let data: Value = serde_json::from_str(&data_str)
        .unwrap_or_else(|e| panic!("SSE data line was not JSON: {e}: {data_str:?}"));
    Some(SseEvent {
        event: event.unwrap_or_default(),
        id,
        data,
    })
}

// ---------------------------------------------------------------------
// Golden fixtures
// ---------------------------------------------------------------------

pub fn fixtures_dir() -> std::path::PathBuf {
    std::path::Path::new(env!("CARGO_MANIFEST_DIR")).join("fixtures/operator-v1")
}

/// Load and parse one `fixtures/operator-v1/<name>` file as a `serde_json::Value`.
pub fn load_fixture(name: &str) -> Value {
    let path = fixtures_dir().join(name);
    let raw =
        std::fs::read_to_string(&path).unwrap_or_else(|e| panic!("read fixture {path:?}: {e}"));
    serde_json::from_str(&raw).unwrap_or_else(|e| panic!("parse fixture {path:?} as JSON: {e}"))
}
