//! Shared loopback-test plumbing for `wasl-runtime-control`, mirroring
//! `wasl-operator/tests/common/mod.rs`'s `spawn_router`/`SseReader` pattern
//! (the explicit precedent `docs/specs/runtime-control-api.md` names).

use std::collections::VecDeque;
use std::pin::Pin;
use std::sync::atomic::{AtomicUsize, Ordering};
use std::sync::Arc;
use std::time::Duration;

use async_trait::async_trait;
use bytes::Bytes;
use futures::{stream, Stream, StreamExt};
use serde_json::Value;
use tokio::sync::Mutex as AsyncMutex;

use wasl_runtime::{
    BusyPolicy, CancelReason, DeliveryTarget, EventSeq, NativeSessionId, RuntimeCapabilities,
    RuntimeError, RuntimeProvider, ShutdownMode, SpawnContext, StreamItem, TargetId, TargetKey,
    TargetStatus, TurnEvent, TurnId, TurnReceipt, TurnRequest,
};
use wasl_runtime_control::{RuntimeControlHub, RuntimeKind};

pub fn client() -> reqwest::Client {
    reqwest::Client::builder()
        .timeout(Duration::from_secs(5))
        .build()
        .expect("reqwest client")
}

/// Bind an ephemeral `127.0.0.1` port, mount `wasl_runtime_control::router(hub)`,
/// and spawn `axum::serve`. Binding happens synchronously before the task is
/// spawned so connections queue immediately (no readiness race) -- the exact
/// `wasl-operator::tests::common::spawn_router` idiom.
pub async fn spawn_router(hub: RuntimeControlHub) -> std::net::SocketAddr {
    // D3: the peer-CRUD tests drive `/v1/peers` over this same fresh registry.
    let peers = std::sync::Arc::new(std::sync::RwLock::new(
        wasl_registry::PeerRegistry::new(),
    ));
    let app = wasl_runtime_control::router(hub, peers);
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
// A tiny hand-rolled SSE reader over `reqwest::Response::bytes_stream()`
// (there is no SSE client crate in this workspace) -- byte-for-byte the
// same `event:`/`id:`/`data:`/comment framing `wasl-operator`'s reader
// parses, adapted for this crate's `id:` being a per-target EventSeq.
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
        return None;
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
// A scriptable fake DeliveryTarget + RuntimeProvider for loopback tests:
// a bounded event queue a test can push into, so the SSE stream can be
// driven deterministically (no sleeps).
// ---------------------------------------------------------------------

#[derive(Debug)]
pub struct ScriptedTarget {
    target_id: TargetId,
    events: AsyncMutex<VecDeque<StreamItem>>,
    submitted: AtomicUsize,
}

impl ScriptedTarget {
    pub fn new(target_id: &str, items: Vec<StreamItem>) -> Arc<Self> {
        Arc::new(Self {
            target_id: TargetId(target_id.to_string()),
            events: AsyncMutex::new(items.into()),
            submitted: AtomicUsize::new(0),
        })
    }

    #[allow(dead_code)]
    pub fn submitted_count(&self) -> usize {
        self.submitted.load(Ordering::SeqCst)
    }
}

#[async_trait]
impl DeliveryTarget for ScriptedTarget {
    fn target_id(&self) -> &TargetId {
        &self.target_id
    }

    fn capabilities(&self) -> RuntimeCapabilities {
        RuntimeCapabilities {
            persistent_process: true,
            persistent_conversation: true,
            structured_turns: true,
            streaming_output: false,
            attachable: true,
            interruptible: false,
            structured_provenance: true,
            resumable: true,
        }
    }

    async fn status(&self) -> TargetStatus {
        TargetStatus::Idle {
            since: "2026-07-02T12:00:00.000Z".to_string(),
        }
    }

    async fn submit_turn(&self, _req: TurnRequest, _busy_policy: BusyPolicy) -> TurnReceipt {
        self.submitted.fetch_add(1, Ordering::SeqCst);
        TurnReceipt::Started {
            turn_id: TurnId("turn-loopback".to_string()),
            first_seq: EventSeq(1),
        }
    }

    async fn events(&self, after_seq: EventSeq) -> wasl_runtime::TurnEventStream {
        let items: Vec<StreamItem> = self
            .events
            .lock()
            .await
            .iter()
            .filter(|item| match item {
                StreamItem::Event { seq, .. } => seq.0 > after_seq.0,
                StreamItem::Truncated { .. } => true,
            })
            .cloned()
            .collect();
        Box::pin(stream::iter(items))
    }

    async fn cancel_turn(
        &self,
        _turn_id: TurnId,
        _reason: CancelReason,
    ) -> Result<(), RuntimeError> {
        Err(RuntimeError::Unsupported {
            operation: "cancel_turn".to_string(),
        })
    }

    async fn shutdown(&self, _mode: ShutdownMode) -> Result<(), RuntimeError> {
        Ok(())
    }
}

#[async_trait]
impl wasl_runtime::NativeGateResolver for ScriptedTarget {
    async fn resolve_native_gate(
        &self,
        _turn_id: TurnId,
        _decision: wasl_proto::GateDecision,
    ) -> Result<(), RuntimeError> {
        Err(RuntimeError::GateUnresolvable {
            detail: "no pending gate".to_string(),
        })
    }
}

#[async_trait]
impl wasl_runtime::ToolCallAnswerer for ScriptedTarget {
    async fn answer_tool_call(&self, _turn_id: TurnId, _value: String) -> Result<(), RuntimeError> {
        Ok(())
    }
}

pub struct ScriptedProvider {
    pub target: Arc<ScriptedTarget>,
    pub refuse_admission: bool,
    /// Records the last [`SpawnContext`] received by `target_for_with_context`.
    /// `None` means no context call has occurred yet.
    pub last_context: Arc<AsyncMutex<Option<SpawnContext>>>,
}

impl ScriptedProvider {
    /// Create a provider without context recording (backward-compat with existing tests).
    pub fn new(target: Arc<ScriptedTarget>, refuse_admission: bool) -> Arc<Self> {
        Arc::new(Self {
            target,
            refuse_admission,
            last_context: Arc::new(AsyncMutex::new(None)),
        })
    }

    /// Convenience: snapshot the last recorded context.
    pub async fn recorded_context(&self) -> Option<SpawnContext> {
        self.last_context.lock().await.clone()
    }
}

#[async_trait]
impl RuntimeProvider for ScriptedProvider {
    async fn target_for(&self, _key: &TargetKey) -> Result<Arc<dyn DeliveryTarget>, RuntimeError> {
        if self.refuse_admission {
            return Err(RuntimeError::AdmissionRefused {
                detail: "per-sender cap reached".to_string(),
            });
        }
        Ok(self.target.clone())
    }

    /// Override: records the `SpawnContext` before delegating to `target_for`.
    async fn target_for_with_context(
        &self,
        key: &TargetKey,
        context: SpawnContext,
    ) -> Result<Arc<dyn DeliveryTarget>, RuntimeError> {
        *self.last_context.lock().await = Some(context);
        self.target_for(key).await
    }

    async fn list_targets(&self) -> Vec<TargetId> {
        vec![self.target.target_id().clone()]
    }
}

pub fn session_ready_event(seq: u64) -> StreamItem {
    StreamItem::Event {
        seq: EventSeq(seq),
        event: TurnEvent::SessionReady {
            native: Some(NativeSessionId("ses_loopback".to_string())),
        },
    }
}

pub fn assistant_delta_event(seq: u64, text: &str) -> StreamItem {
    StreamItem::Event {
        seq: EventSeq(seq),
        event: TurnEvent::AssistantDelta {
            turn_id: TurnId("turn-loopback".to_string()),
            text: text.to_string(),
        },
    }
}

pub fn make_hub(provider: Arc<dyn RuntimeProvider>) -> RuntimeControlHub {
    RuntimeControlHub::new(
        provider,
        "wasl-a".to_string(),
        "host-1".to_string(),
        "0.1.0".to_string(),
        RuntimeKind::Managed,
    )
}

/// Helper: build a `ScriptedProvider` (non-refusing) backed by `target` and
/// return the Arc so tests can call `recorded_context()` on it later.
pub fn make_recording_provider(
    target: Arc<ScriptedTarget>,
) -> Arc<ScriptedProvider> {
    ScriptedProvider::new(target, false)
}
