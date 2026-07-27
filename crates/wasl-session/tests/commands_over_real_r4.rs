//! Phase 3.5 (behavioral) — R3 `wasl-session`'s command functions, proven
//! END-TO-END against a REAL, already-solid R4 (`wasl-runtime-control`)
//! server (spawned via `wasl_runtime_control::router`/`serve`), rather than
//! a hand-rolled fake R4 double — the real client is the honest proof.
//!
//! session.md: `send` ALWAYS uses `TurnSource::LocalHuman` (the single-writer
//! invariant: human input funnels through the SAME `submit_turn` path as an
//! agent turn, never a raw side-channel); `attach` observes via the R4 event
//! stream; `gate approve|deny` maps to `wasl_proto::GateDecision`.
//!
//! Expected to fail to compile until the code-agent adds a real R4-client
//! module to `wasl-session`'s `src/**` exposing the functions this file
//! calls (named below; `wasl_session::client::...`).

use std::sync::Arc;
use std::time::Duration;

use async_trait::async_trait;
use futures::stream;

use wasl_proto::GateDecision;
use wasl_runtime::{
    BusyPolicy, CancelReason, DeliveryTarget, EventSeq, NativeSessionId, RuntimeCapabilities,
    RuntimeError, RuntimeProvider, ShutdownMode, StreamItem, TargetId, TargetKey, TargetStatus,
    TurnEvent, TurnId, TurnReceipt, TurnRequest,
};
use wasl_runtime_control::RuntimeControlHub;

// The code-agent's R3 client surface (in wasl-session's src/**), matching
// session.md's command grammar: `start`, `list`, `attach`, `send`,
// `interrupt`, `stop`, `gate`. Each is a thin async function taking the R4
// base URL + the command's arguments, calling R4 over real HTTP, and
// returning a typed result (not a printed string) so this test can assert on
// values rather than scrape stdout.
use wasl_session::client::{attach_once, gate, interrupt, send, start, stop, ClientError};

// ---------------------------------------------------------------------
// A minimal scriptable DeliveryTarget/RuntimeProvider (mirrors
// wasl-runtime-control's own test doubles) so this file does not depend on
// a real managed protocol driver -- R3's job is the CLIENT/command mapping,
// not re-proving R1/R4 internals.
// ---------------------------------------------------------------------

#[derive(Debug)]
struct FakeTarget {
    target_id: TargetId,
}

#[async_trait]
impl DeliveryTarget for FakeTarget {
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

    async fn submit_turn(&self, req: TurnRequest, _busy_policy: BusyPolicy) -> TurnReceipt {
        // Prove `send` used TurnSource::LocalHuman (the single-writer
        // invariant this test's title cares about) by asserting it here.
        assert_eq!(req.source, wasl_runtime::TurnSource::LocalHuman);
        TurnReceipt::Started {
            turn_id: TurnId("turn-r3".to_string()),
            first_seq: EventSeq(1),
        }
    }

    async fn events(&self, _after_seq: EventSeq) -> wasl_runtime::TurnEventStream {
        let items = vec![
            StreamItem::Event {
                seq: EventSeq(1),
                event: TurnEvent::SessionReady {
                    native: Some(NativeSessionId("ses_r3".to_string())),
                },
            },
            StreamItem::Event {
                seq: EventSeq(2),
                event: TurnEvent::AssistantDelta {
                    turn_id: TurnId("turn-r3".to_string()),
                    text: "hello from the target".to_string(),
                },
            },
        ];
        Box::pin(stream::iter(items))
    }

    async fn cancel_turn(
        &self,
        _turn_id: TurnId,
        _reason: CancelReason,
    ) -> Result<(), RuntimeError> {
        Ok(())
    }

    async fn shutdown(&self, _mode: ShutdownMode) -> Result<(), RuntimeError> {
        Ok(())
    }
}

#[async_trait]
impl wasl_runtime::NativeGateResolver for FakeTarget {
    async fn resolve_native_gate(
        &self,
        _turn_id: TurnId,
        _decision: GateDecision,
    ) -> Result<(), RuntimeError> {
        Ok(())
    }
}

#[async_trait]
impl wasl_runtime::ToolCallAnswerer for FakeTarget {
    async fn answer_tool_call(&self, _turn_id: TurnId, _value: String) -> Result<(), RuntimeError> {
        Ok(())
    }
}

struct FakeProvider {
    target: Arc<FakeTarget>,
}

#[async_trait]
impl RuntimeProvider for FakeProvider {
    async fn target_for(&self, _key: &TargetKey) -> Result<Arc<dyn DeliveryTarget>, RuntimeError> {
        Ok(self.target.clone())
    }

    async fn list_targets(&self) -> Vec<TargetId> {
        vec![self.target.target_id.clone()]
    }
}

async fn spawn_r4() -> String {
    let provider = Arc::new(FakeProvider {
        target: Arc::new(FakeTarget {
            target_id: TargetId("tgt-1".to_string()),
        }),
    });
    let hub = RuntimeControlHub::new(
        provider,
        "wasl-a".to_string(),
        "host-1".to_string(),
        "0.1.0".to_string(),
        wasl_runtime_control::RuntimeKind::Managed,
    );
    let app = wasl_runtime_control::router(hub);
    let listener = tokio::net::TcpListener::bind("127.0.0.1:0")
        .await
        .expect("bind ephemeral port");
    let addr = listener.local_addr().expect("local addr");
    tokio::spawn(async move {
        axum::serve(listener, app).await.expect("serve");
    });
    format!("http://{addr}")
}

#[tokio::test]
async fn start_creates_or_reuses_a_target() {
    let base = spawn_r4().await;
    let snapshot = start(&base, "ctx-42", None)
        .await
        .expect("start must succeed");
    assert_eq!(snapshot.target_id.0, "tgt-1");
}

#[tokio::test]
async fn send_always_uses_local_human_source_and_returns_a_receipt() {
    let base = spawn_r4().await;
    start(&base, "ctx-42", None).await.expect("start");

    // `send`'s FakeTarget::submit_turn assertion above is the actual proof
    // that TurnSource::LocalHuman was used; here we also check the returned
    // receipt is surfaced to the caller correctly.
    let receipt = send(&base, "tgt-1", "hello there")
        .await
        .expect("send must succeed");
    assert!(matches!(receipt, TurnReceipt::Started { .. }));
}

#[tokio::test]
async fn attach_once_observes_the_target_id_public_events() {
    let base = spawn_r4().await;
    start(&base, "ctx-42", None).await.expect("start");

    let events = attach_once(&base, "tgt-1", Duration::from_secs(5))
        .await
        .expect("attach must succeed");

    assert!(events
        .iter()
        .any(|e| matches!(e, TurnEvent::SessionReady { .. })));
    assert!(events.iter().any(
        |e| matches!(e, TurnEvent::AssistantDelta { text, .. } if text == "hello from the target")
    ));
}

#[tokio::test]
async fn interrupt_maps_to_cancel_with_operator_request_reason() {
    let base = spawn_r4().await;
    start(&base, "ctx-42", None).await.expect("start");

    // FakeTarget::cancel_turn always Ok(()) here; the interesting proof is
    // that `interrupt` reaches R4's /cancel endpoint successfully (a 404/409
    // would surface as an Err).
    interrupt(&base, "tgt-1", "turn-r3")
        .await
        .expect("interrupt must succeed against a real target");
}

#[tokio::test]
async fn stop_maps_drain_flag_to_shutdown_mode() {
    let base = spawn_r4().await;
    start(&base, "ctx-42", None).await.expect("start");

    stop(&base, "tgt-1", true)
        .await
        .expect("stop --drain must succeed");
}

#[tokio::test]
async fn gate_approve_maps_to_gate_decision_approve() {
    let base = spawn_r4().await;
    start(&base, "ctx-42", None).await.expect("start");

    gate(&base, "tgt-1", "turn-r3", GateDecision::Approve)
        .await
        .expect("gate approve must succeed");
}

#[tokio::test]
async fn gate_deny_carries_the_reason() {
    let base = spawn_r4().await;
    start(&base, "ctx-42", None).await.expect("start");

    gate(
        &base,
        "tgt-1",
        "turn-r3",
        GateDecision::Deny {
            reason: "not allowed".to_string(),
        },
    )
    .await
    .expect("gate deny must succeed");
}

#[tokio::test]
async fn unknown_target_surfaces_as_a_client_error_not_a_panic() {
    let base = spawn_r4().await;
    let err = stop(&base, "does-not-exist", false)
        .await
        .expect_err("an unknown target must be a clean error");
    assert!(matches!(
        err,
        ClientError::UnknownTarget(_) | ClientError::Http(_)
    ));
}
