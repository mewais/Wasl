#![cfg(feature = "live-cli")]
//! Phase 5 — THE central genericity proof
//! (`docs/design/live-runtime-impl-plan.md` Phase 5: "conformance runs
//! UNCHANGED across ephemeral+claude+opencode; any per-protocol branch =
//! leak = FAIL"). This is the SAME `conformance()` function, copied
//! BYTE-IDENTICAL a second time (first copied for Phase 4's
//! `claude_conformance.rs`, from `crates/wasl-runtime/tests/
//! conformance_suite.rs`) -- now run against a REAL `opencode acp`-backed
//! target. If this passes with the EXACT SAME assertion logic that already
//! passed against `EphemeralTarget` (Phase 3) and `ClaudeStreamProtocol`
//! (Phase 4), the generic R1 actor/core is proven driver-agnostic across
//! THREE independently-different wire protocols (headless per-turn adapter,
//! claude stream-json, opencode ACP JSON-RPC) with ZERO new `ProtocolEvent`/
//! `TargetStatus` variants needed anywhere.
//!
//! Run: `cargo test -p wasl-runtime-protocol --features live-cli
//! opencode_acp_conformance -- --test-threads=1`, with `WASL_LIVE_NONCE` set
//! to a fresh value and an isolated opencode config in the environment
//! (`OPENCODE_DISABLE_PROJECT_CONFIG=1`, `OPENCODE_DISABLE_AUTOCOMPACT=1`,
//! `XDG_CONFIG_HOME`/`XDG_DATA_HOME` pointed at a big-context-model config,
//! per `docs/specs/runtime-protocol.md` §"`--features live-cli` discipline" --
//! this is an orchestrator/deployment responsibility, exactly like
//! `wasl-adapter-opencode/tests/live.rs`'s own documented precedent, NOT
//! something this test file sets itself). Costs real `opencode` CLI model
//! calls (exactly the 2 turns `conformance()` itself issues for
//! `persistent_conversation`).

use std::sync::{Arc, Mutex};

use futures::StreamExt;

use wasl_adapter::{Delivery, DeliveryKind};
use wasl_audit::{AuditEntry, AuditError, Sink};
use wasl_runtime::{
    BusyPolicy, Causation, DeliveryTarget, EventSeq, ManagedConfig, MonotonicClock, StreamItem,
    TargetKey, TurnEvent, TurnId, TurnReceipt, TurnRequest, TurnSource, WaslManagedRuntimeProvider,
};
use wasl_runtime_protocol::OpenCodeAcpProtocol;

struct NoopSink;

impl Sink for NoopSink {
    fn append(&mut self, _entry: &AuditEntry) -> Result<(), AuditError> {
        Ok(())
    }
}

// =====================================================================
// Verbatim copy of crates/wasl-runtime/tests/conformance_suite.rs's
// sample_request / drain_events / completed_reply_for / conformance() --
// IDENTICAL to the copy in claude_conformance.rs. DO NOT edit the logic
// below independently of those two files.
// =====================================================================

fn sample_request(context_id: String, text: String) -> TurnRequest {
    TurnRequest {
        delivery: Delivery {
            context_id,
            from: "peer-a".to_string(),
            kind: DeliveryKind::Consult,
            text,
            data: vec![],
        },
        source: TurnSource::Agent {
            from: wasl_proto::AgentId("peer-a".to_string()),
        },
        idempotency_key: None,
        causation: Causation {
            root_request: wasl_proto::RequestId("r1".to_string()),
            parent_request: None,
            path: vec![],
            hop: 0,
        },
    }
}

async fn drain_events(stream: wasl_runtime::TurnEventStream) -> Vec<TurnEvent> {
    stream
        .filter_map(|item| async move {
            match item {
                StreamItem::Event { event, .. } => Some(event),
                StreamItem::Truncated { .. } => None,
            }
        })
        .collect()
        .await
}

fn completed_reply_for(events: &[TurnEvent], turn_id: &TurnId) -> Option<String> {
    events.iter().find_map(|e| match e {
        TurnEvent::TurnCompleted {
            turn_id: t, reply, ..
        } if t == turn_id => Some(reply.clone()),
        _ => None,
    })
}

async fn conformance(target: &dyn DeliveryTarget) {
    let caps = target.capabilities();
    let target_key = target.target_id().0.clone();

    if caps.persistent_conversation {
        let remember_req = sample_request(target_key.clone(), "remember VIOLET-7F29".to_string());
        let remember_receipt = target.submit_turn(remember_req, BusyPolicy::Reject).await;
        let remember_turn_id = match remember_receipt {
            TurnReceipt::Started { turn_id, .. } => turn_id,
            other => panic!("expected Started for 'remember' turn, got {other:?}"),
        };
        let remember_events = drain_events(target.events(EventSeq(0)).await).await;
        let remember_reply = completed_reply_for(&remember_events, &remember_turn_id)
            .expect("no TurnCompleted correlated to the 'remember' turn_id");
        assert!(
            remember_reply.contains("VIOLET-7F29"),
            "expected the 'remember' turn's OWN reply to echo the token, got {remember_reply:?}"
        );

        let recall_req = sample_request(target_key.clone(), "recall".to_string());
        let recall_receipt = target.submit_turn(recall_req, BusyPolicy::Reject).await;
        let recall_turn_id = match recall_receipt {
            TurnReceipt::Started { turn_id, .. } => turn_id,
            other => panic!("expected Started for 'recall' turn, got {other:?}"),
        };
        let recall_events = drain_events(target.events(EventSeq(0)).await).await;
        let recall_reply = completed_reply_for(&recall_events, &recall_turn_id).expect(
            "no TurnCompleted correlated to the 'recall' turn_id (NOT the 'remember' turn's)",
        );
        assert!(
            recall_reply.contains("VIOLET-7F29"),
            "persistent_conversation claimed but 'recall' turn's OWN reply {recall_reply:?} did not echo the token"
        );
    }

    if caps.interruptible {
        let req = sample_request(target_key.clone(), "a cancellable turn".to_string());
        let receipt = target.submit_turn(req, BusyPolicy::Reject).await;
        let turn_id = match receipt {
            TurnReceipt::Started { turn_id, .. } => turn_id,
            other => panic!("expected Started, got {other:?}"),
        };
        let result = target
            .cancel_turn(turn_id, wasl_runtime::CancelReason::OperatorRequest)
            .await;
        assert!(
            result.is_ok(),
            "interruptible claimed but cancel_turn errored: {result:?}"
        );
    }
}

// =====================================================================
// The live test: build a REAL opencode-acp-backed managed target and run
// the verbatim conformance() against it -- the genericity proof.
// =====================================================================

#[tokio::test]
async fn opencode_acp_protocol_passes_conformance() {
    let nonce = std::env::var("WASL_LIVE_NONCE").expect(
        "WASL_LIVE_NONCE must be set (to a fresh value) to run opencode_acp_conformance.rs",
    );
    let context_id = format!("wasl-runtime-protocol-opencode-conformance-{nonce}");

    let provider: WaslManagedRuntimeProvider<OpenCodeAcpProtocol> =
        WaslManagedRuntimeProvider::with_seams(
            OpenCodeAcpProtocol::new("opencode"),
            ManagedConfig::default(),
            Arc::new(MonotonicClock),
            Arc::new(Mutex::new(NoopSink)),
        );

    let target = provider
        .target_for(&TargetKey(context_id))
        .await
        .expect("target_for must succeed against the real opencode acp CLI");

    let caps = target.capabilities();
    assert!(
        caps.persistent_conversation,
        "test premise: OpenCodeAcpProtocol must claim persistent_conversation \
         for the conformance body above to exercise anything"
    );
    assert!(
        caps.streaming_output,
        "test premise: unlike claude, opencode-acp claims REAL streaming -- \
         this run is also the live proof that a streaming-capable driver \
         satisfies the SAME conformance() body with no special-casing"
    );

    conformance(&*target).await;
}
