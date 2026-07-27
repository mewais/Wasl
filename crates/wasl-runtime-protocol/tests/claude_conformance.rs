#![cfg(feature = "live-cli")]
//! Phase 4 — the shared capability-gated conformance suite
//! (`docs/specs/runtime-protocol.md` §5, `docs/design/live-runtime-impl-plan.md`
//! Phase 3 "Land the capability-gated conformance suite here (Phases 4/5 depend
//! on it)"), run for REAL against a real `claude` CLI process via
//! `ClaudeStreamProtocol` + `WaslManagedRuntimeProvider`.
//!
//! `conformance()` below (from `sample_request` through the function body) is
//! copied BYTE-IDENTICAL from `crates/wasl-runtime/tests/conformance_suite.rs`
//! (same logic, same assertions, same capability-gating) — Rust integration
//! tests are separate per-crate binaries with no cross-crate import mechanism,
//! so a verbatim copy, clearly marked as such, is how "reused UNCHANGED against
//! real managed drivers in later phases" (that file's own doc comment) is
//! realized across the R1/R2 crate boundary. Only the imports/target
//! construction differ (a real claude-backed target instead of `FakeAdapter`-
//! backed `EphemeralTarget`); the assertion body is untouched.
//!
//! Run: `cargo test -p wasl-runtime-protocol --features live-cli
//! claude_conformance -- --test-threads=1`, with `WASL_LIVE_NONCE` set to a
//! fresh value. Costs real `claude` CLI model calls (exactly the 2 turns
//! `conformance()` itself issues for `persistent_conversation`).

use std::sync::{Arc, Mutex};

use futures::StreamExt;

use wasl_adapter::{Delivery, DeliveryKind};
use wasl_audit::{AuditEntry, AuditError, Sink};
use wasl_runtime::{
    BusyPolicy, Causation, DeliveryTarget, EventSeq, ManagedConfig, MonotonicClock, StreamItem,
    TargetKey, TurnEvent, TurnId, TurnReceipt, TurnRequest, TurnSource, WaslManagedRuntimeProvider,
};
use wasl_runtime_protocol::ClaudeStreamProtocol;

/// A `Sink` that discards every entry — the live conformance run needs no
/// audit inspection, only a valid seam to satisfy `with_seams`.
struct NoopSink;

impl Sink for NoopSink {
    fn append(&mut self, _entry: &AuditEntry) -> Result<(), AuditError> {
        Ok(())
    }
}

// =====================================================================
// Verbatim copy of crates/wasl-runtime/tests/conformance_suite.rs's
// sample_request / drain_events / completed_reply_for / conformance().
// DO NOT edit the logic below independently of that file.
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
// The live test: build a REAL claude-backed managed target and run the
// verbatim conformance() against it.
// =====================================================================

#[tokio::test]
async fn claude_stream_protocol_passes_conformance() {
    let nonce = std::env::var("WASL_LIVE_NONCE")
        .expect("WASL_LIVE_NONCE must be set (to a fresh value) to run claude_conformance.rs");
    let context_id = format!("wasl-runtime-protocol-claude-conformance-{nonce}");

    let provider: WaslManagedRuntimeProvider<ClaudeStreamProtocol> =
        WaslManagedRuntimeProvider::with_seams(
            ClaudeStreamProtocol::new("claude"),
            ManagedConfig::default(),
            Arc::new(MonotonicClock),
            Arc::new(Mutex::new(NoopSink)),
        );

    let target = provider
        .target_for(&TargetKey(context_id))
        .await
        .expect("target_for must succeed against the real claude CLI");

    let caps = target.capabilities();
    assert!(
        caps.persistent_conversation,
        "test premise: ClaudeStreamProtocol must claim persistent_conversation \
         for the conformance body above to exercise anything"
    );

    conformance(&*target).await;
}
