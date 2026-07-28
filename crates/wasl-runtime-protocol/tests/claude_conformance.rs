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
//! **Post-landing fix (found by actually running this against a real driver
//! for the first time):** the event-draining helper was originally
//! `drain_events` (`stream.collect()`), which only ever worked because
//! Phase 3's `EphemeralTarget::events()` is a finite, always-terminating
//! stream. A REAL managed target's `events()` is a genuinely LIVE tail that
//! never ends while the target is alive (§5.1/the journal's broadcast-live-tail
//! design) — `.collect()` against it hangs FOREVER. Replaced with
//! `drain_until` (bounded, stops once the turn(s) of interest have a
//! `TurnCompleted`/`TurnFailed`), fixed FIRST in the original
//! `conformance_suite.rs` then mirrored here so the two stay identical.
//!
//! Run: `cargo test -p wasl-runtime-protocol --features live-cli
//! claude_conformance -- --test-threads=1`, with `WASL_LIVE_NONCE` set to a
//! fresh value. Costs real `claude` CLI model calls (exactly the 2 turns
//! `conformance()` itself issues for `persistent_conversation`).

use std::sync::{Arc, Mutex};
use std::time::Duration;

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

/// Drain `stream` until `done(&events_so_far)` is true, or `bound` elapses (a
/// hang-guard, never the normal exit path). A REAL managed target's
/// `events()` is a genuinely LIVE tail that never ends while the target is
/// alive (unlike Phase 3's `EphemeralTarget`, whose stream is finite) — a
/// naive `.collect()` hangs FOREVER against it. See the identical fix +
/// rationale in `crates/wasl-runtime/tests/conformance_suite.rs::drain_until`
/// (the ORIGINAL this file copies), fixed there first then mirrored here.
async fn drain_until(
    mut stream: wasl_runtime::TurnEventStream,
    bound: Duration,
    mut done: impl FnMut(&[TurnEvent]) -> bool,
) -> Vec<TurnEvent> {
    let mut out = Vec::new();
    let deadline = tokio::time::Instant::now() + bound;
    loop {
        if done(&out) {
            break;
        }
        let remaining = deadline.saturating_duration_since(tokio::time::Instant::now());
        if remaining.is_zero() {
            break;
        }
        match tokio::time::timeout(remaining, stream.next()).await {
            Ok(Some(StreamItem::Event { event, .. })) => out.push(event),
            Ok(Some(StreamItem::Truncated { .. })) => continue,
            Ok(None) => break,
            Err(_) => break,
        }
    }
    out
}

fn completed_reply_for(events: &[TurnEvent], turn_id: &TurnId) -> Option<String> {
    events.iter().find_map(|e| match e {
        TurnEvent::TurnCompleted {
            turn_id: t, reply, ..
        } if t == turn_id => Some(reply.clone()),
        _ => None,
    })
}

/// Diagnostic summary of what `drain_until` actually saw before giving up
/// (either the predicate was satisfied, or the hang-guard bound elapsed) --
/// so a failure tells us WHERE the pipeline got stuck (never even got
/// SessionReady? got deltas but no terminal? something else entirely?)
/// instead of just "no TurnCompleted correlated."
fn describe_events(events: &[TurnEvent]) -> String {
    if events.is_empty() {
        return "<no events observed at all>".to_string();
    }
    events
        .iter()
        .map(|e| match e {
            TurnEvent::SessionReady { native } => format!("SessionReady{{native:{native:?}}}"),
            TurnEvent::TurnAccepted { turn_id, .. } => format!("TurnAccepted{{{turn_id:?}}}"),
            TurnEvent::AssistantDelta { turn_id, text } => {
                format!("AssistantDelta{{{turn_id:?}, {} chars}}", text.len())
            }
            TurnEvent::TurnCompleted { turn_id, .. } => format!("TurnCompleted{{{turn_id:?}}}"),
            TurnEvent::TurnFailed { turn_id, error, .. } => {
                format!("TurnFailed{{{turn_id:?}, {error}}}")
            }
            TurnEvent::ProcessExited { code } => format!("ProcessExited{{code:{code:?}}}"),
            other => format!("{other:?}"),
        })
        .collect::<Vec<_>>()
        .join(", ")
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
        let remember_events = drain_until(
            target.events(EventSeq(0)).await,
            Duration::from_secs(240),
            |evs| completed_reply_for(evs, &remember_turn_id).is_some(),
        )
        .await;
        let remember_reply = completed_reply_for(&remember_events, &remember_turn_id)
            .unwrap_or_else(|| {
                panic!(
                    "no TurnCompleted correlated to the 'remember' turn_id ({:?}) within the \
                     240s hang-guard bound. Events actually observed: {}",
                    remember_turn_id,
                    describe_events(&remember_events)
                )
            });
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
        let recall_events = drain_until(
            target.events(EventSeq(0)).await,
            Duration::from_secs(240),
            |evs| completed_reply_for(evs, &recall_turn_id).is_some(),
        )
        .await;
        let recall_reply =
            completed_reply_for(&recall_events, &recall_turn_id).unwrap_or_else(|| {
                panic!(
                    "no TurnCompleted correlated to the 'recall' turn_id ({:?}) within the \
                     240s hang-guard bound. Events actually observed: {}",
                    recall_turn_id,
                    describe_events(&recall_events)
                )
            });
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
