//! Phase-2A coverage-closing suite — native-gate Deny + resolve-error arms
//! (§9), Tool* events (§5/§7.4), TurnFailed-from-transport certainty (§7.4/§8.1),
//! a LocalHuman-sourced turn's composition (§8.2/§4.1), and the over-
//! `delta_max_bytes` AssistantDelta truncation (§11 `cap_delta`).
//!
//! Behavioral; drives spec-required, IMPLEMENTED-but-previously-undriven actor
//! paths through the `support::ScriptedProtocol` scripted transport.
//!
//! Spec citations (docs/specs/runtime.md):
//!   * native-gate resolution — §9 (lines 709-725): resolve_native_gate; Deny
//!     answers the CLI's prompt over the transport, emits NativeGateResolved;
//!     resolving with no pending gate / a wrong turn_id ⇒ the defined error.
//!   * Tool* — §5 (lines 449-450), §7.4 (lines 615-616): ToolStarted ⇒
//!     Busy{RunningTool}+ToolStarted event; ToolFinished ⇒ Busy{Generating}+
//!     ToolFinished event.
//!   * TurnFailed-from-transport ⇒ certainty Completed (a definite in-session
//!     failure is a known outcome) — §7.4 (line 619), §8.1 (line 648).
//!   * LocalHuman composition — §4.1 compose arm, §8.2; source funnels through
//!     the same single actor (§Single-writer invariant lines 67-81).
//!   * delta_max_bytes — §11 (line 780): an over-cap AssistantDelta is truncated.

mod support;

use std::sync::Arc;

use wasl_proto::GateDecision;
use wasl_runtime::{
    BusyPhase, BusyPolicy, DeliveryCertainty, DeliveryTarget, EventSeq, IdempotencyKey,
    NativeGateDetail, ProtocolEvent, RuntimeError, TargetStatus, TurnEvent, TurnId, TurnReceipt,
    TurnRequest, TurnSource,
};

use support::{
    consult, delivery, drain_items, events_of, key, provider, resolve_gate, root_causation,
    sink_arc, tight_config, ProtocolScript, RecordingSink, ScriptedProtocol, TestClock,
    TransportCall,
};

use wasl_adapter::DeliveryKind;

fn busy_forever_script() -> ProtocolScript {
    ProtocolScript::new()
        .ready(Some("ses-1"))
        .emit(ProtocolEvent::TurnAccepted { native_turn: None })
}

// =====================================================================
// Item 3a — resolve_native_gate(Deny): the actor answers "deny: <reason>" over
// the single-writer transport and emits NativeGateResolved{Deny}. §9.
// =====================================================================

#[tokio::test]
async fn resolving_native_gate_deny_emits_resolved_and_answers_transport() {
    let proto = ScriptedProtocol::new();
    let handle = proto.install(
        "ctx-gate-deny",
        ProtocolScript::new()
            .ready(Some("ses-1"))
            .emit(ProtocolEvent::TurnAccepted { native_turn: None })
            .emit(ProtocolEvent::NativeGateRequested {
                detail: NativeGateDetail {
                    summary: "delete file?".into(),
                    preview: "rm x".into(),
                },
            }),
    );
    let clock = TestClock::new();
    let sink = sink_arc(RecordingSink::default());
    // A larger `journal_capacity` than `tight_config()`'s default (8): this
    // test re-subscribes from `EventSeq(0)` TWICE (a full replay), and the
    // total event count (SessionReady, an optional intervening
    // `StatusChanged{Idle}`/`TurnQueued` per §7.1 row 582's `Starting`-phase
    // handling, TurnAccepted, 2x StatusChanged, NativeGateRequested,
    // StatusChanged{AwaitingGate}, NativeGateResolved, StatusChanged) can
    // exceed 8 — this test's actual intent (denying a gate) has nothing to do
    // with exercising journal eviction/truncation, so give it enough room
    // rather than pinning to the tight, truncation-prone default.
    let config = wasl_runtime::ManagedConfig {
        journal_capacity: 32,
        ..tight_config()
    };
    let prov = provider(proto, config, clock, sink);
    let target: Arc<dyn DeliveryTarget> = prov
        .target_for(&key("ctx-gate-deny"))
        .await
        .expect("target");

    let started = target
        .submit_turn(consult("ctx-gate-deny", "go"), BusyPolicy::Reject)
        .await;
    let turn_id = match started {
        TurnReceipt::Started { turn_id, .. } => turn_id,
        other => panic!("expected Started, got {other:?}"),
    };

    // Let the gate surface.
    let s0 = target.events(EventSeq(0)).await;
    let _ = drain_items(s0, 8).await;

    // Deny the gate through the single-writer actor (§9 option (i)).
    let decision = GateDecision::Deny {
        reason: "not allowed".into(),
    };
    let res = resolve_gate(&target, turn_id.clone(), decision.clone()).await;
    assert_eq!(res, Ok(()), "resolving a gate with Deny succeeds (§9)");

    let s1 = target.events(EventSeq(0)).await;
    let items = drain_items(s1, 16).await;
    let evs = events_of(&items);
    assert!(
        evs.iter().any(|e| matches!(
            e,
            TurnEvent::NativeGateResolved { turn_id: t, decision: GateDecision::Deny { reason } }
                if *t == turn_id && reason == "not allowed"
        )),
        "Deny resolution emits NativeGateResolved{{Deny}} (§9), got {evs:?}"
    );

    // The actor answered the gate over the transport it solely holds, and the
    // answer carries the deny reason (the `deny: {reason}` compose arm, §9 line 846).
    let sent: Vec<String> = handle
        .shared
        .calls()
        .iter()
        .filter_map(|c| match c {
            TransportCall::SendTurn { prompt, .. } => Some(prompt.clone()),
            _ => None,
        })
        .collect();
    assert!(
        sent.iter()
            .any(|p| p.contains("deny") && p.contains("not allowed")),
        "the Deny answer is sent over the transport carrying the reason (§9), sent = {sent:?}"
    );
}

// =====================================================================
// Item 3b — resolve_native_gate when NO gate is pending ⇒ Err(GateUnresolvable).
// §9 (a gate that is not pending cannot be resolved).
// =====================================================================

#[tokio::test]
async fn resolving_gate_with_no_pending_gate_is_unresolvable() {
    let proto = ScriptedProtocol::new();
    // Busy (turn accepted) but NO gate ever requested.
    proto.install("ctx-nogate", busy_forever_script());
    let clock = TestClock::new();
    let sink = sink_arc(RecordingSink::default());
    let prov = provider(proto, tight_config(), clock, sink);
    let target: Arc<dyn DeliveryTarget> =
        prov.target_for(&key("ctx-nogate")).await.expect("target");

    let started = target
        .submit_turn(consult("ctx-nogate", "go"), BusyPolicy::Reject)
        .await;
    let turn_id = match started {
        TurnReceipt::Started { turn_id, .. } => turn_id,
        other => panic!("expected Started, got {other:?}"),
    };
    // Drain to reach the busy-but-no-gate state.
    let s0 = target.events(EventSeq(0)).await;
    let _ = drain_items(s0, 8).await;

    let res = resolve_gate(&target, turn_id, GateDecision::Approve).await;
    assert!(
        matches!(res, Err(RuntimeError::GateUnresolvable { .. })),
        "resolving with no pending gate ⇒ Err(GateUnresolvable) (§9), got {res:?}"
    );
}

// =====================================================================
// Item 3c — resolve_native_gate for the WRONG turn_id (a gate IS pending, but
// on a different turn) ⇒ Err(NotFound). §9.
// =====================================================================

#[tokio::test]
async fn resolving_gate_for_wrong_turn_id_is_not_found() {
    let proto = ScriptedProtocol::new();
    proto.install(
        "ctx-gate-wrong",
        ProtocolScript::new()
            .ready(Some("ses-1"))
            .emit(ProtocolEvent::TurnAccepted { native_turn: None })
            .emit(ProtocolEvent::NativeGateRequested {
                detail: NativeGateDetail {
                    summary: "write?".into(),
                    preview: "echo hi".into(),
                },
            }),
    );
    let clock = TestClock::new();
    let sink = sink_arc(RecordingSink::default());
    let prov = provider(proto, tight_config(), clock, sink);
    let target: Arc<dyn DeliveryTarget> = prov
        .target_for(&key("ctx-gate-wrong"))
        .await
        .expect("target");

    let _started = target
        .submit_turn(consult("ctx-gate-wrong", "go"), BusyPolicy::Reject)
        .await;
    // Let the gate surface (on the real active turn).
    let s0 = target.events(EventSeq(0)).await;
    let _ = drain_items(s0, 8).await;

    // Resolve with a turn_id that does NOT hold the pending gate.
    let res = resolve_gate(
        &target,
        TurnId("ctx-gate-wrong#turn-999".into()),
        GateDecision::Approve,
    )
    .await;
    assert!(
        matches!(res, Err(RuntimeError::NotFound { .. })),
        "resolving a gate for the wrong turn_id ⇒ Err(NotFound) (§9), got {res:?}"
    );
}

// =====================================================================
// Item 4 — Tool* events: the transport emits ToolStarted then ToolFinished
// mid-turn; the actor moves BusyPhase to RunningTool then back to Generating,
// and the public stream reflects ToolStarted/ToolFinished for the turn. §5/§7.4.
// =====================================================================

#[tokio::test]
async fn tool_started_then_finished_transitions_phase_and_surfaces_events() {
    let proto = ScriptedProtocol::new();
    // The turn runs a tool, then finishes. We hold at RunningTool via a gate so
    // the intermediate phase is observable, then release ToolFinished + complete.
    let handle = proto.install(
        "ctx-tool",
        ProtocolScript::new()
            .ready(Some("ses-1"))
            .emit(ProtocolEvent::TurnAccepted { native_turn: None })
            .emit(ProtocolEvent::ToolStarted {
                name: "grep".into(),
            })
            .gated(
                "finish_tool",
                ProtocolEvent::ToolFinished {
                    name: "grep".into(),
                    ok: true,
                },
            )
            .emit(ProtocolEvent::TurnCompleted {
                reply: "found it".into(),
            }),
    );
    let clock = TestClock::new();
    let sink = sink_arc(RecordingSink::default());
    let prov = provider(proto, tight_config(), clock, sink);
    let target: Arc<dyn DeliveryTarget> = prov.target_for(&key("ctx-tool")).await.expect("target");

    let started = target
        .submit_turn(consult("ctx-tool", "search"), BusyPolicy::Reject)
        .await;
    let turn_id = match started {
        TurnReceipt::Started { turn_id, .. } => turn_id,
        other => panic!("expected Started, got {other:?}"),
    };

    // Drain up to the tool-start (the gate holds ToolFinished back, so the
    // stream stops after ToolStarted and this drain returns via the hang-guard
    // once the pre-tool + ToolStarted events are collected). The RunningTool
    // phase is established once ToolStarted is pumped.
    let s0 = target.events(EventSeq(0)).await;
    let prefix = drain_items(s0, 8).await;
    let prefix_evs = events_of(&prefix);
    assert!(
        prefix_evs.iter().any(|e| matches!(
            e, TurnEvent::ToolStarted { turn_id: t, name } if *t == turn_id && name == "grep"
        )),
        "the tool start surfaces ToolStarted (§5/§7.4), got {prefix_evs:?}"
    );
    // Mid-tool the target is Busy{RunningTool}.
    assert!(
        matches!(
            target.status().await,
            TargetStatus::Busy {
                phase: BusyPhase::RunningTool,
                ..
            }
        ),
        "mid-tool the phase is Busy{{RunningTool}} (§7.4), got {:?}",
        target.status().await
    );

    // Release the tool finish, then drain to completion.
    handle.release("finish_tool");
    let s1 = target.events(EventSeq(0)).await;
    let items = drain_items(s1, 32).await;
    let evs = events_of(&items);
    assert!(
        evs.iter().any(|e| matches!(
            e, TurnEvent::ToolFinished { turn_id: t, name, ok: true } if *t == turn_id && name == "grep"
        )),
        "the tool finish surfaces ToolFinished{{ok:true}} (§5/§7.4), got {evs:?}"
    );
    // Ordering: ToolStarted precedes ToolFinished precedes TurnCompleted.
    let i_start = evs
        .iter()
        .position(|e| matches!(e, TurnEvent::ToolStarted { .. }));
    let i_fin = evs
        .iter()
        .position(|e| matches!(e, TurnEvent::ToolFinished { .. }));
    let i_done = evs
        .iter()
        .position(|e| matches!(e, TurnEvent::TurnCompleted { .. }));
    assert!(
        i_start < i_fin && i_fin < i_done,
        "Tool activity is ordered ToolStarted<ToolFinished<TurnCompleted, got {evs:?}"
    );
}

// =====================================================================
// Item 5 — TurnFailed FROM the transport (a definite in-session failure, not a
// crash): certainty becomes Completed (the transport told us the outcome), NOT
// OutcomeUnknown, and the public stream shows TurnFailed. §7.4 line 619 / §8.1.
// =====================================================================

#[tokio::test]
async fn transport_turn_failed_is_completed_certainty_not_outcome_unknown() {
    let proto = ScriptedProtocol::new();
    proto.install(
        "ctx-failed",
        ProtocolScript::new()
            .ready(Some("ses-1"))
            .emit(ProtocolEvent::TurnAccepted { native_turn: None })
            .emit(ProtocolEvent::TurnFailed {
                error: "model refused".into(),
            }),
    );
    let clock = TestClock::new();
    let sink = sink_arc(RecordingSink::default());
    let prov = provider(proto, tight_config(), clock, sink);
    let target: Arc<dyn DeliveryTarget> =
        prov.target_for(&key("ctx-failed")).await.expect("target");

    let started = target
        .submit_turn(consult("ctx-failed", "go"), BusyPolicy::Reject)
        .await;
    let turn_id = match started {
        TurnReceipt::Started { turn_id, .. } => turn_id,
        other => panic!("expected Started, got {other:?}"),
    };

    let stream = target.events(EventSeq(0)).await;
    let items = drain_items(stream, 32).await;
    let evs = events_of(&items);

    let failed_certainty = evs.iter().find_map(|e| match e {
        TurnEvent::TurnFailed {
            turn_id: t,
            certainty,
            error,
        } if *t == turn_id => Some((*certainty, error.clone())),
        _ => None,
    });
    match failed_certainty {
        Some((certainty, error)) => {
            assert_eq!(
                certainty,
                DeliveryCertainty::Completed,
                "a transport-reported TurnFailed is a KNOWN outcome ⇒ Completed, never \
                 OutcomeUnknown (§8.1 line 648), got {certainty:?}"
            );
            assert!(
                error.contains("model refused"),
                "the failure error is surfaced, got {error:?}"
            );
        }
        None => panic!("expected a TurnFailed for the turn, got {evs:?}"),
    }
    // The turn is definitely NOT reported as a crash-style OutcomeUnknown.
    assert!(
        !evs.iter().any(|e| matches!(
            e,
            TurnEvent::TurnFailed {
                certainty: DeliveryCertainty::OutcomeUnknown,
                ..
            }
        )),
        "a definite in-session TurnFailed must not be classified OutcomeUnknown, got {evs:?}"
    );

    // After the failure the target returns to Idle (queue empty), ready for a
    // fresh turn (a known outcome does not fault the session).
    assert!(
        matches!(target.status().await, TargetStatus::Idle { .. }),
        "after a definite TurnFailed the target returns to Idle (§7.4), got {:?}",
        target.status().await
    );
}

// =====================================================================
// Item 6 — a LocalHuman-sourced turn: compose/injection handles TurnSource::
// LocalHuman (the `[{kind} from local]` compose arm, §4.1) distinctly from the
// Agent arm; the turn is accepted and injected, and the TurnAccepted carries the
// LocalHuman source. §8.2 / §4.1.
// =====================================================================

#[tokio::test]
async fn local_human_sourced_turn_composes_and_injects() {
    let proto = ScriptedProtocol::new();
    let handle = proto.install(
        "ctx-human",
        ProtocolScript::new()
            .ready(Some("ses-1"))
            .happy_turn("hi ", "hello human"),
    );
    let clock = TestClock::new();
    let sink = sink_arc(RecordingSink::default());
    let prov = provider(proto, tight_config(), clock, sink);
    let target: Arc<dyn DeliveryTarget> = prov.target_for(&key("ctx-human")).await.expect("target");

    // A LocalHuman turn (R3 `send`), funneled through the same single actor.
    let req = TurnRequest {
        delivery: delivery("ctx-human", DeliveryKind::Message, "please summarize"),
        source: TurnSource::LocalHuman,
        idempotency_key: None,
        causation: root_causation("human-root"),
    };
    let started = target.submit_turn(req, BusyPolicy::Reject).await;
    let turn_id = match started {
        TurnReceipt::Started { turn_id, .. } => turn_id,
        other => panic!("expected Started for a LocalHuman turn, got {other:?}"),
    };

    let stream = target.events(EventSeq(0)).await;
    let items = drain_items(stream, 32).await;
    let evs = events_of(&items);
    // The TurnAccepted carries the LocalHuman source (distinct from Agent).
    assert!(
        evs.iter().any(|e| matches!(
            e,
            TurnEvent::TurnAccepted { turn_id: t, source: TurnSource::LocalHuman }
                if *t == turn_id
        )),
        "a LocalHuman turn's TurnAccepted carries TurnSource::LocalHuman (§4.1), got {evs:?}"
    );
    // Injection happened, and the composed prompt used the LocalHuman header
    // (`[Message from local]`), not an agent-from header (§4.1 compose arm).
    let sent: Vec<String> = handle
        .shared
        .calls()
        .iter()
        .filter_map(|c| match c {
            TransportCall::SendTurn { prompt, .. } => Some(prompt.clone()),
            _ => None,
        })
        .collect();
    assert_eq!(sent.len(), 1, "the LocalHuman turn is injected once");
    assert!(
        sent[0].contains("from local"),
        "a LocalHuman turn composes the `from local` header, not an agent header (§4.1), got {:?}",
        sent[0]
    );
    assert!(
        evs.iter()
            .any(|e| matches!(e, TurnEvent::TurnCompleted { turn_id: t, .. } if *t == turn_id)),
        "the LocalHuman turn completes normally, got {evs:?}"
    );
}

// =====================================================================
// Item 6 (contrast) — an Agent-sourced turn composes the `from {agent}` header,
// proving the compose LocalHuman arm is a genuine distinct branch, not the only
// path taken.
// =====================================================================

#[tokio::test]
async fn agent_sourced_turn_composes_from_agent_header() {
    let proto = ScriptedProtocol::new();
    let handle = proto.install(
        "ctx-agentsrc",
        ProtocolScript::new()
            .ready(Some("ses-1"))
            .happy_turn("x", "reply"),
    );
    let clock = TestClock::new();
    let sink = sink_arc(RecordingSink::default());
    let prov = provider(proto, tight_config(), clock, sink);
    let target: Arc<dyn DeliveryTarget> =
        prov.target_for(&key("ctx-agentsrc")).await.expect("target");

    // `consult(..)` uses TurnSource::Agent { from: wasl-b } (support builder).
    let _ = target
        .submit_turn(consult("ctx-agentsrc", "ask"), BusyPolicy::Reject)
        .await;
    let s = target.events(EventSeq(0)).await;
    let _ = drain_items(s, 16).await;

    let sent: Vec<String> = handle
        .shared
        .calls()
        .iter()
        .filter_map(|c| match c {
            TransportCall::SendTurn { prompt, .. } => Some(prompt.clone()),
            _ => None,
        })
        .collect();
    assert_eq!(sent.len(), 1);
    assert!(
        sent[0].contains("from wasl-b") && !sent[0].contains("from local"),
        "an Agent turn composes the `from {{agent}}` header, not the LocalHuman arm (§4.1), got {:?}",
        sent[0]
    );
}

// =====================================================================
// Item 7 — an over-`delta_max_bytes` AssistantDelta is TRUNCATED (§11 cap_delta):
// the public delta text is bounded, not the unbounded original.
// =====================================================================

#[tokio::test]
async fn oversized_assistant_delta_is_truncated_to_the_bound() {
    let proto = ScriptedProtocol::new();
    // A delta far larger than the configured delta_max_bytes.
    let big = "a".repeat(4096);
    proto.install(
        "ctx-cap",
        ProtocolScript::new()
            .ready(Some("ses-1"))
            .emit(ProtocolEvent::TurnAccepted { native_turn: None })
            .emit(ProtocolEvent::AssistantDelta { text: big.clone() })
            .emit(ProtocolEvent::TurnCompleted {
                reply: "done".into(),
            }),
    );
    let clock = TestClock::new();
    let sink = sink_arc(RecordingSink::default());
    // A tiny delta cap so the oversized delta is truncated.
    let mut cfg = tight_config();
    cfg.delta_max_bytes = 64;
    let prov = provider(proto, cfg, clock, sink);
    let target: Arc<dyn DeliveryTarget> = prov.target_for(&key("ctx-cap")).await.expect("target");

    let _ = target
        .submit_turn(consult("ctx-cap", "long"), BusyPolicy::Reject)
        .await;
    let stream = target.events(EventSeq(0)).await;
    let items = drain_items(stream, 32).await;
    let evs = events_of(&items);

    let delta_text = evs.iter().find_map(|e| match e {
        TurnEvent::AssistantDelta { text, .. } => Some(text.clone()),
        _ => None,
    });
    let text = delta_text.expect("the turn emits an AssistantDelta");
    assert!(
        text.len() < big.len(),
        "an over-cap delta must be truncated below the original {} bytes, got {} bytes",
        big.len(),
        text.len()
    );
    // The truncation keeps the leading cap bytes and appends the truncation
    // marker (§11 cap_delta), so it stays bounded and clearly flagged.
    assert!(
        text.starts_with(&"a".repeat(64)),
        "the truncated delta keeps the leading delta_max_bytes prefix"
    );
    assert!(
        text.contains("truncated"),
        "an over-cap delta is flagged truncated (§11), got {text:?}"
    );
}

// =====================================================================
// Item 7 (contrast) — a delta WITHIN the bound is passed through verbatim,
// proving the truncation is a real conditional, not always applied.
// =====================================================================

#[tokio::test]
async fn small_assistant_delta_passes_through_verbatim() {
    let proto = ScriptedProtocol::new();
    proto.install(
        "ctx-small-delta",
        ProtocolScript::new()
            .ready(Some("ses-1"))
            .emit(ProtocolEvent::TurnAccepted { native_turn: None })
            .emit(ProtocolEvent::AssistantDelta {
                text: "short".into(),
            })
            .emit(ProtocolEvent::TurnCompleted {
                reply: "done".into(),
            }),
    );
    let clock = TestClock::new();
    let sink = sink_arc(RecordingSink::default());
    let mut cfg = tight_config();
    cfg.delta_max_bytes = 64;
    let prov = provider(proto, cfg, clock, sink);
    let target: Arc<dyn DeliveryTarget> = prov
        .target_for(&key("ctx-small-delta"))
        .await
        .expect("target");

    let _ = target
        .submit_turn(consult("ctx-small-delta", "hi"), BusyPolicy::Reject)
        .await;
    let stream = target.events(EventSeq(0)).await;
    let items = drain_items(stream, 32).await;
    let evs = events_of(&items);
    let delta_text = evs.iter().find_map(|e| match e {
        TurnEvent::AssistantDelta { text, .. } => Some(text.clone()),
        _ => None,
    });
    assert_eq!(
        delta_text.as_deref(),
        Some("short"),
        "a within-bound delta is passed through verbatim (no truncation marker), got {delta_text:?}"
    );
}

// Keep IdempotencyKey referenced (used by other suites; a reader sees the dedupe
// key type is part of the request surface exercised across the suite).
#[allow(dead_code)]
fn _idem_key() -> IdempotencyKey {
    IdempotencyKey("k".into())
}
