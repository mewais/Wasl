//! Phase-1 TYPE-EXISTENCE / SHAPE smoke tests for `wasl-runtime` (R1).
//!
//! Phase-1 is a TYPE-ONLY surface. These tests construct each small value type
//! the spec defines and assert basic, spec-pinned shape facts — NO behavior
//! (no actor, no queue, no driver). That is Phase-2.
//!
//! Spec citations (docs/specs/runtime.md):
//!   * Id newtypes — §1 (lines 106-133): TargetId, TargetKey, NativeSessionId,
//!     TurnId, EventSeq (Copy, Ord), IdempotencyKey. All serde newtypes.
//!   * EventSeq — §1 (lines 128-129): `Copy` + `Ord`, starts at 1, per-target
//!     monotonic; serializes as a bare u64.
//!   * RuntimeError — §13 (lines 809-831): a DEFINED taxonomy; each variant
//!     `impl Display + std::error::Error`, distinct Display, distinct match arms.
//!   * TurnRequest — §3.1 (lines 256-266): delivery + source + idempotency_key +
//!     causation. Type-existence + field-shape only.
//!   * ShutdownMode / CancelReason — §3 (lines 244-247).
//!   * StreamItem — §3 (lines 216-223): Event { seq, event } | Truncated {…}.
//!   * ComposedPrompt — §6 (line 554): pub tuple over String.
//!
//! TEST-FIRST / EXPECTED RED: will not compile until the coder creates
//! `wasl-runtime` and emits these types.

use std::error::Error;

use wasl_proto::{AgentId, RequestId};
use wasl_runtime::{
    BusyPolicy, CancelReason, Causation, ComposedPrompt, EventSeq, IdempotencyKey, NativeSessionId,
    RuntimeError, ShutdownMode, StreamItem, TargetId, TargetKey, TargetStatus, TurnEvent, TurnId,
    TurnRequest, TurnSource,
};

// =====================================================================
// Id newtypes — construct each; the inner String/u64 is accessible; equality
// + hashing behave; EventSeq is Copy + Ord.
// =====================================================================

#[test]
fn id_newtypes_construct_and_expose_inner() {
    let target_id = TargetId("tgt-1".to_string());
    let target_key = TargetKey("ctx-42".to_string());
    let native = NativeSessionId("ses_abc".to_string());
    let turn_id = TurnId("turn-7".to_string());
    let idem = IdempotencyKey("key-1".to_string());

    assert_eq!(target_id.0, "tgt-1");
    assert_eq!(target_key.0, "ctx-42");
    assert_eq!(native.0, "ses_abc");
    assert_eq!(turn_id.0, "turn-7");
    assert_eq!(idem.0, "key-1");

    // Clone + Eq (all are `#[derive(Clone, PartialEq, Eq)]` per §1).
    assert_eq!(target_id.clone(), target_id);
    assert_eq!(turn_id.clone(), turn_id);
}

#[test]
fn event_seq_is_copy_ord_and_serializes_as_bare_u64() {
    let a = EventSeq(1);
    let b = EventSeq(2);
    // Copy: using `a` after passing it by value must still compile.
    let a_copy = a;
    assert_eq!(a_copy, EventSeq(1));
    // Ord: per-target monotonic ordering (§1 line 128).
    assert!(a < b);
    assert_eq!(a.0, 1);

    // Serializes transparently as a bare integer (control-api.md `id:` is a bare
    // u64; first_seq in receipt_started.json is `13`, not `{"0":13}`).
    let json_s = serde_json::to_string(&EventSeq(13)).expect("serialize EventSeq");
    assert_eq!(json_s, "13");
    let back: EventSeq = serde_json::from_str("13").expect("deserialize EventSeq");
    assert_eq!(back, EventSeq(13));
}

#[test]
fn id_newtypes_are_hashable_for_map_keys() {
    // §1: TargetId/TargetKey/TurnId/NativeSessionId/IdempotencyKey derive Hash
    // (they key the target registry / idempotency map).
    use std::collections::HashSet;
    let mut set: HashSet<TargetKey> = HashSet::new();
    assert!(set.insert(TargetKey("ctx-1".to_string())));
    assert!(!set.insert(TargetKey("ctx-1".to_string())));

    let mut turns: HashSet<TurnId> = HashSet::new();
    assert!(turns.insert(TurnId("turn-1".to_string())));
    assert!(turns.insert(TurnId("turn-2".to_string())));
    assert_eq!(turns.len(), 2);
}

// =====================================================================
// RuntimeError — the DEFINED taxonomy (§13): distinct Display, non-empty,
// implements std::error::Error, distinct match arms.
// =====================================================================

fn all_runtime_errors() -> Vec<RuntimeError> {
    vec![
        RuntimeError::SpawnFailed {
            detail: "no binary".to_string(),
        },
        RuntimeError::Transport {
            detail: "broken pipe".to_string(),
        },
        RuntimeError::Unavailable {
            status: TargetStatus::Offline,
        },
        RuntimeError::Unsupported {
            operation: "cancel".to_string(),
        },
        RuntimeError::AdmissionRefused {
            detail: "per-sender cap".to_string(),
        },
        RuntimeError::CausationRejected {
            detail: "cycle".to_string(),
        },
        RuntimeError::NotFound {
            detail: "turn-9".to_string(),
        },
        RuntimeError::GateUnresolvable {
            detail: "not awaiting".to_string(),
        },
        RuntimeError::Exhausted {
            detail: "queue full".to_string(),
        },
        RuntimeError::Shutdown,
    ]
}

#[test]
fn runtime_error_has_ten_taxonomy_variants() {
    // §13 enumerates exactly ten variants. Constructing all ten guards the
    // taxonomy shape (a missing/renamed variant fails to compile).
    assert_eq!(all_runtime_errors().len(), 10);
}

#[test]
fn runtime_error_display_is_non_empty_for_every_variant() {
    for err in all_runtime_errors() {
        let rendered = format!("{err}");
        assert!(
            !rendered.is_empty(),
            "RuntimeError Display must be non-empty: {err:?}"
        );
    }
}

#[test]
fn runtime_error_display_is_distinct_across_variants() {
    // §13: "distinct `Display`, distinct match arms. No catch-all string error."
    let rendered: Vec<String> = all_runtime_errors()
        .iter()
        .map(|e| format!("{e}"))
        .collect();
    let unique: std::collections::BTreeSet<&String> = rendered.iter().collect();
    assert_eq!(
        unique.len(),
        rendered.len(),
        "each RuntimeError variant must render a DISTINCT Display string; got: {rendered:?}"
    );
}

#[test]
fn runtime_error_implements_std_error() {
    let err = RuntimeError::Shutdown;
    let as_std: &dyn Error = &err;
    // A defined taxonomy has no wrapped source at this layer.
    assert!(as_std.source().is_none());
    assert!(!as_std.to_string().is_empty());
}

#[test]
fn runtime_error_is_clone_eq() {
    // §13 derives Debug, Clone, PartialEq, Eq.
    let err = RuntimeError::AdmissionRefused {
        detail: "x".to_string(),
    };
    assert_eq!(err.clone(), err);
    assert_ne!(err, RuntimeError::Shutdown);
}

// =====================================================================
// TurnRequest — §3.1: the admission payload shape (delivery + source +
// idempotency_key + causation). Type-existence + field-access only.
// =====================================================================

#[test]
fn turn_request_constructs_with_all_fields() {
    let req = TurnRequest {
        delivery: wasl_adapter::Delivery {
            context_id: "ctx-42".to_string(),
            from: "wasl-b".to_string(),
            kind: wasl_adapter::DeliveryKind::Consult,
            text: "hello".to_string(),
            data: vec![],
        },
        source: TurnSource::Agent {
            from: AgentId("wasl-b".to_string()),
        },
        idempotency_key: Some(IdempotencyKey("k-1".to_string())),
        causation: Causation {
            root_request: RequestId("req-1".to_string()),
            parent_request: None,
            path: vec![],
            hop: 0,
        },
    };
    assert_eq!(req.delivery.context_id, "ctx-42");
    assert_eq!(req.causation.hop, 0);
    assert_eq!(req.idempotency_key, Some(IdempotencyKey("k-1".to_string())));
    // None idempotency_key ⇒ no dedupe (§3.1 line 263).
    let req2 = TurnRequest {
        idempotency_key: None,
        ..req.clone()
    };
    assert_eq!(req2.idempotency_key, None);
    assert_eq!(req2, req2.clone());
}

// =====================================================================
// ShutdownMode / CancelReason — §3 small enums (Copy where noted).
// =====================================================================

#[test]
fn shutdown_mode_variants_exist_and_are_copy() {
    let drain = ShutdownMode::Drain;
    let immediate = ShutdownMode::Immediate;
    // Copy (§3 line 243 derives Copy).
    let _d2 = drain;
    let _i2 = immediate;
    assert_ne!(drain, immediate);
    assert_eq!(drain, ShutdownMode::Drain);
}

#[test]
fn cancel_reason_four_variants_exist() {
    let reasons = [
        CancelReason::OperatorRequest,
        CancelReason::A2aCancel,
        CancelReason::Superseded,
        CancelReason::Shutdown,
    ];
    let unique: std::collections::BTreeSet<String> =
        reasons.iter().map(|r| format!("{r:?}")).collect();
    assert_eq!(unique.len(), 4, "4 distinct CancelReason variants");
}

// =====================================================================
// StreamItem — §3 (lines 216-223): Event { seq, event } | Truncated {…}.
// This is the item type of the events() stream. Type-existence + fields only
// (no streaming behavior — Phase-2).
// =====================================================================

#[test]
fn stream_item_event_variant_pairs_seq_with_turn_event() {
    let item = StreamItem::Event {
        seq: EventSeq(5),
        event: TurnEvent::ProcessExited { code: Some(0) },
    };
    match item {
        StreamItem::Event { seq, event } => {
            assert_eq!(seq, EventSeq(5));
            assert_eq!(event, TurnEvent::ProcessExited { code: Some(0) });
        }
        StreamItem::Truncated { .. } => panic!("expected Event variant"),
    }
}

#[test]
fn stream_item_truncated_variant_carries_window() {
    // §3 line 222: Truncated { earliest, latest } — the retained window.
    let item = StreamItem::Truncated {
        earliest: EventSeq(8),
        latest: EventSeq(20),
    };
    match item {
        StreamItem::Truncated { earliest, latest } => {
            assert_eq!(earliest, EventSeq(8));
            assert_eq!(latest, EventSeq(20));
            assert!(earliest < latest);
        }
        StreamItem::Event { .. } => panic!("expected Truncated variant"),
    }
}

// =====================================================================
// ComposedPrompt / BusyPolicy defaults sanity — type-existence only.
// =====================================================================

#[test]
fn composed_prompt_wraps_string() {
    // §6 line 554: pub tuple newtype over String (the model-visible wrapper).
    let cp = ComposedPrompt("[Consult from wasl-b]\n\nhello".to_string());
    assert!(cp.0.contains("hello"));
    assert_eq!(cp.clone(), cp);
}

#[test]
fn busy_policy_variants_carry_durations() {
    // §4.2: Queue/QueueRelated carry a std::time::Duration; Reject is bare.
    let q = BusyPolicy::Queue {
        max_wait: std::time::Duration::from_millis(30_000),
    };
    let qr = BusyPolicy::QueueRelated {
        max_wait: std::time::Duration::from_millis(120_000),
    };
    let r = BusyPolicy::Reject;
    assert_ne!(q, r);
    assert_ne!(q, qr);
    assert_eq!(r, BusyPolicy::Reject);
}
