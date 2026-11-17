//! Phase-1 SERDE WIRE-FORM tests for `wasl-runtime` (R1).
//!
//! Phase-1 is a TYPE-ONLY surface: these tests assert the EXACT serde wire forms
//! (tag, casing, field names) the specs PIN, and that each wire type round-trips.
//! NO behavior is exercised (no actor, no queue, no driver) — that is Phase-2.
//!
//! Authoritative wire pins:
//!   * `TurnEvent` — runtime.md §5 (lines 436-468): `#[serde(tag = "type",
//!     rename_all = "snake_case")]`, internally tagged, snake_case variant tags.
//!     The full variant list is enumerated in control-api.md line 151-154.
//!   * `RuntimeCapabilities` — runtime.md §2 (lines 155-165): 8 named bool
//!     fields; serialized as a JSON object in control-api.md §GET /v1/targets
//!     (lines 70-74) with exactly those field names.
//!   * `DeliveryCertainty` — wasl_proto (snake_case; §8.1 lines 638-642), carried
//!     by `TurnCompleted`/`TurnFailed` (§5 lines 457,459) and re-used verbatim.
//!   * `TargetStatus` — internally tagged `{"type":"idle",...}` per control-api.md
//!     line 75 + §5.6 boundary invariant; variants from runtime.md §4.1.
//!   * `TurnReceipt` — internally tagged `{"type":"started",...}` per
//!     control-api.md lines 195-199; variants from runtime.md §3 (lines 227-241).
//!   * `TurnSource` — `{"type":"agent","from":"…"}` | `{"type":"local_human"}`
//!     per control-api.md line 186; runtime.md §3.1 (lines 268-273).
//!   * `BusyPhase` — runtime.md §4.1 (lines 303-311).
//!   * `BusyPolicy` — `{"type":"queue","max_wait_ms":…}` per control-api.md
//!     line 181; runtime.md §4.2 (lines 322-332). Durations are integer ms on
//!     the wire (control-api.md §Conventions line 25).
//!   * `NativeGateDetail` — runtime.md §4.1 (line 314): serde `{summary,preview}`.
//!   * `Causation` — control-api.md line 182: `root_request`/`parent_request` are
//!     bare JSON strings (wasl_proto::RequestId is `#[serde(transparent)]`),
//!     `path` is a JSON array of AgentId strings; runtime.md §8.2 (lines 688-694).
//!
//! TEST-FIRST / EXPECTED RED: this file will NOT compile until the coder creates
//! `wasl-runtime` and emits these types with the specified serde forms.

use serde_json::json;

use wasl_runtime::{
    AgentMode, BusyPhase, BusyPolicy, Causation, CompactionInfo, NativeGateDetail, PlanEntry,
    RuntimeCapabilities, SlashCommandInfo, TargetStatus, TurnEvent, TurnId, TurnReceipt,
    TurnSource, UsageSnapshot,
};

// wasl_proto owns DeliveryCertainty (§8.1); TurnEvent re-uses it verbatim.
use wasl_proto::{AgentId, DeliveryCertainty, GateDecision, RequestId};

// ---------------------------------------------------------------------
// Helpers
// ---------------------------------------------------------------------

fn roundtrip_json<T>(value: &T) -> serde_json::Value
where
    T: serde::Serialize,
{
    serde_json::to_value(value).expect("serialize to serde_json::Value")
}

// =====================================================================
// RuntimeCapabilities — 8 independent named booleans (§2).
// =====================================================================

fn all_caps(v: bool) -> RuntimeCapabilities {
    RuntimeCapabilities {
        persistent_process: v,
        persistent_conversation: v,
        structured_turns: v,
        streaming_output: v,
        attachable: v,
        interruptible: v,
        structured_provenance: v,
        resumable: v,
    }
}

#[test]
fn runtime_capabilities_has_all_eight_named_bool_fields() {
    // Distinct per-field values so a swapped/duplicated field is caught.
    let caps = RuntimeCapabilities {
        persistent_process: true,
        persistent_conversation: false,
        structured_turns: true,
        streaming_output: false,
        attachable: true,
        interruptible: false,
        structured_provenance: true,
        resumable: false,
    };
    let value = roundtrip_json(&caps);
    assert_eq!(
        value,
        json!({
            "persistent_process": true,
            "persistent_conversation": false,
            "structured_turns": true,
            "streaming_output": false,
            "attachable": true,
            "interruptible": false,
            "structured_provenance": true,
            "resumable": false
        }),
        "RuntimeCapabilities must serialize to a JSON object with exactly the 8 \
         spec field names (runtime.md §2 / control-api.md §GET /v1/targets)"
    );
}

#[test]
fn runtime_capabilities_round_trips() {
    for v in [true, false] {
        let caps = all_caps(v);
        let json_s = serde_json::to_string(&caps).expect("serialize");
        let back: RuntimeCapabilities = serde_json::from_str(&json_s).expect("deserialize");
        assert_eq!(back, caps);
    }
}

// =====================================================================
// DeliveryCertainty — re-used from wasl_proto; the 5 snake_case variants
// (§8.1). TurnEvent carries it, so its wire form is part of R1's surface.
// =====================================================================

#[test]
fn delivery_certainty_five_snake_case_variants_round_trip() {
    let cases = [
        (DeliveryCertainty::NotInjected, "\"not_injected\""),
        (DeliveryCertainty::Injected, "\"injected\""),
        (DeliveryCertainty::Completed, "\"completed\""),
        (
            DeliveryCertainty::FailedBeforeInjection,
            "\"failed_before_injection\"",
        ),
        (DeliveryCertainty::OutcomeUnknown, "\"outcome_unknown\""),
    ];
    for (value, expected) in cases {
        let json_s = serde_json::to_string(&value).expect("serialize DeliveryCertainty");
        assert_eq!(json_s, expected, "certainty wire string for {value:?}");
        let back: DeliveryCertainty = serde_json::from_str(&json_s).expect("deserialize");
        assert_eq!(back, value);
    }
}

// =====================================================================
// TurnEvent — the PUBLIC vocabulary, internally tagged snake_case (§5).
// One representative per variant; assert the pinned tag + fields; round-trip.
// =====================================================================

fn tid() -> TurnId {
    TurnId("turn-7".to_string())
}

fn assert_turn_event_roundtrips(ev: &TurnEvent, expected: serde_json::Value) {
    let value = roundtrip_json(ev);
    assert_eq!(value, expected, "TurnEvent wire form for {ev:?}");
    // Round-trip back into the typed value.
    let back: TurnEvent = serde_json::from_value(value).expect("deserialize TurnEvent");
    assert_eq!(&back, ev, "TurnEvent must round-trip: {ev:?}");
}

#[test]
fn turn_event_session_ready_wire_form() {
    // native is Option<NativeSessionId>; None here (first-turn, no id yet).
    assert_turn_event_roundtrips(
        &TurnEvent::SessionReady { native: None },
        json!({ "type": "session_ready", "native": null }),
    );
}

#[test]
fn turn_event_turn_accepted_wire_form() {
    assert_turn_event_roundtrips(
        &TurnEvent::TurnAccepted {
            turn_id: tid(),
            source: TurnSource::LocalHuman,
        },
        json!({
            "type": "turn_accepted",
            "turn_id": "turn-7",
            "source": { "type": "local_human" }
        }),
    );
}

#[test]
fn turn_event_turn_queued_wire_form() {
    assert_turn_event_roundtrips(
        &TurnEvent::TurnQueued {
            turn_id: tid(),
            position: 2,
        },
        json!({ "type": "turn_queued", "turn_id": "turn-7", "position": 2 }),
    );
}

#[test]
fn turn_event_assistant_delta_wire_form() {
    // control-api.md line 144 pins this exact shape.
    assert_turn_event_roundtrips(
        &TurnEvent::AssistantDelta {
            turn_id: tid(),
            text: "PON".to_string(),
        },
        json!({ "type": "assistant_delta", "turn_id": "turn-7", "text": "PON" }),
    );
}

#[test]
fn turn_event_tool_started_wire_form() {
    assert_turn_event_roundtrips(
        &TurnEvent::ToolStarted {
            turn_id: tid(),
            name: "grep".to_string(),
            input: json!({ "pattern": "foo" }),
        },
        json!({
            "type": "tool_started", "turn_id": "turn-7", "name": "grep",
            "input": { "pattern": "foo" }
        }),
    );
}

#[test]
fn turn_event_tool_started_wire_form_null_input() {
    // Absent driver args → `input: Null`, which serializes as `"input": null`
    // (the field is `#[serde(default)]`, not skip-on-null, so it is always
    // present on the wire — a stable shape for observers).
    assert_turn_event_roundtrips(
        &TurnEvent::ToolStarted {
            turn_id: tid(),
            name: "grep".to_string(),
            input: serde_json::Value::Null,
        },
        json!({ "type": "tool_started", "turn_id": "turn-7", "name": "grep", "input": null }),
    );
}

#[test]
fn turn_event_tool_finished_wire_form() {
    assert_turn_event_roundtrips(
        &TurnEvent::ToolFinished {
            turn_id: tid(),
            name: "grep".to_string(),
            ok: true,
            result: Some(json!("3 matches")),
        },
        json!({
            "type": "tool_finished", "turn_id": "turn-7", "name": "grep", "ok": true,
            "result": "3 matches"
        }),
    );
}

#[test]
fn turn_event_tool_finished_wire_form_no_result() {
    // Absent driver output → `result: None`, which is skipped on the wire
    // (`skip_serializing_if = "Option::is_none"`), keeping the finished frame
    // lean when the driver reports only success/failure (e.g. codex).
    assert_turn_event_roundtrips(
        &TurnEvent::ToolFinished {
            turn_id: tid(),
            name: "grep".to_string(),
            ok: true,
            result: None,
        },
        json!({
            "type": "tool_finished", "turn_id": "turn-7", "name": "grep", "ok": true
        }),
    );
}

#[test]
fn turn_event_native_gate_requested_wire_form() {
    assert_turn_event_roundtrips(
        &TurnEvent::NativeGateRequested {
            turn_id: tid(),
            detail: NativeGateDetail {
                summary: "run rm?".to_string(),
                preview: "rm -rf /tmp/x".to_string(),
            },
        },
        json!({
            "type": "native_gate_requested",
            "turn_id": "turn-7",
            "detail": { "summary": "run rm?", "preview": "rm -rf /tmp/x" }
        }),
    );
}

#[test]
fn turn_event_native_gate_resolved_wire_form() {
    // decision is wasl_proto::GateDecision (tag = "decision", lowercase).
    assert_turn_event_roundtrips(
        &TurnEvent::NativeGateResolved {
            turn_id: tid(),
            decision: GateDecision::Approve,
        },
        json!({
            "type": "native_gate_resolved",
            "turn_id": "turn-7",
            "decision": { "decision": "approve" }
        }),
    );
}

#[test]
fn turn_event_turn_completed_wire_form_carries_certainty() {
    // control-api.md line 148 pins: {"type":"turn_completed","turn_id":…,
    // "reply":…,"certainty":"completed"}.
    assert_turn_event_roundtrips(
        &TurnEvent::TurnCompleted {
            turn_id: tid(),
            reply: "PONG".to_string(),
            certainty: DeliveryCertainty::Completed,
        },
        json!({
            "type": "turn_completed",
            "turn_id": "turn-7",
            "reply": "PONG",
            "certainty": "completed"
        }),
    );
}

#[test]
fn turn_event_turn_failed_wire_form_carries_certainty() {
    assert_turn_event_roundtrips(
        &TurnEvent::TurnFailed {
            turn_id: tid(),
            error: "model error".to_string(),
            certainty: DeliveryCertainty::Completed,
        },
        json!({
            "type": "turn_failed",
            "turn_id": "turn-7",
            "error": "model error",
            "certainty": "completed"
        }),
    );
}

#[test]
fn turn_event_turn_expired_wire_form() {
    assert_turn_event_roundtrips(
        &TurnEvent::TurnExpired { turn_id: tid() },
        json!({ "type": "turn_expired", "turn_id": "turn-7" }),
    );
}

#[test]
fn turn_event_status_changed_wire_form() {
    // Carries a TargetStatus (internally tagged, snake_case).
    assert_turn_event_roundtrips(
        &TurnEvent::StatusChanged {
            status: TargetStatus::Stopped,
        },
        json!({ "type": "status_changed", "status": { "type": "stopped" } }),
    );
}

#[test]
fn turn_event_process_exited_wire_form() {
    assert_turn_event_roundtrips(
        &TurnEvent::ProcessExited { code: Some(0) },
        json!({ "type": "process_exited", "code": 0 }),
    );
    assert_turn_event_roundtrips(
        &TurnEvent::ProcessExited { code: None },
        json!({ "type": "process_exited", "code": null }),
    );
}

#[test]
fn turn_event_all_fourteen_variant_tags_are_snake_case_and_distinct() {
    // The complete variant tag set (original 14 + 7 new informational). Assert the
    // "type" tag of a representative of every variant is snake_case and unique.
    let events = vec![
        TurnEvent::SessionReady { native: None },
        TurnEvent::TurnAccepted {
            turn_id: tid(),
            source: TurnSource::LocalHuman,
        },
        TurnEvent::TurnQueued {
            turn_id: tid(),
            position: 0,
        },
        TurnEvent::AssistantDelta {
            turn_id: tid(),
            text: String::new(),
        },
        TurnEvent::ToolStarted {
            turn_id: tid(),
            name: String::new(),
            input: serde_json::Value::Null,
        },
        TurnEvent::ToolFinished {
            turn_id: tid(),
            name: String::new(),
            ok: false,
            result: None,
        },
        TurnEvent::NativeGateRequested {
            turn_id: tid(),
            detail: NativeGateDetail {
                summary: String::new(),
                preview: String::new(),
            },
        },
        TurnEvent::NativeGateResolved {
            turn_id: tid(),
            decision: GateDecision::Approve,
        },
        TurnEvent::TurnCompleted {
            turn_id: tid(),
            reply: String::new(),
            certainty: DeliveryCertainty::Completed,
        },
        TurnEvent::TurnFailed {
            turn_id: tid(),
            error: String::new(),
            certainty: DeliveryCertainty::Completed,
        },
        TurnEvent::TurnCancelled {
            turn_id: tid(),
            reason: wasl_runtime::CancelReason::OperatorRequest,
        },
        TurnEvent::TurnExpired { turn_id: tid() },
        TurnEvent::StatusChanged {
            status: TargetStatus::Stopped,
        },
        TurnEvent::ProcessExited { code: None },
        // Informational / observability (additive, 7 new variants).
        TurnEvent::ThinkingDelta {
            text: String::new(),
        },
        TurnEvent::UsageUpdate {
            usage: UsageSnapshot::default(),
            context_pct: None,
        },
        TurnEvent::PlanUpdated { entries: vec![] },
        TurnEvent::ModesUpdated {
            available_modes: vec![],
            current_mode: None,
        },
        TurnEvent::ModelChanged {
            model: String::new(),
        },
        TurnEvent::SlashCommandsUpdated { commands: vec![] },
        TurnEvent::Compaction {
            info: CompactionInfo::default(),
        },
    ];

    let expected_tags = [
        "session_ready",
        "turn_accepted",
        "turn_queued",
        "assistant_delta",
        "tool_started",
        "tool_finished",
        "native_gate_requested",
        "native_gate_resolved",
        "turn_completed",
        "turn_failed",
        "turn_cancelled",
        "turn_expired",
        "status_changed",
        "process_exited",
        // Informational additions.
        "thinking_delta",
        "usage_update",
        "plan_updated",
        "modes_updated",
        "model_changed",
        "slash_commands_updated",
        "compaction",
    ];
    assert_eq!(
        events.len(),
        expected_tags.len(),
        "21 TurnEvent variants (14 original + 7 informational)"
    );

    let mut seen = std::collections::BTreeSet::new();
    for (ev, expected_tag) in events.iter().zip(expected_tags.iter()) {
        let value = roundtrip_json(ev);
        let tag = value
            .get("type")
            .and_then(|t| t.as_str())
            .expect("TurnEvent must be internally tagged with a `type` string");
        assert_eq!(tag, *expected_tag, "wrong tag for {ev:?}");
        assert!(
            seen.insert(tag.to_string()),
            "duplicate TurnEvent tag: {tag}"
        );
    }
    assert_eq!(seen.len(), 21, "21 distinct snake_case TurnEvent tags");
}

// =====================================================================
// TargetStatus — internally tagged, snake_case; variants from §4.1.
// control-api.md line 75 pins `{"type":"idle","since":"…"}`.
// =====================================================================

#[test]
fn target_status_idle_wire_form() {
    let status = TargetStatus::Idle {
        since: "2026-07-02T12:00:00.000Z".to_string(),
    };
    let value = roundtrip_json(&status);
    assert_eq!(
        value,
        json!({ "type": "idle", "since": "2026-07-02T12:00:00.000Z" })
    );
    let back: TargetStatus = serde_json::from_value(value).expect("deserialize");
    assert_eq!(back, status);
}

#[test]
fn target_status_offline_and_stopped_are_bare_tags() {
    assert_eq!(
        roundtrip_json(&TargetStatus::Offline),
        json!({ "type": "offline" })
    );
    assert_eq!(
        roundtrip_json(&TargetStatus::Starting),
        json!({ "type": "starting" })
    );
    assert_eq!(
        roundtrip_json(&TargetStatus::Stopped),
        json!({ "type": "stopped" })
    );
}

#[test]
fn target_status_busy_wire_form() {
    let status = TargetStatus::Busy {
        active_turn: tid(),
        source: TurnSource::Agent {
            from: AgentId("wasl-b".to_string()),
        },
        phase: BusyPhase::Generating,
        since: "2026-07-02T12:00:00.000Z".to_string(),
        queued: 1,
    };
    let value = roundtrip_json(&status);
    assert_eq!(
        value,
        json!({
            "type": "busy",
            "active_turn": "turn-7",
            "source": { "type": "agent", "from": "wasl-b" },
            "phase": "generating",
            "since": "2026-07-02T12:00:00.000Z",
            "queued": 1
        })
    );
    let back: TargetStatus = serde_json::from_value(value).expect("deserialize");
    assert_eq!(back, status);
}

#[test]
fn target_status_faulted_wire_form() {
    let status = TargetStatus::Faulted {
        error: "child died".to_string(),
        recoverable: false,
    };
    let value = roundtrip_json(&status);
    assert_eq!(
        value,
        json!({ "type": "faulted", "error": "child died", "recoverable": false })
    );
    let back: TargetStatus = serde_json::from_value(value).expect("deserialize");
    assert_eq!(back, status);
}

#[test]
fn target_status_all_nine_variant_tags_are_snake_case_and_distinct() {
    // §4.1 enumerates: Offline, Starting, Idle, Busy, AwaitingGate,
    // Interrupting, Draining, Faulted, Stopped.
    let statuses = vec![
        TargetStatus::Offline,
        TargetStatus::Starting,
        TargetStatus::Idle {
            since: "t".to_string(),
        },
        TargetStatus::Busy {
            active_turn: tid(),
            source: TurnSource::LocalHuman,
            phase: BusyPhase::Dispatching,
            since: "t".to_string(),
            queued: 0,
        },
        TargetStatus::AwaitingGate {
            active_turn: tid(),
            detail: NativeGateDetail {
                summary: String::new(),
                preview: String::new(),
            },
            since: "t".to_string(),
            queued: 0,
        },
        TargetStatus::Interrupting {
            active_turn: tid(),
            since: "t".to_string(),
        },
        TargetStatus::Draining {
            active_turn: tid(),
            since: "t".to_string(),
        },
        TargetStatus::Faulted {
            error: String::new(),
            recoverable: true,
        },
        TargetStatus::Stopped,
    ];
    let expected = [
        "offline",
        "starting",
        "idle",
        "busy",
        "awaiting_gate",
        "interrupting",
        "draining",
        "faulted",
        "stopped",
    ];
    assert_eq!(statuses.len(), expected.len(), "9 TargetStatus variants");
    let mut seen = std::collections::BTreeSet::new();
    for (status, expected_tag) in statuses.iter().zip(expected.iter()) {
        let value = roundtrip_json(status);
        let tag = value
            .get("type")
            .and_then(|t| t.as_str())
            .expect("TargetStatus must be internally tagged");
        assert_eq!(tag, *expected_tag, "wrong tag for {status:?}");
        assert!(seen.insert(tag.to_string()), "dup TargetStatus tag {tag}");
    }
    assert_eq!(seen.len(), 9, "9 distinct snake_case TargetStatus tags");
}

// =====================================================================
// BusyPhase — the six phases (§4.1). Carried inside Busy; snake_case.
// =====================================================================

#[test]
fn busy_phase_six_snake_case_variants_round_trip() {
    let cases = [
        (BusyPhase::Dispatching, "\"dispatching\""),
        (BusyPhase::Generating, "\"generating\""),
        (BusyPhase::RunningTool, "\"running_tool\""),
        (BusyPhase::WaitingForTool, "\"waiting_for_tool\""),
        (BusyPhase::WaitingForInput, "\"waiting_for_input\""),
        (BusyPhase::Completing, "\"completing\""),
    ];
    let mut seen = std::collections::BTreeSet::new();
    for (value, expected) in cases {
        let json_s = serde_json::to_string(&value).expect("serialize BusyPhase");
        assert_eq!(json_s, expected, "BusyPhase wire string for {value:?}");
        let back: BusyPhase = serde_json::from_str(&json_s).expect("deserialize");
        assert_eq!(back, value);
        seen.insert(json_s);
    }
    assert_eq!(seen.len(), 6, "6 distinct BusyPhase tags");
}

// =====================================================================
// TurnSource — {"type":"agent","from":"…"} | {"type":"local_human"} (§3.1;
// control-api.md line 186).
// =====================================================================

#[test]
fn turn_source_agent_wire_form() {
    let src = TurnSource::Agent {
        from: AgentId("wasl-b".to_string()),
    };
    let value = roundtrip_json(&src);
    assert_eq!(value, json!({ "type": "agent", "from": "wasl-b" }));
    let back: TurnSource = serde_json::from_value(value).expect("deserialize");
    assert_eq!(back, src);
}

#[test]
fn turn_source_local_human_wire_form() {
    let src = TurnSource::LocalHuman;
    let value = roundtrip_json(&src);
    assert_eq!(value, json!({ "type": "local_human" }));
    let back: TurnSource = serde_json::from_value(value).expect("deserialize");
    assert_eq!(back, src);
}

// =====================================================================
// TurnReceipt — internally tagged (control-api.md lines 195-199); variants
// from §3 (Started/Queued/RejectedBusy/RejectedUnavailable/Duplicate).
// =====================================================================

#[test]
fn turn_receipt_started_wire_form() {
    // control-api.md line 195: {"type":"started","turn_id":"turn-7","first_seq":13}.
    let receipt = TurnReceipt::Started {
        turn_id: tid(),
        first_seq: wasl_runtime::EventSeq(13),
    };
    let value = roundtrip_json(&receipt);
    assert_eq!(
        value,
        json!({ "type": "started", "turn_id": "turn-7", "first_seq": 13 })
    );
    let back: TurnReceipt = serde_json::from_value(value).expect("deserialize");
    assert_eq!(back, receipt);
}

#[test]
fn turn_receipt_queued_wire_form() {
    let receipt = TurnReceipt::Queued {
        turn_id: tid(),
        position: 3,
    };
    let value = roundtrip_json(&receipt);
    assert_eq!(
        value,
        json!({ "type": "queued", "turn_id": "turn-7", "position": 3 })
    );
    let back: TurnReceipt = serde_json::from_value(value).expect("deserialize");
    assert_eq!(back, receipt);
}

#[test]
fn turn_receipt_rejected_busy_wire_form() {
    let receipt = TurnReceipt::RejectedBusy { active_turn: tid() };
    let value = roundtrip_json(&receipt);
    assert_eq!(
        value,
        json!({ "type": "rejected_busy", "active_turn": "turn-7" })
    );
    let back: TurnReceipt = serde_json::from_value(value).expect("deserialize");
    assert_eq!(back, receipt);
}

#[test]
fn turn_receipt_rejected_unavailable_wire_form() {
    let receipt = TurnReceipt::RejectedUnavailable {
        status: TargetStatus::Offline,
    };
    let value = roundtrip_json(&receipt);
    assert_eq!(
        value,
        json!({ "type": "rejected_unavailable", "status": { "type": "offline" } })
    );
    let back: TurnReceipt = serde_json::from_value(value).expect("deserialize");
    assert_eq!(back, receipt);
}

#[test]
fn turn_receipt_duplicate_wraps_original() {
    // §3 line 240: Duplicate { original: Box<TurnReceipt> } — wraps verbatim.
    let receipt = TurnReceipt::Duplicate {
        original: Box::new(TurnReceipt::Started {
            turn_id: tid(),
            first_seq: wasl_runtime::EventSeq(13),
        }),
    };
    let value = roundtrip_json(&receipt);
    assert_eq!(
        value,
        json!({
            "type": "duplicate",
            "original": { "type": "started", "turn_id": "turn-7", "first_seq": 13 }
        })
    );
    let back: TurnReceipt = serde_json::from_value(value).expect("deserialize");
    assert_eq!(back, receipt);
}

// =====================================================================
// BusyPolicy — {"type":"queue","max_wait_ms":…} etc. (control-api.md line 181;
// §4.2). Durations are integer milliseconds on the wire (§Conventions line 25).
// =====================================================================

#[test]
fn busy_policy_queue_wire_form_uses_integer_ms() {
    let policy = BusyPolicy::Queue {
        max_wait: std::time::Duration::from_millis(30_000),
    };
    let value = roundtrip_json(&policy);
    assert_eq!(value, json!({ "type": "queue", "max_wait_ms": 30000 }));
    let back: BusyPolicy = serde_json::from_value(value).expect("deserialize");
    assert_eq!(back, policy);
}

#[test]
fn busy_policy_reject_wire_form() {
    let policy = BusyPolicy::Reject;
    let value = roundtrip_json(&policy);
    assert_eq!(value, json!({ "type": "reject" }));
    let back: BusyPolicy = serde_json::from_value(value).expect("deserialize");
    assert_eq!(back, policy);
}

#[test]
fn busy_policy_queue_related_wire_form_uses_integer_ms() {
    let policy = BusyPolicy::QueueRelated {
        max_wait: std::time::Duration::from_millis(120_000),
    };
    let value = roundtrip_json(&policy);
    assert_eq!(
        value,
        json!({ "type": "queue_related", "max_wait_ms": 120000 })
    );
    let back: BusyPolicy = serde_json::from_value(value).expect("deserialize");
    assert_eq!(back, policy);
}

// =====================================================================
// NativeGateDetail — serde {summary, preview} (§4.1 line 314).
// =====================================================================

#[test]
fn native_gate_detail_wire_form_round_trips() {
    let detail = NativeGateDetail {
        summary: "allow write?".to_string(),
        preview: "echo hi > f".to_string(),
    };
    let value = roundtrip_json(&detail);
    assert_eq!(
        value,
        json!({ "summary": "allow write?", "preview": "echo hi > f" })
    );
    let back: NativeGateDetail = serde_json::from_value(value).expect("deserialize");
    assert_eq!(back, detail);
}

// =====================================================================
// Causation — root_request/parent_request as BARE STRINGS (RequestId is
// #[serde(transparent)]); path as an array of AgentId strings; hop int.
// control-api.md lines 182,190; runtime.md §8.2.
// =====================================================================

#[test]
fn causation_root_and_parent_serialize_as_bare_strings() {
    let causation = Causation {
        root_request: RequestId("req-1".to_string()),
        parent_request: Some(RequestId("req-0".to_string())),
        path: vec![AgentId("wasl-a".to_string()), AgentId("wasl-b".to_string())],
        hop: 2,
    };
    let value = roundtrip_json(&causation);
    assert_eq!(
        value,
        json!({
            "root_request": "req-1",
            "parent_request": "req-0",
            "path": ["wasl-a", "wasl-b"],
            "hop": 2
        }),
        "RequestId is transparent → bare JSON strings; path is array of AgentId \
         strings (control-api.md line 182,190)"
    );
    let back: Causation = serde_json::from_value(value).expect("deserialize");
    assert_eq!(back, causation);
}

#[test]
fn causation_root_turn_has_null_parent_and_empty_path_hop_zero() {
    // control-api.md line 182: root turns carry `parent_request: null`, `path: []`,
    // `hop: 0`.
    let causation = Causation {
        root_request: RequestId("req-1".to_string()),
        parent_request: None,
        path: vec![],
        hop: 0,
    };
    let value = roundtrip_json(&causation);
    assert_eq!(
        value,
        json!({
            "root_request": "req-1",
            "parent_request": null,
            "path": [],
            "hop": 0
        })
    );
    let back: Causation = serde_json::from_value(value).expect("deserialize");
    assert_eq!(back, causation);
}

// =====================================================================
// New informational TurnEvent variants — wire-form round-trip tests.
// Each test pins the exact snake_case tag + field names + round-trip.
// =====================================================================

#[test]
fn turn_event_thinking_delta_wire_form() {
    assert_turn_event_roundtrips(
        &TurnEvent::ThinkingDelta {
            text: "hmm...".to_string(),
        },
        json!({ "type": "thinking_delta", "text": "hmm..." }),
    );
}

#[test]
fn turn_event_usage_update_wire_form_with_context_pct() {
    let ev = TurnEvent::UsageUpdate {
        usage: UsageSnapshot {
            input_tokens: 100,
            output_tokens: 50,
            cache_read_tokens: 20,
            cache_creation_tokens: 5,
            cost_usd: 0.002,
        },
        context_pct: Some(42),
    };
    assert_turn_event_roundtrips(
        &ev,
        json!({
            "type": "usage_update",
            "usage": {
                "input_tokens": 100,
                "output_tokens": 50,
                "cache_read_tokens": 20,
                "cache_creation_tokens": 5,
                "cost_usd": 0.002
            },
            "context_pct": 42
        }),
    );
}

#[test]
fn turn_event_usage_update_wire_form_no_context_pct() {
    let ev = TurnEvent::UsageUpdate {
        usage: UsageSnapshot::default(),
        context_pct: None,
    };
    let value = roundtrip_json(&ev);
    // context_pct is None → skip_serializing_if → absent from JSON.
    assert_eq!(
        value.get("context_pct"),
        None,
        "context_pct is omitted when None"
    );
    assert_eq!(
        value.get("type").and_then(|t| t.as_str()),
        Some("usage_update")
    );
    let back: TurnEvent = serde_json::from_value(value).expect("deserialize");
    assert_eq!(back, ev);
}

#[test]
fn turn_event_plan_updated_wire_form() {
    let ev = TurnEvent::PlanUpdated {
        entries: vec![
            PlanEntry {
                content: "step 1".to_string(),
                priority: "high".to_string(),
            },
            PlanEntry {
                content: "step 2".to_string(),
                priority: String::new(),
            },
        ],
    };
    assert_turn_event_roundtrips(
        &ev,
        json!({
            "type": "plan_updated",
            "entries": [
                { "content": "step 1", "priority": "high" },
                { "content": "step 2", "priority": "" }
            ]
        }),
    );
}

#[test]
fn turn_event_modes_updated_wire_form() {
    let ev = TurnEvent::ModesUpdated {
        available_modes: vec![
            AgentMode {
                id: "plan".to_string(),
                label: "Plan mode".to_string(),
                description: "Review before acting".to_string(),
            },
            AgentMode {
                id: "default".to_string(),
                label: "Default".to_string(),
                description: String::new(),
            },
        ],
        current_mode: Some("plan".to_string()),
    };
    assert_turn_event_roundtrips(
        &ev,
        json!({
            "type": "modes_updated",
            "available_modes": [
                { "id": "plan", "label": "Plan mode", "description": "Review before acting" },
                { "id": "default", "label": "Default", "description": "" }
            ],
            "current_mode": "plan"
        }),
    );
}

#[test]
fn turn_event_modes_updated_empty_modes_omitted_from_json() {
    // empty available_modes → skip_serializing_if → absent; only current_mode present.
    let ev = TurnEvent::ModesUpdated {
        available_modes: vec![],
        current_mode: Some("default".to_string()),
    };
    let value = roundtrip_json(&ev);
    assert_eq!(
        value.get("available_modes"),
        None,
        "empty available_modes is omitted"
    );
    assert_eq!(value["current_mode"], json!("default"));
    let back: TurnEvent = serde_json::from_value(value).expect("deserialize");
    assert_eq!(back, ev);
}

#[test]
fn turn_event_model_changed_wire_form() {
    assert_turn_event_roundtrips(
        &TurnEvent::ModelChanged {
            model: "claude-sonnet-4.6".to_string(),
        },
        json!({ "type": "model_changed", "model": "claude-sonnet-4.6" }),
    );
}

#[test]
fn turn_event_slash_commands_updated_wire_form() {
    let ev = TurnEvent::SlashCommandsUpdated {
        commands: vec![
            SlashCommandInfo {
                name: "/help".to_string(),
                description: "Show help".to_string(),
                argument_hint: "<topic>".to_string(),
            },
            SlashCommandInfo {
                name: "/compact".to_string(),
                description: String::new(),
                argument_hint: String::new(),
            },
        ],
    };
    assert_turn_event_roundtrips(
        &ev,
        json!({
            "type": "slash_commands_updated",
            "commands": [
                { "name": "/help", "description": "Show help", "argument_hint": "<topic>" },
                { "name": "/compact", "description": "", "argument_hint": "" }
            ]
        }),
    );
}

#[test]
fn turn_event_compaction_wire_form() {
    let ev = TurnEvent::Compaction {
        info: CompactionInfo {
            trigger: "manual".to_string(),
            pre_tokens: 50_000,
            post_tokens: 12_000,
            cumulative_dropped_tokens: 38_000,
            duration_ms: 1_200,
        },
    };
    assert_turn_event_roundtrips(
        &ev,
        json!({
            "type": "compaction",
            "info": {
                "trigger": "manual",
                "pre_tokens": 50000,
                "post_tokens": 12000,
                "cumulative_dropped_tokens": 38000,
                "duration_ms": 1200
            }
        }),
    );
}
