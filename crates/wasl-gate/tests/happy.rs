//! Black-box happy-path tests for `wasl_gate` per docs/specs/gate.md §Test plan (G1-G9).

use wasl_gate::{
    build_request, choose_mode, elicitation_for, outcome_of, ClientCaps, GateLedger, GateMode,
    GateOutcome,
};
use wasl_proto::{AgentId, ElicitationAction, ElicitationResponse, GateDecision, GateKind};

// ---------------------------------------------------------------------
// G1 — choose_mode priority: elicitation > marsad > prepare_commit
// ---------------------------------------------------------------------

#[test]
fn g1_choose_mode_elicitation_only() {
    let caps = ClientCaps {
        elicitation: true,
        marsad: false,
    };
    assert_eq!(choose_mode(&caps), GateMode::Elicitation);
}

#[test]
fn g1_choose_mode_elicitation_and_marsad_elicitation_wins() {
    let caps = ClientCaps {
        elicitation: true,
        marsad: true,
    };
    assert_eq!(choose_mode(&caps), GateMode::Elicitation);
}

#[test]
fn g1_choose_mode_marsad_only() {
    let caps = ClientCaps {
        elicitation: false,
        marsad: true,
    };
    assert_eq!(choose_mode(&caps), GateMode::Marsad);
}

#[test]
fn g1_choose_mode_neither_is_prepare_commit() {
    let caps = ClientCaps {
        elicitation: false,
        marsad: false,
    };
    assert_eq!(choose_mode(&caps), GateMode::PrepareCommit);
}

#[test]
fn g1_gate_mode_serde_snake_case_round_trip() {
    let cases = [
        (GateMode::Elicitation, "\"elicitation\""),
        (GateMode::Marsad, "\"marsad\""),
        (GateMode::PrepareCommit, "\"prepare_commit\""),
    ];
    for (mode, expected_json) in cases {
        let json = serde_json::to_string(&mode).expect("serialize GateMode");
        assert_eq!(json, expected_json, "serialized form for {mode:?}");
        let back: GateMode = serde_json::from_str(&json).expect("deserialize GateMode");
        assert_eq!(back, mode, "round-trip for {mode:?}");
    }
}

// ---------------------------------------------------------------------
// G2 — ClientCaps serde round-trips
// ---------------------------------------------------------------------

#[test]
fn g2_client_caps_serde_round_trip_all_combos() {
    for elicitation in [true, false] {
        for marsad in [true, false] {
            let caps = ClientCaps { elicitation, marsad };
            let json = serde_json::to_string(&caps).expect("serialize ClientCaps");
            let back: ClientCaps = serde_json::from_str(&json).expect("deserialize ClientCaps");
            assert_eq!(back, caps, "round-trip for {caps:?}");
        }
    }
}

#[test]
fn g2_client_caps_serde_field_names() {
    let caps = ClientCaps {
        elicitation: true,
        marsad: false,
    };
    let value: serde_json::Value = serde_json::to_value(caps).expect("to_value");
    assert_eq!(value["elicitation"], serde_json::json!(true));
    assert_eq!(value["marsad"], serde_json::json!(false));
}

// ---------------------------------------------------------------------
// G3 — build_request: summary verb+count per GateKind, preview verbatim, id/kind/from carried through
// ---------------------------------------------------------------------

fn agent(id: &str) -> AgentId {
    AgentId(id.to_string())
}

fn targets(n: usize) -> Vec<String> {
    (0..n).map(|i| format!("peer-{i}")).collect()
}

#[test]
fn g3_build_request_consult_summary() {
    let one = build_request(
        "req-1".to_string(),
        GateKind::Consult,
        agent("alice"),
        &targets(1),
        "preview text".to_string(),
    );
    assert_eq!(one.summary, "consult 1 peer(s)");

    let three = build_request(
        "req-2".to_string(),
        GateKind::Consult,
        agent("alice"),
        &targets(3),
        "preview text".to_string(),
    );
    assert_eq!(three.summary, "consult 3 peer(s)");
}

#[test]
fn g3_build_request_command_summary() {
    let one = build_request(
        "req-1".to_string(),
        GateKind::Command,
        agent("bob"),
        &targets(1),
        "do the thing".to_string(),
    );
    assert_eq!(one.summary, "command 1 peer(s)");

    let three = build_request(
        "req-2".to_string(),
        GateKind::Command,
        agent("bob"),
        &targets(3),
        "do the thing".to_string(),
    );
    assert_eq!(three.summary, "command 3 peer(s)");
}

#[test]
fn g3_build_request_message_summary() {
    let one = build_request(
        "req-1".to_string(),
        GateKind::Message,
        agent("carol"),
        &targets(1),
        "hello".to_string(),
    );
    assert_eq!(one.summary, "message to 1 peer(s)");

    let three = build_request(
        "req-2".to_string(),
        GateKind::Message,
        agent("carol"),
        &targets(3),
        "hello".to_string(),
    );
    assert_eq!(three.summary, "message to 3 peer(s)");
}

#[test]
fn g3_build_request_transfer_summary() {
    let one = build_request(
        "req-1".to_string(),
        GateKind::Transfer,
        agent("dave"),
        &targets(1),
        "/path/to/file".to_string(),
    );
    assert_eq!(one.summary, "transfer to 1 peer(s)");

    let three = build_request(
        "req-2".to_string(),
        GateKind::Transfer,
        agent("dave"),
        &targets(3),
        "/path/to/file".to_string(),
    );
    assert_eq!(three.summary, "transfer to 3 peer(s)");
}

#[test]
fn g3_build_request_preview_stored_verbatim() {
    let preview = "line one\nline two -- verbatim, no mutation!".to_string();
    let req = build_request(
        "req-x".to_string(),
        GateKind::Message,
        agent("erin"),
        &targets(2),
        preview.clone(),
    );
    assert_eq!(req.preview, preview);
}

#[test]
fn g3_build_request_id_kind_from_carried_through() {
    let req = build_request(
        "req-carry".to_string(),
        GateKind::Transfer,
        agent("frank"),
        &targets(2),
        "preview".to_string(),
    );
    assert_eq!(req.id, "req-carry");
    assert_eq!(req.kind, GateKind::Transfer);
    assert_eq!(req.from, agent("frank"));
}

// ---------------------------------------------------------------------
// G4 — elicitation_for: message contains summary + preview + ends with approval prompt
// ---------------------------------------------------------------------

#[test]
fn g4_elicitation_for_message_contents_and_shape() {
    let req = build_request(
        "req-e".to_string(),
        GateKind::Message,
        agent("gina"),
        &targets(2),
        "the preview body".to_string(),
    );
    let elicitation = elicitation_for(&req);

    assert!(
        elicitation.message.contains(&req.summary),
        "message should contain the summary: {}",
        elicitation.message
    );
    assert!(
        elicitation.message.contains(&req.preview),
        "message should contain the preview: {}",
        elicitation.message
    );
    assert!(
        elicitation.message.ends_with("Approve?"),
        "message should end with the approval prompt: {}",
        elicitation.message
    );

    let expected = format!("{}\n{}\n\nApprove?", req.summary, req.preview);
    assert_eq!(elicitation.message, expected);
}

// ---------------------------------------------------------------------
// G5 — outcome_of: Accept→Approved; Decline→Denied("declined"); Cancel→Dismissed
// ---------------------------------------------------------------------

#[test]
fn g5_outcome_of_accept_is_approved() {
    let resp = ElicitationResponse {
        action: ElicitationAction::Accept,
    };
    assert_eq!(outcome_of(&resp), GateOutcome::Approved);
}

#[test]
fn g5_outcome_of_decline_is_denied_declined() {
    let resp = ElicitationResponse {
        action: ElicitationAction::Decline,
    };
    assert_eq!(
        outcome_of(&resp),
        GateOutcome::Denied {
            reason: "declined".to_string()
        }
    );
}

#[test]
fn g5_outcome_of_cancel_is_dismissed() {
    let resp = ElicitationResponse {
        action: ElicitationAction::Cancel,
    };
    assert_eq!(outcome_of(&resp), GateOutcome::Dismissed);
}

// ---------------------------------------------------------------------
// G6 — GateOutcome::to_decision mapping
// ---------------------------------------------------------------------

#[test]
fn g6_to_decision_approved_maps_to_some_approve() {
    let outcome = GateOutcome::Approved;
    assert_eq!(outcome.to_decision(), Some(GateDecision::Approve));
}

#[test]
fn g6_to_decision_denied_maps_to_some_deny_with_reason() {
    let outcome = GateOutcome::Denied {
        reason: "declined".to_string(),
    };
    assert_eq!(
        outcome.to_decision(),
        Some(GateDecision::Deny {
            reason: "declined".to_string()
        })
    );
}

#[test]
fn g6_to_decision_denied_preserves_custom_reason() {
    let outcome = GateOutcome::Denied {
        reason: "policy violation".to_string(),
    };
    assert_eq!(
        outcome.to_decision(),
        Some(GateDecision::Deny {
            reason: "policy violation".to_string()
        })
    );
}

#[test]
fn g6_to_decision_dismissed_maps_to_none() {
    let outcome = GateOutcome::Dismissed;
    assert_eq!(outcome.to_decision(), None);
}

// ---------------------------------------------------------------------
// G7 — ledger happy path
// ---------------------------------------------------------------------

#[test]
fn g7_ledger_prepare_peek_commit_approve_happy_path() {
    let mut ledger = GateLedger::new();
    let token = wasl_proto::ApprovalToken("tok-1".to_string());
    let req = build_request(
        "req-1".to_string(),
        GateKind::Consult,
        agent("henry"),
        &targets(1),
        "preview".to_string(),
    );

    ledger.prepare(token.clone(), req.clone());

    assert!(ledger.is_pending(&token));
    assert_eq!(ledger.pending_count(), 1);
    assert_eq!(ledger.peek(&token), Some(&req));

    let result = ledger.commit(&token, GateDecision::Approve);
    assert_eq!(result, Ok((req, GateDecision::Approve)));

    assert!(!ledger.is_pending(&token));
    assert_eq!(ledger.pending_count(), 0);
}

// ---------------------------------------------------------------------
// G8 — ledger re-prepare overwrites, no error
// ---------------------------------------------------------------------

#[test]
fn g8_ledger_reprepare_same_token_overwrites() {
    let mut ledger = GateLedger::new();
    let token = wasl_proto::ApprovalToken("tok-reprepare".to_string());

    let req_v1 = build_request(
        "req-v1".to_string(),
        GateKind::Consult,
        agent("iris"),
        &targets(1),
        "first version".to_string(),
    );
    let req_v2 = build_request(
        "req-v2".to_string(),
        GateKind::Command,
        agent("iris"),
        &targets(2),
        "second version".to_string(),
    );

    ledger.prepare(token.clone(), req_v1.clone());
    assert_eq!(ledger.pending_count(), 1);
    assert_eq!(ledger.peek(&token), Some(&req_v1));

    ledger.prepare(token.clone(), req_v2.clone());
    assert_eq!(ledger.pending_count(), 1, "overwrite must not add an entry");
    assert_eq!(ledger.peek(&token), Some(&req_v2), "peek must show the new request");
    assert!(ledger.is_pending(&token));
}

// ---------------------------------------------------------------------
// G9 — ledger tracks distinct tokens independently
// ---------------------------------------------------------------------

#[test]
fn g9_ledger_two_distinct_tokens_independent() {
    let mut ledger = GateLedger::new();
    let token_a = wasl_proto::ApprovalToken("tok-a".to_string());
    let token_b = wasl_proto::ApprovalToken("tok-b".to_string());

    let req_a = build_request(
        "req-a".to_string(),
        GateKind::Consult,
        agent("jack"),
        &targets(1),
        "preview a".to_string(),
    );
    let req_b = build_request(
        "req-b".to_string(),
        GateKind::Transfer,
        agent("kate"),
        &targets(2),
        "preview b".to_string(),
    );

    ledger.prepare(token_a.clone(), req_a.clone());
    ledger.prepare(token_b.clone(), req_b.clone());

    assert_eq!(ledger.pending_count(), 2);
    assert!(ledger.is_pending(&token_a));
    assert!(ledger.is_pending(&token_b));

    let result = ledger.commit(&token_a, GateDecision::Approve);
    assert_eq!(result, Ok((req_a, GateDecision::Approve)));

    assert!(!ledger.is_pending(&token_a), "committed token no longer pending");
    assert!(ledger.is_pending(&token_b), "other token remains pending");
    assert_eq!(ledger.pending_count(), 1);
    assert_eq!(ledger.peek(&token_b), Some(&req_b));
}
