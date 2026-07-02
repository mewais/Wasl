//! `tests/hub.rs` — UNIT tests for the `OperatorHub` core: no HTTP, no tokio
//! runtime. Mirrors the spec's §Test plan H1–H13 (`docs/specs/operator.md`).
//! Uses a settable fake `Clock` (for deterministic wire timestamps) and the
//! crate's own `CounterGateIds` (for deterministic gate ids), draining
//! `hub.subscribe()` receivers with `try_recv`.
//!
//! Test-first: this MUST fail to compile until `src/` defines the public API
//! (`OperatorHub`, `Frame`, `SeqFrame`, `DecisionBody`, `DecideError`, ...).

mod common;

use std::sync::Arc;
use std::time::Duration;

use serde_json::json;

use common::{
    at, fresh_hub_with_clock, hub_with_cfg, node_info, req_call10, req_call9, FailingSink,
    FakeClock, OrderCheckingSink, RecordingSink, T0_MS,
};

use wasl_audit::{AuditEntry, EntryKind, Sink};
use wasl_gate::{GateMode, GateOutcome};
use wasl_proto::AgentId;

use wasl_operator::{
    DecideError, DecidedBy, DecisionBody, DecisionOutcome, Frame, HubConfig, OperatorHub, SeqFrame,
    TeeSink,
};

// ── H1: open -> decide approved: the full blocking rendezvous ─────────────

#[test]
fn h1_open_decide_approved_full_rendezvous() {
    let clock = Arc::new(FakeClock::new(at(T0_MS)));
    let hub = fresh_hub_with_clock(clock);
    let (last_seq0, mut rx) = hub.subscribe();
    assert_eq!(last_seq0, 0);

    let w = hub.open_gate(GateMode::External, &req_call9());

    let f1 = rx.try_recv().expect("gate_requested frame");
    assert_eq!(f1.seq, 1);
    match f1.frame {
        Frame::GateRequested {
            gate_id,
            mode,
            decidable,
            request,
            requested_at,
            timeout_ms,
            deadline,
        } => {
            assert_eq!(gate_id, "gate-1");
            assert_eq!(mode, GateMode::External);
            assert!(decidable);
            assert_eq!(request, req_call9());
            assert_eq!(requested_at, "2026-07-02T12:00:00.000Z");
            assert_eq!(timeout_ms, 120_000);
            assert_eq!(deadline, "2026-07-02T12:02:00.000Z");
        }
        other => panic!("expected GateRequested, got {other:?}"),
    }

    // Run `wait` on its own thread so the rendezvous genuinely crosses
    // threads (the sync/async bridge this crate exists for).
    let hub_for_wait = hub.clone();
    let handle = std::thread::spawn(move || hub_for_wait.wait(w));
    std::thread::sleep(Duration::from_millis(20));

    hub.decide(
        "gate-1",
        DecisionBody {
            outcome: DecisionOutcome::Approved,
            reason: None,
        },
    )
    .expect("decide ok");

    let outcome = handle.join().expect("wait thread panicked");
    assert_eq!(outcome, GateOutcome::Approved);

    let f2 = rx.try_recv().expect("gate_decided frame");
    assert_eq!(f2.seq, 2);
    match f2.frame {
        Frame::GateDecided {
            gate_id,
            outcome,
            decided_by,
            reason,
            decided_at,
        } => {
            assert_eq!(gate_id, "gate-1");
            assert_eq!(outcome, DecisionOutcome::Approved);
            assert_eq!(decided_by, DecidedBy::Operator);
            assert_eq!(reason, None);
            assert_eq!(decided_at, "2026-07-02T12:00:00.000Z");
        }
        other => panic!("expected GateDecided, got {other:?}"),
    }

    let snap = serde_json::to_value(hub.snapshot()).unwrap();
    assert!(snap["gates"].as_array().unwrap().is_empty());
}

// ── H2 / H2b: deny with and without a reason ───────────────────────────────

#[test]
fn h2_deny_with_reason_carries_reason_on_frame() {
    let hub = fresh_hub_with_clock(Arc::new(FakeClock::new(at(T0_MS))));
    let (_last, mut rx) = hub.subscribe();
    let _w = hub.open_gate(GateMode::External, &req_call9());
    rx.try_recv().expect("gate_requested"); // discard

    hub.decide(
        "gate-1",
        DecisionBody {
            outcome: DecisionOutcome::Denied,
            reason: Some("not during business hours".to_string()),
        },
    )
    .expect("decide ok");

    let f = rx.try_recv().expect("gate_decided").frame;
    match f {
        Frame::GateDecided {
            outcome, reason, ..
        } => {
            assert_eq!(outcome, DecisionOutcome::Denied);
            assert_eq!(reason, Some("not during business hours".to_string()));
        }
        other => panic!("expected GateDecided, got {other:?}"),
    }
}

#[test]
fn h2b_deny_without_reason_defaults_to_denied_by_operator() {
    let hub = fresh_hub_with_clock(Arc::new(FakeClock::new(at(T0_MS))));
    let (_last, mut rx) = hub.subscribe();
    let _w = hub.open_gate(GateMode::External, &req_call9());
    rx.try_recv().expect("gate_requested");

    hub.decide(
        "gate-1",
        DecisionBody {
            outcome: DecisionOutcome::Denied,
            reason: None,
        },
    )
    .expect("decide ok");

    let f = rx.try_recv().expect("gate_decided").frame;
    match f {
        Frame::GateDecided { reason, .. } => {
            assert_eq!(reason, Some("denied by operator".to_string()));
        }
        other => panic!("expected GateDecided, got {other:?}"),
    }
}

// ── H3: POST-dismiss resolves `wait` with decided_by = operator ───────────

#[test]
fn h3_post_dismiss_resolves_wait_with_decided_by_operator() {
    let hub = fresh_hub_with_clock(Arc::new(FakeClock::new(at(T0_MS))));
    let (_last, mut rx) = hub.subscribe();
    let w = hub.open_gate(GateMode::External, &req_call9());
    rx.try_recv().expect("gate_requested");

    let hub_for_wait = hub.clone();
    let handle = std::thread::spawn(move || hub_for_wait.wait(w));
    std::thread::sleep(Duration::from_millis(20));

    hub.decide(
        "gate-1",
        DecisionBody {
            outcome: DecisionOutcome::Dismissed,
            reason: None,
        },
    )
    .expect("decide ok");

    let outcome = handle.join().expect("wait thread panicked");
    assert_eq!(outcome, GateOutcome::Dismissed);

    let f = rx.try_recv().expect("gate_decided").frame;
    match f {
        Frame::GateDecided {
            outcome,
            decided_by,
            ..
        } => {
            assert_eq!(outcome, DecisionOutcome::Dismissed);
            // Distinguishes a POST-dismiss from a timeout-dismiss (H4).
            assert_eq!(decided_by, DecidedBy::Operator);
        }
        other => panic!("expected GateDecided, got {other:?}"),
    }
}

// ── H4: timeout dismisses, decided_by = timeout ────────────────────────────

#[test]
fn h4_timeout_dismisses_with_decided_by_timeout() {
    let clock = Arc::new(FakeClock::new(at(T0_MS)));
    let cfg = HubConfig {
        decision_timeout: Duration::from_millis(20),
        channel_capacity: 256,
    };
    let hub = hub_with_cfg(clock, cfg);
    let (_last, mut rx) = hub.subscribe();
    let w = hub.open_gate(GateMode::External, &req_call9());
    rx.try_recv().expect("gate_requested");

    // No `decide` call: real time elapses past the (short) decision_timeout.
    let outcome = hub.wait(w);
    assert_eq!(outcome, GateOutcome::Dismissed);

    let f = rx.try_recv().expect("gate_decided").frame;
    match f {
        Frame::GateDecided {
            gate_id,
            outcome,
            decided_by,
            reason,
            ..
        } => {
            assert_eq!(gate_id, "gate-1");
            assert_eq!(outcome, DecisionOutcome::Dismissed);
            assert_eq!(decided_by, DecidedBy::Timeout);
            assert_eq!(reason, None);
        }
        other => panic!("expected GateDecided, got {other:?}"),
    }
}

// ── H5: decide after timeout -> AlreadyDecided ─────────────────────────────

#[test]
fn h5_decide_after_timeout_is_already_decided() {
    let clock = Arc::new(FakeClock::new(at(T0_MS)));
    let cfg = HubConfig {
        decision_timeout: Duration::from_millis(20),
        channel_capacity: 256,
    };
    let hub = hub_with_cfg(clock, cfg);
    let w = hub.open_gate(GateMode::External, &req_call9());
    let outcome = hub.wait(w);
    assert_eq!(outcome, GateOutcome::Dismissed);

    let err = hub
        .decide(
            "gate-1",
            DecisionBody {
                outcome: DecisionOutcome::Approved,
                reason: None,
            },
        )
        .unwrap_err();
    assert_eq!(err, DecideError::AlreadyDecided);
}

// ── H6: decide on an unknown gate id -> UnknownGate ────────────────────────

#[test]
fn h6_decide_unknown_gate_id_errors() {
    let hub = fresh_hub_with_clock(Arc::new(FakeClock::new(at(T0_MS))));
    let err = hub
        .decide(
            "gate-999",
            DecisionBody {
                outcome: DecisionOutcome::Approved,
                reason: None,
            },
        )
        .unwrap_err();
    assert_eq!(err, DecideError::UnknownGate);
}

// ── H7: double decide -> second call AlreadyDecided ────────────────────────

#[test]
fn h7_double_decide_second_call_already_decided() {
    let hub = fresh_hub_with_clock(Arc::new(FakeClock::new(at(T0_MS))));
    let _w = hub.open_gate(GateMode::External, &req_call9());
    hub.decide(
        "gate-1",
        DecisionBody {
            outcome: DecisionOutcome::Approved,
            reason: None,
        },
    )
    .expect("first decide ok");

    let err = hub
        .decide(
            "gate-1",
            DecisionBody {
                outcome: DecisionOutcome::Denied,
                reason: None,
            },
        )
        .unwrap_err();
    assert_eq!(err, DecideError::AlreadyDecided);
}

// ── H8: the `wait` race arm's constituents, exercised deterministically ───

#[test]
fn h8_timeout_gate_after_decide_is_already_decided_and_outcome_is_buffered() {
    let hub = fresh_hub_with_clock(Arc::new(FakeClock::new(at(T0_MS))));
    let w = hub.open_gate(GateMode::External, &req_call9());

    hub.decide(
        "gate-1",
        DecisionBody {
            outcome: DecisionOutcome::Approved,
            reason: None,
        },
    )
    .expect("decide ok");

    // `decide` sends on the waiter channel and broadcasts while holding the
    // pending-table lock, so a timeout landing after it must see the gate
    // already resolved...
    let err = hub.timeout_gate("gate-1").unwrap_err();
    assert_eq!(err, DecideError::AlreadyDecided);

    // ...and the buffered outcome must be immediately recv-able (the
    // invariant the `wait` race arm relies on to be safe).
    let outcome = hub.wait(w);
    assert_eq!(outcome, GateOutcome::Approved);
}

// ── H9: announce / resolve_announced (observe-only mirror) ────────────────

#[test]
fn h9_announce_is_observe_only_and_resolve_announced_maps_outcomes() {
    let hub = fresh_hub_with_clock(Arc::new(FakeClock::new(at(T0_MS))));
    let (_last, mut rx) = hub.subscribe();

    let gid = hub.announce(GateMode::Elicitation, &req_call10());
    let f = rx.try_recv().expect("gate_requested (announced)").frame;
    match f {
        Frame::GateRequested {
            gate_id,
            mode,
            decidable,
            ..
        } => {
            assert_eq!(gate_id, gid);
            assert_eq!(mode, GateMode::Elicitation);
            assert!(!decidable, "announced gates must be decidable: false");
        }
        other => panic!("expected GateRequested, got {other:?}"),
    }

    // Never appears in the pending snapshot.
    let snap = serde_json::to_value(hub.snapshot()).unwrap();
    assert!(snap["gates"].as_array().unwrap().is_empty());

    // Deciding it is impossible: it never entered `pending`.
    let err = hub
        .decide(
            &gid,
            DecisionBody {
                outcome: DecisionOutcome::Approved,
                reason: None,
            },
        )
        .unwrap_err();
    assert_eq!(err, DecideError::UnknownGate);

    // resolve_announced: Approved.
    hub.resolve_announced(&gid, &GateOutcome::Approved, DecidedBy::Elicitation);
    let f2 = rx.try_recv().expect("gate_decided").frame;
    match f2 {
        Frame::GateDecided {
            gate_id,
            outcome,
            decided_by,
            reason,
            ..
        } => {
            assert_eq!(gate_id, gid);
            assert_eq!(outcome, DecisionOutcome::Approved);
            assert_eq!(decided_by, DecidedBy::Elicitation);
            assert_eq!(reason, None);
        }
        other => panic!("expected GateDecided, got {other:?}"),
    }

    // resolve_announced: Denied{reason}.
    let gid2 = hub.announce(GateMode::Elicitation, &req_call10());
    rx.try_recv().expect("gate_requested #2");
    hub.resolve_announced(
        &gid2,
        &GateOutcome::Denied {
            reason: "declined".to_string(),
        },
        DecidedBy::Elicitation,
    );
    let f3 = rx.try_recv().expect("gate_decided #2").frame;
    match f3 {
        Frame::GateDecided {
            outcome, reason, ..
        } => {
            assert_eq!(outcome, DecisionOutcome::Denied);
            assert_eq!(reason, Some("declined".to_string()));
        }
        other => panic!("expected GateDecided, got {other:?}"),
    }

    // resolve_announced: Dismissed.
    let gid3 = hub.announce(GateMode::Elicitation, &req_call10());
    rx.try_recv().expect("gate_requested #3");
    hub.resolve_announced(&gid3, &GateOutcome::Dismissed, DecidedBy::Elicitation);
    let f4 = rx.try_recv().expect("gate_decided #3").frame;
    match f4 {
        Frame::GateDecided {
            outcome, reason, ..
        } => {
            assert_eq!(outcome, DecisionOutcome::Dismissed);
            assert_eq!(reason, None);
        }
        other => panic!("expected GateDecided, got {other:?}"),
    }
}

// ── H10: snapshot ordering + age_ms ─────────────────────────────────────────

#[test]
fn h10_snapshot_orders_by_requested_at_then_gate_id_with_correct_age() {
    let clock = Arc::new(FakeClock::new(at(T0_MS)));
    let hub = fresh_hub_with_clock(clock.clone());
    let (last_seq0, mut rx) = hub.subscribe();
    assert_eq!(last_seq0, 0);

    let _w1 = hub.open_gate(GateMode::External, &req_call9()); // gate-1 @ T0
    rx.try_recv().expect("gate_requested #1");

    clock.set(at(T0_MS + 1_000));
    let _w2 = hub.open_gate(GateMode::External, &req_call10()); // gate-2 @ T0+1s
    rx.try_recv().expect("gate_requested #2");

    clock.set(at(T0_MS + 2_500));
    let snap = serde_json::to_value(hub.snapshot()).unwrap();
    let gates = snap["gates"].as_array().unwrap();
    assert_eq!(gates.len(), 2);
    assert_eq!(gates[0]["gate_id"], json!("gate-1"));
    assert_eq!(gates[0]["age_ms"], json!(2_500));
    assert_eq!(gates[1]["gate_id"], json!("gate-2"));
    assert_eq!(gates[1]["age_ms"], json!(1_500));
    assert_eq!(snap["last_seq"], json!(2));

    // Tie-break by gate_id when requested_at is identical.
    let _w3 = hub.open_gate(GateMode::External, &req_call9()); // gate-3, same instant
    rx.try_recv().expect("gate_requested #3");
    let _w4 = hub.open_gate(GateMode::External, &req_call9()); // gate-4, same instant
    rx.try_recv().expect("gate_requested #4");

    let snap2 = serde_json::to_value(hub.snapshot()).unwrap();
    let gates2 = snap2["gates"].as_array().unwrap();
    assert_eq!(gates2.len(), 4);
    assert_eq!(gates2[2]["gate_id"], json!("gate-3"));
    assert_eq!(gates2[3]["gate_id"], json!("gate-4"));
}

// ── H11: TeeSink — durable-append-then-broadcast ────────────────────────────

#[test]
fn h11a_tee_sink_broadcasts_only_after_durable_append_succeeds() {
    let hub = fresh_hub_with_clock(Arc::new(FakeClock::new(at(T0_MS))));
    let (_last, rx) = hub.subscribe();
    let rx = Arc::new(std::sync::Mutex::new(rx));
    let sink = OrderCheckingSink::new(rx.clone());
    let mut tee = TeeSink::new(sink, hub.clone());

    let entry = AuditEntry {
        seq: 1,
        kind: EntryKind::Message,
        actor: AgentId("wasl-a".to_string()),
        targets: vec![AgentId("wasl-b".to_string())],
        summary: "message to 1 peer(s)".to_string(),
        decision: None,
    };
    tee.append(&entry).expect("append ok");

    // The order-checking sink already asserted nothing was broadcast DURING
    // its own append(); now confirm the frame arrives AFTER append returns.
    let seq_frame = rx.lock().unwrap().try_recv().expect("frame after append");
    match seq_frame.frame {
        Frame::Audit { entry: got } => assert_eq!(got, entry),
        other => panic!("expected Audit, got {other:?}"),
    }
}

#[test]
fn h11b_tee_sink_inner_error_propagates_and_broadcasts_nothing() {
    let hub = fresh_hub_with_clock(Arc::new(FakeClock::new(at(T0_MS))));
    let (_last, mut rx) = hub.subscribe();
    let mut tee = TeeSink::new(FailingSink::new("disk full"), hub.clone());

    let entry = AuditEntry {
        seq: 1,
        kind: EntryKind::Command,
        actor: AgentId("wasl-a".to_string()),
        targets: vec![],
        summary: "boom".to_string(),
        decision: None,
    };
    let err = tee.append(&entry).unwrap_err();
    assert_eq!(err, wasl_audit::AuditError::Sink("disk full".to_string()));

    assert!(matches!(
        rx.try_recv(),
        Err(tokio::sync::broadcast::error::TryRecvError::Empty)
    ));
}

#[test]
fn h11c_tee_sink_ok_with_zero_subscribers() {
    let hub = fresh_hub_with_clock(Arc::new(FakeClock::new(at(T0_MS))));
    let mut tee = TeeSink::new(RecordingSink::default(), hub); // no subscribe() call
    let entry = AuditEntry {
        seq: 1,
        kind: EntryKind::Message,
        actor: AgentId("wasl-a".to_string()),
        targets: vec![],
        summary: "no one is listening".to_string(),
        decision: None,
    };
    tee.append(&entry)
        .expect("append ok even with zero subscribers");
    assert_eq!(tee.into_inner().len(), 1);
}

#[test]
fn h11d_tee_sink_into_inner_returns_the_wrapped_sink() {
    let hub = fresh_hub_with_clock(Arc::new(FakeClock::new(at(T0_MS))));
    let mut tee = TeeSink::new(RecordingSink::default(), hub);
    let entry = AuditEntry {
        seq: 1,
        kind: EntryKind::Message,
        actor: AgentId("wasl-a".to_string()),
        targets: vec![],
        summary: "s".to_string(),
        decision: None,
    };
    tee.append(&entry).unwrap();
    let inner = tee.into_inner();
    assert_eq!(inner.snapshot(), vec![entry]);
}

// ── H12: serde/wire ──────────────────────────────────────────────────────

#[test]
fn h12_frame_variants_serialize_the_wire_payload_untagged() {
    let gr = Frame::GateRequested {
        gate_id: "gate-1".to_string(),
        mode: GateMode::External,
        decidable: true,
        request: req_call9(),
        requested_at: "2026-07-02T12:00:00.000Z".to_string(),
        timeout_ms: 120_000,
        deadline: "2026-07-02T12:02:00.000Z".to_string(),
    };
    let v = serde_json::to_value(&gr).unwrap();
    assert_eq!(v["gate_id"], json!("gate-1"));
    assert_eq!(v["mode"], json!("external"));
    assert_eq!(v["decidable"], json!(true));
    assert!(
        v.get("GateRequested").is_none(),
        "Frame must serialize untagged (no variant-name wrapper), got {v}"
    );
    assert_eq!(gr.event_name(), "gate_requested");

    let gd = Frame::GateDecided {
        gate_id: "gate-1".to_string(),
        outcome: DecisionOutcome::Denied,
        decided_by: DecidedBy::Operator,
        reason: Some("nope".to_string()),
        decided_at: "2026-07-02T12:00:03.500Z".to_string(),
    };
    let v2 = serde_json::to_value(&gd).unwrap();
    assert_eq!(v2["outcome"], json!("denied"));
    assert_eq!(v2["decided_by"], json!("operator"));
    assert_eq!(v2["reason"], json!("nope"));
    assert_eq!(gd.event_name(), "gate_decided");

    let entry = AuditEntry {
        seq: 1,
        kind: EntryKind::Message,
        actor: AgentId("a".to_string()),
        targets: vec![],
        summary: "s".to_string(),
        decision: None,
    };
    let au = Frame::Audit {
        entry: entry.clone(),
    };
    let v3 = serde_json::to_value(&au).unwrap();
    assert_eq!(
        v3,
        json!({ "entry": serde_json::to_value(&entry).unwrap() })
    );
    assert_eq!(au.event_name(), "audit");

    let sf = SeqFrame { seq: 7, frame: gd };
    let vsf = serde_json::to_value(&sf).unwrap();
    assert_eq!(vsf["seq"], json!(7));
    assert_eq!(vsf["frame"]["outcome"], json!("denied"));
}

#[test]
fn h12_decision_body_deserializes_both_directions() {
    let denied: DecisionBody = serde_json::from_str(r#"{"outcome":"denied","reason":"x"}"#)
        .expect("deserialize denied+reason");
    assert_eq!(denied.outcome, DecisionOutcome::Denied);
    assert_eq!(denied.reason, Some("x".to_string()));

    let approved: DecisionBody =
        serde_json::from_str(r#"{"outcome":"approved"}"#).expect("deserialize approved");
    assert_eq!(approved.outcome, DecisionOutcome::Approved);
    assert_eq!(approved.reason, None, "reason must default to None");
}

#[test]
fn h12_decided_by_and_decision_outcome_casing() {
    assert_eq!(
        serde_json::to_value(DecidedBy::Operator).unwrap(),
        json!("operator")
    );
    assert_eq!(
        serde_json::to_value(DecidedBy::Elicitation).unwrap(),
        json!("elicitation")
    );
    assert_eq!(
        serde_json::to_value(DecidedBy::Timeout).unwrap(),
        json!("timeout")
    );

    assert_eq!(
        serde_json::to_value(DecisionOutcome::Approved).unwrap(),
        json!("approved")
    );
    assert_eq!(
        serde_json::to_value(DecisionOutcome::Denied).unwrap(),
        json!("denied")
    );
    assert_eq!(
        serde_json::to_value(DecisionOutcome::Dismissed).unwrap(),
        json!("dismissed")
    );
}

#[test]
fn h12_decide_error_display_is_non_empty_and_distinct_and_is_std_error() {
    fn assert_is_error<E: std::error::Error>(_e: &E) {}

    let unknown = DecideError::UnknownGate;
    let already = DecideError::AlreadyDecided;
    let du = unknown.to_string();
    let da = already.to_string();
    assert!(!du.is_empty());
    assert!(!da.is_empty());
    assert_ne!(du, da);
    assert_is_error(&unknown);
    assert_is_error(&already);
    let _boxed: Box<dyn std::error::Error> = Box::new(DecideError::UnknownGate);
}

#[test]
fn h12_hub_config_default_values() {
    let cfg = HubConfig::default();
    assert_eq!(cfg.decision_timeout, Duration::from_secs(120));
    assert_eq!(cfg.channel_capacity, 256);
}

// ── H13: multi-subscriber at the hub level ─────────────────────────────────

#[test]
fn h13_multiple_subscribers_see_identical_seq_frames() {
    let hub = fresh_hub_with_clock(Arc::new(FakeClock::new(at(T0_MS))));
    let (_l1, mut rx1) = hub.subscribe();
    let (_l2, mut rx2) = hub.subscribe();

    let _w = hub.open_gate(GateMode::External, &req_call9());

    let f1 = rx1.try_recv().expect("rx1 frame");
    let f2 = rx2.try_recv().expect("rx2 frame");
    assert_eq!(f1.seq, f2.seq);
    assert_eq!(f1.frame, f2.frame);
}

// ── Bonus: SystemClock / CounterGateIds defaults actually work ────────────

#[test]
fn bonus_new_uses_system_clock_and_counter_gate_ids_by_default() {
    let hub = OperatorHub::new(node_info(), HubConfig::default());
    let (_last, mut rx) = hub.subscribe();
    let _w = hub.open_gate(GateMode::External, &req_call9());
    let f = rx.try_recv().expect("gate_requested");
    match f.frame {
        Frame::GateRequested {
            gate_id,
            requested_at,
            ..
        } => {
            assert_eq!(gate_id, "gate-1");
            assert!(
                requested_at.ends_with('Z') && requested_at.contains('T'),
                "requested_at must be RFC3339 UTC, got {requested_at:?}"
            );
        }
        other => panic!("expected GateRequested, got {other:?}"),
    }
}

// ── Bonus: the raw lag/skip-and-continue mechanics loopback.rs's L6 relies on ──

#[test]
fn bonus_lagged_subscriber_recovers_to_the_still_buffered_frames() {
    let cfg = HubConfig {
        decision_timeout: Duration::from_secs(120),
        channel_capacity: 2,
    };
    let hub = hub_with_cfg(Arc::new(FakeClock::new(at(T0_MS))), cfg);
    let (_last, mut rx) = hub.subscribe();

    for seq in 1..=20u64 {
        hub.publish_audit(&common::filler_entry(seq));
    }

    // A receiver that never drained a capacity-2 channel through 20 sends
    // must observe a `Lagged` error (the durable record is unaffected —
    // this is purely a broadcast-fan-out concern)...
    assert!(matches!(
        rx.try_recv(),
        Err(tokio::sync::broadcast::error::TryRecvError::Lagged(_))
    ));

    // ...and then recover, continuing to receive the still-buffered tail
    // (skip-and-continue: no close, no permanent error).
    let recovered = rx.try_recv().expect("recovers to a buffered frame");
    match recovered.frame {
        Frame::Audit { entry } => assert_eq!(entry.seq, 19),
        other => panic!("expected Audit, got {other:?}"),
    }
    let recovered2 = rx.try_recv().expect("second buffered frame");
    match recovered2.frame {
        Frame::Audit { entry } => assert_eq!(entry.seq, 20),
        other => panic!("expected Audit, got {other:?}"),
    }
}
