//! `wasl-node` two-node loopback e2e (N1–N10 of `docs/specs/node.md`).
//!
//! Every send goes through the REAL gate (`wasl_server` over the `FakeSurfacer` +
//! recording/failing `Sink`), the REAL A2A transport (`wasl_dispatch` → an HTTP
//! `wasl_a2a_server` on a loopback port), and the REAL receive executor
//! (`wasl_node::receiver` → `wasl_adapter::AdapterExecutor`). The ONLY fake is the
//! innermost `AgentAdapter` (the last-inch CLI bridge, untested here). Node A (the
//! sender) and each receiver run on distinct `127.0.0.1` ports = distinct "hosts",
//! same code path. Listeners bind before serve; serve tasks abort on drop.

mod common;

use common::{
    agent, caps_elicitation, caps_prepare_commit, echo_peer, wasl_proto_card, FailingSink,
    FakeSurfacer, RecordingSink,
};

use wasl_adapter::DeliveryKind;
use wasl_audit::EntryKind;
use wasl_dispatch::DispatchReport;
use wasl_node::{GateMode, Node, NodeError, Sent};
use wasl_proto::{
    ApprovalToken, CommandParams, ConsultParams, ConsultResult, GateDecision, HostId, MessageParams,
};

/// A throwaway approval token for the synchronous (non Tier-2) tiers, where the
/// gate ignores the token and decides inline.
fn tok() -> ApprovalToken {
    ApprovalToken("tok".to_string())
}

// ── N1 — approved consult, two nodes ─────────────────────────────────────────

#[tokio::test]
async fn n1_approved_consult_delivers_and_audits() {
    let peer_b = echo_peer().await;

    let mut node = Node::new(
        agent("A"),
        HostId("host-a".to_string()),
        caps_elicitation(),
        FakeSurfacer::approve(),
        RecordingSink::new(),
    );
    assert_eq!(node.mode(), GateMode::Elicitation);
    node.register(wasl_proto_card("B", &peer_b.url));

    let sent = node
        .consult(
            "m-1".to_string(),
            tok(),
            ConsultParams {
                peers: vec![agent("B")],
                question: "q".to_string(),
            },
        )
        .await
        .expect("gate/audit ok");

    match sent {
        Sent::Delivered { result, report } => {
            assert_eq!(result.replies.len(), 1);
            assert_eq!(result.replies[0].from, agent("B"));
            assert_eq!(result.replies[0].body, "echo:q");
            assert_eq!(report, DispatchReport::default());
        }
        other => panic!("expected Delivered, got {other:?}"),
    }

    let entries = node.audit_entries();
    assert_eq!(entries.len(), 1);
    assert_eq!(entries[0].kind, EntryKind::Decision);
    assert_eq!(entries[0].decision, Some(GateDecision::Approve));

    let deliveries = peer_b.deliveries();
    assert_eq!(deliveries.len(), 1);
    assert_eq!(deliveries[0].kind, DeliveryKind::Consult);
    assert_eq!(deliveries[0].from, "A");
    assert_eq!(deliveries[0].text, "q");
}

// ── N2 — command, two nodes ──────────────────────────────────────────────────

#[tokio::test]
async fn n2_approved_command_delivers() {
    let peer_b = echo_peer().await;

    let mut node = Node::new(
        agent("A"),
        HostId("host-a".to_string()),
        caps_elicitation(),
        FakeSurfacer::approve(),
        RecordingSink::new(),
    );
    node.register(wasl_proto_card("B", &peer_b.url));

    let sent = node
        .command(
            "m-1".to_string(),
            tok(),
            CommandParams {
                peers: vec![agent("B")],
                task: "do-it".to_string(),
            },
        )
        .await
        .expect("gate/audit ok");

    match sent {
        Sent::Delivered { result, report } => {
            assert_eq!(result.replies.len(), 1);
            assert_eq!(result.replies[0].from, agent("B"));
            assert_eq!(result.replies[0].body, "echo:do-it");
            assert_eq!(report, DispatchReport::default());
        }
        other => panic!("expected Delivered, got {other:?}"),
    }

    let deliveries = peer_b.deliveries();
    assert_eq!(deliveries.len(), 1);
    assert_eq!(deliveries[0].kind, DeliveryKind::Command);
    assert_eq!(deliveries[0].text, "do-it");
}

// ── N3 — message, two nodes ──────────────────────────────────────────────────

#[tokio::test]
async fn n3_approved_message_delivers() {
    let peer_b = echo_peer().await;

    let mut node = Node::new(
        agent("A"),
        HostId("host-a".to_string()),
        caps_elicitation(),
        FakeSurfacer::approve(),
        RecordingSink::new(),
    );
    node.register(wasl_proto_card("B", &peer_b.url));

    let sent = node
        .message(
            "m-1".to_string(),
            tok(),
            MessageParams {
                peers: vec![agent("B")],
                payload: "ping".to_string(),
            },
        )
        .await
        .expect("gate/audit ok");

    match sent {
        Sent::Delivered { result, report } => {
            assert_eq!(result.delivered, vec![agent("B")]);
            assert_eq!(report, DispatchReport::default());
        }
        other => panic!("expected Delivered, got {other:?}"),
    }

    let deliveries = peer_b.deliveries();
    assert_eq!(deliveries.len(), 1);
    assert_eq!(deliveries[0].kind, DeliveryKind::Message);
    assert_eq!(deliveries[0].text, "ping");
}

// ── N4 — multicast consult across two servers ────────────────────────────────

#[tokio::test]
async fn n4_multicast_consult_replies_in_order() {
    let peer_b = echo_peer().await;
    let peer_c = echo_peer().await;

    let mut node = Node::new(
        agent("A"),
        HostId("host-a".to_string()),
        caps_elicitation(),
        FakeSurfacer::approve(),
        RecordingSink::new(),
    );
    node.register(wasl_proto_card("B", &peer_b.url));
    node.register(wasl_proto_card("C", &peer_c.url));

    let sent = node
        .consult(
            "m-1".to_string(),
            tok(),
            ConsultParams {
                peers: vec![agent("B"), agent("C")],
                question: "q".to_string(),
            },
        )
        .await
        .expect("gate/audit ok");

    match sent {
        Sent::Delivered { result, report } => {
            assert_eq!(result.replies.len(), 2);
            assert_eq!(result.replies[0].from, agent("B"));
            assert_eq!(result.replies[1].from, agent("C"));
            assert_eq!(result.replies[0].body, "echo:q");
            assert_eq!(result.replies[1].body, "echo:q");
            assert_eq!(report, DispatchReport::default());
        }
        other => panic!("expected Delivered, got {other:?}"),
    }

    assert_eq!(peer_b.deliveries().len(), 1);
    assert_eq!(peer_c.deliveries().len(), 1);
    assert_eq!(peer_b.deliveries()[0].kind, DeliveryKind::Consult);
    assert_eq!(peer_c.deliveries()[0].kind, DeliveryKind::Consult);
}

// ── N5 — denied → no dispatch ────────────────────────────────────────────────

#[tokio::test]
async fn n5_denied_blocks_and_never_dispatches() {
    let peer_b = echo_peer().await;

    let mut node = Node::new(
        agent("A"),
        HostId("host-a".to_string()),
        caps_elicitation(),
        FakeSurfacer::deny("not allowed"),
        RecordingSink::new(),
    );
    node.register(wasl_proto_card("B", &peer_b.url));

    let sent = node
        .consult(
            "m-1".to_string(),
            tok(),
            ConsultParams {
                peers: vec![agent("B")],
                question: "q".to_string(),
            },
        )
        .await
        .expect("gate/audit ok");

    match sent {
        Sent::Blocked { reason } => assert_eq!(reason, "not allowed"),
        other => panic!("expected Blocked, got {other:?}"),
    }

    let entries = node.audit_entries();
    assert_eq!(entries.len(), 1);
    assert_eq!(
        entries[0].decision,
        Some(GateDecision::Deny {
            reason: "not allowed".to_string()
        })
    );

    assert!(
        peer_b.deliveries().is_empty(),
        "a denied send must never reach the peer"
    );
}

// ── N6 — dismissed → Blocked("dismissed") ────────────────────────────────────

#[tokio::test]
async fn n6_dismissed_blocks_with_no_decision() {
    let peer_b = echo_peer().await;

    let mut node = Node::new(
        agent("A"),
        HostId("host-a".to_string()),
        caps_elicitation(),
        FakeSurfacer::dismiss(),
        RecordingSink::new(),
    );
    node.register(wasl_proto_card("B", &peer_b.url));

    let sent = node
        .consult(
            "m-1".to_string(),
            tok(),
            ConsultParams {
                peers: vec![agent("B")],
                question: "q".to_string(),
            },
        )
        .await
        .expect("gate/audit ok");

    match sent {
        Sent::Blocked { reason } => assert_eq!(reason, "dismissed"),
        other => panic!("expected Blocked, got {other:?}"),
    }

    let entries = node.audit_entries();
    assert_eq!(entries.len(), 1);
    assert_eq!(entries[0].decision, None);

    assert!(
        peer_b.deliveries().is_empty(),
        "a dismissed send must never reach the peer"
    );
}

// ── N7 — Tier-2 pending → commit → dispatch ──────────────────────────────────

#[tokio::test]
async fn n7_tier2_pending_then_commit_dispatches() {
    let peer_b = echo_peer().await;

    let mut node = Node::new(
        agent("A"),
        HostId("host-a".to_string()),
        caps_prepare_commit(),
        FakeSurfacer::approve(), // never surfaced in Tier-2; it defers
        RecordingSink::new(),
    );
    assert_eq!(node.mode(), GateMode::PrepareCommit);
    node.register(wasl_proto_card("B", &peer_b.url));

    let token = ApprovalToken("t2-token".to_string());
    let params = ConsultParams {
        peers: vec![agent("B")],
        question: "q".to_string(),
    };

    let pending = node
        .consult("m-1".to_string(), token.clone(), params.clone())
        .await
        .expect("gate/audit ok");

    match pending {
        Sent::Pending { token: t } => assert_eq!(t, token),
        other => panic!("expected Pending, got {other:?}"),
    }
    assert_eq!(node.pending_count(), 1);
    assert!(
        peer_b.deliveries().is_empty(),
        "a pending Tier-2 gate must not dispatch yet"
    );
    // Nothing recorded until the commit resolves the deferred gate.
    assert!(node.audit_entries().is_empty());

    let committed = node
        .commit_consult(&token, GateDecision::Approve, params)
        .await
        .expect("gate/audit ok");

    match committed {
        Sent::Delivered { result, report } => {
            assert_eq!(result.replies.len(), 1);
            assert_eq!(result.replies[0].from, agent("B"));
            assert_eq!(result.replies[0].body, "echo:q");
            assert_eq!(report, DispatchReport::default());
        }
        other => panic!("expected Delivered, got {other:?}"),
    }

    assert_eq!(node.pending_count(), 0);
    let entries = node.audit_entries();
    assert_eq!(entries.len(), 1);
    assert_eq!(entries[0].decision, Some(GateDecision::Approve));

    let deliveries = peer_b.deliveries();
    assert_eq!(deliveries.len(), 1);
    assert_eq!(deliveries[0].kind, DeliveryKind::Consult);
    assert_eq!(deliveries[0].from, "A");
}

// ── N8 — partial multicast (one unregistered ghost) ──────────────────────────

#[tokio::test]
async fn n8_partial_multicast_reports_unknown() {
    let peer_b = echo_peer().await;

    let mut node = Node::new(
        agent("A"),
        HostId("host-a".to_string()),
        caps_elicitation(),
        FakeSurfacer::approve(),
        RecordingSink::new(),
    );
    // Only B is registered; "ghost" is unknown to the registry.
    node.register(wasl_proto_card("B", &peer_b.url));

    let sent = node
        .consult(
            "m-1".to_string(),
            tok(),
            ConsultParams {
                peers: vec![agent("B"), agent("ghost")],
                question: "q".to_string(),
            },
        )
        .await
        .expect("gate/audit ok");

    match sent {
        Sent::Delivered { result, report } => {
            assert_eq!(result.replies.len(), 1);
            assert_eq!(result.replies[0].from, agent("B"));
            assert_eq!(result.replies[0].body, "echo:q");
            assert_eq!(report.unknown, vec![agent("ghost")]);
            assert!(report.failed.is_empty());
        }
        other => panic!("expected Delivered, got {other:?}"),
    }

    assert_eq!(peer_b.deliveries().len(), 1);
}

// ── N9 — gate/audit (sink) failure aborts before dispatch ────────────────────

#[tokio::test]
async fn n9_failing_sink_errors_before_dispatch() {
    let peer_b = echo_peer().await;

    let mut node = Node::new(
        agent("A"),
        HostId("host-a".to_string()),
        caps_elicitation(),
        FakeSurfacer::approve(), // approved, yet the audit sink fails
        FailingSink,
    );
    node.register(wasl_proto_card("B", &peer_b.url));

    let result = node
        .consult(
            "m-1".to_string(),
            tok(),
            ConsultParams {
                peers: vec![agent("B")],
                question: "q".to_string(),
            },
        )
        .await;

    match result {
        Err(NodeError::Gate(_)) => {}
        other => panic!("expected Err(NodeError::Gate), got {other:?}"),
    }

    assert!(
        peer_b.deliveries().is_empty(),
        "a durability failure must abort the send before it reaches the peer"
    );
}

// ── N10 — NodeError / Sent basics (pure) ─────────────────────────────────────

#[test]
fn n10_error_display_and_sent_equality() {
    // NodeError: Display surfaces the detail, and it is a std::error::Error.
    let err = NodeError::Gate("sink offline".to_string());
    let shown = format!("{err}");
    assert!(
        shown.contains("sink offline"),
        "Display should surface the gate detail, got {shown:?}"
    );
    let as_error: &dyn std::error::Error = &err;
    assert!(as_error.source().is_none());
    assert_eq!(err, NodeError::Gate("sink offline".to_string()));
    assert_ne!(err, NodeError::Gate("other".to_string()));

    // Sent equality / clone (concrete R).
    let blocked: Sent<ConsultResult> = Sent::Blocked {
        reason: "dismissed".to_string(),
    };
    assert_eq!(blocked, blocked.clone());
    assert_eq!(
        blocked,
        Sent::Blocked {
            reason: "dismissed".to_string()
        }
    );

    let pending: Sent<ConsultResult> = Sent::Pending { token: tok() };
    assert_ne!(blocked, pending);

    let delivered: Sent<ConsultResult> = Sent::Delivered {
        result: ConsultResult { replies: vec![] },
        report: DispatchReport::default(),
    };
    assert_eq!(delivered, delivered.clone());
    assert_ne!(delivered, blocked);
}
