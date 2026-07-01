//! Unit tests for the sync request→reply heart (`resolve_and_dispatch`).
//!
//! These exercise the whole gate-and-dispatch invariant with fake Mesh/Runner/
//! Sink: an approved call dispatches over the tested cores with the right result;
//! a denied/dismissed call is `Blocked` and the delivery seams are NEVER touched;
//! an audit-sink failure fails the call durability-first; and an approved
//! transfer yields a `TransferResult`.

mod common;

use common::{registry_with, FailingSink, FakeMesh, FakeRunner, RecordingSink};

use wasl_audit::{AuditLog, EntryKind};
use wasl_gate::GateOutcome;
use wasl_mcp::handle::{resolve_and_dispatch, HandleError, ToolCall, ToolCtx, ToolReply};
use wasl_proto::{AgentId, ConsultParams, GateDecision, HostId, MessageParams, TransferParams};
use wasl_transfer::Tool;

fn ids() -> (AgentId, HostId) {
    (AgentId("me".to_string()), HostId("h1".to_string()))
}

// ---------------------------------------------------------------------
// Approved paths dispatch over the tested cores
// ---------------------------------------------------------------------

#[test]
fn approved_consult_dispatches_and_collects_replies() {
    let (self_id, self_host) = ids();
    let registry = registry_with(&[("peerA", "h1"), ("peerB", "h1")]);
    let ctx = ToolCtx {
        self_id: &self_id,
        self_host: &self_host,
        registry: &registry,
    };
    let mut mesh = FakeMesh::new()
        .with_reply("peerA", "answerA")
        .with_reply("peerB", "answerB");
    let mut runner = FakeRunner::new();
    let mut audit = AuditLog::new(RecordingSink::new());

    let call = ToolCall::Consult(ConsultParams {
        peers: vec![AgentId("peerA".to_string()), AgentId("peerB".to_string())],
        question: "status?".to_string(),
    });

    let reply = resolve_and_dispatch(
        &ctx,
        &GateOutcome::Approved,
        call,
        "base",
        0,
        &mut mesh,
        &mut runner,
        &mut audit,
    )
    .expect("approved consult must succeed");

    match reply {
        ToolReply::Consult(res) => {
            assert_eq!(res.replies.len(), 2, "one reply per delivered peer");
            assert_eq!(res.replies[0].from, AgentId("peerA".to_string()));
            assert_eq!(res.replies[0].body, "answerA");
            assert_eq!(res.replies[1].from, AgentId("peerB".to_string()));
            assert_eq!(res.replies[1].body, "answerB");
        }
        other => panic!("expected Consult reply, got {other:?}"),
    }

    // The mesh was actually used, once per peer.
    assert_eq!(mesh.calls.len(), 2, "both peers delivered");
    // The decision was recorded as an Approve on the Consult entry.
    let entries = audit.entries();
    assert_eq!(entries.len(), 1);
    assert_eq!(entries[0].kind, EntryKind::Consult);
    assert_eq!(entries[0].decision, Some(GateDecision::Approve));
    assert_eq!(entries[0].actor, self_id);
    assert_eq!(entries[0].targets.len(), 2);
}

#[test]
fn approved_message_reports_delivered_peers() {
    let (self_id, self_host) = ids();
    let registry = registry_with(&[("peerA", "h1")]);
    let ctx = ToolCtx {
        self_id: &self_id,
        self_host: &self_host,
        registry: &registry,
    };
    let mut mesh = FakeMesh::new();
    let mut runner = FakeRunner::new();
    let mut audit = AuditLog::new(RecordingSink::new());

    let call = ToolCall::Message(MessageParams {
        peers: vec![AgentId("peerA".to_string())],
        payload: "hi".to_string(),
    });

    let reply = resolve_and_dispatch(
        &ctx,
        &GateOutcome::Approved,
        call,
        "base",
        7,
        &mut mesh,
        &mut runner,
        &mut audit,
    )
    .expect("approved message must succeed");

    match reply {
        ToolReply::Message(res) => {
            assert_eq!(res.delivered, vec![AgentId("peerA".to_string())]);
        }
        other => panic!("expected Message reply, got {other:?}"),
    }
    assert_eq!(mesh.calls.len(), 1);
    assert_eq!(audit.entries()[0].seq, 7, "caller-supplied seq is recorded");
}

#[test]
fn approved_transfer_yields_transfer_result() {
    let (self_id, self_host) = ids();
    let registry = registry_with(&[]);
    let ctx = ToolCtx {
        self_id: &self_id,
        self_host: &self_host,
        registry: &registry,
    };
    let mut mesh = FakeMesh::new();
    let mut runner = FakeRunner::new();
    let mut audit = AuditLog::new(RecordingSink::new());

    let call = ToolCall::Transfer {
        tool: Tool::Scp,
        params: TransferParams {
            src_host: HostId("h1".to_string()),
            dst_hosts: vec![HostId("h2".to_string()), HostId("h3".to_string())],
            path: "/data/f".to_string(),
        },
    };

    let reply = resolve_and_dispatch(
        &ctx,
        &GateOutcome::Approved,
        call,
        "base",
        0,
        &mut mesh,
        &mut runner,
        &mut audit,
    )
    .expect("approved transfer must succeed");

    match reply {
        ToolReply::Transfer(res) => {
            assert_eq!(
                res.delivered,
                vec![HostId("h2".to_string()), HostId("h3".to_string())],
                "both destination hosts delivered"
            );
        }
        other => panic!("expected Transfer reply, got {other:?}"),
    }
    // The runner ran one job per destination; the mesh was untouched (transfer is
    // host-addressed, not peer-addressed).
    assert_eq!(runner.argvs.len(), 2);
    assert!(mesh.calls.is_empty());
    // Recorded as a Transfer decision with the destination hosts as targets.
    let entry = &audit.entries()[0];
    assert_eq!(entry.kind, EntryKind::Transfer);
    assert_eq!(entry.decision, Some(GateDecision::Approve));
    assert_eq!(entry.targets.len(), 2);
}

// ---------------------------------------------------------------------
// The gate is never bypassed: non-Approved never dispatches
// ---------------------------------------------------------------------

#[test]
fn denied_consult_is_blocked_and_mesh_never_called() {
    let (self_id, self_host) = ids();
    let registry = registry_with(&[("peerA", "h1")]);
    let ctx = ToolCtx {
        self_id: &self_id,
        self_host: &self_host,
        registry: &registry,
    };
    let mut mesh = FakeMesh::new().with_reply("peerA", "should-never-be-sent");
    let mut runner = FakeRunner::new();
    let mut audit = AuditLog::new(RecordingSink::new());

    let call = ToolCall::Consult(ConsultParams {
        peers: vec![AgentId("peerA".to_string())],
        question: "secret?".to_string(),
    });

    let reply = resolve_and_dispatch(
        &ctx,
        &GateOutcome::Denied {
            reason: "nope".to_string(),
        },
        call,
        "base",
        0,
        &mut mesh,
        &mut runner,
        &mut audit,
    )
    .expect("denial is a valid (recorded) outcome, not an error");

    match reply {
        ToolReply::Blocked { reason } => assert_eq!(reason, "nope"),
        other => panic!("expected Blocked, got {other:?}"),
    }

    // The invariant: the delivery seams were NEVER touched.
    assert!(mesh.calls.is_empty(), "denied call must not dispatch");
    assert!(runner.argvs.is_empty(), "denied call must not run transfer");

    // The denial was still recorded (durable decision trail).
    let entry = &audit.entries()[0];
    assert_eq!(
        entry.decision,
        Some(GateDecision::Deny {
            reason: "nope".to_string()
        })
    );
}

#[test]
fn dismissed_call_is_blocked_records_no_decision_and_never_dispatches() {
    let (self_id, self_host) = ids();
    let registry = registry_with(&[("peerA", "h1")]);
    let ctx = ToolCtx {
        self_id: &self_id,
        self_host: &self_host,
        registry: &registry,
    };
    let mut mesh = FakeMesh::new();
    let mut runner = FakeRunner::new();
    let mut audit = AuditLog::new(RecordingSink::new());

    let call = ToolCall::Consult(ConsultParams {
        peers: vec![AgentId("peerA".to_string())],
        question: "q?".to_string(),
    });

    let reply = resolve_and_dispatch(
        &ctx,
        &GateOutcome::Dismissed,
        call,
        "base",
        0,
        &mut mesh,
        &mut runner,
        &mut audit,
    )
    .expect("dismissal is recorded, not an error");

    match reply {
        ToolReply::Blocked { reason } => assert_eq!(reason, "dismissed"),
        other => panic!("expected Blocked, got {other:?}"),
    }
    assert!(mesh.calls.is_empty(), "dismissed call must not dispatch");
    // A dismissal carries no gate decision (per GateOutcome::to_decision).
    assert_eq!(audit.entries()[0].decision, None);
}

// ---------------------------------------------------------------------
// Durability-first: audit sink failure fails the call, no dispatch
// ---------------------------------------------------------------------

#[test]
fn audit_sink_failure_fails_the_call_and_never_dispatches() {
    let (self_id, self_host) = ids();
    let registry = registry_with(&[("peerA", "h1")]);
    let ctx = ToolCtx {
        self_id: &self_id,
        self_host: &self_host,
        registry: &registry,
    };
    let mut mesh = FakeMesh::new().with_reply("peerA", "answer");
    let mut runner = FakeRunner::new();
    // The sink always errors.
    let mut audit = AuditLog::new(FailingSink);

    let call = ToolCall::Consult(ConsultParams {
        peers: vec![AgentId("peerA".to_string())],
        question: "q?".to_string(),
    });

    let err = resolve_and_dispatch(
        &ctx,
        &GateOutcome::Approved, // even an approval must fail if it can't be recorded
        call,
        "base",
        0,
        &mut mesh,
        &mut runner,
        &mut audit,
    )
    .expect_err("a sink failure must fail the call");

    match err {
        HandleError::Audit(detail) => assert!(
            detail.contains("disk full"),
            "error surfaces the sink detail, got: {detail}"
        ),
    }

    // Durability-first: nothing dispatched because the decision could not be
    // recorded — even though the outcome was Approved.
    assert!(
        mesh.calls.is_empty(),
        "no dispatch when the decision cannot be durably recorded"
    );
    assert!(runner.argvs.is_empty());
}

// ---------------------------------------------------------------------
// ToolReply helper
// ---------------------------------------------------------------------

#[test]
fn tool_reply_is_blocked_flag() {
    assert!(ToolReply::Blocked {
        reason: "x".to_string()
    }
    .is_blocked());
    assert!(!ToolReply::Message(wasl_proto::MessageResult { delivered: vec![] }).is_blocked());
}
