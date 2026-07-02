//! Cross-crate end-to-end scenario suite for Wasl (spec: `docs/specs/e2e.md`).
//!
//! Each test embodies the "server loop": build a `Gatekeeper` (wasl-server) over
//! a `Surfacer` + audit `Sink`, call `authorize(...)` (or `commit(...)` for
//! Tier-2), and ONLY on `Verdict::Proceed` dispatch through the daemon
//! (`consult`/`command`/`message`/`transfer`). Every test asserts BOTH halves of
//! the seam — the recorded gate decision AND the dispatch outcome — with concrete
//! values, so the two agree with the verdict. Tier-3 (external authority) records
//! its decision into the generic audit log — the observation feed IS the audit log.

mod common;

use common::{agent, card, host, FakeMesh, FakeRunner, FakeSink, ScriptSurfacer};

use wasl_audit::EntryKind;
use wasl_gate::{ClientCaps, GateMode};
use wasl_proto::{
    AgentId, ApprovalToken, CommandParams, ConsultParams, GateDecision, GateKind, HostId,
    MessageParams, TransferParams,
};
use wasl_server::{Gatekeeper, ServerError, Verdict};
use wasl_transfer::Tool;

// ---------------------------------------------------------------------
// Shared constants
// ---------------------------------------------------------------------

fn self_host() -> HostId {
    host("host-a")
}

fn caller() -> AgentId {
    agent("caller")
}

/// Capabilities → gate mode fixtures.
fn caps_elicitation() -> ClientCaps {
    ClientCaps {
        elicitation: true,
        external: false,
    }
}

fn caps_external() -> ClientCaps {
    ClientCaps {
        elicitation: false,
        external: true,
    }
}

fn caps_prepare_commit() -> ClientCaps {
    ClientCaps {
        elicitation: false,
        external: false,
    }
}

// ---------------------------------------------------------------------
// X1 — approved consult, end to end
// ---------------------------------------------------------------------

#[test]
fn x1_approved_consult_gated_then_dispatched() {
    // --- gate side: caps elicitation, surfacer approves ---
    let mut gk = Gatekeeper::new(
        caps_elicitation(),
        ScriptSurfacer::approving(),
        FakeSink::ok(),
    );
    assert_eq!(gk.mode(), GateMode::Elicitation);

    let targets = vec![agent("p1"), agent("p2")];
    let verdict = gk
        .authorize(
            "req-x1".to_string(),
            GateKind::Consult,
            caller(),
            targets.clone(),
            "please review".to_string(),
            ApprovalToken("tok-x1".to_string()),
            1,
        )
        .expect("elicitation gate must not error with a healthy sink");
    assert_eq!(verdict, Verdict::Proceed);

    // exactly one Decision entry carrying Some(Approve).
    let entries = gk.audit_entries();
    assert_eq!(entries.len(), 1);
    assert_eq!(entries[0].kind, EntryKind::Decision);
    assert_eq!(entries[0].actor, caller());
    assert_eq!(entries[0].targets, targets);
    assert_eq!(entries[0].decision, Some(GateDecision::Approve));

    // --- dispatch side: 2 local peers over a Mesh returning replies ---
    let mut registry = wasl_registry::PeerRegistry::new();
    registry.register(card("p1", "host-a"));
    registry.register(card("p2", "host-a"));

    let mut mesh = FakeMesh::new()
        .with_reply("p1", "yes from p1")
        .with_reply("p2", "no from p2");

    let params = ConsultParams {
        peers: targets.clone(),
        question: "please review".to_string(),
    };
    let (result, report) = wasl_daemon::consult(
        &registry,
        &self_host(),
        "env-x1",
        &caller(),
        &params,
        &mut mesh,
    );

    // replies from both peers, in target order, exact bodies.
    assert_eq!(result.replies.len(), 2);
    assert_eq!(result.replies[0].from, agent("p1"));
    assert_eq!(result.replies[0].body, "yes from p1");
    assert_eq!(result.replies[1].from, agent("p2"));
    assert_eq!(result.replies[1].body, "no from p2");

    // report empty — nothing unknown/failed.
    assert!(report.unknown.is_empty());
    assert!(report.failed.is_empty());

    // seam agreement: the gate approved AND the daemon delivered both.
    assert_eq!(mesh.calls.len(), 2);
}

// ---------------------------------------------------------------------
// X2 — denied → no dispatch, Mesh untouched
// ---------------------------------------------------------------------

#[test]
fn x2_denied_short_circuits_no_dispatch() {
    let mut gk = Gatekeeper::new(
        caps_elicitation(),
        ScriptSurfacer::denying("nope"),
        FakeSink::ok(),
    );

    let targets = vec![agent("p1")];
    let verdict = gk
        .authorize(
            "req-x2".to_string(),
            GateKind::Consult,
            caller(),
            targets.clone(),
            "please review".to_string(),
            ApprovalToken("tok-x2".to_string()),
            7,
        )
        .expect("healthy sink records the deny without error");
    assert_eq!(
        verdict,
        Verdict::Denied {
            reason: "nope".to_string()
        }
    );

    // audit recorded Some(Deny).
    let entries = gk.audit_entries();
    assert_eq!(entries.len(), 1);
    assert_eq!(
        entries[0].decision,
        Some(GateDecision::Deny {
            reason: "nope".to_string()
        })
    );

    // The composition MUST NOT dispatch. We prove it by never calling the daemon
    // (the branch is only reachable on Proceed) and the Mesh is never touched.
    let mut registry = wasl_registry::PeerRegistry::new();
    registry.register(card("p1", "host-a"));
    let mut mesh = FakeMesh::new().with_reply("p1", "unreachable");

    if verdict == Verdict::Proceed {
        let params = ConsultParams {
            peers: targets,
            question: "please review".to_string(),
        };
        let _ = wasl_daemon::consult(
            &registry,
            &self_host(),
            "env-x2",
            &caller(),
            &params,
            &mut mesh,
        );
    }

    assert!(
        mesh.calls.is_empty(),
        "Mesh must be untouched when the gate denies"
    );
}

// ---------------------------------------------------------------------
// X3 — Tier-2 prepare → commit → dispatch
// ---------------------------------------------------------------------

#[test]
fn x3_tier2_prepare_commit_then_dispatch() {
    let mut gk = Gatekeeper::new(
        caps_prepare_commit(),
        ScriptSurfacer::approving(),
        FakeSink::ok(),
    );
    assert_eq!(gk.mode(), GateMode::PrepareCommit);

    let targets = vec![agent("p1"), agent("p2")];
    let token = ApprovalToken("tok-x3".to_string());
    let verdict = gk
        .authorize(
            "req-x3".to_string(),
            GateKind::Command,
            caller(),
            targets.clone(),
            "do the thing".to_string(),
            token.clone(),
            10,
        )
        .expect("prepare never touches the sink");
    assert_eq!(
        verdict,
        Verdict::Pending {
            token: token.clone()
        }
    );

    // Tier-2 defers: no audit yet, surfacer not called.
    assert!(gk.audit_entries().is_empty());
    assert_eq!(gk.pending_count(), 1);

    // commit(Approve) → Proceed + Some(Approve) recorded.
    let committed = gk
        .commit(&token, GateDecision::Approve, 11)
        .expect("commit of a pending token with a healthy sink");
    assert_eq!(committed, Verdict::Proceed);

    let entries = gk.audit_entries();
    assert_eq!(entries.len(), 1);
    assert_eq!(entries[0].kind, EntryKind::Decision);
    assert_eq!(entries[0].targets, targets);
    assert_eq!(entries[0].decision, Some(GateDecision::Approve));

    // dispatch a command over the registry.
    let mut registry = wasl_registry::PeerRegistry::new();
    registry.register(card("p1", "host-a"));
    registry.register(card("p2", "host-b"));

    let mut mesh = FakeMesh::new()
        .with_reply("p1", "done p1")
        .with_reply("p2", "done p2");

    let params = CommandParams {
        peers: targets,
        task: "do the thing".to_string(),
    };
    let (result, report) = wasl_daemon::command(
        &registry,
        &self_host(),
        "env-x3",
        &caller(),
        &params,
        &mut mesh,
    );

    assert_eq!(result.replies.len(), 2);
    assert_eq!(result.replies[0].from, agent("p1"));
    assert_eq!(result.replies[0].body, "done p1");
    assert_eq!(result.replies[1].from, agent("p2"));
    assert_eq!(result.replies[1].body, "done p2");
    assert!(report.unknown.is_empty());
    assert!(report.failed.is_empty());
}

// ---------------------------------------------------------------------
// X4 — Tier-3 via an external authority (decision → the generic audit log)
// ---------------------------------------------------------------------

#[test]
fn x4_tier3_external_authority_records_decision_and_dispatches() {
    // Tier-3: caps select the External mode; the gate is surfaced to an external
    // authority (any `Surfacer` — e.g. a connector living outside Wasl) that
    // approves. The decision lands in the GENERIC audit log — there is no
    // Wasl-specific "connector feed"; the audit log IS the observation feed.
    let mut gk = Gatekeeper::new(caps_external(), ScriptSurfacer::approving(), FakeSink::ok());
    assert_eq!(gk.mode(), GateMode::External);

    let targets = vec![agent("p1")];
    let verdict = gk
        .authorize(
            "req-x4".to_string(),
            GateKind::Message,
            caller(),
            targets.clone(),
            "ping".to_string(),
            ApprovalToken("tok-x4".to_string()),
            3,
        )
        .expect("external path with a healthy sink");
    assert_eq!(verdict, Verdict::Proceed);

    // The external decision is recorded to the generic audit log.
    let entries = gk.audit_entries();
    assert_eq!(entries.len(), 1);
    assert_eq!(entries[0].kind, EntryKind::Decision);
    assert_eq!(entries[0].actor, caller());
    assert_eq!(entries[0].decision, Some(GateDecision::Approve));

    // dispatch a message → delivered = [p1].
    let mut registry = wasl_registry::PeerRegistry::new();
    registry.register(card("p1", "host-a"));
    let mut mesh = FakeMesh::new().with_reply("p1", "pong");

    let params = MessageParams {
        peers: targets,
        payload: "ping".to_string(),
    };
    let (result, report) = wasl_daemon::message(
        &registry,
        &self_host(),
        "env-x4",
        &caller(),
        &params,
        &mut mesh,
    );

    assert_eq!(result.delivered, vec![agent("p1")]);
    assert!(report.unknown.is_empty());
    assert!(report.failed.is_empty());
}

// ---------------------------------------------------------------------
// X5 — approved transfer
// ---------------------------------------------------------------------

#[test]
fn x5_approved_transfer_via_runner() {
    let mut gk = Gatekeeper::new(
        caps_elicitation(),
        ScriptSurfacer::approving(),
        FakeSink::ok(),
    );

    let dst1 = host("host-b");
    let dst2 = host("host-c");
    // gate targets for transfer are host-addressed; represent them as agent ids
    // for the audit record (the gate is content-agnostic here).
    let verdict = gk
        .authorize(
            "req-x5".to_string(),
            GateKind::Transfer,
            caller(),
            vec![agent("host-b"), agent("host-c")],
            "/data/file".to_string(),
            ApprovalToken("tok-x5".to_string()),
            20,
        )
        .expect("elicitation gate with healthy sink");
    assert_eq!(verdict, Verdict::Proceed);
    assert_eq!(gk.audit_entries()[0].decision, Some(GateDecision::Approve));

    // On proceed: transfer over a Runner with 2 dst hosts, all ok.
    let params = TransferParams {
        src_host: self_host(),
        dst_hosts: vec![dst1.clone(), dst2.clone()],
        path: "/data/file".to_string(),
    };
    let mut runner = FakeRunner::new();
    let (result, failures) =
        wasl_daemon::transfer(Tool::Scp, &params, &mut runner).expect("non-empty dst list");

    assert_eq!(result.delivered, vec![dst1, dst2]);
    assert!(failures.is_empty());
    assert_eq!(runner.argvs.len(), 2);
}

// ---------------------------------------------------------------------
// X6 — multicast with an unknown peer + a failed delivery
// ---------------------------------------------------------------------

#[test]
fn x6_multicast_unknown_and_failed_partition() {
    let mut gk = Gatekeeper::new(
        caps_elicitation(),
        ScriptSurfacer::approving(),
        FakeSink::ok(),
    );

    // 3 targets: p1 delivers, p2 fails, ghost is unregistered.
    let targets = vec![agent("p1"), agent("p2"), agent("ghost")];
    let verdict = gk
        .authorize(
            "req-x6".to_string(),
            GateKind::Consult,
            caller(),
            targets.clone(),
            "poll".to_string(),
            ApprovalToken("tok-x6".to_string()),
            30,
        )
        .expect("elicitation gate with healthy sink");
    assert_eq!(verdict, Verdict::Proceed);

    // exactly one gate decision entry.
    let decisions: Vec<_> = gk
        .audit_entries()
        .iter()
        .filter(|e| e.decision.is_some())
        .collect();
    assert_eq!(decisions.len(), 1);
    assert_eq!(decisions[0].decision, Some(GateDecision::Approve));

    let mut registry = wasl_registry::PeerRegistry::new();
    registry.register(card("p1", "host-a"));
    registry.register(card("p2", "host-a"));
    // "ghost" is intentionally NOT registered.

    let mut mesh = FakeMesh::new()
        .with_reply("p1", "p1 reply")
        .with_failure("p2", "timeout");

    let params = ConsultParams {
        peers: targets,
        question: "poll".to_string(),
    };
    let (result, report) = wasl_daemon::consult(
        &registry,
        &self_host(),
        "env-x6",
        &caller(),
        &params,
        &mut mesh,
    );

    // replies only for the delivered peer.
    assert_eq!(result.replies.len(), 1);
    assert_eq!(result.replies[0].from, agent("p1"));
    assert_eq!(result.replies[0].body, "p1 reply");

    // report partitions: ghost unknown, p2 failed.
    assert_eq!(report.unknown, vec![agent("ghost")]);
    assert_eq!(report.failed, vec![agent("p2")]);

    // partial multicast survived: p1 and p2 were both attempted (ghost was not
    // resolvable so never handed to the mesh).
    assert_eq!(mesh.calls.len(), 2);
}

// ---------------------------------------------------------------------
// X7 — Tier-2 commit denied → no dispatch
// ---------------------------------------------------------------------

#[test]
fn x7_tier2_commit_denied_no_dispatch() {
    let mut gk = Gatekeeper::new(
        caps_prepare_commit(),
        ScriptSurfacer::approving(),
        FakeSink::ok(),
    );

    let targets = vec![agent("p1")];
    let token = ApprovalToken("tok-x7".to_string());
    let pending = gk
        .authorize(
            "req-x7".to_string(),
            GateKind::Command,
            caller(),
            targets.clone(),
            "do it".to_string(),
            token.clone(),
            40,
        )
        .expect("prepare never touches the sink");
    assert_eq!(
        pending,
        Verdict::Pending {
            token: token.clone()
        }
    );
    assert!(gk.audit_entries().is_empty());

    // commit(Deny) → Denied; audit Some(Deny).
    let verdict = gk
        .commit(
            &token,
            GateDecision::Deny {
                reason: "vetoed".to_string(),
            },
            41,
        )
        .expect("commit of a pending token with a healthy sink");
    assert_eq!(
        verdict,
        Verdict::Denied {
            reason: "vetoed".to_string()
        }
    );

    let entries = gk.audit_entries();
    assert_eq!(entries.len(), 1);
    assert_eq!(
        entries[0].decision,
        Some(GateDecision::Deny {
            reason: "vetoed".to_string()
        })
    );

    // no dispatch: Mesh untouched.
    let mut registry = wasl_registry::PeerRegistry::new();
    registry.register(card("p1", "host-a"));
    let mut mesh = FakeMesh::new().with_reply("p1", "unreachable");

    if verdict == Verdict::Proceed {
        let params = CommandParams {
            peers: targets,
            task: "do it".to_string(),
        };
        let _ = wasl_daemon::command(
            &registry,
            &self_host(),
            "env-x7",
            &caller(),
            &params,
            &mut mesh,
        );
    }
    assert!(
        mesh.calls.is_empty(),
        "Mesh must be untouched when the commit denies"
    );
}

// ---------------------------------------------------------------------
// X8 — gate audit failure aborts before dispatch
// ---------------------------------------------------------------------

#[test]
fn x8_audit_failure_aborts_before_dispatch() {
    // sink scripted to fail: the decision cannot be durably recorded.
    let mut gk = Gatekeeper::new(
        caps_elicitation(),
        ScriptSurfacer::approving(),
        FakeSink::failing(),
    );

    let targets = vec![agent("p1")];
    let result = gk.authorize(
        "req-x8".to_string(),
        GateKind::Consult,
        caller(),
        targets.clone(),
        "please review".to_string(),
        ApprovalToken("tok-x8".to_string()),
        50,
    );

    // authorize returns Err(Audit); nothing durably recorded.
    match result {
        Err(ServerError::Audit(_)) => {}
        other => panic!("expected Err(ServerError::Audit(_)), got {other:?}"),
    }
    assert!(
        gk.audit_entries().is_empty(),
        "durability-first: a failed append caches nothing"
    );

    // The composition surfaces the error and does NOT dispatch — the daemon/Mesh
    // is never invoked.
    let mut registry = wasl_registry::PeerRegistry::new();
    registry.register(card("p1", "host-a"));
    let mut mesh = FakeMesh::new().with_reply("p1", "unreachable");

    if let Ok(Verdict::Proceed) = result {
        let params = ConsultParams {
            peers: targets,
            question: "please review".to_string(),
        };
        let _ = wasl_daemon::consult(
            &registry,
            &self_host(),
            "env-x8",
            &caller(),
            &params,
            &mut mesh,
        );
    }
    assert!(
        mesh.calls.is_empty(),
        "no dispatch when the gate audit fails"
    );
}
