//! Cross-crate end-to-end scenario suite for Wasl (spec: `docs/specs/e2e.md`).
//!
//! Each test embodies the "server loop": build a `Gatekeeper` (wasl-server) over
//! a `Surfacer` + audit `Sink`, call `authorize(...)` (or `commit(...)` for
//! Tier-2), and ONLY on `Verdict::Proceed` dispatch over the LIVE A2A path
//! (`wasl_dispatch::{consult,command,message}` against a REAL loopback
//! `wasl_a2a_server`, driven by a REAL `AdapterExecutor` over a FAKE innermost
//! `AgentAdapter`). Every test asserts BOTH halves of the seam — the recorded
//! gate decision AND the dispatch outcome — with concrete values, so the two
//! agree with the verdict. Tier-3 (external authority) records its decision
//! into the generic audit log — the observation feed IS the audit log.
//!
//! See `AGENTS.md` for the old (legacy `wasl-daemon`+`FakeMesh`) -> new (live
//! A2A) scenario mapping.

mod common;

use common::{
    agent, dead_url, host, http_client, register, spawn_echo, FakeRunner, FakeSink, ScriptSurfacer,
};

use wasl_audit::EntryKind;
use wasl_dispatch::{command, consult, message};
use wasl_gate::{ClientCaps, GateMode};
use wasl_proto::{
    AgentId, ApprovalToken, CommandParams, ConsultParams, GateDecision, GateKind, HostId,
    MessageParams, TransferParams,
};
use wasl_registry::PeerRegistry;
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

#[tokio::test]
async fn x1_approved_consult_gated_then_dispatched() {
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

    // --- dispatch side: 2 REAL loopback peers over the live A2A path ---
    let peer1 = spawn_echo().await;
    let peer2 = spawn_echo().await;

    let mut registry = PeerRegistry::new();
    register(&mut registry, "p1", &peer1.url);
    register(&mut registry, "p2", &peer2.url);

    let params = ConsultParams {
        peers: targets.clone(),
        question: "please review".to_string(),
    };
    let (result, report) = consult(&registry, &caller(), "env-x1", &params, &http_client()).await;

    // replies from both peers, in target order, exact bodies.
    assert_eq!(result.replies.len(), 2);
    assert_eq!(result.replies[0].from, agent("p1"));
    assert_eq!(result.replies[0].body, "reply:please review");
    assert_eq!(result.replies[1].from, agent("p2"));
    assert_eq!(result.replies[1].body, "reply:please review");

    // report empty — nothing unknown/failed.
    assert!(report.unknown.is_empty());
    assert!(report.failed.is_empty());

    // seam agreement: the gate approved AND the live A2A path delivered both.
    assert_eq!(peer1.deliveries().len(), 1);
    assert_eq!(peer2.deliveries().len(), 1);
}

// ---------------------------------------------------------------------
// X2 — denied → no dispatch, peer never reached
// ---------------------------------------------------------------------

#[tokio::test]
async fn x2_denied_short_circuits_no_dispatch() {
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

    // The composition MUST NOT dispatch. We prove it by never calling
    // `wasl_dispatch::consult` (the branch is only reachable on Proceed) — the
    // live peer is up but never reached.
    let peer1 = spawn_echo().await;
    let mut registry = PeerRegistry::new();
    register(&mut registry, "p1", &peer1.url);

    if verdict == Verdict::Proceed {
        let params = ConsultParams {
            peers: targets,
            question: "please review".to_string(),
        };
        let _ = consult(&registry, &caller(), "env-x2", &params, &http_client()).await;
    }

    assert!(
        peer1.deliveries().is_empty(),
        "the live A2A peer must be untouched when the gate denies"
    );
}

// ---------------------------------------------------------------------
// X3 — Tier-2 prepare → commit → dispatch
// ---------------------------------------------------------------------

#[tokio::test]
async fn x3_tier2_prepare_commit_then_dispatch() {
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

    // dispatch a command over the live A2A path.
    let peer1 = spawn_echo().await;
    let peer2 = spawn_echo().await;

    let mut registry = PeerRegistry::new();
    register(&mut registry, "p1", &peer1.url);
    register(&mut registry, "p2", &peer2.url);

    let params = CommandParams {
        peers: targets,
        task: "do the thing".to_string(),
    };
    let (result, report) = command(&registry, &caller(), "env-x3", &params, &http_client()).await;

    assert_eq!(result.replies.len(), 2);
    assert_eq!(result.replies[0].from, agent("p1"));
    assert_eq!(result.replies[0].body, "reply:do the thing");
    assert_eq!(result.replies[1].from, agent("p2"));
    assert_eq!(result.replies[1].body, "reply:do the thing");
    assert!(report.unknown.is_empty());
    assert!(report.failed.is_empty());

    assert_eq!(peer1.deliveries().len(), 1);
    assert_eq!(peer2.deliveries().len(), 1);
}

// ---------------------------------------------------------------------
// X4 — Tier-3 via an external authority (decision → the generic audit log)
// ---------------------------------------------------------------------

#[tokio::test]
async fn x4_tier3_external_authority_records_decision_and_dispatches() {
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

    // dispatch a message over the live A2A path → delivered = [p1].
    let peer1 = spawn_echo().await;
    let mut registry = PeerRegistry::new();
    register(&mut registry, "p1", &peer1.url);

    let params = MessageParams {
        peers: targets,
        payload: "ping".to_string(),
    };
    let (result, report) = message(&registry, &caller(), "env-x4", &params, &http_client()).await;

    assert_eq!(result.delivered, vec![agent("p1")]);
    assert!(report.unknown.is_empty());
    assert!(report.failed.is_empty());
    assert_eq!(peer1.deliveries().len(), 1);
}

// ---------------------------------------------------------------------
// X5 — approved transfer
// ---------------------------------------------------------------------
//
// Transfer is host-addressed, not peer-addressed, so it never went through the
// daemon's `Mesh`/dispatch's A2A path — `wasl_daemon::transfer` was a one-line
// passthrough to `wasl_transfer::execute`. X5 now calls that directly.

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
        wasl_transfer::execute(Tool::Scp, &params, &mut runner).expect("non-empty dst list");

    assert_eq!(result.delivered, vec![dst1, dst2]);
    assert!(failures.is_empty());
    assert_eq!(runner.argvs.len(), 2);
}

// ---------------------------------------------------------------------
// X6 — multicast with an unknown peer + a failed delivery
// ---------------------------------------------------------------------

#[tokio::test]
async fn x6_multicast_unknown_and_failed_partition() {
    let mut gk = Gatekeeper::new(
        caps_elicitation(),
        ScriptSurfacer::approving(),
        FakeSink::ok(),
    );

    // 3 targets: p1 delivers, p2 fails (dead loopback port = transport failure),
    // ghost is unregistered.
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

    let peer1 = spawn_echo().await;
    let dead = dead_url().await;

    let mut registry = PeerRegistry::new();
    register(&mut registry, "p1", &peer1.url);
    register(&mut registry, "p2", &dead);
    // "ghost" is intentionally NOT registered.

    let params = ConsultParams {
        peers: targets,
        question: "poll".to_string(),
    };
    let (result, report) = consult(&registry, &caller(), "env-x6", &params, &http_client()).await;

    // replies only for the delivered peer.
    assert_eq!(result.replies.len(), 1);
    assert_eq!(result.replies[0].from, agent("p1"));
    assert_eq!(result.replies[0].body, "reply:poll");

    // report partitions: ghost unknown, p2 failed.
    assert_eq!(report.unknown, vec![agent("ghost")]);
    assert_eq!(report.failed, vec![agent("p2")]);

    // partial multicast survived: p1 was really reached over the live A2A path.
    assert_eq!(peer1.deliveries().len(), 1);
}

// ---------------------------------------------------------------------
// X7 — Tier-2 commit denied → no dispatch
// ---------------------------------------------------------------------

#[tokio::test]
async fn x7_tier2_commit_denied_no_dispatch() {
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

    // no dispatch: the live A2A peer is untouched.
    let peer1 = spawn_echo().await;
    let mut registry = PeerRegistry::new();
    register(&mut registry, "p1", &peer1.url);

    if verdict == Verdict::Proceed {
        let params = CommandParams {
            peers: targets,
            task: "do it".to_string(),
        };
        let _ = command(&registry, &caller(), "env-x7", &params, &http_client()).await;
    }
    assert!(
        peer1.deliveries().is_empty(),
        "the live A2A peer must be untouched when the commit denies"
    );
}

// ---------------------------------------------------------------------
// X8 — gate audit failure aborts before dispatch
// ---------------------------------------------------------------------

#[tokio::test]
async fn x8_audit_failure_aborts_before_dispatch() {
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

    // The composition surfaces the error and does NOT dispatch — the live A2A
    // peer is never invoked.
    let peer1 = spawn_echo().await;
    let mut registry = PeerRegistry::new();
    register(&mut registry, "p1", &peer1.url);

    if let Ok(Verdict::Proceed) = result {
        let params = ConsultParams {
            peers: targets,
            question: "please review".to_string(),
        };
        let _ = consult(&registry, &caller(), "env-x8", &params, &http_client()).await;
    }
    assert!(
        peer1.deliveries().is_empty(),
        "no dispatch when the gate audit fails"
    );
}
