//! Error / edge-case black-box suite for `wasl_daemon` — covers E1..E3 of
//! `docs/specs/daemon.md` §Test plan.

mod common;

use common::{agent, card, host, FakeMesh, FakeRunner};

use wasl_proto::{AgentId, EnvelopeKind, TransferParams};
use wasl_registry::PeerRegistry;
use wasl_transfer::{Tool, TransferError};

use wasl_daemon::{dispatch, transfer};

// ---------------------------------------------------------------------
// E1 — transfer with empty dst_hosts → Err(NoDestinations), runner untouched.
// ---------------------------------------------------------------------
#[test]
fn e1_transfer_no_destinations() {
    let params = TransferParams {
        src_host: host("h0"),
        dst_hosts: Vec::new(),
        path: "/data/file".to_string(),
    };

    let mut runner = FakeRunner::new();

    let err = transfer(Tool::Scp, &params, &mut runner)
        .expect_err("empty dst_hosts must be an error");
    assert_eq!(err, TransferError::NoDestinations);

    // The runner was never invoked.
    assert!(runner.argvs.is_empty());
}

// ---------------------------------------------------------------------
// E2 — transfer partial: runner fails one dst → Ok with that host in failures,
//      the other delivered.
// ---------------------------------------------------------------------
#[test]
fn e2_transfer_partial_failure() {
    let params = TransferParams {
        src_host: host("h0"),
        dst_hosts: vec![host("h1"), host("h2")],
        path: "/data/file".to_string(),
    };

    // Fail only h2.
    let mut runner = FakeRunner::new().failing("h2");

    let (result, failures) =
        transfer(Tool::Scp, &params, &mut runner).expect("partial failure is still Ok");

    // h1 delivered; h2 recorded as a Failed error naming that host.
    assert_eq!(result.delivered, vec![host("h1")]);
    assert_eq!(failures.len(), 1);
    match &failures[0] {
        TransferError::Failed { dst_host, .. } => assert_eq!(dst_host, &host("h2")),
        other => panic!("expected Failed{{h2}}, got {other:?}"),
    }

    // Both jobs were attempted (failure does not abort the rest).
    assert_eq!(runner.argvs.len(), 2);
    assert_eq!(runner.argvs[0].last().unwrap(), "h1:/data/file");
    assert_eq!(runner.argvs[1].last().unwrap(), "h2:/data/file");
}

// ---------------------------------------------------------------------
// E3 — all-unknown: every target unregistered → delivered empty,
//      report.unknown = all, Mesh never called.
// ---------------------------------------------------------------------
#[test]
fn e3_all_unknown_targets() {
    let mut reg = PeerRegistry::new();
    // Register an unrelated peer so the registry is non-empty but has none of
    // the targets.
    reg.register(card("other", "h0"));

    let self_host = host("h0");
    let from = agent("caller");
    let targets = vec![agent("x"), agent("y"), agent("z")];

    let mut mesh = FakeMesh::new();

    let out = dispatch(
        &reg,
        &self_host,
        "cx",
        &from,
        EnvelopeKind::Consult,
        "q",
        &targets,
        &mut mesh,
    );

    assert!(out.delivered.is_empty());
    assert!(out.replies.is_empty());
    assert!(out.report.failed.is_empty());
    // Every target reported unknown, in target order.
    assert_eq!(
        out.report.unknown,
        vec![agent("x"), agent("y"), agent("z")] as Vec<AgentId>
    );

    // The Mesh was never asked to deliver anything.
    assert!(mesh.calls.is_empty());
}
