//! Happy-path black-box suite for `wasl_daemon` — covers D1..D10 of
//! `docs/specs/daemon.md` §Test plan.

mod common;

use common::{agent, card, host, FakeMesh, FakeRunner};

use wasl_proto::{
    AgentId, CommandParams, ConsultParams, EnvelopeKind, MessageParams, TransferParams,
};
use wasl_registry::{PeerRegistry, Resolved};
use wasl_transfer::Tool;

use wasl_daemon::{command, consult, dispatch, message, transfer, DispatchReport};

// ---------------------------------------------------------------------
// D1 — dispatch all-local consult: two same-host peers.
// ---------------------------------------------------------------------
#[test]
fn d1_dispatch_all_local_consult() {
    let mut reg = PeerRegistry::new();
    reg.register(card("a", "h0"));
    reg.register(card("b", "h0"));

    let self_host = host("h0");
    let from = agent("caller");
    let targets = vec![agent("a"), agent("b")];

    let mut mesh = FakeMesh::new().with_reply("a", "yes").with_reply("b", "no");

    let out = dispatch(
        &reg,
        &self_host,
        "cx",
        &from,
        EnvelopeKind::Consult,
        "why?",
        &targets,
        &mut mesh,
    );

    // Both delivered, in target order.
    assert_eq!(out.delivered, vec![agent("a"), agent("b")]);
    // Replies carry each peer as `from` and the fake's scripted body.
    assert_eq!(out.replies.len(), 2);
    assert_eq!(out.replies[0].from, agent("a"));
    assert_eq!(out.replies[0].body, "yes");
    assert_eq!(out.replies[1].from, agent("b"));
    assert_eq!(out.replies[1].body, "no");
    // Empty report.
    assert!(out.report.unknown.is_empty());
    assert!(out.report.failed.is_empty());

    // The Mesh saw two envelopes with ids base:0 / base:1, Consult kind, right `to`.
    assert_eq!(mesh.calls.len(), 2);
    assert_eq!(mesh.calls[0].env.id, "cx:0");
    assert_eq!(mesh.calls[1].env.id, "cx:1");
    assert_eq!(mesh.calls[0].env.kind, EnvelopeKind::Consult);
    assert_eq!(mesh.calls[1].env.kind, EnvelopeKind::Consult);
    assert_eq!(mesh.calls[0].env.body, "why?");
    assert_eq!(mesh.calls[0].env.from, from);
    assert_eq!(FakeMesh::to_of(&mesh.calls[0]), &agent("a"));
    assert_eq!(FakeMesh::to_of(&mesh.calls[1]), &agent("b"));
    // Both routes resolved Local (same host).
    assert_eq!(mesh.calls[0].resolved, Resolved::Local { peer: agent("a") });
    assert_eq!(mesh.calls[1].resolved, Resolved::Local { peer: agent("b") });
}

// ---------------------------------------------------------------------
// D2 — cross-host: a remote peer resolves Remote{host} and its reply is kept.
// ---------------------------------------------------------------------
#[test]
fn d2_dispatch_cross_host_remote() {
    let mut reg = PeerRegistry::new();
    reg.register(card("r", "h1")); // remote: different host than self.

    let self_host = host("h0");
    let from = agent("caller");
    let targets = vec![agent("r")];

    let mut mesh = FakeMesh::new().with_reply("r", "remote-ok");

    let out = dispatch(
        &reg,
        &self_host,
        "cx",
        &from,
        EnvelopeKind::Consult,
        "ping",
        &targets,
        &mut mesh,
    );

    assert_eq!(out.delivered, vec![agent("r")]);
    assert_eq!(out.replies.len(), 1);
    assert_eq!(out.replies[0].from, agent("r"));
    assert_eq!(out.replies[0].body, "remote-ok");
    assert!(out.report.unknown.is_empty());
    assert!(out.report.failed.is_empty());

    // The Mesh received a Remote route with the target's host attached.
    assert_eq!(mesh.calls.len(), 1);
    assert_eq!(
        mesh.calls[0].resolved,
        Resolved::Remote {
            peer: agent("r"),
            host: host("h1"),
        }
    );
    assert_eq!(mesh.calls[0].env.id, "cx:0");
}

// ---------------------------------------------------------------------
// D3 — command wrapper: CommandResult{replies}, kind Command, body = task.
// ---------------------------------------------------------------------
#[test]
fn d3_command_wrapper() {
    let mut reg = PeerRegistry::new();
    reg.register(card("a", "h0"));
    reg.register(card("b", "h0"));

    let self_host = host("h0");
    let from = agent("caller");
    let params = CommandParams {
        peers: vec![agent("a"), agent("b")],
        task: "do-it".to_string(),
    };

    let mut mesh = FakeMesh::new()
        .with_reply("a", "started-a")
        .with_reply("b", "started-b");

    let (result, report) = command(&reg, &self_host, "cmd", &from, &params, &mut mesh);

    assert_eq!(result.replies.len(), 2);
    assert_eq!(result.replies[0].from, agent("a"));
    assert_eq!(result.replies[0].body, "started-a");
    assert_eq!(result.replies[1].from, agent("b"));
    assert_eq!(result.replies[1].body, "started-b");
    assert!(report.unknown.is_empty());
    assert!(report.failed.is_empty());

    // Envelopes carried the Command kind and the task body.
    assert_eq!(mesh.calls.len(), 2);
    assert_eq!(mesh.calls[0].env.kind, EnvelopeKind::Command);
    assert_eq!(mesh.calls[1].env.kind, EnvelopeKind::Command);
    assert_eq!(mesh.calls[0].env.body, "do-it");
    assert_eq!(mesh.calls[0].env.id, "cmd:0");
    assert_eq!(mesh.calls[1].env.id, "cmd:1");
}

// ---------------------------------------------------------------------
// D4 — message wrapper: MessageResult{delivered}, kind Message, body = payload,
//      independent of reply bodies.
// ---------------------------------------------------------------------
#[test]
fn d4_message_wrapper() {
    let mut reg = PeerRegistry::new();
    reg.register(card("a", "h0"));
    reg.register(card("b", "h0"));

    let self_host = host("h0");
    let from = agent("caller");
    let params = MessageParams {
        peers: vec![agent("a"), agent("b")],
        payload: "fyi".to_string(),
    };

    // No scripted replies at all: message must not depend on reply bodies.
    let mut mesh = FakeMesh::new();

    let (result, report) = message(&reg, &self_host, "msg", &from, &params, &mut mesh);

    assert_eq!(result.delivered, vec![agent("a"), agent("b")]);
    assert!(report.unknown.is_empty());
    assert!(report.failed.is_empty());

    assert_eq!(mesh.calls.len(), 2);
    assert_eq!(mesh.calls[0].env.kind, EnvelopeKind::Message);
    assert_eq!(mesh.calls[1].env.kind, EnvelopeKind::Message);
    assert_eq!(mesh.calls[0].env.body, "fyi");
    assert_eq!(mesh.calls[0].env.id, "msg:0");
    assert_eq!(mesh.calls[1].env.id, "msg:1");
    assert_eq!(FakeMesh::to_of(&mesh.calls[0]), &agent("a"));
    assert_eq!(FakeMesh::to_of(&mesh.calls[1]), &agent("b"));
}

// ---------------------------------------------------------------------
// D5 — multicast ordering: 3 peers → ids base:0..2 in target order, replies
//      in the same order.
// ---------------------------------------------------------------------
#[test]
fn d5_multicast_ordering() {
    let mut reg = PeerRegistry::new();
    reg.register(card("p0", "h0"));
    reg.register(card("p1", "h0"));
    reg.register(card("p2", "h0"));

    let self_host = host("h0");
    let from = agent("caller");
    // Deliberately non-sorted target order to prove order follows targets.
    let targets = vec![agent("p2"), agent("p0"), agent("p1")];

    let mut mesh = FakeMesh::new()
        .with_reply("p2", "r2")
        .with_reply("p0", "r0")
        .with_reply("p1", "r1");

    let out = dispatch(
        &reg,
        &self_host,
        "mc",
        &from,
        EnvelopeKind::Consult,
        "q",
        &targets,
        &mut mesh,
    );

    // Delivered + replies follow the target order exactly.
    assert_eq!(out.delivered, vec![agent("p2"), agent("p0"), agent("p1")]);
    assert_eq!(
        out.replies
            .iter()
            .map(|r| r.from.clone())
            .collect::<Vec<AgentId>>(),
        vec![agent("p2"), agent("p0"), agent("p1")]
    );
    assert_eq!(
        out.replies
            .iter()
            .map(|r| r.body.clone())
            .collect::<Vec<String>>(),
        vec!["r2".to_string(), "r0".to_string(), "r1".to_string()]
    );

    // Envelope ids are base:0..2 in target order.
    assert_eq!(mesh.calls.len(), 3);
    assert_eq!(mesh.calls[0].env.id, "mc:0");
    assert_eq!(mesh.calls[1].env.id, "mc:1");
    assert_eq!(mesh.calls[2].env.id, "mc:2");
    assert_eq!(FakeMesh::to_of(&mesh.calls[0]), &agent("p2"));
    assert_eq!(FakeMesh::to_of(&mesh.calls[1]), &agent("p0"));
    assert_eq!(FakeMesh::to_of(&mesh.calls[2]), &agent("p1"));
}

// ---------------------------------------------------------------------
// D6 — unknown peer: an unregistered target → report.unknown, never delivered,
//      others still succeed.
// ---------------------------------------------------------------------
#[test]
fn d6_unknown_peer_partitions() {
    let mut reg = PeerRegistry::new();
    reg.register(card("a", "h0"));
    reg.register(card("c", "h0"));
    // "b" intentionally not registered.

    let self_host = host("h0");
    let from = agent("caller");
    let targets = vec![agent("a"), agent("b"), agent("c")];

    let mut mesh = FakeMesh::new().with_reply("a", "ra").with_reply("c", "rc");

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

    // Only the known peers delivered; the unknown one is reported and not delivered.
    assert_eq!(out.delivered, vec![agent("a"), agent("c")]);
    assert_eq!(out.report.unknown, vec![agent("b")]);
    assert!(out.report.failed.is_empty());
    assert_eq!(out.replies.len(), 2);
    assert_eq!(out.replies[0].from, agent("a"));
    assert_eq!(out.replies[1].from, agent("c"));

    // Mesh was only asked to deliver the two known peers (unknown never reaches it).
    assert_eq!(mesh.calls.len(), 2);
    assert_eq!(FakeMesh::to_of(&mesh.calls[0]), &agent("a"));
    assert_eq!(FakeMesh::to_of(&mesh.calls[1]), &agent("c"));
    // Fan-out numbers over resolved peers, so ids are contiguous 0,1.
    assert_eq!(mesh.calls[0].env.id, "cx:0");
    assert_eq!(mesh.calls[1].env.id, "cx:1");
}

// ---------------------------------------------------------------------
// D7 — partial failure: fail the 2nd of 3 → delivered [1st,3rd], failed [2nd],
//      replies only for delivered (a failure doesn't abort the rest).
// ---------------------------------------------------------------------
#[test]
fn d7_partial_failure_does_not_abort() {
    let mut reg = PeerRegistry::new();
    reg.register(card("a", "h0"));
    reg.register(card("b", "h0"));
    reg.register(card("c", "h0"));

    let self_host = host("h0");
    let from = agent("caller");
    let targets = vec![agent("a"), agent("b"), agent("c")];

    let mut mesh = FakeMesh::new()
        .with_reply("a", "ra")
        .with_failure("b", "down")
        .with_reply("c", "rc");

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

    assert_eq!(out.delivered, vec![agent("a"), agent("c")]);
    assert_eq!(out.report.failed, vec![agent("b")]);
    assert!(out.report.unknown.is_empty());
    // Replies only for the delivered peers, in delivery order.
    assert_eq!(out.replies.len(), 2);
    assert_eq!(out.replies[0].from, agent("a"));
    assert_eq!(out.replies[0].body, "ra");
    assert_eq!(out.replies[1].from, agent("c"));
    assert_eq!(out.replies[1].body, "rc");

    // All three were attempted (failure did not abort iteration).
    assert_eq!(mesh.calls.len(), 3);
    assert_eq!(FakeMesh::to_of(&mesh.calls[0]), &agent("a"));
    assert_eq!(FakeMesh::to_of(&mesh.calls[1]), &agent("b"));
    assert_eq!(FakeMesh::to_of(&mesh.calls[2]), &agent("c"));
}

// ---------------------------------------------------------------------
// D8 — empty targets → empty Dispatch, Mesh never called.
// ---------------------------------------------------------------------
#[test]
fn d8_empty_targets() {
    let reg = PeerRegistry::new();
    let self_host = host("h0");
    let from = agent("caller");
    let targets: Vec<AgentId> = Vec::new();

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
    assert!(out.report.unknown.is_empty());
    assert!(out.report.failed.is_empty());
    // Mesh was never asked to deliver anything.
    assert!(mesh.calls.is_empty());
}

// ---------------------------------------------------------------------
// D9 — transfer all-success via fake Runner → delivered = all dst hosts,
//      no failures.
// ---------------------------------------------------------------------
#[test]
fn d9_transfer_all_success() {
    let params = TransferParams {
        src_host: host("h0"),
        dst_hosts: vec![host("h1"), host("h2")],
        path: "/data/file".to_string(),
    };

    let mut runner = FakeRunner::new();

    let (result, failures) =
        transfer(Tool::Scp, &params, &mut runner).expect("non-empty dst_hosts must be Ok");

    assert_eq!(result.delivered, vec![host("h1"), host("h2")]);
    assert!(failures.is_empty());

    // Runner ran one job per destination, with the src/dst-addressed scp argv.
    assert_eq!(runner.argvs.len(), 2);
    assert_eq!(
        runner.argvs[0],
        vec![
            "scp".to_string(),
            "-3".to_string(),
            "h0:/data/file".to_string(),
            "h1:/data/file".to_string(),
        ]
    );
    assert_eq!(
        runner.argvs[1],
        vec![
            "scp".to_string(),
            "-3".to_string(),
            "h0:/data/file".to_string(),
            "h2:/data/file".to_string(),
        ]
    );
}

// ---------------------------------------------------------------------
// D10 — DispatchReport serde round-trip (default empty + populated).
// ---------------------------------------------------------------------
#[test]
fn d10_dispatch_report_serde_round_trip() {
    // Default is empty on both fields.
    let empty = DispatchReport::default();
    assert!(empty.unknown.is_empty());
    assert!(empty.failed.is_empty());

    let empty_json = serde_json::to_string(&empty).expect("serialize default");
    let empty_back: DispatchReport =
        serde_json::from_str(&empty_json).expect("deserialize default");
    assert_eq!(empty_back, empty);

    // Populated round-trip preserves both partitions exactly.
    let populated = DispatchReport {
        unknown: vec![agent("u1"), agent("u2")],
        failed: vec![agent("f1")],
    };
    let json = serde_json::to_string(&populated).expect("serialize populated");
    let back: DispatchReport = serde_json::from_str(&json).expect("deserialize populated");
    assert_eq!(back, populated);
    assert_eq!(back.unknown, vec![agent("u1"), agent("u2")]);
    assert_eq!(back.failed, vec![agent("f1")]);
}

// consult wrapper exercised alongside D1's dispatch to prove the typed wrapper
// builds ConsultResult{replies} with the Consult kind + question body.
#[test]
fn d1_consult_wrapper() {
    let mut reg = PeerRegistry::new();
    reg.register(card("a", "h0"));
    reg.register(card("b", "h0"));

    let self_host = host("h0");
    let from = agent("caller");
    let params = ConsultParams {
        peers: vec![agent("a"), agent("b")],
        question: "why?".to_string(),
    };

    let mut mesh = FakeMesh::new().with_reply("a", "yes").with_reply("b", "no");

    let (result, report) = consult(&reg, &self_host, "cx", &from, &params, &mut mesh);

    assert_eq!(result.replies.len(), 2);
    assert_eq!(result.replies[0].from, agent("a"));
    assert_eq!(result.replies[0].body, "yes");
    assert_eq!(result.replies[1].from, agent("b"));
    assert_eq!(result.replies[1].body, "no");
    assert!(report.unknown.is_empty());
    assert!(report.failed.is_empty());

    assert_eq!(mesh.calls.len(), 2);
    assert_eq!(mesh.calls[0].env.kind, EnvelopeKind::Consult);
    assert_eq!(mesh.calls[0].env.body, "why?");
    assert_eq!(mesh.calls[0].env.id, "cx:0");
}
