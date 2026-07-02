//! Black-box loopback suite for `wasl_dispatch` (D1-D9 per `docs/specs/dispatch.md`).
//!
//! Every test drives the send→receive→reply path END-TO-END over 127.0.0.1
//! against a REAL `wasl_a2a_server` running a REAL `wasl_adapter::AdapterExecutor`
//! over a FAKE innermost `AgentAdapter` (the only fake). Peers are resolved
//! through a real `PeerRegistry` by their A2A endpoint. The suite MUST fail to
//! compile until `wasl_dispatch::{consult, command, message, DispatchReport}`
//! exist (test-first).

mod common;

use common::{agent, dead_url, http_client, register, spawn_echo, spawn_failing};

use wasl_adapter::DeliveryKind;
use wasl_dispatch::{command, consult, message, DispatchReport};
use wasl_proto::{CommandParams, ConsultParams, MessageParams, Reply};
use wasl_registry::PeerRegistry;

// ── D1 ───────────────────────────────────────────────────────────────────────
// consult([p1,p2]) → replies in target order; report empty; BOTH fake adapters
// observed a Delivery{ kind: Consult, from: caller, text: q } — proving the verb
// + sender crossed the wire.
#[tokio::test]
async fn d1_consult_fanout_replies_in_order_and_verb_crosses_wire() {
    let caller = agent("caller");
    let p1 = agent("p1");
    let p2 = agent("p2");

    let peer1 = spawn_echo().await;
    let peer2 = spawn_echo().await;

    let mut reg = PeerRegistry::new();
    register(&mut reg, &p1, Some(peer1.url.clone()));
    register(&mut reg, &p2, Some(peer2.url.clone()));

    let params = ConsultParams {
        peers: vec![p1.clone(), p2.clone()],
        question: "q".to_string(),
    };
    let (result, report) = consult(&reg, &caller, "base-1", &params, &http_client()).await;

    assert_eq!(
        result.replies,
        vec![
            Reply {
                from: p1.clone(),
                body: "reply:q".to_string(),
            },
            Reply {
                from: p2.clone(),
                body: "reply:q".to_string(),
            },
        ],
    );
    assert_eq!(report, DispatchReport::default());

    for peer in [&peer1, &peer2] {
        let deliveries = peer.deliveries();
        assert_eq!(
            deliveries.len(),
            1,
            "each peer receives exactly one delivery"
        );
        let d = &deliveries[0];
        assert_eq!(d.kind, DeliveryKind::Consult);
        assert_eq!(d.from, "caller");
        assert_eq!(d.text, "q");
        assert_eq!(d.context_id, "base-1");
        assert!(d.data.is_empty());
    }
}

// ── D2 ───────────────────────────────────────────────────────────────────────
// command([p1]) → adapter saw kind: Command; reply collected.
#[tokio::test]
async fn d2_command_carries_command_verb_and_collects_reply() {
    let caller = agent("boss");
    let p1 = agent("worker");

    let peer1 = spawn_echo().await;

    let mut reg = PeerRegistry::new();
    register(&mut reg, &p1, Some(peer1.url.clone()));

    let params = CommandParams {
        peers: vec![p1.clone()],
        task: "build it".to_string(),
    };
    let (result, report) = command(&reg, &caller, "base-2", &params, &http_client()).await;

    assert_eq!(
        result.replies,
        vec![Reply {
            from: p1.clone(),
            body: "reply:build it".to_string(),
        }],
    );
    assert_eq!(report, DispatchReport::default());

    let deliveries = peer1.deliveries();
    assert_eq!(deliveries.len(), 1);
    assert_eq!(deliveries[0].kind, DeliveryKind::Command);
    assert_eq!(deliveries[0].from, "boss");
    assert_eq!(deliveries[0].text, "build it");
}

// ── D3 ───────────────────────────────────────────────────────────────────────
// message([p1,p2]) → MessageResult.delivered = [p1,p2]; adapters saw kind: Message.
#[tokio::test]
async fn d3_message_reports_delivered_and_carries_message_verb() {
    let caller = agent("caller");
    let p1 = agent("p1");
    let p2 = agent("p2");

    let peer1 = spawn_echo().await;
    let peer2 = spawn_echo().await;

    let mut reg = PeerRegistry::new();
    register(&mut reg, &p1, Some(peer1.url.clone()));
    register(&mut reg, &p2, Some(peer2.url.clone()));

    let params = MessageParams {
        peers: vec![p1.clone(), p2.clone()],
        payload: "fyi".to_string(),
    };
    let (result, report) = message(&reg, &caller, "base-3", &params, &http_client()).await;

    assert_eq!(result.delivered, vec![p1.clone(), p2.clone()]);
    assert_eq!(report, DispatchReport::default());

    for peer in [&peer1, &peer2] {
        let deliveries = peer.deliveries();
        assert_eq!(deliveries.len(), 1);
        assert_eq!(deliveries[0].kind, DeliveryKind::Message);
        assert_eq!(deliveries[0].from, "caller");
        assert_eq!(deliveries[0].text, "fyi");
    }
}

// ── D4 ───────────────────────────────────────────────────────────────────────
// unknown peer (not registered) mixed with a known one → report.unknown has it,
// it is NOT delivered; the known peer still succeeds.
#[tokio::test]
async fn d4_unknown_peer_partitioned_known_peer_succeeds() {
    let caller = agent("caller");
    let ghost = agent("ghost");
    let p1 = agent("p1");

    let peer1 = spawn_echo().await;

    let mut reg = PeerRegistry::new();
    // ghost is intentionally NOT registered.
    register(&mut reg, &p1, Some(peer1.url.clone()));

    let params = ConsultParams {
        peers: vec![ghost.clone(), p1.clone()],
        question: "q".to_string(),
    };
    let (result, report) = consult(&reg, &caller, "base-4", &params, &http_client()).await;

    assert_eq!(
        result.replies,
        vec![Reply {
            from: p1.clone(),
            body: "reply:q".to_string(),
        }],
        "only the resolved peer produces a reply",
    );
    assert_eq!(report.unknown, vec![ghost.clone()]);
    assert!(report.failed.is_empty());

    // The known peer really was reached; the ghost never was (no server exists).
    let deliveries = peer1.deliveries();
    assert_eq!(deliveries.len(), 1);
    assert_eq!(deliveries[0].from, "caller");
}

// ── D5 ───────────────────────────────────────────────────────────────────────
// peer registered but endpoint: None → report.unknown (unroutable), not sent.
#[tokio::test]
async fn d5_endpoint_none_is_unroutable() {
    let caller = agent("caller");
    let p1 = agent("p1");

    let mut reg = PeerRegistry::new();
    register(&mut reg, &p1, None);

    let params = ConsultParams {
        peers: vec![p1.clone()],
        question: "q".to_string(),
    };
    let (result, report) = consult(&reg, &caller, "base-5", &params, &http_client()).await;

    assert!(result.replies.is_empty());
    assert_eq!(report.unknown, vec![p1.clone()]);
    assert!(report.failed.is_empty());
}

// ── D6 ───────────────────────────────────────────────────────────────────────
// transport failure: a peer at a dead port → report.failed; a co-addressed
// healthy peer still succeeds (partial multicast survives).
#[tokio::test]
async fn d6_transport_failure_is_failed_and_partial_multicast_survives() {
    let caller = agent("caller");
    let dead = agent("dead");
    let live = agent("live");

    let healthy = spawn_echo().await;
    let dead_endpoint = dead_url().await;

    let mut reg = PeerRegistry::new();
    register(&mut reg, &dead, Some(dead_endpoint));
    register(&mut reg, &live, Some(healthy.url.clone()));

    let params = ConsultParams {
        peers: vec![dead.clone(), live.clone()],
        question: "q".to_string(),
    };
    let (result, report) = consult(&reg, &caller, "base-6", &params, &http_client()).await;

    assert_eq!(
        result.replies,
        vec![Reply {
            from: live.clone(),
            body: "reply:q".to_string(),
        }],
        "the healthy co-target still succeeds",
    );
    assert_eq!(report.failed, vec![dead.clone()]);
    assert!(report.unknown.is_empty());

    // The healthy peer was reached; the dead one produced no delivery.
    assert_eq!(healthy.deliveries().len(), 1);
}

// ── D7 ───────────────────────────────────────────────────────────────────────
// executor failure: a peer whose fake adapter returns AdapterError → the A2A Task
// comes back Failed → report.failed (distinct source, same bucket as transport).
#[tokio::test]
async fn d7_executor_failure_returns_failed_task() {
    let caller = agent("caller");
    let broken = agent("broken");

    let peer = spawn_failing("boom").await;

    let mut reg = PeerRegistry::new();
    register(&mut reg, &broken, Some(peer.url.clone()));

    let params = ConsultParams {
        peers: vec![broken.clone()],
        question: "q".to_string(),
    };
    let (result, report) = consult(&reg, &caller, "base-7", &params, &http_client()).await;

    assert!(result.replies.is_empty());
    assert_eq!(report.failed, vec![broken.clone()]);
    assert!(report.unknown.is_empty());

    // Distinct from a transport failure: the message DID reach the executor (the
    // fake adapter observed it) — the failure came from the executor returning
    // an error, not from a dead socket.
    let deliveries = peer.deliveries();
    assert_eq!(deliveries.len(), 1);
    assert_eq!(deliveries[0].kind, DeliveryKind::Consult);
}

// ── D8 ───────────────────────────────────────────────────────────────────────
// multicast ordering: 3 healthy peers → replies in target order (and each peer
// saw the shared context_id = base_id).
#[tokio::test]
async fn d8_multicast_preserves_target_order() {
    let caller = agent("caller");
    let p1 = agent("p1");
    let p2 = agent("p2");
    let p3 = agent("p3");

    let peer1 = spawn_echo().await;
    let peer2 = spawn_echo().await;
    let peer3 = spawn_echo().await;

    let mut reg = PeerRegistry::new();
    register(&mut reg, &p1, Some(peer1.url.clone()));
    register(&mut reg, &p2, Some(peer2.url.clone()));
    register(&mut reg, &p3, Some(peer3.url.clone()));

    let params = ConsultParams {
        peers: vec![p1.clone(), p2.clone(), p3.clone()],
        question: "q3".to_string(),
    };
    let (result, report) = consult(&reg, &caller, "multi", &params, &http_client()).await;

    assert_eq!(
        result.replies,
        vec![
            Reply {
                from: p1.clone(),
                body: "reply:q3".to_string(),
            },
            Reply {
                from: p2.clone(),
                body: "reply:q3".to_string(),
            },
            Reply {
                from: p3.clone(),
                body: "reply:q3".to_string(),
            },
        ],
        "replies come back in target order regardless of concurrent completion",
    );
    assert_eq!(report, DispatchReport::default());

    for peer in [&peer1, &peer2, &peer3] {
        let deliveries = peer.deliveries();
        assert_eq!(deliveries.len(), 1);
        assert_eq!(deliveries[0].context_id, "multi");
    }
}

// ── D9 ───────────────────────────────────────────────────────────────────────
// DispatchReport Default is empty; equality distinguishes empty vs populated.
#[test]
fn d9_report_default_and_equality() {
    let empty = DispatchReport::default();
    assert!(empty.unknown.is_empty());
    assert!(empty.failed.is_empty());
    assert_eq!(empty, DispatchReport::default());

    let populated = DispatchReport {
        unknown: vec![agent("x")],
        failed: vec![agent("y")],
    };
    assert_ne!(populated, DispatchReport::default());
    assert_eq!(
        populated,
        DispatchReport {
            unknown: vec![agent("x")],
            failed: vec![agent("y")],
        },
    );
    // A difference in either field breaks equality.
    assert_ne!(
        DispatchReport {
            unknown: vec![agent("x")],
            failed: vec![],
        },
        DispatchReport {
            unknown: vec![],
            failed: vec![agent("x")],
        },
    );
}
