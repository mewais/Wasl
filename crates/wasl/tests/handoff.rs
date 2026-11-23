//! Cross-host delegation with handoff gates (real daemons, hub, fake CLI).

mod common;

use common::*;
use serde_json::json;
use wasit_fake_cli::scenario::{Scenario, Turn};
use wasl_proto::*;

fn gated_worker() -> Scenario {
    Scenario::builder()
        .turn(
            Turn::builder()
                .say("let me check")
                .tool("Bash", json!({"command": "expr 2 + 2"}))
                .needs_permission()
                .result_if_allowed("4")
                .done()
                .say("The answer is 4."),
        )
        .build()
}

/// orchestrator@a → worker@b over an `ask` edge: the gate appears on the orchestrator's
/// snapshot, allow-once lets the task through, the worker's own native gate is answered, and
/// the result comes back to `task_wait`.
#[tokio::test]
async fn ask_edge_gate_allow_once_then_result_returns() {
    let s = Scratch::new();
    let a = Daemon::start(&s, "a").await;
    let b = Daemon::start(&s, "b").await;
    let hub = TestHub::start(&[&a, &b]).await;
    let orch = a.session(&Scenario::builder().build()).await;
    let worker = b.session(&gated_worker()).await;
    let t = topology(
        1,
        &[(&orch, "orchestrator"), (&worker, "worker")],
        &[(&orch, &worker, EdgePolicy::Ask)],
        Limits::default(),
    );
    a.set_topology(&t).await;
    b.set_topology(&t).await;

    let peers: PeerList = a
        .call(methods::PEERS_LIST, json!({"from": orch.session}))
        .await;
    assert_eq!(peers.peers.len(), 1);
    assert_eq!(peers.peers[0].card.name, "worker");
    assert_eq!(peers.peers[0].card.status, PeerStatus::Idle);

    let task = a.delegate1(&orch, &worker, "what is 2+2?").await;
    assert_eq!(
        task.state,
        TaskState::PendingApproval {
            via: ApprovalVia::Gate
        }
    );
    let gate = a.handoff_gate(&orch, &task.id).await;
    match &gate {
        wasit_core::GateRequest::Handoff {
            instruction, to, ..
        } => {
            assert_eq!(instruction, "what is 2+2?");
            assert!(to.contains("worker"));
        }
        other => panic!("not a handoff gate: {other:?}"),
    }
    // Still pending: nothing delivered yet.
    assert!(matches!(
        a.status(&task.id).await,
        TaskState::PendingApproval { .. }
    ));
    a.answer(
        &orch,
        gate.request_id(),
        HandoffDecision::AllowOnce.to_gate(),
    )
    .await;
    b.approve_native_gate(&worker).await;
    let done = a.wait_done(&task.id).await;
    assert_eq!(result_summary(&done), "The answer is 4.");
    // The worker saw a provenance header.
    let snap = b.client.snapshot(&sid(&worker)).await.unwrap();
    assert!(snap
        .title
        .as_deref()
        .is_some_and(|t| t.contains(&format!("[wasl task {}]", task.id))));
    // Audit trail on the sender.
    let audit = std::fs::read_to_string(a.dir.join("wasl/audit.ndjson")).unwrap();
    assert!(audit.contains("\"kind\":\"delegated\""));
    assert!(audit.contains("\"by\":\"gate\""));
    assert!(audit.contains("\"state\":\"completed\""));
    hub.shutdown().await;
}

/// "Always allow" in the gate updates the edge policy (event on `wasl.edges`), and the next
/// delegation passes without a gate.
#[tokio::test]
async fn always_allow_updates_policy_and_skips_the_next_gate() {
    let s = Scratch::new();
    let a = Daemon::start(&s, "a").await;
    let b = Daemon::start(&s, "b").await;
    let hub = TestHub::start(&[&a, &b]).await;
    let orch = a.session(&Scenario::builder().build()).await;
    let worker = b.session(&slow_worker("done", 1)).await;
    let t = topology(
        1,
        &[(&orch, "orchestrator"), (&worker, "worker")],
        &[(&orch, &worker, EdgePolicy::Ask)],
        Limits::default(),
    );
    a.set_topology(&t).await;
    b.set_topology(&t).await;
    let mut edges = a.edges_topic();

    let t1 = a.delegate1(&orch, &worker, "first").await;
    let gate = a.handoff_gate(&orch, &t1.id).await;
    a.answer(
        &orch,
        gate.request_id(),
        HandoffDecision::AlwaysAllow.to_gate(),
    )
    .await;
    assert_eq!(result_summary(&a.wait_done(&t1.id).await), "done");
    let ev = within("edge event", async {
        let (_, v) = edges.next().await.unwrap().unwrap();
        serde_json::from_value::<EdgeEvent>(v).unwrap()
    })
    .await;
    assert_eq!(ev.policy, EdgePolicy::AlwaysAllow);
    assert_eq!(ev.cause, EdgeChangeCause::Decision);
    assert_eq!((ev.from, ev.to), (orch.clone(), worker.clone()));
    let topo: TopologyList = a.call(methods::TOPOLOGY_GET, json!(null)).await;
    assert_eq!(topo.workstreams[0].edges[0].policy, EdgePolicy::AlwaysAllow);

    let t2 = a.delegate1(&orch, &worker, "second").await;
    assert_eq!(t2.state, TaskState::Queued);
    assert_eq!(result_summary(&a.wait_done(&t2.id).await), "done");
    hub.shutdown().await;
}

/// A denied gate ends the task as Denied with a message for the agent; a `deny` edge rejects
/// at once; a session with no edges gets "no connections configured".
#[tokio::test]
async fn deny_decision_deny_policy_and_no_connections() {
    let s = Scratch::new();
    let a = Daemon::start(&s, "a").await;
    let b = Daemon::start(&s, "b").await;
    let hub = TestHub::start(&[&a, &b]).await;
    let orch = a.session(&Scenario::builder().build()).await;
    let w1 = b.session(&slow_worker("x", 1)).await;
    let w2 = b.session(&slow_worker("y", 1)).await;
    let t = topology(
        1,
        &[(&orch, "orchestrator"), (&w1, "w1"), (&w2, "w2")],
        &[
            (&orch, &w1, EdgePolicy::Ask),
            (&orch, &w2, EdgePolicy::Deny),
        ],
        Limits::default(),
    );
    a.set_topology(&t).await;
    b.set_topology(&t).await;

    let t1 = a.delegate1(&orch, &w1, "please").await;
    let gate = a.handoff_gate(&orch, &t1.id).await;
    a.answer(
        &orch,
        gate.request_id(),
        HandoffDecision::Deny {
            reason: Some("not now".into()),
        }
        .to_gate(),
    )
    .await;
    match a.wait_done(&t1.id).await {
        TaskState::Denied { reason } => {
            assert!(
                reason.contains("denied") && reason.contains("not now"),
                "{reason}"
            )
        }
        other => panic!("expected denied, got {other:?}"),
    }

    let r = a
        .delegate(&orch, &[&w2], TaskMode::Command, "do it", None)
        .await;
    let e = r.results[0].error.as_ref().expect("rejected");
    assert_eq!(e.code, codes::DENIED);
    assert!(r.results[0].task.is_none());

    // A session without edges.
    let r = b
        .delegate(&w1, &[&orch], TaskMode::Command, "hello", None)
        .await;
    let e = r.results[0].error.as_ref().expect("rejected");
    assert_eq!(e.code, codes::NO_EDGE);
    assert!(
        e.message.contains("no connections configured"),
        "{}",
        e.message
    );
    let peers: PeerList = b
        .call(methods::PEERS_LIST, json!({"from": w1.session}))
        .await;
    assert!(peers.peers.is_empty());

    // Edges added later take effect live.
    let t2 = topology(
        2,
        &[(&orch, "orchestrator"), (&w1, "w1"), (&w2, "w2")],
        &[
            (&orch, &w1, EdgePolicy::Ask),
            (&orch, &w2, EdgePolicy::Deny),
            (&w1, &orch, EdgePolicy::AlwaysAllow),
        ],
        Limits::default(),
    );
    a.set_topology(&t2).await;
    b.set_topology(&t2).await;
    let peers: PeerList = b
        .call(methods::PEERS_LIST, json!({"from": w1.session}))
        .await;
    assert_eq!(peers.peers.len(), 1);
    // Older versions are refused, the same version is a no-op.
    assert!(b
        .try_call::<TopologyApplied>(methods::TOPOLOGY_SET, json!(t))
        .await
        .is_err());
    let same: TopologyApplied = b.call(methods::TOPOLOGY_SET, json!(t2)).await;
    assert!(!same.changed);
    hub.shutdown().await;
}

/// The pending handoff gate survives a restart of the sender's daemon and still works.
#[tokio::test]
async fn pending_gate_survives_sender_restart() {
    let s = Scratch::new();
    let mut a = Daemon::start(&s, "a").await;
    let b = Daemon::start(&s, "b").await;
    let hub = TestHub::start(&[&a, &b]).await;
    let orch = a.session(&Scenario::builder().build()).await;
    let worker = b.session(&slow_worker("survived", 1)).await;
    let t = topology(
        1,
        &[(&orch, "orchestrator"), (&worker, "worker")],
        &[(&orch, &worker, EdgePolicy::Ask)],
        Limits::default(),
    );
    a.set_topology(&t).await;
    b.set_topology(&t).await;
    let task = a.delegate1(&orch, &worker, "hold on").await;
    a.handoff_gate(&orch, &task.id).await;
    a.restart().await;
    hub.connected(&[&a, &b]).await;
    let gate = a.handoff_gate(&orch, &task.id).await;
    assert!(matches!(
        a.status(&task.id).await,
        TaskState::PendingApproval { .. }
    ));
    a.answer(
        &orch,
        gate.request_id(),
        HandoffDecision::AllowOnce.to_gate(),
    )
    .await;
    assert_eq!(result_summary(&a.wait_done(&task.id).await), "survived");
    hub.shutdown().await;
}

/// A handoff gate lives as long as its task and the turn that delegated: it is withdrawn when
/// that turn is interrupted, when the task is cancelled or expires, and when the sending
/// session's process ends; each time the task
/// ends without being delivered and a late "allow" is refused.
#[tokio::test]
async fn handoff_gate_is_withdrawn_on_interrupt_cancel_and_expiry() {
    let s = Scratch::new();
    let a = Daemon::start(&s, "a").await;
    let b = Daemon::start(&s, "b").await;
    let hub = TestHub::start(&[&a, &b]).await;
    let orch = a.session(&busy_until_interrupted()).await;
    let worker = b.session(&gated_worker()).await;
    let t = topology(
        1,
        &[(&orch, "orchestrator"), (&worker, "worker")],
        &[(&orch, &worker, EdgePolicy::Ask)],
        Limits::default(),
    );
    a.set_topology(&t).await;
    b.set_topology(&t).await;

    // Interrupted: the delegating turn is interrupted while the gate waits.
    a.start_turn(&orch).await;
    let t1 = a.delegate1(&orch, &worker, "first").await;
    let g1 = a.handoff_gate(&orch, &t1.id).await;
    a.interrupt(&orch).await;
    let why = match a.wait_done(&t1.id).await {
        TaskState::Cancelled { reason } => {
            assert!(reason.contains("interrupted"), "{reason}");
            reason
        }
        other => panic!("expected cancelled, got {other:?}"),
    };
    // The session says why too: the code, and the sentence the task records.
    let r = a
        .assert_withdrawn(&orch, g1.request_id(), "allow_once", "turn_interrupted")
        .await;
    assert_eq!(r.detail.as_deref(), Some(why.as_str()));
    // The session itself resolved the gate, before the interrupted turn completed.
    a.assert_gate_resolved_before_its_turn_completed(&orch, g1.request_id(), true)
        .await;

    // Cancelled by the sender (the session is idle now: the gate is not tied to a turn).
    let t2 = a.delegate1(&orch, &worker, "second").await;
    let g2 = a.handoff_gate(&orch, &t2.id).await;
    let _: TaskBrief = a.call(methods::TASK_CANCEL, json!({"id": t2.id})).await;
    let r = a
        .assert_withdrawn(&orch, g2.request_id(), "allow_once", "cancelled")
        .await;
    assert_eq!(r.detail.as_deref(), Some("cancelled by the sender"));
    assert!(matches!(
        a.status(&t2.id).await,
        TaskState::Cancelled { .. }
    ));

    // Expired while asking.
    let r = a
        .delegate(&orch, &[&worker], TaskMode::Consult, "third", Some(1))
        .await;
    let t3 = r.results[0].task.clone().unwrap();
    let g3 = a.handoff_gate(&orch, &t3.id).await;
    assert_eq!(a.wait_done(&t3.id).await, TaskState::Expired);
    let r = a
        .assert_withdrawn(&orch, g3.request_id(), "allow_once", "deadline_passed")
        .await;
    assert_eq!(r.detail.as_deref(), Some("deadline passed"));

    // Decided without the gate (a supervisor or an elicitation answers): the gate comes down
    // saying so.
    let t5 = a.delegate1(&orch, &worker, "fifth").await;
    let g5 = a.handoff_gate(&orch, &t5.id).await;
    let _: TaskBrief = a
        .call(
            methods::TASK_DECIDE,
            json!({"id": t5.id, "decision": {"decision": "deny", "reason": "no thanks"}}),
        )
        .await;
    let r = a
        .assert_withdrawn(&orch, g5.request_id(), "allow_once", "decided_elsewhere")
        .await;
    assert!(r.detail.is_some_and(|d| d.contains("elicitation")));
    assert!(matches!(a.status(&t5.id).await, TaskState::Denied { .. }));

    // The sending session's process ends (its CLI and MCP client with it).
    let t4 = a.delegate1(&orch, &worker, "fourth").await;
    let g4 = a.handoff_gate(&orch, &t4.id).await;
    a.client.stop(&sid(&orch)).await.unwrap();
    assert!(matches!(
        a.wait_done(&t4.id).await,
        TaskState::Cancelled { .. }
    ));
    // The runtime ended the gate with the process it was stopped with; Wasl said nothing.
    let r = a
        .assert_withdrawn(&orch, g4.request_id(), "allow_once", "session_stopped")
        .await;
    assert_eq!(r.detail, None);

    // Nothing ever reached the worker.
    let snap = b.client.snapshot(&sid(&worker)).await.unwrap();
    assert_eq!(snap.turns_completed, 0);
    assert!(snap.turn.is_none());
    hub.shutdown().await;
}
