//! Store-and-forward and recovery: the uplink going away, and daemon restarts on either side
//! while a task is in flight.

mod common;

use common::*;
use wasit_fake_cli::scenario::Scenario;
use wasl_proto::*;

async fn pair(s: &Scratch, worker_ms: u64) -> (Daemon, Daemon, AgentId, AgentId) {
    let a = Daemon::start(s, "a").await;
    let b = Daemon::start(s, "b").await;
    let orch = a.session(&Scenario::builder().build()).await;
    let worker = b.session(&slow_worker("delivered", worker_ms)).await;
    let t = topology(
        1,
        &[(&orch, "o"), (&worker, "w")],
        &[(&orch, &worker, EdgePolicy::AlwaysAllow)],
        Limits::default(),
    );
    a.set_topology(&t).await;
    b.set_topology(&t).await;
    (a, b, orch, worker)
}

/// With no hub the task stays queued; once a hub links the hosts it is delivered and the
/// result comes back.
#[tokio::test]
async fn uplink_down_queues_then_delivers_on_reconnect() {
    let s = Scratch::new();
    let (a, b, orch, worker) = pair(&s, 1).await;
    let task = a.delegate1(&orch, &worker, "when you can").await;
    assert_eq!(task.state, TaskState::Queued);
    let w = a.wait(&[&task.id], 300).await;
    assert!(w.timed_out);
    assert_eq!(w.tasks[0].state, TaskState::Queued);
    let hub = TestHub::start(&[&a, &b]).await;
    assert_eq!(result_summary(&a.wait_done(&task.id).await), "delivered");
    // The hub goes away and comes back: a second task still gets through.
    hub.shutdown().await;
    let t2 = a.delegate1(&orch, &worker, "again").await;
    let hub = TestHub::start(&[&a, &b]).await;
    assert_eq!(result_summary(&a.wait_done(&t2.id).await), "delivered");
    hub.shutdown().await;
}

/// The sender's daemon is killed while the worker runs; the worker finishes, holds the result
/// until the sender is back, and `task_wait` on the restarted sender returns it.
#[tokio::test]
async fn sender_restart_mid_task_still_gets_the_result() {
    let s = Scratch::new();
    let (mut a, b, orch, worker) = pair(&s, 1500).await;
    let hub = TestHub::start(&[&a, &b]).await;
    let mut bev = b.tasks_topic();
    let task = a.delegate1(&orch, &worker, "slow one").await;
    next_task_event(&mut bev, "worker running", |e| {
        e.side == Side::Inbound && matches!(e.view.state, TaskState::Running { .. })
    })
    .await;
    a.kill().await;
    next_task_event(&mut bev, "worker completed", |e| {
        e.side == Side::Inbound && matches!(e.view.state, TaskState::Completed { .. })
    })
    .await;
    a.restart().await;
    assert_eq!(result_summary(&a.wait_done(&task.id).await), "delivered");
    hub.shutdown().await;
}

/// The receiver's daemon is killed mid-turn (the CLI keeps running in its shim); after the
/// restart the node recovers the task from its journal, finds the turn's end in the session's
/// replayed events and reports the result.
#[tokio::test]
async fn receiver_restart_mid_task_still_delivers_the_result() {
    let s = Scratch::new();
    let (a, mut b, orch, worker) = pair(&s, 2000).await;
    let hub = TestHub::start(&[&a, &b]).await;
    let mut aev = a.tasks_topic();
    let task = a.delegate1(&orch, &worker, "slow one").await;
    next_task_event(&mut aev, "running", |e| {
        e.view.task.id == task.id && matches!(e.view.state, TaskState::Running { .. })
    })
    .await;
    b.restart().await;
    assert_eq!(result_summary(&a.wait_done(&task.id).await), "delivered");
    // Exactly one turn ran for the task (no resubmission).
    let snap = b.client.snapshot(&sid(&worker)).await.unwrap();
    assert_eq!(snap.turns_completed, 1);
    hub.shutdown().await;
}
