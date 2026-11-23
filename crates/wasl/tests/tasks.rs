//! Task engine behaviour across daemons: parallel fan-out, FIFO per receiver, cancel,
//! cycles, depth, deadlines, budget, standalone approval.

mod common;

use std::collections::HashSet;

use common::*;
use serde_json::json;
use wasit_fake_cli::scenario::{Scenario, Step, Turn, UsageStep};
use wasl_proto::*;

fn waiting_worker() -> Scenario {
    let mut b = Scenario::builder();
    for _ in 0..3 {
        b = b.turn(Turn::builder().say("thinking hard").wait_for_interrupt());
    }
    b.build()
}

/// Fan-out to three workers on two other hosts runs in parallel: every task is running before
/// any completes.
#[tokio::test]
async fn fan_out_to_three_workers_runs_in_parallel() {
    let s = Scratch::new();
    let a = Daemon::start(&s, "a").await;
    let b = Daemon::start(&s, "b").await;
    let c = Daemon::start(&s, "c").await;
    let hub = TestHub::start(&[&a, &b, &c]).await;
    let orch = a.session(&Scenario::builder().build()).await;
    let w1 = b.session(&slow_worker("one", 1500)).await;
    let w2 = b.session(&slow_worker("two", 1500)).await;
    let w3 = c.session(&slow_worker("three", 1500)).await;
    let t = topology(
        1,
        &[(&orch, "o"), (&w1, "w1"), (&w2, "w2"), (&w3, "w3")],
        &[
            (&orch, &w1, EdgePolicy::AlwaysAllow),
            (&orch, &w2, EdgePolicy::AlwaysAllow),
            (&orch, &w3, EdgePolicy::AlwaysAllow),
        ],
        Limits::default(),
    );
    for d in [&a, &b, &c] {
        d.set_topology(&t).await;
    }
    let mut events = a.tasks_topic();
    let r = a
        .delegate(&orch, &[&w1, &w2, &w3], TaskMode::Command, "go", None)
        .await;
    let ids: Vec<TaskId> = r
        .results
        .iter()
        .map(|o| o.task.as_ref().unwrap().id.clone())
        .collect();
    let mut running = HashSet::new();
    let mut completed_before_all_running = false;
    within("all running", async {
        while running.len() < 3 {
            let (_, v) = events.next().await.unwrap().unwrap();
            let e: TaskEvent = serde_json::from_value(v).unwrap();
            if e.side != Side::Outbound || !ids.contains(&e.view.task.id) {
                continue;
            }
            match e.view.state {
                TaskState::Running { .. } => {
                    running.insert(e.view.task.id.clone());
                }
                TaskState::Completed { .. } => completed_before_all_running = true,
                _ => {}
            }
        }
    })
    .await;
    assert!(
        !completed_before_all_running,
        "a task completed before all three were running: delivery is serial"
    );
    let refs: Vec<&TaskId> = ids.iter().collect();
    let done = a.wait(&refs, 55_000).await;
    assert!(!done.timed_out);
    let mut answers: Vec<String> = done
        .tasks
        .iter()
        .map(|t| result_summary(&t.state))
        .collect();
    answers.sort();
    assert_eq!(answers, ["one", "three", "two"]);
    hub.shutdown().await;
}

/// A busy worker takes tasks in FIFO order; a queued task can be cancelled (dequeued) and a
/// running one is interrupted.
#[tokio::test]
async fn busy_worker_queues_fifo_and_cancel_running_and_queued() {
    let s = Scratch::new();
    let a = Daemon::start(&s, "a").await;
    let b = Daemon::start(&s, "b").await;
    let hub = TestHub::start(&[&a, &b]).await;
    let orch = a.session(&Scenario::builder().build()).await;
    let worker = b
        .session(
            &Scenario::builder()
                .turn(Turn::when("first").say("busy").sleep_ms(1500).say("r1"))
                .turn(Turn::when("second").say("r2"))
                .turn(Turn::when("fourth").say("hold").wait_for_interrupt())
                .build(),
        )
        .await;
    let t = topology(
        1,
        &[(&orch, "o"), (&worker, "w")],
        &[(&orch, &worker, EdgePolicy::AlwaysAllow)],
        Limits::default(),
    );
    a.set_topology(&t).await;
    b.set_topology(&t).await;
    let mut events = a.tasks_topic();
    // FIFO is judged on the receiver's own task events (reports to the sender travel
    // concurrently and may be reordered).
    let mut worker_events = b.tasks_topic();
    let t1 = a.delegate1(&orch, &worker, "first").await;
    next_task_event(&mut events, "t1 running", |e| {
        e.view.task.id == t1.id && matches!(e.view.state, TaskState::Running { .. })
    })
    .await;
    let t2 = a.delegate1(&orch, &worker, "second").await;
    let t3 = a.delegate1(&orch, &worker, "third").await;
    // t3 is queued behind t1 and t2 on the worker: cancel it.
    let c: TaskBrief = a
        .call(
            methods::TASK_CANCEL,
            json!({"id": t3.id, "reason": "changed my mind"}),
        )
        .await;
    assert!(matches!(c.state, TaskState::Cancelled { .. }));
    // FIFO: t2 starts only after t1 completed.
    let mut order = Vec::new();
    within("t2 completes", async {
        loop {
            let (_, v) = worker_events.next().await.unwrap().unwrap();
            let e: TaskEvent = serde_json::from_value(v).unwrap();
            if e.side != Side::Inbound {
                continue;
            }
            let tag = (e.view.task.id.clone(), e.view.state.name());
            if !order.contains(&tag) {
                order.push(tag);
            }
            if e.view.task.id == t2.id && e.view.state.is_terminal() {
                return;
            }
        }
    })
    .await;
    let pos = |id: &TaskId, st: &str| order.iter().position(|(i, s)| i == id && *s == st);
    let t1_done = pos(&t1.id, "completed").expect("t1 completed");
    let t2_running = pos(&t2.id, "running").expect("t2 ran");
    assert!(t1_done < t2_running, "not FIFO: {order:?}");
    assert_eq!(result_summary(&a.wait_done(&t2.id).await), "r2");
    // The worker never ran t3.
    let inbound: TaskList = b.call(methods::TASKS_LIST, json!(null)).await;
    let w3 = inbound.inbound.iter().find(|v| v.task.id == t3.id);
    if let Some(v) = w3 {
        assert!(matches!(v.state, TaskState::Cancelled { .. }), "{v:?}");
    }

    // Cancel a running task: the worker's turn is interrupted.
    let t4 = a.delegate1(&orch, &worker, "fourth").await;
    next_task_event(&mut events, "t4 running", |e| {
        e.view.task.id == t4.id && matches!(e.view.state, TaskState::Running { .. })
    })
    .await;
    let _: TaskBrief = a.call(methods::TASK_CANCEL, json!({"id": t4.id})).await;
    assert!(matches!(
        a.status(&t4.id).await,
        TaskState::Cancelled { .. }
    ));
    b.wait_snapshot(&sid(&worker), "worker interrupted and idle", |s| {
        s.phase.name() == "idle" && s.last_turn.as_ref().is_some_and(|t| t.was_interrupted)
    })
    .await;
    hub.shutdown().await;
}

/// A → B → A is rejected with an explicit error; so is a delegation beyond the depth limit.
#[tokio::test]
async fn cycles_and_depth_are_rejected() {
    let s = Scratch::new();
    let a = Daemon::start(&s, "a").await;
    let b = Daemon::start(&s, "b").await;
    let hub = TestHub::start(&[&a, &b]).await;
    let orch = a.session(&Scenario::builder().build()).await;
    let mid = b.session(&waiting_worker()).await;
    let leaf = b.session(&slow_worker("leaf", 1)).await;
    let t = topology(
        1,
        &[(&orch, "orch"), (&mid, "mid"), (&leaf, "leaf")],
        &[
            (&orch, &mid, EdgePolicy::AlwaysAllow),
            (&mid, &orch, EdgePolicy::AlwaysAllow),
            (&mid, &leaf, EdgePolicy::AlwaysAllow),
        ],
        Limits {
            max_depth: 1,
            ..Limits::default()
        },
    );
    a.set_topology(&t).await;
    b.set_topology(&t).await;
    let mut events = a.tasks_topic();
    let t1 = a.delegate1(&orch, &mid, "think").await;
    next_task_event(&mut events, "running", |e| {
        e.view.task.id == t1.id && matches!(e.view.state, TaskState::Running { .. })
    })
    .await;
    // mid (working on t1 from orch) tries to delegate back to orch.
    let r = b
        .delegate(&mid, &[&orch], TaskMode::Consult, "?", None)
        .await;
    let e = r.results[0].error.as_ref().expect("cycle rejected");
    assert_eq!(e.code, codes::CYCLE);
    assert!(
        e.message.contains("cycle") && e.message.contains("orch"),
        "{}",
        e.message
    );
    // mid → leaf would be hop 2 > max_depth 1.
    let r = b
        .delegate(&mid, &[&leaf], TaskMode::Consult, "?", None)
        .await;
    let e = r.results[0].error.as_ref().expect("depth rejected");
    assert_eq!(e.code, codes::MAX_DEPTH);
    let _: TaskBrief = a.call(methods::TASK_CANCEL, json!({"id": t1.id})).await;
    hub.shutdown().await;
}

/// A task past its deadline is Expired at the sender and its turn is interrupted.
#[tokio::test]
async fn deadline_expires_task_and_interrupts_the_worker() {
    let s = Scratch::new();
    let a = Daemon::start(&s, "a").await;
    let b = Daemon::start(&s, "b").await;
    let hub = TestHub::start(&[&a, &b]).await;
    let orch = a.session(&Scenario::builder().build()).await;
    let worker = b.session(&waiting_worker()).await;
    let t = topology(
        1,
        &[(&orch, "o"), (&worker, "w")],
        &[(&orch, &worker, EdgePolicy::AlwaysAllow)],
        Limits::default(),
    );
    a.set_topology(&t).await;
    b.set_topology(&t).await;
    let r = a
        .delegate(&orch, &[&worker], TaskMode::Command, "forever", Some(2))
        .await;
    let id = r.results[0].task.as_ref().unwrap().id.clone();
    assert_eq!(a.wait_done(&id).await, TaskState::Expired);
    b.wait_snapshot(&sid(&worker), "worker interrupted", |s| {
        s.phase.name() == "idle" && s.last_turn.as_ref().is_some_and(|t| t.was_interrupted)
    })
    .await;
    hub.shutdown().await;
}

/// A task delegated by an agent working on a task inherits that task's deadline: asking for
/// more time (or the workstream default) never outlives the parent, and the delegated task
/// expires with it.
#[tokio::test]
async fn delegated_task_inherits_the_parent_deadline() {
    let s = Scratch::new();
    let a = Daemon::start(&s, "a").await;
    let b = Daemon::start(&s, "b").await;
    let hub = TestHub::start(&[&a, &b]).await;
    let orch = a.session(&Scenario::builder().build()).await;
    let mid = b.session(&waiting_worker()).await;
    let leaf = b.session(&waiting_worker()).await;
    let t = topology(
        1,
        &[(&orch, "orch"), (&mid, "mid"), (&leaf, "leaf")],
        &[
            (&orch, &mid, EdgePolicy::AlwaysAllow),
            (&mid, &leaf, EdgePolicy::AlwaysAllow),
        ],
        Limits::default(),
    );
    a.set_topology(&t).await;
    b.set_topology(&t).await;
    let mut a_events = a.tasks_topic();
    let mut b_events = b.tasks_topic();
    let started = std::time::Instant::now();
    let r = a
        .delegate(&orch, &[&mid], TaskMode::Command, "think", Some(4))
        .await;
    let t1 = r.results[0].task.as_ref().unwrap().id.clone();
    let parent = next_task_event(&mut a_events, "parent running", |e| {
        e.view.task.id == t1 && matches!(e.view.state, TaskState::Running { .. })
    })
    .await
    .view
    .task;
    // mid asks for ten minutes, then for the workstream default: both capped by t1.
    let mut kids = Vec::new();
    for want in [Some(600), None] {
        let r = b
            .delegate(&mid, &[&leaf], TaskMode::Command, "dig", want)
            .await;
        let id = r.results[0].task.as_ref().expect("delegated").id.clone();
        let child = next_task_event(&mut b_events, "child created", |e| e.view.task.id == id)
            .await
            .view
            .task;
        assert!(
            child.deadline_ms <= parent.deadline_ms,
            "asked {want:?}: child {} > parent {}",
            child.deadline_ms,
            parent.deadline_ms
        );
        assert_eq!(child.parent.as_ref(), Some(&t1));
        kids.push(id);
    }
    // The first child expires with its parent, not ten minutes later.
    assert_eq!(b.wait_done(&kids[0]).await, TaskState::Expired);
    assert!(
        started.elapsed() < std::time::Duration::from_secs(30),
        "{:?}",
        started.elapsed()
    );
    assert_eq!(a.wait_done(&t1).await, TaskState::Expired);
    hub.shutdown().await;
}

/// Once the workstream's reported cost reaches its budget, further delegations are rejected.
#[tokio::test]
async fn budget_limit_rejects_further_delegations() {
    let s = Scratch::new();
    let a = Daemon::start(&s, "a").await;
    let b = Daemon::start(&s, "b").await;
    let hub = TestHub::start(&[&a, &b]).await;
    let orch = a.session(&Scenario::builder().build()).await;
    let costly = Turn::builder().say("pricey").step(Step::Usage(UsageStep {
        cost_usd: 0.5,
        ..UsageStep::default()
    }));
    let worker = b
        .session(
            &Scenario::builder()
                .turn(costly.clone())
                .turn(costly)
                .build(),
        )
        .await;
    let t = topology(
        1,
        &[(&orch, "o"), (&worker, "w")],
        &[(&orch, &worker, EdgePolicy::AlwaysAllow)],
        Limits {
            budget_usd: Some(0.4),
            ..Limits::default()
        },
    );
    a.set_topology(&t).await;
    b.set_topology(&t).await;
    let t1 = a.delegate1(&orch, &worker, "spend").await;
    match a.wait_done(&t1.id).await {
        TaskState::Completed { result } => {
            let cost = result.usage.expect("usage reported").cost_usd;
            assert!((cost - 0.5).abs() < 1e-9, "cost {cost}");
        }
        other => panic!("{other:?}"),
    }
    let r = a
        .delegate(&orch, &[&worker], TaskMode::Command, "again", None)
        .await;
    assert_eq!(
        r.results[0].error.as_ref().expect("budget").code,
        codes::BUDGET
    );
    hub.shutdown().await;
}

/// Without a supervisor attached, an `ask` edge waits for the MCP front-end's decision
/// (elicitation), and delivery on the same host is a direct call.
#[tokio::test]
async fn standalone_same_host_waits_for_elicitation_decision() {
    let s = Scratch::new();
    let a = Daemon::start(&s, "a").await;
    let orch = a.session(&Scenario::builder().build()).await;
    let worker = a.session(&slow_worker("local", 1)).await;
    let t = topology(
        1,
        &[(&orch, "o"), (&worker, "w")],
        &[(&orch, &worker, EdgePolicy::Ask)],
        Limits::default(),
    );
    a.set_topology(&t).await;
    let t1 = a.delegate1(&orch, &worker, "hi").await;
    assert_eq!(
        t1.state,
        TaskState::PendingApproval {
            via: ApprovalVia::Elicitation
        }
    );
    let d: TaskBrief = a
        .call(
            methods::TASK_DECIDE,
            json!(DecideRequest {
                id: t1.id.clone(),
                decision: HandoffDecision::AllowOnce
            }),
        )
        .await;
    assert_eq!(d.state, TaskState::Queued);
    assert_eq!(result_summary(&a.wait_done(&t1.id).await), "local");
}

/// A paused workstream rejects new delegations (`paused`) until a newer topology resumes it.
#[tokio::test]
async fn paused_workstream_rejects_new_delegations_until_resumed() {
    let s = Scratch::new();
    let a = Daemon::start(&s, "a").await;
    let orch = a.session(&Scenario::builder().build()).await;
    let w = a.session(&slow_worker("resumed", 1)).await;
    let mut t = topology(
        1,
        &[(&orch, "o"), (&w, "w")],
        &[(&orch, &w, EdgePolicy::AlwaysAllow)],
        Limits::default(),
    );
    t.paused = true;
    a.set_topology(&t).await;
    let r = a
        .delegate(&orch, &[&w], TaskMode::Command, "go", None)
        .await;
    let e = r.results[0].error.as_ref().expect("rejected while paused");
    assert_eq!(e.code, codes::PAUSED);
    assert!(r.results[0].task.is_none());

    t.version = 2;
    t.paused = false;
    a.set_topology(&t).await;
    let task = a.delegate1(&orch, &w, "go").await;
    assert_eq!(result_summary(&a.wait_done(&task.id).await), "resumed");
}

/// A receiver that hands the work to background subagents and ends its turn ("started them"):
/// the task is not done at that turn. Its result is the turn the agent starts by itself when the
/// work ended (the turn's completion alone, the first one after the prompt, was the old answer).
#[tokio::test]
async fn a_receiver_working_in_the_background_answers_with_the_turn_that_follows_its_work() {
    use wasit_fake_cli::scenario::BackgroundStep;
    let s = Scratch::new();
    let a = Daemon::start(&s, "a").await;
    let b = Daemon::start(&s, "b").await;
    let hub = TestHub::start(&[&a, &b]).await;
    let orch = a.session(&Scenario::builder().build()).await;
    let worker = b
        .session(
            &Scenario::builder()
                .turn(
                    Turn::builder()
                        .background(BackgroundStep {
                            kind: "local_agent".into(),
                            description: "research".into(),
                            after_ms: 500,
                            then: vec![Step::Say {
                                text: "THE FINAL ANSWER".into(),
                                chunks: None,
                            }],
                            ..BackgroundStep::default()
                        })
                        .say("I started the research agent"),
                )
                .build(),
        )
        .await;
    let t = topology(
        1,
        &[(&orch, "o"), (&worker, "w")],
        &[(&orch, &worker, EdgePolicy::AlwaysAllow)],
        Limits::default(),
    );
    a.set_topology(&t).await;
    b.set_topology(&t).await;
    let task = a.delegate1(&orch, &worker, "research it").await;
    assert_eq!(
        result_summary(&a.wait_done(&task.id).await),
        "THE FINAL ANSWER"
    );
    b.wait_snapshot(&sid(&worker), "idle after the agent's own turn", |s| {
        s.phase.name() == "idle" && s.turns_completed == 2
    })
    .await;
    hub.shutdown().await;
}

/// Cancelling a task whose receiver only waits for background work stops that work (the receiver's
/// turn ended long ago: there is no turn to interrupt) and ends the task cancelled.
#[tokio::test]
async fn cancelling_a_task_whose_receiver_waits_for_background_work_stops_the_work() {
    use wasit_fake_cli::scenario::BackgroundStep;
    let s = Scratch::new();
    let a = Daemon::start(&s, "a").await;
    let b = Daemon::start(&s, "b").await;
    let hub = TestHub::start(&[&a, &b]).await;
    let orch = a.session(&Scenario::builder().build()).await;
    let worker = b
        .session(
            &Scenario::builder()
                .turn(
                    Turn::builder()
                        .background(BackgroundStep {
                            kind: "local_bash".into(),
                            description: "long job".into(),
                            command: Some("sleep 600".into()),
                            after_ms: 600_000,
                            ..BackgroundStep::default()
                        })
                        .say("started the job"),
                )
                .build(),
        )
        .await;
    let t = topology(
        1,
        &[(&orch, "o"), (&worker, "w")],
        &[(&orch, &worker, EdgePolicy::AlwaysAllow)],
        Limits::default(),
    );
    a.set_topology(&t).await;
    b.set_topology(&t).await;
    let task = a.delegate1(&orch, &worker, "run the job").await;
    // The receiver's turn ended; the job runs: the session is working, the task is not done.
    b.wait_snapshot(&sid(&worker), "background work with no turn", |s| {
        s.turns_completed == 1 && s.turn.is_none() && s.background_running == 1
    })
    .await;
    assert!(matches!(
        a.status(&task.id).await,
        TaskState::Running { .. } | TaskState::Queued
    ));
    let _: TaskBrief = a.call(methods::TASK_CANCEL, json!({"id": task.id})).await;
    assert!(matches!(
        a.status(&task.id).await,
        TaskState::Cancelled { .. }
    ));
    b.wait_snapshot(&sid(&worker), "the job stopped", |s| {
        s.phase.name() == "idle" && s.background_running == 0
    })
    .await;
    hub.shutdown().await;
}
