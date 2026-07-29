//! Hardening-2 §3.2 (end-to-end, REAL process) — `shutdown(Immediate)` called
//! through the generic actor / provider (`WaslManagedRuntimeProvider`) MUST
//! actually stop the underlying REAL child process, not merely flip the target to
//! a terminal status while the actor task parks forever holding the live child.
//!
//! This is the real-process companion to `wasl-runtime`'s scripted-double test
//! (`tests/shutdown_stops_process.rs`). Here the child is a genuine OS process
//! (`wasl-fake-agent` via `FakeLineProtocol`), driven through the Phase-2A
//! `WaslManagedRuntimeProvider` exactly as `hold_queueing.rs` does. Shutdown is
//! requested via the PUBLIC `DeliveryTarget::shutdown` seam — NOT by dropping the
//! transport, NOT by calling `interrupt()` directly — so it proves the ACTOR's
//! shutdown path stops the process.
//!
//! Bug being pinned (hardening-plan §3.2): `stop()` sets the actor terminal and
//! emits `Stopped` but never touches `self.transport`; the actor then parks in
//! `PumpNext::Wait` still holding the transport (and the live child) alive until
//! the last `ManagedTarget` handle drops — so "shutting down" a target today does
//! NOT stop its real process. `runtime.md` §7.3 Immediate row requires "kill
//! transport" — which, for a real driver, means the child is dead.
//!
//! EXPECTED RED against the current unfixed actor: the `target` Arc + `provider`
//! are kept ALIVE in scope while we assert the child is dead, so the child stays
//! alive (the transport is never released) and the bounded aliveness poll fails.

#![cfg(unix)]

mod support;

use std::sync::{Arc, Mutex};
use std::time::Duration;

use futures::StreamExt;

use support::*;

use wasl_adapter::{Delivery, DeliveryKind};
use wasl_proto::{AgentId, RequestId};
use wasl_runtime::{
    BusyPolicy, Causation, DeliveryTarget, EventSeq, IdempotencyKey, ManagedConfig, MonotonicClock,
    ShutdownMode, StreamItem, TargetKey, TargetStatus, TurnEvent, TurnRequest, TurnSource,
    WaslManagedRuntimeProvider,
};
use wasl_runtime_protocol::FakeLineProtocol;

struct NullSink;
impl wasl_audit::Sink for NullSink {
    fn append(&mut self, _e: &wasl_audit::AuditEntry) -> Result<(), wasl_audit::AuditError> {
        Ok(())
    }
}

fn make_req(text: &str, idem: &str) -> TurnRequest {
    TurnRequest {
        delivery: Delivery {
            context_id: "ctx-real-shutdown".to_string(),
            from: "wasl-b".to_string(),
            kind: DeliveryKind::Consult,
            text: text.to_string(),
            data: vec![],
        },
        source: TurnSource::Agent {
            from: AgentId("wasl-b".to_string()),
        },
        idempotency_key: Some(IdempotencyKey(idem.to_string())),
        causation: Causation {
            root_request: RequestId("root".to_string()),
            parent_request: None,
            path: vec![],
            hop: 0,
        },
    }
}

/// `kill -0` aliveness probe (no `unsafe`/`libc` in the test crate — the same
/// black-box pattern `supervision.rs` uses).
fn pid_alive(pid: u32) -> bool {
    std::process::Command::new("kill")
        .arg("-0")
        .arg(pid.to_string())
        .stdout(std::process::Stdio::null())
        .stderr(std::process::Stdio::null())
        .status()
        .map(|s| s.success())
        .unwrap_or(false)
}

async fn reaped_within(pid: u32, bound: Duration) -> bool {
    let deadline = tokio::time::Instant::now() + bound;
    while tokio::time::Instant::now() < deadline {
        if !pid_alive(pid) {
            return true;
        }
        tokio::time::sleep(Duration::from_millis(20)).await;
    }
    !pid_alive(pid)
}

/// Learn the REAL child's OS pid by running a `recall pid` turn through the
/// generic target and reading the `TurnCompleted` reply off the event stream.
async fn learn_child_pid(target: &Arc<dyn DeliveryTarget>) -> u32 {
    let mut stream = target.events(EventSeq(0)).await;
    // A real spawned process may still be `Starting` when this submit races its
    // `SessionReady` (runtime.md §7.1 row 582), so the receipt can legitimately be
    // `Started` OR `Queued` (promoted the instant ready arrives) — both carry a
    // `turn_id` we correlate on the stream. Use `Queue` so a `Starting` race
    // queues-then-promotes rather than being rejected.
    let receipt = target
        .submit_turn(
            make_req(&req_recall_pid(), "idem-pid"),
            BusyPolicy::Queue {
                max_wait: Duration::from_secs(5),
            },
        )
        .await;
    let turn_id = match receipt {
        wasl_runtime::TurnReceipt::Started { turn_id, .. }
        | wasl_runtime::TurnReceipt::Queued { turn_id, .. } => turn_id,
        other => panic!("recall-pid submit must Start or Queue, got {other:?}"),
    };
    // Drain until this turn's TurnCompleted, bounded so a hang fails fast.
    let reply = tokio::time::timeout(Duration::from_secs(5), async {
        loop {
            match stream.next().await {
                Some(StreamItem::Event {
                    event:
                        TurnEvent::TurnCompleted {
                            turn_id: t, reply, ..
                        },
                    ..
                }) if t == turn_id => return reply,
                Some(_) => {}
                None => panic!("event stream ended before the pid turn completed"),
            }
        }
    })
    .await
    .expect("the recall-pid turn must complete");
    reply.trim().parse::<u32>().expect("decimal pid reply")
}

// =====================================================================
// §3.2 e2e — shutdown(Immediate) through the provider/actor kills the REAL child.
// =====================================================================

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn immediate_shutdown_through_the_actor_kills_the_real_child() {
    let proto = FakeLineProtocol::new(fake_agent_bin());
    let sink: Arc<Mutex<dyn wasl_audit::Sink + Send>> = Arc::new(Mutex::new(NullSink));
    let provider = WaslManagedRuntimeProvider::with_seams(
        proto,
        ManagedConfig::default(),
        Arc::new(MonotonicClock),
        sink,
    );

    let target: Arc<dyn DeliveryTarget> = provider
        .target_for(&TargetKey("ctx-real-shutdown".to_string()))
        .await
        .expect("get-or-create the managed target backed by the real fake agent");

    // Learn the real child's pid (proves a real process was spawned).
    let child_pid = learn_child_pid(&target).await;
    assert!(
        pid_alive(child_pid),
        "the real fake-agent child is alive before shutdown: {child_pid}"
    );

    // Put the target into a genuinely-busy, never-completing state so the child
    // is live and held by the actor at shutdown time. (By now the session is
    // ready/idle after the pid turn, so this Starts; accept Queued too for
    // robustness, then confirm the target actually reaches Busy.)
    let held = target
        .submit_turn(
            make_req(&req_hold(), "idem-hold"),
            BusyPolicy::Queue {
                max_wait: Duration::from_secs(5),
            },
        )
        .await;
    assert!(
        matches!(
            held,
            wasl_runtime::TurnReceipt::Started { .. } | wasl_runtime::TurnReceipt::Queued { .. }
        ),
        "the hold turn must Start or Queue, got {held:?}"
    );
    tokio::time::timeout(Duration::from_secs(5), async {
        loop {
            if matches!(target.status().await, TargetStatus::Busy { .. }) {
                return;
            }
            tokio::task::yield_now().await;
        }
    })
    .await
    .expect("the hold turn must reach Busy (the child is genuinely held) before shutdown");

    // Shut down via the PUBLIC actor seam (NOT drop, NOT interrupt directly).
    target
        .shutdown(ShutdownMode::Immediate)
        .await
        .expect("immediate shutdown through the actor");

    assert_eq!(
        target.status().await,
        TargetStatus::Stopped,
        "Immediate shutdown ⇒ Stopped (§7.3)"
    );

    // THE §3.2 ASSERTION: the REAL child must be dead — and we assert this WHILE
    // `target` (a ManagedTarget Arc) and `provider` are still alive in scope, so
    // the only thing that could have killed it is the shutdown path itself. The
    // current actor parks holding the transport, so the child survives → RED.
    assert!(
        reaped_within(child_pid, Duration::from_secs(2)).await,
        "shutdown(Immediate) through the actor must actually STOP the real child \
         process (§7.3 kill transport / §3.2) — pid {child_pid} is still alive 2 s \
         after shutdown while the target/provider are still held, meaning the \
         actor went terminal but never released the transport"
    );

    // Keep the handles alive to the very end so the RED is unambiguous (the child
    // must have died from the shutdown, not from these dropping).
    drop(target);
    drop(provider);
}
