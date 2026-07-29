//! Hardening-2 §4.9 — escalation must signal the child's whole PROCESS GROUP,
//! not only the leader PID: a CLI that spawns its own subprocesses (we have
//! direct evidence the real `claude` CLI spawns subprocess MCP servers) must not
//! leave those subprocesses orphaned-and-alive when the session is force-killed.
//!
//! Bug being pinned (hardening-plan §4.9): `best_effort_signal` signals only the
//! single leader PID. The fix: spawn the child in its own process group/session
//! and signal the WHOLE group (negative PID) on escalation, while still reaping
//! only the direct child via `wait()`.
//!
//! This drives a REAL OS child via `FakeLineProtocol` + the `wasl-fake-agent`
//! fixture's new `spawn_child_process` command: the fake spawns a REAL grandchild
//! (`sleep 30`) in the fake's process group, reports the grandchild PID over
//! stdout, then never exits and ignores the gentle signals (forcing the
//! escalation ladder to SIGKILL). The test learns the grandchild PID, forces the
//! session down, and asserts the GRANDCHILD is ALSO dead afterward — not just the
//! direct child.
//!
//! EXPECTED RED against the current unfixed `fake_line.rs`: only the leader PID is
//! signalled, so the grandchild is orphaned and survives its full `sleep 30`,
//! well past the assertion bound.

#![cfg(unix)]

mod support;

use std::time::Duration;

use support::*;

use wasl_runtime::{ComposedPrompt, InteractiveProtocol, ProtocolEvent, SpawnOptions};
use wasl_runtime_protocol::{FakeLineProtocol, SupervisionTimeouts};

fn prompt(line: &str) -> ComposedPrompt {
    ComposedPrompt(line.to_string())
}

/// Short, injected supervision timeouts so the escalation ladder (the fake
/// ignores gentle signals, so it climbs to SIGKILL) resolves quickly.
fn fast_timeouts() -> SupervisionTimeouts {
    SupervisionTimeouts {
        interrupt_ack: Duration::from_millis(100),
        term_after: Duration::from_millis(100),
        kill_after: Duration::from_millis(100),
        hang_watchdog: Duration::from_millis(200),
    }
}

/// `kill -0` aliveness probe — the same black-box pattern `supervision.rs` uses,
/// no `unsafe`/`libc` in the test crate.
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

/// Poll `pid_alive` until it reports gone, bounded by `bound`.
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

async fn spawn_and_learn_pid(
    proto: &FakeLineProtocol,
) -> (Box<dyn wasl_runtime::InteractiveTransport>, u32) {
    let mut tx = proto
        .spawn(&SpawnOptions::default())
        .await
        .expect("spawn fake agent");
    let ready = next_or_hang(&mut *tx, "ready").await.expect("ready");
    assert!(is_session_ready(&ready), "ready: {ready:?}");
    tx.send_turn(&prompt(&req_recall_pid())).await.expect("pid");
    let evs = drain_to_terminal(&mut *tx, "pid turn").await;
    let pid = evs
        .iter()
        .find_map(completed_reply)
        .expect("pid reply")
        .trim()
        .parse::<u32>()
        .expect("decimal pid");
    (tx, pid)
}

/// Read events until an `AssistantDelta` carrying a decimal PID arrives (the
/// grandchild's PID, reported by the `spawn_child_process` command), bounded so a
/// misbehaving fixture fails fast rather than hanging.
async fn read_grandchild_pid(tx: &mut dyn wasl_runtime::InteractiveTransport) -> u32 {
    let deadline = tokio::time::Instant::now() + Duration::from_secs(3);
    while tokio::time::Instant::now() < deadline {
        match tokio::time::timeout(Duration::from_millis(300), tx.next_event()).await {
            Ok(Some(ProtocolEvent::AssistantDelta { text })) => {
                if let Ok(pid) = text.trim().parse::<u32>() {
                    return pid;
                }
            }
            Ok(Some(_)) => {} // accepted / other — keep reading
            Ok(None) => panic!("stream ended before the grandchild pid delta"),
            Err(_) => {} // no event this slice — keep waiting
        }
    }
    panic!("did not observe the grandchild pid delta within the bound");
}

// =====================================================================
// §4.9 — force-killing the session must kill the GRANDCHILD too (process-group
// signalling), not orphan it.
// =====================================================================

#[tokio::test]
async fn force_kill_also_kills_the_grandchild_via_process_group_signal() {
    let proto = FakeLineProtocol::with_timeouts(fake_agent_bin(), fast_timeouts());
    let (mut tx, leader_pid) = spawn_and_learn_pid(&proto).await;

    // The fake spawns a REAL grandchild (`sleep 30`) in its process group and
    // reports the grandchild PID; then it ignores gentle signals and never exits.
    tx.send_turn(&prompt(&req_spawn_child_process(30)))
        .await
        .expect("send_turn spawn_child_process");

    let grandchild_pid = read_grandchild_pid(&mut *tx).await;
    assert_ne!(
        grandchild_pid, leader_pid,
        "the grandchild is a distinct OS process from the fake leader"
    );
    assert!(
        pid_alive(grandchild_pid),
        "the grandchild (sleep 30) is alive after being spawned: {grandchild_pid}"
    );

    // Force the session down. Because the fake ignores SIGINT/SIGTERM, the
    // escalation ladder must climb to SIGKILL. The direct child (the fake) is
    // reaped either way; the QUESTION is whether the grandchild dies too.
    drop(tx);

    // The direct child must be reaped (sanity — this already works today).
    assert!(
        reaped_within(leader_pid, Duration::from_secs(3)).await,
        "the direct child (the fake leader) must be reaped on force-kill: \
         {leader_pid}"
    );

    // THE §4.9 ASSERTION: the grandchild must ALSO be dead. Signalling only the
    // leader PID (the current bug) orphans the grandchild, which then survives its
    // full `sleep 30`; signalling the whole process group (the fix) kills it too.
    assert!(
        reaped_within(grandchild_pid, Duration::from_secs(3)).await,
        "force-killing the session must kill the GRANDCHILD too (process-group \
         signalling, §4.9), not orphan it — the grandchild pid {grandchild_pid} \
         is still alive, so only the leader was signalled"
    );
}
