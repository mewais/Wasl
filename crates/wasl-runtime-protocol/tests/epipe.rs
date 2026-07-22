//! Phase-2B item 8 — the M2 EPIPE lesson applied to the R2 IO shell.
//!
//! Exactly as `wasl-adapter-codex/tests/process_invoker.rs`'s
//! `stdin_broken_pipe_on_ignoring_child_is_ignored` proves for the per-turn
//! adapters: if the fake agent stops reading stdin (or exits) BEFORE the driver
//! finishes writing a turn, the resulting `BrokenPipe`/`WriteZero` on the stdin
//! write MUST be treated as BENIGN — swallowed, not propagated as a failure of
//! the whole call (`docs/specs/runtime-protocol.md` §1.4 supervision + the
//! adapter crates' M2 fix pattern).
//!
//! EXPECTED RED: FakeLineProtocol + the fake's `stop_reading_stdin` / crash
//! modes do not exist yet.

mod support;

use support::*;

use wasl_runtime::{ComposedPrompt, InteractiveProtocol, SpawnOptions};
use wasl_runtime_protocol::FakeLineProtocol;

fn driver() -> FakeLineProtocol {
    FakeLineProtocol::new(fake_agent_bin())
}

fn prompt(line: &str) -> ComposedPrompt {
    ComposedPrompt(line.to_string())
}

#[tokio::test]
async fn send_turn_after_child_stopped_reading_stdin_is_benign() {
    let proto = driver();
    let mut tx = proto
        .spawn(&SpawnOptions::default())
        .await
        .expect("spawn fake agent");
    let _ = next_or_hang(&mut *tx, "ready").await.expect("ready");

    // Tell the child to STOP reading its stdin (it keeps stdout open long enough
    // to acknowledge, then closes its read end). This turn itself is delivered.
    tx.send_turn(&prompt(&req_stop_reading_stdin()))
        .await
        .expect("the stop-reading request itself is delivered");
    // Drain whatever the child emits for that request (may be an accept/ack or a
    // completion — bounded, we don't assert its exact shape here).
    let _ = tokio::time::timeout(HANG_GUARD, tx.next_event()).await;

    // NOW write another turn. The child is no longer reading stdin, so the
    // driver's write will hit BrokenPipe/WriteZero. Per the M2 fix this MUST be
    // benign: `send_turn` returns Ok (the write error is swallowed), NOT a
    // `RuntimeError::Transport` about a broken pipe.
    let res = tx.send_turn(&prompt(&req_say("into-the-void"))).await;
    assert_write_benign(&res, "child stopped reading stdin (M2 EPIPE lesson)");
}

#[tokio::test]
async fn send_turn_after_child_exited_is_benign_not_a_broken_pipe_error() {
    // The stronger form: the child has fully EXITED (crash) before the write.
    // Writing to a dead child's stdin is EPIPE; it must be swallowed the same
    // way (the call does not blow up with a transport error).
    let proto = driver();
    let mut tx = proto
        .spawn(&SpawnOptions::default())
        .await
        .expect("spawn fake agent");
    let _ = next_or_hang(&mut *tx, "ready").await.expect("ready");

    // Crash the child and drain to its ProcessExited so we KNOW it is gone.
    tx.send_turn(&prompt(&req_crash(0)))
        .await
        .expect("crash req");
    let evs = drain_to_process_exit(&mut *tx, "crash").await;
    assert_eq!(count_process_exits(&evs), 1, "child exited: {evs:?}");

    // Write to the now-dead child. BrokenPipe on stdin must be benign (Ok), not a
    // propagated failure — the same swallow the adapter crates apply.
    let res = tx.send_turn(&prompt(&req_say("nobody-home"))).await;
    assert_write_benign(&res, "child already exited before the write (M2 EPIPE)");
}
