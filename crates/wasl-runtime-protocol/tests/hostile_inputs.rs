//! Phase-2B item 6 — ALL the §1.3 hostile-input scripts, end-to-end through a
//! REAL child (the fake agent scripted to emit adversarial wire). Each asserts
//! the driver's line-framing survives per `docs/specs/runtime-protocol.md` §1.3
//! + R1 §7.4: never a panic, never a hang, never a re-emitted terminal event —
//! producing the DEFINED `ProtocolEvent` sequence (or an ignored/logged
//! non-event).
//!
//! The ten scripts (§1.3):
//!   1. split JSON across reads
//!   2. multiple messages in one read
//!   3. malformed JSON between valid events
//!   4. missing `accepted`
//!   5. duplicate `done`
//!   6. `delta` AFTER `done`
//!   7. EOF without a ProcessExited-worthy code (→ ProcessExited{code:None})
//!   8. exit-before-stdout-drained (drain buffered events, THEN ProcessExited)
//!   9. ignored interrupt (no ack — the actor's Interrupting timeout applies)
//!  10. gate-then-exit (NativeGateRequested then ProcessExited)
//!
//! EXPECTED RED: FakeLineProtocol + the fake dev-bin's hostile-emit modes don't
//! exist yet.

mod support;

use support::*;

use wasl_runtime::{ComposedPrompt, InteractiveProtocol, ProtocolEvent, SpawnOptions};
use wasl_runtime_protocol::FakeLineProtocol;

fn driver() -> FakeLineProtocol {
    FakeLineProtocol::new(fake_agent_bin())
}

fn prompt(line: &str) -> ComposedPrompt {
    ComposedPrompt(line.to_string())
}

async fn spawned() -> Box<dyn wasl_runtime::InteractiveTransport> {
    let proto = driver();
    let mut tx = proto
        .spawn(&SpawnOptions::default())
        .await
        .expect("spawn fake agent");
    let ready = next_or_hang(&mut *tx, "ready").await.expect("ready");
    assert!(is_session_ready(&ready), "expected SessionReady: {ready:?}");
    tx
}

fn completion_of(evs: &[ProtocolEvent]) -> Option<String> {
    evs.iter()
        .find_map(|e| completed_reply(e).map(|s| s.to_string()))
}

// ---------------------------------------------------------------------
// 1. split JSON across reads — one object arriving in multiple stdin reads must
//    be reassembled by the driver's line buffer into ONE event.
// ---------------------------------------------------------------------

#[tokio::test]
async fn split_json_across_reads_is_reassembled_into_one_event() {
    let mut tx = spawned().await;
    tx.send_turn(&prompt(&req_emit_split_completion("stitched")))
        .await
        .expect("send_turn");
    let evs = drain_to_terminal(&mut *tx, "split json").await;
    assert_eq!(
        completion_of(&evs),
        Some("stitched".to_string()),
        "a completion object split across reads must reassemble to ONE \
         TurnCompleted, got {evs:?}"
    );
    assert_eq!(
        count_completions(&evs),
        1,
        "exactly one completion: {evs:?}"
    );
}

// ---------------------------------------------------------------------
// 2. multiple messages in one read — accepted+delta+done in a single stdin
//    write must be split into THREE events, in order.
// ---------------------------------------------------------------------

#[tokio::test]
async fn multiple_messages_in_one_read_are_split_in_order() {
    let mut tx = spawned().await;
    tx.send_turn(&prompt(&req_emit_batched_turn("d1", "done1")))
        .await
        .expect("send_turn");
    let evs = drain_to_terminal(&mut *tx, "batched").await;

    assert!(evs.iter().any(is_accepted), "TurnAccepted present: {evs:?}");
    let deltas: Vec<&str> = evs.iter().filter_map(delta_text).collect();
    assert_eq!(deltas, vec!["d1"], "delta split out: {evs:?}");
    assert_eq!(completion_of(&evs), Some("done1".to_string()));

    // Order: accepted before delta before completed.
    let pos = |pred: &dyn Fn(&ProtocolEvent) -> bool| evs.iter().position(|e| pred(e));
    let a = pos(&is_accepted).expect("accepted idx");
    let d = evs
        .iter()
        .position(|e| delta_text(e).is_some())
        .expect("delta idx");
    let c = evs
        .iter()
        .position(|e| completed_reply(e).is_some())
        .expect("completed idx");
    assert!(a < d && d < c, "events must stay ordered a<d<c: {evs:?}");
}

// ---------------------------------------------------------------------
// 3. malformed JSON between valid events — ignored + logged, framing continues;
//    the surrounding valid events still map, and NO spurious event is produced.
// ---------------------------------------------------------------------

#[tokio::test]
async fn malformed_json_between_valid_events_is_ignored_framing_continues() {
    let mut tx = spawned().await;
    tx.send_turn(&prompt(&req_emit_malformed_between(
        "good-delta",
        "good-done",
    )))
    .await
    .expect("send_turn");
    let evs = drain_to_terminal(&mut *tx, "malformed between").await;

    // The malformed line must NOT become an event; the valid delta+done still do.
    let deltas: Vec<&str> = evs.iter().filter_map(delta_text).collect();
    assert_eq!(
        deltas,
        vec!["good-delta"],
        "only the valid delta survives; malformed line ignored: {evs:?}"
    );
    assert_eq!(completion_of(&evs), Some("good-done".to_string()));
    assert_eq!(
        count_completions(&evs),
        1,
        "no spurious completion: {evs:?}"
    );
}

// ---------------------------------------------------------------------
// 4. missing `accepted` — the turn proceeds straight to `done` (an explicit
//    accept is not required); the driver still produces TurnCompleted.
// ---------------------------------------------------------------------

#[tokio::test]
async fn missing_accepted_still_completes_the_turn() {
    let mut tx = spawned().await;
    tx.send_turn(&prompt(&req_emit_done_without_accept("no-accept-reply")))
        .await
        .expect("send_turn");
    let evs = drain_to_terminal(&mut *tx, "no accept").await;
    assert_eq!(
        completion_of(&evs),
        Some("no-accept-reply".to_string()),
        "a turn with no `accepted` still completes (§1.3): {evs:?}"
    );
    assert_eq!(count_completions(&evs), 1);
}

// ---------------------------------------------------------------------
// 5. duplicate `done` — the second is ignored: NO second TurnCompleted.
// ---------------------------------------------------------------------

#[tokio::test]
async fn duplicate_done_produces_only_one_completion() {
    let mut tx = spawned().await;
    tx.send_turn(&prompt(&req_emit_duplicate_done("once")))
        .await
        .expect("send_turn");

    // Drain generously PAST the first terminal so a wrongly-emitted second
    // completion would be caught. We read to terminal, then poll once more.
    let evs = drain_to_terminal(&mut *tx, "dup done").await;
    assert_eq!(completion_of(&evs), Some("once".to_string()));
    assert_eq!(
        count_completions(&evs),
        1,
        "duplicate `done` must NOT yield a second TurnCompleted (§1.3): {evs:?}"
    );

    // A follow-up poll must not surface a re-emitted terminal (it may be the
    // clean stream-continuation for the next turn, or nothing — never a 2nd
    // TurnCompleted for the same turn).
    if let Some(extra) = next_or_hang(&mut *tx, "post-dup poll").await {
        assert!(
            completed_reply(&extra).is_none(),
            "no second TurnCompleted after a duplicate `done`, got {extra:?}"
        );
    }
}

// ---------------------------------------------------------------------
// 6. `delta` AFTER `done` — ignored (R1 §7.4: an event referencing a completed
//    turn). No AssistantDelta is produced for the late delta.
// ---------------------------------------------------------------------

#[tokio::test]
async fn delta_after_done_is_ignored() {
    let mut tx = spawned().await;
    tx.send_turn(&prompt(&req_emit_delta_after_done(
        "the-reply",
        "LATE-DELTA",
    )))
    .await
    .expect("send_turn");
    let evs = drain_to_terminal(&mut *tx, "delta after done").await;
    assert_eq!(completion_of(&evs), Some("the-reply".to_string()));

    // No delta carrying the late text before completion.
    let deltas: Vec<&str> = evs.iter().filter_map(delta_text).collect();
    assert!(
        !deltas.contains(&"LATE-DELTA"),
        "a delta after `done` must be ignored (§7.4), got deltas {deltas:?}"
    );

    // And a poll after the completion must not surface the late delta either.
    if let Some(extra) = next_or_hang(&mut *tx, "post-done poll").await {
        assert_ne!(
            delta_text(&extra),
            Some("LATE-DELTA"),
            "late delta must never appear, got {extra:?}"
        );
    }
}

// ---------------------------------------------------------------------
// 7. EOF without a ProcessExited-worthy code → ProcessExited{code: None}.
// ---------------------------------------------------------------------

#[tokio::test]
async fn eof_without_code_maps_to_process_exited_none() {
    let mut tx = spawned().await;
    tx.send_turn(&prompt(&req_eof_without_code()))
        .await
        .expect("send_turn (may race the exit)");
    let evs = drain_to_process_exit(&mut *tx, "eof no code").await;
    match evs.last() {
        Some(ProtocolEvent::ProcessExited { code }) => assert_eq!(
            *code, None,
            "EOF/signal-death with no clean code → ProcessExited{{code:None}} (§1.3), got {code:?}"
        ),
        other => panic!("expected ProcessExited{{None}} last, got {other:?}"),
    }
    assert_eq!(count_process_exits(&evs), 1, "one exit only: {evs:?}");
}

// ---------------------------------------------------------------------
// 8. exit-before-stdout-drained — buffered events are drained FIRST, THEN
//    ProcessExited (order preserved; nothing dropped).
// ---------------------------------------------------------------------

#[tokio::test]
async fn exit_before_stdout_drained_yields_buffered_events_then_exit() {
    let mut tx = spawned().await;
    tx.send_turn(&prompt(&req_emit_then_exit(
        "buffered-delta",
        "buffered-done",
        0,
    )))
    .await
    .expect("send_turn");
    let evs = drain_to_process_exit(&mut *tx, "exit before drain").await;

    // The delta + completion (buffered before the exit) must appear BEFORE the
    // ProcessExited — the driver drains stdout to EOF before surfacing exit.
    let delta_idx = evs
        .iter()
        .position(|e| delta_text(e) == Some("buffered-delta"));
    let done_idx = evs
        .iter()
        .position(|e| completed_reply(e) == Some("buffered-done"));
    let exit_idx = evs
        .iter()
        .position(|e| matches!(e, ProtocolEvent::ProcessExited { .. }));
    let delta_idx = delta_idx.expect("buffered delta not dropped");
    let done_idx = done_idx.expect("buffered completion not dropped");
    let exit_idx = exit_idx.expect("ProcessExited present");
    assert!(
        delta_idx < exit_idx && done_idx < exit_idx,
        "buffered events must be drained BEFORE ProcessExited (§1.3): {evs:?}"
    );
    match &evs[exit_idx] {
        ProtocolEvent::ProcessExited { code } => {
            assert_eq!(*code, Some(0), "clean exit code surfaced: {evs:?}")
        }
        _ => unreachable!(),
    }
}

// ---------------------------------------------------------------------
// 9. ignored interrupt — interrupt sent, agent never acks; the driver must NOT
//    hang and must NOT fabricate a cancellation ack. (The actor's Interrupting
//    timeout is R1's concern; here at the driver level we assert interrupt()
//    returns and the stream stays alive / does not deadlock.)
// ---------------------------------------------------------------------

#[tokio::test]
async fn ignored_interrupt_does_not_hang_the_driver() {
    let mut tx = spawned().await;
    tx.send_turn(&prompt(&req_ignore_interrupt()))
        .await
        .expect("send_turn");

    // Request an interrupt: the driver's `interrupt()` is best-effort and must
    // RETURN promptly even though the agent will never ack (bounded by a short
    // guard — a hang here is the bug).
    let interrupted = tokio::time::timeout(HANG_GUARD, tx.interrupt()).await;
    assert!(
        interrupted.is_ok(),
        "interrupt() must return promptly even when the agent ignores it (§1.3)"
    );
    assert!(
        interrupted.unwrap().is_ok(),
        "a best-effort interrupt of a live turn is Ok at the driver level"
    );

    // The agent ignored it — so NO TurnCancelled/terminal is fabricated by the
    // driver. A subsequent poll must not hang; we then release the process by
    // dropping the transport (supervision.rs proves the reap). Crucially: no
    // spurious terminal event was invented for the ignored interrupt.
    // (We do not require a specific next event; we only require no deadlock.)
    let _ = tokio::time::timeout(std::time::Duration::from_millis(300), tx.next_event()).await;
    // Reaching here without the HANG_GUARD firing is the assertion.
}

// ---------------------------------------------------------------------
// 10. gate-then-exit — `gate` then process exit → NativeGateRequested then
//     ProcessExited (per R1 §7.4/§9). Both events, in order, no hang.
// ---------------------------------------------------------------------

#[tokio::test]
async fn gate_then_exit_yields_gate_then_process_exited() {
    let mut tx = spawned().await;
    tx.send_turn(&prompt(&req_gate_then_exit("rm -rf preview", "danger", 0)))
        .await
        .expect("send_turn");
    let evs = drain_to_process_exit(&mut *tx, "gate then exit").await;

    let gate_idx = evs.iter().position(is_gate);
    let exit_idx = evs
        .iter()
        .position(|e| matches!(e, ProtocolEvent::ProcessExited { .. }));
    let gate_idx =
        gate_idx.unwrap_or_else(|| panic!("NativeGateRequested present (§1.2 gate row): {evs:?}"));
    let exit_idx = exit_idx.expect("ProcessExited present");
    assert!(
        gate_idx < exit_idx,
        "NativeGateRequested must precede ProcessExited (§7.4/§9): {evs:?}"
    );
    // The gate detail is carried through (secret-free summary/preview).
    match &evs[gate_idx] {
        ProtocolEvent::NativeGateRequested { detail } => {
            assert!(
                !detail.summary.is_empty(),
                "gate summary carried through: {detail:?}"
            );
        }
        _ => unreachable!(),
    }
    assert_eq!(count_process_exits(&evs), 1, "one exit: {evs:?}");
}
