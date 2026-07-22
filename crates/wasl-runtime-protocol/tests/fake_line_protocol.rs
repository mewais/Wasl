//! Phase-2B PRIMARY suite: `FakeLineProtocol` end-to-end through a REAL child
//! process — the long-lived in-repo fake structured agent.
//!
//! Layer-2 testability (runtime.md §14): unlike R1's in-memory scripted
//! `InteractiveTransport` (layer 1), THIS suite spawns the fake agent as a real
//! OS child (via `env!("CARGO_BIN_EXE_wasl-fake-agent")`) and speaks its
//! line-oriented-JSON wire through the `FakeLineProtocol` driver, asserting the
//! §1.2 wire → `ProtocolEvent` mapping AND the property no headless spawn can
//! have: context retention across turns in the SAME live process.
//!
//! Task items covered here: **2** (spawn + basic turn), **3** (context-retention
//! proof — SAME OS PID served both turns), **5** (simulate-crash → ProcessExited,
//! no hang). The hostile-input scripts (item 6) live in
//! `tests/hostile_inputs.rs`; process supervision (item 7) in
//! `tests/supervision.rs`; the M2 EPIPE benignity (item 8) in
//! `tests/epipe.rs`; `hold`/queueing (item 4) in `tests/hold_queueing.rs`.
//!
//! EXPECTED RED: `wasl_runtime_protocol::FakeLineProtocol` and the
//! `wasl-fake-agent` dev-bin do not exist in the empty R2 skeleton yet. See
//! `tests/support/mod.rs` for the full "CODER MUST ADD" contract.

mod support;

use support::*;

use wasl_runtime::{InteractiveProtocol, ProtocolEvent, RuntimeCapabilities, SpawnOptions};
use wasl_runtime_protocol::FakeLineProtocol;

/// A `FakeLineProtocol` bound to the compiled dev-bin.
fn driver() -> FakeLineProtocol {
    FakeLineProtocol::new(fake_agent_bin())
}

// =====================================================================
// Item 2 — spawn + basic turn: SessionReady → TurnAccepted → AssistantDelta →
// TurnCompleted, mapped correctly onto ProtocolEvent (§1.2 table).
// =====================================================================

#[tokio::test]
async fn spawn_yields_session_ready_with_a_native_id() {
    let proto = driver();
    let mut tx = proto
        .spawn(&SpawnOptions::default())
        .await
        .expect("spawn fake agent");

    // The very first event a freshly-spawned fake emits is `ready` → SessionReady
    // carrying a native id (§1.2: `{"ev":"ready","native":...}` → SessionReady).
    let ev = next_or_hang(&mut *tx, "first event after spawn")
        .await
        .expect("a first event");
    match ev {
        ProtocolEvent::SessionReady { native } => {
            assert!(
                native.is_some(),
                "the fake agent reports a native session id on ready (§1.2)"
            );
        }
        other => panic!("first event must be SessionReady, got {other:?}"),
    }
}

#[tokio::test]
async fn basic_turn_maps_accepted_delta_completed() {
    let proto = driver();
    let mut tx = proto
        .spawn(&SpawnOptions::default())
        .await
        .expect("spawn fake agent");

    // Consume the ready event.
    let ready = next_or_hang(&mut *tx, "ready").await.expect("ready");
    assert!(
        is_session_ready(&ready),
        "expected SessionReady, got {ready:?}"
    );

    // Drive one turn that produces a reply.
    tx.send_turn(&prompt(&req_say("hi-there")))
        .await
        .expect("send_turn accepted");

    let evs = drain_to_terminal(&mut *tx, "basic turn").await;

    // §1.2 mapping: accepted → TurnAccepted, then AssistantDelta(s), then
    // TurnCompleted{reply}. `accepted` MAY be present (some scripts omit it,
    // covered in hostile_inputs); a plain `say` emits it.
    assert!(
        evs.iter().any(is_accepted),
        "a `say` turn emits TurnAccepted, got {evs:?}"
    );
    assert_eq!(
        completion_of(&evs),
        Some("hi-there".to_string()),
        "TurnCompleted must carry the fake's reply verbatim, got {evs:?}"
    );
    assert_eq!(
        count_completions(&evs),
        1,
        "exactly one TurnCompleted, got {evs:?}"
    );
}

#[tokio::test]
async fn assistant_delta_text_is_mapped_verbatim() {
    let proto = driver();
    let mut tx = proto
        .spawn(&SpawnOptions::default())
        .await
        .expect("spawn fake agent");
    let _ = next_or_hang(&mut *tx, "ready").await.expect("ready");

    // A batched turn deterministically emits a delta then a done.
    tx.send_turn(&prompt(&req_emit_batched_turn("chunk-A", "final")))
        .await
        .expect("send_turn");
    let evs = drain_to_terminal(&mut *tx, "delta turn").await;

    let deltas: Vec<&str> = evs.iter().filter_map(delta_text).collect();
    assert_eq!(
        deltas,
        vec!["chunk-A"],
        "AssistantDelta text mapped verbatim (§1.2), got {evs:?}"
    );
    assert_eq!(completion_of(&evs), Some("final".to_string()));
}

#[tokio::test]
async fn driver_reports_an_honest_non_all_false_capability_report() {
    // The fake is allowed to claim a full-featured managed report (runtime.md §14:
    // the fake proves the state machine; only a REAL driver may not claim
    // interruptible/streaming until captured). A driver reporting all-false is a
    // FAIL (impl-plan Phase-1); assert the fake's report is honest & non-trivial.
    let proto = driver();
    let caps = proto.capabilities();
    let all_false = RuntimeCapabilities {
        persistent_process: false,
        persistent_conversation: false,
        structured_turns: false,
        streaming_output: false,
        attachable: false,
        interruptible: false,
        structured_provenance: false,
        resumable: false,
    };
    assert_ne!(caps, all_false, "a driver reporting all-false is a FAIL");
    assert!(
        caps.persistent_process && caps.persistent_conversation,
        "the fake is a persistent, context-retaining process (§1.1): {caps:?}"
    );
    assert!(
        caps.structured_turns,
        "the fake has reliable turn boundaries (§1.2 `done`): {caps:?}"
    );
}

// =====================================================================
// Item 3 — context-retention proof: `remember` on turn 1, `recall` on turn 2 in
// the SAME spawned process; assert the SAME OS PID served both turns (the
// property Phase-2A's in-memory fake could not prove) AND the fact is retained.
// =====================================================================

#[tokio::test]
async fn remember_then_recall_retains_the_fact_across_two_turns() {
    let proto = driver();
    let mut tx = proto
        .spawn(&SpawnOptions::default())
        .await
        .expect("spawn fake agent");
    let _ = next_or_hang(&mut *tx, "ready").await.expect("ready");

    // Turn 1: remember a distinctive token (§1.1 fact map).
    tx.send_turn(&prompt(&req_remember("codeword", "VIOLET-7F29")))
        .await
        .expect("send_turn remember");
    let t1 = drain_to_terminal(&mut *tx, "remember turn").await;
    assert_eq!(count_completions(&t1), 1, "remember turn completes: {t1:?}");

    // Turn 2: recall it — over the SAME never-closed stdin, SAME process.
    tx.send_turn(&prompt(&req_recall("codeword")))
        .await
        .expect("send_turn recall");
    let t2 = drain_to_terminal(&mut *tx, "recall turn").await;

    assert_eq!(
        completion_of(&t2),
        Some("VIOLET-7F29".to_string()),
        "the fact must survive across turns in the SAME live process (§1.1): {t2:?}"
    );
}

#[tokio::test]
async fn same_os_pid_serves_both_turns() {
    let proto = driver();
    let mut tx = proto
        .spawn(&SpawnOptions::default())
        .await
        .expect("spawn fake agent");
    let _ = next_or_hang(&mut *tx, "ready").await.expect("ready");

    // `recall pid` replies with the fake's OWN OS pid (decimal) — the coder's
    // fake MUST honor this (support::req_recall_pid contract). Two turns; the pid
    // reply must be byte-identical → one process served both (mirrors the
    // spike's `pid_turn1==pid_turn2` VERIFIED proof, runtime-protocol.md §0′).
    tx.send_turn(&prompt(&req_recall_pid()))
        .await
        .expect("send_turn pid #1");
    let t1 = drain_to_terminal(&mut *tx, "pid turn 1").await;
    let pid1 = completion_of(&t1).expect("pid reply 1");

    tx.send_turn(&prompt(&req_recall_pid()))
        .await
        .expect("send_turn pid #2");
    let t2 = drain_to_terminal(&mut *tx, "pid turn 2").await;
    let pid2 = completion_of(&t2).expect("pid reply 2");

    assert_eq!(
        pid1, pid2,
        "the SAME OS process must serve both turns (persistent_process), got pid1={pid1} pid2={pid2}"
    );
    assert!(
        pid1.trim().parse::<u32>().is_ok(),
        "the pid reply must be a decimal OS pid, got {pid1:?}"
    );
}

#[tokio::test]
async fn a_remembered_fact_and_the_pid_are_both_stable_across_a_third_turn() {
    // Stronger multi-turn proof: interleave remember/recall with a pid check so
    // a driver that secretly re-spawns per turn (losing the fact AND changing the
    // pid) is caught on BOTH axes at once.
    let proto = driver();
    let mut tx = proto
        .spawn(&SpawnOptions::default())
        .await
        .expect("spawn fake agent");
    let _ = next_or_hang(&mut *tx, "ready").await.expect("ready");

    tx.send_turn(&prompt(&req_recall_pid()))
        .await
        .expect("pid #1");
    let pid_a = completion_of(&drain_to_terminal(&mut *tx, "pid a").await).expect("pid a");

    tx.send_turn(&prompt(&req_remember("k", "kept")))
        .await
        .expect("remember");
    let _ = drain_to_terminal(&mut *tx, "remember").await;

    tx.send_turn(&prompt(&req_recall("k")))
        .await
        .expect("recall");
    let recalled =
        completion_of(&drain_to_terminal(&mut *tx, "recall").await).expect("recall reply");
    assert_eq!(recalled, "kept", "fact retained across 3 turns");

    tx.send_turn(&prompt(&req_recall_pid()))
        .await
        .expect("pid #2");
    let pid_b = completion_of(&drain_to_terminal(&mut *tx, "pid b").await).expect("pid b");
    assert_eq!(pid_a, pid_b, "one process across all three turns");
}

// =====================================================================
// Item 5 — simulate-crash: the agent exits mid-turn; assert ProtocolEvent::
// ProcessExited is produced and the driver does NOT hang.
// =====================================================================

#[tokio::test]
async fn crash_mid_turn_yields_process_exited_and_does_not_hang() {
    let proto = driver();
    let mut tx = proto
        .spawn(&SpawnOptions::default())
        .await
        .expect("spawn fake agent");
    let _ = next_or_hang(&mut *tx, "ready").await.expect("ready");

    // Tell the agent to exit mid-turn with a non-zero code (§1.1 crash).
    tx.send_turn(&prompt(&req_crash(7)))
        .await
        .expect("send_turn crash (write may race the exit but must not error here)");

    // The driver must surface ProcessExited (mapped from process exit, §1.2) and
    // the drain must TERMINATE (no hang) — enforced by the short per-event guard.
    let evs = drain_to_process_exit(&mut *tx, "crash turn").await;
    assert_eq!(
        count_process_exits(&evs),
        1,
        "exactly one ProcessExited after a mid-turn crash, got {evs:?}"
    );
    match evs.last() {
        Some(ProtocolEvent::ProcessExited { code }) => {
            assert_eq!(
                *code,
                Some(7),
                "the crash exit code (7) is surfaced, got {code:?}"
            );
        }
        other => panic!("last event must be ProcessExited, got {other:?}"),
    }
}

#[tokio::test]
async fn next_event_returns_none_after_process_exited_terminal() {
    // After ProcessExited (the terminal for the transport, R1 doc), `next_event`
    // must end the stream (`None`) rather than blocking forever or re-emitting a
    // terminal event.
    let proto = driver();
    let mut tx = proto
        .spawn(&SpawnOptions::default())
        .await
        .expect("spawn fake agent");
    let _ = next_or_hang(&mut *tx, "ready").await.expect("ready");

    tx.send_turn(&prompt(&req_crash(3))).await.expect("crash");
    let evs = drain_to_process_exit(&mut *tx, "crash").await;
    assert_eq!(count_process_exits(&evs), 1, "one exit: {evs:?}");

    // One more poll: must be a clean end, not a hang, not a duplicate terminal.
    let after = next_or_hang(&mut *tx, "after ProcessExited").await;
    assert!(
        after.is_none(),
        "stream must end (None) after ProcessExited, got {after:?}"
    );
}

// --- local helpers -------------------------------------------------------

use wasl_runtime::ComposedPrompt;

fn prompt(line: &str) -> ComposedPrompt {
    // `send_turn` takes a ComposedPrompt; for the fake, the composed prompt text
    // IS the request line the driver writes to stdin (the driver appends the
    // newline framing). The tester controls the exact request bytes this way.
    ComposedPrompt(line.to_string())
}

fn completion_of(evs: &[ProtocolEvent]) -> Option<String> {
    evs.iter()
        .find_map(|e| completed_reply(e).map(|s| s.to_string()))
}
