//! Phase 4 — `ClaudeStreamProtocol` non-live smoke test (no CLI spawn, runs in
//! default `cargo test -p wasl-runtime-protocol`, no `--features live-cli`).
//!
//! Pins the LOCKED capability report (`docs/specs/runtime-protocol.md` §2):
//! `persistent_process: true, persistent_conversation: true,
//! structured_turns: true, streaming_output: false, attachable: true,
//! interruptible: false, structured_provenance: true, resumable: true`. The
//! two `false`s are the honest ⛔ UNKNOWN cells (untested interruptibility;
//! only whole-block, non-streaming output was captured) — a driver reporting
//! all-false, or rounding either up to `true` without a follow-up capture,
//! is a FAIL per repo convention (`fake_line.rs`'s own
//! `capabilities_are_honest_non_all_false` test is the sibling precedent).

use wasl_runtime_protocol::{ClaudeStreamProtocol, InteractiveProtocol};

#[test]
fn capabilities_are_exactly_the_locked_report() {
    let driver = ClaudeStreamProtocol::new("claude");
    let caps = driver.capabilities();

    assert!(
        caps.persistent_process,
        "VERIFIED same-pid multi-turn (spike)"
    );
    assert!(
        caps.persistent_conversation,
        "VERIFIED context retention across turns (spike)"
    );
    assert!(
        caps.structured_turns,
        "VERIFIED system/init + result turn boundaries (spike)"
    );
    assert!(
        !caps.streaming_output,
        "⛔ UNKNOWN cell: only whole assistant blocks were captured, no token \
         deltas — must stay false until a follow-up streaming capture"
    );
    assert!(
        caps.attachable,
        "a persistent stdin/stdout connection is observable by a viewer"
    );
    assert!(
        !caps.interruptible,
        "⛔ UNKNOWN cell: interruptibility was untested in Phase-0' — must stay \
         false until a follow-up interrupt capture"
    );
    assert!(
        caps.structured_provenance,
        "system/init session_id + result request_id give machine-usable \
         correlation ids (VERIFIED)"
    );
    assert!(
        caps.resumable,
        "re-attach with the same --session-id uuid (VERIFIED shape, within \
         one node-process lifetime)"
    );
}

#[test]
fn capabilities_are_not_all_false() {
    let driver = ClaudeStreamProtocol::new("claude");
    let caps = driver.capabilities();
    let all_false = !caps.persistent_process
        && !caps.persistent_conversation
        && !caps.structured_turns
        && !caps.streaming_output
        && !caps.attachable
        && !caps.interruptible
        && !caps.structured_provenance
        && !caps.resumable;
    assert!(
        !all_false,
        "an all-false capability report is a FAIL (repo convention, \
         fake_line.rs precedent) — ClaudeStreamProtocol has 6 genuinely \
         VERIFIED true capabilities"
    );
}
