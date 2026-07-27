//! Phase 5 — `OpenCodeAcpProtocol` non-live smoke test (no CLI spawn, runs in
//! default `cargo test -p wasl-runtime-protocol`, no `--features live-cli`).
//!
//! Pins the LOCKED capability report (`docs/specs/runtime-protocol.md` §3.1):
//! `persistent_process: true, persistent_conversation: true,
//! structured_turns: true, streaming_output: true, attachable: true,
//! interruptible: false, structured_provenance: true, resumable: true`. The
//! key genericity/differentiation point vs Phase 4's `ClaudeStreamProtocol`:
//! `streaming_output` is `true` here (VERIFIED — ACP's `agent_message_chunk`
//! notifications are REAL deltas, unlike claude's whole-block-only output).
//! `interruptible` stays the one honest ⛔ UNKNOWN cell — untested in
//! Phase-0', same as every other driver.

use wasl_runtime_protocol::{InteractiveProtocol, OpenCodeAcpProtocol};

#[test]
fn capabilities_are_exactly_the_locked_report() {
    let driver = OpenCodeAcpProtocol::new("opencode");
    let caps = driver.capabilities();

    assert!(
        caps.persistent_process,
        "VERIFIED same-pid multi-turn (spike, out/opencode/acp/verdict-acp.json)"
    );
    assert!(
        caps.persistent_conversation,
        "VERIFIED context retention across turns (spike)"
    );
    assert!(
        caps.structured_turns,
        "VERIFIED JSON-RPC result-with-stopReason turn boundaries (spike)"
    );
    assert!(
        caps.streaming_output,
        "VERIFIED REAL streaming deltas via session/update agent_message_chunk \
         (spike) -- the genericity differentiator vs ClaudeStreamProtocol"
    );
    assert!(
        caps.attachable,
        "a persistent stdio JSON-RPC connection is observable by a viewer"
    );
    assert!(
        !caps.interruptible,
        "⛔ UNKNOWN cell: interruptibility was untested in Phase-0' -- must \
         stay false until a follow-up capture"
    );
    assert!(
        caps.structured_provenance,
        "sessionId + JSON-RPC request ids give machine-usable correlation \
         (VERIFIED)"
    );
    assert!(
        caps.resumable,
        "advertised via sessionCapabilities{{resume,fork,list,close}} \
         (VERIFIED capability advertisement; the resume RPC's exact wire \
         shape itself is a documented, uncaptured gap -- see \
         OpenCodeAcpProtocol::resume's doc comment)"
    );
}

#[test]
fn capabilities_are_not_all_false() {
    let driver = OpenCodeAcpProtocol::new("opencode");
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
        "an all-false capability report is a FAIL (repo convention) -- \
         OpenCodeAcpProtocol has 7 genuinely VERIFIED true capabilities"
    );
}

/// The Phase-5 genericity signal, at the type level: `OpenCodeAcpProtocol`'s
/// `streaming_output` differs from `ClaudeStreamProtocol`'s -- proving these
/// are genuinely independent, driver-reported facts (§2 "all 8 fields are
/// INDEPENDENT"), not a copy-pasted, rounded-up capability report.
#[test]
fn streaming_output_genuinely_differs_from_the_claude_driver() {
    let opencode_caps = OpenCodeAcpProtocol::new("opencode").capabilities();
    let claude_caps = wasl_runtime_protocol::ClaudeStreamProtocol::new("claude").capabilities();
    assert!(opencode_caps.streaming_output);
    assert!(!claude_caps.streaming_output);
    assert_ne!(
        opencode_caps.streaming_output, claude_caps.streaming_output,
        "two real managed drivers must be able to report genuinely different \
         capability values for the SAME field -- an honest, non-rounded-up \
         report, not a hardcoded shared constant"
    );
}
