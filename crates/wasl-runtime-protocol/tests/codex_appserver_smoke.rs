//! Phase-6-equivalent (codex) — `CodexAppServerProtocol` non-live smoke test
//! (no CLI spawn, runs in default `cargo test -p wasl-runtime-protocol`, no
//! `--features live-cli`). Mirrors `claude_stream_smoke.rs` /
//! `opencode_acp_smoke.rs`: pins the LOCKED capability report and the
//! honest-non-all-false convention.
//!
//! ==========================================================================
//! ⚠️  DOES NOT COMPILE until the coder creates `CodexAppServerProtocol` in the
//! NEW `src/codex_appserver.rs` and re-exports it at the crate root
//! (`pub use codex_appserver::CodexAppServerProtocol;` in src/lib.rs). That
//! unresolved-import build failure IS the red state for this file. ⚠️
//! ==========================================================================
//!
//! The capability report grounded in the real spike
//! (`spikes/live-substrate/out/codex_appserver/verdict.json`):
//!   * persistent_process   = true  (VERIFIED: pid_turn1 == pid_turn2 == 7,
//!                                    "P_persistent_process": "verified-true")
//!   * persistent_conversation = true (VERIFIED: turn 2 recalled the turn-1
//!                                    token, "C_context_retained":"verified-true")
//!   * structured_turns     = true  (VERIFIED: machine-detectable turn/completed
//!                                    boundary both turns, "B_turn_boundary":
//!                                    "verified-true")
//!   * streaming_output     = true  (VERIFIED: REAL incremental
//!                                    item/agentMessage/delta notifications, e.g.
//!                                    events.jsonl lines 29-35 stream the token
//!                                    "V","IO","LET","-","7","F","29" — like
//!                                    opencode, unlike claude's whole-block output)
//!   * attachable           = true  (a persistent stdio JSON-RPC connection is
//!                                    observable by a viewer)
//!   * interruptible        = false ⛔ UNKNOWN CELL: turn/interrupt is
//!                                    schema-ADVERTISED but was NOT triggered
//!                                    live ("interruptible":
//!                                    "advertised-turn/interrupt (not triggered
//!                                    live)"). Per the honest-residual
//!                                    convention (claude/opencode keep this
//!                                    false until a follow-up interrupt capture),
//!                                    it stays false — an advertised-but-not-
//!                                    exercised capability is NOT rounded up.
//!   * structured_provenance = true (native thread id [UUIDv7] + JSON-RPC
//!                                    request ids + per-turn ids give
//!                                    machine-usable correlation, VERIFIED)
//!   * resumable            = true  (thread model + sessionId; the resume RPC's
//!                                    exact wire shape is a documented,
//!                                    uncaptured gap — the coder documents it on
//!                                    CodexAppServerProtocol::resume, same as
//!                                    OpenCodeAcpProtocol::resume's ASSUMED row).
//!
//! NOTE the two `false`-vs-`true` differences from claude prove genuine
//! per-driver independence (§the genericity signal): codex streams
//! (streaming_output:true) where claude does not.

use wasl_runtime_protocol::{CodexAppServerProtocol, InteractiveProtocol};

#[test]
fn capabilities_are_exactly_the_locked_report() {
    let driver = CodexAppServerProtocol::new("codex");
    let caps = driver.capabilities();

    assert!(
        caps.persistent_process,
        "VERIFIED same-pid multi-turn (verdict.json pid_turn1==pid_turn2)"
    );
    assert!(
        caps.persistent_conversation,
        "VERIFIED context retention across turns (turn 2 recalled the token)"
    );
    assert!(
        caps.structured_turns,
        "VERIFIED turn/completed boundaries both turns (verdict.json B)"
    );
    assert!(
        caps.streaming_output,
        "VERIFIED REAL streaming deltas via item/agentMessage/delta \
         (events.jsonl lines 29-35) -- like opencode, unlike claude"
    );
    assert!(
        caps.attachable,
        "a persistent stdio JSON-RPC connection is observable by a viewer"
    );
    assert!(
        !caps.interruptible,
        "⛔ UNKNOWN cell: turn/interrupt is schema-ADVERTISED but was NOT \
         triggered live -- must stay false until a follow-up interrupt capture \
         (same honest-residual convention as claude/opencode)"
    );
    assert!(
        caps.structured_provenance,
        "native thread id (UUIDv7) + JSON-RPC request ids + per-turn ids give \
         machine-usable correlation (VERIFIED)"
    );
    assert!(
        caps.resumable,
        "thread model + sessionId (VERIFIED shape; the resume RPC's exact wire \
         shape itself is a documented, uncaptured gap -- see \
         CodexAppServerProtocol::resume's doc comment)"
    );
}

#[test]
fn capabilities_are_not_all_false() {
    let driver = CodexAppServerProtocol::new("codex");
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
         CodexAppServerProtocol has 7 genuinely VERIFIED true capabilities"
    );
}

/// The genericity signal at the type level: `CodexAppServerProtocol`'s
/// `streaming_output` matches opencode's (true) but DIFFERS from claude's
/// (false) -- proving these are genuinely independent, driver-reported facts,
/// not a copy-pasted rounded-up report.
#[test]
fn streaming_output_matches_opencode_but_differs_from_claude() {
    let codex_caps = CodexAppServerProtocol::new("codex").capabilities();
    let claude_caps = wasl_runtime_protocol::ClaudeStreamProtocol::new("claude").capabilities();
    let opencode_caps = wasl_runtime_protocol::OpenCodeAcpProtocol::new("opencode").capabilities();
    assert!(codex_caps.streaming_output);
    assert!(opencode_caps.streaming_output);
    assert!(!claude_caps.streaming_output);
    assert_ne!(
        codex_caps.streaming_output, claude_caps.streaming_output,
        "two real managed drivers must be able to report genuinely different \
         capability values for the SAME field -- an honest, non-rounded-up \
         report, not a hardcoded shared constant"
    );
}
