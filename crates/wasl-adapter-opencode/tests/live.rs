//! REAL `opencode` CLI end-to-end check — behind `--features live-cli`,
//! EXCLUDED from default test runs (gated with `#![cfg(feature = "live-cli")]`,
//! a cargo feature, NOT `#[ignore]`). Spends real tokens against the actual
//! `opencode` binary; run explicitly with
//! `cargo test -p wasl-adapter-opencode --features live-cli`.
//!
//! Proves, through the PUBLIC adapter (not internals): a fresh context's first
//! turn round-trips via `ProcessCliInvoker` + `OpencodeAdapter` and LEARNS the
//! opencode-minted session id (the ledger), and a second delivery in the SAME
//! context resumes that SAME session (`opencode run -s <id>`) rather than
//! starting a new one — i.e. session continuity actually works against the
//! real CLI, not just in the fake-invoker wiring tests.
//!
//! Per the spec's cost/context rule, this uses the cheapest large-context model
//! (`amd-anthropic/Claude-Haiku-4.5`, to avoid the 32k-limit overflow trap) via
//! `OpencodeAdapter::with_model`, and exactly 2 model calls. The orchestrator
//! runs this with an isolated opencode config (`OPENCODE_DISABLE_AUTOCOMPACT=1`,
//! big-context model, no MCP catalog) on a host giving opencode >=~4 GB RSS.

#![cfg(feature = "live-cli")]

use wasl_adapter::{AgentAdapter, Delivery, DeliveryKind};
use wasl_adapter_opencode::{OpencodeAdapter, ProcessCliInvoker};

#[tokio::test]
async fn live_opencode_round_trip_and_resume_continuity() {
    // A caller-supplied nonce keeps the context (and thus the opencode session
    // the adapter learns) FRESH across reruns.
    let nonce = std::env::var("WASL_LIVE_NONCE")
        .expect("WASL_LIVE_NONCE must be set (to a fresh value) to run tests/live.rs");
    let context_id = format!("wasl-adapter-opencode-live-{nonce}");

    let adapter = OpencodeAdapter::with_model(
        ProcessCliInvoker,
        "opencode",
        "amd-anthropic/Claude-Haiku-4.5",
    );

    // First turn: fresh context -> Turn::First (`opencode run --format json
    // -m amd-anthropic/Claude-Haiku-4.5 "<prompt>"`).
    let ping = Delivery {
        context_id: context_id.clone(),
        from: "wasl-live-test".to_string(),
        kind: DeliveryKind::Consult,
        text: "Reply with exactly the word: PING_OK".to_string(),
        data: vec![],
    };
    let reply1 = adapter
        .deliver(&ping)
        .await
        .expect("first turn should succeed against the real opencode CLI");
    assert!(
        reply1.contains("PING_OK"),
        "first-turn reply did not contain PING_OK: {reply1}"
    );

    let sid = adapter
        .session_of(&context_id)
        .expect("adapter should have learned an opencode session_id from the JSONL stream");
    assert!(
        sid.starts_with("ses_"),
        "learned session_id does not start with ses_: {sid}"
    );

    // Second turn, SAME context -> Turn::Continuation(sid) (`opencode run
    // --format json -s <sid> -m amd-anthropic/Claude-Haiku-4.5 "<prompt>"`).
    // If `-s` did not actually preserve context, opencode would have no memory
    // of what word was requested and this would fail.
    let followup = Delivery {
        context_id: context_id.clone(),
        from: "wasl-live-test".to_string(),
        kind: DeliveryKind::Consult,
        text: "What exact word did I ask for? Answer with just that word.".to_string(),
        data: vec![],
    };
    let reply2 = adapter
        .deliver(&followup)
        .await
        .expect("second (resumed) turn should succeed against the real opencode CLI");
    assert!(
        reply2.contains("PING_OK"),
        "resumed-turn reply did not contain PING_OK (-s resume continuity broken): {reply2}"
    );

    // The session id must be BYTE-STABLE across resume (verified live in the
    // spec's capture): same sid before and after the second turn.
    let sid_after = adapter
        .session_of(&context_id)
        .expect("session_id should still be present after the resumed turn");
    assert_eq!(
        sid_after, sid,
        "session_id changed across a resumed turn (expected byte-stable)"
    );
}
