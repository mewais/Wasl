//! REAL Claude Code CLI end-to-end check — behind `--features live-cli`, EXCLUDED
//! from default test runs (gated with `#![cfg(feature = "live-cli")]`, a cargo
//! feature, NOT `#[ignore]`). Spends real tokens against the actual `claude`
//! binary; run explicitly with
//! `cargo test -p wasl-adapter-claude --features live-cli`.
//!
//! Proves, through the PUBLIC adapter (not internals): a fresh context's first
//! turn round-trips via `ProcessCliInvoker` + `ClaudeAdapter`, and a second
//! delivery in the SAME context resumes the same claude session (`--resume`)
//! rather than starting a new one — i.e. session continuity actually works
//! against the real CLI, not just in the fake-invoker wiring tests.

#![cfg(feature = "live-cli")]

use wasl_adapter::{AgentAdapter, Delivery, DeliveryKind};
use wasl_adapter_claude::{ClaudeAdapter, ProcessCliInvoker};

#[tokio::test]
async fn live_claude_round_trip_and_resume_continuity() {
    // A caller-supplied nonce keeps the context (and thus the derived claude
    // session uuid) FRESH across reruns, so consecutive live runs never
    // collide with a previous run's already-used session id.
    let nonce = std::env::var("WASL_LIVE_NONCE")
        .expect("WASL_LIVE_NONCE must be set (to a fresh value) to run tests/live.rs");
    let context_id = format!("wasl-adapter-claude-live-{nonce}");

    let adapter = ClaudeAdapter::new(ProcessCliInvoker);

    // First turn: fresh context -> Turn::First (`--session-id`).
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
        .expect("first turn should succeed against the real claude CLI");
    assert!(
        reply1.contains("PING_OK"),
        "first-turn reply did not contain PING_OK: {reply1}"
    );

    // Second turn, SAME context -> Turn::Continuation (`--resume`). If
    // `--resume` did not actually preserve context, claude would have no
    // memory of what word was requested and this would fail.
    let followup = Delivery {
        context_id,
        from: "wasl-live-test".to_string(),
        kind: DeliveryKind::Consult,
        text: "What exact word did I ask for? Answer with just that word.".to_string(),
        data: vec![],
    };
    let reply2 = adapter
        .deliver(&followup)
        .await
        .expect("second (resumed) turn should succeed against the real claude CLI");
    assert!(
        reply2.contains("PING_OK"),
        "resumed-turn reply did not contain PING_OK (--resume continuity broken): {reply2}"
    );
}
