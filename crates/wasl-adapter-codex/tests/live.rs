//! REAL OpenAI Codex CLI end-to-end check — behind `--features live-cli`,
//! EXCLUDED from default test runs (gated with `#![cfg(feature = "live-cli")]`,
//! a cargo feature, NOT `#[ignore]`). Spends real tokens against the actual
//! `codex` binary; run explicitly with
//! `cargo test -p wasl-adapter-codex --features live-cli`.
//!
//! Proves, through the PUBLIC adapter (not internals): a fresh context's first
//! turn round-trips via `ProcessCliInvoker` + `CodexAdapter` and LEARNS the
//! codex-minted thread id (the ledger), and a second delivery in the SAME
//! context resumes that SAME thread (`codex exec resume`) rather than
//! starting a new one — i.e. session continuity actually works against the
//! real CLI, not just in the fake-invoker wiring tests.
//!
//! Per the spec's free-tier rule, this uses the cheapest model
//! (`gpt-5.4-mini`) via `CodexAdapter::with_model`, and exactly 2 model calls.

#![cfg(feature = "live-cli")]

use wasl_adapter::{AgentAdapter, Delivery, DeliveryKind};
use wasl_adapter_codex::{CodexAdapter, ProcessCliInvoker};

/// Cheap, dependency-free structural check that `s` looks like a UUID
/// (8-4-4-4-12 hex groups). Deliberately NOT a full RFC-4122 parse (this
/// crate carries no `uuid` dependency by design — codex mints its own thread
/// ids, we never construct or validate them beyond "did the CLI hand us
/// something id-shaped").
fn looks_like_uuid(s: &str) -> bool {
    let groups: Vec<&str> = s.split('-').collect();
    let expected_lens = [8, 4, 4, 4, 12];
    groups.len() == expected_lens.len()
        && groups
            .iter()
            .zip(expected_lens.iter())
            .all(|(g, len)| g.len() == *len && g.chars().all(|c| c.is_ascii_hexdigit()))
}

#[tokio::test]
async fn live_codex_round_trip_and_resume_continuity() {
    // A caller-supplied nonce keeps the context (and thus the codex thread the
    // adapter learns) FRESH across reruns.
    let nonce = std::env::var("WASL_LIVE_NONCE")
        .expect("WASL_LIVE_NONCE must be set (to a fresh value) to run tests/live.rs");
    let context_id = format!("wasl-adapter-codex-live-{nonce}");

    let adapter = CodexAdapter::with_model(ProcessCliInvoker, "codex", "gpt-5.4-mini");

    // First turn: fresh context -> Turn::First (`codex exec --json
    // --skip-git-repo-check --sandbox read-only -m gpt-5.4-mini -`).
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
        .expect("first turn should succeed against the real codex CLI");
    assert!(
        reply1.contains("PING_OK"),
        "first-turn reply did not contain PING_OK: {reply1}"
    );

    let tid = adapter
        .thread_of(&context_id)
        .expect("adapter should have learned a codex thread_id from thread.started");
    assert!(
        looks_like_uuid(&tid),
        "learned thread_id does not look like a UUID: {tid}"
    );

    // Second turn, SAME context -> Turn::Continuation(tid) (`codex exec
    // resume <tid> --json --skip-git-repo-check -m gpt-5.4-mini -`). If
    // `resume` did not actually preserve context, codex would have no memory
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
        .expect("second (resumed) turn should succeed against the real codex CLI");
    assert!(
        reply2.contains("PING_OK"),
        "resumed-turn reply did not contain PING_OK (exec resume continuity broken): {reply2}"
    );

    // The thread id must be BYTE-STABLE across resume (verified live in the
    // research doc): same tid before and after the second turn.
    let tid_after = adapter
        .thread_of(&context_id)
        .expect("thread_id should still be present after the resumed turn");
    assert_eq!(
        tid_after, tid,
        "thread_id changed across a resumed turn (expected byte-stable)"
    );
}
