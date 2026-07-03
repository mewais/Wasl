//! `AntigravityAdapter::deliver` wiring, exercised via a FAKE `CliInvoker`
//! defined here. The fake records EVERY `(argv, stdin)` it was called with
//! (in order) and returns a scriptable, per-call `Result` (a FIFO queue),
//! letting us drive and assert the ledger contract from
//! `docs/specs/adapter-antigravity.md`: compose -> ledger lookup (First vs.
//! Continuation, per-context) -> invoke (prompt travels in ARGV, not stdin —
//! the one deliberate deviation from the claude/codex seam) -> the blind
//! single fresh-conversation retry (Continuation-only) -> parse -> ledger
//! update (ONLY on full success).
//!
//! UPDATED 2026-07-03 (W8'-fix, orchestrator live verification): argv
//! assertions below call `build_argv` directly (not a hardcoded literal), so
//! they automatically track the corrected `--output-format json` / `-p`-last
//! argv shape once `src/lib.rs` was fixed. The success-mapping fixtures
//! (`error_status_stream`, `tool_denial_decoration_stream`) were updated to
//! match the corrected `parse_reply` contract: `status`/`error` no longer
//! gate success, only `conversation_id`/`response` presence does.
//!
//! NOTE: `async-trait` is NOT a dev-dependency of this crate, so we cannot use
//! the `#[async_trait]` macro in tests. Instead we implement `CliInvoker` by
//! hand, matching the `#[async_trait::async_trait]` desugaring of the trait
//! method (a `Pin<Box<dyn Future + Send + 'async_trait>>` return).
//!
//! The adapter takes ownership of its invoker, so the fake keeps its recording
//! state behind `Arc`s and is `Clone`: we clone a handle, move one copy into
//! the adapter, and inspect the retained copy after the call(s).

use std::collections::VecDeque;
use std::sync::{Arc, Mutex};

use wasl_adapter::{AdapterError, AgentAdapter, Delivery, DeliveryKind};
use wasl_adapter_antigravity::{
    build_argv, compose_prompt, AntigravityAdapter, CliInvoker, Turn, DEFAULT_MODEL,
};

/// Fake invoker: records every call (in order) and returns pre-scripted
/// results, one per call, in the order they were given.
#[derive(Clone)]
struct FakeInvoker {
    calls: Arc<Mutex<Vec<(Vec<String>, String)>>>,
    results: Arc<Mutex<VecDeque<Result<String, AdapterError>>>>,
}

impl FakeInvoker {
    /// `results` are consumed FIFO, one per `invoke` call. Panics if `invoke`
    /// is called more times than scripted results were supplied (a test bug,
    /// not a code-under-test outcome).
    fn new(results: Vec<Result<String, AdapterError>>) -> Self {
        FakeInvoker {
            calls: Arc::new(Mutex::new(Vec::new())),
            results: Arc::new(Mutex::new(results.into_iter().collect())),
        }
    }

    fn calls(&self) -> Vec<(Vec<String>, String)> {
        self.calls.lock().unwrap().clone()
    }
}

impl CliInvoker for FakeInvoker {
    // Hand-written to match the `#[async_trait]` desugaring of `invoke`.
    fn invoke<'life0, 'life1, 'life2, 'async_trait>(
        &'life0 self,
        argv: &'life1 [String],
        stdin: &'life2 str,
    ) -> core::pin::Pin<
        Box<
            dyn core::future::Future<Output = Result<String, AdapterError>>
                + core::marker::Send
                + 'async_trait,
        >,
    >
    where
        'life0: 'async_trait,
        'life1: 'async_trait,
        'life2: 'async_trait,
        Self: 'async_trait,
    {
        // Record the call synchronously (owned copies), then return the next
        // scripted result.
        self.calls
            .lock()
            .unwrap()
            .push((argv.to_vec(), stdin.to_string()));
        let result = self
            .results
            .lock()
            .unwrap()
            .pop_front()
            .expect("FakeInvoker.invoke called more times than scripted results were provided");
        Box::pin(async move { result })
    }
}

fn sample_delivery(context_id: &str) -> Delivery {
    Delivery {
        context_id: context_id.to_string(),
        from: "alice".to_string(),
        kind: DeliveryKind::Consult,
        text: "what is up?".to_string(),
        data: vec![],
    }
}

/// A happy `agy -p` print-mode JSON result line carrying `cid` and `text`.
fn ok_stream(cid: &str, text: &str) -> String {
    format!("{{\"conversation_id\":\"{cid}\",\"status\":\"SUCCESS\",\"response\":\"{text}\"}}\n")
}

/// A genuinely parse-FAILING print-mode JSON result line: invoker succeeds
/// (exit 0), but `parse_reply` must error under the CORRECTED mapping
/// (2026-07-03 W8'-fix): success requires conversation_id AND response BOTH
/// non-empty. This line carries a `conversation_id` on purpose (the
/// ledger-untouched-on-failure rule must hold even so) but an EMPTY
/// `response` — NOT a `status:"ERROR"` line with a non-empty response, since
/// under the corrected mapping such a line is now a SUCCESS (see
/// `tool_denial_decoration_stream` below / `tests/pure.rs`'s
/// `VERIFIED_TOOL_DENIAL_DECORATION_SAMPLE`).
fn error_status_stream() -> String {
    "{\"conversation_id\":\"cid-parse-fail\",\"status\":\"ERROR\",\"response\":\"\",\"error\":\"boom\"}\n"
        .to_string()
}

/// VERIFIED (real, not synthetic): the exact tool-denial-decoration reply the
/// ORCHESTRATOR observed live (agy 1.0.15, 2026-07-03) — `status:"ERROR"` +
/// non-empty `error`, but ALSO a non-empty, on-topic `response`. Under the
/// corrected mapping (W8'-fix) this is a full SUCCESS through the whole
/// adapter, ledger update included — the same bytes as
/// `tests/pure.rs`'s `VERIFIED_TOOL_DENIAL_DECORATION_SAMPLE`.
fn tool_denial_decoration_stream() -> String {
    "{\"conversation_id\":\"97197d46-2091-40e2-b36f-0dcedd34bcc4\",\"status\":\"ERROR\",\"response\":\"It looks like your request was just the letter \\\"x\\\". ...\",\"error\":\"Permission denied for read_file(/home/mewais/.gemini/antigravity-cli). Matches hardcoded system protection boundary rule.\",\"duration_seconds\":2.70532965,\"num_turns\":1,\"usage\":{}}\n"
        .to_string()
}

fn err(message: &str) -> AdapterError {
    AdapterError {
        message: message.to_string(),
    }
}

// ---------------------------------------------------------------------------
// learn / reuse / per-context
// ---------------------------------------------------------------------------

#[tokio::test]
async fn deliver_first_turn_learns_the_conversation_id() {
    let d = sample_delivery("ctx-42");
    let fake = FakeInvoker::new(vec![Ok(ok_stream("c1", "all good"))]);
    let handle = fake.clone();

    let adapter = AntigravityAdapter::new(fake);
    let out = adapter.deliver(&d).await;
    assert_eq!(out, Ok("all good".to_string()));
    assert_eq!(adapter.conversation_of("ctx-42"), Some("c1".to_string()));

    let calls = handle.calls();
    assert_eq!(calls.len(), 1);
    let prompt = compose_prompt(&d);
    assert_eq!(
        calls[0].0,
        build_argv("agy", DEFAULT_MODEL, Turn::First, &prompt)
    );
    assert_eq!(
        calls[0].1, "",
        "the prompt travels in argv (last element), NOT stdin: stdin must be empty"
    );
}

#[tokio::test]
async fn deliver_second_call_same_context_resumes_the_learned_conversation() {
    let d = sample_delivery("ctx-42");
    let fake = FakeInvoker::new(vec![
        Ok(ok_stream("c1", "first")),
        Ok(ok_stream("c1", "second")),
    ]);
    let handle = fake.clone();

    let adapter = AntigravityAdapter::new(fake);
    assert_eq!(adapter.deliver(&d).await, Ok("first".to_string()));
    assert_eq!(adapter.deliver(&d).await, Ok("second".to_string()));

    let calls = handle.calls();
    assert_eq!(calls.len(), 2);
    let prompt = compose_prompt(&d);
    assert_eq!(
        calls[0].0,
        build_argv("agy", DEFAULT_MODEL, Turn::First, &prompt)
    );
    assert_eq!(
        calls[1].0,
        build_argv("agy", DEFAULT_MODEL, Turn::Continuation("c1"), &prompt)
    );
    assert_eq!(calls[0].1, "");
    assert_eq!(calls[1].1, "");
}

#[tokio::test]
async fn deliver_different_context_is_first_turn_again() {
    let d1 = sample_delivery("ctx-a");
    let d2 = sample_delivery("ctx-b");
    let fake = FakeInvoker::new(vec![Ok(ok_stream("ca", "a")), Ok(ok_stream("cb", "b"))]);
    let handle = fake.clone();

    let adapter = AntigravityAdapter::new(fake);
    assert_eq!(adapter.deliver(&d1).await, Ok("a".to_string()));
    // A DIFFERENT context must be tracked independently: First again, not
    // Continuation, even though the adapter already learned ctx-a's
    // conversation_id.
    assert_eq!(adapter.deliver(&d2).await, Ok("b".to_string()));

    let calls = handle.calls();
    let prompt1 = compose_prompt(&d1);
    let prompt2 = compose_prompt(&d2);
    assert_eq!(
        calls[0].0,
        build_argv("agy", DEFAULT_MODEL, Turn::First, &prompt1)
    );
    assert_eq!(
        calls[1].0,
        build_argv("agy", DEFAULT_MODEL, Turn::First, &prompt2)
    );
    assert_eq!(adapter.conversation_of("ctx-a"), Some("ca".to_string()));
    assert_eq!(adapter.conversation_of("ctx-b"), Some("cb".to_string()));
}

// ---------------------------------------------------------------------------
// blind single fresh-conversation retry (Continuation-only)
// ---------------------------------------------------------------------------

#[tokio::test]
async fn continuation_error_retries_once_as_fresh_first_and_relearns() {
    let d = sample_delivery("ctx-fallback");
    let fake = FakeInvoker::new(vec![
        Ok(ok_stream("c1", "first")),  // learns c1
        Err(err("boom")),              // Continuation(c1) fails
        Ok(ok_stream("c2", "second")), // blind retry as First succeeds, learns c2
        Ok(ok_stream("c2", "third")),  // next deliver resumes c2
    ]);
    let handle = fake.clone();

    let adapter = AntigravityAdapter::new(fake);
    assert_eq!(adapter.deliver(&d).await, Ok("first".to_string()));
    assert_eq!(adapter.deliver(&d).await, Ok("second".to_string()));
    assert_eq!(
        adapter.conversation_of("ctx-fallback"),
        Some("c2".to_string())
    );

    // A THIRD delivery resumes c2 (the freshly-learned conversation), not c1.
    assert_eq!(adapter.deliver(&d).await, Ok("third".to_string()));

    let calls = handle.calls();
    assert_eq!(calls.len(), 4);
    let prompt = compose_prompt(&d);
    assert_eq!(
        calls[0].0,
        build_argv("agy", DEFAULT_MODEL, Turn::First, &prompt)
    );
    assert_eq!(
        calls[1].0,
        build_argv("agy", DEFAULT_MODEL, Turn::Continuation("c1"), &prompt)
    );
    // The blind retry is a FRESH First attempt, not Continuation("c1") again.
    assert_eq!(
        calls[2].0,
        build_argv("agy", DEFAULT_MODEL, Turn::First, &prompt)
    );
    assert_eq!(
        calls[3].0,
        build_argv("agy", DEFAULT_MODEL, Turn::Continuation("c2"), &prompt)
    );
    for call in &calls {
        assert_eq!(call.1, "", "stdin must always be empty for agy");
    }
}

#[tokio::test]
async fn continuation_error_then_retry_error_propagates_without_a_second_retry() {
    let d = sample_delivery("ctx-no-double-retry");
    let fake = FakeInvoker::new(vec![
        Ok(ok_stream("c1", "first")), // learns c1
        Err(err("boom")),             // Continuation(c1) fails
        Err(err("still broken")),     // blind First retry ALSO fails
    ]);
    let handle = fake.clone();

    let adapter = AntigravityAdapter::new(fake);
    assert_eq!(adapter.deliver(&d).await, Ok("first".to_string()));
    let out = adapter.deliver(&d).await;
    assert_eq!(out, Err(err("still broken")));

    // Exactly 2 invocations for the failing deliver: the Continuation attempt
    // + the one blind retry. The retry's own error does NOT trigger a further
    // retry.
    assert_eq!(handle.calls().len(), 3);
    // The ledger is UNCHANGED on failure: still bound to the original c1.
    assert_eq!(
        adapter.conversation_of("ctx-no-double-retry"),
        Some("c1".to_string())
    );
}

#[tokio::test]
async fn first_turn_invoker_error_propagates_without_any_retry() {
    let d = sample_delivery("ctx-fresh-error");
    let fake = FakeInvoker::new(vec![
        Err(err("spawn agy: No such file or directory")),
        Ok(ok_stream("c9", "ok now")),
    ]);
    let handle = fake.clone();

    let adapter = AntigravityAdapter::new(fake);
    let out = adapter.deliver(&d).await;
    assert_eq!(out, Err(err("spawn agy: No such file or directory")));

    // First-turn errors have nothing to fall back to (agy mints ids, so an
    // "already in use" collision cannot exist): exactly 1 invocation, no
    // retry.
    assert_eq!(handle.calls().len(), 1);
    assert_eq!(adapter.conversation_of("ctx-fresh-error"), None);

    // The next delivery for this SAME context is STILL a First attempt
    // (nothing was ever learned).
    assert_eq!(adapter.deliver(&d).await, Ok("ok now".to_string()));
    let calls = handle.calls();
    assert_eq!(calls.len(), 2);
    let prompt = compose_prompt(&d);
    assert_eq!(
        calls[1].0,
        build_argv("agy", DEFAULT_MODEL, Turn::First, &prompt)
    );
    assert_eq!(
        adapter.conversation_of("ctx-fresh-error"),
        Some("c9".to_string())
    );
}

// ---------------------------------------------------------------------------
// parse failure never triggers a retry and never touches the ledger
// ---------------------------------------------------------------------------

#[tokio::test]
async fn parse_failure_on_first_turn_does_not_retry_or_touch_ledger() {
    let d = sample_delivery("ctx-parse-fail-first");
    let fake = FakeInvoker::new(vec![Ok(error_status_stream())]);
    let handle = fake.clone();

    let adapter = AntigravityAdapter::new(fake);
    let err_out = adapter.deliver(&d).await.unwrap_err();
    assert!(
        err_out.message.contains("agy turn failed:"),
        "message was: {}",
        err_out.message
    );

    // The invoker succeeded (exit 0); ONLY parsing failed (empty response —
    // see error_status_stream's doc comment: under the corrected mapping,
    // status:"ERROR" alone does NOT fail the turn). No retry: exactly
    // 1 invocation, and the ledger is untouched (nothing was ever learned) —
    // EVEN THOUGH the line carried a conversation_id ("cid-parse-fail"): a
    // conversation whose last turn produced no usable response must not be
    // resumed blind.
    assert_eq!(handle.calls().len(), 1);
    assert_eq!(adapter.conversation_of("ctx-parse-fail-first"), None);
}

#[tokio::test]
async fn tool_denial_decoration_reply_is_a_success_and_learns_the_ledger() {
    // CORRECTED 2026-07-03 (W8'-fix): a reply carrying status:"ERROR" and a
    // populated error, but ALSO a non-empty response, is a genuine SUCCESS
    // through the WHOLE adapter — deliver returns Ok, and the ledger is
    // updated with the learned conversation_id, exactly like any other
    // successful turn. This is the adapter-level regression test mirroring
    // tests/pure.rs's parse_reply_verified_tool_denial_decoration_sample_is_ok_with_full_response.
    let d = sample_delivery("ctx-tool-denial");
    let fake = FakeInvoker::new(vec![Ok(tool_denial_decoration_stream())]);
    let handle = fake.clone();

    let adapter = AntigravityAdapter::new(fake);
    let out = adapter.deliver(&d).await;
    assert_eq!(
        out,
        Ok("It looks like your request was just the letter \"x\". ...".to_string())
    );
    assert_eq!(
        adapter.conversation_of("ctx-tool-denial"),
        Some("97197d46-2091-40e2-b36f-0dcedd34bcc4".to_string())
    );
    assert_eq!(handle.calls().len(), 1);
}

// ---------------------------------------------------------------------------
// with_bin / with_model / malformed stdout
// ---------------------------------------------------------------------------

#[tokio::test]
async fn with_bin_overrides_program_name() {
    let d = sample_delivery("ctx-bin");
    let fake = FakeInvoker::new(vec![Ok(ok_stream("c1", "ok"))]);
    let handle = fake.clone();

    let adapter = AntigravityAdapter::with_bin(fake, "agy-next");
    let out = adapter.deliver(&d).await;
    assert_eq!(out, Ok("ok".to_string()));

    let calls = handle.calls();
    assert_eq!(calls[0].0[0], "agy-next");
    let prompt = compose_prompt(&d);
    assert_eq!(
        calls[0].0,
        build_argv("agy-next", DEFAULT_MODEL, Turn::First, &prompt)
    );
}

#[tokio::test]
async fn with_model_passes_custom_model_on_first_and_continuation() {
    let d = sample_delivery("ctx-model");
    let fake = FakeInvoker::new(vec![
        Ok(ok_stream("c1", "first")),
        Ok(ok_stream("c1", "second")),
    ]);
    let handle = fake.clone();

    let adapter = AntigravityAdapter::with_model(fake, "agy", "Gemini 3.5 Flash (High)");
    assert_eq!(adapter.deliver(&d).await, Ok("first".to_string()));
    assert_eq!(adapter.deliver(&d).await, Ok("second".to_string()));

    let calls = handle.calls();
    let prompt = compose_prompt(&d);
    assert_eq!(
        calls[0].0,
        build_argv("agy", "Gemini 3.5 Flash (High)", Turn::First, &prompt)
    );
    assert_eq!(
        calls[1].0,
        build_argv(
            "agy",
            "Gemini 3.5 Flash (High)",
            Turn::Continuation("c1"),
            &prompt
        )
    );
    assert!(calls[0].0.iter().any(|a| a == "Gemini 3.5 Flash (High)"));
    assert!(calls[1].0.iter().any(|a| a == "Gemini 3.5 Flash (High)"));
}

#[tokio::test]
async fn malformed_stdout_yields_parse_error() {
    let d = sample_delivery("ctx-malformed");
    let fake = FakeInvoker::new(vec![Ok("not json".to_string())]);

    let adapter = AntigravityAdapter::new(fake);
    let err_out = adapter.deliver(&d).await.unwrap_err();
    assert!(
        err_out
            .message
            .contains("agy output had no JSON result line"),
        "message was: {}",
        err_out.message
    );
}
