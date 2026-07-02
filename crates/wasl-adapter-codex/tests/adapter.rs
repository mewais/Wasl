//! `CodexAdapter::deliver` wiring, exercised via a FAKE `CliInvoker` defined
//! here. The fake records EVERY `(argv, stdin)` it was called with (in order)
//! and returns a scriptable, per-call `Result` (a FIFO queue), letting us
//! drive and assert the ledger contract from `docs/specs/adapter-codex.md`:
//! compose -> ledger lookup (First vs. Continuation, per-context) -> invoke ->
//! the blind single fresh-thread retry (Continuation-only) -> parse -> ledger
//! update (ONLY on full success).
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
use wasl_adapter_codex::{build_argv, compose_prompt, CliInvoker, CodexAdapter, Turn};

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

/// A happy `codex exec --json` JSONL stream: `thread.started` carrying `tid`,
/// then one `agent_message` carrying `text`.
fn ok_stream(tid: &str, text: &str) -> String {
    format!(
        "{{\"type\":\"thread.started\",\"thread_id\":\"{tid}\"}}\n\
{{\"type\":\"item.completed\",\"item\":{{\"type\":\"agent_message\",\"text\":\"{text}\"}}}}\n"
    )
}

/// A `turn.failed` JSONL stream: invoker succeeds (exit 0), but the turn
/// itself failed, so `parse_reply` must error.
fn turn_failed_stream() -> String {
    "{\"type\":\"thread.started\",\"thread_id\":\"tid-parse-fail\"}\n{\"type\":\"turn.failed\"}\n"
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
async fn deliver_first_turn_learns_the_thread_id() {
    let d = sample_delivery("ctx-42");
    let fake = FakeInvoker::new(vec![Ok(ok_stream("t1", "all good"))]);
    let handle = fake.clone();

    let adapter = CodexAdapter::new(fake);
    let out = adapter.deliver(&d).await;
    assert_eq!(out, Ok("all good".to_string()));
    assert_eq!(adapter.thread_of("ctx-42"), Some("t1".to_string()));

    let calls = handle.calls();
    assert_eq!(calls.len(), 1);
    assert_eq!(calls[0].0, build_argv("codex", None, Turn::First));
    assert_eq!(calls[0].1, compose_prompt(&d));
}

#[tokio::test]
async fn deliver_second_call_same_context_resumes_the_learned_thread() {
    let d = sample_delivery("ctx-42");
    let fake = FakeInvoker::new(vec![
        Ok(ok_stream("t1", "first")),
        Ok(ok_stream("t1", "second")),
    ]);
    let handle = fake.clone();

    let adapter = CodexAdapter::new(fake);
    assert_eq!(adapter.deliver(&d).await, Ok("first".to_string()));
    assert_eq!(adapter.deliver(&d).await, Ok("second".to_string()));

    let calls = handle.calls();
    assert_eq!(calls.len(), 2);
    assert_eq!(calls[0].0, build_argv("codex", None, Turn::First));
    assert_eq!(
        calls[1].0,
        build_argv("codex", None, Turn::Continuation("t1"))
    );
}

#[tokio::test]
async fn deliver_different_context_is_first_turn_again() {
    let d1 = sample_delivery("ctx-a");
    let d2 = sample_delivery("ctx-b");
    let fake = FakeInvoker::new(vec![Ok(ok_stream("ta", "a")), Ok(ok_stream("tb", "b"))]);
    let handle = fake.clone();

    let adapter = CodexAdapter::new(fake);
    assert_eq!(adapter.deliver(&d1).await, Ok("a".to_string()));
    // A DIFFERENT context must be tracked independently: First again, not
    // Continuation, even though the adapter already learned ctx-a's thread.
    assert_eq!(adapter.deliver(&d2).await, Ok("b".to_string()));

    let calls = handle.calls();
    assert_eq!(calls[0].0, build_argv("codex", None, Turn::First));
    assert_eq!(calls[1].0, build_argv("codex", None, Turn::First));
    assert_eq!(adapter.thread_of("ctx-a"), Some("ta".to_string()));
    assert_eq!(adapter.thread_of("ctx-b"), Some("tb".to_string()));
}

// ---------------------------------------------------------------------------
// blind single fresh-thread retry (Continuation-only)
// ---------------------------------------------------------------------------

#[tokio::test]
async fn continuation_error_retries_once_as_fresh_first_and_relearns() {
    let d = sample_delivery("ctx-fallback");
    let fake = FakeInvoker::new(vec![
        Ok(ok_stream("t1", "first")),  // learns t1
        Err(err("boom")),              // Continuation(t1) fails
        Ok(ok_stream("t2", "second")), // blind retry as First succeeds, learns t2
        Ok(ok_stream("t2", "third")),  // next deliver resumes t2
    ]);
    let handle = fake.clone();

    let adapter = CodexAdapter::new(fake);
    assert_eq!(adapter.deliver(&d).await, Ok("first".to_string()));
    assert_eq!(adapter.deliver(&d).await, Ok("second".to_string()));
    assert_eq!(adapter.thread_of("ctx-fallback"), Some("t2".to_string()));

    // A THIRD delivery resumes t2 (the freshly-learned thread), not t1.
    assert_eq!(adapter.deliver(&d).await, Ok("third".to_string()));

    let calls = handle.calls();
    assert_eq!(calls.len(), 4);
    assert_eq!(calls[0].0, build_argv("codex", None, Turn::First));
    assert_eq!(
        calls[1].0,
        build_argv("codex", None, Turn::Continuation("t1"))
    );
    // The blind retry is a FRESH First attempt, not Continuation("t1") again.
    assert_eq!(calls[2].0, build_argv("codex", None, Turn::First));
    assert_eq!(
        calls[3].0,
        build_argv("codex", None, Turn::Continuation("t2"))
    );
}

#[tokio::test]
async fn continuation_error_then_retry_error_propagates_without_a_second_retry() {
    let d = sample_delivery("ctx-no-double-retry");
    let fake = FakeInvoker::new(vec![
        Ok(ok_stream("t1", "first")), // learns t1
        Err(err("boom")),             // Continuation(t1) fails
        Err(err("still broken")),     // blind First retry ALSO fails
    ]);
    let handle = fake.clone();

    let adapter = CodexAdapter::new(fake);
    assert_eq!(adapter.deliver(&d).await, Ok("first".to_string()));
    let out = adapter.deliver(&d).await;
    assert_eq!(out, Err(err("still broken")));

    // Exactly 2 invocations for the failing deliver: the Continuation attempt
    // + the one blind retry. The retry's own error does NOT trigger a further
    // retry.
    assert_eq!(handle.calls().len(), 3);
    // The ledger is UNCHANGED on failure: still bound to the original t1.
    assert_eq!(
        adapter.thread_of("ctx-no-double-retry"),
        Some("t1".to_string())
    );
}

#[tokio::test]
async fn first_turn_invoker_error_propagates_without_any_retry() {
    let d = sample_delivery("ctx-fresh-error");
    let fake = FakeInvoker::new(vec![
        Err(err("spawn codex: No such file or directory")),
        Ok(ok_stream("t9", "ok now")),
    ]);
    let handle = fake.clone();

    let adapter = CodexAdapter::new(fake);
    let out = adapter.deliver(&d).await;
    assert_eq!(out, Err(err("spawn codex: No such file or directory")));

    // First-turn errors have nothing to fall back to (codex mints ids, so an
    // "already in use" collision cannot exist): exactly 1 invocation, no
    // retry.
    assert_eq!(handle.calls().len(), 1);
    assert_eq!(adapter.thread_of("ctx-fresh-error"), None);

    // The next delivery for this SAME context is STILL a First attempt
    // (nothing was ever learned).
    assert_eq!(adapter.deliver(&d).await, Ok("ok now".to_string()));
    let calls = handle.calls();
    assert_eq!(calls.len(), 2);
    assert_eq!(calls[1].0, build_argv("codex", None, Turn::First));
    assert_eq!(adapter.thread_of("ctx-fresh-error"), Some("t9".to_string()));
}

// ---------------------------------------------------------------------------
// parse failure never triggers a retry and never touches the ledger
// ---------------------------------------------------------------------------

#[tokio::test]
async fn parse_failure_on_first_turn_does_not_retry_or_touch_ledger() {
    let d = sample_delivery("ctx-parse-fail-first");
    let fake = FakeInvoker::new(vec![Ok(turn_failed_stream())]);
    let handle = fake.clone();

    let adapter = CodexAdapter::new(fake);
    let err_out = adapter.deliver(&d).await.unwrap_err();
    assert!(
        err_out.message.contains("codex turn failed:"),
        "message was: {}",
        err_out.message
    );

    // The invoker succeeded (exit 0); ONLY parsing failed. No retry: exactly
    // 1 invocation, and the ledger is untouched (nothing was ever learned).
    assert_eq!(handle.calls().len(), 1);
    assert_eq!(adapter.thread_of("ctx-parse-fail-first"), None);
}

// ---------------------------------------------------------------------------
// with_bin / with_model / malformed stdout
// ---------------------------------------------------------------------------

#[tokio::test]
async fn with_bin_overrides_program_name() {
    let d = sample_delivery("ctx-bin");
    let fake = FakeInvoker::new(vec![Ok(ok_stream("t1", "ok"))]);
    let handle = fake.clone();

    let adapter = CodexAdapter::with_bin(fake, "codex-cli");
    let out = adapter.deliver(&d).await;
    assert_eq!(out, Ok("ok".to_string()));

    let calls = handle.calls();
    assert_eq!(calls[0].0[0], "codex-cli");
    assert_eq!(calls[0].0, build_argv("codex-cli", None, Turn::First));
}

#[tokio::test]
async fn with_model_passes_dash_m_on_first_and_continuation() {
    let d = sample_delivery("ctx-model");
    let fake = FakeInvoker::new(vec![
        Ok(ok_stream("t1", "first")),
        Ok(ok_stream("t1", "second")),
    ]);
    let handle = fake.clone();

    let adapter = CodexAdapter::with_model(fake, "codex", "gpt-5.4-mini");
    assert_eq!(adapter.deliver(&d).await, Ok("first".to_string()));
    assert_eq!(adapter.deliver(&d).await, Ok("second".to_string()));

    let calls = handle.calls();
    assert_eq!(
        calls[0].0,
        build_argv("codex", Some("gpt-5.4-mini"), Turn::First)
    );
    assert_eq!(
        calls[1].0,
        build_argv("codex", Some("gpt-5.4-mini"), Turn::Continuation("t1"))
    );
    assert!(calls[0].0.iter().any(|a| a == "-m"));
    assert!(calls[1].0.iter().any(|a| a == "-m"));
}

#[tokio::test]
async fn malformed_stdout_yields_parse_error() {
    let d = sample_delivery("ctx-malformed");
    let fake = FakeInvoker::new(vec![Ok("not json".to_string())]);

    let adapter = CodexAdapter::new(fake);
    let err_out = adapter.deliver(&d).await.unwrap_err();
    assert!(
        err_out.message.contains("parse codex output line:"),
        "message was: {}",
        err_out.message
    );
}
