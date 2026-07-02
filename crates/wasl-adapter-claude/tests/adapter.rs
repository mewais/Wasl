//! `ClaudeAdapter::deliver` wiring, exercised via a FAKE `CliInvoker` defined
//! here. The fake records EVERY `(argv, stdin)` it was called with (in order)
//! and returns a scriptable, per-call `Result` (a FIFO queue), letting us drive
//! and assert the v2 contract: compose -> session_uuid -> build_argv (First vs.
//! Continuation, per-context) -> invoke -> the symmetric one-retry fallback ->
//! parse.
//!
//! NOTE: `async-trait` is NOT a dev-dependency of this crate, so we cannot use
//! the `#[async_trait]` macro in tests. Instead we implement `CliInvoker` by
//! hand, matching the `#[async_trait::async_trait]` desugaring of the trait
//! method (a `Pin<Box<dyn Future + Send + 'async_trait>>` return).
//!
//! The adapter takes ownership of its invoker, so the fake keeps its recording
//! state behind `Arc`s and is `Clone`: we clone a handle, move one copy into the
//! adapter, and inspect the retained copy after the call(s).

use std::collections::VecDeque;
use std::sync::{Arc, Mutex};

use wasl_adapter::{AdapterError, AgentAdapter, Delivery, DeliveryKind};
use wasl_adapter_claude::{
    build_argv, compose_prompt, session_uuid, ClaudeAdapter, CliInvoker, Turn,
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

fn success_json(result: &str) -> String {
    format!(r#"{{"type":"result","is_error":false,"result":"{result}"}}"#)
}

fn err(message: &str) -> AdapterError {
    AdapterError {
        message: message.to_string(),
    }
}

// ---------------------------------------------------------------------------
// happy path: first turn, second turn (same context), different context
// ---------------------------------------------------------------------------

#[tokio::test]
async fn deliver_first_turn_uses_session_id_argv() {
    let d = sample_delivery("ctx-42");
    let fake = FakeInvoker::new(vec![Ok(success_json("all good"))]);
    let handle = fake.clone();

    let adapter = ClaudeAdapter::new(fake);
    let out = adapter.deliver(&d).await;
    assert_eq!(out, Ok("all good".to_string()));

    let calls = handle.calls();
    assert_eq!(calls.len(), 1);
    let su = session_uuid(&d.context_id);
    assert_eq!(calls[0].0, build_argv("claude", &su, Turn::First));
    assert_eq!(calls[0].1, compose_prompt(&d));
}

#[tokio::test]
async fn deliver_second_call_same_context_uses_resume_argv() {
    let d = sample_delivery("ctx-42");
    let fake = FakeInvoker::new(vec![Ok(success_json("first")), Ok(success_json("second"))]);
    let handle = fake.clone();

    let adapter = ClaudeAdapter::new(fake);
    assert_eq!(adapter.deliver(&d).await, Ok("first".to_string()));
    assert_eq!(adapter.deliver(&d).await, Ok("second".to_string()));

    let calls = handle.calls();
    assert_eq!(calls.len(), 2);
    let su = session_uuid(&d.context_id);
    assert_eq!(calls[0].0, build_argv("claude", &su, Turn::First));
    assert_eq!(calls[1].0, build_argv("claude", &su, Turn::Continuation));
}

#[tokio::test]
async fn deliver_different_context_is_first_turn_again() {
    let d1 = sample_delivery("ctx-a");
    let d2 = sample_delivery("ctx-b");
    let fake = FakeInvoker::new(vec![Ok(success_json("a")), Ok(success_json("b"))]);
    let handle = fake.clone();

    let adapter = ClaudeAdapter::new(fake);
    assert_eq!(adapter.deliver(&d1).await, Ok("a".to_string()));
    // A DIFFERENT context must be tracked independently: First again, not
    // Continuation, even though the adapter already started ctx-a.
    assert_eq!(adapter.deliver(&d2).await, Ok("b".to_string()));

    let calls = handle.calls();
    assert_eq!(
        calls[0].0,
        build_argv("claude", &session_uuid("ctx-a"), Turn::First)
    );
    assert_eq!(
        calls[1].0,
        build_argv("claude", &session_uuid("ctx-b"), Turn::First)
    );
}

// ---------------------------------------------------------------------------
// symmetric one-retry fallback
// ---------------------------------------------------------------------------

#[tokio::test]
async fn fallback_a_already_in_use_retries_once_with_resume_then_marks_started() {
    let d = sample_delivery("ctx-fallback-a");
    let fake = FakeInvoker::new(vec![
        Err(err(
            "claude exit 1: Error: Session ID 00000000-0000-0000-0000-000000000000 is already in use.",
        )),
        Ok(success_json("resumed")),
        Ok(success_json("third")),
    ]);
    let handle = fake.clone();

    let adapter = ClaudeAdapter::new(fake);
    // First deliver: initial attempt (First) errors "is already in use";
    // adapter retries once as Continuation, which succeeds.
    assert_eq!(adapter.deliver(&d).await, Ok("resumed".to_string()));
    // Having marked the context started on that success, a THIRD call goes
    // straight to Continuation (no fallback needed).
    assert_eq!(adapter.deliver(&d).await, Ok("third".to_string()));

    let calls = handle.calls();
    assert_eq!(calls.len(), 3);
    let su = session_uuid(&d.context_id);
    assert_eq!(calls[0].0, build_argv("claude", &su, Turn::First));
    assert_eq!(calls[1].0, build_argv("claude", &su, Turn::Continuation));
    assert_eq!(calls[2].0, build_argv("claude", &su, Turn::Continuation));
}

#[tokio::test]
async fn fallback_b_no_conversation_found_retries_once_with_session_id() {
    let d = sample_delivery("ctx-fallback-b");
    let fake = FakeInvoker::new(vec![
        Ok(success_json("first")),
        Err(err(
            "claude exit 1: No conversation found with session ID: 00000000-0000-0000-0000-000000000000",
        )),
        Ok(success_json("restarted")),
    ]);
    let handle = fake.clone();

    let adapter = ClaudeAdapter::new(fake);
    // Marks the context started.
    assert_eq!(adapter.deliver(&d).await, Ok("first".to_string()));
    // Second deliver: Continuation attempt errors "No conversation found";
    // adapter retries once as First, which succeeds.
    assert_eq!(adapter.deliver(&d).await, Ok("restarted".to_string()));

    let calls = handle.calls();
    assert_eq!(calls.len(), 3);
    let su = session_uuid(&d.context_id);
    assert_eq!(calls[0].0, build_argv("claude", &su, Turn::First));
    assert_eq!(calls[1].0, build_argv("claude", &su, Turn::Continuation));
    assert_eq!(calls[2].0, build_argv("claude", &su, Turn::First));
}

#[tokio::test]
async fn fallback_retry_error_propagates_without_a_second_retry() {
    let d = sample_delivery("ctx-no-double-retry");
    let fake = FakeInvoker::new(vec![
        Err(err(
            "claude exit 1: Error: Session ID ... is already in use.",
        )),
        Err(err("still broken")),
    ]);
    let handle = fake.clone();

    let adapter = ClaudeAdapter::new(fake);
    let out = adapter.deliver(&d).await;
    assert_eq!(out, Err(err("still broken")));

    // Exactly 2 invocations: the original attempt + the one fallback retry.
    // The fallback error does NOT trigger a further retry.
    assert_eq!(handle.calls().len(), 2);
}

#[tokio::test]
async fn unrelated_invoker_error_propagates_without_any_retry() {
    let d = sample_delivery("ctx-unrelated-error");
    let fake = FakeInvoker::new(vec![Err(err("spawn claude: No such file or directory"))]);
    let handle = fake.clone();

    let adapter = ClaudeAdapter::new(fake);
    let out = adapter.deliver(&d).await;
    assert_eq!(out, Err(err("spawn claude: No such file or directory")));

    // An error that matches neither fallback substring must propagate
    // immediately: exactly 1 invocation, no retry attempted.
    assert_eq!(handle.calls().len(), 1);
}

// ---------------------------------------------------------------------------
// with_bin / malformed stdout
// ---------------------------------------------------------------------------

#[tokio::test]
async fn with_bin_overrides_program_name() {
    let d = sample_delivery("ctx-bin");
    let fake = FakeInvoker::new(vec![Ok(success_json("ok"))]);
    let handle = fake.clone();

    let adapter = ClaudeAdapter::with_bin(fake, "claude-cli");
    let out = adapter.deliver(&d).await;
    assert_eq!(out, Ok("ok".to_string()));

    let calls = handle.calls();
    assert_eq!(calls[0].0[0], "claude-cli");
    assert_eq!(
        calls[0].0,
        build_argv("claude-cli", &session_uuid(&d.context_id), Turn::First)
    );
}

#[tokio::test]
async fn malformed_stdout_yields_parse_error() {
    let d = sample_delivery("ctx-malformed");
    let fake = FakeInvoker::new(vec![Ok("not json".to_string())]);

    let adapter = ClaudeAdapter::new(fake);
    let err = adapter.deliver(&d).await.unwrap_err();
    assert!(
        err.message.contains("parse"),
        "message was: {}",
        err.message
    );
}
