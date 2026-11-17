//! `OpencodeAdapter::deliver` wiring, exercised via a FAKE `CliInvoker` defined
//! here. The fake records EVERY `(argv, stdin)` it was called with (in order)
//! and returns a scriptable, per-call `Result` (a FIFO queue), letting us drive
//! and assert the ledger contract from `docs/specs/adapter-opencode.md`:
//! compose -> ledger lookup (First vs. Continuation, per-context) -> invoke
//! (prompt travels in ARGV as the LAST element, stdin is EMPTY — opencode's
//! deliberate seam shape) -> the MATCHED single fresh-First retry
//! (Continuation-only, ONLY when the invoker error contains "Session not
//! found") -> parse -> ledger update (ONLY on full success).
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
use wasl_adapter_opencode::{build_argv, compose_prompt, CliInvoker, OpencodeAdapter, Turn};

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

/// A happy `opencode run --format json` JSONL stream whose events carry
/// sessionID `sid`: a `step_start`, a `text` part carrying `text`, and a
/// `step_finish`.
fn ok_stream(sid: &str, text: &str) -> String {
    format!(
        "{{\"type\":\"step_start\",\"sessionID\":\"{sid}\",\"part\":{{\"messageID\":\"msg_1\",\"type\":\"step-start\"}}}}\n\
{{\"type\":\"text\",\"sessionID\":\"{sid}\",\"part\":{{\"messageID\":\"msg_1\",\"type\":\"text\",\"text\":\"{text}\",\"time\":{{\"start\":1,\"end\":2}}}}}}\n\
{{\"type\":\"step_finish\",\"sessionID\":\"{sid}\",\"part\":{{\"messageID\":\"msg_1\",\"reason\":\"stop\",\"type\":\"step-finish\"}}}}\n"
    )
}

/// A `type=="error"` JSONL stream: invoker succeeds (exit 0), but the turn
/// itself failed, so `parse_reply` must error.
fn error_stream() -> String {
    "{\"type\":\"error\",\"sessionID\":\"ses_parse_fail\",\"error\":{\"name\":\"ContextOverflowError\",\"data\":{\"message\":\"boom\"}}}\n"
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
async fn deliver_first_turn_learns_the_session_id() {
    let d = sample_delivery("ctx-42");
    let fake = FakeInvoker::new(vec![Ok(ok_stream("s1", "all good"))]);
    let handle = fake.clone();

    let adapter = OpencodeAdapter::new(fake);
    let out = adapter.deliver(&d).await;
    assert_eq!(out, Ok("all good".to_string()));
    assert_eq!(adapter.session_of("ctx-42"), Some("s1".to_string()));

    let calls = handle.calls();
    assert_eq!(calls.len(), 1);
    let prompt = compose_prompt(&d);
    assert_eq!(
        calls[0].0,
        build_argv("opencode", None, Turn::First, &prompt)
    );
    // opencode's prompt is the LAST argv element; stdin must be empty.
    assert_eq!(
        calls[0].0.last().map(String::as_str),
        Some(prompt.as_str()),
        "prompt must be the last argv element: {:?}",
        calls[0].0
    );
    assert_eq!(
        calls[0].1, "",
        "the prompt travels in argv (last element), NOT stdin: stdin must be empty"
    );
}

#[tokio::test]
async fn deliver_second_call_same_context_resumes_the_learned_session() {
    let d = sample_delivery("ctx-42");
    let fake = FakeInvoker::new(vec![
        Ok(ok_stream("s1", "first")),
        Ok(ok_stream("s1", "second")),
    ]);
    let handle = fake.clone();

    let adapter = OpencodeAdapter::new(fake);
    assert_eq!(adapter.deliver(&d).await, Ok("first".to_string()));
    assert_eq!(adapter.deliver(&d).await, Ok("second".to_string()));

    let calls = handle.calls();
    assert_eq!(calls.len(), 2);
    let prompt = compose_prompt(&d);
    assert_eq!(
        calls[0].0,
        build_argv("opencode", None, Turn::First, &prompt)
    );
    // The LEARNED id is spliced in after -s, not derived.
    assert_eq!(
        calls[1].0,
        build_argv("opencode", None, Turn::Continuation("s1"), &prompt)
    );
    assert_eq!(calls[0].1, "");
    assert_eq!(calls[1].1, "");
}

#[tokio::test]
async fn deliver_different_context_is_first_turn_again() {
    let d1 = sample_delivery("ctx-a");
    let d2 = sample_delivery("ctx-b");
    let fake = FakeInvoker::new(vec![Ok(ok_stream("sa", "a")), Ok(ok_stream("sb", "b"))]);
    let handle = fake.clone();

    let adapter = OpencodeAdapter::new(fake);
    assert_eq!(adapter.deliver(&d1).await, Ok("a".to_string()));
    // A DIFFERENT context must be tracked independently: First again, not
    // Continuation, even though the adapter already learned ctx-a's session.
    assert_eq!(adapter.deliver(&d2).await, Ok("b".to_string()));

    let calls = handle.calls();
    let prompt1 = compose_prompt(&d1);
    let prompt2 = compose_prompt(&d2);
    assert_eq!(
        calls[0].0,
        build_argv("opencode", None, Turn::First, &prompt1)
    );
    assert_eq!(
        calls[1].0,
        build_argv("opencode", None, Turn::First, &prompt2)
    );
    assert_eq!(adapter.session_of("ctx-a"), Some("sa".to_string()));
    assert_eq!(adapter.session_of("ctx-b"), Some("sb".to_string()));
}

// ---------------------------------------------------------------------------
// MATCHED single fresh-First retry (Continuation-only, "Session not found")
// ---------------------------------------------------------------------------

#[tokio::test]
async fn continuation_session_not_found_retries_once_as_fresh_first_and_relearns() {
    let d = sample_delivery("ctx-fallback");
    let fake = FakeInvoker::new(vec![
        Ok(ok_stream("s1", "first")), // learns s1
        // Continuation(s1) fails with an error CONTAINING "Session not found"
        // (opencode's verified stale-session stderr, wrapped by the invoker).
        Err(err("opencode exit status: 1: Error: Session not found")),
        Ok(ok_stream("s2", "second")), // matched retry as First succeeds, learns s2
        Ok(ok_stream("s2", "third")),  // next deliver resumes s2
    ]);
    let handle = fake.clone();

    let adapter = OpencodeAdapter::new(fake);
    assert_eq!(adapter.deliver(&d).await, Ok("first".to_string()));
    assert_eq!(adapter.deliver(&d).await, Ok("second".to_string()));
    assert_eq!(adapter.session_of("ctx-fallback"), Some("s2".to_string()));

    // A THIRD delivery resumes s2 (the freshly-learned session), not s1.
    assert_eq!(adapter.deliver(&d).await, Ok("third".to_string()));

    let calls = handle.calls();
    assert_eq!(calls.len(), 4);
    let prompt = compose_prompt(&d);
    assert_eq!(
        calls[0].0,
        build_argv("opencode", None, Turn::First, &prompt)
    );
    assert_eq!(
        calls[1].0,
        build_argv("opencode", None, Turn::Continuation("s1"), &prompt)
    );
    // The matched retry is a FRESH First attempt, not Continuation("s1") again.
    assert_eq!(
        calls[2].0,
        build_argv("opencode", None, Turn::First, &prompt)
    );
    assert_eq!(
        calls[3].0,
        build_argv("opencode", None, Turn::Continuation("s2"), &prompt)
    );
    for call in &calls {
        assert_eq!(call.1, "", "stdin must always be empty for opencode");
    }
}

#[tokio::test]
async fn continuation_non_matching_error_propagates_without_retry() {
    let d = sample_delivery("ctx-env-error");
    let fake = FakeInvoker::new(vec![
        Ok(ok_stream("s1", "first")), // learns s1
        // Continuation(s1) fails with a NON-matching (environmental) error:
        // no "Session not found" substring -> propagate, do NOT retry.
        Err(err("spawn opencode: No such file or directory")),
    ]);
    let handle = fake.clone();

    let adapter = OpencodeAdapter::new(fake);
    assert_eq!(adapter.deliver(&d).await, Ok("first".to_string()));
    let out = adapter.deliver(&d).await;
    assert_eq!(out, Err(err("spawn opencode: No such file or directory")));

    // EXACTLY 1 invocation for the failing deliver (the Continuation attempt);
    // no matched retry, because the error is environmental, not a stale session.
    assert_eq!(handle.calls().len(), 2);
    // The ledger is UNCHANGED on failure: still bound to the original s1.
    assert_eq!(adapter.session_of("ctx-env-error"), Some("s1".to_string()));
}

#[tokio::test]
async fn continuation_session_not_found_then_retry_error_propagates_without_a_second_retry() {
    let d = sample_delivery("ctx-no-double-retry");
    let fake = FakeInvoker::new(vec![
        Ok(ok_stream("s1", "first")), // learns s1
        Err(err("opencode exit status: 1: Error: Session not found")), // Continuation(s1) fails, matched
        Err(err("still broken")), // fresh-First retry ALSO fails
    ]);
    let handle = fake.clone();

    let adapter = OpencodeAdapter::new(fake);
    assert_eq!(adapter.deliver(&d).await, Ok("first".to_string()));
    let out = adapter.deliver(&d).await;
    // The RETRY's own error propagates (a First failure has nothing to fall
    // back to).
    assert_eq!(out, Err(err("still broken")));

    // Exactly 2 invocations for the failing deliver: the Continuation attempt
    // + the one matched retry. The retry's own error does NOT trigger a further
    // retry.
    assert_eq!(handle.calls().len(), 3);
    // The ledger is UNCHANGED on failure: still bound to the original s1.
    assert_eq!(
        adapter.session_of("ctx-no-double-retry"),
        Some("s1".to_string())
    );
}

#[tokio::test]
async fn first_turn_invoker_error_propagates_without_any_retry() {
    let d = sample_delivery("ctx-fresh-error");
    let fake = FakeInvoker::new(vec![
        Err(err("spawn opencode: No such file or directory")),
        Ok(ok_stream("s9", "ok now")),
    ]);
    let handle = fake.clone();

    let adapter = OpencodeAdapter::new(fake);
    let out = adapter.deliver(&d).await;
    assert_eq!(out, Err(err("spawn opencode: No such file or directory")));

    // First-turn errors have nothing to fall back to (opencode mints ids):
    // exactly 1 invocation, no retry.
    assert_eq!(handle.calls().len(), 1);
    assert_eq!(adapter.session_of("ctx-fresh-error"), None);

    // The next delivery for this SAME context is STILL a First attempt
    // (nothing was ever learned).
    assert_eq!(adapter.deliver(&d).await, Ok("ok now".to_string()));
    let calls = handle.calls();
    assert_eq!(calls.len(), 2);
    let prompt = compose_prompt(&d);
    assert_eq!(
        calls[1].0,
        build_argv("opencode", None, Turn::First, &prompt)
    );
    assert_eq!(
        adapter.session_of("ctx-fresh-error"),
        Some("s9".to_string())
    );
}

// ---------------------------------------------------------------------------
// parse failure never triggers a retry and never touches the ledger
// ---------------------------------------------------------------------------

#[tokio::test]
async fn parse_failure_on_first_turn_does_not_retry_or_touch_ledger() {
    let d = sample_delivery("ctx-parse-fail-first");
    // Invoker succeeds (exit 0) but returns a `type=="error"` stream: the turn
    // is a parse failure, not an invoker failure.
    let fake = FakeInvoker::new(vec![Ok(error_stream())]);
    let handle = fake.clone();

    let adapter = OpencodeAdapter::new(fake);
    let err_out = adapter.deliver(&d).await.unwrap_err();
    assert!(
        err_out.message.contains("ContextOverflowError"),
        "message was: {}",
        err_out.message
    );

    // The invoker succeeded (exit 0); ONLY parsing failed. No retry: exactly
    // 1 invocation, and the ledger is untouched (nothing was ever learned).
    assert_eq!(handle.calls().len(), 1);
    assert_eq!(adapter.session_of("ctx-parse-fail-first"), None);
}

// ---------------------------------------------------------------------------
// with_bin / with_model / malformed stdout
// ---------------------------------------------------------------------------

#[tokio::test]
async fn with_bin_overrides_program_name() {
    let d = sample_delivery("ctx-bin");
    let fake = FakeInvoker::new(vec![Ok(ok_stream("s1", "ok"))]);
    let handle = fake.clone();

    let adapter = OpencodeAdapter::with_bin(fake, "oc");
    let out = adapter.deliver(&d).await;
    assert_eq!(out, Ok("ok".to_string()));

    let calls = handle.calls();
    assert_eq!(calls[0].0[0], "oc");
    let prompt = compose_prompt(&d);
    assert_eq!(calls[0].0, build_argv("oc", None, Turn::First, &prompt));
    assert_eq!(calls[0].1, "");
}

#[tokio::test]
async fn with_model_passes_dash_m_on_first_and_continuation() {
    let d = sample_delivery("ctx-model");
    let fake = FakeInvoker::new(vec![
        Ok(ok_stream("s1", "first")),
        Ok(ok_stream("s1", "second")),
    ]);
    let handle = fake.clone();

    let adapter = OpencodeAdapter::with_model(fake, "opencode", "amd-anthropic/Claude-Haiku-4.5");
    assert_eq!(adapter.deliver(&d).await, Ok("first".to_string()));
    assert_eq!(adapter.deliver(&d).await, Ok("second".to_string()));

    let calls = handle.calls();
    let prompt = compose_prompt(&d);
    assert_eq!(
        calls[0].0,
        build_argv(
            "opencode",
            Some("amd-anthropic/Claude-Haiku-4.5"),
            Turn::First,
            &prompt
        )
    );
    assert_eq!(
        calls[1].0,
        build_argv(
            "opencode",
            Some("amd-anthropic/Claude-Haiku-4.5"),
            Turn::Continuation("s1"),
            &prompt
        )
    );
    // `-m <model>` present on BOTH First and Continuation calls.
    for call in &calls {
        let m_pos = call
            .0
            .iter()
            .position(|a| a == "-m")
            .unwrap_or_else(|| panic!("-m must be present in every call: {:?}", call.0));
        assert_eq!(
            call.0.get(m_pos + 1).map(String::as_str),
            Some("amd-anthropic/Claude-Haiku-4.5"),
            "-m must be immediately followed by the model: {:?}",
            call.0
        );
    }
}

#[tokio::test]
async fn malformed_stdout_yields_parse_error() {
    let d = sample_delivery("ctx-malformed");
    let fake = FakeInvoker::new(vec![Ok("not json".to_string())]);

    let adapter = OpencodeAdapter::new(fake);
    let err_out = adapter.deliver(&d).await.unwrap_err();
    assert!(
        err_out.message.contains("parse opencode output line:"),
        "message was: {}",
        err_out.message
    );
    assert_eq!(adapter.session_of("ctx-malformed"), None);
}
