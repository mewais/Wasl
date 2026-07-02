//! `ClaudeAdapter::deliver` wiring, exercised via a FAKE `CliInvoker` defined
//! here. The fake records the `(argv, stdin)` it was called with and returns a
//! scripted `Result`, letting us assert compose -> build_argv -> invoke -> parse.
//!
//! NOTE: `async-trait` is NOT a dev-dependency of this crate, so we cannot use the
//! `#[async_trait]` macro in tests. Instead we implement `CliInvoker` by hand,
//! matching the `#[async_trait::async_trait]` desugaring of the trait method
//! (a `Pin<Box<dyn Future + Send + 'async_trait>>` return).
//!
//! The adapter takes ownership of its invoker, so the fake keeps its recording
//! state behind `Arc`s and is `Clone`: we clone a handle, move one copy into the
//! adapter, and inspect the retained copy after the call.

use std::sync::{Arc, Mutex};

use wasl_adapter::{AdapterError, AgentAdapter, Delivery, DeliveryKind};
use wasl_adapter_claude::{build_argv, compose_prompt, ClaudeAdapter, CliInvoker};

/// Fake invoker: records the last call and returns a pre-scripted result.
#[derive(Clone)]
struct FakeInvoker {
    calls: Arc<Mutex<Option<(Vec<String>, String)>>>,
    reply: Arc<Result<String, AdapterError>>,
}

impl FakeInvoker {
    fn new(reply: Result<String, AdapterError>) -> Self {
        FakeInvoker {
            calls: Arc::new(Mutex::new(None)),
            reply: Arc::new(reply),
        }
    }

    fn last_call(&self) -> (Vec<String>, String) {
        self.calls
            .lock()
            .unwrap()
            .clone()
            .expect("invoker was never called")
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
        // Record the call synchronously (owned copies), then return the script.
        *self.calls.lock().unwrap() = Some((argv.to_vec(), stdin.to_string()));
        let result = (*self.reply).clone();
        Box::pin(async move { result })
    }
}

fn sample_delivery() -> Delivery {
    Delivery {
        context_id: "ctx-42".to_string(),
        from: "alice".to_string(),
        kind: DeliveryKind::Consult,
        text: "what is up?".to_string(),
        data: vec![],
    }
}

#[tokio::test]
async fn deliver_happy_path_wires_compose_argv_and_parses() {
    let d = sample_delivery();
    let fake = FakeInvoker::new(Ok(
        r#"{"type":"result","is_error":false,"result":"all good"}"#.to_string(),
    ));
    let handle = fake.clone();

    let adapter = ClaudeAdapter::new(fake);
    let out = adapter.deliver(&d).await;
    assert_eq!(out, Ok("all good".to_string()));

    let (argv_seen, stdin_seen) = handle.last_call();
    assert_eq!(argv_seen, build_argv("claude", &d.context_id));
    assert_eq!(stdin_seen, compose_prompt(&d));
}

#[tokio::test]
async fn with_bin_overrides_program_name() {
    let d = sample_delivery();
    let fake = FakeInvoker::new(Ok(
        r#"{"type":"result","is_error":false,"result":"ok"}"#.to_string()
    ));
    let handle = fake.clone();

    let adapter = ClaudeAdapter::with_bin(fake, "claude-cli");
    let out = adapter.deliver(&d).await;
    assert_eq!(out, Ok("ok".to_string()));

    let (argv_seen, _stdin) = handle.last_call();
    assert_eq!(argv_seen[0], "claude-cli");
    assert_eq!(argv_seen, build_argv("claude-cli", &d.context_id));
}

#[tokio::test]
async fn invoker_error_propagates() {
    let d = sample_delivery();
    let fake = FakeInvoker::new(Err(AdapterError {
        message: "nope".to_string(),
    }));

    let adapter = ClaudeAdapter::new(fake);
    let out = adapter.deliver(&d).await;
    assert_eq!(
        out,
        Err(AdapterError {
            message: "nope".to_string()
        })
    );
}

#[tokio::test]
async fn malformed_stdout_yields_parse_error() {
    let d = sample_delivery();
    let fake = FakeInvoker::new(Ok("not json".to_string()));

    let adapter = ClaudeAdapter::new(fake);
    let err = adapter.deliver(&d).await.unwrap_err();
    assert!(
        err.message.contains("parse"),
        "message was: {}",
        err.message
    );
}
