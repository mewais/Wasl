//! Tests that `WaslManagedRuntimeProvider::target_for_with_context` dispatches
//! to the driver's `resume(native, opts)` when `context.resume` is `Some(id)`,
//! and to `spawn(opts)` when it is `None`.
//!
//! # Design
//!
//! We use a `DispatchRecordingProtocol` that separately logs every `spawn` and
//! every `resume` call.  This lets us assert that the EXACT right method was
//! called, not just that A method was called.
//!
//! # Spec references
//!
//! - `SpawnContext.resume` — the new field (docs in struct definition).
//! - `WaslManagedRuntimeProvider::create_target` — the dispatch site in
//!   `src/provider.rs`.
//! - `InteractiveProtocol::resume` — `src/lib.rs` §6.

mod support;

use std::sync::{Arc, Mutex};

use wasl_runtime::{
    ComposedPrompt, InteractiveProtocol, InteractiveTransport, ManagedConfig, MonotonicClock,
    NativeSessionId, ProtocolEvent, RuntimeCapabilities, RuntimeError, SpawnContext, SpawnOptions,
    TargetKey, WaslManagedRuntimeProvider,
};

use support::{sink_arc, RecordingSink};

// -------------------------------------------------------------------
// NullTransport — trivial transport: next_event returns None immediately.
// The actor moves to Faulted/Stopped.  The test only cares which
// InteractiveProtocol method was called, not that turns can be submitted.
// -------------------------------------------------------------------

struct NullTransport;

#[async_trait::async_trait]
impl InteractiveTransport for NullTransport {
    async fn send_turn(&mut self, _prompt: &ComposedPrompt) -> Result<(), RuntimeError> {
        Ok(())
    }

    async fn next_event(&mut self) -> Option<ProtocolEvent> {
        None
    }

    async fn interrupt(&mut self) -> Result<(), RuntimeError> {
        Ok(())
    }
}

// -------------------------------------------------------------------
// DispatchRecord — one log entry per call.
// -------------------------------------------------------------------

#[derive(Debug, Clone, PartialEq, Eq)]
enum DispatchCall {
    Spawn,
    Resume { native: String },
}

// -------------------------------------------------------------------
// DispatchRecordingProtocol — records spawn vs resume separately.
// -------------------------------------------------------------------

struct DispatchRecordingProtocol {
    calls: Arc<Mutex<Vec<DispatchCall>>>,
}

impl DispatchRecordingProtocol {
    fn new() -> (Arc<Self>, Arc<Mutex<Vec<DispatchCall>>>) {
        let calls = Arc::new(Mutex::new(Vec::new()));
        let proto = Arc::new(Self {
            calls: calls.clone(),
        });
        (proto, calls)
    }

    fn calls_snapshot(calls: &Arc<Mutex<Vec<DispatchCall>>>) -> Vec<DispatchCall> {
        calls.lock().unwrap().clone()
    }
}

#[async_trait::async_trait]
impl InteractiveProtocol for DispatchRecordingProtocol {
    fn capabilities(&self) -> RuntimeCapabilities {
        RuntimeCapabilities {
            persistent_process: false,
            persistent_conversation: false,
            structured_turns: false,
            streaming_output: false,
            attachable: false,
            interruptible: false,
            structured_provenance: false,
            resumable: true,
        }
    }

    async fn spawn(
        &self,
        _opts: &SpawnOptions,
    ) -> Result<Box<dyn InteractiveTransport>, RuntimeError> {
        self.calls.lock().unwrap().push(DispatchCall::Spawn);
        Ok(Box::new(NullTransport))
    }

    async fn resume(
        &self,
        native: &NativeSessionId,
        _opts: &SpawnOptions,
    ) -> Result<Box<dyn InteractiveTransport>, RuntimeError> {
        self.calls.lock().unwrap().push(DispatchCall::Resume {
            native: native.0.clone(),
        });
        Ok(Box::new(NullTransport))
    }
}

// -------------------------------------------------------------------
// Helper: build a WaslManagedRuntimeProvider over the dispatch recorder.
// -------------------------------------------------------------------

fn make_provider(
    proto: Arc<DispatchRecordingProtocol>,
) -> WaslManagedRuntimeProvider<Arc<DispatchRecordingProtocol>> {
    WaslManagedRuntimeProvider::with_seams(
        proto,
        ManagedConfig::default(),
        Arc::new(MonotonicClock),
        sink_arc(RecordingSink::default()),
    )
}

fn key(s: &str) -> TargetKey {
    TargetKey(s.to_string())
}

// ===================================================================
// 1. No resume field (None) → spawn() is called.
// ===================================================================

#[tokio::test]
async fn no_resume_in_context_calls_spawn() {
    let (proto, calls) = DispatchRecordingProtocol::new();
    let prov = make_provider(proto);

    let ctx = SpawnContext {
        resume: None,
        ..SpawnContext::default()
    };

    prov.target_for_with_context(&key("ctx-spawn-path"), ctx)
        .await
        .expect("target must be created");

    let recorded = DispatchRecordingProtocol::calls_snapshot(&calls);
    assert_eq!(
        recorded,
        vec![DispatchCall::Spawn],
        "context.resume = None must call the driver's spawn(), not resume(); got: {recorded:?}"
    );
}

// ===================================================================
// 2. resume = Some(id) → resume(id, opts) is called (not spawn).
// ===================================================================

#[tokio::test]
async fn resume_some_in_context_calls_resume_with_the_native_id() {
    let (proto, calls) = DispatchRecordingProtocol::new();
    let prov = make_provider(proto);

    let native_id = "ses_0123456789abcdef".to_string();
    let ctx = SpawnContext {
        resume: Some(native_id.clone()),
        ..SpawnContext::default()
    };

    prov.target_for_with_context(&key("ctx-resume-path"), ctx)
        .await
        .expect("target must be created even when resuming");

    let recorded = DispatchRecordingProtocol::calls_snapshot(&calls);
    assert_eq!(
        recorded,
        vec![DispatchCall::Resume {
            native: native_id.clone(),
        }],
        "context.resume = Some(\"{native_id}\") must call the driver's resume(NativeSessionId(id), \
         ...) and NOT spawn(); got: {recorded:?}"
    );
}

// ===================================================================
// 3. resume = Some with extra context fields (model, cwd) — the opts
//    still carry the remaining context into the resume call.
// ===================================================================

#[tokio::test]
async fn resume_some_passes_other_context_fields_to_driver() {
    // We use a version of the recorder that also captures the SpawnContext seen
    // on resume, so we can assert the model/cwd etc. are NOT stripped.
    struct ContextCapturingProtocol {
        calls: Arc<Mutex<Vec<(DispatchCall, SpawnContext)>>>,
    }

    impl ContextCapturingProtocol {
        fn new() -> (Arc<Self>, Arc<Mutex<Vec<(DispatchCall, SpawnContext)>>>) {
            let calls = Arc::new(Mutex::new(Vec::new()));
            let proto = Arc::new(Self {
                calls: calls.clone(),
            });
            (proto, calls)
        }
    }

    #[async_trait::async_trait]
    impl InteractiveProtocol for ContextCapturingProtocol {
        fn capabilities(&self) -> RuntimeCapabilities {
            RuntimeCapabilities {
                resumable: true,
                ..RuntimeCapabilities {
                    persistent_process: false,
                    persistent_conversation: false,
                    structured_turns: false,
                    streaming_output: false,
                    attachable: false,
                    interruptible: false,
                    structured_provenance: false,
                    resumable: false,
                }
            }
        }

        async fn spawn(
            &self,
            opts: &SpawnOptions,
        ) -> Result<Box<dyn InteractiveTransport>, RuntimeError> {
            self.calls
                .lock()
                .unwrap()
                .push((DispatchCall::Spawn, opts.context().clone()));
            Ok(Box::new(NullTransport))
        }

        async fn resume(
            &self,
            native: &NativeSessionId,
            opts: &SpawnOptions,
        ) -> Result<Box<dyn InteractiveTransport>, RuntimeError> {
            self.calls.lock().unwrap().push((
                DispatchCall::Resume {
                    native: native.0.clone(),
                },
                opts.context().clone(),
            ));
            Ok(Box::new(NullTransport))
        }
    }

    let (proto, calls) = ContextCapturingProtocol::new();
    let prov = WaslManagedRuntimeProvider::with_seams(
        proto,
        ManagedConfig::default(),
        Arc::new(MonotonicClock),
        sink_arc(RecordingSink::default()),
    );

    let ctx = SpawnContext {
        resume: Some("ses_abc".to_string()),
        model: Some("claude-opus-4-5".to_string()),
        cwd: Some("/my/project".to_string()),
        ..SpawnContext::default()
    };

    prov.target_for_with_context(&key("ctx-resume-ctx"), ctx.clone())
        .await
        .expect("target must succeed");

    let recorded = calls.lock().unwrap().clone();
    assert_eq!(recorded.len(), 1, "exactly one call must occur");
    let (dispatch, seen_ctx) = &recorded[0];

    assert_eq!(
        *dispatch,
        DispatchCall::Resume {
            native: "ses_abc".to_string()
        },
        "resume must be dispatched, not spawn; got: {dispatch:?}"
    );
    assert_eq!(
        seen_ctx.model,
        Some("claude-opus-4-5".to_string()),
        "model must be threaded into the opts for the resume call"
    );
    assert_eq!(
        seen_ctx.cwd,
        Some("/my/project".to_string()),
        "cwd must be threaded into the opts for the resume call"
    );
}

// ===================================================================
// 4. get-or-create idempotency: a second call with the SAME key and a
//    resume id does NOT call resume again (returns the cached target).
// ===================================================================

#[tokio::test]
async fn idempotency_prevents_second_resume_call() {
    let (proto, calls) = DispatchRecordingProtocol::new();
    let prov = make_provider(proto);

    let ctx = SpawnContext {
        resume: Some("ses_idem".to_string()),
        ..SpawnContext::default()
    };

    // First call: creates the target via resume.
    let t1 = prov
        .target_for_with_context(&key("ctx-idem-resume"), ctx.clone())
        .await
        .expect("first call must succeed");

    // Second call with same key: must return the cached target WITHOUT calling
    // resume (or spawn) again.
    let t2 = prov
        .target_for_with_context(&key("ctx-idem-resume"), ctx.clone())
        .await
        .expect("second call must succeed");

    assert_eq!(
        t1.target_id(),
        t2.target_id(),
        "same key must return the same cached target"
    );

    let recorded = DispatchRecordingProtocol::calls_snapshot(&calls);
    assert_eq!(
        recorded.len(),
        1,
        "resume must be called exactly ONCE for a key — the second call must hit the \
         get-or-create cache; got {recorded:?}"
    );
}
