//! Tests that `RuntimeProvider::target_for_with_context` threads the
//! [`wasl_runtime::SpawnContext`] through to the driver's `spawn` call, and
//! that the DEFAULT trait impl delegates to `target_for` (context ignored).
//!
//! # Test doubles built here
//!
//! * `RecordingProtocol` — a minimal `InteractiveProtocol` that records the
//!   `SpawnOptions` (specifically `context()`) it saw on `spawn`.
//! * `NullTransport` — a trivial `InteractiveTransport` that does nothing
//!   (no events, no turns): enough for the actor to start without panicking.
//!
//! # What is expected RED
//!
//! * `WaslManagedRuntimeProvider::target_for_with_context` currently calls
//!   `create_target(key, context)` which calls `self.protocol.spawn(&opts)`.
//!   The `opts` passed contain `context`.  The test asserts the recorder saw
//!   exactly the context we passed — which will only work once the
//!   `todo!()` bodies in `routing.rs` are filled in.  The managed provider
//!   itself is NOT `todo!()`, so the managed-provider recorder test should
//!   pass at compilation but the routing-provider related tests will panic.
//!
//! The suite is also expected to FAIL on the `ProtocolRoutingProvider` tests
//! because `routing.rs` has `todo!()` in all method bodies.

mod support;

use std::collections::BTreeMap;
use std::sync::{Arc, Mutex};

use wasl_runtime::{
    BusyPolicy, CancelReason, ComposedPrompt, DeliveryTarget, EventSeq, InteractiveProtocol,
    InteractiveTransport, ManagedConfig, MonotonicClock, NativeGateResolver, NativeSessionId,
    ProtocolEvent, ProtocolRoutingProvider, RuntimeCapabilities, RuntimeError, RuntimeProvider,
    ShutdownMode, SpawnContext, SpawnOptions, TargetId, TargetKey, TargetStatus, ToolCallAnswerer,
    TurnEventStream, TurnId, TurnReceipt, TurnRequest, WaslManagedRuntimeProvider,
};

use support::{sink_arc, RecordingSink};

// ------------------------------------------------------------------
// NullTransport — does nothing; `next_event` returns None immediately
// so the actor sees process-exited and moves to Faulted/Stopped.
// We only need the transport to exist so spawn() can return it.
// ------------------------------------------------------------------

struct NullTransport;

#[async_trait::async_trait]
impl InteractiveTransport for NullTransport {
    async fn send_turn(&mut self, _prompt: &ComposedPrompt) -> Result<(), RuntimeError> {
        Ok(())
    }

    async fn next_event(&mut self) -> Option<ProtocolEvent> {
        // Return None immediately → ProcessExited will be synthesized by the
        // actor; that moves the target to Faulted.  This is fine for our tests
        // which only care that spawn() was called with the right context, not
        // that turns can be submitted.
        None
    }

    async fn interrupt(&mut self) -> Result<(), RuntimeError> {
        Ok(())
    }
}

// ------------------------------------------------------------------
// RecordingProtocol — captures `SpawnOptions` on every `spawn` call.
// ------------------------------------------------------------------

/// The last `SpawnOptions` seen by `RecordingProtocol::spawn`.  The whole
/// `SpawnOptions` is cloned (it is `Clone`) so the test can inspect both the
/// `context()` and `target_key()`.
#[derive(Default)]
struct SpawnRecord {
    options: Vec<SpawnOptions>,
}

struct RecordingProtocol {
    record: Arc<Mutex<SpawnRecord>>,
}

impl RecordingProtocol {
    fn new() -> (Arc<Self>, Arc<Mutex<SpawnRecord>>) {
        let record = Arc::new(Mutex::new(SpawnRecord::default()));
        let proto = Arc::new(RecordingProtocol {
            record: record.clone(),
        });
        (proto, record)
    }

    /// The `SpawnContext` seen on the most recent `spawn` call, if any.
    fn last_context(record: &Arc<Mutex<SpawnRecord>>) -> Option<SpawnContext> {
        record
            .lock()
            .unwrap()
            .options
            .last()
            .map(|o| o.context().clone())
    }

    /// How many times `spawn` was called.
    fn spawn_count(record: &Arc<Mutex<SpawnRecord>>) -> usize {
        record.lock().unwrap().options.len()
    }

    fn all_contexts(record: &Arc<Mutex<SpawnRecord>>) -> Vec<SpawnContext> {
        record
            .lock()
            .unwrap()
            .options
            .iter()
            .map(|o| o.context().clone())
            .collect()
    }
}

#[async_trait::async_trait]
impl InteractiveProtocol for RecordingProtocol {
    fn capabilities(&self) -> RuntimeCapabilities {
        RuntimeCapabilities {
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

    async fn spawn(
        &self,
        opts: &SpawnOptions,
    ) -> Result<Box<dyn InteractiveTransport>, RuntimeError> {
        self.record.lock().unwrap().options.push(opts.clone());
        Ok(Box::new(NullTransport))
    }

    async fn resume(
        &self,
        _native: &NativeSessionId,
        opts: &SpawnOptions,
    ) -> Result<Box<dyn InteractiveTransport>, RuntimeError> {
        self.spawn(opts).await
    }
}

// ------------------------------------------------------------------
// Helper: build a WaslManagedRuntimeProvider with the recording driver.
// ------------------------------------------------------------------

fn make_provider(
    proto: Arc<RecordingProtocol>,
) -> WaslManagedRuntimeProvider<Arc<RecordingProtocol>> {
    let config = ManagedConfig::default();
    let clock = Arc::new(MonotonicClock);
    let sink = sink_arc(RecordingSink::default());
    WaslManagedRuntimeProvider::with_seams(proto, config, clock, sink)
}

fn key(s: &str) -> TargetKey {
    TargetKey(s.to_string())
}

// ==================================================================
// 1. target_for_with_context threads the context to spawn.
// ==================================================================

#[tokio::test]
async fn target_for_with_context_threads_context_to_driver_spawn() {
    let (proto, record) = RecordingProtocol::new();
    let prov = make_provider(proto);

    let mut env = BTreeMap::new();
    env.insert("MY_VAR".to_string(), "hello".to_string());

    let ctx = SpawnContext {
        protocol: Some("claude".to_string()),
        cwd: Some("/my/project".to_string()),
        model: Some("claude-opus-4-5".to_string()),
        posture: Some("bypassPermissions".to_string()),
        env: env.clone(),
        setup: Some("npm install".to_string()),
        bin: Some("/usr/local/bin/claude".to_string()),
        disallowed_tools: vec!["Bash".to_string()],
        mcp_config: None,
    };

    let _target = prov
        .target_for_with_context(&key("ctx-recorder"), ctx.clone())
        .await
        .expect("target_for_with_context must succeed");

    assert_eq!(
        RecordingProtocol::spawn_count(&record),
        1,
        "spawn must be called exactly once for a new key"
    );

    let seen = RecordingProtocol::last_context(&record)
        .expect("spawn must have been called and context recorded");

    assert_eq!(
        seen.protocol, ctx.protocol,
        "protocol must be threaded through to driver spawn"
    );
    assert_eq!(
        seen.cwd, ctx.cwd,
        "cwd must be threaded through to driver spawn"
    );
    assert_eq!(
        seen.model, ctx.model,
        "model must be threaded through to driver spawn"
    );
    assert_eq!(
        seen.posture, ctx.posture,
        "posture must be threaded through to driver spawn"
    );
    assert_eq!(
        seen.env, ctx.env,
        "env must be threaded through to driver spawn"
    );
    assert_eq!(
        seen.setup, ctx.setup,
        "setup must be threaded through to driver spawn"
    );
    assert_eq!(
        seen.bin, ctx.bin,
        "bin must be threaded through to driver spawn"
    );
    assert_eq!(
        seen.disallowed_tools, ctx.disallowed_tools,
        "disallowed_tools must be threaded through to driver spawn"
    );
}

// ==================================================================
// 2. Legacy target_for (no context) yields SpawnContext::default().
// ==================================================================

#[tokio::test]
async fn legacy_target_for_yields_default_spawn_context() {
    let (proto, record) = RecordingProtocol::new();
    let prov = make_provider(proto);

    let _target = prov
        .target_for(&key("ctx-legacy"))
        .await
        .expect("target_for must succeed");

    assert_eq!(
        RecordingProtocol::spawn_count(&record),
        1,
        "spawn must be called exactly once"
    );

    let seen = RecordingProtocol::last_context(&record)
        .expect("spawn must have been called");

    assert_eq!(
        seen,
        SpawnContext::default(),
        "target_for (no context) must pass SpawnContext::default() to the driver; got {seen:?}"
    );
}

// ==================================================================
// 3. get-or-create is idempotent per key with context — second call
//    with the SAME key does NOT call spawn again.
// ==================================================================

#[tokio::test]
async fn target_for_with_context_is_idempotent_per_key() {
    let (proto, record) = RecordingProtocol::new();
    let prov = make_provider(proto);

    let ctx_a = SpawnContext {
        cwd: Some("/project-a".to_string()),
        ..SpawnContext::default()
    };
    let ctx_b = SpawnContext {
        cwd: Some("/project-b".to_string()),
        ..SpawnContext::default()
    };

    // First call: creates the target (spawns the driver).
    let t1 = prov
        .target_for_with_context(&key("ctx-idem"), ctx_a.clone())
        .await
        .expect("first target_for_with_context must succeed");

    // Second call with the SAME key (but different context): must return the
    // existing target WITHOUT spawning again.
    let t2 = prov
        .target_for_with_context(&key("ctx-idem"), ctx_b.clone())
        .await
        .expect("second target_for_with_context must succeed");

    assert_eq!(
        t1.target_id(),
        t2.target_id(),
        "same key must return the same target (get-or-create is idempotent per key)"
    );

    assert_eq!(
        RecordingProtocol::spawn_count(&record),
        1,
        "spawn must be called exactly once for the same key — the second call \
         must return the cached target without re-spawning"
    );
}

// ==================================================================
// 4. Different keys each get their own spawn call with their own context.
// ==================================================================

#[tokio::test]
async fn different_keys_each_spawn_with_their_own_context() {
    let (proto, record) = RecordingProtocol::new();
    let prov = make_provider(proto);

    let ctx_x = SpawnContext {
        model: Some("model-x".to_string()),
        ..SpawnContext::default()
    };
    let ctx_y = SpawnContext {
        model: Some("model-y".to_string()),
        ..SpawnContext::default()
    };

    let _tx = prov
        .target_for_with_context(&key("ctx-x"), ctx_x.clone())
        .await
        .expect("ctx-x must succeed");

    let _ty = prov
        .target_for_with_context(&key("ctx-y"), ctx_y.clone())
        .await
        .expect("ctx-y must succeed");

    assert_eq!(
        RecordingProtocol::spawn_count(&record),
        2,
        "two distinct keys must each produce one spawn call"
    );

    let contexts = RecordingProtocol::all_contexts(&record);
    assert_eq!(contexts.len(), 2);

    // Both model values must appear (order may differ — each was a separate spawn).
    let models: Vec<Option<String>> = contexts.into_iter().map(|c| c.model).collect();
    assert!(
        models.contains(&Some("model-x".to_string())),
        "ctx-x must have spawned with model-x; got {models:?}"
    );
    assert!(
        models.contains(&Some("model-y".to_string())),
        "ctx-y must have spawned with model-y; got {models:?}"
    );
}

// ==================================================================
// 5. DEFAULT RuntimeProvider trait method delegates to target_for
//    (context-ignoring providers use the default impl).
//
//    We prove this using the trait's DEFAULT impl directly: construct a
//    tiny inline provider that ONLY implements `target_for` / `list_targets`,
//    address it as `&dyn RuntimeProvider`, call `target_for_with_context`,
//    and assert that (a) it succeeds and (b) the context is discarded (the
//    same target that `target_for` would return comes back).
// ==================================================================

/// A minimal context-ignoring provider: always returns a fresh ephemeral
/// target, ignores the key. Used only to prove the default-impl delegation.
struct MinimalProvider {
    inner: wasl_runtime::EphemeralRuntimeProvider<FakeAdapter>,
}

#[derive(Default)]
struct FakeAdapter;

#[async_trait::async_trait]
impl wasl_adapter::AgentAdapter for FakeAdapter {
    async fn deliver(
        &self,
        _d: &wasl_adapter::Delivery,
    ) -> Result<String, wasl_adapter::AdapterError> {
        Ok("ok".to_string())
    }
}

#[async_trait::async_trait]
impl RuntimeProvider for MinimalProvider {
    async fn target_for(
        &self,
        key: &TargetKey,
    ) -> Result<Arc<dyn wasl_runtime::DeliveryTarget>, RuntimeError> {
        self.inner.target_for(key).await
    }

    async fn list_targets(&self) -> Vec<wasl_runtime::TargetId> {
        self.inner.list_targets().await
    }

    // target_for_with_context is NOT overridden → uses the default impl which
    // calls target_for(key) and ignores the context.
}

#[tokio::test]
async fn default_trait_impl_delegates_to_target_for_ignoring_context() {
    let prov = MinimalProvider {
        inner: wasl_runtime::EphemeralRuntimeProvider::new(FakeAdapter),
    };

    let non_default_ctx = SpawnContext {
        model: Some("ignored-model".to_string()),
        cwd: Some("/ignored".to_string()),
        ..SpawnContext::default()
    };

    // Call through the default impl (target_for_with_context on the trait).
    let dyn_prov: &dyn RuntimeProvider = &prov;
    let target = dyn_prov
        .target_for_with_context(&key("ctx-default-impl"), non_default_ctx)
        .await
        .expect("default impl must succeed (it delegates to target_for)");

    // The returned target's id equals the key (EphemeralRuntimeProvider does this).
    assert_eq!(
        target.target_id().0,
        "ctx-default-impl",
        "default impl must return the same target as target_for would (the context is ignored)"
    );

    // Also confirm a direct target_for call gives a target with the same id shape.
    let direct = dyn_prov
        .target_for(&key("ctx-default-impl-2"))
        .await
        .expect("target_for must succeed");
    assert_eq!(
        direct.target_id().0,
        "ctx-default-impl-2",
        "target_for returns a target whose id matches the key"
    );
}

use wasl_adapter;

// ==================================================================
// 6. ProtocolRoutingProvider overrides target_for_with_context and threads
//    the context's `protocol` field to the matching sub-provider's spawn.
//    These tests are RED because ProtocolRoutingProvider::new is todo!().
// ==================================================================

/// Build a StubRecordingProvider whose targets encode the label in their id.
/// Used to verify which sub-provider the routing provider delegates to.
struct RoutingRecordingProvider {
    label: String,
    record: Arc<Mutex<Vec<SpawnContext>>>,
}

impl RoutingRecordingProvider {
    fn new(label: &str) -> (Arc<Self>, Arc<Mutex<Vec<SpawnContext>>>) {
        let record = Arc::new(Mutex::new(Vec::new()));
        let p = Arc::new(RoutingRecordingProvider {
            label: label.to_string(),
            record: record.clone(),
        });
        (p, record)
    }

    fn contexts_seen(record: &Arc<Mutex<Vec<SpawnContext>>>) -> Vec<SpawnContext> {
        record.lock().unwrap().clone()
    }
}

// A distinguishable target whose id encodes the label and key.
#[derive(Debug)]
struct LabeledTarget(TargetId);

#[async_trait::async_trait]
impl DeliveryTarget for LabeledTarget {
    fn target_id(&self) -> &TargetId { &self.0 }
    fn capabilities(&self) -> RuntimeCapabilities {
        RuntimeCapabilities {
            persistent_process: false, persistent_conversation: false,
            structured_turns: false, streaming_output: false,
            attachable: false, interruptible: false,
            structured_provenance: false, resumable: false,
        }
    }
    async fn status(&self) -> TargetStatus { TargetStatus::Offline }
    async fn submit_turn(&self, _r: TurnRequest, _p: BusyPolicy) -> TurnReceipt {
        TurnReceipt::RejectedUnavailable { status: TargetStatus::Offline }
    }
    async fn events(&self, _a: EventSeq) -> TurnEventStream {
        Box::pin(futures::stream::empty())
    }
    async fn cancel_turn(&self, _t: TurnId, _r: CancelReason) -> Result<(), RuntimeError> {
        Err(RuntimeError::Unsupported { operation: "cancel".into() })
    }
    async fn shutdown(&self, _m: ShutdownMode) -> Result<(), RuntimeError> { Ok(()) }
}

#[async_trait::async_trait]
impl NativeGateResolver for LabeledTarget {
    async fn resolve_native_gate(&self, _t: TurnId, _d: wasl_proto::GateDecision) -> Result<(), RuntimeError> {
        Err(RuntimeError::Unsupported { operation: "gate".into() })
    }
}

#[async_trait::async_trait]
impl ToolCallAnswerer for LabeledTarget {
    async fn answer_tool_call(&self, _t: TurnId, _v: String) -> Result<(), RuntimeError> {
        Err(RuntimeError::Unsupported { operation: "tool".into() })
    }
}

#[async_trait::async_trait]
impl RuntimeProvider for RoutingRecordingProvider {
    async fn target_for(&self, k: &TargetKey) -> Result<Arc<dyn DeliveryTarget>, RuntimeError> {
        self.record.lock().unwrap().push(SpawnContext::default());
        Ok(Arc::new(LabeledTarget(TargetId(format!("{}/{}", self.label, k.0)))))
    }

    async fn list_targets(&self) -> Vec<TargetId> {
        vec![TargetId(format!("{}/listed", self.label))]
    }

    async fn target_for_with_context(&self, k: &TargetKey, ctx: SpawnContext) -> Result<Arc<dyn DeliveryTarget>, RuntimeError> {
        self.record.lock().unwrap().push(ctx);
        Ok(Arc::new(LabeledTarget(TargetId(format!("{}/{}", self.label, k.0)))))
    }
}

/// Build a `ProtocolRoutingProvider` with two sub-providers.
/// This call panics with todo!() because routing.rs::new is not implemented.
fn routing_provider_with_two_protocols(
    claude: Arc<RoutingRecordingProvider>,
    opencode: Arc<RoutingRecordingProvider>,
) -> ProtocolRoutingProvider {
    let mut map: BTreeMap<String, Arc<dyn RuntimeProvider>> = BTreeMap::new();
    map.insert("claude".to_string(), claude as Arc<dyn RuntimeProvider>);
    map.insert("opencode-acp".to_string(), opencode as Arc<dyn RuntimeProvider>);
    ProtocolRoutingProvider::new(map, "claude".to_string())
}

/// ProtocolRoutingProvider::target_for_with_context must route the SpawnContext
/// (including model/cwd/etc.) through to the chosen sub-provider's spawn.
/// RED: ProtocolRoutingProvider::new panics with todo!().
#[tokio::test]
async fn routing_provider_threads_full_context_to_sub_provider_spawn() {
    let (claude, claude_rec) = RoutingRecordingProvider::new("claude");
    let (opencode, _) = RoutingRecordingProvider::new("opencode-acp");
    let router = routing_provider_with_two_protocols(claude, opencode);

    let ctx = SpawnContext {
        protocol: Some("claude".to_string()),
        model: Some("claude-opus-4-5".to_string()),
        cwd: Some("/project".to_string()),
        ..SpawnContext::default()
    };

    let _target = router
        .target_for_with_context(&key("ctx-route-ctx"), ctx.clone())
        .await
        .expect("routing must succeed");

    let seen = RoutingRecordingProvider::contexts_seen(&claude_rec);
    assert_eq!(seen.len(), 1, "claude sub-provider must be called exactly once");
    let seen_ctx = &seen[0];
    assert_eq!(
        seen_ctx.model,
        Some("claude-opus-4-5".to_string()),
        "model must be threaded to the sub-provider"
    );
    assert_eq!(
        seen_ctx.cwd,
        Some("/project".to_string()),
        "cwd must be threaded to the sub-provider"
    );
}

/// ProtocolRoutingProvider::target_for_with_context with None protocol must
/// route to the default sub-provider with the full context.
/// RED: ProtocolRoutingProvider::new panics with todo!().
#[tokio::test]
async fn routing_provider_with_none_protocol_routes_to_default_with_context() {
    let (claude, claude_rec) = RoutingRecordingProvider::new("claude");
    let (opencode, opencode_rec) = RoutingRecordingProvider::new("opencode-acp");
    let router = routing_provider_with_two_protocols(claude, opencode);

    // context.protocol is None → must use default "claude"
    let ctx = SpawnContext {
        model: Some("my-model".to_string()),
        ..SpawnContext::default()
    };
    assert!(ctx.protocol.is_none(), "precondition");

    let _target = router
        .target_for_with_context(&key("ctx-default"), ctx.clone())
        .await
        .expect("must succeed with None protocol");

    // Claude (the default) must have been called; opencode must not.
    let claude_seen = RoutingRecordingProvider::contexts_seen(&claude_rec);
    assert_eq!(
        claude_seen.len(),
        1,
        "default provider 'claude' must be called once; got {claude_seen:?}"
    );
    assert_eq!(
        claude_seen[0].model,
        Some("my-model".to_string()),
        "model must be threaded to the default sub-provider"
    );

    let opencode_seen = RoutingRecordingProvider::contexts_seen(&opencode_rec);
    assert!(
        opencode_seen.is_empty(),
        "opencode must not be called when protocol is None and default is claude"
    );
}
