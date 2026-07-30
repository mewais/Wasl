//! Tests for [`wasl_runtime::ProtocolRoutingProvider`].
//!
//! All `ProtocolRoutingProvider` method bodies are `todo!()` — these tests are
//! expected to COMPILE but FAIL at runtime with a `todo!()` panic until the
//! Phase-2 coder fills the routing logic in `src/routing.rs`.
//!
//! # What the suite proves (once the coder fills the bodies)
//!
//! 1. `target_for_with_context` routes by `context.protocol` to the matching
//!    sub-provider.
//! 2. When `context.protocol` is `None` it falls back to `default_protocol`.
//! 3. A subsequent context-free `target_for(key)` routes to the SAME sub-provider
//!    that was chosen on the first `target_for_with_context` for that key.
//! 4. `list_targets` returns the union of all sub-providers' `list_targets`.
//!
//! # Test doubles
//!
//! Each sub-provider is a tiny `StubProvider` that always returns the same
//! pre-baked `Arc<dyn DeliveryTarget>` and records which keys it was called with.
//! The target's `target_id()` encodes the protocol name so the routing test can
//! distinguish which sub-provider served the request.

use std::collections::BTreeMap;
use std::sync::{Arc, Mutex};

use wasl_runtime::{
    BusyPolicy, CancelReason, DeliveryTarget, EphemeralRuntimeProvider, EventSeq,
    NativeGateResolver, ProtocolRoutingProvider, RuntimeCapabilities, RuntimeError, RuntimeProvider,
    ShutdownMode, SpawnContext, TargetId, TargetKey, TargetStatus, ToolCallAnswerer,
    TurnEventStream, TurnId, TurnReceipt, TurnRequest,
};
use wasl_adapter;

fn key(s: &str) -> TargetKey {
    TargetKey(s.to_string())
}

fn ctx_with_protocol(protocol: &str) -> SpawnContext {
    SpawnContext {
        protocol: Some(protocol.to_string()),
        ..SpawnContext::default()
    }
}

// ------------------------------------------------------------------
// StubTarget — a minimal DeliveryTarget whose target_id encodes a label
// so tests can identify which sub-provider answered.
// ------------------------------------------------------------------

#[derive(Debug)]
struct StubTarget {
    id: TargetId,
}

#[async_trait::async_trait]
impl DeliveryTarget for StubTarget {
    fn target_id(&self) -> &TargetId {
        &self.id
    }

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

    async fn status(&self) -> TargetStatus {
        TargetStatus::Offline
    }

    async fn submit_turn(&self, _req: TurnRequest, _policy: BusyPolicy) -> TurnReceipt {
        TurnReceipt::RejectedUnavailable {
            status: TargetStatus::Offline,
        }
    }

    async fn events(&self, _after_seq: EventSeq) -> TurnEventStream {
        Box::pin(futures::stream::empty())
    }

    async fn cancel_turn(
        &self,
        _turn_id: TurnId,
        _reason: CancelReason,
    ) -> Result<(), RuntimeError> {
        Err(RuntimeError::Unsupported {
            operation: "cancel_turn".to_string(),
        })
    }

    async fn shutdown(&self, _mode: ShutdownMode) -> Result<(), RuntimeError> {
        Ok(())
    }
}

#[async_trait::async_trait]
impl NativeGateResolver for StubTarget {
    async fn resolve_native_gate(
        &self,
        _turn_id: TurnId,
        _decision: wasl_proto::GateDecision,
    ) -> Result<(), RuntimeError> {
        Err(RuntimeError::Unsupported {
            operation: "resolve_native_gate".to_string(),
        })
    }
}

#[async_trait::async_trait]
impl ToolCallAnswerer for StubTarget {
    async fn answer_tool_call(&self, _turn_id: TurnId, _value: String) -> Result<(), RuntimeError> {
        Err(RuntimeError::Unsupported {
            operation: "answer_tool_call".to_string(),
        })
    }
}

// ------------------------------------------------------------------
// StubProvider — a RuntimeProvider that returns a StubTarget whose id
// encodes `"{protocol_label}/{key}"` so routing assertions are readable.
// ------------------------------------------------------------------

struct StubProvider {
    label: String,
    /// Keys this provider was asked for, in call order.
    keys_seen: Mutex<Vec<TargetKey>>,
    /// Fixed list of TargetIds to return from list_targets.
    listed: Vec<TargetId>,
}

impl StubProvider {
    fn new(label: &str, listed: Vec<TargetId>) -> Arc<Self> {
        Arc::new(StubProvider {
            label: label.to_string(),
            keys_seen: Mutex::new(Vec::new()),
            listed,
        })
    }

    fn keys_seen(&self) -> Vec<TargetKey> {
        self.keys_seen.lock().unwrap().clone()
    }
}

#[async_trait::async_trait]
impl RuntimeProvider for StubProvider {
    async fn target_for(
        &self,
        key: &TargetKey,
    ) -> Result<Arc<dyn DeliveryTarget>, RuntimeError> {
        self.keys_seen.lock().unwrap().push(key.clone());
        let id = TargetId(format!("{}/{}", self.label, key.0));
        Ok(Arc::new(StubTarget { id }))
    }

    async fn list_targets(&self) -> Vec<TargetId> {
        self.listed.clone()
    }

    // target_for_with_context: use the default impl which delegates to target_for.
    // The router calls this; the stub records the key and returns a labeled target.
}

// ------------------------------------------------------------------
// Helper: build a ProtocolRoutingProvider with two sub-providers
// ("claude" and "opencode-acp") and default "claude".
// ------------------------------------------------------------------

fn two_provider_router(
    claude: Arc<StubProvider>,
    opencode: Arc<StubProvider>,
) -> ProtocolRoutingProvider {
    let mut map: BTreeMap<String, Arc<dyn RuntimeProvider>> = BTreeMap::new();
    map.insert("claude".to_string(), claude as Arc<dyn RuntimeProvider>);
    map.insert(
        "opencode-acp".to_string(),
        opencode as Arc<dyn RuntimeProvider>,
    );
    ProtocolRoutingProvider::new(map, "claude".to_string())
}

// ==================================================================
// 1. Routes to "claude" sub-provider when context.protocol == "claude".
// ==================================================================

#[tokio::test]
async fn routes_to_claude_when_context_protocol_is_claude() {
    let claude = StubProvider::new(
        "claude",
        vec![TargetId("claude/existing".to_string())],
    );
    let opencode = StubProvider::new("opencode-acp", vec![]);
    let router = two_provider_router(claude.clone(), opencode.clone());

    let ctx = ctx_with_protocol("claude");
    let target = router
        .target_for_with_context(&key("ctx-a"), ctx)
        .await
        .expect("routing to 'claude' must succeed");

    assert!(
        target.target_id().0.starts_with("claude/"),
        "target routed to 'claude' sub-provider must have id starting with 'claude/'; \
         got {:?}",
        target.target_id()
    );

    // The opencode provider must NOT have been called.
    assert!(
        opencode.keys_seen().is_empty(),
        "opencode sub-provider must not be called when context.protocol is 'claude'; \
         it was called with: {:?}",
        opencode.keys_seen()
    );
}

// ==================================================================
// 2. Routes to "opencode-acp" when context.protocol == "opencode-acp".
// ==================================================================

#[tokio::test]
async fn routes_to_opencode_when_context_protocol_is_opencode_acp() {
    let claude = StubProvider::new("claude", vec![]);
    let opencode = StubProvider::new("opencode-acp", vec![]);
    let router = two_provider_router(claude.clone(), opencode.clone());

    let ctx = ctx_with_protocol("opencode-acp");
    let target = router
        .target_for_with_context(&key("ctx-b"), ctx)
        .await
        .expect("routing to 'opencode-acp' must succeed");

    assert!(
        target.target_id().0.starts_with("opencode-acp/"),
        "target routed to 'opencode-acp' must have id starting with 'opencode-acp/'; \
         got {:?}",
        target.target_id()
    );

    assert!(
        claude.keys_seen().is_empty(),
        "claude sub-provider must not be called when context.protocol is 'opencode-acp'; \
         it was called with: {:?}",
        claude.keys_seen()
    );
}

// ==================================================================
// 3. Falls back to default_protocol when context.protocol is None.
// ==================================================================

#[tokio::test]
async fn falls_back_to_default_protocol_when_context_protocol_is_none() {
    let claude = StubProvider::new("claude", vec![]);
    let opencode = StubProvider::new("opencode-acp", vec![]);
    let router = two_provider_router(claude.clone(), opencode.clone());

    // context.protocol = None → must use default "claude"
    let ctx = SpawnContext::default();
    assert!(ctx.protocol.is_none(), "precondition: context has no protocol");

    let target = router
        .target_for_with_context(&key("ctx-c"), ctx)
        .await
        .expect("routing with None protocol must fall back to default and succeed");

    assert!(
        target.target_id().0.starts_with("claude/"),
        "when context.protocol is None the router must fall back to 'claude' (the default); \
         got {:?}",
        target.target_id()
    );

    assert!(
        opencode.keys_seen().is_empty(),
        "opencode must not be called when context.protocol is None and default is 'claude'"
    );
}

// ==================================================================
// 4. A context-free `target_for(key)` routes to the SAME sub-provider
//    that was chosen at first `target_for_with_context` for that key.
// ==================================================================

#[tokio::test]
async fn context_free_target_for_routes_to_same_provider_as_first_call() {
    let claude = StubProvider::new("claude", vec![]);
    let opencode = StubProvider::new("opencode-acp", vec![]);
    let router = two_provider_router(claude.clone(), opencode.clone());

    // First call: explicitly route to "opencode-acp".
    let ctx = ctx_with_protocol("opencode-acp");
    let first = router
        .target_for_with_context(&key("ctx-d"), ctx)
        .await
        .expect("first call must succeed");
    assert!(
        first.target_id().0.starts_with("opencode-acp/"),
        "first call must route to opencode-acp; got {:?}",
        first.target_id()
    );

    // Second call: context-free target_for for the same key.  Must route to
    // "opencode-acp" again (the recorded protocol for that key), NOT to the
    // default "claude".
    let second = router
        .target_for(&key("ctx-d"))
        .await
        .expect("context-free target_for must succeed");

    assert!(
        second.target_id().0.starts_with("opencode-acp/"),
        "context-free target_for for a key that was first routed to 'opencode-acp' must \
         return the same sub-provider, not the default 'claude'; got {:?}",
        second.target_id()
    );
}

// ==================================================================
// 5. Context-free target_for for a FRESH key routes to default_protocol.
// ==================================================================

#[tokio::test]
async fn context_free_target_for_for_fresh_key_routes_to_default() {
    let claude = StubProvider::new("claude", vec![]);
    let opencode = StubProvider::new("opencode-acp", vec![]);
    let router = two_provider_router(claude.clone(), opencode.clone());

    // No prior call for "ctx-fresh" — target_for must use the default protocol.
    let target = router
        .target_for(&key("ctx-fresh"))
        .await
        .expect("context-free target_for for a fresh key must succeed");

    assert!(
        target.target_id().0.starts_with("claude/"),
        "context-free target_for for a fresh key must use default 'claude'; \
         got {:?}",
        target.target_id()
    );
}

// ==================================================================
// 6. list_targets returns the union of all sub-providers' list_targets.
// ==================================================================

#[tokio::test]
async fn list_targets_returns_union_of_all_sub_providers() {
    let claude = StubProvider::new(
        "claude",
        vec![
            TargetId("claude/t1".to_string()),
            TargetId("claude/t2".to_string()),
        ],
    );
    let opencode = StubProvider::new(
        "opencode-acp",
        vec![TargetId("opencode-acp/t3".to_string())],
    );
    let router = two_provider_router(claude, opencode);

    let listed = router.list_targets().await;

    // Must contain all three ids from both sub-providers.
    let has = |id: &str| listed.iter().any(|t| t.0 == id);

    assert!(
        has("claude/t1"),
        "list_targets must include claude/t1; got {listed:?}"
    );
    assert!(
        has("claude/t2"),
        "list_targets must include claude/t2; got {listed:?}"
    );
    assert!(
        has("opencode-acp/t3"),
        "list_targets must include opencode-acp/t3; got {listed:?}"
    );
    assert_eq!(
        listed.len(),
        3,
        "list_targets must return exactly the union (3 items); got {listed:?}"
    );
}

// ==================================================================
// 7. Unknown protocol returns an error (not a panic or silent mis-route).
// ==================================================================

#[tokio::test]
async fn unknown_protocol_returns_error_not_panic() {
    let claude = StubProvider::new("claude", vec![]);
    let opencode = StubProvider::new("opencode-acp", vec![]);
    let router = two_provider_router(claude, opencode);

    let ctx = ctx_with_protocol("codex-unknown");
    let result = router
        .target_for_with_context(&key("ctx-unknown"), ctx)
        .await;

    assert!(
        result.is_err(),
        "routing to an unknown protocol must return an error, not silently pick the default; \
         got Ok(..) instead"
    );
}

// ==================================================================
// 8. ProtocolRoutingProvider is itself a RuntimeProvider (object-safe).
// ==================================================================

#[tokio::test]
async fn routing_provider_is_object_safe_as_runtime_provider() {
    let claude = StubProvider::new("claude", vec![]);
    let opencode = StubProvider::new("opencode-acp", vec![]);
    let router = two_provider_router(claude, opencode);

    // Must be usable as a trait object.
    let dyn_prov: &dyn RuntimeProvider = &router;

    let ctx = ctx_with_protocol("claude");
    let target = dyn_prov
        .target_for_with_context(&key("ctx-objsafe"), ctx)
        .await
        .expect("routing via &dyn RuntimeProvider must succeed");

    assert!(
        target.target_id().0.starts_with("claude/"),
        "routing via the trait object must still route correctly; got {:?}",
        target.target_id()
    );
}
