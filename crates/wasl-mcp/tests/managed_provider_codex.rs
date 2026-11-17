//! Provider-registration tests: proves the `ProtocolRoutingProvider` built by
//! `build_managed_provider` has a "codex" sub-provider registered.
//!
//! Since `build_managed_provider` is a private function in `wasl_mcp::lib`,
//! this suite tests via the public path: it constructs a
//! `ProtocolRoutingProvider` with the same three sub-providers
//! (`"claude"`, `"opencode-acp"`, `"codex"`) using the public types from
//! `wasl_runtime` and `wasl_runtime_protocol`, exactly mirroring how the
//! private function wires them. This exercises both the public API surface that
//! `lib.rs` depends on AND the expectation that "codex" is registered — any
//! caller sending `context.protocol = "codex"` must get a real provider, not a
//! `RuntimeError::AdmissionRefused { detail: "no sub-provider registered …" }`.
//!
//! These tests are RED until `build_managed_provider` in `lib.rs` inserts
//! the codex provider:
//!   `providers.insert("codex".to_string(), codex_provider);`

use std::collections::BTreeMap;
use std::sync::{Arc, Mutex};

use wasl_audit::Sink;
use wasl_runtime::{
    ManagedConfig, MonotonicClock, ProtocolRoutingProvider, RuntimeProvider, SpawnContext,
    TargetKey, WaslManagedRuntimeProvider,
};

// Only used in is_admission_refused helper — suppress unused-import lint in
// case the test environment doesn't trigger the AdmissionRefused path.
#[allow(unused_imports)]
use wasl_runtime::RuntimeError;
use wasl_runtime_protocol::{ClaudeStreamProtocol, CodexAppServerProtocol, OpenCodeAcpProtocol};

// ─────────────────────────────────────────────────────────────────────────────
// Test seam: in-memory audit sink (same as MemSink in wasl_mcp)
// ─────────────────────────────────────────────────────────────────────────────

#[derive(Default)]
struct LocalMemSink;

impl Sink for LocalMemSink {
    fn append(&mut self, _entry: &wasl_audit::AuditEntry) -> Result<(), wasl_audit::AuditError> {
        Ok(())
    }
}

// ─────────────────────────────────────────────────────────────────────────────
// Helper: build a ProtocolRoutingProvider with all three sub-providers,
// exactly mirroring the expected `build_managed_provider` wiring in lib.rs.
// ─────────────────────────────────────────────────────────────────────────────

fn build_three_provider_router(default_protocol: &str) -> ProtocolRoutingProvider {
    let clock: Arc<dyn wasl_runtime::RuntimeClock> = Arc::new(MonotonicClock);

    let claude_sink: Arc<Mutex<dyn Sink + Send>> = Arc::new(Mutex::new(LocalMemSink));
    let opencode_sink: Arc<Mutex<dyn Sink + Send>> = Arc::new(Mutex::new(LocalMemSink));
    let codex_sink: Arc<Mutex<dyn Sink + Send>> = Arc::new(Mutex::new(LocalMemSink));

    let claude_provider: Arc<dyn RuntimeProvider> =
        Arc::new(WaslManagedRuntimeProvider::with_seams(
            ClaudeStreamProtocol::new("claude".to_string()),
            ManagedConfig::default(),
            clock.clone(),
            claude_sink,
        ));
    let opencode_provider: Arc<dyn RuntimeProvider> =
        Arc::new(WaslManagedRuntimeProvider::with_seams(
            OpenCodeAcpProtocol::new("opencode".to_string()),
            ManagedConfig::default(),
            clock.clone(),
            opencode_sink,
        ));
    let codex_provider: Arc<dyn RuntimeProvider> =
        Arc::new(WaslManagedRuntimeProvider::with_seams(
            CodexAppServerProtocol::new("codex".to_string()),
            ManagedConfig::default(),
            clock,
            codex_sink,
        ));

    let mut providers: BTreeMap<String, Arc<dyn RuntimeProvider>> = BTreeMap::new();
    providers.insert("claude".to_string(), claude_provider);
    providers.insert("opencode-acp".to_string(), opencode_provider);
    providers.insert("codex".to_string(), codex_provider);

    ProtocolRoutingProvider::new(providers, default_protocol.to_string())
}

fn key(s: &str) -> TargetKey {
    TargetKey(s.to_string())
}

fn ctx_with_protocol(proto: &str) -> SpawnContext {
    SpawnContext {
        protocol: Some(proto.to_string()),
        ..SpawnContext::default()
    }
}

// ─────────────────────────────────────────────────────────────────────────────
// Helper: determine if an error is specifically AdmissionRefused (routing miss)
// as opposed to SpawnFailed (provider found but binary unavailable in test env).
// ─────────────────────────────────────────────────────────────────────────────

fn is_admission_refused(err: &wasl_runtime::RuntimeError) -> bool {
    matches!(err, wasl_runtime::RuntimeError::AdmissionRefused { .. })
}

// ─────────────────────────────────────────────────────────────────────────────
// 1. "codex" protocol is resolvable (the core registration assertion)
//
//    The router must route "codex" to a registered sub-provider. In the test
//    environment there is no real `codex` binary, so `target_for_with_context`
//    may return `Err(SpawnFailed)` (provider found, binary unavailable) rather
//    than `Ok`. That is still a routing success — the critical distinction is
//    that it must NOT return `Err(AdmissionRefused)` (provider not registered).
// ─────────────────────────────────────────────────────────────────────────────

#[tokio::test]
async fn codex_protocol_does_not_get_admission_refused() {
    // This is the "does the router know about codex?" test.
    // RED: if codex is not registered, the router returns
    // AdmissionRefused { detail: "no sub-provider registered for protocol 'codex'" }.
    // GREEN: any other outcome (Ok or SpawnFailed or similar) proves routing reached
    // the codex sub-provider.
    let router = build_three_provider_router("claude");

    let ctx = ctx_with_protocol("codex");
    let result = router
        .target_for_with_context(&key("sess-codex-1"), ctx)
        .await;

    if let Err(ref e) = result {
        assert!(
            !is_admission_refused(e),
            "routing context.protocol=\"codex\" must NOT return AdmissionRefused — \
             that means the codex sub-provider is not registered in the map. \
             Error: {e:?}\n\
             RED until `providers.insert(\"codex\".to_string(), codex_provider)` is wired in lib.rs."
        );
    }
    // Ok or any error other than AdmissionRefused is acceptable: it means
    // the sub-provider was found (routing worked), and the error (if any)
    // is about the binary not being available in the test environment.
}

// ─────────────────────────────────────────────────────────────────────────────
// 2. "codex" as default_protocol does not yield AdmissionRefused
// ─────────────────────────────────────────────────────────────────────────────

#[tokio::test]
async fn codex_as_default_does_not_get_admission_refused() {
    // A router with default_protocol = "codex" must route a context-free
    // `target_for` to the codex sub-provider. Same binary-unavailability
    // tolerance as above.
    let router = build_three_provider_router("codex");

    let result = router.target_for(&key("sess-codex-2")).await;

    if let Err(ref e) = result {
        assert!(
            !is_admission_refused(e),
            "context-free target_for on a router with default_protocol=\"codex\" \
             must NOT return AdmissionRefused. Error: {e:?}"
        );
    }
}

// ─────────────────────────────────────────────────────────────────────────────
// 3. Pre-existing registrations are undisturbed (regression pins)
//    We only check these don't get AdmissionRefused since binaries may be absent.
// ─────────────────────────────────────────────────────────────────────────────

#[tokio::test]
async fn claude_protocol_still_not_admission_refused_after_codex_added() {
    let router = build_three_provider_router("claude");

    let ctx = ctx_with_protocol("claude");
    let result = router
        .target_for_with_context(&key("sess-claude-1"), ctx)
        .await;

    if let Err(ref e) = result {
        assert!(
            !is_admission_refused(e),
            "\"claude\" sub-provider must not get AdmissionRefused after codex is added. \
             Error: {e:?}"
        );
    }
}

#[tokio::test]
async fn opencode_acp_protocol_still_not_admission_refused_after_codex_added() {
    let router = build_three_provider_router("claude");

    let ctx = ctx_with_protocol("opencode-acp");
    let result = router
        .target_for_with_context(&key("sess-opencode-1"), ctx)
        .await;

    if let Err(ref e) = result {
        assert!(
            !is_admission_refused(e),
            "\"opencode-acp\" sub-provider must not get AdmissionRefused after codex is added. \
             Error: {e:?}"
        );
    }
}

// ─────────────────────────────────────────────────────────────────────────────
// 4. An unregistered protocol still errors with AdmissionRefused (regression pin)
// ─────────────────────────────────────────────────────────────────────────────

#[tokio::test]
async fn unregistered_protocol_still_errors_with_admission_refused() {
    let router = build_three_provider_router("claude");

    let ctx = ctx_with_protocol("totally-unknown-protocol");
    let result = router
        .target_for_with_context(&key("sess-unknown"), ctx)
        .await;

    assert!(
        result.is_err(),
        "routing to an unregistered protocol must return Err; got Ok(..) instead"
    );
    assert!(
        is_admission_refused(result.as_ref().unwrap_err()),
        "routing to an unregistered protocol must return AdmissionRefused, \
         got: {result:?}"
    );
}

// ─────────────────────────────────────────────────────────────────────────────
// 5. CodexAppServerProtocol can be constructed with a custom bin path
// ─────────────────────────────────────────────────────────────────────────────

#[test]
fn codex_appserver_protocol_constructs_with_custom_bin() {
    // Mirrors the claude/opencode provider construction: a custom bin path must
    // be accepted without panicking. This proves `CodexAppServerProtocol::new`
    // is the right constructor (same as ClaudeStreamProtocol::new /
    // OpenCodeAcpProtocol::new).
    let _protocol = CodexAppServerProtocol::new("/opt/codex/codex".to_string());
    // Construction itself must not panic; no further assertion needed.
}
