//! Live spawn-context tests: verify that `claude_argv` options are honoured by a
//! real `claude` CLI session. Gated behind `--features live-cli` following the
//! repo convention established in `wasl-adapter-claude/tests/live.rs` and
//! `wasl-runtime-protocol/tests/claude_conformance.rs` — default CI never
//! spends tokens; orchestrator runs explicit `--features live-cli` passes on
//! keyed hosts.
//!
//! These tests are INTENTIONALLY MINIMAL — they exist to give the orchestrator a
//! live seam to pin: "spawn a session with context.cwd=X and assert the first
//! SessionReady arrives from a process whose working directory is X." A future
//! code-agent fills in the body after `claude_argv` is implemented (todo!() →
//! real logic) and live-verified.

#![cfg(feature = "live-cli")]

use std::sync::{Arc, Mutex};
use std::time::Duration;

use futures::StreamExt;

use wasl_audit::{AuditEntry, AuditError, Sink};
use wasl_runtime::{
    BusyPolicy, EventSeq, ManagedConfig, MonotonicClock, SpawnContext, StreamItem, TargetKey,
    TurnReceipt, WaslManagedRuntimeProvider,
};
use wasl_runtime_protocol::ClaudeStreamProtocol;

/// No-op audit sink (identical pattern to `claude_conformance.rs`).
struct NoopSink;
impl Sink for NoopSink {
    fn append(&mut self, _entry: &AuditEntry) -> Result<(), AuditError> {
        Ok(())
    }
}

/// Drain events until the predicate is true or 90s elapses.
async fn drain_until(
    mut stream: wasl_runtime::TurnEventStream,
    bound: Duration,
    mut done: impl FnMut(&[wasl_runtime::TurnEvent]) -> bool,
) -> Vec<wasl_runtime::TurnEvent> {
    let mut out = Vec::new();
    let deadline = tokio::time::Instant::now() + bound;
    loop {
        if done(&out) {
            break;
        }
        let remaining = deadline.saturating_duration_since(tokio::time::Instant::now());
        if remaining.is_zero() {
            break;
        }
        match tokio::time::timeout(remaining, stream.next()).await {
            Ok(Some(StreamItem::Event { event, .. })) => out.push(event),
            Ok(Some(StreamItem::Truncated { .. })) => continue,
            Ok(None) | Err(_) => break,
        }
    }
    out
}

/// Spawn a real claude session with a known `cwd` and a cheap model, ask it to
/// print the current working directory, and assert the reply contains the
/// expected path. This verifies that `claude_argv` (once implemented) passes
/// the options through to the real process correctly.
///
/// Requirements:
///   * A real `claude` CLI is on PATH.
///   * `WASL_LIVE_NONCE` is set to a fresh value (avoids session id collision).
///   * The MARSAD_LIVE_MODEL env var (or a cheap default) is used to minimise
///     token spend.
///
/// Run:
///   WASL_LIVE_NONCE=1 cargo test -p wasl-runtime-protocol --features live-cli \
///     live_spawn_context -- --nocapture
#[tokio::test]
async fn claude_session_runs_in_requested_cwd() {
    let nonce = std::env::var("WASL_LIVE_NONCE")
        .expect("WASL_LIVE_NONCE must be set to run live_spawn_context tests");

    // Use a cheap model; allow the caller to override via env.
    let model =
        std::env::var("MARSAD_LIVE_MODEL").unwrap_or_else(|_| "claude-haiku-4-5".to_string());

    // A predictable temp dir we can assert against.
    let tmp = std::env::temp_dir();
    let cwd = tmp
        .to_str()
        .expect("temp dir path must be valid UTF-8")
        .to_string();

    let context = SpawnContext {
        cwd: Some(cwd.clone()),
        model: Some(model),
        ..SpawnContext::default()
    };

    let context_id = format!("wasl-live-spawn-context-{nonce}");

    let provider: WaslManagedRuntimeProvider<ClaudeStreamProtocol> =
        WaslManagedRuntimeProvider::with_seams(
            ClaudeStreamProtocol::new("claude"),
            ManagedConfig::default(),
            Arc::new(MonotonicClock),
            Arc::new(Mutex::new(NoopSink)),
        );

    // Build a SpawnOptions with our context embedded.
    // The provider uses context.cwd to set Command::current_dir inside spawn_child.
    // We verify this by asking claude to print its cwd and asserting the reply.
    let target = provider
        .target_for_with_context(&TargetKey(context_id), context)
        .await
        .expect("target_for_with_context must succeed");

    use wasl_adapter::{Delivery, DeliveryKind};
    use wasl_runtime::{Causation, TurnEvent, TurnSource};

    let req = wasl_runtime::TurnRequest {
        delivery: Delivery {
            context_id: "live-spawn-ctx".to_string(),
            from: "test".to_string(),
            kind: DeliveryKind::Consult,
            // Ask claude to print its working directory so we can assert it.
            text: "Print the absolute path of your current working directory. \
                   Reply with ONLY the path, nothing else."
                .to_string(),
            data: vec![],
        },
        source: TurnSource::Agent {
            from: wasl_proto::AgentId("test".to_string()),
        },
        idempotency_key: None,
        causation: Causation {
            root_request: wasl_proto::RequestId("r1".to_string()),
            parent_request: None,
            path: vec![],
            hop: 0,
        },
    };

    let receipt = target.submit_turn(req, BusyPolicy::Reject).await;
    let turn_id = match receipt {
        TurnReceipt::Started { turn_id, .. } => turn_id,
        other => panic!("expected TurnReceipt::Started, got {other:?}"),
    };

    let events = drain_until(
        target.events(EventSeq(0)).await,
        Duration::from_secs(90),
        |evs| {
            evs.iter()
                .any(|e| matches!(e, TurnEvent::TurnCompleted { turn_id: t, .. } if t == &turn_id))
        },
    )
    .await;

    let reply = events
        .iter()
        .find_map(|e| match e {
            TurnEvent::TurnCompleted {
                turn_id: t, reply, ..
            } if t == &turn_id => Some(reply.clone()),
            _ => None,
        })
        .unwrap_or_else(|| panic!("no TurnCompleted observed within 90s; events: {events:?}"));

    // The model may include trailing newlines or minor formatting — trim.
    let reply_trimmed = reply.trim();
    assert!(
        reply_trimmed.contains(cwd.trim_end_matches('/')),
        "claude's reported cwd {reply_trimmed:?} must contain the requested cwd {cwd:?}; \
         this means ClaudeStreamProtocol did not pass cwd through to the OS process"
    );
}

/// Verify that context.model is honoured: spawn with an explicit model name and
/// assert the session/init event (mapped to SessionReady) carries that model in
/// the underlying wire (indirectly: the session at least starts without error).
/// A full model-echo assertion would require parsing the raw `system/init` line,
/// which is an impl detail; this test at minimum confirms the process starts
/// with the model flag present in argv.
#[tokio::test]
async fn claude_session_starts_with_explicit_model() {
    let nonce = std::env::var("WASL_LIVE_NONCE")
        .expect("WASL_LIVE_NONCE must be set to run live_spawn_context tests");

    let model =
        std::env::var("MARSAD_LIVE_MODEL").unwrap_or_else(|_| "claude-haiku-4-5".to_string());

    let context = SpawnContext {
        model: Some(model.clone()),
        ..SpawnContext::default()
    };

    let context_id = format!("wasl-live-model-context-{nonce}");

    let provider: WaslManagedRuntimeProvider<ClaudeStreamProtocol> =
        WaslManagedRuntimeProvider::with_seams(
            ClaudeStreamProtocol::new("claude"),
            ManagedConfig::default(),
            Arc::new(MonotonicClock),
            Arc::new(Mutex::new(NoopSink)),
        );

    // Session must at minimum start (SessionReady) without panicking.
    let target = provider
        .target_for_with_context(&TargetKey(context_id), context)
        .await
        .expect("target_for_with_context must succeed");

    let events = drain_until(
        target.events(EventSeq(0)).await,
        Duration::from_secs(60),
        |evs| {
            evs.iter()
                .any(|e| matches!(e, wasl_runtime::TurnEvent::SessionReady { .. }))
        },
    )
    .await;

    assert!(
        events
            .iter()
            .any(|e| matches!(e, wasl_runtime::TurnEvent::SessionReady { .. })),
        "SessionReady must be received within 60s when spawning with model={model:?}; \
         this suggests claude_argv did not pass --model correctly"
    );
}
