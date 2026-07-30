#![cfg(feature = "live-cli")]
//! Phase-6-equivalent (codex) — the `--features live-cli` end-to-end proof,
//! mirroring `opencode_acp_conformance.rs` + `opencode_acp_live_streaming_and_queueing.rs`
//! but against a REAL `codex app-server --stdio`-backed managed target. This
//! is the SAME capability-gated `conformance()` body copied byte-for-byte a
//! THIRD time (after claude + opencode); if it passes with the identical
//! assertion logic that already passed against `EphemeralTarget`,
//! `ClaudeStreamProtocol`, and `OpenCodeAcpProtocol`, the generic R1 actor/core
//! is proven driver-agnostic across a FOURTH independently-different wire
//! protocol (codex app-server JSON-RPC-over-stdio) with ZERO new
//! `ProtocolEvent`/`TargetStatus` variants.
//!
//! ==========================================================================
//! ⚠️  DOES NOT COMPILE until the coder creates `CodexAppServerProtocol`
//! (`src/codex_appserver.rs`, re-exported at the crate root). Compiled/run ONLY
//! under `--features live-cli`; default `cargo test` never touches it, so it
//! costs ZERO API usage in normal CI. That unresolved-import failure (under the
//! feature) is the red state. ⚠️
//! ==========================================================================
//!
//! Run: `cargo test -p wasl-runtime-protocol --features live-cli
//! codex_appserver_live -- --test-threads=1`, with `WASL_LIVE_NONCE` set to a
//! fresh value. Costs real `codex` CLI model calls (the 2 turns
//! `conformance()` issues + the pid/queueing probes).
//!
//! REAL-CLI ENVIRONMENT (from `spikes/live-substrate/codex_appserver_probe.py`):
//! codex app-server hard-requires
//!   (a) an ISOLATED, WRITABLE `CODEX_HOME` with a seeded `config.toml` — its
//!       sqlite state/logs/goals DBs init HARD-FAIL otherwise; and
//!   (b) `OPENAI_API_KEY` present (any value; the REAL auth is a header baked
//!       into config.toml).
//! `isolated_codex_env()` below seeds (a) into a fresh per-run temp CODEX_HOME
//! and ensures (b), mirroring the probe. The MODEL/auth CONTENT of config.toml
//! (the real `Ocp-Apim-Subscription-Key`, model provider, etc.) is an
//! ORCHESTRATOR/deployment responsibility supplied via the ambient environment
//! (`WASL_CODEX_CONFIG_TOML` — a path to a ready config.toml to copy, or the
//! ambient `CODEX_HOME` already seeded), NOT hardcoded here — exactly the
//! `opencode_acp_conformance.rs` "isolated env is an orchestrator
//! responsibility, documented not self-set" precedent. codex is Node/V8-based
//! and can OOM under ~1GB, so the deployment must budget memory generously.
//!
//! NOTE `CodexAppServerProtocol` itself is responsible for pointing the spawned
//! child at this CODEX_HOME + OPENAI_API_KEY. Two clean shapes are acceptable
//! (the coder picks one; this file supports BOTH via the constructor probe
//! below): the driver READS `CODEX_HOME`/`OPENAI_API_KEY` from its own ambient
//! env at spawn (so exporting them in-process here suffices), OR the driver
//! exposes `with_codex_home(bin, codex_home)`. This file sets the env vars
//! (covering the ambient-env shape) AND passes the path where a builder exists.

use std::path::PathBuf;
use std::sync::{Arc, Mutex};
use std::time::Duration;

use futures::StreamExt;

use wasl_adapter::{Delivery, DeliveryKind};
use wasl_audit::{AuditEntry, AuditError, Sink};
use wasl_runtime::{
    BusyPolicy, Causation, DeliveryTarget, EventSeq, ManagedConfig, MonotonicClock, StreamItem,
    TargetKey, TurnEvent, TurnId, TurnReceipt, TurnRequest, TurnSource, WaslManagedRuntimeProvider,
};
use wasl_runtime_protocol::CodexAppServerProtocol;

struct NoopSink;

impl Sink for NoopSink {
    fn append(&mut self, _entry: &AuditEntry) -> Result<(), AuditError> {
        Ok(())
    }
}

/// Seed an ISOLATED, writable `CODEX_HOME` for this run and ensure
/// `OPENAI_API_KEY` is present, mirroring the probe script's setup. Returns the
/// CODEX_HOME path (also exported into this process's env so a driver that
/// reads its ambient env at spawn picks it up).
///
/// The config.toml CONTENT is orchestrator-supplied: if `WASL_CODEX_CONFIG_TOML`
/// points at a ready config.toml, it is copied in; else if the ambient
/// `CODEX_HOME` already has one, that whole home is reused. A test lacking any
/// real codex config cannot make real model calls anyway — that is the
/// deployment's job, exactly as `opencode_acp_conformance.rs` documents for its
/// own XDG-config premise.
fn isolated_codex_env(test_name: &str) -> PathBuf {
    let nonce = std::env::var("WASL_LIVE_NONCE")
        .expect("WASL_LIVE_NONCE must be set (to a fresh value) to run codex_appserver_live.rs");

    // Prefer an orchestrator-provided, already-seeded CODEX_HOME verbatim.
    if let Ok(home) = std::env::var("WASL_CODEX_HOME") {
        let home = PathBuf::from(home);
        std::env::set_var("CODEX_HOME", &home);
        ensure_openai_api_key();
        return home;
    }

    // Otherwise build a fresh per-run home and seed config.toml from the
    // orchestrator-supplied source.
    let home = std::env::temp_dir().join(format!("wasl-codex-home-{test_name}-{nonce}"));
    std::fs::create_dir_all(&home).expect("create isolated CODEX_HOME");

    let config_src = std::env::var("WASL_CODEX_CONFIG_TOML").expect(
        "WASL_CODEX_CONFIG_TOML (path to a ready codex config.toml with the real \
         model provider + auth) OR WASL_CODEX_HOME (a pre-seeded CODEX_HOME) must \
         be set -- the model/auth content is an orchestrator/deployment \
         responsibility, not hardcoded in this test (same discipline as \
         opencode_acp_conformance.rs)",
    );
    std::fs::copy(&config_src, home.join("config.toml"))
        .expect("copy the orchestrator-supplied config.toml into the isolated CODEX_HOME");

    std::env::set_var("CODEX_HOME", &home);
    ensure_openai_api_key();
    home
}

/// codex's own internal check requires `OPENAI_API_KEY` be present (any value);
/// the REAL auth is baked into config.toml. Mirrors the probe's
/// `env.setdefault("OPENAI_API_KEY", ... "x")`.
fn ensure_openai_api_key() {
    if std::env::var("OPENAI_API_KEY").is_err() {
        std::env::set_var("OPENAI_API_KEY", "x");
    }
}

fn build_provider() -> WaslManagedRuntimeProvider<CodexAppServerProtocol> {
    WaslManagedRuntimeProvider::with_seams(
        CodexAppServerProtocol::new("codex"),
        ManagedConfig::default(),
        Arc::new(MonotonicClock),
        Arc::new(Mutex::new(NoopSink)),
    )
}

fn sample_request(context_id: String, text: String) -> TurnRequest {
    TurnRequest {
        delivery: Delivery {
            context_id,
            from: "peer-a".to_string(),
            kind: DeliveryKind::Consult,
            text,
            data: vec![],
        },
        source: TurnSource::Agent {
            from: wasl_proto::AgentId("peer-a".to_string()),
        },
        idempotency_key: None,
        causation: Causation {
            root_request: wasl_proto::RequestId("r1".to_string()),
            parent_request: None,
            path: vec![],
            hop: 0,
        },
    }
}

fn human_turn(context_id: &str, text: &str) -> TurnRequest {
    TurnRequest {
        delivery: Delivery {
            context_id: context_id.to_string(),
            from: "wasl-live-test".to_string(),
            kind: DeliveryKind::Consult,
            text: text.to_string(),
            data: vec![],
        },
        source: TurnSource::LocalHuman,
        idempotency_key: None,
        causation: Causation {
            root_request: wasl_proto::RequestId("live-root-1".to_string()),
            parent_request: None,
            path: vec![],
            hop: 0,
        },
    }
}

fn a2a_turn(context_id: &str, text: &str) -> TurnRequest {
    TurnRequest {
        delivery: Delivery {
            context_id: context_id.to_string(),
            from: "a2a-peer".to_string(),
            kind: DeliveryKind::Consult,
            text: text.to_string(),
            data: vec![],
        },
        source: TurnSource::Agent {
            from: wasl_proto::AgentId("a2a-peer".to_string()),
        },
        idempotency_key: None,
        causation: Causation {
            root_request: wasl_proto::RequestId("live-root-2".to_string()),
            parent_request: None,
            path: vec![],
            hop: 0,
        },
    }
}

/// Bounded drain (hang-guard). A real managed target's `events()` is a LIVE
/// tail that never ends, so `.collect()` would hang forever — the exact bug the
/// claude/opencode live tests document. Stops as soon as `done` is satisfied.
async fn drain_until(
    mut stream: wasl_runtime::TurnEventStream,
    bound: Duration,
    mut done: impl FnMut(&[TurnEvent]) -> bool,
) -> Vec<TurnEvent> {
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
            Ok(None) => break,
            Err(_) => break,
        }
    }
    out
}

fn completed_reply_for(events: &[TurnEvent], turn_id: &TurnId) -> Option<String> {
    events.iter().find_map(|e| match e {
        TurnEvent::TurnCompleted {
            turn_id: t, reply, ..
        } if t == turn_id => Some(reply.clone()),
        _ => None,
    })
}

fn describe_events(events: &[TurnEvent]) -> String {
    if events.is_empty() {
        return "<no events observed at all>".to_string();
    }
    events
        .iter()
        .map(|e| match e {
            TurnEvent::SessionReady { native } => format!("SessionReady{{native:{native:?}}}"),
            TurnEvent::TurnAccepted { turn_id, .. } => format!("TurnAccepted{{{turn_id:?}}}"),
            TurnEvent::AssistantDelta { turn_id, text } => {
                format!("AssistantDelta{{{turn_id:?}, {} chars}}", text.len())
            }
            TurnEvent::TurnCompleted { turn_id, .. } => format!("TurnCompleted{{{turn_id:?}}}"),
            TurnEvent::TurnFailed { turn_id, error, .. } => {
                format!("TurnFailed{{{turn_id:?}, {error}}}")
            }
            TurnEvent::ProcessExited { code } => format!("ProcessExited{{code:{code:?}}}"),
            other => format!("{other:?}"),
        })
        .collect::<Vec<_>>()
        .join(", ")
}

// =====================================================================
// Verbatim copy of the shared conformance() body (identical to the copies in
// crates/wasl-runtime/tests/conformance_suite.rs, claude_conformance.rs,
// opencode_acp_conformance.rs). DO NOT edit the logic independently of those.
// =====================================================================
async fn conformance(target: &dyn DeliveryTarget) {
    let caps = target.capabilities();
    let target_key = target.target_id().0.clone();

    if caps.persistent_conversation {
        let remember_req = sample_request(target_key.clone(), "remember VIOLET-7F29".to_string());
        let remember_receipt = target.submit_turn(remember_req, BusyPolicy::Reject).await;
        let remember_turn_id = match remember_receipt {
            TurnReceipt::Started { turn_id, .. } => turn_id,
            other => panic!("expected Started for 'remember' turn, got {other:?}"),
        };
        let remember_events = drain_until(
            target.events(EventSeq(0)).await,
            Duration::from_secs(240),
            |evs| completed_reply_for(evs, &remember_turn_id).is_some(),
        )
        .await;
        let remember_reply = completed_reply_for(&remember_events, &remember_turn_id)
            .unwrap_or_else(|| {
                panic!(
                    "no TurnCompleted correlated to the 'remember' turn_id ({:?}) within the \
                     240s hang-guard bound. Events actually observed: {}",
                    remember_turn_id,
                    describe_events(&remember_events)
                )
            });
        assert!(
            remember_reply.contains("VIOLET-7F29"),
            "expected the 'remember' turn's OWN reply to echo the token, got {remember_reply:?}"
        );

        let recall_req = sample_request(target_key.clone(), "recall".to_string());
        let recall_receipt = target.submit_turn(recall_req, BusyPolicy::Reject).await;
        let recall_turn_id = match recall_receipt {
            TurnReceipt::Started { turn_id, .. } => turn_id,
            other => panic!("expected Started for 'recall' turn, got {other:?}"),
        };
        let recall_events = drain_until(
            target.events(EventSeq(0)).await,
            Duration::from_secs(240),
            |evs| completed_reply_for(evs, &recall_turn_id).is_some(),
        )
        .await;
        let recall_reply = completed_reply_for(&recall_events, &recall_turn_id).unwrap_or_else(|| {
            panic!(
                "no TurnCompleted correlated to the 'recall' turn_id ({:?}) (NOT the 'remember' turn's) within the \
                 240s hang-guard bound. Events actually observed: {}",
                recall_turn_id,
                describe_events(&recall_events)
            )
        });
        assert!(
            recall_reply.contains("VIOLET-7F29"),
            "persistent_conversation claimed but 'recall' turn's OWN reply {recall_reply:?} did not echo the token"
        );
    }

    if caps.interruptible {
        let req = sample_request(target_key.clone(), "a cancellable turn".to_string());
        let receipt = target.submit_turn(req, BusyPolicy::Reject).await;
        let turn_id = match receipt {
            TurnReceipt::Started { turn_id, .. } => turn_id,
            other => panic!("expected Started, got {other:?}"),
        };
        let result = target
            .cancel_turn(turn_id, wasl_runtime::CancelReason::OperatorRequest)
            .await;
        assert!(
            result.is_ok(),
            "interruptible claimed but cancel_turn errored: {result:?}"
        );
    }
}

// =====================================================================
// The live tests.
// =====================================================================

/// THE central genericity proof: the verbatim `conformance()` against a REAL
/// codex-app-server-backed managed target. Passing here (with the exact logic
/// that already passed for ephemeral/claude/opencode) proves the generic core
/// is driver-agnostic across codex too.
#[tokio::test]
async fn codex_app_server_protocol_passes_conformance() {
    let _home = isolated_codex_env("conformance");
    let nonce = std::env::var("WASL_LIVE_NONCE").expect("WASL_LIVE_NONCE must be set");
    let context_id = format!("wasl-runtime-protocol-codex-conformance-{nonce}");

    let provider = build_provider();
    let target = provider
        .target_for(&TargetKey(context_id))
        .await
        .expect("target_for must succeed against the real codex app-server CLI");

    let caps = target.capabilities();
    assert!(
        caps.persistent_conversation,
        "test premise: CodexAppServerProtocol must claim persistent_conversation \
         for the conformance body to exercise anything"
    );
    assert!(
        caps.streaming_output,
        "test premise: codex claims REAL streaming (item/agentMessage/delta) -- \
         this run is also the live proof that a streaming driver satisfies the \
         SAME conformance() body with no special-casing"
    );

    conformance(&*target).await;
}

/// §5.3: the SAME process/native thread serves a human `send` and a follow-up
/// A2A `consult` (NOT a fresh spawn). codex's native id is SERVER-assigned (the
/// `thread/started`/`thread/start`-response thread.id, a UUIDv7) -- so the
/// proof is "the SAME server-assigned id is observed both times, and it looks
/// like a real codex thread id", not an independently-recomputed value.
#[tokio::test]
async fn same_process_and_native_thread_serve_a_human_send_and_an_a2a_consult() {
    let _home = isolated_codex_env("pid");
    let nonce = std::env::var("WASL_LIVE_NONCE").expect("WASL_LIVE_NONCE must be set");
    let context_id = format!("wasl-runtime-protocol-codex-live-pid-{nonce}");

    let provider = build_provider();
    let target = provider
        .target_for(&TargetKey(context_id.clone()))
        .await
        .expect("target_for must succeed against the real codex app-server CLI");

    let receipt1 = target
        .submit_turn(
            human_turn(&context_id, "Reply with exactly the word: PING_OK"),
            BusyPolicy::Reject,
        )
        .await;
    let turn1_id = match receipt1 {
        TurnReceipt::Started { turn_id, .. } => turn_id,
        other => panic!("expected Started for the human turn, got {other:?}"),
    };

    let events_after_1 = drain_until(
        target.events(EventSeq(0)).await,
        Duration::from_secs(240),
        |evs| completed_reply_for(evs, &turn1_id).is_some(),
    )
    .await;
    let native_1 = events_after_1
        .iter()
        .find_map(|e| match e {
            TurnEvent::SessionReady { native } => native.clone(),
            _ => None,
        })
        .unwrap_or_else(|| {
            panic!(
                "no SessionReady observed after turn 1 (turn_id {:?}) within the 240s \
                 hang-guard bound. Events actually observed: {}",
                turn1_id,
                describe_events(&events_after_1)
            )
        });
    // codex thread ids are UUIDv7 (e.g. 019fb381-27a4-76c3-... from the spike):
    // dash-segmented, 36 chars. Assert the shape without pinning the value.
    assert_eq!(
        native_1.0.len(),
        36,
        "codex native thread id is a 36-char UUID, got {:?}",
        native_1.0
    );
    assert_eq!(
        native_1.0.matches('-').count(),
        4,
        "codex native thread id is a dash-segmented UUID, got {:?}",
        native_1.0
    );

    let reply1 = completed_reply_for(&events_after_1, &turn1_id).unwrap_or_else(|| {
        panic!(
            "no TurnCompleted correlated to the human turn's OWN turn_id ({:?}) within the \
             240s hang-guard bound. Events actually observed: {}",
            turn1_id,
            describe_events(&events_after_1)
        )
    });
    assert!(
        reply1.contains("PING_OK"),
        "human turn's own reply did not contain PING_OK: {reply1:?}"
    );

    let receipt2 = target
        .submit_turn(
            a2a_turn(
                &context_id,
                "What exact word did I ask for? Answer with just that word.",
            ),
            BusyPolicy::Reject,
        )
        .await;
    let turn2_id = match receipt2 {
        TurnReceipt::Started { turn_id, .. } => turn_id,
        other => panic!("expected Started for the A2A turn, got {other:?}"),
    };
    assert_ne!(turn1_id, turn2_id, "the two turns must have distinct ids");

    let events_after_2 = drain_until(
        target.events(EventSeq(0)).await,
        Duration::from_secs(240),
        |evs| completed_reply_for(evs, &turn2_id).is_some(),
    )
    .await;
    let reply2 = completed_reply_for(&events_after_2, &turn2_id).unwrap_or_else(|| {
        panic!(
            "no TurnCompleted correlated to the A2A turn's OWN turn_id ({:?}) within the \
             240s hang-guard bound. Events actually observed: {}",
            turn2_id,
            describe_events(&events_after_2)
        )
    });
    assert!(
        reply2.contains("PING_OK"),
        "the SAME process must recall the human turn's token on the A2A turn \
         (session continuity, not a fresh spawn); got {reply2:?}"
    );

    // No second, DIFFERENT native thread id ever appeared (no re-spawn).
    for native in events_after_2.iter().filter_map(|e| match e {
        TurnEvent::SessionReady { native } => native.clone(),
        _ => None,
    }) {
        assert_eq!(
            native, native_1,
            "a second, DIFFERENT native thread id appeared -- this would mean \
             the A2A turn got a FRESH spawn instead of reusing the same live \
             process"
        );
    }
}

/// The streaming-differentiating property (vs claude): `TurnCompleted.reply`
/// carries the full final text even though codex's own `turn/start` JSON-RPC
/// response is only an inProgress ACK (the text arrives via streaming
/// `item/agentMessage/delta` notifications + the final agentMessage item in
/// `turn/completed`). Proves at least one AssistantDelta fired AND the reply is
/// the full answer.
#[tokio::test]
async fn assistant_delta_streams_and_turn_completed_carries_the_full_reply() {
    let _home = isolated_codex_env("stream");
    let nonce = std::env::var("WASL_LIVE_NONCE").expect("WASL_LIVE_NONCE must be set");
    let context_id = format!("wasl-runtime-protocol-codex-live-stream-{nonce}");

    let provider = build_provider();
    let target = provider
        .target_for(&TargetKey(context_id.clone()))
        .await
        .expect("target_for must succeed against the real codex app-server CLI");

    let receipt = target
        .submit_turn(
            human_turn(&context_id, "Reply with exactly the word: STREAM_OK"),
            BusyPolicy::Reject,
        )
        .await;
    let turn_id = match receipt {
        TurnReceipt::Started { turn_id, .. } => turn_id,
        other => panic!("expected Started, got {other:?}"),
    };

    let events = drain_until(
        target.events(EventSeq(0)).await,
        Duration::from_secs(240),
        |evs| completed_reply_for(evs, &turn_id).is_some(),
    )
    .await;

    let deltas: Vec<&str> = events
        .iter()
        .filter_map(|e| match e {
            TurnEvent::AssistantDelta { turn_id: t, text } if *t == turn_id => Some(text.as_str()),
            _ => None,
        })
        .collect();
    assert!(
        !deltas.is_empty(),
        "streaming_output claimed but no AssistantDelta was observed for this \
         turn's OWN turn_id -- codex streams via item/agentMessage/delta"
    );

    let reply = completed_reply_for(&events, &turn_id).unwrap_or_else(|| {
        panic!(
            "no TurnCompleted correlated to this turn's OWN turn_id ({:?}) within the \
             240s hang-guard bound. Events actually observed: {}",
            turn_id,
            describe_events(&events)
        )
    });
    assert!(
        reply.contains("STREAM_OK"),
        "TurnCompleted.reply must carry the full final text (codex's turn/start \
         response is only an inProgress ACK); got {reply:?}"
    );
}
