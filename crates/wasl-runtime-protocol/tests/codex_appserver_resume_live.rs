#![cfg(feature = "live-cli")]
//! Live-CLI proof that `CodexAppServerProtocol::resume` GENUINELY works across a
//! FRESH, SEPARATE OS process — closing a previously-ASSUMED gap.
//!
//! ==========================================================================
//! WHAT THIS PROVES (and why it is DISTINCT from `codex_appserver_live.rs`)
//! ==========================================================================
//! `codex_appserver_live.rs`'s
//! `same_process_and_native_thread_serve_a_human_send_and_an_a2a_consult`
//! proves SAME-process continuity: one live child serves two turns and recalls
//! context because the process never died. That does NOT exercise `resume` at
//! all — the driver's `resume` (implemented against the `thread/resume`
//! JSON-RPC method, schema-verified via `codex app-server
//! generate-json-schema`) had ONLY ever been exercised end-to-end by a
//! standalone throwaway Python probe
//! (`spikes/live-substrate/codex_appserver_resume_probe.py`, already run + PASSED:
//! R1_resume_succeeds=true, R2_context_retained=true) — never as a
//! regression-guarded test in the crate's real suite.
//!
//! This file makes that a PERMANENT proof: a thread started by one OS process,
//! after that process has FULLY EXITED (transport A dropped → child reaped), is
//! resumed by a genuinely FRESH `spawn`-equivalent `resume` call producing a
//! SECOND, SEPARATE OS process (transport B), and the resumed session recalls a
//! distinctive token planted BEFORE the first process exited. If resume were a
//! silent no-op / fresh-thread mint, R2's recall would fail — so this asserts
//! the real cross-process round-trip, not merely "resume returned Ok".
//!
//! ==========================================================================
//! DRIVER-SEAM DISCIPLINE (why direct `InteractiveProtocol`, not a managed
//! provider)
//! ==========================================================================
//! This drives `CodexAppServerProtocol` DIRECTLY as an `InteractiveProtocol`
//! (`spawn`/`resume` → `Box<dyn InteractiveTransport>`), then drives each
//! transport directly (`send_turn`/`next_event`) — exactly the R1 precedent
//! `crates/wasl-runtime/tests/provider_and_journal.rs`'s
//! `interactive_protocol_arc_forwards_capabilities_and_resume` establishes for
//! exercising `resume(&native, &opts)` against a protocol WITHOUT wrapping it in
//! a second full managed-provider. A managed provider mints a single live target
//! and reuses its process; it has no public API to force "kill the process, then
//! resume its native id in a brand-new process." The driver seam is the only
//! surface that expresses that, so this test speaks to it directly.
//!
//! ==========================================================================
//! COST + ENVIRONMENT
//! ==========================================================================
//! ⚠️  Costs REAL `codex` CLI model calls: 2 turns issued across 2 SEPARATE OS
//! processes (the plant turn on transport A, the recall turn on transport B).
//! Compiled/run ONLY under `--features live-cli`; default `cargo test` never
//! touches it, so it costs ZERO API usage in normal CI.
//!
//! Run: `cargo test -p wasl-runtime-protocol --features live-cli
//! codex_appserver_resume_live -- --test-threads=1`, with `WASL_LIVE_NONCE` set
//! to a fresh value.
//!
//! REAL-CLI ENVIRONMENT (identical contract to `codex_appserver_live.rs`): codex
//! app-server hard-requires (a) an ISOLATED, WRITABLE `CODEX_HOME` with a seeded
//! `config.toml` (its sqlite state/logs/goals DBs init HARD-FAIL otherwise) and
//! (b) `OPENAI_API_KEY` present (any value; the REAL auth is a header baked into
//! config.toml). The `isolated_codex_env`/`ensure_openai_api_key` helpers below
//! are copied VERBATIM from `codex_appserver_live.rs` so this file behaves
//! identically under the same env-var contract (`WASL_LIVE_NONCE`,
//! `WASL_CODEX_CONFIG_TOML`/`WASL_CODEX_HOME`, `OPENAI_API_KEY`). The MODEL/auth
//! CONTENT of config.toml is an ORCHESTRATOR/deployment responsibility supplied
//! via the ambient environment (`WASL_CODEX_CONFIG_TOML` — a path to a ready
//! config.toml to copy, or a pre-seeded `WASL_CODEX_HOME`), NEVER hardcoded here.
//! No real credential value appears anywhere in this file.

use std::path::PathBuf;
use std::time::Duration;

use wasl_runtime::{
    ComposedPrompt, InteractiveProtocol, InteractiveTransport, NativeSessionId, ProtocolEvent,
    SpawnOptions,
};
use wasl_runtime_protocol::CodexAppServerProtocol;

// =====================================================================
// Live-env helpers — copied VERBATIM from `codex_appserver_live.rs` so this
// file behaves identically under the same env-var contract. (This crate's
// `tests/support/mod.rs` is the scripted-fake-driver support layer, not the
// live-env helpers, so there is nothing shared to pull these from; the existing
// live file declares them inline and this one must too.)
// =====================================================================

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
    let nonce = std::env::var("WASL_LIVE_NONCE").expect(
        "WASL_LIVE_NONCE must be set (to a fresh value) to run codex_appserver_resume_live.rs",
    );

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

// =====================================================================
// Direct-transport drive helpers (the driver seam, not a managed provider).
// =====================================================================

/// A compact, human-readable rendering of a `ProtocolEvent` stream for
/// panic messages (so a timeout/failure reports what actually happened).
fn describe(events: &[ProtocolEvent]) -> String {
    if events.is_empty() {
        return "<no events observed at all>".to_string();
    }
    events
        .iter()
        .map(|e| match e {
            ProtocolEvent::SessionReady { native } => format!("SessionReady{{native:{native:?}}}"),
            ProtocolEvent::TurnAccepted { native_turn } => {
                format!("TurnAccepted{{native_turn:{native_turn:?}}}")
            }
            ProtocolEvent::AssistantDelta { text } => {
                format!("AssistantDelta{{{} chars}}", text.len())
            }
            ProtocolEvent::ToolStarted { name } => format!("ToolStarted{{{name}}}"),
            ProtocolEvent::ToolFinished { name, ok } => format!("ToolFinished{{{name}, ok:{ok}}}"),
            ProtocolEvent::NativeGateRequested { detail } => {
                format!("NativeGateRequested{{{}}}", detail.summary)
            }
            ProtocolEvent::TurnCompleted { reply } => {
                format!("TurnCompleted{{{} chars}}", reply.len())
            }
            ProtocolEvent::TurnFailed { error } => format!("TurnFailed{{{error}}}"),
            ProtocolEvent::ProcessExited { code } => format!("ProcessExited{{code:{code:?}}}"),
        })
        .collect::<Vec<_>>()
        .join(", ")
}

/// The outcome of driving ONE turn to a terminal event on a transport.
struct TurnOutcome {
    /// The native session id observed via `SessionReady` during the drive, if
    /// any (only the first spawn's transport observes it on the first turn).
    native: Option<NativeSessionId>,
    /// The accumulated `AssistantDelta` text + the `TurnCompleted` reply — the
    /// final answer text (codex streams deltas AND carries the full reply in
    /// `TurnCompleted`). Concatenating both is robust to either shape.
    reply: String,
    /// All events observed (for diagnostics on failure).
    events: Vec<ProtocolEvent>,
}

/// Drive a transport (that has ALREADY had `send_turn` called on it) by polling
/// `next_event()` until the turn reaches a terminal event (`TurnCompleted`,
/// `TurnFailed`, or `ProcessExited`) or the bounded timeout elapses. Captures
/// any `SessionReady` native id seen along the way and the assistant text.
///
/// Panics with a clear, event-dump-carrying message on timeout, `TurnFailed`,
/// or a premature `ProcessExited`.
async fn drive_turn_to_terminal(
    transport: &mut Box<dyn InteractiveTransport>,
    bound: Duration,
    phase: &str,
) -> TurnOutcome {
    let deadline = tokio::time::Instant::now() + bound;
    let mut events: Vec<ProtocolEvent> = Vec::new();
    let mut native: Option<NativeSessionId> = None;
    let mut accumulated = String::new();

    loop {
        let remaining = deadline.saturating_duration_since(tokio::time::Instant::now());
        if remaining.is_zero() {
            panic!(
                "[{phase}] timed out after {bound:?} waiting for a terminal event. \
                 Events observed: {}",
                describe(&events)
            );
        }
        match tokio::time::timeout(remaining, transport.next_event()).await {
            Ok(Some(event)) => {
                match &event {
                    ProtocolEvent::SessionReady { native: n } => {
                        if let Some(n) = n {
                            native = Some(n.clone());
                        }
                    }
                    ProtocolEvent::AssistantDelta { text } => {
                        accumulated.push_str(text);
                    }
                    ProtocolEvent::TurnCompleted { reply } => {
                        events.push(event.clone());
                        // Prefer the full reply if present; otherwise fall back
                        // to the accumulated deltas.
                        let reply = if reply.is_empty() {
                            accumulated.clone()
                        } else {
                            format!("{accumulated}{reply}")
                        };
                        return TurnOutcome {
                            native,
                            reply,
                            events,
                        };
                    }
                    ProtocolEvent::TurnFailed { error } => {
                        events.push(event.clone());
                        panic!(
                            "[{phase}] the turn FAILED inside the session: {error}. \
                             Events observed: {}",
                            describe(&events)
                        );
                    }
                    ProtocolEvent::ProcessExited { code } => {
                        events.push(event.clone());
                        panic!(
                            "[{phase}] the process EXITED (code {code:?}) before the turn \
                             completed. Events observed: {}",
                            describe(&events)
                        );
                    }
                    _ => {}
                }
                events.push(event);
            }
            Ok(None) => {
                // A quiescent `None` while the turn is still in flight: keep
                // polling until the bounded deadline (the driver returns `None`
                // on a bounded idle breather, not necessarily turn-end).
                continue;
            }
            Err(_) => {
                panic!(
                    "[{phase}] timed out after {bound:?} on next_event() waiting for a \
                     terminal event. Events observed: {}",
                    describe(&events)
                );
            }
        }
    }
}

/// A generous, bounded per-turn wait (real model calls across a fresh process).
const TURN_BOUND: Duration = Duration::from_secs(60);

// =====================================================================
// THE test: cross-process resume recalls pre-exit conversational context.
// =====================================================================

/// A thread started by one OS process is resumed by a FRESH, SEPARATE OS
/// process (after the original has fully exited) and the resumed session recalls
/// a distinctive token planted before the exit.
///
/// Fresh token per run (derived from `WASL_LIVE_NONCE`) so a stale-context echo
/// from a previous run can never produce a false positive.
#[tokio::test]
async fn resume_in_a_fresh_process_recalls_pre_exit_context() {
    let _home = isolated_codex_env("resume");
    let nonce = std::env::var("WASL_LIVE_NONCE").expect("WASL_LIVE_NONCE must be set");

    // A distinctive, run-unique token. Uppercased + dash-joined so it survives
    // as a single recognizable substring in the model's reply.
    let token = format!("RESUME-TOKEN-{}", nonce.to_uppercase());

    let protocol = CodexAppServerProtocol::new("codex");

    // ---- Process A: spawn, plant the token, drive to terminal, then EXIT. ----
    let mut transport_a = protocol
        .spawn(&SpawnOptions::default())
        .await
        .expect("spawn transport A must succeed against the real codex app-server CLI");

    let plant_prompt = ComposedPrompt(format!(
        "Remember this exact token for later: {token}. Do not use any tools. \
         Reply with just: OK"
    ));
    transport_a
        .send_turn(&plant_prompt)
        .await
        .expect("send_turn on transport A (the plant turn) must be accepted by the CLI");

    let plant = drive_turn_to_terminal(&mut transport_a, TURN_BOUND, "process-A/plant").await;

    let native = plant.native.clone().unwrap_or_else(|| {
        panic!(
            "process A never emitted SessionReady with a native id -- cannot resume \
             without the server-assigned thread id. Events observed: {}",
            describe(&plant.events)
        )
    });
    // codex thread ids are UUIDv7 (dash-segmented, 36 chars). Assert the shape
    // (not the value) so a garbage/empty id is caught before we try to resume it.
    assert_eq!(
        native.0.len(),
        36,
        "codex native thread id must be a 36-char UUID, got {:?}",
        native.0
    );
    assert_eq!(
        native.0.matches('-').count(),
        4,
        "codex native thread id must be a dash-segmented UUID, got {:?}",
        native.0
    );

    // ---- Fully exit process A: drop the transport, then wait out the exit. ----
    // Dropping the transport triggers the driver's reap-on-drop (kill signal +
    // kill_on_drop kernel backstop + detached supervisor wait). The bounded
    // wait after the drop closes any race where the original process might still
    // be exiting before `resume` spawns a fresh one.
    drop(transport_a);
    tokio::time::sleep(Duration::from_millis(750)).await;

    // ---- Process B: resume the native thread in a FRESH, SEPARATE process. ----
    let resume_result = protocol.resume(&native, &SpawnOptions::default()).await;
    let mut transport_b = match resume_result {
        Ok(t) => t,
        Err(e) => panic!(
            "resume of native thread {:?} in a fresh process returned Err (this is the \
             gap the test exists to catch): {e}",
            native.0
        ),
    };

    let recall_prompt = ComposedPrompt(
        "What token did I ask you to remember? Reply with just the token, nothing else, \
         no tools."
            .to_string(),
    );
    transport_b
        .send_turn(&recall_prompt)
        .await
        .expect("send_turn on transport B (the recall turn) must be accepted by the CLI");

    let recall = drive_turn_to_terminal(&mut transport_b, TURN_BOUND, "process-B/recall").await;

    assert!(
        recall.reply.contains(&token),
        "the resumed (fresh-process) session did NOT recall the pre-exit token. \
         Expected reply to contain {token:?}, got {:?}. Events observed: {}",
        recall.reply,
        describe(&recall.events)
    );

    drop(transport_b);
}
