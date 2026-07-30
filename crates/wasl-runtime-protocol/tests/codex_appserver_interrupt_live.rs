#![cfg(feature = "live-cli")]
//! Live-CLI proof that `CodexAppServerProtocol`'s interrupt support GENUINELY
//! aborts an in-flight, tool-running turn well before its natural completion —
//! at the REAL DRIVER level, not just the raw JSON-RPC probe.
//!
//! ==========================================================================
//! WHAT THIS PROVES (and why it is DISTINCT from `codex_appserver_live.rs`)
//! ==========================================================================
//! `codex_appserver_live.rs`'s `conformance()` body has an
//! `if caps.interruptible { ... cancel_turn ... assert Ok ... }` branch that,
//! now that codex reports `interruptible: true`, exercises `cancel_turn` and
//! asserts it returns `Ok`. That proves the cancel PATH returns cleanly — it
//! does NOT prove the interrupt genuinely stopped the model MID-FLIGHT rather
//! than after the turn would have finished anyway. The distinction only bites on
//! a turn that would otherwise take a LONG time.
//!
//! This file makes the genuine-abort a PERMANENT, driver-level proof — the same
//! proof shape as the standalone Python probe
//! (`spikes/live-substrate/codex_appserver_interrupt_probe.py`, already run +
//! PASSED: I1_interrupt_accepted=true, I2_turn_actually_stopped_early=true,
//! `out/codex_appserver_interrupt/interrupt_verdict.json`), but through the
//! actual Rust `CodexAppServerProtocol` driver/transport instead of hand-rolled
//! JSON-RPC: we ask codex to run a genuinely slow `sleep` shell command via its
//! tool-call mechanism, wait until we observe the tool call ACTUALLY START
//! (`ProtocolEvent::ToolStarted` — proof the sleep has begun executing, not just
//! been requested), fire `transport.interrupt()`, and assert the turn reaches a
//! terminal event in SIGNIFICANTLY LESS time than the sleep's natural duration.
//! If the interrupt were a silent no-op, the terminal event would only arrive
//! after the full sleep — so the TIMING is the real proof, not merely "some
//! terminal event eventually arrived".
//!
//! ==========================================================================
//! R1 EVENT-VOCABULARY REASONING (why the terminal is `TurnFailed`/`TurnCompleted`)
//! ==========================================================================
//! R1's internal `ProtocolEvent` (`wasl-runtime/src/lib.rs` §6) has NO dedicated
//! "cancelled" / "interrupted" variant. Its only turn-terminal variants are
//! `TurnCompleted`, `TurnFailed`, and `ProcessExited`. Therefore a driver-level
//! interrupted outcome MUST surface as one of those — codex reports
//! `turn/completed` with `status:"interrupted"`, which the driver maps onto the
//! R1 vocabulary. This test asserts the outcome is a genuine turn-terminal
//! (`TurnCompleted` or `TurnFailed`, NOT a premature `ProcessExited`) AND that
//! the timing proves an early abort. It does NOT read the driver's `src` to
//! shape which of the two the mapping picks — it accepts either turn-terminal
//! and leans the whole proof on the timing bound.
//!
//! ==========================================================================
//! DRIVER-SEAM DISCIPLINE (why direct `InteractiveProtocol`, not a managed
//! provider)
//! ==========================================================================
//! This drives `CodexAppServerProtocol` DIRECTLY as an `InteractiveProtocol`
//! (`spawn` → `Box<dyn InteractiveTransport>`), then drives the transport
//! directly (`send_turn`/`next_event`/`interrupt`) — the same driver-seam
//! precedent `codex_appserver_resume_live.rs` uses. A managed provider's public
//! API exposes `cancel_turn` (already exercised by the conformance branch) but
//! not the raw "wait for ToolStarted, then call `transport.interrupt()` and
//! measure the elapsed-to-terminal" shape this timing proof needs; the driver
//! seam is the only surface that expresses it, so this test speaks to it
//! directly.
//!
//! ==========================================================================
//! COST + ENVIRONMENT
//! ==========================================================================
//! ⚠️  Costs a REAL `codex` CLI model call: ONE turn that issues a real shell
//! tool call (a genuine `sleep`), interrupted mid-flight. Compiled/run ONLY
//! under `--features live-cli`; default `cargo test` never touches it, so it
//! costs ZERO API usage in normal CI.
//!
//! Run: `cargo test -p wasl-runtime-protocol --features live-cli
//! codex_appserver_interrupt_live -- --test-threads=1`, with `WASL_LIVE_NONCE`
//! set to a fresh value.
//!
//! REAL-CLI ENVIRONMENT (identical contract to `codex_appserver_live.rs` /
//! `codex_appserver_resume_live.rs`): codex app-server hard-requires (a) an
//! ISOLATED, WRITABLE `CODEX_HOME` with a seeded `config.toml` (its sqlite
//! state/logs/goals DBs init HARD-FAIL otherwise) and (b) `OPENAI_API_KEY`
//! present (any value; the REAL auth is a header baked into config.toml). The
//! `isolated_codex_env`/`ensure_openai_api_key` helpers below are copied VERBATIM
//! from those files so this file behaves identically under the same env-var
//! contract (`WASL_LIVE_NONCE`, `WASL_CODEX_CONFIG_TOML`/`WASL_CODEX_HOME`,
//! `OPENAI_API_KEY`). The MODEL/auth CONTENT of config.toml is an
//! ORCHESTRATOR/deployment responsibility supplied via the ambient environment
//! (`WASL_CODEX_CONFIG_TOML` — a path to a ready config.toml to copy, or a
//! pre-seeded `WASL_CODEX_HOME`), NEVER hardcoded here. No real credential value
//! appears anywhere in this file.
//!
//! NOTE the interrupt must genuinely run under codex's DEFAULT approval/sandbox
//! policy so the shell `sleep` tool call executes WITHOUT blocking on an
//! interactive approval gate (the driver never sets `approvalPolicy`/`sandbox`,
//! so codex falls back to its own/config.toml default — the same premise under
//! which the Python probe observed the tool call start and be interrupted). The
//! orchestrator-supplied config.toml is responsible for a default under which a
//! shell tool call proceeds unattended.

use std::path::PathBuf;
use std::time::Duration;

use wasl_runtime::{
    ComposedPrompt, InteractiveProtocol, InteractiveTransport, ProtocolEvent, SpawnOptions,
};
use wasl_runtime_protocol::CodexAppServerProtocol;

// =====================================================================
// Live-env helpers — copied VERBATIM from `codex_appserver_live.rs` /
// `codex_appserver_resume_live.rs` so this file behaves identically under the
// same env-var contract. (This crate's `tests/support/mod.rs` is the
// scripted-fake-driver support layer, not the live-env helpers, so there is
// nothing shared to pull these from; the existing live files declare them inline
// and this one must too.)
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
        "WASL_LIVE_NONCE must be set (to a fresh value) to run codex_appserver_interrupt_live.rs",
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
// Direct-transport diagnostics (the driver seam, not a managed provider) —
// the same panic-message discipline as `codex_appserver_resume_live.rs`'s
// `describe()` helper.
// =====================================================================

/// A compact, human-readable rendering of a `ProtocolEvent` stream for panic
/// messages (so a timeout/failure reports what actually happened). Copied in the
/// same shape as `codex_appserver_resume_live.rs::describe` — test binaries in
/// this crate cannot share code via `mod` across separate integration-test
/// binaries, and the existing frozen `describe()` lives in a different file this
/// task must not edit, so it is duplicated here.
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

// =====================================================================
// Timing bounds. The sleep is comfortably longer than the interrupt
// round-trip (~30ms in the probe) but short enough that a FAILED interrupt does
// not waste excessive live-CLI time waiting the whole thing out; the overall
// test deadline is a hard ceiling so a never-interrupting scenario cannot hang
// forever.
// =====================================================================

/// The genuinely-slow shell tool the turn asks codex to run. 25s is far longer
/// than the ~30ms interrupt round-trip the probe measured, so a mid-flight
/// interrupt lands with a huge margin, yet short enough that a totally-failed
/// interrupt is bounded (the test deadline below caps it anyway).
const SLEEP_SECS: u64 = 25;

/// The early-abort proof bound: a genuine interrupt lands the terminal well
/// under this (the probe saw ~4.3s total). If the interrupt were a no-op, the
/// terminal would only arrive after the full `SLEEP_SECS` (~25s) — so a terminal
/// observed in < 20s cannot be explained by the sleep having simply finished.
/// The comfortable margin below 25s is deliberate.
const EARLY_ABORT_BOUND: Duration = Duration::from_secs(20);

/// Hard overall ceiling on waiting for the tool call to START. Generous enough
/// for a real model to decide to call the shell tool, bounded so a model that
/// never issues the tool call fails the test instead of hanging.
const TOOL_START_BOUND: Duration = Duration::from_secs(45);

/// Hard overall ceiling on waiting for the terminal AFTER the interrupt is
/// sent. Comfortably above the sleep's natural duration so that even a fully
/// FAILED interrupt terminates the test (as a FAILURE, via the timing assert)
/// rather than hanging — but the early-abort assert fires long before this on a
/// working interrupt.
const POST_INTERRUPT_BOUND: Duration = Duration::from_secs(40);

// =====================================================================
// THE test: a driver-level interrupt genuinely aborts an in-flight,
// tool-running turn well before its natural completion.
// =====================================================================

/// Ask codex to run a real `sleep 25` shell command via its tool-call
/// mechanism; once the tool call is observed to ACTUALLY START, interrupt the
/// turn and assert it reaches a terminal event far sooner than the 25s the sleep
/// would naturally take. This is the driver-level analog of the Python probe's
/// I1 (interrupt accepted) + I2 (turn actually stopped early).
#[tokio::test]
async fn interrupt_aborts_an_in_flight_tool_running_turn_early() {
    let _home = isolated_codex_env("interrupt");

    let protocol = CodexAppServerProtocol::new("codex");
    let mut transport: Box<dyn InteractiveTransport> = protocol
        .spawn(&SpawnOptions::default())
        .await
        .expect("spawn transport must succeed against the real codex app-server CLI");

    // A prompt that forces codex to actually EXECUTE a slow shell command via
    // its tool-call mechanism (not just describe one). The `sleep` genuinely
    // blocks the tool call for SLEEP_SECS, giving a wide, unambiguous window in
    // which to interrupt mid-flight.
    let prompt = ComposedPrompt(format!(
        "Run this exact shell command using your shell tool, and wait for it to \
         finish before replying: sleep {SLEEP_SECS}. After it finishes, reply with \
         just: done"
    ));
    transport
        .send_turn(&prompt)
        .await
        .expect("send_turn issuing the slow shell tool call must be accepted by the CLI");

    // ---- Phase 1: poll until the tool call ACTUALLY STARTS. ----
    // `ToolStarted` is our signal that the sleep has genuinely begun executing
    // (not merely been requested) — the driver-level counterpart of the probe's
    // `tool_started_at_rel`. We record when the turn started so the final
    // early-abort assert measures elapsed-since-turn-start, exactly like the
    // probe's `completed_at_rel`.
    let turn_started_at = tokio::time::Instant::now();
    let tool_start_deadline = turn_started_at + TOOL_START_BOUND;

    let mut events: Vec<ProtocolEvent> = Vec::new();
    loop {
        let remaining = tool_start_deadline.saturating_duration_since(tokio::time::Instant::now());
        if remaining.is_zero() {
            panic!(
                "timed out after {TOOL_START_BOUND:?} waiting for the shell tool call to \
                 START (ProtocolEvent::ToolStarted) -- cannot prove a mid-flight interrupt \
                 without the sleep actually running. Events observed: {}",
                describe(&events)
            );
        }
        match tokio::time::timeout(remaining, transport.next_event()).await {
            Ok(Some(event)) => {
                match &event {
                    ProtocolEvent::ToolStarted { .. } => {
                        events.push(event);
                        break;
                    }
                    // A terminal BEFORE the tool even started means the turn
                    // ended without running the sleep — the test premise (a
                    // genuinely in-flight tool call to interrupt) never held.
                    ProtocolEvent::TurnCompleted { .. }
                    | ProtocolEvent::TurnFailed { .. }
                    | ProtocolEvent::ProcessExited { .. } => {
                        events.push(event);
                        panic!(
                            "the turn reached a terminal event BEFORE the shell tool call \
                             ever started -- codex did not run the `sleep {SLEEP_SECS}` tool \
                             call, so there was no in-flight tool-running turn to interrupt. \
                             Events observed: {}",
                            describe(&events)
                        );
                    }
                    _ => {
                        events.push(event);
                    }
                }
            }
            // A quiescent `None` while the turn is in flight: keep polling until
            // the bounded deadline (the driver returns `None` on a bounded idle
            // breather, not necessarily turn-end).
            Ok(None) => continue,
            Err(_) => {
                panic!(
                    "timed out after {TOOL_START_BOUND:?} on next_event() waiting for the \
                     shell tool call to START. Events observed: {}",
                    describe(&events)
                );
            }
        }
    }

    // ---- Phase 2: interrupt the genuinely in-flight tool-running turn. ----
    // Fire the interrupt IMMEDIATELY after observing ToolStarted and record the
    // timestamp. `interrupt()` returning Err is a hard failure surfaced loudly
    // (not swallowed): the whole point is that the driver's interrupt path works.
    let interrupt_sent_at = tokio::time::Instant::now();
    transport.interrupt().await.unwrap_or_else(|e| {
        panic!(
            "transport.interrupt() returned Err on a genuinely in-flight, tool-running turn \
             (this is the failure this test exists to catch): {e}. Events observed: {}",
            describe(&events)
        )
    });

    // ---- Phase 3: poll until a TURN-terminal, measuring elapsed time. ----
    // R1 has no dedicated "cancelled" ProtocolEvent, so an interrupted outcome
    // surfaces as TurnCompleted or TurnFailed (codex's status:"interrupted"
    // turn/completed mapped onto R1's vocabulary). A premature ProcessExited
    // (the child dying) is NOT an acceptable interrupt outcome here.
    let post_interrupt_deadline = interrupt_sent_at + POST_INTERRUPT_BOUND;
    let terminal = loop {
        let remaining =
            post_interrupt_deadline.saturating_duration_since(tokio::time::Instant::now());
        if remaining.is_zero() {
            panic!(
                "timed out after {POST_INTERRUPT_BOUND:?} waiting for a terminal event AFTER \
                 sending interrupt() -- the interrupt did not stop the turn (it would have run \
                 the full `sleep {SLEEP_SECS}` and beyond). Elapsed since turn start: {:?}. \
                 Events observed: {}",
                turn_started_at.elapsed(),
                describe(&events)
            );
        }
        match tokio::time::timeout(remaining, transport.next_event()).await {
            Ok(Some(event)) => {
                let is_terminal = matches!(
                    event,
                    ProtocolEvent::TurnCompleted { .. }
                        | ProtocolEvent::TurnFailed { .. }
                        | ProtocolEvent::ProcessExited { .. }
                );
                events.push(event.clone());
                if is_terminal {
                    break event;
                }
            }
            Ok(None) => continue,
            Err(_) => {
                panic!(
                    "timed out after {POST_INTERRUPT_BOUND:?} on next_event() waiting for a \
                     terminal event AFTER interrupt(). Elapsed since turn start: {:?}. \
                     Events observed: {}",
                    turn_started_at.elapsed(),
                    describe(&events)
                );
            }
        }
    };

    let elapsed_since_turn_start = turn_started_at.elapsed();

    // (b) THE REAL PROOF: the terminal arrived far sooner than the sleep's
    // natural duration. A no-op interrupt would only terminate after the full
    // ~25s sleep; observing a terminal under 20s cannot be explained by the
    // sleep having simply finished on its own.
    assert!(
        elapsed_since_turn_start < EARLY_ABORT_BOUND,
        "the turn did NOT stop early: {elapsed_since_turn_start:?} elapsed since turn start \
         (bound {EARLY_ABORT_BOUND:?}). A genuine interrupt aborts a `sleep {SLEEP_SECS}` \
         tool call in well under this; taking this long means the interrupt was a no-op and \
         the sleep ran (near) to completion. Terminal: {}. Events observed: {}",
        describe(std::slice::from_ref(&terminal)),
        describe(&events)
    );

    // (c) The terminal must be a genuine TURN terminal (TurnCompleted or
    // TurnFailed), NOT a premature ProcessExited (which would mean the child
    // died rather than the turn being cleanly interrupted). codex maps its
    // status:"interrupted" turn/completed onto one of these two; R1 offers no
    // third turn-terminal, so an interrupted outcome MUST be one of them.
    match &terminal {
        ProtocolEvent::TurnFailed { error } => {
            // codex reports status:"interrupted", which the driver surfaces as a
            // failed turn; its error text must be non-empty/sensible, not blank.
            assert!(
                !error.trim().is_empty(),
                "interrupted turn surfaced as TurnFailed but with an empty/blank error text; \
                 an interrupt outcome must carry a sensible diagnostic. Events observed: {}",
                describe(&events)
            );
        }
        ProtocolEvent::TurnCompleted { .. } => {
            // Also acceptable: the driver may map the interrupted turn onto a
            // completed turn. The early-abort timing above is what proves it was
            // genuinely interrupted, not run to natural completion.
        }
        ProtocolEvent::ProcessExited { code } => {
            panic!(
                "interrupt produced a premature ProcessExited (code {code:?}) instead of a \
                 clean turn-terminal (TurnCompleted/TurnFailed) -- the child died rather than \
                 the turn being cleanly interrupted. Events observed: {}",
                describe(&events)
            );
        }
        other => {
            panic!(
                "unreachable: the terminal loop only breaks on a turn/process terminal, got \
                 {}. Events observed: {}",
                describe(std::slice::from_ref(other)),
                describe(&events)
            );
        }
    }

    drop(transport);
}
