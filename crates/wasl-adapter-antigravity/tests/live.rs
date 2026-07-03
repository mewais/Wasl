//! REAL Antigravity CLI (`agy`) end-to-end checks — behind `--features
//! live-cli`, EXCLUDED from default test runs (gated with `#![cfg(feature =
//! "live-cli")]`, a cargo feature, NOT `#[ignore]`). Spends real tokens
//! against the actual `agy` binary; run explicitly with
//! `cargo test -p wasl-adapter-antigravity --features live-cli`.
//!
//! Per `docs/specs/adapter-antigravity.md` ("Test plan / tests/live.rs"),
//! this crate's contract is RESEARCHED-NOT-LIVE-VERIFIED (agy 1.0.15,
//! 2026-07-02): not one `agy -p` invocation was ever run before this suite.
//! Two tests, in the spec's mandated order, 3 model calls total:
//!
//! 1. `permission_probe` — THE FIRST-PRIORITY TEST, resolving THE PERMISSION
//!    QUESTION (is `-p` mode headless-safe WITHOUT
//!    `--dangerously-skip-permissions`?). Drives `ProcessCliInvoker` DIRECTLY
//!    (not the adapter). Asserts ONLY that the invocation terminates within a
//!    generous test-side bound — it must NOT assert which of the trichotomy
//!    (tool ran / tool denied but turn completed / hung to --print-timeout)
//!    occurred, since that is exactly the unverified thing this probe exists
//!    to observe. The outcome is printed for the orchestrator to fold back
//!    into the crate AGENTS.md contract table and the spec's interim-decision
//!    NOTE.
//! 2. `two_turn_continuity` — the adapter proper, 2 calls, `DEFAULT_MODEL`
//!    ("Gemini 3.5 Flash (Low)", the free-tier cheapest entry — this run
//!    doubles as the `--model` display-string verification). Confirms
//!    `--conversation` continuity AND a byte-stable `conversation_of` (the
//!    designed detector for the resume-argv-rejection failure mode: a blind
//!    retry would mint a NEW id and lose the word).
//!
//! Probe phrasing follows the house probe-design rule (see
//! `scripts/live-verify.sh`'s `RAND_WORD` comment): bare "reply with exactly:
//! <token>" phrasing has been OBSERVED live to trip prompt-injection
//! heuristics and get refused on at least one sibling CLI. So, like the
//! shell harness's tier probes, `two_turn_continuity` phrases its token as a
//! mundane build-tag statement/question, never as an imperative "reply with
//! exactly" instruction.
//!
//! UPDATED 2026-07-03 (W8'-fix, orchestrator live verification): the
//! orchestrator's first-ever live `agy -p` runs found the original argv
//! contract wrong (see `docs/specs/adapter-antigravity.md`'s "W8'-fix
//! correction"). `two_turn_continuity` drives the adapter via `deliver`, so
//! it automatically picked up the corrected `build_argv` shape with no
//! changes needed here. `permission_probe` builds its argv by hand and has
//! been fixed to match (`-p` + prompt moved to the strictly-last two
//! positions) — this is a test-code correctness fix, not a live re-run; the
//! orchestrator is responsible for re-running `--features live-cli`.

#![cfg(feature = "live-cli")]

use std::time::Duration;

use wasl_adapter::{AgentAdapter, Delivery, DeliveryKind};
use wasl_adapter_antigravity::{AntigravityAdapter, CliInvoker, ProcessCliInvoker, DEFAULT_MODEL};

/// Cheap, dependency-free structural check that `s` looks like a UUID
/// (8-4-4-4-12 hex groups). Deliberately NOT a full RFC-4122 parse (this
/// crate carries no `uuid` dependency by design — agy mints its own
/// conversation ids, we never construct or validate them beyond "did the CLI
/// hand us something id-shaped").
fn looks_like_uuid(s: &str) -> bool {
    let groups: Vec<&str> = s.split('-').collect();
    let expected_lens = [8, 4, 4, 4, 12];
    groups.len() == expected_lens.len()
        && groups
            .iter()
            .zip(expected_lens.iter())
            .all(|(g, len)| g.len() == *len && g.chars().all(|c| c.is_ascii_hexdigit()))
}

/// THE FIRST-PRIORITY live test — resolves THE PERMISSION QUESTION.
///
/// argv is built by hand (not `build_argv`, which always includes
/// `--dangerously-skip-permissions`): deliberately WITHOUT the skip flag, a
/// short explicit `--print-timeout`, on a fresh workspace with no pre-seeded
/// permission rules, asking for a shell command (a tool call agy must decide
/// whether to run headlessly).
///
/// CORRECTED 2026-07-03 (W8'-fix, orchestrator live verification — see
/// `docs/specs/adapter-antigravity.md`'s "W8'-fix correction"): `-p` and its
/// prompt are now the strictly-last two argv elements (a live run showed `-p`
/// consumes the very next argv token as its value, so placing it before other
/// flags is a bug, not a stylistic choice). `--output-format json` is
/// intentionally OMITTED here: this probe only checks a raw stdout substring
/// (`agy_probe_hi`), not the JSON contract, so plain-text output is fine.
#[tokio::test]
async fn permission_probe() {
    let inv = ProcessCliInvoker;
    let argv: Vec<String> = vec![
        "agy".to_string(),
        "--model".to_string(),
        DEFAULT_MODEL.to_string(),
        "--sandbox".to_string(),
        "--print-timeout".to_string(),
        "45s".to_string(),
        "-p".to_string(),
        "run the shell command: echo agy_probe_hi".to_string(),
    ];

    // Test-side bound: generous relative to the explicit 45s --print-timeout,
    // so a hang-to-timeout resolves within the bound rather than tripping it.
    let outcome = tokio::time::timeout(Duration::from_secs(90), inv.invoke(&argv, "")).await;

    let result = outcome.expect(
        "permission_probe TRICHOTOMY (c): agy did NOT terminate within the 90s test-side bound \
         — most likely hung on an interactive permission prompt with no TTY. This CONFIRMS the \
         skip flag is mandatory for tool-touching deliveries (the spec's interim decision, now \
         observed rather than assumed).",
    );

    match result {
        Ok(stdout) => {
            if stdout.contains("agy_probe_hi") {
                println!(
                    "permission_probe TRICHOTOMY (a): the shell command RAN (headless \
                     auto-allow) — alarming; the skip flag is moot for this delivery but the \
                     --sandbox flag is load-bearing. stdout:\n{stdout}"
                );
            } else {
                println!(
                    "permission_probe TRICHOTOMY (b): the tool call was denied but the turn \
                     completed (exit 0, no probe output) — auto-deny. The skip flag is \
                     confirmed necessary for tool-touching deliveries. stdout:\n{stdout}"
                );
            }
        }
        Err(e) => {
            println!(
                "permission_probe TRICHOTOMY (b, as a non-zero-exit/spawn error rather than a \
                 hang): the turn did not hang, but did not succeed either: {e}"
            );
        }
    }
}

/// The adapter proper: 2 calls in the SAME context, proving `--conversation`
/// continuity + a byte-stable learned `conversation_id` through the public
/// `AntigravityAdapter` API.
#[tokio::test]
async fn two_turn_continuity() {
    // A caller-supplied nonce keeps the context (and thus the agy
    // conversation the adapter learns) FRESH across reruns.
    let nonce = std::env::var("WASL_LIVE_NONCE")
        .expect("WASL_LIVE_NONCE must be set (to a fresh value) to run tests/live.rs");
    let context_id = format!("wasl-adapter-antigravity-live-{nonce}");

    // House probe-design rule: a mundane build-tag statement, never a bare
    // "reply with exactly: <token>" imperative.
    let tag = format!("build-{nonce}");

    let adapter = AntigravityAdapter::new(ProcessCliInvoker);

    // First turn: fresh context -> Turn::First (`agy -p --model
    // "Gemini 3.5 Flash (Low)" --sandbox --dangerously-skip-permissions
    // "<prompt>"`).
    let opening = Delivery {
        context_id: context_id.clone(),
        from: "wasl-live-test".to_string(),
        kind: DeliveryKind::Consult,
        text: format!("Today's build tag is {tag}. Please acknowledge."),
        data: vec![],
    };
    let reply1 = adapter
        .deliver(&opening)
        .await
        .expect("first turn should succeed against the real agy CLI");
    assert!(
        reply1.contains(&tag),
        "first-turn reply did not contain the build tag {tag}: {reply1}"
    );

    let cid = adapter
        .conversation_of(&context_id)
        .expect("adapter should have learned an agy conversation_id from the print-mode result");
    assert!(
        looks_like_uuid(&cid),
        "learned conversation_id does not look like a UUID: {cid}"
    );

    // Second turn, SAME context -> Turn::Continuation(cid) (`agy
    // --conversation <cid> -p --model "Gemini 3.5 Flash (Low)" --sandbox
    // --dangerously-skip-permissions "<prompt>"`). If `--conversation` did
    // not actually preserve context (or was rejected, triggering the blind
    // fresh-conversation retry), agy would have no memory of the build tag
    // and/or `conversation_of` would have changed.
    let followup = Delivery {
        context_id: context_id.clone(),
        from: "wasl-live-test".to_string(),
        kind: DeliveryKind::Consult,
        text: "What is today's build tag? Reply with just the tag.".to_string(),
        data: vec![],
    };
    let reply2 = adapter
        .deliver(&followup)
        .await
        .expect("second (resumed) turn should succeed against the real agy CLI");
    assert!(
        reply2.contains(&tag),
        "resumed-turn reply did not contain the build tag {tag} (--conversation continuity \
         broken, or a blind retry silently started a fresh conversation): {reply2}"
    );

    // The conversation id must be BYTE-STABLE across the resumed turn: same
    // cid before and after. A changed cid here is the designed detector for
    // the resume-argv-rejection failure mode (--sandbox/--dangerously-
    // skip-permissions rejected alongside --conversation), which would have
    // silently triggered the blind fresh-conversation retry.
    let cid_after = adapter
        .conversation_of(&context_id)
        .expect("conversation_id should still be present after the resumed turn");
    assert_eq!(
        cid_after, cid,
        "conversation_id changed across a resumed turn (expected byte-stable); this is the \
         resume-argv-rejection failure mode the spec calls out"
    );
}
