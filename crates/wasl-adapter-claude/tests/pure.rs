//! Genuine unit coverage of the pure core: `compose_prompt`, `session_uuid`
//! (+ `WASL_SESSION_NS`), `build_argv`, `parse_reply`. These test only the public
//! API + `wasl_adapter` public types + the `uuid` crate (dev-dependency, used
//! only to construct/compare expected values, never to reimplement the
//! behavior under test).
//!
//! v2 contract (see `docs/specs/adapter-claude.md`): claude session ids MUST be
//! UUIDs, deterministically derived from the A2A `context_id` via UUIDv5 under a
//! fixed namespace; `build_argv` now takes that UUID plus a `Turn` (First uses
//! `--session-id`, Continuation uses `--resume`) instead of a raw session-id
//! string.

use uuid::Uuid;
use wasl_adapter::{Delivery, DeliveryKind};
use wasl_adapter_claude::{
    build_argv, compose_prompt, parse_reply, session_uuid, Turn, WASL_SESSION_NS,
};

fn delivery(kind: DeliveryKind, from: &str, text: &str, data: Vec<serde_json::Value>) -> Delivery {
    Delivery {
        context_id: "ctx-1".to_string(),
        from: from.to_string(),
        kind,
        text: text.to_string(),
        data,
    }
}

// ---------------------------------------------------------------------------
// compose_prompt (contract unchanged from v1)
// ---------------------------------------------------------------------------

#[test]
fn compose_prompt_consult() {
    let d = delivery(DeliveryKind::Consult, "alice", "what is 2+2?", vec![]);
    assert_eq!(compose_prompt(&d), "[Consult from alice]\n\nwhat is 2+2?");
}

#[test]
fn compose_prompt_command() {
    let d = delivery(DeliveryKind::Command, "bob", "run the build", vec![]);
    assert_eq!(compose_prompt(&d), "[Command from bob]\n\nrun the build");
}

#[test]
fn compose_prompt_message() {
    let d = delivery(DeliveryKind::Message, "carol", "fyi, deploy done", vec![]);
    assert_eq!(
        compose_prompt(&d),
        "[Message from carol]\n\nfyi, deploy done"
    );
}

#[test]
fn compose_prompt_with_data_appends_attached_block() {
    let v1 = serde_json::json!({"k": 1});
    let v2 = serde_json::json!(["a", "b"]);
    let d = delivery(
        DeliveryKind::Command,
        "bob",
        "process these",
        vec![v1.clone(), v2.clone()],
    );

    let expected = format!(
        "[Command from bob]\n\nprocess these\n\nAttached data:\n{}\n{}",
        v1, v2
    );
    assert_eq!(compose_prompt(&d), expected);
}

#[test]
fn compose_prompt_empty_data_has_no_attached_block() {
    let d = delivery(DeliveryKind::Consult, "alice", "just text", vec![]);
    let out = compose_prompt(&d);
    assert!(
        !out.contains("Attached data:"),
        "unexpected data block: {out}"
    );
    assert_eq!(out, "[Consult from alice]\n\njust text");
}

// ---------------------------------------------------------------------------
// session_uuid / WASL_SESSION_NS
// ---------------------------------------------------------------------------

#[test]
fn wasl_session_ns_is_the_fixed_namespace() {
    // Guards the constant itself against accidental drift.
    assert_eq!(
        WASL_SESSION_NS,
        Uuid::parse_str("2441e851-1801-440d-a8e9-214d1df08e6e").unwrap()
    );
}

#[test]
fn session_uuid_is_deterministic_for_the_same_context() {
    assert_eq!(session_uuid("ctx-1"), session_uuid("ctx-1"));
    assert_eq!(
        session_uuid("some-other-context"),
        session_uuid("some-other-context")
    );
}

#[test]
fn session_uuid_is_distinct_across_contexts() {
    assert_ne!(session_uuid("ctx-1"), session_uuid("ctx-2"));
}

#[test]
fn session_uuid_pinned_context_matches_hard_coded_uuid() {
    // Computed independently (Python's `uuid.uuid5` AND the `uuid` crate's
    // `Uuid::new_v5`, cross-checked) for namespace
    // `2441e851-1801-440d-a8e9-214d1df08e6e` and name `"ctx-1"`. If this ever
    // fails, either the namespace constant or the derivation changed —
    // both are contract-breaking (existing claude sessions would no longer
    // resolve).
    let expected = Uuid::parse_str("a2b3b7b9-517a-5e88-b0a2-b1004eab5845").unwrap();
    assert_eq!(session_uuid("ctx-1"), expected);
}

// ---------------------------------------------------------------------------
// build_argv (v2 signature: bin, &Uuid, Turn)
// ---------------------------------------------------------------------------

#[test]
fn build_argv_first_turn_uses_session_id() {
    let su = session_uuid("ctx-1");
    let expected: Vec<String> = vec![
        "claude",
        "-p",
        "--output-format",
        "json",
        "--session-id",
        &su.to_string(),
    ]
    .into_iter()
    .map(String::from)
    .collect();
    assert_eq!(build_argv("claude", &su, Turn::First), expected);
}

#[test]
fn build_argv_continuation_turn_uses_resume() {
    let su = session_uuid("ctx-1");
    let expected: Vec<String> = vec![
        "claude",
        "-p",
        "--output-format",
        "json",
        "--resume",
        &su.to_string(),
    ]
    .into_iter()
    .map(String::from)
    .collect();
    assert_eq!(build_argv("claude", &su, Turn::Continuation), expected);
}

#[test]
fn build_argv_honors_the_given_bin() {
    let su = session_uuid("ctx-1");
    let argv = build_argv("claude-cli", &su, Turn::First);
    assert_eq!(argv[0], "claude-cli");
}

// ---------------------------------------------------------------------------
// parse_reply (contract unchanged from v1)
// ---------------------------------------------------------------------------

#[test]
fn parse_reply_success() {
    let stdout = r#"{"type":"result","is_error":false,"result":"hi there"}"#;
    assert_eq!(parse_reply(stdout), Ok("hi there".to_string()));
}

#[test]
fn parse_reply_error_with_error_field() {
    let stdout = r#"{"is_error":true,"error":"boom"}"#;
    let err = parse_reply(stdout).unwrap_err();
    assert_eq!(err.message, "boom");
}

#[test]
fn parse_reply_error_falls_back_to_result() {
    let stdout = r#"{"is_error":true,"result":"partial"}"#;
    let err = parse_reply(stdout).unwrap_err();
    assert_eq!(err.message, "partial");
}

#[test]
fn parse_reply_error_default_message() {
    let stdout = r#"{"is_error":true}"#;
    let err = parse_reply(stdout).unwrap_err();
    assert_eq!(err.message, "claude reported an error");
}

#[test]
fn parse_reply_success_missing_result() {
    let stdout = r#"{"is_error":false}"#;
    let err = parse_reply(stdout).unwrap_err();
    assert!(
        err.message.contains("no result field"),
        "message was: {}",
        err.message
    );
}

#[test]
fn parse_reply_malformed_json() {
    let err = parse_reply("not json").unwrap_err();
    assert!(
        err.message.contains("parse"),
        "message was: {}",
        err.message
    );
}
