//! Genuine unit coverage of the pure core: `compose_prompt`, `build_argv`,
//! `parse_reply`. These test only the public API + `wasl_adapter` public types.

use wasl_adapter::{Delivery, DeliveryKind};
use wasl_adapter_claude::{build_argv, compose_prompt, parse_reply};

fn delivery(kind: DeliveryKind, from: &str, text: &str, data: Vec<serde_json::Value>) -> Delivery {
    Delivery {
        context_id: "ctx-1".to_string(),
        from: from.to_string(),
        kind,
        text: text.to_string(),
        data,
    }
}

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

#[test]
fn build_argv_exact() {
    let expected: Vec<String> = vec![
        "claude",
        "-p",
        "--output-format",
        "json",
        "--session-id",
        "ctx-1",
    ]
    .into_iter()
    .map(String::from)
    .collect();
    assert_eq!(build_argv("claude", "ctx-1"), expected);
}

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
