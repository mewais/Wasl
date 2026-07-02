//! Genuine unit coverage of the pure core: `compose_prompt`, `build_argv` +
//! `Turn`, `parse_reply` + `CodexReply`. These test only the public API +
//! `wasl_adapter` public types.
//!
//! Fixtures are the research doc's CAPTURED JSONL, byte-for-byte, not invented
//! (see `docs/specs/adapter-codex.md`, "Test plan / tests/pure.rs").

use wasl_adapter::{Delivery, DeliveryKind};
use wasl_adapter_codex::{build_argv, compose_prompt, parse_reply, CodexReply, Turn};

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
// compose_prompt (IDENTICAL composition to wasl-adapter-claude)
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
// build_argv
// ---------------------------------------------------------------------------

fn strs(parts: &[&str]) -> Vec<String> {
    parts.iter().map(|s| s.to_string()).collect()
}

#[test]
fn build_argv_first_no_model_is_exact() {
    let argv = build_argv("codex", None, Turn::First);
    assert_eq!(
        argv,
        strs(&[
            "codex",
            "exec",
            "--json",
            "--skip-git-repo-check",
            "--sandbox",
            "read-only",
            "-",
        ])
    );
}

#[test]
fn build_argv_first_has_no_resume() {
    let argv = build_argv("codex", None, Turn::First);
    assert!(
        !argv.iter().any(|a| a == "resume"),
        "First argv must not contain resume: {argv:?}"
    );
}

#[test]
fn build_argv_continuation_no_model_is_exact() {
    let tid = "019f237a-f03d-7710-ab8f-7af32b222df8";
    let argv = build_argv("codex", None, Turn::Continuation(tid));
    assert_eq!(
        argv,
        strs(&[
            "codex",
            "exec",
            "resume",
            tid,
            "--json",
            "--skip-git-repo-check",
            "-",
        ])
    );
}

#[test]
fn build_argv_continuation_has_no_sandbox() {
    let tid = "019f237a-f03d-7710-ab8f-7af32b222df8";
    let argv = build_argv("codex", None, Turn::Continuation(tid));
    assert!(
        !argv.iter().any(|a| a == "--sandbox"),
        "Continuation argv must not contain --sandbox (rejected by `codex exec resume`): {argv:?}"
    );
}

#[test]
fn build_argv_first_with_model_is_exact() {
    let argv = build_argv("codex", Some("gpt-5.4-mini"), Turn::First);
    assert_eq!(
        argv,
        strs(&[
            "codex",
            "exec",
            "--json",
            "--skip-git-repo-check",
            "--sandbox",
            "read-only",
            "-m",
            "gpt-5.4-mini",
            "-",
        ])
    );
}

#[test]
fn build_argv_continuation_with_model_is_exact() {
    let tid = "019f237a-f03d-7710-ab8f-7af32b222df8";
    let argv = build_argv("codex", Some("gpt-5.4-mini"), Turn::Continuation(tid));
    assert_eq!(
        argv,
        strs(&[
            "codex",
            "exec",
            "resume",
            tid,
            "--json",
            "--skip-git-repo-check",
            "-m",
            "gpt-5.4-mini",
            "-",
        ])
    );
}

#[test]
fn build_argv_model_none_is_absent_from_both() {
    let tid = "019f237a-f03d-7710-ab8f-7af32b222df8";
    let first = build_argv("codex", None, Turn::First);
    let cont = build_argv("codex", None, Turn::Continuation(tid));
    assert!(!first.iter().any(|a| a == "-m"), "First: {first:?}");
    assert!(!cont.iter().any(|a| a == "-m"), "Continuation: {cont:?}");
}

#[test]
fn build_argv_honors_the_given_bin() {
    let argv = build_argv("codex-cli", None, Turn::First);
    assert_eq!(argv[0], "codex-cli");
}

// ---------------------------------------------------------------------------
// parse_reply
// ---------------------------------------------------------------------------

const HAPPY_FIXTURE: &str = "{\"type\":\"thread.started\",\"thread_id\":\"019f237a-f03d-7710-ab8f-7af32b222df8\"}\n\
{\"type\":\"turn.started\"}\n\
{\"type\":\"item.completed\",\"item\":{\"id\":\"item_0\",\"type\":\"agent_message\",\"text\":\"OK\"}}\n\
{\"type\":\"turn.completed\",\"usage\":{\"input_tokens\":9065,\"cached_input_tokens\":3456,\"output_tokens\":15,\"reasoning_output_tokens\":8}}\n";

#[test]
fn parse_reply_happy_path() {
    let reply = parse_reply(HAPPY_FIXTURE).expect("happy fixture should parse");
    assert_eq!(
        reply,
        CodexReply {
            thread_id: "019f237a-f03d-7710-ab8f-7af32b222df8".to_string(),
            text: "OK".to_string(),
        }
    );
}

#[test]
fn parse_reply_tolerates_blank_lines_unknown_events_and_non_message_items() {
    let stdout = "\n   \n\
{\"type\":\"thread.started\",\"thread_id\":\"019f237a-f03d-7710-ab8f-7af32b222df8\"}\n\
\n\
{\"type\":\"item.started\"}\n\
{\"type\":\"item.completed\",\"item\":{\"type\":\"reasoning\"}}\n\
{\"type\":\"item.completed\",\"item\":{\"id\":\"item_0\",\"type\":\"agent_message\",\"text\":\"OK\"}}\n\
   \n\
{\"type\":\"turn.completed\"}\n";
    let reply = parse_reply(stdout).expect("tolerant fixture should parse");
    assert_eq!(
        reply,
        CodexReply {
            thread_id: "019f237a-f03d-7710-ab8f-7af32b222df8".to_string(),
            text: "OK".to_string(),
        }
    );
}

#[test]
fn parse_reply_last_agent_message_wins() {
    let stdout = "{\"type\":\"thread.started\",\"thread_id\":\"tid-1\"}\n\
{\"type\":\"item.completed\",\"item\":{\"type\":\"agent_message\",\"text\":\"first\"}}\n\
{\"type\":\"item.completed\",\"item\":{\"type\":\"agent_message\",\"text\":\"second\"}}\n";
    let reply = parse_reply(stdout).expect("should parse");
    assert_eq!(reply.text, "second");
    assert_eq!(reply.thread_id, "tid-1");
}

#[test]
fn parse_reply_missing_thread_started_is_an_error() {
    let stdout =
        "{\"type\":\"item.completed\",\"item\":{\"type\":\"agent_message\",\"text\":\"OK\"}}\n";
    let err = parse_reply(stdout).unwrap_err();
    assert!(
        err.message.contains("no thread.started"),
        "message was: {}",
        err.message
    );
}

#[test]
fn parse_reply_missing_agent_message_is_an_error() {
    let stdout = "{\"type\":\"thread.started\",\"thread_id\":\"tid-1\"}\n\
{\"type\":\"turn.completed\"}\n";
    let err = parse_reply(stdout).unwrap_err();
    assert!(
        err.message.contains("no agent_message"),
        "message was: {}",
        err.message
    );
}

#[test]
fn parse_reply_turn_failed_is_an_error_with_raw_line() {
    let stdout = "{\"type\":\"thread.started\",\"thread_id\":\"tid-1\"}\n\
{\"type\":\"turn.failed\"}\n";
    let err = parse_reply(stdout).unwrap_err();
    assert!(
        err.message.contains("codex turn failed:"),
        "message was: {}",
        err.message
    );
    assert!(
        err.message.contains("{\"type\":\"turn.failed\"}"),
        "message did not contain raw line: {}",
        err.message
    );
}

#[test]
fn parse_reply_malformed_json_line_is_a_parse_error() {
    let err = parse_reply("not json").unwrap_err();
    assert!(
        err.message.contains("parse codex output line:"),
        "message was: {}",
        err.message
    );
}

#[test]
fn parse_reply_json_without_string_type_is_a_parse_error() {
    let err = parse_reply("{\"thread_id\":\"tid-1\"}").unwrap_err();
    assert!(
        err.message.contains("parse codex output line:"),
        "message was: {}",
        err.message
    );
}

#[test]
fn parse_reply_empty_stdout_is_no_thread_started_error() {
    let err = parse_reply("").unwrap_err();
    assert!(
        err.message.contains("no thread.started"),
        "message was: {}",
        err.message
    );
}
