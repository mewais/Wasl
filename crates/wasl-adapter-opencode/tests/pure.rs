//! Genuine unit coverage of the pure core: `compose_prompt`, `build_argv` +
//! `Turn`, `parse_reply` + `OpencodeReply`. These test only the public API +
//! `wasl_adapter` public types.
//!
//! Fixtures are the CAPTURED JSONL — byte-for-byte from the 2026-07-20 opencode
//! 1.18.4 runs, not invented (see `docs/specs/adapter-opencode.md`, "Test plan
//! / tests/pure.rs").

use wasl_adapter::{Delivery, DeliveryKind};
use wasl_adapter_opencode::{build_argv, compose_prompt, parse_reply, OpencodeReply, Turn};

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
// compose_prompt (IDENTICAL composition to wasl-adapter-claude/-codex)
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
//   First             -> [bin, "run", "--format", "json", ("-m", model)?, prompt]
//   Continuation(sid) -> [bin, "run", "--format", "json", "-s", sid, ("-m", model)?, prompt]
//   prompt is ALWAYS the LAST element; -m accepted on BOTH turns.
// ---------------------------------------------------------------------------

fn strs(parts: &[&str]) -> Vec<String> {
    parts.iter().map(|s| s.to_string()).collect()
}

#[test]
fn build_argv_first_no_model_is_exact() {
    let argv = build_argv("opencode", None, Turn::First, "hello world");
    assert_eq!(
        argv,
        strs(&["opencode", "run", "--format", "json", "hello world"])
    );
}

#[test]
fn build_argv_first_has_no_resume_flag() {
    let argv = build_argv("opencode", None, Turn::First, "hi");
    assert!(
        !argv.iter().any(|a| a == "-s"),
        "First argv must not contain -s (resume is Continuation-only): {argv:?}"
    );
}

#[test]
fn build_argv_continuation_no_model_is_exact() {
    let sid = "ses_07ed5b65cffePjAU3JRLYTeADV";
    let argv = build_argv("opencode", None, Turn::Continuation(sid), "hello world");
    assert_eq!(
        argv,
        strs(&[
            "opencode",
            "run",
            "--format",
            "json",
            "-s",
            sid,
            "hello world",
        ])
    );
}

#[test]
fn build_argv_first_with_model_is_exact() {
    let argv = build_argv(
        "opencode",
        Some("amd-anthropic/Claude-Haiku-4.5"),
        Turn::First,
        "hello world",
    );
    assert_eq!(
        argv,
        strs(&[
            "opencode",
            "run",
            "--format",
            "json",
            "-m",
            "amd-anthropic/Claude-Haiku-4.5",
            "hello world",
        ])
    );
}

#[test]
fn build_argv_continuation_with_model_is_exact() {
    let sid = "ses_07ed5b65cffePjAU3JRLYTeADV";
    let argv = build_argv(
        "opencode",
        Some("amd-anthropic/Claude-Haiku-4.5"),
        Turn::Continuation(sid),
        "hello world",
    );
    assert_eq!(
        argv,
        strs(&[
            "opencode",
            "run",
            "--format",
            "json",
            "-s",
            sid,
            "-m",
            "amd-anthropic/Claude-Haiku-4.5",
            "hello world",
        ])
    );
}

#[test]
fn build_argv_model_none_is_absent_from_both() {
    let sid = "ses_07ed5b65cffePjAU3JRLYTeADV";
    let first = build_argv("opencode", None, Turn::First, "hi");
    let cont = build_argv("opencode", None, Turn::Continuation(sid), "hi");
    assert!(!first.iter().any(|a| a == "-m"), "First: {first:?}");
    assert!(!cont.iter().any(|a| a == "-m"), "Continuation: {cont:?}");
}

#[test]
fn build_argv_prompt_is_strictly_last_on_first() {
    let argv = build_argv("opencode", None, Turn::First, "the actual prompt text");
    assert_eq!(
        argv.last().map(String::as_str),
        Some("the actual prompt text"),
        "prompt must be the strictly-last argv element: {argv:?}"
    );
}

#[test]
fn build_argv_prompt_is_strictly_last_on_first_with_model() {
    let argv = build_argv(
        "opencode",
        Some("amd-anthropic/Claude-Haiku-4.5"),
        Turn::First,
        "the actual prompt text",
    );
    assert_eq!(
        argv.last().map(String::as_str),
        Some("the actual prompt text"),
        "prompt must be the strictly-last argv element even with -m present: {argv:?}"
    );
}

#[test]
fn build_argv_prompt_is_strictly_last_on_continuation() {
    let sid = "ses_07ed5b65cffePjAU3JRLYTeADV";
    let argv = build_argv(
        "opencode",
        None,
        Turn::Continuation(sid),
        "the actual prompt text",
    );
    assert_eq!(
        argv.last().map(String::as_str),
        Some("the actual prompt text"),
        "prompt must be the strictly-last argv element on continuation: {argv:?}"
    );
}

#[test]
fn build_argv_prompt_is_strictly_last_on_continuation_with_model() {
    let sid = "ses_07ed5b65cffePjAU3JRLYTeADV";
    let argv = build_argv(
        "opencode",
        Some("amd-anthropic/Claude-Haiku-4.5"),
        Turn::Continuation(sid),
        "the actual prompt text",
    );
    assert_eq!(
        argv.last().map(String::as_str),
        Some("the actual prompt text"),
        "prompt must be the strictly-last argv element on continuation with -m: {argv:?}"
    );
}

#[test]
fn build_argv_resume_flag_present_only_on_continuation() {
    let sid = "ses_07ed5b65cffePjAU3JRLYTeADV";
    let first = build_argv("opencode", None, Turn::First, "hi");
    let cont = build_argv("opencode", None, Turn::Continuation(sid), "hi");
    assert!(
        !first.iter().any(|a| a == "-s"),
        "First must not carry -s: {first:?}"
    );
    // -s is immediately followed by the learned session id on continuation.
    let s_pos = cont
        .iter()
        .position(|a| a == "-s")
        .expect("Continuation must carry -s");
    assert_eq!(
        cont.get(s_pos + 1).map(String::as_str),
        Some(sid),
        "-s must be immediately followed by the session id: {cont:?}"
    );
}

#[test]
fn build_argv_honors_the_given_bin() {
    let argv = build_argv("oc", None, Turn::First, "hi");
    assert_eq!(argv[0], "oc");
}

// ---------------------------------------------------------------------------
// parse_reply
//
// Fixtures below are CAPTURED verbatim from `docs/specs/adapter-opencode.md`'s
// "Test plan" section (2026-07-20, opencode 1.18.4) — NOT hand-typed.
// ---------------------------------------------------------------------------

/// Happy first turn: `step_start`, a `text` part carrying "PONG", then a
/// `step_finish` with reason "stop". Session `ses_07ed5b65cffePjAU3JRLYTeADV`.
/// Byte-for-byte from the spec (`turn1b.out`).
const HAPPY_FIXTURE: &str = concat!(
    "{\"type\":\"step_start\",\"timestamp\":1784578462654,\"sessionID\":\"ses_07ed5b65cffePjAU3JRLYTeADV\",\"part\":{\"id\":\"prt_f812a5364001zQ8vo13Yuy1k6w\",\"messageID\":\"msg_f812a4d44001pqw91yKuNKhijv\",\"sessionID\":\"ses_07ed5b65cffePjAU3JRLYTeADV\",\"type\":\"step-start\"}}\n",
    "{\"type\":\"text\",\"timestamp\":1784578462655,\"sessionID\":\"ses_07ed5b65cffePjAU3JRLYTeADV\",\"part\":{\"id\":\"prt_f812a536e001tTpqTtarThtbN4\",\"messageID\":\"msg_f812a4d44001pqw91yKuNKhijv\",\"sessionID\":\"ses_07ed5b65cffePjAU3JRLYTeADV\",\"type\":\"text\",\"text\":\"PONG\",\"time\":{\"start\":1784578462574,\"end\":1784578462586}}}\n",
    "{\"type\":\"step_finish\",\"timestamp\":1784578462656,\"sessionID\":\"ses_07ed5b65cffePjAU3JRLYTeADV\",\"part\":{\"id\":\"prt_f812a538c001nDzxxVVSwioNQy\",\"reason\":\"stop\",\"messageID\":\"msg_f812a4d44001pqw91yKuNKhijv\",\"sessionID\":\"ses_07ed5b65cffePjAU3JRLYTeADV\",\"type\":\"step-finish\",\"tokens\":{\"total\":8820,\"input\":3,\"output\":6,\"reasoning\":0,\"cache\":{\"write\":8811,\"read\":0}},\"cost\":0}}\n",
);

/// In-stream error (from the overflow run): `type=="error"` carrying
/// `.error.name == "ContextOverflowError"`. Byte-for-byte from the spec.
const CONTEXT_OVERFLOW_FIXTURE: &str = "{\"type\":\"error\",\"timestamp\":1784576908836,\"sessionID\":\"ses_07eed7be0ffeyspV4SRn90vWfl\",\"error\":{\"name\":\"ContextOverflowError\",\"data\":{\"message\":\"This model's maximum context length is 32000 tokens...\"}}}";

#[test]
fn parse_reply_happy_path() {
    let reply = parse_reply(HAPPY_FIXTURE).expect("happy fixture should parse");
    assert_eq!(
        reply,
        OpencodeReply {
            session_id: "ses_07ed5b65cffePjAU3JRLYTeADV".to_string(),
            text: "PONG".to_string(),
        }
    );
}

#[test]
fn parse_reply_tolerates_blank_lines_and_unknown_event_types() {
    let stdout = format!(
        "\n   \n{}\n{}\n{}\n",
        // step_start (ignored for text)
        "{\"type\":\"step_start\",\"sessionID\":\"ses_07ed5b65cffePjAU3JRLYTeADV\",\"part\":{\"messageID\":\"msg_a\",\"type\":\"step-start\"}}",
        // an unknown/future event type: carried nothing, must be ignored
        "{\"type\":\"tool\",\"sessionID\":\"ses_07ed5b65cffePjAU3JRLYTeADV\",\"part\":{\"messageID\":\"msg_a\",\"type\":\"tool\"}}",
        // the real text event
        "{\"type\":\"text\",\"sessionID\":\"ses_07ed5b65cffePjAU3JRLYTeADV\",\"part\":{\"messageID\":\"msg_a\",\"type\":\"text\",\"text\":\"PONG\",\"time\":{\"start\":1,\"end\":2}}}",
    );
    let reply = parse_reply(&stdout).expect("tolerant fixture should parse");
    assert_eq!(
        reply,
        OpencodeReply {
            session_id: "ses_07ed5b65cffePjAU3JRLYTeADV".to_string(),
            text: "PONG".to_string(),
        }
    );
}

#[test]
fn parse_reply_assembles_multiple_parts_of_one_message_by_time_start_order() {
    // Two `text` parts sharing ONE messageID, delivered OUT of time.start order
    // in the stream -> concatenated in time.start order ("Hello, world").
    let stdout = concat!(
        "{\"type\":\"text\",\"sessionID\":\"ses_x\",\"part\":{\"messageID\":\"msg_1\",\"type\":\"text\",\"text\":\", world\",\"time\":{\"start\":20,\"end\":30}}}\n",
        "{\"type\":\"text\",\"sessionID\":\"ses_x\",\"part\":{\"messageID\":\"msg_1\",\"type\":\"text\",\"text\":\"Hello\",\"time\":{\"start\":10,\"end\":15}}}\n",
    );
    let reply = parse_reply(stdout).expect("multi-part fixture should parse");
    assert_eq!(reply.session_id, "ses_x");
    assert_eq!(reply.text, "Hello, world");
}

#[test]
fn parse_reply_last_message_id_wins() {
    // Two DIFFERENT messageIDs -> the LAST message's text is the reply.
    let stdout = concat!(
        "{\"type\":\"text\",\"sessionID\":\"ses_x\",\"part\":{\"messageID\":\"msg_1\",\"type\":\"text\",\"text\":\"first message\",\"time\":{\"start\":10,\"end\":15}}}\n",
        "{\"type\":\"text\",\"sessionID\":\"ses_x\",\"part\":{\"messageID\":\"msg_2\",\"type\":\"text\",\"text\":\"second message\",\"time\":{\"start\":20,\"end\":25}}}\n",
    );
    let reply = parse_reply(stdout).expect("two-message fixture should parse");
    assert_eq!(reply.session_id, "ses_x");
    assert_eq!(reply.text, "second message");
}

#[test]
fn parse_reply_error_event_is_an_error_with_name_and_message() {
    let err = parse_reply(CONTEXT_OVERFLOW_FIXTURE).unwrap_err();
    assert!(
        err.message.contains("ContextOverflowError"),
        "message did not contain the error name: {}",
        err.message
    );
    assert!(
        err.message
            .contains("This model's maximum context length is 32000 tokens..."),
        "message did not contain the error data message: {}",
        err.message
    );
}

#[test]
fn parse_reply_no_session_id_is_an_error() {
    // A text part, but no top-level sessionID anywhere.
    let stdout = "{\"type\":\"text\",\"part\":{\"messageID\":\"msg_1\",\"type\":\"text\",\"text\":\"hi\",\"time\":{\"start\":1,\"end\":2}}}\n";
    let err = parse_reply(stdout).unwrap_err();
    assert!(
        err.message.contains("no sessionID"),
        "message was: {}",
        err.message
    );
}

#[test]
fn parse_reply_session_id_but_no_text_part_is_an_error() {
    // A sessionID is present (via step_start), but no `text` event ever arrives.
    let stdout = concat!(
        "{\"type\":\"step_start\",\"sessionID\":\"ses_07ed5b65cffePjAU3JRLYTeADV\",\"part\":{\"messageID\":\"msg_a\",\"type\":\"step-start\"}}\n",
        "{\"type\":\"step_finish\",\"sessionID\":\"ses_07ed5b65cffePjAU3JRLYTeADV\",\"part\":{\"messageID\":\"msg_a\",\"reason\":\"stop\",\"type\":\"step-finish\"}}\n",
    );
    let err = parse_reply(stdout).unwrap_err();
    assert!(
        err.message.contains("no assistant text"),
        "message was: {}",
        err.message
    );
}

#[test]
fn parse_reply_non_json_line_is_a_parse_error() {
    let err = parse_reply("not json").unwrap_err();
    assert!(
        err.message.contains("parse opencode output line:"),
        "message was: {}",
        err.message
    );
}

#[test]
fn parse_reply_empty_stdout_is_the_no_session_id_error() {
    let err = parse_reply("").unwrap_err();
    assert!(
        err.message.contains("no sessionID"),
        "message was: {}",
        err.message
    );
}
