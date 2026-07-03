//! Genuine unit coverage of the pure core: `compose_prompt`, `build_argv` +
//! `Turn`, `parse_reply` + `AgyReply`. These test only the public API +
//! `wasl_adapter` public types.
//!
//! HONESTY NOTE — UPDATED 2026-07-03 (W8′-fix, orchestrator live
//! verification; per `docs/specs/adapter-antigravity.md`'s "W8′-fix
//! correction" section and updated "Test plan / tests/pure.rs"): the
//! original spec's argv shape AND success mapping were both wrong, found by
//! the orchestrator's first-ever live `agy -p` runs (agy 1.0.15, 2026-07-03).
//! Two fixtures below are now REAL captured lines rather than synthetic
//! schema-derived ones: `VERIFIED_SUCCESS_SAMPLE` (the first-ever observed
//! `status:"SUCCESS"` reply) and `VERIFIED_TOOL_DENIAL_DECORATION_SAMPLE`
//! (the sample previously named `VERIFIED_ERROR_SAMPLE` — same bytes, but
//! under the corrected mapping this is now a documented SUCCESS, not an
//! error, because its `response` is non-empty; `status`/`error` no longer
//! gate the success/failure branch, see `parse_reply`'s doc comment in
//! `src/lib.rs`). Remaining fixtures used only for structural/line-selection
//! coverage stay synthetic (hand-built, clearly not captured output).

use wasl_adapter::{Delivery, DeliveryKind};
use wasl_adapter_antigravity::{
    build_argv, compose_prompt, parse_reply, AgyReply, Turn, DEFAULT_MODEL,
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
// DEFAULT_MODEL
// ---------------------------------------------------------------------------

#[test]
fn default_model_is_the_pinned_cheapest_entry() {
    assert_eq!(DEFAULT_MODEL, "Gemini 3.5 Flash (Low)");
}

// ---------------------------------------------------------------------------
// build_argv
// ---------------------------------------------------------------------------

fn strs(parts: &[&str]) -> Vec<String> {
    parts.iter().map(|s| s.to_string()).collect()
}

#[test]
fn build_argv_first_is_exact() {
    // CORRECTED 2026-07-03 (W8'-fix): "--output-format json" is the hidden
    // flag required for JSON output; "-p" + the prompt are the strictly-last
    // two elements (a live run showed -p consumes the very next argv token as
    // its value, so it cannot precede other flags).
    let argv = build_argv("agy", DEFAULT_MODEL, Turn::First, "hello world");
    assert_eq!(
        argv,
        strs(&[
            "agy",
            "--output-format",
            "json",
            "--model",
            "Gemini 3.5 Flash (Low)",
            "--sandbox",
            "--dangerously-skip-permissions",
            "-p",
            "hello world",
        ])
    );
}

#[test]
fn build_argv_first_has_no_conversation_flag() {
    let argv = build_argv("agy", DEFAULT_MODEL, Turn::First, "hi");
    assert!(
        !argv.iter().any(|a| a == "--conversation"),
        "First argv must not contain --conversation: {argv:?}"
    );
}

#[test]
fn build_argv_continuation_is_exact() {
    let cid = "97197d46-2091-40e2-b36f-0dcedd34bcc4";
    let argv = build_argv("agy", DEFAULT_MODEL, Turn::Continuation(cid), "hello world");
    assert_eq!(
        argv,
        strs(&[
            "agy",
            "--output-format",
            "json",
            "--conversation",
            cid,
            "--model",
            "Gemini 3.5 Flash (Low)",
            "--sandbox",
            "--dangerously-skip-permissions",
            "-p",
            "hello world",
        ])
    );
}

#[test]
fn build_argv_prompt_is_strictly_last_on_first() {
    let argv = build_argv("agy", DEFAULT_MODEL, Turn::First, "the actual prompt text");
    assert_eq!(
        argv.last().map(String::as_str),
        Some("the actual prompt text"),
        "prompt must be the strictly-last argv element (it is -p's flag value): {argv:?}"
    );
    assert_eq!(
        argv.get(argv.len().wrapping_sub(2)).map(String::as_str),
        Some("-p"),
        "-p must be the second-to-last argv element, immediately before the prompt: {argv:?}"
    );
}

#[test]
fn build_argv_prompt_is_strictly_last_on_continuation() {
    let cid = "97197d46-2091-40e2-b36f-0dcedd34bcc4";
    let argv = build_argv(
        "agy",
        DEFAULT_MODEL,
        Turn::Continuation(cid),
        "the actual prompt text",
    );
    assert_eq!(
        argv.last().map(String::as_str),
        Some("the actual prompt text"),
        "prompt must be the strictly-last argv element: {argv:?}"
    );
    assert_eq!(
        argv.get(argv.len().wrapping_sub(2)).map(String::as_str),
        Some("-p"),
        "-p must be the second-to-last argv element, immediately before the prompt: {argv:?}"
    );
}

#[test]
fn build_argv_output_format_json_is_immediately_after_bin() {
    // "--output-format json" is the HIDDEN flag (VERIFIED live 2026-07-03)
    // required for JSON output; it must precede --conversation/--model/etc.
    let cid = "some-cid";
    let first = build_argv("agy", DEFAULT_MODEL, Turn::First, "hi");
    let cont = build_argv("agy", DEFAULT_MODEL, Turn::Continuation(cid), "hi");
    for argv in [&first, &cont] {
        assert_eq!(
            (argv[1].as_str(), argv[2].as_str()),
            ("--output-format", "json"),
            "--output-format json must be argv[1..3], right after bin: {argv:?}"
        );
    }
}

#[test]
fn build_argv_honors_custom_bin_and_model_on_first() {
    let argv = build_argv("agy-next", "Gemini 3.5 Flash (High)", Turn::First, "hi");
    assert_eq!(
        argv,
        strs(&[
            "agy-next",
            "--output-format",
            "json",
            "--model",
            "Gemini 3.5 Flash (High)",
            "--sandbox",
            "--dangerously-skip-permissions",
            "-p",
            "hi",
        ])
    );
}

#[test]
fn build_argv_honors_custom_bin_and_model_on_continuation() {
    let cid = "some-cid";
    let argv = build_argv(
        "agy-next",
        "Gemini 3.5 Flash (High)",
        Turn::Continuation(cid),
        "hi",
    );
    assert_eq!(
        argv,
        strs(&[
            "agy-next",
            "--output-format",
            "json",
            "--conversation",
            cid,
            "--model",
            "Gemini 3.5 Flash (High)",
            "--sandbox",
            "--dangerously-skip-permissions",
            "-p",
            "hi",
        ])
    );
}

#[test]
fn build_argv_has_no_print_timeout_or_continue_flags() {
    let cid = "some-cid";
    let first = build_argv("agy", DEFAULT_MODEL, Turn::First, "hi");
    let cont = build_argv("agy", DEFAULT_MODEL, Turn::Continuation(cid), "hi");
    for argv in [&first, &cont] {
        assert!(
            !argv.iter().any(|a| a == "--print-timeout"),
            "adapter argv must leave --print-timeout at its 5m0s default: {argv:?}"
        );
        assert!(
            !argv.iter().any(|a| a == "--continue" || a == "-c"),
            "adapter argv must never use --continue/-c (workspace-global, racy): {argv:?}"
        );
    }
}

// ---------------------------------------------------------------------------
// parse_reply — CORRECTED 2026-07-03 (W8'-fix, orchestrator live
// verification): success is now keyed off conversation_id + response
// presence ONLY. status/error are never consulted to gate the branch; they
// only ride along inside the raw line that decorates a failure message.
// ---------------------------------------------------------------------------

/// VERIFIED (real, not synthetic): the first-ever observed `status:"SUCCESS"`
/// `agy --output-format json -p` reply, captured live by the ORCHESTRATOR on
/// this machine (agy 1.0.15, 2026-07-03). Per the spec's "W8'-fix
/// correction", this exact line MUST be a pure-test fixture verbatim.
const VERIFIED_SUCCESS_SAMPLE: &str = "{\"conversation_id\":\"3d206228-0e90-48b4-9c05-4a6d14ff3d8b\",\"status\":\"SUCCESS\",\"response\":\"bt-7731\\n\",\"error\":null,\"num_turns\":1}";

/// VERIFIED (real, not synthetic): the exact JSON line the ORCHESTRATOR
/// observed live on this machine (agy 1.0.15, 2026-07-02/03, exit 0) where a
/// tool call was denied (`status:"ERROR"` + non-empty `error`) but the model
/// still produced a non-empty, on-topic `response` narrating the denial —
/// "tool-denial decoration". Under the CORRECTED mapping this is a SUCCESS
/// (`response` is present and non-empty), not an error: `status`/`error` no
/// longer gate the branch. This was previously named `VERIFIED_ERROR_SAMPLE`
/// and asserted as an `Err` before the W8'-fix correction; same bytes, new
/// (correct) expected outcome.
const VERIFIED_TOOL_DENIAL_DECORATION_SAMPLE: &str = "{\"conversation_id\":\"97197d46-2091-40e2-b36f-0dcedd34bcc4\",\"status\":\"ERROR\",\"response\":\"It looks like your request was just the letter \\\"x\\\". ...\",\"error\":\"Permission denied for read_file(/home/mewais/.gemini/antigravity-cli). Matches hardcoded system protection boundary rule.\",\"duration_seconds\":2.70532965,\"num_turns\":1,\"usage\":{}}";

#[test]
fn parse_reply_verified_success_sample_is_ok() {
    let reply =
        parse_reply(VERIFIED_SUCCESS_SAMPLE).expect("verified live SUCCESS sample should parse");
    assert_eq!(
        reply,
        AgyReply {
            conversation_id: "3d206228-0e90-48b4-9c05-4a6d14ff3d8b".to_string(),
            text: "bt-7731\n".to_string(),
        }
    );
}

#[test]
fn parse_reply_verified_tool_denial_decoration_sample_is_ok_with_full_response() {
    // The central regression test for the W8'-fix correction: a reply
    // carrying status:"ERROR" and a non-empty error MUST still be Ok when
    // response is non-empty. This must fail loudly if a future edit
    // reintroduces status/error gating.
    let reply = parse_reply(VERIFIED_TOOL_DENIAL_DECORATION_SAMPLE).expect(
        "a good response alongside status:\"ERROR\" must still be Ok under the corrected mapping",
    );
    assert_eq!(
        reply.conversation_id,
        "97197d46-2091-40e2-b36f-0dcedd34bcc4"
    );
    assert_eq!(
        reply.text,
        "It looks like your request was just the letter \"x\". ..."
    );
}

// --- status/error are irrelevant to success (the corrected-mapping contract) ---

#[test]
fn parse_reply_any_status_value_is_ok_when_conversation_id_and_response_present() {
    for status in [
        "SUCCESS",
        "ERROR",
        "COMPLETED",
        "TIMEOUT",
        "CANCELLED",
        "some-unknown-value",
    ] {
        let stdout = format!(
            "{{\"conversation_id\":\"cid-1\",\"status\":\"{status}\",\"response\":\"hi\"}}"
        );
        let reply = parse_reply(&stdout)
            .unwrap_or_else(|e| panic!("status {status} should be Ok, got: {}", e.message));
        assert_eq!(reply.conversation_id, "cid-1");
        assert_eq!(reply.text, "hi");
    }
}

#[test]
fn parse_reply_status_absent_is_ok() {
    let stdout = "{\"conversation_id\":\"cid-1\",\"response\":\"hi\"}";
    let reply = parse_reply(stdout).expect("absent status must not affect success");
    assert_eq!(reply.text, "hi");
}

#[test]
fn parse_reply_any_error_value_is_ok_when_conversation_id_and_response_present() {
    for stdout in [
        "{\"conversation_id\":\"cid-1\",\"status\":\"SUCCESS\",\"response\":\"hi\",\"error\":null}",
        "{\"conversation_id\":\"cid-1\",\"status\":\"SUCCESS\",\"response\":\"hi\",\"error\":\"\"}",
        "{\"conversation_id\":\"cid-1\",\"status\":\"SUCCESS\",\"response\":\"hi\"}",
        // A non-empty error combined with an unrelated status string: proves
        // neither field is consulted, only conversation_id/response shape.
        "{\"conversation_id\":\"cid-1\",\"status\":\"SUCCESS\",\"response\":\"hi\",\"error\":\"boom\"}",
        "{\"conversation_id\":\"cid-1\",\"status\":\"whatever\",\"response\":\"hi\",\"error\":\"boom\"}",
    ] {
        let reply = parse_reply(stdout)
            .unwrap_or_else(|e| panic!("error/status must not gate success ({stdout}): {}", e.message));
        assert_eq!(reply.text, "hi");
    }
}

// --- failure mapping: single unified shape --------------------------------------

#[test]
fn parse_reply_missing_conversation_id_is_an_error() {
    let stdout = "{\"status\":\"SUCCESS\",\"response\":\"hi\"}";
    let err = parse_reply(stdout).unwrap_err();
    assert!(
        err.message.contains("agy turn failed:"),
        "message was: {}",
        err.message
    );
    assert!(
        err.message.contains(stdout),
        "message did not contain the raw line: {}",
        err.message
    );
}

#[test]
fn parse_reply_empty_conversation_id_is_an_error() {
    let stdout = "{\"conversation_id\":\"\",\"status\":\"SUCCESS\",\"response\":\"hi\"}";
    let err = parse_reply(stdout).unwrap_err();
    assert!(
        err.message.contains("agy turn failed:"),
        "message was: {}",
        err.message
    );
}

#[test]
fn parse_reply_missing_response_key_is_an_error() {
    let stdout = "{\"conversation_id\":\"cid-1\",\"status\":\"SUCCESS\"}";
    let err = parse_reply(stdout).unwrap_err();
    assert!(
        err.message.contains("agy turn failed:"),
        "message was: {}",
        err.message
    );
}

#[test]
fn parse_reply_non_string_response_is_an_error() {
    let stdout = "{\"conversation_id\":\"cid-1\",\"status\":\"SUCCESS\",\"response\":42}";
    let err = parse_reply(stdout).unwrap_err();
    assert!(
        err.message.contains("agy turn failed:"),
        "message was: {}",
        err.message
    );
}

#[test]
fn parse_reply_empty_response_string_is_an_error() {
    // CHANGED under the corrected mapping (previously a documented Ok
    // tolerance): response must be a NON-empty string for success.
    let stdout = "{\"conversation_id\":\"cid-1\",\"status\":\"SUCCESS\",\"response\":\"\"}";
    let err = parse_reply(stdout).unwrap_err();
    assert!(
        err.message.contains("agy turn failed:"),
        "message was: {}",
        err.message
    );
}

#[test]
fn parse_reply_response_present_but_conversation_id_missing_is_still_an_error() {
    // A non-empty response alone is not enough: both fields are required.
    let stdout = "{\"status\":\"SUCCESS\",\"response\":\"looks fine\"}";
    let err = parse_reply(stdout).unwrap_err();
    assert!(
        err.message.contains("agy turn failed:"),
        "message was: {}",
        err.message
    );
}

// --- malformed JSON -----------------------------------------------------------

#[test]
fn parse_reply_malformed_json_only_is_no_json_result_line() {
    let err = parse_reply("not json").unwrap_err();
    assert!(
        err.message.contains("agy output had no JSON result line"),
        "message was: {}",
        err.message
    );
}

// --- line selection / multi-line stdout handling -------------------------------

#[test]
fn parse_reply_tolerates_blank_lines_and_a_stray_banner() {
    let stdout =
        format!("\n   \nAntigravity CLI v1.0.15 starting...\n{VERIFIED_SUCCESS_SAMPLE}\n\n");
    let reply = parse_reply(&stdout).expect("should parse despite banner/blank lines");
    assert_eq!(reply.text, "bt-7731\n");
    assert_eq!(
        reply.conversation_id,
        "3d206228-0e90-48b4-9c05-4a6d14ff3d8b"
    );
}

#[test]
fn parse_reply_skips_non_object_json_lines() {
    let stdout = format!("[1,2,3]\n{VERIFIED_SUCCESS_SAMPLE}\n");
    let reply = parse_reply(&stdout).expect("a JSON array line must not count as the result line");
    assert_eq!(reply.text, "bt-7731\n");
}

#[test]
fn parse_reply_two_json_object_lines_last_one_wins() {
    let first = "{\"conversation_id\":\"cid-a\",\"status\":\"SUCCESS\",\"response\":\"first\"}";
    let second = "{\"conversation_id\":\"cid-b\",\"status\":\"SUCCESS\",\"response\":\"second\"}";
    let stdout = format!("{first}\n{second}\n");
    let reply = parse_reply(&stdout).expect("should parse");
    assert_eq!(reply.conversation_id, "cid-b");
    assert_eq!(reply.text, "second");
}

#[test]
fn parse_reply_empty_stdout_is_no_json_result_line() {
    let err = parse_reply("").unwrap_err();
    assert!(
        err.message.contains("agy output had no JSON result line"),
        "message was: {}",
        err.message
    );
}

#[test]
fn parse_reply_pure_garbage_multiline_is_no_json_result_line() {
    let err = parse_reply("not json\nstill not json\n***\n").unwrap_err();
    assert!(
        err.message.contains("agy output had no JSON result line"),
        "message was: {}",
        err.message
    );
}

// --- ignored fields -------------------------------------------------------------

#[test]
fn parse_reply_ignores_usage_num_turns_duration_seconds_when_absent() {
    let stdout = "{\"conversation_id\":\"cid-1\",\"status\":\"SUCCESS\",\"response\":\"hi\"}";
    let reply = parse_reply(stdout).expect("should be Ok without usage/num_turns/duration_seconds");
    assert_eq!(reply.text, "hi");
}

#[test]
fn parse_reply_ignores_usage_of_unexpected_shape() {
    let stdout = "{\"conversation_id\":\"cid-1\",\"status\":\"SUCCESS\",\"response\":\"hi\",\"usage\":\"not-an-object\",\"num_turns\":\"weird\",\"duration_seconds\":\"nope\"}";
    let reply = parse_reply(stdout)
        .expect("unusual usage/num_turns/duration_seconds shapes must be ignored, not errors");
    assert_eq!(reply.text, "hi");
}
