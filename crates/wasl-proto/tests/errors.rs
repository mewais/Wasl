//! Black-box error-path tests for `wasl_proto`'s public API.
//!
//! Covers docs/specs/proto.md §Test plan cases E1–E3. Every test is a real
//! `#[test]` with meaningful assertions. This file will NOT compile until
//! the code-agent implements the spec in `crates/wasl-proto/src/**` — that
//! is the expected state.

use std::error::Error;

use wasl_proto::{
    ProtoError, parse_command, parse_consult, parse_message, parse_transfer,
};

// ---------------------------------------------------------------------
// E1 — parse_consult on valid JSON -> Ok with fields; on malformed JSON ->
// Err(ProtoError::Malformed(_)).
// ---------------------------------------------------------------------

#[test]
fn e1_parse_consult_valid_json_ok_with_fields() {
    let json = r#"{"peers":["a","b"],"question":"how's it going?"}"#;
    let params = parse_consult(json).expect("valid consult JSON should parse");
    assert_eq!(params.peers.len(), 2);
    assert_eq!(params.peers[0].as_str(), "a");
    assert_eq!(params.peers[1].as_str(), "b");
    assert_eq!(params.question, "how's it going?");
}

#[test]
fn e1_parse_consult_malformed_json_is_err_malformed() {
    let json = r#"{"peers": not valid json"#;
    let err = parse_consult(json).expect_err("malformed JSON should fail to parse");
    match err {
        ProtoError::Malformed(msg) => assert!(!msg.is_empty()),
    }
}

#[test]
fn e1_parse_consult_missing_field_is_err_malformed() {
    // Missing required "question" field.
    let json = r#"{"peers":["a"]}"#;
    let err = parse_consult(json).expect_err("missing required field should fail to parse");
    match err {
        ProtoError::Malformed(msg) => assert!(!msg.is_empty()),
    }
}

// ---------------------------------------------------------------------
// E2 — same for parse_command / parse_message / parse_transfer (valid +
// malformed each).
// ---------------------------------------------------------------------

#[test]
fn e2_parse_command_valid_json_ok_with_fields() {
    let json = r#"{"peers":["a"],"task":"run the build"}"#;
    let params = parse_command(json).expect("valid command JSON should parse");
    assert_eq!(params.peers.len(), 1);
    assert_eq!(params.peers[0].as_str(), "a");
    assert_eq!(params.task, "run the build");
}

#[test]
fn e2_parse_command_malformed_json_is_err_malformed() {
    let json = r#"{{{ not json"#;
    let err = parse_command(json).expect_err("malformed JSON should fail to parse");
    match err {
        ProtoError::Malformed(msg) => assert!(!msg.is_empty()),
    }
}

#[test]
fn e2_parse_message_valid_json_ok_with_fields() {
    let json = r#"{"peers":["a","b","c"],"payload":"hello all"}"#;
    let params = parse_message(json).expect("valid message JSON should parse");
    assert_eq!(params.peers.len(), 3);
    assert_eq!(params.payload, "hello all");
}

#[test]
fn e2_parse_message_malformed_json_is_err_malformed() {
    let json = r#"not even json at all"#;
    let err = parse_message(json).expect_err("malformed JSON should fail to parse");
    match err {
        ProtoError::Malformed(msg) => assert!(!msg.is_empty()),
    }
}

#[test]
fn e2_parse_transfer_valid_json_ok_with_fields() {
    let json = r#"{"src_host":"host-a","dst_hosts":["host-b","host-c"],"path":"/data/file"}"#;
    let params = parse_transfer(json).expect("valid transfer JSON should parse");
    assert_eq!(params.src_host.as_str(), "host-a");
    assert_eq!(params.dst_hosts.len(), 2);
    assert_eq!(params.path, "/data/file");
}

#[test]
fn e2_parse_transfer_malformed_json_is_err_malformed() {
    let json = r#"{"src_host": "host-a", "dst_hosts": [unterminated"#;
    let err = parse_transfer(json).expect_err("malformed JSON should fail to parse");
    match err {
        ProtoError::Malformed(msg) => assert!(!msg.is_empty()),
    }
}

// ---------------------------------------------------------------------
// E3 — ProtoError Display non-empty; std::error::Error implemented
// (source() is None).
// ---------------------------------------------------------------------

#[test]
fn e3_proto_error_display_is_non_empty() {
    let err = ProtoError::Malformed("bad input".to_string());
    let rendered = format!("{err}");
    assert!(!rendered.is_empty());
    assert!(rendered.contains("bad input"));
}

#[test]
fn e3_proto_error_implements_std_error_source_is_none() {
    let err = ProtoError::Malformed("oops".to_string());
    let as_std_error: &dyn Error = &err;
    assert!(as_std_error.source().is_none());
}

#[test]
fn e3_proto_error_equality_and_clone() {
    let err1 = ProtoError::Malformed("x".to_string());
    let err2 = err1.clone();
    assert_eq!(err1, err2);
}
