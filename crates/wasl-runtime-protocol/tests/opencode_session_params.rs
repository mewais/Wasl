//! Black-box tests for `wasl_runtime_protocol::opencode_session_params`.
//!
//! Contract (from `src/opencode_acp.rs` `perform_handshake` + `todo!()` banner):
//!
//!   session/new  → `{ "cwd": cwd, "mcpServers": [] }`
//!   session/load → `{ "sessionId": requested_session_id, "cwd": cwd, "mcpServers": [] }`
//!
//! The `cwd` comes from the `cwd` argument (not `std::env::current_dir()`).
//! The function is pure — no IO, no process state.
//!
//! All tests are RED until the coder fills in the `todo!()` body.

use wasl_runtime_protocol::opencode_session_params;

// ------------------------------------------------------------------
// Helpers
// ------------------------------------------------------------------

fn session_new(cwd: &str) -> serde_json::Value {
    opencode_session_params("session/new", None, cwd)
}

fn session_load(session_id: &str, cwd: &str) -> serde_json::Value {
    opencode_session_params("session/load", Some(session_id), cwd)
}

// ------------------------------------------------------------------
// 1. session/new — cwd field carries the passed argument
// ------------------------------------------------------------------

#[test]
fn session_new_carries_passed_cwd() {
    let params = session_new("/home/user/project");

    assert_eq!(
        params["cwd"].as_str(),
        Some("/home/user/project"),
        "session/new params must carry the cwd argument in the 'cwd' field"
    );
}

#[test]
fn session_new_mcp_servers_is_empty_array() {
    let params = session_new("/tmp/test");

    assert_eq!(
        params["mcpServers"],
        serde_json::json!([]),
        "session/new params must have mcpServers = []"
    );
}

#[test]
fn session_new_does_not_include_session_id_field() {
    let params = session_new("/tmp/test");

    assert!(
        params.get("sessionId").is_none(),
        "session/new params must NOT include a 'sessionId' field"
    );
}

// ------------------------------------------------------------------
// 2. session/load — carries sessionId + cwd
// ------------------------------------------------------------------

#[test]
fn session_load_carries_session_id() {
    let params = session_load("ses_abc123", "/work/project");

    assert_eq!(
        params["sessionId"].as_str(),
        Some("ses_abc123"),
        "session/load params must carry the requested_session_id in 'sessionId'"
    );
}

#[test]
fn session_load_carries_passed_cwd() {
    let params = session_load("ses_abc123", "/work/project");

    assert_eq!(
        params["cwd"].as_str(),
        Some("/work/project"),
        "session/load params must carry the cwd argument in 'cwd'"
    );
}

#[test]
fn session_load_mcp_servers_is_empty_array() {
    let params = session_load("ses_abc123", "/work/project");

    assert_eq!(
        params["mcpServers"],
        serde_json::json!([]),
        "session/load params must have mcpServers = []"
    );
}

// ------------------------------------------------------------------
// 3. Back-compat: session/new shape matches today's handshake exactly
//    (field-for-field against the inline json!() in perform_handshake)
// ------------------------------------------------------------------

#[test]
fn session_new_shape_matches_todays_handshake_field_for_field() {
    // The current `perform_handshake` builds:
    //   serde_json::json!({ "cwd": cwd, "mcpServers": [] })
    // when session_method != "session/load".
    // Pin that exact shape.
    let cwd = "/repo/wasl";
    let params = session_new(cwd);

    let expected = serde_json::json!({
        "cwd": cwd,
        "mcpServers": []
    });

    assert_eq!(
        params, expected,
        "session/new params must equal the exact JSON shape from perform_handshake"
    );
}

// ------------------------------------------------------------------
// 4. Back-compat: session/load shape matches today's handshake exactly
// ------------------------------------------------------------------

#[test]
fn session_load_shape_matches_todays_handshake_field_for_field() {
    // The current `perform_handshake` builds:
    //   serde_json::json!({
    //       "sessionId": requested_session_id.unwrap_or_default(),
    //       "cwd": cwd,
    //       "mcpServers": []
    //   })
    // when session_method == "session/load".
    let cwd = "/repo/wasl";
    let sid = "ses_079566635ffeaDOzjr6jJ5LKa7";
    let params = session_load(sid, cwd);

    let expected = serde_json::json!({
        "sessionId": sid,
        "cwd": cwd,
        "mcpServers": []
    });

    assert_eq!(
        params, expected,
        "session/load params must equal the exact JSON shape from perform_handshake"
    );
}

// ------------------------------------------------------------------
// 5. session/load with requested_session_id=None uses empty string
//    (mirrors `unwrap_or_default()` in perform_handshake)
// ------------------------------------------------------------------

#[test]
fn session_load_none_session_id_uses_empty_string() {
    let params = opencode_session_params("session/load", None, "/tmp");

    assert_eq!(
        params["sessionId"].as_str(),
        Some(""),
        "session/load with no session_id must emit sessionId: \"\" (unwrap_or_default)"
    );
}

// ------------------------------------------------------------------
// 6. Different cwd values are faithfully forwarded
// ------------------------------------------------------------------

#[test]
fn session_new_cwd_is_not_process_current_dir() {
    // The function takes cwd from the arg, NOT std::env::current_dir().
    // Use a sentinel that is unlikely to equal the real process cwd.
    let sentinel = "/wasl-test-sentinel-do-not-exist/subdir";
    let params = session_new(sentinel);

    assert_eq!(
        params["cwd"].as_str(),
        Some(sentinel),
        "cwd must come from the argument, not process current dir"
    );
}

#[test]
fn session_load_cwd_is_forwarded_from_arg() {
    let sentinel = "/wasl-test-sentinel-do-not-exist/subdir";
    let params = session_load("some-id", sentinel);

    assert_eq!(
        params["cwd"].as_str(),
        Some(sentinel),
        "cwd must come from the argument, not process current dir"
    );
}

// ------------------------------------------------------------------
// 7. Return type is a serde_json::Value (not a string, not an error)
// ------------------------------------------------------------------

#[test]
fn return_value_is_a_json_object() {
    let params = session_new("/tmp");
    assert!(
        params.is_object(),
        "opencode_session_params must return a JSON object, got {params:?}"
    );
}

#[test]
fn session_load_return_value_is_a_json_object() {
    let params = session_load("s", "/tmp");
    assert!(
        params.is_object(),
        "opencode_session_params must return a JSON object, got {params:?}"
    );
}

// ------------------------------------------------------------------
// 8. Exactly three fields on session/new, exactly three on session/load
// ------------------------------------------------------------------

#[test]
fn session_new_has_exactly_two_top_level_fields() {
    let params = session_new("/tmp");
    let obj = params.as_object().expect("must be object");
    // "cwd" + "mcpServers" = 2 fields (no extra keys)
    assert_eq!(
        obj.len(),
        2,
        "session/new must have exactly 2 fields (cwd, mcpServers); got {obj:?}"
    );
}

#[test]
fn session_load_has_exactly_three_top_level_fields() {
    let params = session_load("s", "/tmp");
    let obj = params.as_object().expect("must be object");
    // "sessionId" + "cwd" + "mcpServers" = 3 fields
    assert_eq!(
        obj.len(),
        3,
        "session/load must have exactly 3 fields (sessionId, cwd, mcpServers); got {obj:?}"
    );
}
