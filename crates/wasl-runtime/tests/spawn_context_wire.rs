//! Serde round-trip tests for [`wasl_runtime::SpawnContext`].
//!
//! Spec: `src/lib.rs` — `SpawnContext` is `#[serde(rename_all = "snake_case")]`
//! with every field annotated `#[serde(default, skip_serializing_if = …)]` so
//! that a default (all-None/empty) value serializes to `{}` and missing fields
//! deserialize to their defaults.
//!
//! These tests are EXPECTED RED until the bodies of any `todo!()`-gated paths
//! that touch `SpawnContext` construction are filled in.  The struct itself
//! already compiles, so the tests compile and then FAIL asserting the exact
//! field names / skip-if semantics.

use std::collections::BTreeMap;

use serde_json::json;
use wasl_runtime::SpawnContext;

// ------------------------------------------------------------------
// Helpers
// ------------------------------------------------------------------

fn roundtrip(ctx: &SpawnContext) -> serde_json::Value {
    serde_json::to_value(ctx).expect("SpawnContext must serialize without error")
}

fn from_json(v: serde_json::Value) -> SpawnContext {
    serde_json::from_value(v).expect("SpawnContext must deserialize without error")
}

// ==================================================================
// 1. Full struct round-trip — all fields populated, exact field names.
// ==================================================================

#[test]
fn full_spawn_context_round_trips_with_exact_field_names() {
    let mut env = BTreeMap::new();
    env.insert("FOO".to_string(), "bar".to_string());
    env.insert("ZAP".to_string(), "baz".to_string());

    let ctx = SpawnContext {
        protocol: Some("claude".to_string()),
        cwd: Some("/workspace/my-project".to_string()),
        model: Some("claude-opus-4-5".to_string()),
        posture: Some("bypassPermissions".to_string()),
        env: env.clone(),
        setup: Some("npm install".to_string()),
        bin: Some("/usr/local/bin/claude".to_string()),
        disallowed_tools: vec!["Bash".to_string(), "Write".to_string()],
        mcp_config: Some("{\"mcpServers\":{\"wasl\":{}}}".to_string()),
        resume: None,
    };

    let value = roundtrip(&ctx);

    // Pin exact field name casing — every field is `snake_case`.
    assert_eq!(
        value["protocol"], "claude",
        "field must be named 'protocol' (snake_case)"
    );
    assert_eq!(
        value["cwd"], "/workspace/my-project",
        "field must be named 'cwd'"
    );
    assert_eq!(
        value["model"], "claude-opus-4-5",
        "field must be named 'model'"
    );
    assert_eq!(
        value["posture"], "bypassPermissions",
        "field must be named 'posture'"
    );
    assert_eq!(value["setup"], "npm install", "field must be named 'setup'");
    assert_eq!(
        value["bin"], "/usr/local/bin/claude",
        "field must be named 'bin'"
    );
    assert_eq!(
        value["disallowed_tools"],
        json!(["Bash", "Write"]),
        "field must be named 'disallowed_tools' (snake_case)"
    );
    assert_eq!(
        value["mcp_config"], "{\"mcpServers\":{\"wasl\":{}}}",
        "field must be named 'mcp_config' (snake_case)"
    );
    // env is a BTreeMap — must serialize as an object.
    assert_eq!(
        value["env"],
        json!({"FOO": "bar", "ZAP": "baz"}),
        "field must be named 'env' and serialize as a JSON object"
    );

    // Round-trip back: deserialized value must equal original.
    let back: SpawnContext = serde_json::from_value(value).expect("deserialize full SpawnContext");
    assert_eq!(back.protocol, ctx.protocol, "protocol round-trips");
    assert_eq!(back.cwd, ctx.cwd, "cwd round-trips");
    assert_eq!(back.model, ctx.model, "model round-trips");
    assert_eq!(back.posture, ctx.posture, "posture round-trips");
    assert_eq!(back.env, ctx.env, "env round-trips");
    assert_eq!(back.setup, ctx.setup, "setup round-trips");
    assert_eq!(back.bin, ctx.bin, "bin round-trips");
    assert_eq!(
        back.disallowed_tools, ctx.disallowed_tools,
        "disallowed_tools round-trips"
    );
}

// ==================================================================
// 2. Empty JSON `{}` deserializes to all-default SpawnContext.
// ==================================================================

#[test]
fn empty_json_object_deserializes_to_all_defaults() {
    let ctx = from_json(json!({}));

    assert!(
        ctx.protocol.is_none(),
        "missing 'protocol' must default to None, got {:?}",
        ctx.protocol
    );
    assert!(
        ctx.cwd.is_none(),
        "missing 'cwd' must default to None, got {:?}",
        ctx.cwd
    );
    assert!(
        ctx.model.is_none(),
        "missing 'model' must default to None, got {:?}",
        ctx.model
    );
    assert!(
        ctx.posture.is_none(),
        "missing 'posture' must default to None, got {:?}",
        ctx.posture
    );
    assert!(
        ctx.env.is_empty(),
        "missing 'env' must default to empty BTreeMap, got {:?}",
        ctx.env
    );
    assert!(
        ctx.setup.is_none(),
        "missing 'setup' must default to None, got {:?}",
        ctx.setup
    );
    assert!(
        ctx.bin.is_none(),
        "missing 'bin' must default to None, got {:?}",
        ctx.bin
    );
    assert!(
        ctx.disallowed_tools.is_empty(),
        "missing 'disallowed_tools' must default to empty Vec, got {:?}",
        ctx.disallowed_tools
    );
}

// ==================================================================
// 3. Partial JSON `{"cwd":"/p","model":"m"}` — other fields default.
// ==================================================================

#[test]
fn partial_json_sets_named_fields_others_remain_default() {
    let ctx = from_json(json!({"cwd": "/p", "model": "m"}));

    assert_eq!(
        ctx.cwd,
        Some("/p".to_string()),
        "cwd must be populated from the JSON"
    );
    assert_eq!(
        ctx.model,
        Some("m".to_string()),
        "model must be populated from the JSON"
    );

    // All other fields must be at their defaults.
    assert!(
        ctx.protocol.is_none(),
        "unset protocol must remain None, got {:?}",
        ctx.protocol
    );
    assert!(
        ctx.posture.is_none(),
        "unset posture must remain None, got {:?}",
        ctx.posture
    );
    assert!(
        ctx.env.is_empty(),
        "unset env must remain empty, got {:?}",
        ctx.env
    );
    assert!(
        ctx.setup.is_none(),
        "unset setup must remain None, got {:?}",
        ctx.setup
    );
    assert!(
        ctx.bin.is_none(),
        "unset bin must remain None, got {:?}",
        ctx.bin
    );
    assert!(
        ctx.disallowed_tools.is_empty(),
        "unset disallowed_tools must remain empty, got {:?}",
        ctx.disallowed_tools
    );
}

// ==================================================================
// 4. BTreeMap env serializes with sorted keys (deterministic order).
// ==================================================================

#[test]
fn env_btreemap_serializes_with_sorted_keys_deterministically() {
    // Insert in reverse alphabetical order — BTreeMap keeps them sorted.
    let mut env = BTreeMap::new();
    env.insert("ZEBRA".to_string(), "z".to_string());
    env.insert("ALPHA".to_string(), "a".to_string());
    env.insert("MANGO".to_string(), "m".to_string());

    let ctx = SpawnContext {
        env: env.clone(),
        ..SpawnContext::default()
    };

    let value = roundtrip(&ctx);
    let env_obj = value["env"].as_object().expect("env must be a JSON object");

    // Collect keys as a Vec — serde_json preserves BTreeMap insertion order in
    // the JSON object representation (which for BTreeMap is sorted order).
    let keys: Vec<&str> = env_obj.keys().map(String::as_str).collect();
    assert_eq!(
        keys,
        vec!["ALPHA", "MANGO", "ZEBRA"],
        "BTreeMap env must serialize with keys in sorted (alphabetical) order; got {keys:?}"
    );

    // And the values are correct.
    assert_eq!(env_obj["ALPHA"], "a");
    assert_eq!(env_obj["MANGO"], "m");
    assert_eq!(env_obj["ZEBRA"], "z");

    // Serializing twice must produce identical JSON bytes (deterministic).
    let s1 = serde_json::to_string(&ctx).unwrap();
    let s2 = serde_json::to_string(&ctx).unwrap();
    assert_eq!(s1, s2, "SpawnContext serialization must be deterministic");
}

// ==================================================================
// 5. Default SpawnContext serializes to `{}` (skip_serializing_if works).
// ==================================================================

#[test]
fn default_spawn_context_serializes_to_empty_json_object() {
    let ctx = SpawnContext::default();
    let value = roundtrip(&ctx);

    assert_eq!(
        value,
        json!({}),
        "SpawnContext::default() must serialize to {{}} — all skip_serializing_if predicates \
         must fire for None options and empty collections; got: {value}"
    );

    // Also check the raw JSON string to be sure there are no stray fields.
    let json_str = serde_json::to_string(&ctx).expect("serialize");
    assert_eq!(
        json_str, "{}",
        "default SpawnContext JSON string must be exactly '{{}}', got: {json_str:?}"
    );
}

// ==================================================================
// 6. Only populated fields appear in the serialized output.
// ==================================================================

#[test]
fn only_populated_fields_appear_in_serialized_output() {
    // Set just `protocol` and `disallowed_tools`; everything else stays default.
    let ctx = SpawnContext {
        protocol: Some("opencode-acp".to_string()),
        disallowed_tools: vec!["Bash".to_string()],
        ..SpawnContext::default()
    };

    let value = roundtrip(&ctx);
    let obj = value.as_object().expect("must serialize to object");

    // Only these two keys must be present.
    assert!(
        obj.contains_key("protocol"),
        "protocol must appear when set"
    );
    assert!(
        obj.contains_key("disallowed_tools"),
        "disallowed_tools must appear when set"
    );

    // None of the all-default fields must appear.
    for absent_key in &["cwd", "model", "posture", "env", "setup", "bin"] {
        assert!(
            !obj.contains_key(*absent_key),
            "default field '{absent_key}' must be omitted from serialized output \
             (skip_serializing_if must fire), but found it in: {obj:?}"
        );
    }
}

// ==================================================================
// 7. `protocol` field uses the exact string `"protocol"` (not "driver"
//    or any other alias) — pin the wire name to prevent future renames.
// ==================================================================

#[test]
fn protocol_field_serializes_under_exact_key_name_protocol() {
    let ctx = SpawnContext {
        protocol: Some("claude".to_string()),
        ..SpawnContext::default()
    };

    let value = roundtrip(&ctx);
    assert!(
        value.get("protocol").is_some(),
        "the field must be keyed 'protocol' in JSON, not 'driver' or anything else; got: {value}"
    );
    assert_eq!(
        value["protocol"], "claude",
        "protocol value must round-trip verbatim"
    );

    // Deserializing from the exact key name must work.
    let back = from_json(json!({"protocol": "opencode-acp"}));
    assert_eq!(back.protocol, Some("opencode-acp".to_string()));
}

// ==================================================================
// 8. `disallowed_tools` field preserves insertion order on round-trip.
// ==================================================================

#[test]
fn disallowed_tools_preserves_order_on_round_trip() {
    let tools = vec!["Bash".to_string(), "Write".to_string(), "Edit".to_string()];
    let ctx = SpawnContext {
        disallowed_tools: tools.clone(),
        ..SpawnContext::default()
    };

    let value = roundtrip(&ctx);
    let arr = value["disallowed_tools"]
        .as_array()
        .expect("disallowed_tools must serialize as a JSON array");

    let got: Vec<String> = arr
        .iter()
        .map(|v| {
            v.as_str()
                .expect("array element must be a string")
                .to_string()
        })
        .collect();

    assert_eq!(
        got, tools,
        "disallowed_tools must preserve element order on round-trip; got {got:?}"
    );
}

// ==================================================================
// 9. `resume` field — the new native-session-id-for-resume field.
//
// a) Absent from JSON → defaults to None (backward-compatible callers).
// b) Some("id") → serializes as `"resume": "id"`, round-trips exactly.
// c) None → omitted from serialized output (skip_serializing_if fires).
// d) Default SpawnContext still serializes to exactly `{}` (no `resume`
//    key added).
// ==================================================================

#[test]
fn resume_absent_from_json_defaults_to_none() {
    // Existing JSON that never had a `resume` key must still deserialize.
    let ctx = from_json(json!({"cwd": "/project"}));
    assert!(
        ctx.resume.is_none(),
        "missing 'resume' field must default to None (backward-compatible); got {:?}",
        ctx.resume
    );
}

#[test]
fn resume_some_round_trips_verbatim() {
    let native_id = "ses_07abc123def456".to_string();
    let ctx = SpawnContext {
        resume: Some(native_id.clone()),
        ..SpawnContext::default()
    };

    // Serializes under the exact key "resume".
    let value = roundtrip(&ctx);
    assert_eq!(
        value.get("resume").and_then(|v| v.as_str()),
        Some(native_id.as_str()),
        "Some resume must serialize as `\"resume\": \"<id>\"` under the exact key 'resume'; \
         got: {value}"
    );

    // Deserializes back to Some(id).
    let back: SpawnContext = serde_json::from_value(value).expect("round-trip must deserialize");
    assert_eq!(
        back.resume,
        Some(native_id.clone()),
        "resume must round-trip to Some(\"{native_id}\"); got {:?}",
        back.resume
    );
}

#[test]
fn resume_none_is_omitted_from_serialized_output() {
    let ctx = SpawnContext {
        model: Some("claude-opus-4-5".to_string()),
        resume: None,
        ..SpawnContext::default()
    };

    let value = roundtrip(&ctx);
    assert!(
        value.get("resume").is_none(),
        "resume = None must be omitted from serialized output \
         (skip_serializing_if = is_none must fire); got: {value}"
    );
}

#[test]
fn default_spawn_context_still_serializes_to_empty_object_with_resume_none() {
    // Adding `resume` must NOT break the `{}` invariant for default.
    let ctx = SpawnContext::default();
    assert!(
        ctx.resume.is_none(),
        "SpawnContext::default().resume must be None"
    );

    let value = roundtrip(&ctx);
    assert_eq!(
        value,
        json!({}),
        "SpawnContext::default() must still serialize to {{}} after adding the `resume` field; \
         got: {value}"
    );
}

#[test]
fn resume_field_uses_exact_key_name_resume() {
    // Pin the wire key name so a future rename is caught immediately.
    let ctx = SpawnContext {
        resume: Some("ses_xyz".to_string()),
        ..SpawnContext::default()
    };

    let value = roundtrip(&ctx);
    // Must be keyed exactly "resume".
    assert!(
        value
            .as_object()
            .map(|o| o.contains_key("resume"))
            .unwrap_or(false),
        "resume must serialize under the exact key 'resume'; got: {value}"
    );
    // Must NOT appear under any other alias.
    for wrong_key in &["native_id", "native_session_id", "session_id", "native"] {
        assert!(
            value.get(*wrong_key).is_none(),
            "resume must not appear under the alias '{wrong_key}'; got: {value}"
        );
    }

    // Deserializing from the exact key name.
    let back = from_json(json!({"resume": "ses_abc"}));
    assert_eq!(
        back.resume,
        Some("ses_abc".to_string()),
        "deserializing {{\"resume\": \"ses_abc\"}} must yield Some(\"ses_abc\")"
    );
}
