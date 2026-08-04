//! Managed-runtime codex config tests: `[runtime] default_protocol = "codex"`
//! parses OK (no `UnknownProtocol`), and `codex_bin` round-trips as expected.
//!
//! SCOPE: mirrors `tests/config_managed_runtime.rs` but for the codex protocol
//! addition:
//!   1. `default_protocol = "codex"` parses OK (the new valid protocol value).
//!   2. `codex_bin` field round-trips in a full `[runtime]` block.
//!   3. `codex_bin` field is `None` by default (RuntimeConfig::default() and
//!      absent `[runtime]` section both leave it as `None`).
//!   4. `codex_bin` in isolation sets only that field (others stay at defaults).
//!   5. "codex" is accepted alongside "claude" and "opencode-acp" in the
//!      `known_protocols_are_accepted` pin.
//!   6. Unknown-protocol rejection is unaffected — truly unknown values still
//!      yield `ConfigError::UnknownProtocol`.
//!
//! These tests are RED until the implementation:
//!   a) Adds `"codex"` to the `default_protocol` validation match in
//!      `src/config.rs`.
//!   b) Adds `codex_bin: Option<String>` to `RawRuntime`/`RuntimeConfig`
//!      alongside `claude_bin`/`opencode_bin`.

use wasl_mcp::{Config, ConfigError, RuntimeConfig, RuntimeKind};

// ─────────────────────────────────────────────────────────────────────────────
// 1. default_protocol = "codex" parses OK
// ─────────────────────────────────────────────────────────────────────────────

#[test]
fn default_protocol_codex_is_accepted() {
    // "codex" must be accepted after this block, just like "claude" and
    // "opencode-acp". RED until the validation arm is added.
    let cfg =
        Config::from_toml_str("[runtime]\nkind = \"managed\"\ncontrol_bind = \"127.0.0.1:9000\"\ndefault_protocol = \"codex\"\n")
            .expect(
                "default_protocol = \"codex\" must parse OK (currently UnknownProtocol — RED)",
            );
    assert_eq!(cfg.runtime.default_protocol, "codex");
    assert_eq!(cfg.runtime.kind, RuntimeKind::Managed);
    assert_eq!(
        cfg.runtime.control_bind,
        Some("127.0.0.1:9000".to_string())
    );
}

#[test]
fn default_protocol_codex_minimal_config_parses() {
    // Just the protocol key alone, no kind/control_bind — must parse.
    let cfg = Config::from_toml_str("[runtime]\ndefault_protocol = \"codex\"\n")
        .expect("minimal [runtime] with default_protocol = \"codex\" must parse");
    assert_eq!(cfg.runtime.default_protocol, "codex");
    // Everything else at defaults.
    assert_eq!(cfg.runtime.kind, RuntimeKind::Ephemeral);
    assert_eq!(cfg.runtime.control_bind, None);
}

// ─────────────────────────────────────────────────────────────────────────────
// 2. codex_bin round-trips in a full [runtime] block
// ─────────────────────────────────────────────────────────────────────────────

#[test]
fn codex_bin_is_captured_verbatim_in_full_runtime_block() {
    let toml = concat!(
        "[runtime]\n",
        "kind             = \"managed\"\n",
        "control_bind     = \"127.0.0.1:9100\"\n",
        "default_protocol = \"codex\"\n",
        "codex_bin        = \"/opt/codex/codex\"\n",
    );
    let cfg = Config::from_toml_str(toml)
        .expect("full [runtime] block with codex_bin must parse");

    assert_eq!(cfg.runtime.kind, RuntimeKind::Managed);
    assert_eq!(
        cfg.runtime.control_bind,
        Some("127.0.0.1:9100".to_string())
    );
    assert_eq!(cfg.runtime.default_protocol, "codex");
    assert_eq!(
        cfg.runtime.codex_bin,
        Some("/opt/codex/codex".to_string()),
        "codex_bin must be captured verbatim"
    );
}

#[test]
fn codex_bin_struct_equality_round_trip() {
    // Checks via PartialEq against the expected struct shape, catching any
    // accidental field swap (mirrors `full_runtime_block_struct_equality`
    // in config_managed_runtime.rs).
    let toml = concat!(
        "[runtime]\n",
        "kind             = \"managed\"\n",
        "control_bind     = \"127.0.0.1:9100\"\n",
        "default_protocol = \"codex\"\n",
        "claude_bin       = \"claude\"\n",
        "opencode_bin     = \"opencode\"\n",
        "codex_bin        = \"codex\"\n",
    );
    let cfg = Config::from_toml_str(toml).expect("full [runtime] block must parse");

    let expected_runtime = RuntimeConfig {
        kind: RuntimeKind::Managed,
        control_bind: Some("127.0.0.1:9100".to_string()),
        default_protocol: "codex".to_string(),
        claude_bin: Some("claude".to_string()),
        opencode_bin: Some("opencode".to_string()),
        codex_bin: Some("codex".to_string()),
    };
    assert_eq!(
        cfg.runtime, expected_runtime,
        "RuntimeConfig must equal the expected struct for the full [runtime] block \
         (including codex_bin)"
    );
}

// ─────────────────────────────────────────────────────────────────────────────
// 3. codex_bin defaults to None (RuntimeConfig::default() and absent section)
// ─────────────────────────────────────────────────────────────────────────────

#[test]
fn codex_bin_defaults_to_none_in_runtime_config_default() {
    let d = RuntimeConfig::default();
    assert_eq!(d.codex_bin, None, "default codex_bin is None");
    // Also verify the pre-existing defaults are unchanged.
    assert_eq!(d.claude_bin, None);
    assert_eq!(d.opencode_bin, None);
    assert_eq!(d.control_bind, None);
    assert_eq!(d.default_protocol, "claude");
}

#[test]
fn absent_runtime_section_leaves_codex_bin_at_none() {
    let cfg = Config::from_toml_str("").expect("empty config must parse");
    assert_eq!(
        cfg.runtime.codex_bin, None,
        "absent [runtime] section -> codex_bin defaults to None"
    );
}

#[test]
fn runtime_kind_only_leaves_codex_bin_at_none() {
    let cfg = Config::from_toml_str("[runtime]\nkind = \"managed\"\n")
        .expect("[runtime] with kind only must parse");
    assert_eq!(
        cfg.runtime.codex_bin, None,
        "kind-only [runtime] -> codex_bin defaults to None"
    );
}

// ─────────────────────────────────────────────────────────────────────────────
// 4. codex_bin in isolation sets only that field
// ─────────────────────────────────────────────────────────────────────────────

#[test]
fn codex_bin_only_sets_only_that_field() {
    let cfg =
        Config::from_toml_str("[runtime]\ncodex_bin = \"/usr/local/bin/codex\"\n")
            .expect("[runtime] with codex_bin only must parse");

    assert_eq!(
        cfg.runtime.codex_bin,
        Some("/usr/local/bin/codex".to_string()),
        "codex_bin must be captured"
    );
    // Everything else must stay at its default.
    assert_eq!(cfg.runtime.kind, RuntimeKind::Ephemeral);
    assert_eq!(cfg.runtime.control_bind, None);
    assert_eq!(cfg.runtime.default_protocol, "claude");
    assert_eq!(cfg.runtime.claude_bin, None);
    assert_eq!(cfg.runtime.opencode_bin, None);
}

// ─────────────────────────────────────────────────────────────────────────────
// 5. "codex" accepted alongside "claude" and "opencode-acp"
// ─────────────────────────────────────────────────────────────────────────────

#[test]
fn all_three_known_protocols_are_accepted() {
    // "claude" is the default and was already tested in config_managed_runtime.rs;
    // this test verifies all three in one place for clarity.
    for proto in &["claude", "opencode-acp", "codex"] {
        let toml = format!("[runtime]\ndefault_protocol = \"{proto}\"\n");
        let cfg = Config::from_toml_str(&toml)
            .unwrap_or_else(|e| panic!("protocol {proto:?} must be accepted; got: {e:?}"));
        assert_eq!(
            cfg.runtime.default_protocol,
            *proto,
            "default_protocol must be captured verbatim for {proto:?}"
        );
    }
}

// ─────────────────────────────────────────────────────────────────────────────
// 6. Unknown protocols are still rejected (regression pin)
// ─────────────────────────────────────────────────────────────────────────────

#[test]
fn truly_unknown_protocol_is_still_rejected() {
    // "grpc-over-carrier-pigeon" is not and will never be a Wasl protocol.
    let result = Config::from_toml_str(
        "[runtime]\ndefault_protocol = \"grpc-over-carrier-pigeon\"\n",
    );
    assert!(
        result.is_err(),
        "a genuinely unknown default_protocol must still be rejected"
    );
    match result.expect_err("must be Err") {
        ConfigError::UnknownProtocol(p) => assert_eq!(
            p, "grpc-over-carrier-pigeon",
            "the rejected value must be preserved verbatim"
        ),
        other => panic!(
            "expected ConfigError::UnknownProtocol, got {other:?}"
        ),
    }
}

#[test]
fn codex_protocol_match_is_case_sensitive() {
    // "Codex" (capitalized) must NOT be accepted — only exact "codex" is valid.
    let result = Config::from_toml_str("[runtime]\ndefault_protocol = \"Codex\"\n");
    assert!(
        result.is_err(),
        "\"Codex\" must not be accepted; only exact lowercase \"codex\" is valid"
    );
}
