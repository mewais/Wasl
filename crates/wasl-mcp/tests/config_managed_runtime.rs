//! Managed-runtime config tests: `RuntimeConfig` frozen-API additions
//! (`control_bind`, `default_protocol`, `claude_bin`, `opencode_bin`) parsed
//! from `[runtime]` TOML, alongside the existing `RuntimeKind` toggle.
//!
//! SCOPE: covers three areas:
//!   1. Full `[runtime]` parse with all new fields set.
//!   2. Back-compat: missing `[runtime]` table and partial `[runtime]` (kind
//!      only) both leave the new fields at their defaults.
//!   3. Validation (some tests are RED until the coder implements validation):
//!      a bad `control_bind` → `ConfigError::InvalidBind`; an unknown
//!      `default_protocol` → a config error (variant TBD, asserted as `Err(_)`
//!      until the coder adds the variant).
//!
//! This file MUST NOT edit `src/**`. Tests that are RED are intentional: they
//! document behavior that `todo!()`-scaffolded or not-yet-validated code must
//! eventually satisfy.

use wasl_mcp::{Config, ConfigError, RuntimeConfig, RuntimeKind};

// ── helpers ──────────────────────────────────────────────────────────────────

/// Build a full `[runtime]` block with all four new fields set.
fn full_runtime_toml() -> &'static str {
    concat!(
        "[runtime]\n",
        "kind             = \"managed\"\n",
        "control_bind     = \"127.0.0.1:8080\"\n",
        "default_protocol = \"opencode-acp\"\n",
        "claude_bin       = \"claude\"\n",
        "opencode_bin     = \"opencode\"\n",
    )
}

// ─────────────────────────────────────────────────────────────────────────────
// 1. Full parse — all four new fields, kind = "managed"
// ─────────────────────────────────────────────────────────────────────────────

#[test]
fn full_runtime_block_all_fields_exact() {
    let cfg = Config::from_toml_str(full_runtime_toml())
        .expect("a full [runtime] block with all four new fields must parse");

    // kind
    assert_eq!(
        cfg.runtime.kind,
        RuntimeKind::Managed,
        "kind = \"managed\" must parse to RuntimeKind::Managed"
    );
    // control_bind
    assert_eq!(
        cfg.runtime.control_bind,
        Some("127.0.0.1:8080".to_string()),
        "control_bind must be captured verbatim"
    );
    // default_protocol
    assert_eq!(
        cfg.runtime.default_protocol, "opencode-acp",
        "default_protocol must be captured verbatim"
    );
    // claude_bin
    assert_eq!(
        cfg.runtime.claude_bin,
        Some("claude".to_string()),
        "claude_bin must be captured verbatim"
    );
    // opencode_bin
    assert_eq!(
        cfg.runtime.opencode_bin,
        Some("opencode".to_string()),
        "opencode_bin must be captured verbatim"
    );
}

#[test]
fn full_runtime_block_struct_equality() {
    // Same as above but checked via PartialEq against the expected struct,
    // catching any accidental field swap.
    let cfg =
        Config::from_toml_str(full_runtime_toml()).expect("the full [runtime] block must parse");

    let expected_runtime = RuntimeConfig {
        kind: RuntimeKind::Managed,
        control_bind: Some("127.0.0.1:8080".to_string()),
        default_protocol: "opencode-acp".to_string(),
        claude_bin: Some("claude".to_string()),
        opencode_bin: Some("opencode".to_string()),
        codex_bin: None,
    };

    assert_eq!(
        cfg.runtime, expected_runtime,
        "RuntimeConfig must equal the expected struct for the full [runtime] block"
    );
}

// ─────────────────────────────────────────────────────────────────────────────
// 2. Back-compat: no [runtime] section and partial [runtime] (kind only)
// ─────────────────────────────────────────────────────────────────────────────

#[test]
fn no_runtime_section_all_new_fields_default() {
    // A config written before the new fields existed must parse byte-identically:
    // kind == Ephemeral (existing default), all new fields at their defaults.
    let cfg = Config::from_toml_str("").expect("an empty config must still parse");

    assert_eq!(
        cfg.runtime.kind,
        RuntimeKind::Ephemeral,
        "absent [runtime] section -> kind defaults to Ephemeral"
    );
    assert_eq!(
        cfg.runtime.control_bind, None,
        "absent [runtime] section -> control_bind defaults to None"
    );
    assert_eq!(
        cfg.runtime.default_protocol, "claude",
        "absent [runtime] section -> default_protocol defaults to \"claude\""
    );
    assert_eq!(
        cfg.runtime.claude_bin, None,
        "absent [runtime] section -> claude_bin defaults to None"
    );
    assert_eq!(
        cfg.runtime.opencode_bin, None,
        "absent [runtime] section -> opencode_bin defaults to None"
    );
}

#[test]
fn no_runtime_section_equals_default_runtime_config() {
    // Structurally assert that the empty document produces exactly the Default.
    let cfg = Config::from_toml_str("").expect("empty config must parse");
    assert_eq!(
        cfg.runtime,
        RuntimeConfig::default(),
        "an absent [runtime] section must produce exactly RuntimeConfig::default()"
    );
}

#[test]
fn runtime_kind_managed_only_new_fields_default() {
    // A [runtime] section with only `kind = "managed"` (and nothing else) must
    // leave all new fields at their defaults.
    let cfg = Config::from_toml_str("[runtime]\nkind = \"managed\"\n")
        .expect("[runtime] with kind only must parse");

    assert_eq!(cfg.runtime.kind, RuntimeKind::Managed);
    assert_eq!(
        cfg.runtime.control_bind, None,
        "kind-only [runtime] -> control_bind defaults to None"
    );
    assert_eq!(
        cfg.runtime.default_protocol, "claude",
        "kind-only [runtime] -> default_protocol defaults to \"claude\""
    );
    assert_eq!(
        cfg.runtime.claude_bin, None,
        "kind-only [runtime] -> claude_bin defaults to None"
    );
    assert_eq!(
        cfg.runtime.opencode_bin, None,
        "kind-only [runtime] -> opencode_bin defaults to None"
    );
}

#[test]
fn runtime_default_protocol_claude_is_the_default() {
    // Confirm "claude" is the hard-coded default_protocol, matching RuntimeConfig::default().
    assert_eq!(
        RuntimeConfig::default().default_protocol,
        "claude",
        "RuntimeConfig::default().default_protocol must be \"claude\""
    );
}

#[test]
fn runtime_new_bins_default_to_none() {
    let d = RuntimeConfig::default();
    assert_eq!(d.claude_bin, None, "default claude_bin is None");
    assert_eq!(d.opencode_bin, None, "default opencode_bin is None");
    assert_eq!(d.control_bind, None, "default control_bind is None");
}

// ─────────────────────────────────────────────────────────────────────────────
// 2b. Partial field coverage: individual new fields in isolation
// ─────────────────────────────────────────────────────────────────────────────

#[test]
fn runtime_control_bind_only_sets_only_that_field() {
    let cfg = Config::from_toml_str("[runtime]\ncontrol_bind = \"0.0.0.0:9090\"\n")
        .expect("[runtime] with control_bind only must parse");

    assert_eq!(
        cfg.runtime.control_bind,
        Some("0.0.0.0:9090".to_string()),
        "control_bind must be captured"
    );
    // Everything else must stay at its default.
    assert_eq!(cfg.runtime.kind, RuntimeKind::Ephemeral);
    assert_eq!(cfg.runtime.default_protocol, "claude");
    assert_eq!(cfg.runtime.claude_bin, None);
    assert_eq!(cfg.runtime.opencode_bin, None);
}

#[test]
fn runtime_claude_bin_only_sets_only_that_field() {
    let cfg = Config::from_toml_str("[runtime]\nclaude_bin = \"/opt/claude/claude\"\n")
        .expect("[runtime] with claude_bin only must parse");

    assert_eq!(
        cfg.runtime.claude_bin,
        Some("/opt/claude/claude".to_string())
    );
    assert_eq!(cfg.runtime.kind, RuntimeKind::Ephemeral);
    assert_eq!(cfg.runtime.control_bind, None);
    assert_eq!(cfg.runtime.default_protocol, "claude");
    assert_eq!(cfg.runtime.opencode_bin, None);
}

#[test]
fn runtime_opencode_bin_only_sets_only_that_field() {
    let cfg = Config::from_toml_str("[runtime]\nopencode_bin = \"/usr/local/bin/opencode\"\n")
        .expect("[runtime] with opencode_bin only must parse");

    assert_eq!(
        cfg.runtime.opencode_bin,
        Some("/usr/local/bin/opencode".to_string())
    );
    assert_eq!(cfg.runtime.kind, RuntimeKind::Ephemeral);
    assert_eq!(cfg.runtime.control_bind, None);
    assert_eq!(cfg.runtime.default_protocol, "claude");
    assert_eq!(cfg.runtime.claude_bin, None);
}

#[test]
fn runtime_default_protocol_only_sets_only_that_field() {
    let cfg = Config::from_toml_str("[runtime]\ndefault_protocol = \"claude\"\n")
        .expect("[runtime] with explicit default_protocol = \"claude\" must parse");

    assert_eq!(cfg.runtime.default_protocol, "claude");
    // Everything else at default.
    assert_eq!(cfg.runtime.kind, RuntimeKind::Ephemeral);
    assert_eq!(cfg.runtime.control_bind, None);
    assert_eq!(cfg.runtime.claude_bin, None);
    assert_eq!(cfg.runtime.opencode_bin, None);
}

// ─────────────────────────────────────────────────────────────────────────────
// 3. Validation — RED until the coder implements the validation logic
// ─────────────────────────────────────────────────────────────────────────────

/// A bad `control_bind` value (not parseable as a SocketAddr) must be rejected
/// with `ConfigError::InvalidBind { section: "runtime", … }`, matching the
/// crate's existing `a2a.bind`/`operator.bind` validation convention.
///
/// RED: `from_toml_str` currently passes `control_bind` through without
/// calling `validate_bind("runtime", …)`, so this returns `Ok` instead of
/// `Err(InvalidBind { section: "runtime", … })`. The test is a real assertion
/// — it documents the intended behavior so the coder's task is unambiguous.
#[test]
fn bad_control_bind_is_rejected_with_invalid_bind() {
    let err = Config::from_toml_str(
        "[runtime]\nkind = \"managed\"\ncontrol_bind = \"not-a-socket-addr\"\n",
    )
    .expect_err(
        "a malformed runtime.control_bind must be rejected at config-load time, \
         not silently stored and then fail later at serve() time",
    );

    match err {
        ConfigError::InvalidBind {
            section,
            value,
            detail: _,
        } => {
            assert_eq!(
                section, "runtime",
                "the InvalidBind section must be \"runtime\""
            );
            assert_eq!(
                value, "not-a-socket-addr",
                "the offending value must be preserved verbatim"
            );
        }
        other => {
            panic!("expected ConfigError::InvalidBind {{ section: \"runtime\", … }}, got {other:?}")
        }
    }
}

/// An unknown `default_protocol` must be rejected.
///
/// RED: `from_toml_str` currently accepts any string for `default_protocol`
/// (no validation). This asserts `Err(_)` — the exact error variant is TBD
/// (the coder will add it, e.g. `ConfigError::UnknownProtocol`). The test is
/// a real assertion — it will catch the first moment validation is wired.
#[test]
fn unknown_default_protocol_is_rejected() {
    // "rpc-over-carrier-pigeon" is not a protocol Wasl knows about;
    // only "claude" and "opencode-acp" are valid.
    let result =
        Config::from_toml_str("[runtime]\ndefault_protocol = \"rpc-over-carrier-pigeon\"\n");
    assert!(
        result.is_err(),
        "an unknown default_protocol must be rejected — currently returns Ok (RED)"
    );
}

/// A near-miss: "Claude" (capitalized) is not the same as "claude", matching
/// the case-sensitivity precedent of `adapter.kind` and `runtime.kind`.
///
/// RED: same root cause as `unknown_default_protocol_is_rejected` — no
/// validation on `default_protocol` is wired yet.
#[test]
fn default_protocol_match_is_case_sensitive() {
    // "Claude" (capital C) must NOT silently match the valid value "claude".
    let result = Config::from_toml_str("[runtime]\ndefault_protocol = \"Claude\"\n");
    assert!(
        result.is_err(),
        "default_protocol match must be case-sensitive; \"Claude\" is not \"claude\" (RED)"
    );
}

/// The two explicitly valid protocol values must continue to parse without error.
/// This pin ensures validation never over-rejects.
#[test]
fn known_protocols_are_accepted() {
    // "claude" is the default; verify it also parses fine when set explicitly.
    let cfg = Config::from_toml_str("[runtime]\ndefault_protocol = \"claude\"\n")
        .expect("\"claude\" is a known protocol and must be accepted");
    assert_eq!(cfg.runtime.default_protocol, "claude");

    // "opencode-acp" is the second known protocol.
    let cfg = Config::from_toml_str("[runtime]\ndefault_protocol = \"opencode-acp\"\n")
        .expect("\"opencode-acp\" is a known protocol and must be accepted");
    assert_eq!(cfg.runtime.default_protocol, "opencode-acp");
}
