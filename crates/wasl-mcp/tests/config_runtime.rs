//! Phase 3 — the `runtime = "ephemeral"|"managed"` config toggle
//! (docs/design/live-runtime-impl-plan.md Decision 8), mirroring the
//! `[adapter] kind`/`[gate] authority` precedent in `src/config.rs` exactly:
//! a `[runtime]` table with a `kind` key, exact-match validated, defaulting to
//! `Ephemeral` when the whole `[runtime]` section (or the `kind` key) is
//! absent — so a config file written before this feature existed parses
//! byte-identically to today (the "zero behavior change on the default path"
//! requirement).
//!
//! SCOPE (deliberately narrow): this file tests ONLY the config-parsing side
//! (`RuntimeKind`, its default, and its exact-match validation). It does NOT
//! test wiring a `RuntimeKind::Managed` config value to an actual
//! `WaslManagedRuntimeProvider` — per runtime.md §4.7, that wiring (an
//! exhaustive match building the right provider) is explicitly DEFERRED past
//! this phase, since there is no real managed protocol driver yet (that is
//! Phase 4/5). Testing it now would mean inventing a wiring contract ahead of
//! its own spec.
//!
//! Expected to fail to compile until `RuntimeKind` and `Config.runtime` (or
//! equivalent) exist in `crates/wasl-mcp/src/config.rs` — the test-first
//! baseline.

use wasl_mcp::{Config, ConfigError, RuntimeKind};

#[test]
fn absent_runtime_section_defaults_to_ephemeral() {
    let cfg = Config::from_toml_str("").expect("an empty config must still parse");
    assert_eq!(
        cfg.runtime.kind,
        RuntimeKind::Ephemeral,
        "no [runtime] section at all -> today's byte-identical default behavior"
    );
}

#[test]
fn kind_ephemeral_parses_explicitly() {
    let cfg = Config::from_toml_str("[runtime]\nkind = \"ephemeral\"\n")
        .expect("\"ephemeral\" must parse");
    assert_eq!(cfg.runtime.kind, RuntimeKind::Ephemeral);
}

#[test]
fn kind_managed_parses() {
    let cfg =
        Config::from_toml_str("[runtime]\nkind = \"managed\"\n").expect("\"managed\" must parse");
    assert_eq!(cfg.runtime.kind, RuntimeKind::Managed);
}

#[test]
fn runtime_section_present_but_kind_key_absent_defaults_to_ephemeral() {
    // An explicit [runtime] table with no `kind` key at all must still default
    // to Ephemeral -- the default applies at the key level, not only when the
    // whole section is missing.
    let cfg = Config::from_toml_str("[runtime]\n").expect("an empty [runtime] table must parse");
    assert_eq!(cfg.runtime.kind, RuntimeKind::Ephemeral);
}

#[test]
fn unknown_runtime_kind_is_rejected() {
    let err = Config::from_toml_str("[runtime]\nkind = \"hybrid\"\n")
        .expect_err("an unknown runtime kind must be rejected");
    match err {
        ConfigError::UnknownRuntimeKind(value) => {
            assert_eq!(
                value, "hybrid",
                "the rejected value must be preserved verbatim"
            )
        }
        other => panic!("expected ConfigError::UnknownRuntimeKind, got {other:?}"),
    }
}

#[test]
fn runtime_kind_is_case_sensitive_exact_match() {
    // Mirrors adapter.kind's case-sensitivity precedent (e.g. "Codex" is
    // unknown even though "codex" is valid): "Ephemeral" (capitalized) must
    // NOT silently match "ephemeral".
    let err = Config::from_toml_str("[runtime]\nkind = \"Ephemeral\"\n").expect_err(
        "capitalized \"Ephemeral\" must NOT match the lowercase kind, same \
                     case-sensitivity precedent as adapter.kind/gate.authority",
    );
    assert!(matches!(err, ConfigError::UnknownRuntimeKind(v) if v == "Ephemeral"));
}
