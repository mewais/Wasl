//! Precedence + path-resolution tests for `wasl_mcp::Config`, per
//! `docs/specs/node-config.md`'s `config_resolve.rs` plan.
//!
//! All env is captured as pure `EnvOverrides` data — no test in this file ever
//! calls `std::env::set_var`/`remove_var` on the real process. Filesystem
//! interaction (for `load_with`) goes through `tempfile` tempdirs only.

use std::path::PathBuf;

use wasl_mcp::{Config, ConfigError, EnvOverrides};

/// A file that sets identity, audit path, and the a2a bind — enough to prove
/// "env overrides file" without touching adapter/peers (env never touches
/// those per the precedence table).
fn file_with_identity_audit_bind() -> &'static str {
    r#"
[identity]
agent_id = "file-agent"
host_id  = "file-host"

[audit]
path = "/file/audit.jsonl"

[a2a]
bind = "127.0.0.1:1111"
"#
}

fn env_with_all_four_value_overrides() -> EnvOverrides {
    EnvOverrides {
        config: None,
        xdg_config_home: None,
        home: None,
        agent_id: Some("env-agent".to_string()),
        host_id: Some("env-host".to_string()),
        audit_path: Some("/env/audit.jsonl".to_string()),
        a2a_bind: Some("127.0.0.1:9999".to_string()),
        content: None,
    }
}

/// Machine-independent delivery: `WASL_CONFIG_CONTENT` (inline TOML) is loaded
/// by VALUE and must take precedence over a `WASL_CONFIG` file PATH — a path
/// minted on the caller's machine is meaningless where wasl-mcpd actually runs
/// (a remote node). This is what lets Marsad hand a worker's A2A MCP server its
/// peer config without shipping a filesystem path across hosts.
#[test]
fn load_with_inline_content_takes_precedence_over_bogus_path() {
    let env = EnvOverrides {
        content: Some(file_with_identity_audit_bind().to_string()),
        // A path that does NOT exist anywhere: if content weren't preferred, this
        // explicit path would make load_with fail with ConfigError::Read.
        config: Some("/no/such/wasl-worker-abc.toml".to_string()),
        ..Default::default()
    };
    let cfg = Config::load_with(&env).expect("inline WASL_CONFIG_CONTENT must load, ignoring path");
    assert_eq!(cfg.identity.agent_id, "file-agent");
    assert_eq!(cfg.identity.host_id, "file-host");
    assert_eq!(cfg.a2a.bind, Some("127.0.0.1:1111".to_string()));
}

/// Env-var overlays still apply ON TOP of inline content, same as for a file.
#[test]
fn load_with_inline_content_still_honors_env_overrides() {
    let env = EnvOverrides {
        content: Some(file_with_identity_audit_bind().to_string()),
        agent_id: Some("env-agent".to_string()),
        ..Default::default()
    };
    let cfg = Config::load_with(&env).expect("inline content loads");
    assert_eq!(cfg.identity.agent_id, "env-agent", "env override wins over inline content");
    assert_eq!(cfg.identity.host_id, "file-host", "un-overridden inline value kept");
}

// ---------------------------------------------------------------------
// Precedence: env over file, env over defaults, empty-string env, unvalidated bind
// ---------------------------------------------------------------------

#[test]
fn apply_env_all_four_override_file_values() {
    let cfg = Config::from_toml_str(file_with_identity_audit_bind()).expect("valid file");
    let overlaid = cfg.apply_env(&env_with_all_four_value_overrides());

    assert_eq!(overlaid.identity.agent_id, "env-agent");
    assert_eq!(overlaid.identity.host_id, "env-host");
    assert_eq!(overlaid.audit.path, "/env/audit.jsonl");
    assert_eq!(overlaid.a2a.bind, Some("127.0.0.1:9999".to_string()));
}

#[test]
fn apply_env_default_overrides_leave_file_values_untouched() {
    let cfg = Config::from_toml_str(file_with_identity_audit_bind()).expect("valid file");
    let overlaid = cfg.clone().apply_env(&EnvOverrides::default());
    assert_eq!(
        overlaid, cfg,
        "no env set → the file's values pass through unchanged"
    );
}

#[test]
fn apply_env_overrides_builtin_defaults_too() {
    let overlaid = Config::default().apply_env(&env_with_all_four_value_overrides());

    assert_eq!(overlaid.identity.agent_id, "env-agent");
    assert_eq!(overlaid.identity.host_id, "env-host");
    assert_eq!(overlaid.audit.path, "/env/audit.jsonl");
    assert_eq!(overlaid.a2a.bind, Some("127.0.0.1:9999".to_string()));
}

#[test]
fn apply_env_set_but_empty_string_still_overrides() {
    let env = EnvOverrides {
        agent_id: Some("".to_string()),
        ..Default::default()
    };
    let overlaid = Config::default().apply_env(&env);
    assert_eq!(
        overlaid.identity.agent_id, "",
        "Ok(\"\") is used as-is, matching today's env semantics"
    );
}

#[test]
fn apply_env_a2a_bind_is_not_validated_and_stored_verbatim() {
    let env = EnvOverrides {
        a2a_bind: Some("garbage".to_string()),
        ..Default::default()
    };
    let overlaid = Config::default().apply_env(&env);
    assert_eq!(
        overlaid.a2a.bind,
        Some("garbage".to_string()),
        "the lenient parse happens in run(), not in apply_env"
    );
}

// ---------------------------------------------------------------------
// resolve_path
// ---------------------------------------------------------------------

#[test]
fn resolve_path_explicit_config_wins_even_with_xdg_and_home_set() {
    let env = EnvOverrides {
        config: Some("/explicit/config.toml".to_string()),
        xdg_config_home: Some("/xdg".to_string()),
        home: Some("/home/u".to_string()),
        ..Default::default()
    };
    assert_eq!(
        Config::resolve_path(&env),
        Some(PathBuf::from("/explicit/config.toml"))
    );
}

#[test]
fn resolve_path_explicit_config_wins_even_when_set_to_empty_string() {
    // Per the spec: WASL_CONFIG set (even to "") names the path. Path
    // resolution just returns that value; whether the resulting path is
    // readable is `load_with`'s concern, not `resolve_path`'s.
    let env = EnvOverrides {
        config: Some("".to_string()),
        xdg_config_home: Some("/xdg".to_string()),
        ..Default::default()
    };
    assert_eq!(Config::resolve_path(&env), Some(PathBuf::from("")));
}

#[test]
fn resolve_path_xdg_used_when_set_and_nonempty() {
    let env = EnvOverrides {
        xdg_config_home: Some("/xdg".to_string()),
        home: Some("/home/u".to_string()),
        ..Default::default()
    };
    assert_eq!(
        Config::resolve_path(&env),
        Some(PathBuf::from("/xdg/wasl/config.toml"))
    );
}

#[test]
fn resolve_path_home_fallback_when_xdg_unset() {
    let env = EnvOverrides {
        home: Some("/home/u".to_string()),
        ..Default::default()
    };
    assert_eq!(
        Config::resolve_path(&env),
        Some(PathBuf::from("/home/u/.config/wasl/config.toml"))
    );
}

#[test]
fn resolve_path_xdg_empty_falls_through_to_home() {
    let env = EnvOverrides {
        xdg_config_home: Some("".to_string()),
        home: Some("/home/u".to_string()),
        ..Default::default()
    };
    assert_eq!(
        Config::resolve_path(&env),
        Some(PathBuf::from("/home/u/.config/wasl/config.toml"))
    );
}

#[test]
fn resolve_path_none_when_both_unset() {
    assert_eq!(Config::resolve_path(&EnvOverrides::default()), None);
}

#[test]
fn resolve_path_none_when_both_set_but_empty() {
    let env = EnvOverrides {
        xdg_config_home: Some("".to_string()),
        home: Some("".to_string()),
        ..Default::default()
    };
    assert_eq!(Config::resolve_path(&env), None);
}

// ---------------------------------------------------------------------
// load_with — explicit path
// ---------------------------------------------------------------------

#[test]
fn load_with_explicit_path_valid_file_loaded_and_env_overlaid() {
    let dir = tempfile::tempdir().expect("tempdir");
    let path = dir.path().join("config.toml");
    std::fs::write(&path, file_with_identity_audit_bind()).expect("write config");

    let env = EnvOverrides {
        config: Some(path.to_string_lossy().into_owned()),
        host_id: Some("env-host".to_string()),
        ..Default::default()
    };
    let cfg = Config::load_with(&env).expect("a valid explicit config must load");

    assert_eq!(
        cfg.identity.agent_id, "file-agent",
        "not overridden by any env here"
    );
    assert_eq!(
        cfg.identity.host_id, "env-host",
        "env overlay still applies on top of the file"
    );
    assert_eq!(cfg.audit.path, "/file/audit.jsonl");
    assert_eq!(cfg.a2a.bind, Some("127.0.0.1:1111".to_string()));
}

#[test]
fn load_with_explicit_path_nonexistent_is_read_error() {
    let dir = tempfile::tempdir().expect("tempdir");
    let missing = dir.path().join("does-not-exist.toml");
    let env = EnvOverrides {
        config: Some(missing.to_string_lossy().into_owned()),
        ..Default::default()
    };

    let err = Config::load_with(&env).expect_err("an explicitly-named missing file must error");
    match err {
        ConfigError::Read { path, .. } => {
            assert_eq!(path, missing.to_string_lossy().into_owned());
        }
        other => panic!("expected ConfigError::Read, got {other:?}"),
    }
}

// ---------------------------------------------------------------------
// load_with — default path
// ---------------------------------------------------------------------

#[test]
fn load_with_default_path_missing_file_yields_defaults() {
    let home = tempfile::tempdir().expect("tempdir");
    // No file created anywhere under `home` — the default path is simply absent.
    let env = EnvOverrides {
        home: Some(home.path().to_string_lossy().into_owned()),
        ..Default::default()
    };
    let cfg = Config::load_with(&env).expect("a missing default-path file is not an error");
    assert_eq!(
        cfg,
        Config::default(),
        "missing default path == fully-default config"
    );
}

#[test]
fn load_with_default_path_present_file_loaded() {
    let home = tempfile::tempdir().expect("tempdir");
    let config_dir = home.path().join(".config").join("wasl");
    std::fs::create_dir_all(&config_dir).expect("mkdir -p");
    std::fs::write(
        config_dir.join("config.toml"),
        file_with_identity_audit_bind(),
    )
    .expect("write default-path config");

    let env = EnvOverrides {
        home: Some(home.path().to_string_lossy().into_owned()),
        ..Default::default()
    };
    let cfg = Config::load_with(&env).expect("a valid default-path file must load");

    assert_eq!(cfg.identity.agent_id, "file-agent");
    assert_eq!(cfg.identity.host_id, "file-host");
    assert_eq!(cfg.audit.path, "/file/audit.jsonl");
    assert_eq!(cfg.a2a.bind, Some("127.0.0.1:1111".to_string()));
}

#[test]
fn load_with_default_path_present_invalid_toml_is_error() {
    let home = tempfile::tempdir().expect("tempdir");
    let config_dir = home.path().join(".config").join("wasl");
    std::fs::create_dir_all(&config_dir).expect("mkdir -p");
    std::fs::write(config_dir.join("config.toml"), "this is not toml").expect("write bad config");

    let env = EnvOverrides {
        home: Some(home.path().to_string_lossy().into_owned()),
        ..Default::default()
    };
    let err =
        Config::load_with(&env).expect_err("an existing-but-invalid default-path file must error");
    assert!(matches!(err, ConfigError::Toml(_)), "got: {err:?}");
}
