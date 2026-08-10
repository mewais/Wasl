//! Pure parse/validate/`Display` tests for `wasl_mcp::Config`, per
//! `docs/specs/node-config.md`'s `config_pure.rs` plan.
//!
//! Every test here goes through `Config::from_toml_str` only (no env, no fs) —
//! or, for the pinned `Display` strings, constructs a `ConfigError` variant
//! directly (still no env/fs — just a data literal). This file is expected to
//! fail to COMPILE until `crates/wasl-mcp/src/config.rs` exists and is
//! re-exported from the crate root: that failure is the test-first baseline.

use wasl_mcp::{
    config::{peer_names, PeerName},
    A2aConfig, AdapterConfig, AdapterKind, AuditConfig, Config, ConfigError, GateConfig,
    IdentityConfig, OperatorConfig, PeerEntry, RuntimeConfig,
};

/// The complete example from the spec: every section, every key, two peers.
fn full_example_toml() -> &'static str {
    r#"
[identity]
agent_id = "planner"
host_id  = "hostA"

[a2a]
bind = "127.0.0.1:4242"

[operator]
bind = "127.0.0.1:4243"

[audit]
path = "/var/lib/wasl/audit.jsonl"

[adapter]
kind = "claude"
bin  = "/opt/claude/claude"

[[peers]]
agent_id = "reviewer"
host_id  = "hostB"
url      = "http://10.0.0.7:4242/"

[[peers]]
agent_id = "builder"
host_id  = "hostA"
url      = "http://127.0.0.1:4343/"
"#
}

// ---------------------------------------------------------------------
// Full / empty / partial parse
// ---------------------------------------------------------------------

#[test]
fn full_parse_every_field_exact() {
    let cfg = Config::from_toml_str(full_example_toml()).expect("the complete example must parse");

    let expected = Config {
        identity: IdentityConfig {
            agent_id: "planner".to_string(),
            host_id: "hostA".to_string(),
            host_name: String::new(),
        },
        a2a: A2aConfig {
            bind: Some("127.0.0.1:4242".to_string()),
        },
        operator: OperatorConfig {
            bind: Some("127.0.0.1:4243".to_string()),
        },
        gate: GateConfig::default(),
        runtime: RuntimeConfig::default(),
        audit: AuditConfig {
            path: "/var/lib/wasl/audit.jsonl".to_string(),
        },
        adapter: AdapterConfig {
            kind: AdapterKind::Claude,
            bin: Some("/opt/claude/claude".to_string()),
        },
        peers: vec![
            PeerEntry {
                agent_id: "reviewer".to_string(),
                host_id: "hostB".to_string(),
                url: "http://10.0.0.7:4242/".to_string(),
                name: String::new(),
                host_name: String::new(),
            },
            PeerEntry {
                agent_id: "builder".to_string(),
                host_id: "hostA".to_string(),
                url: "http://127.0.0.1:4343/".to_string(),
                name: String::new(),
                host_name: String::new(),
            },
        ],
    };

    assert_eq!(
        cfg, expected,
        "every field of the complete example, both peers in order"
    );
}

#[test]
fn empty_string_is_fully_default() {
    let cfg = Config::from_toml_str("").expect("an empty document is a fully-default config");
    assert_eq!(cfg, Config::default());

    // Spelled out explicitly per the precedence table's built-in-default column.
    assert_eq!(cfg.identity.agent_id, "wasl-local");
    assert_eq!(cfg.identity.host_id, "localhost");
    assert_eq!(cfg.audit.path, "wasl-audit.jsonl");
    assert_eq!(cfg.a2a.bind, None);
    assert_eq!(cfg.operator.bind, None);
    assert_eq!(cfg.adapter.kind, AdapterKind::Claude);
    assert_eq!(cfg.adapter.bin, None);
    assert!(cfg.peers.is_empty());
}

#[test]
fn partial_file_sets_only_the_specified_field() {
    let cfg =
        Config::from_toml_str("[identity]\nagent_id = \"x\"\n").expect("partial file must parse");

    assert_eq!(cfg.identity.agent_id, "x", "the one key present");
    // Everything else stays at its built-in default.
    assert_eq!(cfg.identity.host_id, "localhost");
    assert_eq!(cfg.audit.path, "wasl-audit.jsonl");
    assert_eq!(cfg.a2a.bind, None);
    assert_eq!(cfg.operator.bind, None);
    assert_eq!(cfg.adapter.kind, AdapterKind::Claude);
    assert_eq!(cfg.adapter.bin, None);
    assert!(cfg.peers.is_empty());
}

#[test]
fn operator_bind_is_parsed_but_unused() {
    let cfg = Config::from_toml_str("[operator]\nbind = \"127.0.0.1:1\"\n")
        .expect("a lone operator.bind must parse and validate");

    assert_eq!(cfg.operator.bind, Some("127.0.0.1:1".to_string()));
    // Nothing else was touched by this key — still every other built-in default.
    assert_eq!(cfg.identity.agent_id, "wasl-local");
    assert_eq!(cfg.identity.host_id, "localhost");
    assert_eq!(cfg.audit.path, "wasl-audit.jsonl");
    assert_eq!(cfg.a2a.bind, None);
    assert_eq!(cfg.adapter.kind, AdapterKind::Claude);
    assert!(cfg.peers.is_empty());
}

// ---------------------------------------------------------------------
// Feature A — USER-facing names on identity + peers
// ---------------------------------------------------------------------

/// The names (`identity.host_name`, `peers[].name`, `peers[].host_name`) parse
/// when present and populate the name side-table `list_peers` reads.
#[test]
fn names_parse_on_identity_and_peers() {
    let toml = r#"
[identity]
agent_id = "draft-node-abc"
host_id  = "host-16"
host_name = "gpu-box"

[[peers]]
agent_id = "draft-node-writer"
host_id  = "host-17"
url      = "http://10.0.0.7:48000/"
name = "Writer"
host_name = "cpu-box"
"#;
    let cfg = Config::from_toml_str(toml).expect("names must parse");
    assert_eq!(cfg.identity.host_name, "gpu-box");
    assert_eq!(cfg.peers.len(), 1);
    assert_eq!(cfg.peers[0].name, "Writer");
    assert_eq!(cfg.peers[0].host_name, "cpu-box");

    let names = peer_names(&cfg.peers);
    assert_eq!(
        names.get("draft-node-writer"),
        Some(&PeerName {
            name: "Writer".to_string(),
            host_name: "cpu-box".to_string(),
        }),
        "peer_names maps agent_id -> display names"
    );
}

/// Forward-compat: a roster from an un-upgraded Marsad that emits NO name /
/// host_name keys still parses, with the names defaulting to empty (never a
/// hard error). This is the graceful-degradation guarantee for the existing
/// ArchAttention roster before its first re-derivation.
#[test]
fn nameless_peer_and_identity_still_parse() {
    let toml = r#"
[identity]
agent_id = "draft-node-abc"
host_id  = "host-16"

[[peers]]
agent_id = "draft-node-writer"
host_id  = "host-17"
url      = "http://10.0.0.7:48000/"
"#;
    let cfg = Config::from_toml_str(toml).expect("a name-less roster must still parse");
    assert_eq!(cfg.identity.host_name, "", "absent host_name defaults empty");
    assert_eq!(cfg.peers[0].name, "", "absent peer name defaults empty");
    assert_eq!(cfg.peers[0].host_name, "", "absent peer host_name defaults empty");
}

// ---------------------------------------------------------------------
// TOML-shape errors
// ---------------------------------------------------------------------

#[test]
fn invalid_toml_syntax_is_toml_error() {
    let err = Config::from_toml_str("this is not toml").expect_err("garbage TOML must not parse");
    assert!(matches!(err, ConfigError::Toml(_)), "got: {err:?}");
}

#[test]
fn wrong_type_is_toml_error() {
    let err = Config::from_toml_str("[identity]\nagent_id = 7\n")
        .expect_err("a non-string agent_id must fail");
    assert!(matches!(err, ConfigError::Toml(_)), "got: {err:?}");
}

#[test]
fn unknown_key_in_section_is_toml_error() {
    let err = Config::from_toml_str("[identity]\nagent = \"x\"\n")
        .expect_err("a typo'd key must be rejected, not silently dropped");
    match err {
        ConfigError::Toml(detail) => {
            assert!(
                detail.contains("agent"),
                "offending key name missing from detail: {detail}"
            );
        }
        other => panic!("expected ConfigError::Toml, got {other:?}"),
    }
}

#[test]
fn unknown_top_level_section_is_toml_error() {
    let err = Config::from_toml_str("[bogus]\nx = 1\n")
        .expect_err("an unrecognized top-level section must be rejected");
    match err {
        ConfigError::Toml(detail) => {
            assert!(
                detail.contains("bogus"),
                "offending section name missing from detail: {detail}"
            );
        }
        other => panic!("expected ConfigError::Toml, got {other:?}"),
    }
}

// ---------------------------------------------------------------------
// adapter.kind validation
// ---------------------------------------------------------------------

#[test]
fn unknown_adapter_kind_is_rejected() {
    // W7 (`docs/specs/adapter-codex.md`) makes "codex" a KNOWN kind, so this
    // pinned test (originally written against "codex" before W7 landed, per
    // node-config.md's own W2-era test plan) now uses a kind that remains
    // unknown even after W7 — same precedent as the two frozen-file edits
    // `docs/specs/mcp-gating.md` §9 authorized for W4. See
    // `crates/wasl-mcp/AGENTS.md` for the note; `tests/config_codex.rs`
    // covers "codex" itself (now a valid kind) plus a fresh
    // still-unknown-kind probe for the W7 world.
    let err =
        Config::from_toml_str("[adapter]\nkind = \"gpt\"\n").expect_err("gpt is not a known kind");
    let display = err.to_string();
    assert!(
        display.contains("unknown adapter kind: gpt"),
        "display was: {display}"
    );
    match err {
        ConfigError::UnknownAdapterKind(k) => {
            assert_eq!(k, "gpt", "offending kind preserved verbatim")
        }
        other => panic!("expected ConfigError::UnknownAdapterKind, got {other:?}"),
    }
}

#[test]
fn adapter_kind_match_is_case_sensitive() {
    let err = Config::from_toml_str("[adapter]\nkind = \"Claude\"\n")
        .expect_err("only the exact lowercase \"claude\" is accepted");
    match err {
        ConfigError::UnknownAdapterKind(k) => assert_eq!(k, "Claude"),
        other => panic!("expected ConfigError::UnknownAdapterKind, got {other:?}"),
    }
}

// ---------------------------------------------------------------------
// peer url validation
// ---------------------------------------------------------------------

#[test]
fn malformed_peer_url_relative_is_rejected() {
    let toml = "[[peers]]\nagent_id = \"p1\"\nhost_id = \"h1\"\nurl = \"not-a-url\"\n";
    let err = Config::from_toml_str(toml).expect_err("a relative url must be rejected");
    match err {
        ConfigError::MalformedPeerUrl {
            agent_id,
            url,
            detail: _,
        } => {
            assert_eq!(agent_id, "p1");
            assert_eq!(url, "not-a-url");
        }
        other => panic!("expected ConfigError::MalformedPeerUrl, got {other:?}"),
    }
}

#[test]
fn malformed_peer_url_wrong_scheme_is_rejected() {
    let toml = "[[peers]]\nagent_id = \"p1\"\nhost_id = \"h1\"\nurl = \"ftp://x/\"\n";
    let err = Config::from_toml_str(toml).expect_err("a non-http(s) scheme must be rejected");
    match err {
        ConfigError::MalformedPeerUrl {
            agent_id,
            url,
            detail: _,
        } => {
            assert_eq!(agent_id, "p1");
            assert_eq!(url, "ftp://x/");
        }
        other => panic!("expected ConfigError::MalformedPeerUrl, got {other:?}"),
    }
}

#[test]
fn valid_https_peer_url_is_accepted() {
    let toml = "[[peers]]\nagent_id = \"p1\"\nhost_id = \"h1\"\nurl = \"https://example.com/\"\n";
    let cfg = Config::from_toml_str(toml).expect("a valid https url must be accepted");
    assert_eq!(cfg.peers.len(), 1);
    assert_eq!(cfg.peers[0].url, "https://example.com/");
}

#[test]
fn duplicate_peer_agent_id_is_rejected() {
    let toml = concat!(
        "[[peers]]\n",
        "agent_id = \"b\"\n",
        "host_id = \"h1\"\n",
        "url = \"https://a.example/\"\n",
        "\n",
        "[[peers]]\n",
        "agent_id = \"b\"\n",
        "host_id = \"h2\"\n",
        "url = \"https://b.example/\"\n",
    );
    let err = Config::from_toml_str(toml).expect_err("a repeated agent_id must be rejected");
    let display = err.to_string();
    assert!(
        display.contains("duplicate peer agent_id: b"),
        "display was: {display}"
    );
    match err {
        ConfigError::DuplicatePeer(id) => assert_eq!(id, "b"),
        other => panic!("expected ConfigError::DuplicatePeer, got {other:?}"),
    }
}

// ---------------------------------------------------------------------
// bind validation
// ---------------------------------------------------------------------

#[test]
fn invalid_a2a_bind_is_rejected() {
    let err = Config::from_toml_str("[a2a]\nbind = \"not-an-addr\"\n")
        .expect_err("a malformed a2a.bind must fail at load, not at run()");
    match err {
        ConfigError::InvalidBind {
            section,
            value,
            detail: _,
        } => {
            assert_eq!(section, "a2a");
            assert_eq!(value, "not-an-addr");
        }
        other => panic!("expected ConfigError::InvalidBind, got {other:?}"),
    }
}

#[test]
fn invalid_operator_bind_is_rejected() {
    let err = Config::from_toml_str("[operator]\nbind = \"not-an-addr\"\n")
        .expect_err("a malformed operator.bind must fail at load too");
    match err {
        ConfigError::InvalidBind {
            section,
            value,
            detail: _,
        } => {
            assert_eq!(section, "operator");
            assert_eq!(value, "not-an-addr");
        }
        other => panic!("expected ConfigError::InvalidBind, got {other:?}"),
    }
}

// ---------------------------------------------------------------------
// Display — pinned strings for every variant
// ---------------------------------------------------------------------

#[test]
fn display_strings_are_pinned_exactly() {
    assert_eq!(
        ConfigError::Read {
            path: "/etc/wasl/config.toml".to_string(),
            detail: "permission denied".to_string(),
        }
        .to_string(),
        "read config /etc/wasl/config.toml: permission denied"
    );

    assert_eq!(
        ConfigError::Toml("missing field `bind`".to_string()).to_string(),
        "invalid config TOML: missing field `bind`"
    );

    assert_eq!(
        ConfigError::UnknownAdapterKind("codex".to_string()).to_string(),
        "unknown adapter kind: codex"
    );

    assert_eq!(
        ConfigError::MalformedPeerUrl {
            agent_id: "reviewer".to_string(),
            url: "not-a-url".to_string(),
            detail: "relative URL without a base".to_string(),
        }
        .to_string(),
        "malformed peer url for reviewer: not-a-url: relative URL without a base"
    );

    assert_eq!(
        ConfigError::DuplicatePeer("b".to_string()).to_string(),
        "duplicate peer agent_id: b"
    );

    assert_eq!(
        ConfigError::InvalidBind {
            section: "a2a".to_string(),
            value: "not-an-addr".to_string(),
            detail: "invalid socket address syntax".to_string(),
        }
        .to_string(),
        "invalid a2a.bind \"not-an-addr\": invalid socket address syntax"
    );
}
