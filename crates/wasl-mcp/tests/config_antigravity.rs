//! Antigravity adapter selectability tests (W8′b), per
//! `docs/specs/adapter-antigravity.md`'s "Node-config integration" delta on
//! top of `docs/specs/node-config.md` (mirroring W7b's `config_codex.rs` for
//! the codex arm).
//!
//! `tests/config_pure.rs` is FROZEN (a pinned acceptance fixture for W2/W4)
//! so this is a NEW file rather than an extension of it. It covers:
//!   1. `kind = "antigravity"` parses to `AdapterKind::Antigravity`.
//!   2. `kind = "antigravity"` with a `bin` override (default bin is `"agy"`).
//!   3. unknown-kind pins are intact: a kind that is still genuinely unknown
//!      even after this block (incl. the spec-called-out near-misses
//!      `"Antigravity"` and `"agy"` itself, which are NOT valid `kind`
//!      values — only the exact literal string `"antigravity"` is).
//!   4. `build_adapter` returns the antigravity-backed `NodeAdapter::Antigravity`
//!      variant for `AdapterKind::Antigravity` (the exhaustive-match
//!      delegating enum from the spec), and the pre-existing Claude/Codex
//!      arms are undisturbed.
//!   5. a full config -> registry -> loopback A2A dispatch proof where the
//!      RECEIVE side is a real `NodeAdapter::Antigravity` (built via
//!      `build_adapter` from a config with a stub `agy` bin emitting the
//!      spec's VERIFIED SUCCESS JSON shape), proving the whole receive path
//!      composes end to end.
//!
//! This file is expected to fail to COMPILE until `AdapterKind::Antigravity`
//! and `NodeAdapter::Antigravity` exist in `crates/wasl-mcp/src/config.rs` —
//! that failure is the test-first baseline.
//!
//! Unlike W7b, THIS block requires NO edit to any previously-existing test
//! file: `"antigravity"`/`"agy"`/`"Antigravity"` are not used as an
//! unknown-kind probe anywhere in the frozen suites (`tests/config_pure.rs`
//! and `tests/config_codex.rs` both probe `"gpt"`, which stays unknown after
//! this block too) — verified by grep before writing this file.

use std::os::unix::fs::PermissionsExt;
use std::sync::Arc;
use std::time::Duration;

use wasl_a2a_server::A2aServer;
use wasl_a2a_types::{AgentCapabilities, AgentCard as A2aCard};
use wasl_adapter::AdapterExecutor;
use wasl_audit::AuditLog;
use wasl_gate::GateOutcome;
use wasl_mcp::handle::{resolve_and_dispatch, ToolCall, ToolCtx, ToolReply};
use wasl_mcp::{
    build_adapter, registry_from_peers, AdapterConfig, AdapterKind, Config, ConfigError, MemSink,
    NodeAdapter,
};
use wasl_proto::{AgentId, ConsultParams, HostId};

fn agent(id: &str) -> AgentId {
    AgentId(id.to_string())
}

fn host(id: &str) -> HostId {
    HostId(id.to_string())
}

// ---------------------------------------------------------------------
// 1 + 2: kind = "antigravity" parses, with and without a bin override
// ---------------------------------------------------------------------

#[test]
fn kind_antigravity_parses_to_adapter_kind_antigravity() {
    let cfg = Config::from_toml_str("[adapter]\nkind = \"antigravity\"\n")
        .expect("antigravity is now a known kind in this block");
    assert_eq!(cfg.adapter.kind, AdapterKind::Antigravity);
    assert_eq!(
        cfg.adapter.bin, None,
        "no bin key -> absent, same meaning as the claude/codex arms (default bin is \"agy\")"
    );
}

#[test]
fn kind_antigravity_with_bin_override_is_captured_verbatim() {
    let cfg = Config::from_toml_str(
        "[adapter]\nkind = \"antigravity\"\nbin  = \"/opt/antigravity/agy\"\n",
    )
    .expect("antigravity + bin must parse");
    assert_eq!(cfg.adapter.kind, AdapterKind::Antigravity);
    assert_eq!(cfg.adapter.bin, Some("/opt/antigravity/agy".to_string()));
}

// ---------------------------------------------------------------------
// 3: unknown-kind pins are intact
// ---------------------------------------------------------------------

#[test]
fn unknown_adapter_kind_error_still_pinned_for_a_kind_that_is_still_unknown() {
    let err = Config::from_toml_str("[adapter]\nkind = \"gpt\"\n")
        .expect_err("gpt is not a known kind even after this block");
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
fn adapter_kind_antigravity_case_variant_is_still_unknown() {
    // Exact, case-sensitive match per the spec: "Antigravity" must NOT be
    // accepted (spec explicitly calls this near-miss out).
    let err = Config::from_toml_str("[adapter]\nkind = \"Antigravity\"\n")
        .expect_err("only the exact lowercase \"antigravity\" is accepted");
    match err {
        ConfigError::UnknownAdapterKind(k) => assert_eq!(k, "Antigravity"),
        other => panic!("expected ConfigError::UnknownAdapterKind, got {other:?}"),
    }
}

#[test]
fn adapter_kind_agy_bin_name_is_still_unknown_as_a_kind() {
    // The spec explicitly calls out "agy" itself (the binary name) as NOT a
    // valid `kind` value — only "antigravity" is.
    let err = Config::from_toml_str("[adapter]\nkind = \"agy\"\n")
        .expect_err("\"agy\" is a bin name, not a kind value");
    match err {
        ConfigError::UnknownAdapterKind(k) => assert_eq!(k, "agy"),
        other => panic!("expected ConfigError::UnknownAdapterKind, got {other:?}"),
    }
}

// ---------------------------------------------------------------------
// 4: build_adapter selects the antigravity-backed NodeAdapter variant,
// undisturbed pre-existing arms
// ---------------------------------------------------------------------

#[test]
fn build_adapter_selects_the_antigravity_variant_for_adapter_kind_antigravity() {
    let cfg = AdapterConfig {
        kind: AdapterKind::Antigravity,
        bin: None,
    };
    let adapter = build_adapter(&cfg);
    match adapter {
        NodeAdapter::Antigravity(_) => {}
        NodeAdapter::Claude(_) => panic!("kind Antigravity must build NodeAdapter::Antigravity"),
        NodeAdapter::Codex(_) => panic!("kind Antigravity must build NodeAdapter::Antigravity"),
    }
}

#[test]
fn build_adapter_still_selects_the_claude_variant_for_adapter_kind_claude() {
    let cfg = AdapterConfig {
        kind: AdapterKind::Claude,
        bin: None,
    };
    let adapter = build_adapter(&cfg);
    match adapter {
        NodeAdapter::Claude(_) => {}
        other => panic!(
            "kind Claude must build NodeAdapter::Claude, got a different variant: {}",
            match other {
                NodeAdapter::Claude(_) => "Claude",
                NodeAdapter::Codex(_) => "Codex",
                NodeAdapter::Antigravity(_) => "Antigravity",
            }
        ),
    }
}

#[test]
fn build_adapter_still_selects_the_codex_variant_for_adapter_kind_codex() {
    let cfg = AdapterConfig {
        kind: AdapterKind::Codex,
        bin: None,
    };
    let adapter = build_adapter(&cfg);
    match adapter {
        NodeAdapter::Codex(_) => {}
        other => panic!(
            "kind Codex must build NodeAdapter::Codex, got a different variant: {}",
            match other {
                NodeAdapter::Claude(_) => "Claude",
                NodeAdapter::Codex(_) => "Codex",
                NodeAdapter::Antigravity(_) => "Antigravity",
            }
        ),
    }
}

// ---------------------------------------------------------------------
// 5: config -> registry -> loopback dispatch, RECEIVE side is a real
// NodeAdapter::Antigravity driving a stub `agy` bin.
// ---------------------------------------------------------------------

/// The spec's VERIFIED SUCCESS JSON shape (`docs/specs/adapter-antigravity.md`,
/// the W8′-fix correction / "Test plan" section), byte-for-byte:
/// `VERIFIED_SUCCESS_SAMPLE` — the first-ever observed `status:"SUCCESS"`
/// live reply.
const VERIFIED_SUCCESS_SAMPLE: &str = concat!(
    "{\"conversation_id\":\"3d206228-0e90-48b4-9c05-4a6d14ff3d8b\",\"status\":\"SUCCESS\",",
    "\"response\":\"bt-7731\\n\",\"error\":null,\"num_turns\":1}\n",
);

/// Write an executable shell-script stand-in for the `agy` CLI that emits the
/// fixture above verbatim, but only after confirming the prompt arrived the
/// way the spec says it must: as the LAST ARGV element, immediately preceded
/// by the `-p` flag (`build_argv`'s pinned `["-p", prompt]` tail) — NOT on
/// stdin. agy has no evidenced stdin-prompt form (`docs/specs/
/// adapter-antigravity.md`, "Prompt in argv"), so this stub deliberately
/// reads `$#`/positional argv, never stdin, to prove the plumbing composed
/// the prompt into argv rather than piping it.
fn write_agy_stub(dir: &std::path::Path) -> std::path::PathBuf {
    let script_path = dir.join("agy-stub.sh");
    let script = [
        "#!/bin/sh",
        "# agy's prompt arrives as the LAST argv element (preceded by -p),",
        "# never on stdin -- assert that shape here instead of reading stdin.",
        "if [ \"$#\" -lt 2 ]; then",
        "  echo \"expected at least 2 argv elements (a -p flag + a prompt value), got $#\" >&2",
        "  exit 1",
        "fi",
        "shift $(($# - 2))",
        "flag=\"$1\"",
        "prompt=\"$2\"",
        "if [ \"$flag\" != \"-p\" ]; then",
        "  echo \"expected argv[-2] to be -p, got: $flag\" >&2",
        "  exit 1",
        "fi",
        "if [ -z \"$prompt\" ]; then",
        "  echo \"expected a non-empty prompt as the last argv element\" >&2",
        "  exit 1",
        "fi",
    ]
    .join("\n");
    let content = format!("{script}\ncat <<'JSON'\n{VERIFIED_SUCCESS_SAMPLE}JSON\n");
    std::fs::write(&script_path, content).expect("write agy stub script");
    let mut perms = std::fs::metadata(&script_path)
        .expect("stat stub")
        .permissions();
    perms.set_mode(0o755);
    std::fs::set_permissions(&script_path, perms).expect("chmod +x stub");
    script_path
}

fn a2a_card() -> A2aCard {
    A2aCard {
        name: "antigravity-loopback-peer".to_string(),
        description: "wasl-mcp config-antigravity loopback peer".to_string(),
        url: "http://127.0.0.1/".to_string(),
        version: "0.1.0".to_string(),
        capabilities: AgentCapabilities {
            streaming: false,
            push_notifications: false,
        },
        default_input_modes: vec!["text/plain".to_string()],
        default_output_modes: vec!["text/plain".to_string()],
        skills: vec![],
    }
}

struct Peer {
    url: String,
    handle: tokio::task::JoinHandle<()>,
}

impl Drop for Peer {
    fn drop(&mut self) {
        self.handle.abort();
    }
}

/// Spin a REAL `A2aServer` over a REAL `AdapterExecutor` wrapping a REAL
/// `NodeAdapter::Antigravity` (built from a config-declared stub bin via
/// `build_adapter`), bound before `axum::serve` spawns (no readiness race).
async fn spawn_antigravity_receiver(bin_path: &std::path::Path) -> Peer {
    let cfg = AdapterConfig {
        kind: AdapterKind::Antigravity,
        bin: Some(bin_path.to_string_lossy().into_owned()),
    };
    let adapter = build_adapter(&cfg);
    assert!(
        matches!(adapter, NodeAdapter::Antigravity(_)),
        "the receive side must be the antigravity-backed variant"
    );

    let executor = Arc::new(AdapterExecutor::new(adapter));
    let server = Arc::new(A2aServer::new(a2a_card(), executor));
    let app = server.router();

    let listener = tokio::net::TcpListener::bind("127.0.0.1:0")
        .await
        .expect("bind ephemeral port");
    let port = listener.local_addr().expect("local addr").port();
    let handle = tokio::spawn(async move {
        axum::serve(listener, app).await.expect("serve");
    });

    Peer {
        url: format!("http://127.0.0.1:{port}/"),
        handle,
    }
}

/// A `Runner` that must never be invoked — this suite only drives `Consult`,
/// which never reaches the transfer seam.
struct UnusedRunner;

impl wasl_transfer::Runner for UnusedRunner {
    fn run(&mut self, _argv: &[String]) -> Result<(), String> {
        panic!("a Consult call must never touch the transfer runner");
    }
}

#[tokio::test]
async fn config_antigravity_receive_composes_over_real_loopback_a2a() {
    let dir = tempfile::tempdir().expect("tempdir");
    let stub = write_agy_stub(dir.path());

    // RECEIVE side: a real A2aServer over a real AdapterExecutor wrapping a
    // real NodeAdapter::Antigravity driving the stub `agy` bin.
    let peer = spawn_antigravity_receiver(&stub).await;

    // SEND side: config -> registry, exactly mirroring config_codex.rs's
    // loopback proof, but pointed at the antigravity-backed receiver above.
    let toml = format!(
        "[[peers]]\nagent_id = \"antigravity-peer\"\nhost_id = \"loopback\"\nurl = \"{}\"\n",
        peer.url
    );
    let cfg = Config::from_toml_str(&toml).expect("a single valid peer entry must parse");
    let registry = registry_from_peers(&cfg.peers);

    let self_id = agent("me");
    let self_host = host("h1");
    let ctx = ToolCtx {
        self_id: &self_id,
        self_host: &self_host,
        registry: &registry,
    };
    let http = reqwest::Client::builder()
        .timeout(Duration::from_secs(5))
        .build()
        .expect("build reqwest client");
    let mut runner = UnusedRunner;
    let mut audit = AuditLog::new(MemSink::new());

    let call = ToolCall::Consult(ConsultParams {
        peers: vec![agent("antigravity-peer")],
        question: "status?".to_string(),
    });

    let reply = resolve_and_dispatch(
        &ctx,
        &GateOutcome::Approved,
        call,
        "config-antigravity-consult",
        0,
        &http,
        &mut runner,
        &mut audit,
    )
    .await
    .expect("an approved consult over the antigravity-backed peer must succeed");

    match reply {
        ToolReply::Consult(res) => {
            assert_eq!(res.replies.len(), 1);
            assert_eq!(res.replies[0].from, agent("antigravity-peer"));
            assert_eq!(
                res.replies[0].body, "bt-7731\n",
                "the reply text must be the VERIFIED_SUCCESS_SAMPLE's `response` field verbatim, \
                 proving compose_prompt -> build_argv (prompt as the last two argv elements) -> \
                 the real child process -> parse_reply all composed correctly"
            );
        }
        other => panic!("expected Consult reply, got {other:?}"),
    }
}
