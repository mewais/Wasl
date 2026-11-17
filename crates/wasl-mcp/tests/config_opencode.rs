//! Opencode adapter selectability tests (W9), per
//! `docs/specs/adapter-opencode.md`'s "Node-config integration" delta on top of
//! `docs/specs/node-config.md` (mirroring W7b's `config_codex.rs`).
//!
//! `tests/config_pure.rs` is FROZEN (a pinned acceptance fixture for W2/W4) so
//! this is a NEW file rather than an extension of it. It covers:
//!   1. `kind = "opencode"` parses to `AdapterKind::Opencode`.
//!   2. `kind = "opencode"` with a `bin` override (default bin is `"opencode"`).
//!   3. unknown-kind pins are intact: a kind that is still genuinely unknown
//!      even after this block (incl. the spec-called-out near-miss
//!      `"Opencode"`, which is NOT a valid `kind` value — only the exact
//!      literal string `"opencode"` is — plus a clearly-bogus kind).
//!   4. `build_adapter` returns the opencode-backed `NodeAdapter::Opencode`
//!      variant for `AdapterKind::Opencode` (the exhaustive-match delegating
//!      enum from the spec), and the pre-existing Claude/Codex arms
//!      are undisturbed.
//!   5. a full config -> registry -> loopback A2A dispatch proof where the
//!      RECEIVE side is a real `NodeAdapter::Opencode` (built via
//!      `build_adapter` from a config with a stub `opencode` bin emitting the
//!      spec's JSONL shape), proving the whole receive path composes end to end.
//!
//! This file is expected to fail to COMPILE until `AdapterKind::Opencode` and
//! `NodeAdapter::Opencode` exist in `crates/wasl-mcp/src/config.rs` — that
//! failure is the test-first baseline.

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
// 1 + 2: kind = "opencode" parses, with and without a bin override
// ---------------------------------------------------------------------

#[test]
fn kind_opencode_parses_to_adapter_kind_opencode() {
    let cfg = Config::from_toml_str("[adapter]\nkind = \"opencode\"\n")
        .expect("opencode is now a known kind in this block");
    assert_eq!(cfg.adapter.kind, AdapterKind::Opencode);
    assert_eq!(
        cfg.adapter.bin, None,
        "no bin key -> absent, same meaning as the claude/codex arms \
         (default bin is \"opencode\")"
    );
}

#[test]
fn kind_opencode_with_bin_override_is_captured_verbatim() {
    let cfg =
        Config::from_toml_str("[adapter]\nkind = \"opencode\"\nbin  = \"/opt/opencode/oc\"\n")
            .expect("opencode + bin must parse");
    assert_eq!(cfg.adapter.kind, AdapterKind::Opencode);
    assert_eq!(cfg.adapter.bin, Some("/opt/opencode/oc".to_string()));
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
fn adapter_kind_opencode_case_variant_is_still_unknown() {
    // Exact, case-sensitive match per the spec: "Opencode" must NOT be accepted
    // (spec explicitly calls this near-miss out).
    let err = Config::from_toml_str("[adapter]\nkind = \"Opencode\"\n")
        .expect_err("only the exact lowercase \"opencode\" is accepted");
    match err {
        ConfigError::UnknownAdapterKind(k) => assert_eq!(k, "Opencode"),
        other => panic!("expected ConfigError::UnknownAdapterKind, got {other:?}"),
    }
}

#[test]
fn adapter_kind_clearly_bogus_value_is_unknown() {
    let err = Config::from_toml_str("[adapter]\nkind = \"totally-not-a-real-adapter\"\n")
        .expect_err("a clearly-bogus kind must be rejected");
    match err {
        ConfigError::UnknownAdapterKind(k) => assert_eq!(k, "totally-not-a-real-adapter"),
        other => panic!("expected ConfigError::UnknownAdapterKind, got {other:?}"),
    }
}

// ---------------------------------------------------------------------
// 4: build_adapter selects the opencode-backed NodeAdapter variant,
// undisturbed pre-existing arms
// ---------------------------------------------------------------------

#[test]
fn build_adapter_selects_the_opencode_variant_for_adapter_kind_opencode() {
    let cfg = AdapterConfig {
        kind: AdapterKind::Opencode,
        bin: None,
    };
    let adapter = build_adapter(&cfg);
    match adapter {
        NodeAdapter::Opencode(_) => {}
        NodeAdapter::Claude(_) => panic!("kind Opencode must build NodeAdapter::Opencode"),
        NodeAdapter::Codex(_) => panic!("kind Opencode must build NodeAdapter::Opencode"),
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
        NodeAdapter::Codex(_) => panic!("kind Claude must build NodeAdapter::Claude"),
        NodeAdapter::Opencode(_) => panic!("kind Claude must build NodeAdapter::Claude"),
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
        NodeAdapter::Claude(_) => panic!("kind Codex must build NodeAdapter::Codex"),
        NodeAdapter::Opencode(_) => panic!("kind Codex must build NodeAdapter::Codex"),
    }
}

// ---------------------------------------------------------------------
// 5: config -> registry -> loopback dispatch, RECEIVE side is a real
// NodeAdapter::Opencode driving a stub `opencode` bin.
// ---------------------------------------------------------------------

/// Write an executable shell-script stand-in for the `opencode` CLI. It
/// emulates opencode's verified contract: the invocation is
/// `run --format json [-s <id>] [-m <model>] <positional-prompt>`, the prompt
/// is the LAST argv element, and stdin is IGNORED (opencode's prompt is
/// positional). It emits a JSONL stream — a `step_start`, a `text` event
/// carrying the reply at `.part.text` (with `.part.messageID` and
/// `.part.type == "text"`), and a `step_finish` — each with a top-level
/// `sessionID` (`ses_stub…`). This stub deliberately inspects argv (asserting
/// a non-empty positional prompt as the last element, never on stdin) rather
/// than reading stdin, proving the plumbing composed the prompt into argv.
fn write_opencode_stub(dir: &std::path::Path) -> std::path::PathBuf {
    let script_path = dir.join("opencode-stub.sh");
    let script = [
        "#!/bin/sh",
        "# opencode's prompt arrives as the LAST argv element (positional),",
        "# never on stdin -- assert that shape here instead of reading stdin.",
        "if [ \"$#\" -lt 1 ]; then",
        "  echo \"expected at least a positional prompt argv element, got $#\" >&2",
        "  exit 1",
        "fi",
        "# The first argv element must be the `run` subcommand.",
        "if [ \"$1\" != \"run\" ]; then",
        "  echo \"expected argv[1] to be 'run', got: $1\" >&2",
        "  exit 1",
        "fi",
        "# The prompt is the strictly-last argv element and must be non-empty.",
        "eval \"prompt=\\${$#}\"",
        "if [ -z \"$prompt\" ]; then",
        "  echo \"expected a non-empty prompt as the last argv element\" >&2",
        "  exit 1",
        "fi",
        "sid=\"ses_stub000000000000000000000000\"",
        "printf '%s\\n' '{\"type\":\"step_start\",\"sessionID\":\"'\"$sid\"'\",\"part\":{\"messageID\":\"msg_1\",\"type\":\"step-start\"}}'",
        "printf '%s\\n' '{\"type\":\"text\",\"sessionID\":\"'\"$sid\"'\",\"part\":{\"messageID\":\"msg_1\",\"type\":\"text\",\"text\":\"PONG\",\"time\":{\"start\":1,\"end\":2}}}'",
        "printf '%s\\n' '{\"type\":\"step_finish\",\"sessionID\":\"'\"$sid\"'\",\"part\":{\"messageID\":\"msg_1\",\"reason\":\"stop\",\"type\":\"step-finish\"}}'",
    ]
    .join("\n");
    std::fs::write(&script_path, format!("{script}\n")).expect("write opencode stub script");
    let mut perms = std::fs::metadata(&script_path)
        .expect("stat stub")
        .permissions();
    perms.set_mode(0o755);
    std::fs::set_permissions(&script_path, perms).expect("chmod +x stub");
    script_path
}

fn a2a_card() -> A2aCard {
    A2aCard {
        name: "opencode-loopback-peer".to_string(),
        description: "wasl-mcp config-opencode loopback peer".to_string(),
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
/// `NodeAdapter::Opencode` (built from a config-declared stub bin via
/// `build_adapter`), bound before `axum::serve` spawns (no readiness race).
async fn spawn_opencode_receiver(bin_path: &std::path::Path) -> Peer {
    let cfg = AdapterConfig {
        kind: AdapterKind::Opencode,
        bin: Some(bin_path.to_string_lossy().into_owned()),
    };
    let adapter = build_adapter(&cfg);
    assert!(
        matches!(adapter, NodeAdapter::Opencode(_)),
        "the receive side must be the opencode-backed variant"
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
async fn config_opencode_receive_composes_over_real_loopback_a2a() {
    let dir = tempfile::tempdir().expect("tempdir");
    let stub = write_opencode_stub(dir.path());

    // RECEIVE side: a real A2aServer over a real AdapterExecutor wrapping a
    // real NodeAdapter::Opencode driving the stub `opencode` bin.
    let peer = spawn_opencode_receiver(&stub).await;

    // SEND side: config -> registry, exactly mirroring config_codex.rs's
    // loopback proof, but pointed at the
    // opencode-backed receiver above.
    let toml = format!(
        "[[peers]]\nagent_id = \"opencode-peer\"\nhost_id = \"loopback\"\nurl = \"{}\"\n",
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
        peers: vec![agent("opencode-peer")],
        question: "status?".to_string(),
    });

    let reply = resolve_and_dispatch(
        &ctx,
        &GateOutcome::Approved,
        call,
        "config-opencode-consult",
        0,
        &http,
        &mut runner,
        &mut audit,
    )
    .await
    .expect("an approved consult over the opencode-backed peer must succeed");

    match reply {
        ToolReply::Consult(res) => {
            assert_eq!(res.replies.len(), 1);
            assert_eq!(res.replies[0].from, agent("opencode-peer"));
            assert_eq!(
                res.replies[0].body, "PONG",
                "the reply text must be the stub's `.part.text` verbatim, proving \
                 compose_prompt -> build_argv (prompt as the last positional argv element) -> \
                 the real child process -> parse_reply all composed correctly"
            );
        }
        other => panic!("expected Consult reply, got {other:?}"),
    }
}
