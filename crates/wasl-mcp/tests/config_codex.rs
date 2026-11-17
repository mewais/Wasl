//! Codex adapter selectability tests (W7b), per
//! `docs/specs/adapter-codex.md`'s "Node-config integration" delta on top of
//! `docs/specs/node-config.md`.
//!
//! `tests/config_pure.rs` is FROZEN (a pinned acceptance fixture for W2/W4) so
//! this is a NEW file rather than an extension of it. It covers:
//!   1. `kind = "codex"` parses to `AdapterKind::Codex`.
//!   2. `kind = "codex"` with a `bin` override.
//!   3. an unknown kind (something still NOT "codex" or "claude") is still
//!      rejected with the pinned `UnknownAdapterKind` error/message.
//!   4. `build_adapter` returns the codex-backed `NodeAdapter::Codex` variant
//!      for `AdapterKind::Codex` (the exhaustive-match delegating enum from
//!      the spec).
//!   5. a full config -> registry -> loopback A2A dispatch proof where the
//!      RECEIVE side is a real `NodeAdapter::Codex` (built via `build_adapter`
//!      from a config with a stub `codex` bin emitting the spec's captured
//!      JSONL shape), proving the whole receive path composes end to end.
//!
//! This file is expected to fail to COMPILE until `AdapterKind::Codex` and
//! `NodeAdapter` exist in `crates/wasl-mcp/src/config.rs` — that failure is
//! the test-first baseline (see also the one authorized edit to the frozen
//! `tests/config_pure.rs`'s `unknown_adapter_kind_is_rejected`, documented in
//! `crates/wasl-mcp/AGENTS.md`, which is now the ONLY test still asserting
//! "codex" itself is unknown-content, changed to a still-unknown kind since
//! this block makes "codex" a real one).

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
// 1 + 2: kind = "codex" parses, with and without a bin override
// ---------------------------------------------------------------------

#[test]
fn kind_codex_parses_to_adapter_kind_codex() {
    let cfg = Config::from_toml_str("[adapter]\nkind = \"codex\"\n")
        .expect("codex is now a known kind in this block");
    assert_eq!(cfg.adapter.kind, AdapterKind::Codex);
    assert_eq!(
        cfg.adapter.bin, None,
        "no bin key -> absent, same meaning as the claude arm"
    );
}

#[test]
fn kind_codex_with_bin_override_is_captured_verbatim() {
    let cfg =
        Config::from_toml_str("[adapter]\nkind = \"codex\"\nbin  = \"/opt/codex/codex-cli\"\n")
            .expect("codex + bin must parse");
    assert_eq!(cfg.adapter.kind, AdapterKind::Codex);
    assert_eq!(cfg.adapter.bin, Some("/opt/codex/codex-cli".to_string()));
}

// ---------------------------------------------------------------------
// 3: unknown kind error message still pinned (using a kind that is still
// genuinely unknown now that both "claude" and "codex" are valid)
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
fn adapter_kind_codex_case_variant_is_still_unknown() {
    // Exact, case-sensitive match per the spec: "Codex" must NOT be accepted.
    let err = Config::from_toml_str("[adapter]\nkind = \"Codex\"\n")
        .expect_err("only the exact lowercase \"codex\" is accepted");
    match err {
        ConfigError::UnknownAdapterKind(k) => assert_eq!(k, "Codex"),
        other => panic!("expected ConfigError::UnknownAdapterKind, got {other:?}"),
    }
}

// ---------------------------------------------------------------------
// 4: build_adapter returns the codex-backed NodeAdapter variant
// ---------------------------------------------------------------------

#[test]
fn build_adapter_selects_the_codex_variant_for_adapter_kind_codex() {
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

#[test]
fn build_adapter_still_selects_the_claude_variant_for_adapter_kind_claude() {
    // The exhaustive match is preserved: adding Codex must not disturb the
    // pre-existing Claude arm.
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

// ---------------------------------------------------------------------
// 5: config -> registry -> loopback dispatch, RECEIVE side is a real
// NodeAdapter::Codex driving a stub `codex` bin.
// ---------------------------------------------------------------------

/// The spec's captured JSONL fixture (`docs/specs/adapter-codex.md`, "Test
/// plan" section), byte-for-byte: `thread.started` + `turn.started` +
/// `item.completed` (an `agent_message` with text "OK") + `turn.completed`.
const CAPTURED_JSONL_FIXTURE: &str = concat!(
    "{\"type\":\"thread.started\",\"thread_id\":\"019f237a-f03d-7710-ab8f-7af32b222df8\"}\n",
    "{\"type\":\"turn.started\"}\n",
    "{\"type\":\"item.completed\",\"item\":{\"id\":\"item_0\",\"type\":\"agent_message\",\"text\":\"OK\"}}\n",
    "{\"type\":\"turn.completed\",\"usage\":{\"input_tokens\":9065,\"cached_input_tokens\":3456,\"output_tokens\":15,\"reasoning_output_tokens\":8}}\n",
);

/// Write an executable shell-script stand-in for the `codex` CLI that emits
/// the fixture above verbatim regardless of argv/stdin (a stub, not the real
/// binary — proves ONLY that the adapter's plumbing composes correctly).
fn write_codex_stub(dir: &std::path::Path) -> std::path::PathBuf {
    let script_path = dir.join("codex-stub.sh");
    std::fs::write(
        &script_path,
        format!("#!/bin/sh\ncat <<'JSONL'\n{CAPTURED_JSONL_FIXTURE}JSONL\n"),
    )
    .expect("write codex stub script");
    let mut perms = std::fs::metadata(&script_path)
        .expect("stat stub")
        .permissions();
    perms.set_mode(0o755);
    std::fs::set_permissions(&script_path, perms).expect("chmod +x stub");
    script_path
}

fn a2a_card() -> A2aCard {
    A2aCard {
        name: "codex-loopback-peer".to_string(),
        description: "wasl-mcp config-codex loopback peer".to_string(),
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
/// `NodeAdapter::Codex` (built from a config-declared stub bin via
/// `build_adapter`), bound before `axum::serve` spawns (no readiness race).
async fn spawn_codex_receiver(bin_path: &std::path::Path) -> Peer {
    let cfg = AdapterConfig {
        kind: AdapterKind::Codex,
        bin: Some(bin_path.to_string_lossy().into_owned()),
    };
    let adapter = build_adapter(&cfg);
    assert!(
        matches!(adapter, NodeAdapter::Codex(_)),
        "the receive side must be the codex-backed variant"
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
async fn config_codex_receive_composes_over_real_loopback_a2a() {
    let dir = tempfile::tempdir().expect("tempdir");
    let stub = write_codex_stub(dir.path());

    // RECEIVE side: a real A2aServer over a real AdapterExecutor wrapping a
    // real NodeAdapter::Codex driving the stub `codex` bin.
    let peer = spawn_codex_receiver(&stub).await;

    // SEND side: config -> registry, exactly mirroring config_wiring.rs's
    // loopback proof, but pointed at the codex-backed receiver above.
    let toml = format!(
        "[[peers]]\nagent_id = \"codex-peer\"\nhost_id = \"loopback\"\nurl = \"{}\"\n",
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
        peers: vec![agent("codex-peer")],
        question: "status?".to_string(),
    });

    let reply = resolve_and_dispatch(
        &ctx,
        &GateOutcome::Approved,
        call,
        "config-codex-consult",
        0,
        &http,
        &mut runner,
        &mut audit,
    )
    .await
    .expect("an approved consult over the codex-backed peer must succeed");

    match reply {
        ToolReply::Consult(res) => {
            assert_eq!(res.replies.len(), 1);
            assert_eq!(res.replies[0].from, agent("codex-peer"));
            assert_eq!(
                res.replies[0].body, "OK",
                "the reply text must be the fixture's agent_message text, proving \
                 compose_prompt -> build_argv -> the real child process -> parse_reply \
                 all composed correctly"
            );
        }
        other => panic!("expected Consult reply, got {other:?}"),
    }
}
