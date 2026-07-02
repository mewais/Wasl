//! `live_client` — a small, real MCP client used ONLY by `scripts/live-verify.sh`
//! (W5 live dress rehearsal). It is deliberately NOT a test: it spawns a REAL
//! `wasl-mcpd` binary as a child process (real stdio, real OS process — not the
//! in-memory duplex the `tests/**` suites use) and drives it as a real rmcp
//! client, so the orchestrator can prove the actual serve loops in
//! `wasl_mcp::run()` (stdio MCP + A2A receive + operator HTTP) run at all.
//!
//! Three modes, selected by argv[1]:
//!
//!   elicit          <mcpd_bin> <config> <peer> <question>
//!     Declares MCP elicitation capability and auto-accepts every elicitation
//!     request, then calls `consult`. Drives the Tier-1 path.
//!
//!   bare-consult     <mcpd_bin> <config> <peer> <question>
//!     A client with NO elicitation capability. Calls `consult` and simply
//!     waits for the tool call to return — used for the Tier-3 external-first
//!     leg, where the gate blocks on an operator decision the orchestrator
//!     supplies concurrently via `curl POST`.
//!
//!   prepare-commit   <mcpd_bin> <config> <peer> <question> [poll_timeout_secs]
//!     A client with NO elicitation capability, against a node configured for
//!     `[gate] authority = "prepare-commit"`. Calls `consult` (expects a
//!     `Pending` reply), prints the token, then polls the `commit` tool until
//!     the human decision (delivered externally, e.g. via `curl POST` to the
//!     operator) lands or the timeout elapses.
//!
//! Every mode prints ONE machine-parseable `WASL_LIVE::...` line to stdout as
//! its last line of output and exits 0 on a successful (non-blocked, non-error)
//! outcome, non-zero otherwise — `scripts/live-verify.sh` greps this line.

use std::env;
use std::process::ExitCode;
use std::time::Duration;

use rmcp::model::{
    CallToolRequestParam, CallToolResult, ClientCapabilities, ClientInfo,
    CreateElicitationRequestParam, CreateElicitationResult, ElicitationAction,
};
use rmcp::service::RequestContext;
use rmcp::transport::TokioChildProcess;
use rmcp::{ClientHandler, ErrorData as McpError, Peer, RoleClient, ServiceExt};
use serde_json::{json, Value};
use tokio::process::Command;

/// A client that declares elicitation capability and always accepts.
#[derive(Clone)]
struct AutoAcceptClient;

impl ClientHandler for AutoAcceptClient {
    fn get_info(&self) -> ClientInfo {
        let mut info = ClientInfo::default();
        info.capabilities = ClientCapabilities::builder().enable_elicitation().build();
        info
    }

    async fn create_elicitation(
        &self,
        _request: CreateElicitationRequestParam,
        _context: RequestContext<RoleClient>,
    ) -> Result<CreateElicitationResult, McpError> {
        eprintln!("live_client: elicitation received -> auto-accepting");
        Ok(CreateElicitationResult {
            action: ElicitationAction::Accept,
            content: None,
        })
    }
}

/// A client with no declared capabilities at all (the "caps-less" client the
/// Tier-2/Tier-3 legs need: `detect_caps` reads `elicitation: false`).
#[derive(Clone)]
struct BareClient;

impl ClientHandler for BareClient {
    fn get_info(&self) -> ClientInfo {
        ClientInfo::default()
    }
}

fn spawn_node(mcpd_bin: &str, config: &str) -> TokioChildProcess {
    let mut cmd = Command::new(mcpd_bin);
    cmd.env("WASL_CONFIG", config);
    TokioChildProcess::new(cmd).unwrap_or_else(|e| {
        eprintln!("live_client: failed to spawn {mcpd_bin}: {e}");
        std::process::exit(2);
    })
}

fn text_of(res: &CallToolResult) -> String {
    res.content
        .first()
        .and_then(|c| c.as_text())
        .map(|t| t.text.clone())
        .unwrap_or_default()
}

fn is_error(res: &CallToolResult) -> bool {
    res.is_error.unwrap_or(false)
}

async fn call_tool(peer: &Peer<RoleClient>, name: &str, args: Value) -> CallToolResult {
    peer.call_tool(CallToolRequestParam {
        name: name.to_string().into(),
        arguments: args.as_object().cloned(),
    })
    .await
    .unwrap_or_else(|e| {
        eprintln!("live_client: call_tool({name}) transport error: {e}");
        std::process::exit(2);
    })
}

/// Parse the pinned Tier-2 pending body `{"status":"pending","token":"...",...}`.
fn pending_token(text: &str) -> Option<String> {
    let v: Value = serde_json::from_str(text).ok()?;
    if v.get("status")?.as_str()? != "pending" {
        return None;
    }
    v.get("token")?.as_str().map(|s| s.to_string())
}

async fn run_elicit(mcpd_bin: &str, config: &str, peer: &str, question: &str) -> ExitCode {
    let transport = spawn_node(mcpd_bin, config);
    let client = AutoAcceptClient
        .serve(transport)
        .await
        .expect("client must initialize against a real wasl-mcpd");
    let res = call_tool(
        &client,
        "consult",
        json!({"peers": [peer], "question": question}),
    )
    .await;
    let _ = client.cancel().await;

    if is_error(&res) {
        println!("WASL_LIVE::ERR {}", text_of(&res));
        ExitCode::FAILURE
    } else {
        println!("WASL_LIVE::OK {}", text_of(&res));
        ExitCode::SUCCESS
    }
}

async fn run_bare_consult(mcpd_bin: &str, config: &str, peer: &str, question: &str) -> ExitCode {
    let transport = spawn_node(mcpd_bin, config);
    let client = BareClient
        .serve(transport)
        .await
        .expect("client must initialize against a real wasl-mcpd");
    let res = call_tool(
        &client,
        "consult",
        json!({"peers": [peer], "question": question}),
    )
    .await;
    let _ = client.cancel().await;

    if is_error(&res) {
        println!("WASL_LIVE::ERR {}", text_of(&res));
        ExitCode::FAILURE
    } else {
        println!("WASL_LIVE::OK {}", text_of(&res));
        ExitCode::SUCCESS
    }
}

async fn run_prepare_commit(
    mcpd_bin: &str,
    config: &str,
    peer: &str,
    question: &str,
    poll_timeout: Duration,
) -> ExitCode {
    let transport = spawn_node(mcpd_bin, config);
    let client = BareClient
        .serve(transport)
        .await
        .expect("client must initialize against a real wasl-mcpd");

    let prepared = call_tool(
        &client,
        "consult",
        json!({"peers": [peer], "question": question}),
    )
    .await;
    if is_error(&prepared) {
        println!("WASL_LIVE::ERR {}", text_of(&prepared));
        let _ = client.cancel().await;
        return ExitCode::FAILURE;
    }
    let token = match pending_token(&text_of(&prepared)) {
        Some(t) => t,
        None => {
            println!(
                "WASL_LIVE::ERR expected a pending reply, got: {}",
                text_of(&prepared)
            );
            let _ = client.cancel().await;
            return ExitCode::FAILURE;
        }
    };
    println!("WASL_LIVE::PENDING {token}");

    let deadline = tokio::time::Instant::now() + poll_timeout;
    loop {
        let res = call_tool(&client, "commit", json!({"token": token})).await;
        let text = text_of(&res);
        if is_error(&res) {
            println!("WASL_LIVE::COMMIT_ERR {text}");
            let _ = client.cancel().await;
            return ExitCode::FAILURE;
        }
        if pending_token(&text).is_none() {
            // Not the pending body any more -> the real dispatched reply.
            println!("WASL_LIVE::COMMIT_OK {text}");
            let _ = client.cancel().await;
            return ExitCode::SUCCESS;
        }
        if tokio::time::Instant::now() >= deadline {
            println!("WASL_LIVE::COMMIT_ERR timed out waiting for the human decision");
            let _ = client.cancel().await;
            return ExitCode::FAILURE;
        }
        tokio::time::sleep(Duration::from_millis(250)).await;
    }
}

#[tokio::main]
async fn main() -> ExitCode {
    let args: Vec<String> = env::args().collect();
    let usage = "usage: live_client <elicit|bare-consult|prepare-commit> <mcpd_bin> <config> <peer> <question> [poll_timeout_secs]";
    if args.len() < 6 {
        eprintln!("{usage}");
        return ExitCode::from(2);
    }
    let mode = args[1].as_str();
    let mcpd_bin = args[2].as_str();
    let config = args[3].as_str();
    let peer = args[4].as_str();
    let question = args[5].as_str();

    match mode {
        "elicit" => run_elicit(mcpd_bin, config, peer, question).await,
        "bare-consult" => run_bare_consult(mcpd_bin, config, peer, question).await,
        "prepare-commit" => {
            let poll_secs: u64 = args.get(6).and_then(|s| s.parse().ok()).unwrap_or(30);
            run_prepare_commit(
                mcpd_bin,
                config,
                peer,
                question,
                Duration::from_secs(poll_secs),
            )
            .await
        }
        other => {
            eprintln!("live_client: unknown mode {other:?}\n{usage}");
            ExitCode::from(2)
        }
    }
}
