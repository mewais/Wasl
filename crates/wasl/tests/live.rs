//! Live: real CLIs delegating to each other through the injected Wasl MCP server.
//!
//! Opt-in: `WASL_LIVE=1 cargo test -p wasl --test live -- --ignored --nocapture`. Needs
//! `claude` (authenticated), `opencode` and `kilo` on PATH and network access. Each run prints
//! whether the orchestrator CLI asked for its own permission before calling a Wasl tool (it
//! should not: the server is injected as trusted).

// The harness uses Unix sockets under /tmp and `kill`; Windows runs the unit tests only.
#![cfg(unix)]

mod common;

use std::time::Duration;

use common::*;
use wasit_core::{AgentEvent, Command, GateDecision, GateRequest};
use wasit_runtime::wire::{CreateSession, SessionEventKind, SessionId};
use wasl_proto::*;

fn enabled() -> bool {
    std::env::var("WASL_LIVE").is_ok_and(|v| v == "1")
}

struct Outcome {
    reply: String,
    handoff_gates: u32,
    native_wasl_prompts: Vec<String>,
}

async fn run(
    orch_driver: &str,
    orch_model: &str,
    worker_driver: &str,
    worker_model: &str,
) -> Outcome {
    let s = Scratch::new();
    let d = Daemon::start_real(&s, "live").await;
    // A supervisor is attached in the product; here the test plays it (role "supervisor").
    let sup = wasit_runtime::RuntimeClient::connect_endpoint(
        wasit_control::local_endpoint(&d.socket()),
        wasit_control::Hello::new("test", "test", "supervisor"),
    );
    sup.list().await.unwrap();

    let create = |driver: &str, model: &str| {
        let id = SessionId::random();
        let mut req = CreateSession::new(driver);
        req.id = Some(id.clone());
        req.cwd = Some(d.dir.to_string_lossy().into_owned());
        req.model = Some(model.into());
        let mut mcp = wasl_mcp::mcp_server_config(
            &wasl_bin(),
            &["mcp".to_string()],
            &d.socket(),
            id.as_str(),
        );
        // Control experiment: without the pre-allow, does the CLI prompt?
        if std::env::var("WASL_LIVE_UNTRUSTED").is_ok_and(|v| v == "1") {
            mcp.trusted = false;
        }
        req.config.mcp_servers = vec![mcp];
        (id, req)
    };
    let (wid, wreq) = create(worker_driver, worker_model);
    let r = d.client.create(wreq).await.unwrap();
    assert!(r.unsupported.is_empty(), "{:?}", r.unsupported);
    let (oid, oreq) = create(orch_driver, orch_model);
    let r = d.client.create(oreq).await.unwrap();
    assert!(r.unsupported.is_empty(), "{:?}", r.unsupported);
    for id in [&wid, &oid] {
        d.wait_snapshot(id, "idle", |s| s.phase.name() == "idle")
            .await;
    }
    let orch = AgentId::new("live", oid.0.clone());
    let worker = AgentId::new("live", wid.0.clone());
    d.set_topology(&topology(
        1,
        &[(&orch, "orchestrator"), (&worker, "worker")],
        &[(&orch, &worker, EdgePolicy::Ask)],
        Limits::default(),
    ))
    .await;

    let start = d.client.snapshot(&oid).await.unwrap().cursor;
    let mut events = d.client.subscribe_session(&oid, Some(start));
    d.client
        .command(
            &oid,
            Command::Prompt {
                text: "Use the wasl `consult` tool to ask the peer named \"worker\" exactly: \
                       \"What is 17 + 25? Reply with just the number.\" Then reply with only \
                       the number it answered."
                    .into(),
            },
        )
        .await
        .unwrap();
    let mut out = Outcome {
        reply: String::new(),
        handoff_gates: 0,
        native_wasl_prompts: vec![],
    };
    let res = tokio::time::timeout(Duration::from_secs(300), async {
        let mut started = false;
        let mut text = String::new();
        loop {
            let (_, ev) = events.next().await.expect("events").expect("event");
            match ev.kind {
                SessionEventKind::Agent { event } => match event {
                    AgentEvent::TurnStarted { .. } => started = true,
                    AgentEvent::AssistantDelta { text: t, .. } if started => text.push_str(&t),
                    AgentEvent::GateRequested { gate } => {
                        match &gate {
                            GateRequest::Handoff { .. } => out.handoff_gates += 1,
                            GateRequest::Permission { tool_name, .. }
                                if tool_name.contains("wasl") =>
                            {
                                out.native_wasl_prompts.push(tool_name.clone());
                            }
                            _ => {}
                        }
                        println!("gate on the orchestrator: {gate:?}");
                        let rid = gate.request_id().to_string();
                        let decision = match &gate {
                            GateRequest::Permission {
                                elicitation_choices,
                                ..
                            } if !elicitation_choices.is_empty() => {
                                let c = elicitation_choices
                                    .iter()
                                    .find(|c| {
                                        c.value.contains("allow")
                                            || c.label.to_lowercase().contains("allow")
                                    })
                                    .unwrap_or(&elicitation_choices[0]);
                                GateDecision::Approve {
                                    updated_input: None,
                                    permission_updates: None,
                                    elicitation_content: Some(
                                        serde_json::json!({c.key.clone(): c.value.clone()}),
                                    ),
                                }
                            }
                            _ => GateDecision::approve(),
                        };
                        sup.command(
                            &oid,
                            Command::AnswerGate {
                                request_id: rid,
                                decision,
                            },
                        )
                        .await
                        .unwrap();
                    }
                    AgentEvent::TurnCompleted { reply, .. } if started => {
                        out.reply = reply.filter(|r| !r.is_empty()).unwrap_or(text);
                        return;
                    }
                    _ => {}
                },
                SessionEventKind::Exited { .. } | SessionEventKind::Lost { .. } => {
                    panic!("orchestrator ended: {:?}", ev.kind)
                }
                _ => {}
            }
        }
    })
    .await;
    let tasks: TaskList = d.call(methods::TASKS_LIST, serde_json::json!(null)).await;
    println!(
        "tasks: {:#?}",
        tasks
            .outbound
            .iter()
            .map(|t| (&t.task.to, &t.state))
            .collect::<Vec<_>>()
    );
    assert!(
        res.is_ok(),
        "the orchestrator's turn did not finish in time"
    );
    let snap = d.client.snapshot(&oid).await.unwrap();
    if out.reply.is_empty() {
        out.reply = snap.last_turn.and_then(|t| t.reply).unwrap_or_default();
    }
    for id in [&oid, &wid] {
        let _ = d.client.stop(id).await; // best effort
    }
    out
}

fn report(name: &str, o: &Outcome) {
    println!(
        "LIVE {name}: reply={:?} handoff_gates={} native_wasl_prompts={:?}",
        o.reply, o.handoff_gates, o.native_wasl_prompts
    );
}

#[tokio::test]
#[ignore]
async fn live_claude_orchestrator_opencode_worker() {
    if !enabled() {
        eprintln!("skipped: set WASL_LIVE=1");
        return;
    }
    let o = run(
        "claude",
        "haiku",
        "opencode",
        "opencode/deepseek-v4.1-flash-free",
    )
    .await;
    report("claude->opencode", &o);
    assert!(o.handoff_gates >= 1);
    assert!(o.reply.contains("42"), "reply: {:?}", o.reply);
}

#[tokio::test]
#[ignore]
async fn live_kilo_orchestrator_opencode_worker() {
    if !enabled() {
        eprintln!("skipped: set WASL_LIVE=1");
        return;
    }
    let o = run(
        "kilo",
        "kilo/kilo-auto/free",
        "opencode",
        "opencode/deepseek-v4.1-flash-free",
    )
    .await;
    report("kilo->opencode", &o);
    assert!(o.handoff_gates >= 1);
    assert!(o.reply.contains("42"), "reply: {:?}", o.reply);
}

#[tokio::test]
#[ignore]
async fn live_opencode_orchestrator_kilo_worker() {
    if !enabled() {
        eprintln!("skipped: set WASL_LIVE=1");
        return;
    }
    let o = run(
        "opencode",
        "opencode/deepseek-v4.1-flash-free",
        "kilo",
        "kilo/kilo-auto/free",
    )
    .await;
    report("opencode->kilo", &o);
    assert!(o.handoff_gates >= 1);
    assert!(o.reply.contains("42"), "reply: {:?}", o.reply);
}
