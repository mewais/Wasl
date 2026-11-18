//! `wasl mcp` as a real stdio child driven by an rmcp client, against a real daemon.

// The harness uses Unix sockets under /tmp and `kill`; Windows runs the unit tests only.
#![cfg(unix)]

mod common;

use common::*;
use rmcp::model::CallToolRequestParam;
use rmcp::service::RunningService;
use rmcp::transport::TokioChildProcess;
use rmcp::{RoleClient, ServiceExt};
use serde_json::{json, Value};
use wasit_fake_cli::scenario::Scenario;
use wasl_proto::*;

async fn mcp_client(d: &Daemon, session: &AgentId) -> RunningService<RoleClient, ()> {
    let mut cmd = tokio::process::Command::new(wasl_bin());
    cmd.args([
        "mcp",
        "--socket",
        &d.socket().to_string_lossy(),
        "--session",
        &session.session,
    ]);
    let transport = TokioChildProcess::new(cmd).expect("spawn wasl mcp");
    within("mcp initialize", ().serve(transport))
        .await
        .expect("mcp client")
}

async fn tool(c: &RunningService<RoleClient, ()>, name: &str, args: Value) -> (bool, Value) {
    let r = within(
        name,
        c.call_tool(CallToolRequestParam {
            name: name.to_string().into(),
            arguments: args.as_object().cloned(),
        }),
    )
    .await
    .unwrap_or_else(|e| panic!("{name}: {e}"));
    let text = r
        .content
        .iter()
        .filter_map(|c| c.as_text().map(|t| t.text.clone()))
        .collect::<String>();
    let v = serde_json::from_str(&text).unwrap_or(Value::String(text));
    (r.is_error.unwrap_or(false), v)
}

#[tokio::test]
async fn mcp_tools_end_to_end_with_a_daemon_restart_mid_wait() {
    let s = Scratch::new();
    let mut a = Daemon::start(&s, "a").await;
    let orch = a.session(&Scenario::builder().build()).await;
    let fast = a.session(&slow_worker("fast answer", 1)).await;
    let slow = a.session(&slow_worker("slow answer", 2500)).await;
    let gated = a.session(&slow_worker("never", 1)).await;
    let lonely = a.session(&Scenario::builder().build()).await;
    let t = topology(
        1,
        &[
            (&orch, "orch"),
            (&fast, "fast"),
            (&slow, "slow"),
            (&gated, "gated"),
        ],
        &[
            (&orch, &fast, EdgePolicy::AlwaysAllow),
            (&orch, &slow, EdgePolicy::AlwaysAllow),
            (&orch, &gated, EdgePolicy::Ask),
        ],
        Limits::default(),
    );
    a.set_topology(&t).await;

    let c = mcp_client(&a, &orch).await;
    let tools = within("list tools", c.list_tools(Default::default()))
        .await
        .unwrap();
    let mut names: Vec<String> = tools.tools.iter().map(|t| t.name.to_string()).collect();
    names.sort();
    assert_eq!(
        names,
        [
            "consult",
            "delegate",
            "list_peers",
            "message",
            "task_cancel",
            "task_status",
            "task_wait"
        ]
    );

    let (err, peers) = tool(&c, "list_peers", json!({})).await;
    assert!(!err);
    assert_eq!(peers["peers"].as_array().unwrap().len(), 3);

    // consult = delegate + wait, addressed by name.
    let (err, v) = tool(&c, "consult", json!({"to": ["fast"], "question": "q?"})).await;
    assert!(!err, "{v}");
    assert_eq!(v["tasks"][0]["state"], "completed");
    assert_eq!(v["tasks"][0]["result"], "fast answer");

    // Ask edge without a supervisor, and this client cannot elicit: denied with a reason.
    let (_, v) = tool(&c, "delegate", json!({"to": ["gated"], "instruction": "x"})).await;
    assert_eq!(v["tasks"][0]["state"], "denied", "{v}");
    assert!(v["tasks"][0]["error"]
        .as_str()
        .unwrap()
        .contains("cannot ask"));

    // Async: delegate returns at once; the daemon restarts while task_wait is pending; the
    // wait resumes and returns the result.
    let (err, v) = tool(
        &c,
        "delegate",
        json!({"to": ["slow"], "instruction": "take your time"}),
    )
    .await;
    assert!(!err, "{v}");
    let id = v["tasks"][0]["id"].as_str().unwrap().to_string();
    let waiter = {
        let c = c.peer().clone();
        let id = id.clone();
        tokio::spawn(async move {
            c.call_tool(CallToolRequestParam {
                name: "task_wait".into(),
                arguments: json!({"ids": [id], "timeout_secs": 50})
                    .as_object()
                    .cloned(),
            })
            .await
        })
    };
    let mut ev = a.tasks_topic();
    next_task_event(&mut ev, "slow running", |e| {
        e.view.task.id.as_str() == id && matches!(e.view.state, TaskState::Running { .. })
    })
    .await;
    a.restart().await;
    let r = within("resumed wait", waiter).await.unwrap().unwrap();
    let text: String = r
        .content
        .iter()
        .filter_map(|c| c.as_text().map(|t| t.text.clone()))
        .collect();
    let v: Value = serde_json::from_str(&text).unwrap();
    assert_eq!(v["tasks"][0]["state"], "completed", "{v}");
    assert_eq!(v["tasks"][0]["result"], "slow answer");

    let (_, v) = tool(&c, "task_status", json!({"ids": [id]})).await;
    assert_eq!(v["tasks"][0]["state"], "completed");
    let (_, v) = tool(&c, "message", json!({"to": ["fast"], "text": "fyi"})).await;
    assert_eq!(v["sent"].as_array().unwrap().len(), 1, "{v}");
    let (err, v) = tool(&c, "task_cancel", json!({"id": "tnope"})).await;
    assert!(err, "{v}");
    c.cancel().await.unwrap();

    // A session without connections.
    let c = mcp_client(&a, &lonely).await;
    let (err, v) = tool(&c, "list_peers", json!({})).await;
    assert!(!err);
    assert_eq!(v["peers"], json!([]));
    assert!(v["note"].as_str().unwrap().contains("no connections"));
    let (err, v) = tool(&c, "delegate", json!({"to": ["orch"], "instruction": "hi"})).await;
    assert!(err);
    assert!(v.to_string().contains("no connections configured"), "{v}");
    c.cancel().await.unwrap();
}
