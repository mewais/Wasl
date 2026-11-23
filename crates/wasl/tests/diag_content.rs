//! No content in any diagnostics file or stderr capture (real `wasl serve` daemons, a hub, the
//! fake CLI; design §10 "leak test", Wasl's part).
//!
//! A delegation whose prompt, peer names, role and description, progress text, result and CLI
//! stderr carry unique markers runs through a handoff gate; a second is denied with a reason that
//! carries a marker; a third fails inside the receiving CLI; a request fails on a task id that
//! carries a marker; and the node starts over a journal with an unreadable line that carries a
//! marker (the parse error quotes it). Afterwards the daemons are stopped with SIGTERM (they
//! write what they recorded first) and every file under their `diag/` directories and their
//! stderr captures is searched for each marker, raw and JSON-escaped. The same test asserts that
//! the expected records ARE there (so it cannot pass by writing nothing) and that the markers DID
//! reach the places that may hold them (the task states, the audit trail): the content flowed.

#![cfg(unix)]

mod common;

use std::path::{Path, PathBuf};
use std::process::Stdio;

use common::*;
use serde_json::{json, Value};
use wasit_fake_cli::scenario::{Scenario, Turn};
use wasl_proto::*;

/// A marker: `core` is unique to this run and made of id characters (so it would survive any
/// typed field), `text` is what the content channel carries around it, with characters JSON
/// escapes.
struct Marker {
    core: String,
    text: String,
}

impl Marker {
    fn new(channel: &str) -> Marker {
        let core = format!("MARK-{channel}-{:08x}", fastrand::u32(..));
        let text = format!("{core} \"quoted\"\nsecond line");
        Marker { core, text }
    }

    /// The forms a leak may take in a file.
    fn needles(&self) -> Vec<(String, Vec<u8>)> {
        let escaped = serde_json::to_string(&self.text).unwrap();
        let escaped = escaped[1..escaped.len() - 1].to_owned();
        vec![
            ("core".into(), self.core.clone().into_bytes()),
            ("text".into(), self.text.clone().into_bytes()),
            ("json-escaped text".into(), escaped.into_bytes()),
            (
                "core as UTF-16LE".into(),
                self.core
                    .encode_utf16()
                    .flat_map(u16::to_le_bytes)
                    .collect(),
            ),
        ]
    }
}

fn contains(hay: &[u8], needle: &[u8]) -> bool {
    !needle.is_empty() && hay.windows(needle.len()).any(|w| w == needle)
}

/// Every regular file under `dir`.
fn files(dir: &Path, out: &mut Vec<PathBuf>) {
    let Ok(rd) = std::fs::read_dir(dir) else {
        return;
    };
    for e in rd.flatten() {
        let p = e.path();
        if p.is_dir() {
            files(&p, out);
        } else {
            out.push(p);
        }
    }
}

/// The parsed records of every `*.ndjson*` file under `dir`, header lines left out.
fn records(dir: &Path) -> Vec<Value> {
    let mut all = Vec::new();
    files(dir, &mut all);
    let mut out = Vec::new();
    for f in all {
        let text = std::fs::read_to_string(&f).unwrap();
        for l in text.lines() {
            let v: Value =
                serde_json::from_str(l).unwrap_or_else(|e| panic!("{f:?}: not JSON ({e}): {l}"));
            if v.get("diag_file").is_none() {
                out.push(v);
            }
        }
    }
    out
}

fn find<'a>(recs: &'a [Value], ev: &str) -> Vec<&'a Value> {
    recs.iter().filter(|r| r["ev"] == ev).collect()
}

/// A topology of `ws` whose peers have a name, a role and a description of their own.
fn described(
    ws: &str,
    agents: &[(&AgentId, &Marker, &Marker, &Marker)],
    edges: &[(&AgentId, &AgentId, EdgePolicy)],
) -> Topology {
    Topology {
        workstream: ws.into(),
        version: 1,
        peers: agents
            .iter()
            .map(|(a, name, role, description)| PeerCard {
                agent: (*a).clone(),
                name: name.text.clone(),
                driver: "claude".into(),
                role: role.text.clone(),
                description: description.text.clone(),
                status: PeerStatus::Unknown,
            })
            .collect(),
        edges: edges
            .iter()
            .map(|(f, t, p)| Edge {
                from: (*f).clone(),
                to: (*t).clone(),
                policy: *p,
            })
            .collect(),
        limits: Limits::default(),
        paused: false,
    }
}

#[tokio::test]
async fn no_content_in_any_diagnostics_file_or_stderr() {
    let prompt1 = Marker::new("prompt1");
    let prompt2 = Marker::new("prompt2");
    let prompt3 = Marker::new("prompt3");
    let name = Marker::new("name");
    let role = Marker::new("role");
    let description = Marker::new("description");
    let workstream = Marker::new("workstream");
    let progress = Marker::new("progress");
    let result = Marker::new("result");
    let reason = Marker::new("reason");
    let stderr = Marker::new("stderr");
    let failure = Marker::new("failure");
    let journal = Marker::new("journal");
    let unknown_task = Marker::new("task");
    let markers = [
        &prompt1,
        &prompt2,
        &prompt3,
        &name,
        &role,
        &description,
        &workstream,
        &progress,
        &result,
        &reason,
        &stderr,
        &failure,
        &journal,
        &unknown_task,
    ];

    let s = Scratch::new();
    // Daemon `a` starts over a journal whose only line is unreadable and quotes a marker.
    std::fs::create_dir_all(s.0.join("a/wasl")).unwrap();
    std::fs::write(
        s.0.join("a/wasl/journal.ndjson"),
        format!(
            "{{\"seq\":{},\"rev\":1,\"t\":1,\"rec\":\"topology_removed\",\"workstream\":\"w\"}}\n",
            serde_json::to_string(&journal.text).unwrap()
        ),
    )
    .unwrap();
    let mut a = Daemon::start_quiet(&s, "a").await;
    let mut b = Daemon::start_quiet(&s, "b").await;
    let hub = TestHub::start(&[&a, &b]).await;

    let orch = a.session(&Scenario::builder().build()).await;
    // Worker 1: narrates (progress), then answers with the result marker.
    let w1 = b
        .session(
            &Scenario::builder()
                .turn(
                    Turn::builder()
                        .say(progress.text.clone())
                        .sleep_ms(1)
                        .say(result.text.clone()),
                )
                .build(),
        )
        .await;
    // Worker 2 would answer, but its handoff is denied.
    let w2 = b.session(&slow_worker("never asked", 1)).await;
    // Worker 3's CLI writes to stderr and then fails.
    let w3 = b
        .session(
            &Scenario::builder()
                .turn(
                    Turn::builder()
                        .say("starting")
                        .stderr(stderr.text.clone())
                        .fail(failure.text.clone()),
                )
                .build(),
        )
        .await;
    let (n1, n2, n3) = (Marker::new("n1"), Marker::new("n2"), Marker::new("n3"));
    let topo = described(
        &workstream.text,
        &[
            (&orch, &name, &role, &description),
            (&w1, &n1, &role, &description),
            (&w2, &n2, &role, &description),
            (&w3, &n3, &role, &description),
        ],
        &[
            (&orch, &w1, EdgePolicy::Ask),
            (&orch, &w2, EdgePolicy::Ask),
            (&orch, &w3, EdgePolicy::AlwaysAllow),
        ],
    );
    for d in [&a, &b] {
        d.set_topology(&topo).await;
    }
    let markers_in_peers = {
        let peers: PeerList = a
            .call(methods::PEERS_LIST, json!({"from": orch.session}))
            .await;
        serde_json::to_string(&peers).unwrap()
    };
    assert!(markers_in_peers.contains(&n1.core), "{markers_in_peers}");
    assert!(markers_in_peers.contains(&description.core));

    // 1. Handoff gate allowed once: the task runs and its result comes back.
    let t1 = a.delegate1(&orch, &w1, &prompt1.text).await;
    let gate = a.handoff_gate(&orch, &t1.id).await;
    a.answer(
        &orch,
        gate.request_id(),
        HandoffDecision::AllowOnce.to_gate(),
    )
    .await;
    let r1 = result_summary(&a.wait_done(&t1.id).await);
    assert!(r1.contains(&result.core), "the result flowed: {r1}");

    // 2. Denied with a reason an agent would read.
    let t2 = a.delegate1(&orch, &w2, &prompt2.text).await;
    let gate = a.handoff_gate(&orch, &t2.id).await;
    a.answer(
        &orch,
        gate.request_id(),
        HandoffDecision::Deny {
            reason: Some(reason.text.clone()),
        }
        .to_gate(),
    )
    .await;
    match a.wait_done(&t2.id).await {
        TaskState::Denied { reason: r } => {
            assert!(r.contains(&reason.core), "the reason flowed: {r}")
        }
        other => panic!("expected denied, got {other:?}"),
    }

    // 3. The receiving CLI fails.
    let t3 = a.delegate1(&orch, &w3, &prompt3.text).await;
    let end3 = a.wait_done(&t3.id).await;
    assert!(end3.is_terminal(), "{end3:?}");

    // 4. A request that fails on an id carrying a marker (its message names the id).
    let bad = a
        .try_call::<TaskBrief>(
            methods::TASK_CANCEL,
            json!({"id": unknown_task.core.clone()}),
        )
        .await;
    assert!(
        matches!(&bad, Err(wasit_control::CallError::Rpc(e))
            if e.code == codes::UNKNOWN_TASK && e.message.contains(&unknown_task.core)),
        "{bad:?}"
    );

    // The audit trail (not a diagnostics file) is where the prompt and the reason belong.
    let audit = std::fs::read_to_string(a.dir.join("wasl/audit.ndjson")).unwrap();
    assert!(audit.contains(&prompt1.core) && audit.contains(&reason.core));

    hub.shutdown().await;
    // A bridge of our own, ended by EOF: the short-lived process that writes `aux.ndjson`, with
    // its stderr captured.
    let bridge_stderr = s.0.join("bridge.stderr");
    let status = within(
        "bridge exit",
        tokio::process::Command::new(wasl_bin())
            .args(["bridge", "--no-start", "--data"])
            .arg(&a.dir)
            .args(["--host-id", "a"])
            .env_remove("WASL_LOG")
            .stdin(Stdio::null())
            .stdout(Stdio::null())
            .stderr(std::fs::File::create(&bridge_stderr).unwrap())
            .kill_on_drop(true)
            .status(),
    )
    .await
    .unwrap();
    assert!(status.success(), "{status}");
    a.terminate().await;
    b.terminate().await;

    // ── nothing of any marker in any file ─────────────────────────────────────
    let mut scanned = Vec::new();
    for d in [&a, &b] {
        files(&d.dir.join("diag"), &mut scanned);
        scanned.push(d.dir.join("serve.log"));
    }
    scanned.push(bridge_stderr);
    assert!(
        scanned.iter().any(|f| f.ends_with("diag/daemon.ndjson")),
        "{scanned:?}"
    );
    assert!(scanned.iter().any(|f| f.ends_with("diag/aux.ndjson")));
    let mut leaks = Vec::new();
    for f in &scanned {
        let bytes = std::fs::read(f).unwrap_or_else(|e| panic!("{f:?}: {e}"));
        for m in markers {
            for (form, needle) in m.needles() {
                if contains(&bytes, &needle) {
                    leaks.push(format!("{} ({form}) in {}", m.core, f.display()));
                }
            }
        }
    }
    assert!(
        leaks.is_empty(),
        "content in diagnostics:\n{}",
        leaks.join("\n")
    );

    // ── and the records that should be there are ──────────────────────────────
    let ra = records(&a.dir.join("diag"));
    let rb = records(&b.dir.join("diag"));
    for (who, recs) in [("a", &ra), ("b", &rb)] {
        for ev in [
            "process.start",
            "wasl.daemon.start",
            "wasl.daemon.adopted",
            "wasl.journal.opened",
            "wasl.daemon.ready",
            "wasl.daemon.shutdown",
        ] {
            assert!(!find(recs, ev).is_empty(), "{who}: no {ev}: {recs:?}");
        }
        let shutdown = find(recs, "wasl.daemon.shutdown");
        assert_eq!(shutdown[0]["reason"], "SIGTERM");
        let start = find(recs, "wasl.daemon.start")[0];
        assert_eq!(start["host"], who);
        assert_eq!(start["src"], "daemon");
    }
    // The unreadable journal line: where it was, and only the size and fingerprint of its text.
    let opened = find(&ra, "wasl.journal.opened")[0];
    assert_eq!(
        (opened["records"].as_u64(), opened["skipped"].as_u64()),
        (Some(0), Some(1))
    );
    let skipped = find(&ra, "wasl.journal.line_skipped");
    assert_eq!(skipped.len(), 1, "{skipped:?}");
    assert_eq!(skipped[0]["lvl"], "warn");
    assert_eq!(skipped[0]["line"], 1);
    let e = &skipped[0]["error"];
    assert_eq!(e["code"], "journal_line");
    assert!(
        e["len"].as_u64().unwrap() > journal.text.len() as u64,
        "{e}"
    );
    assert!(e["fp"].is_string() && e.get("text").is_none(), "{e}");
    // The requests that carried the content, by method and time only.
    let delegates: Vec<_> = find(&ra, "wasl.request")
        .into_iter()
        .filter(|r| r["method"] == methods::DELEGATE)
        .collect();
    assert_eq!(delegates.len(), 3, "{delegates:?}");
    assert!(find(&ra, "wasl.request")
        .iter()
        .any(|r| r["method"] == methods::TOPOLOGY_SET));
    assert!(find(&rb, "wasl.request")
        .iter()
        .any(|r| r["method"] == methods::NODE_DELIVER));
    // The failed request: its code, never its message.
    let failed = find(&ra, "wasl.request_failed");
    assert_eq!(failed.len(), 1, "{failed:?}");
    assert_eq!(failed[0]["method"], methods::TASK_CANCEL);
    assert_eq!(failed[0]["code"], codes::UNKNOWN_TASK);
    assert_eq!(failed[0]["lvl"], "warn");
    assert!(failed[0].get("error").is_none() && failed[0].get("message").is_none());
    // The bridge's own process record, in the shared file (the hub's bridges append there too).
    let aux = ra
        .iter()
        .filter(|r| r["src"] == "bridge" && r["ev"] == "process.start")
        .count();
    assert!(aux >= 1, "{ra:?}");
}
