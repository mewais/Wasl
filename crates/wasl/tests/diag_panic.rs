//! A panic in a `wasl` process leaves a `diag.panic` record with its location and the kind and
//! length of its payload, and nothing of the message: not in the diagnostics file, nowhere on
//! disk, and nothing on stderr. `wasl mcp`, the helper an agent CLI starts, has no data
//! directory and no sink: its panic prints and records nothing at all. `WASL_LOG` is the
//! developer override that lets the usual panic message through.
//!
//! The panics are real: the test build of `wasl` (the `panic-test` feature, which only this
//! package's own dev-dependency turns on) panics with a formatted message holding a marker once
//! the process's diagnostics are set up.

mod common;

use std::path::Path;
use std::process::Output;

use common::*;
use serde_json::Value;

fn marker() -> String {
    format!("PANIC-CONTENT-MARKER-{:08x}", fastrand::u32(..))
}

/// Runs `wasl <args> --panic-test <marker>` to its end.
async fn run(args: &[&str], marker: &str, log: bool) -> Output {
    let mut cmd = tokio::process::Command::new(wasl_bin());
    cmd.args(args).arg("--panic-test").arg(marker);
    if log {
        cmd.env("WASL_LOG", "info");
    } else {
        cmd.env_remove("WASL_LOG");
    }
    // The process ends on its own: waiting for it is the signal.
    within(
        "the process ended",
        cmd.stdin(std::process::Stdio::null())
            .kill_on_drop(true)
            .output(),
    )
    .await
    .unwrap()
}

fn records(file: &Path) -> Vec<Value> {
    std::fs::read_to_string(file)
        .unwrap()
        .lines()
        .map(|l| serde_json::from_str(l).unwrap_or_else(|e| panic!("not JSON ({e}): {l:?}")))
        .collect()
}

fn files(dir: &Path, out: &mut Vec<(std::path::PathBuf, Vec<u8>)>) {
    for e in std::fs::read_dir(dir).unwrap().flatten() {
        let p = e.path();
        if p.is_dir() {
            files(&p, out);
        } else if let Ok(b) = std::fs::read(&p) {
            out.push((p, b));
        }
    }
}

/// The one `diag.panic` record of `file`: its shape, and that nothing under `data` holds the
/// marker.
fn assert_recorded_without_the_message(file: &Path, data: &Path, marker: &str, src: &str) {
    let log = records(file);
    let panics: Vec<_> = log.iter().filter(|r| r["ev"] == "diag.panic").collect();
    assert_eq!(panics.len(), 1, "{log:?}");
    let p = panics[0];
    assert_eq!(p["src"], src);
    assert_eq!(p["lvl"], "error");
    assert_eq!(p["thread"], "main");
    assert_eq!(p["payload_kind"], "string");
    assert_eq!(
        p["len"],
        format!("wasl panic test: {marker}").chars().count() as u64
    );
    assert!(p.get("static_text").is_none(), "{p}");
    assert!(p["location"].as_str().unwrap().contains("main.rs:"), "{p}");
    let mut all = Vec::new();
    files(data, &mut all);
    for (path, bytes) in &all {
        assert!(
            !bytes.windows(marker.len()).any(|w| w == marker.as_bytes()),
            "the panic message is in {path:?}"
        );
    }
}

#[tokio::test]
async fn a_daemon_panic_is_recorded_without_its_message() {
    let (s, m) = (Scratch::new(), marker());
    let data = s.0.join("d");
    let data_arg = data.to_string_lossy().into_owned();
    let out = run(&["serve", "--data", &data_arg, "--host-id", "p"], &m, false).await;
    assert_eq!(out.status.code(), Some(101), "{out:?}");
    assert!(
        out.stderr.is_empty(),
        "{}",
        String::from_utf8_lossy(&out.stderr)
    );
    assert!(out.stdout.is_empty());
    let file = data.join("diag/daemon.ndjson");
    assert_recorded_without_the_message(&file, &data, &m, "daemon");
    // It panicked after it started: its start is in the same file, and nothing says it is ready.
    let log = records(&file);
    assert!(log.iter().any(|r| r["ev"] == "wasl.daemon.start"));
    assert!(!log.iter().any(|r| r["ev"] == "wasl.daemon.ready"));
}

#[tokio::test]
async fn a_bridge_panic_is_recorded_in_the_shared_file_without_its_message() {
    let (s, m) = (Scratch::new(), marker());
    let data = s.0.join("d");
    std::fs::create_dir_all(&data).unwrap();
    let data_arg = data.to_string_lossy().into_owned();
    let out = run(&["bridge", "--data", &data_arg, "--no-start"], &m, false).await;
    assert_eq!(out.status.code(), Some(101), "{out:?}");
    assert!(
        out.stderr.is_empty(),
        "{}",
        String::from_utf8_lossy(&out.stderr)
    );
    assert!(out.stdout.is_empty());
    assert_recorded_without_the_message(&data.join("diag/aux.ndjson"), &data, &m, "bridge");
}

#[tokio::test]
async fn an_mcp_panic_prints_and_records_nothing() {
    let (s, m) = (Scratch::new(), marker());
    let socket = s.0.join("none.sock");
    let socket_arg = socket.to_string_lossy().into_owned();
    let args = ["mcp", "--socket", &socket_arg, "--session", "abcdefghijkl"];
    let out = run(&args, &m, false).await;
    assert_eq!(out.status.code(), Some(101), "{out:?}");
    assert!(
        out.stderr.is_empty(),
        "{}",
        String::from_utf8_lossy(&out.stderr)
    );
    assert!(out.stdout.is_empty());
    // It knows a socket and a session, no data directory: nothing was written anywhere.
    let mut all = Vec::new();
    files(&s.0, &mut all);
    assert!(all.is_empty(), "{all:?}");

    // The developer override lets the usual message through (so a developer sees it).
    let out = run(&args, &m, true).await;
    assert_eq!(out.status.code(), Some(101), "{out:?}");
    assert!(
        String::from_utf8_lossy(&out.stderr).contains(&m),
        "{}",
        String::from_utf8_lossy(&out.stderr)
    );
}
