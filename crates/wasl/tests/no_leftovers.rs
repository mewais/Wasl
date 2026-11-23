//! The guard of the test hygiene: a test that fails leaves nothing behind. The processes a
//! daemon test starts are detached on purpose (a session's shim, its guard and the CLI survive
//! their daemon), so the harness ends them itself (`Scratch`'s drop, which runs while a
//! failing test unwinds). This test runs a scenario with real daemons and sessions — one daemon
//! killed while its session runs on, one still running — and panics in the middle; then it
//! asserts that every process the scenario started has ended and its directory is gone. It
//! checks only the processes it started itself, named by the processes' own reports.
#![cfg(target_os = "linux")]

mod common;

use std::path::PathBuf;
use std::sync::{Arc, Mutex};

use common::*;
use wasit_fake_cli::harness::PidWatch;
use wasit_fake_cli::scenario::Scenario;

/// What the failing scenario started, noted as it goes.
#[derive(Default)]
struct Started {
    root: PathBuf,
    /// A handle on each process, opened while it was alive.
    watches: Vec<(String, PidWatch)>,
}

impl Started {
    fn note(&mut self, what: impl Into<String>, pid: u32) {
        let what = what.into();
        let watch = PidWatch::open(pid).unwrap_or_else(|| panic!("{what} ({pid}) is not alive"));
        self.watches.push((what, watch));
    }
}

/// The parent of `pid` (the guard is the parent of the CLI).
fn parent_of(pid: u32) -> u32 {
    let stat = std::fs::read_to_string(format!("/proc/{pid}/stat")).unwrap();
    // `pid (comm) state ppid …`; the name may hold spaces and parentheses.
    let after = &stat[stat.rfind(')').unwrap() + 2..];
    after.split(' ').nth(1).unwrap().parse().unwrap()
}

/// A daemon with one idle session, the processes noted: the daemon, the session's shim, its
/// guard and its CLI.
async fn daemon_with_session(started: &Mutex<Started>, scratch: &Scratch, host: &str) -> Daemon {
    let d = Daemon::start(scratch, host).await;
    let daemon = d.client.host_info().await.unwrap().pid;
    let agent = d.session(&Scenario::default()).await;
    let shim = wasit_shim::ShimClient::connect(&d.dir.join("sessions").join(&agent.session))
        .await
        .unwrap();
    let (shim_pid, cli) = (shim.shim_pid(), shim.child_pid());
    let mut s = started.lock().unwrap();
    s.note(format!("{host} daemon"), daemon);
    s.note(format!("{host} shim"), shim_pid);
    s.note(format!("{host} guard"), parent_of(cli));
    s.note(format!("{host} CLI"), cli);
    d
}

async fn failing_scenario(started: Arc<Mutex<Started>>) {
    let scratch = Scratch::new();
    started.lock().unwrap().root = scratch.0.clone();
    let mut gone = daemon_with_session(&started, &scratch, "h1").await;
    // Its session goes on in its shim.
    gone.kill().await;
    let _running = daemon_with_session(&started, &scratch, "h2").await;
    panic!("the test fails in the middle");
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn a_failing_test_leaves_no_process_and_no_directory_behind() {
    let started = Arc::new(Mutex::new(Started::default()));
    // On a task of its own, so that its panic unwinds its locals (the harness's drop runs)
    // while this test goes on.
    let outcome = tokio::spawn(failing_scenario(started.clone())).await;
    assert!(outcome.unwrap_err().is_panic(), "the scenario must panic");

    let started = started.lock().unwrap();
    assert_eq!(started.watches.len(), 8, "the scenario started 8 processes");
    for (what, watch) in &started.watches {
        assert!(
            watch.is_gone(),
            "the {what} is still running when its test has failed"
        );
    }
    assert!(
        !started.root.exists(),
        "{} was not removed",
        started.root.display()
    );
}
