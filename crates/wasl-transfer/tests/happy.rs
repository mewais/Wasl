//! Black-box happy-path tests for `wasl_transfer` per docs/specs/transfer.md §Test plan.
//! Cases: X1 (argv builders), X2 (Tool serde/default), X3 (plan), X4 (execute all-success),
//! X5 (execute partial failure), X6 (TransferJob serde round-trip).

use wasl_proto::{HostId, TransferParams, TransferResult};
use wasl_transfer::{
    argv_for, execute, plan, rsync_argv, scp_argv, Runner, Tool, TransferError, TransferJob,
};

fn hid(s: &str) -> HostId {
    HostId(s.to_string())
}

// ---------------------------------------------------------------------
// Fake Runner — records received argvs, scriptable to fail by call index.
// ---------------------------------------------------------------------

struct FakeRunner {
    /// argvs received, in call order.
    received: Vec<Vec<String>>,
    /// 0-based call indices that should fail, with their failure detail.
    fail_at: Vec<(usize, String)>,
}

impl FakeRunner {
    fn new() -> Self {
        FakeRunner {
            received: Vec::new(),
            fail_at: Vec::new(),
        }
    }

    fn failing_at(indices_and_details: &[(usize, &str)]) -> Self {
        FakeRunner {
            received: Vec::new(),
            fail_at: indices_and_details
                .iter()
                .map(|(i, d)| (*i, d.to_string()))
                .collect(),
        }
    }
}

impl Runner for FakeRunner {
    fn run(&mut self, argv: &[String]) -> Result<(), String> {
        let call_index = self.received.len();
        self.received.push(argv.to_vec());
        if let Some((_, detail)) = self.fail_at.iter().find(|(i, _)| *i == call_index) {
            Err(detail.clone())
        } else {
            Ok(())
        }
    }
}

// ---------------------------------------------------------------------
// X1 — scp_argv / rsync_argv / argv_for
// ---------------------------------------------------------------------

#[test]
fn x1_scp_argv_builds_exact_vec() {
    let src = hid("hostA");
    let dst = hid("hostB");

    let argv = scp_argv(&src, &dst, "/data/file.bin");

    assert_eq!(
        argv,
        vec![
            "scp".to_string(),
            "-3".to_string(),
            "hostA:/data/file.bin".to_string(),
            "hostB:/data/file.bin".to_string(),
        ]
    );
}

#[test]
fn x1_rsync_argv_builds_exact_vec() {
    let src = hid("hostA");
    let dst = hid("hostB");

    let argv = rsync_argv(&src, &dst, "/data/file.bin");

    assert_eq!(
        argv,
        vec![
            "rsync".to_string(),
            "-a".to_string(),
            "hostA:/data/file.bin".to_string(),
            "hostB:/data/file.bin".to_string(),
        ]
    );
}

#[test]
fn x1_argv_for_dispatches_on_tool() {
    let src = hid("hostA");
    let dst = hid("hostB");

    let via_scp = argv_for(Tool::Scp, &src, &dst, "/x");
    let via_rsync = argv_for(Tool::Rsync, &src, &dst, "/x");

    assert_eq!(via_scp, scp_argv(&src, &dst, "/x"));
    assert_eq!(via_rsync, rsync_argv(&src, &dst, "/x"));
    assert_ne!(via_scp, via_rsync, "scp and rsync argv must differ");
}

// ---------------------------------------------------------------------
// X2 — Tool serde lowercase round-trip; default is Scp
// ---------------------------------------------------------------------

#[test]
fn x2_tool_serde_lowercase_round_trip() {
    let scp_json = serde_json::to_string(&Tool::Scp).expect("Tool::Scp must serialize");
    assert_eq!(scp_json, "\"scp\"");
    let rsync_json = serde_json::to_string(&Tool::Rsync).expect("Tool::Rsync must serialize");
    assert_eq!(rsync_json, "\"rsync\"");

    let scp_back: Tool = serde_json::from_str(&scp_json).expect("\"scp\" must deserialize");
    assert_eq!(scp_back, Tool::Scp);
    let rsync_back: Tool = serde_json::from_str(&rsync_json).expect("\"rsync\" must deserialize");
    assert_eq!(rsync_back, Tool::Rsync);
}

#[test]
fn x2_tool_default_is_scp() {
    assert_eq!(Tool::default(), Tool::Scp);
}

// ---------------------------------------------------------------------
// X3 — plan: order, empty, single
// ---------------------------------------------------------------------

#[test]
fn x3_plan_three_destinations_yields_three_jobs_in_order() {
    let params = TransferParams {
        src_host: hid("src"),
        dst_hosts: vec![hid("d0"), hid("d1"), hid("d2")],
        path: "/tmp/payload".to_string(),
    };

    let jobs = plan(Tool::Scp, &params);

    assert_eq!(jobs.len(), 3);
    assert_eq!(jobs[0].dst_host, hid("d0"));
    assert_eq!(jobs[1].dst_host, hid("d1"));
    assert_eq!(jobs[2].dst_host, hid("d2"));

    for (i, dst) in params.dst_hosts.iter().enumerate() {
        assert_eq!(
            jobs[i].argv,
            argv_for(Tool::Scp, &params.src_host, dst, &params.path),
            "job {i} argv must match argv_for"
        );
    }
}

#[test]
fn x3_plan_empty_destinations_yields_empty_vec() {
    let params = TransferParams {
        src_host: hid("src"),
        dst_hosts: vec![],
        path: "/tmp/x".to_string(),
    };

    let jobs = plan(Tool::Rsync, &params);

    assert!(jobs.is_empty());
    assert_eq!(jobs, Vec::<TransferJob>::new());
}

#[test]
fn x3_plan_single_destination_yields_one_job() {
    let params = TransferParams {
        src_host: hid("src"),
        dst_hosts: vec![hid("solo")],
        path: "/tmp/only".to_string(),
    };

    let jobs = plan(Tool::Rsync, &params);

    assert_eq!(jobs.len(), 1);
    assert_eq!(jobs[0].dst_host, hid("solo"));
    assert_eq!(
        jobs[0].argv,
        rsync_argv(&hid("src"), &hid("solo"), "/tmp/only")
    );
}

// ---------------------------------------------------------------------
// X4 — execute all-success
// ---------------------------------------------------------------------

#[test]
fn x4_execute_all_success_delivers_all_in_order_no_failures() {
    let params = TransferParams {
        src_host: hid("src"),
        dst_hosts: vec![hid("d0"), hid("d1"), hid("d2")],
        path: "/tmp/payload".to_string(),
    };
    let mut runner = FakeRunner::new();

    let result = execute(Tool::Scp, &params, &mut runner).expect("all-success execute must be Ok");
    let (transfer_result, failures) = result;

    assert_eq!(
        transfer_result,
        TransferResult {
            delivered: vec![hid("d0"), hid("d1"), hid("d2")]
        }
    );
    assert!(failures.is_empty());
    assert_eq!(failures, Vec::<TransferError>::new());

    // The runner received exactly the argvs plan() would have produced, in order.
    let expected_jobs = plan(Tool::Scp, &params);
    let expected_argvs: Vec<Vec<String>> = expected_jobs.iter().map(|j| j.argv.clone()).collect();
    assert_eq!(runner.received, expected_argvs);
    assert_eq!(runner.received.len(), 3);
}

// ---------------------------------------------------------------------
// X5 — execute partial failure: a mid failure doesn't abort the rest
// ---------------------------------------------------------------------

#[test]
fn x5_execute_partial_failure_second_of_three_delivered_skips_it_others_continue() {
    let params = TransferParams {
        src_host: hid("src"),
        dst_hosts: vec![hid("d0"), hid("d1"), hid("d2")],
        path: "/tmp/payload".to_string(),
    };
    let mut runner = FakeRunner::failing_at(&[(1, "connection refused")]);

    let (transfer_result, failures) = execute(Tool::Rsync, &params, &mut runner)
        .expect("partial failure is still an Ok(..) execute");

    assert_eq!(
        transfer_result,
        TransferResult {
            delivered: vec![hid("d0"), hid("d2")]
        }
    );
    assert_eq!(
        failures,
        vec![TransferError::Failed {
            dst_host: hid("d1"),
            detail: "connection refused".to_string()
        }]
    );

    // The runner ran on all 3 destinations, in order, despite the mid failure.
    assert_eq!(runner.received.len(), 3);
    assert_eq!(
        runner.received[0],
        rsync_argv(&hid("src"), &hid("d0"), "/tmp/payload")
    );
    assert_eq!(
        runner.received[1],
        rsync_argv(&hid("src"), &hid("d1"), "/tmp/payload")
    );
    assert_eq!(
        runner.received[2],
        rsync_argv(&hid("src"), &hid("d2"), "/tmp/payload")
    );
}

// ---------------------------------------------------------------------
// X6 — TransferJob serde round-trip
// ---------------------------------------------------------------------

#[test]
fn x6_transfer_job_serde_round_trip() {
    let job = TransferJob {
        dst_host: hid("hostB"),
        argv: vec![
            "scp".to_string(),
            "-3".to_string(),
            "hostA:/x".to_string(),
            "hostB:/x".to_string(),
        ],
    };

    let json = serde_json::to_string(&job).expect("TransferJob must serialize");
    let round_tripped: TransferJob =
        serde_json::from_str(&json).expect("TransferJob must deserialize");

    assert_eq!(round_tripped, job);
}
