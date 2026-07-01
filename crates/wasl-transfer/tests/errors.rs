//! Black-box error-path tests for `wasl_transfer` per docs/specs/transfer.md §Test plan.
//! Cases: E1 (execute with empty dst_hosts), E2 (TransferError Display/Error impl).

use wasl_proto::{HostId, TransferParams};
use wasl_transfer::{execute, Runner, Tool, TransferError};

fn hid(s: &str) -> HostId {
    HostId(s.to_string())
}

// ---------------------------------------------------------------------
// Fake Runner — records received argvs; used here to prove it is never called.
// ---------------------------------------------------------------------

struct FakeRunner {
    received: Vec<Vec<String>>,
}

impl FakeRunner {
    fn new() -> Self {
        FakeRunner {
            received: Vec::new(),
        }
    }
}

impl Runner for FakeRunner {
    fn run(&mut self, argv: &[String]) -> Result<(), String> {
        self.received.push(argv.to_vec());
        Ok(())
    }
}

// ---------------------------------------------------------------------
// E1 — execute with empty dst_hosts
// ---------------------------------------------------------------------

#[test]
fn e1_execute_with_empty_destinations_errs_no_destinations_runner_never_called() {
    let params = TransferParams {
        src_host: hid("src"),
        dst_hosts: vec![],
        path: "/tmp/x".to_string(),
    };
    let mut runner = FakeRunner::new();

    let result = execute(Tool::Scp, &params, &mut runner);

    assert_eq!(result, Err(TransferError::NoDestinations));
    assert!(
        runner.received.is_empty(),
        "runner must never be called when there are no destinations"
    );
}

// ---------------------------------------------------------------------
// E2 — TransferError Display + std::error::Error
// ---------------------------------------------------------------------

#[test]
fn e2_transfer_error_display_is_nonempty_distinct_and_failed_carries_fields() {
    let no_dst = TransferError::NoDestinations;
    let failed = TransferError::Failed {
        dst_host: hid("hostX"),
        detail: "timed out".to_string(),
    };

    let no_dst_msg = no_dst.to_string();
    let failed_msg = failed.to_string();

    assert!(!no_dst_msg.is_empty());
    assert!(!failed_msg.is_empty());
    assert_ne!(no_dst_msg, failed_msg, "Display must differ per variant");

    assert!(
        failed_msg.contains("hostX"),
        "Failed Display must carry the dst_host, got: {failed_msg}"
    );
    assert!(
        failed_msg.contains("timed out"),
        "Failed Display must carry the detail, got: {failed_msg}"
    );

    // Failed carries its fields for programmatic access too.
    match &failed {
        TransferError::Failed { dst_host, detail } => {
            assert_eq!(dst_host, &hid("hostX"));
            assert_eq!(detail, "timed out");
        }
        TransferError::NoDestinations => panic!("expected Failed variant"),
    }

    // std::error::Error impl: usable as a trait object, source() defaults to None.
    let as_error: &dyn std::error::Error = &no_dst;
    assert!(as_error.source().is_none());
    let as_error: &dyn std::error::Error = &failed;
    assert!(as_error.source().is_none());
}
