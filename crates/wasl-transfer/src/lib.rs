//! wasl-transfer — the transfer tool (WX): pure scp -3 / rsync argv builders for
//! direct host-A->host-B (and A->many) transfers, plus a `Runner` trait so the
//! actual process exec lives behind a mockable seam. See `docs/specs/transfer.md`.

use std::fmt;

use serde::{Deserialize, Serialize};
use wasl_proto::{HostId, TransferParams, TransferResult};

// ---------------------------------------------------------------------
// 1. Types
// ---------------------------------------------------------------------

#[derive(Debug, Default, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "lowercase")]
pub enum Tool {
    #[default]
    Scp,
    Rsync,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct TransferJob {
    pub dst_host: HostId,
    pub argv: Vec<String>,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub enum TransferError {
    NoDestinations,
    Failed { dst_host: HostId, detail: String },
}

impl fmt::Display for TransferError {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            TransferError::NoDestinations => write!(f, "transfer has no destination hosts"),
            TransferError::Failed { dst_host, detail } => {
                write!(f, "transfer to {} failed: {detail}", dst_host.as_str())
            }
        }
    }
}

impl std::error::Error for TransferError {}

// ---------------------------------------------------------------------
// 2. Argv builders (pure, deterministic)
// ---------------------------------------------------------------------

/// scp -3 <src_host>:<path> <dst_host>:<path>  (the `-3` routes A→B via this host's control
/// connection without staging the whole file locally; direct A→B semantics).
pub fn scp_argv(src_host: &HostId, dst_host: &HostId, path: &str) -> Vec<String> {
    vec![
        "scp".to_string(),
        "-3".to_string(),
        format!("{}:{path}", src_host.as_str()),
        format!("{}:{path}", dst_host.as_str()),
    ]
}

/// rsync -a <src_host>:<path> <dst_host>:<path>
pub fn rsync_argv(src_host: &HostId, dst_host: &HostId, path: &str) -> Vec<String> {
    vec![
        "rsync".to_string(),
        "-a".to_string(),
        format!("{}:{path}", src_host.as_str()),
        format!("{}:{path}", dst_host.as_str()),
    ]
}

pub fn argv_for(tool: Tool, src_host: &HostId, dst_host: &HostId, path: &str) -> Vec<String> {
    match tool {
        Tool::Scp => scp_argv(src_host, dst_host, path),
        Tool::Rsync => rsync_argv(src_host, dst_host, path),
    }
}

// ---------------------------------------------------------------------
// 3. Plan (fan-out a multicast transfer)
// ---------------------------------------------------------------------

/// One TransferJob per destination host (multicast/broadcast). Empty dst_hosts → empty vec.
pub fn plan(tool: Tool, params: &TransferParams) -> Vec<TransferJob> {
    params
        .dst_hosts
        .iter()
        .map(|dst| TransferJob {
            dst_host: dst.clone(),
            argv: argv_for(tool, &params.src_host, dst, &params.path),
        })
        .collect()
}

// ---------------------------------------------------------------------
// 4. Runner trait + execute
// ---------------------------------------------------------------------

pub trait Runner {
    /// Run one transfer; `Ok(())` on success, `Err(detail)` on failure.
    fn run(&mut self, argv: &[String]) -> Result<(), String>;
}

/// Execute a planned transfer over the runner. Runs every job; a job failure is recorded but does NOT
/// abort the rest (partial multicast). Returns the hosts that succeeded (`TransferResult.delivered`) plus
/// the failures. `NoDestinations` if `params.dst_hosts` is empty.
pub fn execute(
    tool: Tool,
    params: &TransferParams,
    runner: &mut dyn Runner,
) -> Result<(TransferResult, Vec<TransferError>), TransferError> {
    let jobs = plan(tool, params);
    if jobs.is_empty() {
        return Err(TransferError::NoDestinations);
    }

    let mut delivered = Vec::new();
    let mut failures = Vec::new();

    for job in jobs {
        match runner.run(&job.argv) {
            Ok(()) => delivered.push(job.dst_host),
            Err(detail) => failures.push(TransferError::Failed {
                dst_host: job.dst_host,
                detail,
            }),
        }
    }

    Ok((TransferResult { delivered }, failures))
}
