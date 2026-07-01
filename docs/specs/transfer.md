# WX — `wasl-transfer` — spec (authoritative)

**Tier:** L1 (pure argv + trait-mocked exec). **Deps:** `wasl-proto`, `serde` (+ `serde_json` dev). No
`unsafe`. The ONLY IO is behind the `Runner` trait — unit tests use a fake; real process exec is the
daemon's concern.

Re-use `wasl_proto::{HostId, TransferParams, TransferResult}`.

## 1. Types
```rust
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "lowercase")]
pub enum Tool { Scp, Rsync }        // default Scp

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct TransferJob { pub dst_host: HostId, pub argv: Vec<String> }

#[derive(Debug, Clone, PartialEq, Eq)]
pub enum TransferError { NoDestinations, Failed { dst_host: HostId, detail: String } }  // Display + Error
```

## 2. Argv builders (pure, deterministic)
Direct host-A→host-B, NOT routed through the local machine.
```rust
/// scp -3 <src_host>:<path> <dst_host>:<path>  (the `-3` routes A→B via this host's control
/// connection without staging the whole file locally; direct A→B semantics).
pub fn scp_argv(src_host: &HostId, dst_host: &HostId, path: &str) -> Vec<String>;
// => ["scp", "-3", "{src}:{path}", "{dst}:{path}"]

/// rsync -a <src_host>:<path> <dst_host>:<path>
pub fn rsync_argv(src_host: &HostId, dst_host: &HostId, path: &str) -> Vec<String>;
// => ["rsync", "-a", "{src}:{path}", "{dst}:{path}"]

pub fn argv_for(tool: Tool, src_host: &HostId, dst_host: &HostId, path: &str) -> Vec<String>;
```

## 3. Plan (fan-out a multicast transfer)
```rust
/// One TransferJob per destination host (multicast/broadcast). Empty dst_hosts → empty vec.
pub fn plan(tool: Tool, params: &TransferParams) -> Vec<TransferJob>;
```
Each job's `argv = argv_for(tool, &params.src_host, &dst, &params.path)`; jobs in `dst_hosts` order.

## 4. Runner trait + execute
```rust
pub trait Runner {
    /// Run one transfer; `Ok(())` on success, `Err(detail)` on failure.
    fn run(&mut self, argv: &[String]) -> Result<(), String>;
}

/// Execute a planned transfer over the runner. Runs every job; a job failure is recorded but does NOT
/// abort the rest (partial multicast). Returns the hosts that succeeded (`TransferResult.delivered`) plus
/// the failures. `NoDestinations` if `params.dst_hosts` is empty.
pub fn execute<R: Runner>(tool: Tool, params: &TransferParams, runner: &mut R)
    -> Result<(TransferResult, Vec<TransferError>), TransferError>;
```
- Empty `dst_hosts` → `Err(TransferError::NoDestinations)`.
- Else run each job; collect `delivered: Vec<HostId>` (succeeded, in order) and a `Vec<TransferError::Failed>`
  for the failures (in order). Return `Ok((TransferResult{delivered}, failures))`.

## Test plan (`tests/happy.rs`, `tests/errors.rs`; 100% line+region). Real asserting `#[test]`s.
A **fake Runner** in the tests records argvs and is scriptable to fail specific destinations.
**happy.rs**
- X1  `scp_argv` / `rsync_argv` exact vecs for a src/dst/path; `argv_for` dispatches on `Tool`.
- X2  `Tool` serde lowercase round-trip; default is `Scp` (assert via a Default impl or documented constant).
- X3  `plan`: 3 dst hosts → 3 jobs in order, each argv correct; empty dst → empty vec; 1 dst → 1 job.
- X4  `execute` all-success: fake runner returns Ok for all → `delivered` = all dst hosts in order,
      failures empty; the runner received the exact argvs from `plan`.
- X5  `execute` partial: fake runner fails the 2nd of 3 dsts → `delivered` = [1st,3rd], failures =
      [Failed{2nd}] — proves a failure doesn't abort the rest.
- X6  `TransferJob` serde round-trip.
**errors.rs**
- E1  `execute` with empty `dst_hosts` → `Err(TransferError::NoDestinations)` (runner never called).
- E2  `TransferError` Display non-empty + distinct per variant; `Failed` carries the host + detail;
      `std::error::Error` implemented.

Coverage: hit both tools, plan empty/one/many, execute all-ok / partial-fail / no-destinations. Delete
unreachable branches rather than leaving them uncovered.
