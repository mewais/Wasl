# E2E — `wasl-e2e` — working contract
**Purpose:** prove the Wasl crates compose end-to-end. Integration-test-only crate (empty src) that wires
`wasl-server` (Gatekeeper) with the LIVE A2A dispatch path (`wasl-dispatch` over a real loopback
`wasl-a2a-server`), plus `wasl-audit`, `wasl-registry`, `wasl-transfer`, over their trait seams with fakes,
exercising full flows. **Spec:** `../../docs/specs/e2e.md`
- **code/test:** this crate is TESTS-ONLY — everything lives in `tests/**`. No production src beyond the doc stub.
- Done: `cargo test -p wasl-e2e` green; scenarios cover approve/deny/Tier-2/Tier-3/transfer/partial-multicast end to end; no unsafe; fmt + `clippy --all-targets -- -D warnings` clean. NOT a 100%-coverage unit (it has no src) — but every test is real, asserting the composed behavior. No `#[ignore]`/stub/tautology.

## W9 — retirement of the legacy transport stack (dispatch-half migration)
The dispatch half of every scenario used to run over the **legacy** `wasl-daemon` `Mesh` seam (driven by a
`FakeMesh` in `tests/common/mod.rs`), which was never the live send path. `wasl-daemon` and `wasl-mailbox`
have been deleted; the dispatch half now runs over the LIVE A2A path — `wasl_dispatch::{consult,command,
message}` against a REAL `wasl_a2a_server` (axum) on an ephemeral `127.0.0.1` port, driven by a REAL
`wasl_adapter::AdapterExecutor` wrapping a FAKE innermost `AgentAdapter` — the same pattern
`wasl-dispatch/tests/loopback.rs` and `wasl-node/tests/e2e.rs` use. The gate half (`wasl-server`
`Gatekeeper` over `ScriptSurfacer`/`FakeSink`) is unchanged — it never depended on the daemon/mesh.

Transfer (X5) never went through the daemon's `Mesh` in the first place: `wasl_daemon::transfer` was a
one-line passthrough to `wasl_transfer::execute`. X5 now calls `wasl_transfer::execute` directly over the
same `FakeRunner`, with identical assertions — no live-path change was needed there.

### Scenario mapping (old `FakeMesh` → new live-A2A loopback), 1:1, no coverage lost
| # | Old (legacy `wasl-daemon` + `FakeMesh`) | New (live `wasl-dispatch` + loopback `wasl-a2a-server`) |
|---|---|---|
| X1 | `wasl_daemon::consult` over a `FakeMesh` scripted with per-peer reply bodies; asserted `mesh.calls.len()`. | `wasl_dispatch::consult` against two REAL `spawn_echo()` loopback peers; asserted `peer.deliveries().len()` per peer — the reply body now genuinely crossed a real A2A HTTP round-trip. |
| X2 | Verdict::Denied guarded a conditional call to `wasl_daemon::consult`; asserted `mesh.calls.is_empty()`. | Same conditional guard around `wasl_dispatch::consult`; asserted `peer.deliveries().is_empty()` on a REAL (but never-reached) loopback peer. |
| X3 | Tier-2 commit(Approve) then `wasl_daemon::command` over `FakeMesh`. | Tier-2 commit(Approve) then `wasl_dispatch::command` against two REAL loopback peers; same reply/report assertions plus `peer.deliveries()`. |
| X4 | Tier-3 external-authority approve then `wasl_daemon::message` over `FakeMesh`. | Tier-3 external-authority approve then `wasl_dispatch::message` against one REAL loopback peer; `MessageResult.delivered` + `peer.deliveries()`. |
| X5 | `wasl_daemon::transfer(Tool::Scp, params, &mut FakeRunner)`. | `wasl_transfer::execute(Tool::Scp, params, &mut FakeRunner)` — identical call shape/assertions; `wasl_daemon::transfer` was only ever a passthrough to this function, so nothing about the transfer path itself changed. |
| X6 | `wasl_daemon::consult` over a `FakeMesh` with `with_reply`/`with_failure`/an unregistered "ghost"; asserted `mesh.calls.len()`. | `wasl_dispatch::consult` with one REAL `spawn_echo()` peer, one REAL-but-unreachable peer (`dead_url()` — a bound-then-dropped loopback port, modelling a genuine transport failure), and an unregistered "ghost"; same `report.unknown`/`report.failed` partition, plus `peer1.deliveries()`. |
| X7 | Tier-2 commit(Deny) guarded a conditional call to `wasl_daemon::command`; asserted `mesh.calls.is_empty()`. | Same guard around `wasl_dispatch::command`; asserted `peer.deliveries().is_empty()`. |
| X8 | Failing audit `Sink` guarded a conditional call to `wasl_daemon::consult`; asserted `mesh.calls.is_empty()`. | Same guard around `wasl_dispatch::consult`; asserted `peer.deliveries().is_empty()`. |

`FakeMesh` (the `wasl_daemon::Mesh` fake) is gone from `tests/common/mod.rs`; in its place: `Peer` /
`spawn_echo` / `spawn_failing` / `dead_url` / `register` / `card` — real loopback A2A fixtures mirroring
`wasl-dispatch/tests/common` and `wasl-node/tests/common`. `spawn_failing` (executor-level `AgentAdapter`
error, as opposed to `dead_url`'s transport-level failure) is carried over from the sibling suites for
fixture parity but is not exercised by any X1-X8 scenario (that distinction is already covered by
`wasl-dispatch`'s own D6/D7 tests) — it stays available (allowed dead code) rather than being removed, to
keep this crate's fixtures a faithful subset of the sibling loopback suites' seam.

`ScriptSurfacer` and `FakeSink` (the gate-side fakes) are unchanged — the gate never touched the
daemon/mesh, so no mapping is needed for them.
