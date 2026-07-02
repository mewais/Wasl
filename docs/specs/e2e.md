# E2E — `wasl-e2e` — cross-crate scenario suite — spec (authoritative)

**Tests-only crate** (empty `src`). Composes the whole Wasl stack over its real trait seams, driven by
fakes, to prove the blocks integrate into the intended system: a tool call is **gated** (server, three
tiers) and, only on approval, **dispatched** to peers (daemon) and **recorded** (audit) — with the Tier-3
path routed through the **external authority**. This is the "inter-block → e2e" layer: no new production
logic, just the composition the real MCP server/daemon binary will wire.

**Deps (dev):** the wasl crates it composes — `wasl-proto`, `wasl-gate`, `wasl-registry`, `wasl-mailbox`,
`wasl-transfer`, `wasl-audit`, `wasl-server`, `wasl-daemon`, `serde`, `serde_json`. No `unsafe`. Not a
100%-coverage unit (no src of its own) — but every test is a real `#[test]` asserting the **composed**
behavior end to end. No `#[ignore]`/stub/tautology.

## The composition under test (the "server loop" the tests embody)
For a consult/command/message call:
1. Build a `Gatekeeper<S,K>` (wasl-server) with the client's `ClientCaps`, a `Surfacer`, and an audit `Sink`.
2. `authorize(...)` → `Verdict`. **Only** on `Verdict::Proceed` do we dispatch; `Denied`/`Dismissed` short-
   circuit (no dispatch); `Pending` waits for a `commit(...)` that yields `Proceed`.
3. On proceed, `wasl_daemon::{consult|command|message}(registry, self_host, base_id, from, params, mesh)` →
   the typed result + `DispatchReport`.
4. Assert BOTH the gate side (the audit log recorded the decision) AND the dispatch side (replies/delivered/
   report) — i.e. the two halves agree and the flow is coherent.
For transfer: gate → on proceed, `wasl_daemon::transfer(tool, params, runner)`.
For Tier-3: the gate is surfaced (via the `Surfacer`) to an EXTERNAL authority; the decision it returns is
recorded to the GENERIC audit log — there is no Wasl-specific "connector feed", the audit log IS the
observation feed. (Any external authority — e.g. a supervisor living outside Wasl — implements the same
`Surfacer` seam and/or an audit `Sink`.)

Provide fakes in the test crate (may live in `tests/common/mod.rs` with `#![allow(dead_code)]`): a scriptable
`Surfacer`, a `Sink` (records; can fail), a `Mesh` (per-peer reply/fail), and a `Runner` (per-dst fail).

## Scenarios (`tests/`; real `#[test]`s)
- **X1 approved consult, end to end** — caps elicitation; surfacer Approves. `authorize` → `Proceed` and the
  audit log has one `Decision` entry `Some(Approve)`. Then `consult` over a registry with 2 local peers +
  a Mesh returning replies → `ConsultResult{replies}` (both peers, in order), `report` empty. Asserts the
  gate approved AND the dispatch delivered — the full happy path.
- **X2 denied → no dispatch** — surfacer Denies. `authorize` → `Denied{reason}`; audit has `Some(Deny)`.
  The composition MUST NOT dispatch (assert by never calling the daemon; the Mesh is never touched). Proves
  the gate actually gates.
- **X3 Tier-2 prepare→commit→dispatch** — caps neither (PrepareCommit). `authorize` → `Pending{token}`, no
  audit yet, surfacer not called. `commit(token, Approve)` → `Proceed` + audit `Some(Approve)`. Then dispatch
  a command over the registry → `CommandResult{replies}`.
- **X4 Tier-3 via an external authority** — caps `{elicitation:false, external:true}` (mode `External`). A
  `Surfacer` (any external authority) approves. `authorize` → `Proceed`; the decision is recorded to the
  GENERIC audit log (`Decision` entry, `Some(Approve)`). Then dispatch a message → `MessageResult{delivered}`.
  Ties server (gate+audit) + daemon; the external authority is just a `Surfacer` impl.
- **X5 approved transfer** — gate Approve; then `wasl_daemon::transfer(Tool::Scp, params, runner)` over a
  fake Runner (2 dst hosts, all-ok) → `TransferResult{delivered}` = both hosts, no failures.
- **X6 multicast with an unknown peer + a failed delivery** — 3 targets, one unregistered, Mesh fails one of
  the resolved: after an approved gate, `consult` → replies only for the delivered, `report.unknown` = the
  unregistered, `report.failed` = the failing peer; the delivered set proves partial multicast survived. One
  audit decision entry for the (single) gate.
- **X7 Tier-2 commit denied → no dispatch** — prepare, then `commit(token, Deny{reason})` → `Denied`; audit
  `Some(Deny)`; the composition does not dispatch.
- **X8 gate audit failure aborts before dispatch** — sink scripted to fail; `authorize` → `Err(Audit)` (the
  decision wasn't durably recorded), so the composition surfaces the error and does NOT dispatch. Proves the
  durability-first gate protects the dispatch.

Each scenario asserts the SEAM AGREEMENT: the audit record and the dispatch outcome are consistent with the
gate verdict (approved ⇒ recorded + dispatched; denied/dismissed/error ⇒ recorded-or-error + NOT dispatched).
Assert concrete values (reply bodies, delivered peers, report partitions, audit `decision`), never bare counts.
