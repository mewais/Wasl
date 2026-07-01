# WC — `wasl-marsad` — spec (authoritative)

**Tier:** L1 (pure logic + trait-mocked link). **Deps:** `wasl-proto`, `wasl-gate`, `serde` (+ `serde_json`
dev). No `unsafe`. The ONLY IO is behind the `MarsadLink` trait — unit tests use a fake; the real HTTP/IPC
link to Marsad is the daemon/server's concern.

Re-use `wasl_proto::{AgentId, GateRequest, GateDecision}` and `wasl_gate::GateOutcome`. Observations carry a
caller-provided `seq` (deterministic — no clock in this crate), mirroring `wasl-audit`.

The connector is Wasl's **Tier-3** path: Wasl surfaces a HITL gate into Marsad exactly as Marsad surfaces
any other permission, Marsad returns the human decision, and Wasl also **notifies Marsad of auto-approved
interactions** and **emits an observation feed** of all activity. Durability-first, like `wasl-audit`: a feed
item is cached locally ONLY after the link accepts it.

## Types
```rust
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum ObsKind { Consult, Command, Message, Transfer, Gate, AutoApproved }

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct Observation {
    pub seq: u64,                         // caller-provided monotonic sequence (deterministic ordering)
    pub kind: ObsKind,
    pub from: AgentId,                    // who initiated
    pub to: Vec<AgentId>,                 // recipients (one, or many for multicast)
    pub summary: String,                  // human-readable, secret-free
    #[serde(default)]
    pub decision: Option<GateDecision>,   // Some(_) when this obs records a gate/auto-approve decision
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub enum ConnError { Transport(String) } // impl Display + std::error::Error
```

## MarsadLink trait (the mockable seam to Marsad)
```rust
pub trait MarsadLink {
    /// Surface a HITL gate to Marsad and BLOCK for the human decision.
    fn request_decision(&mut self, req: &GateRequest) -> Result<GateDecision, ConnError>;
    /// Emit an observation onto Marsad's feed.
    fn emit(&mut self, obs: &Observation) -> Result<(), ConnError>;
}
```

## Connector
```rust
pub struct Connector<L: MarsadLink> { /* private: link + in-memory Vec<Observation> feed cache */ }
```
- `new(link: L) -> Self`.
- `gate(&mut self, req: &GateRequest, seq: u64) -> Result<GateOutcome, ConnError>` — Tier-3 gate: call
  `link.request_decision(req)`; on `Ok(decision)`, `emit` an `Observation{ seq, kind: Gate, from: req.from,
  to: vec![], summary: req.summary, decision: Some(decision) }`; on the emit `Ok`, cache it; return the
  outcome (`decision_to_outcome`). If `request_decision` errs → propagate, emit/cache nothing. If `emit`
  errs → propagate, cache nothing (the decision was made but not durably observed — surface the error).
- `notify_auto_approved(&mut self, seq: u64, kind: ObsKind, from: AgentId, to: Vec<AgentId>, summary:
  String) -> Result<(), ConnError>` — the auto-approved fallback: build an `Observation` with
  `decision: Some(GateDecision::Approve)` and `emit` + cache it (durability-first). Propagate + don't cache
  on link error. (The `kind` is the underlying interaction kind — Consult/Command/Message/Transfer.)
- `observe(&mut self, obs: Observation) -> Result<(), ConnError>` — emit a plain (non-decision) observation
  + cache on `Ok`; propagate + don't cache on error.
- `feed(&self) -> &[Observation]` — the cache, in emit order. `len`/`is_empty`.
- `by_kind(&self, kind: ObsKind) -> Vec<&Observation>` — matching kind, in order.
- `decisions(&self) -> Vec<&Observation>` — observations where `decision.is_some()`, in order.

## Decision → outcome (WC-local helper; pub)
```rust
/// Map Marsad's concrete decision to a gate outcome (Marsad never "dismisses").
pub fn decision_to_outcome(d: &GateDecision) -> GateOutcome;
// Approve => GateOutcome::Approved ; Deny{reason} => GateOutcome::Denied{reason}
```

## Test plan (`tests/happy.rs`, `tests/errors.rs`; 100% line+region). Real asserting `#[test]`s. Provide a
**fake MarsadLink** in the tests: scriptable decision to return from `request_decision`, records emitted
observations, and a fail flag for both methods (for the error cases).
**happy.rs**
- C1  `gate`: link returns `Approve` → returns `GateOutcome::Approved`, and `feed()` has one `Gate` obs
      carrying `decision: Some(Approve)`, `from == req.from`, `summary == req.summary`. A second case: link
      returns `Deny{reason}` → `GateOutcome::Denied{reason}` and the feed obs carries `Some(Deny)`.
- C2  `notify_auto_approved`: emits ONE `AutoApproved` obs with `decision: Some(Approve)`, the given
      kind/from/to/summary; `feed()` reflects it in order after a prior `observe`.
- C3  `observe`: a plain obs (`decision: None`) is emitted + cached; multiple `observe`/`gate`/`notify`
      calls preserve emit order in `feed()`; `len`/`is_empty`.
- C4  `by_kind` (matching + non-matching present) and `decisions` (both an Approve-carrying and a
      None-carrying obs present → only the decision one surfaces; assert an Approve and a Deny both surface).
- C5  `decision_to_outcome`: `Approve` → `Approved`; `Deny{reason}` → `Denied{reason}` (reason preserved).
- C6  serde: `Observation` round-trips incl. `decision: None` default (field absent) + `Some(Approve)` /
      `Some(Deny)`; `ObsKind` snake_case round-trip for all 6 variants.
**errors.rs**
- E1  `gate` when `request_decision` fails → `Err(ConnError::Transport(_))` AND `feed()` stays empty
      (nothing emitted/cached).
- E2  `gate` when `request_decision` succeeds but `emit` fails → `Err(Transport)` AND `feed()` stays empty
      (durability-first: the decision wasn't durably observed).
- E3  `observe` / `notify_auto_approved` when `emit` fails → `Err(Transport)` AND the obs is NOT cached
      (len unchanged) — durability-first invariant.
- E4  `ConnError` Display non-empty + carries the transport detail; `std::error::Error` implemented.

Coverage: hit gate ok(Approve)/ok(Deny)/decision-err/emit-err, notify ok/err, observe ok/err, both filters
with matching + non-matching, decision_to_outcome both arms, the None/Some serde. Delete unreachable
branches rather than leaving them uncovered.
