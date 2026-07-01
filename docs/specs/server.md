# WS — `wasl-server` (gate+audit spine) — spec (authoritative)

**Tier:** L1 (pure orchestration + trait-mocked seams). **Deps:** `wasl-proto`, `wasl-gate`, `wasl-audit`,
`serde` (+ `serde_json` dev). No `unsafe`. The ONLY IO is behind the `Surfacer` trait (surfacing a gate to
a human) and `wasl_audit::Sink` (durable log) — unit tests use fakes.

This is the server request **spine**: for every outgoing tool call, run the three-tier
permission-before-send gate and record the decision to the audit log. Tier-1 (elicitation) and Tier-3
(marsad) surface synchronously via `Surfacer`; Tier-2 (prepare/commit) defers through the gate ledger and
resolves on a later `commit`. Routing/transport (registry + mailbox + transfer dispatch) is the daemon's
job (WD) and is explicitly OUT of scope here.

Re-use: `wasl_proto::{AgentId, GateKind, GateRequest, GateDecision, ApprovalToken}`,
`wasl_gate::{ClientCaps, GateMode, GateOutcome, choose_mode, build_request}`,
`wasl_audit::{AuditEntry, EntryKind, AuditLog, Sink, AuditError}`.

## Types
```rust
/// Surface a gate request to the human in the chosen synchronous mode and return
/// the outcome. Only called for Tier-1 (Elicitation) and Tier-3 (Marsad); Tier-2
/// (PrepareCommit) never calls this — it defers via the ledger.
pub trait Surfacer {
    fn surface(&mut self, mode: GateMode, req: &GateRequest) -> GateOutcome;
}

/// The result of authorizing (or committing) a gated tool call.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case", tag = "verdict")]
pub enum Verdict {
    Proceed,                          // approved — the caller may send
    Denied { reason: String },        // human denied
    Dismissed,                        // human dismissed/cancelled (no decision)
    Pending { token: ApprovalToken }, // Tier-2: awaiting a later commit()
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub enum ServerError {
    Audit(String),      // the audit sink rejected the decision record
    Gate(GateError),    // ledger error on commit (unknown / already-resolved token)
}   // impl Display + std::error::Error; From<GateError>
```

## Gatekeeper
```rust
pub struct Gatekeeper<S: Surfacer, K: Sink> { /* private: caps, surfacer, ledger, audit: AuditLog<K>,
                                                 pending: HashMap<ApprovalToken,(GateKind,AgentId,Vec<AgentId>)> */ }
```
- `new(caps: ClientCaps, surfacer: S, sink: K) -> Self`.
- `mode(&self) -> GateMode` — `choose_mode(&caps)`.
- `authorize(&mut self, id: String, kind: GateKind, from: AgentId, targets: Vec<AgentId>, preview: String,
   token: ApprovalToken, seq: u64) -> Result<Verdict, ServerError>`:
  - Build the request: `build_request(id, kind, from.clone(), &target_strs, preview)` where `target_strs`
    is the `targets` mapped to their `String` ids.
  - Branch on `mode()`:
    - **Elicitation | Marsad (synchronous):** `let outcome = surfacer.surface(mode, &req);` then
      `record_decision(...)` (see below) with the outcome's decision, and map the outcome to a verdict:
      `Approved → Proceed`, `Denied{reason} → Denied{reason}`, `Dismissed → Dismissed`.
    - **PrepareCommit (Tier-2):** `ledger.prepare(token.clone(), req)`, remember
      `pending.insert(token.clone(), (kind, from, targets))`, and return `Verdict::Pending{ token }`.
      Do NOT record to the audit log yet (no decision has been made).
- `commit(&mut self, token: &ApprovalToken, decision: GateDecision, seq: u64) -> Result<Verdict, ServerError>`:
  - `let (_req, decision) = ledger.commit(token, decision)?;` (propagates `GateError` as `ServerError::Gate`).
  - Pull `(kind, from, targets)` from `pending.remove(token)` (present iff the ledger had it).
  - `record_decision(seq, kind, from, targets, Some(decision.clone()))?;`
  - Return `Proceed` if `decision.is_approved()` else `Denied{ reason }` (the deny reason).
- `pending_count(&self) -> usize` — `ledger.pending_count()`.
- `audit_entries(&self) -> &[AuditEntry]` — the recorded decision log (delegates to the `AuditLog`).

### `record_decision` (private helper)
Builds an `AuditEntry { seq, kind: EntryKind::Decision, actor: from, targets, summary, decision }` (a
secret-free `summary` such as `format!("gate {kind:?}")` or the request summary) and calls
`self.audit.record(entry)`, mapping `AuditError` → `ServerError::Audit(_)`. Because the audit log is
durability-first, a sink failure means the decision was made but not durably recorded → propagate the error.
The `decision` is `outcome.to_decision()` for the synchronous path (None for `Dismissed`) and
`Some(decision)` for the commit path.

## Test plan (`tests/happy.rs`, `tests/errors.rs`; 100% line+region). Real asserting `#[test]`s. Provide a
**fake Surfacer** (scriptable to return Approved / Denied{reason} / Dismissed, and asserts the mode it was
called with) and a **fake Sink** (records entries; scriptable to fail) in the tests.
**happy.rs**
- S1  `mode`: caps{elicitation:true} → Elicitation; {elicitation:false,marsad:true} → Marsad;
      {false,false} → PrepareCommit.
- S2  Tier-1 approve: caps elicitation, surfacer returns Approved → `Verdict::Proceed`; the audit log has one
      `Decision` entry with `actor==from`, `targets` matching, `decision==Some(Approve)`; the surfacer was
      called with `GateMode::Elicitation`.
- S3  Tier-1 deny: Approved→ replace with Denied{reason} → `Verdict::Denied{reason}` and audit
      `Some(Deny{reason})`.
- S4  Tier-1 dismiss: Dismissed → `Verdict::Dismissed` and an audit entry with `decision == None` (recorded
      but no decision).
- S5  Tier-3 (marsad) approve: caps{elicitation:false,marsad:true}, surfacer called with `GateMode::Marsad`,
      Approved → Proceed + audit Some(Approve).
- S6  Tier-2 prepare: caps{false,false} → `authorize` returns `Verdict::Pending{token}`, `pending_count()==1`,
      the surfacer was NOT called, and NO audit entry was recorded yet.
- S7  Tier-2 commit approve: after S6's prepare, `commit(token, Approve)` → `Verdict::Proceed`,
      `pending_count()==0`, audit has one `Some(Approve)` entry with the original kind/from/targets.
- S8  Tier-2 commit deny: `commit(token, Deny{reason})` → `Verdict::Denied{reason}` + audit `Some(Deny)`.
- S9  serde: `Verdict` round-trips for each variant (Proceed / Denied / Dismissed / Pending{token}).
**errors.rs**
- E1  Tier-1 audit failure: sink scripted to fail → `authorize` returns `Err(ServerError::Audit(_))` and the
      audit log stays empty (durability-first).
- E2  Tier-2 commit unknown token → `Err(ServerError::Gate(GateError::UnknownToken))`.
- E3  Tier-2 commit already-resolved: commit once (ok), commit the same token again →
      `Err(ServerError::Gate(GateError::AlreadyResolved))`.
- E4  Tier-2 commit audit failure: prepare ok, sink fails on commit → `Err(ServerError::Audit(_))`.
- E5  `ServerError` Display non-empty + distinct per variant; `From<GateError>` works;
      `std::error::Error` implemented.

Coverage: hit each mode; each synchronous outcome (approve/deny/dismiss); Tier-2 prepare + commit
approve/deny + both ledger errors; audit failure on both the sync and commit paths. Delete unreachable
branches rather than leaving them uncovered.
