# WG — `wasl-gate` — spec (authoritative)

**Tier:** L0/L1 (pure). **Deps:** `wasl-proto`, `serde` (+ `serde_json` dev). No IO, no `unsafe`.

The permission-before-send gate. All types `#[derive(Debug, Clone, PartialEq, Eq)]` (+ serde where noted).
Re-use `wasl_proto::{GateRequest, GateKind, GateDecision, AgentId, ElicitationCreate, ElicitationResponse,
ElicitationAction, ApprovalToken}`.

## 1. Client capabilities + mode
```rust
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
pub struct ClientCaps {
    pub elicitation: bool,   // the calling MCP client supports server-initiated elicitation
    pub external: bool,        // a external authority is attached (can surface + observe)
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum GateMode { Elicitation, external authority, PrepareCommit }
```
`pub fn choose_mode(caps: &ClientCaps) -> GateMode` — **priority: elicitation > external > prepare_commit.**
- `elicitation` true → `Elicitation` (clean in-band path; Claude/Codex).
- else `external` true → `external authority` (no client elicitation but external authority present, e.g. Gemini+external authority — surface
  direct to external authority).
- else → `PrepareCommit` (self-owned handshake; standalone Gemini / any no-elicitation client).

## 2. Building the human-facing gate + elicitation
```rust
/// Build a secret-free gate request. `preview` is the caller's already-sanitized content preview.
pub fn build_request(id: String, kind: GateKind, from: AgentId, targets: &[String], preview: String) -> GateRequest;
```
- `summary` is derived: `"{verb} {N} peer(s)"` where verb = consult→"consult", command→"command",
  message→"message to", transfer→"transfer to"; N = `targets.len()`. (e.g. `"message to 2 peer(s)"`.)
- `preview` is stored verbatim (caller guarantees no secrets — the MCP elicitation rule).

```rust
/// The message-only elicitation to send for this gate (empty schema at the MCP layer).
pub fn elicitation_for(req: &GateRequest) -> ElicitationCreate;
```
- `message` = `"{summary}\n{preview}\n\nApprove?"` (human approval prompt; contains the summary + preview
  from the request, never anything else).

## 3. Elicitation → outcome
```rust
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum GateOutcome {
    Approved,
    Denied { reason: String },
    Dismissed,   // Cancel — neither approve nor deny; caller retries/aborts
}
```
`pub fn outcome_of(resp: &ElicitationResponse) -> GateOutcome`
- Accept → `Approved`; Decline → `Denied { reason: "declined" }`; Cancel → `Dismissed`.
`GateOutcome::to_decision(&self) -> Option<GateDecision>`
- `Approved → Some(Approve)`; `Denied{reason} → Some(Deny{reason})`; `Dismissed → None`.

## 4. Prepare→commit ledger (Tier-2, self-owned; also used to hold ANY pending gate awaiting a decision)
```rust
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum GateError { UnknownToken, AlreadyResolved }   // impl Display + std::error::Error

#[derive(Debug, Clone, Default)]
pub struct GateLedger { /* private */ }
```
- `GateLedger::new() -> Self` (also `Default`).
- `prepare(&mut self, token: ApprovalToken, req: GateRequest)` — record a pending gate under `token`.
  A duplicate token overwrites (re-prepare) — no error.
- `is_pending(&self, token: &ApprovalToken) -> bool`.
- `pending_count(&self) -> usize`.
- `peek(&self, token: &ApprovalToken) -> Option<&GateRequest>` — the pending request, if any.
- `commit(&mut self, token: &ApprovalToken, decision: GateDecision) -> Result<(GateRequest, GateDecision), GateError>`
  - unknown token → `Err(UnknownToken)`; a token already committed → `Err(AlreadyResolved)`; else remove
    from pending, return the `(request, decision)` pair. (After a successful commit the token is no longer
    pending and re-committing it → `AlreadyResolved`, NOT `UnknownToken` — the ledger remembers resolved
    tokens.)

## Test plan (test-agent: `tests/happy.rs`, `tests/errors.rs`; 100% line+region). Real asserting `#[test]`s.

**happy.rs**
- G1  `choose_mode`: `{elicitation:true, external:false}`→Elicitation; `{true,true}`→Elicitation (elicitation
      wins); `{false,true}`→external authority; `{false,false}`→PrepareCommit. `GateMode` serde snake_case round-trips.
- G2  `ClientCaps` serde round-trips.
- G3  `build_request`: summary verb+count for each `GateKind` (consult/command/message/transfer) with 1 and
      3 targets; preview stored verbatim; id/kind/from carried through.
- G4  `elicitation_for`: message contains the summary and the preview and ends with the approval prompt.
- G5  `outcome_of`: Accept→Approved; Decline→Denied("declined"); Cancel→Dismissed.
- G6  `GateOutcome::to_decision`: Approved→Some(Approve); Denied→Some(Deny{reason}); Dismissed→None.
- G7  ledger happy path: `prepare` then `is_pending` true, `pending_count` 1, `peek` returns the request;
      `commit` with Approve returns `(request, Approve)`, then `is_pending` false, `pending_count` 0.
- G8  ledger: re-`prepare` same token overwrites (no error, count stays 1, peek shows the new request).
- G9  ledger: two distinct tokens tracked independently; committing one leaves the other pending.

**errors.rs**
- E1  `commit` unknown token → `Err(UnknownToken)`.
- E2  `commit` a token, then `commit` it again → `Err(AlreadyResolved)` (not UnknownToken).
- E3  `commit` with a Deny decision returns `(request, Deny{reason})` and clears pending.
- E4  `GateError` Display non-empty + distinct per variant; `std::error::Error` implemented.

Coverage: hit every mode branch, every outcome/decision arm, and both ledger error paths. Delete
unreachable branches rather than leaving them uncovered.
