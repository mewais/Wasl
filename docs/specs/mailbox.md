# WM — `wasl-mailbox` — spec (authoritative)

**Tier:** L0 (pure). **Deps:** `wasl-proto`, `serde` (+ `serde_json` dev). No IO, no `unsafe`.
Re-use `wasl_proto::{Envelope, EnvelopeKind, AgentId, Reply}`. Delivery IO is the daemon's job — this
crate builds the envelopes to deliver and correlates the replies to a multicast.

## 1. Fan-out (one send → many recipients)
```rust
/// Build one Envelope per recipient for a multicast/broadcast send. Each envelope's `to` is the SINGLE
/// recipient; `id = format!("{base_id}:{i}")` for recipient index i (stable, ordered); `from`/`kind`/`body`
/// shared. Empty `recipients` → empty vec.
pub fn fan_out(base_id: &str, from: &AgentId, kind: EnvelopeKind, body: &str, recipients: &[AgentId]) -> Vec<Envelope>;
```

## 2. Mailbox (per-owner inbox)
```rust
#[derive(Debug, Clone)]
pub struct Mailbox { /* private: owner + Vec<Envelope> */ }
```
- `new(owner: AgentId) -> Self`; `owner(&self) -> &AgentId`.
- `deliver(&mut self, env: Envelope)` — push to the inbox (FIFO).
- `len(&self) -> usize`; `is_empty(&self) -> bool`.
- `peek(&self) -> &[Envelope]` — inbox without draining.
- `drain(&mut self) -> Vec<Envelope>` — take + clear the inbox, preserving delivery order.

## 3. Correlation (multicast reply fan-in)
```rust
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum CorrelationError { UnexpectedReplier(AgentId), DuplicateReply(AgentId) }  // Display + std::error::Error

#[derive(Debug, Clone)]
pub struct Correlation { /* private */ }
```
- `new(expected: &[AgentId]) -> Self` — a pending multicast awaiting a reply from each expected peer
  (dedup the expected set, preserving first-appearance order).
- `record(&mut self, from: AgentId, body: String) -> Result<(), CorrelationError>`
  - replier not in `expected` → `Err(UnexpectedReplier(from))`;
  - already replied → `Err(DuplicateReply(from))`;
  - else record the `Reply { from, body }`.
- `is_complete(&self) -> bool` — every expected peer has replied.
- `pending(&self) -> Vec<AgentId>` — expected peers that haven't replied yet, in expected order.
- `replies(&self) -> Vec<Reply>` — collected replies, in the order recorded.

## Test plan (`tests/happy.rs`, `tests/errors.rs`; 100% line+region). Real asserting `#[test]`s.
**happy.rs**
- M1  `fan_out`: 3 recipients → 3 envelopes, each `to == [recipient]`, ids `base:0/base:1/base:2`, shared
      from/kind/body; empty recipients → empty vec; 1 recipient → 1 envelope (`base:0`).
- M2  `Mailbox`: deliver 3 → len 3, peek shows them in order, drain returns them in order + empties (len 0).
- M3  `Correlation`: expected [a,b]; record a then b → `is_complete` true; `replies` in record order;
      `pending` shrinks [a,b]→[b]→[] as replies arrive.
- M4  `Correlation::new` dedups expected (e.g. [a,a,b] → pending [a,b]); recording a once completes a.
- M5  serde: `Envelope`/`Reply` round-trip via wasl-proto (sanity that fan_out output serializes).
**errors.rs**
- E1  `record` from a peer not in expected → `Err(UnexpectedReplier(id))`; state unchanged (not complete).
- E2  `record` twice from the same peer → 2nd is `Err(DuplicateReply(id))`; only one reply stored.
- E3  `CorrelationError` Display non-empty + distinct per variant + carries the id; `std::error::Error` impl.

Coverage: hit fan-out empty/one/many, mailbox empty-drain, correlation complete/incomplete, both error
arms. Delete unreachable branches rather than leaving them uncovered.
