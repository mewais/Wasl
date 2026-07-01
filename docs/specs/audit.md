# WA — `wasl-audit` — spec (authoritative)

**Tier:** L1 (pure query + trait-mocked sink). **Deps:** `wasl-proto`, `serde` (+ `serde_json` dev). No
`unsafe`. The ONLY IO is behind the `Sink` trait — unit tests use a fake; real durable IO is the daemon's.
Re-use `wasl_proto::{AgentId, GateDecision}`. Entries carry a caller-provided `seq` (deterministic — no
clock in this crate).

## Types
```rust
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "lowercase")]
pub enum EntryKind { Consult, Command, Message, Transfer, Decision }

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct AuditEntry {
    pub seq: u64,                         // caller-provided monotonic sequence (deterministic ordering)
    pub kind: EntryKind,
    pub actor: AgentId,                   // who initiated
    pub targets: Vec<AgentId>,            // recipients (one, or many for multicast)
    pub summary: String,                  // human-readable, secret-free
    #[serde(default)]
    pub decision: Option<GateDecision>,   // Some(_) when this entry records a gate decision
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub enum AuditError { Sink(String) }      // impl Display + std::error::Error
```

## Sink trait (append-only durability)
```rust
pub trait Sink {
    fn append(&mut self, entry: &AuditEntry) -> Result<(), AuditError>;
}
```

## AuditLog
```rust
pub struct AuditLog<S: Sink> { /* private: sink + in-memory Vec<AuditEntry> cache */ }
```
- `new(sink: S) -> Self`.
- `record(&mut self, entry: AuditEntry) -> Result<(), AuditError>` — append to the sink FIRST; on
  `Ok`, push to the in-memory cache; on `Err`, propagate and do NOT cache (the entry isn't durable).
- `entries(&self) -> &[AuditEntry]` — the cache, in record order.
- `len(&self) -> usize`; `is_empty(&self) -> bool`.
- `by_actor(&self, actor: &AgentId) -> Vec<&AuditEntry>` — entries whose `actor == actor`, in record order.
- `by_kind(&self, kind: EntryKind) -> Vec<&AuditEntry>` — entries of that kind, in record order.
- `decisions(&self) -> Vec<&AuditEntry>` — entries where `decision.is_some()`, in record order.

## Test plan (`tests/happy.rs`, `tests/errors.rs`; 100% line+region). Real asserting `#[test]`s. Provide a
**fake Sink** in the tests that records appended entries and can be scripted to fail.
**happy.rs**
- A1  `record` appends to the sink AND caches (entries() reflects it, in order); `len`/`is_empty`.
- A2  `by_actor`: only that actor's entries, in record order.
- A3  `by_kind`: only that kind (test at least Consult + Transfer + Decision).
- A4  `decisions`: only entries carrying a `Some(decision)`; assert both an Approve and a Deny surface.
- A5  serde: `AuditEntry` round-trips incl. `decision: None` default and `Some(Approve)`/`Some(Deny)`;
      `EntryKind` lowercase round-trip.
**errors.rs**
- E1  `record` when the sink `append` fails → `Err(AuditError::Sink(_))` AND the entry is NOT cached
      (entries() unchanged / len not incremented) — durability-first invariant.
- E2  `AuditError` Display non-empty + carries the sink detail; `std::error::Error` implemented.

Coverage: hit record ok/err, each filter (by_actor/by_kind/decisions) with matching + non-matching
entries, the None/Some decision serde. Delete unreachable branches rather than leaving them uncovered.
