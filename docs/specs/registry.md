# WR — `wasl-registry` — spec (authoritative)

**Tier:** L0/L1 (pure). **Deps:** `wasl-proto`, `serde` (+ `serde_json` dev). No IO, no `unsafe`.
Re-use `wasl_proto::{AgentCard, AgentId, HostId}`. Mesh IO (delivering to a remote daemon) is the daemon
block; this crate only tracks peers and resolves whether a target is local or remote.

## Types
```rust
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum Route { Local, Remote }         // Remote carries the host via the resolve return (see below)

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case", tag = "route")]
pub enum Resolved {
    Local { peer: AgentId },
    Remote { peer: AgentId, host: HostId },
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub enum RegistryError { UnknownPeer(AgentId) }   // impl Display + std::error::Error

#[derive(Debug, Clone, Default)]
pub struct PeerRegistry { /* private */ }
```

## PeerRegistry (pure store, keyed by AgentId)
- `new() -> Self` (+ `Default`).
- `register(&mut self, card: AgentCard)` — insert/overwrite by `card.id`.
- `unregister(&mut self, id: &AgentId) -> bool` — remove; returns whether it was present.
- `get(&self, id: &AgentId) -> Option<&AgentCard>`.
- `contains(&self, id: &AgentId) -> bool`.
- `len(&self) -> usize`; `is_empty(&self) -> bool`.
- `list(&self) -> Vec<&AgentCard>` — all cards, **sorted by `id` ascending** (deterministic).
- `by_host(&self, host: &HostId) -> Vec<&AgentCard>` — cards whose `host == host`, sorted by id.

## Route resolution
`pub fn resolve(&self, self_host: &HostId, target: &AgentId) -> Result<Resolved, RegistryError>`
- Unknown target → `Err(RegistryError::UnknownPeer(target.clone()))`.
- Target's `host == self_host` → `Ok(Resolved::Local { peer })`.
- Else → `Ok(Resolved::Remote { peer, host })` (the target's host).

`pub fn resolve_many(&self, self_host: &HostId, targets: &[AgentId]) -> (Vec<Resolved>, Vec<AgentId>)`
- Resolve each; return `(resolved_ok, unknown_targets)` preserving input order. (Used for multicast: some
  peers resolvable, some unknown — the caller decides how to handle partial failure.)

## Test plan (`tests/happy.rs`, `tests/errors.rs`; 100% line+region). Real asserting `#[test]`s.
**happy.rs**
- R1  register + get + contains + len/is_empty; register same id overwrites (len unchanged, new card returned).
- R2  unregister present → true and gone; unregister absent → false.
- R3  `list` sorted by id ascending regardless of insertion order.
- R4  `by_host` returns only that host's cards, sorted; empty for an unknown host.
- R5  `resolve` local: target on `self_host` → `Resolved::Local{peer}`.
- R6  `resolve` remote: target on another host → `Resolved::Remote{peer,host}` with the right host.
- R7  `resolve_many`: mix of local+remote+unknown → resolved list (order preserved) + unknown list (order preserved).
- R8  serde: `Resolved` round-trips (`Local`/`Remote` tagged snake_case); `Route` round-trips.
**errors.rs**
- E1  `resolve` unknown target → `Err(UnknownPeer(id))` with the right id.
- E2  `RegistryError` Display non-empty + mentions the peer id; `std::error::Error` implemented.

Coverage: hit every branch (overwrite, present/absent unregister, local/remote/unknown resolve, partial
multicast). Delete unreachable branches rather than leaving them uncovered.
