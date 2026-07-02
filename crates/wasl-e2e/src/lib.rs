//! wasl-e2e — cross-crate scenario suite (no library surface of its own). The
//! real content is in `tests/`, which composes the three-tier GATE spine —
//! `Gatekeeper` (wasl-server) over a `Surfacer` + audit `Sink`, covering Tier-1
//! elicitation, Tier-2 prepare/commit, and Tier-3 external-authority — and, only
//! on `Verdict::Proceed`, dispatch + audit. Proves the gate tiers × verbs matrix
//! agrees with the recorded decision. See `docs/specs/e2e.md`.
//!
//! NOTE (v2): the dispatch half here still runs over the **legacy** `wasl-daemon`
//! `Mesh` seam (driven by a `FakeMesh`), which is NOT the live path. The live
//! send/receive path is A2A (`wasl-dispatch` + `wasl-node`), and its end-to-end
//! composition over REAL loopback transport is proven by `wasl-node`'s two-node
//! e2e and `wasl-mcp`'s loopback heart tests. This suite is retained for its
//! three-tier-gate coverage; `wasl-daemon` remains only as its dispatch backend.
