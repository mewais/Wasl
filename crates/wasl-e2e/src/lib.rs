//! wasl-e2e — cross-crate scenario suite (no library surface of its own). The
//! real content is in `tests/`, which composes the three-tier GATE spine —
//! `Gatekeeper` (wasl-server) over a `Surfacer` + audit `Sink`, covering Tier-1
//! elicitation, Tier-2 prepare/commit, and Tier-3 external-authority — and, only
//! on `Verdict::Proceed`, dispatch (`wasl-dispatch`) + audit over a REAL loopback
//! `wasl-a2a-server`. Proves the gate tiers × verbs matrix agrees with the
//! recorded decision AND that the decision actually reaches the live A2A send
//! path. See `docs/specs/e2e.md`.
//!
//! NOTE (v2, post-W9): the dispatch half now runs over the LIVE A2A path
//! (`wasl-dispatch` → a real axum `wasl_a2a_server` on `127.0.0.1`, driven by a
//! real `wasl_adapter::AdapterExecutor` wrapping a fake innermost `AgentAdapter`
//! — mirroring `wasl-node`'s two-node e2e and `wasl-dispatch`'s loopback suite).
//! The legacy `wasl-daemon`/`wasl-mailbox` sync `Mesh` stack has been retired and
//! deleted; see `AGENTS.md` for the old→new scenario mapping.
