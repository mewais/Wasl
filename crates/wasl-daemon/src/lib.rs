//! wasl-daemon — the per-host daemon routing core (WD).
//!
//! Resolves peers against the registry and dispatches an already-gated tool call:
//! consult/command/message fan out one envelope per peer (multicast) and collect
//! replies; transfer delegates to the transfer tool. Local vs cross-host delivery
//! is decided here and performed behind the `Mesh` trait (`wasl_transfer::Runner`
//! for transfers) — the real socket event loop is a later thin binary shell.
//! See `docs/specs/daemon.md`.
