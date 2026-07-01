//! wasl-marsad — optional Tier-3 Marsad connector (WC).
//!
//! When the gate mode is `Marsad` (or as an explicit fallback), a HITL gate is
//! surfaced to Marsad, which returns the human decision; auto-approved
//! interactions are still notified; and every interaction feeds an observation
//! stream Marsad can render. Pure logic over a mockable `MarsadLink` trait — the
//! real HTTP/IPC link lives in the daemon/server. See `docs/specs/marsad-connector.md`.
