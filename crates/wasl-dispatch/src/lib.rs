//! wasl-dispatch — the send-side fan-out: resolve peers and dispatch a gated
//! consult/command/message to them over A2A, concurrently, collecting replies.
//!
//! For each target peer it looks up the peer's A2A endpoint in the registry,
//! builds an A2A `Message` carrying the Wasl verb + sender in `metadata.wasl`
//! (the convention `wasl-adapter` reads on the receive side), sends it with the
//! A2A client, and extracts the reply from the returned Task. Unknown peers and
//! per-peer failures are partitioned into a report (partial multicast never
//! aborts the rest). Async; the A2A HTTP is the client's concern.
//! See `docs/specs/dispatch.md`.
