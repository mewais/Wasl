//! wasl-proto — Wasl wire + domain types (WP).
//!
//! Pure, serde-only types for the Wasl agent-to-agent MCP layer: agent cards,
//! the four tool params/results (consult / command / message / transfer, each
//! supporting one-or-many peers for multicast), reply envelopes, the gate
//! request/decision/token types, and the MCP `elicitation/create` wire shapes.
//!
//! See `docs/specs/proto.md` for the authoritative contract. Placeholder; the
//! code-agent implements the public API defined there.
