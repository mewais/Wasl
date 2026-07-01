//! wasl-a2a-types — faithful A2A (Agent2Agent) wire types (pure serde).
//!
//! A2A is JSON-RPC 2.0 over HTTP with camelCase fields. This crate models the
//! subset Wasl uses: Agent Card, Message + Part (text/file/data), Task + status
//! lifecycle, Artifact, the JSON-RPC params/results for message/send,
//! message/stream, tasks/get, tasks/cancel, and the SSE update events. No IO.
//! See `docs/specs/a2a-types.md`.
