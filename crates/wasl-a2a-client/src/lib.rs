//! wasl-a2a-client — the A2A client: the SEND side of a node.
//!
//! A reqwest-backed client that POSTs A2A JSON-RPC (message/send, message/stream
//! SSE, tasks/get, tasks/cancel) to a peer node's A2A endpoint and parses the
//! Task / AgentCard / stream events. Every method is a request/response with no
//! un-coverable shell, so this crate is unit-tested to 100% against a real
//! loopback `wasl-a2a-server`. See `docs/specs/a2a-client.md`.
