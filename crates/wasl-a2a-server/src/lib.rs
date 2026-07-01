//! wasl-a2a-server — the A2A (Agent2Agent) HTTP server: the RECEIVE side of a node.
//!
//! An axum JSON-RPC 2.0 endpoint that accepts message/send + message/stream (SSE)
//! + tasks/get + tasks/cancel, manages the Task lifecycle, and runs each incoming
//! message through a `TaskExecutor` (the seam that drives the local agent). The
//! request-handling core is unit-tested to 100%; the axum bind/serve loop is the
//! IO shell (loopback-tested). See `docs/specs/a2a-server.md`.
