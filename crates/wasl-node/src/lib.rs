//! wasl-node — the per-host node: the send/receive unit that composes the tested cores.
//!
//! SEND: `Node::{consult,command,message}` run the permission gate (`wasl_server::Gatekeeper`,
//! recording every decision to the audit `Sink`) and, ONLY on approval, dispatch to peers over A2A
//! (`wasl_dispatch`), returning the replies. RECEIVE: `Node::receiver` builds the A2A server
//! (`wasl_a2a_server`) backed by an `AgentAdapter` (`wasl_adapter`) that injects into the local agent.
//! No new decision/routing logic — pure composition. See `docs/specs/node.md`.
