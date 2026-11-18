//! wasl-node — the agent-to-agent layer of one host daemon, embedded next to the host's
//! `wasit_runtime::SessionManager` and served under `wasl.*` by [`NodeService`].
//!
//! * **Topology** — live state pushed by a supervisor (`wasl.topology.set`, versioned,
//!   idempotent) and journaled; nothing is read from config files. A session with no outgoing
//!   edge has no peers.
//! * **Tasks** — asynchronous and durable: [`Node::delegate`] returns task ids at once;
//!   [`Node::wait`] long-polls on per-task watches without holding any lock; every state change
//!   is journaled (`<dir>/journal.ndjson`) and recovered after a daemon restart.
//! * **Handoff gates** — an `ask` edge raises a `GateRequest::Handoff` on the *sending*
//!   session through the runtime's external-gate facility, so it shows in that session's
//!   `pending_gates` and survives restarts; the answer (allow once / always allow / deny)
//!   arrives on the session's event stream. Without an attached supervisor the task waits for
//!   [`Node::decide`] from the MCP front-end (elicitation).
//! * **Routing** — same-host delivery is a direct call; other hosts are reached through a
//!   [`PeerTransport`] (by default [`UplinkTransport`] over the hub link), store-and-forward
//!   with retries and deadlines.
//! * **Delivery** — a task becomes a turn of the receiving session (provenance header +
//!   instruction), FIFO per session; progress and the result are derived from its events.
//! * **Safety** — causation chain and hop count on every task (cycles and depth rejected),
//!   per-task deadlines, optional per-workstream budget.
//! * **Events** — topics `wasl.tasks` / `wasl.edges` (cursor = journal seq, replayable) and an
//!   audit trail (`<dir>/audit.ndjson`).
//!
//! Extension points for later rounds (`spawn_agent`, `transfer`): new task modes and new
//! node-to-node methods on the same journal/pump; nothing is exposed to agents yet.

#![forbid(unsafe_code)]

mod delivery;
mod gate;
mod journal;
mod node;
mod pump;
mod service;
mod state;
mod transport;

pub use node::{Node, NodeConfig, MAX_WAIT};
pub use service::NodeService;
pub use transport::{PeerTransport, TransportError, UplinkTransport};
