//! wasl-runtime-protocol — R2 — concrete interactive drivers.
//!
//! This crate is the **one place** CLI-specific / ACP-specific / opencode-specific /
//! codex-specific wire knowledge is allowed. It provides the concrete
//! [`InteractiveProtocol`] drivers that map each substrate's native wire shape
//! onto R1's generic [`ProtocolEvent`]. **It depends on R1 and never the
//! reverse** (the driver-seam traits + the internal [`ProtocolEvent`] enum
//! live in R1; only concrete drivers live here).
//!
//! ## Drivers
//!
//! - [`FakeLineProtocol`] — the long-lived in-repo fake structured agent (the
//!   `wasl-fake-agent` dev-bin) over a line-oriented-JSON wire. The
//!   deterministic substrate the process/framing/supervision plumbing below
//!   is proven against; no real CLI, no network, no cost.
//! - [`ClaudeStreamProtocol`] — the real `claude` CLI over its
//!   `stream-json` duplex (a raw NDJSON stream with no request/response
//!   envelope).
//! - [`OpenCodeAcpProtocol`] — the real `opencode` CLI over its `acp`
//!   JSON-RPC-over-stdio interface (the primary managed driver: the only
//!   opencode substrate with real streaming deltas).
//! - [`CodexAppServerProtocol`] — the real `codex` CLI over its
//!   `app-server --stdio` JSON-RPC-over-stdio interface (the same wire
//!   *family* as opencode's ACP — request/response envelopes plus async
//!   notifications — but a different vendor's CLI with its own message
//!   shapes).
//!
//! Every real-CLI driver owns the same process-IO-shell discipline: a
//! concurrent stderr drain, a hang watchdog, bounded interrupt→TERM→KILL
//! escalation, benign `BrokenPipe`/`WriteZero` stdin handling, and
//! reap-on-drop. The `opencode`/`codex` drivers additionally share their
//! bounded-line-reading and JSON-RPC-handshake-classification logic (see
//! `bounded_io`/`process_supervision`/`json_rpc_handshake`, single
//! implementations, not per-driver copies).
//!
//! See `AGENTS.md` for this crate's landing history (per-driver capability
//! reports, VERIFIED/ASSUMED wire-shape provenance, and coverage honesty) and
//! `docs/specs/runtime-protocol.md` for the authoritative wire-shape spec.
//!
//! The re-export below makes the R1-owned generic contract these drivers
//! implement / map onto explicit at the crate root.
//!
//! [`InteractiveProtocol`]: wasl_runtime::InteractiveProtocol
//! [`ProtocolEvent`]: wasl_runtime::ProtocolEvent

#![forbid(unsafe_code)]

mod bounded_io;
mod claude_stream;
mod codex_appserver;
mod fake_line;
mod json_rpc_handshake;
mod opencode_acp;
mod process_supervision;

pub use claude_stream::{claude_argv, map_claude_line, ClaudeStreamProtocol, ClaudeToolTracker};
pub use codex_appserver::{map_codex_line, CodexAppServerProtocol, CodexToolTracker};
pub use fake_line::{FakeLineProtocol, SupervisionTimeouts};
pub use json_rpc_handshake::{
    classify_handshake_line, classify_handshake_line as classify_codex_handshake_line,
    HandshakeLineClass, HandshakeLineClass as CodexHandshakeLineClass,
};
pub use opencode_acp::{map_acp_line_correlated, opencode_session_params, OpenCodeAcpProtocol};
pub use process_supervision::next_wait;

// Wasit re-exports so callers can use SpawnSpec / AgentEvent via this crate.
pub use wasit_claude;
pub use wasit_codex;
pub use wasit_core;
pub use wasit_opencode;

// The R1-owned generic driver contract every R2 driver implements/maps onto.
// Re-surfaced here to make the R2 → R1 edge explicit.
pub use wasl_runtime::{
    ComposedPrompt, InteractiveProtocol, InteractiveTransport, NativeSessionId, ProtocolEvent,
    RuntimeCapabilities, RuntimeError, SpawnContext, SpawnOptions,
};
