//! wasl-runtime-protocol — R2 — concrete interactive drivers (Phase-1 skeleton).
//!
//! This crate is the **one place** CLI-specific / ACP-specific / opencode-specific
//! wire knowledge is allowed. It provides the concrete [`InteractiveProtocol`]
//! drivers that map each substrate's native wire shape onto R1's generic
//! [`ProtocolEvent`]. **It depends on R1 and never the reverse** (the plan's
//! cycle-fix: the driver-seam traits + the internal [`ProtocolEvent`] enum live
//! in R1; only concrete drivers live here).
//!
//! ## Phase-2B scope (THIS phase) — `FakeLineProtocol`
//!
//! Phase-2B lands the first concrete driver: [`FakeLineProtocol`] over the
//! long-lived in-repo fake structured agent (the `wasl-fake-agent` dev-bin). It
//! spawns the fake as a REAL OS child, speaks its line-oriented-JSON wire, maps
//! each event to R1's [`ProtocolEvent`], and owns the §1.4 process supervision
//! (concurrent stderr drain, hang watchdog, bounded interrupt→TERM→KILL
//! escalation, the M2 `BrokenPipe`/`WriteZero`-is-benign stdin fix, reaping on
//! drop). The remaining concrete drivers (`ClaudeStreamProtocol` (Phase 4),
//! `OpenCodeAcpProtocol` / `OpenCodeServeProtocol` (Phase 5)) land later.
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

pub use claude_stream::{map_claude_line, ClaudeStreamProtocol, ClaudeToolTracker};
pub use codex_appserver::{map_codex_line, CodexAppServerProtocol, CodexToolTracker};
pub use fake_line::{FakeLineProtocol, SupervisionTimeouts};
pub use json_rpc_handshake::{
    classify_handshake_line, classify_handshake_line as classify_codex_handshake_line,
    HandshakeLineClass, HandshakeLineClass as CodexHandshakeLineClass,
};
pub use opencode_acp::{map_acp_line_correlated, OpenCodeAcpProtocol};
pub use process_supervision::next_wait;

// The R1-owned generic driver contract every R2 driver implements/maps onto.
// Re-surfaced here to make the R2 → R1 edge explicit.
pub use wasl_runtime::{
    ComposedPrompt, InteractiveProtocol, InteractiveTransport, NativeSessionId, ProtocolEvent,
    RuntimeCapabilities, RuntimeError, SpawnOptions,
};
