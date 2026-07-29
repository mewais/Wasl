//! Hardening-4 §4.5 — DOCUMENTED TEST GAP (intentional, for this phase).
//!
//! This file contains NO runnable test. It documents, per the tester's charter,
//! exactly what §4.5 needs tested, why it cannot be tested black-box from THIS
//! crate's integration suite, and who must write the test that does cover it. This
//! is a legitimate, explicitly-called-for documented gap — not a stub, not a
//! skipped test, and not a failure on the tester's part.
//!
//! ## The bug (hardening-plan §4.5, confirmed against the real source)
//!
//! `crates/wasl-runtime-protocol/src/opencode_acp.rs`'s `OpenCodeAcpTransport`
//! carries a transport-level reply accumulator `accumulated_reply: String`
//! (`opencode_acp.rs:297`). It exists because opencode's own `session/prompt`
//! response carries NO reply text (see the Phase-5 accumulation-architecture
//! design), so `next_event` accumulates every `AssistantDelta`:
//!
//! ```ignore
//! Some(ProtocolEvent::AssistantDelta { text }) => {
//!     self.accumulated_reply.push_str(&text);   // opencode_acp.rs:495 — NO CAP
//!     ...
//! }
//! Some(ProtocolEvent::TurnCompleted { .. }) => {
//!     let ev = ProtocolEvent::TurnCompleted {
//!         reply: std::mem::take(&mut self.accumulated_reply),  // :502
//!     };
//!     ...
//! }
//! ```
//!
//! The `push_str` at `opencode_acp.rs:495` has NO size cap of its own. The
//! actor's `actor.rs::cap_delta` only truncates the PUBLIC event AFTER the fact,
//! so the transport-level accumulator can balloon FIRST — a long-running turn (or
//! a runaway/malfunctioning opencode emitting a huge stream of chunks) grows
//! `accumulated_reply` without bound before any downstream cap ever applies.
//!
//! ## What must be tested (the coder's `#[cfg(test)]` unit test to write)
//!
//! A transport-level BOUNDED accumulator: `accumulated_reply` must stop growing /
//! truncate early once it reaches a configured byte cap, mirroring
//! `actor.rs::cap_delta`'s discipline at the TRANSPORT level rather than relying
//! solely on the actor's downstream cap. Concretely, feeding a sequence of
//! `AssistantDelta`s whose total length exceeds the cap must leave the
//! accumulator (and the reply carried by the resulting `TurnCompleted`) bounded at
//! or below that cap — never the full unbounded concatenation.
//!
//! ## Why this CANNOT be a black-box test in THIS crate's integration suite
//!
//! Driving `OpenCodeAcpProtocol` end-to-end requires a REAL `opencode` CLI
//! speaking ACP JSON-RPC over stdio. There is no live opencode binary /
//! credentials / `--features live-cli` execution available in this sandbox (the
//! Phase-5 landing notes confirm the driver's supervise/handshake/accumulate
//! plumbing is only meaningfully exercised by DEFERRED live-cli runs). The
//! `wasl-fake-agent` fixture speaks the `FakeLineProtocol` line wire, NOT the ACP
//! JSON-RPC wire, so it cannot drive `OpenCodeAcpTransport` at all — there is no
//! in-repo real-process substrate for the opencode driver the way the fake agent
//! is for `FakeLineProtocol`.
//!
//! The accumulator is furthermore DRIVER-INTERNAL transport state: `next_event`'s
//! `push_str`/`mem::take` accumulation is not reachable through any public
//! `wasl-runtime-protocol` seam without a live process feeding real
//! `agent_message_chunk` lines. `opencode_acp.rs` already establishes the crate's
//! convention for pinning exactly this kind of driver-internal logic WITHOUT a
//! live process: `#[cfg(test)]` unit tests inside `src/opencode_acp.rs`
//! (`opencode_acp.rs:785`), the same pattern used to pin byte-exact `map_acp_line`
//! mapping fixtures.
//!
//! ## Who writes it, and why the tester cannot
//!
//! The bounded-accumulator unit test belongs INSIDE `src/opencode_acp.rs`'s
//! `#[cfg(test)] mod tests`, next to the existing mapping-fixture unit tests —
//! which is `src/**`, off-limits to the test-agent per this crate's `AGENTS.md`
//! ("test-agent: write ONLY `crates/wasl-runtime-protocol/tests/**`"; the ONE
//! `src/**` file the tester may touch is `src/bin/wasl-fake-agent.rs`, the fixture
//! binary, which is irrelevant to the ACP driver). The coder therefore writes this
//! `#[cfg(test)]` unit test alongside the §4.5 fix, exactly as they already own the
//! `next_wait`/`map_acp_line` unit tests for this same file. This is normal and
//! expected for driver-internal accumulator logic per this crate's conventions.
//!
//! This gap is scoped to the opencode driver ONLY. `FakeLineProtocol` does not
//! accumulate a reply (its `done` carries the reply directly), and
//! `ClaudeStreamProtocol` likewise carries whole assistant blocks — the
//! unbounded-accumulator concern is specific to opencode's no-reply-in-response
//! wire, so there is nothing to test here for the other two drivers.
