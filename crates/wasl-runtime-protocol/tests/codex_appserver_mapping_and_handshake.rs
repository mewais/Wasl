//! Phase-6-equivalent (codex) — the test-agent's RED spec for the NEW
//! `CodexAppServerProtocol` driver's PURE wire-mapping surface: a
//! `map_codex_line`-equivalent per-line map, a `CodexToolTracker`-equivalent
//! stateful id→command correlator, a `next_wait` in-flight wait policy, and a
//! `classify_codex_handshake_line` handshake classifier. This mirrors, as
//! closely as the codex wire allows, the two precedent RED spec files:
//!   * `tests/opencode_handshake_and_correlation_hardening.rs` (the
//!     `classify_handshake_line`/`HandshakeLineClass` + `map_acp_line_correlated`
//!     precedent), and
//!   * `tests/claude_tool_call_mapping_hardening.rs` (the pure `map_claude_line`
//!     + stateful `ClaudeToolTracker` id→name split precedent).
//!
//! ==========================================================================
//! ⚠️⚠️  THIS FILE DOES NOT COMPILE AGAINST THE CURRENT src/ (NO codex driver) ⚠️⚠️
//! ==========================================================================
//!
//! It CANNOT, and that is intentional and unavoidable — the codex driver does
//! not exist yet. `crates/wasl-runtime-protocol/src/` today has `claude_stream`,
//! `fake_line`, and `opencode_acp`; there is NO `codex_appserver` module, and
//! `src/lib.rs` re-exports nothing codex-related. Per this crate's `AGENTS.md`
//! the test-agent may write ONLY `tests/**`; `tests/**` cannot see private
//! `src/**` items (verified precedent: `classify_handshake_line`,
//! `map_acp_line_correlated`, `map_claude_line`, `ClaudeToolTracker` all had to
//! be `pub` + crate-root re-exported before the two precedent files above could
//! compile). So the ONLY way to prove the codex mapping from a `tests/**` file
//! is to have the coder CREATE and EXPOSE the entrypoints below.
//!
//! ==========================================================================
//! GROUND TRUTH — every fixture below is copied VERBATIM from the real captured
//! codex `app-server --stdio` wire in this worktree:
//!   * `spikes/live-substrate/out/codex_appserver/events.jsonl` (196 lines, the
//!     full P/C/B proof: initialize response id=1, thread/start response id=2 +
//!     `thread/started` notification, two turns, and one tool-call turn), and
//!   * `spikes/live-substrate/out/codex_appserver/codex_toolcall_capture.jsonl`
//!     (156 lines, the tool-call turn slice — same `commandExecution` item as
//!     events.jsonl lines 118/119).
//! Nothing here is invented; each fixture carries its source line number.
//!
//! ==========================================================================
//! EXACT NEW API SURFACE THE CODER MUST CREATE (so this file compiles + the
//! tests below flip from RED to GREEN). Names/shapes are chosen from what the
//! real captured wire actually does + the claude/opencode precedents. If the
//! coder picks different names, THIS file's `use` block + the references below
//! are the single re-map point.
//! ==========================================================================
//!
//! ```ignore
//! // NEW module: crates/wasl-runtime-protocol/src/codex_appserver.rs
//! // Re-exported at the crate root (add to src/lib.rs, mirroring the existing
//! //   `pub use claude_stream::{map_claude_line, ClaudeStreamProtocol, ClaudeToolTracker};`
//! //   and `pub use opencode_acp::{classify_handshake_line, map_acp_line_correlated,
//! //    HandshakeLineClass, OpenCodeAcpProtocol};` lines):
//! //
//! //   mod codex_appserver;
//! //   pub use codex_appserver::{
//! //       map_codex_line, CodexToolTracker, classify_codex_handshake_line,
//! //       CodexHandshakeLineClass, CodexAppServerProtocol,
//! //   };
//!
//! /// PURE per-line map of one codex app-server NDJSON line (no cross-line
//! /// state). Mirrors `map_claude_line`'s pure/stateless contract. Maps:
//! ///   * `thread/started` NOTIFICATION -> SessionReady{ native: Some(thread.id) }
//! ///   * `item/agentMessage/delta`      -> AssistantDelta{ text: params.delta }
//! ///   * `turn/completed` status=completed  -> TurnCompleted{ reply } where
//! ///        `reply` is preferred from the FINAL agentMessage item's `text` in
//! ///        `turn.items` (see the reply-source note in the tests — the real
//! ///        `turn/completed` carries the full final text in
//! ///        `turn.items[..].text`, `itemsView:"summary"`), falling back to ""
//! ///        when no agentMessage item is present.
//! ///   * `turn/completed` status=failed     -> TurnFailed{ error: turn.error.message }
//! ///   * `turn/completed` status=interrupted -> TurnFailed{ error: <fixed marker> }
//! ///        (there is NO ProtocolEvent::TurnCancelled in R1's vocabulary —
//! ///        verified against wasl-runtime/src/lib.rs — so interrupted maps to
//! ///        TurnFailed, mirroring how claude/opencode reuse the existing
//! ///        terminal vocabulary; see the interrupted test's note).
//! ///   * an `item/started`/`item/completed` for a `commandExecution` item, seen
//! ///        by the PURE map ALONE, maps `item/started` -> ToolStarted{ name }
//! ///        (name = the item's `command` string) but `item/completed` ->
//! ///        vec![] at the pure layer (ok/name correlation is the tracker's job
//! ///        — mirrors claude's pure-`tool_result`-is-vec![] discipline).
//! ///   * every noise line (`remoteControl/status/changed`, `warning`,
//! ///        `thread/status/changed`, `item/reasoning/summaryTextDelta`,
//! ///        reasoning/userMessage items, id-correlated turn/start acks, blank/
//! ///        malformed) -> vec![].
//! pub fn map_codex_line(line: &str) -> Vec<ProtocolEvent>;
//!
//! /// STATEFUL per-turn correlator for codex `commandExecution` items, the
//! /// codex counterpart of `ClaudeToolTracker`. codex's `item/completed` does
//! /// carry `command` in THIS capture, but correlation is done BY ID (the
//! /// item's `id`, present on BOTH `item/started` and `item/completed`) — the
//! /// same robust id→something discipline claude uses for a tool_result that
//! /// (unlike codex's) never re-echoes the name. The transport owns one of
//! /// these and feeds EVERY stdout line through `observe_line`.
//! #[derive(Debug, Default)]
//! pub struct CodexToolTracker { /* private: id -> command(name) map */ }
//!
//! impl CodexToolTracker {
//!     pub fn new() -> Self;
//!     /// Map ONE stdout line, threading commandExecution id→name state:
//!     ///   * commandExecution `item/started`: record `id -> command`, THEN emit
//!     ///     the same `ToolStarted{name}` the pure map emits.
//!     ///   * commandExecution `item/completed`: recover the name for this
//!     ///     item's `id` (from the recorded map), emit
//!     ///     `ToolFinished{ name, ok: exitCode == 0 }`; if the id was never
//!     ///     recorded, emit vec![] (ignored-not-misrouted, mirroring the
//!     ///     claude/opencode "ignore a mismatch rather than fabricate" rule).
//!     ///   * everything else: delegate to the pure `map_codex_line` unchanged.
//!     pub fn observe_line(&mut self, line: &str) -> Vec<ProtocolEvent>;
//! }
//!
//! /// Classification of ONE codex handshake line vs the id currently awaited.
//! /// PURE (line, expected_id) -> class. codex's `thread/start` response (id=2)
//! /// is PRECEDED on the wire by a `thread/started` NOTIFICATION (no id) — the
//! /// same notification-before-response hazard `classify_handshake_line` solves
//! /// for opencode. `perform_handshake` must LOOP reading lines under ONE
//! /// overall deadline, skipping Notifications, accepting the ExpectedResponse,
//! /// failing on an Anomaly.
//! #[derive(Debug, Clone, PartialEq, Eq)]
//! pub enum CodexHandshakeLineClass { Notification, ExpectedResponse, Anomaly }
//!
//! pub fn classify_codex_handshake_line(line: &str, expected_id: u64) -> CodexHandshakeLineClass;
//!
//! /// The in-flight wait-duration policy `pull_one` uses (BYTE-IDENTICAL to
//! /// claude_stream.rs / opencode_acp.rs's private `next_wait`, just made
//! /// crate-visible + re-exported so this test file can pin it — the same
//! /// regression-guard the two precedent drivers keep in their own in-src
//! /// #[cfg(test)] mod tests). While a turn is in flight, wait the FULL
//! /// remaining hang-watchdog budget (never cap at poll_idle — the event-loss
//! /// bug the two other drivers were fixed for); when idle, wait poll_idle.
//! pub fn next_wait(
//!     turn_in_flight: bool,
//!     elapsed: std::time::Duration,
//!     hang_watchdog: std::time::Duration,
//!     poll_idle: std::time::Duration,
//! ) -> std::time::Duration;
//! ```
//!
//! (`CodexAppServerProtocol` — the driver struct itself — is exercised by the
//! smoke test `tests/codex_appserver_smoke.rs` and the `--features live-cli`
//! test `tests/codex_appserver_live.rs`, not by this pure-mapping file.)
//!
//! Until the coder lands the above, `cargo build -p wasl-runtime-protocol --tests`
//! FAILS to resolve `wasl_runtime_protocol::{map_codex_line, CodexToolTracker,
//! classify_codex_handshake_line, CodexHandshakeLineClass}` — that build failure
//! IS the red state for every test in this file. Expected and correct per the
//! task brief and the two precedent files.
//!
//! ==========================================================================
//! REAL-DATA GAPS / RESOLVED AMBIGUITIES (the same ASSUMED-row discipline as
//! claude/opencode use for uncaptured shapes — documented, not silently guessed):
//!
//!   * `turn/completed` status=failed → the failure OBJECT shape. The real
//!     capture is ALL SUCCESS (no failed/interrupted turn was captured — the
//!     probe never hit one; `grep 'status": "failed\|interrupted'` over both
//!     files returns 0). The `failed` fixture below uses the SCHEMA-confirmed
//!     shape the probe's own docstring + `run_turn` documents (`turn.status`,
//!     `turn.error.message`) — the codex analog of claude's
//!     `maps_result_failure_...` "not-spike-captured, standard shape" fixture.
//!     This is a documented ASSUMED row, grounded in the schema, not invented.
//!   * `status=interrupted` → same: NOT captured; maps to TurnFailed with a
//!     fixed marker string (there is no TurnCancelled variant to map to).
//!     Documented ASSUMED row.
//!   * `commandExecution` `item/completed` DOES re-echo `command` in THIS
//!     capture (VERIFIED — events.jsonl line 119 carries
//!     `"command":"/bin/bash -lc 'ls -a'"`). The tracker still correlates BY ID
//!     (robust if a future codex drops it), matching the claude precedent's
//!     id→name discipline; the tracker's name for BOTH started and completed is
//!     the SAME `command` string, so the round-trip is self-consistent.
//!   * The OTHER `item.type`s (`fileChange`, `mcpToolCall`, `dynamicToolCall`,
//!     `webSearch`, `collabAgentToolCall`) — only `commandExecution` has real
//!     captured data. Their concrete field shapes are NOT captured, so they are
//!     an explicit documented gap: `map_codex_line`/`CodexToolTracker` map ONLY
//!     `commandExecution`; the others fall through to vec![] until a real
//!     capture pins them. (Same posture as claude's ASSUMED tool rows.)
//! ==========================================================================

use std::time::Duration;

use wasl_runtime_protocol::{
    classify_codex_handshake_line, map_codex_line, next_wait, CodexHandshakeLineClass,
    CodexToolTracker, NativeSessionId, ProtocolEvent,
};

// The native codex thread id (== sessionId, a UUIDv7) from the real capture.
const NATIVE_THREAD_ID: &str = "019fb381-27a4-76c3-ade1-74582e7439a7";

// ==========================================================================
// Real captured fixtures (verbatim, source line noted). Serde is
// order-insensitive for objects, so these are byte-faithful to the capture's
// key set/values even though whitespace is normalized by the capture writer.
// ==========================================================================

/// events.jsonl line 4 — the `thread/started` NOTIFICATION (no `id`), carrying
/// the native thread. Its `thread.id` (== `thread.sessionId`) is the native id.
const REAL_THREAD_STARTED: &str = r#"{"method": "thread/started", "params": {"thread": {"id": "019fb381-27a4-76c3-ade1-74582e7439a7", "extra": null, "sessionId": "019fb381-27a4-76c3-ade1-74582e7439a7", "forkedFromId": null, "parentThreadId": null, "preview": "", "ephemeral": false, "isPinned": false, "historyMode": "legacy", "modelProvider": "amd", "createdAt": 1785423014, "updatedAt": 1785423014, "recencyAt": 1785423014, "status": {"type": "idle"}, "cwd": "/x", "cliVersion": "0.146.0", "source": "vscode", "turns": []}}, "emittedAtMs": 1785423014011}"#;

/// events.jsonl line 14 — a single `item/agentMessage/delta` notification.
const REAL_AGENT_MESSAGE_DELTA_OK: &str = r#"{"method": "item/agentMessage/delta", "params": {"threadId": "019fb381-27a4-76c3-ade1-74582e7439a7", "turnId": "019fb381-2888-7851-96ed-28fb5b97c43c", "itemId": "msg_0fd67d01968b142b006a6b64a8e01881908a3efe5479fa2e01", "delta": "OK"}, "emittedAtMs": 1785423017231}"#;

/// events.jsonl line 30 — an `item/agentMessage/delta` with a different value.
const REAL_AGENT_MESSAGE_DELTA_IO: &str = r#"{"method": "item/agentMessage/delta", "params": {"threadId": "019fb381-27a4-76c3-ade1-74582e7439a7", "turnId": "019fb381-3661-7102-8c0e-8ed5f18aca47", "itemId": "msg_0fd67d01968b142b006a6b64abd12881909bd03aa9bb93bfa3", "delta": "IO"}, "emittedAtMs": 1785423020159}"#;

/// events.jsonl line 19 — `turn/completed` status=completed; `turn.items` carries
/// the FINAL agentMessage item with its full `text` ("OK"), `itemsView:"summary"`.
const REAL_TURN_COMPLETED_OK: &str = r#"{"method": "turn/completed", "params": {"threadId": "019fb381-27a4-76c3-ade1-74582e7439a7", "turn": {"id": "019fb381-2888-7851-96ed-28fb5b97c43c", "items": [{"type": "agentMessage", "id": "msg_0fd67d01968b142b006a6b64a8e01881908a3efe5479fa2e01", "text": "OK", "phase": null, "memoryCitation": null}], "itemsView": "summary", "status": "completed", "error": null, "startedAt": 1785423014, "completedAt": 1785423017, "durationMs": 3524}}, "emittedAtMs": 1785423017563}"#;

/// events.jsonl line 40 — the second turn's `turn/completed`; final item text is
/// the recalled token "VIOLET-7F29".
const REAL_TURN_COMPLETED_TOKEN: &str = r#"{"method": "turn/completed", "params": {"threadId": "019fb381-27a4-76c3-ade1-74582e7439a7", "turn": {"id": "019fb381-3661-7102-8c0e-8ed5f18aca47", "items": [{"type": "agentMessage", "id": "msg_0fd67d01968b142b006a6b64abd12881909bd03aa9bb93bfa3", "text": "VIOLET-7F29", "phase": null, "memoryCitation": null}], "itemsView": "summary", "status": "completed", "error": null, "startedAt": 1785423017, "completedAt": 1785423020, "durationMs": 2880}}, "emittedAtMs": 1785423020455}"#;

/// events.jsonl line 118 — commandExecution `item/started` (status inProgress).
const REAL_COMMAND_STARTED: &str = r#"{"method": "item/started", "params": {"item": {"type": "commandExecution", "id": "call_7eMQCIGACwAsXAYbv6E8jkMw", "pluginId": null, "scriptPath": null, "command": "/bin/bash -lc 'ls -a'", "cwd": "/x", "processId": "74858", "source": "unifiedExecStartup", "status": "inProgress", "commandActions": [{"type": "listFiles", "command": "ls -a", "path": null}], "aggregatedOutput": null, "exitCode": null, "durationMs": null}, "threadId": "019fb381-27a4-76c3-ade1-74582e7439a7", "turnId": "019fb381-41aa-70f2-bcfa-616db781619b", "startedAtMs": 1785423025329}, "emittedAtMs": 1785423025347}"#;

/// events.jsonl line 119 — commandExecution `item/completed` (status completed,
/// exitCode 0). SAME item `id` as REAL_COMMAND_STARTED.
const REAL_COMMAND_COMPLETED_OK: &str = r#"{"method": "item/completed", "params": {"item": {"type": "commandExecution", "id": "call_7eMQCIGACwAsXAYbv6E8jkMw", "pluginId": null, "scriptPath": null, "command": "/bin/bash -lc 'ls -a'", "cwd": "/x", "processId": "74858", "source": "unifiedExecStartup", "status": "completed", "commandActions": [{"type": "listFiles", "command": "ls -a", "path": null}], "aggregatedOutput": ".\n..\nREADME.md\n", "exitCode": 0, "durationMs": 0}, "threadId": "019fb381-27a4-76c3-ade1-74582e7439a7", "turnId": "019fb381-41aa-70f2-bcfa-616db781619b", "completedAtMs": 1785423025337}, "emittedAtMs": 1785423025347}"#;

// Noise lines (verbatim). NONE map to any event.
/// events.jsonl line 2.
const NOISE_REMOTE_CONTROL: &str = r#"{"method": "remoteControl/status/changed", "params": {"status": "disabled", "serverName": "MKM-L1-MOHEWAIS", "installationId": "e32b72a3-877a-4f95-b034-fe3fcd1705ac", "environmentId": null}, "emittedAtMs": 1785423013769}"#;
/// events.jsonl line 6.
const NOISE_WARNING: &str = r#"{"method": "warning", "params": {"threadId": "019fb381-27a4-76c3-ade1-74582e7439a7", "message": "Model metadata for `gpt-5-codex` not found. Defaulting to fallback metadata; this can degrade performance and cause issues."}, "emittedAtMs": 1785423014061}"#;
/// events.jsonl line 7.
const NOISE_THREAD_STATUS: &str = r#"{"method": "thread/status/changed", "params": {"threadId": "019fb381-27a4-76c3-ade1-74582e7439a7", "status": {"type": "active", "activeFlags": []}}, "emittedAtMs": 1785423014208}"#;
/// events.jsonl line 50 — a reasoning summary delta (127 of these in one turn;
/// MUST NOT be mis-mapped as assistant text).
const NOISE_REASONING_SUMMARY_DELTA: &str = r#"{"method": "item/reasoning/summaryTextDelta", "params": {"threadId": "019fb381-27a4-76c3-ade1-74582e7439a7", "turnId": "019fb381-41aa-70f2-bcfa-616db781619b", "itemId": "rs_x", "delta": " the", "summaryIndex": 0}, "emittedAtMs": 1785423023780}"#;

fn m(line: &str) -> Vec<ProtocolEvent> {
    map_codex_line(line)
}

// ==========================================================================
// Requirement 1 — `thread/started` NOTIFICATION -> SessionReady{ native }.
// The native id is thread.id (== thread.sessionId), the UUIDv7.
// ==========================================================================

#[test]
fn thread_started_notification_maps_to_session_ready_with_native_thread_id() {
    assert_eq!(
        m(REAL_THREAD_STARTED),
        vec![ProtocolEvent::SessionReady {
            native: Some(NativeSessionId(NATIVE_THREAD_ID.to_string()))
        }],
        "the real captured thread/started notification (events.jsonl line 4) \
         must map to SessionReady with native = thread.id (the UUIDv7 \
         {NATIVE_THREAD_ID}, == thread.sessionId)"
    );
}

// ==========================================================================
// Requirement 2 — `item/agentMessage/delta` -> AssistantDelta{ text: delta }.
// This is the incremental assistant text. Reasoning summary deltas are a
// DIFFERENT method and must NOT be mapped here (see the noise tests).
// ==========================================================================

#[test]
fn agent_message_delta_maps_to_assistant_delta_ok() {
    assert_eq!(
        m(REAL_AGENT_MESSAGE_DELTA_OK),
        vec![ProtocolEvent::AssistantDelta {
            text: "OK".to_string()
        }],
        "item/agentMessage/delta (events.jsonl line 14) must map to \
         AssistantDelta with text = params.delta (\"OK\")"
    );
}

#[test]
fn agent_message_delta_maps_the_delta_value_verbatim() {
    assert_eq!(
        m(REAL_AGENT_MESSAGE_DELTA_IO),
        vec![ProtocolEvent::AssistantDelta {
            text: "IO".to_string()
        }],
        "a second real delta (events.jsonl line 30) maps its own delta value \
         (\"IO\") — each delta line maps independently, no accumulation at the \
         pure layer"
    );
}

// ==========================================================================
// Requirement 3 — `turn/completed` status=completed -> TurnCompleted{ reply }.
//
// DESIGN DECISION (grounded in the real capture): the reply is taken from the
// FINAL agentMessage item's `text` in `turn.items`, NOT re-accumulated from
// deltas. The real turn/completed (events.jsonl lines 19/40/196) carries
// `itemsView:"summary"` with `turn.items` = the final agentMessage item(s),
// each with the FULL text ("OK" / "VIOLET-7F29" / "ls -a listed 11 entries").
// This is the authoritative final text — preferring it makes the PURE map
// self-sufficient for the reply (a transport-level delta accumulator like
// opencode's is then a belt-and-braces fallback, not the source of truth).
// ==========================================================================

#[test]
fn turn_completed_ok_maps_to_turn_completed_with_final_item_text() {
    assert_eq!(
        m(REAL_TURN_COMPLETED_OK),
        vec![ProtocolEvent::TurnCompleted {
            reply: "OK".to_string()
        }],
        "turn/completed status=completed (events.jsonl line 19) must map to \
         TurnCompleted with reply taken from the final agentMessage item's \
         text in turn.items (\"OK\"), which the real capture carries in full \
         (itemsView:\"summary\")"
    );
}

#[test]
fn turn_completed_prefers_the_final_agent_message_item_text_recalled_token() {
    assert_eq!(
        m(REAL_TURN_COMPLETED_TOKEN),
        vec![ProtocolEvent::TurnCompleted {
            reply: "VIOLET-7F29".to_string()
        }],
        "the second turn's turn/completed (events.jsonl line 40) carries the \
         recalled token as its final agentMessage item text — TurnCompleted.reply \
         must be that full text (\"VIOLET-7F29\"), proving the reply is sourced \
         from turn.items[..].text, not re-derived"
    );
}

// ==========================================================================
// Requirement 4 — `turn/completed` status=failed -> TurnFailed{ error }.
//
// DOCUMENTED ASSUMED ROW: no failed turn was captured (both files are 100%
// success; grep over both returns 0 failed/interrupted turns). This fixture
// uses the SCHEMA-confirmed failure shape the probe's own docstring +
// `run_turn` document (`turn.status == "failed"`, `turn.error.message`), the
// codex analog of claude_stream.rs's "not-spike-captured, standard shape"
// error fixture. Grounded in the schema, not invented.
// ==========================================================================

#[test]
fn turn_completed_failed_maps_to_turn_failed_with_error_message() {
    let failed = r#"{"method": "turn/completed", "params": {"threadId": "019fb381-27a4-76c3-ade1-74582e7439a7", "turn": {"id": "019fb381-2888-7851-96ed-28fb5b97c43c", "items": [], "itemsView": "summary", "status": "failed", "error": {"message": "boom"}, "startedAt": 1785423014, "completedAt": 1785423017, "durationMs": 100}}, "emittedAtMs": 1785423017563}"#;
    assert_eq!(
        m(failed),
        vec![ProtocolEvent::TurnFailed {
            error: "boom".to_string()
        }],
        "turn/completed status=failed must map to TurnFailed with \
         error = turn.error.message (\"boom\"). There is no separate \
         turn/failed method — failure rides on the turn/completed status \
         (schema-confirmed; ASSUMED shape, no failed turn was captured live)"
    );
}

// ==========================================================================
// Requirement 5 — `turn/completed` status=interrupted -> TurnFailed.
//
// There is NO ProtocolEvent::TurnCancelled variant in R1's vocabulary
// (VERIFIED against crates/wasl-runtime/src/lib.rs: the ProtocolEvent variants
// are SessionReady, TurnAccepted, AssistantDelta, ToolStarted, ToolFinished,
// NativeGateRequested, TurnCompleted, TurnFailed, ProcessExited — no cancel
// variant). So an interrupted terminal maps to TurnFailed, reusing the
// existing terminal vocabulary exactly as claude/opencode do. DOCUMENTED
// ASSUMED ROW: no interrupted turn was captured.
// ==========================================================================

#[test]
fn turn_completed_interrupted_maps_to_turn_failed_not_a_nonexistent_cancel_variant() {
    let interrupted = r#"{"method": "turn/completed", "params": {"threadId": "019fb381-27a4-76c3-ade1-74582e7439a7", "turn": {"id": "019fb381-2888-7851-96ed-28fb5b97c43c", "items": [], "itemsView": "summary", "status": "interrupted", "error": null, "startedAt": 1785423014, "completedAt": 1785423017, "durationMs": 100}}, "emittedAtMs": 1785423017563}"#;
    let out = m(interrupted);
    assert_eq!(
        out.len(),
        1,
        "an interrupted turn must produce exactly one terminal event"
    );
    assert!(
        matches!(out[0], ProtocolEvent::TurnFailed { .. }),
        "turn/completed status=interrupted must map to a TurnFailed terminal \
         (R1 has NO TurnCancelled ProtocolEvent variant — verified against \
         wasl-runtime/src/lib.rs); got {:?}",
        out[0]
    );
}

// ==========================================================================
// Requirement 6 — `turn/completed` status=inProgress is NOT terminal.
// The turn/start ack (`id`-correlated response) and any non-terminal status
// must NOT end the turn. Only completed/interrupted/failed are terminal.
// ==========================================================================

#[test]
fn turn_start_ack_response_and_in_progress_are_not_terminal() {
    // events.jsonl line 5 — the id-correlated turn/start ACK (status inProgress).
    let ack = r#"{"id": 3, "result": {"turn": {"id": "019fb381-2888-7851-96ed-28fb5b97c43c", "items": [], "itemsView": "notLoaded", "status": "inProgress", "error": null, "startedAt": null, "completedAt": null, "durationMs": null}}}"#;
    assert_eq!(
        m(ack),
        vec![],
        "the turn/start ack response (events.jsonl line 5, result.turn.status \
         inProgress) is just an ack, NOT a completion — it must map to vec![]"
    );
    // events.jsonl line 8 — turn/started notification (status inProgress).
    let started = r#"{"method": "turn/started", "params": {"threadId": "019fb381-27a4-76c3-ade1-74582e7439a7", "turn": {"id": "019fb381-2888-7851-96ed-28fb5b97c43c", "items": [], "itemsView": "notLoaded", "status": "inProgress", "error": null, "startedAt": 1785423014, "completedAt": null, "durationMs": null}}, "emittedAtMs": 1785423014209}"#;
    assert_eq!(
        m(started),
        vec![],
        "turn/started (events.jsonl line 8) is not a terminal boundary"
    );
}

// ==========================================================================
// Requirement 7 — commandExecution item mapping.
//   * PURE `item/started`   -> ToolStarted{ name = command }.
//   * PURE `item/completed`  -> vec![] (ok/name is the tracker's job).
//   * STATEFUL round-trip via CodexToolTracker -> ToolStarted then
//     ToolFinished{ name, ok: exitCode == 0 }, correlated BY ID.
// The name is the `command` string (the real item has no "name" field; the
// command IS the human-meaningful identity — documented design choice,
// mirroring "map the field that carries the tool's identity").
// ==========================================================================

#[test]
fn pure_command_started_maps_to_tool_started_with_the_command_as_name() {
    assert_eq!(
        m(REAL_COMMAND_STARTED),
        vec![ProtocolEvent::ToolStarted {
            name: "/bin/bash -lc 'ls -a'".to_string()
        }],
        "commandExecution item/started (events.jsonl line 118) must map to \
         ToolStarted with name = item.command (the command string is the \
         tool's identity; the item carries no separate `name` field)"
    );
}

#[test]
fn pure_command_completed_alone_maps_to_empty_ok_is_the_trackers_job() {
    assert_eq!(
        m(REAL_COMMAND_COMPLETED_OK),
        vec![],
        "the PURE per-line map of a commandExecution item/completed must be \
         vec![] — ToolFinished{{name,ok}} is produced by the stateful \
         CodexToolTracker (id-correlated), mirroring how the pure map_claude_line \
         leaves a tool_result unmapped"
    );
}

#[test]
fn tracker_correlates_command_started_then_completed_by_id_ok_true() {
    let mut tracker = CodexToolTracker::new();
    assert_eq!(
        tracker.observe_line(REAL_COMMAND_STARTED),
        vec![ProtocolEvent::ToolStarted {
            name: "/bin/bash -lc 'ls -a'".to_string()
        }],
        "the tracker emits the same ToolStarted the pure map emits on the \
         commandExecution item/started, AND records its id->command for the \
         later item/completed"
    );
    assert_eq!(
        tracker.observe_line(REAL_COMMAND_COMPLETED_OK),
        vec![ProtocolEvent::ToolFinished {
            name: "/bin/bash -lc 'ls -a'".to_string(),
            ok: true,
        }],
        "the correlated item/completed (SAME id call_7eMQ..., exitCode 0) must \
         map to ToolFinished with the recovered name and ok = (exitCode == 0) = \
         true (events.jsonl lines 118/119)"
    );
}

#[test]
fn tracker_ok_is_false_when_exit_code_is_nonzero() {
    // Same id as REAL_COMMAND_STARTED, but a nonzero exitCode. The nonzero-exit
    // shape is grounded: `exitCode` is a real captured field (0 on success);
    // ok = (exitCode == 0) makes a nonzero code ok:false. (The command text is
    // the same real command; only exitCode differs.)
    let mut tracker = CodexToolTracker::new();
    let _ = tracker.observe_line(REAL_COMMAND_STARTED);
    let failed_exit = r#"{"method": "item/completed", "params": {"item": {"type": "commandExecution", "id": "call_7eMQCIGACwAsXAYbv6E8jkMw", "command": "/bin/bash -lc 'ls -a'", "status": "completed", "aggregatedOutput": "boom", "exitCode": 2, "durationMs": 0}, "threadId": "019fb381-27a4-76c3-ade1-74582e7439a7", "turnId": "019fb381-41aa-70f2-bcfa-616db781619b", "completedAtMs": 1785423025337}, "emittedAtMs": 1785423025347}"#;
    assert_eq!(
        tracker.observe_line(failed_exit),
        vec![ProtocolEvent::ToolFinished {
            name: "/bin/bash -lc 'ls -a'".to_string(),
            ok: false,
        }],
        "a commandExecution item/completed with a NONZERO exitCode must map to \
         ToolFinished{{ ok: false }} (ok is derived as exitCode == 0)"
    );
}

#[test]
fn tracker_ignores_a_command_completed_whose_id_matches_no_started() {
    let mut tracker = CodexToolTracker::new();
    let _ = tracker.observe_line(REAL_COMMAND_STARTED);
    let foreign = r#"{"method": "item/completed", "params": {"item": {"type": "commandExecution", "id": "call_UNKNOWN_999", "command": "/bin/echo hi", "status": "completed", "aggregatedOutput": "hi", "exitCode": 0, "durationMs": 0}, "threadId": "019fb381-27a4-76c3-ade1-74582e7439a7", "turnId": "019fb381-41aa-70f2-bcfa-616db781619b", "completedAtMs": 1785423025337}, "emittedAtMs": 1785423025347}"#;
    assert_eq!(
        tracker.observe_line(foreign),
        vec![],
        "a commandExecution item/completed whose id matches no recorded \
         item/started is uncorrelated — the tracker must ignore it (vec![]), \
         NOT misroute it. Correlation is BY ID (mirrors the claude tracker's \
         ignore-a-mismatch discipline)"
    );
}

#[test]
fn tracker_delegates_non_command_lines_to_the_pure_map_unchanged() {
    let mut tracker = CodexToolTracker::new();
    assert_eq!(
        tracker.observe_line(REAL_AGENT_MESSAGE_DELTA_OK),
        vec![ProtocolEvent::AssistantDelta {
            text: "OK".to_string()
        }],
        "a non-command line (agentMessage delta) must still map via the \
         tracker's delegation to the pure map (no regression)"
    );
    assert_eq!(
        tracker.observe_line(REAL_THREAD_STARTED),
        vec![ProtocolEvent::SessionReady {
            native: Some(NativeSessionId(NATIVE_THREAD_ID.to_string()))
        }],
        "thread/started still maps to SessionReady through the tracker"
    );
    assert_eq!(tracker.observe_line(""), vec![]);
    assert_eq!(tracker.observe_line("{not json]"), vec![]);
}

// ==========================================================================
// Requirement 8 — noise lines never map and never crash.
// remoteControl/status/changed, warning, thread/status/changed, and the
// high-volume item/reasoning/summaryTextDelta MUST all be vec![]. The last is
// the sharpest: it is a *delta* on a *different* method than the assistant
// message delta and MUST NOT be mistaken for assistant text.
// ==========================================================================

#[test]
fn noise_lines_map_to_empty() {
    assert_eq!(
        m(NOISE_REMOTE_CONTROL),
        vec![],
        "remoteControl/status/changed"
    );
    assert_eq!(m(NOISE_WARNING), vec![], "warning");
    assert_eq!(m(NOISE_THREAD_STATUS), vec![], "thread/status/changed");
    assert_eq!(
        m(NOISE_REASONING_SUMMARY_DELTA),
        vec![],
        "item/reasoning/summaryTextDelta (127 per real turn) must NOT be \
         mis-mapped as AssistantDelta — it is reasoning noise, not assistant \
         output"
    );
}

#[test]
fn reasoning_and_user_message_items_are_not_tool_calls_and_do_not_map() {
    // events.jsonl line 11 — a reasoning item/started. NOT a tool call.
    let reasoning_started = r#"{"method": "item/started", "params": {"item": {"type": "reasoning", "id": "rs_x", "summary": [], "content": []}, "threadId": "019fb381-27a4-76c3-ade1-74582e7439a7", "turnId": "019fb381-2888-7851-96ed-28fb5b97c43c", "startedAtMs": 1785423016700}, "emittedAtMs": 1785423016700}"#;
    assert_eq!(
        m(reasoning_started),
        vec![],
        "a reasoning item/started is NOT a commandExecution — must not map to \
         ToolStarted"
    );
    // events.jsonl line 9 — a userMessage item/started. NOT a tool call, NOT
    // assistant output (it is the echoed user prompt).
    let user_msg = r#"{"method": "item/started", "params": {"item": {"type": "userMessage", "id": "u_x", "clientId": null, "content": [{"type": "text", "text": "hi", "text_elements": []}]}, "threadId": "019fb381-27a4-76c3-ade1-74582e7439a7", "turnId": "019fb381-2888-7851-96ed-28fb5b97c43c", "startedAtMs": 1785423014241}, "emittedAtMs": 1785423014243}"#;
    assert_eq!(
        m(user_msg),
        vec![],
        "a userMessage item (the echoed prompt) must not map to any event"
    );
    // The agentMessage item/started (empty text) is also not itself a delta.
    // events.jsonl line 13.
    let agent_msg_started = r#"{"method": "item/started", "params": {"item": {"type": "agentMessage", "id": "msg_x", "text": "", "phase": null, "memoryCitation": null}, "threadId": "019fb381-27a4-76c3-ade1-74582e7439a7", "turnId": "019fb381-2888-7851-96ed-28fb5b97c43c", "startedAtMs": 1785423017218}, "emittedAtMs": 1785423017219}"#;
    assert_eq!(
        m(agent_msg_started),
        vec![],
        "the agentMessage item/started (empty text placeholder) is not itself \
         an AssistantDelta — the deltas arrive via item/agentMessage/delta"
    );
}

#[test]
fn blank_and_malformed_lines_map_to_empty() {
    assert_eq!(m(""), vec![]);
    assert_eq!(m("   \t"), vec![]);
    assert_eq!(m("{not json]"), vec![]);
    assert_eq!(m(r#"{"method":"unknown/thing","params":{}}"#), vec![]);
}

// ==========================================================================
// §Handshake classifier — proving the notification-before-response hazard is
// handled: a `thread/started`-shaped NOTIFICATION arriving while awaiting the
// `thread/start` response (id=2) must be classified Notification (skip), NOT
// mistaken for the response. Mirrors opencode's classify_handshake_line tests.
// ==========================================================================

#[test]
fn thread_started_notification_during_handshake_is_classified_notification() {
    // The real thread/started notification (no id) arrives BEFORE the id=2
    // response. While awaiting id=2 it must be an ignorable Notification.
    assert_eq!(
        classify_codex_handshake_line(REAL_THREAD_STARTED, 2),
        CodexHandshakeLineClass::Notification,
        "the thread/started NOTIFICATION (method present, no id) that precedes \
         the thread/start response on the wire must be classified Notification \
         — skip and keep reading, NOT mistaken for the id=2 response (the \
         notification-before-response hazard)"
    );
}

#[test]
fn initialize_response_matching_id_is_expected() {
    // events.jsonl line 1 — the initialize response (id=1).
    let init_resp = r#"{"id": 1, "result": {"userAgent": "wasl-spike/0.146.0", "codexHome": "/x/.codex_home", "platformFamily": "unix", "platformOs": "linux"}}"#;
    assert_eq!(
        classify_codex_handshake_line(init_resp, 1),
        CodexHandshakeLineClass::ExpectedResponse,
        "the initialize response (events.jsonl line 1) whose id matches the \
         awaited id (1) and carries a result is the ExpectedResponse"
    );
}

#[test]
fn thread_start_response_matching_id_is_expected() {
    // events.jsonl line 3 — the thread/start response (id=2), carrying
    // result.thread.id. This is the ExpectedResponse while awaiting id=2.
    let thread_resp = r#"{"id": 2, "result": {"thread": {"id": "019fb381-27a4-76c3-ade1-74582e7439a7", "sessionId": "019fb381-27a4-76c3-ade1-74582e7439a7", "status": {"type": "idle"}, "turns": []}, "model": "gpt-5-codex", "modelProvider": "amd"}}"#;
    assert_eq!(
        classify_codex_handshake_line(thread_resp, 2),
        CodexHandshakeLineClass::ExpectedResponse,
        "the thread/start response (events.jsonl line 3, id=2, carries \
         result.thread.id) is the ExpectedResponse while awaiting id=2"
    );
}

#[test]
fn error_response_for_awaited_id_is_expected() {
    let err = r#"{"id": 1, "error": {"code": -32000, "message": "nope"}}"#;
    assert_eq!(
        classify_codex_handshake_line(err, 1),
        CodexHandshakeLineClass::ExpectedResponse,
        "an error response for the awaited id still satisfies the handshake \
         read (id matched, error present) — the caller surfaces the error"
    );
}

#[test]
fn wrong_id_response_is_anomaly() {
    // Awaiting id=1 but a response tagged id=2 appears.
    let wrong =
        r#"{"id": 2, "result": {"thread": {"id": "019fb381-27a4-76c3-ade1-74582e7439a7"}}}"#;
    assert_eq!(
        classify_codex_handshake_line(wrong, 1),
        CodexHandshakeLineClass::Anomaly,
        "a response with an id different from the awaited one is a protocol \
         anomaly to surface — NOT a Notification (silently skipped) and NOT \
         the ExpectedResponse"
    );
}

#[test]
fn classification_tracks_the_current_expected_id() {
    let id2_resp =
        r#"{"id": 2, "result": {"thread": {"id": "019fb381-27a4-76c3-ade1-74582e7439a7"}}}"#;
    assert_eq!(
        classify_codex_handshake_line(id2_resp, 2),
        CodexHandshakeLineClass::ExpectedResponse,
        "while awaiting id=2 (thread/start) this IS the expected response"
    );
    assert_eq!(
        classify_codex_handshake_line(id2_resp, 1),
        CodexHandshakeLineClass::Anomaly,
        "the very same id=2 line, while awaiting id=1, is a wrong-id anomaly — \
         classification must track the current expected id"
    );
}

#[test]
fn malformed_and_idless_non_notification_handshake_lines_are_anomaly() {
    assert_eq!(
        classify_codex_handshake_line("{not json]", 1),
        CodexHandshakeLineClass::Anomaly,
        "a malformed handshake line is an anomaly to surface"
    );
    // A codex NOTIFICATION legitimately has no `id` (and no `jsonrpc`) — but it
    // ALWAYS has `method`. A line with neither id NOR method is malformed for
    // this protocol and must be an anomaly, guarding against a lazy
    // "no id => notification" shortcut swallowing a broken response.
    let idless_result = r#"{"result": {"thread": {"id": "x"}}}"#;
    assert_eq!(
        classify_codex_handshake_line(idless_result, 1),
        CodexHandshakeLineClass::Anomaly,
        "a line with neither an id nor a method is not a valid codex \
         notification and not the expected response — it is an anomaly"
    );
}

#[test]
fn benign_noise_notifications_during_handshake_are_notifications() {
    // remoteControl/status/changed (events.jsonl line 2) legitimately arrives
    // mid-handshake (it did, right after the id=1 response). It has method, no
    // id -> Notification (skip and keep reading), same as thread/started.
    assert_eq!(
        classify_codex_handshake_line(NOISE_REMOTE_CONTROL, 2),
        CodexHandshakeLineClass::Notification,
        "remoteControl/status/changed (events.jsonl line 2) is a notification \
         (method, no id) that really does interleave the handshake — it must \
         be skipped, not treated as an anomaly"
    );
}

// ==========================================================================
// §next_wait — the in-flight wait-duration policy `pull_one` uses. This is the
// EXACT table-driven regression guard claude_stream.rs / opencode_acp.rs keep
// in their own in-src #[cfg(test)] mod tests (same 4 cases). The bug it guards
// against: while a turn was in flight the wait was capped at the short
// POLL_IDLE (500ms) breather regardless of the real hang-watchdog budget, so
// pull_one returned a spurious None; the generic R1 actor then parked on its
// mailbox with no re-poll timer and a real event arriving later (codex's own
// events land seconds in — tool turns took 10s in the capture) was LOST
// FOREVER. The fix waits the FULL remaining watchdog budget while in flight.
// Reverting next_wait to the old POLL_IDLE.min(...) body makes the first case
// FAIL — the fail-before/pass-after proof.
// ==========================================================================

const WATCHDOG: Duration = Duration::from_secs(60);
const IDLE: Duration = Duration::from_millis(500);

#[test]
fn next_wait_in_flight_uses_full_watchdog_not_poll_idle() {
    // In flight, only 10ms elapsed of a 60s budget: the wait MUST be ~60s
    // (the full remaining budget), NOT capped at the 500ms POLL_IDLE.
    let w = next_wait(true, Duration::from_millis(10), WATCHDOG, IDLE);
    assert_eq!(
        w,
        WATCHDOG - Duration::from_millis(10),
        "in-flight wait must be the full remaining watchdog budget, not POLL_IDLE"
    );
    assert!(
        w > IDLE,
        "the in-flight wait must exceed POLL_IDLE ({IDLE:?}); got {w:?} \
         (the old bug capped it at POLL_IDLE and lost slow real events)"
    );
}

#[test]
fn next_wait_in_flight_near_watchdog_returns_small_remainder() {
    // 59.9s elapsed of a 60s budget → ~100ms remaining (the true remainder).
    let elapsed = Duration::from_millis(59_900);
    let w = next_wait(true, elapsed, WATCHDOG, IDLE);
    assert_eq!(
        w,
        Duration::from_millis(100),
        "near the watchdog, the wait is the small true remainder"
    );
}

#[test]
fn next_wait_in_flight_past_watchdog_clamps_to_one_millisecond() {
    // Elapsed >= watchdog → saturating_sub is zero, clamped up to 1ms so the
    // timeout still fires (and pull_one then trips the watchdog terminal).
    let w = next_wait(true, WATCHDOG + Duration::from_secs(5), WATCHDOG, IDLE);
    assert_eq!(w, Duration::from_millis(1));
}

#[test]
fn next_wait_idle_is_exactly_poll_idle_regardless_of_elapsed() {
    // Not in flight → always exactly POLL_IDLE, no matter the elapsed value.
    assert_eq!(
        next_wait(false, Duration::from_millis(0), WATCHDOG, IDLE),
        IDLE
    );
    assert_eq!(
        next_wait(false, Duration::from_secs(120), WATCHDOG, IDLE),
        IDLE
    );
}
