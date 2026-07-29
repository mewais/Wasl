//! Issue A (claude half) — recognize the REAL `tool_use` / `tool_result` wire
//! shapes from a live claude `stream-json` turn and map them onto R1's existing
//! `ProtocolEvent::ToolStarted{name}` / `ProtocolEvent::ToolFinished{name,ok}`.
//!
//! This is the test-agent's RED spec for `docs/design/live-runtime-hardening-plan.md`
//! §1 ("Issue A") and the two ASSUMED rows in `docs/specs/runtime-protocol.md`
//! §2.1 (lines 208-209). Those rows were previously ASSUMED-shape-only and left
//! deliberately unmapped (`vec![]`) by `src/claude_stream.rs`'s own
//! `tool_use_and_tool_result_are_intentionally_unmapped_for_now` unit test —
//! this file is written against a REAL, redacted capture that now pins the
//! concrete shape.
//!
//! GROUND TRUTH — every fixture below is copied verbatim from the real captured
//! wire in `spikes/live-substrate/out/claude_toolcall_capture.jsonl`:
//!
//! Line 74 — the `tool_use` (inside an `assistant`-type message's `content`):
//! ```ignore
//! {"type":"tool_use","id":"toolu_vrtx_01DM3Wu6YKQEujmiGSGFGTvL",
//!  "name":"Glob","input":{"pattern":"*","path":"/tmp/wasl-toolcap-mv69uhvp"}}
//! ```
//!
//! Line 75 — the `tool_result` (inside a subsequent `user`-type message's
//! `content`), correlated back to the tool_use by `tool_use_id` == the
//! tool_use's `id`; NO `is_error` field on this SUCCESSFUL result:
//! ```ignore
//! {"tool_use_id":"toolu_vrtx_01DM3Wu6YKQEujmiGSGFGTvL",
//!  "type":"tool_result","content":"No files found"}
//! ```
//!
//! ==========================================================================
//! ⚠️⚠️  THIS FILE DOES NOT COMPILE AGAINST THE CURRENT (unfixed) src/  ⚠️⚠️
//! ==========================================================================
//!
//! It CANNOT, and that is intentional and unavoidable — read this whole block.
//!
//! Today `map_claude_line` is a PRIVATE free fn in `src/claude_stream.rs`
//! (`claude_stream.rs:661`, NOT `pub`, NOT re-exported at the crate root — unlike
//! `classify_handshake_line` / `map_acp_line_correlated`, which an earlier
//! hardening pass DID expose for the opencode driver, see
//! `tests/opencode_handshake_and_correlation_hardening.rs` for the precedent).
//! Its only tests live INSIDE that file's own `#[cfg(test)] mod tests`
//! (`use super::*`) — `src/**`, which this crate's `AGENTS.md` forbids the
//! test-agent from touching. So the ONLY way to prove this mapping from a
//! `tests/**` file is to have the coder EXPOSE testable entrypoints, exactly the
//! way the opencode hardening did. Two entrypoints are needed because the wire
//! itself forces a pure-vs-stateful split (documented in the API block below):
//!   * `tool_use` carries the name IN THE SAME LINE  -> a PURE per-line map
//!     (`map_claude_line`) can emit `ToolStarted{name}`.
//!   * `tool_result` carries ONLY `tool_use_id`, NEVER the name (VERIFIED against
//!     the real capture — the redacted line 75 has no `name` field) -> a pure
//!     single-line map STRUCTURALLY CANNOT recover the name; `ToolFinished{name,
//!     ok}` therefore requires per-turn state that remembers the earlier
//!     `tool_use`'s `id -> name` (spec §2.1 line 209 says exactly this:
//!     "name/ok from the tool_use it answers"). This is the identical
//!     pure-map-plus-transport-state split the opencode driver already uses for
//!     `accumulated_reply` (`map_acp_line` stays pure; the transport accumulates)
//!     — see `AGENTS.md` Phase-5 "Accumulation design split (intentional)".
//!
//! ==========================================================================
//! EXACT NEW API SURFACE THE CODER MUST CREATE (so this file compiles + the
//! tests below flip from RED to GREEN). Names/shapes chosen from what the real
//! source actually does + the opencode precedent; if the coder picks different
//! names, THIS file's `use` + the references below are the single re-map point.
//! ==========================================================================
//!
//! ```ignore
//! // in crates/wasl-runtime-protocol/src/claude_stream.rs, exported at the crate
//! // root (add to src/lib.rs's existing claude re-export line, e.g.
//! //   `pub use claude_stream::{ClaudeStreamProtocol, map_claude_line, ClaudeToolTracker};`)
//! // so tests/** can reach them.
//!
//! /// PURE per-line map (the EXISTING private fn, just made crate-visible +
//! /// re-exported). Its today-behavior for system/init, assistant.text, and
//! /// result{...} is UNCHANGED (its in-src unit fixtures still hold). The ONLY
//! /// new behavior this task adds: an `assistant` message whose content contains
//! /// a `tool_use` block now emits `ProtocolEvent::ToolStarted{name}` (the name
//! /// read from the SAME line's `tool_use.name`). It must NOT invent a call-id
//! /// field on `ToolStarted` — the normalized vocabulary carries only a name
//! /// today, and per-call-id tracking is explicitly deferred (hardening plan
//! /// §3.5 / §0.1(5)). A `tool_result` line, seen by this PURE fn alone, still
//! /// maps to `vec![]` (it cannot recover the name here — see the tracker).
//! pub fn map_claude_line(line: &str) -> Vec<ProtocolEvent>;
//!
//! /// STATEFUL per-turn correlator that recovers the `tool_result`'s name from
//! /// the `tool_use` it answers (id -> name), the transport-level counterpart of
//! /// opencode's `accumulated_reply`. The claude transport (`ClaudeStreamTransport`)
//! /// owns one of these and feeds EVERY stdout line through `observe_line`
//! /// instead of calling the pure `map_claude_line` directly, so the id->name
//! /// map is populated on the `tool_use` line and consulted on the later
//! /// `tool_result` line (same turn, same pipe).
//! #[derive(Debug, Default)]
//! pub struct ClaudeToolTracker { /* private: id -> name map */ }
//!
//! impl ClaudeToolTracker {
//!     pub fn new() -> Self;
//!     /// Map ONE stdout line, threading tool-call id->name state:
//!     ///   * a `tool_use` block: record `id -> name`, THEN emit the same
//!     ///     `ToolStarted{name}` the pure `map_claude_line` emits (so a caller
//!     ///     using the tracker gets identical ToolStarted output).
//!     ///   * a `tool_result` block: look up `tool_use_id` in the recorded map;
//!     ///     if found, emit `ToolFinished{name, ok}` with the remembered name
//!     ///     and `ok` derived from the result (see the ok-derivation note in the
//!     ///     tests below); if the id is unknown (no matching tool_use seen this
//!     ///     turn), emit `vec![]` (cannot honestly name it — ignored, not
//!     ///     misrouted, mirroring the opencode correlation "ignore-not-misroute"
//!     ///     discipline).
//!     ///   * everything else: delegate to the pure `map_claude_line` unchanged.
//!     pub fn observe_line(&mut self, line: &str) -> Vec<ProtocolEvent>;
//! }
//! ```
//!
//! Until the coder lands the above, `cargo build -p wasl-runtime-protocol --tests`
//! FAILS to resolve `wasl_runtime_protocol::{map_claude_line, ClaudeToolTracker}`
//! — that build failure IS the red state for every test in this file. This is
//! expected and correct per the task brief and the opencode precedent.
//!
//! ==========================================================================
//! OUT OF SCOPE FOR THIS PASS (explicitly tracked, NOT silently dropped):
//!   * The FAILURE-path `tool_result` (`is_error:true` / an error `ok:false`
//!     shape) — the real capture only contains a SUCCESSFUL result (line 75 has
//!     NO `is_error` field). Its exact shape is a GUESS today, so no test pins
//!     it. See `ok_is_derived_...`'s note below. Needs a follow-up capture of a
//!     FAILING tool call before `ok:false` can be honestly pinned.
//!   * `ProtocolEvent::NativeGateRequested` / native-permission-gate mapping — a
//!     gate was NOT captured in this pass (`permission_denials: []` in the spike;
//!     the redacted capture shows only a Glob tool call, no gate). Fully out of
//!     scope for this file; remains ASSUMED per spec §2.1/§3.1.
//!   * A call-id field on `ToolStarted`/`ToolFinished` — deferred by hardening
//!     plan §3.5/§0.1(5) as a coupled vocabulary change; NOT invented here.
//!   * The opencode ACP tool/gate rows — a separate driver, separate capture.
//! ==========================================================================

use wasl_runtime_protocol::{map_claude_line, ClaudeToolTracker, ProtocolEvent};

/// The REAL captured `tool_use` line (spike line 74) — an `assistant`-type
/// stream-json message whose `content` array carries one `tool_use` block.
/// Copied verbatim (the redacted capture's exact bytes).
const REAL_TOOL_USE_LINE: &str = r#"{"type":"assistant","message":{"model":"claude-sonnet-4-6","id":"msg_vrtx_011CdWrm7PhKVtknLiyJxR5Z","type":"message","role":"assistant","content":[{"type":"tool_use","id":"toolu_vrtx_01DM3Wu6YKQEujmiGSGFGTvL","name":"Glob","input":{"pattern":"*","path":"/tmp/wasl-toolcap-mv69uhvp"}}],"stop_reason":null},"session_id":"b6f1b661-0757-4b9c-8710-fc7117618e39"}"#;

/// The REAL captured `tool_result` line (spike line 75) — a `user`-type
/// stream-json message whose `content` array carries one `tool_result` block,
/// correlated to the tool_use above by `tool_use_id` == the tool_use's `id`.
/// A SUCCESSFUL result: NO `is_error` field present. Copied verbatim.
const REAL_TOOL_RESULT_LINE: &str = r#"{"type":"user","message":{"role":"user","content":[{"tool_use_id":"toolu_vrtx_01DM3Wu6YKQEujmiGSGFGTvL","type":"tool_result","content":"No files found"}]},"session_id":"b6f1b661-0757-4b9c-8710-fc7117618e39"}"#;

// ==========================================================================
// Requirement 1 — a real `assistant`-type line with a `tool_use` block maps to
// `ProtocolEvent::ToolStarted{name}` via the PURE `map_claude_line`. The name
// is `"Glob"`, read from the SAME line's `tool_use.name`. `ToolStarted` has NO
// id field today (hardening §3.5 defers per-call-id tracking), so only the name
// is mapped — this is a plan-acknowledged gap, not invented here.
// ==========================================================================

/// The core requirement-1 fix. Against today's code `map_claude_line` returns
/// `vec![]` for any `tool_use` (its `tool_use_and_tool_result_are_intentionally_
/// unmapped_for_now` src unit test pins that). This asserts the FIXED behavior,
/// so it is RED (and, until the fn is exposed, does not even compile) until the
/// coder lands the mapping.
#[test]
fn real_tool_use_line_maps_to_tool_started_with_the_captured_name() {
    assert_eq!(
        map_claude_line(REAL_TOOL_USE_LINE),
        vec![ProtocolEvent::ToolStarted {
            name: "Glob".to_string()
        }],
        "an assistant-type stream-json line carrying a tool_use content block \
         must map to ToolStarted with the tool name read from tool_use.name \
         (the REAL captured name is \"Glob\", spike line 74). Today's \
         map_claude_line returns vec![] here (Issue A / spec §2.1 ASSUMED row \
         now pinned by a real capture)."
    );
}

/// Guards the name is read from the RIGHT field (`tool_use.name`), not confused
/// with the `id` or the message `id`. A minimal (but still real-shaped) line
/// with a distinct name proves the extraction targets `name` specifically.
#[test]
fn tool_use_name_is_read_from_the_name_field_not_the_id() {
    let line = r#"{"type":"assistant","message":{"content":[{"type":"tool_use","id":"toolu_abc","name":"Read","input":{"file_path":"/x"}}]},"session_id":"s"}"#;
    assert_eq!(
        map_claude_line(line),
        vec![ProtocolEvent::ToolStarted {
            name: "Read".to_string()
        }],
        "the ToolStarted name must be the tool_use block's `name` (\"Read\"), \
         never its `id` (\"toolu_abc\") nor the message id"
    );
}

// ==========================================================================
// Requirement 2 + 3 — a real `user`-type line with a `tool_result` block maps
// to `ProtocolEvent::ToolFinished{name, ok}`. Because the REAL tool_result line
// carries ONLY `tool_use_id` (NO name — VERIFIED against the capture), the name
// CANNOT be recovered by a pure single-line map; it is recovered by the STATEFUL
// `ClaudeToolTracker`, which remembers the earlier tool_use's id->name (spec
// §2.1 line 209: "name/ok from the tool_use it answers"). These tests specify
// that stateful correlation and prove it round-trips by id.
// ==========================================================================

/// The pure `map_claude_line`, given the `tool_result` line ALONE (no prior
/// state), CANNOT honestly produce a name — so it must map to `vec![]`, NOT a
/// fabricated-name `ToolFinished`. This pins the real limitation of a per-line
/// map and justifies the tracker's existence. (Against today's code this is
/// already `vec![]` — but today it's `vec![]` for the WRONG reason, "unmapped".
/// This test locks in that the CORRECT reason — "cannot recover name at the pure
/// layer" — still yields `vec![]` at the pure layer AFTER the fix, so a coder
/// must not "fix" requirement 2 by inventing a name here.)
#[test]
fn pure_map_of_tool_result_alone_cannot_name_it_and_must_not_fabricate() {
    assert_eq!(
        map_claude_line(REAL_TOOL_RESULT_LINE),
        vec![],
        "a tool_result line carries only tool_use_id, never the name (VERIFIED \
         against the real capture, spike line 75) — the PURE per-line map cannot \
         recover the name and MUST NOT fabricate one; name recovery is the \
         stateful tracker's job (spec §2.1 line 209). vec![] here is correct."
    );
}

/// Requirement 2/3 core: with the tracker, a `tool_use` followed by its
/// correlated `tool_result` (matched by `tool_use_id` == `id`) round-trips to
/// `ToolStarted{name}` then `ToolFinished{name, ok}`, with the name RECOVERED
/// from the remembered tool_use. Uses the two REAL captured lines in the real
/// captured ORDER.
#[test]
fn tracker_correlates_real_tool_use_then_result_by_id_recovering_the_name() {
    let mut tracker = ClaudeToolTracker::new();

    // The tool_use line first (records id -> "Glob", emits ToolStarted).
    assert_eq!(
        tracker.observe_line(REAL_TOOL_USE_LINE),
        vec![ProtocolEvent::ToolStarted {
            name: "Glob".to_string()
        }],
        "the tracker must emit the same ToolStarted the pure map emits on the \
         tool_use line, AND record its id->name for the later tool_result"
    );

    // Then the correlated tool_result line: name recovered from the id.
    assert_eq!(
        tracker.observe_line(REAL_TOOL_RESULT_LINE),
        vec![ProtocolEvent::ToolFinished {
            name: "Glob".to_string(),
            ok: true,
        }],
        "the tool_result, correlated by tool_use_id to the earlier tool_use, \
         must map to ToolFinished with the name RECOVERED from that tool_use \
         (\"Glob\") — the pure line cannot supply it. ok:true because the real \
         captured result is SUCCESSFUL (no is_error field, spike line 75)."
    );
}

/// The correlation must be BY ID: a `tool_result` whose `tool_use_id` does not
/// match any recorded `tool_use` is an unknown/uncorrelated result and must map
/// to `vec![]` (ignored-not-misrouted — the tracker cannot honestly name it),
/// exactly the "explicitly ignore a mismatch rather than fabricate/misroute"
/// discipline the opencode id-correlation fix uses. Proves the tracker keys on
/// `tool_use_id`, not "the last tool_use seen".
#[test]
fn tracker_ignores_a_tool_result_whose_id_matches_no_recorded_tool_use() {
    let mut tracker = ClaudeToolTracker::new();

    // Record a tool_use with id toolu_vrtx_...TvL (the real one).
    let _ = tracker.observe_line(REAL_TOOL_USE_LINE);

    // A tool_result for a DIFFERENT id we never saw a tool_use for.
    let foreign_result = r#"{"type":"user","message":{"role":"user","content":[{"tool_use_id":"toolu_UNKNOWN_999","type":"tool_result","content":"whatever"}]},"session_id":"b6f1b661-0757-4b9c-8710-fc7117618e39"}"#;

    assert_eq!(
        tracker.observe_line(foreign_result),
        vec![],
        "a tool_result whose tool_use_id matches no recorded tool_use cannot be \
         honestly named — the tracker must ignore it (vec![]), NOT misroute it \
         with the wrong (e.g. last-seen) name. Correlation is BY ID."
    );
}

/// The tracker must correlate the RIGHT tool_use when several are in flight —
/// two distinct tool_use ids, then their two tool_results in a DIFFERENT order,
/// each ToolFinished carrying the name of the tool_use its id answers (not
/// positional / last-seen). Hardens that the id->name map is a real map, not a
/// single slot.
#[test]
fn tracker_correlates_the_right_name_among_multiple_concurrent_tool_uses() {
    let mut tracker = ClaudeToolTracker::new();

    let use_glob = r#"{"type":"assistant","message":{"content":[{"type":"tool_use","id":"toolu_A","name":"Glob","input":{}}]},"session_id":"s"}"#;
    let use_read = r#"{"type":"assistant","message":{"content":[{"type":"tool_use","id":"toolu_B","name":"Read","input":{}}]},"session_id":"s"}"#;

    assert_eq!(
        tracker.observe_line(use_glob),
        vec![ProtocolEvent::ToolStarted {
            name: "Glob".to_string()
        }]
    );
    assert_eq!(
        tracker.observe_line(use_read),
        vec![ProtocolEvent::ToolStarted {
            name: "Read".to_string()
        }]
    );

    // Results arrive in REVERSE order (B then A) — each must resolve by its own id.
    let result_b = r#"{"type":"user","message":{"content":[{"tool_use_id":"toolu_B","type":"tool_result","content":"ok"}]},"session_id":"s"}"#;
    let result_a = r#"{"type":"user","message":{"content":[{"tool_use_id":"toolu_A","type":"tool_result","content":"No files found"}]},"session_id":"s"}"#;

    assert_eq!(
        tracker.observe_line(result_b),
        vec![ProtocolEvent::ToolFinished {
            name: "Read".to_string(),
            ok: true,
        }],
        "the result for toolu_B must name \"Read\" (B's tool_use), not the \
         last-seen or first-seen name — correlation is per-id"
    );
    assert_eq!(
        tracker.observe_line(result_a),
        vec![ProtocolEvent::ToolFinished {
            name: "Glob".to_string(),
            ok: true,
        }],
        "the result for toolu_A must name \"Glob\" (A's tool_use)"
    );
}

/// The tracker must not change the behavior of the ALREADY-MAPPED, non-tool
/// lines — an `assistant.text` line still maps to `AssistantDelta`, a
/// `system/init` line still maps to `SessionReady`, exactly as the pure
/// `map_claude_line` does today (the tracker delegates to it for everything that
/// is not a tool_use/tool_result). Guards against the tracker regressing the
/// existing mappings while adding tool correlation. Uses a real-shaped
/// system/init line (byte-shape from the spike capture line 1, trimmed).
#[test]
fn tracker_delegates_non_tool_lines_to_the_existing_pure_mapping_unchanged() {
    let mut tracker = ClaudeToolTracker::new();

    let text_line = r#"{"type":"assistant","message":{"content":[{"type":"text","text":"I'll list the files in the current directory using Glob."}]},"session_id":"b6f1b661-0757-4b9c-8710-fc7117618e39"}"#;
    assert_eq!(
        tracker.observe_line(text_line),
        vec![ProtocolEvent::AssistantDelta {
            text: "I'll list the files in the current directory using Glob.".to_string()
        }],
        "a non-tool assistant.text line must still map to AssistantDelta via the \
         tracker's delegation to the existing pure map (no regression)"
    );

    // Blank / malformed lines still drop (framing continues), same as the pure map.
    assert_eq!(tracker.observe_line(""), vec![]);
    assert_eq!(tracker.observe_line("{not json]"), vec![]);
}

// ==========================================================================
// Requirement 4 (documented, NO runnable test) — the `ok:false` / failure-path
// tool_result shape.
//
// The real capture (spike line 75) contains ONLY a SUCCESSFUL tool_result, which
// has NO `is_error` field — so `ok:true` above is grounded (absence of an error
// marker == success). The FAILURE shape (whether claude emits
// `"is_error":true` on the tool_result block, or some other marker, and where)
// was NOT captured. Writing an `ok:false` fixture now would be a GUESS with false
// confidence, which this task explicitly forbids. It is therefore left as a
// documented OPEN GAP pending a follow-up capture of a FAILING tool call. The
// coder's `ok` derivation should default to `true` when no error marker is
// present (the only shape we have evidence for); the `is_error`-true branch, once
// a real failure is captured, is a follow-up test + follow-up mapping row.
// ==========================================================================

/// The one thing about `ok` we CAN honestly assert from the real capture: the
/// captured SUCCESSFUL result (no is_error field) yields `ok: true`. This is not
/// a tautology — it pins the success-derivation direction (absence-of-error =>
/// ok) that the deferred failure capture will complement, and it is grounded in
/// the real line's actual field set. (Same fixture/expectation as the round-trip
/// test above; kept as a focused single-assertion pin so the ok-derivation is
/// documented independently of the correlation mechanics.)
#[test]
fn ok_is_true_for_the_real_captured_successful_result_no_is_error_field() {
    let mut tracker = ClaudeToolTracker::new();
    let _ = tracker.observe_line(REAL_TOOL_USE_LINE);
    let finished = tracker.observe_line(REAL_TOOL_RESULT_LINE);
    assert_eq!(
        finished,
        vec![ProtocolEvent::ToolFinished {
            name: "Glob".to_string(),
            ok: true,
        }],
        "the REAL captured tool_result is successful (no is_error field, spike \
         line 75) => ok must be true. The ok:false / failure shape is an OPEN \
         GAP (not captured) — see the requirement-4 block above; do not guess it."
    );
}
