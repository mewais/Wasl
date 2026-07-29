//! Hardening-5 (`docs/design/live-runtime-hardening-plan.md` §6 item 6) —
//! opencode ACP handshake + JSON-RPC-id correlation robustness. This is the
//! test-agent's RED spec for §4.6 (handshake tolerance of interleaved
//! notifications), §4.8 (response/turn correlation by JSON-RPC id), and the
//! documented §4.7 gap.
//!
//! ==========================================================================
//! ⚠️⚠️  THIS FILE DOES NOT COMPILE AGAINST THE CURRENT (unfixed) src/  ⚠️⚠️
//! ==========================================================================
//!
//! It CANNOT, and that is intentional and unavoidable — read this whole block.
//!
//! The two behaviors §4.6 and §4.8 require are TODAY implemented entirely inside
//! PRIVATE items of `crates/wasl-runtime-protocol/src/opencode_acp.rs`:
//!   * §4.6 handshake line handling: `perform_handshake` (private method) reads
//!     EXACTLY ONE line per request via `read_handshake_line` (private fn) then
//!     hands it to `parse_handshake_line(line, expected_id, step)` (private fn,
//!     `opencode_acp.rs:854`), which returns `Err(SpawnFailed)` the instant the
//!     line's `id` != `expected_id`. A legally-interleaved JSON-RPC NOTIFICATION
//!     (has `method`, no `id`) therefore ABORTS THE HANDSHAKE instead of being
//!     skipped — the exact §4.6 bug. There is NO loop, NO notification tolerance,
//!     and NO pure classifier distinguishing "skip this and keep reading" from
//!     "this is a real protocol anomaly."
//!   * §4.8 response correlation: `map_acp_line` (private fn, `opencode_acp.rs:889`)
//!     turns any `result{stopReason}` / `error` into `TurnCompleted` / `TurnFailed`
//!     WITHOUT ever checking the JSON-RPC `id` against the outstanding request id.
//!
//! NONE of `map_acp_line`, `parse_handshake_line`, `read_handshake_line`, or
//! `perform_handshake` is `pub` (verified against the real file this session), so
//! NONE is reachable from `tests/**` (the only surface the test-agent may write).
//! The crate's existing unit tests for these live INSIDE `src/opencode_acp.rs`'s
//! own `#[cfg(test)] mod tests` (`use super::*`) — `src/**`, which this crate's
//! `AGENTS.md` forbids the test-agent from touching. And `spawn` is hard-wired to
//! `tokio::process::Command` (`opencode_acp.rs:226`) with NO transport-injection
//! seam, so a mocked-stdio spawn test is ALSO impossible (option (1) in the task
//! brief is CLOSED). Per the task brief this leaves exactly option (2): specify
//! the fix as failing-to-compile tests against NEW, `tests/**`-reachable pure
//! functions the coder must EXTRACT and EXPOSE — the identical
//! extract-a-pure-fn-and-unit-test-it discipline this file already uses for
//! `map_acp_line`/`next_wait`, just made crate-visible so the fix is provable
//! from an out-of-`src` test.
//!
//! ==========================================================================
//! EXACT NEW API SURFACE THE CODER MUST CREATE (so this file compiles + the
//! tests below flip from RED to GREEN). Names/shapes chosen from what the real
//! source actually does; if the coder picks different names, THIS file's `use`
//! + the enum/fn references are the single re-map point.
//! ==========================================================================
//!
//! ```ignore
//! // in crates/wasl-runtime-protocol/src/opencode_acp.rs, exported at the crate
//! // root (`pub use opencode_acp::{HandshakeLineClass, classify_handshake_line,
//! //  map_acp_line_correlated};` in src/lib.rs) so tests/** can reach them.
//!
//! /// §4.6 — classification of ONE JSON-RPC line read during the handshake,
//! /// given the id we are currently waiting for. A PURE function of (line,
//! /// expected_id): no IO, no async. `perform_handshake` must LOOP reading
//! /// lines (bounded by ONE overall handshake deadline, not a fresh per-line
//! /// timeout), calling this on each and acting on the class:
//! ///   Notification   => ignore, keep reading
//! ///   ExpectedResponse=> the handshake response for `expected_id` — done
//! ///   Anomaly        => a wrong-id response OR a malformed line — surface/log
//! ///                     (NOT silently treated the same as a benign Notification)
//! #[derive(Debug, Clone, PartialEq, Eq)]
//! pub enum HandshakeLineClass {
//!     /// A JSON-RPC notification: has `method`, has NO `id`. Ignore & keep reading.
//!     Notification,
//!     /// The response we are waiting for: `id` == expected_id AND has `result`
//!     /// or `error`. The handshake step is satisfied by this line.
//!     ExpectedResponse,
//!     /// A protocol anomaly: a response carrying a DIFFERENT id, or a line that
//!     /// is neither a valid notification nor the expected response (malformed
//!     /// JSON, an id-less non-notification, etc). Must be surfaced, not skipped.
//!     Anomaly,
//! }
//!
//! pub fn classify_handshake_line(line: &str, expected_id: u64) -> HandshakeLineClass;
//!
//! // §4.8 — id-correlated mapping. Either ADD this correlated entrypoint and have
//! // `map_acp_line` delegate to it with the outstanding id threaded from the
//! // transport, OR change `map_acp_line`'s own signature to take the expected id;
//! // this file targets the ADDED-entrypoint shape (least churn to the existing
//! // pure `map_acp_line` fixtures). A `result{stopReason}`/`error` whose `id`
//! // does NOT match `expected_id` must NOT be mapped to `TurnCompleted`/
//! // `TurnFailed`; it is an unmatched/anomalous line (mapped to `vec![]`, i.e.
//! // ignored-not-misrouted — the "explicitly ignore a mismatched id rather than
//! // relying on an unstated single-outstanding-request invariant" fix). Notifs
//! // (session/update deltas, which carry no id) map exactly as before.
//! pub fn map_acp_line_correlated(line: &str, expected_id: u64) -> Vec<ProtocolEvent>;
//! ```
//!
//! Until the coder lands the above, `cargo build -p wasl-runtime-protocol --tests`
//! FAILS to resolve `wasl_runtime_protocol::{HandshakeLineClass,
//! classify_handshake_line, map_acp_line_correlated}` — that build failure IS the
//! red state for every test in this file. This is expected and correct per the
//! task brief.

use wasl_runtime_protocol::{
    classify_handshake_line, map_acp_line_correlated, HandshakeLineClass, ProtocolEvent,
};

// ==========================================================================
// §4.6 — the handshake must TOLERATE a legally-interleaved JSON-RPC
// notification while waiting for a response, and must DISTINGUISH it from a
// genuine protocol anomaly. Each case pins one arm of the required three-way
// classification (`live-runtime-hardening-plan.md` §4.6 + §0.1(4)).
// ==========================================================================

/// A notification (has `method`, NO `id`) that arrives while we await the
/// `initialize` response (id=1) must be classified `Notification` — "ignore and
/// keep reading". Against today's code there is no classifier at all AND the
/// inline logic (`parse_handshake_line`) would REJECT this line as a wrong-id
/// error and abort the handshake — the exact §4.6 bug.
#[test]
fn interleaved_notification_is_classified_ignore_not_anomaly() {
    // A real ACP `session/update`-shaped notification: has `method`, no `id`.
    let notif = r#"{"jsonrpc":"2.0","method":"session/update","params":{"sessionId":"ses_x","update":{"sessionUpdate":"available_commands_update","availableCommands":[]}}}"#;
    assert_eq!(
        classify_handshake_line(notif, 1),
        HandshakeLineClass::Notification,
        "a JSON-RPC notification (method present, no id) interleaved during the \
         handshake must be classified as an ignorable Notification — NOT aborted \
         and NOT confused with an anomaly (§4.6)"
    );
}

/// The awaited response itself (matching `id`, carries `result`) must be
/// classified `ExpectedResponse` so the handshake loop terminates on it.
#[test]
fn matching_id_result_response_is_classified_expected() {
    let resp = r#"{"jsonrpc":"2.0","id":1,"result":{"protocolVersion":1,"agentCapabilities":{}}}"#;
    assert_eq!(
        classify_handshake_line(resp, 1),
        HandshakeLineClass::ExpectedResponse,
        "the response whose id matches the outstanding handshake id and carries \
         a result must be the ExpectedResponse that ends the read loop (§4.6)"
    );
}

/// An `error` response for the awaited id is still the EXPECTED response (the
/// handshake step is satisfied — it is then the caller's job to surface the
/// error), NOT a notification and NOT an anomaly. Pins that `error` is a valid
/// terminating shape for the awaited id, matching the plan's "(matching id, has
/// result/error) => done" clause.
#[test]
fn matching_id_error_response_is_classified_expected() {
    let err = r#"{"jsonrpc":"2.0","id":1,"error":{"code":-32000,"message":"nope"}}"#;
    assert_eq!(
        classify_handshake_line(err, 1),
        HandshakeLineClass::ExpectedResponse,
        "an error response for the awaited id still SATISFIES the handshake read \
         (id matched; result/error present) — it terminates the loop as the \
         expected response, and the caller surfaces the error (§4.6)"
    );
}

/// A response carrying a DIFFERENT id (not the one we await) is a protocol
/// ANOMALY — it must be surfaced/logged, NOT silently skipped like a benign
/// notification (§4.6 clause (c), §0.1(4): "a wrong-id response is a real signal
/// something is off, not routine noise").
#[test]
fn wrong_id_response_is_classified_anomaly_not_ignored() {
    // We await id=1 (initialize) but see a response tagged id=2.
    let wrong = r#"{"jsonrpc":"2.0","id":2,"result":{"sessionId":"ses_x"}}"#;
    assert_eq!(
        classify_handshake_line(wrong, 1),
        HandshakeLineClass::Anomaly,
        "a response with an id different from the outstanding one is a protocol \
         anomaly to surface — it must NOT be classified Notification (silently \
         skipped) nor ExpectedResponse (§4.6 clause c)"
    );
}

/// A malformed / non-JSON line during the handshake is also an anomaly (neither
/// a valid notification nor the expected response), not silently skipped.
#[test]
fn malformed_line_is_classified_anomaly() {
    assert_eq!(
        classify_handshake_line("{not json]", 1),
        HandshakeLineClass::Anomaly,
        "a malformed line during the handshake is an anomaly to surface, not a \
         benign notification (§4.6 clause c)"
    );
}

/// An id-LESS line that is ALSO not a notification (no `method`) — e.g. a bare
/// `result` object with no `id` and no `method` — is malformed-for-JSON-RPC and
/// must be an anomaly, not mistaken for an ignorable notification. Guards against
/// a lazy "no id => notification" shortcut that would swallow a broken response.
#[test]
fn idless_non_notification_is_anomaly_not_notification() {
    let idless_result = r#"{"jsonrpc":"2.0","result":{"sessionId":"ses_x"}}"#;
    assert_eq!(
        classify_handshake_line(idless_result, 1),
        HandshakeLineClass::Anomaly,
        "a line with neither an id nor a method is not a valid notification and \
         not the expected response — it is an anomaly, not silently ignored \
         (guards against a 'no id => notification' shortcut) (§4.6)"
    );
}

/// The classifier must be relative to the CURRENT expected id: the SAME line
/// that is `ExpectedResponse` while awaiting id=2 is an `Anomaly` while awaiting
/// id=1. Pins that the second handshake round-trip (session/new, id=2) uses the
/// updated expected id, not a hard-coded one.
#[test]
fn classification_is_relative_to_expected_id() {
    let id2_resp = r#"{"jsonrpc":"2.0","id":2,"result":{"sessionId":"ses_x"}}"#;
    assert_eq!(
        classify_handshake_line(id2_resp, 2),
        HandshakeLineClass::ExpectedResponse,
        "while awaiting id=2 (session/new) this IS the expected response"
    );
    assert_eq!(
        classify_handshake_line(id2_resp, 1),
        HandshakeLineClass::Anomaly,
        "the very same id=2 line, while awaiting id=1, is a wrong-id anomaly — \
         classification must track the current expected id (§4.6)"
    );
}

// ==========================================================================
// §4.8 — response/turn correlation by JSON-RPC id. `map_acp_line` today maps a
// `result{stopReason}` / `error` to a terminal event WITHOUT checking the id
// against the outstanding request. These tests specify the FIXED behavior: a
// mismatched-id terminal line must NOT be misrouted into the current turn; a
// matching-id one still maps; id-less notifications are unaffected.
// (`live-runtime-hardening-plan.md` §4.8, land-together-with-§4.6.)
// ==========================================================================

/// The core §4.8 fix: a prompt `result{stopReason}` whose `id` does NOT match
/// the outstanding turn's request id must NOT be mapped to `TurnCompleted`. The
/// CURRENT code (no id check) WOULD emit `TurnCompleted` regardless — this test
/// asserts the FIXED behavior (ignore-not-misroute), so it is RED until the fix.
#[test]
fn stop_reason_result_with_mismatched_id_is_not_mapped_to_completed() {
    // Outstanding turn is id=3, but a stale/foreign result tagged id=7 arrives.
    let foreign = r#"{"jsonrpc":"2.0","id":7,"result":{"stopReason":"end_turn","usage":{}}}"#;
    assert_eq!(
        map_acp_line_correlated(foreign, 3),
        vec![],
        "a result with an id != the outstanding request id must NOT be misrouted \
         into the current turn as TurnCompleted — it is explicitly ignored, not \
         relied-upon-invariant accepted (§4.8). The current, unfixed map_acp_line \
         has no id check and WOULD wrongly emit TurnCompleted here."
    );
}

/// Symmetric §4.8 case for the error path: a JSON-RPC `error` for a NON-matching
/// id must not become the current turn's `TurnFailed`.
#[test]
fn error_with_mismatched_id_is_not_mapped_to_failed() {
    let foreign_err = r#"{"jsonrpc":"2.0","id":7,"error":{"code":-32000,"message":"boom"}}"#;
    assert_eq!(
        map_acp_line_correlated(foreign_err, 3),
        vec![],
        "an error response for an id != the outstanding request id must NOT be \
         misrouted into the current turn as TurnFailed (§4.8)"
    );
}

/// Positive control: with a MATCHING id the correlated mapper still produces the
/// same terminal event `map_acp_line` produces today — proving the id check
/// GATES misrouting without breaking the correct path. (Byte-identical fixture
/// to the existing `maps_prompt_result_stop_reason_...` src unit test.)
#[test]
fn stop_reason_result_with_matching_id_still_maps_to_completed() {
    let matching = r#"{"jsonrpc":"2.0","id":3,"result":{"stopReason":"end_turn","usage":{}}}"#;
    assert_eq!(
        map_acp_line_correlated(matching, 3),
        vec![ProtocolEvent::TurnCompleted {
            reply: String::new()
        }],
        "a result whose id MATCHES the outstanding request id maps to \
         TurnCompleted exactly as before — the id check only blocks mismatches \
         (§4.8)"
    );
}

/// Positive control for the error path with a matching id: still `TurnFailed`
/// with the same message extraction as today's `map_acp_line`.
#[test]
fn error_with_matching_id_still_maps_to_failed() {
    let matching_err = r#"{"jsonrpc":"2.0","id":3,"error":{"code":-32000,"message":"boom"}}"#;
    assert_eq!(
        map_acp_line_correlated(matching_err, 3),
        vec![ProtocolEvent::TurnFailed {
            error: "boom".to_string()
        }],
        "an error whose id MATCHES the outstanding request id maps to TurnFailed \
         exactly as before (§4.8)"
    );
}

/// Notifications (id-less `session/update` deltas) are correlation-agnostic:
/// `agent_message_chunk` still maps to `AssistantDelta` regardless of the
/// expected id, because a notification carries no id to correlate. Proves the
/// §4.8 id-gating applies ONLY to id-bearing responses, not to streaming deltas.
#[test]
fn agent_message_chunk_notification_maps_regardless_of_expected_id() {
    let chunk = r#"{"jsonrpc":"2.0","method":"session/update","params":{"sessionId":"ses_x","update":{"sessionUpdate":"agent_message_chunk","messageId":"m1","content":{"type":"text","text":"OK"}}}}"#;
    assert_eq!(
        map_acp_line_correlated(chunk, 999),
        vec![ProtocolEvent::AssistantDelta {
            text: "OK".to_string()
        }],
        "an id-less notification (streaming delta) is unaffected by id \
         correlation — it maps the same for any expected id (§4.8 applies only to \
         id-bearing responses)"
    );
}

// ==========================================================================
// §4.7 — DOCUMENTED, INTENTIONALLY-SKIPPED gap (no runnable test; per the task
// brief's explicit "if you can't devise a clean test, document why and skip"
// instruction, same policy as prior hardening phases and this crate's existing
// `opencode_accumulator_bound_gap.rs`).
//
// §4.7 requires the stderr-drain `tokio::spawn` to happen IMMEDIATELY after the
// child's stdio handles are taken (opencode_acp.rs:255-260), BEFORE any
// handshake write/read — currently it is spawned only AFTER both handshake
// round-trips complete (opencode_acp.rs:272-282). This is purely a matter of the
// ORDER of statements inside the private async `spawn_or_resume` method relative
// to a `tokio::spawn` call. It is:
//   * not a pure function of plain data (the §4.6/§4.8 tests' testable unit),
//   * not observable without a real child that actually fills its OS stderr pipe
//     buffer during its OWN startup and blocks on the write — which needs a live
//     opencode process (or an ACP-speaking fixture; the only fixture,
//     `wasl-fake-agent`, speaks the unrelated FakeLineProtocol line wire, per
//     `opencode_accumulator_bound_gap.rs`), neither available in this sandbox,
//   * and even with a live process the failure is a timing-dependent deadlock,
//     not a deterministic assertion — a flaky test, not a clean one.
// Forcing a weak/flaky test here would violate the no-tautology / no-stub rule.
// The correct verification is the coder moving the `tokio::spawn(stderr drain)`
// above the handshake in `spawn_or_resume`, confirmed by the dev-manager's code
// audit (an ordering review), NOT by a unit test. This gap is called out here so
// its absence is explicit and accounted-for, not a silent omission.
// ==========================================================================
