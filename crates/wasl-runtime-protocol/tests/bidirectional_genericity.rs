//! Phase-2C — THE BIDIRECTIONAL GENERICITY GATE (council #1, the sharper probe).
//!
//! ==========================================================================
//! TEST-SUITE CONCLUSION (stated PLAINLY, before any code lands — this OUTCOME,
//! not just a green bar, IS the Phase-2C deliverable per the council's framing).
//! ==========================================================================
//!
//! BACKGROUND. `claude-stdin` and `opencode-ACP` are BOTH unidirectional turn
//! loops: submit prompt → stream deltas/tool events → completion. Proving
//! `wasl_runtime::ProtocolEvent` survives a WIRE-FORMAT change (Phase 5's ACP)
//! does NOT prove it survives a CONTROL-FLOW change. The sharper test: a fake
//! agent that MID-TURN emits a `ToolStarted`-shaped event that BLOCKS the turn
//! until the SINGLE-WRITER ACTOR sends a value back over the SAME transport it
//! is reading from — reusing the actor's existing single write path, with NO new
//! `ProtocolEvent` variant and NO second concurrent writer. If the frozen API
//! can carry that, genericity is proven for control-flow, not just wire-format.
//! If it cannot, that is the leak the council predicted.
//!
//! FINAL STATE (Phase-2C LANDED). Genericity for a mid-turn bidirectional
//! callback now holds on BOTH channels:
//!   • the GATE channel — always did, through the spec's designed
//!     `NativeGateRequested` + `resolve_native_gate` answer-back (unchanged); and
//!   • the TOOL channel — it PREVIOUSLY LEAKED (a `ToolStarted`-shaped callback
//!     had no answer-back and the turn hung), and is NOW FIXED by the minimal,
//!     additive `answer_tool_call` extension the finding below recommended and
//!     the orchestrator authorized: a default-no-op
//!     `InteractiveTransport::answer_tool_call(&mut self, name, value)` +
//!     `ToolCallAnswerer::answer_tool_call(turn_id, value)` on the target,
//!     reached ONLY through the actor's serialized mailbox
//!     (`Command::AnswerToolCall`) — NO new `ProtocolEvent` variant, NO second
//!     writer. The recommended-target probe
//!     (`recommended_target_answer_tool_call_completes_the_blocked_turn`) is now
//!     ACTIVE and green, exercising that fix end-to-end. The narrative below is
//!     retained as the finding that produced this outcome.
//!
//! WHAT I FOUND, reading the FROZEN API (runtime.md §6, actor.rs, provider.rs):
//!
//!   The `InteractiveTransport` trait (runtime.md §6, lines 517-526) is exactly:
//!
//!       async fn send_turn(&mut self, prompt: &ComposedPrompt) -> Result<(), _>;
//!       async fn next_event(&mut self) -> Option<ProtocolEvent>;
//!       async fn interrupt(&mut self) -> Result<(), RuntimeError>;
//!
//!   Three `&mut self` methods, single-owner. There is NO `answer_tool_call`.
//!   `send_turn` is the ONLY value-sending method — and semantically it means
//!   "start a NEW turn", not "answer THIS mid-turn call".
//!
//!   HOWEVER — the bidirectional answer-back mechanism the council asked for
//!   ALREADY EXISTS IN THE FROZEN API, for exactly ONE event: the native gate.
//!   `ProtocolEvent::NativeGateRequested{detail}` is a mid-turn event that puts
//!   the actor into `AwaitingGate` (busy, not idle); the actor answers it back
//!   over the SAME transport via `resolve_native_gate(turn_id, decision)` →
//!   `Command::ResolveGate` → the single actor task calls `transport.send_turn`
//!   with the answer (actor.rs `resolve_gate`, lines ~882-923). This is funneled
//!   through the actor's OWN mailbox — the single writer — NOT a second raw
//!   handle. So a mid-turn BLOCK-until-answered round trip, reusing the single
//!   writer, with NO new `ProtocolEvent` variant, IS achievable TODAY.
//!
//!   THE LEAK (now FIXED — see FINAL STATE above): in the FROZEN (pre-2C) API
//!   that answer-back was bound to `NativeGateRequested` ALONE. A
//!   `ProtocolEvent::ToolStarted{name}` (actor.rs on_protocol_event, lines
//!   ~553-559) maps to `TurnEvent::ToolStarted` + `Busy{RunningTool}` and had NO
//!   answer-back path. `resolve_native_gate` on a turn that is NOT awaiting a
//!   gate returns `Err(GateUnresolvable)` (actor.rs `resolve_gate`, lines
//!   ~887-891) — and it STILL correctly does (probe (B) below pins exactly that,
//!   unchanged). Before Phase-2C there was NO generic `answer_tool_call`, so a
//!   turn blocked behind a `ToolStarted`-shaped callback could NOT be answered
//!   through the frozen API and HUNG — a genuine, provable control-flow leak.
//!   Phase-2C closed it with the minimal additive `answer_tool_call` extension
//!   (FINAL STATE above); the tool channel now completes the blocked turn.
//!
//! DECISION-READY VERDICT for the orchestrator:
//!
//!   • GENERICITY HOLDS for a mid-turn bidirectional callback IFF the driver maps
//!     that callback onto `ProtocolEvent::NativeGateRequested` (which IS the
//!     spec's designed mid-turn block/answer event). Test
//!     `ask_wasl_via_gate_completes_when_the_actor_answers_back_single_writer`
//!     PROVES this end-to-end through the REAL actor + REAL process, using the
//!     EXISTING enum, the EXISTING `resolve_native_gate`, and the SINGLE writer.
//!     → No new `ProtocolEvent` variant, no second writer. Genericity is REAL for
//!       control-flow — but ONLY through the gate channel.
//!
//!   • GENERICITY *USED TO* LEAK for a callback mapped onto
//!     `ProtocolEvent::ToolStarted`, and is NOW RESTORED via the tool channel.
//!     Test `ask_wasl_via_toolstarted_hangs_no_answerback_in_frozen_api` still
//!     PROVES the ORIGINAL leak on the NATURAL frozen-API attempt: the actor
//!     observes `ToolStarted`, the fake blocks reading one more stdin line, and
//!     `resolve_native_gate` — the only mid-turn answer-back method in the frozen
//!     API — rejects (`GateUnresolvable`) because the turn is `Busy{RunningTool}`,
//!     not `AwaitingGate` (and `submit_turn` again is "start a new turn", not
//!     "answer this one"), so via THAT path the turn never completes within the
//!     bound. That test is unchanged and remains correct — `resolve_native_gate`
//!     is NOT the right tool for a tool-callback. The RESTORATION is proven
//!     SEPARATELY by `recommended_target_answer_tool_call_completes_the_blocked_turn`
//!     (now ACTIVE): with the landed `answer_tool_call`, the SAME
//!     `ToolStarted`-blocked turn completes with the correlated reply. So the tool
//!     channel now has a first-class answer-back too — genericity HOLDS on both
//!     channels.
//!
//!   THE MINIMAL FIX (my recommendation as tester — NOW ADOPTED by the
//!   orchestrator and LANDED by the coder in `wasl-runtime`): to make a
//!   `ToolStarted`-shaped bidirectional callback first-class rather than forcing
//!   all mid-turn callbacks through the gate channel, do NOT add a `ProtocolEvent`
//!   variant and do NOT add a second writer. Instead add ONE default-no-op method
//!   to `InteractiveTransport`, preserving object-safety and the single-writer
//!   invariant (the actor is still the sole caller):
//!
//!       async fn answer_tool_call(&mut self, _name: &str, _value: &str)
//!           -> Result<(), RuntimeError> { Ok(()) }   // default no-op
//!
//!   plus an actor `Command::AnswerToolCall{turn_id, value}` reached ONLY through
//!   the existing mailbox (same single task that owns the `Box<dyn
//!   InteractiveTransport>`), surfaced to callers as
//!   `ToolCallAnswerer::answer_tool_call(turn_id, value)` on the
//!   `Arc<dyn DeliveryTarget>` (a supertrait, mirroring `NativeGateResolver`
//!   exactly). A default body keeps every existing implementor (claude-stdin,
//!   opencode-ACP, `FakeLineProtocol`) compiling unchanged and object-safe; the
//!   unidirectional drivers simply never receive the command. The RECOMMENDED-
//!   TARGET test below
//!   (`recommended_target_answer_tool_call_completes_the_blocked_turn`) was
//!   written against exactly that hypothesis; now that the method has LANDED it is
//!   ACTIVE (uncommented) and GREEN, exercising the fix end-to-end. It was left
//!   commented ONLY until the spec-level decision was made and the API landed —
//!   the coder was NOT to silently invent an `InteractiveTransport` extension.
//!
//! FAKE-AGENT WIRE (LANDED): the two `ask_wasl_*` commands specified in the
//! doc comments on `req_ask_wasl_*` below were implemented by the coder in
//! `src/bin/wasl-fake-agent.rs` (the fake agent BINARY is coder territory; this
//! file only SPECIFIES its wire, exactly as Phase-2B's tester specified the
//! hostile scripts without writing the bin). All live probes in this file are now
//! GREEN.
//!
//! ==========================================================================
//! EXACT NEW FAKE-AGENT WIRE BEHAVIOR THE CODER MUST IMPLEMENT (2 commands).
//! ==========================================================================
//!
//! Both commands prove the SAME control-flow shape (mid-turn emit → BLOCK on one
//! more stdin line over the SAME pipe → then terminal). They differ ONLY in
//! which existing event the mid-turn notification uses, so the two probes can
//! isolate "gate channel works" from "tool channel leaks":
//!
//!   (A) `{"cmd":"ask_wasl_gate","key":"<K>"}`  — the WORKS case.
//!       1. emit `{"ev":"accepted"}`
//!       2. emit `{"ev":"gate","summary":"ask_wasl","preview":"<K>"}`
//!          (the EXISTING `gate`→`NativeGateRequested` mapping — no new event).
//!       3. BLOCK reading exactly ONE MORE line from its OWN stdin (the SAME pipe
//!          the actor writes turns/answers into). That line is the actor's
//!          `resolve_native_gate` answer (the fake need not parse it — receiving
//!          ANY line is the unblock; a real CLI would read approve/deny).
//!       4. emit `{"ev":"done","reply":"answered:<K>"}` and end the turn.
//!       (This is a small generalization of the existing `gate` command, which
//!        already blocks on one stdin line then emits `done` "gate-resolved";
//!        `ask_wasl_gate` differs only in the deterministic `reply` payload so
//!        the test can assert the round trip completed with a correlated value.)
//!
//!   (B) `{"cmd":"ask_wasl_tool","key":"<K>"}`  — the LEAK case (sharper probe).
//!       1. emit `{"ev":"accepted"}`
//!       2. emit `{"ev":"tool_start","name":"ask_wasl"}`
//!          (the EXISTING `tool_start`→`ToolStarted` mapping — no new event).
//!       3. BLOCK reading exactly ONE MORE line from its OWN stdin — the answer
//!          that must arrive over the SAME pipe before the turn can proceed.
//!       4. ONLY after that line arrives: emit `{"ev":"tool_end","name":
//!          "ask_wasl","ok":true}` then `{"ev":"done","reply":"answered:<K>"}`.
//!       The point: through the FROZEN API there is NO way for the single-writer
//!       actor to send that line AS AN ANSWER TO THIS TOOL CALL, so the turn
//!       stays blocked and the test proves the hang. (If the minimal fix lands,
//!       the actor's `answer_tool_call` would send that line and (B) would
//!       complete — the recommended-target test is written against exactly that.)
//!
//! No sleeps drive logic. Every wait is a SHORT bounded `tokio::time::timeout`
//! so a genuinely-stuck turn FAILS FAST with a clear message, never hangs CI.

mod support;

use std::sync::{Arc, Mutex};
use std::time::Duration;

use futures::StreamExt;

use wasl_adapter::{Delivery, DeliveryKind};
use wasl_proto::{AgentId, GateDecision, RequestId};
use wasl_runtime::{
    BusyPolicy, Causation, DeliveryTarget, EventSeq, IdempotencyKey, ManagedConfig, MonotonicClock,
    NativeGateResolver, RuntimeProvider, StreamItem, TargetKey, TurnEvent, TurnId, TurnReceipt,
    TurnRequest, TurnSource, WaslManagedRuntimeProvider,
};
use wasl_runtime_protocol::FakeLineProtocol;

use support::fake_agent_bin;

// =====================================================================
// New request-line builders for the two Phase-2C wire commands. These are
// TESTER territory (string builders over the fake's stdin wire, exactly like
// every `req_*` in support/mod.rs); the fake-agent BIN behavior they specify is
// CODER territory (see the two-command spec in this file's header doc comment).
// =====================================================================

/// (A) `ask_wasl_gate <key>` — mid-turn emit a `gate` (→ `NativeGateRequested`),
/// then BLOCK on one more stdin line (the actor's `resolve_native_gate` answer),
/// then complete `done` with `reply:"answered:<key>"`. The WORKS case.
fn req_ask_wasl_gate(key: &str) -> String {
    format!(r#"{{"cmd":"ask_wasl_gate","key":"{key}"}}"#)
}

/// (B) `ask_wasl_tool <key>` — mid-turn emit a `tool_start` name `ask_wasl`
/// (→ `ToolStarted`), then BLOCK on one more stdin line that must arrive over the
/// SAME pipe as an ANSWER TO THIS TOOL CALL, then `tool_end` + `done` with
/// `reply:"answered:<key>"`. The LEAK case (the council's sharper probe).
fn req_ask_wasl_tool(key: &str) -> String {
    format!(r#"{{"cmd":"ask_wasl_tool","key":"{key}"}}"#)
}

// =====================================================================
// Harness (mirrors hold_queueing.rs's real-process + real-actor harness EXACTLY:
// FakeLineProtocol → WaslManagedRuntimeProvider::with_seams → target_for →
// submit_turn, driving the REAL Phase-2A actor over a REAL spawned fake agent).
// =====================================================================

/// A no-op sink — these probes assert control-flow, not audit (mirrors
/// hold_queueing.rs's `NullSink`).
struct NullSink;
impl wasl_audit::Sink for NullSink {
    fn append(&mut self, _e: &wasl_audit::AuditEntry) -> Result<(), wasl_audit::AuditError> {
        Ok(())
    }
}

/// Build a managed target backed by a REAL spawned fake-agent process (one live
/// target per test, keyed by `ctx`).
async fn managed_target(ctx: &str) -> Arc<dyn DeliveryTarget> {
    let proto = FakeLineProtocol::new(fake_agent_bin());
    let sink: Arc<Mutex<dyn wasl_audit::Sink + Send>> = Arc::new(Mutex::new(NullSink));
    let provider = WaslManagedRuntimeProvider::with_seams(
        proto,
        ManagedConfig::default(),
        Arc::new(MonotonicClock),
        sink,
    );
    provider
        .target_for(&TargetKey(ctx.to_string()))
        .await
        .expect("get-or-create the managed target backed by the real fake agent")
}

/// A `Consult` turn carrying `text` (the fake's stdin request line) under `ctx`.
fn consult_req(ctx: &str, text: &str) -> TurnRequest {
    TurnRequest {
        delivery: Delivery {
            context_id: ctx.to_string(),
            from: "wasl-b".to_string(),
            kind: DeliveryKind::Consult,
            text: text.to_string(),
            data: vec![],
        },
        source: TurnSource::Agent {
            from: AgentId("wasl-b".to_string()),
        },
        idempotency_key: Some(IdempotencyKey(format!("idem-{text}"))),
        causation: Causation {
            root_request: RequestId("root".to_string()),
            parent_request: None,
            path: vec![],
            hop: 0,
        },
    }
}

/// A SHORT bounded bound: a genuinely-stuck turn fails fast here (not a CI hang).
const PROBE_TIMEOUT: Duration = Duration::from_secs(3);

/// Drain up to `n` public `TurnEvent`s from a fresh `events(0)` subscription,
/// bounded by `PROBE_TIMEOUT` so a stuck turn returns what it has so far rather
/// than hanging. (The fake's `POLL_IDLE` is 500ms, so a few seconds is ample for
/// real events; a turn that never completes simply yields no terminal in time.)
async fn drain_events(target: &Arc<dyn DeliveryTarget>, n: usize) -> Vec<TurnEvent> {
    let mut stream = target.events(EventSeq(0)).await;
    let mut out = Vec::new();
    for _ in 0..n {
        match tokio::time::timeout(PROBE_TIMEOUT, stream.next()).await {
            Ok(Some(StreamItem::Event { event, .. })) => out.push(event),
            Ok(Some(StreamItem::Truncated { .. })) => break,
            Ok(None) => break,
            Err(_) => break, // bounded: no further event in time — stop draining
        }
    }
    out
}

fn has_tool_started(evs: &[TurnEvent], name: &str) -> bool {
    evs.iter()
        .any(|e| matches!(e, TurnEvent::ToolStarted { name: n, .. } if n == name))
}

fn has_gate_requested(evs: &[TurnEvent]) -> bool {
    evs.iter()
        .any(|e| matches!(e, TurnEvent::NativeGateRequested { .. }))
}

fn completion_reply(evs: &[TurnEvent]) -> Option<String> {
    evs.iter().find_map(|e| match e {
        TurnEvent::TurnCompleted { reply, .. } => Some(reply.clone()),
        _ => None,
    })
}

fn turn_terminated(evs: &[TurnEvent]) -> bool {
    evs.iter().any(|e| {
        matches!(
            e,
            TurnEvent::TurnCompleted { .. }
                | TurnEvent::TurnFailed { .. }
                | TurnEvent::TurnCancelled { .. }
                | TurnEvent::ProcessExited { .. }
        )
    })
}

// =====================================================================
// PROBE (A) — GENERICITY HOLDS via the gate channel.
//
// A mid-turn bidirectional callback mapped onto the EXISTING
// `NativeGateRequested` event is answered back by the SINGLE-WRITER actor over
// the SAME transport via the EXISTING `resolve_native_gate` — NO new
// `ProtocolEvent` variant, NO second writer. The blocked turn COMPLETES only
// after the actor answers. This proves control-flow genericity IS real through
// the spec's designated mid-turn block/answer channel.
// =====================================================================

#[tokio::test]
async fn ask_wasl_via_gate_completes_when_the_actor_answers_back_single_writer() {
    let ctx = "ctx-2c-gate";
    let target = managed_target(ctx).await;

    // Submit a turn whose body is `ask_wasl_gate VIOLET-7F29`: mid-turn the fake
    // emits a gate and BLOCKS reading one more stdin line.
    let started = target
        .submit_turn(
            consult_req(ctx, &req_ask_wasl_gate("VIOLET-7F29")),
            BusyPolicy::Reject,
        )
        .await;
    let turn_id: TurnId = match started {
        TurnReceipt::Started { turn_id, .. } => turn_id,
        other => panic!("first submit must Start the turn, got {other:?}"),
    };

    // The actor observes the mid-turn gate — using the EXISTING enum. And the
    // turn does NOT complete yet: it is genuinely BLOCKED awaiting the answer.
    let pre = drain_events(&target, 8).await;
    assert!(
        has_gate_requested(&pre),
        "the actor must observe NativeGateRequested mid-turn (existing enum, no new \
         variant), got {pre:?}"
    );
    assert!(
        completion_reply(&pre).is_none(),
        "the turn must NOT complete before the actor answers back — it is BLOCKED \
         (bidirectional control flow), got {pre:?}"
    );

    // Answer back through the SINGLE-WRITER actor: resolve_native_gate funnels the
    // answer through the actor's OWN mailbox (Command::ResolveGate → the one task
    // that owns the transport calls send_turn) — NOT a second raw handle. This IS
    // the frozen API's bidirectional answer-back mechanism.
    tokio::time::timeout(
        PROBE_TIMEOUT,
        target.resolve_native_gate(turn_id.clone(), GateDecision::Approve),
    )
    .await
    .expect("resolve_native_gate must not hang")
    .expect("resolving the pending gate through the single writer succeeds (§9)");

    // NOW — and only now — the blocked turn proceeds to completion, carrying the
    // correlated answer payload. The round trip closed over the SAME transport,
    // via the SAME single writer, with the EXISTING enum.
    let post = drain_events(&target, 16).await;
    assert_eq!(
        completion_reply(&post),
        Some("answered:VIOLET-7F29".to_string()),
        "after the single-writer answer-back, the blocked turn completes with the \
         correlated reply — bidirectional genericity HOLDS via the gate channel: {post:?}"
    );

    let _ = target.shutdown(wasl_runtime::ShutdownMode::Immediate).await;
}

// =====================================================================
// PROBE (B) — GENERICITY LEAKS via the tool channel (the council's sharper
// probe). A mid-turn callback mapped onto `ToolStarted` has NO answer-back path
// in the FROZEN API: `resolve_native_gate` rejects (the turn is Busy{RunningTool},
// not AwaitingGate), and `submit_turn` means "new turn", not "answer this one".
// The turn HANGS. This test demonstrates the leak as a genuine, bounded hang —
// NOT a compile error — so the orchestrator can decide whether the minimal
// `InteractiveTransport::answer_tool_call` extension is warranted.
// =====================================================================

#[tokio::test]
async fn ask_wasl_via_toolstarted_hangs_no_answerback_in_frozen_api() {
    let ctx = "ctx-2c-tool";
    let target = managed_target(ctx).await;

    let started = target
        .submit_turn(
            consult_req(ctx, &req_ask_wasl_tool("VIOLET-7F29")),
            BusyPolicy::Reject,
        )
        .await;
    let turn_id: TurnId = match started {
        TurnReceipt::Started { turn_id, .. } => turn_id,
        other => panic!("first submit must Start the turn, got {other:?}"),
    };

    // The actor observes ToolStarted mid-turn — the EXISTING enum carries the
    // NOTIFICATION half fine (no new variant needed just to NOTICE the callback).
    let pre = drain_events(&target, 8).await;
    assert!(
        has_tool_started(&pre, "ask_wasl"),
        "the actor must observe ToolStarted{{name:\"ask_wasl\"}} mid-turn (existing \
         enum), got {pre:?}"
    );
    assert!(
        !turn_terminated(&pre),
        "the turn must be BLOCKED behind the tool call — not yet terminated, got {pre:?}"
    );

    // ATTEMPT THE MOST NATURAL THING THE FROZEN API ALLOWS to answer this tool
    // call, and assert it does NOT resolve the block:
    //
    //   The ONLY value-returning methods reachable on the returned
    //   `Arc<dyn DeliveryTarget>` are `submit_turn` (start a NEW turn — wrong
    //   semantics) and `resolve_native_gate` (answer a NATIVE GATE). There is NO
    //   `answer_tool_call`. So the single natural attempt is
    //   `resolve_native_gate` — which MUST fail, because the turn is
    //   Busy{RunningTool}, not AwaitingGate (actor.rs resolve_gate guard).
    let attempt = tokio::time::timeout(
        PROBE_TIMEOUT,
        target.resolve_native_gate(turn_id.clone(), GateDecision::Approve),
    )
    .await
    .expect("resolve_native_gate returns promptly (it rejects, it does not block)");
    assert!(
        attempt.is_err(),
        "PROVEN LEAK: the frozen API has NO way to answer a ToolStarted-blocked \
         turn — resolve_native_gate rejects because the turn is not AwaitingGate \
         (it is Busy{{RunningTool}}). There is no answer_tool_call. Got {attempt:?}"
    );

    // With no answer-back mechanism, the turn NEVER completes within the bound.
    // This is the leak demonstrated as a genuine hang (bounded so CI fails fast),
    // NOT a missing type / compile error.
    let post = drain_events(&target, 16).await;
    assert!(
        completion_reply(&post).is_none(),
        "PROVEN LEAK: a ToolStarted-blocked turn cannot be answered through the \
         frozen InteractiveTransport contract, so it never completes — \
         `ProtocolEvent` genericity is proven ONLY for unidirectional turn loops \
         and the gate channel, NOT for a `ToolStarted`-shaped mid-turn callback. \
         Drained (incomplete) events: {post:?}"
    );

    let _ = target.shutdown(wasl_runtime::ShutdownMode::Immediate).await;
}

// =====================================================================
// SINGLE-WRITER INVARIANT CHECK — whatever mechanism resolves a mid-turn
// callback, NO code path may give a caller a second concurrent holder of the
// transport. The answer-back MUST funnel through the actor's serialized mailbox
// (the SAME single task that owns `Box<dyn InteractiveTransport>`), never a raw
// second handle exposed to callers.
//
// This is a STRUCTURAL assertion over the FROZEN public API: the only trait a
// generic caller holds is `Arc<dyn DeliveryTarget>` (+ its supertrait
// `NativeGateResolver`). NONE of its methods hand back the transport, a stdin
// handle, or any writer — every value-sending method (`submit_turn`,
// `resolve_native_gate`) returns a plain receipt/`Result`, so a caller CANNOT
// obtain a second writer. If a future `answer_tool_call` fix lands, it MUST keep
// this shape (a mailbox command on the same actor task), which the
// recommended-target test below encodes.
// =====================================================================

#[tokio::test]
async fn answer_back_never_exposes_a_second_writer_to_callers() {
    let ctx = "ctx-2c-singlewriter";
    let target = managed_target(ctx).await;

    // Drive the WORKS path (gate) so a real answer-back round trip happens, then
    // assert the caller only ever touched the serialized `Arc<dyn DeliveryTarget>`
    // API — never a raw transport/stdin handle.
    let started = target
        .submit_turn(
            consult_req(ctx, &req_ask_wasl_gate("K1")),
            BusyPolicy::Reject,
        )
        .await;
    let turn_id = match started {
        TurnReceipt::Started { turn_id, .. } => turn_id,
        other => panic!("expected Started, got {other:?}"),
    };
    let _ = drain_events(&target, 8).await;

    // The answer-back is a method ON the target handle — it routes through the
    // actor mailbox (Command::ResolveGate), the SAME task that owns the transport.
    // There is NO API on `Arc<dyn DeliveryTarget>` / `NativeGateResolver` that
    // returns a writer, a `ChildStdin`, or a `&mut dyn InteractiveTransport`, so a
    // second concurrent writer is UNREPRESENTABLE at the public seam. Two
    // concurrent answer-backs would simply be two serialized mailbox commands on
    // one actor — never two raw writers. We exercise a concurrent second call to
    // make the serialization explicit: both go through the mailbox, neither holds
    // a raw handle.
    let t2 = target.clone();
    let tid2 = turn_id.clone();
    let concurrent = tokio::spawn(async move {
        // A second, concurrent resolve attempt from another task: it also can
        // ONLY go through the mailbox (no raw handle exists to grab). It resolves
        // or errors, but it can NEVER be a second physical writer.
        let _ = tokio::time::timeout(
            PROBE_TIMEOUT,
            t2.resolve_native_gate(tid2, GateDecision::Approve),
        )
        .await;
    });
    let primary = tokio::time::timeout(
        PROBE_TIMEOUT,
        target.resolve_native_gate(turn_id, GateDecision::Approve),
    )
    .await
    .expect("resolve does not hang");
    let _ = primary;
    let _ = concurrent.await;

    // The turn still completes exactly once and cleanly — the single writer was
    // never contended by a raw second handle (the mailbox serialized both calls).
    let post = drain_events(&target, 16).await;
    let completions = post
        .iter()
        .filter(|e| matches!(e, TurnEvent::TurnCompleted { .. }))
        .count();
    assert!(
        completions <= 1,
        "the single-writer actor must complete the turn AT MOST once even under \
         concurrent answer-backs (both serialized through the one mailbox, no raw \
         second writer), got {completions} completions: {post:?}"
    );

    let _ = target.shutdown(wasl_runtime::ShutdownMode::Immediate).await;
}

// =====================================================================
// RECOMMENDED-TARGET (LANDED) — GENERICITY RESTORED via the tool channel.
//
// The orchestrator adopted the minimal spec extension and the coder landed it in
// `wasl-runtime`: a DEFAULT-no-op
// `InteractiveTransport::answer_tool_call(&mut self, name, value)` + an actor
// `Command::AnswerToolCall` reached only through the existing serialized mailbox,
// exposed on the target as `ToolCallAnswerer::answer_tool_call(turn_id, value)`
// (a supertrait of `DeliveryTarget`, mirroring `NativeGateResolver` exactly).
//
// This test encodes EXACTLY that acceptance criterion, now ACTIVE: after
// observing ToolStarted (and confirming the turn is BLOCKED, not yet completed),
// the caller answers via the new method (through the single writer), and the
// previously-hanging (B) turn COMPLETES with the correlated reply — genuinely
// exercising the fix, not a tautology. It was left COMMENTED only until the
// spec-level decision was made and the API landed; now that it has, it is
// uncommented and green. The two probes above (gate-completes, toolstarted-hangs-
// on-the-natural-attempt) are UNCHANGED and still correct.
// =====================================================================
#[tokio::test]
async fn recommended_target_answer_tool_call_completes_the_blocked_turn() {
    let ctx = "ctx-2c-toolfix";
    let target = managed_target(ctx).await;
    let started = target
        .submit_turn(
            consult_req(ctx, &req_ask_wasl_tool("VIOLET-7F29")),
            BusyPolicy::Reject,
        )
        .await;
    let turn_id = match started {
        TurnReceipt::Started { turn_id, .. } => turn_id,
        other => panic!("expected Started, got {other:?}"),
    };

    // The actor observes the mid-turn ToolStarted (existing enum), and the turn is
    // genuinely BLOCKED behind the tool call: it has NOT completed before we answer.
    let pre = drain_events(&target, 8).await;
    assert!(
        has_tool_started(&pre, "ask_wasl"),
        "the actor must observe ToolStarted{{name:\"ask_wasl\"}} mid-turn: {pre:?}"
    );
    assert!(
        completion_reply(&pre).is_none(),
        "the turn must NOT complete before the tool call is answered — it is BLOCKED \
         behind the ToolStarted-shaped callback: {pre:?}"
    );

    // THE MINIMAL FIX under test: a generic, non-CLI-specific answer-back for a
    // tool call, routed through the single-writer actor mailbox (NOT a raw
    // second handle). Object-safe: default-no-op on the trait so claude/ACP
    // implementors compile unchanged. This is the tool-channel counterpart of
    // `resolve_native_gate` — reached via the `ToolCallAnswerer` supertrait on the
    // SAME `Arc<dyn DeliveryTarget>`, funneled through `Command::AnswerToolCall` to
    // the one actor task that owns the transport.
    tokio::time::timeout(
        PROBE_TIMEOUT,
        target.answer_tool_call(turn_id, "VIOLET-7F29".to_string()),
    )
    .await
    .expect("answer_tool_call must not hang")
    .expect("answer_tool_call routes the answer through the single writer");

    // NOW — and only now — the previously-hanging (B) turn proceeds to completion,
    // carrying the correlated reply. The round trip closed over the SAME transport,
    // via the SAME single writer, with NO new `ProtocolEvent` variant.
    let post = drain_events(&target, 16).await;
    assert_eq!(
        completion_reply(&post),
        Some("answered:VIOLET-7F29".to_string()),
        "with the minimal answer_tool_call fix, the ToolStarted-blocked turn \
         completes via the single writer — genericity restored for the tool \
         channel too: {post:?}"
    );
    let _ = target.shutdown(wasl_runtime::ShutdownMode::Immediate).await;
}
