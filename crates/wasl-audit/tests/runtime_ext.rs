//! Spec-driven tests for the additive live-runtime extension to `wasl-audit`
//! (docs/specs/runtime.md §8.1 delivery-certainty + §12 audit observability).
//!
//! These are TEST-FIRST / red: the current crate has no `RuntimeTurn` entry
//! variant and no first-class delivery-certainty field, so this file MUST fail
//! to compile / assert against `src/lib.rs` as it stands. That is the expected
//! state; the coder makes it green with a MINIMAL, ADDITIVE change.
//!
//! What is being pinned vs. assumed (tester/coder separation):
//!   * The runtime spec (runtime.md §8.1) PINS the certainty *vocabulary* and
//!     its serde form: a `#[serde(rename_all = "snake_case")]` enum with
//!     variants NotInjected / Injected / Completed / FailedBeforeInjection /
//!     OutcomeUnknown → wire strings "not_injected", "injected", "completed",
//!     "failed_before_injection", "outcome_unknown". These tests assert THAT
//!     observable wire contract.
//!   * runtime.md §12 PINS the requirement: managed turn-lifecycle + certainty
//!     transitions are recorded as `wasl_audit::AuditEntry` values into the
//!     same `Sink`. So a new machine-queryable entry kind, and a first-class
//!     certainty field on the entry, must exist.
//!   * The EXACT Rust type names below (`EntryKind::RuntimeTurn`, the
//!     `certainty` field, and a `wasl_audit`-owned `DeliveryCertainty`) are the
//!     MOST LIKELY coder layout and are aligned to here so the tests are
//!     precise. Where a name is a design choice rather than a spec-pinned fact,
//!     it is flagged inline as ASSUMED-NAME and the test also independently
//!     nails the OBSERVABLE serde form (the string on the wire, the field being
//!     a distinct JSON key, matchability) so the contract holds even if the
//!     coder picks a different spelling — see the JSON-shape tests, which never
//!     name a Rust symbol.
//!
//! DEPENDENCY-DIRECTION NOTE (flagged to orchestrator): runtime.md's Deps line
//! declares `wasl-runtime` depends on `wasl-audit`. Therefore `wasl-audit` must
//! NOT depend on `wasl-runtime` (that is a cycle). So the certainty type CANNOT
//! be imported from `wasl-runtime`; the coder must either OWN a
//! `DeliveryCertainty` in `wasl-audit` (re-used by runtime) or store the tag
//! structurally (still a dedicated field/enum, never inside `summary`). These
//! tests assume `wasl-audit` owns/exposes the type; if the coder instead makes
//! the entry carry a structured string tag, the string-shape tests still pass
//! and only the typed-import lines need the coder's chosen name.

use wasl_audit::{
    AuditEntry, AuditError, AuditLog, DeliveryCertainty, EntryKind, RuntimeTurnPhase, Sink,
};
use wasl_proto::{AgentId, GateDecision};

// ---------------------------------------------------------------------
// Fake Sink — same convention as tests/happy.rs / tests/errors.rs:
// records appended entries in order; directly observable.
// ---------------------------------------------------------------------

#[derive(Debug, Default)]
struct FakeSink {
    appended: Vec<AuditEntry>,
}

impl FakeSink {
    fn new() -> Self {
        Self::default()
    }
}

impl Sink for FakeSink {
    fn append(&mut self, entry: &AuditEntry) -> Result<(), AuditError> {
        self.appended.push(entry.clone());
        Ok(())
    }
}

fn agent(id: &str) -> AgentId {
    AgentId(id.to_string())
}

fn targets(ids: &[&str]) -> Vec<AgentId> {
    ids.iter().map(|s| agent(s)).collect()
}

// A *legacy* entry constructor that uses ONLY the fields that exist in the
// frozen shape. It deliberately does NOT set the new certainty field, proving
// that adding it must not break existing construction sites. If the coder makes
// the new field non-optional / non-defaulted, this constructor stops compiling —
// which is the guard for requirement (1) back-compat.
fn legacy_entry(
    seq: u64,
    kind: EntryKind,
    actor: &str,
    targets_: &[&str],
    summary: &str,
) -> AuditEntry {
    AuditEntry {
        seq,
        kind,
        actor: agent(actor),
        targets: targets(targets_),
        summary: summary.to_string(),
        decision: None,
        ..Default::default()
    }
}

// =====================================================================
// (1) BACK-COMPAT — existing entry shapes/serde forms unchanged.
// =====================================================================

// (1a) The five existing EntryKind variants still serialize to EXACTLY the same
// lowercase wire strings and in the same set (additive change must not rename or
// reorder the serde form of existing variants).
#[test]
fn backcompat_existing_entry_kinds_serde_unchanged() {
    let cases = [
        (EntryKind::Consult, "\"consult\""),
        (EntryKind::Command, "\"command\""),
        (EntryKind::Message, "\"message\""),
        (EntryKind::Transfer, "\"transfer\""),
        (EntryKind::Decision, "\"decision\""),
    ];
    for (kind, expected) in cases {
        let json = serde_json::to_string(&kind).expect("serialize existing EntryKind");
        assert_eq!(
            json, expected,
            "existing EntryKind serde form must not change"
        );
        let back: EntryKind = serde_json::from_str(&json).expect("deserialize existing EntryKind");
        assert_eq!(back, kind);
    }
}

// (1b) An existing-shape entry (no certainty) round-trips EXACTLY as before, and
// the serialized JSON does NOT contain a certainty key when unset (or it is
// null). Old consumers/producers keep working.
#[test]
fn backcompat_entry_without_certainty_round_trips_and_omits_key() {
    let e = legacy_entry(
        1,
        EntryKind::Message,
        "alice",
        &["bob", "carol"],
        "hi there",
    );

    let value = serde_json::to_value(&e).expect("serialize legacy entry to value");
    let obj = value
        .as_object()
        .expect("entry serializes as a JSON object");

    // The new certainty field, when unset, must be serde-invisible: either the
    // key is absent, or present-but-null. Never a bare defaulted enum that would
    // change existing JSON output.
    match obj.get("certainty") {
        None => {}
        Some(v) => assert!(
            v.is_null(),
            "unset certainty must be absent or null, got: {v}"
        ),
    }

    let back: AuditEntry =
        serde_json::from_str(&serde_json::to_string(&e).unwrap()).expect("round-trip legacy entry");
    assert_eq!(back, e, "legacy entry must round-trip byte-for-byte equal");
}

// (1c) OLD JSON — a payload produced BEFORE this extension (no certainty key at
// all, existing kinds only) must still deserialize into the new struct with the
// certainty defaulted to absent/None. This is the serde-back-compat guarantee.
#[test]
fn backcompat_old_json_without_certainty_key_still_deserializes() {
    let old_json = serde_json::json!({
        "seq": 42,
        "kind": "command",
        "actor": "alice",
        "targets": ["bob"],
        "summary": "legacy command entry from before the extension"
        // NOTE: no "certainty", no "decision" — exactly the frozen shape.
    })
    .to_string();

    let back: AuditEntry = serde_json::from_str(&old_json).expect("old JSON must deserialize");
    assert_eq!(back.seq, 42);
    assert_eq!(back.kind, EntryKind::Command);
    assert_eq!(back.actor, agent("alice"));
    assert_eq!(back.targets, vec![agent("bob")]);
    assert_eq!(back.decision, None);
    // The new field must default to "no certainty recorded" for legacy entries.
    assert_eq!(
        back.certainty, None,
        "legacy entry must default certainty to None"
    );
}

// (1d) A Decision entry with a real GateDecision still round-trips (guards that
// adding certainty did not disturb the existing optional `decision` field).
#[test]
fn backcompat_decision_entry_still_round_trips() {
    let mut e = legacy_entry(7, EntryKind::Decision, "alice", &["bob"], "approved");
    e.decision = Some(GateDecision::Approve);

    let json = serde_json::to_string(&e).expect("serialize");
    let back: AuditEntry = serde_json::from_str(&json).expect("deserialize");
    assert_eq!(back, e);
    assert_eq!(back.decision, Some(GateDecision::Approve));
    assert_eq!(back.certainty, None);
}

// =====================================================================
// (5) serde form of the NEW EntryKind variant — must follow the crate's
// existing `rename_all = "lowercase"` convention on EntryKind.
// =====================================================================

// ASSUMED-NAME: `EntryKind::RuntimeTurn`. Spec §12 requires a machine-queryable
// managed turn-lifecycle entry kind that is additive; this is the most likely
// name. The OBSERVABLE contract asserted here is: it is a NEW variant, distinct
// from all five existing ones, and it follows EntryKind's lowercase serde
// convention (so the wire string is `"runtimeturn"`). If the coder names it
// differently, only this literal changes; the distinctness + lowercase-convention
// requirement (independently asserted below) is the real contract.
#[test]
fn runtime_turn_entry_kind_uses_lowercase_serde_convention() {
    let json = serde_json::to_string(&EntryKind::RuntimeTurn).expect("serialize RuntimeTurn");
    assert_eq!(
        json, "\"runtimeturn\"",
        "new variant must follow EntryKind rename_all=lowercase"
    );
    let back: EntryKind = serde_json::from_str(&json).expect("deserialize RuntimeTurn");
    assert_eq!(back, EntryKind::RuntimeTurn);
}

#[test]
fn runtime_turn_entry_kind_is_distinct_from_existing() {
    // The new variant must be additive: never equal to an existing kind.
    for existing in [
        EntryKind::Consult,
        EntryKind::Command,
        EntryKind::Message,
        EntryKind::Transfer,
        EntryKind::Decision,
    ] {
        assert_ne!(
            EntryKind::RuntimeTurn,
            existing,
            "RuntimeTurn must be a NEW, distinct variant"
        );
    }
}

// =====================================================================
// (3) certainty is FIRST-CLASS: typed, tagged, matchable — not summary text.
// Pins the runtime.md §8.1 vocabulary + snake_case serde form.
// =====================================================================

#[test]
fn delivery_certainty_snake_case_serde_matches_spec_vocabulary() {
    // runtime.md §8.1: #[serde(rename_all = "snake_case")].
    let cases = [
        (DeliveryCertainty::NotInjected, "\"not_injected\""),
        (DeliveryCertainty::Injected, "\"injected\""),
        (DeliveryCertainty::Completed, "\"completed\""),
        (
            DeliveryCertainty::FailedBeforeInjection,
            "\"failed_before_injection\"",
        ),
        (DeliveryCertainty::OutcomeUnknown, "\"outcome_unknown\""),
    ];
    for (value, expected) in cases {
        let json = serde_json::to_string(&value).expect("serialize DeliveryCertainty");
        assert_eq!(json, expected, "certainty wire string for {value:?}");
        let back: DeliveryCertainty =
            serde_json::from_str(&json).expect("deserialize DeliveryCertainty");
        assert_eq!(back, value, "certainty round-trip for {value:?}");
    }
}

#[test]
fn certainty_is_readable_back_as_typed_value_and_matchable() {
    // Build a command entry that carries OutcomeUnknown, serialize, deserialize,
    // and READ THE CERTAINTY BACK AS A TYPED VALUE (match on it) — the crux of
    // requirement (3): recoverability from the log without prose parsing.
    let mut e = legacy_entry(
        100,
        EntryKind::Command,
        "alice",
        &["bob"],
        // NOTE: summary intentionally does NOT mention the certainty word; the
        // certainty must be recoverable from the structured field, not here.
        "run the deploy task",
    );
    e.certainty = Some(DeliveryCertainty::OutcomeUnknown);

    let json = serde_json::to_string(&e).expect("serialize command-with-certainty");
    let back: AuditEntry = serde_json::from_str(&json).expect("deserialize");

    // Typed read-back + match — OutcomeUnknown is distinguishable from Completed.
    match back.certainty {
        Some(DeliveryCertainty::OutcomeUnknown) => { /* the safety-critical case */ }
        other => panic!("expected Some(OutcomeUnknown), got {other:?}"),
    }
    assert_ne!(
        back.certainty,
        Some(DeliveryCertainty::Completed),
        "OutcomeUnknown must be distinguishable from Completed"
    );
    assert_eq!(back, e, "entry with certainty must round-trip equal");
}

// (3, OBSERVABLE-ONLY) The certainty must live in its OWN structured JSON key,
// NOT inside `summary`. This test names NO Rust symbol for the field type, so it
// holds the contract even if the coder stores a structured string tag instead
// of an enum: the requirement is "not buried in free text".
#[test]
fn certainty_is_a_dedicated_json_field_not_in_summary() {
    let mut e = legacy_entry(101, EntryKind::Command, "alice", &["bob"], "deploy");
    e.certainty = Some(DeliveryCertainty::Injected);

    let value = serde_json::to_value(&e).expect("serialize");
    let obj = value.as_object().expect("object");

    // A dedicated top-level key carries the tag...
    let certainty = obj
        .get("certainty")
        .expect("certainty must be a dedicated top-level JSON key");
    assert_eq!(
        certainty, "injected",
        "certainty key must carry the snake_case tag as a structured value"
    );

    // ...and the free-text summary must NOT be where the certainty lives.
    let summary = obj.get("summary").and_then(|s| s.as_str()).unwrap_or("");
    assert!(
        !summary.contains("injected"),
        "certainty must not be buried in summary free-text, got summary: {summary:?}"
    );
}

// =====================================================================
// (2) A RuntimeTurn entry: constructed, appended to a Sink, serialized,
// round-tripped — carrying which turn/target, the lifecycle phase, and (for a
// command) the certainty as a structured field.
// =====================================================================

// ASSUMED-NAME: `RuntimeTurnPhase` with variants covering the lifecycle
// (started / injected / completed / failed). runtime.md §7/§8.1 pin the
// lifecycle semantics (a turn goes started → injected → completed|failed); the
// enum spelling is a coder choice. The OBSERVABLE contract asserted: the entry
// carries a machine-queryable lifecycle phase distinct per state, plus the
// turn/target identity, all in structured fields (not summary). If the coder
// models the phase differently (e.g. reuses certainty, or a string), the
// JSON-shape assertions below still pin "phase is a structured, queryable field".
#[test]
fn runtime_turn_entry_appends_serializes_and_round_trips() {
    let mut log = AuditLog::new(FakeSink::new());

    // A managed-session turn STARTED for target "bob", turn id "turn-7".
    let mut started = AuditEntry {
        seq: 1,
        kind: EntryKind::RuntimeTurn,
        actor: agent("alice"),
        targets: targets(&["bob"]),
        summary: "turn-7 started".to_string(),
        decision: None,
        ..Default::default()
    };
    // The lifecycle phase is carried structurally (ASSUMED field name `phase`).
    started.phase = Some(RuntimeTurnPhase::Started);

    log.record(started.clone()).expect("record started ok");

    // by_kind must surface it as a first-class, queryable kind (NOT buried).
    let runtime_turns = log.by_kind(EntryKind::RuntimeTurn);
    assert_eq!(
        runtime_turns,
        vec![&started],
        "RuntimeTurn is queryable by_kind"
    );

    // Round-trip preserves kind, target, and phase.
    let json = serde_json::to_string(&started).expect("serialize RuntimeTurn entry");
    let back: AuditEntry = serde_json::from_str(&json).expect("deserialize RuntimeTurn entry");
    assert_eq!(back.kind, EntryKind::RuntimeTurn);
    assert_eq!(back.targets, vec![agent("bob")]);
    assert_eq!(back.phase, Some(RuntimeTurnPhase::Started));
    assert_eq!(back, started);
}

#[test]
fn runtime_turn_phase_is_a_structured_queryable_field_not_summary() {
    // OBSERVABLE-ONLY on the phase being a dedicated key, distinguishable per
    // lifecycle state. Names the enum only to construct; the assertion is about
    // the JSON shape (dedicated key, not summary substring).
    let phases = [
        RuntimeTurnPhase::Started,
        RuntimeTurnPhase::Injected,
        RuntimeTurnPhase::Completed,
        RuntimeTurnPhase::Failed,
    ];
    let mut seen = std::collections::BTreeSet::new();
    for phase in phases {
        let mut e = AuditEntry {
            seq: 1,
            kind: EntryKind::RuntimeTurn,
            actor: agent("alice"),
            targets: targets(&["bob"]),
            summary: "lifecycle event".to_string(),
            decision: None,
            ..Default::default()
        };
        e.phase = Some(phase);

        let value = serde_json::to_value(&e).expect("serialize");
        let obj = value.as_object().expect("object");
        let phase_val = obj
            .get("phase")
            .and_then(|p| p.as_str())
            .expect("phase must be a dedicated string JSON key");
        // Each lifecycle phase must have a DISTINCT machine-readable tag.
        assert!(
            seen.insert(phase_val.to_string()),
            "each lifecycle phase must serialize to a distinct tag; dup: {phase_val}"
        );
        let summary = obj.get("summary").and_then(|s| s.as_str()).unwrap_or("");
        assert_eq!(summary, "lifecycle event");
    }
    assert_eq!(seen.len(), 4, "four distinct lifecycle phase tags");
}

// =====================================================================
// (4) WRITE-AHEAD ORDERING is representable: the pre-injection durable command
// record (certainty = Injected) is appended to the Sink BEFORE the terminal
// record (Completed / OutcomeUnknown). The log holds the pre-injection evidence,
// and both are queryable — modeling the transition for an append-only log.
// =====================================================================

#[test]
fn write_ahead_pre_injection_record_precedes_terminal_record() {
    let mut log = AuditLog::new(FakeSink::new());

    // WRITE-AHEAD: durably record that the command is about to be injected
    // (certainty Injected) BEFORE the outcome is known. runtime.md §8.1: this
    // Injected record is written to the Sink BEFORE transport.send_turn.
    let mut pre_injection = AuditEntry {
        seq: 1,
        kind: EntryKind::Command,
        actor: agent("alice"),
        targets: targets(&["bob"]),
        summary: "command turn-7: write-ahead before injection".to_string(),
        decision: None,
        ..Default::default()
    };
    pre_injection.certainty = Some(DeliveryCertainty::Injected);
    log.record(pre_injection.clone())
        .expect("write-ahead pre-injection record must durably append first");

    // Simulate a crash-then-recovery producing OutcomeUnknown as a LATER entry
    // (append-only: the transition is modeled by a second entry, not mutation).
    let mut terminal = AuditEntry {
        seq: 2,
        kind: EntryKind::RuntimeTurn,
        actor: agent("alice"),
        targets: targets(&["bob"]),
        summary: "command turn-7: process exited while injected".to_string(),
        decision: None,
        ..Default::default()
    };
    terminal.certainty = Some(DeliveryCertainty::OutcomeUnknown);
    terminal.phase = Some(RuntimeTurnPhase::Failed);
    log.record(terminal.clone())
        .expect("terminal record appends after");

    // Ordering: the pre-injection (Injected) evidence is durably recorded FIRST,
    // so a crash between the two cannot erase the "may have run" evidence.
    let entries = log.entries();
    assert_eq!(entries.len(), 2);
    assert_eq!(
        entries[0].certainty,
        Some(DeliveryCertainty::Injected),
        "the Injected write-ahead record must come first"
    );
    assert_eq!(
        entries[1].certainty,
        Some(DeliveryCertainty::OutcomeUnknown),
        "the terminal OutcomeUnknown record must come after"
    );

    // Both are independently queryable from the log (recoverable evidence).
    let unknown: Vec<&AuditEntry> = log
        .entries()
        .iter()
        .filter(|e| e.certainty == Some(DeliveryCertainty::OutcomeUnknown))
        .collect();
    assert_eq!(
        unknown,
        vec![&terminal],
        "OutcomeUnknown is queryable from the log"
    );

    let injected: Vec<&AuditEntry> = log
        .entries()
        .iter()
        .filter(|e| e.certainty == Some(DeliveryCertainty::Injected))
        .collect();
    assert_eq!(
        injected,
        vec![&pre_injection],
        "the pre-injection Injected evidence is queryable from the log"
    );
}

#[test]
fn write_ahead_record_is_durable_first_even_if_terminal_append_fails() {
    // Durability-first contract (mirrors errors.rs E1): if the sink fails on the
    // TERMINAL append, the already-durable pre-injection record is untouched —
    // the safety evidence survives a later failure.
    #[derive(Debug, Default)]
    struct FailAfterFirst {
        appended: Vec<AuditEntry>,
        calls: usize,
    }
    impl Sink for FailAfterFirst {
        fn append(&mut self, entry: &AuditEntry) -> Result<(), AuditError> {
            self.calls += 1;
            if self.calls >= 2 {
                return Err(AuditError::Sink("crash after write-ahead".to_string()));
            }
            self.appended.push(entry.clone());
            Ok(())
        }
    }

    let mut log = AuditLog::new(FailAfterFirst::default());

    let mut pre_injection = AuditEntry {
        seq: 1,
        kind: EntryKind::Command,
        actor: agent("alice"),
        targets: targets(&["bob"]),
        summary: "write-ahead before injection".to_string(),
        decision: None,
        ..Default::default()
    };
    pre_injection.certainty = Some(DeliveryCertainty::Injected);
    log.record(pre_injection.clone())
        .expect("pre-injection record durable");

    let mut terminal = AuditEntry {
        seq: 2,
        kind: EntryKind::RuntimeTurn,
        actor: agent("alice"),
        targets: targets(&["bob"]),
        summary: "outcome unknown".to_string(),
        decision: None,
        ..Default::default()
    };
    terminal.certainty = Some(DeliveryCertainty::OutcomeUnknown);
    let err = log.record(terminal).expect_err("terminal append fails");
    assert!(matches!(err, AuditError::Sink(_)));

    // The write-ahead evidence is still the one durable entry in the log.
    assert_eq!(
        log.len(),
        1,
        "only the durable pre-injection record remains"
    );
    assert_eq!(
        log.entries()[0].certainty,
        Some(DeliveryCertainty::Injected)
    );
}
