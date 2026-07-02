//! Happy-path black-box suite for `wasl_server` (spec §Test plan S1-S9).

mod common;

use common::{FakeSink, FakeSurfacer};

use wasl_audit::EntryKind;
use wasl_gate::{ClientCaps, GateMode, GateOutcome};
use wasl_proto::{AgentId, ApprovalToken, GateDecision, GateKind};
use wasl_server::{Gatekeeper, Verdict};

fn from_agent() -> AgentId {
    AgentId("alice".to_string())
}

fn targets() -> Vec<AgentId> {
    vec![AgentId("bob".to_string()), AgentId("carol".to_string())]
}

fn caps(elicitation: bool, external: bool) -> ClientCaps {
    ClientCaps {
        elicitation,
        external,
    }
}

/// S1 — `mode()` selects Elicitation / External / PrepareCommit per caps.
#[test]
fn s1_mode_selection() {
    let elic = Gatekeeper::new(
        caps(true, false),
        FakeSurfacer::new(GateOutcome::Approved),
        FakeSink::ok(),
    );
    assert_eq!(elic.mode(), GateMode::Elicitation);

    let elic_both = Gatekeeper::new(
        caps(true, true),
        FakeSurfacer::new(GateOutcome::Approved),
        FakeSink::ok(),
    );
    assert_eq!(
        elic_both.mode(),
        GateMode::Elicitation,
        "elicitation takes priority over external"
    );

    let external = Gatekeeper::new(
        caps(false, true),
        FakeSurfacer::new(GateOutcome::Approved),
        FakeSink::ok(),
    );
    assert_eq!(external.mode(), GateMode::External);

    let prepare = Gatekeeper::new(
        caps(false, false),
        FakeSurfacer::new(GateOutcome::Approved),
        FakeSink::ok(),
    );
    assert_eq!(prepare.mode(), GateMode::PrepareCommit);
}

/// S2 — Tier-1 approve: Approved → Proceed, one Decision audit entry with actor/targets
/// and `Some(Approve)`; the surfacer is called exactly once with `GateMode::Elicitation`.
#[test]
fn s2_tier1_approve() {
    let surfacer = FakeSurfacer::new(GateOutcome::Approved);
    let surf_log = surfacer.log();
    let mut gk = Gatekeeper::new(caps(true, false), surfacer, FakeSink::ok());

    let verdict = gk
        .authorize(
            "req-1".to_string(),
            GateKind::Consult,
            from_agent(),
            targets(),
            "preview text".to_string(),
            ApprovalToken("tok-1".to_string()),
            7,
        )
        .expect("authorize should succeed");

    assert_eq!(verdict, Verdict::Proceed);
    assert_eq!(gk.pending_count(), 0);

    // Surfacer was called once, with the Elicitation mode.
    let log = surf_log.borrow();
    assert_eq!(log.calls, 1);
    assert_eq!(log.last_mode, Some(GateMode::Elicitation));

    let entries = gk.audit_entries();
    assert_eq!(entries.len(), 1, "exactly one Decision entry recorded");
    let entry = &entries[0];
    assert_eq!(entry.kind, EntryKind::Decision);
    assert_eq!(entry.actor, from_agent());
    assert_eq!(entry.targets, targets());
    assert_eq!(entry.seq, 7);
    assert_eq!(entry.decision, Some(GateDecision::Approve));
}

/// S3 — Tier-1 deny: Denied{reason} → Verdict::Denied{reason}, audit `Some(Deny{reason})`.
#[test]
fn s3_tier1_deny() {
    let reason = "not allowed".to_string();
    let mut gk = Gatekeeper::new(
        caps(true, false),
        FakeSurfacer::new(GateOutcome::Denied {
            reason: reason.clone(),
        }),
        FakeSink::ok(),
    );

    let verdict = gk
        .authorize(
            "req-3".to_string(),
            GateKind::Command,
            from_agent(),
            targets(),
            "preview".to_string(),
            ApprovalToken("tok-3".to_string()),
            3,
        )
        .expect("authorize should succeed");

    assert_eq!(
        verdict,
        Verdict::Denied {
            reason: reason.clone()
        }
    );

    let entries = gk.audit_entries();
    assert_eq!(entries.len(), 1);
    assert_eq!(entries[0].kind, EntryKind::Decision);
    assert_eq!(entries[0].decision, Some(GateDecision::Deny { reason }));
}

/// S4 — Tier-1 dismiss: Dismissed → Verdict::Dismissed, audit entry with `decision == None`.
#[test]
fn s4_tier1_dismiss() {
    let mut gk = Gatekeeper::new(
        caps(true, false),
        FakeSurfacer::new(GateOutcome::Dismissed),
        FakeSink::ok(),
    );

    let verdict = gk
        .authorize(
            "req-4".to_string(),
            GateKind::Message,
            from_agent(),
            targets(),
            "preview".to_string(),
            ApprovalToken("tok-4".to_string()),
            4,
        )
        .expect("authorize should succeed");

    assert_eq!(verdict, Verdict::Dismissed);

    let entries = gk.audit_entries();
    assert_eq!(entries.len(), 1, "dismiss is recorded but has no decision");
    assert_eq!(entries[0].kind, EntryKind::Decision);
    assert_eq!(entries[0].decision, None);
    assert_eq!(entries[0].actor, from_agent());
    assert_eq!(entries[0].targets, targets());
}

/// S5 — Tier-3 (external) approve: the surfacer is called with `GateMode::External`,
/// Approved → Proceed, audit `Some(Approve)`. Asserts the exact mode passed to the surfacer.
#[test]
fn s5_tier3_external_approve() {
    let surfacer = FakeSurfacer::new(GateOutcome::Approved);
    let surf_log = surfacer.log();
    let mut gk = Gatekeeper::new(caps(false, true), surfacer, FakeSink::ok());

    assert_eq!(gk.mode(), GateMode::External);

    let verdict = gk
        .authorize(
            "req-5".to_string(),
            GateKind::Transfer,
            from_agent(),
            targets(),
            "preview".to_string(),
            ApprovalToken("tok-5".to_string()),
            5,
        )
        .expect("authorize should succeed");

    assert_eq!(verdict, Verdict::Proceed);

    // The surfacer received exactly the External mode.
    let log = surf_log.borrow();
    assert_eq!(log.calls, 1);
    assert_eq!(log.last_mode, Some(GateMode::External));

    let entries = gk.audit_entries();
    assert_eq!(entries.len(), 1);
    assert_eq!(entries[0].kind, EntryKind::Decision);
    assert_eq!(entries[0].decision, Some(GateDecision::Approve));
}

/// S6 — Tier-2 prepare: authorize returns `Pending{token}`, `pending_count()==1`,
/// the surfacer was NOT called, and NO audit entry was recorded.
#[test]
fn s6_tier2_prepare() {
    let token = ApprovalToken("tok-6".to_string());
    let surfacer = FakeSurfacer::new(GateOutcome::Approved);
    let surf_log = surfacer.log();
    let mut gk = Gatekeeper::new(caps(false, false), surfacer, FakeSink::ok());

    let verdict = gk
        .authorize(
            "req-6".to_string(),
            GateKind::Consult,
            from_agent(),
            targets(),
            "preview".to_string(),
            token.clone(),
            6,
        )
        .expect("authorize should succeed");

    assert_eq!(verdict, Verdict::Pending { token });
    assert_eq!(gk.pending_count(), 1);
    assert!(
        gk.audit_entries().is_empty(),
        "no decision has been made yet"
    );
    assert_eq!(
        surf_log.borrow().calls,
        0,
        "Tier-2 must not surface synchronously"
    );
}

/// S7 — Tier-2 commit approve: after a prepare, `commit(token, Approve)` → Proceed,
/// `pending_count()==0`, audit has one `Some(Approve)` entry with the original kind/from/targets.
#[test]
fn s7_tier2_commit_approve() {
    let token = ApprovalToken("tok-7".to_string());
    let mut gk = Gatekeeper::new(
        caps(false, false),
        FakeSurfacer::new(GateOutcome::Approved),
        FakeSink::ok(),
    );

    let pending = gk
        .authorize(
            "req-7".to_string(),
            GateKind::Command,
            from_agent(),
            targets(),
            "preview".to_string(),
            token.clone(),
            10,
        )
        .expect("prepare should succeed");
    assert_eq!(
        pending,
        Verdict::Pending {
            token: token.clone()
        }
    );
    assert_eq!(gk.pending_count(), 1);

    let verdict = gk
        .commit(&token, GateDecision::Approve, 11)
        .expect("commit should succeed");

    assert_eq!(verdict, Verdict::Proceed);
    assert_eq!(gk.pending_count(), 0);

    let entries = gk.audit_entries();
    assert_eq!(
        entries.len(),
        1,
        "commit records exactly one Decision entry"
    );
    let entry = &entries[0];
    assert_eq!(entry.kind, EntryKind::Decision);
    assert_eq!(entry.actor, from_agent(), "original from preserved");
    assert_eq!(entry.targets, targets(), "original targets preserved");
    assert_eq!(entry.seq, 11);
    assert_eq!(entry.decision, Some(GateDecision::Approve));
}

/// S8 — Tier-2 commit deny: `commit(token, Deny{reason})` → Verdict::Denied{reason} + audit Some(Deny).
#[test]
fn s8_tier2_commit_deny() {
    let token = ApprovalToken("tok-8".to_string());
    let reason = "committed denial".to_string();
    let mut gk = Gatekeeper::new(
        caps(false, false),
        FakeSurfacer::new(GateOutcome::Approved),
        FakeSink::ok(),
    );

    gk.authorize(
        "req-8".to_string(),
        GateKind::Message,
        from_agent(),
        targets(),
        "preview".to_string(),
        token.clone(),
        20,
    )
    .expect("prepare should succeed");

    let verdict = gk
        .commit(
            &token,
            GateDecision::Deny {
                reason: reason.clone(),
            },
            21,
        )
        .expect("commit should succeed");

    assert_eq!(
        verdict,
        Verdict::Denied {
            reason: reason.clone()
        }
    );
    assert_eq!(gk.pending_count(), 0);

    let entries = gk.audit_entries();
    assert_eq!(entries.len(), 1);
    assert_eq!(entries[0].kind, EntryKind::Decision);
    assert_eq!(entries[0].decision, Some(GateDecision::Deny { reason }));
}

/// S9 — serde: `Verdict` round-trips for each of the four variants,
/// with the documented `tag = "verdict"`, snake_case tags.
#[test]
fn s9_verdict_serde_round_trip() {
    let cases = vec![
        Verdict::Proceed,
        Verdict::Denied {
            reason: "nope".to_string(),
        },
        Verdict::Dismissed,
        Verdict::Pending {
            token: ApprovalToken("tok-9".to_string()),
        },
    ];

    for v in cases {
        let json = serde_json::to_string(&v).expect("serialize");
        let back: Verdict = serde_json::from_str(&json).expect("deserialize");
        assert_eq!(back, v, "round-trip must preserve {v:?}");
    }

    // Assert the on-the-wire tags are exactly the snake_case verdict discriminants.
    assert_eq!(
        serde_json::to_value(Verdict::Proceed).unwrap(),
        serde_json::json!({ "verdict": "proceed" })
    );
    assert_eq!(
        serde_json::to_value(Verdict::Dismissed).unwrap(),
        serde_json::json!({ "verdict": "dismissed" })
    );
    assert_eq!(
        serde_json::to_value(Verdict::Denied {
            reason: "x".to_string()
        })
        .unwrap(),
        serde_json::json!({ "verdict": "denied", "reason": "x" })
    );
    assert_eq!(
        serde_json::to_value(Verdict::Pending {
            token: ApprovalToken("t".to_string())
        })
        .unwrap(),
        serde_json::json!({ "verdict": "pending", "token": "t" })
    );
}
