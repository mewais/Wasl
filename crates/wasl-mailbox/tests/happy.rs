//! Black-box happy-path tests for `wasl_mailbox` per docs/specs/mailbox.md §Test plan.
//! Cases: M1 (fan_out), M2 (Mailbox), M3/M4 (Correlation), M5 (serde round-trip).

use wasl_mailbox::{fan_out, Correlation, Mailbox};
use wasl_proto::{AgentId, Envelope, EnvelopeKind, Reply};

fn aid(s: &str) -> AgentId {
    AgentId(s.to_string())
}

// ---------------------------------------------------------------------
// M1 — fan_out
// ---------------------------------------------------------------------

#[test]
fn m1_fan_out_many_recipients_builds_one_envelope_each() {
    let from = aid("orchestrator");
    let recipients = vec![aid("r0"), aid("r1"), aid("r2")];

    let envelopes = fan_out(
        "base",
        &from,
        EnvelopeKind::Command,
        "do the thing",
        &recipients,
    );

    assert_eq!(envelopes.len(), 3);

    assert_eq!(envelopes[0].id, "base:0");
    assert_eq!(envelopes[1].id, "base:1");
    assert_eq!(envelopes[2].id, "base:2");

    for (i, env) in envelopes.iter().enumerate() {
        assert_eq!(
            env.to,
            vec![recipients[i].clone()],
            "envelope {i} must address only its recipient"
        );
        assert_eq!(env.from, from, "envelope {i} shares from");
        assert_eq!(env.kind, EnvelopeKind::Command, "envelope {i} shares kind");
        assert_eq!(env.body, "do the thing", "envelope {i} shares body");
    }
}

#[test]
fn m1_fan_out_empty_recipients_yields_empty_vec() {
    let from = aid("orchestrator");
    let recipients: Vec<AgentId> = vec![];

    let envelopes = fan_out("base", &from, EnvelopeKind::Message, "hello", &recipients);

    assert!(envelopes.is_empty());
    assert_eq!(envelopes, Vec::<Envelope>::new());
}

#[test]
fn m1_fan_out_single_recipient_yields_one_envelope_indexed_zero() {
    let from = aid("orchestrator");
    let recipients = vec![aid("solo")];

    let envelopes = fan_out(
        "base",
        &from,
        EnvelopeKind::Consult,
        "question?",
        &recipients,
    );

    assert_eq!(envelopes.len(), 1);
    assert_eq!(envelopes[0].id, "base:0");
    assert_eq!(envelopes[0].to, vec![aid("solo")]);
    assert_eq!(envelopes[0].from, from);
    assert_eq!(envelopes[0].kind, EnvelopeKind::Consult);
    assert_eq!(envelopes[0].body, "question?");
}

// ---------------------------------------------------------------------
// M2 — Mailbox
// ---------------------------------------------------------------------

fn envelope(id: &str, to: &AgentId) -> Envelope {
    Envelope {
        id: id.to_string(),
        kind: EnvelopeKind::Message,
        from: aid("sender"),
        to: vec![to.clone()],
        body: format!("body-{id}"),
    }
}

#[test]
fn m2_mailbox_deliver_len_peek_drain_preserve_fifo_order() {
    let owner = aid("owner");
    let mut mailbox = Mailbox::new(owner.clone());

    assert_eq!(mailbox.owner(), &owner);
    assert_eq!(mailbox.len(), 0);
    assert!(mailbox.is_empty());
    assert_eq!(mailbox.peek(), &[] as &[Envelope]);

    let e0 = envelope("m0", &owner);
    let e1 = envelope("m1", &owner);
    let e2 = envelope("m2", &owner);

    mailbox.deliver(e0.clone());
    mailbox.deliver(e1.clone());
    mailbox.deliver(e2.clone());

    assert_eq!(mailbox.len(), 3);
    assert!(!mailbox.is_empty());
    assert_eq!(mailbox.peek(), &[e0.clone(), e1.clone(), e2.clone()]);
    // peek must not drain.
    assert_eq!(mailbox.len(), 3);

    let drained = mailbox.drain();
    assert_eq!(drained, vec![e0, e1, e2]);

    // drain empties the inbox.
    assert_eq!(mailbox.len(), 0);
    assert!(mailbox.is_empty());
    assert_eq!(mailbox.peek(), &[] as &[Envelope]);
    assert_eq!(mailbox.drain(), Vec::<Envelope>::new());
}

// ---------------------------------------------------------------------
// M3 — Correlation happy path: completion, replies order, pending shrinking
// ---------------------------------------------------------------------

#[test]
fn m3_correlation_completes_and_tracks_pending_and_replies_in_order() {
    let a = aid("a");
    let b = aid("b");
    let mut corr = Correlation::new(&[a.clone(), b.clone()]);

    assert!(!corr.is_complete());
    assert_eq!(corr.pending(), vec![a.clone(), b.clone()]);
    assert_eq!(corr.replies(), Vec::<Reply>::new());

    corr.record(a.clone(), "reply-a".to_string())
        .expect("a is expected");
    assert!(!corr.is_complete());
    assert_eq!(corr.pending(), vec![b.clone()]);
    assert_eq!(
        corr.replies(),
        vec![Reply {
            from: a.clone(),
            body: "reply-a".to_string()
        }]
    );

    corr.record(b.clone(), "reply-b".to_string())
        .expect("b is expected");
    assert!(corr.is_complete());
    assert_eq!(corr.pending(), Vec::<AgentId>::new());
    assert_eq!(
        corr.replies(),
        vec![
            Reply {
                from: a.clone(),
                body: "reply-a".to_string()
            },
            Reply {
                from: b.clone(),
                body: "reply-b".to_string()
            },
        ]
    );
}

// ---------------------------------------------------------------------
// M4 — Correlation::new dedups expected, preserving first-appearance order
// ---------------------------------------------------------------------

#[test]
fn m4_correlation_new_dedups_expected_preserving_first_appearance_order() {
    let a = aid("a");
    let b = aid("b");
    let mut corr = Correlation::new(&[a.clone(), a.clone(), b.clone()]);

    assert_eq!(corr.pending(), vec![a.clone(), b.clone()]);

    corr.record(a.clone(), "once".to_string())
        .expect("a is expected");

    // Recording 'a' once fully satisfies the deduped expectation for a.
    assert_eq!(corr.pending(), vec![b.clone()]);
    assert!(!corr.is_complete());
    assert_eq!(
        corr.replies(),
        vec![Reply {
            from: a.clone(),
            body: "once".to_string()
        }]
    );

    corr.record(b.clone(), "second".to_string())
        .expect("b is expected");
    assert!(corr.is_complete());
    assert_eq!(corr.pending(), Vec::<AgentId>::new());
}

// ---------------------------------------------------------------------
// M5 — serde round-trip sanity for Envelope/Reply (from fan_out output)
// ---------------------------------------------------------------------

#[test]
fn m5_fan_out_envelope_and_reply_serde_round_trip() {
    let from = aid("orchestrator");
    let recipients = vec![aid("r0"), aid("r1")];
    let envelopes = fan_out("base", &from, EnvelopeKind::Command, "payload", &recipients);

    for env in &envelopes {
        let json = serde_json::to_string(env).expect("envelope must serialize");
        let round_tripped: Envelope =
            serde_json::from_str(&json).expect("envelope must deserialize");
        assert_eq!(&round_tripped, env);
    }

    let reply = Reply {
        from: aid("r0"),
        body: "ack".to_string(),
    };
    let json = serde_json::to_string(&reply).expect("reply must serialize");
    let round_tripped: Reply = serde_json::from_str(&json).expect("reply must deserialize");
    assert_eq!(round_tripped, reply);
}
