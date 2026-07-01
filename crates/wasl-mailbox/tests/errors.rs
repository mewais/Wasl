//! Black-box error-path tests for `wasl_mailbox` per docs/specs/mailbox.md §Test plan.
//! Cases: E1 (unexpected replier), E2 (duplicate reply), E3 (CorrelationError Display/Error impl).

use wasl_mailbox::{Correlation, CorrelationError};
use wasl_proto::AgentId;

fn aid(s: &str) -> AgentId {
    AgentId(s.to_string())
}

// ---------------------------------------------------------------------
// E1 — record from a peer not in expected
// ---------------------------------------------------------------------

#[test]
fn e1_record_from_unexpected_peer_is_rejected_and_state_unchanged() {
    let a = aid("a");
    let b = aid("b");
    let stranger = aid("stranger");
    let mut corr = Correlation::new(&[a.clone(), b.clone()]);

    let result = corr.record(stranger.clone(), "hi".to_string());

    assert_eq!(result, Err(CorrelationError::UnexpectedReplier(stranger.clone())));

    // State unchanged: nothing recorded, still incomplete, pending untouched.
    assert!(!corr.is_complete());
    assert_eq!(corr.pending(), vec![a.clone(), b.clone()]);
    assert!(corr.replies().is_empty());
}

// ---------------------------------------------------------------------
// E2 — record twice from the same peer
// ---------------------------------------------------------------------

#[test]
fn e2_duplicate_reply_from_same_peer_is_rejected_only_first_stored() {
    let a = aid("a");
    let b = aid("b");
    let mut corr = Correlation::new(&[a.clone(), b.clone()]);

    corr.record(a.clone(), "first".to_string()).expect("first reply from a is expected");

    let second = corr.record(a.clone(), "second".to_string());

    assert_eq!(second, Err(CorrelationError::DuplicateReply(a.clone())));

    // Only one reply stored for a, and it is the first one.
    let replies = corr.replies();
    assert_eq!(replies.len(), 1);
    assert_eq!(replies[0].from, a);
    assert_eq!(replies[0].body, "first");

    // a is no longer pending (it replied once); b still pending; not complete.
    assert_eq!(corr.pending(), vec![b.clone()]);
    assert!(!corr.is_complete());
}

// ---------------------------------------------------------------------
// E3 — CorrelationError Display + std::error::Error
// ---------------------------------------------------------------------

#[test]
fn e3_correlation_error_display_is_nonempty_distinct_and_carries_id() {
    let id = aid("peer-x");

    let unexpected = CorrelationError::UnexpectedReplier(id.clone());
    let duplicate = CorrelationError::DuplicateReply(id.clone());

    let unexpected_msg = unexpected.to_string();
    let duplicate_msg = duplicate.to_string();

    assert!(!unexpected_msg.is_empty());
    assert!(!duplicate_msg.is_empty());
    assert_ne!(unexpected_msg, duplicate_msg, "Display must differ per variant");

    assert!(
        unexpected_msg.contains("peer-x"),
        "UnexpectedReplier Display must carry the id, got: {unexpected_msg}"
    );
    assert!(
        duplicate_msg.contains("peer-x"),
        "DuplicateReply Display must carry the id, got: {duplicate_msg}"
    );

    // std::error::Error impl: usable as a trait object, source() defaults to None.
    let as_error: &dyn std::error::Error = &unexpected;
    assert!(as_error.source().is_none());
    let as_error: &dyn std::error::Error = &duplicate;
    assert!(as_error.source().is_none());
}
