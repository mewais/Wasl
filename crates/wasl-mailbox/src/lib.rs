//! wasl-mailbox — message routing (WM): inbox/outbox, multicast fan-out and
//! reply fan-in correlation. Pure; the daemon does the actual delivery IO.
//! See `docs/specs/mailbox.md`.

use std::fmt;

use wasl_proto::{AgentId, Envelope, EnvelopeKind, Reply};

// ---------------------------------------------------------------------
// 1. Fan-out
// ---------------------------------------------------------------------

/// Build one [`Envelope`] per recipient for a multicast/broadcast send. Each
/// envelope's `to` is the single recipient; `id = format!("{base_id}:{i}")`
/// for recipient index `i` (stable, ordered); `from`/`kind`/`body` shared.
/// Empty `recipients` yields an empty vec.
pub fn fan_out(
    base_id: &str,
    from: &AgentId,
    kind: EnvelopeKind,
    body: &str,
    recipients: &[AgentId],
) -> Vec<Envelope> {
    recipients
        .iter()
        .enumerate()
        .map(|(i, recipient)| Envelope {
            id: format!("{base_id}:{i}"),
            kind,
            from: from.clone(),
            to: vec![recipient.clone()],
            body: body.to_string(),
        })
        .collect()
}

// ---------------------------------------------------------------------
// 2. Mailbox
// ---------------------------------------------------------------------

/// Per-owner inbox. FIFO delivery order is preserved by `peek`/`drain`.
#[derive(Debug, Clone)]
pub struct Mailbox {
    owner: AgentId,
    inbox: Vec<Envelope>,
}

impl Mailbox {
    /// Create a new, empty mailbox for `owner`.
    pub fn new(owner: AgentId) -> Self {
        Self {
            owner,
            inbox: Vec::new(),
        }
    }

    /// The owning agent.
    pub fn owner(&self) -> &AgentId {
        &self.owner
    }

    /// Push an envelope onto the inbox (FIFO).
    pub fn deliver(&mut self, env: Envelope) {
        self.inbox.push(env);
    }

    /// Number of envelopes currently in the inbox.
    pub fn len(&self) -> usize {
        self.inbox.len()
    }

    /// Whether the inbox is empty.
    pub fn is_empty(&self) -> bool {
        self.inbox.is_empty()
    }

    /// View the inbox without draining it.
    pub fn peek(&self) -> &[Envelope] {
        &self.inbox
    }

    /// Take and clear the inbox, preserving delivery order.
    pub fn drain(&mut self) -> Vec<Envelope> {
        std::mem::take(&mut self.inbox)
    }
}

// ---------------------------------------------------------------------
// 3. Correlation
// ---------------------------------------------------------------------

/// Errors from [`Correlation::record`].
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum CorrelationError {
    /// The replier is not among the expected peers.
    UnexpectedReplier(AgentId),
    /// The peer already recorded a reply.
    DuplicateReply(AgentId),
}

impl fmt::Display for CorrelationError {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            CorrelationError::UnexpectedReplier(id) => {
                write!(
                    f,
                    "unexpected replier: {} is not among the expected peers",
                    id.as_str()
                )
            }
            CorrelationError::DuplicateReply(id) => {
                write!(f, "duplicate reply: {} already replied", id.as_str())
            }
        }
    }
}

impl std::error::Error for CorrelationError {}

/// Tracks a pending multicast awaiting a reply from each expected peer, and
/// correlates the replies as they arrive.
#[derive(Debug, Clone)]
pub struct Correlation {
    expected: Vec<AgentId>,
    replies: Vec<Reply>,
}

impl Correlation {
    /// A pending multicast awaiting a reply from each expected peer. The
    /// `expected` set is deduped, preserving first-appearance order.
    pub fn new(expected: &[AgentId]) -> Self {
        let mut deduped: Vec<AgentId> = Vec::with_capacity(expected.len());
        for id in expected {
            if !deduped.contains(id) {
                deduped.push(id.clone());
            }
        }
        Self {
            expected: deduped,
            replies: Vec::new(),
        }
    }

    /// Record a reply from `from`. Errors (state left unchanged) if `from`
    /// is not expected, or if `from` already replied.
    pub fn record(&mut self, from: AgentId, body: String) -> Result<(), CorrelationError> {
        if !self.expected.contains(&from) {
            return Err(CorrelationError::UnexpectedReplier(from));
        }
        if self.replies.iter().any(|r| r.from == from) {
            return Err(CorrelationError::DuplicateReply(from));
        }
        self.replies.push(Reply { from, body });
        Ok(())
    }

    /// Whether every expected peer has replied.
    pub fn is_complete(&self) -> bool {
        self.pending().is_empty()
    }

    /// Expected peers that haven't replied yet, in expected order.
    pub fn pending(&self) -> Vec<AgentId> {
        self.expected
            .iter()
            .filter(|id| !self.replies.iter().any(|r| &&r.from == id))
            .cloned()
            .collect()
    }

    /// Collected replies, in the order recorded.
    pub fn replies(&self) -> Vec<Reply> {
        self.replies.clone()
    }
}
