//! Why a request ended: the one vocabulary for how a spawn request, or a handoff gate, stops
//! waiting for a human. Wasl owns it. The code is carried everywhere the ending is told: the
//! spawn's journal record ([`crate::spawn::SpawnEnded`], the payload of
//! [`topics::SPAWNS`](crate::topics::SPAWNS)), the reason on the gate resolution the session
//! records ([`End::gate_reason`], read by whoever follows the session), and the node's
//! diagnostics. The words for people belong to the UI; the human-facing *detail*, where there
//! is one, is the same sentence the agent's tool result carries (what the gate already showed:
//! names, and what the human or the supervisor said).

use serde::{Deserialize, Serialize};
use wasit_core::GateReason;

use crate::codes;

/// The longest detail carried with an ending, in characters (the session's bound for a gate
/// resolution's detail).
pub const END_DETAIL_MAX: usize = GateReason::DETAIL_MAX;

/// How a request ended (a fixed set of machine words).
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash, Serialize, Deserialize)]
#[cfg_attr(feature = "ts", derive(ts_rs::TS))]
#[serde(rename_all = "snake_case")]
pub enum EndCode {
    /// Spawn: the agent was started.
    Launched,
    /// Spawn: the human denied it (also an answer that is neither allow nor deny).
    Denied,
    /// Spawn: the asking turn ended before anyone answered, so its `spawn_agent` call can no
    /// longer receive a result.
    TurnEnded,
    /// Spawn or handoff: the asking (delegating) turn was interrupted before the gate was
    /// answered.
    TurnInterrupted,
    /// Spawn: the agent's tool call was cancelled. Handoff: the task was cancelled (by the
    /// sender, a supervisor or the human).
    Cancelled,
    /// Spawn: nothing waited for the call any more (the tool process exited or its connection
    /// closed) and nobody came back within the grace period.
    CallerGone,
    /// Spawn or handoff: the asking session's process ended (or was lost, or stopped) before
    /// the gate was answered.
    SessionEnded,
    /// Spawn or handoff: the asking session was deleted, or its events could not be followed.
    SessionGone,
    /// Spawn or handoff: the gate was taken down by somebody other than this node, with the
    /// request still waiting.
    GateGone,
    /// Spawn or handoff: the deadline passed before the request was answered (or, for a spawn,
    /// before it was approved or even asked for).
    DeadlinePassed,
    /// Spawn: it was approved, but the task it was for had ended meanwhile.
    TaskEnded,
    /// Spawn: the supervisor refused the plan (policy, limits, unknown or unavailable host or
    /// driver, a name in use, a paused workstream, …); `cause` is its error code.
    Refused,
    /// Spawn: it was approved but the supervisor could not launch it; `cause` is the failure's
    /// error code (`spawn_unavailable` when the supervisor stayed unreachable).
    LaunchFailed,
    /// Spawn or handoff: the gate could not be raised on the asking session.
    GateUnavailable,
    /// Handoff: the task was decided without the gate (a supervisor or an elicitation
    /// answered), so the gate was taken down.
    DecidedElsewhere,
    /// The record is from before endings were recorded: only its error code is known.
    Unrecorded,
}

impl EndCode {
    /// The machine word, as serialized.
    pub fn as_str(&self) -> &'static str {
        match self {
            EndCode::Launched => "launched",
            EndCode::Denied => "denied",
            EndCode::TurnEnded => "turn_ended",
            EndCode::TurnInterrupted => "turn_interrupted",
            EndCode::Cancelled => "cancelled",
            EndCode::CallerGone => "caller_gone",
            EndCode::SessionEnded => "session_ended",
            EndCode::SessionGone => "session_gone",
            EndCode::GateGone => "gate_gone",
            EndCode::DeadlinePassed => "deadline_passed",
            EndCode::TaskEnded => "task_ended",
            EndCode::Refused => "refused",
            EndCode::LaunchFailed => "launch_failed",
            EndCode::GateUnavailable => "gate_unavailable",
            EndCode::DecidedElsewhere => "decided_elsewhere",
            EndCode::Unrecorded => "unrecorded",
        }
    }

    /// The error code a spawn request that ended this way fails the agent's `spawn_agent`
    /// call with, when no more specific one came from the supervisor (see [`End::rpc_code`]).
    pub fn spawn_error_code(&self) -> &'static str {
        match self {
            EndCode::Denied => codes::SPAWN_DENIED,
            EndCode::DeadlinePassed | EndCode::TaskEnded => codes::SPAWN_EXPIRED,
            EndCode::Refused => codes::SPAWN_REFUSED,
            EndCode::LaunchFailed | EndCode::GateUnavailable => codes::SPAWN_UNAVAILABLE,
            _ => codes::SPAWN_WITHDRAWN,
        }
    }
}

/// How a request ended: the code, and for the endings the supervisor decides (`refused`,
/// `launch_failed`) the supervisor's own error code.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[cfg_attr(feature = "ts", derive(ts_rs::TS))]
pub struct End {
    pub code: EndCode,
    /// The supervisor's error code for `refused` and `launch_failed` (a machine word).
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub cause: Option<String>,
}

impl End {
    pub fn new(code: EndCode) -> Self {
        End { code, cause: None }
    }

    pub fn caused_by(code: EndCode, cause: impl Into<String>) -> Self {
        End {
            code,
            cause: Some(cause.into()),
        }
    }

    /// The ending of a record written before endings were recorded.
    pub fn unrecorded() -> Self {
        End::new(EndCode::Unrecorded)
    }

    /// The error code of the spawn request's failure: the supervisor's when it gave one, else
    /// the one for this code.
    pub fn rpc_code(&self) -> String {
        match &self.cause {
            Some(c) => c.clone(),
            None => self.code.spawn_error_code().to_string(),
        }
    }

    /// The gate resolution this ending stands for: the code, and `detail` (bounded) as the
    /// human-facing text.
    pub fn gate_reason(&self, detail: &str) -> GateReason {
        let reason = GateReason::new(self.code.as_str());
        match bounded(detail) {
            Some(d) => reason.with_detail(d),
            None => reason,
        }
    }
}

/// `text` cut to [`END_DETAIL_MAX`] characters (the last one an ellipsis when it was cut),
/// `None` when it is empty.
pub fn bounded(text: &str) -> Option<String> {
    let text = text.trim();
    if text.is_empty() {
        return None;
    }
    if text.chars().count() <= END_DETAIL_MAX {
        return Some(text.to_string());
    }
    let (i, _) = text.char_indices().nth(END_DETAIL_MAX - 1)?;
    let mut cut = text[..i].trim_end().to_string();
    cut.push('…');
    Some(cut)
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn codes_serialize_as_their_words_and_the_detail_is_bounded() {
        for c in [
            EndCode::Launched,
            EndCode::Denied,
            EndCode::TurnEnded,
            EndCode::TurnInterrupted,
            EndCode::Cancelled,
            EndCode::CallerGone,
            EndCode::SessionEnded,
            EndCode::SessionGone,
            EndCode::GateGone,
            EndCode::DeadlinePassed,
            EndCode::TaskEnded,
            EndCode::Refused,
            EndCode::LaunchFailed,
            EndCode::GateUnavailable,
            EndCode::DecidedElsewhere,
            EndCode::Unrecorded,
        ] {
            assert_eq!(serde_json::to_value(c).unwrap(), c.as_str());
            assert!(End::new(c).gate_reason("x").check().is_ok());
        }
        let long = "é".repeat(END_DETAIL_MAX + 50);
        let d = bounded(&long).unwrap();
        assert_eq!(d.chars().count(), END_DETAIL_MAX);
        assert_eq!(
            bounded(&"é".repeat(END_DETAIL_MAX))
                .unwrap()
                .chars()
                .count(),
            END_DETAIL_MAX
        );
        assert!(End::new(EndCode::Denied).gate_reason(&long).check().is_ok());
        assert_eq!(bounded("  "), None);
        assert_eq!(
            End::caused_by(EndCode::Refused, "spawn_limit").rpc_code(),
            "spawn_limit"
        );
        assert_eq!(End::new(EndCode::CallerGone).rpc_code(), "spawn_withdrawn");
    }
}
