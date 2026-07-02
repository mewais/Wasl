//! wasl-mcp `gating` (W4) — the pure seams of the three-tier gate wiring: real
//! client-capability detection, tier selection by the configured authority,
//! the rmcp <-> `wasl_proto` elicitation-action type conversion, and the
//! `GateOutcome` -> `GateDecision` mapping the Tier-2 ledger commit needs.
//! No IO, no tokio — everything here is a plain function over plain data, unit
//! tested without a transport (`tests/gating_unit.rs`). See
//! `docs/specs/mcp-gating.md` §§1-6.

use wasl_gate::{choose_mode, ClientCaps, GateMode, GateOutcome};
use wasl_proto::GateDecision;

// ---------------------------------------------------------------------
// 1. GateAuthority — which tier(s) may decide, in priority order
// ---------------------------------------------------------------------

/// Which authority may decide a gate, in priority order. Chosen ONCE per call
/// from config + detected caps (see [`select_mode`]).
#[derive(Debug, Clone, Copy, PartialEq, Eq, Default)]
pub enum GateAuthority {
    /// Tier-1 or nothing: elicitation-capable client -> Elicitation; otherwise
    /// auto-deny. EXACTLY today's behavior; the default.
    #[default]
    Elicitation,
    /// Tier-1 -> Tier-3 -> Tier-2: delegate to `wasl_gate::choose_mode`
    /// verbatim (elicitation > external > prepare_commit).
    ElicitationFirst,
    /// Tier-3 -> Tier-1 -> Tier-2: prefer the operator even for
    /// elicitation-capable clients (central approval desks).
    ExternalFirst,
    /// Always Tier-2: every call defers via prepare->commit regardless of
    /// caps.
    PrepareCommit,
}

// ---------------------------------------------------------------------
// 2. detect_caps — the real rmcp 0.9 client-capability surface
// ---------------------------------------------------------------------

/// The REAL [`ClientCaps`] for this call: elicitation from the client's
/// declared MCP capabilities (`None` peer_info — pre-initialize — counts as
/// false, matching rmcp's own `Peer::supports_elicitation` rule), external =
/// an `OperatorHub` is attached to this server.
pub fn detect_caps(
    client: Option<&rmcp::model::ClientInfo>,
    operator_attached: bool,
) -> ClientCaps {
    ClientCaps {
        elicitation: client
            .map(|c| c.capabilities.elicitation.is_some())
            .unwrap_or(false),
        external: operator_attached,
    }
}

// ---------------------------------------------------------------------
// 3. select_mode — the authority x caps truth table
// ---------------------------------------------------------------------

/// Select the tier for this call, or `None` = auto-deny (no authority can
/// decide). `ElicitationFirst` is `wasl_gate::choose_mode` verbatim — do not
/// re-derive it.
pub fn select_mode(authority: GateAuthority, caps: &ClientCaps) -> Option<GateMode> {
    match authority {
        GateAuthority::Elicitation => caps.elicitation.then_some(GateMode::Elicitation),
        GateAuthority::ElicitationFirst => Some(choose_mode(caps)),
        GateAuthority::ExternalFirst => Some(if caps.external {
            GateMode::External
        } else if caps.elicitation {
            GateMode::Elicitation
        } else {
            GateMode::PrepareCommit
        }),
        GateAuthority::PrepareCommit => Some(GateMode::PrepareCommit),
    }
}

// ---------------------------------------------------------------------
// 4. proto_action — type conversion only, no decision logic
// ---------------------------------------------------------------------

/// rmcp's `ElicitationAction` is a different type from `wasl_proto`'s; this
/// converts the TYPE only. The tested core (`wasl_gate::outcome_of`) owns the
/// action -> outcome SEMANTICS — composing the two is the dedup this block
/// requires (§6).
pub fn proto_action(a: rmcp::model::ElicitationAction) -> wasl_proto::ElicitationAction {
    match a {
        rmcp::model::ElicitationAction::Accept => wasl_proto::ElicitationAction::Accept,
        rmcp::model::ElicitationAction::Decline => wasl_proto::ElicitationAction::Decline,
        rmcp::model::ElicitationAction::Cancel => wasl_proto::ElicitationAction::Cancel,
    }
}

// ---------------------------------------------------------------------
// 5. ledger_decision_of — the Tier-2 ledger's outcome -> decision mapping
// ---------------------------------------------------------------------

/// The `GateLedger` needs SOME [`GateDecision`] to retire a Tier-2 token on
/// `commit` (it has no "no decision" state, unlike the audit log). The AUDIT
/// decision recorded for the call is `outcome.to_decision()` (which correctly
/// records `None` for a dismissal) — this mapping is ledger-bookkeeping only,
/// a distinct, narrower concern.
pub fn ledger_decision_of(outcome: &GateOutcome) -> GateDecision {
    match outcome {
        GateOutcome::Approved => GateDecision::Approve,
        GateOutcome::Denied { reason } => GateDecision::Deny {
            reason: reason.clone(),
        },
        GateOutcome::Dismissed => GateDecision::Deny {
            reason: "dismissed".to_string(),
        },
    }
}
