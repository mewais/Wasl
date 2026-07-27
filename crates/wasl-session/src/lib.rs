//! wasl-session — R3 — Wasl's local control surface (Phase-1 skeleton).
//!
//! `wasl-session` is a thin CLI over a versioned control-plane (R4) client. It is
//! **Model A only**: a human **observes** via `attach` and **injects** via
//! `send`, both mediated by the single-writer actor; a human never types into the
//! underlying process directly.
//!
//! ## Phase-1 scope
//!
//! This is the crate/bin skeleton + the dependency edge to R1. The CLI renders
//! ONLY the PUBLIC vocabulary R1 owns ([`wasl_runtime::TurnEvent`],
//! [`wasl_runtime::TargetStatus`], [`wasl_runtime::TurnReceipt`],
//! [`wasl_runtime::TurnSource`], [`wasl_runtime::CancelReason`],
//! [`wasl_runtime::ShutdownMode`], [`wasl_runtime::TargetId`]) plus
//! `wasl_proto::GateDecision`; it never sees the internal
//! [`wasl_runtime::ProtocolEvent`]. The command / arg SEMANTICS and their mapping
//! to R4 requests are Phase-2 behavioral work against a fake R4 client — NOT
//! Phase-1.
//!
//! The one spec-pinned Phase-1 mapping provided here is the
//! `gate <target> approve|deny` → `wasl_proto::GateDecision` mapping
//! (session.md §gate): `approve` → `GateDecision::Approve`;
//! `deny [--reason]` → `GateDecision::Deny { reason }`.

#![forbid(unsafe_code)]

use wasl_proto::GateDecision;

pub mod client;

/// The `wasl session gate <target> approve|deny [--reason]` argument (session.md
/// §gate). This is the PUBLIC vocabulary the CLI expresses over R4 — not a
/// control-plane action (that is Phase-2).
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum GateArg {
    /// `approve` → [`GateDecision::Approve`].
    Approve,
    /// `deny [--reason]` → [`GateDecision::Deny`] with the (defaulted) reason.
    Deny {
        /// The deny reason (defaults to a node-standard reason when unset).
        reason: String,
    },
}

/// The node-standard deny reason used when `--reason` is not supplied
/// (session.md §gate: "defaults to a node-standard reason").
pub const DEFAULT_DENY_REASON: &str = "denied by operator";

impl GateArg {
    /// Map the CLI gate argument to the public `wasl_proto::GateDecision` the CLI
    /// expresses over R4 (session.md §gate lines 87-89).
    pub fn to_decision(self) -> GateDecision {
        match self {
            GateArg::Approve => GateDecision::Approve,
            GateArg::Deny { reason } => GateDecision::Deny { reason },
        }
    }

    /// Parse the `approve|deny` subcommand token into a [`GateArg`], applying the
    /// node-standard default deny reason when none is given.
    pub fn parse(token: &str, reason: Option<&str>) -> Option<GateArg> {
        match token {
            "approve" => Some(GateArg::Approve),
            "deny" => Some(GateArg::Deny {
                reason: reason.unwrap_or(DEFAULT_DENY_REASON).to_string(),
            }),
            _ => None,
        }
    }
}

/// The `wasl session …` usage text (Phase-1 skeleton; the commands' behavior is
/// Phase-2).
pub const USAGE: &str = "\
wasl-session — Wasl local control surface

USAGE:
    wasl session start     <target-key> [--protocol claude|opencode-acp|opencode-serve]
    wasl session list
    wasl session attach    <target-id>
    wasl session send      <target-id> <text>
    wasl session interrupt <target-id>
    wasl session stop      <target-id> [--drain]
    wasl session gate      <target-id> approve|deny [--reason <text>]

GLOBAL OPTIONS:
    --endpoint <uds-path|url>   runtime owner's control-plane address
    --json                      machine-readable output
";
