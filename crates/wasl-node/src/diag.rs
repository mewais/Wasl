//! How the node's errors reach a diagnostics record (see [`wasl_proto::diag`] for the events).
//!
//! Everything here is `foreign`: the error's code, its length and a fingerprint, never its
//! text. An `RpcError` or a `CommandError` carries a message that may quote what a peer, a CLI
//! or an agent wrote (a refused command's reason, a task id in a sentence that names a task's
//! state, a supervisor's explanation); a `TransportError`'s message is a peer's or the hub's;
//! a stream failure is the runtime's journal reader's text. The node's own error types that
//! are built from OS errors only (`JournalError`, `AuditError`) have a `diag()` of their own.

use wasit_control::RpcError;
use wasit_diag::{ErrorInfo, Ident};
use wasit_runtime::wire::{CommandError, RuntimeError, SessionId};
use wasl_proto::diag::foreign;

use crate::transport::TransportError;

/// An error the control layer returned (a peer's, the supervisor's, or this node's own, whose
/// messages are formatted with ids and states): its code.
pub(crate) fn rpc_error(e: &RpcError) -> ErrorInfo {
    foreign(&e.code, &e.message)
}

/// A command the session refused or could not take: the runtime's code for it.
pub(crate) fn command_error(e: &CommandError) -> ErrorInfo {
    foreign(e.code(), &e.to_string())
}

/// An operation the runtime failed (reading a session's journal, continuing a session).
pub(crate) fn runtime_error(e: &RuntimeError) -> ErrorInfo {
    foreign(e.code(), &e.to_string())
}

/// A node-to-node call that failed.
pub(crate) fn transport_error(e: &TransportError) -> ErrorInfo {
    foreign(
        if e.retryable {
            "transport_retryable"
        } else {
            "transport"
        },
        &e.message,
    )
}

/// A failure of a session's event stream.
pub(crate) fn stream_error(text: &str) -> ErrorInfo {
    foreign("stream", text)
}

/// A session id as a record field.
pub(crate) fn session(id: &SessionId) -> Ident {
    Ident::new(&id.to_string())
}
