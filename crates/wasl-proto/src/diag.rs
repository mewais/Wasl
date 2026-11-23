//! Wasl's diagnostics: every log record of the node, the MCP front-end, the audit trail and the
//! `wasl` daemon is a [`WaslEvent`]. A record holds ids, sizes, counts, timings, states and
//! error codes, never content: what an agent wrote (prompts, titles, results, progress text,
//! handoff reasons), what people named things (agent and session names, descriptions, spawn
//! request text) or what was read back from the task journal or the audit trail.
//!
//! * **Ids.** A task, a gate or spawn request and a session are [`Ident`]s (the type admits
//!   `[A-Za-z0-9._:@+-]`, so a text that is not an id shows as `<other:N>`). An agent address
//!   `host/session` has a `/` and so is written as two fields, `host` and `session`. Workstream
//!   ids are not written: a workstream is named by the supervisor, and a name is content.
//! * **Errors** reach a record through one of three constructors of [`ErrorInfo`]: `io` for an
//!   [`std::io::Error`] (kind and OS number), `ours` for the text of one of this repository's
//!   own error types that names an operation and an OS reason (a call site per type, pinned in
//!   `crates/wasl/tests/diag_sites.rs`), and `foreign` for everything else: an `RpcError`
//!   or `CommandError` (its message can carry what a peer or a CLI wrote), a transport's
//!   message, a `serde` error read from the journal (its text quotes the offending value), a
//!   stream failure. `foreign` keeps the code, the length and a fingerprint, no text.
//! * **Rates.** A variant is admitted a few hundred times a minute (`diag.suppressed` says how
//!   many were left out); none is emitted per streamed event or per frame.
//!
//! What this does not hold: the task, gate and spawn *trail* (a projection of the journal for
//! the UI) is not built yet.

use wasit_diag::{Code, ErrorInfo, HomePath, Ident, Ms};

use crate::TaskId;

wasit_diag::diag_enum! {
    /// Which follower of a session's events resubscribed.
    pub enum Watcher {
        /// The gate watcher of a handoff or spawn request.
        Gate,
        /// The observer of a delivered task's turn.
        Observer,
    }
}

wasit_diag::diag_enum! {
    /// Why an outbound task was being withdrawn when that failed.
    pub enum WithdrawCause {
        /// The task's deadline passed.
        Deadline,
    }
}

wasit_diag::diag_enum! {
    /// Which kind of state a journal write that did not become durable was for.
    pub enum JournalSite {
        /// A task (outbound or inbound).
        Task,
        /// A spawn request.
        Spawn,
    }
}

wasit_diag::diag_enum! {
    /// A unit of node-to-node work.
    pub enum SendWork {
        /// Hand an approved task to its receiver.
        Deliver,
        /// Forward a withdrawal.
        Cancel,
        /// Report a state back to the sender.
        Update,
    }
}

wasit_diag::diag_enum! {
    /// Why the status of a peer on another host is not known.
    pub enum PeerStatusFailure {
        /// No route to other hosts is attached to this node.
        NoRoute,
        /// The call to the host's node failed; see `error`.
        CallFailed,
        /// The host's node did not answer within the bound.
        TimedOut,
        /// The host answered with something that is not a status reply.
        BadReply,
        /// The request could not be encoded.
        BadRequest,
    }
}

wasit_diag::diag_events! {
    /// Events of Wasl (`src` `daemon` for the standalone daemon; the node and the MCP front-end
    /// write into the sink of whichever process embeds them).
    pub enum WaslEvent {
        // ── the standalone daemon ──────────────────────────────────────────
        /// `wasl serve` holds its single-instance lock and its diagnostics are installed.
        #[diag(ev = "wasl.daemon.start")]
        DaemonStart { host: Ident, data: HomePath, socket: HomePath },
        /// The sessions under the data directory were adopted.
        #[diag(ev = "wasl.daemon.adopted")]
        DaemonAdopted { running: u32, finished: u32, errors: u32 },
        /// One session directory could not be adopted (the reason is from the runtime: only
        /// its size and fingerprint are kept).
        #[diag(ev = "wasl.daemon.adopt_error", lvl = warn)]
        DaemonAdoptError { session: Ident, error: ErrorInfo },
        /// The sessions directory could not be scanned.
        #[diag(ev = "wasl.daemon.scan_failed", lvl = error)]
        DaemonScanFailed { error: ErrorInfo },
        /// The readiness line was printed.
        #[diag(ev = "wasl.daemon.ready")]
        DaemonReady { pid: u32 },
        /// The daemon stops serving (sessions keep running).
        #[diag(ev = "wasl.daemon.shutdown")]
        DaemonShutdown { reason: Code },
        /// Termination signals cannot be handled: the daemon only stops when killed.
        #[diag(ev = "wasl.daemon.signal_handlers_failed", lvl = error)]
        DaemonSignalHandlersFailed {},

        // ── requests served by the node ────────────────────────────────────
        /// A state-changing `wasl.*` request was answered (reads and long polls are not
        /// recorded unless they fail).
        #[diag(ev = "wasl.request")]
        Request { method: Code, ms: Ms },
        /// A `wasl.*` request failed; `code` is the error code (never its message). This is
        /// also where a failure of the MCP front-end's calls shows, since that process has no
        /// diagnostics of its own.
        #[diag(ev = "wasl.request_failed", lvl = warn)]
        RequestFailed { method: Code, code: Code, ms: Ms },

        // ── the node's journal ─────────────────────────────────────────────
        /// The journal was read and compacted: `records` kept, `skipped` lines unreadable
        /// (and dropped by the compaction).
        #[diag(ev = "wasl.journal.opened")]
        JournalOpened { path: HomePath, records: u32, skipped: u32 },
        /// A journal line could not be read and is dropped by the compaction; `error` is its
        /// parse failure (which quotes the offending text: only its size and fingerprint are
        /// kept).
        #[diag(ev = "wasl.journal.line_skipped", lvl = warn)]
        JournalLineSkipped { line: u64, error: ErrorInfo },
        /// A record could not be encoded.
        #[diag(ev = "wasl.journal.encode_failed", lvl = error)]
        JournalEncodeFailed { record: Code, id: Option<Ident>, error: ErrorInfo },
        /// A batch could not be appended to the journal file: none of its `records` is
        /// durable (`ids` lists the tasks and spawn requests among them).
        #[diag(ev = "wasl.journal.append_failed", lvl = error)]
        JournalAppendFailed {
            path: HomePath,
            records: u32,
            ids: Vec<Ident>,
            error: ErrorInfo,
        },
        /// A record was refused at submission (the writer's queue is full or the writer is
        /// gone): it is not journaled.
        #[diag(ev = "wasl.journal.submit_failed", lvl = error)]
        JournalSubmitFailed { record: Code, id: Option<Ident>, error: ErrorInfo },
        /// A record was not durable although the node's memory already holds the new state:
        /// after a crash the state is the older one. The records themselves are named by the
        /// `wasl.journal.append_failed` / `wasl.journal.submit_failed` just before.
        #[diag(ev = "wasl.journal.not_durable", lvl = error)]
        JournalNotDurable { site: JournalSite, error: ErrorInfo },

        // ── the audit trail ────────────────────────────────────────────────
        /// An entry could not be recorded; `kind` is the entry's kind and `task` its task.
        #[diag(ev = "wasl.audit.not_recorded", lvl = error)]
        AuditNotRecorded { kind: Code, task: Option<Ident>, error: ErrorInfo },
        /// The audit file could not be synced: the entries of that batch are not durable.
        #[diag(ev = "wasl.audit.sync_failed", lvl = error)]
        AuditSyncFailed { path: HomePath, error: ErrorInfo },
        /// One entry of the audit file was not written.
        #[diag(ev = "wasl.audit.entry_lost", lvl = error)]
        AuditEntryLost { path: HomePath, kind: Code, task: Option<Ident>, error: ErrorInfo },

        // ── gates, tasks and peers ─────────────────────────────────────────
        /// A session's event stream failed and was resubscribed from the last cursor. `id` is
        /// the gate request id (gate watcher) or the task id (observer).
        #[diag(ev = "wasl.stream.resubscribe", lvl = warn)]
        StreamResubscribe { watcher: Watcher, session: Ident, id: Ident, error: ErrorInfo },
        /// A human answered a handoff gate and the decision could not be applied for a reason
        /// that is not a state race (the audit trail or journal failed): the gate is resolved
        /// on the session, the task stays pending approval.
        #[diag(ev = "wasl.gate.decide_failed", lvl = error)]
        GateDecideFailed { task: Ident, error: ErrorInfo },
        /// A handoff decision was refused because the task had moved on (cancelled, expired,
        /// decided elsewhere): nothing was applied.
        #[diag(ev = "wasl.gate.decide_refused", lvl = warn)]
        GateDecideRefused { task: Ident, error: ErrorInfo },
        /// A gate was taken down on its session because its request ended; `reason` is the
        /// code of the ending (the text the agent is told is never recorded).
        #[diag(ev = "wasl.gate.withdrawn")]
        GateWithdrawn { session: Ident, gate: Ident, reason: Code },
        /// Taking a gate down on its session failed (usually because it was answered or the
        /// session ended: nothing is pending any more).
        #[diag(ev = "wasl.gate.withdraw_failed", lvl = debug)]
        GateWithdrawFailed { session: Ident, gate: Ident, error: ErrorInfo },
        /// Withdrawing an outbound task failed.
        #[diag(ev = "wasl.task.withdraw_failed", lvl = warn)]
        TaskWithdrawFailed { task: Ident, cause: WithdrawCause, error: ErrorInfo },
        /// Expiring an inbound task whose deadline passed failed.
        #[diag(ev = "wasl.task.expire_inbound_failed", lvl = warn)]
        TaskExpireInboundFailed { task: Ident, error: ErrorInfo },
        /// Interrupting the receiving session's turn of a withdrawn task failed.
        #[diag(ev = "wasl.task.interrupt_failed", lvl = warn)]
        TaskInterruptFailed { task: Ident, session: Ident, error: ErrorInfo },
        /// After a restart the head of the receiving session's journal could not be read, so
        /// the node cannot tell whether the task's prompt was sent; it assumes it was not.
        #[diag(ev = "wasl.task.recovery_cursor_failed", lvl = warn)]
        TaskRecoveryCursorFailed { task: Ident, session: Ident, error: ErrorInfo },
        /// A node-to-node call failed retryably; it is retried after `delay`.
        #[diag(ev = "wasl.send.retry", lvl = debug)]
        SendRetry { work: SendWork, task: Ident, host: Ident, attempt: u32, delay: Ms, error: ErrorInfo },
        /// A node-to-node call failed for good.
        #[diag(ev = "wasl.send.failed", lvl = warn)]
        SendFailed { work: SendWork, task: Ident, host: Ident, error: ErrorInfo },
        /// The status of the peers on `host` is not known (shown as `unreachable`).
        #[diag(ev = "wasl.peers.status_failed", lvl = warn, budget = 120)]
        PeersStatusFailed {
            host: Ident,
            reason: PeerStatusFailure,
            sessions: u32,
            error: Option<ErrorInfo>,
        },

        // ── spawning ───────────────────────────────────────────────────────
        /// A spawn request reached its final state: `code` is how it ended (`launched`, or why
        /// not) and `cause` the supervisor's error code for a refusal or a failed launch. The
        /// words that go with it (names, what the human or the supervisor said) are not
        /// recorded.
        #[diag(ev = "wasl.spawn.ended")]
        SpawnEnded { request: Ident, code: Code, cause: Option<Code> },
        /// The topology the supervisor returned for a spawn could not be applied.
        #[diag(ev = "wasl.spawn.topology_failed", lvl = warn)]
        SpawnTopologyFailed { request: Ident, error: ErrorInfo },
        /// The spawn authority was not reachable; the call is retried after `delay`.
        #[diag(ev = "wasl.spawn.authority_retry", lvl = debug)]
        SpawnAuthorityRetry { delay: Ms, error: ErrorInfo },

        // ── the MCP front-end ──────────────────────────────────────────────
        /// The MCP front-end could not report a handoff decision to the node. (Only recorded
        /// when the process that embeds the front-end has a sink: `wasl mcp` has none.)
        #[diag(ev = "wasl.mcp.decision_report_failed", lvl = warn)]
        McpDecisionReportFailed { task: Ident, error: ErrorInfo },
        /// The MCP front-end could not withdraw a spawn request of a cancelled tool call.
        #[diag(ev = "wasl.mcp.spawn_cancel_failed", lvl = warn)]
        McpSpawnCancelFailed { request: Ident, error: ErrorInfo },
    }
}

/// Records `event` in this process's sink (nothing without one).
pub fn emit(event: WaslEvent) {
    wasit_diag::emit(&event);
}

/// A task id as a record field.
pub fn task(id: &TaskId) -> Ident {
    Ident::new(id.as_str())
}

/// A count as a record field (saturating).
pub fn count(n: usize) -> u32 {
    u32::try_from(n).unwrap_or(u32::MAX)
}

/// An error whose text came from outside this code (a peer, a CLI, a parser reading stored
/// content): `code`, the text's length and its fingerprint, never the text.
pub fn foreign(code: &str, text: &str) -> ErrorInfo {
    ErrorInfo::foreign(Code::new(code), text)
}
