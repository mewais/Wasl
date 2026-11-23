//! Every `WaslEvent` writes exactly the record header and its declared fields, with its declared
//! name and level; and no declared event is missing from the table below.

use std::collections::BTreeSet;
use std::path::PathBuf;
use std::time::Duration;

use wasit_diag::{Code, DiagEvent, ErrorInfo, HomePath, Ident, Ms, Sink, SinkConfig};
use wasl_proto::diag::{
    foreign, JournalSite, PeerStatusFailure, SendWork, WaslEvent, Watcher, WithdrawCause,
};

const HEADER: [&str; 7] = ["b", "seq", "t", "m", "src", "lvl", "ev"];

/// A directory of this test's own, removed afterwards.
struct Dir(PathBuf);

impl Dir {
    fn new(tag: &str) -> Dir {
        let p = std::env::temp_dir().join(format!("wasl-proto-diag-{}-{tag}", std::process::id()));
        let _ = std::fs::remove_dir_all(&p);
        std::fs::create_dir_all(&p).unwrap();
        Dir(p)
    }
}

impl Drop for Dir {
    fn drop(&mut self) {
        let _ = std::fs::remove_dir_all(&self.0);
    }
}

/// The records `events` make in a sink of their own, parsed (the header line is dropped).
fn records(tag: &str, events: &[WaslEvent]) -> Vec<serde_json::Value> {
    let dir = Dir::new(tag);
    let cfg = SinkConfig::new(Code::st("daemon"), Ident::st("b1"), &dir.0, "e.ndjson");
    let sink = Sink::start(cfg).unwrap();
    for e in events {
        sink.emit(e);
    }
    assert!(sink.shutdown(Duration::from_secs(30)));
    let text = std::fs::read_to_string(dir.0.join("e.ndjson")).unwrap();
    let mut lines: Vec<serde_json::Value> = text
        .lines()
        .map(|l| serde_json::from_str(l).unwrap_or_else(|e| panic!("not JSON ({e}): {l:?}")))
        .collect();
    assert_eq!(lines.len(), events.len() + 1, "header and records: {text}");
    lines.remove(0);
    lines
}

fn err() -> ErrorInfo {
    foreign("code", "some text")
}

fn id(s: &'static str) -> Ident {
    Ident::st(s)
}

/// Every variant with the name, level and field names it declares. A variant added to the
/// enum without a row here is caught by `the_table_has_every_declared_event`.
fn table() -> Vec<(WaslEvent, &'static str, &'static str, Vec<&'static str>)> {
    let path = || HomePath::new(std::path::Path::new("/tmp/wasl"));
    vec![
        (
            WaslEvent::DaemonStart {
                host: id("a"),
                data: path(),
                socket: path(),
            },
            "wasl.daemon.start",
            "info",
            vec!["host", "data", "socket"],
        ),
        (
            WaslEvent::DaemonAdopted {
                running: 1,
                finished: 2,
                errors: 0,
            },
            "wasl.daemon.adopted",
            "info",
            vec!["running", "finished", "errors"],
        ),
        (
            WaslEvent::DaemonAdoptError {
                session: id("abcdefghijkl"),
                error: err(),
            },
            "wasl.daemon.adopt_error",
            "warn",
            vec!["session", "error"],
        ),
        (
            WaslEvent::DaemonScanFailed { error: err() },
            "wasl.daemon.scan_failed",
            "error",
            vec!["error"],
        ),
        (
            WaslEvent::DaemonReady { pid: 7 },
            "wasl.daemon.ready",
            "info",
            vec!["pid"],
        ),
        (
            WaslEvent::DaemonShutdown {
                reason: Code::st("SIGTERM"),
            },
            "wasl.daemon.shutdown",
            "info",
            vec!["reason"],
        ),
        (
            WaslEvent::DaemonSignalHandlersFailed {},
            "wasl.daemon.signal_handlers_failed",
            "error",
            vec![],
        ),
        (
            WaslEvent::Request {
                method: Code::st("wasl.delegate"),
                ms: Ms(3),
            },
            "wasl.request",
            "info",
            vec!["method", "ms"],
        ),
        (
            WaslEvent::RequestFailed {
                method: Code::st("wasl.delegate"),
                code: Code::st("no_edge"),
                ms: Ms(3),
            },
            "wasl.request_failed",
            "warn",
            vec!["method", "code", "ms"],
        ),
        (
            WaslEvent::JournalOpened {
                path: path(),
                records: 4,
                skipped: 1,
            },
            "wasl.journal.opened",
            "info",
            vec!["path", "records", "skipped"],
        ),
        (
            WaslEvent::JournalLineSkipped {
                line: 9,
                error: err(),
            },
            "wasl.journal.line_skipped",
            "warn",
            vec!["line", "error"],
        ),
        (
            WaslEvent::JournalEncodeFailed {
                record: Code::st("out"),
                id: Some(id("tabc")),
                error: err(),
            },
            "wasl.journal.encode_failed",
            "error",
            vec!["record", "id", "error"],
        ),
        (
            WaslEvent::JournalAppendFailed {
                path: path(),
                records: 2,
                ids: vec![id("tabc"), id("tdef")],
                error: err(),
            },
            "wasl.journal.append_failed",
            "error",
            vec!["path", "records", "ids", "error"],
        ),
        (
            WaslEvent::JournalSubmitFailed {
                record: Code::st("spawn"),
                id: Some(id("r1")),
                error: err(),
            },
            "wasl.journal.submit_failed",
            "error",
            vec!["record", "id", "error"],
        ),
        (
            WaslEvent::JournalNotDurable {
                site: JournalSite::Task,
                error: err(),
            },
            "wasl.journal.not_durable",
            "error",
            vec!["site", "error"],
        ),
        (
            WaslEvent::AuditNotRecorded {
                kind: Code::st("decided"),
                task: Some(id("tabc")),
                error: err(),
            },
            "wasl.audit.not_recorded",
            "error",
            vec!["kind", "task", "error"],
        ),
        (
            WaslEvent::AuditSyncFailed {
                path: path(),
                error: err(),
            },
            "wasl.audit.sync_failed",
            "error",
            vec!["path", "error"],
        ),
        (
            WaslEvent::AuditEntryLost {
                path: path(),
                kind: Code::st("finished"),
                task: Some(id("tabc")),
                error: err(),
            },
            "wasl.audit.entry_lost",
            "error",
            vec!["path", "kind", "task", "error"],
        ),
        (
            WaslEvent::StreamResubscribe {
                watcher: Watcher::Gate,
                session: id("abcdefghijkl"),
                id: id("wasl-handoff-tabc"),
                error: err(),
            },
            "wasl.stream.resubscribe",
            "warn",
            vec!["watcher", "session", "id", "error"],
        ),
        (
            WaslEvent::GateDecideFailed {
                task: id("tabc"),
                error: err(),
            },
            "wasl.gate.decide_failed",
            "error",
            vec!["task", "error"],
        ),
        (
            WaslEvent::GateDecideRefused {
                task: id("tabc"),
                error: err(),
            },
            "wasl.gate.decide_refused",
            "warn",
            vec!["task", "error"],
        ),
        (
            WaslEvent::GateWithdrawn {
                session: id("abcdefghijkl"),
                gate: id("wasl-handoff-tabc"),
                reason: Code::st("turn_interrupted"),
            },
            "wasl.gate.withdrawn",
            "info",
            vec!["session", "gate", "reason"],
        ),
        (
            WaslEvent::GateWithdrawFailed {
                session: id("abcdefghijkl"),
                gate: id("wasl-handoff-tabc"),
                error: err(),
            },
            "wasl.gate.withdraw_failed",
            "debug",
            vec!["session", "gate", "error"],
        ),
        (
            WaslEvent::TaskWithdrawFailed {
                task: id("tabc"),
                cause: WithdrawCause::TurnInterrupted,
                error: err(),
            },
            "wasl.task.withdraw_failed",
            "warn",
            vec!["task", "cause", "error"],
        ),
        (
            WaslEvent::TaskExpireInboundFailed {
                task: id("tabc"),
                error: err(),
            },
            "wasl.task.expire_inbound_failed",
            "warn",
            vec!["task", "error"],
        ),
        (
            WaslEvent::TaskInterruptFailed {
                task: id("tabc"),
                session: id("abcdefghijkl"),
                error: err(),
            },
            "wasl.task.interrupt_failed",
            "warn",
            vec!["task", "session", "error"],
        ),
        (
            WaslEvent::TaskRecoveryCursorFailed {
                task: id("tabc"),
                session: id("abcdefghijkl"),
                error: err(),
            },
            "wasl.task.recovery_cursor_failed",
            "warn",
            vec!["task", "session", "error"],
        ),
        (
            WaslEvent::SendRetry {
                work: SendWork::Deliver,
                task: id("tabc"),
                host: id("b"),
                attempt: 2,
                delay: Ms(400),
                error: err(),
            },
            "wasl.send.retry",
            "debug",
            vec!["work", "task", "host", "attempt", "delay", "error"],
        ),
        (
            WaslEvent::SendFailed {
                work: SendWork::Update,
                task: id("tabc"),
                host: id("b"),
                error: err(),
            },
            "wasl.send.failed",
            "warn",
            vec!["work", "task", "host", "error"],
        ),
        (
            WaslEvent::PeersStatusFailed {
                host: id("b"),
                reason: PeerStatusFailure::CallFailed,
                sessions: 2,
                error: Some(err()),
            },
            "wasl.peers.status_failed",
            "warn",
            vec!["host", "reason", "sessions", "error"],
        ),
        (
            WaslEvent::SpawnEnded {
                request: id("r1"),
                code: Code::st("refused"),
                cause: Some(Code::st("spawn_limit")),
            },
            "wasl.spawn.ended",
            "info",
            vec!["request", "code", "cause"],
        ),
        (
            WaslEvent::SpawnTopologyFailed {
                request: id("r1"),
                error: err(),
            },
            "wasl.spawn.topology_failed",
            "warn",
            vec!["request", "error"],
        ),
        (
            WaslEvent::SpawnAuthorityRetry {
                delay: Ms(200),
                error: err(),
            },
            "wasl.spawn.authority_retry",
            "debug",
            vec!["delay", "error"],
        ),
        (
            WaslEvent::McpDecisionReportFailed {
                task: id("tabc"),
                error: err(),
            },
            "wasl.mcp.decision_report_failed",
            "warn",
            vec!["task", "error"],
        ),
        (
            WaslEvent::McpSpawnCancelFailed {
                request: id("r1"),
                error: err(),
            },
            "wasl.mcp.spawn_cancel_failed",
            "warn",
            vec!["request", "error"],
        ),
    ]
}

#[test]
fn every_event_writes_the_header_and_its_declared_fields_only() {
    let table = table();
    let events: Vec<WaslEvent> = table.iter().map(|r| r.0.clone()).collect();
    let lines = records("all", &events);
    for ((event, ev, lvl, fields), v) in table.iter().zip(&lines) {
        assert_eq!(event.ev(), *ev);
        let keys: BTreeSet<&str> = v.as_object().unwrap().keys().map(String::as_str).collect();
        let want: BTreeSet<&str> = HEADER.iter().chain(fields).copied().collect();
        assert_eq!(keys, want, "{v}");
        assert_eq!(v["ev"], *ev);
        assert_eq!(v["lvl"], *lvl, "{ev}");
        assert_eq!(v["src"], "daemon");
    }
}

#[test]
fn an_absent_optional_field_is_left_out_and_enums_are_words() {
    let lines = records(
        "optional",
        &[
            WaslEvent::PeersStatusFailed {
                host: id("b"),
                reason: PeerStatusFailure::NoRoute,
                sessions: 1,
                error: None,
            },
            WaslEvent::AuditNotRecorded {
                kind: Code::st("rejected"),
                task: None,
                error: err(),
            },
            WaslEvent::TaskWithdrawFailed {
                task: id("tabc"),
                cause: WithdrawCause::TurnInterrupted,
                error: err(),
            },
        ],
    );
    assert!(lines[0].get("error").is_none(), "{}", lines[0]);
    assert_eq!(lines[0]["reason"], "no_route");
    assert!(lines[1].get("task").is_none(), "{}", lines[1]);
    assert_eq!(lines[2]["cause"], "turn_interrupted");
}

#[test]
fn the_table_has_every_declared_event() {
    let source = include_str!("../src/diag.rs");
    let declared: BTreeSet<String> = source
        .lines()
        .filter_map(|l| l.trim().strip_prefix("#[diag(ev = \""))
        .filter_map(|rest| rest.split('"').next())
        .map(str::to_owned)
        .collect();
    assert!(declared.len() > 30, "{declared:?}");
    let tabled: BTreeSet<String> = table().iter().map(|r| r.1.to_owned()).collect();
    assert_eq!(declared, tabled);
}

/// Text from a peer, a parser or an agent reaches a record as its size and fingerprint only.
#[test]
fn a_foreign_error_keeps_no_text() {
    let secret = "SECRET-PEER-TEXT-1f9c";
    let lines = records(
        "foreign",
        &[WaslEvent::JournalLineSkipped {
            line: 1,
            error: foreign(
                "journal_line",
                &format!("invalid type: string \"{secret}\""),
            ),
        }],
    );
    let text = lines[0].to_string();
    assert!(!text.contains(secret), "{text}");
    let e = &lines[0]["error"];
    assert_eq!(e["code"], "journal_line");
    assert!(e["len"].as_u64().unwrap() > secret.len() as u64);
    assert!(e["fp"].is_string());
    assert!(e.get("text").is_none(), "{e}");
}

/// An id that is not shaped like one (here: free text with spaces) shows as its length.
#[test]
fn a_task_id_that_is_not_an_id_shows_as_its_length() {
    let title = "Refactor the billing module";
    let lines = records(
        "ident",
        &[WaslEvent::GateDecideRefused {
            task: Ident::new(title),
            error: err(),
        }],
    );
    assert_eq!(
        lines[0]["task"],
        format!("<other:{}>", title.chars().count())
    );
}
