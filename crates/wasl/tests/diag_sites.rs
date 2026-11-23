//! What may reach a log record stays reviewable (the same mechanism as `spawn_sites.rs`): in the
//! Wasl tree nothing implements `DiagValue`/`DiagEvent` by hand or registers a field type,
//! free text (`DiagText::new(`) is made nowhere, `ErrorInfo::ours(` (free text from any
//! `Display`) is made only at the sites listed below with the number of calls each is
//! allowed, and no `tracing` macro is called. Adding a site is a reviewed change to this file.

use std::collections::BTreeMap;
use std::path::Path;

use wasit_process::scan;

/// Patterns that extend what a record may hold. None is allowed anywhere in Wasl: its events
/// need only the value types `wasit-diag` has (ids, codes, counts, durations, paths of its own
/// layout, `ErrorInfo`).
const PATTERNS: &[&str] = &[
    "diag_value!",
    "DiagValue for ",
    "DiagEvent for ",
    "Sealed for ",
    "SealedEvent for ",
    "DiagText::new(",
];

/// `ErrorInfo::ours(` call sites: file (relative to the repository root), how many calls, and
/// why the text cannot carry content. Every other error reaches a record through
/// `ErrorInfo::io` (kind and OS number) or `ErrorInfo::foreign` (code, length, fingerprint).
const OURS: &[(&str, usize, &str)] = &[
    (
        "crates/wasl-audit/src/lib.rs",
        1,
        "`AuditError::diag`: the text of the audit writer's and opener's errors is an operation \
         plus the OS's reason (`append: …`, `sync: …`, `create <dir>: …`, `writer is gone`); \
         `read_entries` reports a bad line by number and kind, never by its text (unit test \
         `an_unreadable_entry_is_reported_without_its_text`)",
    ),
    (
        "crates/wasl-node/src/journal.rs",
        1,
        "`JournalError::diag`: built only from `io::Error`s of the journal's own file \
         operations, a serialization failure of the compacted file, `Busy` and `writer is \
         gone`; the journal's *content* never enters it (an unreadable line is `foreign`, see \
         `the_parse_error_of_a_journal_line_quotes_the_line`)",
    ),
    (
        "crates/wasl/src/main.rs",
        1,
        "`RuntimeError` of `SessionManager::adopt_all`'s directory scan: only `io_err(\"read \
         <sessions dir>\" | \"scan sessions\", e)`, an operation and the OS's reason (the same \
         call `wasit-hostd` makes); a per-session adopt failure is `foreign`",
    ),
];

/// Macros of the `tracing` crate called with a path or imported by name.
const TRACING: &[&str] = &[
    "tracing::trace!",
    "tracing::debug!",
    "tracing::info!",
    "tracing::warn!",
    "tracing::error!",
    "tracing::event!",
    "tracing::span!",
];

fn root() -> std::path::PathBuf {
    Path::new(env!("CARGO_MANIFEST_DIR")).join("../..")
}

#[test]
fn field_types_and_free_text_are_made_nowhere() {
    let found = scan::find(&root(), &[], |line| {
        PATTERNS.iter().any(|p| scan::mentions(line, p))
    });
    assert!(
        found.is_empty(),
        "new diagnostics field types or free-text sites need review (allow-list in {}):\n{}",
        file!(),
        found.join("\n")
    );
}

#[test]
fn error_text_from_display_only_in_reviewed_places() {
    let found = scan::find(&root(), &[], |line| {
        scan::mentions(line, "ErrorInfo::ours(")
    });
    let mut per_file: BTreeMap<String, usize> = BTreeMap::new();
    for hit in &found {
        // `<file>:<line>: <text>`
        let file = hit.split(':').next().unwrap().to_owned();
        *per_file.entry(file).or_default() += 1;
    }
    let allowed: BTreeMap<String, usize> =
        OURS.iter().map(|(f, n, _)| ((*f).to_owned(), *n)).collect();
    assert_eq!(
        per_file,
        allowed,
        "new, moved or removed `ErrorInfo::ours` sites need review (allow-list in {}):\n{}",
        file!(),
        found.join("\n")
    );
    for (file, _, why) in OURS {
        assert!(!why.is_empty(), "{file} needs a justification");
    }
}

#[test]
fn no_log_goes_through_tracing() {
    let found = scan::find(&root(), &[], |line| {
        TRACING.iter().any(|p| scan::mentions(line, p))
    });
    assert!(
        found.is_empty(),
        "log with a `WaslEvent` (wasl-proto/src/diag.rs), not `tracing`:\n{}",
        found.join("\n")
    );
}

#[test]
fn the_scan_sees_each_pattern() {
    for line in [
        "wasit_diag::diag_value!(TaskId => Ident, |t| Ident::new(&t.0));",
        "impl DiagValue for Prompt {",
        "impl wasit_diag::DiagEvent for X {",
        "impl wasit_diag::__private::Sealed for X {}",
        "let t = DiagText::new(Origin::Own, &e.to_string());",
    ] {
        assert!(PATTERNS.iter().any(|p| scan::mentions(line, p)), "{line}");
    }
    assert!(!PATTERNS
        .iter()
        .any(|p| scan::mentions("let t = MyDiagText::new(x);", p)));
    assert!(scan::mentions(
        "error: ErrorInfo::ours(Code::st(\"x\"), &e),",
        "ErrorInfo::ours("
    ));
    assert!(!scan::mentions(
        "MyErrorInfo::ours(c, &e)",
        "ErrorInfo::ours("
    ));
    assert!(TRACING
        .iter()
        .any(|p| scan::mentions("    tracing::warn!(error = %e, \"x\");", p)));
}
