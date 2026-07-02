# E1 — `wasl-filesink` — durable append-JSONL audit `Sink` — spec (authoritative)

**Tier:** L1 (real file IO; fully testable in-env with temp files + `/dev/full`). **Deps:** `wasl-audit`
(the `Sink` trait + `AuditEntry` + `AuditError`), `serde_json`. Dev: `wasl-proto` (to build entries),
`tempfile`. No `unsafe`. This is a concrete durable `Sink` so a node's audit survives process restart. It
adds NO new audit logic — it only persists what `wasl-audit` already models.

**Testability (honesty):** FULLY testable here. Persistence across process restart is proven by writing with
one `FileSink`, dropping it, and reading the bytes back with `read_entries`. The write-error branch is
exercised deterministically against `/dev/full` (Linux always-ENOSPC device). Crash-consistency via `fsync`
is OUT OF SCOPE (bytes are handed to the OS immediately; std `File` is unbuffered — no in-process buffer to
lose — but power-loss durability would need `sync_all`, which this crate does not call). State that honestly.

## Format
One JSON object per line (JSONL): each `append` writes `serde_json::to_string(entry)` followed by a single
`\n`. The file is opened `create(true).append(true)` so re-opening the same path keeps appending after prior
records. `read_entries` parses the file back into `Vec<AuditEntry>`, skipping blank lines.

## API
```rust
use std::path::Path;
use wasl_audit::{AuditEntry, AuditError, Sink};

/// A durable audit `Sink` that appends one JSON line per entry to a file.
pub struct FileSink { /* file: std::fs::File, path: std::path::PathBuf */ }

impl FileSink {
    /// Open (creating if absent) `path` for append. Errors map to `AuditError::Sink`.
    pub fn open<P: AsRef<Path>>(path: P) -> Result<Self, AuditError>;
    /// The path this sink writes to.
    pub fn path(&self) -> &Path;
}

impl Sink for FileSink {
    /// Serialize `entry` to one JSON line and append it (json + '\n'). std `File` is unbuffered, so the
    /// bytes reach the OS immediately; no `flush`/`fsync` is performed. Any IO error -> `AuditError::Sink`.
    fn append(&mut self, entry: &AuditEntry) -> Result<(), AuditError>;
}

/// Read a JSONL audit file back into entries (blank lines skipped). Missing file / bad JSON ->
/// `AuditError::Sink`.
pub fn read_entries<P: AsRef<Path>>(path: P) -> Result<Vec<AuditEntry>, AuditError>;
```

### `append` (exact shape)
```rust
fn append(&mut self, entry: &AuditEntry) -> Result<(), AuditError> {
    let mut line = serde_json::to_string(entry).map_err(sink)?;
    line.push('\n');
    use std::io::Write;
    self.file.write_all(line.as_bytes()).map_err(sink)
}
```
where `sink` is a single named helper `fn sink<E: Display>(e: E) -> AuditError { AuditError::Sink(e.to_string()) }`
used at EVERY error site (open/write/read/parse). Using one named function instead of per-site closures is
deliberate: a never-called closure is a distinct llvm-cov *function* and costs a missed line; the named
helper is reached by the other paths, so the unreachable serialize error costs only a *region*, not a line.

NOTE (coverage honesty, verified): the `?` error branch of `serde_json::to_string` is UNREACHABLE for
`AuditEntry` (its `Serialize` cannot fail). Result: LINE 100% (35/35), FUNCTIONS 100% (6/6), REGIONS 94.55%
(52/55) — the residual regions are that one unreachable branch and its `?`-expansion. Do NOT fake it with
dead code or a panic. The `write` error arm IS covered (via `/dev/full`). `--fail-under-lines 100` exits 0.

## Test plan (`tests/*.rs`; real files; line-100)
Use `tempfile::tempdir()` for temp paths (add `tempfile` as a dev-dependency). Build `AuditEntry`s with
`wasl_proto` types.
- **open + append + read round-trip:** open a temp path, append 2–3 distinct entries (mix of a `Decision`
  entry with `Some(decision)` and a plain `Consult`/`Message` with `decision: None`), then `read_entries` the
  same path → exactly those entries in order, field-for-field equal. `path()` returns the opened path.
- **persistence across "restart":** open, append one entry, `drop` the sink; open the SAME path again, append
  a second entry, drop; `read_entries` → BOTH entries in order (proves append-mode continuation on re-open).
- **open error:** `FileSink::open` on a path whose parent dir does not exist → `Err(AuditError::Sink(_))`.
- **write error:** `FileSink::open("/dev/full")` then `append` → `Err(AuditError::Sink(_))` (ENOSPC). (Linux
  only; the test may guard on `Path::new("/dev/full").exists()` and, if absent, still assert nothing — but in
  THIS env `/dev/full` exists, so require the error.)
- **read: missing file** → `Err(AuditError::Sink(_))`.
- **read: blank line skipped** → write a file by hand containing one valid JSON line, a blank line, then
  another valid JSON line; `read_entries` returns exactly the two entries (blank line ignored).
- **read: malformed line** → write a file containing `not json`; `read_entries` → `Err(AuditError::Sink(_))`.

No `#[ignore]`, no `todo!`, no tautologies. Every test asserts a concrete value. Tests compile ONLY against
the public API (`FileSink`, `read_entries`) + `wasl_audit`/`wasl_proto` public types.
