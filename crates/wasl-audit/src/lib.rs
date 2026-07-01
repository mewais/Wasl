//! wasl-audit — durable, human-auditable log of every inter-agent interaction and
//! gate decision (WA). Pure query logic over an append-only `Sink` trait (the
//! daemon supplies a real file/git-backed sink). See `docs/specs/audit.md`.
