//! wasl-adapter-claude — a concrete `wasl_adapter::AgentAdapter` (E3): the last
//! inch that injects an incoming `Delivery` into the local **Claude Code** CLI
//! and returns its reply.
//!
//! ⚠️ Testability: there is NO `claude` binary in the build environment. The pure
//! core (`compose_prompt`/`build_argv`/`parse_reply`), the `deliver` wiring (via a
//! fake `CliInvoker`), and `ProcessCliInvoker`'s process plumbing (via stand-in
//! commands) are all tested. The REAL Claude-CLI CONTRACT (flag names, prompt on
//! stdin, JSON reply schema, `--session-id` persistence) is an ASSUMPTION encoded
//! in `build_argv`/`parse_reply` and is UNTESTED HERE — verify it against a real
//! `claude` binary. See `docs/specs/adapter-claude.md`.

// Implementation is provided by the code-agent per docs/specs/adapter-claude.md.
