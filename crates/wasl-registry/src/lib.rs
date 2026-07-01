//! wasl-registry — peer registry + agent cards + mesh route resolution (WR).
//!
//! Pure: track which agents exist and on which host, resolve whether a target
//! peer is Local (same daemon) or Remote (another host's daemon). Actual mesh
//! IO lives in the daemon block. See `docs/specs/registry.md`. Placeholder.
