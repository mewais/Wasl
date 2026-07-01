//! wasl-mesh — the concrete socket transport behind `wasl_daemon::Mesh` (IO shell).
//!
//! Blocking `std::net` implementation: a `TcpMesh` client that delivers an
//! `Envelope` to a resolved remote peer's host over a length-framed JSON line and
//! reads back the reply body, plus a `serve` side the per-host daemon runs to
//! accept envelopes and answer them via a local `Handler`. Pure framing +
//! address resolution are unit-tested; the socket roundtrip is loopback-tested.
//! See `docs/specs/mesh.md`.
