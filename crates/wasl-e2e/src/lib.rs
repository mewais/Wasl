//! wasl-e2e — cross-crate end-to-end scenario suite (no library surface of its
//! own). The real content is in `tests/`, which composes the whole Wasl stack —
//! gate (server) -> dispatch (daemon) -> audit -> Marsad connector — over the
//! same trait seams the production binary wires, driven by fakes. Proves the
//! blocks compose into the intended system. See `docs/specs/e2e.md`.
