# MESH — `wasl-mesh` — working contract
**Purpose:** the concrete socket transport for `wasl_daemon::Mesh` — a blocking `std::net` `TcpMesh` client
(deliver an Envelope to a remote peer's host, read the reply) + a `serve` side the daemon runs. **Spec:** `../../docs/specs/mesh.md`
- Blocking (fits the sync `Mesh` trait); no tokio. Deps: `wasl-proto` + `wasl-registry` (Resolved) + `wasl-daemon` (Mesh) + serde/serde_json.
- Done: `cargo test -p wasl-mesh` green (framing/addr-book unit tests + a 127.0.0.1 loopback roundtrip test); `clippy -p wasl-mesh --all-targets -D warnings` + fmt clean; no unsafe; no `unimplemented!`/`todo!` in shipped paths. Pure parts (encode/decode/addr lookup/Resolved matching) fully covered; inherently-nondeterministic raw-socket error branches covered where deterministic (e.g. connect-refused), noted otherwise.
