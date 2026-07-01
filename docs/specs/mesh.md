# MESH — `wasl-mesh` — socket transport — spec (authoritative)

**Tier:** L2 (IO shell; blocking `std::net`). **Deps:** `wasl-proto`, `wasl-registry`, `wasl-daemon`,
`serde`, `serde_json`. No `unsafe`, no async/tokio (the `wasl_daemon::Mesh` trait is synchronous). This is
the concrete transport behind `Mesh` — the pure framing + address resolution are unit-tested; the socket
roundtrip is verified by a `127.0.0.1` loopback integration test.

Re-use: `wasl_proto::{Envelope, HostId}`, `wasl_registry::Resolved`, `wasl_daemon::Mesh`.

## Wire framing (pure, deterministic)
Newline-delimited JSON: one `Envelope` per line out, one reply **body** line back.
```rust
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum MeshError { Encode(String), Decode(String), Io(String), NoAddress(HostId), Local }
// impl Display + std::error::Error

/// `serde_json(env)` + '\n'. Errors -> MeshError::Encode.
pub fn encode_envelope(env: &Envelope) -> Result<String, MeshError>;
/// Parse one JSON line into an Envelope. Errors -> MeshError::Decode.
pub fn decode_envelope(line: &str) -> Result<Envelope, MeshError>;
```

## Address book
```rust
/// Maps a HostId to a dialable socket address ("host:port"). The daemon builds this from its peer config.
#[derive(Debug, Clone, Default)]
pub struct AddrBook { /* private HashMap<HostId, String> */ }
impl AddrBook {
    pub fn new() -> Self;
    pub fn insert(&mut self, host: HostId, addr: impl Into<String>);
    pub fn get(&self, host: &HostId) -> Option<&str>;
}
```

## Client: `TcpMesh` implements `wasl_daemon::Mesh`
```rust
pub struct TcpMesh { /* AddrBook + connect/read timeout */ }
impl TcpMesh {
    pub fn new(book: AddrBook) -> Self;             // default timeout
    pub fn with_timeout(book: AddrBook, timeout: std::time::Duration) -> Self;
}
impl wasl_daemon::Mesh for TcpMesh {
    /// `Resolved::Local{..}` -> `Err("local")` (local delivery is the daemon's in-process concern, not the
    /// socket transport). `Resolved::Remote{host,..}` -> look up the addr (`NoAddress` if absent), connect
    /// (`std::net::TcpStream::connect` + set read/write timeouts), write `encode_envelope(env)`, read one
    /// reply line, return the trimmed reply body. Any IO failure -> `Err(MeshError::Io(_).to_string())`.
    fn deliver(&mut self, resolved: &Resolved, env: &Envelope) -> Result<String, String>;
}
```
(The trait returns `Result<String,String>`, so map `MeshError` via `.to_string()`.)

## Server side (what the per-host daemon runs)
```rust
/// Produce a reply body for one received envelope (the daemon routes it to the local agent + returns its
/// reply). The real impl lives in the daemon binary; unit tests use a fake.
pub trait Handler {
    fn handle(&mut self, env: &Envelope) -> String;
}

/// Read exactly one framed envelope from `stream`, call `handler`, write the reply body line back.
/// Used per accepted connection. Errors on malformed input / IO.
pub fn serve_conn<S: std::io::Read + std::io::Write>(stream: &mut S, handler: &mut dyn Handler)
    -> Result<(), MeshError>;

/// Blocking accept loop: for each incoming connection on `listener`, `serve_conn` once with a handler from
/// `make_handler`. Returns when the listener errors. (Thin; the loopback test drives `serve_conn` directly
/// and/or spawns this in a thread.)
pub fn serve(listener: &std::net::TcpListener, handler: &mut dyn Handler) -> Result<(), MeshError>;
```

## Test plan (`tests/`; real `#[test]`s, green — this is an IO crate, not a 100%-coverage unit, but cover
every pure branch + the loopback roundtrip + the deterministic error paths).
**framing/addr (pure):**
- M1  `encode_envelope`→`decode_envelope` round-trips an Envelope (incl. multi-`to`); decode of malformed
      JSON → `MeshError::Decode`.
- M2  `AddrBook` insert/get; `get` on an absent host → `None`.
- M3  `MeshError` Display non-empty + distinct per variant; `std::error::Error` implemented.
**client/server (loopback, 127.0.0.1):**
- M4  `serve_conn` over an in-memory duplex (or a `Cursor`-backed rig): given a framed envelope in, it calls
      the handler and writes the handler's reply body line out; assert the handler saw the exact envelope and
      the output is the reply.
- M5  **loopback roundtrip:** bind a `TcpListener` on `127.0.0.1:0`, spawn a thread that accepts one
      connection and `serve_conn`s with a fake `Handler` (reply = e.g. `format!("ack:{}", env.body)`); build a
      `TcpMesh` whose `AddrBook` maps a `HostId` to that listener's addr; call `deliver(&Resolved::Remote{host,
      host_id}, &env)`; assert the returned reply == `"ack:<body>"` and the handler saw the right envelope.
- M6  `deliver` on `Resolved::Local{..}` → `Err` (contains "local"); does NOT touch the network.
- M7  `deliver` to a host absent from the `AddrBook` → `Err` (NoAddress); to a refused/dead address (bind
      then drop, or an unused port) → `Err` (Io) — the deterministic connect-failure path.

Keep timeouts short (e.g. 2s) so tests never hang. Prefer `serve_conn` over a raw stream for M4 to keep the
pure framing path deterministic; use the real socket only for M5/M7.
