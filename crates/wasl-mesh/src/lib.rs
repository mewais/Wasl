//! wasl-mesh — the concrete socket transport behind `wasl_daemon::Mesh` (IO shell).
//!
//! Blocking `std::net` implementation: a `TcpMesh` client that delivers an
//! `Envelope` to a resolved remote peer's host over a newline-framed JSON line
//! and reads back the reply body, plus a `serve` side the per-host daemon runs to
//! accept envelopes and answer them via a local `Handler`. Pure framing +
//! address resolution are unit-tested; the socket roundtrip is loopback-tested.
//! See `docs/specs/mesh.md`.

use std::collections::HashMap;
use std::fmt;
use std::io::{BufRead, BufReader, Read, Write};
use std::net::{TcpListener, TcpStream};
use std::time::Duration;

use wasl_daemon::Mesh;
use wasl_proto::{Envelope, HostId};
use wasl_registry::Resolved;

// ---------------------------------------------------------------------
// 1. Errors
// ---------------------------------------------------------------------

/// Errors produced by the mesh transport (framing, addressing, and IO).
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum MeshError {
    /// Serializing an `Envelope` to JSON failed.
    Encode(String),
    /// Parsing a line into an `Envelope` failed.
    Decode(String),
    /// A socket / stream IO operation failed.
    Io(String),
    /// No dialable address is registered for the target host.
    NoAddress(HostId),
    /// The route resolved as local; the socket transport does not handle it.
    Local,
}

impl fmt::Display for MeshError {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            MeshError::Encode(msg) => write!(f, "mesh encode error: {msg}"),
            MeshError::Decode(msg) => write!(f, "mesh decode error: {msg}"),
            MeshError::Io(msg) => write!(f, "mesh io error: {msg}"),
            MeshError::NoAddress(host) => {
                write!(f, "no mesh address for host: {}", host.as_str())
            }
            MeshError::Local => write!(f, "local delivery is not a mesh transport concern"),
        }
    }
}

impl std::error::Error for MeshError {}

impl From<std::io::Error> for MeshError {
    fn from(e: std::io::Error) -> Self {
        MeshError::Io(e.to_string())
    }
}

// ---------------------------------------------------------------------
// 2. Wire framing (pure, deterministic)
// ---------------------------------------------------------------------

/// Serialize `env` to a single JSON line (`serde_json` + `'\n'`).
pub fn encode_envelope(env: &Envelope) -> Result<String, MeshError> {
    let mut s = serde_json::to_string(env).map_err(|e| MeshError::Encode(e.to_string()))?;
    s.push('\n');
    Ok(s)
}

/// Parse one JSON line into an `Envelope`.
pub fn decode_envelope(line: &str) -> Result<Envelope, MeshError> {
    serde_json::from_str(line.trim_end_matches(['\n', '\r']))
        .map_err(|e| MeshError::Decode(e.to_string()))
}

// ---------------------------------------------------------------------
// 3. Address book
// ---------------------------------------------------------------------

/// Maps a `HostId` to a dialable socket address (`"host:port"`). The daemon
/// builds this from its peer config.
#[derive(Debug, Clone, Default)]
pub struct AddrBook {
    addrs: HashMap<HostId, String>,
}

impl AddrBook {
    pub fn new() -> Self {
        Self::default()
    }

    /// Insert or overwrite the address for `host`.
    pub fn insert(&mut self, host: HostId, addr: impl Into<String>) {
        self.addrs.insert(host, addr.into());
    }

    /// Look up the dialable address for `host`, if any.
    pub fn get(&self, host: &HostId) -> Option<&str> {
        self.addrs.get(host).map(String::as_str)
    }
}

// ---------------------------------------------------------------------
// 4. Client: TcpMesh
// ---------------------------------------------------------------------

const DEFAULT_TIMEOUT: Duration = Duration::from_secs(10);

/// Blocking `std::net` client implementing [`wasl_daemon::Mesh`].
#[derive(Debug, Clone)]
pub struct TcpMesh {
    book: AddrBook,
    timeout: Duration,
}

impl TcpMesh {
    /// Build with the default connect/read/write timeout.
    pub fn new(book: AddrBook) -> Self {
        Self {
            book,
            timeout: DEFAULT_TIMEOUT,
        }
    }

    /// Build with an explicit connect/read/write timeout.
    pub fn with_timeout(book: AddrBook, timeout: Duration) -> Self {
        Self { book, timeout }
    }

    /// Connect to `addr`, write the framed envelope, and read back one reply
    /// line (trimmed body). All IO failures surface as `MeshError::Io`.
    fn round_trip(&self, addr: &str, env: &Envelope) -> Result<String, MeshError> {
        let stream = self.connect(addr)?;
        stream.set_read_timeout(Some(self.timeout))?;
        stream.set_write_timeout(Some(self.timeout))?;

        let line = encode_envelope(env)?;

        // Write the request on one handle.
        let mut writer = stream.try_clone()?;
        writer.write_all(line.as_bytes())?;
        writer.flush()?;

        // Read a single reply line back.
        let mut reader = BufReader::new(stream);
        let mut reply = String::new();
        reader.read_line(&mut reply)?;
        Ok(reply.trim().to_string())
    }

    /// Connect within the configured timeout (falls back to a plain connect if
    /// the address cannot be resolved to a single `SocketAddr`).
    fn connect(&self, addr: &str) -> Result<TcpStream, MeshError> {
        use std::net::ToSocketAddrs;
        let mut last: Option<std::io::Error> = None;
        match addr.to_socket_addrs() {
            Ok(iter) => {
                for sa in iter {
                    match TcpStream::connect_timeout(&sa, self.timeout) {
                        Ok(s) => return Ok(s),
                        Err(e) => last = Some(e),
                    }
                }
                Err(MeshError::Io(match last {
                    Some(e) => e.to_string(),
                    None => format!("no socket address resolved for {addr}"),
                }))
            }
            Err(e) => Err(MeshError::Io(e.to_string())),
        }
    }
}

impl Mesh for TcpMesh {
    fn deliver(&mut self, resolved: &Resolved, env: &Envelope) -> Result<String, String> {
        let host = match resolved {
            Resolved::Local { .. } => return Err(MeshError::Local.to_string()),
            Resolved::Remote { host, .. } => host,
        };
        let addr = match self.book.get(host) {
            Some(a) => a.to_string(),
            None => return Err(MeshError::NoAddress(host.clone()).to_string()),
        };
        self.round_trip(&addr, env).map_err(|e| e.to_string())
    }
}

// ---------------------------------------------------------------------
// 5. Server side
// ---------------------------------------------------------------------

/// Produce a reply body for one received envelope. The real impl (routing to a
/// local agent) lives in the daemon binary; tests use a fake.
pub trait Handler {
    fn handle(&mut self, env: &Envelope) -> String;
}

/// Read exactly one framed envelope from `stream`, call `handler`, and write the
/// reply body line back.
pub fn serve_conn<S: Read + Write>(
    stream: &mut S,
    handler: &mut dyn Handler,
) -> Result<(), MeshError> {
    let mut reader = BufReader::new(&mut *stream);
    let mut line = String::new();
    let n = reader.read_line(&mut line)?;
    if n == 0 {
        return Err(MeshError::Io(
            "connection closed before envelope".to_string(),
        ));
    }
    let env = decode_envelope(&line)?;
    let mut reply = handler.handle(&env);
    reply.push('\n');
    stream.write_all(reply.as_bytes())?;
    stream.flush()?;
    Ok(())
}

/// Blocking accept loop: for each incoming connection, `serve_conn` once with
/// `handler`. Returns when the listener errors.
pub fn serve(listener: &TcpListener, handler: &mut dyn Handler) -> Result<(), MeshError> {
    for conn in listener.incoming() {
        let mut stream = conn?;
        serve_conn(&mut stream, handler)?;
    }
    Ok(())
}
