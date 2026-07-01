//! Integration tests for wasl-mesh (M1-M7 per docs/specs/mesh.md).

use std::error::Error;
use std::io::{Read, Write};
use std::net::TcpListener;
use std::sync::mpsc;
use std::thread;
use std::time::Duration;

use wasl_daemon::Mesh;
use wasl_mesh::{
    decode_envelope, encode_envelope, serve_conn, AddrBook, Handler, MeshError, TcpMesh,
};
use wasl_proto::{AgentId, Envelope, EnvelopeKind, HostId};
use wasl_registry::Resolved;

// ---------------------------------------------------------------------
// Helpers
// ---------------------------------------------------------------------

fn agent(id: &str) -> AgentId {
    AgentId(id.to_string())
}

fn host(id: &str) -> HostId {
    HostId(id.to_string())
}

fn envelope(to: Vec<&str>, body: &str) -> Envelope {
    Envelope {
        id: "e1".to_string(),
        kind: EnvelopeKind::Consult,
        from: agent("alice"),
        to: to.into_iter().map(agent).collect(),
        body: body.to_string(),
    }
}

/// A recording `Handler` that replies `ack:<body>` and captures the envelope.
struct AckHandler {
    seen: mpsc::Sender<Envelope>,
}

impl Handler for AckHandler {
    fn handle(&mut self, env: &Envelope) -> String {
        let _ = self.seen.send(env.clone());
        format!("ack:{}", env.body)
    }
}

/// An in-memory duplex: request bytes are handed in; response bytes accumulate.
struct MemRig {
    read_buf: std::io::Cursor<Vec<u8>>,
    written: Vec<u8>,
}

impl MemRig {
    fn new(input: &str) -> Self {
        Self {
            read_buf: std::io::Cursor::new(input.as_bytes().to_vec()),
            written: Vec::new(),
        }
    }
}

impl Read for MemRig {
    fn read(&mut self, buf: &mut [u8]) -> std::io::Result<usize> {
        self.read_buf.read(buf)
    }
}

impl Write for MemRig {
    fn write(&mut self, buf: &[u8]) -> std::io::Result<usize> {
        self.written.write(buf)
    }
    fn flush(&mut self) -> std::io::Result<()> {
        Ok(())
    }
}

// ---------------------------------------------------------------------
// M1 — framing round-trip + malformed decode
// ---------------------------------------------------------------------

#[test]
fn m1_encode_decode_round_trip() {
    let env = envelope(vec!["bob", "carol"], "hello mesh");
    let line = encode_envelope(&env).expect("encode");
    assert!(line.ends_with('\n'), "framed line must end with newline");
    let back = decode_envelope(&line).expect("decode");
    assert_eq!(back, env, "round-trip must preserve the envelope");
    assert_eq!(back.to.len(), 2, "multi-`to` must survive the round-trip");
}

#[test]
fn m1_decode_malformed_is_decode_error() {
    let err = decode_envelope("{not valid json").unwrap_err();
    assert!(matches!(err, MeshError::Decode(_)), "got {err:?}");
}

// ---------------------------------------------------------------------
// M2 — AddrBook insert/get + absent -> None
// ---------------------------------------------------------------------

#[test]
fn m2_addr_book_insert_get_and_absent() {
    let mut book = AddrBook::new();
    book.insert(host("h1"), "127.0.0.1:9000");
    assert_eq!(book.get(&host("h1")), Some("127.0.0.1:9000"));
    assert_eq!(book.get(&host("absent")), None);

    // Overwrite semantics.
    book.insert(host("h1"), "10.0.0.1:1");
    assert_eq!(book.get(&host("h1")), Some("10.0.0.1:1"));
}

// ---------------------------------------------------------------------
// M3 — MeshError Display distinct + std::error::Error
// ---------------------------------------------------------------------

#[test]
fn m3_mesh_error_display_distinct_and_error_impl() {
    let variants = [
        MeshError::Encode("x".into()),
        MeshError::Decode("x".into()),
        MeshError::Io("x".into()),
        MeshError::NoAddress(host("h1")),
        MeshError::Local,
    ];
    let strings: Vec<String> = variants.iter().map(|e| e.to_string()).collect();
    for s in &strings {
        assert!(!s.is_empty(), "Display must be non-empty");
    }
    // All distinct.
    for i in 0..strings.len() {
        for j in (i + 1)..strings.len() {
            assert_ne!(strings[i], strings[j], "variants must render distinctly");
        }
    }
    // std::error::Error implemented (usable as a trait object).
    let e: &dyn Error = &MeshError::Local;
    assert!(e.source().is_none());
}

// ---------------------------------------------------------------------
// M4 — serve_conn over an in-memory rig
// ---------------------------------------------------------------------

#[test]
fn m4_serve_conn_in_memory() {
    let env = envelope(vec!["bob"], "ping");
    let framed = encode_envelope(&env).expect("encode");
    let mut rig = MemRig::new(&framed);

    let (tx, rx) = mpsc::channel();
    let mut handler = AckHandler { seen: tx };
    serve_conn(&mut rig, &mut handler).expect("serve_conn");

    let seen = rx.recv().expect("handler must see the envelope");
    assert_eq!(seen, env, "handler saw the exact envelope");

    let out = String::from_utf8(rig.written).expect("utf8");
    assert_eq!(out, "ack:ping\n", "output must be the reply body line");
}

#[test]
fn m4_serve_conn_malformed_input_errors() {
    let mut rig = MemRig::new("{garbage\n");
    let (tx, _rx) = mpsc::channel();
    let mut handler = AckHandler { seen: tx };
    let err = serve_conn(&mut rig, &mut handler).unwrap_err();
    assert!(matches!(err, MeshError::Decode(_)), "got {err:?}");
}

// ---------------------------------------------------------------------
// M5 — loopback roundtrip
// ---------------------------------------------------------------------

#[test]
fn m5_loopback_roundtrip() {
    let listener = TcpListener::bind("127.0.0.1:0").expect("bind");
    let addr = listener.local_addr().expect("addr");

    let (tx, rx) = mpsc::channel();
    let server = thread::spawn(move || {
        let mut handler = AckHandler { seen: tx };
        let (mut stream, _peer) = listener.accept().expect("accept");
        stream
            .set_read_timeout(Some(Duration::from_secs(2)))
            .unwrap();
        serve_conn(&mut stream, &mut handler).expect("serve_conn");
    });

    let mut book = AddrBook::new();
    book.insert(host("h-remote"), addr.to_string());
    let mut mesh = TcpMesh::with_timeout(book, Duration::from_secs(2));

    let env = envelope(vec!["bob"], "payload-42");
    let resolved = Resolved::Remote {
        peer: agent("bob"),
        host: host("h-remote"),
    };
    let reply = mesh.deliver(&resolved, &env).expect("deliver");
    assert_eq!(reply, "ack:payload-42", "reply must be the handler body");

    let seen = rx.recv_timeout(Duration::from_secs(2)).expect("seen");
    assert_eq!(seen, env, "handler saw the right envelope");

    server.join().expect("join server thread");
}

// ---------------------------------------------------------------------
// M6 — deliver on Local -> Err("local"), no network
// ---------------------------------------------------------------------

#[test]
fn m6_deliver_local_errors_without_network() {
    // Empty AddrBook => if a socket were attempted it could only fail via a
    // network path; a Local route must short-circuit before any lookup/connect.
    let mut mesh = TcpMesh::new(AddrBook::new());
    let env = envelope(vec!["bob"], "x");
    let resolved = Resolved::Local { peer: agent("bob") };
    let err = mesh.deliver(&resolved, &env).unwrap_err();
    assert!(
        err.to_lowercase().contains("local"),
        "local error must mention 'local', got {err:?}"
    );
}

// ---------------------------------------------------------------------
// M7 — NoAddress + connect-refused Io
// ---------------------------------------------------------------------

#[test]
fn m7_deliver_absent_host_is_no_address() {
    let mut mesh = TcpMesh::new(AddrBook::new());
    let env = envelope(vec!["bob"], "x");
    let resolved = Resolved::Remote {
        peer: agent("bob"),
        host: host("nowhere"),
    };
    let err = mesh.deliver(&resolved, &env).unwrap_err();
    assert!(
        err.contains("no mesh address") && err.contains("nowhere"),
        "absent host must be a NoAddress error, got {err:?}"
    );
}

#[test]
fn m7_deliver_dead_address_is_io_error() {
    // Bind a listener, note its addr, then drop it -> connections are refused.
    let listener = TcpListener::bind("127.0.0.1:0").expect("bind");
    let addr = listener.local_addr().expect("addr");
    drop(listener);

    let mut book = AddrBook::new();
    book.insert(host("dead"), addr.to_string());
    let mut mesh = TcpMesh::with_timeout(book, Duration::from_millis(500));

    let env = envelope(vec!["bob"], "x");
    let resolved = Resolved::Remote {
        peer: agent("bob"),
        host: host("dead"),
    };
    let err = mesh.deliver(&resolved, &env).unwrap_err();
    assert!(
        err.contains("mesh io error"),
        "refused connect must be an Io error, got {err:?}"
    );
}
