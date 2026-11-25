//! `wasl`: Wasl without Marsad — a host daemon (Wasit session runtime + Wasl node) and its
//! helper entries in one binary.
//!
//! ```text
//! wasl serve  --data <dir> [--socket <path>] [--host-id <id>] [options]
//! wasl bridge --data <dir> [--socket <path>] [--host-id <id>] [--no-start] [options]
//! wasl mcp    --socket <path> --session <id>
//! wasl shim   <wasit-shim arguments>
//! ```
//!
//! * `serve` takes the single-instance lock next to the socket, re-adopts the sessions under
//!   `<dir>/sessions/`, opens the Wasl node (`<dir>/wasl/`), prints one JSON readiness line and
//!   serves the runtime's methods plus `wasl.*` until SIGTERM / SIGINT. Sessions keep running
//!   when it exits. The socket defaults to `<dir>/wasl.sock` (Windows: a named pipe derived
//!   from that path). A connection whose peer says `role: "hub"` is the node's uplink.
//! * `bridge` pipes stdio to the daemon (starting a detached `serve` with the same options
//!   unless `--no-start`) — the command a hub runs to link this host.
//! * `mcp` is the stdio MCP server an agent CLI launches (see `wasl-mcp`).
//! * `shim` is the per-session process holder entry.
//!
//! Diagnostics (`wasit-diag`): typed, content-free events (ids, sizes, counts, timings, error
//! codes; never a prompt, a result, a name or a reason an agent wrote). Once `serve` holds its
//! lock they go to `<dir>/diag/daemon.ndjson` (8 MiB, three older generations; the directory is
//! `0700`, the file `0600`) and a panic leaves a `diag.panic` record there instead of text on
//! stderr; `bridge` appends its few records to the machine's shared `<dir>/diag/aux.ndjson`
//! (which the daemon keeps within 1 MiB); `mcp` is a helper an agent CLI starts, knows a socket
//! and a session but no data directory, and has no sink: its effects are recorded by the
//! daemon methods it calls, and a panic there prints and records nothing. `tracing` output
//! (the libraries' and third parties') is counted per minute, not written. Developer override:
//! `WASL_LOG=<filter>` prints plain `tracing` text (no colour) on stderr and lets a panic
//! print its usual message too; `WASIT_DIAG_QUEUE=<n>` sizes the record queue.
//!
//! Options: `--driver-bin <driver>=<path>`, `--driver-env <driver>=<KEY>=<VALUE>` (repeatable),
//! `--handshake-timeout-ms N`, `--command-timeout-ms N`, `--stop-grace-ms N`,
//! `--spawn-wait-ms N` (how long a spawn request waits for an unreachable supervisor),
//! `--spawn-reattach-ms N` (how long an asking spawn request waits for its caller to come back
//! before it is withdrawn).

use std::ffi::OsString;
use std::path::PathBuf;
use std::process::ExitCode;
use std::sync::Arc;
use std::time::Duration;

use tracing_subscriber::prelude::*;
use wasit_control::{
    bind_local_exclusive, local_endpoint, ConnConfig, ControlServer, Hello, ServiceMux,
};
use wasit_core::DriverId;
use wasit_diag::{Code, ErrorInfo, HomePath, Ident, RotateShared, SinkConfig};
use wasit_runtime::{
    Discovery, DiscoveryConfig, RuntimeConfig, RuntimeService, SessionManager, ShimLauncher,
};
use wasl_node::{Node, NodeConfig, NodeService};
use wasl_proto::diag::{count, emit, WaslEvent};

const BUILD: &str = concat!("wasl/", env!("CARGO_PKG_VERSION"));
/// The build as a diagnostics identifier (which has no `/`).
const BUILD_ID: &str = concat!("wasl-", env!("CARGO_PKG_VERSION"));
/// The developer override: plain `tracing` text on stderr, and the usual panic message.
const DEV_ENV: &str = "WASL_LOG";
/// The shared file short-lived processes of this data directory (`bridge`) append to; the
/// daemon rotates it when it holds more than `AUX_ROTATE` bytes, a writer stops at `AUX_CAP`.
const AUX_FILE: &str = "aux.ndjson";
const AUX_ROTATE: u64 = 1 << 20;
const AUX_CAP: u64 = 4 << 20;

/// `--panic-test <marker>` (any subcommand): panics with a message holding the marker once the
/// process's diagnostics are set up. For `tests/diag_panic.rs`; only in builds with the
/// `panic-test` feature, which this package's dev-dependency on itself turns on.
#[cfg(feature = "panic-test")]
static PANIC_MARKER: std::sync::OnceLock<String> = std::sync::OnceLock::new();

/// Takes `--panic-test <marker>` out of `args`.
#[cfg(feature = "panic-test")]
fn take_panic_test(mut args: Vec<String>) -> Vec<String> {
    if let Some(i) = args.iter().position(|a| a == "--panic-test") {
        if i + 1 < args.len() {
            let marker = args.remove(i + 1);
            args.remove(i);
            let _ = PANIC_MARKER.set(marker);
        }
    }
    args
}

#[cfg(feature = "panic-test")]
fn panic_point() {
    if let Some(m) = PANIC_MARKER.get() {
        panic!("wasl panic test: {m}");
    }
}

#[cfg(not(feature = "panic-test"))]
fn panic_point() {}

/// Installs this process's diagnostics sink and the panic hook, and records `process.start`.
/// A failure has nowhere to go but stderr.
fn install_diagnostics(cfg: SinkConfig) {
    match wasit_diag::install(cfg) {
        Ok(()) => {
            wasit_diag::install_panic_hook(std::env::var_os(DEV_ENV).is_some());
            wasit_diag::announce_process();
        }
        Err(e) => eprintln!("wasl: no diagnostics: {e}"),
    }
}

#[derive(Debug, Clone)]
struct Args {
    data: PathBuf,
    socket: Option<PathBuf>,
    host_id: Option<String>,
    driver_bin: Vec<(String, String)>,
    driver_env: Vec<(String, String, String)>,
    handshake_ms: Option<u64>,
    stop_grace_ms: Option<u64>,
    command_ms: Option<u64>,
    spawn_wait_ms: Option<u64>,
    spawn_reattach_ms: Option<u64>,
    no_start: bool,
    /// The options as given minus bridge-only flags (passed on when `bridge` starts `serve`).
    raw: Vec<String>,
}

fn parse(args: &[String]) -> Result<Args, String> {
    let mut a = Args {
        data: PathBuf::new(),
        socket: None,
        host_id: None,
        driver_bin: Vec::new(),
        driver_env: Vec::new(),
        handshake_ms: None,
        stop_grace_ms: None,
        command_ms: None,
        spawn_wait_ms: None,
        spawn_reattach_ms: None,
        no_start: false,
        raw: args
            .iter()
            .filter(|a| *a != "--no-start")
            .cloned()
            .collect(),
    };
    let mut data = None;
    let mut it = args.iter();
    while let Some(flag) = it.next() {
        let mut val = || it.next().cloned().ok_or(format!("{flag} needs a value"));
        let num = |v: String| v.parse::<u64>().map_err(|e| format!("{flag}: {e}"));
        match flag.as_str() {
            "--data" => data = Some(PathBuf::from(val()?)),
            "--socket" => a.socket = Some(PathBuf::from(val()?)),
            "--host-id" => a.host_id = Some(val()?),
            "--no-start" => a.no_start = true,
            "--driver-bin" => {
                let v = val()?;
                let (d, p) = v
                    .split_once('=')
                    .ok_or(format!("--driver-bin expects DRIVER=PATH, got {v:?}"))?;
                a.driver_bin.push((d.into(), p.into()));
            }
            "--driver-env" => {
                let v = val()?;
                let (d, kv) = v
                    .split_once('=')
                    .ok_or(format!("--driver-env expects DRIVER=KEY=VALUE, got {v:?}"))?;
                let (k, val) = kv
                    .split_once('=')
                    .ok_or(format!("--driver-env expects DRIVER=KEY=VALUE, got {v:?}"))?;
                a.driver_env.push((d.into(), k.into(), val.into()));
            }
            "--handshake-timeout-ms" => a.handshake_ms = Some(num(val()?)?),
            "--stop-grace-ms" => a.stop_grace_ms = Some(num(val()?)?),
            "--command-timeout-ms" => a.command_ms = Some(num(val()?)?),
            "--spawn-wait-ms" => a.spawn_wait_ms = Some(num(val()?)?),
            "--spawn-reattach-ms" => a.spawn_reattach_ms = Some(num(val()?)?),
            other => return Err(format!("unknown option {other}")),
        }
    }
    a.data = data.ok_or("--data <dir> is required")?;
    Ok(a)
}

fn socket_path(a: &Args) -> PathBuf {
    a.socket.clone().unwrap_or_else(|| a.data.join("wasl.sock"))
}

fn runtime_config(a: &Args) -> Result<RuntimeConfig, String> {
    let launcher = ShimLauncher::current_exe("shim").map_err(|e| e.to_string())?;
    let mut cfg = RuntimeConfig::new(a.data.clone(), launcher);
    for (d, p) in &a.driver_bin {
        let id = DriverId::new(d.clone());
        cfg.registry.get(&id).map_err(|e| e.to_string())?;
        cfg.overrides.entry(id).or_default().binary = Some(p.clone());
    }
    for (d, k, v) in &a.driver_env {
        let id = DriverId::new(d.clone());
        cfg.registry.get(&id).map_err(|e| e.to_string())?;
        cfg.overrides
            .entry(id)
            .or_default()
            .env
            .push((k.clone(), v.clone()));
    }
    if let Some(ms) = a.handshake_ms {
        cfg.timeouts.handshake = Duration::from_millis(ms);
    }
    if let Some(ms) = a.stop_grace_ms {
        cfg.timeouts.stop_grace = Duration::from_millis(ms);
    }
    if let Some(ms) = a.command_ms {
        cfg.timeouts.command = Duration::from_millis(ms);
    }
    Ok(cfg)
}

fn default_host_id() -> String {
    std::env::var("HOSTNAME")
        .or_else(|_| std::env::var("COMPUTERNAME"))
        .unwrap_or_else(|_| "localhost".into())
}

async fn serve(a: Args) -> ExitCode {
    let code = serve_until_stopped(a).await;
    // Waits (bounded) for the writer to write what was recorded, also when the daemon could
    // not start; the process is ending.
    let _written = wasit_diag::shutdown();
    code
}

async fn serve_until_stopped(a: Args) -> ExitCode {
    if let Err(e) = tokio::fs::create_dir_all(&a.data).await {
        eprintln!("wasl: cannot create {}: {e}", a.data.display());
        return ExitCode::from(1);
    }
    let cfg = match runtime_config(&a) {
        Ok(c) => c,
        Err(e) => {
            eprintln!("wasl: {e}");
            return ExitCode::from(2);
        }
    };
    let listener = match bind_local_exclusive(socket_path(&a)).await {
        Ok(l) => l,
        Err(e) => {
            eprintln!("wasl: cannot listen: {e}");
            return ExitCode::from(3);
        }
    };
    // The lock is held: this process owns the data directory's `daemon.ndjson`.
    let mut sink = SinkConfig::new(
        Code::st("daemon"),
        Ident::st(BUILD_ID),
        a.data.join("diag"),
        "daemon.ndjson",
    );
    sink.rotate_shared = Some(RotateShared {
        file: AUX_FILE.into(),
        max_bytes: AUX_ROTATE,
        generations: 1,
    });
    install_diagnostics(sink);
    let host_id = a.host_id.clone().unwrap_or_else(default_host_id);
    emit(WaslEvent::DaemonStart {
        host: Ident::new(&host_id),
        data: HomePath::new(&a.data),
        socket: HomePath::new(&socket_path(&a)),
    });
    panic_point();
    let discovery = Discovery::new(DiscoveryConfig::from_runtime(&cfg));
    let manager = match SessionManager::new(cfg).await {
        Ok(m) => m,
        Err(e) => {
            eprintln!("wasl: {e}");
            return ExitCode::from(1);
        }
    };
    match manager.adopt_all().await {
        Ok(r) => {
            emit(WaslEvent::DaemonAdopted {
                running: count(r.running.len()),
                finished: count(r.finished.len()),
                errors: count(r.errors.len()),
            });
            for (dir, e) in r.errors {
                emit(WaslEvent::DaemonAdoptError {
                    session: Ident::new(
                        &dir.file_name()
                            .map_or_else(String::new, |n| n.to_string_lossy().into_owned()),
                    ),
                    // The runtime's reason can quote a file of the session: size and
                    // fingerprint only.
                    error: wasl_proto::diag::foreign("adopt", &e),
                });
            }
        }
        // Only `io_err` of the runtime's directory scan: an operation and the OS's reason.
        Err(e) => emit(WaslEvent::DaemonScanFailed {
            error: ErrorInfo::ours(Code::st("scan"), &e),
        }),
    }
    let mut cfg = NodeConfig::new(host_id.clone(), a.data.join("wasl"));
    if let Some(ms) = a.spawn_wait_ms {
        cfg.spawn_connect_wait = std::time::Duration::from_millis(ms);
    }
    if let Some(ms) = a.spawn_reattach_ms {
        cfg.spawn_reattach_grace = std::time::Duration::from_millis(ms);
    }
    let node = match Node::open(cfg, manager.clone()).await {
        Ok(n) => n,
        Err(e) => {
            eprintln!("wasl: cannot open the Wasl node: {e}");
            return ExitCode::from(1);
        }
    };
    let runtime = RuntimeService::new(manager, discovery, host_id.clone(), BUILD);
    let mux = ServiceMux::new()
        .with(
            wasit_runtime::wire::ROUTES.iter().copied(),
            Arc::new(runtime),
        )
        .and_then(|m| {
            m.with(
                wasl_proto::ROUTES.iter().copied(),
                Arc::new(NodeService::new(node.clone())),
            )
        });
    let mux = match mux {
        Ok(m) => m,
        Err(e) => {
            eprintln!("wasl: {e}");
            return ExitCode::from(1);
        }
    };
    let server = ControlServer::new(
        Arc::new(mux),
        Hello::new(BUILD, host_id, "daemon"),
        ConnConfig::default(),
    );
    node.attach_server(&server);
    println!(
        "{}",
        serde_json::json!({"ready": true, "pid": std::process::id(), "socket": socket_path(&a)})
    );
    emit(WaslEvent::DaemonReady {
        pid: std::process::id(),
    });
    tokio::select! {
        r = server.serve(listener) => match r {
            Ok(()) => {
                emit(WaslEvent::DaemonShutdown { reason: Code::st("serve_ended") });
                ExitCode::SUCCESS
            }
            Err(e) => {
                eprintln!("wasl: serve failed: {e}");
                emit(WaslEvent::DaemonShutdown { reason: Code::st("serve_failed") });
                ExitCode::from(1)
            }
        },
        what = termination() => {
            emit(WaslEvent::DaemonShutdown { reason: Code::new(what) });
            server.shutdown(Duration::from_secs(2)).await;
            ExitCode::SUCCESS
        }
    }
}

#[cfg(unix)]
async fn termination() -> &'static str {
    use tokio::signal::unix::{signal, SignalKind};
    match (
        signal(SignalKind::terminate()),
        signal(SignalKind::interrupt()),
    ) {
        (Ok(mut t), Ok(mut i)) => tokio::select! {
            _ = t.recv() => "SIGTERM",
            _ = i.recv() => "SIGINT",
        },
        _ => {
            emit(WaslEvent::DaemonSignalHandlersFailed {});
            std::future::pending().await
        }
    }
}

#[cfg(windows)]
async fn termination() -> &'static str {
    match tokio::signal::ctrl_c().await {
        Ok(()) => "ctrl-c",
        Err(_) => {
            emit(WaslEvent::DaemonSignalHandlersFailed {});
            std::future::pending().await
        }
    }
}

async fn bridge(a: Args) -> ExitCode {
    // A short-lived process of the machine: its few records (start, a panic) are appended to
    // the shared file the daemon keeps within bounds.
    install_diagnostics(SinkConfig::shared(
        Code::st("bridge"),
        Ident::st(BUILD_ID),
        a.data.join("diag"),
        AUX_FILE,
        AUX_CAP,
    ));
    panic_point();
    let ep = local_endpoint(&socket_path(&a));
    let raw = a.raw.clone();
    let data = a.data.clone();
    let no_start = a.no_start;
    let start = move || async move {
        if no_start {
            return Err(std::io::Error::other(
                "the daemon is not running (--no-start)",
            ));
        }
        start_daemon(raw, data).await
    };
    match wasit_control::bridge(
        tokio::io::stdin(),
        tokio::io::stdout(),
        &ep,
        start,
        wasit_control::BridgeOptions::default(),
    )
    .await
    {
        Ok(()) => ExitCode::SUCCESS,
        Err(e) => {
            eprintln!("wasl bridge: {e}");
            ExitCode::from(1)
        }
    }
}

/// Start `serve` detached with the same options and wait (bounded by the bridge) for its
/// readiness line.
async fn start_daemon(raw: Vec<String>, data: PathBuf) -> std::io::Result<()> {
    use std::process::Stdio;
    use tokio::io::AsyncBufReadExt;
    tokio::fs::create_dir_all(&data).await?;
    // The daemon's stderr: crash output and developer (`WASL_LOG`) text, never diagnostics.
    let log = wasit_diag::private::open(&data.join("wasl.log"), |o| {
        o.append(true);
    })?;
    if log.metadata()?.len() > (1 << 20) {
        log.set_len(0)?;
    }
    let exe = std::env::current_exe()?;
    let mut cmd = wasit_process::detached_command(exe);
    cmd.arg("serve")
        .args(&raw)
        .stdin(Stdio::null())
        .stdout(Stdio::piped())
        .stderr(Stdio::from(log));
    let mut child = wasit_process::spawn_detached(&mut cmd)?;
    let stdout = child
        .stdout
        .take()
        .ok_or_else(|| std::io::Error::other("no stdout"))?;
    let mut line = String::new();
    tokio::io::BufReader::new(stdout)
        .read_line(&mut line)
        .await?;
    // EOF without a readiness line usually means another instance won the lock; the bridge's
    // connect retries decide.
    drop(child);
    Ok(())
}

fn usage() -> ExitCode {
    eprintln!(
        "usage: wasl serve|bridge --data <dir> [--socket <path>] [options] | wasl mcp --socket <path> --session <id> | wasl shim <args>"
    );
    ExitCode::from(2)
}

fn main() -> ExitCode {
    let mut args = std::env::args_os().skip(1);
    let Some(cmd) = args.next() else {
        return usage();
    };
    if cmd == "shim" {
        // Before anything else: the shim builds its own runtime, and its guard re-exec comes
        // back through here with the same argv.
        let rest: Vec<OsString> = args.collect();
        let code = wasit_shim::run(rest);
        return ExitCode::from(u8::try_from(code).unwrap_or(1));
    }
    let rest: Vec<String> = match args.map(|a| a.into_string()).collect() {
        Ok(v) => v,
        Err(bad) => {
            eprintln!("wasl: argument is not UTF-8: {bad:?}");
            return ExitCode::from(2);
        }
    };
    // A driver's wrapper (the bridge of a Kilo session): the sessions this host starts launch it.
    if let Some(code) = cmd
        .to_str()
        .and_then(|name| wasit_runtime::run_wrapper(name, &rest))
    {
        return code;
    }
    #[cfg(feature = "panic-test")]
    let rest = take_panic_test(rest);
    // `tracing` is counted into the diagnostics sink; its text goes nowhere unless a developer
    // sets `WASL_LOG` (then to stderr, without colour).
    let text = std::env::var_os(DEV_ENV).map(|_| {
        tracing_subscriber::fmt::layer()
            .with_ansi(false)
            .with_writer(std::io::stderr)
            .with_filter(
                tracing_subscriber::EnvFilter::try_from_env(DEV_ENV)
                    .unwrap_or_else(|_| tracing_subscriber::EnvFilter::new("info")),
            )
    });
    let _ = tracing_subscriber::registry()
        .with(wasit_diag::tracing_layer())
        .with(text)
        .try_init(); // only fails if a subscriber is already set
    if cmd == "mcp" {
        // No sink (see the module docs), but the hook: a panic prints and records nothing
        // unless a developer asked for stderr output.
        wasit_diag::install_panic_hook(std::env::var_os(DEV_ENV).is_some());
        panic_point();
        return wasl_mcp::run(&rest);
    }
    let parsed = match parse(&rest) {
        Ok(a) => a,
        Err(e) => {
            eprintln!("wasl: {e}");
            return usage();
        }
    };
    let rt = match tokio::runtime::Builder::new_multi_thread()
        .enable_all()
        .build()
    {
        Ok(rt) => rt,
        Err(e) => {
            eprintln!("wasl: cannot start the runtime: {e}");
            return ExitCode::from(1);
        }
    };
    match cmd.to_str() {
        Some("serve") => rt.block_on(serve(parsed)),
        Some("bridge") => {
            let code = rt.block_on(bridge(parsed));
            // Waits (bounded) for the writer; the process is ending.
            let _written = wasit_diag::shutdown();
            code
        }
        _ => usage(),
    }
}
