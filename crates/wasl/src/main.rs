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
//! Options: `--driver-bin <driver>=<path>`, `--driver-env <driver>=<KEY>=<VALUE>` (repeatable),
//! `--handshake-timeout-ms N`, `--command-timeout-ms N`, `--stop-grace-ms N`,
//! `--spawn-wait-ms N` (how long a spawn request waits for an unreachable supervisor).

use std::ffi::OsString;
use std::path::PathBuf;
use std::process::ExitCode;
use std::sync::Arc;
use std::time::Duration;

use wasit_control::{
    bind_local_exclusive, local_endpoint, ConnConfig, ControlServer, Hello, ServiceMux,
};
use wasit_core::DriverId;
use wasit_runtime::{
    Discovery, DiscoveryConfig, RuntimeConfig, RuntimeService, SessionManager, ShimLauncher,
};
use wasl_node::{Node, NodeConfig, NodeService};

const BUILD: &str = concat!("wasl/", env!("CARGO_PKG_VERSION"));

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
            for (dir, e) in r.errors {
                tracing::warn!(dir = %dir.display(), error = %e, "session not adopted");
            }
        }
        Err(e) => tracing::error!(error = %e, "cannot scan sessions"),
    }
    let host_id = a.host_id.clone().unwrap_or_else(default_host_id);
    let mut cfg = NodeConfig::new(host_id.clone(), a.data.join("wasl"));
    if let Some(ms) = a.spawn_wait_ms {
        cfg.spawn_connect_wait = std::time::Duration::from_millis(ms);
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
    tokio::select! {
        r = server.serve(listener) => {
            if let Err(e) = r {
                eprintln!("wasl: serve failed: {e}");
                return ExitCode::from(1);
            }
        }
        what = termination() => {
            tracing::info!("{what}: shutting down (sessions keep running)");
            server.shutdown(Duration::from_secs(2)).await;
        }
    }
    ExitCode::SUCCESS
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
            tracing::error!("cannot install signal handlers");
            std::future::pending().await
        }
    }
}

#[cfg(windows)]
async fn termination() -> &'static str {
    match tokio::signal::ctrl_c().await {
        Ok(()) => "ctrl-c",
        Err(_) => std::future::pending().await,
    }
}

async fn bridge(a: Args) -> ExitCode {
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
    let log = std::fs::OpenOptions::new()
        .create(true)
        .append(true)
        .open(data.join("wasl.log"))?;
    let exe = std::env::current_exe()?;
    let mut cmd = tokio::process::Command::new(exe);
    cmd.arg("serve")
        .args(&raw)
        .stdin(Stdio::null())
        .stdout(Stdio::piped())
        .stderr(Stdio::from(log))
        .kill_on_drop(false);
    #[cfg(unix)]
    cmd.process_group(0);
    #[cfg(windows)]
    {
        const DETACHED_PROCESS: u32 = 0x0000_0008;
        const CREATE_NEW_PROCESS_GROUP: u32 = 0x0000_0200;
        cmd.creation_flags(DETACHED_PROCESS | CREATE_NEW_PROCESS_GROUP);
    }
    let mut child = cmd.spawn()?;
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
    let _ = tracing_subscriber::fmt()
        .with_writer(std::io::stderr)
        .with_env_filter(
            tracing_subscriber::EnvFilter::try_from_env("WASL_LOG")
                .unwrap_or_else(|_| tracing_subscriber::EnvFilter::new("info")),
        )
        .try_init(); // only fails if a subscriber is already set
    if cmd == "mcp" {
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
        Some("bridge") => rt.block_on(bridge(parsed)),
        _ => usage(),
    }
}
