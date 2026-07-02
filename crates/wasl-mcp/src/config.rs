//! wasl-mcp node configuration (W2) — identity, binds, audit path, adapter
//! selection, and the peer list that populates the registry at startup.
//!
//! A TOML file (all sections/keys optional) overlaid by four legacy env vars
//! (unchanged precedence/semantics from today's `wasl-mcp::run()`), overlaid
//! on built-in defaults. `WASL_CONFIG` is the only new env var, and it only
//! names the file to read — see `docs/specs/node-config.md` (authoritative).
//!
//! Parsing/validation (`from_toml_str`, `apply_env`, `resolve_path`) is pure —
//! no env reads, no filesystem IO — so the precedence/validation rules are
//! black-box tested without ever touching the real process env. `load_with`
//! is the only function in this module that touches the filesystem, and
//! `load`/`EnvOverrides::from_process_env` are the only functions that touch
//! the real process env.

use std::collections::HashSet;
use std::fmt;
use std::net::SocketAddr;
use std::path::PathBuf;

use serde::Deserialize;

// ---------------------------------------------------------------------
// 1. Public config shape
// ---------------------------------------------------------------------

/// The effective node configuration (file + env overlay + defaults).
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct Config {
    pub identity: IdentityConfig,
    pub a2a: A2aConfig,
    pub operator: OperatorConfig,
    pub audit: AuditConfig,
    pub adapter: AdapterConfig,
    pub peers: Vec<PeerEntry>,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct IdentityConfig {
    pub agent_id: String,
    pub host_id: String,
}

/// The A2A receive bind. Stored as a string (not `SocketAddr`) even though the
/// FILE value is validated at load time: the env override (`WASL_A2A_BIND`)
/// replaces this string unvalidated, and `run()` re-parses the *effective*
/// string leniently, exactly as it parses `WASL_A2A_BIND` today.
#[derive(Debug, Clone, Default, PartialEq, Eq)]
pub struct A2aConfig {
    pub bind: Option<String>,
}

/// RESERVED FOR W3: parsed + validated here, but deliberately unused by
/// `run()` in this block.
#[derive(Debug, Clone, Default, PartialEq, Eq)]
pub struct OperatorConfig {
    pub bind: Option<String>,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct AuditConfig {
    pub path: String,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct AdapterConfig {
    pub kind: AdapterKind,
    pub bin: Option<String>,
}

/// The adapter selector. Exactly one variant in this block; W7 (codex) adds
/// more.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum AdapterKind {
    Claude,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct PeerEntry {
    pub agent_id: String,
    pub host_id: String,
    pub url: String,
}

impl Default for Config {
    fn default() -> Self {
        Config {
            identity: IdentityConfig {
                agent_id: "wasl-local".to_string(),
                host_id: "localhost".to_string(),
            },
            a2a: A2aConfig::default(),
            operator: OperatorConfig::default(),
            audit: AuditConfig {
                path: "wasl-audit.jsonl".to_string(),
            },
            adapter: AdapterConfig {
                kind: AdapterKind::Claude,
                bin: None,
            },
            peers: Vec::new(),
        }
    }
}

// ---------------------------------------------------------------------
// 2. ConfigError
// ---------------------------------------------------------------------

/// Distinct, testable config-loading/validation errors.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum ConfigError {
    /// The config file could not be read (explicit `WASL_CONFIG` path
    /// missing, or any existing file unreadable).
    Read { path: String, detail: String },
    /// Invalid TOML (syntax, wrong types, unknown keys).
    Toml(String),
    /// `adapter.kind` is not a known adapter.
    UnknownAdapterKind(String),
    /// A peer url is not an absolute http/https URL.
    MalformedPeerUrl {
        agent_id: String,
        url: String,
        detail: String,
    },
    /// Two `[[peers]]` entries share an agent_id.
    DuplicatePeer(String),
    /// `a2a.bind` / `operator.bind` does not parse as a socket address.
    InvalidBind {
        section: String,
        value: String,
        detail: String,
    },
}

impl fmt::Display for ConfigError {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            ConfigError::Read { path, detail } => write!(f, "read config {path}: {detail}"),
            ConfigError::Toml(detail) => write!(f, "invalid config TOML: {detail}"),
            ConfigError::UnknownAdapterKind(kind) => write!(f, "unknown adapter kind: {kind}"),
            ConfigError::MalformedPeerUrl {
                agent_id,
                url,
                detail,
            } => write!(f, "malformed peer url for {agent_id}: {url}: {detail}"),
            ConfigError::DuplicatePeer(id) => write!(f, "duplicate peer agent_id: {id}"),
            ConfigError::InvalidBind {
                section,
                value,
                detail,
            } => write!(f, "invalid {section}.bind {value:?}: {detail}"),
        }
    }
}

impl std::error::Error for ConfigError {}

// ---------------------------------------------------------------------
// 3. TOML deserialization shape (raw, pre-validation)
// ---------------------------------------------------------------------
//
// `adapter.kind` and `peers[].url` deserialize as plain strings here and are
// validated in the explicit second step below (`from_toml_str`), so their
// failures are the distinct `UnknownAdapterKind`/`MalformedPeerUrl` errors,
// never a generic `Toml` error. Every struct denies unknown fields so a
// typo'd key fails loud instead of being silently dropped.

#[derive(Debug, Deserialize, Default)]
#[serde(deny_unknown_fields, default)]
struct RawConfig {
    identity: RawIdentity,
    a2a: RawBind,
    operator: RawBind,
    audit: RawAudit,
    adapter: RawAdapter,
    peers: Vec<RawPeer>,
}

#[derive(Debug, Deserialize)]
#[serde(deny_unknown_fields, default)]
struct RawIdentity {
    agent_id: String,
    host_id: String,
}

impl Default for RawIdentity {
    fn default() -> Self {
        RawIdentity {
            agent_id: "wasl-local".to_string(),
            host_id: "localhost".to_string(),
        }
    }
}

#[derive(Debug, Deserialize, Default)]
#[serde(deny_unknown_fields, default)]
struct RawBind {
    bind: Option<String>,
}

#[derive(Debug, Deserialize)]
#[serde(deny_unknown_fields, default)]
struct RawAudit {
    path: String,
}

impl Default for RawAudit {
    fn default() -> Self {
        RawAudit {
            path: "wasl-audit.jsonl".to_string(),
        }
    }
}

#[derive(Debug, Deserialize)]
#[serde(deny_unknown_fields, default)]
struct RawAdapter {
    kind: String,
    bin: Option<String>,
}

impl Default for RawAdapter {
    fn default() -> Self {
        RawAdapter {
            kind: "claude".to_string(),
            bin: None,
        }
    }
}

#[derive(Debug, Deserialize)]
#[serde(deny_unknown_fields)]
struct RawPeer {
    agent_id: String,
    host_id: String,
    url: String,
}

// ---------------------------------------------------------------------
// 4. EnvOverrides — the pure env seam
// ---------------------------------------------------------------------

/// Everything `Config` reads from the environment, captured as plain data so
/// resolution is pure and tests never mutate process-global env.
#[derive(Debug, Clone, Default, PartialEq, Eq)]
pub struct EnvOverrides {
    pub config: Option<String>,
    pub xdg_config_home: Option<String>,
    pub home: Option<String>,
    pub agent_id: Option<String>,
    pub host_id: Option<String>,
    pub audit_path: Option<String>,
    pub a2a_bind: Option<String>,
}

impl EnvOverrides {
    /// Snapshot the real process env.
    pub fn from_process_env() -> Self {
        EnvOverrides {
            config: std::env::var("WASL_CONFIG").ok(),
            xdg_config_home: std::env::var("XDG_CONFIG_HOME").ok(),
            home: std::env::var("HOME").ok(),
            agent_id: std::env::var("WASL_AGENT_ID").ok(),
            host_id: std::env::var("WASL_HOST_ID").ok(),
            audit_path: std::env::var("WASL_AUDIT_PATH").ok(),
            a2a_bind: std::env::var("WASL_A2A_BIND").ok(),
        }
    }
}

// ---------------------------------------------------------------------
// 5. Config: parse, validate, overlay, resolve, load
// ---------------------------------------------------------------------

impl Config {
    /// Parse + validate a TOML document. Pure (no env, no fs). Applies
    /// defaults for missing sections/keys; runs ALL validation in order: TOML
    /// syntax/shape -> binds -> adapter kind -> peer urls -> duplicate peers.
    pub fn from_toml_str(s: &str) -> Result<Config, ConfigError> {
        let raw: RawConfig = toml::from_str(s).map_err(|e| ConfigError::Toml(e.to_string()))?;

        let a2a_bind = validate_bind("a2a", raw.a2a.bind)?;
        let operator_bind = validate_bind("operator", raw.operator.bind)?;

        let kind = match raw.adapter.kind.as_str() {
            "claude" => AdapterKind::Claude,
            other => return Err(ConfigError::UnknownAdapterKind(other.to_string())),
        };

        for p in &raw.peers {
            validate_peer_url(&p.agent_id, &p.url)?;
        }

        let mut seen = HashSet::with_capacity(raw.peers.len());
        for p in &raw.peers {
            if !seen.insert(p.agent_id.clone()) {
                return Err(ConfigError::DuplicatePeer(p.agent_id.clone()));
            }
        }

        let peers = raw
            .peers
            .into_iter()
            .map(|p| PeerEntry {
                agent_id: p.agent_id,
                host_id: p.host_id,
                url: p.url,
            })
            .collect();

        Ok(Config {
            identity: IdentityConfig {
                agent_id: raw.identity.agent_id,
                host_id: raw.identity.host_id,
            },
            a2a: A2aConfig { bind: a2a_bind },
            operator: OperatorConfig {
                bind: operator_bind,
            },
            audit: AuditConfig {
                path: raw.audit.path,
            },
            adapter: AdapterConfig {
                kind,
                bin: raw.adapter.bin,
            },
            peers,
        })
    }

    /// Overlay env values (the four value overrides; `config`/xdg/home are
    /// for path resolution only, unused here). Pure. Env wins.
    pub fn apply_env(mut self, env: &EnvOverrides) -> Config {
        if let Some(v) = &env.agent_id {
            self.identity.agent_id = v.clone();
        }
        if let Some(v) = &env.host_id {
            self.identity.host_id = v.clone();
        }
        if let Some(v) = &env.audit_path {
            self.audit.path = v.clone();
        }
        if let Some(v) = &env.a2a_bind {
            self.a2a.bind = Some(v.clone());
        }
        self
    }

    /// Resolve the config path from `env`. Pure. `None` = no file to read
    /// (use defaults).
    ///
    /// 1. `env.config` set (even to `""`) wins outright.
    /// 2. Else `env.xdg_config_home` set-and-non-empty ->
    ///    `$XDG_CONFIG_HOME/wasl/config.toml`.
    /// 3. Else `env.home` set-and-non-empty -> `$HOME/.config/wasl/config.toml`.
    /// 4. Else `None`.
    pub fn resolve_path(env: &EnvOverrides) -> Option<PathBuf> {
        if let Some(path) = &env.config {
            return Some(PathBuf::from(path));
        }
        if let Some(xdg) = &env.xdg_config_home {
            if !xdg.is_empty() {
                return Some(PathBuf::from(xdg).join("wasl").join("config.toml"));
            }
        }
        if let Some(home) = &env.home {
            if !home.is_empty() {
                return Some(
                    PathBuf::from(home)
                        .join(".config")
                        .join("wasl")
                        .join("config.toml"),
                );
            }
        }
        None
    }

    /// The full pipeline against captured env: resolve path -> read file
    /// (explicit-path missing -> `Err(Read)`; default-path missing ->
    /// defaults; existing-but-unreadable -> `Err(Read)`) -> `from_toml_str`
    /// -> `apply_env`.
    ///
    /// No resolvable path (`resolve_path` -> `None`) is folded into the same
    /// "nothing to read" arm as a missing default-path file: both mean
    /// "there is no file here", and `fs::read_to_string` on the empty
    /// `PathBuf` used as the no-path sentinel reliably fails with
    /// `NotFound`, same as a genuinely missing default-path file.
    pub fn load_with(env: &EnvOverrides) -> Result<Config, ConfigError> {
        let explicit = env.config.is_some();
        let path = Config::resolve_path(env).unwrap_or_default();
        let cfg = match std::fs::read_to_string(&path) {
            Ok(contents) => Config::from_toml_str(&contents)?,
            Err(err) if !explicit && err.kind() == std::io::ErrorKind::NotFound => {
                Config::default()
            }
            Err(err) => {
                return Err(ConfigError::Read {
                    path: path.to_string_lossy().into_owned(),
                    detail: err.to_string(),
                })
            }
        };
        Ok(cfg.apply_env(env))
    }

    /// `load_with(&EnvOverrides::from_process_env())` — the one-liner `run()`
    /// calls.
    pub fn load() -> Result<Config, ConfigError> {
        Config::load_with(&EnvOverrides::from_process_env())
    }
}

fn validate_bind(section: &str, bind: Option<String>) -> Result<Option<String>, ConfigError> {
    match bind {
        None => Ok(None),
        Some(value) => match value.parse::<SocketAddr>() {
            Ok(_) => Ok(Some(value)),
            Err(err) => Err(ConfigError::InvalidBind {
                section: section.to_string(),
                value,
                detail: err.to_string(),
            }),
        },
    }
}

fn validate_peer_url(agent_id: &str, url: &str) -> Result<(), ConfigError> {
    let malformed = |detail: String| ConfigError::MalformedPeerUrl {
        agent_id: agent_id.to_string(),
        url: url.to_string(),
        detail,
    };
    let parsed = reqwest::Url::parse(url).map_err(|err| malformed(err.to_string()))?;
    match parsed.scheme() {
        "http" | "https" => Ok(()),
        other => Err(malformed(format!("unsupported URL scheme: {other}"))),
    }
}

// ---------------------------------------------------------------------
// 6. Consumption seams: registry population + adapter factory
// ---------------------------------------------------------------------

/// Build the peer registry from the config's peer entries.
pub fn registry_from_peers(peers: &[PeerEntry]) -> wasl_registry::PeerRegistry {
    let mut registry = wasl_registry::PeerRegistry::new();
    for peer in peers {
        registry.register(wasl_proto::AgentCard {
            id: wasl_proto::AgentId(peer.agent_id.clone()),
            host: wasl_proto::HostId(peer.host_id.clone()),
            endpoint: Some(peer.url.clone()),
        });
    }
    registry
}

/// Construct the receive-side adapter selected by the config. Written as an
/// exhaustive match on `AdapterKind` so adding a kind (W7 codex) forces a
/// decision here.
pub fn build_adapter(
    cfg: &AdapterConfig,
) -> wasl_adapter_claude::ClaudeAdapter<wasl_adapter_claude::ProcessCliInvoker> {
    match cfg.kind {
        AdapterKind::Claude => match &cfg.bin {
            None => wasl_adapter_claude::ClaudeAdapter::new(wasl_adapter_claude::ProcessCliInvoker),
            Some(bin) => wasl_adapter_claude::ClaudeAdapter::with_bin(
                wasl_adapter_claude::ProcessCliInvoker,
                bin.clone(),
            ),
        },
    }
}
