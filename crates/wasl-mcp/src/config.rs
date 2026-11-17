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

use crate::gating::GateAuthority;

// ---------------------------------------------------------------------
// 1. Public config shape
// ---------------------------------------------------------------------

/// The effective node configuration (file + env overlay + defaults).
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct Config {
    pub identity: IdentityConfig,
    pub a2a: A2aConfig,
    pub operator: OperatorConfig,
    pub gate: GateConfig,
    pub audit: AuditConfig,
    pub adapter: AdapterConfig,
    pub runtime: RuntimeConfig,
    pub peers: Vec<PeerEntry>,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct IdentityConfig {
    pub agent_id: String,
    pub host_id: String,
    /// The USER-facing display name of the host this node serves (from Marsad's
    /// host registry `PersistedHost.label`). Distinct from the opaque `host_id`
    /// (e.g. `"host-16"`): this is what a human named the machine (e.g.
    /// `"gpu-box"`). Empty when unknown (older config / un-upgraded caller).
    /// Surfaced to the inner agent via `list_peers`'s self identity so it can
    /// describe its OWN host by name.
    pub host_name: String,
}

/// The A2A receive bind. Stored as a string (not `SocketAddr`) even though the
/// FILE value is validated at load time: the env override (`WASL_A2A_BIND`)
/// replaces this string unvalidated, and `run()` re-parses the *effective*
/// string leniently, exactly as it parses `WASL_A2A_BIND` today.
#[derive(Debug, Clone, Default, PartialEq, Eq)]
pub struct A2aConfig {
    pub bind: Option<String>,
}

/// The node's operator surface bind (W2 parsed + validated it, W3 shipped the
/// crate it wires to, W4 `run()` CONSUMES it — see `docs/specs/mcp-gating.md` §1/§8).
#[derive(Debug, Clone, Default, PartialEq, Eq)]
pub struct OperatorConfig {
    pub bind: Option<String>,
}

/// Which authority may decide a gate (`docs/specs/mcp-gating.md` §1/§3).
/// `Default` = [`GateAuthority::Elicitation`] — exactly today's behavior.
#[derive(Debug, Clone, Copy, Default, PartialEq, Eq)]
pub struct GateConfig {
    pub authority: GateAuthority,
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

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct RuntimeConfig {
    pub kind: RuntimeKind,
    /// Optional TCP socket address for the runtime-control API (HTTP). `None`
    /// means the control API is not started.
    pub control_bind: Option<String>,
    /// Which protocol to use by default when no protocol is specified in a
    /// `CreateTargetRequest`. Defaults to `"claude"`.
    pub default_protocol: String,
    /// Override the `claude` binary path. `None` means use the system `PATH`.
    pub claude_bin: Option<String>,
    /// Override the `opencode` binary path. `None` means use the system `PATH`.
    pub opencode_bin: Option<String>,
    /// Override the `codex` binary path. `None` means use the system `PATH`.
    pub codex_bin: Option<String>,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum RuntimeKind {
    Ephemeral,
    Managed,
}

/// The adapter selector. W7 (`docs/specs/adapter-codex.md`) adds `Codex`
/// alongside the original `Claude`; W9 (`docs/specs/adapter-opencode.md`) adds
/// `Opencode`.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum AdapterKind {
    Claude,
    Codex,
    Opencode,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct PeerEntry {
    pub agent_id: String,
    pub host_id: String,
    pub url: String,
    /// The peer node's USER-facing display name (the sibling session's
    /// `SessionView.name`, e.g. `"Writer"`), so the inner agent can address a
    /// peer by the name a human gave it rather than an opaque
    /// `draft-node-<uuid>` agent_id. Empty when unknown (older roster / an
    /// un-upgraded caller that did not emit names). Serde-defaulted so a
    /// name-less `[[peers]]` block from an un-upgraded Marsad still parses.
    pub name: String,
    /// The peer node's host display name (its host's `PersistedHost.label`,
    /// e.g. `"gpu-box"`), distinct from the opaque `host_id`. Empty when
    /// unknown. Serde-defaulted (forward-compatible).
    pub host_name: String,
}

impl Default for Config {
    fn default() -> Self {
        Config {
            identity: IdentityConfig {
                agent_id: "wasl-local".to_string(),
                host_id: "localhost".to_string(),
                host_name: String::new(),
            },
            a2a: A2aConfig::default(),
            operator: OperatorConfig::default(),
            gate: GateConfig::default(),
            audit: AuditConfig {
                path: "wasl-audit.jsonl".to_string(),
            },
            adapter: AdapterConfig {
                kind: AdapterKind::Claude,
                bin: None,
            },
            runtime: RuntimeConfig::default(),
            peers: Vec::new(),
        }
    }
}

impl Default for RuntimeConfig {
    fn default() -> Self {
        RuntimeConfig {
            kind: RuntimeKind::Ephemeral,
            control_bind: None,
            default_protocol: "claude".to_string(),
            claude_bin: None,
            opencode_bin: None,
            codex_bin: None,
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
    /// `runtime.kind` is not a known runtime kind.
    UnknownRuntimeKind(String),
    /// `gate.authority` is not one of the four known authorities.
    UnknownGateAuthority(String),
    /// A peer url is not an absolute http/https URL.
    MalformedPeerUrl {
        agent_id: String,
        url: String,
        detail: String,
    },
    /// Two `[[peers]]` entries share an agent_id.
    DuplicatePeer(String),
    /// `a2a.bind` / `operator.bind` / `runtime.control_bind` does not parse as
    /// a socket address.
    InvalidBind {
        section: String,
        value: String,
        detail: String,
    },
    /// `runtime.default_protocol` is not one of the known protocol names
    /// (`"claude"`, `"opencode-acp"`).
    UnknownProtocol(String),
}

impl fmt::Display for ConfigError {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            ConfigError::Read { path, detail } => write!(f, "read config {path}: {detail}"),
            ConfigError::Toml(detail) => write!(f, "invalid config TOML: {detail}"),
            ConfigError::UnknownAdapterKind(kind) => write!(f, "unknown adapter kind: {kind}"),
            ConfigError::UnknownRuntimeKind(kind) => write!(f, "unknown runtime kind: {kind}"),
            ConfigError::UnknownGateAuthority(v) => write!(f, "unknown gate authority: {v}"),
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
            ConfigError::UnknownProtocol(proto) => {
                write!(f, "unknown runtime.default_protocol: {proto}")
            }
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
    gate: RawGate,
    audit: RawAudit,
    adapter: RawAdapter,
    runtime: RawRuntime,
    peers: Vec<RawPeer>,
}

#[derive(Debug, Deserialize)]
#[serde(deny_unknown_fields, default)]
struct RawIdentity {
    agent_id: String,
    host_id: String,
    /// User-facing host display name. Serde-defaulted (empty) so an un-upgraded
    /// config with no `host_name` still parses.
    host_name: String,
}

impl Default for RawIdentity {
    fn default() -> Self {
        RawIdentity {
            agent_id: "wasl-local".to_string(),
            host_id: "localhost".to_string(),
            host_name: String::new(),
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
struct RawGate {
    authority: String,
}

impl Default for RawGate {
    fn default() -> Self {
        RawGate {
            authority: "elicitation".to_string(),
        }
    }
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
#[serde(deny_unknown_fields, default)]
struct RawRuntime {
    kind: String,
    control_bind: Option<String>,
    default_protocol: String,
    claude_bin: Option<String>,
    opencode_bin: Option<String>,
    codex_bin: Option<String>,
}

impl Default for RawRuntime {
    fn default() -> Self {
        RawRuntime {
            kind: "ephemeral".to_string(),
            control_bind: None,
            default_protocol: "claude".to_string(),
            claude_bin: None,
            opencode_bin: None,
            codex_bin: None,
        }
    }
}

#[derive(Debug, Deserialize, Default)]
#[serde(deny_unknown_fields, default)]
struct RawPeer {
    agent_id: String,
    host_id: String,
    url: String,
    /// Peer display name; serde-defaulted (empty) so a name-less `[[peers]]`
    /// block from an un-upgraded Marsad still parses.
    name: String,
    /// Peer host display name; serde-defaulted (empty).
    host_name: String,
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
    /// The WHOLE config as inline TOML content (`WASL_CONFIG_CONTENT`), an
    /// alternative to the `WASL_CONFIG` file PATH. Takes precedence over the path.
    /// This exists so a caller that spawns `wasl-mcpd` on a DIFFERENT machine
    /// (e.g. Marsad launching a worker's A2A MCP server on a remote node) can
    /// deliver the config by VALUE — a path minted on the caller's filesystem is
    /// meaningless on the node's. Empty string is treated as unset.
    pub content: Option<String>,
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
            content: std::env::var("WASL_CONFIG_CONTENT")
                .ok()
                .filter(|s| !s.is_empty()),
        }
    }
}

// ---------------------------------------------------------------------
// 5. Config: parse, validate, overlay, resolve, load
// ---------------------------------------------------------------------

impl Config {
    /// Parse + validate a TOML document. Pure (no env, no fs). Applies
    /// defaults for missing sections/keys; runs ALL validation in order: TOML
    /// syntax/shape -> binds -> adapter kind -> gate authority -> peer urls ->
    /// duplicate peers.
    pub fn from_toml_str(s: &str) -> Result<Config, ConfigError> {
        let raw: RawConfig = toml::from_str(s).map_err(|e| ConfigError::Toml(e.to_string()))?;

        let a2a_bind = validate_bind("a2a", raw.a2a.bind)?;
        let operator_bind = validate_bind("operator", raw.operator.bind)?;

        let kind = match raw.adapter.kind.as_str() {
            "claude" => AdapterKind::Claude,
            "codex" => AdapterKind::Codex,
            "opencode" => AdapterKind::Opencode,
            other => return Err(ConfigError::UnknownAdapterKind(other.to_string())),
        };

        let runtime_control_bind = validate_bind("runtime", raw.runtime.control_bind)?;

        let runtime_kind = match raw.runtime.kind.as_str() {
            "ephemeral" => RuntimeKind::Ephemeral,
            "managed" => RuntimeKind::Managed,
            other => return Err(ConfigError::UnknownRuntimeKind(other.to_string())),
        };

        match raw.runtime.default_protocol.as_str() {
            "claude" | "opencode-acp" | "codex" => {}
            other => return Err(ConfigError::UnknownProtocol(other.to_string())),
        };

        let authority = match raw.gate.authority.as_str() {
            "elicitation" => GateAuthority::Elicitation,
            "elicitation-first" => GateAuthority::ElicitationFirst,
            "external-first" => GateAuthority::ExternalFirst,
            "prepare-commit" => GateAuthority::PrepareCommit,
            other => return Err(ConfigError::UnknownGateAuthority(other.to_string())),
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
                name: p.name,
                host_name: p.host_name,
            })
            .collect();

        Ok(Config {
            identity: IdentityConfig {
                agent_id: raw.identity.agent_id,
                host_id: raw.identity.host_id,
                host_name: raw.identity.host_name,
            },
            a2a: A2aConfig { bind: a2a_bind },
            operator: OperatorConfig {
                bind: operator_bind,
            },
            gate: GateConfig { authority },
            audit: AuditConfig {
                path: raw.audit.path,
            },
            adapter: AdapterConfig {
                kind,
                bin: raw.adapter.bin,
            },
            runtime: RuntimeConfig {
                kind: runtime_kind,
                control_bind: runtime_control_bind,
                default_protocol: raw.runtime.default_protocol,
                claude_bin: raw.runtime.claude_bin,
                opencode_bin: raw.runtime.opencode_bin,
                codex_bin: raw.runtime.codex_bin,
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
        // Inline content (WASL_CONFIG_CONTENT) takes precedence over any file
        // path — a machine-independent delivery so a caller spawning wasl-mcpd on
        // another host need not (and cannot) ship a filesystem path. Env overlays
        // still apply on top, exactly as for the file path.
        if let Some(content) = &env.content {
            let cfg = Config::from_toml_str(content)?;
            return Ok(cfg.apply_env(env));
        }
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

/// One peer's USER-facing display names, keyed by `agent_id` in [`peer_names`].
/// Kept alongside (not inside) the frozen `wasl_registry`/`wasl_proto::AgentCard`
/// so the name surface is additive and does not perturb the peer-routing types.
#[derive(Debug, Clone, Default, PartialEq, Eq)]
pub struct PeerName {
    /// The peer node's display name (e.g. `"Writer"`). Empty when unknown.
    pub name: String,
    /// The peer node's host display name (e.g. `"gpu-box"`). Empty when unknown.
    pub host_name: String,
}

/// Build the `agent_id -> PeerName` map from the config's peer entries — the
/// human-name side-table the `list_peers` tool and the name-alias resolver read.
/// Deterministic (a `BTreeMap`) and pure.
pub fn peer_names(peers: &[PeerEntry]) -> std::collections::BTreeMap<String, PeerName> {
    peers
        .iter()
        .map(|p| {
            (
                p.agent_id.clone(),
                PeerName {
                    name: p.name.clone(),
                    host_name: p.host_name.clone(),
                },
            )
        })
        .collect()
}

/// The receive-side adapter selected by config: an exhaustive-match
/// delegating enum, NOT `Box<dyn AgentAdapter>` — a trait object would erase
/// the compile-time forcing function that made adding `Codex`/`Opencode`
/// here require touching this match. See `docs/specs/adapter-codex.md`'s and `docs/specs/adapter-opencode.md`'s
/// "Node-config integration" sections (the authoritative shape for this type).
pub enum NodeAdapter {
    Claude(wasl_adapter_claude::ClaudeAdapter<wasl_adapter_claude::ProcessCliInvoker>),
    Codex(wasl_adapter_codex::CodexAdapter<wasl_adapter_codex::ProcessCliInvoker>),
    Opencode(wasl_adapter_opencode::OpencodeAdapter<wasl_adapter_opencode::ProcessCliInvoker>),
}

#[async_trait::async_trait]
impl wasl_adapter::AgentAdapter for NodeAdapter {
    async fn deliver(
        &self,
        d: &wasl_adapter::Delivery,
    ) -> Result<String, wasl_adapter::AdapterError> {
        match self {
            NodeAdapter::Claude(inner) => wasl_adapter::AgentAdapter::deliver(inner, d).await,
            NodeAdapter::Codex(inner) => wasl_adapter::AgentAdapter::deliver(inner, d).await,
            NodeAdapter::Opencode(inner) => wasl_adapter::AgentAdapter::deliver(inner, d).await,
        }
    }
}

/// Construct the receive-side adapter selected by the config. Written as an
/// exhaustive match on `AdapterKind` so adding a kind (W7 codex, W9
/// opencode) forces a decision here.
pub fn build_adapter(cfg: &AdapterConfig) -> NodeAdapter {
    let adapter_bin = cfg.bin.clone();
    match cfg.kind {
        AdapterKind::Claude => match &adapter_bin {
            None => NodeAdapter::Claude(wasl_adapter_claude::ClaudeAdapter::new(
                wasl_adapter_claude::ProcessCliInvoker,
            )),
            Some(bin) => NodeAdapter::Claude(wasl_adapter_claude::ClaudeAdapter::with_bin(
                wasl_adapter_claude::ProcessCliInvoker,
                bin.clone(),
            )),
        },
        AdapterKind::Codex => match &adapter_bin {
            None => NodeAdapter::Codex(wasl_adapter_codex::CodexAdapter::new(
                wasl_adapter_codex::ProcessCliInvoker,
            )),
            Some(bin) => NodeAdapter::Codex(wasl_adapter_codex::CodexAdapter::with_bin(
                wasl_adapter_codex::ProcessCliInvoker,
                bin.clone(),
            )),
        },
        AdapterKind::Opencode => match &adapter_bin {
            None => NodeAdapter::Opencode(wasl_adapter_opencode::OpencodeAdapter::new(
                wasl_adapter_opencode::ProcessCliInvoker,
            )),
            Some(bin) => NodeAdapter::Opencode(wasl_adapter_opencode::OpencodeAdapter::with_bin(
                wasl_adapter_opencode::ProcessCliInvoker,
                bin.clone(),
            )),
        },
    }
}
