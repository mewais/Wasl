# W2 — `wasl-mcp` node config — identity, peers, adapter selection — spec (authoritative)

**Tier:** L2 (config module inside the existing IO shell). **Deps added to `wasl-mcp`:** `toml` (parse;
add `toml = "0.8"` to `[workspace.dependencies]`). Dev: `tempfile` (temp config files). No other dep
changes; peer-URL validation reuses `reqwest::Url` (already a direct dependency). No `unsafe`.

**Crate boundary — why a module in `wasl-mcp`, NOT a new crate:** this config has exactly one consumer —
`wasl-mcpd`'s `run()`/`build_handler()` in `wasl-mcp` (`crates/wasl-mcp/src/lib.rs`). Its vocabulary is
already this crate's dependency set (`wasl_proto::{AgentId, HostId, AgentCard}`, `wasl_registry::PeerRegistry`,
`wasl_adapter_claude`), so a separate crate would add a workspace member, a version row, and a coverage gate
for zero reuse. W3's `wasl-operator` crate does not read config files itself — it is handed its bind by this
crate. If a second binary ever needs the same config, extraction is a mechanical move of one module.
Location: `crates/wasl-mcp/src/config.rs`, re-exported from the crate root
(`pub use config::{Config, ConfigError, EnvOverrides, …}`).

## Why

Today `run()` builds its node from four env vars and nothing else (`crates/wasl-mcp/src/lib.rs`):
`WASL_AGENT_ID` (default `"wasl-local"`), `WASL_HOST_ID` (default `"localhost"`), `WASL_AUDIT_PATH`
(default `"wasl-audit.jsonl"`), `WASL_A2A_BIND` (optional; unset → no A2A receive server; set-but-invalid →
stderr warning, receive server not started). The `PeerRegistry` is constructed **empty**
(`wasl_registry::PeerRegistry::new()` in `build_handler()`), so a running node can never dispatch to any
peer, and the receive-side adapter is hard-wired to `ClaudeAdapter::new(ProcessCliInvoker)`. This block adds
a TOML config file that can declare identity, binds, audit path, adapter selection, and — the load-bearing
part — the peer list that populates the registry at startup. Env vars keep working exactly as today, as
overrides.

## The TOML file

Complete example (every key shown; every section and every key is optional):

```toml
[identity]
agent_id = "planner"            # string; default "wasl-local"
host_id  = "hostA"              # string; default "localhost"

[a2a]
bind = "127.0.0.1:4242"         # socket address; default absent = A2A receive server not started

[operator]                      # RESERVED FOR W3: parsed + validated, but UNUSED by run() in this block
bind = "127.0.0.1:4243"         # socket address; default absent

[audit]
path = "/var/lib/wasl/audit.jsonl"   # string; default "wasl-audit.jsonl"

[adapter]
kind = "claude"                 # the ONLY accepted value in this block; default "claude"
bin  = "/opt/claude/claude"     # optional; default absent → ClaudeAdapter::new (bin "claude")

[[peers]]                       # array of tables; default []
agent_id = "reviewer"           # required per entry
host_id  = "hostB"              # required per entry
url      = "http://10.0.0.7:4242/"   # required per entry; absolute http/https URL

[[peers]]
agent_id = "builder"
host_id  = "hostA"
url      = "http://127.0.0.1:4343/"
```

Rules:
- A missing section or key takes its default. An **empty or absent file is a fully-default config**.
- **Unknown keys anywhere are rejected** (`#[serde(deny_unknown_fields)]` on every config struct) — a typo'd
  key must fail loud as `ConfigError::Toml`, never be silently ignored.
- `adapter.kind` and `peers[].url` deserialize as plain strings and are validated in a **second, explicit
  step** so their failures are the distinct errors below, not generic TOML errors.
- `a2a.bind` / `operator.bind` are strings that must parse as `std::net::SocketAddr` (host:port) at load
  time (`ConfigError::InvalidBind`). They are *stored* as strings (see env precedence below).
- Identity strings are taken as-is (including empty), matching today's env behavior exactly.
- Peer `agent_id`s must be unique (exact, case-sensitive string equality). Rationale:
  `PeerRegistry::register(&mut self, card: AgentCard)` silently **overwrites** the card keyed by `card.id`
  (`crates/wasl-registry/src/lib.rs`), so a duplicate in the file would silently drop a peer — reject it at
  load instead. A peer whose `agent_id` equals the node's own identity is NOT rejected in this block.

## Config path resolution

1. If `WASL_CONFIG` is set (even to the empty string): that value is the config path. The file **must**
   exist and be readable — a missing/unreadable explicitly-named file is `ConfigError::Read` (an operator
   who points at a file must never get silent defaults).
2. Else, the default path: `$XDG_CONFIG_HOME/wasl/config.toml` when `XDG_CONFIG_HOME` is set and non-empty,
   otherwise `$HOME/.config/wasl/config.toml` when `HOME` is set and non-empty, otherwise no path.
3. A **missing** file at the default path (or no path at all) is NOT an error: the config is
   `Config::default()` — this is what preserves current behavior. A file that *exists* at the default path
   but is unreadable or invalid IS an error (a real config must never be half-ignored).

## Precedence (env overrides file overrides built-in default)

| Effective value    | Env var (wins)    | File key            | Built-in default                     |
|--------------------|-------------------|---------------------|--------------------------------------|
| config file path   | `WASL_CONFIG`     | —                   | XDG default path above               |
| agent id           | `WASL_AGENT_ID`   | `identity.agent_id` | `"wasl-local"`                       |
| host id            | `WASL_HOST_ID`    | `identity.host_id`  | `"localhost"`                        |
| audit path         | `WASL_AUDIT_PATH` | `audit.path`        | `"wasl-audit.jsonl"`                 |
| A2A receive bind   | `WASL_A2A_BIND`   | `a2a.bind`          | absent (receive server not started)  |
| operator bind      | — (W3 may add)    | `operator.bind`     | absent                               |
| adapter kind       | —                 | `adapter.kind`      | `"claude"`                           |
| adapter bin        | —                 | `adapter.bin`       | absent (`ClaudeAdapter::new` → `"claude"`) |
| peers              | —                 | `peers`             | `[]`                                 |

- A **set-but-empty** env var overrides (matches today's `std::env::var(..).unwrap_or_else(..)` semantics:
  `Ok("")` is used as-is).
- **Bind leniency split (backward-compat critical):** a malformed bind in the FILE fails `Config::load()`
  (`InvalidBind`); a malformed bind from the ENV keeps today's behavior byte-for-byte — `run()` prints the
  existing warning (`WASL_A2A_BIND=… is not a valid socket address … A2A receive server not started.`) and
  continues. This is why `a2a.bind` is stored as `Option<String>`: the env override replaces the string
  unvalidated, and `run()` parses the *effective* string exactly as it parses `WASL_A2A_BIND` today.
- There are NO new env vars in this block. `WASL_CONFIG` is the only addition, and it only names the file.

## Public API (`crates/wasl-mcp/src/config.rs`)

```rust
/// The effective node configuration (file + env overlay + defaults).
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct Config {
    pub identity: IdentityConfig,   // { pub agent_id: String, pub host_id: String }
    pub a2a: A2aConfig,             // { pub bind: Option<String> }  — file value pre-validated as SocketAddr
    pub operator: OperatorConfig,   // { pub bind: Option<String> }  — parsed + validated; UNUSED until W3
    pub audit: AuditConfig,         // { pub path: String }
    pub adapter: AdapterConfig,     // { pub kind: AdapterKind, pub bin: Option<String> }
    pub peers: Vec<PeerEntry>,      // { pub agent_id: String, pub host_id: String, pub url: String }
}

/// The adapter selector. Exactly one variant in this block; W7 (codex) adds more.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum AdapterKind { Claude }

/// Everything `Config` reads from the environment, captured as plain data so
/// resolution is pure and tests never mutate process-global env.
#[derive(Debug, Clone, Default, PartialEq, Eq)]
pub struct EnvOverrides {
    pub config: Option<String>,           // WASL_CONFIG
    pub xdg_config_home: Option<String>,  // XDG_CONFIG_HOME
    pub home: Option<String>,             // HOME
    pub agent_id: Option<String>,         // WASL_AGENT_ID
    pub host_id: Option<String>,          // WASL_HOST_ID
    pub audit_path: Option<String>,       // WASL_AUDIT_PATH
    pub a2a_bind: Option<String>,         // WASL_A2A_BIND
}

impl EnvOverrides {
    /// Snapshot the real process env (each via `std::env::var(..).ok()`).
    pub fn from_process_env() -> Self;
}

impl Default for Config { /* the built-in defaults column of the table above */ }

impl Config {
    /// Parse + validate a TOML document. Pure (no env, no fs). Applies defaults
    /// for missing sections/keys; runs ALL validation (order: TOML syntax/shape →
    /// binds → adapter kind → peer urls → duplicate peers).
    pub fn from_toml_str(s: &str) -> Result<Config, ConfigError>;

    /// Overlay env values (the four value overrides; `config`/xdg/home are for
    /// path resolution only, unused here). Pure. Env wins per the table.
    pub fn apply_env(self, env: &EnvOverrides) -> Config;

    /// Resolve the config path from `env` per "Config path resolution" above.
    /// Pure. `None` = no file to read (use defaults).
    pub fn resolve_path(env: &EnvOverrides) -> Option<std::path::PathBuf>;

    /// The full pipeline against captured env: resolve path → read file
    /// (explicit-path missing → Err(Read); default-path missing → defaults;
    /// existing-but-unreadable → Err(Read)) → from_toml_str → apply_env.
    pub fn load_with(env: &EnvOverrides) -> Result<Config, ConfigError>;

    /// `load_with(&EnvOverrides::from_process_env())` — the one-liner `run()` calls.
    pub fn load() -> Result<Config, ConfigError>;
}
```

### `ConfigError` — distinct, testable variants

```rust
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum ConfigError {
    /// The config file could not be read (explicit `WASL_CONFIG` path missing, or
    /// any existing file unreadable). Display: "read config {path}: {detail}"
    Read { path: String, detail: String },
    /// Invalid TOML (syntax, wrong types, unknown keys). Display: "invalid config TOML: {detail}"
    Toml(String),
    /// `adapter.kind` is not a known adapter. Display: "unknown adapter kind: {kind}"
    UnknownAdapterKind(String),
    /// A peer url is not an absolute http/https URL.
    /// Display: "malformed peer url for {agent_id}: {url}: {detail}"
    MalformedPeerUrl { agent_id: String, url: String, detail: String },
    /// Two `[[peers]]` entries share an agent_id. Display: "duplicate peer agent_id: {agent_id}"
    DuplicatePeer(String),
    /// `a2a.bind` / `operator.bind` does not parse as a socket address.
    /// Display: "invalid {section}.bind {value:?}: {detail}"
    InvalidBind { section: String, value: String, detail: String },
}
impl std::fmt::Display for ConfigError { /* the strings above */ }
impl std::error::Error for ConfigError {}
```

Validation details:
- **kind:** exact, case-sensitive match `"claude"` → `AdapterKind::Claude`; anything else →
  `UnknownAdapterKind` (deserialize as `String` first — do NOT let serde produce a generic error here).
- **peer url:** `reqwest::Url::parse(&url)` must succeed AND the scheme must be `http` or `https` →
  else `MalformedPeerUrl` (relative URLs, `ftp://`, garbage all fail). Stored verbatim (dispatch uses the
  endpoint string as-is; the existing convention is a trailing slash, e.g. `http://127.0.0.1:4242/`, but
  the trailing slash is NOT enforced).
- **duplicate:** first duplicate encountered wins the error report; validation stops at the first error
  overall (one error per load — `Result`, not an error list).

## Consumption — `build_handler(cfg)` and `run()`

### Registry population (the currently-empty registry gets real peers)

New pure, public helper (this is the black-box test seam):

```rust
/// Build the peer registry from the config's peer entries.
pub fn registry_from_peers(peers: &[PeerEntry]) -> wasl_registry::PeerRegistry;
```

For each entry it registers a `wasl_proto::AgentCard` via the existing API — actual signatures:

```rust
// wasl-registry/src/lib.rs
pub fn register(&mut self, card: AgentCard)               // insert/overwrite keyed by card.id
pub fn get(&self, id: &AgentId) -> Option<&AgentCard>
// wasl-proto/src/lib.rs
pub struct AgentCard {
    // W0 (2026-07-02): the consumer-vocabulary `driver`/`workstream` fields are REMOVED from
    // wasl_proto::AgentCard before this block lands (generalization mandate).
    pub id: AgentId,
    pub host: HostId,
    pub endpoint: Option<String>,     // set to Some(peer.url)
}
```

i.e. `registry.register(AgentCard { id: AgentId(p.agent_id), host: HostId(p.host_id),
endpoint: Some(p.url) })`. `endpoint` is what dispatch routes by
(`registry.get(peer).and_then(|card| card.endpoint.clone())` — `crates/wasl-dispatch/src/lib.rs:53`);
`host` feeds `PeerRegistry::resolve`/`by_host`. Duplicates cannot occur here (rejected at load).

### Adapter factory (replaces the hard-wired construction in `run()`)

```rust
/// Construct the receive-side adapter selected by the config. Written as an
/// exhaustive match on AdapterKind so adding a kind (W7 codex) forces a decision
/// here. Returns the concrete type while there is one kind — no boxing yet.
pub fn build_adapter(cfg: &AdapterConfig)
    -> wasl_adapter_claude::ClaudeAdapter<wasl_adapter_claude::ProcessCliInvoker>;
```

Using the existing constructors (actual signatures, `crates/wasl-adapter-claude/src/lib.rs`):

```rust
impl<I: CliInvoker> ClaudeAdapter<I> {
    pub fn new(invoker: I) -> Self;                              // bin = "claude"
    pub fn with_bin(invoker: I, bin: impl Into<String>) -> Self;
}
```

`bin: None` → `ClaudeAdapter::new(ProcessCliInvoker)`; `bin: Some(b)` →
`ClaudeAdapter::with_bin(ProcessCliInvoker, b)`.

### `build_handler` and `run()` (in `crates/wasl-mcp/src/lib.rs`)

- `fn build_handler(cfg: &Config) -> WaslMcp<DefaultSink, ProcessRunner>` (takes the config; stays
  crate-private — the public seams are `registry_from_peers`/`build_adapter`): identity from
  `cfg.identity` (replacing `self_identity()`), `registry_from_peers(&cfg.peers)` (replacing
  `PeerRegistry::new()`), `open_audit` at `cfg.audit.path` (replacing `audit_path()`; the loud
  fall-back-to-`MemSink` warning behavior is unchanged), same `reqwest::Client` + `ProcessRunner` +
  `WaslState::new` + `WaslMcp::new` wiring as today.
- `run()`: first `let cfg = match Config::load() { Ok(c) => c, Err(e) => { eprintln!("wasl-mcpd: config
  error: {e}"); std::process::exit(2); } }` — an invalid config file fails LOUD before anything starts.
  Then exactly today's flow with config-fed values: if `cfg.a2a.bind` is `Some(bind)` (post-env-overlay),
  parse it as `SocketAddr` with today's lenient warn-and-skip on failure, build the
  `wasl_a2a_types::AgentCard` for the receive endpoint from `cfg.identity.agent_id` (NOTE for tests: this
  is the A2A-wire `wasl_a2a_types::AgentCard`, a different type from the registry's
  `wasl_proto::AgentCard`), construct the adapter via `build_adapter(&cfg.adapter)`, and `tokio::spawn` the
  serve loop via the existing `wasl_node::receiver(card, adapter)` (`pub fn receiver<A: AgentAdapter +
  'static>(card: wasl_a2a_types::AgentCard, adapter: A) -> A2aServer`). `cfg.operator` is read, validated,
  and then **deliberately ignored** (W3 consumes it) — no operator surface starts in this block.
- Delete the now-dead `self_identity()` / `audit_path()` env readers (their behavior lives in
  `EnvOverrides` + `apply_env`).

## Backward compatibility (byte-for-byte)

With **no config file** at the resolved path and **no `WASL_CONFIG`**, `Config::load()` must return exactly
`Config::default().apply_env(&EnvOverrides::from_process_env())`, and `wasl-mcpd` must behave exactly as
today: same identity defaults (`wasl-local`/`localhost`), same audit path default + same fallback warning
text, empty registry, adapter constructed only when a bind is set and identically to today
(`ClaudeAdapter::new(ProcessCliInvoker)`), same `WASL_A2A_BIND` semantics including the exact
invalid-address warning string, no operator surface, no new stdout/stderr output on the happy path. The
four existing env vars override exactly as before. The ONLY new externally-visible behaviors are: (a) a
config file is read when present, (b) `WASL_CONFIG` is honored, (c) an invalid/unreadable config file
aborts startup with exit code 2 and a `wasl-mcpd: config error: …` line on stderr.

## Test plan (black-box: public API + spawned binary only)

### `tests/config_pure.rs` — parse + defaults + errors (pure, no env, no fs)
All via `Config::from_toml_str`:
- **full parse:** the complete example above → every field asserted exactly (both peers present, in order).
- **empty string** → `Ok(Config::default())`; assert the default values from the precedence table
  explicitly (identity `wasl-local`/`localhost`, audit `wasl-audit.jsonl`, binds `None`, kind `Claude`,
  bin `None`, peers empty).
- **partial file** (only `[identity] agent_id = "x"`) → that field set, every other field default.
- **operator parsed:** a file with only `[operator] bind = "127.0.0.1:1"` → `Ok`, `operator.bind` populated
  (parsed-but-unused is still parsed).
- **invalid TOML syntax** (`this is not toml`) → `Err(ConfigError::Toml(_))`.
- **wrong type** (`[identity] agent_id = 7`) → `Err(ConfigError::Toml(_))`.
- **unknown key** (`[identity] agent = "x"`, and a top-level `[bogus]` section) → `Err(ConfigError::Toml(_))`
  with the offending name in the detail.
- **unknown adapter kind** (`kind = "codex"`, and case variant `"Claude"`) →
  `Err(ConfigError::UnknownAdapterKind(k))` with `k` preserved.
- **malformed peer url:** relative (`"not-a-url"`) and wrong scheme (`"ftp://x/"`) →
  `Err(ConfigError::MalformedPeerUrl{..})` carrying the peer's `agent_id` and the bad `url`; a valid
  `https://` url is accepted.
- **duplicate peer agent_id** (two `[[peers]]` with `agent_id = "b"`, different urls) →
  `Err(ConfigError::DuplicatePeer(id))`.
- **invalid bind:** `[a2a] bind = "not-an-addr"` → `Err(ConfigError::InvalidBind{section: "a2a", ..})`;
  same for `[operator]` with `section: "operator"`.
- **Display:** each variant's `to_string()` contains its documented substrings (e.g.
  `"unknown adapter kind: codex"`, `"duplicate peer agent_id: b"`).

### `tests/config_resolve.rs` — precedence + path resolution (pure `EnvOverrides`; fs via tempfile; NEVER `std::env::set_var`)
- **env wins over file:** file sets all of identity/audit/a2a.bind; `apply_env` with all four overrides set
  → env values everywhere; with `EnvOverrides::default()` → file values untouched.
- **env wins over defaults:** `Config::default().apply_env(..)` with the four set → env values.
- **empty-string env overrides:** `agent_id: Some("".into())` → effective agent id `""` (today's semantics).
- **env bind is NOT validated:** `apply_env` with `a2a_bind: Some("garbage".into())` succeeds and stores
  the string (the lenient parse happens in `run()`, not in config).
- **resolve_path:** `config: Some(p)` → `Some(p)` (even when xdg/home also set); else xdg set-and-non-empty
  → `$XDG_CONFIG_HOME/wasl/config.toml`; else home set-and-non-empty → `$HOME/.config/wasl/config.toml`;
  xdg empty falls through to home; both unset/empty → `None`.
- **load_with, explicit path:** tempfile with a valid config → loaded + env-overlaid; `config` pointing at
  a nonexistent path → `Err(ConfigError::Read{..})` (explicit path must exist).
- **load_with, default path missing:** `EnvOverrides { home: Some(tempdir), .. }` with no file inside →
  `Ok(Config::default())` (missing default = defaults, NOT an error).
- **load_with, default path present:** put a config at `<tempdir>/.config/wasl/config.toml` → its values
  load; make it invalid TOML → `Err(ConfigError::Toml(_))` (an existing bad file is never ignored).

### `tests/config_wiring.rs` — registry population + adapter factory (the point of the block)
- **registry_from_peers:** from the two-peer example → `len() == 2`; `get(&AgentId("reviewer"))` →
  `Some(card)` with `card.host == HostId("hostB")`, `card.endpoint == Some("http://10.0.0.7:4242/")`,
  `card.endpoint == Some(url)` and post-W0 card shape; empty peers → `is_empty()`.
- **config-declared peer is actually dispatchable (loopback):** reuse `tests/handle.rs`'s harness — spin a
  real `wasl_a2a_server::A2aServer` over a fake recording `AgentAdapter` on `127.0.0.1:0`; build the config
  FROM TOML text whose single peer's `url` is the bound address; `registry_from_peers` → drive
  `resolve_and_dispatch` (Approved consult) through that registry → `ToolReply::Consult` carries the fake's
  reply and the fake SAW the delivery. (Proves the file → registry → real A2A route end-to-end.)
- **adapter factory, default bin:** `build_adapter` on kind Claude/bin None, then `deliver` a consult
  `Delivery` — since no `claude` binary exists in the test env this errs; assert the `AdapterError` detail
  mentions spawning `claude` (proves `new`, i.e. default bin, was used). *(Alternative if flaky on a box
  where `claude` exists: skip this negative probe and rely on the stub test below for both arms.)*
- **adapter factory, custom bin:** write an executable stub script (tempfile, `chmod +x`) that prints
  `{"result":"stub-ok"}`; `build_adapter` with `bin = Some(stub_path)`; `deliver` a consult →
  `Ok("stub-ok")` (proves `with_bin` plumbing with a real child process, `tests/process_invoker.rs` style).

### `tests/config_bin.rs` — the binary's fail-loud path (real process; env set on the CHILD only)
- `std::process::Command::new(env!("CARGO_BIN_EXE_wasl-mcpd")).env("WASL_CONFIG", bad_path)` where
  `bad_path` doesn't exist → exit status 2, stderr contains `wasl-mcpd: config error:`.
- Same with `WASL_CONFIG` at a tempfile containing invalid TOML → exit 2, stderr contains
  `invalid config TOML`.
  (Only the error path is spawn-tested — the happy path blocks serving stdio and stays covered by the
  existing `tests/smoke.rs` in-memory-transport suite, which must remain green and UNTOUCHED.)

Existing `tests/handle.rs` and `tests/smoke.rs` stay green unmodified (backward-compat proof at the test
level). Bind-before-spawn, short timeouts, abort/join server tasks; tests never mutate process-global env
(child-process env only). `cargo test -p wasl-mcp` green; `cargo clippy -p wasl-mcp --all-targets -- -D
warnings` clean; `cargo fmt` clean.

## Coverage bar

The `config` module's pure surface (`from_toml_str`, `apply_env`, `resolve_path`, validation, `Display`,
`registry_from_peers`, `build_adapter`) is genuinely 100%-line covered by the suites above — no dead arms,
no tautologies. `load_with` is covered via tempfiles. Acceptable enumerated residuals (document in
AGENTS.md): `EnvOverrides::from_process_env` and `Config::load` (thin process-env snapshots), the
`run()`/`main` glue including `exit(2)` (exercised only by the spawned-binary tests, which llvm-cov may not
attribute), and the pre-existing UNTESTED-HERE A2A receive shell — same stance as `docs/specs/mcp-a2a.md`:
do NOT claim a coverage % for the IO shell, and do NOT let overall `wasl-mcp` coverage regress. No
`#[ignore]`, no `todo!`, no `unsafe`.

## Honesty

- ✅ verified here: parse/validate/precedence semantics, path resolution, registry population from a config
  file all the way through a real loopback A2A dispatch, adapter bin selection through a real child
  process, fail-loud startup on bad config.
- ❌ UNTESTED HERE (unchanged from mcp-a2a.md): the rmcp stdio serve loop, the live A2A receive serve loop,
  driving a real `claude` CLI (W5's live verification covers the last inch).
