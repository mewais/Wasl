//! wasl-registry — peer registry + agent cards + peer route resolution (WR).
//!
//! Pure: track which agents exist and on which host, resolve whether a target
//! peer is Local (same node) or Remote (another host's node). Actual peer
//! delivery IO lives in the dispatch block (over A2A). See `docs/specs/registry.md`.

use std::collections::HashMap;
use std::fmt;

use serde::{Deserialize, Serialize};
use wasl_proto::{AgentCard, AgentId, HostId};

/// Whether a resolved target is on this node or another host's node.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum Route {
    Local,
    Remote,
}

/// The outcome of resolving a peer: local (same host) or remote (with the
/// target's host attached).
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case", tag = "route")]
pub enum Resolved {
    Local { peer: AgentId },
    Remote { peer: AgentId, host: HostId },
}

/// Errors produced while resolving peers against the registry.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum RegistryError {
    UnknownPeer(AgentId),
}

impl fmt::Display for RegistryError {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            RegistryError::UnknownPeer(id) => write!(f, "unknown peer: {}", id.as_str()),
        }
    }
}

impl std::error::Error for RegistryError {}

/// Pure in-memory store of agent cards, keyed by `AgentId`.
#[derive(Debug, Clone, Default)]
pub struct PeerRegistry {
    peers: HashMap<AgentId, AgentCard>,
}

impl PeerRegistry {
    pub fn new() -> Self {
        Self::default()
    }

    /// Insert or overwrite the card keyed by `card.id`.
    pub fn register(&mut self, card: AgentCard) {
        self.peers.insert(card.id.clone(), card);
    }

    /// Remove a peer by id; returns whether it was present.
    pub fn unregister(&mut self, id: &AgentId) -> bool {
        self.peers.remove(id).is_some()
    }

    pub fn get(&self, id: &AgentId) -> Option<&AgentCard> {
        self.peers.get(id)
    }

    pub fn contains(&self, id: &AgentId) -> bool {
        self.peers.contains_key(id)
    }

    pub fn len(&self) -> usize {
        self.peers.len()
    }

    pub fn is_empty(&self) -> bool {
        self.peers.is_empty()
    }

    /// All cards, sorted by id ascending (deterministic).
    pub fn list(&self) -> Vec<&AgentCard> {
        let mut cards: Vec<&AgentCard> = self.peers.values().collect();
        cards.sort_by(|a, b| a.id.as_str().cmp(b.id.as_str()));
        cards
    }

    /// Cards whose host matches `host`, sorted by id ascending.
    pub fn by_host(&self, host: &HostId) -> Vec<&AgentCard> {
        let mut cards: Vec<&AgentCard> = self.peers.values().filter(|c| &c.host == host).collect();
        cards.sort_by(|a, b| a.id.as_str().cmp(b.id.as_str()));
        cards
    }

    /// Resolve whether `target` is local to `self_host` or remote, given the
    /// registered card's host.
    pub fn resolve(&self, self_host: &HostId, target: &AgentId) -> Result<Resolved, RegistryError> {
        let card = self
            .peers
            .get(target)
            .ok_or_else(|| RegistryError::UnknownPeer(target.clone()))?;

        if &card.host == self_host {
            Ok(Resolved::Local {
                peer: card.id.clone(),
            })
        } else {
            Ok(Resolved::Remote {
                peer: card.id.clone(),
                host: card.host.clone(),
            })
        }
    }

    /// Resolve each target, returning `(resolved_ok, unknown_targets)` while
    /// preserving the relative input order of each list.
    pub fn resolve_many(
        &self,
        self_host: &HostId,
        targets: &[AgentId],
    ) -> (Vec<Resolved>, Vec<AgentId>) {
        let mut resolved = Vec::new();
        let mut unknown = Vec::new();

        for target in targets {
            match self.resolve(self_host, target) {
                Ok(r) => resolved.push(r),
                Err(RegistryError::UnknownPeer(id)) => unknown.push(id),
            }
        }

        (resolved, unknown)
    }
}
