//! Protocol-routing [`RuntimeProvider`]: one Wasl node serves multiple
//! protocols (claude, opencode-acp, …) through a single provider seam.
//!
//! [`ProtocolRoutingProvider`] holds a map of per-protocol
//! `Arc<dyn RuntimeProvider>` instances and a default protocol name. When
//! `target_for` is called (no context), it uses the default; when
//! `target_for_with_context` is called, it reads `context.protocol`, falls
//! back to the default, and delegates to the matching sub-provider. It also
//! remembers which protocol a key resolved to (in `routed`) so that a
//! subsequent context-less `target_for` for the same key re-uses the same
//! sub-provider.
//!
//! All method bodies are `todo!()` — this is the Phase-0 scaffold; Phase-2
//! will fill the real routing logic.

use std::collections::{BTreeMap, HashMap};
use std::sync::Mutex;

/// Protocol-routing [`crate::RuntimeProvider`]: one node, many protocols.
///
/// Constructed with a `BTreeMap<String, Arc<dyn RuntimeProvider>>` keyed by
/// protocol name (e.g. `"claude"`, `"opencode-acp"`) and a `default_protocol`
/// naming the fallback.
pub struct ProtocolRoutingProvider {
    providers: BTreeMap<String, std::sync::Arc<dyn crate::RuntimeProvider>>,
    default_protocol: String,
    /// Maps a `TargetKey` → the protocol that was chosen at first creation.
    routed: Mutex<HashMap<crate::TargetKey, String>>,
}

impl ProtocolRoutingProvider {
    /// Construct a new routing provider.
    ///
    /// * `providers` — one entry per protocol name; the map must contain at
    ///   least the `default_protocol`.
    /// * `default_protocol` — name of the fallback when a request carries no
    ///   explicit protocol.
    pub fn new(
        providers: BTreeMap<String, std::sync::Arc<dyn crate::RuntimeProvider>>,
        default_protocol: String,
    ) -> Self {
        todo!("store fields into ProtocolRoutingProvider {{ providers, default_protocol, routed: Mutex::new(HashMap::new()) }}")
    }
}

#[async_trait::async_trait]
impl crate::RuntimeProvider for ProtocolRoutingProvider {
    async fn target_for(
        &self,
        key: &crate::TargetKey,
    ) -> Result<std::sync::Arc<dyn crate::DeliveryTarget>, crate::RuntimeError> {
        todo!("look up routed[key] or default_protocol; delegate to matching sub-provider")
    }

    async fn list_targets(&self) -> Vec<crate::TargetId> {
        todo!("union of all sub-provider list_targets() results")
    }

    async fn target_for_with_context(
        &self,
        key: &crate::TargetKey,
        context: crate::SpawnContext,
    ) -> Result<std::sync::Arc<dyn crate::DeliveryTarget>, crate::RuntimeError> {
        todo!(
            "pick protocol from context.protocol.clone().unwrap_or(default_protocol.clone()); \
             record routed[key] = chosen_protocol; \
             delegate to providers[chosen_protocol].target_for_with_context(key, context)"
        )
    }
}
