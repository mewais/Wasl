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
        ProtocolRoutingProvider {
            providers,
            default_protocol,
            routed: Mutex::new(HashMap::new()),
        }
    }
}

#[async_trait::async_trait]
impl crate::RuntimeProvider for ProtocolRoutingProvider {
    async fn target_for(
        &self,
        key: &crate::TargetKey,
    ) -> Result<std::sync::Arc<dyn crate::DeliveryTarget>, crate::RuntimeError> {
        // Look up the protocol recorded at first creation for this key, or fall
        // back to the default protocol.
        let proto = {
            let routed = self.routed.lock().unwrap();
            routed
                .get(key)
                .cloned()
                .unwrap_or_else(|| self.default_protocol.clone())
        };
        // Delegate to the sub-provider (lock is released before the await).
        let sub =
            self.providers
                .get(&proto)
                .ok_or_else(|| crate::RuntimeError::AdmissionRefused {
                    detail: format!("no sub-provider registered for protocol '{proto}'"),
                })?;
        sub.target_for(key).await
    }

    async fn list_targets(&self) -> Vec<crate::TargetId> {
        // Union of all sub-providers' list_targets().
        let mut all = Vec::new();
        for sub in self.providers.values() {
            all.extend(sub.list_targets().await);
        }
        all
    }

    async fn target_for_with_context(
        &self,
        key: &crate::TargetKey,
        context: crate::SpawnContext,
    ) -> Result<std::sync::Arc<dyn crate::DeliveryTarget>, crate::RuntimeError> {
        // Pick the protocol: explicit from context, or fall back to the default.
        let proto = context
            .protocol
            .clone()
            .unwrap_or_else(|| self.default_protocol.clone());

        // Look up the sub-provider before recording or awaiting anything.
        let sub =
            self.providers
                .get(&proto)
                .ok_or_else(|| crate::RuntimeError::AdmissionRefused {
                    detail: format!("no sub-provider registered for protocol '{proto}'"),
                })?;
        let sub = sub.clone();

        // Record the chosen protocol for this key (so a later context-free
        // target_for routes to the same sub-provider). Do this before the
        // await so the mapping is visible even if the delegate fails.
        {
            let mut routed = self.routed.lock().unwrap();
            routed.entry(key.clone()).or_insert_with(|| proto.clone());
        }

        // Delegate to the sub-provider (mutex is fully released above).
        sub.target_for_with_context(key, context).await
    }
}
