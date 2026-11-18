//! How node-to-node messages reach another host. Same-host delivery is a direct call and never
//! goes through a transport; the default transport relays over the control links
//! ([`UplinkTransport`]). Other transports (e.g. direct HTTP for standalone setups) implement
//! [`PeerTransport`].

use std::time::Duration;

use async_trait::async_trait;
use serde_json::Value;
use wasit_control::{CallError, HostId, Uplink, UplinkChanges};

/// Why a node-to-node call failed.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct TransportError {
    pub message: String,
    /// Worth retrying later (link down, host unreachable, timeout).
    pub retryable: bool,
}

impl std::fmt::Display for TransportError {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.write_str(&self.message)
    }
}

/// A route to other hosts' nodes.
#[async_trait]
pub trait PeerTransport: Send + Sync + 'static {
    /// Call `method` on the node of `host`.
    async fn call(&self, host: &str, method: &str, params: Value) -> Result<Value, TransportError>;

    /// A signal that fires when routes may have changed (the pump retries at once).
    fn changes(&self) -> Option<UplinkChanges> {
        None
    }
}

/// Relay through the hub over an [`Uplink`].
pub struct UplinkTransport {
    uplink: Uplink,
    timeout: Duration,
}

impl UplinkTransport {
    pub fn new(uplink: Uplink, timeout: Duration) -> Self {
        UplinkTransport { uplink, timeout }
    }
}

#[async_trait]
impl PeerTransport for UplinkTransport {
    async fn call(&self, host: &str, method: &str, params: Value) -> Result<Value, TransportError> {
        self.uplink
            .call(&HostId::from(host), method, params, self.timeout)
            .await
            .map_err(|e| {
                let retryable = match &e {
                    // A handler error from the far node is final; routing errors are not.
                    // `host_unknown`: the hub has no link to that host right now (store and
                    // forward until it has).
                    CallError::Rpc(r) => {
                        r.is_retryable() || r.code == wasit_control::codes::HOST_UNKNOWN
                    }
                    other => other.is_retryable(),
                };
                TransportError {
                    message: e.to_string(),
                    retryable,
                }
            })
    }

    fn changes(&self) -> Option<UplinkChanges> {
        Some(self.uplink.changes())
    }
}
