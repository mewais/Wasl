//! wasl-adapter — the last inch: bridge an incoming A2A Task to the real local agent.
//!
//! Defines the `AgentAdapter` trait (deliver a message to a live agent session,
//! get its reply), the `Delivery` extraction from an A2A `Message` (Wasl verb +
//! provenance travel in `metadata.wasl`), and `AdapterExecutor` which implements
//! `wasl_a2a_server::TaskExecutor` by calling the adapter. The trait + fake +
//! extraction are fully unit-tested; the CONCRETE CLI-driving adapters are a
//! separate block (UNTESTED HERE). See `docs/specs/adapter.md`.
