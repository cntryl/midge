//! Runtime - Actor-based background task execution
//!
//! Deterministic actor framework for compaction, flushing, WAL, cloud ops, GC, and manifest.
//! All engine state mutations flow through actors via message passing.
//!
//! # Architecture
//!
//! - **`EventLoop`**: Receives messages and dispatches to actors
//! - **State**: Centralized mutable state owned by runtime
//! - **Actors**: Stateless handlers that process messages and return state updates
//! - **Actors**: Stateless handlers that process messages and return state updates

pub mod actors;
pub(crate) mod ddl;
pub mod durability;
pub mod event_loop;
pub(crate) mod hybrid_persistence;
pub mod intent_persistence;
pub(crate) mod read_resources;
pub mod read_snapshot;
pub mod snapshot_cache;
pub(crate) mod snapshot_pins;
pub mod state;
pub(crate) mod storage_residue;
pub(crate) mod transaction_spill;

mod config;
mod handle;
mod lifecycle;
mod protocol;
mod router;
#[path = "runtime.rs"]
mod runtime_worker;

pub use event_loop::EventLoop;
pub use intent_persistence::IntentPersistence;
pub use read_snapshot::ReadSnapshot;

pub use crate::types::ConflictPolicy;
pub use state::RuntimeState;

pub(crate) use config::RecoveredCloudActiveWal;
pub use config::RuntimeConfig;
pub(crate) use config::{CloudRuntimePolicy, CloudWalSealPolicy};
pub use handle::RuntimeHandle;
pub(crate) use lifecycle::{RuntimeLifecycle, RuntimeLifecycleState, RuntimeTransactionGuard};
pub(crate) use protocol::next_request_id;
#[cfg(test)]
pub use protocol::CompactionPlan;
pub use protocol::{
    FileMeta, IntentLogEntry, KeyAssertion, PublicationPhase, RuntimeMsg, RuntimeResponse,
    TransactionOp,
};
pub(crate) use protocol::{SpilledTransactionSubmission, TransactionSubmission};
pub(crate) use router::ResponseRouter;
pub use runtime_worker::Runtime;
#[cfg(test)]
pub(crate) use runtime_worker::RUNTIME_QUEUE_CAPACITY;

#[cfg(test)]
mod tests;
