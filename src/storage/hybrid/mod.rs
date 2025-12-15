//! Hybrid storage with cloud integration and disk pressure management
//!
//! Provides:
//! - **HybridStorage**: Combines local and cloud backends with WAL durability
//! - **StorageBudgetActor**: Tracks disk usage and enforces watermarks
//! - **StorageBudgetPolicy**: Watermark thresholds and eviction strategy
//! - **DiskState**: Disk usage accounting
//!
//! # Architecture
//!
//! - **backend.rs**: HybridStorage - main coordinator for local + cloud storage
//! - **actor.rs**: StorageBudgetActor - disk space management
//! - **policy.rs**: StorageBudgetPolicy - watermark thresholds
//! - **state.rs**: DiskState - disk accounting
//!
//! # Two Storage Roles
//!
//! HybridStorage manages:
//! 1. **Object Storage**: SSTs via submit_read/write/delete/list
//! 2. **WAL Durability**: Segments via enqueue_wal_segment() + process_uploads()
//!
//! WAL Flow: local write → enqueue_wal_segment() → process_uploads() → CloudAck
//!
//! # Usage
//!
//! ```ignore
//! use midge::storage::hybrid::{
//!     HybridStorage, StorageBudgetPolicy, StorageBudgetActor, StorageBudgetEvent
//! };
//!
//! let policy = StorageBudgetPolicy::new(1024 * 1024 * 1024); // 1 GB
//! let mut actor = StorageBudgetActor::new(policy);
//!
//! // Try to reserve space for a flush
//! match actor.handle_event(StorageBudgetEvent::ReserveForFlush { est_size: 100_000 }) {
//!     Some(ReservationResult::Ok) => { /* proceed with flush */ },
//!     Some(ReservationResult::WaitForCloudUpload) => { /* wait */ },
//!     Some(ReservationResult::WaitForCompaction) => { /* trigger compaction */ },
//!     Some(ReservationResult::RejectNoSpace) => { /* backpressure */ },
//!     None => { /* no reservation needed */ },
//! }
//! ```

pub mod actor;
pub mod backend;
pub mod policy;
pub mod state;

// Re-exports for convenience
pub use actor::{ReservationResult, StorageBudgetActor, StorageBudgetEvent};
pub use backend::{HybridStorage, UploadState, UploadStatus};
pub use policy::{EvictionStrategy, StorageBudgetPolicy};
pub use state::{AtomicDiskState, DiskState};
