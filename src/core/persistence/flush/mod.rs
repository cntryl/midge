//! Memtable flushing subsystem.
//!
//! This module handles the asynchronous flushing of memtable contents to on-disk SST files.
//! The flush process works in two phases:
//!
//! 1. **Memtable Rollover**: When the active memtable reaches capacity, the WAL is rotated
//!    and memtable entries are drained and sent to a background flush worker thread.
//!
//! 2. **Background Flush**: The worker thread writes the entries to a new SST file,
//!    updates the manifest, and removes the corresponding WAL file.
//!
//! This asynchronous design allows writes to continue with minimal latency while flushing
//! happens in the background.
//!
//! # Module Structure
//!
//! - **`worker`**: Background flush worker thread management (FlushJob, FlushMsg, spawn)
//! - **`process`**: Core flush job processing and WAL pruning logic
//! - **`bounds`**: Key bounds computation and synchronous flush path
//! - **`stats`**: Flush statistics and metrics calculation helpers
//! - **`traits`**: FlushOutput trait for extensible notifications

pub mod bounds;
pub mod process;
pub mod stats;
pub mod traits;
pub mod worker;

// Re-export primary types at module level for convenience
pub use bounds::{
    compute_bounds, flush_memtable_to_sst, rollover_and_queue_flush, FlushConfig, KeyBounds,
};
pub use process::{determine_safe_prune_sequence, prune_old_wal_files};
pub use stats::FlushStats;
pub use traits::{CallbackFlushOutput, FlushOutput, NullFlushOutput};
pub use worker::{spawn_flush_worker, FlushJob, FlushMsg, FlushWorker, FlushWorkerConfig};
