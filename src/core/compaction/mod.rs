//! Compaction subsystem for LSM-tree maintenance.
//!
//! This module contains all compaction-related functionality:
//! - `coordinator.rs` - Background compaction worker coordination
//! - `execution/` - Compaction execution logic (merge, dedup, write SSTs)
//! - `filter.rs` - Compaction filter trait and implementations
//! - `strategy.rs` - Compaction strategy (leveled compaction)

pub mod coordinator;
pub mod execution;
pub mod filter;
pub mod strategy;

// For backward compatibility, keep executor.rs as a re-export facade
#[allow(deprecated)]
pub mod executor;

// Re-export commonly used types
pub use coordinator::{CompactionCoordinator, CompactionMsg, CompactionWorkerConfig};
pub use execution::CompactionVersion;
pub use filter::CompactionFilter;
pub use strategy::{CompactionPlan, Compactor, LeveledCompactionConfig};

// Re-export crate-internal executor functions for engine use
pub(crate) use executor::{
    apply_compaction_filter, collect_compaction_versions, deduplicate_versions,
    deduplicate_versions_snapshot_aware, filter_safe_tombstones, sort_versions_for_output,
};
