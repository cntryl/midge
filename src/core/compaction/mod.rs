//! Compaction subsystem for LSM-tree maintenance.

pub mod controller;
pub mod executor;
pub mod filter;
pub mod strategy;

// Public API
pub use controller::{CompactionController, CompactionMsg, CompactionWorkerConfig};
pub use executor::CompactionVersion;
pub use filter::CompactionFilter;
pub use strategy::{CompactionPlan, Compactor, LeveledCompactionConfig};

// Crate-internal executor functions
pub(crate) use executor::{
    apply_compaction_filter, collect_compaction_versions, deduplicate_versions,
    filter_safe_tombstones, sort_versions_for_output,
};
