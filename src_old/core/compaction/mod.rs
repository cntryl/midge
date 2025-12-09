//! Compaction subsystem for LSM-tree maintenance.

pub mod controller;
pub mod executor;
pub mod filter;
pub mod log_manager;
pub mod planner;
pub mod planner_controller;
pub mod strategy;

// Public API
pub use controller::{CompactionController, CompactionMsg, CompactionWorkerConfig};
pub use executor::CompactionVersion;
pub use filter::CompactionFilter;
pub use log_manager::CompactionLogManager;
pub use planner::{CompactionLog, CompactionTask, Planner};
pub use planner_controller::PlannerController;
pub use strategy::{CompactionPlan, Compactor, LeveledCompactionConfig};

// Crate-internal executor functions
pub(crate) use executor::{
    apply_compaction_filter, collect_compaction_versions, deduplicate_versions,
    filter_safe_tombstones, sort_versions_for_output,
};
