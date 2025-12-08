//! Manifest tracking for SSTs, column families, and cloud checkpoints.
//!
//! The manifest is the single source of truth for database metadata,
//! including which SST files exist at each level, column family configurations,
//! and cloud upload state.

mod cloud;
mod column_families;
mod io;
mod queries;
mod segment;
mod segment_flush;
mod types;
mod version_manager;
mod version_set;

// Re-export public API
pub use segment::{Segment, SegmentId, SegmentRef, SegmentSequencer, SegmentState};
pub use segment_flush::{
    create_segment_from_entries, promote_segment_to_l0, seal_segment_on_flush,
    update_manifest_with_segment,
};
pub use types::{CloudCheckpoint, ColumnFamilyMeta, FileMeta, Manifest};
pub use version_manager::VersionManager;
pub use version_set::{AtomicVersionSet, VersionEdit, VersionSet};
