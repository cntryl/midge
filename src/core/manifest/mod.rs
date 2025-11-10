//! Manifest tracking for SSTs, column families, and cloud checkpoints.
//!
//! The manifest is the single source of truth for database metadata,
//! including which SST files exist at each level, column family configurations,
//! and cloud upload state.

mod cloud;
mod column_families;
mod io;
mod queries;
mod types;

// Re-export public API
pub use types::{CloudCheckpoint, ColumnFamilyMeta, FileMeta, Manifest};
