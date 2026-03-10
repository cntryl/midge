//! Metadata - manifest and version management
//!
//! Tracks SST files, levels, and version history

pub mod format;
pub mod journal;
pub mod manifest;
pub mod persistence;

pub use format::{ensure_or_create_format_marker, validate_format_marker};
pub use journal::{append_edit, append_edit_batch, ManifestEdit};
#[allow(unused_imports)]
// ColumnFamilyMeta re-exported for public API; used by persistence tests via crate::metadata
pub use manifest::{CloudCheckpoint, ColumnFamilyMeta, FileMeta, Manifest};
pub use persistence::ManifestPersistence;
