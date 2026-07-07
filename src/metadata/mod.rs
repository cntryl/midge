//! Metadata - manifest and version management
//!
//! Tracks SST files, levels, and version history

pub mod format;
pub mod journal;
pub mod manifest;
pub mod persistence;

pub use format::{ensure_or_create_format_marker, validate_format_marker};
pub use journal::{append_edit, append_edit_batch, ManifestEdit};
pub use manifest::{ColumnFamilyMeta, FileMeta, Manifest};
pub use persistence::ManifestPersistence;
