//! Metadata - manifest and version management
//!
//! Tracks SST files, levels, and version history

pub mod files;
pub mod format;
pub mod journal;
mod key_bounds;
pub mod manifest;
pub mod persistence;
pub(crate) mod store;

pub use format::{ensure_or_create_format_marker, validate_format_marker};
#[cfg(test)]
pub use journal::append_edit;
pub use journal::ManifestEdit;
pub use manifest::{ColumnFamilyMeta, FileMeta, Manifest};
pub use persistence::ManifestPersistence;
