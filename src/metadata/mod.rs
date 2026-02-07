//! Metadata - manifest and version management
//!
//! Tracks SST files, levels, and version history

pub mod journal;
pub mod manifest;
pub mod persistence;

pub use journal::{append_edit, append_edit_batch, ManifestEdit};
#[allow(unused_imports)]
pub use manifest::{CloudCheckpoint, ColumnFamilyMeta, FileMeta, Manifest};
pub use persistence::ManifestPersistence;
