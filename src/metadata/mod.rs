//! Metadata - manifest and version management
//!
//! Tracks SST files, levels, and version history

pub mod journal;
pub mod manifest;
pub mod persistence;
pub mod version_manager;
pub mod version_set;

pub use journal::{
    append_edit, append_edit_batch, append_fsync_marker, replay_journal, truncate_journal,
    ManifestEdit,
};
pub use manifest::{CloudCheckpoint, ColumnFamilyMeta, FileMeta, Manifest};
pub use persistence::ManifestPersistence;
pub use version_manager::VersionManager;
pub use version_set::VersionSet;

/// Version identifier
#[derive(Copy, Clone, Debug, Eq, PartialEq, Ord, PartialOrd)]
pub struct Version(pub u64);

/// SST file metadata
#[derive(Clone, Debug)]
pub struct SstFileInfo {
    pub id: u64,
    pub level: u32,
    pub min_key: Vec<u8>,
    pub max_key: Vec<u8>,
    pub size_bytes: u64,
}
