//! Metadata - manifest and version management
//!
//! Tracks SST files, levels, and version history

pub mod manifest;
pub mod version_set;
pub mod version_manager;
pub mod sst_catalog;
pub mod invariants;

pub use manifest::{Manifest, FileMeta, ColumnFamilyMeta, CloudCheckpoint};
pub use version_set::VersionSet;
pub use version_manager::VersionManager;
pub use sst_catalog::SstCatalog;
pub use invariants::Invariants;

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

