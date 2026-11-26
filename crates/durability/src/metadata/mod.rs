//! Metadata - manifest and version management
//!
//! Tracks SST files, levels, and version history

pub mod accounted_fs;
pub mod accounting;
#[cfg(feature = "internal-testing")]
pub mod checkpoint_policy_probe;
pub mod cloud_manifest;
pub mod codec;
pub mod files;
pub mod format;
pub mod frontiers;
pub mod intent;
pub mod intent_persistence;
pub mod journal;
mod key_bounds;
pub mod local_authority;
pub mod manifest;
pub mod persistence;
#[cfg(any(test, feature = "internal-testing"))]
#[doc(hidden)]
pub mod recovery_progress_fixture;
pub mod store;

pub use format::{ensure_or_create_format_marker, validate_format_marker};
pub use frontiers::FamilyFrontier;
#[cfg(any(test, feature = "internal-testing"))]
pub use journal::append_edit;
pub use journal::ManifestEdit;
pub use manifest::{ColumnFamilyMeta, FileMeta, Manifest};
pub use persistence::ManifestPersistence;
