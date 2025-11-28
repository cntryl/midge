//! VersionSet: Immutable snapshot of database state for lock-free reads
//!
//! The VersionSet provides an immutable view of:
//! - Current manifest state (SST files, column families, etc.)
//!
//! Used with ArcSwap for atomic visibility transitions during flush/compaction.
//! SST readers are opened on-demand using the manifest metadata.

use arc_swap::ArcSwap;
use std::collections::HashSet;
use std::sync::Arc;

use crate::core::manifest::types::{FileMeta, Manifest};
use crate::error::MidgeResult;

/// Immutable snapshot of database state.
/// Cloned and updated when files are added/removed, then published atomically.
///
/// Note: SST readers are NOT cached here - they're opened on-demand.
/// This keeps VersionSet simple and cloneable. Reader caching happens
/// at a different layer (table_cache, bloom_cache, etc.).
#[derive(Clone)]
pub struct VersionSet {
    /// Current manifest state
    pub manifest: Manifest,
}

impl VersionSet {
    /// Create a new VersionSet from a manifest.
    pub fn new(manifest: Manifest) -> Self {
        Self { manifest }
    }

    /// Apply a version edit to create a new VersionSet.
    /// This creates a clone with the edit applied.
    pub fn apply_edit(&self, edit: VersionEdit) -> MidgeResult<Self> {
        let mut new_manifest = self.manifest.clone();
        Self::apply_edit_to_manifest(&mut new_manifest, edit);
        Ok(Self {
            manifest: new_manifest,
        })
    }

    /// Apply multiple edits in a single clone operation.
    /// Much more efficient than calling apply_edit repeatedly (O(n) vs O(n²)).
    pub fn apply_edits(&self, edits: impl IntoIterator<Item = VersionEdit>) -> MidgeResult<Self> {
        let mut new_manifest = self.manifest.clone();
        for edit in edits {
            Self::apply_edit_to_manifest(&mut new_manifest, edit);
        }
        Ok(Self {
            manifest: new_manifest,
        })
    }

    /// Apply a single edit to a manifest in place (no cloning).
    fn apply_edit_to_manifest(manifest: &mut Manifest, edit: VersionEdit) {
        match edit {
            VersionEdit::AddFile { file } => {
                let name = file.name.clone();
                manifest.files.push(*file);
                manifest.ssts.push(name);
            }
            VersionEdit::RemoveFiles { names } => {
                // For small remove sets, linear search is faster than HashSet overhead
                if names.len() <= 4 {
                    manifest.files.retain(|f| !names.contains(&f.name));
                    manifest.ssts.retain(|s| !names.contains(s));
                } else {
                    let names_set: HashSet<&str> = names.iter().map(|s| s.as_str()).collect();
                    manifest
                        .files
                        .retain(|f| !names_set.contains(f.name.as_str()));
                    manifest.ssts.retain(|s| !names_set.contains(s.as_str()));
                }
            }
            VersionEdit::UpdateSequence { sequence } => {
                manifest.last_persisted_sequence = sequence;
            }
            VersionEdit::CombinedAddRemove { add, remove } => {
                let name = add.name.clone();
                manifest.files.push(*add);
                manifest.ssts.push(name);
                // For small remove sets, linear search is faster than HashSet overhead
                if remove.len() <= 4 {
                    manifest.files.retain(|f| !remove.contains(&f.name));
                    manifest.ssts.retain(|s| !remove.contains(s));
                } else {
                    let remove_set: HashSet<&str> = remove.iter().map(|s| s.as_str()).collect();
                    manifest
                        .files
                        .retain(|f| !remove_set.contains(f.name.as_str()));
                    manifest.ssts.retain(|s| !remove_set.contains(s.as_str()));
                }
            }
        }
    }
}

/// Edit operations that modify the VersionSet.
/// Processed serially by the VersionManager actor.
#[derive(Debug, Clone)]
pub enum VersionEdit {
    /// Add a new SST file to the version
    AddFile { file: Box<FileMeta> },
    /// Remove SST files from the version (compaction)
    RemoveFiles { names: Vec<String> },
    /// Update last persisted sequence number
    UpdateSequence { sequence: u64 },
    /// Atomic combination of adding one file and removing a set of files.
    /// Prevents interleaving flush AddFile between compaction AddFile/RemoveFiles.
    CombinedAddRemove {
        add: Box<FileMeta>,
        remove: Vec<String>,
    },
}

/// Wrapper for atomic version set operations.
/// Provides convenience methods for common version transitions.
pub struct AtomicVersionSet {
    inner: Arc<ArcSwap<VersionSet>>,
}

impl AtomicVersionSet {
    /// Create a new atomic version set.
    pub fn new(version: VersionSet) -> Self {
        Self {
            inner: Arc::new(ArcSwap::from_pointee(version)),
        }
    }

    /// Load current version (lock-free).
    pub fn load(&self) -> Arc<VersionSet> {
        self.inner.load_full()
    }

    /// Store a new version atomically.
    pub fn store(&self, version: Arc<VersionSet>) {
        self.inner.store(version);
    }

    /// Get Arc reference to the ArcSwap for direct access.
    pub fn as_arc(&self) -> Arc<ArcSwap<VersionSet>> {
        Arc::clone(&self.inner)
    }
}

impl Clone for AtomicVersionSet {
    fn clone(&self) -> Self {
        Self {
            inner: Arc::clone(&self.inner),
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::core::manifest::types::{FileMeta, Manifest};

    #[test]
    fn should_create_empty_version_set() {
        // Arrange
        let manifest = Manifest::default();

        // Act
        let version = VersionSet::new(manifest);

        // Assert
        assert_eq!(version.manifest.ssts.len(), 0);
    }

    #[test]
    fn should_apply_add_file_edit() {
        // Arrange
        let manifest = Manifest::default();
        let version = VersionSet::new(manifest);

        let file = FileMeta {
            name: "test.sst".to_string(),
            level: 0,
            size_bytes: 1024,
            ..Default::default()
        };

        // Act
        let new_version = version
            .apply_edit(VersionEdit::AddFile {
                file: Box::new(file),
            })
            .unwrap();

        // Assert
        assert_eq!(new_version.manifest.ssts.len(), 1);
        assert_eq!(new_version.manifest.files.len(), 1);
        assert_eq!(new_version.manifest.files[0].name, "test.sst");
    }

    #[test]
    fn should_apply_remove_files_edit() {
        // Arrange
        let mut manifest = Manifest::default();
        manifest.ssts.push("test1.sst".to_string());
        manifest.ssts.push("test2.sst".to_string());
        manifest.files.push(FileMeta {
            name: "test1.sst".to_string(),
            level: 0,
            size_bytes: 1024,
            ..Default::default()
        });
        manifest.files.push(FileMeta {
            name: "test2.sst".to_string(),
            level: 0,
            size_bytes: 1024,
            ..Default::default()
        });

        let version = VersionSet::new(manifest);

        // Act
        let new_version = version
            .apply_edit(VersionEdit::RemoveFiles {
                names: vec!["test1.sst".to_string()],
            })
            .unwrap();

        // Assert
        assert_eq!(new_version.manifest.ssts.len(), 1);
        assert_eq!(new_version.manifest.files.len(), 1);
        assert_eq!(new_version.manifest.files[0].name, "test2.sst");
    }

    #[test]
    fn should_update_sequence_number() {
        // Arrange
        let manifest = Manifest::default();
        let version = VersionSet::new(manifest);

        // Act
        let new_version = version
            .apply_edit(VersionEdit::UpdateSequence { sequence: 100 })
            .unwrap();

        // Assert
        assert_eq!(new_version.manifest.last_persisted_sequence, 100);
    }

    #[test]
    fn should_apply_multiple_edits_in_batch() {
        // Arrange
        let manifest = Manifest::default();
        let version = VersionSet::new(manifest);

        let edits = vec![
            VersionEdit::AddFile {
                file: Box::new(FileMeta {
                    name: "test1.sst".to_string(),
                    level: 0,
                    size_bytes: 1024,
                    ..Default::default()
                }),
            },
            VersionEdit::AddFile {
                file: Box::new(FileMeta {
                    name: "test2.sst".to_string(),
                    level: 1,
                    size_bytes: 2048,
                    ..Default::default()
                }),
            },
            VersionEdit::UpdateSequence { sequence: 42 },
        ];

        // Act
        let new_version = version.apply_edits(edits).unwrap();

        // Assert
        assert_eq!(new_version.manifest.files.len(), 2);
        assert_eq!(new_version.manifest.ssts.len(), 2);
        assert_eq!(new_version.manifest.files[0].name, "test1.sst");
        assert_eq!(new_version.manifest.files[1].name, "test2.sst");
        assert_eq!(new_version.manifest.last_persisted_sequence, 42);
    }
}
