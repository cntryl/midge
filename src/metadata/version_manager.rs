//! Version Manager - coordinates manifest updates and version transitions
//!
//! Responsible for:
//! - Accepting manifest updates from writers (compaction, flush, WAL recovery)
//! - Creating new versions atomically
//! - Publishing versions to the VersionSet
//! - Maintaining edit logs for recovery

use crate::common::MidgeResult;
use crate::metadata::manifest::Manifest;
use crate::metadata::version_set::{Version, VersionSet};
use std::sync::atomic::{AtomicU64, Ordering};
use std::sync::{Arc, Mutex};
use std::collections::VecDeque;

/// Edit operation for version updates
#[derive(Debug, Clone)]
pub enum VersionEdit {
    /// Add a new file to manifest
    AddFile {
        cf_id: u32,
        level: u32,
        name: String,
        size_bytes: u64,
        smallest_key: Vec<u8>,
        largest_key: Vec<u8>,
    },
    /// Delete a file from manifest
    DeleteFile {
        cf_id: u32,
        level: u32,
        name: String,
    },
    /// Update sequence number
    UpdateSequence(u64),
    /// Add column family
    AddColumnFamily {
        id: u32,
        name: String,
    },
}

impl VersionEdit {
    pub fn add_file(cf_id: u32, level: u32, name: String, size_bytes: u64,
                    smallest_key: Vec<u8>, largest_key: Vec<u8>) -> Self {
        Self::AddFile { cf_id, level, name, size_bytes, smallest_key, largest_key }
    }

    pub fn delete_file(cf_id: u32, level: u32, name: String) -> Self {
        Self::DeleteFile { cf_id, level, name }
    }

    pub fn update_sequence(seq: u64) -> Self {
        Self::UpdateSequence(seq)
    }

    pub fn add_column_family(id: u32, name: String) -> Self {
        Self::AddColumnFamily { id, name }
    }
}

/// Manages version creation and transitions
pub struct VersionManager {
    /// Current version number (monotonically increasing)
    next_version_id: AtomicU64,
    /// Pending edits not yet applied
    pending_edits: Arc<Mutex<VecDeque<VersionEdit>>>,
    /// Current manifest being built
    current_manifest: Arc<Mutex<Manifest>>,
    /// Version set for publishing versions
    version_set: Arc<VersionSet>,
}

impl VersionManager {
    pub fn new(initial_version_set: Arc<VersionSet>, initial_manifest: Manifest) -> Self {
        Self {
            next_version_id: AtomicU64::new(1),
            pending_edits: Arc::new(Mutex::new(VecDeque::new())),
            current_manifest: Arc::new(Mutex::new(initial_manifest)),
            version_set: initial_version_set,
        }
    }

    /// Submit an edit to the pending queue
    pub fn add_edit(&self, edit: VersionEdit) -> MidgeResult<()> {
        let mut edits = self.pending_edits.lock().unwrap();
        edits.push_back(edit);
        Ok(())
    }

    /// Apply all pending edits to manifest and create new version
    pub fn apply_edits(&self) -> MidgeResult<Arc<Version>> {
        let mut edits = self.pending_edits.lock().unwrap();
        if edits.is_empty() {
            return Err(crate::common::MidgeError::InvalidArgument("No edits to apply".to_string()));
        }

        let mut manifest = self.current_manifest.lock().unwrap();

        // Apply all edits
        while let Some(edit) = edits.pop_front() {
            match edit {
                VersionEdit::AddFile { cf_id, level, name, size_bytes, smallest_key, largest_key } => {
                    manifest.files.push(crate::metadata::manifest::FileMeta {
                        name,
                        level,
                        size_bytes,
                        cf_id,
                        smallest_key: Some(smallest_key),
                        largest_key: Some(largest_key),
                        ..Default::default()
                    });
                }
                VersionEdit::DeleteFile { cf_id: _, level: _, name } => {
                    manifest.files.retain(|f| f.name != name);
                }
                VersionEdit::UpdateSequence(seq) => {
                    manifest.last_persisted_sequence = seq;
                }
                VersionEdit::AddColumnFamily { id, name } => {
                    manifest.column_families.push(crate::metadata::manifest::ColumnFamilyMeta { id, name });
                }
            }
        }

        // Create new version
        let version_id = self.next_version_id.fetch_add(1, Ordering::SeqCst);
        let version = Arc::new(Version::new(version_id, manifest.clone()));

        // Publish to version set
        self.version_set.install_version(version.clone())?;

        Ok(version)
    }

    /// Get current version ID
    pub fn current_version_id(&self) -> u64 {
        self.version_set.current_version_id()
    }

    /// Get current version
    pub fn current_version(&self) -> MidgeResult<Arc<Version>> {
        self.version_set.current_version()
    }

    /// Get pending edit count
    pub fn pending_edit_count(&self) -> usize {
        self.pending_edits.lock().unwrap().len()
    }

    /// Get version set reference
    pub fn version_set(&self) -> &Arc<VersionSet> {
        &self.version_set
    }

    /// Clear pending edits without applying
    pub fn clear_edits(&self) {
        self.pending_edits.lock().unwrap().clear();
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn create_test_version_manager() -> VersionManager {
        let manifest = Manifest::new();
        let version = Arc::new(Version::new(0, manifest));
        let version_set = Arc::new(VersionSet::new(version));
        VersionManager::new(version_set, Manifest::new())
    }

    #[test]
    fn should_create_version_manager_when_instantiated() {
        // Arrange & Act
        let manager = create_test_version_manager();

        // Assert
        assert_eq!(manager.current_version_id(), 0);
        assert_eq!(manager.pending_edit_count(), 0);
    }

    #[test]
    fn should_add_edit_when_add_edit_called() {
        // Arrange
        let manager = create_test_version_manager();
        let edit = VersionEdit::update_sequence(100);

        // Act
        manager.add_edit(edit).unwrap();

        // Assert
        assert_eq!(manager.pending_edit_count(), 1);
    }

    #[test]
    fn should_apply_multiple_edits_when_apply_edits_called() {
        // Arrange
        let manager = create_test_version_manager();
        manager.add_edit(VersionEdit::add_file(
            0, 0, "file1.sst".to_string(), 1024,
            vec![1], vec![10]
        )).unwrap();
        manager.add_edit(VersionEdit::update_sequence(50)).unwrap();

        // Act
        let version = manager.apply_edits().unwrap();

        // Assert
        assert_eq!(version.version_id(), 1);
        assert_eq!(manager.pending_edit_count(), 0);
        assert_eq!(version.file_count(), 1);
    }

    #[test]
    fn should_create_new_version_when_edits_applied() {
        // Arrange
        let manager = create_test_version_manager();
        manager.add_edit(VersionEdit::add_file(
            0, 0, "file1.sst".to_string(), 2048,
            vec![5], vec![15]
        )).unwrap();

        // Act
        manager.apply_edits().unwrap();
        let current = manager.current_version().unwrap();

        // Assert
        assert_eq!(current.version_id(), 1);
        assert_eq!(current.file_count(), 1);
    }

    #[test]
    fn should_return_error_when_applying_empty_edits() {
        // Arrange
        let manager = create_test_version_manager();

        // Act
        let result = manager.apply_edits();

        // Assert
        assert!(result.is_err());
    }

    #[test]
    fn should_delete_file_when_delete_edit_applied() {
        // Arrange
        let manager = create_test_version_manager();
        manager.add_edit(VersionEdit::add_file(
            0, 0, "file1.sst".to_string(), 1024,
            vec![1], vec![10]
        )).unwrap();
        let v1 = manager.apply_edits().unwrap();
        assert_eq!(v1.file_count(), 1);

        // Act
        manager.add_edit(VersionEdit::delete_file(0, 0, "file1.sst".to_string())).unwrap();
        let v2 = manager.apply_edits().unwrap();

        // Assert
        assert_eq!(v2.version_id(), 2);
        assert_eq!(v2.file_count(), 0);
    }

    #[test]
    fn should_add_column_family_when_edit_applied() {
        // Arrange
        let manager = create_test_version_manager();

        // Act
        manager.add_edit(VersionEdit::add_column_family(1, "secondary".to_string())).unwrap();
        let version = manager.apply_edits().unwrap();

        // Assert
        assert!(version.get_cf(1).is_some());
    }

    #[test]
    fn should_clear_edits_when_clear_edits_called() {
        // Arrange
        let manager = create_test_version_manager();
        manager.add_edit(VersionEdit::update_sequence(100)).unwrap();
        manager.add_edit(VersionEdit::update_sequence(101)).unwrap();
        assert_eq!(manager.pending_edit_count(), 2);

        // Act
        manager.clear_edits();

        // Assert
        assert_eq!(manager.pending_edit_count(), 0);
    }

    #[test]
    fn should_publish_versions_to_set_when_edits_applied() {
        // Arrange
        let manager = create_test_version_manager();
        manager.add_edit(VersionEdit::add_file(
            0, 0, "file1.sst".to_string(), 1024,
            vec![1], vec![10]
        )).unwrap();

        // Act
        manager.apply_edits().unwrap();

        // Assert
        assert_eq!(manager.version_set().version_count(), 2); // initial + new
        assert!(manager.version_set().has_version(1));
    }

    #[test]
    fn should_apply_batched_edits_atomically() {
        // Arrange
        let manager = create_test_version_manager();
        manager.add_edit(VersionEdit::add_file(0, 0, "file1.sst".to_string(), 1024, vec![1], vec![10])).unwrap();
        manager.add_edit(VersionEdit::add_file(0, 1, "file2.sst".to_string(), 2048, vec![11], vec![20])).unwrap();
        manager.add_edit(VersionEdit::update_sequence(75)).unwrap();

        // Act
        let version = manager.apply_edits().unwrap();

        // Assert
        assert_eq!(version.file_count(), 2);
        assert_eq!(manager.pending_edit_count(), 0);
    }
}
