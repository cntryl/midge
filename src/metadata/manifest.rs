//! Manifest data structures and operations
//!
//! The manifest is the source of truth for all SST files, column families,
//! and database metadata.

use serde::{Deserialize, Serialize};
use std::collections::HashMap;
use std::sync::atomic::{AtomicU64, Ordering};
use std::sync::Arc;

/// Core manifest structure tracking all SSTs and metadata
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct Manifest {
    /// Last persisted sequence number
    pub last_persisted_sequence: u64,
    /// List of all SST file names
    pub ssts: Vec<String>,
    /// SST file metadata
    #[serde(default)]
    pub files: Vec<FileMeta>,
    /// Column families
    #[serde(default)]
    pub column_families: Vec<ColumnFamilyMeta>,
    /// Cloud checkpoint info for WAL coordination
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub cloud_checkpoint: Option<CloudCheckpoint>,
    /// Next WAL sequence number
    #[serde(default = "default_next_wal_seq")]
    pub next_wal_seq: u64,
    /// Next SST sequence numbers per CF
    #[serde(default)]
    pub next_sst_seqs: HashMap<u32, u64>,
}

impl Default for Manifest {
    fn default() -> Self {
        Self {
            last_persisted_sequence: 0,
            ssts: Vec::new(),
            files: Vec::new(),
            column_families: Vec::new(),
            cloud_checkpoint: None,
            next_wal_seq: 1,
            next_sst_seqs: HashMap::new(),
        }
    }
}

fn default_next_wal_seq() -> u64 {
    1
}

/// Cloud checkpoint for WAL coordination
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct CloudCheckpoint {
    /// Highest WAL sequence fully materialized to cloud
    pub checkpoint_sequence: u64,
    /// SST files covering the checkpoint
    pub covering_ssts: Vec<String>,
}

/// Column family metadata
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct ColumnFamilyMeta {
    pub id: u32,
    pub name: String,
    /// Timestamp when column family was created (milliseconds since epoch)
    #[serde(default)]
    pub created_at: u64,
    /// Timestamp when column family was deleted (None if active)
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub deleted_at: Option<u64>,
}

/// File metadata for an SST
#[derive(Debug, Clone, Default, Serialize, Deserialize)]
pub struct FileMeta {
    pub name: String,
    pub level: u32,
    pub size_bytes: u64,
    #[serde(default)]
    pub cf_id: u32,
    #[serde(default)]
    pub sst_seq: u64,
    #[serde(default)]
    pub smallest_key: Option<Vec<u8>>,
    #[serde(default)]
    pub largest_key: Option<Vec<u8>>,
    #[serde(default)]
    pub smallest_seq: Option<u64>,
    #[serde(default)]
    pub largest_seq: Option<u64>,
    #[serde(default)]
    pub sublevel: u32,
    /// Read counter for compaction prioritization (not persisted)
    /// Tracks how many times this SST has been accessed by reads
    #[serde(skip)]
    pub read_count: Arc<AtomicU64>,
}

impl FileMeta {
    /// Record a read access to this SST
    pub fn record_read(&self) {
        self.read_count.fetch_add(1, Ordering::Relaxed);
    }

    /// Get current read count
    pub fn get_read_count(&self) -> u64 {
        self.read_count.load(Ordering::Relaxed)
    }

    /// Reset read counter (useful after compaction)
    pub fn reset_read_count(&self) {
        self.read_count.store(0, Ordering::Relaxed);
    }
}

impl Manifest {
    /// Create a new manifest
    pub fn new() -> Self {
        Self::default()
    }

    /// Get the next WAL sequence
    pub fn next_wal_seq(&self) -> u64 {
        self.next_wal_seq
    }

    /// Increment WAL sequence
    pub fn increment_wal_seq(&mut self) {
        self.next_wal_seq += 1;
    }

    /// Get all files at a specific level
    pub fn files_at_level(&self, level: u32) -> Vec<&FileMeta> {
        self.files.iter().filter(|f| f.level == level).collect()
    }

    /// Add a file to the manifest
    pub fn add_file(&mut self, file: FileMeta) {
        self.files.push(file);
    }

    /// Remove a file from the manifest
    pub fn remove_file(&mut self, name: &str) {
        self.files.retain(|f| f.name != name);
    }

    // === Column Family Lifecycle ===

    /// Get next available column family ID
    pub fn next_cf_id(&self) -> u32 {
        self.column_families
            .iter()
            .map(|cf| cf.id)
            .max()
            .unwrap_or(0)
            + 1
    }

    /// Create a new column family
    pub fn create_column_family(&mut self, name: String) -> u32 {
        let id = self.next_cf_id();
        let created_at = std::time::SystemTime::now()
            .duration_since(std::time::UNIX_EPOCH)
            .unwrap_or_default()
            .as_millis() as u64;

        self.column_families.push(ColumnFamilyMeta {
            id,
            name,
            created_at,
            deleted_at: None,
        });

        id
    }

    /// Get an active column family by name
    pub fn get_column_family_by_name(&self, name: &str) -> Option<&ColumnFamilyMeta> {
        self.column_families
            .iter()
            .find(|cf| cf.name == name && cf.deleted_at.is_none())
    }

    /// Get an active column family by ID
    pub fn get_column_family_by_id(&self, id: u32) -> Option<&ColumnFamilyMeta> {
        self.column_families
            .iter()
            .find(|cf| cf.id == id && cf.deleted_at.is_none())
    }

    /// Get all active column families
    pub fn active_column_families(&self) -> Vec<&ColumnFamilyMeta> {
        self.column_families
            .iter()
            .filter(|cf| cf.deleted_at.is_none())
            .collect()
    }

    /// Mark a column family as deleted (soft delete for durability)
    pub fn delete_column_family(&mut self, cf_id: u32) -> bool {
        if let Some(cf) = self
            .column_families
            .iter_mut()
            .find(|cf| cf.id == cf_id && cf.deleted_at.is_none())
        {
            cf.deleted_at = Some(
                std::time::SystemTime::now()
                    .duration_since(std::time::UNIX_EPOCH)
                    .unwrap_or_default()
                    .as_millis() as u64,
            );
            true
        } else {
            false
        }
    }

    /// Apply a ManifestEdit (journal replay or live append)
    pub fn apply_edit(&mut self, edit: &crate::metadata::ManifestEdit) {
        match edit {
            crate::metadata::ManifestEdit::AddSst(meta) => {
                self.add_file(meta.clone());
            }
            crate::metadata::ManifestEdit::RemoveSst { name } => {
                self.remove_file(name);
            }
            crate::metadata::ManifestEdit::CreateColumnFamily { id, name, created_at } => {
                self.column_families.push(ColumnFamilyMeta { id: *id, name: name.clone(), created_at: *created_at, deleted_at: None });
            }
            crate::metadata::ManifestEdit::DropColumnFamily { id } => {
                let _ = self.delete_column_family(*id);
            }
            crate::metadata::ManifestEdit::BumpWalSeq { seq } => {
                if *seq > self.last_persisted_sequence {
                    self.last_persisted_sequence = *seq;
                }
            }
            crate::metadata::ManifestEdit::BumpNextSstSeq { cf_id, next_seq } => {
                self.next_sst_seqs.insert(*cf_id, *next_seq);
            }
            crate::metadata::ManifestEdit::SetCloudCheckpoint(cp) => {
                self.cloud_checkpoint = Some(cp.clone());
            }
            crate::metadata::ManifestEdit::Batch(edits) => {
                // Apply each edit in order (atomic at journal record boundary)
                for e in edits {
                    self.apply_edit(e);
                }
            }
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    // ========================================================================
    // Manifest creation and initialization tests
    // ========================================================================

    #[test]
    fn should_create_default_manifest() {
        // Arrange / Act
        let manifest = Manifest::default();

        // Assert
        assert_eq!(manifest.last_persisted_sequence, 0);
        assert!(manifest.ssts.is_empty());
        assert!(manifest.files.is_empty());
        assert!(manifest.column_families.is_empty());
        assert!(manifest.cloud_checkpoint.is_none());
        assert_eq!(manifest.next_wal_seq, 1);
        assert!(manifest.next_sst_seqs.is_empty());
    }

    #[test]
    fn should_create_manifest_via_new() {
        // Arrange / Act
        let manifest = Manifest::new();

        // Assert: same as default
        assert_eq!(manifest.last_persisted_sequence, 0);
        assert!(manifest.ssts.is_empty());
    }

    // ========================================================================
    // WAL sequence tests
    // ========================================================================

    #[test]
    fn should_return_next_wal_seq() {
        // Arrange
        let manifest = Manifest {
            next_wal_seq: 42,
            ..Default::default()
        };

        // Act
        let seq = manifest.next_wal_seq();

        // Assert
        assert_eq!(seq, 42);
    }

    #[test]
    fn should_increment_wal_seq() {
        // Arrange
        let mut manifest = Manifest {
            next_wal_seq: 5,
            ..Default::default()
        };

        // Act
        manifest.increment_wal_seq();

        // Assert
        assert_eq!(manifest.next_wal_seq, 6);
    }

    #[test]
    fn should_increment_wal_seq_multiple_times() {
        // Arrange
        let mut manifest = Manifest {
            next_wal_seq: 1,
            ..Default::default()
        };

        // Act
        manifest.increment_wal_seq();
        manifest.increment_wal_seq();
        manifest.increment_wal_seq();

        // Assert
        assert_eq!(manifest.next_wal_seq, 4);
    }

    // ========================================================================
    // File management tests
    // ========================================================================

    #[test]
    fn should_add_file_to_manifest() {
        // Arrange
        let mut manifest = Manifest::default();
        let file = FileMeta {
            name: "sst_001.sst".to_string(),
            level: 0,
            size_bytes: 1024,
            cf_id: 0,
            sst_seq: 1,
            ..Default::default()
        };

        // Act
        manifest.add_file(file.clone());

        // Assert
        assert_eq!(manifest.files.len(), 1);
        assert_eq!(manifest.files[0].name, "sst_001.sst");
    }

    #[test]
    fn should_add_multiple_files_to_manifest() {
        // Arrange
        let mut manifest = Manifest::default();

        // Act
        for i in 0..5 {
            let file = FileMeta {
                name: format!("sst_{:03}.sst", i),
                level: i as u32,
                size_bytes: 1000 + (i as u64 * 100),
                ..Default::default()
            };
            manifest.add_file(file);
        }

        // Assert
        assert_eq!(manifest.files.len(), 5);
    }

    #[test]
    fn should_get_files_at_level() {
        // Arrange
        let mut manifest = Manifest::default();
        manifest.add_file(FileMeta {
            name: "l0_file1.sst".to_string(),
            level: 0,
            ..Default::default()
        });
        manifest.add_file(FileMeta {
            name: "l0_file2.sst".to_string(),
            level: 0,
            ..Default::default()
        });
        manifest.add_file(FileMeta {
            name: "l1_file1.sst".to_string(),
            level: 1,
            ..Default::default()
        });

        // Act
        let level_0 = manifest.files_at_level(0);
        let level_1 = manifest.files_at_level(1);
        let level_2 = manifest.files_at_level(2);

        // Assert
        assert_eq!(level_0.len(), 2);
        assert_eq!(level_1.len(), 1);
        assert!(level_2.is_empty());
    }

    #[test]
    fn should_remove_file_by_name() {
        // Arrange
        let mut manifest = Manifest::default();
        manifest.add_file(FileMeta {
            name: "file_to_keep.sst".to_string(),
            ..Default::default()
        });
        manifest.add_file(FileMeta {
            name: "file_to_remove.sst".to_string(),
            ..Default::default()
        });

        // Act
        manifest.remove_file("file_to_remove.sst");

        // Assert
        assert_eq!(manifest.files.len(), 1);
        assert_eq!(manifest.files[0].name, "file_to_keep.sst");
    }

    #[test]
    fn should_remove_all_matching_files() {
        // Arrange
        let mut manifest = Manifest::default();
        manifest.add_file(FileMeta {
            name: "remove_me.sst".to_string(),
            ..Default::default()
        });
        manifest.add_file(FileMeta {
            name: "remove_me.sst".to_string(),
            ..Default::default()
        });

        // Act
        manifest.remove_file("remove_me.sst");

        // Assert: all matching files removed
        assert!(manifest.files.is_empty());
    }

    #[test]
    fn should_not_remove_non_existent_file() {
        // Arrange
        let mut manifest = Manifest::default();
        manifest.add_file(FileMeta {
            name: "exists.sst".to_string(),
            ..Default::default()
        });

        // Act
        manifest.remove_file("does_not_exist.sst");

        // Assert: no change
        assert_eq!(manifest.files.len(), 1);
    }

    // ========================================================================
    // Column family management tests
    // ========================================================================

    #[test]
    fn should_get_next_cf_id() {
        // Arrange
        let mut manifest = Manifest::default();

        // Act
        let id1 = manifest.next_cf_id();
        manifest.create_column_family("cf1".to_string());
        let id2 = manifest.next_cf_id();

        // Assert
        assert_eq!(id1, 1);
        assert_eq!(id2, 2);
    }

    #[test]
    fn should_return_one_for_empty_column_families() {
        // Arrange
        let manifest = Manifest::default();

        // Act
        let next_id = manifest.next_cf_id();

        // Assert
        assert_eq!(next_id, 1);
    }

    #[test]
    fn should_create_column_family() {
        // Arrange
        let mut manifest = Manifest::default();

        // Act
        let cf_id = manifest.create_column_family("my_cf".to_string());

        // Assert
        assert_eq!(cf_id, 1);
        assert_eq!(manifest.column_families.len(), 1);
        assert_eq!(manifest.column_families[0].id, 1);
        assert_eq!(manifest.column_families[0].name, "my_cf");
        assert!(manifest.column_families[0].deleted_at.is_none());
    }

    #[test]
    fn should_auto_increment_cf_ids() {
        // Arrange
        let mut manifest = Manifest::default();

        // Act
        let id1 = manifest.create_column_family("cf1".to_string());
        let id2 = manifest.create_column_family("cf2".to_string());
        let id3 = manifest.create_column_family("cf3".to_string());

        // Assert
        assert_eq!(id1, 1);
        assert_eq!(id2, 2);
        assert_eq!(id3, 3);
    }

    #[test]
    fn should_get_column_family_by_name() {
        // Arrange
        let mut manifest = Manifest::default();
        manifest.create_column_family("my_cf".to_string());

        // Act
        let cf = manifest.get_column_family_by_name("my_cf");

        // Assert
        assert!(cf.is_some());
        assert_eq!(cf.unwrap().name, "my_cf");
    }

    #[test]
    fn should_return_none_for_non_existent_cf_name() {
        // Arrange
        let manifest = Manifest::default();

        // Act
        let cf = manifest.get_column_family_by_name("does_not_exist");

        // Assert
        assert!(cf.is_none());
    }

    #[test]
    fn should_get_column_family_by_id() {
        // Arrange
        let mut manifest = Manifest::default();
        let cf_id = manifest.create_column_family("my_cf".to_string());

        // Act
        let cf = manifest.get_column_family_by_id(cf_id);

        // Assert
        assert!(cf.is_some());
        assert_eq!(cf.unwrap().id, cf_id);
    }

    #[test]
    fn should_return_none_for_non_existent_cf_id() {
        // Arrange
        let manifest = Manifest::default();

        // Act
        let cf = manifest.get_column_family_by_id(999);

        // Assert
        assert!(cf.is_none());
    }

    #[test]
    fn should_return_active_column_families() {
        // Arrange
        let mut manifest = Manifest::default();
        manifest.create_column_family("cf1".to_string());
        manifest.create_column_family("cf2".to_string());

        // Act
        let active = manifest.active_column_families();

        // Assert
        assert_eq!(active.len(), 2);
    }

    #[test]
    fn should_exclude_deleted_column_families() {
        // Arrange
        let mut manifest = Manifest::default();
        let id1 = manifest.create_column_family("cf1".to_string());
        let id2 = manifest.create_column_family("cf2".to_string());
        manifest.delete_column_family(id1);

        // Act
        let active = manifest.active_column_families();

        // Assert: only cf2 is active
        assert_eq!(active.len(), 1);
        assert_eq!(active[0].id, id2);
    }

    #[test]
    fn should_delete_column_family_by_id() {
        // Arrange
        let mut manifest = Manifest::default();
        let cf_id = manifest.create_column_family("to_delete".to_string());

        // Act: delete returns true and marks deleted_at
        let deleted = manifest.delete_column_family(cf_id);

        // Assert
        assert!(deleted);
        // After deletion, get_column_family_by_id returns None (filters out deleted)
        let cf = manifest.get_column_family_by_id(cf_id);
        assert!(cf.is_none());
        // But the CF still exists in the list with deleted_at set
        let all_cfs = &manifest.column_families;
        let deleted_cf = all_cfs.iter().find(|cf| cf.id == cf_id).unwrap();
        assert!(deleted_cf.deleted_at.is_some());
    }

    #[test]
    fn should_set_deleted_at_timestamp() {
        // Arrange
        let mut manifest = Manifest::default();
        let cf_id = manifest.create_column_family("cf".to_string());
        let before = std::time::SystemTime::now()
            .duration_since(std::time::UNIX_EPOCH)
            .unwrap()
            .as_millis() as u64;

        // Act
        manifest.delete_column_family(cf_id);
        let after = std::time::SystemTime::now()
            .duration_since(std::time::UNIX_EPOCH)
            .unwrap()
            .as_millis() as u64;

        // Assert: deleted_at is set and within time window
        let cf = manifest
            .column_families
            .iter()
            .find(|cf| cf.id == cf_id)
            .unwrap();
        assert!(cf.deleted_at.is_some());
        let deleted_at = cf.deleted_at.unwrap();
        assert!(deleted_at >= before);
        assert!(deleted_at <= after);
    }

    // ========================================================================
    // Integration tests
    // ========================================================================

    #[test]
    fn should_manage_files_across_levels() {
        // Arrange
        let mut manifest = Manifest::default();

        // Act: Add files to different levels
        for level in 0..4 {
            for i in 0..3 {
                manifest.add_file(FileMeta {
                    name: format!("l{}_f{}.sst", level, i),
                    level,
                    ..Default::default()
                });
            }
        }

        // Assert
        for level in 0..4 {
            assert_eq!(manifest.files_at_level(level).len(), 3);
        }
        assert_eq!(manifest.files.len(), 12);
    }

    #[test]
    fn should_handle_multiple_column_families_with_files() {
        // Arrange
        let mut manifest = Manifest::default();

        // Act
        let cf1_id = manifest.create_column_family("cf1".to_string());
        let cf2_id = manifest.create_column_family("cf2".to_string());

        manifest.add_file(FileMeta {
            cf_id: cf1_id,
            name: "cf1_file.sst".to_string(),
            ..Default::default()
        });
        manifest.add_file(FileMeta {
            cf_id: cf2_id,
            name: "cf2_file.sst".to_string(),
            ..Default::default()
        });

        // Assert
        assert_eq!(manifest.column_families.len(), 2);
        assert_eq!(manifest.files.len(), 2);
        assert_eq!(manifest.files[0].cf_id, cf1_id);
        assert_eq!(manifest.files[1].cf_id, cf2_id);
    }
}
