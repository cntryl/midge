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
    /// Highest manifest journal edit included in the latest checkpoint.
    ///
    /// Older manifests omit this field and therefore replay their complete
    /// journal. Keeping the horizon in the snapshot makes a crash after the
    /// snapshot rename but before journal truncation idempotent.
    #[serde(default)]
    pub edit_checkpoint_id: u64,
}

impl Default for Manifest {
    fn default() -> Self {
        Self {
            last_persisted_sequence: 0,
            files: Vec::new(),
            column_families: Vec::new(),
            cloud_checkpoint: None,
            next_wal_seq: 1,
            next_sst_seqs: HashMap::new(),
            edit_checkpoint_id: 0,
        }
    }
}

fn default_next_wal_seq() -> u64 {
    1
}

fn millis_since_epoch() -> u64 {
    u64::try_from(
        std::time::SystemTime::now()
            .duration_since(std::time::UNIX_EPOCH)
            .unwrap_or_default()
            .as_millis(),
    )
    .unwrap_or(u64::MAX)
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
    /// Sequence frontier at which this column family became invisible.
    ///
    /// SSTs belonging to a dropped family remain authoritative until every
    /// snapshot at or below this frontier has been released.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub drop_sequence: Option<u64>,
    /// SST names captured when the family was dropped.  Keeping this list on
    /// the tombstone makes reclamation retryable after a crash or compaction
    /// of unrelated files.
    #[serde(default, skip_serializing_if = "Vec::is_empty")]
    pub dropped_sst_names: Vec<String>,
    /// Set only after the reclaim edit has removed the captured files from the
    /// authoritative manifest.  The tombstone itself is retained so IDs are
    /// never reused.
    #[serde(default)]
    pub reclaimed: bool,
}

/// File metadata for an SST
#[derive(Debug, Clone, Default, Serialize, Deserialize)]
pub struct FileMeta {
    pub name: String,
    pub level: u32,
    pub size_bytes: u64,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub content_crc32c: Option<u32>,
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
    /// Whether the persisted key bounds include every point key and range
    /// tombstone endpoint in this SST.
    ///
    /// Older manifests omit this additive field and are therefore treated as
    /// untrusted until a maintenance pass verifies the SST contents.
    #[serde(default)]
    pub key_bounds_complete: bool,
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
}

impl Manifest {
    /// Increment WAL sequence
    #[cfg(test)]
    pub fn increment_wal_seq(&mut self) {
        self.next_wal_seq += 1;
    }

    /// Get all files at a specific level
    #[cfg(test)]
    pub fn files_at_level(&self, level: u32) -> Vec<&FileMeta> {
        self.files.iter().filter(|f| f.level == level).collect()
    }

    /// Add a file to the manifest
    pub fn add_file(&mut self, file: FileMeta) {
        if let Some(existing) = self
            .files
            .iter_mut()
            .find(|existing| existing.name == file.name)
        {
            *existing = file;
        } else {
            self.files.push(file);
        }
    }

    /// Remove a file from the manifest
    pub fn remove_file(&mut self, name: &str) {
        self.files.retain(|f| f.name != name);
    }

    // === Column Family Lifecycle ===

    /// Get the next available column family ID.
    ///
    /// # Errors
    ///
    /// Returns [`crate::common::MidgeError::ResourceLimit`] after the last
    /// representable column family ID has been consumed.
    pub fn next_cf_id(&self) -> crate::common::MidgeResult<u32> {
        self.column_families
            .iter()
            .map(|cf| cf.id)
            .max()
            .unwrap_or(0)
            .checked_add(1)
            .ok_or_else(|| {
                crate::common::MidgeError::ResourceLimit(
                    "column family id space exhausted".to_string(),
                )
            })
    }

    /// Create a new column family
    #[cfg(test)]
    pub fn create_column_family(&mut self, name: String) -> u32 {
        let id = self
            .next_cf_id()
            .expect("test manifest column family id space");
        let created_at = millis_since_epoch();

        self.column_families.push(ColumnFamilyMeta {
            id,
            name,
            created_at,
            deleted_at: None,
            drop_sequence: None,
            dropped_sst_names: Vec::new(),
            reclaimed: false,
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
    #[cfg(test)]
    pub fn get_column_family_by_id(&self, id: u32) -> Option<&ColumnFamilyMeta> {
        self.column_families
            .iter()
            .find(|cf| cf.id == id && cf.deleted_at.is_none())
    }

    /// Get all active column families
    #[cfg(test)]
    pub fn active_column_families(&self) -> Vec<&ColumnFamilyMeta> {
        self.column_families
            .iter()
            .filter(|cf| cf.deleted_at.is_none())
            .collect()
    }

    /// Mark a column family as deleted (soft delete for durability).
    #[cfg(test)]
    pub fn delete_column_family(&mut self, cf_id: u32) -> bool {
        self.delete_column_family_with_reclamation(cf_id, 0, Vec::new())
    }

    /// Mark a family deleted while recording the snapshot frontier and the
    /// exact files that must eventually be reclaimed.
    pub fn delete_column_family_with_reclamation(
        &mut self,
        cf_id: u32,
        drop_sequence: u64,
        dropped_sst_names: Vec<String>,
    ) -> bool {
        if let Some(cf) = self
            .column_families
            .iter_mut()
            .find(|cf| cf.id == cf_id && cf.deleted_at.is_none())
        {
            cf.deleted_at = Some(millis_since_epoch());
            cf.drop_sequence = Some(drop_sequence);
            cf.dropped_sst_names = dropped_sst_names;
            cf.reclaimed = false;
            true
        } else {
            false
        }
    }

    /// Return dropped families whose snapshot frontier is no longer pinned.
    pub fn reclaimable_column_family_files(
        &self,
        oldest_snapshot_sequence: Option<u64>,
    ) -> Vec<(u32, Vec<String>)> {
        self.column_families
            .iter()
            .filter(|cf| cf.deleted_at.is_some())
            .filter(|cf| {
                let Some(drop_sequence) = cf.drop_sequence else {
                    return oldest_snapshot_sequence.is_none();
                };
                oldest_snapshot_sequence.is_none_or(|oldest| oldest > drop_sequence)
            })
            .filter_map(|cf| {
                let mut names = cf.dropped_sst_names.clone();
                names.extend(
                    self.files
                        .iter()
                        .filter(|file| file.cf_id == cf.id)
                        .map(|file| file.name.clone()),
                );
                names.sort();
                names.dedup();
                if cf.reclaimed && names.is_empty() {
                    None
                } else {
                    Some((cf.id, names))
                }
            })
            .collect()
    }

    /// Mark a dropped family reclaimed after a durable remove-file edit.
    pub fn mark_column_family_reclaimed(&mut self, cf_id: u32, names: &[String]) -> bool {
        let Some(cf) = self
            .column_families
            .iter_mut()
            .find(|cf| cf.id == cf_id && cf.deleted_at.is_some() && !cf.reclaimed)
        else {
            return false;
        };
        for name in names {
            cf.dropped_sst_names.retain(|candidate| candidate != name);
        }
        cf.reclaimed = cf.dropped_sst_names.is_empty();
        true
    }

    /// Apply a `ManifestEdit` (journal replay or live append)
    pub fn apply_edit(&mut self, edit: &crate::metadata::ManifestEdit) {
        match edit {
            crate::metadata::ManifestEdit::AddSst(meta) => {
                self.add_file(meta.clone());
            }
            crate::metadata::ManifestEdit::RemoveSst { name } => {
                self.remove_file(name);
            }
            crate::metadata::ManifestEdit::CreateColumnFamily {
                id,
                name,
                created_at,
            } => {
                if let Some(existing) = self.column_families.iter_mut().find(|cf| cf.id == *id) {
                    if existing.deleted_at.is_some() {
                        existing.name.clone_from(name);
                        existing.created_at = *created_at;
                        existing.deleted_at = None;
                    }
                } else {
                    self.column_families.push(ColumnFamilyMeta {
                        id: *id,
                        name: name.clone(),
                        created_at: *created_at,
                        deleted_at: None,
                        drop_sequence: None,
                        dropped_sst_names: Vec::new(),
                        reclaimed: false,
                    });
                }
            }
            crate::metadata::ManifestEdit::DropColumnFamilyAt {
                id,
                drop_sequence,
                dropped_sst_names,
            } => {
                let names = if dropped_sst_names.is_empty() {
                    self.files
                        .iter()
                        .filter(|file| file.cf_id == *id)
                        .map(|file| file.name.clone())
                        .collect()
                } else {
                    dropped_sst_names.clone()
                };
                let _ = self.delete_column_family_with_reclamation(*id, *drop_sequence, names);
            }
            crate::metadata::ManifestEdit::ReclaimColumnFamily { id, names } => {
                for name in names {
                    self.remove_file(name);
                }
                let _ = self.mark_column_family_reclaimed(*id, names);
            }
            crate::metadata::ManifestEdit::BumpWalSeq { seq } => {
                if *seq > self.last_persisted_sequence {
                    self.last_persisted_sequence = *seq;
                }
            }
            crate::metadata::ManifestEdit::BumpNextSstSeq { cf_id, next_seq } => {
                let current = self.next_sst_seqs.get(cf_id).copied().unwrap_or(0);
                if *next_seq > current {
                    self.next_sst_seqs.insert(*cf_id, *next_seq);
                }
            }
            crate::metadata::ManifestEdit::SetCloudCheckpoint(cp) => {
                let should_advance = self
                    .cloud_checkpoint
                    .as_ref()
                    .is_none_or(|current| cp.checkpoint_sequence >= current.checkpoint_sequence);
                if should_advance {
                    self.cloud_checkpoint = Some(cp.clone());
                }
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
        // Arrange
        // (no setup)

        // Act
        let manifest = Manifest::default();

        // Assert
        assert_eq!(manifest.last_persisted_sequence, 0);
        assert!(manifest.files.is_empty());
        assert!(manifest.column_families.is_empty());
        assert!(manifest.cloud_checkpoint.is_none());
        assert_eq!(manifest.next_wal_seq, 1);
        assert!(manifest.next_sst_seqs.is_empty());
    }

    // ========================================================================
    // WAL sequence tests
    // ========================================================================

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

    // ========================================================================
    // File management tests
    // ========================================================================

    #[test]
    fn should_add_file_to_manifest() {
        // Arrange
        let mut manifest = Manifest::default();
        let sst_name = crate::sst::file_name(0, 0, 1);
        let file = FileMeta {
            name: sst_name.clone(),
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
        assert_eq!(manifest.files[0].name, sst_name);
    }

    #[test]
    fn should_treat_missing_key_bounds_completeness_as_untrusted() {
        // Arrange
        let json = r#"{
            "name":"legacy.sst",
            "level":1,
            "size_bytes":10,
            "cf_id":0,
            "sst_seq":1,
            "smallest_key":[97],
            "largest_key":[122]
        }"#;

        // Act
        let file: FileMeta = serde_json::from_str(json).expect("deserialize legacy metadata");

        // Assert
        assert!(!file.key_bounds_complete);
    }

    #[test]
    fn should_add_multiple_files_to_manifest() {
        // Arrange
        let mut manifest = Manifest::default();
        let expected_names: Vec<String> = (0_u64..5)
            .map(|i| crate::sst::file_name(0, u32::try_from(i).expect("loop index fits in u32"), i))
            .collect();

        // Act
        for i in 0_u64..5 {
            let idx = usize::try_from(i).expect("loop index fits in usize");
            let file = FileMeta {
                name: expected_names[idx].clone(),
                level: u32::try_from(i).expect("loop index fits in u32"),
                size_bytes: 1000 + (i * 100),
                ..Default::default()
            };
            manifest.add_file(file);
        }

        // Assert: every file entry is present with its own distinct
        // name, level, and size (not just an overall count)
        assert_eq!(manifest.files.len(), 5);
        for i in 0_u64..5 {
            let idx = usize::try_from(i).expect("loop index fits in usize");
            let file = manifest
                .files
                .iter()
                .find(|f| f.name == expected_names[idx])
                .unwrap_or_else(|| panic!("file {idx} missing from manifest"));
            assert_eq!(
                file.level,
                u32::try_from(i).expect("loop index fits in u32")
            );
            assert_eq!(file.size_bytes, 1000 + (i * 100));
        }
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
        let id1 = manifest.next_cf_id().expect("next column family id");
        manifest.create_column_family("cf1".to_string());
        let id2 = manifest.next_cf_id().expect("next column family id");

        // Assert
        assert_eq!(id1, 1);
        assert_eq!(id2, 2);
    }

    #[test]
    fn should_return_resource_limit_when_column_family_id_space_is_exhausted() {
        // Arrange
        let manifest = Manifest {
            column_families: vec![ColumnFamilyMeta {
                id: u32::MAX,
                name: "last-column-family".to_string(),
                created_at: 0,
                deleted_at: None,
                drop_sequence: None,
                dropped_sst_names: Vec::new(),
                reclaimed: false,
            }],
            ..Manifest::default()
        };

        // Act
        let result = manifest.next_cf_id();

        // Assert
        assert!(matches!(
            result,
            Err(crate::common::MidgeError::ResourceLimit(message))
                if message.contains("column family")
        ));
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
        let before = u64::try_from(
            std::time::SystemTime::now()
                .duration_since(std::time::UNIX_EPOCH)
                .unwrap()
                .as_millis(),
        )
        .expect("timestamp fits in u64");

        // Act
        manifest.delete_column_family(cf_id);
        let after = u64::try_from(
            std::time::SystemTime::now()
                .duration_since(std::time::UNIX_EPOCH)
                .unwrap()
                .as_millis(),
        )
        .expect("timestamp fits in u64");

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
                    name: format!("l{level}_f{i}.sst"),
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

    #[test]
    fn should_reclaim_late_manifest_file_when_column_family_was_already_reclaimed() {
        // Arrange: model a legacy crash/race where compaction published after
        // the drop's original frozen file set had already been reclaimed.
        let mut manifest = Manifest::default();
        let cf_id = manifest.create_column_family("dropped".to_string());
        assert!(manifest.delete_column_family_with_reclamation(cf_id, 10, Vec::new()));
        assert!(manifest.mark_column_family_reclaimed(cf_id, &[]));
        let late_output = crate::sst::file_name(cf_id, 1, 99);
        manifest.files.push(FileMeta {
            name: late_output.clone(),
            cf_id,
            level: 1,
            ..Default::default()
        });

        // Act
        let candidates = manifest.reclaimable_column_family_files(None);

        // Assert
        assert_eq!(candidates, vec![(cf_id, vec![late_output])]);
    }
}
