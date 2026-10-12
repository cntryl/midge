//! Manifest data structures and operations
//!
//! The manifest is the source of truth for all SST files, column families,
//! and database metadata.

use super::frontiers::{FamilyFrontier, FlushFrontiers};
use super::ManifestEdit;
use crate::common::{MidgeError, MidgeResult};
use serde::{Deserialize, Serialize};
use std::collections::HashMap;
use std::sync::atomic::{AtomicU64, Ordering};
use std::sync::Arc;

mod transitions;
pub(crate) use transitions::apply_column_family_lifecycle;
pub use transitions::PreparedManifestEdit;
use transitions::{invalid_transition, EditValidator};

/// Core manifest structure tracking all SSTs and metadata
#[derive(Debug, Clone, Default, Serialize, Deserialize)]
pub struct Manifest {
    /// Required persisted per-family flush authority.
    flushed_through: FlushFrontiers,
    /// SST file metadata
    #[serde(default)]
    pub files: Vec<FileMeta>,
    /// Column families
    #[serde(default)]
    pub column_families: Vec<ColumnFamilyMeta>,
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

impl Manifest {
    #[must_use]
    pub fn flush_frontiers(&self) -> &FlushFrontiers {
        &self.flushed_through
    }

    /// Validate snapshot authority without inventing missing family progress.
    ///
    /// # Errors
    /// Returns corruption for missing or inconsistent family, file or frontier state.
    pub fn validate_persisted(&self) -> MidgeResult<()> {
        let mut ids = std::collections::HashSet::new();
        let mut names = std::collections::HashSet::new();
        for family in &self.column_families {
            if family.id == 0 || family.name == "default" || !ids.insert(family.id) {
                return Err(invalid_transition("duplicate or explicit default family"));
            }
            let frontier = self.flushed_through.get(family.id)?;
            if family.deleted_at.is_some() != (frontier == FamilyFrontier::Dropped)
                || (family.reclaimed && family.deleted_at.is_none())
                || (family.deleted_at.is_some() && family.drop_sequence.is_none())
                || (family.deleted_at.is_none()
                    && (family.drop_sequence.is_some() || !family.dropped_sst_names.is_empty()))
            {
                return Err(invalid_transition(
                    "family lifecycle and flush frontier disagree",
                ));
            }
            if family.deleted_at.is_none() && !names.insert(&family.name) {
                return Err(invalid_transition("duplicate live family name"));
            }
            for name in &family.dropped_sst_names {
                crate::cloud_layout::PersistedSstName::parse(name)?;
            }
        }
        for (id, _) in self.flushed_through.iter() {
            if id != 0 && !ids.contains(&id) {
                return Err(invalid_transition("orphan flush frontier"));
            }
        }
        let mut file_names = std::collections::HashSet::new();
        for file in &self.files {
            crate::cloud_layout::PersistedSstName::parse(&file.name).map_err(|error| {
                invalid_transition(format!("manifest SST name is invalid: {error}"))
            })?;
            if !file_names.insert(&file.name) {
                return Err(invalid_transition("duplicate SST name"));
            }
            self.flushed_through.get(file.cf_id)?;
        }
        for id in self.next_sst_seqs.keys() {
            self.flushed_through.get(*id)?;
        }
        Ok(())
    }

    /// Validate an entire edit while retaining an exclusive borrow until commit.
    ///
    /// # Errors
    /// Returns corruption for invalid names, unpaired flushes or invalid state transitions.
    pub fn prepare_edit(&mut self, edit: &ManifestEdit) -> MidgeResult<PreparedManifestEdit<'_>> {
        edit.validate_for_append()?;
        let mut validator = EditValidator::new(self);
        validator.apply(edit)?;
        let steps = validator.steps;
        Ok(PreparedManifestEdit {
            manifest: self,
            steps,
        })
    }

    /// Validate the entire outer batch before committing any of its children.
    ///
    /// # Errors
    /// Returns corruption without changing the manifest when any child is invalid.
    pub fn apply_edit(&mut self, edit: &ManifestEdit) -> MidgeResult<()> {
        self.prepare_edit(edit)?.commit();
        Ok(())
    }

    /// Record that the caller journaled `edit_id` and has already applied that
    /// edit to this in-memory manifest.
    ///
    /// The checkpoint horizon only advances when `edit_id` directly follows it.
    /// Journal ids are contiguous, so a gap means another writer appended an
    /// edit this manifest may not contain; advancing over it would make a later
    /// snapshot skip that edit and truncate it away. In that case the horizon
    /// stays put and the snapshot replays the journal from the older horizon.
    /// Returns whether the horizon advanced.
    pub fn note_applied_journal_edit(&mut self, edit_id: u64) -> bool {
        if self.edit_checkpoint_id.checked_add(1) == Some(edit_id) {
            self.edit_checkpoint_id = edit_id;
            true
        } else {
            false
        }
    }
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
    /// Serialized as a hex string.
    #[serde(default, with = "crate::metadata::key_bounds")]
    pub smallest_key: Option<Vec<u8>>,
    #[serde(default, with = "crate::metadata::key_bounds")]
    pub largest_key: Option<Vec<u8>>,
    #[serde(default)]
    pub smallest_seq: Option<u64>,
    #[serde(default)]
    pub largest_seq: Option<u64>,
    /// Whether the persisted key bounds include every point key and range
    /// tombstone endpoint in this SST.
    ///
    /// An entry without it decodes as `false`, and reads do not prune by its
    /// bounds.
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
    /// Compare the persisted identity fields shared with runtime messages.
    /// Scheduling metadata and read counts do not change SST content.
    #[must_use]
    pub(crate) fn same_identity(&self, other: &Self) -> bool {
        self.name == other.name
            && self.level == other.level
            && self.size_bytes == other.size_bytes
            && self.content_crc32c == other.content_crc32c
            && self.cf_id == other.cf_id
            && self.smallest_key == other.smallest_key
            && self.largest_key == other.largest_key
            && self.smallest_seq == other.smallest_seq
            && self.largest_seq == other.largest_seq
            && self.key_bounds_complete == other.key_bounds_complete
    }

    /// Borrow this entry's recorded proofs for the SST identity checker.
    pub(crate) fn expected_sst(&self) -> crate::types::ExpectedSst<'_> {
        crate::types::ExpectedSst {
            name: &self.name,
            size_bytes: self.size_bytes,
            content_crc32c: self.content_crc32c,
            smallest_key: self.smallest_key.as_deref(),
            largest_key: self.largest_key.as_deref(),
            smallest_seq: self.smallest_seq,
            largest_seq: self.largest_seq,
        }
    }
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

        self.apply_edit(&ManifestEdit::CreateColumnFamily {
            id,
            name,
            created_at,
        })
        .expect("valid test column family");

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
    #[cfg(test)]
    pub fn delete_column_family_with_reclamation(
        &mut self,
        cf_id: u32,
        drop_sequence: u64,
        dropped_sst_names: Vec<String>,
    ) -> bool {
        self.apply_edit(&ManifestEdit::DropColumnFamilyAt {
            id: cf_id,
            drop_sequence,
            dropped_sst_names,
        })
        .is_ok()
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
    #[cfg(test)]
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
}

#[cfg(test)]
mod tests;
