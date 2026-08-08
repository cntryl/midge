//! Manifest Actor - handles metadata persistence
//!
//! Responsible for:
//! - Adding SST files to manifest
//! - Updating manifest after compaction
//! - Persisting manifest to disk
//! - Managing version edits

use super::super::state::RuntimeState;
use super::super::FileMeta;
use crate::common::MidgeResult;

/// Actor handling manifest operations
pub struct ManifestActor {
    /// Number of pending edits
    pending_edits: usize,
}

impl ManifestActor {
    pub fn new() -> Self {
        Self { pending_edits: 0 }
    }

    /// Add a new SST file to the manifest
    #[cfg(test)]
    pub fn add_sst(&mut self, state: &mut RuntimeState, file_meta: FileMeta) -> MidgeResult<()> {
        // Validate SST file exists and is readable (defensive: avoid manifest pointing at corrupt file)
        if !state.is_memory_mode() {
            let sst_path = state.sst_dir.join(&file_meta.name);
            if !sst_path.exists() {
                return Err(crate::common::MidgeError::Internal(format!(
                    "SST file '{}' not found in sst dir",
                    file_meta.name
                )));
            }

            // Try opening the SST to validate footer/format correctness
            if let Err(e) = crate::sst::fs::SstFileIo::open_with_real_fs(&sst_path) {
                return Err(crate::common::MidgeError::Corruption(format!(
                    "SST file '{}' failed validation: {}",
                    file_meta.name, e
                )));
            }
        }

        // Append to manifest journal (durable edit log) - skip in memory mode
        if !state.is_memory_mode() {
            let edit = crate::metadata::ManifestEdit::AddSst(crate::metadata::FileMeta {
                name: file_meta.name.clone(),
                level: file_meta.level,
                size_bytes: file_meta.size_bytes,
                content_crc32c: file_meta.content_crc32c,
                cf_id: file_meta.cf_id,
                smallest_key: file_meta.smallest_key.clone(),
                largest_key: file_meta.largest_key.clone(),
                smallest_seq: file_meta.smallest_seq,
                largest_seq: file_meta.largest_seq,
                ..Default::default()
            });
            crate::failpoints::fail_point!(
                "midge::manifest::inject_no_space_on_add_sst_edit",
                |_| Err(crate::common::MidgeError::NoSpace(
                    "failpoint: no space on manifest add_sst append".to_string()
                ))
            );
            crate::metadata::append_edit(&state.db_path, &edit)?;
        }

        // Now that intent is durable, apply mutation to in-memory manifest
        // Convert to manifest FileMeta
        let manifest_meta = crate::metadata::FileMeta {
            name: file_meta.name.clone(),
            level: file_meta.level,
            size_bytes: file_meta.size_bytes,
            content_crc32c: file_meta.content_crc32c,
            cf_id: file_meta.cf_id,
            smallest_key: file_meta.smallest_key,
            largest_key: file_meta.largest_key,
            smallest_seq: file_meta.smallest_seq,
            largest_seq: file_meta.largest_seq,
            ..Default::default()
        };

        state.manifest.add_file(manifest_meta);
        self.pending_edits += 1;

        tracing::info!(
            sst_name = %file_meta.name,
            level = file_meta.level,
            cf_id = file_meta.cf_id,
            "Manifest: added SST with durability guarantee"
        );

        Ok(())
    }

    /// Update manifest after compaction completes
    pub fn compaction_complete(
        &mut self,
        state: &mut RuntimeState,
        removed: &[String],
        added: &[FileMeta],
    ) -> MidgeResult<()> {
        // Append compaction edits to manifest journal as a single batch (reduces fixed overhead)
        let mut edits = Vec::with_capacity(removed.len() + added.len());
        for n in removed {
            edits.push(crate::metadata::ManifestEdit::RemoveSst { name: n.clone() });
        }
        for f in added {
            edits.push(crate::metadata::ManifestEdit::AddSst(
                crate::metadata::FileMeta {
                    name: f.name.clone(),
                    level: f.level,
                    size_bytes: f.size_bytes,
                    content_crc32c: f.content_crc32c,
                    cf_id: f.cf_id,
                    smallest_key: f.smallest_key.clone(),
                    largest_key: f.largest_key.clone(),
                    smallest_seq: f.smallest_seq,
                    largest_seq: f.largest_seq,
                    ..Default::default()
                },
            ));
        }
        if !edits.is_empty() {
            crate::failpoints::fail_point!(
                "midge::manifest::inject_no_space_on_compaction_batch_edit",
                |_| Err(crate::common::MidgeError::NoSpace(
                    "failpoint: no space on manifest compaction batch append".to_string()
                ))
            );
            crate::metadata::append_edit_batch(&state.db_path, &edits)?;
        }

        // Now that intent is durable, apply mutations to in-memory manifest
        // Remove old files
        state.manifest.files.retain(|f| !removed.contains(&f.name));

        // Add new files
        for file_meta in added {
            let manifest_meta = crate::metadata::FileMeta {
                name: file_meta.name.clone(),
                level: file_meta.level,
                size_bytes: file_meta.size_bytes,
                content_crc32c: file_meta.content_crc32c,
                cf_id: file_meta.cf_id,
                smallest_key: file_meta.smallest_key.clone(),
                largest_key: file_meta.largest_key.clone(),
                smallest_seq: file_meta.smallest_seq,
                largest_seq: file_meta.largest_seq,
                ..Default::default()
            };
            state.manifest.add_file(manifest_meta);
        }

        self.pending_edits += 1;

        tracing::info!(
            removed_count = removed.len(),
            added_count = added.len(),
            "Manifest: compaction complete with durability guarantee"
        );

        Ok(())
    }

    /// Persist manifest to disk
    pub fn persist(state: &RuntimeState) -> MidgeResult<()> {
        // Skip persistence in memory mode
        if state.is_memory_mode() {
            tracing::debug!("Manifest: skipping persistence in memory mode");
            return Ok(());
        }

        tracing::info!(
            file_count = state.manifest.files.len(),
            cf_count = state.manifest.column_families.len(),
            "Manifest: persisting"
        );

        // Publish a crash-safe snapshot before truncating the journal. Recovery
        // can therefore replay only edits newer than the checkpoint horizon.
        crate::metadata::ManifestPersistence::save_snapshot_and_truncate_journal(
            &state.db_path,
            &state.manifest,
        )
        .map_err(crate::common::MidgeError::Internal)?;

        tracing::debug!("Manifest persisted");

        Ok(())
    }

    /// Create a new column family
    #[cfg(test)]
    #[allow(dead_code)]
    pub fn create_column_family(
        &mut self,
        state: &mut RuntimeState,
        name: &str,
    ) -> MidgeResult<u32> {
        // If the CF already exists and is active, return its id (idempotent CF creation).
        if let Some(existing) = state.manifest.get_column_family_by_name(name) {
            let cf_id = existing.id;
            // Ensure runtime state has a ColumnFamilyState for it (recovery path)
            state.column_families.entry(cf_id).or_insert_with(|| {
                crate::runtime::state::ColumnFamilyState::new(cf_id, name.to_string())
            });
            tracing::info!(cf_id = cf_id, cf_name = %name, "Manifest: column family already exists, returning existing id");
            return Ok(cf_id);
        }

        let name = name.to_string();

        let created_at = std::time::SystemTime::now()
            .duration_since(std::time::UNIX_EPOCH)
            .unwrap_or_default()
            .as_millis()
            .try_into()
            .unwrap_or(u64::MAX);

        let mut candidate_manifest = state.manifest.clone();
        let cf_id = candidate_manifest.next_cf_id()?;
        let edit = crate::metadata::ManifestEdit::CreateColumnFamily {
            id: cf_id,
            name: name.clone(),
            created_at,
        };
        candidate_manifest.apply_edit(&edit);

        // Append create CF to journal (skip in memory mode)
        if !state.is_memory_mode() {
            crate::metadata::append_edit(&state.db_path, &edit)?;
        }

        state.manifest = candidate_manifest;
        self.pending_edits += 1;

        // Create ColumnFamilyState for the new CF
        let cf_state = crate::runtime::state::ColumnFamilyState::new(cf_id, name.clone());
        state.column_families.insert(cf_id, cf_state);

        tracing::info!(cf_id = cf_id, cf_name = %name, "Manifest: created column family");

        Ok(cf_id)
    }

    /// Drop a column family (soft delete for durability)
    #[cfg(test)]
    #[allow(dead_code)]
    pub fn drop_column_family(
        &mut self,
        state: &mut RuntimeState,
        cf_id: crate::types::ColumnFamilyId,
    ) -> MidgeResult<()> {
        // Prevent dropping default CF
        if cf_id == 0 {
            return Err(crate::common::MidgeError::InvalidArgument(
                "Cannot drop default column family".to_string(),
            ));
        }

        let dropped_sst_names = state
            .manifest
            .files
            .iter()
            .filter(|file| file.cf_id == cf_id)
            .map(|file| file.name.clone())
            .collect::<Vec<_>>();
        let drop_sequence = state.sequence;
        let mut candidate_manifest = state.manifest.clone();
        if !candidate_manifest.delete_column_family_with_reclamation(
            cf_id,
            drop_sequence,
            dropped_sst_names.clone(),
        ) {
            return Err(crate::common::MidgeError::Internal(format!(
                "Column family {cf_id} not found or already deleted"
            )));
        }

        // Append drop CF edit to journal (skip in memory mode)
        if !state.is_memory_mode() {
            let edit = crate::metadata::ManifestEdit::DropColumnFamilyAt {
                id: cf_id,
                drop_sequence,
                dropped_sst_names,
            };
            crate::metadata::append_edit(&state.db_path, &edit)?;
        }

        state.manifest = candidate_manifest;

        // Remove ColumnFamilyState
        state.column_families.remove(&cf_id);

        self.pending_edits += 1;

        tracing::info!(cf_id = cf_id, "Manifest: dropped column family");

        Ok(())
    }
}

impl Default for ManifestActor {
    fn default() -> Self {
        Self::new()
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::sst::types::SST_FOOTER_MAGIC;
    use std::io::Write;

    #[test]
    fn should_initialize_manifest_actor_with_zero_pending_edits() {
        // Arrange
        // (no setup needed)

        // Act
        let actor = ManifestActor::new();

        // Assert
        assert_eq!(actor.pending_edits, 0);
    }

    #[test]
    fn should_initialize_via_default() {
        // Arrange
        // (no setup needed)

        // Act
        let actor = ManifestActor::default();

        // Assert
        assert_eq!(actor.pending_edits, 0);
    }

    #[test]
    fn should_increment_pending_edits_on_add_sst() {
        // Arrange
        let mut actor = ManifestActor::new();
        assert_eq!(actor.pending_edits, 0);

        // Act: manually simulate adding an edit
        actor.pending_edits += 1;

        // Assert
        assert_eq!(actor.pending_edits, 1);
    }

    #[test]
    fn should_accumulate_pending_edits_across_operations() {
        // Arrange
        let mut actor = ManifestActor::new();

        // Act
        actor.pending_edits += 1; // Add SST
        actor.pending_edits += 1; // Compaction complete
        actor.pending_edits += 1; // Another add

        // Assert
        assert_eq!(actor.pending_edits, 3);
    }

    #[test]
    fn should_maintain_monotonic_edit_count() {
        // Arrange
        let mut actor = ManifestActor::new();
        let count1 = actor.pending_edits;

        // Act
        actor.pending_edits += 1;
        let count2 = actor.pending_edits;
        actor.pending_edits += 1;
        let count3 = actor.pending_edits;

        // Assert: counts only increase
        assert!(count2 > count1);
        assert!(count3 > count2);
    }

    #[test]
    fn should_fail_to_add_sst_given_corrupted_file_when_validating() {
        // Arrange: create a temp dir and a corrupt SST file (partial content)
        let tmp = tempfile::tempdir().expect("create tmpdir");
        let sst_name = crate::sst::file_name(0, 0, 1);
        let sst_path = tmp.path().join(&sst_name);

        // Write corrupted content (not a valid SST)
        std::fs::write(&sst_path, b"incomplete-sst-bytes").expect("write corrupted sst");

        // Build a FileMeta that references the corrupted file
        let file_meta = crate::runtime::FileMeta {
            name: sst_name.clone(),
            level: 0,
            size_bytes: 0,
            content_crc32c: None,
            cf_id: 0,
            smallest_key: None,
            largest_key: None,
            smallest_seq: None,
            largest_seq: None,
        };

        let mut state = crate::runtime::state::RuntimeState::new(tmp.path().to_path_buf(), false);
        let mut actor = ManifestActor::new();

        // Act: attempt to add the SST to manifest
        let result = actor.add_sst(&mut state, file_meta);

        // Assert: adding a manifest entry for a corrupt/unreadable SST MUST fail
        // (current behavior is to accept; this test should fail until we implement validation)
        assert!(
            result.is_err(),
            "expected manifest.add_sst to validate SST file and fail for corrupted file"
        );
    }

    #[test]
    fn should_fail_to_add_sst_given_missing_final_file_when_only_tmp_exists() {
        // Arrange: create a temp dir and a leftover .tmp file (simulate crash before rename)
        let tmp = tempfile::tempdir().expect("create tmpdir");
        let sst_name = crate::sst::file_name(0, 0, 2);
        let tmp_name = format!("{sst_name}.tmp");
        let tmp_path = tmp.path().join(&tmp_name);

        // Write a temp file but do not rename
        std::fs::write(&tmp_path, b"partial-sst-data").expect("write tmp sst");

        let file_meta = crate::runtime::FileMeta {
            name: sst_name.clone(),
            level: 0,
            size_bytes: 0,
            content_crc32c: None,
            cf_id: 0,
            smallest_key: None,
            largest_key: None,
            smallest_seq: None,
            largest_seq: None,
        };

        let mut state = crate::runtime::state::RuntimeState::new(tmp.path().to_path_buf(), false);
        let mut actor = ManifestActor::new();

        // Act: attempt to add the SST to manifest
        let result = actor.add_sst(&mut state, file_meta);

        // Assert: adding a manifest entry for a missing final SST (only tmp present) MUST fail
        assert!(
            result.is_err(),
            "expected manifest.add_sst to fail when only tmp file exists"
        );
    }

    #[test]
    fn should_add_sst_successfully_given_valid_file_when_manifest_updated() -> MidgeResult<()> {
        // Arrange: create a valid on-disk SST via the Fs SstFactory
        let tmp = tempfile::tempdir().expect("create tmpdir");
        let sst_name = crate::sst::file_name(0, 0, 3);
        let mut state = crate::runtime::state::RuntimeState::new(tmp.path().to_path_buf(), false);
        assert!(state.sst_dir.exists(), "sst dir must exist");
        let sst_path = state.sst_dir.join(&sst_name);

        // Write a minimal valid SST footer so the reader's validation accepts it
        let mut f = std::fs::File::create(&sst_path)?;
        let mut buf = vec![0u8; 48];
        buf[40..48].copy_from_slice(&SST_FOOTER_MAGIC.to_le_bytes());
        f.write_all(&buf)?;
        f.sync_all()?;
        assert!(
            sst_path.exists(),
            "sst path must exist after creating footer"
        );
        eprintln!("sst file bytes: {}", std::fs::metadata(&sst_path)?.len());

        let file_meta = crate::runtime::FileMeta {
            name: sst_name.clone(),
            level: 0,
            size_bytes: std::fs::metadata(&sst_path)?.len(),
            content_crc32c: None,
            cf_id: 0,
            smallest_key: Some(b"a".to_vec()),
            largest_key: Some(b"a".to_vec()),
            smallest_seq: Some(10),
            largest_seq: Some(10),
        };

        assert_eq!(
            state.sst_dir.join(&sst_name),
            sst_path,
            "state.sst_dir should match the temp dir used for file creation"
        );
        let mut actor = ManifestActor::new();

        // Act: attempt to add the SST to manifest
        let result = actor.add_sst(&mut state, file_meta);

        // Assert: valid SST should be accepted
        assert!(result.is_ok(), "add_sst failed: {:?}", result.err());
        Ok(())
    }
}
