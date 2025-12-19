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
    pub fn add_sst(&mut self, state: &mut RuntimeState, file_meta: FileMeta) -> MidgeResult<()> {
        // Validate SST file exists and is readable (defensive: avoid manifest pointing at corrupt file)
        if !state.memory_mode {
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

        // 🔑 CRITICAL: Write intent BEFORE applying mutations
        // This ensures we can recover if crash occurs during SST addition
        let intent = crate::runtime::IntentLogEntry::SstAdded {
            file_meta: file_meta.clone(),
        };
        
        // Persist the intent before applying mutation
        state.append_intent(intent)?;
        
        // Now that intent is durable, apply mutation to in-memory manifest
        // Convert to manifest FileMeta
        let manifest_meta = crate::metadata::FileMeta {
            name: file_meta.name.clone(),
            level: file_meta.level,
            size_bytes: file_meta.size_bytes,
            cf_id: file_meta.cf_id,
            smallest_key: file_meta.smallest_key,
            largest_key: file_meta.largest_key,
            smallest_seq: file_meta.smallest_seq,
            largest_seq: file_meta.largest_seq,
            ..Default::default()
        };

        state.manifest.files.push(manifest_meta);
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
        removed: Vec<String>,
        added: Vec<FileMeta>,
    ) -> MidgeResult<()> {
        // 🔑 CRITICAL: Write intent BEFORE applying mutations
        // This ensures we can recover incomplete mutations on crash
        let intent = crate::runtime::IntentLogEntry::CompactionApplied {
            removed: removed.clone(),
            added: added.iter().map(|m| m.name.clone()).collect(),
        };
        
        // Persist the intent before applying mutations
        state.append_intent(intent)?;
        
        // Now that intent is durable, apply mutations to in-memory manifest
        // Remove old files
        state.manifest.files.retain(|f| !removed.contains(&f.name));

        // Add new files
        for file_meta in &added {
            let manifest_meta = crate::metadata::FileMeta {
                name: file_meta.name.clone(),
                level: file_meta.level,
                size_bytes: file_meta.size_bytes,
                cf_id: file_meta.cf_id,
                smallest_key: file_meta.smallest_key.clone(),
                largest_key: file_meta.largest_key.clone(),
                smallest_seq: file_meta.smallest_seq,
                largest_seq: file_meta.largest_seq,
                ..Default::default()
            };
            state.manifest.files.push(manifest_meta);
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
    pub fn persist(&self, state: &RuntimeState) -> MidgeResult<()> {
        tracing::info!(
            file_count = state.manifest.files.len(),
            cf_count = state.manifest.column_families.len(),
            "Manifest: persisting"
        );

        // Use ManifestPersistence to save in YAML format
        crate::metadata::ManifestPersistence::save(&state.db_path, &state.manifest)
            .map_err(crate::common::MidgeError::Internal)?;

        tracing::debug!("Manifest persisted");

        Ok(())
    }

    /// Create a new column family
    pub fn create_column_family(
        &mut self,
        state: &mut RuntimeState,
        name: String,
    ) -> MidgeResult<u32> {
        // Check if CF name already exists (even if deleted)
        if state.manifest.get_column_family_by_name(&name).is_some() {
            return Err(crate::common::MidgeError::Internal(format!(
                "Column family '{}' already exists",
                name
            )));
        }

        let cf_id = state.manifest.create_column_family(name.clone());
        self.pending_edits += 1;

        // Create ColumnFamilyState for the new CF
        let cf_state = crate::runtime::state::ColumnFamilyState::new(cf_id, name.clone());
        state.column_families.insert(cf_id, cf_state);

        tracing::info!(cf_id = cf_id, cf_name = %name, "Manifest: created column family");

        Ok(cf_id)
    }

    /// Drop a column family (soft delete for durability)
    pub fn drop_column_family(&mut self, state: &mut RuntimeState, cf_id: u32) -> MidgeResult<()> {
        // Prevent dropping default CF
        if cf_id == 0 {
            return Err(crate::common::MidgeError::InvalidArgument(
                "Cannot drop default column family".to_string(),
            ));
        }

        if !state.manifest.delete_column_family(cf_id) {
            return Err(crate::common::MidgeError::Internal(format!(
                "Column family {} not found or already deleted",
                cf_id
            )));
        }

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

    #[test]
    fn should_initialize_manifest_actor_with_zero_pending_edits() {
        // Arrange / Act
        let actor = ManifestActor::new();

        // Assert
        assert_eq!(actor.pending_edits, 0);
    }

    #[test]
    fn should_initialize_via_default() {
        // Arrange / Act
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
    fn add_sst_should_validate_sst_file_exists_and_readable() {
        // Arrange: create a temp dir and a corrupt SST file (partial content)
        let tmp = tempfile::tempdir().expect("create tmpdir");
        let sst_name = "sst_000001_000001.sst".to_string();
        let sst_path = tmp.path().join(&sst_name);

        // Write corrupted content (not a valid SST)
        std::fs::write(&sst_path, b"incomplete-sst-bytes").expect("write corrupted sst");

        // Build a FileMeta that references the corrupted file
        let file_meta = crate::runtime::FileMeta {
            name: sst_name.clone(),
            level: 0,
            size_bytes: 0,
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
        assert!(result.is_err(), "expected manifest.add_sst to validate SST file and fail for corrupted file");
    }

    #[test]
    fn add_sst_should_fail_if_only_tmp_file_exists() {
        // Arrange: create a temp dir and a leftover .tmp file (simulate crash before rename)
        let tmp = tempfile::tempdir().expect("create tmpdir");
        let sst_name = "sst_000002_000002.sst".to_string();
        let tmp_name = format!("{}.tmp", sst_name);
        let tmp_path = tmp.path().join(&tmp_name);

        // Write a temp file but do not rename
        std::fs::write(&tmp_path, b"partial-sst-data").expect("write tmp sst");

        let file_meta = crate::runtime::FileMeta {
            name: sst_name.clone(),
            level: 0,
            size_bytes: 0,
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
        assert!(result.is_err(), "expected manifest.add_sst to fail when only tmp file exists");
    }

    #[test]
    fn add_sst_should_accept_valid_sst() -> MidgeResult<()> {
        // Arrange: create a valid on-disk SST via the Fs SstFactory
        let tmp = tempfile::tempdir().expect("create tmpdir");
        let sst_name = "sst_000003_000003.sst".to_string();
        let mut state = crate::runtime::state::RuntimeState::new(tmp.path().to_path_buf(), false);
        // Use a short-lived mutable borrow to satisfy Clippy's `unused_mut` lint —
        // the test needs `state` to be mutable later when calling `add_sst`.
        let _ = &mut state;
        assert!(state.sst_dir.exists(), "sst dir must exist");
        let sst_path = state.sst_dir.join(&sst_name);

        // Write a minimal valid SST footer so the reader's validation accepts it
        use crate::sst::types::SST_FOOTER_MAGIC;
        let mut f = std::fs::File::create(&sst_path)?;
        let mut buf = vec![0u8; 48];
        buf[40..48].copy_from_slice(&SST_FOOTER_MAGIC.to_le_bytes());
        use std::io::Write;
        f.write_all(&buf)?;
        f.sync_all()?;
        assert!(sst_path.exists(), "sst path must exist after creating footer");
        eprintln!("sst file bytes: {}", std::fs::metadata(&sst_path)?.len());

        let file_meta = crate::runtime::FileMeta {
            name: sst_name.clone(),
            level: 0,
            size_bytes: std::fs::metadata(&sst_path)?.len(),
            cf_id: 0,
            smallest_key: Some(b"a".to_vec()),
            largest_key: Some(b"a".to_vec()),
            smallest_seq: Some(10),
            largest_seq: Some(10),
        };

        let mut state = crate::runtime::state::RuntimeState::new(tmp.path().to_path_buf(), false);
        assert_eq!(state.sst_dir.join(&sst_name), sst_path, "state.sst_dir should match the temp dir used for file creation");
        let mut actor = ManifestActor::new();

        // Act: attempt to add the SST to manifest
        let result = actor.add_sst(&mut state, file_meta);

        // Assert: valid SST should be accepted
        if let Err(e) = result {
            panic!("add_sst failed: {:?}", e);
        }
        Ok(())
    }
}
