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
            "Manifest: added SST"
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
        // Remove old files
        state.manifest.files.retain(|f| !removed.contains(&f.name));

        // Add new files
        for file_meta in added {
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
        }

        self.pending_edits += 1;

        tracing::info!(
            removed_count = removed.len(),
            "Manifest: compaction complete"
        );

        Ok(())
    }

    /// Persist manifest to disk
    pub fn persist(&self, state: &RuntimeState) -> MidgeResult<()> {
        let manifest_path = state.db_path.join("MANIFEST");

        tracing::info!(
            path = %manifest_path.display(),
            file_count = state.manifest.files.len(),
            "Manifest: persisting"
        );

        // Serialize manifest to JSON
        let json = serde_json::to_string_pretty(&state.manifest).map_err(|e| {
            crate::common::MidgeError::Internal(format!("Failed to serialize manifest: {}", e))
        })?;

        // Write atomically via temp file
        let temp_path = manifest_path.with_extension("tmp");
        std::fs::write(&temp_path, &json)?;
        std::fs::rename(&temp_path, &manifest_path)?;

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

        tracing::info!(cf_id = cf_id, cf_name = %name, "Manifest: created column family");

        Ok(cf_id)
    }

    /// Drop a column family (soft delete for durability)
    pub fn drop_column_family(&mut self, state: &mut RuntimeState, cf_id: u32) -> MidgeResult<()> {
        if !state.manifest.delete_column_family(cf_id) {
            return Err(crate::common::MidgeError::Internal(format!(
                "Column family {} not found or already deleted",
                cf_id
            )));
        }

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
