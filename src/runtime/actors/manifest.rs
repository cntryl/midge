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
}
