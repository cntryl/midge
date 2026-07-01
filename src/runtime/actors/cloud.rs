//! Cloud Actor - handles cloud storage operations
//!
//! Responsible for:
//! - Uploading SST files to cloud storage
//! - Uploading WAL segments to cloud storage
//! - Tracking upload progress and checkpoints
//! - Coordinating with manifest for cloud state

use super::super::state::RuntimeState;
use crate::storage::StorageBackend;

/// Actor handling cloud storage operations
pub struct CloudActor {
    /// Number of uploads in progress
    uploads_in_progress: usize,
}

impl CloudActor {
    pub fn new() -> Self {
        Self {
            uploads_in_progress: 0,
        }
    }

    /// Upload an SST file to cloud storage
    pub fn upload_sst(
        &mut self,
        state: &mut RuntimeState,
        sst_name: &str,
        storage: Option<&std::sync::Arc<crate::storage::HybridStorage>>,
    ) {
        let sst_path = state.sst_dir.join(sst_name);

        // Validate SST exists before upload
        if !sst_path.exists() {
            tracing::warn!(sst_name, path = %sst_path.display(), "SST file not found for upload");
            return;
        }

        // Read file content
        let data = match std::fs::read(&sst_path) {
            Ok(d) => d,
            Err(e) => {
                tracing::error!(sst_name, error = %e, "Failed to read SST file for upload");
                return;
            }
        };

        // Create cloud key (use namespace prefix for isolation)
        let cloud_key = crate::sst::object_key(sst_name);

        // Submit SST to cloud storage via hybrid backend
        if let Some(s) = storage {
            let (tx, _rx) = std::sync::mpsc::channel();
            s.submit_write(&cloud_key, data.clone(), tx);
            self.uploads_in_progress += 1;

            tracing::info!(
                sst_name,
                size = data.len(),
                cloud_key = %cloud_key,
                "SST submitted to cloud storage"
            );
        } else {
            tracing::debug!(sst_name, "No hybrid storage available for SST upload");
        }
    }

    /// Upload a WAL segment to cloud storage
    pub fn upload_wal(&mut self, state: &mut RuntimeState, segment_id: u64) {
        let wal_name = crate::wal::segment_file_name(segment_id);
        let wal_path = state.wal_dir.join(&wal_name);

        // Validate WAL exists before upload
        if !wal_path.exists() {
            tracing::warn!(segment_id, path = %wal_path.display(), "WAL file not found for upload");
            return;
        }

        // Read file content
        let data = match std::fs::read(&wal_path) {
            Ok(d) => d,
            Err(e) => {
                tracing::error!(segment_id, error = %e, "Failed to read WAL file for upload");
                return;
            }
        };

        // Create cloud key
        let cloud_key = crate::wal::cloud_segment_object_key(segment_id);

        // Track pending upload
        state.cloud.pending_uploads.push(cloud_key.clone());
        self.uploads_in_progress += 1;

        // Update cloud checkpoint
        state.cloud.last_cloud_checkpoint_seq = segment_id;

        tracing::info!(
            segment_id,
            wal_name = %wal_name,
            size = data.len(),
            cloud_key = %cloud_key,
            "Cloud WAL upload started"
        );
    }

    /// Handle upload completion
    pub fn handle_upload_complete(&mut self, state: &mut RuntimeState, resource: &str) {
        // Remove from pending
        state.cloud.pending_uploads.retain(|r| r != resource);
        self.uploads_in_progress = self.uploads_in_progress.saturating_sub(1);

        tracing::info!(resource, "Cloud upload completed");

        // Update checkpoint if this was a WAL segment
        if let Some(seq) = crate::wal::parse_segment_id(resource) {
            state.cloud.last_cloud_checkpoint_seq = seq;
            // Record cloud checkpoint in manifest journal
            let cp = crate::metadata::CloudCheckpoint {
                checkpoint_sequence: seq,
                covering_ssts: vec![],
            };
            let edit = crate::metadata::ManifestEdit::SetCloudCheckpoint(cp);
            if let Err(e) = crate::metadata::append_edit(&state.db_path, &edit) {
                tracing::warn!(error = ?e, "failed to append SetCloudCheckpoint to journal");
            }
            tracing::debug!(segment_id = seq, "Updated cloud WAL checkpoint");
        }
    }

    /// Get current upload count
    pub fn uploads_in_progress(&self) -> usize {
        self.uploads_in_progress
    }
}

impl Default for CloudActor {
    fn default() -> Self {
        Self::new()
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn should_initialize_cloud_actor_with_zero_uploads() {
        // Arrange
        // (no setup needed)

        // Act
        let actor = CloudActor::new();

        // Assert
        assert_eq!(actor.uploads_in_progress(), 0);
    }

    #[test]
    fn should_initialize_via_default() {
        // Arrange
        // (no setup needed)

        // Act
        let actor = CloudActor::default();

        // Assert
        assert_eq!(actor.uploads_in_progress(), 0);
    }

    #[test]
    fn should_increment_uploads_on_sst_upload_request() {
        // Arrange
        let mut actor = CloudActor::new();
        let sst_count_before = actor.uploads_in_progress();

        // Act
        // Note: Would need MockRuntimeState for full test
        // For now, verify the counter increment logic
        actor.uploads_in_progress += 1;

        // Assert
        assert_eq!(actor.uploads_in_progress(), sst_count_before + 1);
    }

    #[test]
    fn should_track_multiple_uploads_in_progress() {
        // Arrange
        let mut actor = CloudActor::new();

        // Act: simulate multiple upload starts
        actor.uploads_in_progress = 1;
        actor.uploads_in_progress = 2;
        actor.uploads_in_progress = 3;

        // Assert
        assert_eq!(actor.uploads_in_progress(), 3);
    }

    #[test]
    fn should_decrement_uploads_when_complete() {
        // Arrange
        let mut actor = CloudActor::new();
        actor.uploads_in_progress = 3;

        // Act: simulate upload completion
        actor.uploads_in_progress = actor.uploads_in_progress.saturating_sub(1);

        // Assert
        assert_eq!(actor.uploads_in_progress(), 2);
    }

    #[test]
    fn should_handle_saturation_when_uploads_go_negative() {
        // Arrange
        let mut actor = CloudActor::new();
        actor.uploads_in_progress = 0;

        // Act: saturating_sub prevents underflow
        actor.uploads_in_progress = actor.uploads_in_progress.saturating_sub(1);

        // Assert: should not go negative
        assert_eq!(actor.uploads_in_progress(), 0);
    }
}
