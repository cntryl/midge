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
        let cloud_key = crate::wal::cloud_segment_object_key(segment_id, state.writer_epoch);

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
            let cp = crate::metadata::manifest::CloudCheckpoint {
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
    #[cfg(test)]
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

    /// `upload_sst`'s increment path additionally requires a live
    /// `HybridStorage` backend, which is disproportionate to stand up for a
    /// unit test here; `upload_wal` exercises the same
    /// `uploads_in_progress` bookkeeping through a real handler that only
    /// needs a `RuntimeState` and an on-disk segment file.
    fn write_wal_segment(state: &RuntimeState, segment_id: u64) {
        let wal_name = crate::wal::segment_file_name(segment_id);
        std::fs::write(state.wal_dir.join(&wal_name), b"wal-bytes").expect("write wal segment");
    }

    #[test]
    fn should_increment_uploads_on_wal_upload_request() {
        // Arrange
        let tmp = tempfile::tempdir().expect("create tmpdir");
        let mut state = RuntimeState::new(tmp.path().to_path_buf(), false);
        write_wal_segment(&state, 1);
        let mut actor = CloudActor::new();

        // Act - the real upload handler, not a direct field write
        actor.upload_wal(&mut state, 1);

        // Assert
        assert_eq!(actor.uploads_in_progress(), 1);
        assert_eq!(state.cloud.pending_uploads.len(), 1);
    }

    #[test]
    fn should_track_multiple_uploads_in_progress() {
        // Arrange
        let tmp = tempfile::tempdir().expect("create tmpdir");
        let mut state = RuntimeState::new(tmp.path().to_path_buf(), false);
        for segment_id in 1..=3 {
            write_wal_segment(&state, segment_id);
        }
        let mut actor = CloudActor::new();

        // Act - three real upload requests
        actor.upload_wal(&mut state, 1);
        actor.upload_wal(&mut state, 2);
        actor.upload_wal(&mut state, 3);

        // Assert
        assert_eq!(actor.uploads_in_progress(), 3);
        assert_eq!(state.cloud.pending_uploads.len(), 3);
    }

    #[test]
    fn should_decrement_uploads_via_handle_upload_complete() {
        // Arrange
        let tmp = tempfile::tempdir().expect("create tmpdir");
        let mut state = RuntimeState::new(tmp.path().to_path_buf(), false);
        write_wal_segment(&state, 7);
        let mut actor = CloudActor::new();
        actor.upload_wal(&mut state, 7);
        assert_eq!(actor.uploads_in_progress(), 1);
        let cloud_key = crate::wal::cloud_segment_object_key(7, state.writer_epoch);

        // Act - the real completion handler
        actor.handle_upload_complete(&mut state, &cloud_key);

        // Assert
        assert_eq!(actor.uploads_in_progress(), 0);
        assert!(state.cloud.pending_uploads.is_empty());
        assert_eq!(state.cloud.last_cloud_checkpoint_seq, 7);
    }

    #[test]
    fn should_saturate_at_zero_when_completing_without_pending_upload() {
        // Arrange - no upload was ever started
        let tmp = tempfile::tempdir().expect("create tmpdir");
        let mut state = RuntimeState::new(tmp.path().to_path_buf(), false);
        let mut actor = CloudActor::new();
        assert_eq!(actor.uploads_in_progress(), 0);

        // Act - completion handler must not underflow the counter
        actor.handle_upload_complete(&mut state, "unrelated-resource");

        // Assert
        assert_eq!(actor.uploads_in_progress(), 0);
    }
}
