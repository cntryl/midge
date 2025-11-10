use crate::common::timestamp;
use crate::error::MidgeResult;

use super::types::{CloudCheckpoint, Manifest};

impl Manifest {
    /// Mark an SST file as uploaded to cloud storage.
    /// Updates the FileMeta with cloud location, checksum, and state.
    pub fn mark_sst_uploaded(
        &mut self,
        sst_name: &str,
        cloud_location: String,
        cloud_checksum: u64,
    ) -> MidgeResult<()> {
        let file = self
            .files
            .iter_mut()
            .find(|f| f.name == sst_name)
            .ok_or_else(|| {
                crate::error::MidgeError::internal(format!(
                    "SST not found in manifest: {}",
                    sst_name
                ))
            })?;

        file.cloud_location = Some(cloud_location);
        file.cloud_checksum = Some(cloud_checksum);
        file.cloud_uploaded_at = Some(timestamp::now());
        file.cloud_state = Some(crate::sst::cloud::SstLifecycleState::Active);
        Ok(())
    }

    /// Update the cloud checkpoint after verifying SSTs are uploaded.
    /// This allows WAL segments up to checkpoint_sequence to be pruned.
    pub fn update_cloud_checkpoint(
        &mut self,
        checkpoint_sequence: u64,
        covering_ssts: Vec<String>,
    ) -> MidgeResult<()> {
        self.cloud_checkpoint = Some(CloudCheckpoint {
            checkpoint_sequence,
            covering_ssts,
            checkpoint_time: timestamp::now(),
        });
        Ok(())
    }

    /// Get the current cloud checkpoint for WAL pruning decisions.
    pub fn get_cloud_checkpoint(&self) -> Option<&CloudCheckpoint> {
        self.cloud_checkpoint.as_ref()
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn should_create_cloud_checkpoint_with_all_ssts() {
        // Arrange
        let mut manifest = Manifest::default();
        assert!(manifest.get_cloud_checkpoint().is_none());

        // Act
        manifest
            .update_cloud_checkpoint(100, vec!["sst1.blob".to_string(), "sst2.blob".to_string()])
            .expect("update checkpoint");

        // Assert
        let loaded = manifest
            .get_cloud_checkpoint()
            .expect("checkpoint should exist");
        assert_eq!(loaded.checkpoint_sequence, 100);
        assert_eq!(loaded.covering_ssts.len(), 2);
    }

    #[test]
    fn should_mark_sst_as_uploaded_to_cloud() {
        use super::super::types::FileMeta;

        // Arrange
        let mut manifest = Manifest::default();
        manifest.files.push(FileMeta {
            name: "upload_test.sst".to_string(),
            level: 1,
            size_bytes: 1024,
            ..Default::default()
        });
        let cloud_path = "s3://bucket/realm/area/resource/sst/upload_test.sst".to_string();
        let checksum = 0x1234567890ABCDEF;

        // Act
        manifest
            .mark_sst_uploaded("upload_test.sst", cloud_path.clone(), checksum)
            .expect("mark uploaded");

        // Assert
        let file = manifest
            .get_file("upload_test.sst")
            .expect("file should exist");
        assert_eq!(file.cloud_location.as_ref().unwrap(), &cloud_path);
        assert_eq!(file.cloud_checksum.unwrap(), checksum);
        assert!(file.cloud_uploaded_at.is_some());
    }
}
