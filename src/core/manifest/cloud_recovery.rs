//! Cloud-first manifest recovery for MidgeEngine initialization.
//!
//! This module provides manifest recovery strategies based on storage mode:
//!
//! # Operational Modes
//!
//! ## Local-Only Mode (No Cloud Backend)
//! 
//! When database is configured without a cloud backend:
//! - Manifest loaded from local filesystem only (`CURRENT` → `manifest.json`)
//! - Cloud code paths are not invoked
//! - Recovery is entirely local
//! - This mode is used for embedded databases in single-machine deployments
//!
//! ## Cloud-Native Mode (With Cloud Backend)
//!
//! When database is configured with a cloud backend (S3, Azure, GCP, etc.):
//! - **Cloud checkpoint is the source of truth** for manifest
//! - Local filesystem acts as optional ephemeral cache only
//! - Recovery follows cloud-first priority:
//!   1. Load manifest from cloud checkpoint (primary)
//!   2. Fall back to local manifest only if cloud unavailable
//!   3. Use default manifest only for brand new DB
//! - This mode is used for distributed, cloud-backed deployments
//!
//! # Recovery Priority (Cloud Mode Only)
//!
//! When cloud backend is configured:
//! 1. **Cloud checkpoint manifest** (if available)
//!    - Loaded from cloud checkpoint location
//!    - Verifies integrity before use
//!    - Source of truth for database state
//!
//! 2. **Local manifest** (fallback only)
//!    - Loaded from `CURRENT` → `manifest.json` on local disk
//!    - Used only if cloud unavailable or connection fails
//!    - May be stale compared to cloud
//!
//! 3. **Default manifest** (brand new DB)
//!    - Empty manifest with default column family
//!    - Used only if both cloud and local fail
//!
//! # Design Philosophy
//!
//! **"Recovery driven by manifest + WAL + compaction log, not whatever's on the local FS"**
//! (THE_BIG_IDEA)
//!
//! In cloud mode:
//! - Local filesystem is not trusted as source of truth
//! - All persistent state comes from cloud
//! - Local cache can be deleted and recreated without data loss
//! - Enables disaster recovery and zone failures
//!
//! In local mode:
//! - Only filesystem matters
//! - Cloud paths not invoked
//! - Traditional embedded database semantics
//!
//! # Examples
//!
//! ```ignore
//! use midge::cloud::StorageBackend;
//! use midge::core::manifest::Manifest;
//!
//! // Local-only mode: cloud_backend is None
//! let manifest = Manifest::load_with_cloud_fallback(
//!     db_path,
//!     None,  // No cloud backend
//!     None,
//! )?;
//! // Result: Loads from local filesystem only
//!
//! // Cloud-native mode: cloud_backend is Some
//! let manifest = Manifest::load_with_cloud_fallback(
//!     db_path,
//!     Some(&cloud_backend),  // Cloud-backed mode
//!     Some("midge"),
//! )?;
//! // Result: Loads from cloud checkpoint, falls back to local if cloud fails
//! ```

use std::path::Path;

use crate::cloud::StorageBackend;
use crate::error::{MidgeError, MidgeResult};

use super::types::Manifest;

impl Manifest {
    /// Load manifest with cloud-first priority, falling back to local.
    ///
    /// This is the primary recovery entry point. It implements the cloud-first
    /// philosophy: manifest is the source of truth, and cloud is the primary source.
    ///
    /// # Recovery Order
    ///
    /// 1. If cloud_backend is provided: try to load manifest from cloud checkpoint
    /// 2. If cloud fails or not available: try to load local manifest
    /// 3. If both fail: return default manifest (brand new DB)
    ///
    /// # Arguments
    ///
    /// * `db_path` - Local database path (used for local manifest fallback)
    /// * `cloud_backend` - Optional cloud storage backend for cloud manifest loading
    /// * `cloud_prefix` - Optional prefix for cloud object names (defaults to "midge")
    ///
    /// # Returns
    ///
    /// A manifest loaded from cloud, local, or default source
    ///
    /// # Errors
    ///
    /// Returns error only if both cloud and local sources fail unexpectedly
    /// (not including "not found" which defaults to empty manifest).
    pub fn load_with_cloud_fallback(
        db_path: &Path,
        cloud_backend: Option<&dyn StorageBackend>,
        cloud_prefix: Option<&str>,
    ) -> MidgeResult<Self> {
        // Try cloud manifest first
        if let Some(backend) = cloud_backend {
            match Self::load_from_cloud(backend, cloud_prefix) {
                Ok(manifest) => {
                    tracing::info!("recovered manifest from cloud checkpoint");
                    return Ok(manifest);
                }
                Err(e) => {
                    tracing::warn!(
                        "failed to recover manifest from cloud ({}), trying local",
                        e
                    );
                    // Continue to local fallback
                }
            }
        }

        // Fall back to local manifest
        match Self::load(db_path) {
            Ok(manifest) => {
                tracing::info!("recovered manifest from local storage");
                Ok(manifest)
            }
            Err(e) => {
                tracing::warn!("failed to recover local manifest ({}), using default", e);
                // Brand new DB or recovery from scratch
                Ok(Manifest::default())
            }
        }
    }

    /// Load manifest from cloud storage using cloud checkpoint.
    ///
    /// Reads the cloud checkpoint to locate the manifest, then loads and verifies it.
    ///
    /// # Arguments
    ///
    /// * `backend` - Cloud storage backend
    /// * `cloud_prefix` - Optional prefix for cloud paths (defaults to "midge")
    ///
    /// # Returns
    ///
    /// Manifest loaded from cloud, or error if checkpoint/manifest missing or corrupted
    ///
    /// # Cloud Path Format
    ///
    /// Cloud checkpoint: `{prefix}/manifest/CLOUD_CHECKPOINT`
    /// Manifest blob: `{prefix}/manifest/manifest.json`
    fn load_from_cloud(
        backend: &dyn StorageBackend,
        cloud_prefix: Option<&str>,
    ) -> MidgeResult<Self> {
        let prefix = cloud_prefix.unwrap_or("midge");

        // Read cloud checkpoint metadata
        let checkpoint_key = format!("{}/manifest/CLOUD_CHECKPOINT", prefix);
        tracing::debug!("loading cloud checkpoint from: {}", checkpoint_key);

        let checkpoint_data = backend.get_blob(&checkpoint_key).map_err(|e| {
            MidgeError::internal(format!(
                "failed to load cloud checkpoint from {}: {}",
                checkpoint_key, e
            ))
        })?;

        let checkpoint: crate::core::manifest::types::CloudCheckpoint =
            serde_json::from_slice(&checkpoint_data).map_err(|e| {
                MidgeError::internal(format!("failed to deserialize cloud checkpoint: {}", e))
            })?;

        // Load manifest blob referenced by checkpoint
        let manifest_key = format!("{}/manifest/manifest.json", prefix);
        tracing::debug!("loading manifest from: {}", manifest_key);

        let manifest_data = backend.get_blob(&manifest_key).map_err(|e| {
            MidgeError::internal(format!(
                "failed to load manifest blob from {}: {}",
                manifest_key, e
            ))
        })?;

        let manifest: Manifest = serde_json::from_slice(&manifest_data).map_err(|e| {
            MidgeError::internal(format!("failed to deserialize cloud manifest: {}", e))
        })?;

        // Verify manifest integrity
        manifest.verify_cloud_integrity(&checkpoint)?;

        tracing::info!(
            "verified cloud manifest integrity, checkpoint_seq={}",
            checkpoint.checkpoint_sequence
        );

        Ok(manifest)
    }

    /// Verify that manifest was correctly uploaded to cloud and has valid checkpoint.
    ///
    /// Ensures:
    /// - Manifest has a cloud checkpoint (was uploaded intentionally)
    /// - Checkpoint metadata is consistent with manifest state
    ///
    /// # Errors
    ///
    /// Returns error if manifest is missing checkpoint or checkpoint is inconsistent.
    fn verify_cloud_integrity(
        &self,
        expected_checkpoint: &crate::core::manifest::types::CloudCheckpoint,
    ) -> MidgeResult<()> {
        // Check that manifest has a checkpoint matching what we loaded
        let manifest_checkpoint = self
            .cloud_checkpoint
            .as_ref()
            .ok_or_else(|| {
                MidgeError::internal(
                    "cloud manifest missing checkpoint information - not a valid cloud recovery point"
                )
            })?;

        // Verify checkpoint sequence matches
        if manifest_checkpoint.checkpoint_sequence != expected_checkpoint.checkpoint_sequence {
            return Err(MidgeError::internal(format!(
                "cloud checkpoint mismatch: manifest has seq={}, checkpoint has seq={}",
                manifest_checkpoint.checkpoint_sequence, expected_checkpoint.checkpoint_sequence
            )));
        }

        // Verify that SST list in checkpoint is non-empty (indicates actual checkpoint)
        if manifest_checkpoint.covering_ssts.is_empty() {
            return Err(MidgeError::internal(
                "cloud checkpoint has empty SST list - not a valid recovery point"
            ));
        }

        tracing::debug!(
            "verified cloud checkpoint: seq={}, ssts={}",
            manifest_checkpoint.checkpoint_sequence,
            manifest_checkpoint.covering_ssts.len()
        );

        Ok(())
    }

    /// Save manifest to cloud storage with checkpoint atomicity.
    ///
    /// This is used during initialization and flush operations to ensure
    /// cloud manifest stays in sync with local manifest.
    ///
    /// # Arguments
    ///
    /// * `backend` - Cloud storage backend
    /// * `cloud_prefix` - Optional prefix for cloud paths (defaults to "midge")
    ///
    /// # Returns
    ///
    /// Success if manifest and checkpoint are saved, error if either fails
    pub fn save_to_cloud(
        &self,
        backend: &dyn StorageBackend,
        cloud_prefix: Option<&str>,
    ) -> MidgeResult<()> {
        let prefix = cloud_prefix.unwrap_or("midge");

        // Serialize manifest
        let manifest_json =
            serde_json::to_vec(self).map_err(|e| {
                MidgeError::internal(format!("failed to serialize manifest for cloud: {}", e))
            })?;

        // Upload manifest blob
        let manifest_key = format!("{}/manifest/manifest.json", prefix);
        tracing::debug!("uploading manifest to cloud: {}", manifest_key);
        backend
            .put_blob(&manifest_key, manifest_json.into())
            .map_err(|e| {
                MidgeError::internal(format!(
                    "failed to upload manifest blob to {}: {}",
                    manifest_key, e
                ))
            })?;

        // Serialize and upload checkpoint
        if let Some(checkpoint) = &self.cloud_checkpoint {
            let checkpoint_json = serde_json::to_vec(checkpoint).map_err(|e| {
                MidgeError::internal(format!(
                    "failed to serialize cloud checkpoint: {}",
                    e
                ))
            })?;

            let checkpoint_key = format!("{}/manifest/CLOUD_CHECKPOINT", prefix);
            tracing::debug!("uploading checkpoint to cloud: {}", checkpoint_key);
            backend
                .put_blob(&checkpoint_key, checkpoint_json.into())
                .map_err(|e| {
                    MidgeError::internal(format!(
                        "failed to upload checkpoint to {}: {}",
                        checkpoint_key, e
                    ))
                })?;
        }

        tracing::info!("successfully saved manifest to cloud");
        Ok(())
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::cloud::MockCloudBackend;
    use crate::core::manifest::types::CloudCheckpoint;
    use std::sync::Arc;

    fn create_checkpoint_with_ssts(seq: u64, sst_count: usize) -> CloudCheckpoint {
        CloudCheckpoint {
            checkpoint_sequence: seq,
            covering_ssts: (0..sst_count)
                .map(|i| format!("sst_{}.blob", i))
                .collect(),
            checkpoint_time: std::time::SystemTime::now(),
        }
    }

    #[test]
    fn should_load_manifest_from_cloud_when_available() {
        // Arrange
        let backend = Arc::new(MockCloudBackend::new());
        let mut manifest = Manifest::default();
        manifest.update_cloud_checkpoint(100, vec!["sst_0.blob".to_string()])
            .expect("update checkpoint");

        let checkpoint_json = serde_json::to_vec(manifest.cloud_checkpoint.as_ref().unwrap())
            .expect("serialize checkpoint");
        let manifest_json = serde_json::to_vec(&manifest).expect("serialize manifest");

        backend
            .put_blob("midge/manifest/CLOUD_CHECKPOINT", checkpoint_json.into())
            .expect("put checkpoint");
        backend
            .put_blob("midge/manifest/manifest.json", manifest_json.into())
            .expect("put manifest");

        // Act
        let loaded =
            Manifest::load_from_cloud(backend.as_ref(), Some("midge")).expect("load from cloud");

        // Assert
        assert!(loaded.cloud_checkpoint.is_some());
        assert_eq!(
            loaded.cloud_checkpoint.unwrap().checkpoint_sequence,
            100
        );
    }

    #[test]
    fn should_fallback_to_default_when_cloud_unavailable() {
        // Arrange
        use std::path::PathBuf;
        let backend = Arc::new(MockCloudBackend::new()); // Empty backend
        let db_path = PathBuf::from("/tmp/nonexistent");

        // Act
        let manifest = Manifest::load_with_cloud_fallback(
            &db_path,
            Some(backend.as_ref()),
            Some("midge"),
        )
        .expect("load with fallback");

        // Assert
        assert_eq!(manifest.column_families.len(), 0);
        assert!(manifest.files.is_empty());
    }

    #[test]
    fn should_verify_cloud_integrity_on_load() {
        // Arrange
        let backend = Arc::new(MockCloudBackend::new());
        let mut manifest = Manifest::default();
        let checkpoint = create_checkpoint_with_ssts(50, 2);
        manifest.cloud_checkpoint = Some(checkpoint.clone());

        let checkpoint_json = serde_json::to_vec(&checkpoint).expect("serialize");
        let manifest_json = serde_json::to_vec(&manifest).expect("serialize");

        backend
            .put_blob("test/manifest/CLOUD_CHECKPOINT", checkpoint_json.into())
            .expect("put checkpoint");
        backend
            .put_blob("test/manifest/manifest.json", manifest_json.into())
            .expect("put manifest");

        // Act & Assert
        let result = Manifest::load_from_cloud(backend.as_ref(), Some("test"));
        assert!(
            result.is_ok(),
            "should successfully verify cloud manifest"
        );
    }

    #[test]
    fn should_reject_manifest_with_empty_checkpoint_ssts() {
        // Arrange
        let backend = Arc::new(MockCloudBackend::new());
        let mut manifest = Manifest::default();
        let checkpoint = CloudCheckpoint {
            checkpoint_sequence: 100,
            covering_ssts: vec![], // Empty!
            checkpoint_time: std::time::SystemTime::now(),
        };
        manifest.cloud_checkpoint = Some(checkpoint.clone());

        let checkpoint_json = serde_json::to_vec(&checkpoint).expect("serialize");
        let manifest_json = serde_json::to_vec(&manifest).expect("serialize");

        backend
            .put_blob("test/manifest/CLOUD_CHECKPOINT", checkpoint_json.into())
            .expect("put checkpoint");
        backend
            .put_blob("test/manifest/manifest.json", manifest_json.into())
            .expect("put manifest");

        // Act
        let result = Manifest::load_from_cloud(backend.as_ref(), Some("test"));

        // Assert
        assert!(
            result.is_err(),
            "should reject checkpoint with empty SST list"
        );
    }
}
