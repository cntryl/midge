//! Manifest persistence - serialization and file I/O
//!
//! Persists manifest state to disk in YAML format to enable
//! recovery of LSM structure across restarts.

use crate::metadata::Manifest;
use std::fs;
use std::path::{Path, PathBuf};

/// Manifest persistence operations
pub struct ManifestPersistence;

impl ManifestPersistence {
    /// Manifest file name
    const MANIFEST_FILE: &'static str = "manifest.yaml";

    /// Get the manifest file path given a database path
    pub fn manifest_path(db_path: &Path) -> PathBuf {
        db_path.join(Self::MANIFEST_FILE)
    }

    /// Snapshot file name
    const MANIFEST_SNAPSHOT: &'static str = "manifest.snapshot";

    /// Get the manifest snapshot path
    pub fn manifest_snapshot_path(db_path: &Path) -> PathBuf {
        db_path.join(Self::MANIFEST_SNAPSHOT)
    }

    /// Load manifest, preferring a binary snapshot plus replaying the journal.
    /// Falls back to legacy YAML manifest if snapshot missing.
    pub fn load(db_path: &Path) -> Result<Manifest, String> {
        // Prefer snapshot when present
        let snap_path = Self::manifest_snapshot_path(db_path);
        let mut manifest = if snap_path.exists() {
            let start = std::time::Instant::now();
            let contents = fs::read_to_string(&snap_path)
                .map_err(|e| format!("failed to read manifest snapshot: {}", e))?;
            let manifest: Manifest = serde_yaml::from_str(&contents)
                .map_err(|e| format!("failed to parse manifest snapshot YAML: {}", e))?;
            tracing::info!(path = ?snap_path, files = manifest.files.len(), cf = manifest.column_families.len(), elapsed_ms = start.elapsed().as_secs_f64() * 1000.0, "manifest snapshot loaded");
            manifest
        } else {
            // Legacy YAML
            let manifest_path = Self::manifest_path(db_path);
            if !manifest_path.exists() {
                tracing::debug!(path = ?manifest_path, "manifest file not found, using default");
                Manifest::default()
            } else {
                // Read file (measure read+parse time)
                let start = std::time::Instant::now();
                let contents = fs::read_to_string(&manifest_path)
                    .map_err(|e| format!("failed to read manifest file: {}", e))?;
                let size_bytes = contents.len() as u64;

                // Deserialize YAML
                let manifest: Manifest = serde_yaml::from_str(&contents)
                    .map_err(|e| format!("failed to parse manifest YAML: {}", e))?;

                let elapsed = start.elapsed();
                tracing::info!(
                    path = ?manifest_path,
                    files_count = manifest.files.len(),
                    cf_count = manifest.column_families.len(),
                    size_bytes,
                    elapsed_ms = elapsed.as_secs_f64() * 1000.0,
                    "manifest loaded successfully"
                );
                manifest
            }
        };

        // Replay journal edits on top of snapshot/manifest
        match crate::metadata::replay_journal(db_path) {
            Ok(edits) => {
                for edit in &edits {
                    manifest.apply_edit(edit);
                }
                tracing::info!(replayed = edits.len(), "manifest journal replayed");
            }
            Err(e) => {
                tracing::warn!(error = %e, "failed to replay manifest journal; proceeding with snapshot only");
            }
        }

        Ok(manifest)
    }

    /// Save manifest to disk in YAML format
    ///
    /// # Arguments
    /// * `db_path` - Path to the database directory
    /// * `manifest` - Manifest to persist
    ///
    /// # Errors
    /// Returns error if write fails
    pub fn save(db_path: &Path, manifest: &Manifest) -> Result<(), String> {
        // Ensure database directory exists
        fs::create_dir_all(db_path)
            .map_err(|e| format!("failed to create database directory: {}", e))?;

        let manifest_path = Self::manifest_path(db_path);

        // Serialize to YAML
        let yaml = serde_yaml::to_string(manifest)
            .map_err(|e| format!("failed to serialize manifest to YAML: {}", e))?;

        // Write atomically by writing to temp file first
        let temp_path = manifest_path.with_extension("yaml.tmp");
        fs::write(&temp_path, &yaml)
            .map_err(|e| format!("failed to write temporary manifest file: {}", e))?;

        // Atomic rename
        fs::rename(&temp_path, &manifest_path)
            .map_err(|e| format!("failed to rename manifest file atomically: {}", e))?;

        tracing::debug!(
            path = ?manifest_path,
            size_bytes = yaml.len(),
            "manifest persisted successfully"
        );

        Ok(())
    }

    /// Save a full manifest snapshot and truncate journal (atomic as possible).
    /// Writes to `manifest.snapshot.tmp` then renames into `manifest.snapshot`.
    pub fn save_snapshot_and_truncate_journal(db_path: &Path, manifest: &Manifest) -> Result<(), String> {
        fs::create_dir_all(db_path)
            .map_err(|e| format!("failed to create database directory: {}", e))?;

        let snap_path = Self::manifest_snapshot_path(db_path);
        let temp = snap_path.with_extension("tmp");

        let yaml = serde_yaml::to_string(manifest)
            .map_err(|e| format!("failed to serialize manifest to YAML: {}", e))?;

        fs::write(&temp, &yaml).map_err(|e| format!("failed to write temp snapshot: {}", e))?;

        // Ensure data is on disk
        let f = std::fs::OpenOptions::new().write(true).open(&temp).map_err(|e| format!("failed to open temp snapshot for sync: {}", e))?;
        f.sync_all().map_err(|e| format!("failed to sync temp snapshot: {}", e))?;

        fs::rename(&temp, &snap_path).map_err(|e| format!("failed to rename snapshot into place: {}", e))?;

        // truncate journal
        crate::metadata::truncate_journal(db_path).map_err(|e| format!("failed to truncate journal: {:?}", e))?;

        tracing::info!(path = ?snap_path, "manifest snapshot written and journal truncated");

        Ok(())
    }

    /// Delete manifest file from disk
    ///
    /// # Arguments
    /// * `db_path` - Path to the database directory
    ///
    /// # Errors
    /// Returns error if delete fails (other than file not existing)
    pub fn delete(db_path: &Path) -> Result<(), String> {
        let manifest_path = Self::manifest_path(db_path);

        if !manifest_path.exists() {
            return Ok(());
        }

        fs::remove_file(&manifest_path)
            .map_err(|e| format!("failed to delete manifest file: {}", e))?;

        tracing::debug!(path = ?manifest_path, "manifest file deleted");

        Ok(())
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::process;

    /// Create a unique temp directory for tests
    fn create_test_dir() -> PathBuf {
        use std::time::{SystemTime, UNIX_EPOCH};
        let pid = process::id();
        let nanos = SystemTime::now()
            .duration_since(UNIX_EPOCH)
            .map(|d| d.as_nanos())
            .unwrap_or(0);
        let test_dir = std::env::temp_dir().join(format!("midge_manifest_test_{}_{}", pid, nanos));
        let _ = std::fs::remove_dir_all(&test_dir);
        std::fs::create_dir_all(&test_dir).expect("failed to create test dir");
        test_dir
    }

    #[test]
    #[allow(clippy::field_reassign_with_default)]
    fn should_roundtrip_manifest_when_persisting() {
        // Arrange
        let test_dir = create_test_dir();
        let mut manifest = Manifest::default();
        manifest.next_wal_seq = 42;
        manifest.last_persisted_sequence = 100;
        manifest
            .column_families
            .push(crate::metadata::ColumnFamilyMeta {
                id: 0,
                name: "default".to_string(),
                created_at: 0,
                deleted_at: None,
            });
        manifest
            .column_families
            .push(crate::metadata::ColumnFamilyMeta {
                id: 1,
                name: "secondary".to_string(),
                created_at: 0,
                deleted_at: None,
            });

        // Act
        ManifestPersistence::save(&test_dir, &manifest).expect("save should succeed");
        let loaded = ManifestPersistence::load(&test_dir).expect("load should succeed");

        // Assert
        assert_eq!(loaded.next_wal_seq, 42);
        assert_eq!(loaded.last_persisted_sequence, 100);
        assert_eq!(loaded.column_families.len(), 2);
        assert_eq!(loaded.column_families[0].name, "default");
        assert_eq!(loaded.column_families[1].name, "secondary");
    }

    #[test]
    fn should_return_default_when_manifest_file_missing() {
        // Arrange
        let test_dir = create_test_dir();

        // Act
        let loaded = ManifestPersistence::load(&test_dir).expect("load should succeed");

        // Assert
        assert_eq!(loaded.next_wal_seq, 1);
        assert_eq!(loaded.last_persisted_sequence, 0);
        assert_eq!(loaded.column_families.len(), 0);
    }

    #[test]
    fn should_preserve_file_metadata_when_persisting() {
        // Arrange
        let test_dir = create_test_dir();
        let mut manifest = Manifest::default();
        manifest.files.push(crate::metadata::FileMeta {
            name: "sst_001.sst".to_string(),
            level: 0,
            size_bytes: 4096,
            cf_id: 0,
            sst_seq: 1,
            smallest_key: Some(vec![1, 2, 3]),
            largest_key: Some(vec![7, 8, 9]),
            smallest_seq: Some(10),
            largest_seq: Some(20),
            sublevel: 0,
            read_count: Default::default(),
        });

        // Act
        ManifestPersistence::save(&test_dir, &manifest).expect("save should succeed");
        let loaded = ManifestPersistence::load(&test_dir).expect("load should succeed");

        // Assert
        assert_eq!(loaded.files.len(), 1);
        assert_eq!(loaded.files[0].name, "sst_001.sst");
        assert_eq!(loaded.files[0].level, 0);
        assert_eq!(loaded.files[0].size_bytes, 4096);
        assert_eq!(loaded.files[0].smallest_key, Some(vec![1, 2, 3]));
        assert_eq!(loaded.files[0].largest_key, Some(vec![7, 8, 9]));
    }

    #[test]
    fn should_delete_manifest_file_when_requested() {
        // Arrange
        let test_dir = create_test_dir();
        let manifest = Manifest::default();
        ManifestPersistence::save(&test_dir, &manifest).expect("save should succeed");
        let manifest_path = ManifestPersistence::manifest_path(&test_dir);
        assert!(
            manifest_path.exists(),
            "manifest file should exist before delete"
        );

        // Act
        ManifestPersistence::delete(&test_dir).expect("delete should succeed");

        // Assert
        assert!(
            !manifest_path.exists(),
            "manifest file should not exist after delete"
        );
    }

    #[test]
    fn should_handle_missing_file_when_deleting() {
        // Arrange
        let test_dir = create_test_dir();

        // Act & Assert
        ManifestPersistence::delete(&test_dir).expect("delete should succeed even if file missing");
    }
}
