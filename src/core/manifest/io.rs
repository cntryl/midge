//! Manifest I/O operations - loading and saving.

use std::path::Path;

use crate::error::MidgeResult;

use super::types::Manifest;

impl Manifest {
    /// Load manifest from disk.
    ///
    /// Reads the CURRENT file to find the active manifest, then loads and deserializes it.
    /// Returns a default manifest if CURRENT or manifest file doesn't exist.
    pub fn load(db_path: &Path) -> MidgeResult<Self> {
        let current_path = db_path.join("CURRENT");
        if !current_path.exists() {
            return Ok(Manifest::default());
        }
        let name =
            std::fs::read_to_string(&current_path).unwrap_or_else(|_| "manifest.json".to_string());
        let name = name.trim();
        let manifest_path = db_path.join(name);
        if !manifest_path.exists() {
            return Ok(Manifest::default());
        }
        let data = std::fs::read(&manifest_path)?;
        let m: Manifest = serde_json::from_slice(&data)?;
        Ok(m)
    }

    /// Load manifest with transient-failure resilience.
    ///
    /// Retries on I/O or deserialization errors and when CURRENT/manifest.json
    /// are temporarily missing (e.g., during atomic replace) to avoid
    /// accidentally defaulting to an empty manifest.
    ///
    /// # Arguments
    /// * `db_path` - Database directory path
    /// * `retries` - Number of retry attempts
    /// * `delay` - Duration to wait between retries
    pub fn load_with_retry(
        db_path: &Path,
        retries: usize,
        delay: std::time::Duration,
    ) -> MidgeResult<Self> {
        let current_path = db_path.join("CURRENT");
        let mut last_err: Option<crate::error::MidgeError> = None;

        for attempt in 0..=retries {
            // Read CURRENT pointer first
            match std::fs::read_to_string(&current_path) {
                Ok(name) => {
                    let name = name.trim();
                    let manifest_path = db_path.join(name);
                    if !manifest_path.exists() {
                        // Likely in the middle of an atomic replace; retry
                        if attempt == retries {
                            // Fall through to default only if no manifest appears after retries
                            return Ok(Manifest::default());
                        }
                    } else {
                        // Try read and parse manifest
                        match std::fs::read(&manifest_path) {
                            Ok(data) => match serde_json::from_slice::<Manifest>(&data) {
                                Ok(m) => return Ok(m),
                                Err(e) => {
                                    last_err = Some(crate::error::MidgeError::from(e));
                                }
                            },
                            Err(e) => {
                                last_err = Some(e.into());
                            }
                        }
                    }
                }
                Err(e) => {
                    // If CURRENT missing/locked, retry
                    last_err = Some(e.into());
                }
            }

            std::thread::sleep(delay);
        }

        // If we reach here and CURRENT/manifest never stabilized, assume brand-new DB only
        // if CURRENT does not exist; otherwise bubble the last error to avoid truncation.
        if !current_path.exists() {
            return Ok(Manifest::default());
        }
        Err(last_err.unwrap_or_else(|| {
            crate::error::MidgeError::internal(
                "manifest load_with_retry failed without specific error",
            )
        }))
    }

    /// Save manifest atomically to disk.
    ///
    /// Serializes the manifest to a temporary file, then atomically renames it to
    /// manifest.json. Updates the CURRENT file to point to the new manifest.
    pub fn save_atomic(&self, db_path: &Path) -> MidgeResult<()> {
        self.save_atomic_with_hooks(db_path, None)
    }

    /// Save with optional test hooks for fault injection testing.
    pub fn save_atomic_with_hooks(
        &self,
        db_path: &Path,
        test_hooks: Option<&crate::common::test_hooks::TestHooks>,
    ) -> MidgeResult<()> {
        std::fs::create_dir_all(db_path)?;

        // Call test hook before manifest update (increments counter and checks for FailSave behavior)
        if let Some(hooks) = test_hooks {
            if hooks.before_manifest_update() {
                // before_manifest_update returns true when FailSave is configured
                return Err(crate::error::MidgeError::internal(
                    "Manifest update failed by test hook (FailSave behavior)",
                ));
            }
        }

        // OPTIMIZATION: Serialize to memory before any I/O operations.
        // This reduces the time between temp file write and atomic rename.
        let data = serde_json::to_vec_pretty(self)?;

        let manifest_path = db_path.join("manifest.json");
        let tmp = db_path.join("manifest.json.tmp");

        // Write serialized data to temp file
        std::fs::write(&tmp, &data)?;

        // Atomic replace via rename
        std::fs::rename(&tmp, &manifest_path)?;

        // Update CURRENT pointer
        std::fs::write(db_path.join("CURRENT"), b"manifest.json")?;

        // Ensure the manifest file and directory entry are durable. Call
        // `sync_data_only` on the manifest file and sync the parent directory
        // so tests can deterministically verify ordering with WAL truncation.
        // If `test_hooks` is provided, `sync_data_only` will honor the
        // configured behavior (e.g., RecordOnly/Skip) and allow fault injection.
        {
            // Sync the manifest file data to stable storage
            let f = std::fs::OpenOptions::new().read(true).open(&manifest_path)?;
            crate::fs::sync_data_only(&f, test_hooks)?;
        }

        // Sync the parent directory (CURRENT and manifest dir entry)
        crate::fs::sync_parent(db_path)?;

        // Signal test hooks that the manifest has been fsynced and is durable
        // before any WAL truncation that may follow.
        if let Some(hooks) = test_hooks {
            hooks.manifest_fsynced_before_wal_truncate();
        }

        // TODO: Implement CorruptAfterSave behavior if needed
        // if let Some(hooks) = test_hooks {
        //     if hooks.manifest_behavior() == ManifestBehavior::CorruptAfterSave {
        //         // Intentionally corrupt the manifest file
        //     }
        // }

        Ok(())
    }
}

#[cfg(test)]
mod tests {
    use super::super::types::FileMeta;
    use super::*;
    use crate::api::column_family::{ColumnFamilyConfig, ColumnFamilyId, DEFAULT_CF_ID};
    use crate::config::{CompactionStyle, CompressionType};

    #[test]
    fn should_create_default_manifest_when_file_does_not_exist() {
        use tempfile::TempDir;

        // Arrange
        let dir = TempDir::new().expect("temp dir");

        // Act
        let manifest = Manifest::load(dir.path()).expect("load");

        // Assert
        assert_eq!(manifest.last_persisted_sequence, 0);
        assert!(manifest.files.is_empty());
        assert!(manifest.column_families.is_empty());
    }

    #[test]
    fn should_save_manifest_atomically() {
        use tempfile::TempDir;

        // Arrange
        let dir = TempDir::new().expect("temp dir");
        let mut manifest = Manifest {
            last_persisted_sequence: 42,
            ..Default::default()
        };
        manifest.files.push(FileMeta {
            name: "test.sst".to_string(),
            level: 1,
            size_bytes: 2048,
            ..Default::default()
        });
        manifest.add_cf(DEFAULT_CF_ID, "default".to_string(), None);

        // Act
        manifest.save_atomic(dir.path()).expect("save");

        // Assert
        let manifest_path = dir.path().join("manifest.json");
        assert!(manifest_path.exists());
    }

    #[test]
    fn should_load_saved_manifest() {
        use tempfile::TempDir;

        // Arrange
        let dir = TempDir::new().expect("temp dir");
        let mut manifest = Manifest {
            last_persisted_sequence: 42,
            ..Default::default()
        };
        manifest.files.push(FileMeta {
            name: "test.sst".to_string(),
            level: 1,
            size_bytes: 2048,
            ..Default::default()
        });
        manifest.add_cf(DEFAULT_CF_ID, "default".to_string(), None);
        manifest.save_atomic(dir.path()).expect("save");

        // Act
        let loaded = Manifest::load(dir.path()).expect("load");

        // Assert
        assert_eq!(loaded.last_persisted_sequence, 42);
        assert_eq!(loaded.files.len(), 1);
        assert_eq!(loaded.files[0].name, "test.sst");
        assert_eq!(loaded.files[0].level, 1);
    }

    #[test]
    fn should_retry_loading_manifest_until_success() {
        use tempfile::TempDir;

        // Arrange
        let dir = TempDir::new().expect("temp dir");
        let manifest = Manifest {
            last_persisted_sequence: 99,
            ..Default::default()
        };
        manifest.save_atomic(dir.path()).expect("save");

        // Act
        let loaded = Manifest::load_with_retry(dir.path(), 3, std::time::Duration::from_millis(10))
            .expect("load with retry");

        // Assert
        assert_eq!(loaded.last_persisted_sequence, 99);
    }

    // === Durability Tests ===

    #[test]
    fn should_atomically_save_manifest_given_valid_data() {
        // Arrange
        let temp_dir = tempfile::tempdir().unwrap();
        let manifest = Manifest {
            last_persisted_sequence: 100,
            ..Default::default()
        };

        // Act
        let result = manifest.save_atomic(temp_dir.path());

        // Assert
        assert!(result.is_ok(), "Atomic save should succeed");
        assert!(
            temp_dir.path().join("manifest.json").exists(),
            "Manifest file should exist"
        );
        assert!(
            temp_dir.path().join("CURRENT").exists(),
            "CURRENT pointer should exist"
        );
    }

    #[test]
    fn should_use_temp_file_during_atomic_save() {
        // Arrange
        let temp_dir = tempfile::tempdir().unwrap();
        let manifest = Manifest {
            last_persisted_sequence: 50,
            ..Default::default()
        };

        // Act
        manifest.save_atomic(temp_dir.path()).unwrap();

        // Assert
        let temp_file = temp_dir.path().join("manifest.json.tmp");
        assert!(
            !temp_file.exists(),
            "Temp file should not exist after atomic rename"
        );
    }

    #[test]
    fn should_preserve_data_integrity_across_save_load_cycle() {
        // Arrange
        let temp_dir = tempfile::tempdir().unwrap();
        let mut original = Manifest {
            last_persisted_sequence: 123,
            ..Default::default()
        };
        original.files.push(FileMeta {
            name: "test.sst".to_string(),
            level: 2,
            size_bytes: 4096,
            ..Default::default()
        });

        // Act
        original.save_atomic(temp_dir.path()).unwrap();
        let loaded =
            Manifest::load_with_retry(temp_dir.path(), 3, std::time::Duration::from_millis(10))
                .unwrap();

        // Assert
        assert_eq!(loaded.last_persisted_sequence, 123);
        assert_eq!(loaded.files.len(), 1);
        assert_eq!(loaded.files[0].name, "test.sst");
        assert_eq!(loaded.files[0].level, 2);
        assert_eq!(loaded.files[0].size_bytes, 4096);
    }

    #[test]
    #[allow(clippy::field_reassign_with_default)]
    fn should_track_last_persisted_sequence_correctly() {
        // Arrange
        let temp_dir = tempfile::tempdir().unwrap();
        let mut manifest = Manifest::default();

        // Act
        manifest.last_persisted_sequence = 10;
        manifest.save_atomic(temp_dir.path()).unwrap();

        manifest.last_persisted_sequence = 20;
        manifest.save_atomic(temp_dir.path()).unwrap();

        let loaded =
            Manifest::load_with_retry(temp_dir.path(), 3, std::time::Duration::from_millis(10))
                .unwrap();

        // Assert
        assert_eq!(loaded.last_persisted_sequence, 20);
    }

    #[test]
    fn should_maintain_file_ordering_across_persistence() {
        // Arrange
        let temp_dir = tempfile::tempdir().unwrap();
        let mut manifest = Manifest::default();

        for i in 0..5 {
            manifest.files.push(FileMeta {
                name: format!("file_{}.sst", i),
                level: i,
                ..Default::default()
            });
        }

        // Act
        manifest.save_atomic(temp_dir.path()).unwrap();
        let loaded =
            Manifest::load_with_retry(temp_dir.path(), 3, std::time::Duration::from_millis(10))
                .unwrap();

        // Assert
        assert_eq!(loaded.files.len(), 5);
        for i in 0..5 {
            assert_eq!(loaded.files[i].name, format!("file_{}.sst", i));
            assert_eq!(loaded.files[i].level, i as u32);
        }
    }

    #[test]
    fn should_save_empty_manifest_successfully() {
        // Arrange
        let temp_dir = tempfile::tempdir().unwrap();
        let manifest = Manifest::default();

        // Act
        let result = manifest.save_atomic(temp_dir.path());

        // Assert
        assert!(result.is_ok());
        assert!(temp_dir.path().join("manifest.json").exists());
    }

    #[test]
    fn should_load_empty_manifest_successfully() {
        // Arrange
        let temp_dir = tempfile::tempdir().unwrap();
        let manifest = Manifest::default();
        manifest.save_atomic(temp_dir.path()).unwrap();

        // Act
        let loaded =
            Manifest::load_with_retry(temp_dir.path(), 3, std::time::Duration::from_millis(10))
                .unwrap();

        // Assert
        assert_eq!(loaded.last_persisted_sequence, 0);
        assert_eq!(loaded.files.len(), 0);
    }

    #[test]
    fn should_update_current_pointer_atomically() {
        // Arrange
        let temp_dir = tempfile::tempdir().unwrap();
        let manifest = Manifest::default();

        // Act
        manifest.save_atomic(temp_dir.path()).unwrap();

        // Assert
        let current_content = std::fs::read(temp_dir.path().join("CURRENT")).unwrap();
        assert_eq!(current_content, b"manifest.json");
    }

    #[test]
    fn should_preserve_column_family_metadata_across_persistence() {
        // Arrange
        let temp_dir = tempfile::tempdir().unwrap();
        let mut manifest = Manifest::default();

        let cf_config = ColumnFamilyConfig {
            memtable_max_bytes: 64 * 1024 * 1024,
            compaction_style: CompactionStyle::SizeTiered,
            compression: CompressionType::Zstd,
            ..Default::default()
        };

        manifest.add_cf(
            ColumnFamilyId::new(5),
            "test_cf".to_string(),
            Some(cf_config),
        );

        // Act
        manifest.save_atomic(temp_dir.path()).unwrap();
        let loaded =
            Manifest::load_with_retry(temp_dir.path(), 3, std::time::Duration::from_millis(10))
                .unwrap();

        // Assert
        let cf = loaded.get_cf(ColumnFamilyId::new(5)).unwrap();
        assert_eq!(cf.name, "test_cf");
        assert!(cf.config.is_some());
        assert_eq!(
            cf.config.as_ref().unwrap().memtable_max_bytes,
            64 * 1024 * 1024
        );
    }
}
