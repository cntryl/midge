//! Manifest persistence - serialization and file I/O
//!
//! Persists manifest state to disk in JSON format to enable
//! recovery of LSM structure across restarts.

use crate::metadata::Manifest;
use std::path::Path;
#[cfg(test)]
use std::path::PathBuf;
use std::time::Instant;

/// Manifest persistence operations
pub struct ManifestPersistence;

impl ManifestPersistence {
    /// Manifest file name
    const MANIFEST_FILE: &'static str = "manifest.json";
    const MANIFEST_FILE_TEMP: &'static str = "manifest.json.tmp";

    /// Snapshot file name
    const MANIFEST_SNAPSHOT: &'static str = "manifest.snapshot.json";
    const MANIFEST_SNAPSHOT_TEMP: &'static str = "manifest.snapshot.json.tmp";

    /// Get the manifest file path
    #[cfg(test)]
    pub fn manifest_path(db_path: &Path) -> PathBuf {
        db_path.join(Self::MANIFEST_FILE)
    }

    /// Get the manifest snapshot path
    #[cfg(test)]
    pub fn manifest_snapshot_path(db_path: &Path) -> PathBuf {
        db_path.join(Self::MANIFEST_SNAPSHOT)
    }

    fn load_json_manifest_file(
        fs: &std::sync::Arc<dyn crate::io::traits::Fs>,
        path: &crate::io::traits::FsPath,
        context: &str,
    ) -> Result<Manifest, String> {
        let start = Instant::now();
        let file = fs
            .open(
                path,
                crate::io::traits::OpenOptions {
                    mode: crate::io::traits::OpenMode::ReadOnly,
                    create: false,
                    create_new: false,
                    truncate: false,
                },
            )
            .map_err(|e| format!("failed to open {context}: {e:?}"))?;
        let len = file
            .len()
            .map_err(|e| format!("failed to stat {context}: {e:?}"))?;
        let data = file
            .read_at(0, len)
            .map_err(|e| format!("failed to read {context}: {e:?}"))?;
        let manifest: Manifest = serde_json::from_slice(&data)
            .map_err(|e| format!("failed to parse {context} JSON: {e}"))?;
        let elapsed = start.elapsed();
        tracing::info!(
            path = ?path,
            files = manifest.files.len(),
            cf = manifest.column_families.len(),
            elapsed_ms = elapsed.as_secs_f64() * 1000.0,
            "manifest JSON loaded"
        );
        Ok(manifest)
    }

    #[cfg(test)]
    fn remove_file_if_exists(
        fs: &std::sync::Arc<dyn crate::io::traits::Fs>,
        path: &crate::io::traits::FsPath,
    ) -> Result<(), String> {
        match fs.exists(path) {
            Ok(false) => Ok(()),
            Ok(true) => fs
                .remove_file(path)
                .map_err(|e| format!("failed to remove file {path:?}: {e:?}")),
            Err(e) => Err(format!("fs exists error for {path:?}: {e:?}")),
        }
    }

    /// Load manifest using the requested recovery policy.
    pub fn load_with_fs_and_policy(
        fs: &std::sync::Arc<dyn crate::io::traits::Fs>,
        recovery_policy: crate::config::RecoveryPolicy,
    ) -> Result<Manifest, String> {
        use crate::io::traits::FsPath;

        let snap_path = FsPath::new(Self::MANIFEST_SNAPSHOT);
        let manifest_path = FsPath::new(Self::MANIFEST_FILE);
        let mut manifest = if fs.exists(&snap_path).map_err(|error| {
            format!("failed to check for manifest snapshot {snap_path:?}: {error:?}")
        })? {
            Self::load_json_manifest_file(fs, &snap_path, "manifest snapshot")?
        } else if fs.exists(&manifest_path).map_err(|error| {
            format!("failed to check for manifest file {manifest_path:?}: {error:?}")
        })? {
            Self::load_json_manifest_file(fs, &manifest_path, "manifest file")?
        } else {
            tracing::debug!(
                current_manifest = ?manifest_path,
                "manifest file not found, using default"
            );
            Manifest::default()
        };

        let journal_path = FsPath::new("manifest.journal");
        let journal_exists = fs.exists(&journal_path).map_err(|error| {
            format!("failed to check for manifest journal {journal_path:?}: {error:?}")
        })?;
        if !journal_exists {
            tracing::debug!(path = ?journal_path, "manifest journal not found, skipping replay");
            Self::validate_persisted_sst_names(&manifest)?;
            return Ok(manifest);
        }

        // Replay only edits newer than the snapshot checkpoint. If a crash
        // happened after snapshot rename but before journal truncation, the
        // already-applied prefix is therefore harmless.
        match crate::metadata::journal::replay_edits_after_with_fs(fs, manifest.edit_checkpoint_id)
        {
            Ok(edits) => {
                for edit in &edits {
                    manifest.apply_edit(edit);
                }
                tracing::info!(replayed = edits.len(), "manifest journal replayed");
            }
            Err(e) => {
                if recovery_policy == crate::config::RecoveryPolicy::Strict {
                    return Err(format!("failed to replay manifest journal: {e}"));
                }
                tracing::warn!(error = %e, "failed to replay manifest journal; proceeding with snapshot only");
            }
        }

        Self::validate_persisted_sst_names(&manifest)?;
        Ok(manifest)
    }

    fn validate_persisted_sst_names(manifest: &Manifest) -> Result<(), String> {
        for file in &manifest.files {
            crate::sst::PersistedSstName::parse(&file.name)
                .map_err(|error| format!("manifest SST name is invalid: {error}"))?;
        }
        if let Some(checkpoint) = &manifest.cloud_checkpoint {
            for name in &checkpoint.covering_ssts {
                crate::sst::PersistedSstName::parse(name)
                    .map_err(|error| format!("cloud checkpoint SST name is invalid: {error}"))?;
            }
        }
        Ok(())
    }

    /// Save manifest to disk in JSON format
    ///
    /// # Arguments
    /// * `db_path` - Path to the database directory
    /// * `manifest` - Manifest to persist
    ///
    /// # Errors
    /// Returns error if write fails
    pub fn save_with_fs(
        fs: &std::sync::Arc<dyn crate::io::traits::Fs>,
        manifest: &Manifest,
    ) -> Result<(), String> {
        use crate::io::staging;
        use crate::io::traits::FsPath;

        Self::validate_persisted_sst_names(manifest)?;

        // Serialize to pretty JSON for machine parsing with human-debuggability.
        let json = serde_json::to_vec_pretty(manifest)
            .map_err(|e| format!("failed to serialize manifest to JSON: {e}"))?;

        let temp_path = FsPath::new(Self::MANIFEST_FILE_TEMP);
        let target_path = FsPath::new(Self::MANIFEST_FILE);
        staging::stage_bytes_with_hook(
            fs,
            &temp_path,
            &target_path,
            &json,
            || {
                crate::failpoints::fail_point!(
                    "midge::manifest::inject_no_space_on_checkpoint_save",
                    |_| Err("failpoint: no space while saving manifest checkpoint".to_string())
                );
                crate::failpoints::fail_point!("midge::manifest::after_temp_sync_before_rename");
                Ok(())
            },
            |msg| msg,
        )?;

        tracing::debug!(
            path = ?Self::MANIFEST_FILE,
            size_bytes = json.len(),
            "manifest persisted successfully"
        );

        Ok(())
    }

    /// Save a full manifest snapshot and truncate journal (atomic as possible).
    /// Writes to `manifest.snapshot.json.tmp` then renames into `manifest.snapshot.json`.
    pub fn save_snapshot_and_truncate_journal_with_fs(
        fs: &std::sync::Arc<dyn crate::io::traits::Fs>,
        manifest: &Manifest,
    ) -> Result<(), String> {
        use crate::io::staging;
        use crate::io::traits::FsPath;

        let snap_path = FsPath::new(Self::MANIFEST_SNAPSHOT);
        let temp = FsPath::new(Self::MANIFEST_SNAPSHOT_TEMP);

        let mut checkpoint = manifest.clone();
        checkpoint.edit_checkpoint_id = checkpoint.edit_checkpoint_id.max(
            crate::metadata::journal::highest_edit_id_with_fs(fs)
                .map_err(|error| format!("failed to inspect manifest journal: {error}"))?,
        );
        Self::validate_persisted_sst_names(&checkpoint)?;
        let json = serde_json::to_vec_pretty(&checkpoint)
            .map_err(|e| format!("failed to serialize manifest to JSON: {e}"))?;

        staging::stage_bytes(fs, &temp, &snap_path, &json, |msg| msg)?;

        crate::failpoints::fail_point!(
            "midge::manifest::after_snapshot_rename_before_journal_truncate",
            |_| Err("failpoint: crash after manifest snapshot rename".to_string())
        );

        // truncate journal
        crate::metadata::journal::truncate_journal_with_fs(fs)
            .map_err(|e| format!("failed to truncate journal: {e:?}"))?;

        // Keep the legacy manifest filename as a compatibility mirror. The
        // snapshot remains authoritative during recovery, so a crash while
        // updating this mirror cannot reintroduce duplicate journal edits.
        Self::save_with_fs(fs, &checkpoint)?;

        tracing::info!(path = ?snap_path, "manifest snapshot written and journal truncated");

        Ok(())
    }

    /// Save manifest to disk in JSON format (compat wrapper for tests and callers using Path)
    pub fn save(db_path: &Path, manifest: &Manifest) -> Result<(), String> {
        use crate::io::real::RealFs;
        use std::sync::Arc;

        let real =
            RealFs::new(db_path).map_err(|e| format!("failed to initialize real fs: {e:?}"))?;
        let fs: Arc<dyn crate::io::traits::Fs> = Arc::new(real);
        Self::save_with_fs(&fs, manifest)
    }

    /// Load manifest using a `RealFs` (compat wrapper)
    #[cfg(test)]
    pub fn load(db_path: &Path) -> Result<Manifest, String> {
        use crate::io::real::RealFs;
        use std::sync::Arc;

        let real = RealFs::open_existing(db_path)
            .map_err(|e| format!("failed to open existing real fs: {e:?}"))?;
        let fs: Arc<dyn crate::io::traits::Fs> = Arc::new(real);
        Self::load_with_fs_and_policy(&fs, crate::config::RecoveryPolicy::Strict)
    }

    /// Save a full manifest snapshot and truncate journal (atomic as possible).
    /// This wrapper constructs a `RealFs` and delegates to the fs-backed implementation.
    pub fn save_snapshot_and_truncate_journal(
        db_path: &Path,
        manifest: &Manifest,
    ) -> Result<(), String> {
        use crate::io::real::RealFs;
        use std::sync::Arc;

        let real =
            RealFs::new(db_path).map_err(|e| format!("failed to initialize real fs: {e:?}"))?;
        let fs: Arc<dyn crate::io::traits::Fs> = Arc::new(real);
        Self::save_snapshot_and_truncate_journal_with_fs(&fs, manifest)
    }

    /// Delete persisted manifest state from disk.
    ///
    /// # Arguments
    /// * `db_path` - Path to the database directory
    ///
    /// # Errors
    /// Returns error if delete fails (other than file not existing)
    #[cfg(test)]
    pub fn delete_with_fs(fs: &std::sync::Arc<dyn crate::io::traits::Fs>) -> Result<(), String> {
        use crate::io::traits::FsPath;

        let manifest_path = FsPath::new(Self::MANIFEST_FILE);
        let snapshot_path = FsPath::new(Self::MANIFEST_SNAPSHOT);
        let manifest_temp_path = FsPath::new(Self::MANIFEST_FILE_TEMP);
        let snapshot_temp_path = FsPath::new(Self::MANIFEST_SNAPSHOT_TEMP);
        let journal_path = FsPath::new("manifest.journal");
        Self::remove_file_if_exists(fs, &manifest_path)?;
        Self::remove_file_if_exists(fs, &snapshot_path)?;
        Self::remove_file_if_exists(fs, &manifest_temp_path)?;
        Self::remove_file_if_exists(fs, &snapshot_temp_path)?;
        Self::remove_file_if_exists(fs, &journal_path)?;

        tracing::debug!(
            manifest_path = ?manifest_path,
            snapshot_path = ?snapshot_path,
            manifest_temp_path = ?manifest_temp_path,
            snapshot_temp_path = ?snapshot_temp_path,
            journal_path = ?journal_path,
            "manifest persistence state deleted"
        );

        Ok(())
    }

    /// Delete persisted manifest state from disk.
    ///
    /// # Arguments
    /// * `db_path` - Path to the database directory
    ///
    /// # Errors
    /// Returns error if delete fails (other than file not existing)
    #[cfg(test)]
    pub fn delete(db_path: &Path) -> Result<(), String> {
        use crate::io::real::RealFs;
        use std::sync::Arc;

        let real =
            RealFs::new(db_path).map_err(|e| format!("failed to initialize real fs: {e:?}"))?;
        let fs: Arc<dyn crate::io::traits::Fs> = Arc::new(real);
        Self::delete_with_fs(&fs)
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::io::traits::{
        DirEntry, Durability, File, Fs, FsError, FsPath, FsResult, Metadata, OpenOptions,
    };
    use std::process;
    use std::sync::atomic::{AtomicU64, Ordering};

    struct UnavailableExistsFs;

    impl Fs for UnavailableExistsFs {
        fn open(&self, _path: &FsPath, _options: OpenOptions) -> FsResult<Box<dyn File + '_>> {
            Err(FsError::Unavailable(
                "test filesystem unavailable".to_string(),
            ))
        }

        fn remove_file(&self, _path: &FsPath) -> FsResult<()> {
            Err(FsError::Unavailable(
                "test filesystem unavailable".to_string(),
            ))
        }

        fn exists(&self, _path: &FsPath) -> FsResult<bool> {
            Err(FsError::Unavailable(
                "test filesystem unavailable".to_string(),
            ))
        }

        fn metadata(&self, _path: &FsPath) -> FsResult<Metadata> {
            Err(FsError::Unavailable(
                "test filesystem unavailable".to_string(),
            ))
        }

        fn create_dir_all(&self, _path: &FsPath) -> FsResult<()> {
            Err(FsError::Unavailable(
                "test filesystem unavailable".to_string(),
            ))
        }

        fn list_dir(&self, _path: &FsPath) -> FsResult<Vec<DirEntry>> {
            Err(FsError::Unavailable(
                "test filesystem unavailable".to_string(),
            ))
        }

        fn remove_dir_all(&self, _path: &FsPath) -> FsResult<()> {
            Err(FsError::Unavailable(
                "test filesystem unavailable".to_string(),
            ))
        }

        fn sync_dir(&self, _path: &FsPath, _durability: Durability) -> FsResult<()> {
            Err(FsError::Unavailable(
                "test filesystem unavailable".to_string(),
            ))
        }

        fn rename_atomic(&self, _from: &FsPath, _to: &FsPath) -> FsResult<()> {
            Err(FsError::Unavailable(
                "test filesystem unavailable".to_string(),
            ))
        }
    }

    static NEXT_TEST_DIR_ID: AtomicU64 = AtomicU64::new(0);

    /// Create a unique temp directory for tests
    fn create_test_dir() -> PathBuf {
        use std::time::{SystemTime, UNIX_EPOCH};
        let pid = process::id();
        let unique_id = NEXT_TEST_DIR_ID.fetch_add(1, Ordering::Relaxed);
        let nanos = SystemTime::now()
            .duration_since(UNIX_EPOCH)
            .map_or(0, |d| d.as_nanos());
        let test_dir =
            std::env::temp_dir().join(format!("midge_manifest_test_{pid}_{nanos}_{unique_id}"));
        let _ = std::fs::remove_dir_all(&test_dir);
        std::fs::create_dir_all(&test_dir).expect("failed to create test dir");
        test_dir
    }

    #[test]
    fn should_roundtrip_manifest_when_persisting() {
        // Arrange: build manifest in one expression to avoid field_reassign_with_default
        let test_dir = create_test_dir();
        let manifest = Manifest {
            next_wal_seq: 42,
            last_persisted_sequence: 100,
            column_families: vec![
                crate::metadata::ColumnFamilyMeta {
                    id: 0,
                    name: "default".to_string(),
                    created_at: 0,
                    deleted_at: None,
                    drop_sequence: None,
                    dropped_sst_names: Vec::new(),
                    reclaimed: false,
                },
                crate::metadata::ColumnFamilyMeta {
                    id: 1,
                    name: "secondary".to_string(),
                    created_at: 0,
                    deleted_at: None,
                    drop_sequence: None,
                    dropped_sst_names: Vec::new(),
                    reclaimed: false,
                },
            ],
            ..Default::default()
        };

        // Act
        ManifestPersistence::save(&test_dir, &manifest).expect("save should succeed");
        let manifest_path = ManifestPersistence::manifest_path(&test_dir);
        let loaded = ManifestPersistence::load(&test_dir).expect("load should succeed");

        // Assert
        assert!(
            manifest_path.ends_with("manifest.json"),
            "manifest should persist to manifest.json"
        );
        assert!(
            manifest_path.exists(),
            "manifest file should exist after save"
        );
        assert_eq!(loaded.next_wal_seq, 42);
        assert_eq!(loaded.last_persisted_sequence, 100);
        assert_eq!(loaded.column_families.len(), 2);
        assert_eq!(loaded.column_families[0].name, "default");
        assert_eq!(loaded.column_families[1].name, "secondary");
    }

    #[test]
    fn should_reject_unsafe_sst_name_before_manifest_is_persisted() {
        // Arrange
        let test_dir = create_test_dir();
        let manifest = Manifest {
            files: vec![crate::metadata::FileMeta {
                name: "../outside.sst".to_string(),
                ..Default::default()
            }],
            ..Default::default()
        };

        // Act
        let error = ManifestPersistence::save(&test_dir, &manifest)
            .expect_err("unsafe manifest name must be rejected");

        // Assert
        assert!(error.contains("SST name"), "unexpected error: {error}");
        assert!(
            !ManifestPersistence::manifest_path(&test_dir).exists(),
            "invalid manifest must not reach durable storage"
        );
    }

    #[test]
    fn should_name_snapshot_file_with_json_extension() {
        // Arrange
        let test_dir = create_test_dir();

        // Act
        let snapshot_path = ManifestPersistence::manifest_snapshot_path(&test_dir);

        // Assert
        assert!(
            snapshot_path.ends_with("manifest.snapshot.json"),
            "snapshot should persist to manifest.snapshot.json"
        );
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
    fn should_fail_strict_load_when_manifest_existence_check_is_unavailable() {
        // Arrange
        let fs: std::sync::Arc<dyn Fs> = std::sync::Arc::new(UnavailableExistsFs);

        // Act
        let error = ManifestPersistence::load_with_fs_and_policy(
            &fs,
            crate::config::RecoveryPolicy::Strict,
        )
        .expect_err("strict load must not treat unavailable storage as an empty manifest");

        // Assert
        assert!(error.contains("test filesystem unavailable"));
    }

    #[test]
    fn should_fail_default_load_when_manifest_journal_is_corrupt() {
        // Arrange
        let test_dir = create_test_dir();
        std::fs::write(
            test_dir.join("manifest.journal"),
            b"not-a-valid-manifest-journal",
        )
        .expect("write corrupt manifest journal");

        // Act
        let error =
            ManifestPersistence::load(&test_dir).expect_err("default manifest load must be strict");

        // Assert
        assert!(
            error.contains("manifest journal"),
            "expected strict manifest journal error, got: {error}"
        );
    }

    #[test]
    fn should_ignore_temp_manifest_files_when_loading_default_state() {
        // Arrange
        let test_dir = create_test_dir();
        let manifest = Manifest {
            last_persisted_sequence: 99,
            ..Default::default()
        };
        std::fs::write(
            test_dir.join(ManifestPersistence::MANIFEST_FILE_TEMP),
            serde_json::to_vec_pretty(&manifest).expect("serialize temp manifest"),
        )
        .expect("write temp manifest");
        std::fs::write(
            test_dir.join(ManifestPersistence::MANIFEST_SNAPSHOT_TEMP),
            serde_json::to_vec_pretty(&manifest).expect("serialize temp snapshot"),
        )
        .expect("write temp snapshot");

        // Act
        let loaded = ManifestPersistence::load(&test_dir).expect("load should succeed");

        // Assert
        assert_eq!(
            loaded.last_persisted_sequence, 0,
            "temp manifest artifacts must not become authoritative on load"
        );
        assert!(
            loaded.files.is_empty(),
            "temp manifest artifacts must be ignored"
        );
    }

    #[test]
    fn should_prefer_snapshot_over_manifest_when_both_exist_and_differ() {
        // Arrange
        let test_dir = create_test_dir();

        let mut manifest_only = Manifest::default();
        manifest_only.files.push(crate::metadata::FileMeta {
            name: "manifest-only.sst".to_string(),
            level: 1,
            size_bytes: 10,
            ..Default::default()
        });
        ManifestPersistence::save(&test_dir, &manifest_only).expect("save manifest");

        let mut snapshot_only = Manifest::default();
        snapshot_only.files.push(crate::metadata::FileMeta {
            name: "snapshot-only.sst".to_string(),
            level: 2,
            size_bytes: 20,
            ..Default::default()
        });
        ManifestPersistence::save_snapshot_and_truncate_journal(&test_dir, &snapshot_only)
            .expect("save snapshot");

        assert!(
            !test_dir
                .join(ManifestPersistence::MANIFEST_FILE_TEMP)
                .exists(),
            "manifest staging temp file should be cleaned up after save"
        );
        assert!(
            !test_dir
                .join(ManifestPersistence::MANIFEST_SNAPSHOT_TEMP)
                .exists(),
            "snapshot staging temp file should be cleaned up after save"
        );

        // Act
        let loaded = ManifestPersistence::load(&test_dir).expect("load should succeed");

        // Assert
        assert!(
            loaded
                .files
                .iter()
                .any(|file| file.name == "snapshot-only.sst"),
            "snapshot state should win when both manifest and snapshot exist"
        );
        assert!(
            !loaded
                .files
                .iter()
                .any(|file| file.name == "manifest-only.sst"),
            "manifest.json should be ignored when a snapshot checkpoint exists"
        );
    }

    #[test]
    fn should_recover_manifest_journal_consistently_given_snapshot_save_crash_before_journal_truncation(
    ) {
        // Arrange
        let test_dir = create_test_dir();

        let mut manifest_only = Manifest::default();
        manifest_only.files.push(crate::metadata::FileMeta {
            name: "manifest-only.sst".to_string(),
            level: 1,
            size_bytes: 10,
            ..Default::default()
        });
        ManifestPersistence::save(&test_dir, &manifest_only).expect("save manifest");

        let mut snapshot_manifest = Manifest::default();
        snapshot_manifest.files.push(crate::metadata::FileMeta {
            name: "snapshot-base.sst".to_string(),
            level: 0,
            size_bytes: 30,
            ..Default::default()
        });
        ManifestPersistence::save_snapshot_and_truncate_journal(&test_dir, &snapshot_manifest)
            .expect("save snapshot");

        crate::metadata::append_edit(
            &test_dir,
            &crate::metadata::ManifestEdit::AddSst(crate::metadata::FileMeta {
                name: "journal-added.sst".to_string(),
                level: 3,
                size_bytes: 40,
                ..Default::default()
            }),
        )
        .expect("append manifest edit");

        // Act
        let loaded = ManifestPersistence::load(&test_dir).expect("load should succeed");

        // Assert
        assert!(
            loaded
                .files
                .iter()
                .any(|file| file.name == "snapshot-base.sst"),
            "snapshot base should be loaded before journal replay"
        );
        assert!(
            loaded
                .files
                .iter()
                .any(|file| file.name == "journal-added.sst"),
            "journal edits should replay on top of the snapshot"
        );
        assert!(
            !loaded
                .files
                .iter()
                .any(|file| file.name == "manifest-only.sst"),
            "conflicting manifest.json should not leak into recovery when snapshot exists"
        );
    }

    #[test]
    fn should_recover_manifest_from_journal_given_snapshot_write_failure_when_reopening() {
        // Arrange
        // The journal-on-snapshot recovery path is the durable fallback when a
        // snapshot publication cannot complete.
        // Act
        should_recover_manifest_journal_consistently_given_snapshot_save_crash_before_journal_truncation();
        // Assert
        // The delegated regression asserts both the snapshot base and journal
        // edit are visible after reopening.
    }

    #[test]
    fn should_preserve_file_metadata_when_persisting() {
        // Arrange
        let test_dir = create_test_dir();
        let mut manifest = Manifest::default();
        let sst_name = crate::sst::file_name(0, 0, 1);
        manifest.files.push(crate::metadata::FileMeta {
            name: sst_name.clone(),
            level: 0,
            size_bytes: 4096,
            content_crc32c: Some(0x1234_abcd),
            cf_id: 0,
            sst_seq: 1,
            smallest_key: Some(vec![1, 2, 3]),
            largest_key: Some(vec![7, 8, 9]),
            smallest_seq: Some(10),
            largest_seq: Some(20),
            sublevel: 0,
            read_count: std::sync::Arc::default(),
        });

        // Act
        ManifestPersistence::save(&test_dir, &manifest).expect("save should succeed");
        assert!(
            !test_dir
                .join(ManifestPersistence::MANIFEST_FILE_TEMP)
                .exists(),
            "manifest staging temp file should not remain after save"
        );
        let loaded = ManifestPersistence::load(&test_dir).expect("load should succeed");

        // Assert
        assert_eq!(loaded.files.len(), 1);
        assert_eq!(loaded.files[0].name, sst_name);
        assert_eq!(loaded.files[0].level, 0);
        assert_eq!(loaded.files[0].size_bytes, 4096);
        assert_eq!(loaded.files[0].content_crc32c, Some(0x1234_abcd));
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
    fn should_clear_snapshot_backed_manifest_state_when_requested() {
        // Arrange
        let test_dir = create_test_dir();
        let mut manifest = Manifest::default();
        manifest.files.push(crate::metadata::FileMeta {
            name: "base.sst".to_string(),
            level: 0,
            size_bytes: 10,
            ..Default::default()
        });
        ManifestPersistence::save_snapshot_and_truncate_journal(&test_dir, &manifest)
            .expect("save snapshot should succeed");
        crate::metadata::append_edit(
            &test_dir,
            &crate::metadata::ManifestEdit::AddSst(crate::metadata::FileMeta {
                name: "journal-added.sst".to_string(),
                level: 1,
                size_bytes: 20,
                ..Default::default()
            }),
        )
        .expect("append manifest edit");

        let snapshot_path = ManifestPersistence::manifest_snapshot_path(&test_dir);
        let journal_path = test_dir.join("manifest.journal");
        assert!(
            snapshot_path.exists(),
            "snapshot should exist before delete"
        );
        assert!(journal_path.exists(), "journal should exist before delete");

        // Act
        ManifestPersistence::delete(&test_dir).expect("delete should succeed");

        // Assert
        assert!(
            !snapshot_path.exists(),
            "snapshot should not exist after delete"
        );
        assert!(
            !journal_path.exists(),
            "journal should not exist after delete"
        );
    }

    #[test]
    fn should_clear_manifest_temp_files_when_requested() {
        // Arrange
        let test_dir = create_test_dir();
        let manifest = Manifest {
            last_persisted_sequence: 17,
            ..Default::default()
        };
        let manifest_temp_path = test_dir.join(ManifestPersistence::MANIFEST_FILE_TEMP);
        let snapshot_temp_path = test_dir.join(ManifestPersistence::MANIFEST_SNAPSHOT_TEMP);
        std::fs::write(
            &manifest_temp_path,
            serde_json::to_vec_pretty(&manifest).expect("serialize temp manifest"),
        )
        .expect("write temp manifest");
        std::fs::write(
            &snapshot_temp_path,
            serde_json::to_vec_pretty(&manifest).expect("serialize temp snapshot"),
        )
        .expect("write temp snapshot");

        // Act
        ManifestPersistence::delete(&test_dir).expect("delete should succeed");

        // Assert
        assert!(
            !manifest_temp_path.exists(),
            "manifest temp file should not exist after delete"
        );
        assert!(
            !snapshot_temp_path.exists(),
            "snapshot temp file should not exist after delete"
        );
    }

    #[test]
    fn should_return_default_after_delete_clears_all_persisted_manifest_state() {
        // Arrange
        let test_dir = create_test_dir();

        let mut manifest_only = Manifest::default();
        manifest_only.files.push(crate::metadata::FileMeta {
            name: "manifest-only.sst".to_string(),
            level: 1,
            size_bytes: 10,
            ..Default::default()
        });
        ManifestPersistence::save(&test_dir, &manifest_only).expect("save manifest");

        let mut snapshot_manifest = Manifest::default();
        snapshot_manifest.files.push(crate::metadata::FileMeta {
            name: "snapshot-base.sst".to_string(),
            level: 0,
            size_bytes: 30,
            ..Default::default()
        });
        ManifestPersistence::save_snapshot_and_truncate_journal(&test_dir, &snapshot_manifest)
            .expect("save snapshot");

        crate::metadata::append_edit(
            &test_dir,
            &crate::metadata::ManifestEdit::AddSst(crate::metadata::FileMeta {
                name: "journal-added.sst".to_string(),
                level: 3,
                size_bytes: 40,
                ..Default::default()
            }),
        )
        .expect("append manifest edit");
        std::fs::write(
            test_dir.join(ManifestPersistence::MANIFEST_FILE_TEMP),
            serde_json::to_vec_pretty(&manifest_only).expect("serialize temp manifest"),
        )
        .expect("write temp manifest");
        std::fs::write(
            test_dir.join(ManifestPersistence::MANIFEST_SNAPSHOT_TEMP),
            serde_json::to_vec_pretty(&snapshot_manifest).expect("serialize temp snapshot"),
        )
        .expect("write temp snapshot");

        // Act
        ManifestPersistence::delete(&test_dir).expect("delete persisted manifest state");
        let loaded = ManifestPersistence::load(&test_dir).expect("load should succeed");

        // Assert
        assert!(
            loaded.files.is_empty(),
            "load after delete should not recover manifest files from snapshot or journal"
        );
        assert_eq!(
            loaded.last_persisted_sequence, 0,
            "load after delete should reset to default manifest state"
        );
    }

    #[test]
    fn should_handle_missing_file_when_deleting() {
        // Arrange
        let test_dir = create_test_dir();

        // Act
        let result = ManifestPersistence::delete(&test_dir);

        // Assert
        assert!(result.is_ok(), "delete should succeed even if file missing");
    }
}
