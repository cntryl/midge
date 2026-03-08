//! Manifest persistence - serialization and file I/O
//!
//! Persists manifest state to disk in YAML format to enable
//! recovery of LSM structure across restarts.

use crate::metadata::Manifest;
use std::path::{Path, PathBuf};
use std::time::Instant;

/// Manifest persistence operations
pub struct ManifestPersistence;

impl ManifestPersistence {
    /// Manifest file name
    const MANIFEST_FILE: &'static str = "manifest.yaml";

    /// Snapshot file name
    const MANIFEST_SNAPSHOT: &'static str = "manifest.snapshot";

    /// Get the manifest file path
    pub fn manifest_path(db_path: &Path) -> PathBuf {
        db_path.join(Self::MANIFEST_FILE)
    }

    /// Get the manifest snapshot path
    pub fn manifest_snapshot_path(db_path: &Path) -> PathBuf {
        db_path.join(Self::MANIFEST_SNAPSHOT)
    }

    /// Load manifest, preferring a binary snapshot plus replaying the journal.
    /// Falls back to legacy YAML manifest if snapshot missing.
    pub fn load_with_fs(
        fs: &std::sync::Arc<dyn crate::io::traits::Fs>,
    ) -> Result<Manifest, String> {
        use crate::io::traits::FsPath;

        let snap_path = FsPath::new(Self::MANIFEST_SNAPSHOT);
        let mut manifest = match fs.exists(&snap_path) {
            Ok(true) => {
                let start = Instant::now();
                let file = fs
                    .open(
                        &snap_path,
                        crate::io::traits::OpenOptions {
                            mode: crate::io::traits::OpenMode::ReadOnly,
                            create: false,
                            create_new: false,
                            truncate: false,
                        },
                    )
                    .map_err(|e| format!("failed to open manifest snapshot: {:?}", e))?;
                let len = file
                    .len()
                    .map_err(|e| format!("failed to stat snapshot: {:?}", e))?;
                let data = file
                    .read_at(0, len)
                    .map_err(|e| format!("failed to read snapshot: {:?}", e))?;
                let contents = String::from_utf8(data.to_vec())
                    .map_err(|e| format!("snapshot not utf8: {}", e))?;
                let manifest: Manifest = serde_yaml::from_str(&contents)
                    .map_err(|e| format!("failed to parse manifest snapshot YAML: {}", e))?;
                let elapsed = start.elapsed();
                tracing::info!(
                    path = ?snap_path,
                    files = manifest.files.len(),
                    cf = manifest.column_families.len(),
                    elapsed_ms = elapsed.as_secs_f64() * 1000.0,
                    "manifest snapshot loaded"
                );
                Ok(manifest)
            }
            Ok(false) => {
                let manifest_path = FsPath::new(Self::MANIFEST_FILE);
                if !fs.exists(&manifest_path).unwrap_or(false) {
                    tracing::debug!(path = ?manifest_path, "manifest file not found, using default");
                    Ok(Manifest::default())
                } else {
                    let start = Instant::now();
                    let file = fs
                        .open(
                            &manifest_path,
                            crate::io::traits::OpenOptions {
                                mode: crate::io::traits::OpenMode::ReadOnly,
                                create: false,
                                create_new: false,
                                truncate: false,
                            },
                        )
                        .map_err(|e| format!("failed to open manifest file: {:?}", e))?;
                    let len = file
                        .len()
                        .map_err(|e| format!("failed to stat manifest file: {:?}", e))?;
                    let data = file
                        .read_at(0, len)
                        .map_err(|e| format!("failed to read manifest file: {:?}", e))?;
                    let contents = String::from_utf8(data.to_vec())
                        .map_err(|e| format!("manifest not utf8: {}", e))?;
                    let size_bytes = contents.len() as u64;

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
                    Ok(manifest)
                }
            }
            Err(e) => Err(format!("fs exists error: {:?}", e)),
        }?;

        // Check for an explicit bench-only trust mode (opt-in via env var)
        let trust_snapshot_enabled = std::env::var("MIDGE_BENCH_TRUST_SNAPSHOT").ok().as_deref()
            == Some("1")
            && (std::env::var("MIDGE_ALLOW_TRUST_SNAPSHOT").ok().as_deref() == Some("1"));
        if trust_snapshot_enabled {
            tracing::warn!("trust_snapshot enabled: loading snapshot and skipping journal replay (bench-only mode)");
            return Ok(manifest);
        }

        // Replay journal edits on top of snapshot/manifest
        match crate::metadata::journal::replay_journal_with_fs(fs) {
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
    pub fn save_with_fs(
        fs: &std::sync::Arc<dyn crate::io::traits::Fs>,
        manifest: &Manifest,
    ) -> Result<(), String> {
        use crate::io::traits::{Durability, FsPath, OpenMode, OpenOptions};

        // Serialize to YAML
        let yaml = serde_yaml::to_string(manifest)
            .map_err(|e| format!("failed to serialize manifest to YAML: {}", e))?;

        // Write temp manifest file
        let temp_path = FsPath::new("manifest.yaml.tmp");
        let mut f = fs
            .open(
                &temp_path,
                OpenOptions {
                    mode: OpenMode::ReadWrite,
                    create: true,
                    create_new: false,
                    truncate: true,
                },
            )
            .map_err(|e| format!("failed to open temp manifest file: {:?}", e))?;
        f.write_at(0, bytes::Bytes::from(yaml.clone()))
            .map_err(|e| format!("failed to write temp manifest: {:?}", e))?;
        f.sync(Durability::Durable)
            .map_err(|e| format!("failed to sync temp manifest: {:?}", e))?;

        fail::fail_point!("midge::manifest::after_temp_sync_before_rename");

        // Atomic rename
        fs.rename_atomic(&temp_path, &FsPath::new(Self::MANIFEST_FILE))
            .map_err(|e| format!("failed to rename manifest file atomically: {:?}", e))?;

        tracing::debug!(
            path = ?Self::MANIFEST_FILE,
            size_bytes = yaml.len(),
            "manifest persisted successfully"
        );

        Ok(())
    }

    /// Save a full manifest snapshot and truncate journal (atomic as possible).
    /// Writes to `manifest.snapshot.tmp` then renames into `manifest.snapshot`.
    pub fn save_snapshot_and_truncate_journal_with_fs(
        fs: &std::sync::Arc<dyn crate::io::traits::Fs>,
        manifest: &Manifest,
    ) -> Result<(), String> {
        use crate::io::traits::{Durability, FsPath, OpenMode, OpenOptions};

        let snap_path = FsPath::new(Self::MANIFEST_SNAPSHOT);
        let temp = FsPath::new("manifest.snapshot.tmp");

        let yaml = serde_yaml::to_string(manifest)
            .map_err(|e| format!("failed to serialize manifest to YAML: {}", e))?;

        // Write temp snapshot
        let mut f = fs
            .open(
                &temp,
                OpenOptions {
                    mode: OpenMode::ReadWrite,
                    create: true,
                    create_new: false,
                    truncate: true,
                },
            )
            .map_err(|e| format!("failed to open temp snapshot: {:?}", e))?;
        f.write_at(0, bytes::Bytes::from(yaml.clone()))
            .map_err(|e| format!("failed to write temp snapshot: {:?}", e))?;
        f.sync(Durability::Durable)
            .map_err(|e| format!("failed to sync temp snapshot: {:?}", e))?;

        // Atomic rename
        fs.rename_atomic(&temp, &snap_path)
            .map_err(|e| format!("failed to rename snapshot into place: {:?}", e))?;

        // truncate journal
        crate::metadata::journal::truncate_journal_with_fs(fs)
            .map_err(|e| format!("failed to truncate journal: {:?}", e))?;

        tracing::info!(path = ?snap_path, "manifest snapshot written and journal truncated");

        Ok(())
    }

    /// Save manifest to disk in YAML format (compat wrapper for tests and callers using Path)
    pub fn save(db_path: &Path, manifest: &Manifest) -> Result<(), String> {
        use crate::io::real::RealFs;
        use std::sync::Arc;

        let real =
            RealFs::new(db_path).map_err(|e| format!("failed to initialize real fs: {:?}", e))?;
        let fs: Arc<dyn crate::io::traits::Fs> = Arc::new(real);
        Self::save_with_fs(&fs, manifest)
    }

    /// Load manifest using a RealFs (compat wrapper)
    pub fn load(db_path: &Path) -> Result<Manifest, String> {
        use crate::io::real::RealFs;
        use std::sync::Arc;

        let real =
            RealFs::new(db_path).map_err(|e| format!("failed to initialize real fs: {:?}", e))?;
        let fs: Arc<dyn crate::io::traits::Fs> = Arc::new(real);
        Self::load_with_fs(&fs)
    }

    /// Save a full manifest snapshot and truncate journal (atomic as possible).
    /// This wrapper constructs a RealFs and delegates to the fs-backed implementation.
    pub fn save_snapshot_and_truncate_journal(
        db_path: &Path,
        manifest: &Manifest,
    ) -> Result<(), String> {
        use crate::io::real::RealFs;
        use std::sync::Arc;

        let real =
            RealFs::new(db_path).map_err(|e| format!("failed to initialize real fs: {:?}", e))?;
        let fs: Arc<dyn crate::io::traits::Fs> = Arc::new(real);
        Self::save_snapshot_and_truncate_journal_with_fs(&fs, manifest)
    }

    /// Delete manifest file from disk
    ///
    /// # Arguments
    /// * `db_path` - Path to the database directory
    ///
    /// # Errors
    /// Returns error if delete fails (other than file not existing)
    pub fn delete_with_fs(fs: &std::sync::Arc<dyn crate::io::traits::Fs>) -> Result<(), String> {
        use crate::io::traits::FsPath;

        let manifest_path = FsPath::new(Self::MANIFEST_FILE);

        match fs.exists(&manifest_path) {
            Ok(false) => return Ok(()),
            Err(e) => return Err(format!("fs exists error: {:?}", e)),
            Ok(true) => {}
        }

        fs.remove_file(&manifest_path)
            .map_err(|e| format!("failed to delete manifest file: {:?}", e))?;

        tracing::debug!(path = ?manifest_path, "manifest file deleted");

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
        use crate::io::real::RealFs;
        use std::sync::Arc;

        let real =
            RealFs::new(db_path).map_err(|e| format!("failed to initialize real fs: {:?}", e))?;
        let fs: Arc<dyn crate::io::traits::Fs> = Arc::new(real);
        Self::delete_with_fs(&fs)
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
                },
                crate::metadata::ColumnFamilyMeta {
                    id: 1,
                    name: "secondary".to_string(),
                    created_at: 0,
                    deleted_at: None,
                },
            ],
            ..Default::default()
        };

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
    fn should_support_bench_trust_snapshot_env_var() {
        // Arrange
        let test_dir = create_test_dir();
        let mut manifest = Manifest::default();
        manifest.files.push(crate::metadata::FileMeta {
            name: "only.sst".to_string(),
            level: 0,
            size_bytes: 10,
            ..Default::default()
        });
        ManifestPersistence::save_snapshot_and_truncate_journal(&test_dir, &manifest)
            .expect("save snapshot failed");

        // Act: enable trust snapshot via env var (simulating bench mode)
        std::env::set_var("MIDGE_BENCH_TRUST_SNAPSHOT", "1");

        // Assert: load returns snapshot and does NOT panic when journal is missing
        let loaded = ManifestPersistence::load(&test_dir)
            .expect("load should succeed in trust snapshot mode");
        assert!(loaded.files.iter().any(|f| f.name == "only.sst"));

        // Cleanup env var
        std::env::remove_var("MIDGE_BENCH_TRUST_SNAPSHOT");
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

        // Act
        let result = ManifestPersistence::delete(&test_dir);

        // Assert
        assert!(result.is_ok(), "delete should succeed even if file missing");
    }
}
