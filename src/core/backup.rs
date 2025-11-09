//! Backup and restore subsystem for Midge database.
//!
//! Provides full and incremental backup capabilities with verification.
//! Backups are atomic, consistent snapshots of the database state.

use serde::{Deserialize, Serialize};
use std::path::{Path, PathBuf};

use crate::error::MidgeResult;

/// Type of backup: full or incremental.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(tag = "type")]
pub enum BackupType {
    /// Full backup containing all SST files and manifest.
    Full,
    /// Incremental backup containing only files changed since a previous backup.
    Incremental { since_backup_id: u64 },
}

/// Information about a single SST file in a backup.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct SstFileInfo {
    /// Original filename (e.g., "000042.sst")
    pub name: String,
    /// File size in bytes
    pub size_bytes: u64,
    /// CRC32 checksum for verification
    pub checksum: u32,
    /// Optional key range (first_key, last_key) for diagnostics
    #[serde(skip_serializing_if = "Option::is_none")]
    pub key_range: Option<(Vec<u8>, Vec<u8>)>,
}

/// Metadata for a single backup.
///
/// Stored as BACKUP_META.json in each backup directory.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct BackupInfo {
    /// Unique backup identifier (monotonically increasing)
    pub backup_id: u64,
    /// Timestamp when backup was created (RFC3339 format)
    pub timestamp: String,
    /// Type of backup (full or incremental)
    pub backup_type: BackupType,
    /// Sequence number at backup time (for point-in-time recovery)
    pub sequence_number: u64,
    /// Total size of backup in bytes
    pub size_bytes: u64,
    /// Number of files in backup
    pub file_count: usize,
    /// List of SST files in this backup
    pub sst_files: Vec<SstFileInfo>,
    /// Manifest filename at backup time
    pub manifest_path: String,
    /// Optional user-provided description
    #[serde(skip_serializing_if = "Option::is_none")]
    pub description: Option<String>,
}

/// Options for creating a backup.
#[derive(Debug, Clone)]
pub struct BackupOptions {
    /// Type of backup to create
    pub backup_type: BackupType,
    /// Optional description for the backup
    pub description: Option<String>,
    /// Whether to verify the backup after creation
    pub verify_after_create: bool,
}

impl Default for BackupOptions {
    fn default() -> Self {
        Self {
            backup_type: BackupType::Full,
            description: None,
            verify_after_create: true,
        }
    }
}

/// Options for restoring a backup.
#[derive(Debug, Clone)]
pub struct RestoreOptions {
    /// Whether to verify backup integrity before restore
    pub verify_before_restore: bool,
    /// Whether to overwrite existing database
    pub overwrite_existing: bool,
}

impl Default for RestoreOptions {
    fn default() -> Self {
        Self {
            verify_before_restore: true,
            overwrite_existing: false,
        }
    }
}

/// Result of backup verification.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum VerifyResult {
    /// Backup is valid and complete
    Valid,
    /// Backup has errors (missing files, checksum mismatches, etc.)
    Invalid { errors: Vec<String> },
}

impl VerifyResult {
    /// Returns true if the backup is valid
    pub fn is_valid(&self) -> bool {
        matches!(self, VerifyResult::Valid)
    }

    /// Returns the list of errors, if any
    pub fn errors(&self) -> Option<&[String]> {
        match self {
            VerifyResult::Valid => None,
            VerifyResult::Invalid { errors } => Some(errors),
        }
    }
}

/// Engine for creating and managing backups.
///
/// # Example
/// ```no_run
/// use cntryl_midge::backup::{BackupEngine, BackupOptions, BackupType};
/// use std::path::Path;
///
/// let db_path = Path::new("/path/to/db");
/// let backup_dir = Path::new("/path/to/backups");
///
/// let mut engine = BackupEngine::open(db_path, backup_dir)?;
///
/// // Create full backup
/// let opts = BackupOptions::default();
/// let info = engine.create_backup(opts)?;
/// println!("Created backup {}", info.backup_id);
///
/// // Create incremental backup
/// let opts = BackupOptions {
///     backup_type: BackupType::Incremental { since_backup_id: info.backup_id },
///     ..Default::default()
/// };
/// let info2 = engine.create_backup(opts)?;
/// # Ok::<(), cntryl_midge::error::MidgeError>(())
/// ```
pub struct BackupEngine {
    /// Path to the database directory
    db_path: PathBuf,
    /// Path to backup storage directory
    backup_dir: PathBuf,
    /// Next backup ID to assign
    next_backup_id: u64,
}

impl BackupEngine {
    /// Open a backup engine for the given database.
    ///
    /// Creates the backup directory if it doesn't exist.
    pub fn open(db_path: impl AsRef<Path>, backup_dir: impl AsRef<Path>) -> MidgeResult<Self> {
        let db_path = db_path.as_ref().to_path_buf();
        let backup_dir = backup_dir.as_ref().to_path_buf();

        // Create backup directory if needed
        std::fs::create_dir_all(&backup_dir)?;

        // Determine next backup ID by scanning existing backups
        let next_backup_id = Self::find_max_backup_id(&backup_dir)? + 1;

        Ok(Self {
            db_path,
            backup_dir,
            next_backup_id,
        })
    }

    /// Create a new backup with the given options.
    pub fn create_backup(&mut self, opts: BackupOptions) -> MidgeResult<BackupInfo> {
        let backup_id = self.next_backup_id;
        self.next_backup_id += 1;

        let backup_path = self.backup_path(backup_id);
        std::fs::create_dir_all(&backup_path)?;

        // Load current manifest to get sequence number and file list
        let manifest = crate::manifest::Manifest::load(&self.db_path)?;
        let sequence_number = manifest.last_persisted_sequence;

        // Collect SST files to backup
        let sst_dir = self.db_path.join("sst");
        let mut sst_files = Vec::new();
        let mut total_size = 0u64;

        match opts.backup_type {
            BackupType::Full => {
                // Backup all SST files
                for file_meta in &manifest.files {
                    let sst_path = sst_dir.join(&file_meta.name);
                    if !sst_path.exists() {
                        continue; // Skip missing files
                    }

                    let size_bytes = std::fs::metadata(&sst_path)?.len();
                    let checksum = Self::compute_checksum(&sst_path)?;
                    let key_range = file_meta.smallest_key.as_ref().and_then(|sk| {
                        file_meta
                            .largest_key
                            .as_ref()
                            .map(|lk| (sk.clone(), lk.clone()))
                    });

                    sst_files.push(SstFileInfo {
                        name: file_meta.name.clone(),
                        size_bytes,
                        checksum,
                        key_range,
                    });

                    // Copy file to backup
                    let dest_path = backup_path.join(&file_meta.name);
                    std::fs::copy(&sst_path, &dest_path)?;

                    total_size += size_bytes;
                }
            }
            BackupType::Incremental { since_backup_id } => {
                // For incremental, only copy files newer than the base backup
                let base_info = self.read_backup_info(since_backup_id)?;
                let base_files: std::collections::HashSet<String> =
                    base_info.sst_files.iter().map(|f| f.name.clone()).collect();

                for file_meta in &manifest.files {
                    // Skip files that exist in base backup
                    if base_files.contains(&file_meta.name) {
                        continue;
                    }

                    let sst_path = sst_dir.join(&file_meta.name);
                    if !sst_path.exists() {
                        continue;
                    }

                    let size_bytes = std::fs::metadata(&sst_path)?.len();
                    let checksum = Self::compute_checksum(&sst_path)?;
                    let key_range = file_meta.smallest_key.as_ref().and_then(|sk| {
                        file_meta
                            .largest_key
                            .as_ref()
                            .map(|lk| (sk.clone(), lk.clone()))
                    });

                    sst_files.push(SstFileInfo {
                        name: file_meta.name.clone(),
                        size_bytes,
                        checksum,
                        key_range,
                    });

                    // Copy file to backup
                    let dest_path = backup_path.join(&file_meta.name);
                    std::fs::copy(&sst_path, &dest_path)?;

                    total_size += size_bytes;
                }
            }
        }

        // Copy manifest
        let manifest_src = self.db_path.join("manifest.json");
        let manifest_dest = backup_path.join("manifest.json");
        if manifest_src.exists() {
            std::fs::copy(&manifest_src, &manifest_dest)?;
            let manifest_size = std::fs::metadata(&manifest_src)?.len();
            total_size += manifest_size;
        } else {
            // Create empty manifest if none exists
            let empty_manifest = crate::manifest::Manifest::default();
            let manifest_json = serde_json::to_vec_pretty(&empty_manifest)?;
            std::fs::write(&manifest_dest, manifest_json)?;
        }

        // Create backup metadata
        let backup_info = BackupInfo {
            backup_id,
            timestamp: chrono::Utc::now().to_rfc3339(),
            backup_type: opts.backup_type,
            sequence_number,
            size_bytes: total_size,
            file_count: sst_files.len() + 1, // +1 for manifest
            sst_files,
            manifest_path: "manifest.json".to_string(),
            description: opts.description,
        };

        // Write backup metadata
        let meta_path = backup_path.join("BACKUP_META.json");
        let meta_json = serde_json::to_string_pretty(&backup_info)?;
        std::fs::write(meta_path, meta_json)?;

        // Verify if requested
        if opts.verify_after_create {
            let verify_result = self.verify_backup(backup_id)?;
            if !verify_result.is_valid() {
                return Err(crate::error::MidgeError::Corruption {
                    message: format!("Backup verification failed: {:?}", verify_result.errors()),
                });
            }
        }

        Ok(backup_info)
    }

    /// List all available backups, sorted by backup ID.
    pub fn list_backups(&self) -> MidgeResult<Vec<BackupInfo>> {
        let mut backups = Vec::new();

        if !self.backup_dir.exists() {
            return Ok(backups);
        }

        for entry in std::fs::read_dir(&self.backup_dir)? {
            let entry = entry?;
            let name = entry.file_name();
            let name_str = name.to_string_lossy();

            if let Some(id_str) = name_str.strip_prefix("backup_") {
                if let Ok(backup_id) = id_str.parse::<u64>() {
                    if let Ok(info) = self.read_backup_info(backup_id) {
                        backups.push(info);
                    }
                }
            }
        }

        backups.sort_by_key(|b| b.backup_id);
        Ok(backups)
    }

    /// Verify a backup's integrity.
    pub fn verify_backup(&self, backup_id: u64) -> MidgeResult<VerifyResult> {
        let backup_path = self.backup_path(backup_id);
        let mut errors = Vec::new();

        // Check backup directory exists
        if !backup_path.exists() {
            errors.push(format!(
                "Backup directory does not exist: {:?}",
                backup_path
            ));
            return Ok(VerifyResult::Invalid { errors });
        }

        // Read and parse metadata
        let info = match self.read_backup_info(backup_id) {
            Ok(info) => info,
            Err(e) => {
                errors.push(format!("Failed to read backup metadata: {}", e));
                return Ok(VerifyResult::Invalid { errors });
            }
        };

        // Verify manifest exists
        let manifest_path = backup_path.join(&info.manifest_path);
        if !manifest_path.exists() {
            errors.push(format!("Manifest file missing: {}", info.manifest_path));
        }

        // Verify each SST file
        for sst_info in &info.sst_files {
            let sst_path = backup_path.join(&sst_info.name);

            if !sst_path.exists() {
                errors.push(format!("SST file missing: {}", sst_info.name));
                continue;
            }

            // Verify file size
            match std::fs::metadata(&sst_path) {
                Ok(meta) => {
                    let actual_size = meta.len();
                    if actual_size != sst_info.size_bytes {
                        errors.push(format!(
                            "SST file size mismatch for {}: expected {}, got {}",
                            sst_info.name, sst_info.size_bytes, actual_size
                        ));
                    }
                }
                Err(e) => {
                    errors.push(format!(
                        "Failed to read metadata for {}: {}",
                        sst_info.name, e
                    ));
                    continue;
                }
            }

            // Verify checksum
            match Self::compute_checksum(&sst_path) {
                Ok(checksum) => {
                    if checksum != sst_info.checksum {
                        errors.push(format!(
                            "Checksum mismatch for {}: expected {}, got {}",
                            sst_info.name, sst_info.checksum, checksum
                        ));
                    }
                }
                Err(e) => {
                    errors.push(format!(
                        "Failed to compute checksum for {}: {}",
                        sst_info.name, e
                    ));
                }
            }
        }

        // Verify incremental backup chain if applicable
        if let BackupType::Incremental { since_backup_id } = info.backup_type {
            let base_path = self.backup_path(since_backup_id);
            if !base_path.exists() {
                errors.push(format!(
                    "Base backup {} not found for incremental backup",
                    since_backup_id
                ));
            }
        }

        if errors.is_empty() {
            Ok(VerifyResult::Valid)
        } else {
            Ok(VerifyResult::Invalid { errors })
        }
    }

    /// Delete old backups, keeping only the most recent `keep_count`.
    pub fn purge_old_backups(&mut self, keep_count: usize) -> MidgeResult<Vec<u64>> {
        let mut backups = self.list_backups()?;
        backups.sort_by_key(|b| b.backup_id);

        if backups.len() <= keep_count {
            return Ok(Vec::new());
        }

        let to_delete_count = backups.len() - keep_count;
        let mut deleted = Vec::new();

        for backup in backups.iter().take(to_delete_count) {
            let backup_path = self.backup_path(backup.backup_id);

            // Check if any newer backup depends on this one
            let is_depended_on = backups.iter().skip(to_delete_count).any(|b| {
                matches!(b.backup_type, BackupType::Incremental { since_backup_id }
                    if since_backup_id == backup.backup_id)
            });

            if is_depended_on {
                // Skip deletion - this backup is needed for incremental chain
                continue;
            }

            // Delete backup directory
            if backup_path.exists() {
                std::fs::remove_dir_all(&backup_path)?;
                deleted.push(backup.backup_id);
            }
        }

        Ok(deleted)
    }

    /// Get the path to a specific backup directory.
    pub fn backup_path(&self, backup_id: u64) -> PathBuf {
        self.backup_dir.join(format!("backup_{:06}", backup_id))
    }

    /// Compute CRC32 checksum of a file.
    fn compute_checksum(path: &Path) -> MidgeResult<u32> {
        use std::io::Read;
        let mut file = std::fs::File::open(path)?;
        let mut hasher = crc32fast::Hasher::new();
        let mut buffer = [0u8; 8192];

        loop {
            let n = file.read(&mut buffer)?;
            if n == 0 {
                break;
            }
            hasher.update(&buffer[..n]);
        }

        Ok(hasher.finalize())
    }

    /// Read backup info from metadata file.
    fn read_backup_info(&self, backup_id: u64) -> MidgeResult<BackupInfo> {
        let backup_path = self.backup_path(backup_id);
        let meta_path = backup_path.join("BACKUP_META.json");

        let contents = std::fs::read_to_string(meta_path)?;
        let info: BackupInfo = serde_json::from_str(&contents)?;

        Ok(info)
    }

    /// Find the maximum backup ID in the backup directory.
    fn find_max_backup_id(backup_dir: &Path) -> MidgeResult<u64> {
        let mut max_id = 0u64;

        if !backup_dir.exists() {
            return Ok(max_id);
        }

        for entry in std::fs::read_dir(backup_dir)? {
            let entry = entry?;
            let name = entry.file_name();
            let name_str = name.to_string_lossy();

            // Parse "backup_NNNNNN" format
            if let Some(id_str) = name_str.strip_prefix("backup_") {
                if let Ok(id) = id_str.parse::<u64>() {
                    max_id = max_id.max(id);
                }
            }
        }

        Ok(max_id)
    }
}

/// Engine for restoring backups.
///
/// # Example
/// ```no_run
/// use cntryl_midge::backup::{RestoreEngine, RestoreOptions};
/// use std::path::Path;
///
/// let backup_dir = Path::new("/path/to/backups");
/// let target_dir = Path::new("/path/to/restore");
///
/// let engine = RestoreEngine::new(backup_dir);
///
/// // Restore latest backup
/// let opts = RestoreOptions::default();
/// engine.restore_latest(target_dir, opts)?;
/// # Ok::<(), cntryl_midge::error::MidgeError>(())
/// ```
pub struct RestoreEngine {
    /// Path to backup storage directory
    backup_dir: PathBuf,
}

impl RestoreEngine {
    /// Create a new restore engine.
    pub fn new(backup_dir: impl AsRef<Path>) -> Self {
        Self {
            backup_dir: backup_dir.as_ref().to_path_buf(),
        }
    }

    /// Restore a specific backup to the target directory.
    pub fn restore_backup(
        &self,
        backup_id: u64,
        target_dir: impl AsRef<Path>,
        opts: RestoreOptions,
    ) -> MidgeResult<()> {
        let target_dir = target_dir.as_ref();

        // Check if target exists
        if target_dir.exists() && !opts.overwrite_existing {
            return Err(crate::error::MidgeError::InvalidData(format!(
                "Target directory already exists: {:?}",
                target_dir
            )));
        }

        // Read backup info
        let info = self.read_backup_info(backup_id)?;

        // Verify backup if requested
        if opts.verify_before_restore {
            let backup_engine = crate::backup::BackupEngine::open(target_dir, &self.backup_dir)?;
            let verify_result = backup_engine.verify_backup(backup_id)?;
            if !verify_result.is_valid() {
                return Err(crate::error::MidgeError::Corruption {
                    message: format!("Backup verification failed: {:?}", verify_result.errors()),
                });
            }
        }

        // Create target directories
        std::fs::create_dir_all(target_dir)?;
        let sst_dir = target_dir.join("sst");
        std::fs::create_dir_all(&sst_dir)?;

        // For incremental backups, need to restore base backup first
        if let BackupType::Incremental { since_backup_id } = info.backup_type {
            self.restore_backup(since_backup_id, target_dir, opts.clone())?;
        }

        // Copy SST files
        let backup_path = self.backup_dir.join(format!("backup_{:06}", backup_id));
        for sst_info in &info.sst_files {
            let src = backup_path.join(&sst_info.name);
            let dest = sst_dir.join(&sst_info.name);
            std::fs::copy(&src, &dest)?;
        }

        // Copy manifest
        let manifest_src = backup_path.join(&info.manifest_path);
        let manifest_dest = target_dir.join(&info.manifest_path);
        std::fs::copy(&manifest_src, &manifest_dest)?;

        // Create CURRENT file
        std::fs::write(target_dir.join("CURRENT"), info.manifest_path.as_bytes())?;

        Ok(())
    }

    /// Restore the most recent backup to the target directory.
    pub fn restore_latest(
        &self,
        target_dir: impl AsRef<Path>,
        opts: RestoreOptions,
    ) -> MidgeResult<()> {
        let latest_id = self
            .find_latest_backup_id()?
            .ok_or_else(|| crate::error::MidgeError::InvalidData("No backups found".to_string()))?;

        self.restore_backup(latest_id, target_dir, opts)
    }

    /// Get backup info by reading its metadata file.
    fn read_backup_info(&self, backup_id: u64) -> MidgeResult<BackupInfo> {
        let backup_path = self.backup_dir.join(format!("backup_{:06}", backup_id));
        let meta_path = backup_path.join("BACKUP_META.json");

        let contents = std::fs::read_to_string(meta_path)?;
        let info: BackupInfo = serde_json::from_str(&contents)?;

        Ok(info)
    }

    /// Find the latest backup ID.
    fn find_latest_backup_id(&self) -> MidgeResult<Option<u64>> {
        let mut max_id = None;

        if !self.backup_dir.exists() {
            return Ok(None);
        }

        for entry in std::fs::read_dir(&self.backup_dir)? {
            let entry = entry?;
            let name = entry.file_name();
            let name_str = name.to_string_lossy();

            if let Some(id_str) = name_str.strip_prefix("backup_") {
                if let Ok(id) = id_str.parse::<u64>() {
                    max_id = Some(max_id.unwrap_or(0).max(id));
                }
            }
        }

        Ok(max_id)
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn should_create_default_backup_options_with_full_type() {
        // Arrange
        // Act
        let opts = BackupOptions::default();

        // Assert
        assert_eq!(opts.backup_type, BackupType::Full);
        assert!(opts.description.is_none());
        assert!(opts.verify_after_create);
    }

    #[test]
    fn should_create_default_restore_options_with_verification() {
        // Arrange
        // Act
        let opts = RestoreOptions::default();

        // Assert
        assert!(opts.verify_before_restore);
        assert!(!opts.overwrite_existing);
    }

    #[test]
    fn should_compare_full_backup_types_for_equality() {
        // Arrange
        let full1 = BackupType::Full;
        let full2 = BackupType::Full;

        // Act
        let is_equal = full1 == full2;

        // Assert
        assert!(is_equal);
    }

    #[test]
    fn should_detect_full_and_incremental_are_not_equal() {
        // Arrange
        let full = BackupType::Full;
        let incremental = BackupType::Incremental {
            since_backup_id: 10,
        };

        // Act
        let is_equal = full == incremental;

        // Assert
        assert!(!is_equal);
    }

    #[test]
    fn should_compare_incremental_backup_types_with_same_id_for_equality() {
        // Arrange
        let incremental1 = BackupType::Incremental {
            since_backup_id: 10,
        };
        let incremental2 = BackupType::Incremental {
            since_backup_id: 10,
        };

        // Act
        let is_equal = incremental1 == incremental2;

        // Assert
        assert!(is_equal);
    }

    #[test]
    fn should_detect_incremental_backup_types_with_different_ids_are_not_equal() {
        // Arrange
        let incremental1 = BackupType::Incremental {
            since_backup_id: 10,
        };
        let incremental2 = BackupType::Incremental {
            since_backup_id: 20,
        };

        // Act
        let is_equal = incremental1 == incremental2;

        // Assert
        assert!(!is_equal);
    }

    #[test]
    fn should_serialize_full_backup_type() {
        // Arrange
        let backup_type = BackupType::Full;

        // Act
        let json = serde_json::to_string(&backup_type).expect("serialize failed");

        // Assert
        assert!(json.contains("Full"));
    }

    #[test]
    fn should_deserialize_full_backup_type() {
        // Arrange
        let backup_type = BackupType::Full;
        let json = serde_json::to_string(&backup_type).expect("serialize failed");

        // Act
        let deserialized: BackupType = serde_json::from_str(&json).expect("deserialize failed");

        // Assert
        assert_eq!(deserialized, backup_type);
    }

    #[test]
    fn should_serialize_incremental_backup_type() {
        // Arrange
        let backup_type = BackupType::Incremental {
            since_backup_id: 42,
        };

        // Act
        let json = serde_json::to_string(&backup_type).expect("serialize failed");

        // Assert
        assert!(json.contains("42"));
    }

    #[test]
    fn should_deserialize_incremental_backup_type() {
        // Arrange
        let backup_type = BackupType::Incremental {
            since_backup_id: 42,
        };
        let json = serde_json::to_string(&backup_type).expect("serialize failed");

        // Act
        let deserialized: BackupType = serde_json::from_str(&json).expect("deserialize failed");

        // Assert
        assert_eq!(deserialized, backup_type);
    }

    #[test]
    fn should_serialize_sst_file_info() {
        // Arrange
        let sst_info = SstFileInfo {
            name: "000042.sst".to_string(),
            size_bytes: 1024,
            checksum: 0x12345678,
            key_range: Some((b"key1".to_vec(), b"key9".to_vec())),
        };

        // Act
        let result = serde_json::to_string(&sst_info);

        // Assert
        assert!(result.is_ok());
    }

    #[test]
    fn should_deserialize_sst_file_info() {
        // Arrange
        let sst_info = SstFileInfo {
            name: "000042.sst".to_string(),
            size_bytes: 1024,
            checksum: 0x12345678,
            key_range: Some((b"key1".to_vec(), b"key9".to_vec())),
        };
        let json = serde_json::to_string(&sst_info).expect("serialize failed");

        // Act
        let deserialized: SstFileInfo = serde_json::from_str(&json).expect("deserialize failed");

        // Assert
        assert_eq!(deserialized.name, sst_info.name);
        assert_eq!(deserialized.size_bytes, sst_info.size_bytes);
        assert_eq!(deserialized.checksum, sst_info.checksum);
        assert_eq!(deserialized.key_range, sst_info.key_range);
    }

    #[test]
    fn should_skip_serializing_none_key_range_in_sst_file_info() {
        // Arrange
        let sst_info = SstFileInfo {
            name: "test.sst".to_string(),
            size_bytes: 512,
            checksum: 0xABCD,
            key_range: None,
        };

        // Act
        let json = serde_json::to_string(&sst_info).expect("serialize failed");

        // Assert
        assert!(!json.contains("key_range"));
    }

    #[test]
    fn should_serialize_backup_info() {
        // Arrange
        let backup_info = BackupInfo {
            backup_id: 1,
            timestamp: "2025-10-25T10:00:00Z".to_string(),
            backup_type: BackupType::Full,
            sequence_number: 12345,
            size_bytes: 1048576,
            file_count: 10,
            sst_files: vec![SstFileInfo {
                name: "000001.sst".to_string(),
                size_bytes: 1024,
                checksum: 0x11111111,
                key_range: None,
            }],
            manifest_path: "manifest.json".to_string(),
            description: Some("Test backup".to_string()),
        };

        // Act
        let result = serde_json::to_string(&backup_info);

        // Assert
        assert!(result.is_ok());
    }

    #[test]
    fn should_deserialize_backup_info() {
        // Arrange
        let backup_info = BackupInfo {
            backup_id: 1,
            timestamp: "2025-10-25T10:00:00Z".to_string(),
            backup_type: BackupType::Full,
            sequence_number: 12345,
            size_bytes: 1048576,
            file_count: 10,
            sst_files: vec![SstFileInfo {
                name: "000001.sst".to_string(),
                size_bytes: 1024,
                checksum: 0x11111111,
                key_range: None,
            }],
            manifest_path: "manifest.json".to_string(),
            description: Some("Test backup".to_string()),
        };
        let json = serde_json::to_string(&backup_info).expect("serialize failed");

        // Act
        let deserialized: BackupInfo = serde_json::from_str(&json).expect("deserialize failed");

        // Assert
        assert_eq!(deserialized.backup_id, backup_info.backup_id);
        assert_eq!(deserialized.sequence_number, backup_info.sequence_number);
        assert_eq!(deserialized.size_bytes, backup_info.size_bytes);
        assert_eq!(deserialized.file_count, backup_info.file_count);
        assert_eq!(deserialized.sst_files.len(), 1);
    }

    #[test]
    fn should_return_true_given_valid_result_when_checking_is_valid() {
        // Arrange
        let result = VerifyResult::Valid;

        // Act
        let is_valid = result.is_valid();

        // Assert
        assert!(is_valid);
    }

    #[test]
    fn should_return_false_given_invalid_result_when_checking_is_valid() {
        // Arrange
        let result = VerifyResult::Invalid {
            errors: vec!["checksum mismatch".to_string()],
        };

        // Act
        let is_valid = result.is_valid();

        // Assert
        assert!(!is_valid);
    }

    #[test]
    fn should_return_none_given_valid_result_when_getting_errors() {
        // Arrange
        let result = VerifyResult::Valid;

        // Act
        let errors = result.errors();

        // Assert
        assert!(errors.is_none());
    }

    #[test]
    fn should_return_errors_given_invalid_result_when_getting_errors() {
        // Arrange
        let error_msgs = vec!["file missing".to_string(), "checksum failed".to_string()];
        let result = VerifyResult::Invalid {
            errors: error_msgs.clone(),
        };

        // Act
        let errors = result.errors();

        // Assert
        assert!(errors.is_some());
        assert_eq!(errors.unwrap(), &error_msgs);
    }

    #[test]
    fn should_clone_backup_options() {
        // Arrange
        let opts = BackupOptions {
            backup_type: BackupType::Incremental { since_backup_id: 5 },
            description: Some("test".to_string()),
            verify_after_create: false,
        };

        // Act
        let cloned = opts.clone();

        // Assert
        assert_eq!(cloned.backup_type, opts.backup_type);
        assert_eq!(cloned.description, opts.description);
        assert_eq!(cloned.verify_after_create, opts.verify_after_create);
    }

    #[test]
    fn should_clone_restore_options() {
        // Arrange
        let opts = RestoreOptions {
            verify_before_restore: false,
            overwrite_existing: true,
        };

        // Act
        let cloned = opts.clone();

        // Assert
        assert_eq!(cloned.verify_before_restore, opts.verify_before_restore);
        assert_eq!(cloned.overwrite_existing, opts.overwrite_existing);
    }
}
