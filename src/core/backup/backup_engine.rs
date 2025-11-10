///! Backup creation and management engine.

use std::path::{Path, PathBuf};

use crate::error::MidgeResult;

use super::types::{BackupInfo, BackupOptions, BackupType, SstFileInfo, VerifyResult};

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
