/// Backup restoration and validation engine.
use std::path::{Path, PathBuf};

use crate::error::MidgeResult;

use super::types::{BackupInfo, BackupType, RestoreOptions};

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
