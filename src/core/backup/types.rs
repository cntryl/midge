/// Common types and options for backup and restore operations.

use serde::{Deserialize, Serialize};

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

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn should_return_true_when_verify_result_is_valid() {
        // Arrange
        let result = VerifyResult::Valid;

        // Act & Assert
        assert!(result.is_valid());
        assert!(result.errors().is_none());
    }

    #[test]
    fn should_return_false_when_verify_result_has_errors() {
        // Arrange
        let result = VerifyResult::Invalid {
            errors: vec!["Error 1".to_string()],
        };

        // Act & Assert
        assert!(!result.is_valid());
        assert_eq!(result.errors().unwrap().len(), 1);
    }

    #[test]
    fn should_create_default_backup_options() {
        // Arrange & Act
        let opts = BackupOptions::default();

        // Assert
        assert!(matches!(opts.backup_type, BackupType::Full));
        assert!(opts.description.is_none());
        assert!(opts.verify_after_create);
    }

    #[test]
    fn should_create_default_restore_options() {
        // Arrange & Act
        let opts = RestoreOptions::default();

        // Assert
        assert!(opts.verify_before_restore);
        assert!(!opts.overwrite_existing);
    }
}
