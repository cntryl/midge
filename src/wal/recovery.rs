//! WAL recovery - replay WAL segments to restore state after crash
//!
//! On startup, all persistent WAL segments are replayed to reconstruct the
//! memtables for each column family. This ensures durability: any operation
//! written to WAL before crash is recovered.

use super::traits::WalReader;
use super::types::WalRecord;
use crate::common::MidgeResult;
use crate::sst::{Memtable, SkipListMemtable};
use std::collections::HashMap;
use std::path::Path;
use std::sync::Arc;

/// Statistics from WAL recovery
#[derive(Debug, Clone)]
pub struct RecoveryStats {
    pub records_recovered: usize,
    pub bytes_recovered: u64,
    pub had_corruption: bool,
}

impl RecoveryStats {
    pub fn new() -> Self {
        Self {
            records_recovered: 0,
            bytes_recovered: 0,
            had_corruption: false,
        }
    }
}

impl Default for RecoveryStats {
    fn default() -> Self {
        Self::new()
    }
}

/// Replay WAL segments and restore column family memtables
///
/// Reads all WAL files in the directory and replays them sequentially,
/// reconstructing the memtables for each column family.
pub fn replay_wal(
    wal_dir: &Path,
    memtables: &mut HashMap<u32, Arc<SkipListMemtable>>,
) -> MidgeResult<RecoveryStats> {
    let mut stats = RecoveryStats::new();

    // Check if WAL directory exists
    if !wal_dir.exists() {
        tracing::info!("WAL directory does not exist, skipping recovery");
        return Ok(stats);
    }

    // List all WAL files
    let wal_files = list_wal_files(wal_dir)?;

    if wal_files.is_empty() {
        tracing::debug!("No WAL files found, recovery complete");
        return Ok(stats);
    }

    tracing::info!(file_count = wal_files.len(), "Starting WAL recovery");

    // Replay each WAL file in order
    for wal_file in wal_files {
        let file_path = wal_dir.join(&wal_file);
        match replay_wal_file(&file_path, memtables, &mut stats) {
            Ok(_) => {
                tracing::debug!(file = %wal_file, records = stats.records_recovered, "WAL file replayed");
            }
            Err(e) => {
                tracing::warn!(file = %wal_file, error = %e, "Error replaying WAL file");
                stats.had_corruption = true;
                // Continue with next file
            }
        }
    }

    tracing::info!(
        records_recovered = stats.records_recovered,
        bytes_recovered = stats.bytes_recovered,
        had_corruption = stats.had_corruption,
        "WAL recovery complete"
    );

    Ok(stats)
}

/// Replay a single WAL file
fn replay_wal_file(
    file_path: &Path,
    memtables: &mut HashMap<u32, Arc<SkipListMemtable>>,
    stats: &mut RecoveryStats,
) -> MidgeResult<()> {
    // Open WAL file for reading
    let mut reader = super::fs::FsWalReader::new(file_path.parent().expect("WAL file path has no parent directory"))?;

    // Replay all records from start of file
    reader.replay(0, |record: &WalRecord| {
        // Get or create memtable for this column family
        let memtable = memtables
            .entry(record.cf_id)
            .or_insert_with(|| Arc::new(SkipListMemtable::new()));

        // Apply the record to the memtable
        match record.op {
            super::types::WalOpKind::Put => {
                if let Some(value) = &record.value {
                    memtable.put(record.key.to_vec(), value.to_vec())?;
                }
            }
            super::types::WalOpKind::Delete => {
                memtable.delete(record.key.to_vec())?;
            }
            super::types::WalOpKind::DeleteRange => {
                // TODO: Implement delete range in memtable
            }
            super::types::WalOpKind::Insert => {
                // Insert is treated same as Put in recovery
                if let Some(value) = &record.value {
                    memtable.put(record.key.to_vec(), value.to_vec())?;
                }
            }
            super::types::WalOpKind::Merge => {
                // TODO: Implement merge operator in memtable
            }
            super::types::WalOpKind::TxnBegin => {
                // Transaction markers are metadata only
            }
            super::types::WalOpKind::TxnCommit => {
                // Transaction markers are metadata only
            }
        }

        // Update stats
        stats.records_recovered += 1;
        stats.bytes_recovered += record.key.len() as u64;
        if let Some(value) = &record.value {
            stats.bytes_recovered += value.len() as u64;
        }

        Ok(())
    })?;

    Ok(())
}

/// List all WAL files in directory, sorted by name (which gives chronological order)
fn list_wal_files(wal_dir: &Path) -> MidgeResult<Vec<String>> {
    let mut files = Vec::new();

    for entry in std::fs::read_dir(wal_dir)? {
        let entry = entry?;
        let path = entry.path();

        // Only include .wal or .log files
        if let Some(extension) = path.extension() {
            if extension == "wal" || extension == "log" {
                if let Some(file_name) = path.file_name() {
                    if let Some(name_str) = file_name.to_str() {
                        files.push(name_str.to_string());
                    }
                }
            }
        }
    }

    // Sort files for deterministic replay order
    files.sort();

    Ok(files)
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::wal::fs::FsWalWriter;
    use crate::wal::types::WalOpKind;
    use crate::wal::WalWriter;
    use bytes::Bytes;

    #[test]
    fn should_initialize_stats_with_zeros_when_created() {
        // Arrange
        // Act
        let stats = RecoveryStats::new();

        // Assert
        assert_eq!(stats.records_recovered, 0);
        assert_eq!(stats.bytes_recovered, 0);
        assert!(!stats.had_corruption);
    }

    #[test]
    fn should_return_empty_stats_when_wal_directory_missing() {
        // Arrange
        let mut memtables = HashMap::new();
        let non_existent = std::env::temp_dir().join("midge_nonexistent_wal_dir_12345");

        // Act
        let stats = replay_wal(&non_existent, &mut memtables).unwrap();

        // Assert
        assert_eq!(stats.records_recovered, 0);
    }

    #[test]
    fn should_recover_put_operations_when_replaying_wal() {
        // Arrange
        let temp_dir = std::env::temp_dir().join("midge_recovery_test_put");
        let wal_dir = temp_dir.join("wal");
        std::fs::create_dir_all(&wal_dir).ok();

        // Write WAL with a put operation
        {
            let writer = FsWalWriter::new(&wal_dir).unwrap();
            let record = WalRecord::new(
                WalOpKind::Put,
                Bytes::from_static(b"test_key"),
                Some(Bytes::from_static(b"test_value")),
                1,
            );
            writer.append_record(&record).unwrap();
            writer.sync().unwrap();
        }

        // Perform recovery
        let mut memtables = HashMap::new();
        let stats = replay_wal(&wal_dir, &mut memtables).unwrap();

        // Assert
        assert!(stats.records_recovered > 0);
        assert!(memtables.contains_key(&0)); // Default CF
        let recovered_memtable = &memtables[&0];
        let value = recovered_memtable.get(b"test_key").unwrap();
        assert_eq!(value, Some(b"test_value".to_vec()));
    }

    #[test]
    fn should_recover_delete_operations_when_replaying_wal() {
        // Arrange
        let temp_dir =
            std::env::temp_dir().join(format!("midge_recovery_test_delete_{}", std::process::id()));
        let wal_dir = temp_dir.join("wal");
        let _ = std::fs::remove_dir_all(&wal_dir);
        std::fs::create_dir_all(&wal_dir).ok();

        // Write WAL with put then delete
        {
            let writer = FsWalWriter::new(&wal_dir).unwrap();
            let put_record = WalRecord::new(
                WalOpKind::Put,
                Bytes::from_static(b"test_key"),
                Some(Bytes::from_static(b"test_value")),
                1,
            );
            writer.append_record(&put_record).unwrap();

            let delete_record =
                WalRecord::new(WalOpKind::Delete, Bytes::from_static(b"test_key"), None, 2);
            writer.append_record(&delete_record).unwrap();
            writer.sync().unwrap();
        }

        // Perform recovery
        let mut memtables = HashMap::new();
        let stats = replay_wal(&wal_dir, &mut memtables).unwrap();

        // Assert
        assert_eq!(
            stats.records_recovered, 2,
            "Should recover exactly 2 records"
        );
        let recovered_memtable = &memtables[&0];
        let value = recovered_memtable.get(b"test_key").unwrap();
        assert_eq!(value, None); // Key should be deleted
    }

    #[test]
    fn should_handle_multiple_column_families_when_recovering() {
        // Arrange
        let temp_dir =
            std::env::temp_dir().join(format!("midge_recovery_test_cf_{}", std::process::id()));
        let wal_dir = temp_dir.join("wal");
        let _ = std::fs::remove_dir_all(&wal_dir);
        std::fs::create_dir_all(&wal_dir).ok();

        // Write WAL with records for multiple CFs
        {
            let writer = FsWalWriter::new(&wal_dir).unwrap();

            let record_cf0 = WalRecord::new_cf(
                0,
                WalOpKind::Put,
                Bytes::from_static(b"key0"),
                Some(Bytes::from_static(b"value0")),
                1,
            );
            writer.append_record(&record_cf0).unwrap();

            let record_cf1 = WalRecord::new_cf(
                1,
                WalOpKind::Put,
                Bytes::from_static(b"key1"),
                Some(Bytes::from_static(b"value1")),
                2,
            );
            writer.append_record(&record_cf1).unwrap();
            writer.sync().unwrap();
        }

        // Perform recovery
        let mut memtables = HashMap::new();
        let stats = replay_wal(&wal_dir, &mut memtables).unwrap();

        // Assert
        assert_eq!(
            stats.records_recovered, 2,
            "Should recover exactly 2 records"
        );
        assert_eq!(memtables.len(), 2, "Should have 2 column families");
        assert!(memtables[&0].get(b"key0").unwrap().is_some());
        assert!(memtables[&1].get(b"key1").unwrap().is_some());
    }
}
