//! WAL recovery - replay WAL segments to restore state after crash
//!
//! On startup, all persistent WAL segments are replayed to reconstruct the
//! memtables for each column family. This ensures durability: any operation
//! written to WAL before crash is recovered.

use super::fs::FsWalReader;
use super::traits::WalReader;
use super::types::{ColumnFamilyId, WalOpKind, WalRecord};
use crate::common::{MidgeError, MidgeResult};
use crate::sst::{Memtable, SkipListMemtable};
use std::collections::HashMap;
use std::path::Path;
use std::sync::Arc;
use tracing::instrument;

/// Statistics from WAL recovery
#[derive(Debug, Clone)]
pub struct RecoveryStats {
    /// Total number of WAL records successfully replayed.
    pub record_count: u64,
    /// Total bytes consumed while replaying WAL (keys + values).
    pub bytes: u64,
    /// Whether any corruption was observed while scanning WAL.
    pub had_corruption: bool,
    /// Maximum sequence number encountered during recovery.
    /// The runtime should restore its sequence counter from this value.
    /// None if no records were recovered.
    pub max_sequence: Option<u64>,
}

impl Default for RecoveryStats {
    fn default() -> Self {
        Self::new()
    }
}

impl RecoveryStats {
    pub fn new() -> Self {
        Self {
            record_count: 0,
            bytes: 0,
            had_corruption: false,
            max_sequence: None,
        }
    }

    fn record(&mut self, record: &WalRecord) {
        self.record_count += 1;
        self.bytes += record.key.len() as u64;
        if let Some(value) = &record.value {
            self.bytes += value.len() as u64;
        }
        if let Some(range_end) = &record.range_end {
            self.bytes += range_end.len() as u64;
        }
        self.max_sequence = Some(self.max_sequence.unwrap_or(0).max(record.seq));
    }

    fn mark_corruption(&mut self) {
        self.had_corruption = true;
    }
}

/// Replay WAL files under `wal_dir`, rebuilding memtables per column family.
///
/// Returns aggregated recovery statistics. Caller is responsible for attaching
/// the recovered memtables to the runtime state.
#[instrument(level = "info", skip(memtables), fields(wal_dir = ?wal_dir))]
pub fn replay_wal(
    wal_dir: &Path,
    memtables: &mut HashMap<ColumnFamilyId, Arc<SkipListMemtable>>,
) -> MidgeResult<RecoveryStats> {
    let mut stats = RecoveryStats::new();

    // Transaction buffering for atomic recovery.
    //
    // Legacy records (without txn_id) are applied immediately.
    // Records tagged with txn_id are only applied if we observe a TxnCommit
    // for that txn_id after a TxnBegin.
    let mut open_txns: std::collections::HashMap<u64, Vec<WalRecord>> =
        std::collections::HashMap::new();
    let mut begun_txns: std::collections::HashSet<u64> = std::collections::HashSet::new();

    if !wal_dir.exists() {
        return Ok(stats);
    }

    tracing::info!(dir = ?wal_dir, "starting wal replay");

    let mut reader = match FsWalReader::new(wal_dir) {
        Ok(r) => r,
        Err(MidgeError::Io(e)) if e.kind() == std::io::ErrorKind::NotFound => return Ok(stats),
        Err(e) => return Err(e),
    };

    let result = reader.replay(0, |record| {
        // Always count records, even if buffered/ignored.
        stats.record(record);

        match record.op {
            WalOpKind::TxnBegin => {
                if let Some(txn_id) = record.txn_id {
                    begun_txns.insert(txn_id);
                    open_txns.entry(txn_id).or_default();
                }
                Ok(())
            }
            WalOpKind::TxnCommit => {
                if let Some(txn_id) = record.txn_id {
                    if begun_txns.remove(&txn_id) {
                        if let Some(records) = open_txns.remove(&txn_id) {
                            for buffered in &records {
                                apply_record(buffered, memtables)?;
                            }
                        }
                    }
                }
                Ok(())
            }
            _ => {
                if let Some(txn_id) = record.txn_id {
                    if begun_txns.contains(&txn_id) {
                        open_txns
                            .entry(txn_id)
                            .or_default()
                            .push(record.clone());
                        return Ok(());
                    }
                }

                apply_record(record, memtables)
            }
        }
    });

    match result {
        Ok(()) => {
            tracing::info!(
                dir = ?wal_dir,
                records = stats.record_count,
                bytes = stats.bytes,
                max_sequence = ?stats.max_sequence,
                had_corruption = stats.had_corruption,
                "wal replay completed"
            );
            Ok(stats)
        }
        Err(MidgeError::Corruption(e)) => {
            stats.mark_corruption();
            tracing::warn!(dir = ?wal_dir, error = %e, "wal replay encountered corruption");
            Err(MidgeError::Corruption(e))
        }
        Err(e) => {
            tracing::error!(dir = ?wal_dir, error = %e, "wal replay failed");
            Err(e)
        }
    }
}

#[instrument(
    level = "debug",
    skip(memtables, record),
    fields(cf_id = record.cf_id, seq = record.seq, op = ?record.op)
)]
fn apply_record(
    record: &WalRecord,
    memtables: &mut HashMap<ColumnFamilyId, Arc<SkipListMemtable>>,
) -> MidgeResult<()> {
    let memtable = memtables
        .entry(record.cf_id)
        .or_insert_with(|| Arc::new(SkipListMemtable::new()));

    match record.op {
        WalOpKind::Put | WalOpKind::Insert => {
            // Skip expired entries during recovery
            if let Some(exp) = record.expiration {
                let now = std::time::SystemTime::now()
                    .duration_since(std::time::UNIX_EPOCH)
                    .map(|d| d.as_millis() as u64)
                    .unwrap_or(0);
                if exp <= now {
                    return Ok(());
                }
            }

            if let Some(value) = &record.value {
                memtable.put_with_exp(record.key.to_vec(), value.to_vec(), record.expiration)?;
            }
        }
        WalOpKind::Delete => {
            memtable.delete(record.key.to_vec())?;
        }
        WalOpKind::DeleteRange => {
            // TODO: range tombstone support; treat as no-op for now.
        }
        WalOpKind::Merge => {
            // Merge operators are not applied during recovery yet.
        }
        WalOpKind::TxnBegin | WalOpKind::TxnCommit => {
            // Transaction markers carry no direct memtable mutation.
        }
    }

    Ok(())
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
        let stats = RecoveryStats::new();
        assert_eq!(stats.record_count, 0);
    }

    #[test]
    fn should_initialize_bytes_with_zero_when_created() {
        let stats = RecoveryStats::new();
        assert_eq!(stats.bytes, 0);
    }

    #[test]
    fn should_return_empty_record_count_when_wal_directory_missing() {
        let mut memtables = HashMap::new();
        let non_existent = std::env::temp_dir().join("midge_nonexistent_wal_dir_12345");
        let stats = replay_wal(&non_existent, &mut memtables).unwrap();
        assert_eq!(stats.record_count, 0);
    }

    #[test]
    fn should_return_none_max_sequence_when_wal_directory_missing() {
        let mut memtables = HashMap::new();
        let non_existent = std::env::temp_dir().join("midge_nonexistent_wal_dir_12345");
        let stats = replay_wal(&non_existent, &mut memtables).unwrap();
        assert_eq!(stats.max_sequence, None);
    }

    #[test]
    fn should_recover_put_record_key_value_when_replaying_wal() {
        let temp_dir = std::env::temp_dir().join("midge_recovery_test_put");
        let wal_dir = temp_dir.join("wal");
        std::fs::create_dir_all(&wal_dir).ok();

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

        let mut memtables = HashMap::new();
        let _stats = replay_wal(&wal_dir, &mut memtables).unwrap();

        let recovered_memtable = &memtables[&0];
        let value = recovered_memtable.get(b"test_key").unwrap();
        assert_eq!(value, Some(b"test_value".to_vec()));
    }

    #[test]
    fn should_increment_record_count_when_replaying_put() {
        let temp_dir = std::env::temp_dir().join("midge_recovery_test_put_count");
        let wal_dir = temp_dir.join("wal");
        std::fs::create_dir_all(&wal_dir).ok();

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

        let mut memtables = HashMap::new();
        let stats = replay_wal(&wal_dir, &mut memtables).unwrap();

        assert!(stats.record_count > 0);
    }

    #[test]
    fn should_track_max_sequence_from_put_record() {
        let temp_dir = std::env::temp_dir().join("midge_recovery_test_put_seq");
        let wal_dir = temp_dir.join("wal");
        std::fs::create_dir_all(&wal_dir).ok();

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

        let mut memtables = HashMap::new();
        let stats = replay_wal(&wal_dir, &mut memtables).unwrap();

        assert_eq!(stats.max_sequence, Some(1));
    }

    #[test]
    fn should_recover_delete_operation_when_replaying_wal() {
        let temp_dir =
            std::env::temp_dir().join(format!("midge_recovery_test_delete_{}", std::process::id()));
        let wal_dir = temp_dir.join("wal");
        let _ = std::fs::remove_dir_all(&wal_dir);
        std::fs::create_dir_all(&wal_dir).ok();

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

        let mut memtables = HashMap::new();
        let _stats = replay_wal(&wal_dir, &mut memtables).unwrap();

        let recovered_memtable = &memtables[&0];
        let value = recovered_memtable.get(b"test_key").unwrap();
        assert_eq!(value, None);
    }

    #[test]
    fn should_count_put_records() {
        // Arrange
        let temp_dir = std::env::temp_dir().join(format!(
            "midge_recovery_test_delete_count_{}",
            std::process::id()
        ));
        let wal_dir = temp_dir.join("wal");
        let _ = std::fs::remove_dir_all(&wal_dir);
        std::fs::create_dir_all(&wal_dir).ok();

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

        // Act
        let mut memtables = HashMap::new();
        let stats = replay_wal(&wal_dir, &mut memtables).unwrap();

        // Assert
        assert_eq!(stats.record_count, 2);

        let _ = std::fs::remove_dir_all(&temp_dir);
    }

    #[test]
    fn should_separate_records_by_column_family_when_recovering() {
        let temp_dir =
            std::env::temp_dir().join(format!("midge_recovery_test_cf_{}", std::process::id()));
        let wal_dir = temp_dir.join("wal");
        let _ = std::fs::remove_dir_all(&wal_dir);
        std::fs::create_dir_all(&wal_dir).ok();

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

        let mut memtables = HashMap::new();
        let _stats = replay_wal(&wal_dir, &mut memtables).unwrap();

        assert_eq!(memtables.len(), 2);
    }

    #[test]
    fn should_recover_both_column_families_with_correct_data() {
        let temp_dir = std::env::temp_dir().join(format!(
            "midge_recovery_test_cf_data_{}",
            std::process::id()
        ));
        let wal_dir = temp_dir.join("wal");
        let _ = std::fs::remove_dir_all(&wal_dir);
        std::fs::create_dir_all(&wal_dir).ok();

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

        let mut memtables = HashMap::new();
        let _stats = replay_wal(&wal_dir, &mut memtables).unwrap();

        assert!(memtables[&0].get(b"key0").unwrap().is_some());
        assert!(memtables[&1].get(b"key1").unwrap().is_some());
    }

    #[test]
    fn should_count_records_across_multiple_column_families() {
        let temp_dir = std::env::temp_dir().join(format!(
            "midge_recovery_test_cf_count_{}",
            std::process::id()
        ));
        let wal_dir = temp_dir.join("wal");
        let _ = std::fs::remove_dir_all(&wal_dir);
        std::fs::create_dir_all(&wal_dir).ok();

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

        let mut memtables = HashMap::new();
        let stats = replay_wal(&wal_dir, &mut memtables).unwrap();

        assert_eq!(stats.record_count, 2);
    }

    #[test]
    fn should_track_max_sequence_across_multiple_records() {
        let temp_dir =
            std::env::temp_dir().join(format!("midge_recovery_test_seq_{}", std::process::id()));
        let wal_dir = temp_dir.join("wal");
        let _ = std::fs::remove_dir_all(&wal_dir);
        std::fs::create_dir_all(&wal_dir).ok();

        {
            let writer = FsWalWriter::new(&wal_dir).unwrap();

            let record1 = WalRecord::new(
                WalOpKind::Put,
                Bytes::from_static(b"key1"),
                Some(Bytes::from_static(b"value1")),
                5,
            );
            writer.append_record(&record1).unwrap();

            let record2 = WalRecord::new(
                WalOpKind::Put,
                Bytes::from_static(b"key2"),
                Some(Bytes::from_static(b"value2")),
                10,
            );
            writer.append_record(&record2).unwrap();

            let record3 = WalRecord::new(
                WalOpKind::Put,
                Bytes::from_static(b"key3"),
                Some(Bytes::from_static(b"value3")),
                7,
            );
            writer.append_record(&record3).unwrap();
            writer.sync().unwrap();
        }

        let mut memtables = HashMap::new();
        let stats = replay_wal(&wal_dir, &mut memtables).unwrap();

        assert_eq!(stats.max_sequence, Some(10));

        let _ = std::fs::remove_dir_all(&temp_dir);
    }

    #[test]
    fn should_count_multiple_records_correctly() {
        let temp_dir =
            std::env::temp_dir().join(format!("midge_recovery_test_count_{}", std::process::id()));
        let wal_dir = temp_dir.join("wal");
        let _ = std::fs::remove_dir_all(&wal_dir);
        std::fs::create_dir_all(&wal_dir).ok();

        {
            let writer = FsWalWriter::new(&wal_dir).unwrap();

            let record1 = WalRecord::new(
                WalOpKind::Put,
                Bytes::from_static(b"key1"),
                Some(Bytes::from_static(b"value1")),
                5,
            );
            writer.append_record(&record1).unwrap();

            let record2 = WalRecord::new(
                WalOpKind::Put,
                Bytes::from_static(b"key2"),
                Some(Bytes::from_static(b"value2")),
                10,
            );
            writer.append_record(&record2).unwrap();

            let record3 = WalRecord::new(
                WalOpKind::Put,
                Bytes::from_static(b"key3"),
                Some(Bytes::from_static(b"value3")),
                7,
            );
            writer.append_record(&record3).unwrap();
            writer.sync().unwrap();
        }

        let mut memtables = HashMap::new();
        let stats = replay_wal(&wal_dir, &mut memtables).unwrap();

        assert_eq!(stats.record_count, 3);

        let _ = std::fs::remove_dir_all(&temp_dir);
    }

    #[test]
    fn should_return_none_max_sequence_when_no_records() {
        let temp_dir =
            std::env::temp_dir().join(format!("midge_recovery_test_empty_{}", std::process::id()));
        let wal_dir = temp_dir.join("wal");
        let _ = std::fs::remove_dir_all(&wal_dir);
        std::fs::create_dir_all(&wal_dir).ok();

        {
            let _writer = FsWalWriter::new(&wal_dir).unwrap();
        }

        let mut memtables = HashMap::new();
        let stats = replay_wal(&wal_dir, &mut memtables).unwrap();

        assert_eq!(stats.max_sequence, None);

        let _ = std::fs::remove_dir_all(&temp_dir);
    }

    #[test]
    fn should_return_zero_record_count_when_no_records() {
        let temp_dir = std::env::temp_dir().join(format!(
            "midge_recovery_test_empty_count_{}",
            std::process::id()
        ));
        let wal_dir = temp_dir.join("wal");
        let _ = std::fs::remove_dir_all(&wal_dir);
        std::fs::create_dir_all(&wal_dir).ok();

        {
            let _writer = FsWalWriter::new(&wal_dir).unwrap();
        }

        let mut memtables = HashMap::new();
        let stats = replay_wal(&wal_dir, &mut memtables).unwrap();

        assert_eq!(stats.record_count, 0);

        let _ = std::fs::remove_dir_all(&temp_dir);
    }

    // =========== TTL/Expiration Tests ===========

    #[test]
    fn should_skip_expired_records_during_recovery() {
        // Arrange
        let temp_dir = std::env::temp_dir().join(format!(
            "midge_recovery_test_expiration_{}",
            std::process::id()
        ));
        let wal_dir = temp_dir.join("wal");
        let _ = std::fs::remove_dir_all(&wal_dir);
        std::fs::create_dir_all(&wal_dir).ok();

        {
            let writer = FsWalWriter::new(&wal_dir).unwrap();
            let mut expired_record = WalRecord::new(
                WalOpKind::Put,
                Bytes::from_static(b"expired_key"),
                Some(Bytes::from_static(b"value")),
                1,
            );
            // Set expiration to the past (1 millisecond after epoch)
            expired_record.expiration = Some(1);
            writer.append_record(&expired_record).unwrap();

            let mut future_record = WalRecord::new(
                WalOpKind::Put,
                Bytes::from_static(b"future_key"),
                Some(Bytes::from_static(b"value")),
                2,
            );
            // Set expiration to far future
            future_record.expiration = Some(u64::MAX);
            writer.append_record(&future_record).unwrap();

            writer.sync().unwrap();
        }

        // Act
        let mut memtables = HashMap::new();
        let stats = replay_wal(&wal_dir, &mut memtables).unwrap();

        // Assert
        let recovered_memtable = &memtables[&0];
        // Expired record should not be present
        assert!(recovered_memtable.get(b"expired_key").unwrap().is_none());
        // Future record should be present
        assert!(recovered_memtable.get(b"future_key").unwrap().is_some());
        // Both records were processed but expired one was skipped during apply
        assert_eq!(stats.record_count, 2);

        let _ = std::fs::remove_dir_all(&temp_dir);
    }

    #[test]
    fn should_track_bytes_accounting_correctly() {
        // Arrange
        let temp_dir =
            std::env::temp_dir().join(format!("midge_recovery_test_bytes_{}", std::process::id()));
        let wal_dir = temp_dir.join("wal");
        let _ = std::fs::remove_dir_all(&wal_dir);
        std::fs::create_dir_all(&wal_dir).ok();

        {
            let writer = FsWalWriter::new(&wal_dir).unwrap();
            let record = WalRecord::new(
                WalOpKind::Put,
                Bytes::from_static(b"key123"),         // 6 bytes
                Some(Bytes::from_static(b"value456")), // 8 bytes
                1,
            );
            writer.append_record(&record).unwrap();
            writer.sync().unwrap();
        }

        // Act
        let mut memtables = HashMap::new();
        let stats = replay_wal(&wal_dir, &mut memtables).unwrap();

        // Assert
        // Should account for key (6) + value (8) = 14 bytes minimum
        assert!(stats.bytes >= 14);
        assert_eq!(stats.record_count, 1);

        let _ = std::fs::remove_dir_all(&temp_dir);
    }

    #[test]
    fn should_handle_delete_range_operations() {
        // Arrange
        let temp_dir = std::env::temp_dir().join(format!(
            "midge_recovery_test_delete_range_{}",
            std::process::id()
        ));
        let wal_dir = temp_dir.join("wal");
        let _ = std::fs::remove_dir_all(&wal_dir);
        std::fs::create_dir_all(&wal_dir).ok();

        {
            let writer = FsWalWriter::new(&wal_dir).unwrap();

            // Add a put record first
            let put_record = WalRecord::new(
                WalOpKind::Put,
                Bytes::from_static(b"key"),
                Some(Bytes::from_static(b"value")),
                1,
            );
            writer.append_record(&put_record).unwrap();

            // DeleteRange is currently a no-op, but should not cause errors
            let mut delete_range_record = WalRecord::new(
                WalOpKind::DeleteRange,
                Bytes::from_static(b"start"),
                None,
                2,
            );
            delete_range_record.range_end = Some(Bytes::from_static(b"end"));
            writer.append_record(&delete_range_record).unwrap();

            writer.sync().unwrap();
        }

        // Act
        let mut memtables = HashMap::new();
        let stats = replay_wal(&wal_dir, &mut memtables).unwrap();

        // Assert
        assert_eq!(stats.record_count, 2);

        let _ = std::fs::remove_dir_all(&temp_dir);
    }

    #[test]
    fn should_handle_merge_operations() {
        // Arrange
        let temp_dir =
            std::env::temp_dir().join(format!("midge_recovery_test_merge_{}", std::process::id()));
        let wal_dir = temp_dir.join("wal");
        let _ = std::fs::remove_dir_all(&wal_dir);
        std::fs::create_dir_all(&wal_dir).ok();

        {
            let writer = FsWalWriter::new(&wal_dir).unwrap();

            // Merge operations are currently not applied, but should not cause errors
            let merge_record = WalRecord::new(
                WalOpKind::Merge,
                Bytes::from_static(b"key"),
                Some(Bytes::from_static(b"merge_data")),
                1,
            );
            writer.append_record(&merge_record).unwrap();
            writer.sync().unwrap();
        }

        // Act
        let mut memtables = HashMap::new();
        let stats = replay_wal(&wal_dir, &mut memtables).unwrap();

        // Assert
        assert_eq!(stats.record_count, 1);

        let _ = std::fs::remove_dir_all(&temp_dir);
    }

    #[test]
    fn should_handle_transaction_markers() {
        // Arrange
        let temp_dir =
            std::env::temp_dir().join(format!("midge_recovery_test_txn_{}", std::process::id()));
        let wal_dir = temp_dir.join("wal");
        let _ = std::fs::remove_dir_all(&wal_dir);
        std::fs::create_dir_all(&wal_dir).ok();

        {
            let writer = FsWalWriter::new(&wal_dir).unwrap();

            let begin_record =
                WalRecord::new(WalOpKind::TxnBegin, Bytes::from_static(b"txn_key"), None, 1);
            writer.append_record(&begin_record).unwrap();

            let put_record = WalRecord::new(
                WalOpKind::Put,
                Bytes::from_static(b"key"),
                Some(Bytes::from_static(b"value")),
                2,
            );
            writer.append_record(&put_record).unwrap();

            let commit_record = WalRecord::new(
                WalOpKind::TxnCommit,
                Bytes::from_static(b"txn_key"),
                None,
                3,
            );
            writer.append_record(&commit_record).unwrap();

            writer.sync().unwrap();
        }

        // Act
        let mut memtables = HashMap::new();
        let stats = replay_wal(&wal_dir, &mut memtables).unwrap();

        // Assert
        assert_eq!(stats.record_count, 3);
        // The Put should have been applied, TxnBegin and TxnCommit are markers
        let recovered_memtable = &memtables[&0];
        assert_eq!(
            recovered_memtable.get(b"key").unwrap(),
            Some(b"value".to_vec())
        );

        let _ = std::fs::remove_dir_all(&temp_dir);
    }
}
