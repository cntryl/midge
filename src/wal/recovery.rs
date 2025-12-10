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
pub fn replay_wal(
    wal_dir: &Path,
    memtables: &mut HashMap<ColumnFamilyId, Arc<SkipListMemtable>>,
) -> MidgeResult<RecoveryStats> {
    let mut stats = RecoveryStats::new();

    if !wal_dir.exists() {
        return Ok(stats);
    }

    let mut reader = match FsWalReader::new(wal_dir) {
        Ok(r) => r,
        Err(MidgeError::Io(e)) if e.kind() == std::io::ErrorKind::NotFound => return Ok(stats),
        Err(e) => return Err(e),
    };

    let result = reader.replay(0, |record| {
        apply_record(record, memtables)?;
        stats.record(record);
        Ok(())
    });

    match result {
        Ok(()) => Ok(stats),
        Err(MidgeError::Corruption(e)) => {
            stats.mark_corruption();
            Err(MidgeError::Corruption(e))
        }
        Err(e) => Err(e),
    }
}

fn apply_record(
    record: &WalRecord,
    memtables: &mut HashMap<ColumnFamilyId, Arc<SkipListMemtable>>,
) -> MidgeResult<()> {
    let memtable = memtables
        .entry(record.cf_id)
        .or_insert_with(|| Arc::new(SkipListMemtable::new()));

    match record.op {
        WalOpKind::Put | WalOpKind::Insert => {
            if let Some(value) = &record.value {
                memtable.put(record.key.to_vec(), value.to_vec())?;
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
        assert_eq!(stats.bytes, 0);
    }

    #[test]
    fn should_return_empty_stats_when_wal_directory_missing() {
        let mut memtables = HashMap::new();
        let non_existent = std::env::temp_dir().join("midge_nonexistent_wal_dir_12345");
        let stats = replay_wal(&non_existent, &mut memtables).unwrap();
        assert_eq!(stats.record_count, 0);
        assert_eq!(stats.max_sequence, None);
    }

    #[test]
    fn should_recover_put_operations_when_replaying_wal() {
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
        let stats = replay_wal(&wal_dir, &mut memtables).unwrap();

        assert!(stats.record_count > 0);
        assert_eq!(stats.max_sequence, Some(1));
        assert!(memtables.contains_key(&0));
        let recovered_memtable = &memtables[&0];
        let value = recovered_memtable.get(b"test_key").unwrap();
        assert_eq!(value, Some(b"test_value".to_vec()));
    }

    #[test]
    fn should_recover_delete_operations_when_replaying_wal() {
        let temp_dir = std::env::temp_dir().join(format!("midge_recovery_test_delete_{}", std::process::id()));
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

            let delete_record = WalRecord::new(WalOpKind::Delete, Bytes::from_static(b"test_key"), None, 2);
            writer.append_record(&delete_record).unwrap();
            writer.sync().unwrap();
        }

        let mut memtables = HashMap::new();
        let stats = replay_wal(&wal_dir, &mut memtables).unwrap();

        assert_eq!(stats.record_count, 2, "Should recover exactly 2 records");
        let recovered_memtable = &memtables[&0];
        let value = recovered_memtable.get(b"test_key").unwrap();
        assert_eq!(value, None);
    }

    #[test]
    fn should_handle_multiple_column_families_when_recovering() {
        let temp_dir = std::env::temp_dir().join(format!("midge_recovery_test_cf_{}", std::process::id()));
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

        assert_eq!(stats.record_count, 2, "Should recover exactly 2 records");
        assert_eq!(memtables.len(), 2, "Should have 2 column families");
        assert!(memtables[&0].get(b"key0").unwrap().is_some());
        assert!(memtables[&1].get(b"key1").unwrap().is_some());
    }

    #[test]
    fn should_track_max_sequence_when_recovering() {
        let temp_dir = std::env::temp_dir().join(format!("midge_recovery_test_seq_{}", std::process::id()));
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
        assert_eq!(stats.max_sequence, Some(10));

        let _ = std::fs::remove_dir_all(&temp_dir);
    }

    #[test]
    fn should_return_none_max_sequence_when_no_records() {
        let temp_dir = std::env::temp_dir().join(format!("midge_recovery_test_empty_{}", std::process::id()));
        let wal_dir = temp_dir.join("wal");
        let _ = std::fs::remove_dir_all(&wal_dir);
        std::fs::create_dir_all(&wal_dir).ok();

        {
            let _writer = FsWalWriter::new(&wal_dir).unwrap();
        }

        let mut memtables = HashMap::new();
        let stats = replay_wal(&wal_dir, &mut memtables).unwrap();

        assert_eq!(stats.record_count, 0);
        assert_eq!(stats.max_sequence, None);

        let _ = std::fs::remove_dir_all(&temp_dir);
    }
}
