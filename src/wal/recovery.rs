//! WAL recovery - replay WAL files to restore state after crash
//!
//! On startup, persistent WAL files are replayed to reconstruct the
//! memtables for each column family.
//!
//! Recovery order:
//! 1) Rotated segment files: `{segment_id}.wal` in ascending segment_id order
//! 2) Active file: `wal.log` (if present)

use super::types::{ColumnFamilyId, WalOpKind, WalRecord};
use crate::common::{MidgeError, MidgeResult};
#[cfg(test)]
use crate::sst::Memtable;
use crate::sst::SkipListMemtable;
use crate::storage::abstraction::{
    OpenMode, OpenOptions, Storage, StorageError, StorageErrorKind, StoragePath,
};
use std::collections::HashMap;
use std::sync::Arc;
use tracing::instrument;

fn map_storage_error(err: StorageError) -> MidgeError {
    match err.kind {
        StorageErrorKind::NotFound => MidgeError::NotFound,
        StorageErrorKind::Unsupported => MidgeError::NotSupported(err.message),
        StorageErrorKind::Corruption => MidgeError::Corruption(err.message),
        StorageErrorKind::InvalidInput => MidgeError::InvalidArgument(err.message),
        _ => MidgeError::Io(std::io::Error::other(err.to_string())),
    }
}

fn join(dir: &StoragePath, leaf: &str) -> StoragePath {
    let base = dir.as_str().trim_end_matches('/');
    if base.is_empty() {
        StoragePath::new(leaf)
    } else {
        StoragePath::new(format!("{base}/{leaf}"))
    }
}

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

    /// Total nanoseconds spent reading WAL files from storage.
    pub wal_read_ns: u128,
    /// Total nanoseconds spent applying records to memtables.
    pub apply_ns: u128,
    /// Total nanoseconds spent in overall replay (per call)
    pub total_replay_ns: u128,
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
            wal_read_ns: 0,
            apply_ns: 0,
            total_replay_ns: 0,
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
#[instrument(level = "info", skip(storage, memtables), fields(wal_dir = ?wal_dir))]
pub fn replay_wal(
    storage: &dyn Storage,
    wal_dir: &StoragePath,
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

    tracing::info!(dir = %wal_dir, "starting wal replay");

    // Collect replay files: rotated segments first, then wal.log.
    let mut segment_files: Vec<(u64, StoragePath)> = Vec::new();
    let mut wal_log_path: Option<StoragePath> = None;

    let entries = match storage.list_dir(wal_dir) {
        Ok(v) => v,
        Err(e) if e.kind == StorageErrorKind::NotFound => return Ok(stats),
        Err(e) => return Err(map_storage_error(e)),
    };

    for entry in entries {
        if entry.is_dir {
            continue;
        }
        let file_name = entry.name;
        if file_name == "wal.log" {
            wal_log_path = Some(join(wal_dir, "wal.log"));
            continue;
        }

        // Match `{segment_id}.wal`
        if let Some(segment_str) = file_name.strip_suffix(".wal") {
            if let Ok(segment_id) = segment_str.parse::<u64>() {
                segment_files.push((segment_id, join(wal_dir, &file_name)));
            }
        }
    }

    segment_files.sort_by_key(|(id, _)| *id);

    let mut replay_paths: Vec<StoragePath> = segment_files.into_iter().map(|(_, p)| p).collect();
    if let Some(wal_log) = wal_log_path {
        replay_paths.push(wal_log);
    }

    // Replay each file in order.
    let mut result: MidgeResult<()> = Ok(());
    for file_path in replay_paths {
        result = replay_wal_file(
            storage,
            &file_path,
            &mut stats,
            memtables,
            &mut open_txns,
            &mut begun_txns,
        );
        if result.is_err() {
            break;
        }
    }

    match result {
        Ok(()) => {
            tracing::info!(
                dir = %wal_dir,
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
            tracing::warn!(dir = %wal_dir, error = %e, "wal replay encountered corruption");
            // Tolerate corruption by returning successfully with whatever state was recovered
            // before the corruption point (commonly a truncated tail record after a crash).
            let _ = e;
            Ok(stats)
        }
        Err(e) => {
            tracing::error!(dir = %wal_dir, error = %e, "wal replay failed");
            Err(e)
        }
    }
}

fn replay_wal_file(
    storage: &dyn Storage,
    file_path: &StoragePath,
    stats: &mut RecoveryStats,
    memtables: &mut HashMap<ColumnFamilyId, Arc<SkipListMemtable>>,
    open_txns: &mut std::collections::HashMap<u64, Vec<WalRecord>>,
    begun_txns: &mut std::collections::HashSet<u64>,
) -> MidgeResult<()> {
    // Guardrail: prevent pathological allocations on corrupted length prefixes.
    const MAX_WAL_RECORD_LEN: usize = 64 * 1024 * 1024; // 64 MiB

    let mut pos: u64 = 0;
    let mut file_read_ns: u128 = 0;
    let mut file_apply_ns: u128 = 0;

    loop {
        // Open file once per iteration and use file length to detect clean EOF.
        let open_start = std::time::Instant::now();
        let file = match storage.open_file(
            file_path,
            OpenOptions {
                mode: OpenMode::ReadOnly,
                create: false,
                create_new: false,
                truncate: false,
                append: false,
            },
        ) {
            Ok(file) => file,
            Err(e) if e.kind == StorageErrorKind::NotFound => {
                stats.wal_read_ns = stats.wal_read_ns.saturating_add(file_read_ns);
                stats.apply_ns = stats.apply_ns.saturating_add(file_apply_ns);
                return Ok(());
            }
            Err(e) => return Err(map_storage_error(e)),
        };
        file_read_ns = file_read_ns.saturating_add(open_start.elapsed().as_nanos());

        let file_len = file.len().map_err(map_storage_error)?;
        if pos == file_len {
            break; // clean EOF
        }
        if pos > file_len {
            return Err(MidgeError::Corruption(format!(
                "WAL replay read past EOF at pos {} in {} (file_len={})",
                pos, file_path, file_len
            )));
        }
        if file_len.saturating_sub(pos) < 4 {
            return Err(MidgeError::Corruption(format!(
                "Incomplete WAL length prefix at pos {} in {} (need 4 bytes, have {})",
                pos,
                file_path,
                file_len.saturating_sub(pos)
            )));
        }

        // Read 4-byte length prefix
        let len_read_start = std::time::Instant::now();
        let len_bytes = file.read_at(pos, 4).map_err(map_storage_error)?;
        file_read_ns = file_read_ns.saturating_add(len_read_start.elapsed().as_nanos());

        if len_bytes.len() < 4 {
            return Err(MidgeError::Corruption(format!(
                "Incomplete WAL length prefix at pos {} in {} (got {} bytes)",
                pos,
                file_path,
                len_bytes.len()
            )));
        }

        let mut len_buf = [0u8; 4];
        len_buf.copy_from_slice(&len_bytes[..4]);

        let len = u32::from_le_bytes(len_buf) as usize;
        if len > MAX_WAL_RECORD_LEN {
            return Err(MidgeError::Corruption(format!(
                "WAL record too large at pos {} in {} (len={})",
                pos, file_path, len
            )));
        }

        let need_end = pos.saturating_add(4).saturating_add(len as u64);
        if need_end > file_len {
            return Err(MidgeError::Corruption(format!(
                "Incomplete WAL record at pos {} in {} (len={}, file_len={})",
                pos, file_path, len, file_len
            )));
        }

        // Read record payload
        let payload_read_start = std::time::Instant::now();
        let buf = file
            .read_at(pos + 4, len as u64)
            .map_err(map_storage_error)?;
        file_read_ns = file_read_ns.saturating_add(payload_read_start.elapsed().as_nanos());

        if buf.len() < len {
            return Err(MidgeError::Corruption(format!(
                "Incomplete WAL record at pos {} in {} (len={}, got={})",
                pos,
                file_path,
                len,
                buf.len()
            )));
        }

        let record = super::encoding::decode(&buf[..])?;

        // Always count records, even if buffered/ignored.
        stats.record(&record);

        match record.op {
            WalOpKind::TxnBegin => {
                if let Some(txn_id) = record.txn_id {
                    begun_txns.insert(txn_id);
                    open_txns.entry(txn_id).or_default();
                }
            }
            WalOpKind::TxnCommit => {
                if let Some(txn_id) = record.txn_id {
                    if begun_txns.remove(&txn_id) {
                        if let Some(records) = open_txns.remove(&txn_id) {
                            for buffered in &records {
                                let apply_start = std::time::Instant::now();
                                apply_record(buffered, memtables)?;
                                file_apply_ns =
                                    file_apply_ns.saturating_add(apply_start.elapsed().as_nanos());
                            }
                        }
                    }
                }
            }
            _ => {
                if let Some(txn_id) = record.txn_id {
                    if begun_txns.contains(&txn_id) {
                        open_txns.entry(txn_id).or_default().push(record);
                        pos += 4 + len as u64;
                        continue;
                    }
                }

                let apply_start = std::time::Instant::now();
                apply_record(&record, memtables)?;
                file_apply_ns = file_apply_ns.saturating_add(apply_start.elapsed().as_nanos());
            }
        }

        pos += 4 + len as u64;
    }

    stats.wal_read_ns = stats.wal_read_ns.saturating_add(file_read_ns);
    stats.apply_ns = stats.apply_ns.saturating_add(file_apply_ns);

    tracing::info!(
        path = %file_path,
        records = stats.record_count,
        bytes = stats.bytes,
        wal_read_ms = (file_read_ns as f64) / 1_000_000.0,
        apply_ms = (file_apply_ns as f64) / 1_000_000.0,
        "replayed wal file"
    );

    Ok(())
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
                memtable.put_with_seq(
                    record.key.to_vec(),
                    value.to_vec(),
                    record.seq,
                    record.expiration,
                )?;
            }
        }
        WalOpKind::Delete => {
            memtable.delete_with_seq(record.key.to_vec(), record.seq)?;
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
    use crate::io::RealFs;
    use crate::storage::abstraction::StoragePath;
    use crate::storage::LocalFsStorage;
    use crate::wal::fs::FsWalWriterIo;
    use crate::wal::types::WalOpKind;
    use crate::wal::WalWriter;
    use bytes::Bytes;
    use std::sync::Arc;
    use tempfile::TempDir;

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
        // Arrange
        let mut memtables = HashMap::new();
        let dir = TempDir::new().unwrap();
        let storage = LocalFsStorage::new(dir.path()).unwrap();
        let non_existent = StoragePath::new("midge_nonexistent_wal_dir_12345");

        // Act
        let stats = replay_wal(&storage, &non_existent, &mut memtables).unwrap();

        // Assert
        assert_eq!(stats.record_count, 0);
    }

    #[test]
    fn should_return_none_max_sequence_when_wal_directory_missing() {
        // Arrange
        let mut memtables = HashMap::new();
        let dir = TempDir::new().unwrap();
        let storage = LocalFsStorage::new(dir.path()).unwrap();
        let non_existent = StoragePath::new("midge_nonexistent_wal_dir_12345");

        // Act
        let stats = replay_wal(&storage, &non_existent, &mut memtables).unwrap();

        // Assert
        assert_eq!(stats.max_sequence, None);
    }

    #[test]
    fn should_recover_put_record_key_value_when_replaying_wal() {
        // Arrange
        let dir = TempDir::new().unwrap();
        let wal_subdir = dir.path().join("wal");
        std::fs::create_dir(&wal_subdir).unwrap();
        let storage = LocalFsStorage::new(dir.path()).unwrap();
        let wal_dir = StoragePath::new("wal");

        {
            let fs = Arc::new(RealFs::new(&wal_subdir).unwrap());
            let writer = FsWalWriterIo::new("wal.log", fs as Arc<dyn crate::io::Fs>).unwrap();
            let record = WalRecord::new(
                WalOpKind::Put,
                Bytes::from_static(b"test_key"),
                Some(Bytes::from_static(b"test_value")),
                1,
            );
            writer.append_record(&record).unwrap();
            writer.sync().unwrap();
        }

        // Act
        let mut memtables = HashMap::new();
        let _stats = replay_wal(&storage, &wal_dir, &mut memtables).unwrap();

        // Assert
        let recovered_memtable = &memtables[&0];
        let value = recovered_memtable.get(b"test_key").unwrap();
        assert_eq!(value, Some(b"test_value".to_vec()));
    }

    #[test]
    fn should_increment_record_count_when_replaying_put() {
        // Arrange
        let dir = TempDir::new().unwrap();
        let wal_subdir = dir.path().join("wal");
        std::fs::create_dir(&wal_subdir).unwrap();
        let storage = LocalFsStorage::new(dir.path()).unwrap();
        let wal_dir = StoragePath::new("wal");

        {
            let fs = Arc::new(RealFs::new(&wal_subdir).unwrap());
            let writer = FsWalWriterIo::new("wal.log", fs as Arc<dyn crate::io::Fs>).unwrap();
            let record = WalRecord::new(
                WalOpKind::Put,
                Bytes::from_static(b"test_key"),
                Some(Bytes::from_static(b"test_value")),
                1,
            );
            writer.append_record(&record).unwrap();
            writer.sync().unwrap();
        }

        // Act
        let mut memtables = HashMap::new();
        let stats = replay_wal(&storage, &wal_dir, &mut memtables).unwrap();

        // Assert
        assert!(stats.record_count > 0);
    }

    #[test]
    fn should_track_max_sequence_from_put_record() {
        // Arrange
        let dir = TempDir::new().unwrap();
        let wal_subdir = dir.path().join("wal");
        std::fs::create_dir(&wal_subdir).unwrap();
        let storage = LocalFsStorage::new(dir.path()).unwrap();
        let wal_dir = StoragePath::new("wal");

        {
            let fs = Arc::new(RealFs::new(&wal_subdir).unwrap());
            let writer = FsWalWriterIo::new("wal.log", fs as Arc<dyn crate::io::Fs>).unwrap();
            let record = WalRecord::new(
                WalOpKind::Put,
                Bytes::from_static(b"test_key"),
                Some(Bytes::from_static(b"test_value")),
                1,
            );
            writer.append_record(&record).unwrap();
            writer.sync().unwrap();
        }

        // Act
        let mut memtables = HashMap::new();
        let stats = replay_wal(&storage, &wal_dir, &mut memtables).unwrap();

        // Assert
        assert_eq!(stats.max_sequence, Some(1));
    }

    #[test]
    fn should_recover_delete_operation_when_replaying_wal() {
        // Arrange
        let dir = TempDir::new().unwrap();
        let wal_subdir = dir.path().join("wal");
        std::fs::create_dir(&wal_subdir).unwrap();
        let storage = LocalFsStorage::new(dir.path()).unwrap();
        let wal_dir = StoragePath::new("wal");

        {
            let fs = Arc::new(RealFs::new(&wal_subdir).unwrap());
            let writer = FsWalWriterIo::new("wal.log", fs as Arc<dyn crate::io::Fs>).unwrap();
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
        let _stats = replay_wal(&storage, &wal_dir, &mut memtables).unwrap();

        // Assert
        let recovered_memtable = &memtables[&0];
        let value = recovered_memtable.get(b"test_key").unwrap();
        assert_eq!(value, None);
    }

    #[test]
    fn should_count_put_records() {
        // Arrange
        let dir = TempDir::new().unwrap();
        let wal_subdir = dir.path().join("wal");
        std::fs::create_dir(&wal_subdir).unwrap();
        let storage = LocalFsStorage::new(dir.path()).unwrap();
        let wal_dir = StoragePath::new("wal");

        {
            let fs = Arc::new(RealFs::new(&wal_subdir).unwrap());
            let writer = FsWalWriterIo::new("wal.log", fs as Arc<dyn crate::io::Fs>).unwrap();
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
        let stats = replay_wal(&storage, &wal_dir, &mut memtables).unwrap();

        // Assert
        assert_eq!(stats.record_count, 2);
    }

    #[test]
    fn should_separate_records_by_column_family_when_recovering() {
        // Arrange
        let dir = TempDir::new().unwrap();
        let wal_subdir = dir.path().join("wal");
        std::fs::create_dir(&wal_subdir).unwrap();
        let storage = LocalFsStorage::new(dir.path()).unwrap();
        let wal_dir = StoragePath::new("wal");

        {
            let fs = Arc::new(RealFs::new(&wal_subdir).unwrap());
            let writer = FsWalWriterIo::new("wal.log", fs as Arc<dyn crate::io::Fs>).unwrap();

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

        // Act
        let mut memtables = HashMap::new();
        let _stats = replay_wal(&storage, &wal_dir, &mut memtables).unwrap();

        // Assert
        assert_eq!(memtables.len(), 2);
    }

    #[test]
    fn should_recover_both_column_families_with_correct_data() {
        // Arrange
        let dir = TempDir::new().unwrap();
        let wal_subdir = dir.path().join("wal");
        std::fs::create_dir(&wal_subdir).unwrap();
        let storage = LocalFsStorage::new(dir.path()).unwrap();
        let wal_dir = StoragePath::new("wal");

        {
            let fs = Arc::new(RealFs::new(&wal_subdir).unwrap());
            let writer = FsWalWriterIo::new("wal.log", fs as Arc<dyn crate::io::Fs>).unwrap();

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

        // Act
        let mut memtables = HashMap::new();
        let _stats = replay_wal(&storage, &wal_dir, &mut memtables).unwrap();

        // Assert
        assert!(memtables[&0].get(b"key0").unwrap().is_some());
        assert!(memtables[&1].get(b"key1").unwrap().is_some());
    }

    #[test]
    fn should_count_records_across_multiple_column_families() {
        // Arrange
        let dir = TempDir::new().unwrap();
        let wal_subdir = dir.path().join("wal");
        std::fs::create_dir(&wal_subdir).unwrap();
        let storage = LocalFsStorage::new(dir.path()).unwrap();
        let wal_dir = StoragePath::new("wal");

        {
            let fs = Arc::new(RealFs::new(&wal_subdir).unwrap());
            let writer = FsWalWriterIo::new("wal.log", fs as Arc<dyn crate::io::Fs>).unwrap();

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

        // Act
        let mut memtables = HashMap::new();
        let stats = replay_wal(&storage, &wal_dir, &mut memtables).unwrap();

        // Assert
        assert_eq!(stats.record_count, 2);
    }

    #[test]
    fn should_track_max_sequence_across_multiple_records() {
        // Arrange
        let dir = TempDir::new().unwrap();
        let wal_subdir = dir.path().join("wal");
        std::fs::create_dir(&wal_subdir).unwrap();
        let storage = LocalFsStorage::new(dir.path()).unwrap();
        let wal_dir = StoragePath::new("wal");

        {
            let fs = Arc::new(RealFs::new(&wal_subdir).unwrap());
            let writer = FsWalWriterIo::new("wal.log", fs as Arc<dyn crate::io::Fs>).unwrap();

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

        // Act
        let mut memtables = HashMap::new();
        let stats = replay_wal(&storage, &wal_dir, &mut memtables).unwrap();

        // Assert
        assert_eq!(stats.max_sequence, Some(10));
    }

    #[test]
    fn should_count_multiple_records_correctly() {
        // Arrange
        let dir = TempDir::new().unwrap();
        let wal_subdir = dir.path().join("wal");
        std::fs::create_dir(&wal_subdir).unwrap();
        let storage = LocalFsStorage::new(dir.path()).unwrap();
        let wal_dir = StoragePath::new("wal");

        {
            let fs = Arc::new(RealFs::new(&wal_subdir).unwrap());
            let writer = FsWalWriterIo::new("wal.log", fs as Arc<dyn crate::io::Fs>).unwrap();

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

        // Act
        let mut memtables = HashMap::new();
        let stats = replay_wal(&storage, &wal_dir, &mut memtables).unwrap();

        // Assert
        assert_eq!(stats.record_count, 3);
    }

    #[test]
    fn should_return_none_max_sequence_when_no_records() {
        // Arrange
        let dir = TempDir::new().unwrap();
        let wal_subdir = dir.path().join("wal");
        std::fs::create_dir(&wal_subdir).unwrap();
        let storage = LocalFsStorage::new(dir.path()).unwrap();
        let wal_dir = StoragePath::new("wal");

        {
            let fs = Arc::new(RealFs::new(&wal_subdir).unwrap());
            let _writer = FsWalWriterIo::new("wal.log", fs as Arc<dyn crate::io::Fs>).unwrap();
        }

        // Act
        let mut memtables = HashMap::new();
        let stats = replay_wal(&storage, &wal_dir, &mut memtables).unwrap();

        // Assert
        assert_eq!(stats.max_sequence, None);
    }

    #[test]
    fn should_return_zero_record_count_when_no_records() {
        // Arrange
        let dir = TempDir::new().unwrap();
        let wal_subdir = dir.path().join("wal");
        std::fs::create_dir(&wal_subdir).unwrap();
        let storage = LocalFsStorage::new(dir.path()).unwrap();
        let wal_dir = StoragePath::new("wal");

        {
            let fs = Arc::new(RealFs::new(&wal_subdir).unwrap());
            let _writer = FsWalWriterIo::new("wal.log", fs as Arc<dyn crate::io::Fs>).unwrap();
        }

        // Act
        let mut memtables = HashMap::new();
        let stats = replay_wal(&storage, &wal_dir, &mut memtables).unwrap();

        // Assert
        assert_eq!(stats.record_count, 0);
    }

    // =========== TTL/Expiration Tests ===========

    #[test]
    fn should_skip_expired_records_during_recovery() {
        // Arrange
        let dir = TempDir::new().unwrap();
        let wal_subdir = dir.path().join("wal");
        std::fs::create_dir(&wal_subdir).unwrap();
        let storage = LocalFsStorage::new(dir.path()).unwrap();
        let wal_dir = StoragePath::new("wal");

        {
            let fs = Arc::new(RealFs::new(&wal_subdir).unwrap());
            let writer = FsWalWriterIo::new("wal.log", fs as Arc<dyn crate::io::Fs>).unwrap();
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
        let stats = replay_wal(&storage, &wal_dir, &mut memtables).unwrap();

        // Assert
        let recovered_memtable = &memtables[&0];
        // Expired record should not be present
        assert!(recovered_memtable.get(b"expired_key").unwrap().is_none());
        // Future record should be present
        assert!(recovered_memtable.get(b"future_key").unwrap().is_some());
        // Both records were processed but expired one was skipped during apply
        assert_eq!(stats.record_count, 2);
    }

    #[test]
    fn should_track_bytes_accounting_correctly() {
        // Arrange
        let dir = TempDir::new().unwrap();
        let wal_subdir = dir.path().join("wal");
        std::fs::create_dir(&wal_subdir).unwrap();
        let storage = LocalFsStorage::new(dir.path()).unwrap();
        let wal_dir = StoragePath::new("wal");

        {
            let fs = Arc::new(RealFs::new(&wal_subdir).unwrap());
            let writer = FsWalWriterIo::new("wal.log", fs as Arc<dyn crate::io::Fs>).unwrap();
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
        let stats = replay_wal(&storage, &wal_dir, &mut memtables).unwrap();

        // Assert
        // Should account for key (6) + value (8) = 14 bytes minimum
        assert!(stats.bytes >= 14);
        assert_eq!(stats.record_count, 1);
    }

    #[test]
    fn should_handle_delete_range_operations() {
        // Arrange
        let dir = TempDir::new().unwrap();
        let wal_subdir = dir.path().join("wal");
        std::fs::create_dir(&wal_subdir).unwrap();
        let storage = LocalFsStorage::new(dir.path()).unwrap();
        let wal_dir = StoragePath::new("wal");

        {
            let fs = Arc::new(RealFs::new(&wal_subdir).unwrap());
            let writer = FsWalWriterIo::new("wal.log", fs as Arc<dyn crate::io::Fs>).unwrap();

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
        let stats = replay_wal(&storage, &wal_dir, &mut memtables).unwrap();

        // Assert
        assert_eq!(stats.record_count, 2);
    }

    #[test]
    fn should_handle_merge_operations() {
        // Arrange
        let dir = TempDir::new().unwrap();
        let wal_subdir = dir.path().join("wal");
        std::fs::create_dir(&wal_subdir).unwrap();
        let storage = LocalFsStorage::new(dir.path()).unwrap();
        let wal_dir = StoragePath::new("wal");

        {
            let fs = Arc::new(RealFs::new(&wal_subdir).unwrap());
            let writer = FsWalWriterIo::new("wal.log", fs as Arc<dyn crate::io::Fs>).unwrap();

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
        let stats = replay_wal(&storage, &wal_dir, &mut memtables).unwrap();

        // Assert
        assert_eq!(stats.record_count, 1);
    }

    #[test]
    fn should_handle_transaction_markers() {
        // Arrange
        let dir = TempDir::new().unwrap();
        let wal_subdir = dir.path().join("wal");
        std::fs::create_dir(&wal_subdir).unwrap();
        let storage = LocalFsStorage::new(dir.path()).unwrap();
        let wal_dir = StoragePath::new("wal");

        {
            let fs = Arc::new(RealFs::new(&wal_subdir).unwrap());
            let writer = FsWalWriterIo::new("wal.log", fs as Arc<dyn crate::io::Fs>).unwrap();

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
        let stats = replay_wal(&storage, &wal_dir, &mut memtables).unwrap();

        // Assert
        assert_eq!(stats.record_count, 3);
        // The Put should have been applied, TxnBegin and TxnCommit are markers
        let recovered_memtable = &memtables[&0];
        assert_eq!(
            recovered_memtable.get(b"key").unwrap(),
            Some(b"value".to_vec())
        );
    }
}
