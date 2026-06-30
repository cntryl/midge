//! WAL recovery - replay WAL files to restore state after crash
//!
//! On startup, persistent WAL files are replayed to reconstruct the
//! memtables for each column family.
//!
//! Recovery order:
//! 1) Rotated segment files: `{segment_id}.wal` in ascending `segment_id` order
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
use std::hash::BuildHasher;
use std::sync::Arc;
use tracing::instrument;

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum ReplayPolicy {
    Strict,
    SalvageValidPrefix,
}

fn is_salvageable_tail_corruption(message: &str) -> bool {
    let lower = message.to_ascii_lowercase();
    (lower.contains("incomplete wal frame header") || lower.contains("incomplete wal record"))
        && !lower.contains("pos 0 ")
}

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

    /// Highest writer epoch seen across all replayed WAL records.
    /// Records with epoch < `max_epoch_seen` are from stale writers and
    /// are skipped during recovery to prevent zombie writes from
    /// corrupting the recovered state.
    pub max_epoch_seen: u64,
    /// Number of WAL records skipped because their `writer_epoch` was
    /// less than the highest epoch observed so far (stale writer).
    pub stale_records_skipped: u64,
}

impl Default for RecoveryStats {
    fn default() -> Self {
        Self::new()
    }
}

impl RecoveryStats {
    #[must_use]
    pub fn new() -> Self {
        Self {
            record_count: 0,
            bytes: 0,
            had_corruption: false,
            max_sequence: None,
            wal_read_ns: 0,
            apply_ns: 0,
            total_replay_ns: 0,
            max_epoch_seen: 0,
            stale_records_skipped: 0,
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
///
/// # Errors
///
/// Returns an error if WAL enumeration, decoding, or record application fails.
pub fn replay_wal<S: BuildHasher>(
    storage: &dyn Storage,
    wal_dir: &StoragePath,
    memtables: &mut HashMap<ColumnFamilyId, Arc<SkipListMemtable>, S>,
) -> MidgeResult<RecoveryStats> {
    replay_wal_with_policy(storage, wal_dir, memtables, ReplayPolicy::Strict)
}

#[instrument(level = "info", skip(storage, memtables), fields(wal_dir = ?wal_dir, replay_policy = ?replay_policy))]
///
/// # Errors
///
/// Returns an error if WAL enumeration, decoding, or record application fails
/// according to the selected replay policy.
pub fn replay_wal_with_policy<S: BuildHasher>(
    storage: &dyn Storage,
    wal_dir: &StoragePath,
    memtables: &mut HashMap<ColumnFamilyId, Arc<SkipListMemtable>, S>,
    replay_policy: ReplayPolicy,
) -> MidgeResult<RecoveryStats> {
    // Invariant: recovery may keep only a verified prefix of the WAL, but it
    // must never materialize a partial frame or reorder committed records.
    let mut stats = RecoveryStats::new();
    let replay_start = std::time::Instant::now();

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
    let mut segment_files: std::collections::BTreeMap<u64, (String, StoragePath)> =
        std::collections::BTreeMap::new();
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
        if file_name == crate::wal::ACTIVE_FILE_NAME {
            wal_log_path = Some(join(wal_dir, crate::wal::ACTIVE_FILE_NAME));
            continue;
        }

        if let Some(segment_id) = crate::wal::parse_segment_id(&file_name) {
            let prefer_candidate =
                segment_files
                    .get(&segment_id)
                    .is_none_or(|(existing_name, _)| {
                        existing_name != &crate::wal::cloud_segment_file_name(segment_id)
                            && file_name == crate::wal::cloud_segment_file_name(segment_id)
                    });

            if prefer_candidate {
                segment_files.insert(segment_id, (file_name.clone(), join(wal_dir, &file_name)));
            }
        }
    }

    let mut replay_paths: Vec<StoragePath> =
        segment_files.into_iter().map(|(_, (_, p))| p).collect();
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

    stats.total_replay_ns = replay_start.elapsed().as_nanos();

    match result {
        Ok(()) => {
            tracing::info!(
                dir = %wal_dir,
                records = stats.record_count,
                bytes = stats.bytes,
                max_sequence = ?stats.max_sequence,
                max_epoch = stats.max_epoch_seen,
                stale_skipped = stats.stale_records_skipped,
                had_corruption = stats.had_corruption,
                "wal replay completed"
            );
            Ok(stats)
        }
        Err(MidgeError::Corruption(e)) => {
            if is_salvageable_tail_corruption(&e) {
                tracing::info!(
                    dir = %wal_dir,
                    error = %e,
                    "wal replay dropped a truncated tail and kept the valid prefix"
                );
                Ok(stats)
            } else if replay_policy == ReplayPolicy::SalvageValidPrefix {
                stats.mark_corruption();
                tracing::warn!(dir = %wal_dir, error = %e, "wal replay encountered corruption");
                // Tolerate corruption by returning successfully with whatever state was recovered
                // before the corruption point (commonly a truncated tail record after a crash).
                Ok(stats)
            } else {
                stats.mark_corruption();
                tracing::warn!(dir = %wal_dir, error = %e, "wal replay encountered corruption");
                Err(MidgeError::Corruption(e))
            }
        }
        Err(e) => {
            tracing::error!(dir = %wal_dir, error = %e, "wal replay failed");
            Err(e)
        }
    }
}

fn replay_wal_file<S: BuildHasher>(
    storage: &dyn Storage,
    file_path: &StoragePath,
    stats: &mut RecoveryStats,
    memtables: &mut HashMap<ColumnFamilyId, Arc<SkipListMemtable>, S>,
    open_txns: &mut std::collections::HashMap<u64, Vec<WalRecord>>,
    begun_txns: &mut std::collections::HashSet<u64>,
) -> MidgeResult<()> {
    let mut pos: u64 = 0;
    let mut file_read_ns: u128 = 0;
    let mut file_apply_ns: u128 = 0;

    loop {
        let Some(file) = open_wal_replay_file(storage, file_path, &mut file_read_ns)? else {
            finalize_wal_file_replay(stats, file_path, file_read_ns, file_apply_ns);
            return Ok(());
        };

        match read_next_wal_frame(&*file, file_path, pos, &mut file_read_ns)? {
            NextWalFrame::Eof => break,
            NextWalFrame::Frame(frame) => {
                let next_pos = frame.next_pos;
                let mut apply_ctx = WalReplayApplyContext {
                    file_path,
                    stats,
                    memtables,
                    open_txns,
                    begun_txns,
                    file_apply_ns: &mut file_apply_ns,
                };
                apply_replayed_wal_record(frame.record, pos, &mut apply_ctx)?;
                pos = next_pos;
            }
        }
    }

    finalize_wal_file_replay(stats, file_path, file_read_ns, file_apply_ns);
    Ok(())
}

struct ReplayedWalFrame {
    record: WalRecord,
    next_pos: u64,
}

struct WalReplayApplyContext<'a, S: BuildHasher> {
    file_path: &'a StoragePath,
    stats: &'a mut RecoveryStats,
    memtables: &'a mut HashMap<ColumnFamilyId, Arc<SkipListMemtable>, S>,
    open_txns: &'a mut std::collections::HashMap<u64, Vec<WalRecord>>,
    begun_txns: &'a mut std::collections::HashSet<u64>,
    file_apply_ns: &'a mut u128,
}

enum NextWalFrame {
    Eof,
    Frame(ReplayedWalFrame),
}

fn open_wal_replay_file<'a>(
    storage: &'a dyn Storage,
    file_path: &StoragePath,
    file_read_ns: &mut u128,
) -> MidgeResult<Option<Box<dyn crate::storage::abstraction::StorageFile + 'a>>> {
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
        Err(e) if e.kind == StorageErrorKind::NotFound => return Ok(None),
        Err(e) => return Err(map_storage_error(e)),
    };
    *file_read_ns = file_read_ns.saturating_add(open_start.elapsed().as_nanos());
    Ok(Some(file))
}

fn read_next_wal_frame(
    file: &dyn crate::storage::abstraction::StorageFile,
    file_path: &StoragePath,
    pos: u64,
    file_read_ns: &mut u128,
) -> MidgeResult<NextWalFrame> {
    let file_len = file.len().map_err(map_storage_error)?;
    if pos == file_len {
        return Ok(NextWalFrame::Eof);
    }
    if pos > file_len {
        return Err(MidgeError::Corruption(format!(
            "WAL replay read past EOF at pos {pos} in {file_path} (file_len={file_len})"
        )));
    }
    if file_len.saturating_sub(pos) < crate::wal::frame::WAL_FRAME_HEADER_LEN as u64 {
        return Err(MidgeError::Corruption(format!(
            "Incomplete WAL frame header at pos {} in {} (need {} bytes, have {})",
            pos,
            file_path,
            crate::wal::frame::WAL_FRAME_HEADER_LEN,
            file_len.saturating_sub(pos)
        )));
    }

    let (len, expected_crc) = read_wal_frame_header(file, pos, file_read_ns)?;
    let payload = read_wal_frame_payload(file, file_path, pos, len, file_len, file_read_ns)?;

    crate::wal::frame::verify_frame_crc(&payload[..len], expected_crc)?;
    let record = super::encoding::decode(&payload[..])?;

    Ok(NextWalFrame::Frame(ReplayedWalFrame {
        record,
        next_pos: pos + crate::wal::frame::WAL_FRAME_HEADER_LEN as u64 + len as u64,
    }))
}

fn read_wal_frame_header(
    file: &dyn crate::storage::abstraction::StorageFile,
    pos: u64,
    file_read_ns: &mut u128,
) -> MidgeResult<(usize, u32)> {
    let header_read_start = std::time::Instant::now();
    let header = file
        .read_at(pos, crate::wal::frame::WAL_FRAME_HEADER_LEN as u64)
        .map_err(map_storage_error)?;
    *file_read_ns = file_read_ns.saturating_add(header_read_start.elapsed().as_nanos());
    crate::wal::frame::decode_frame_header(&header[..])
}

fn read_wal_frame_payload(
    file: &dyn crate::storage::abstraction::StorageFile,
    file_path: &StoragePath,
    pos: u64,
    len: usize,
    file_len: u64,
    file_read_ns: &mut u128,
) -> MidgeResult<Vec<u8>> {
    let need_end = pos
        .saturating_add(crate::wal::frame::WAL_FRAME_HEADER_LEN as u64)
        .saturating_add(len as u64);
    if need_end > file_len {
        return Err(MidgeError::Corruption(format!(
            "Incomplete WAL record at pos {pos} in {file_path} (len={len}, file_len={file_len})"
        )));
    }

    let payload_read_start = std::time::Instant::now();
    let payload = file
        .read_at(
            pos + crate::wal::frame::WAL_FRAME_HEADER_LEN as u64,
            len as u64,
        )
        .map_err(map_storage_error)?;
    *file_read_ns = file_read_ns.saturating_add(payload_read_start.elapsed().as_nanos());

    if payload.len() < len {
        return Err(MidgeError::Corruption(format!(
            "Incomplete WAL record at pos {} in {} (len={}, got={})",
            pos,
            file_path,
            len,
            payload.len()
        )));
    }

    Ok(payload)
}

fn apply_replayed_wal_record<S: BuildHasher>(
    record: WalRecord,
    pos: u64,
    ctx: &mut WalReplayApplyContext<'_, S>,
) -> MidgeResult<()> {
    ctx.stats.record(&record);

    if record.writer_epoch > ctx.stats.max_epoch_seen {
        ctx.stats.max_epoch_seen = record.writer_epoch;
    }
    if record.writer_epoch > 0 && record.writer_epoch < ctx.stats.max_epoch_seen {
        ctx.stats.stale_records_skipped += 1;
        tracing::warn!(
            epoch = record.writer_epoch,
            max_epoch = ctx.stats.max_epoch_seen,
            seq = record.seq,
            op = ?record.op,
            pos = pos,
            file = %ctx.file_path,
            "skipping WAL record from stale writer epoch"
        );
        return Ok(());
    }

    match record.op {
        WalOpKind::TxnBegin => {
            if let Some(txn_id) = record.txn_id {
                ctx.begun_txns.insert(txn_id);
                ctx.open_txns.entry(txn_id).or_default();
            }
        }
        WalOpKind::TxnCommit => {
            if let Some(txn_id) = record.txn_id {
                if ctx.begun_txns.remove(&txn_id) {
                    if let Some(records) = ctx.open_txns.remove(&txn_id) {
                        for buffered in &records {
                            apply_wal_record_to_memtables(
                                buffered,
                                ctx.memtables,
                                ctx.file_apply_ns,
                            )?;
                        }
                    }
                }
            }
        }
        _ => {
            if let Some(txn_id) = record.txn_id {
                if ctx.begun_txns.contains(&txn_id) {
                    ctx.open_txns.entry(txn_id).or_default().push(record);
                    return Ok(());
                }
            }

            apply_wal_record_to_memtables(&record, ctx.memtables, ctx.file_apply_ns)?;
        }
    }

    Ok(())
}

fn apply_wal_record_to_memtables<S: BuildHasher>(
    record: &WalRecord,
    memtables: &mut HashMap<ColumnFamilyId, Arc<SkipListMemtable>, S>,
    file_apply_ns: &mut u128,
) -> MidgeResult<()> {
    let apply_start = std::time::Instant::now();
    apply_record(record, memtables)?;
    *file_apply_ns = file_apply_ns.saturating_add(apply_start.elapsed().as_nanos());
    Ok(())
}

fn finalize_wal_file_replay(
    stats: &mut RecoveryStats,
    file_path: &StoragePath,
    file_read_ns: u128,
    file_apply_ns: u128,
) {
    stats.wal_read_ns = stats.wal_read_ns.saturating_add(file_read_ns);
    stats.apply_ns = stats.apply_ns.saturating_add(file_apply_ns);

    tracing::info!(
        path = %file_path,
        records = stats.record_count,
        bytes = stats.bytes,
        wal_read_ms = std::time::Duration::from_nanos(u64::try_from(file_read_ns).unwrap_or(u64::MAX)).as_secs_f64() * 1000.0,
        apply_ms = std::time::Duration::from_nanos(u64::try_from(file_apply_ns).unwrap_or(u64::MAX)).as_secs_f64() * 1000.0,
        "replayed wal file"
    );
}

#[instrument(
    level = "debug",
    skip(memtables, record),
    fields(cf_id = record.cf_id, seq = record.seq, op = ?record.op)
)]
fn apply_record<S: BuildHasher>(
    record: &WalRecord,
    memtables: &mut HashMap<ColumnFamilyId, Arc<SkipListMemtable>, S>,
) -> MidgeResult<()> {
    // Invariant: memtable reconstruction must match the durable WAL prefix
    // exactly. Expired or incomplete state may be dropped, but visible durable
    // records must be applied in sequence order.
    let memtable = memtables
        .entry(record.cf_id)
        .or_insert_with(|| Arc::new(SkipListMemtable::new()));

    match record.op {
        WalOpKind::Put | WalOpKind::Insert => {
            // Skip expired entries during recovery
            if let Some(exp) = record.expiration {
                let now = std::time::SystemTime::now()
                    .duration_since(std::time::UNIX_EPOCH)
                    .map_or(0, |d| u64::try_from(d.as_millis()).unwrap_or(u64::MAX));
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
            // Apply delete_range to memtable during recovery
            if let Some(end_key) = &record.range_end {
                memtable.delete_range_with_seq(
                    record.key.as_ref(),
                    end_key.as_ref(),
                    record.seq,
                )?;
            }
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

    fn append_raw_bytes(path: &std::path::Path, bytes: &[u8]) {
        use std::io::Write;

        let mut file = std::fs::OpenOptions::new()
            .create(true)
            .append(true)
            .open(path)
            .unwrap();
        file.write_all(bytes).unwrap();
        file.sync_all().unwrap();
    }

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
                1,
            );
            writer.append_record(&put_record).unwrap();

            let delete_record = WalRecord::new(
                WalOpKind::Delete,
                Bytes::from_static(b"test_key"),
                None,
                2,
                1,
            );
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
                1,
            );
            writer.append_record(&put_record).unwrap();

            let delete_record = WalRecord::new(
                WalOpKind::Delete,
                Bytes::from_static(b"test_key"),
                None,
                2,
                1,
            );
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
                1,
            );
            writer.append_record(&record_cf0).unwrap();

            let record_cf1 = WalRecord::new_cf(
                1,
                WalOpKind::Put,
                Bytes::from_static(b"key1"),
                Some(Bytes::from_static(b"value1")),
                2,
                1,
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
                1,
            );
            writer.append_record(&record_cf0).unwrap();

            let record_cf1 = WalRecord::new_cf(
                1,
                WalOpKind::Put,
                Bytes::from_static(b"key1"),
                Some(Bytes::from_static(b"value1")),
                2,
                1,
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
                1,
            );
            writer.append_record(&record_cf0).unwrap();

            let record_cf1 = WalRecord::new_cf(
                1,
                WalOpKind::Put,
                Bytes::from_static(b"key1"),
                Some(Bytes::from_static(b"value1")),
                2,
                1,
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
                1,
            );
            writer.append_record(&record1).unwrap();

            let record2 = WalRecord::new(
                WalOpKind::Put,
                Bytes::from_static(b"key2"),
                Some(Bytes::from_static(b"value2")),
                10,
                1,
            );
            writer.append_record(&record2).unwrap();

            let record3 = WalRecord::new(
                WalOpKind::Put,
                Bytes::from_static(b"key3"),
                Some(Bytes::from_static(b"value3")),
                7,
                1,
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
                1,
            );
            writer.append_record(&record1).unwrap();

            let record2 = WalRecord::new(
                WalOpKind::Put,
                Bytes::from_static(b"key2"),
                Some(Bytes::from_static(b"value2")),
                10,
                1,
            );
            writer.append_record(&record2).unwrap();

            let record3 = WalRecord::new(
                WalOpKind::Put,
                Bytes::from_static(b"key3"),
                Some(Bytes::from_static(b"value3")),
                7,
                1,
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
                1,
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
                1,
            );
            writer.append_record(&put_record).unwrap();

            // DeleteRange is currently a no-op, but should not cause errors
            let mut delete_range_record = WalRecord::new(
                WalOpKind::DeleteRange,
                Bytes::from_static(b"start"),
                None,
                2,
                1,
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

            let begin_record = WalRecord::new(
                WalOpKind::TxnBegin,
                Bytes::from_static(b"txn_key"),
                None,
                1,
                1,
            );
            writer.append_record(&begin_record).unwrap();

            let put_record = WalRecord::new(
                WalOpKind::Put,
                Bytes::from_static(b"key"),
                Some(Bytes::from_static(b"value")),
                2,
                1,
            );
            writer.append_record(&put_record).unwrap();

            let commit_record = WalRecord::new(
                WalOpKind::TxnCommit,
                Bytes::from_static(b"txn_key"),
                None,
                3,
                1,
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

    #[test]
    fn should_not_apply_transaction_ops_without_commit_marker_during_recovery() {
        // Arrange
        let dir = TempDir::new().unwrap();
        let wal_subdir = dir.path().join("wal");
        std::fs::create_dir(&wal_subdir).unwrap();
        let storage = LocalFsStorage::new(dir.path()).unwrap();
        let wal_dir = StoragePath::new("wal");

        {
            let fs = Arc::new(RealFs::new(&wal_subdir).unwrap());
            let writer = FsWalWriterIo::new("wal.log", fs as Arc<dyn crate::io::Fs>).unwrap();

            let mut begin_record =
                WalRecord::new(WalOpKind::TxnBegin, Bytes::from_static(b"txn"), None, 1, 1);
            begin_record.txn_id = Some(42);
            writer.append_record(&begin_record).unwrap();

            let mut put_record = WalRecord::new(
                WalOpKind::Put,
                Bytes::from_static(b"key"),
                Some(Bytes::from_static(b"value")),
                2,
                1,
            );
            put_record.txn_id = Some(42);
            writer.append_record(&put_record).unwrap();

            writer.sync().unwrap();
        }

        // Act
        let mut memtables = HashMap::new();
        let stats = replay_wal(&storage, &wal_dir, &mut memtables).unwrap();

        // Assert
        assert_eq!(stats.record_count, 2);
        assert!(
            !memtables.contains_key(&0),
            "incomplete transactions must not materialize a recovered memtable entry"
        );
    }

    #[test]
    fn should_fail_strict_recovery_on_bad_crc_at_byte_zero() {
        // Arrange
        let dir = TempDir::new().unwrap();
        let wal_subdir = dir.path().join("wal");
        std::fs::create_dir(&wal_subdir).unwrap();
        let storage = LocalFsStorage::new(dir.path()).unwrap();
        let wal_dir = StoragePath::new("wal");
        let wal_path = wal_subdir.join("wal.log");

        let record = WalRecord::new(
            WalOpKind::Put,
            Bytes::from_static(b"crc_key"),
            Some(Bytes::from_static(b"crc_value")),
            1,
            9,
        );
        let payload = crate::wal::encoding::encode(&record).unwrap();
        let mut frame = Vec::new();
        crate::wal::frame::append_frame(&mut frame, &payload).unwrap();
        frame[4] ^= 0x5a;
        append_raw_bytes(&wal_path, &frame);

        let mut memtables = HashMap::new();

        // Act
        let err = replay_wal_with_policy(&storage, &wal_dir, &mut memtables, ReplayPolicy::Strict)
            .unwrap_err();

        // Assert
        match err {
            MidgeError::Corruption(msg) => assert!(msg.contains("CRC mismatch")),
            other => panic!("expected corruption error, got {other:?}"),
        }
    }

    #[test]
    fn should_salvage_valid_prefix_on_bad_crc_tail() {
        // Arrange
        let dir = TempDir::new().unwrap();
        let wal_subdir = dir.path().join("wal");
        std::fs::create_dir(&wal_subdir).unwrap();
        let storage = LocalFsStorage::new(dir.path()).unwrap();
        let wal_dir = StoragePath::new("wal");
        let wal_path = wal_subdir.join("wal.log");

        {
            let fs = Arc::new(RealFs::new(&wal_subdir).unwrap());
            let writer = FsWalWriterIo::new("wal.log", fs as Arc<dyn crate::io::Fs>).unwrap();
            let record = WalRecord::new(
                WalOpKind::Put,
                Bytes::from_static(b"good"),
                Some(Bytes::from_static(b"value")),
                1,
                2,
            );
            writer.append_record(&record).unwrap();
            writer.sync().unwrap();
        }

        let bad_record = WalRecord::new(
            WalOpKind::Put,
            Bytes::from_static(b"bad"),
            Some(Bytes::from_static(b"value")),
            2,
            2,
        );
        let payload = crate::wal::encoding::encode(&bad_record).unwrap();
        let mut frame = Vec::new();
        crate::wal::frame::append_frame(&mut frame, &payload).unwrap();
        frame[5] ^= 0x11;
        append_raw_bytes(&wal_path, &frame);

        let mut memtables = HashMap::new();

        // Act
        let stats = replay_wal_with_policy(
            &storage,
            &wal_dir,
            &mut memtables,
            ReplayPolicy::SalvageValidPrefix,
        )
        .unwrap();

        // Assert
        assert!(stats.had_corruption);
        assert_eq!(stats.record_count, 1);
        assert_eq!(memtables[&0].get(b"good").unwrap(), Some(b"value".to_vec()));
        assert_eq!(memtables[&0].get(b"bad").unwrap(), None);
    }

    #[test]
    fn should_salvage_valid_prefix_on_truncated_tail_frame() {
        // Arrange
        let dir = TempDir::new().unwrap();
        let wal_subdir = dir.path().join("wal");
        std::fs::create_dir(&wal_subdir).unwrap();
        let storage = LocalFsStorage::new(dir.path()).unwrap();
        let wal_dir = StoragePath::new("wal");
        let wal_path = wal_subdir.join("wal.log");

        {
            let fs = Arc::new(RealFs::new(&wal_subdir).unwrap());
            let writer = FsWalWriterIo::new("wal.log", fs as Arc<dyn crate::io::Fs>).unwrap();
            let record = WalRecord::new(
                WalOpKind::Put,
                Bytes::from_static(b"good"),
                Some(Bytes::from_static(b"value")),
                1,
                3,
            );
            writer.append_record(&record).unwrap();
            writer.sync().unwrap();
        }

        let tail_record = WalRecord::new(
            WalOpKind::Put,
            Bytes::from_static(b"tail"),
            Some(Bytes::from_static(b"value")),
            2,
            3,
        );
        let payload = crate::wal::encoding::encode(&tail_record).unwrap();
        let mut frame = Vec::new();
        crate::wal::frame::append_frame(&mut frame, &payload).unwrap();
        frame.truncate(frame.len() - 3);
        append_raw_bytes(&wal_path, &frame);

        let mut memtables = HashMap::new();

        // Act
        let stats = replay_wal_with_policy(
            &storage,
            &wal_dir,
            &mut memtables,
            ReplayPolicy::SalvageValidPrefix,
        )
        .unwrap();

        // Assert
        assert!(!stats.had_corruption);
        assert_eq!(stats.record_count, 1);
        assert_eq!(memtables[&0].get(b"good").unwrap(), Some(b"value".to_vec()));
        assert_eq!(memtables[&0].get(b"tail").unwrap(), None);
    }

    #[test]
    fn should_skip_stale_writer_epoch_records() {
        // Arrange
        let dir = TempDir::new().unwrap();
        let wal_subdir = dir.path().join("wal");
        std::fs::create_dir(&wal_subdir).unwrap();
        let storage = LocalFsStorage::new(dir.path()).unwrap();
        let wal_dir = StoragePath::new("wal");

        {
            let fs = Arc::new(RealFs::new(&wal_subdir).unwrap());
            let writer = FsWalWriterIo::new("wal.log", fs as Arc<dyn crate::io::Fs>).unwrap();

            let fresh = WalRecord::new(
                WalOpKind::Put,
                Bytes::from_static(b"fresh"),
                Some(Bytes::from_static(b"v2")),
                1,
                2,
            );
            writer.append_record(&fresh).unwrap();

            let stale = WalRecord::new(
                WalOpKind::Put,
                Bytes::from_static(b"stale"),
                Some(Bytes::from_static(b"v1")),
                2,
                1,
            );
            writer.append_record(&stale).unwrap();
            writer.sync().unwrap();
        }

        let mut memtables = HashMap::new();

        // Act
        let stats = replay_wal(&storage, &wal_dir, &mut memtables).unwrap();

        // Assert
        assert_eq!(stats.max_epoch_seen, 2);
        assert_eq!(stats.stale_records_skipped, 1);
        assert_eq!(memtables[&0].get(b"fresh").unwrap(), Some(b"v2".to_vec()));
        assert_eq!(memtables[&0].get(b"stale").unwrap(), None);
    }
}
