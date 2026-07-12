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
use crate::io::{File, Fs, FsError, FsPath, OpenMode, OpenOptions};
#[cfg(test)]
use crate::sst::Memtable;
use crate::sst::SkipListMemtable;
use std::collections::HashMap;
use std::hash::BuildHasher;
use std::io::{Read as _, Seek as _, Write as _};
use std::sync::Arc;
use tracing::instrument;

struct RecoveryTxnSpool {
    file: std::fs::File,
    record_count: usize,
}

impl RecoveryTxnSpool {
    fn new() -> MidgeResult<Self> {
        Ok(Self {
            // Anonymous/delete-on-close storage cannot leak a named recovery
            // artifact if the process crashes during replay.
            file: tempfile::tempfile()?,
            record_count: 0,
        })
    }

    fn append(&mut self, record: &WalRecord) -> MidgeResult<()> {
        let payload = super::encoding::encode(record)?;
        let mut frame =
            Vec::with_capacity(super::frame::WAL_FRAME_HEADER_LEN.saturating_add(payload.len()));
        super::frame::append_frame(&mut frame, &payload)?;
        self.file.write_all(&frame)?;
        self.record_count = self.record_count.saturating_add(1);
        Ok(())
    }

    fn replay(mut self, mut visitor: impl FnMut(WalRecord) -> MidgeResult<()>) -> MidgeResult<()> {
        self.file.flush()?;
        self.file.seek(std::io::SeekFrom::Start(0))?;
        for _ in 0..self.record_count {
            let mut header = [0_u8; super::frame::WAL_FRAME_HEADER_LEN];
            self.file.read_exact(&mut header)?;
            let (payload_len, expected_crc) = super::frame::decode_frame_header(&header)?;
            let mut payload = vec![0_u8; payload_len];
            self.file.read_exact(&mut payload)?;
            super::frame::verify_frame_crc(&payload, expected_crc)?;
            visitor(super::encoding::decode(payload.as_slice())?)?;
        }
        let mut trailing = [0_u8; 1];
        if self.file.read(&mut trailing)? != 0 {
            return Err(MidgeError::Corruption(
                "transaction recovery spool has trailing bytes".to_string(),
            ));
        }
        Ok(())
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum ReplayPolicy {
    Strict,
    SalvageValidPrefix,
}

fn is_incomplete_eof_frame(error: &MidgeError) -> bool {
    let MidgeError::Corruption(message) = error else {
        return false;
    };
    let lower = message.to_ascii_lowercase();
    lower.contains("incomplete wal frame header") || lower.contains("incomplete wal record")
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
enum ReplayFileKind {
    Sealed,
    FinalActive,
}

#[derive(Debug)]
struct ReplayFile {
    path: FsPath,
    kind: ReplayFileKind,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
enum ReplayErrorAction {
    TolerateFinalActiveTail,
    SalvageVerifiedPrefix,
    Fail,
}

fn replay_error_action(
    replay_file: &ReplayFile,
    replay_policy: ReplayPolicy,
    error: &MidgeError,
) -> ReplayErrorAction {
    if replay_file.kind == ReplayFileKind::FinalActive && is_incomplete_eof_frame(error) {
        ReplayErrorAction::TolerateFinalActiveTail
    } else if replay_policy == ReplayPolicy::SalvageValidPrefix
        && matches!(error, MidgeError::Corruption(_))
    {
        ReplayErrorAction::SalvageVerifiedPrefix
    } else {
        ReplayErrorAction::Fail
    }
}

fn map_fs_error(err: FsError) -> MidgeError {
    err.into()
}

fn join(dir: &FsPath, leaf: &str) -> FsPath {
    let base = dir.0.trim_end_matches('/');
    if base.is_empty() {
        FsPath::new(leaf)
    } else {
        FsPath::new(format!("{base}/{leaf}"))
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

    /// Highest writer epoch seen across all replayable WAL records.
    /// Lower-epoch records are skipped when they overlap a newer epoch's
    /// sequence frontier or appear after a newer epoch in replay order.
    pub max_epoch_seen: u64,
    /// Number of WAL records skipped because their `writer_epoch` was stale.
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
    storage: &dyn Fs,
    wal_dir: &FsPath,
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
    storage: &dyn Fs,
    wal_dir: &FsPath,
    memtables: &mut HashMap<ColumnFamilyId, Arc<SkipListMemtable>, S>,
    replay_policy: ReplayPolicy,
) -> MidgeResult<RecoveryStats> {
    // Invariant: recovery may keep only a verified prefix of the WAL, but it
    // must never materialize a partial frame or reorder committed records.
    let mut stats = RecoveryStats::new();
    let replay_start = std::time::Instant::now();

    // Transaction buffering for atomic recovery.
    //
    // Legacy split-marker transactions are buffered until TxnCommit.
    // Current TxnBatch records apply atomically from a single validated frame.
    let mut open_txns = std::collections::HashMap::<(u64, u64), RecoveryTxnSpool>::new();

    tracing::info!(dir = %wal_dir, "starting wal replay");

    let replay_paths = collect_replay_paths(storage, wal_dir)?;
    let (epoch_frontiers, max_epoch_scan_had_corruption) =
        discover_writer_epoch_frontiers(storage, &replay_paths, replay_policy)?;
    let max_epoch_seen = epoch_frontiers.max_epoch_seen();
    stats.max_epoch_seen = max_epoch_seen;
    if max_epoch_scan_had_corruption {
        stats.mark_corruption();
    }

    let result = {
        let mut replay_state = WalReplayState {
            stats: &mut stats,
            memtables,
            open_txns: &mut open_txns,
            epoch_frontiers: &epoch_frontiers,
            replay_ordinal: 0,
        };
        replay_wal_paths(storage, &replay_paths, replay_policy, &mut replay_state)
    };

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
            stats.mark_corruption();
            tracing::warn!(dir = %wal_dir, error = %e, "wal replay encountered corruption");
            Err(MidgeError::Corruption(e))
        }
        Err(e) => {
            tracing::error!(dir = %wal_dir, error = %e, "wal replay failed");
            Err(e)
        }
    }
}

fn collect_replay_paths(storage: &dyn Fs, wal_dir: &FsPath) -> MidgeResult<Vec<ReplayFile>> {
    if !storage.exists(wal_dir).map_err(map_fs_error)? {
        return Ok(Vec::new());
    }

    let mut segment_files: std::collections::BTreeMap<u64, (String, FsPath)> =
        std::collections::BTreeMap::new();
    let mut wal_log_path: Option<FsPath> = None;

    let entries = match storage.list_dir(wal_dir) {
        Ok(v) => v,
        Err(FsError::NotFound(_)) => return Ok(Vec::new()),
        Err(e) => return Err(map_fs_error(e)),
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
                        existing_name != &crate::wal::cloud_segment::file_name(segment_id)
                            && file_name == crate::wal::cloud_segment::file_name(segment_id)
                    });

            if prefer_candidate {
                segment_files.insert(segment_id, (file_name.clone(), join(wal_dir, &file_name)));
            }
        }
    }

    let mut replay_paths: Vec<ReplayFile> = segment_files
        .into_iter()
        .map(|(_, (_, path))| ReplayFile {
            path,
            kind: ReplayFileKind::Sealed,
        })
        .collect();
    if let Some(wal_log) = wal_log_path {
        replay_paths.push(ReplayFile {
            path: wal_log,
            kind: ReplayFileKind::FinalActive,
        });
    }

    Ok(replay_paths)
}

struct WalReplayState<'a, S: BuildHasher> {
    stats: &'a mut RecoveryStats,
    memtables: &'a mut HashMap<ColumnFamilyId, Arc<SkipListMemtable>, S>,
    open_txns: &'a mut std::collections::HashMap<(u64, u64), RecoveryTxnSpool>,
    epoch_frontiers: &'a WriterEpochFrontiers,
    replay_ordinal: u64,
}

fn replay_wal_paths<S: BuildHasher>(
    storage: &dyn Fs,
    replay_paths: &[ReplayFile],
    replay_policy: ReplayPolicy,
    replay_state: &mut WalReplayState<'_, S>,
) -> MidgeResult<()> {
    for replay_file in replay_paths {
        if let Err(error) = replay_wal_file(storage, &replay_file.path, replay_state) {
            match replay_error_action(replay_file, replay_policy, &error) {
                ReplayErrorAction::TolerateFinalActiveTail => {
                    tracing::info!(
                        path = %replay_file.path,
                        error = %error,
                        "wal replay dropped an incomplete final active tail"
                    );
                    return Ok(());
                }
                ReplayErrorAction::SalvageVerifiedPrefix => {
                    replay_state.stats.mark_corruption();
                    tracing::warn!(
                        path = %replay_file.path,
                        error = %error,
                        "wal replay stopped at corrupt verified-prefix boundary"
                    );
                    return Ok(());
                }
                ReplayErrorAction::Fail => return Err(error),
            }
        }
    }

    Ok(())
}

#[derive(Debug, Default)]
struct WriterEpochFrontiers {
    first_sequence_by_epoch: std::collections::BTreeMap<u64, u64>,
    first_ordinal_by_epoch: std::collections::BTreeMap<u64, u64>,
}

impl WriterEpochFrontiers {
    fn record(&mut self, record: &WalRecord, ordinal: u64) {
        if record.writer_epoch == 0 {
            return;
        }
        self.first_sequence_by_epoch
            .entry(record.writer_epoch)
            .and_modify(|seq| *seq = (*seq).min(record.seq))
            .or_insert(record.seq);
        self.first_ordinal_by_epoch
            .entry(record.writer_epoch)
            .and_modify(|first_ordinal| *first_ordinal = (*first_ordinal).min(ordinal))
            .or_insert(ordinal);
    }

    fn max_epoch_seen(&self) -> u64 {
        self.first_sequence_by_epoch
            .keys()
            .next_back()
            .copied()
            .unwrap_or(0)
    }

    fn is_stale(&self, record: &WalRecord, ordinal: u64) -> bool {
        if record.writer_epoch == 0 {
            return false;
        }

        self.first_sequence_by_epoch
            .range((
                std::ops::Bound::Excluded(record.writer_epoch),
                std::ops::Bound::Unbounded,
            ))
            .any(|(epoch, first_seq)| {
                record.seq >= *first_seq
                    || self
                        .first_ordinal_by_epoch
                        .get(epoch)
                        .is_some_and(|first_ordinal| *first_ordinal < ordinal)
            })
    }
}

fn discover_writer_epoch_frontiers(
    storage: &dyn Fs,
    replay_paths: &[ReplayFile],
    replay_policy: ReplayPolicy,
) -> MidgeResult<(WriterEpochFrontiers, bool)> {
    let mut frontiers = WriterEpochFrontiers::default();
    let mut had_corruption = false;
    let mut ordinal = 0_u64;

    for replay_file in replay_paths {
        let mut pos = 0_u64;
        let mut file_read_ns = 0_u128;

        loop {
            let Some(file) = open_wal_replay_file(storage, &replay_file.path, &mut file_read_ns)?
            else {
                break;
            };

            match read_next_wal_frame(&*file, &replay_file.path, pos, &mut file_read_ns) {
                Ok(NextWalFrame::Eof) => break,
                Ok(NextWalFrame::Frame(frame)) => {
                    frontiers.record(&frame.record, ordinal);
                    ordinal = ordinal.saturating_add(1);
                    pos = frame.next_pos;
                }
                Err(error) => match replay_error_action(replay_file, replay_policy, &error) {
                    ReplayErrorAction::TolerateFinalActiveTail => {
                        tracing::info!(
                            path = %replay_file.path,
                            error = %error,
                            "writer epoch discovery dropped an incomplete final active tail"
                        );
                        return Ok((frontiers, had_corruption));
                    }
                    ReplayErrorAction::SalvageVerifiedPrefix => {
                        had_corruption = true;
                        tracing::warn!(
                            path = %replay_file.path,
                            error = %error,
                            "stopped writer epoch discovery at corrupt WAL prefix boundary"
                        );
                        return Ok((frontiers, had_corruption));
                    }
                    ReplayErrorAction::Fail => return Err(error),
                },
            }
        }
    }

    Ok((frontiers, had_corruption))
}

fn replay_wal_file<S: BuildHasher>(
    storage: &dyn Fs,
    file_path: &FsPath,
    replay_state: &mut WalReplayState<'_, S>,
) -> MidgeResult<()> {
    let mut pos: u64 = 0;
    let mut file_read_ns: u128 = 0;
    let mut file_apply_ns: u128 = 0;

    loop {
        let Some(file) = open_wal_replay_file(storage, file_path, &mut file_read_ns)? else {
            finalize_wal_file_replay(
                &mut *replay_state.stats,
                file_path,
                file_read_ns,
                file_apply_ns,
            );
            return Ok(());
        };

        match read_next_wal_frame(&*file, file_path, pos, &mut file_read_ns)? {
            NextWalFrame::Eof => break,
            NextWalFrame::Frame(frame) => {
                let next_pos = frame.next_pos;
                let record_ordinal = replay_state.replay_ordinal;
                let mut apply_ctx = WalReplayApplyContext {
                    file_path,
                    stats: &mut *replay_state.stats,
                    memtables: &mut *replay_state.memtables,
                    open_txns: &mut *replay_state.open_txns,
                    epoch_frontiers: replay_state.epoch_frontiers,
                    file_apply_ns: &mut file_apply_ns,
                };
                apply_replayed_wal_record(&frame.record, pos, record_ordinal, &mut apply_ctx)?;
                replay_state.replay_ordinal = replay_state.replay_ordinal.saturating_add(1);
                pos = next_pos;
            }
        }
    }

    finalize_wal_file_replay(
        &mut *replay_state.stats,
        file_path,
        file_read_ns,
        file_apply_ns,
    );
    Ok(())
}

struct ReplayedWalFrame {
    record: WalRecord,
    next_pos: u64,
}

struct WalReplayApplyContext<'a, S: BuildHasher> {
    file_path: &'a FsPath,
    stats: &'a mut RecoveryStats,
    memtables: &'a mut HashMap<ColumnFamilyId, Arc<SkipListMemtable>, S>,
    open_txns: &'a mut std::collections::HashMap<(u64, u64), RecoveryTxnSpool>,
    epoch_frontiers: &'a WriterEpochFrontiers,
    file_apply_ns: &'a mut u128,
}

enum NextWalFrame {
    Eof,
    Frame(ReplayedWalFrame),
}

fn open_wal_replay_file<'a>(
    storage: &'a dyn Fs,
    file_path: &FsPath,
    file_read_ns: &mut u128,
) -> MidgeResult<Option<Box<dyn File + 'a>>> {
    if !storage.exists(file_path).map_err(map_fs_error)? {
        return Ok(None);
    }

    let open_start = std::time::Instant::now();
    let file = match storage.open(
        file_path,
        OpenOptions {
            mode: OpenMode::ReadOnly,
            create: false,
            create_new: false,
            truncate: false,
        },
    ) {
        Ok(file) => file,
        Err(FsError::NotFound(_)) => return Ok(None),
        Err(e) => return Err(map_fs_error(e)),
    };
    *file_read_ns = file_read_ns.saturating_add(open_start.elapsed().as_nanos());
    Ok(Some(file))
}

fn read_next_wal_frame(
    file: &dyn File,
    file_path: &FsPath,
    pos: u64,
    file_read_ns: &mut u128,
) -> MidgeResult<NextWalFrame> {
    let file_len = file.len().map_err(map_fs_error)?;
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
    file: &dyn File,
    pos: u64,
    file_read_ns: &mut u128,
) -> MidgeResult<(usize, u32)> {
    let header_read_start = std::time::Instant::now();
    let header = file
        .read_at(pos, crate::wal::frame::WAL_FRAME_HEADER_LEN as u64)
        .map_err(map_fs_error)?;
    *file_read_ns = file_read_ns.saturating_add(header_read_start.elapsed().as_nanos());
    crate::wal::frame::decode_frame_header(&header[..])
}

fn read_wal_frame_payload(
    file: &dyn File,
    file_path: &FsPath,
    pos: u64,
    len: usize,
    file_len: u64,
    file_read_ns: &mut u128,
) -> MidgeResult<bytes::Bytes> {
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
        .map_err(map_fs_error)?;
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
    record: &WalRecord,
    pos: u64,
    record_ordinal: u64,
    ctx: &mut WalReplayApplyContext<'_, S>,
) -> MidgeResult<()> {
    if ctx.epoch_frontiers.is_stale(record, record_ordinal) {
        ctx.stats.stale_records_skipped += 1;
        tracing::warn!(
            epoch = record.writer_epoch,
            max_epoch = ctx.stats.max_epoch_seen,
            seq = record.seq,
            ordinal = record_ordinal,
            op = ?record.op,
            pos = pos,
            file = %ctx.file_path,
            "skipping WAL record from stale writer epoch"
        );
        return Ok(());
    }

    ctx.stats.record(record);

    match record.op {
        WalOpKind::TxnBatch => {
            let payload = record.value.as_ref().ok_or_else(|| {
                MidgeError::Corruption("transaction batch record missing payload".into())
            })?;
            let batch = super::encoding::decode_txn_batch_payload(record, payload)?;
            for buffered in batch.records {
                let replay_record = WalRecord {
                    cf_id: buffered.cf_id,
                    op: buffered.op,
                    key: buffered.key,
                    value: buffered.value,
                    seq: buffered.seq,
                    expiration: buffered.expiration,
                    range_end: buffered.range_end,
                    txn_id: Some(batch.txn_id),
                    writer_epoch: batch.writer_epoch,
                    compression: None,
                };
                apply_wal_record_to_memtables(&replay_record, ctx.memtables, ctx.file_apply_ns)?;
            }
        }
        WalOpKind::TxnBegin => {
            if let Some(txn_id) = record.txn_id {
                let key = (record.writer_epoch, txn_id);
                match ctx.open_txns.entry(key) {
                    std::collections::hash_map::Entry::Vacant(entry) => {
                        entry.insert(RecoveryTxnSpool::new()?);
                    }
                    std::collections::hash_map::Entry::Occupied(_) => {
                        return Err(MidgeError::Corruption(format!(
                            "duplicate transaction begin for writer epoch {} transaction {txn_id}",
                            record.writer_epoch
                        )));
                    }
                }
            }
        }
        WalOpKind::TxnCommit => {
            if let Some(txn_id) = record.txn_id {
                if let Some(spool) = ctx.open_txns.remove(&(record.writer_epoch, txn_id)) {
                    spool.replay(|buffered| {
                        apply_wal_record_to_memtables(&buffered, ctx.memtables, ctx.file_apply_ns)
                    })?;
                }
            }
        }
        _ => {
            if let Some(txn_id) = record.txn_id {
                if let Some(spool) = ctx.open_txns.get_mut(&(record.writer_epoch, txn_id)) {
                    spool.append(record)?;
                    return Ok(());
                }
            }

            apply_wal_record_to_memtables(record, ctx.memtables, ctx.file_apply_ns)?;
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
    file_path: &FsPath,
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
            // Preserve an expired write as a masking version. Dropping it
            // would allow an older value to resurrect after restart.
            if let Some(exp) = record.expiration {
                if crate::common::time::is_expired_at(
                    Some(exp),
                    crate::common::time::unix_time_millis(),
                ) {
                    memtable.delete_with_seq(record.key.to_vec(), record.seq)?;
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
        WalOpKind::TxnBegin | WalOpKind::TxnCommit | WalOpKind::TxnBatch => {
            // Transaction markers carry no direct memtable mutation.
        }
    }

    Ok(())
}

#[cfg(test)]
mod tests;
