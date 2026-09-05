//! WAL recovery - replay WAL files to restore state after crash
//!
//! On startup, persistent WAL files are replayed to reconstruct the
//! memtables for each column family.
//!
//! Recovery order:
//! 1) Rotated segment files: `{segment_id}.wal` in ascending `segment_id` order
//! 2) Active file: `wal.log` (if present)

use super::types::{ColumnFamilyId, WalOpRole, WalRecord};
use crate::common::{MidgeError, MidgeResult};
use crate::io::{File, Fs, FsError, FsPath, OpenMode, OpenOptions};
use crate::sst::SkipListMemtable;
use std::collections::HashMap;
use std::hash::BuildHasher;
use std::io::{Read as _, Seek as _, Write as _};
use std::sync::Arc;
use tracing::instrument;

#[cfg(test)]
thread_local! {
    static WAL_REPLAY_FILE_OPEN_COUNT: std::cell::Cell<usize> = const { std::cell::Cell::new(0) };
    static WAL_REPLAY_FILE_READ_COUNT: std::cell::Cell<usize> = const { std::cell::Cell::new(0) };
}

#[cfg(test)]
fn reset_wal_replay_file_open_count() {
    WAL_REPLAY_FILE_OPEN_COUNT.set(0);
    WAL_REPLAY_FILE_READ_COUNT.set(0);
}

#[cfg(test)]
fn wal_replay_file_open_count() -> usize {
    WAL_REPLAY_FILE_OPEN_COUNT.get()
}

#[cfg(test)]
fn wal_replay_file_read_count() -> usize {
    WAL_REPLAY_FILE_READ_COUNT.get()
}

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

#[derive(Debug)]
enum ReplayFailure {
    IncompleteTail(MidgeError),
    Error(MidgeError),
}

impl ReplayFailure {
    fn error(&self) -> &MidgeError {
        match self {
            Self::IncompleteTail(error) | Self::Error(error) => error,
        }
    }

    fn into_error(self) -> MidgeError {
        match self {
            Self::IncompleteTail(error) | Self::Error(error) => error,
        }
    }

    fn is_incomplete_tail(&self) -> bool {
        matches!(self, Self::IncompleteTail(_))
    }
}

impl From<MidgeError> for ReplayFailure {
    fn from(error: MidgeError) -> Self {
        Self::Error(error)
    }
}

#[derive(Debug, Clone, Copy, Default, PartialEq, Eq)]
pub(crate) struct VerifiedWalPrefix {
    pub(crate) max_sequence: u64,
    pub(crate) writer_epoch: u64,
    pub(crate) record_count: usize,
    pub(crate) valid_bytes: usize,
}

#[derive(Debug)]
pub(crate) struct WalPrefixInspectionFailure {
    verified_prefix: VerifiedWalPrefix,
    failure: ReplayFailure,
}

impl WalPrefixInspectionFailure {
    pub(crate) fn verified_prefix(&self) -> VerifiedWalPrefix {
        self.verified_prefix
    }

    pub(crate) fn is_incomplete_tail(&self) -> bool {
        self.failure.is_incomplete_tail()
    }

    pub(crate) fn error(&self) -> &MidgeError {
        self.failure.error()
    }
}

fn wal_prefix_failure(
    verified_prefix: VerifiedWalPrefix,
    failure: ReplayFailure,
) -> WalPrefixInspectionFailure {
    WalPrefixInspectionFailure {
        verified_prefix,
        failure,
    }
}

#[cfg(test)]
pub(crate) fn inspect_active_wal_bytes(
    data: &[u8],
) -> Result<VerifiedWalPrefix, WalPrefixInspectionFailure> {
    let mut prefix = VerifiedWalPrefix::default();
    while prefix.valid_bytes < data.len() {
        let Some(header_end) = prefix
            .valid_bytes
            .checked_add(super::frame::WAL_FRAME_HEADER_LEN)
        else {
            return Err(wal_prefix_failure(
                prefix,
                ReplayFailure::Error(MidgeError::Corruption(
                    "active WAL frame offset overflow".to_string(),
                )),
            ));
        };
        if header_end > data.len() {
            return Err(wal_prefix_failure(
                prefix,
                ReplayFailure::IncompleteTail(MidgeError::Corruption(format!(
                    "Incomplete WAL frame header at pos {} (need {} bytes, have {})",
                    prefix.valid_bytes,
                    super::frame::WAL_FRAME_HEADER_LEN,
                    data.len().saturating_sub(prefix.valid_bytes)
                ))),
            ));
        }

        let header = &data[prefix.valid_bytes..header_end];
        if header.iter().all(|byte| *byte == 0)
            && data[prefix.valid_bytes..].iter().all(|byte| *byte == 0)
        {
            return Err(wal_prefix_failure(
                prefix,
                ReplayFailure::IncompleteTail(MidgeError::Corruption(format!(
                    "Zero-filled WAL tail at pos {}",
                    prefix.valid_bytes
                ))),
            ));
        }

        let (payload_len, expected_crc) = match super::frame::decode_frame_header(header) {
            Ok(decoded) => decoded,
            Err(error) => {
                return Err(wal_prefix_failure(prefix, ReplayFailure::Error(error)));
            }
        };
        let Some(payload_end) = header_end.checked_add(payload_len) else {
            return Err(wal_prefix_failure(
                prefix,
                ReplayFailure::Error(MidgeError::Corruption(
                    "active WAL payload offset overflow".to_string(),
                )),
            ));
        };
        if payload_end > data.len() {
            let hides_verified_suffix = contains_verified_wal_frame(&data[header_end..]);
            let error = MidgeError::Corruption(if hides_verified_suffix {
                format!(
                    "WAL frame length at pos {} overruns EOF and hides a verified later frame (len={payload_len}, file_len={})",
                    prefix.valid_bytes,
                    data.len()
                )
            } else {
                format!(
                    "Incomplete WAL record at pos {} (len={payload_len}, file_len={})",
                    prefix.valid_bytes,
                    data.len()
                )
            });
            let failure = if hides_verified_suffix {
                ReplayFailure::Error(error)
            } else {
                ReplayFailure::IncompleteTail(error)
            };
            return Err(wal_prefix_failure(prefix, failure));
        }

        let payload = &data[header_end..payload_end];
        if let Err(error) = super::frame::verify_frame_crc(payload, expected_crc) {
            return Err(wal_prefix_failure(prefix, ReplayFailure::Error(error)));
        }
        let record = match super::encoding::decode(payload) {
            Ok(record) => record,
            Err(error) => {
                return Err(wal_prefix_failure(prefix, ReplayFailure::Error(error)));
            }
        };
        // An active WAL may span a failover and therefore increase epochs,
        // but it must never return to an older, fenced writer.
        if prefix.record_count > 0 && record.writer_epoch < prefix.writer_epoch {
            return Err(wal_prefix_failure(
                prefix,
                ReplayFailure::Error(MidgeError::Corruption(format!(
                    "active WAL writer epoch regressed from {} to {}",
                    prefix.writer_epoch, record.writer_epoch
                ))),
            ));
        }
        prefix.writer_epoch = record.writer_epoch;
        prefix.max_sequence = prefix.max_sequence.max(record.seq);
        prefix.record_count = prefix.record_count.saturating_add(1);
        prefix.valid_bytes = payload_end;
    }
    Ok(prefix)
}

fn replay_error_action(
    replay_file: &ReplayFile,
    replay_policy: ReplayPolicy,
    failure: &ReplayFailure,
) -> ReplayErrorAction {
    if replay_file.kind == ReplayFileKind::FinalActive && failure.is_incomplete_tail() {
        ReplayErrorAction::TolerateFinalActiveTail
    } else if replay_policy == ReplayPolicy::SalvageValidPrefix
        && matches!(failure.error(), MidgeError::Corruption(_))
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
    replay_wal_with_policy_and_filter(storage, wal_dir, memtables, replay_policy, None)
}

pub(crate) fn replay_wal_with_manifest_filter<S: BuildHasher>(
    storage: &dyn Fs,
    wal_dir: &FsPath,
    memtables: &mut HashMap<ColumnFamilyId, Arc<SkipListMemtable>, S>,
    replay_policy: ReplayPolicy,
    should_apply: &dyn Fn(&WalRecord) -> bool,
) -> MidgeResult<RecoveryStats> {
    replay_wal_with_policy_and_filter(
        storage,
        wal_dir,
        memtables,
        replay_policy,
        Some(should_apply),
    )
}

fn replay_wal_with_policy_and_filter<S: BuildHasher>(
    storage: &dyn Fs,
    wal_dir: &FsPath,
    memtables: &mut HashMap<ColumnFamilyId, Arc<SkipListMemtable>, S>,
    replay_policy: ReplayPolicy,
    should_apply: Option<&dyn Fn(&WalRecord) -> bool>,
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
            should_apply,
            seen_records: std::collections::HashMap::new(),
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
    should_apply: Option<&'a dyn Fn(&WalRecord) -> bool>,
    seen_records: std::collections::HashMap<WalRecord, String>,
    replay_ordinal: u64,
}

fn replay_wal_paths<S: BuildHasher>(
    storage: &dyn Fs,
    replay_paths: &[ReplayFile],
    replay_policy: ReplayPolicy,
    replay_state: &mut WalReplayState<'_, S>,
) -> MidgeResult<()> {
    for replay_file in replay_paths {
        if let Err(failure) = replay_wal_file(storage, &replay_file.path, replay_state) {
            match replay_error_action(replay_file, replay_policy, &failure) {
                ReplayErrorAction::TolerateFinalActiveTail => {
                    tracing::info!(
                        path = %replay_file.path,
                        error = %failure.error(),
                        "wal replay dropped an incomplete final active tail"
                    );
                    return Ok(());
                }
                ReplayErrorAction::SalvageVerifiedPrefix => {
                    replay_state.stats.mark_corruption();
                    tracing::warn!(
                        path = %replay_file.path,
                        error = %failure.error(),
                        "wal replay stopped at corrupt verified-prefix boundary"
                    );
                    return Ok(());
                }
                ReplayErrorAction::Fail => return Err(failure.into_error()),
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
        let Some(file) = open_wal_replay_file(storage, &replay_file.path, &mut file_read_ns)?
        else {
            continue;
        };
        let snapshot = read_wal_snapshot(&*file, &replay_file.path, &mut file_read_ns)?;

        loop {
            match read_next_wal_frame(&snapshot, &replay_file.path, pos) {
                Ok(NextWalFrame::Eof) => break,
                Ok(NextWalFrame::Frame(frame)) => {
                    frontiers.record(&frame.record, ordinal);
                    ordinal = ordinal.saturating_add(1);
                    pos = frame.next_pos;
                }
                Err(failure) => match replay_error_action(replay_file, replay_policy, &failure) {
                    ReplayErrorAction::TolerateFinalActiveTail => {
                        tracing::info!(
                            path = %replay_file.path,
                            error = %failure.error(),
                            "writer epoch discovery dropped an incomplete final active tail"
                        );
                        return Ok((frontiers, had_corruption));
                    }
                    ReplayErrorAction::SalvageVerifiedPrefix => {
                        had_corruption = true;
                        tracing::warn!(
                            path = %replay_file.path,
                            error = %failure.error(),
                            "stopped writer epoch discovery at corrupt WAL prefix boundary"
                        );
                        return Ok((frontiers, had_corruption));
                    }
                    ReplayErrorAction::Fail => return Err(failure.into_error()),
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
) -> Result<(), ReplayFailure> {
    let mut pos: u64 = 0;
    let mut file_read_ns: u128 = 0;
    let mut file_apply_ns: u128 = 0;
    let Some(file) = open_wal_replay_file(storage, file_path, &mut file_read_ns)? else {
        finalize_wal_file_replay(
            &mut *replay_state.stats,
            file_path,
            file_read_ns,
            file_apply_ns,
        );
        return Ok(());
    };
    let snapshot = read_wal_snapshot(&*file, file_path, &mut file_read_ns)?;

    loop {
        match read_next_wal_frame(&snapshot, file_path, pos)? {
            NextWalFrame::Eof => break,
            NextWalFrame::Frame(frame) => {
                let next_pos = frame.next_pos;
                let source = file_path.to_string();
                // Writer-epoch discovery assigns an ordinal to every verified
                // frame. Keep this second pass in lockstep even when replay
                // suppresses a cross-file duplicate.
                let record_ordinal = replay_state.replay_ordinal;
                replay_state.replay_ordinal = replay_state.replay_ordinal.saturating_add(1);
                if replay_state
                    .seen_records
                    .get(&frame.record)
                    .is_some_and(|first_source| first_source != &source)
                {
                    pos = next_pos;
                    continue;
                }
                replay_state
                    .seen_records
                    .entry(frame.record.clone())
                    .or_insert(source);
                let mut apply_ctx = WalReplayApplyContext {
                    file_path,
                    stats: &mut *replay_state.stats,
                    memtables: &mut *replay_state.memtables,
                    open_txns: &mut *replay_state.open_txns,
                    epoch_frontiers: replay_state.epoch_frontiers,
                    should_apply: replay_state.should_apply,
                    file_apply_ns: &mut file_apply_ns,
                };
                apply_replayed_wal_record(&frame.record, pos, record_ordinal, &mut apply_ctx)?;
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
    should_apply: Option<&'a dyn Fn(&WalRecord) -> bool>,
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
    #[cfg(test)]
    WAL_REPLAY_FILE_OPEN_COUNT.set(WAL_REPLAY_FILE_OPEN_COUNT.get().saturating_add(1));
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
    snapshot: &[u8],
    file_path: &FsPath,
    pos: u64,
) -> Result<NextWalFrame, ReplayFailure> {
    let file_len = u64::try_from(snapshot.len()).map_err(|_| {
        MidgeError::Corruption(format!(
            "WAL snapshot length does not fit u64 in {file_path}"
        ))
    })?;
    if pos == file_len {
        return Ok(NextWalFrame::Eof);
    }
    if pos > file_len {
        return Err(MidgeError::Corruption(format!(
            "WAL replay read past EOF at pos {pos} in {file_path} (file_len={file_len})"
        ))
        .into());
    }
    if file_len.saturating_sub(pos) < crate::wal::frame::WAL_FRAME_HEADER_LEN as u64 {
        return Err(ReplayFailure::IncompleteTail(MidgeError::Corruption(
            format!(
                "Incomplete WAL frame header at pos {} in {} (need {} bytes, have {})",
                pos,
                file_path,
                crate::wal::frame::WAL_FRAME_HEADER_LEN,
                file_len.saturating_sub(pos)
            ),
        )));
    }

    let header_start = usize::try_from(pos).map_err(|_| {
        MidgeError::Corruption(format!(
            "WAL frame offset does not fit memory in {file_path}"
        ))
    })?;
    let header_end = header_start + crate::wal::frame::WAL_FRAME_HEADER_LEN;
    let header = &snapshot[header_start..header_end];
    let payload_start = pos + crate::wal::frame::WAL_FRAME_HEADER_LEN as u64;
    if header.iter().all(|byte| *byte == 0) && snapshot[header_end..].iter().all(|byte| *byte == 0)
    {
        return Err(ReplayFailure::IncompleteTail(MidgeError::Corruption(
            format!("Zero-filled WAL tail at pos {pos} in {file_path}"),
        )));
    }

    let (len, expected_crc) = crate::wal::frame::decode_frame_header(header)?;
    let need_end = payload_start
        .checked_add(u64::try_from(len).unwrap_or(u64::MAX))
        .ok_or_else(|| {
            MidgeError::Corruption(format!(
                "WAL frame length overflow at pos {pos} in {file_path} (len={len})"
            ))
        })?;
    if need_end > file_len {
        let hides_verified_suffix = contains_verified_wal_frame(&snapshot[header_end..]);
        let error = MidgeError::Corruption(if hides_verified_suffix {
            format!(
                "WAL frame length at pos {pos} in {file_path} overruns EOF and hides a verified later frame (len={len}, file_len={file_len})"
            )
        } else {
            format!(
                "Incomplete WAL record at pos {pos} in {file_path} (len={len}, file_len={file_len})"
            )
        });
        return Err(if hides_verified_suffix {
            ReplayFailure::Error(error)
        } else {
            ReplayFailure::IncompleteTail(error)
        });
    }

    let payload_end = usize::try_from(need_end).map_err(|_| {
        MidgeError::Corruption(format!(
            "WAL payload end does not fit memory in {file_path}"
        ))
    })?;
    let payload = &snapshot[header_end..payload_end];

    crate::wal::frame::verify_frame_crc(payload, expected_crc)?;
    let record = super::encoding::decode(payload)?;

    Ok(NextWalFrame::Frame(ReplayedWalFrame {
        record,
        next_pos: need_end,
    }))
}

fn read_wal_snapshot(
    file: &dyn File,
    file_path: &FsPath,
    file_read_ns: &mut u128,
) -> MidgeResult<bytes::Bytes> {
    let file_len = file.len().map_err(map_fs_error)?;
    if file_len == 0 {
        return Ok(bytes::Bytes::new());
    }
    read_wal_bytes(file, file_path, 0, file_len, file_read_ns)
}

fn read_wal_bytes(
    file: &dyn File,
    file_path: &FsPath,
    offset: u64,
    len: u64,
    file_read_ns: &mut u128,
) -> MidgeResult<bytes::Bytes> {
    let read_start = std::time::Instant::now();
    #[cfg(test)]
    WAL_REPLAY_FILE_READ_COUNT.set(WAL_REPLAY_FILE_READ_COUNT.get().saturating_add(1));
    let bytes = file.read_at(offset, len).map_err(map_fs_error)?;
    *file_read_ns = file_read_ns.saturating_add(read_start.elapsed().as_nanos());
    let expected_len = usize::try_from(len).map_err(|_| {
        MidgeError::Corruption(format!(
            "WAL read length does not fit memory at offset {offset} in {file_path}: {len}"
        ))
    })?;
    if bytes.len() != expected_len {
        return Err(MidgeError::Corruption(format!(
            "Short WAL read at offset {offset} in {file_path} (need={len}, got={})",
            bytes.len()
        )));
    }
    Ok(bytes)
}

fn contains_verified_wal_frame(bytes: &[u8]) -> bool {
    let header_len = crate::wal::frame::WAL_FRAME_HEADER_LEN;
    if bytes.len() < header_len.saturating_add(3) {
        return false;
    }

    for payload_start in header_len..=bytes.len().saturating_sub(3) {
        if !super::encoding::has_current_record_prefix(&bytes[payload_start..]) {
            continue;
        }
        let header_start = payload_start - header_len;
        let Ok((payload_len, expected_crc)) =
            crate::wal::frame::decode_frame_header(&bytes[header_start..payload_start])
        else {
            continue;
        };
        let Some(payload_end) = payload_start.checked_add(payload_len) else {
            continue;
        };
        if payload_end > bytes.len() {
            continue;
        }
        let payload = &bytes[payload_start..payload_end];
        if crate::wal::frame::verify_frame_crc(payload, expected_crc).is_ok()
            && super::encoding::decode_view(payload).is_ok()
        {
            return true;
        }
    }
    false
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

    match record.op.role() {
        WalOpRole::TransactionBatch => {
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
                apply_wal_record_to_memtables(
                    &replay_record,
                    ctx.memtables,
                    ctx.file_apply_ns,
                    ctx.should_apply,
                )?;
            }
        }
        WalOpRole::TransactionBegin => {
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
        WalOpRole::TransactionCommit => {
            if let Some(txn_id) = record.txn_id {
                if let Some(spool) = ctx.open_txns.remove(&(record.writer_epoch, txn_id)) {
                    spool.replay(|buffered| {
                        apply_wal_record_to_memtables(
                            &buffered,
                            ctx.memtables,
                            ctx.file_apply_ns,
                            ctx.should_apply,
                        )
                    })?;
                }
            }
        }
        WalOpRole::ValueWrite | WalOpRole::PointDelete | WalOpRole::RangeDelete => {
            if let Some(txn_id) = record.txn_id {
                if let Some(spool) = ctx.open_txns.get_mut(&(record.writer_epoch, txn_id)) {
                    spool.append(record)?;
                    return Ok(());
                }
            }

            apply_wal_record_to_memtables(
                record,
                ctx.memtables,
                ctx.file_apply_ns,
                ctx.should_apply,
            )?;
        }
    }

    Ok(())
}

fn apply_wal_record_to_memtables<S: BuildHasher>(
    record: &WalRecord,
    memtables: &mut HashMap<ColumnFamilyId, Arc<SkipListMemtable>, S>,
    file_apply_ns: &mut u128,
    should_apply: Option<&dyn Fn(&WalRecord) -> bool>,
) -> MidgeResult<()> {
    if should_apply.is_some_and(|filter| !filter(record)) {
        return Ok(());
    }
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
    // Reconstruct the durable record exactly. Expiration is a read-time
    // visibility rule, never a destructive recovery decision: restart wall
    // clocks may be transiently wrong and must not turn values into tombstones.
    let memtable = memtables
        .entry(record.cf_id)
        .or_insert_with(|| Arc::new(SkipListMemtable::new()));

    match record.op.role() {
        WalOpRole::ValueWrite => {
            if let Some(value) = &record.value {
                memtable.put_with_seq(
                    record.key.to_vec(),
                    value.to_vec(),
                    record.seq,
                    record.expiration,
                )?;
            }
        }
        WalOpRole::PointDelete => {
            memtable.delete_with_seq(record.key.to_vec(), record.seq)?;
        }
        WalOpRole::RangeDelete => {
            // RANGE_END is required for DeleteRange (lsm-spec format/wal.md
            // §5.1); a record missing it is corrupt, not a harmless no-op.
            let Some(end_key) = &record.range_end else {
                return Err(MidgeError::Corruption(format!(
                    "DeleteRange WAL record at seq {} is missing RANGE_END",
                    record.seq
                )));
            };
            memtable.delete_range_with_seq(record.key.as_ref(), end_key.as_ref(), record.seq)?;
        }
        WalOpRole::TransactionBegin
        | WalOpRole::TransactionCommit
        | WalOpRole::TransactionBatch => {
            // Transaction markers carry no direct memtable mutation.
        }
    }

    Ok(())
}

pub(crate) mod streaming;

#[cfg(test)]
mod tests;
