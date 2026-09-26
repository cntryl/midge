//! Cloud replay with bounded reads, transaction buffers, and checkpointable memtables.

use super::{
    apply_record, collect_replay_paths, open_wal_replay_file, replay_error_action, EpochPolicy,
    NextWalFrame, RecoveryStats, ReplayErrorAction, ReplayFailure, ReplayFile, ReplayPolicy,
    ToleratedActiveTail, WalSalvageStop, WriterEpochFrontiers,
};
use crate::common::{MidgeError, MidgeResult};
use crate::io::{Fs, FsPath};
use crate::memtable::{size_bound, SkipListMemtable};
use crate::wal::{types::WalOpRole, WalRecord};
use std::collections::HashMap;
use std::sync::Arc;

mod frame_reader;
#[cfg(test)]
mod tests;

/// Find verified record sequences after a corrupt boundary without replaying
/// any of them. Candidate headers are found in bounded chunks; a candidate
/// counts only after its complete payload passes CRC and record decoding.
pub(crate) fn max_verified_suffix_sequence(
    file: &dyn crate::io::File,
    path: &FsPath,
    start: u64,
    limits: StreamingReplayLimits,
) -> MidgeResult<Option<u64>> {
    use crate::wal::frame::FrameBytes;

    let source = crate::wal::frame::FileFrames::new(file, path);
    let file_len = source.len()?;
    let overlap = crate::wal::frame::WAL_FRAME_HEADER_LEN + 2;
    let chunk_size = limits.max_frame_bytes.min(1024 * 1024).max(overlap + 1);
    let mut pos = start;
    let mut max_sequence = None;
    while file_len.saturating_sub(pos) > overlap as u64 {
        let len = usize::try_from((file_len - pos).min(chunk_size as u64)).map_err(|_| {
            MidgeError::ResourceLimit("WAL suffix read length exceeds usize".into())
        })?;
        let bytes = source.read(pos, len as u64)?;
        for payload_offset in crate::wal::frame::WAL_FRAME_HEADER_LEN..=len.saturating_sub(3) {
            if !crate::wal::encoding::has_current_record_prefix(&bytes[payload_offset..]) {
                continue;
            }
            let header_start = payload_offset - crate::wal::frame::WAL_FRAME_HEADER_LEN;
            let Ok((payload_len, crc)) =
                crate::wal::frame::decode_frame_header(&bytes[header_start..payload_offset])
            else {
                continue;
            };
            let payload_start = pos + payload_offset as u64;
            if payload_len as u64 > file_len - payload_start {
                continue;
            }
            if payload_len > limits.max_frame_bytes {
                return Err(MidgeError::ResourceLimit(
                    "WAL suffix candidate exceeds replay frame limit".into(),
                ));
            }
            let payload = source.read(payload_start, payload_len as u64)?;
            if crate::wal::frame::verify_frame_crc(&payload, crc).is_ok() {
                if let Ok(record) = crate::wal::encoding::decode_view(&payload) {
                    max_sequence = max_sequence.max(Some(record.seq));
                }
            }
        }
        pos += (len - overlap) as u64;
    }
    Ok(max_sequence)
}

// Explicit byte units distinguish allocation bounds from sequence/record limits.
#[allow(clippy::struct_field_names)]
#[derive(Clone, Copy)]
pub(crate) struct StreamingReplayLimits {
    pub max_frame_bytes: usize,
    pub max_pending_txn_bytes: usize,
    pub max_memtable_encoded_bytes: usize,
    pub target_memtable_encoded_bytes: usize,
}

impl StreamingReplayLimits {
    /// Local recovery: no frame bound beyond the WAL record limit (which the
    /// frame header already enforces) and no memory checkpoints. Split-marker
    /// transactions spill to disk instead (`ReplayOptions::spill_pending_txns`).
    pub(crate) fn local() -> Self {
        Self {
            max_frame_bytes: crate::wal::frame::WAL_MAX_RECORD_LEN,
            max_pending_txn_bytes: usize::MAX,
            max_memtable_encoded_bytes: usize::MAX,
            target_memtable_encoded_bytes: usize::MAX,
        }
    }

    /// Largest transaction WAL footprint these limits can replay, whether it
    /// is one batch frame or a split-marker transaction buffered until commit.
    pub(crate) fn max_replayable_txn_bytes(self) -> usize {
        self.max_frame_bytes.min(self.max_pending_txn_bytes)
    }

    fn validate(self) -> MidgeResult<()> {
        if self.max_frame_bytes < crate::wal::frame::WAL_FRAME_HEADER_LEN + 3
            || self.max_pending_txn_bytes == 0
            || self.max_memtable_encoded_bytes == 0
            || self.target_memtable_encoded_bytes == 0
        {
            return Err(MidgeError::InvalidArgument(
                "streaming WAL replay limits must be positive and fit a frame header".into(),
            ));
        }
        Ok(())
    }
}

#[cfg(test)]
thread_local! {
    /// How many records took the slow duplicate rescan.
    static DUPLICATE_RESCANS: std::cell::Cell<usize> = const { std::cell::Cell::new(0) };
}

#[cfg(test)]
pub(crate) fn reset_duplicate_rescans() {
    DUPLICATE_RESCANS.set(0);
}

#[cfg(test)]
pub(crate) fn duplicate_rescans() -> usize {
    DUPLICATE_RESCANS.get()
}

/// Replay behaviour only local recovery needs; cloud replay uses the defaults.
#[derive(Clone, Copy, Default)]
pub(crate) struct ReplayOptions<'a> {
    /// Stop with `Timeout` between frames once this expires.
    pub deadline: Option<&'a crate::common::OperationDeadline>,
    /// Under `SalvageValidPrefix`, stop at the start of a frame whose record
    /// fails to replay (a conflicting sequence, a corrupt batch payload) and
    /// keep the verified prefix, as for a corrupt frame, instead of failing.
    pub salvage_record_errors: bool,
    /// Spool split-marker transactions to an anonymous temporary file rather
    /// than buffering them in memory. Local writers spill transactions too
    /// large to hold, so local replay must not need to hold them. Requires
    /// unbounded memtable limits: a spooled transaction applies without
    /// checkpoint accounting.
    pub spill_pending_txns: bool,
}

type Memtables = HashMap<u32, Arc<SkipListMemtable>>;
type Checkpoint<'a> = dyn FnMut(&mut Memtables, &RecoveryStats) -> MidgeResult<()> + 'a;

struct PendingTxn {
    records: PendingRecords,
    bytes: usize,
}

enum PendingRecords {
    Memory(Vec<WalRecord>),
    Spool(TxnSpool),
}

/// A split-marker transaction buffered on disk until its commit.
struct TxnSpool {
    file: std::fs::File,
    record_count: usize,
}

impl TxnSpool {
    fn new() -> MidgeResult<Self> {
        Ok(Self {
            // Anonymous, delete-on-close storage cannot leak a named recovery
            // artifact if the process crashes during replay.
            file: tempfile::tempfile()?,
            record_count: 0,
        })
    }

    fn append(&mut self, record: &WalRecord) -> MidgeResult<()> {
        use std::io::Write as _;
        let payload = crate::wal::encoding::encode(record)?;
        let mut frame = Vec::with_capacity(
            crate::wal::frame::WAL_FRAME_HEADER_LEN.saturating_add(payload.len()),
        );
        crate::wal::frame::append_frame(&mut frame, &payload)?;
        self.file.write_all(&frame)?;
        self.record_count = self.record_count.saturating_add(1);
        Ok(())
    }

    fn replay(mut self, mut visitor: impl FnMut(WalRecord) -> MidgeResult<()>) -> MidgeResult<()> {
        use std::io::{Read as _, Seek as _, Write as _};
        self.file.flush()?;
        self.file.seek(std::io::SeekFrom::Start(0))?;
        for _ in 0..self.record_count {
            let mut header = [0_u8; crate::wal::frame::WAL_FRAME_HEADER_LEN];
            self.file.read_exact(&mut header)?;
            let (payload_len, expected_crc) = crate::wal::frame::decode_frame_header(&header)?;
            let mut payload = vec![0_u8; payload_len];
            self.file.read_exact(&mut payload)?;
            crate::wal::frame::verify_frame_crc(&payload, expected_crc)?;
            visitor(crate::wal::encoding::decode(payload.as_slice())?)?;
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

struct ReplayState<'a> {
    stats: RecoveryStats,
    memtables: &'a mut Memtables,
    open_txns: HashMap<(u64, u64), PendingTxn>,
    pending_bytes: usize,
    committed_sequence: Option<u64>,
    limits: StreamingReplayLimits,
    options: ReplayOptions<'a>,
    should_apply: Option<&'a dyn Fn(&WalRecord) -> bool>,
    checkpoint: &'a mut Checkpoint<'a>,
}

/// The caller must expose stable, immutable input views across both passes and
/// publish checkpoints durably before removing entries from the memtable map.
pub(crate) fn replay_wal_with_checkpoint(
    storage: &dyn Fs,
    wal_dir: &FsPath,
    memtables: &mut Memtables,
    replay_policy: ReplayPolicy,
    should_apply: Option<&dyn Fn(&WalRecord) -> bool>,
    limits: StreamingReplayLimits,
    checkpoint: &mut Checkpoint<'_>,
) -> MidgeResult<RecoveryStats> {
    replay_wal_with_options(
        storage,
        wal_dir,
        memtables,
        replay_policy,
        should_apply,
        limits,
        ReplayOptions::default(),
        checkpoint,
    )
}

/// `replay_wal_with_checkpoint` with local-recovery behaviour selected.
#[allow(clippy::too_many_arguments)] // Each input is a distinct replay contract.
pub(crate) fn replay_wal_with_options(
    storage: &dyn Fs,
    wal_dir: &FsPath,
    memtables: &mut Memtables,
    replay_policy: ReplayPolicy,
    should_apply: Option<&dyn Fn(&WalRecord) -> bool>,
    limits: StreamingReplayLimits,
    options: ReplayOptions<'_>,
    checkpoint: &mut Checkpoint<'_>,
) -> MidgeResult<RecoveryStats> {
    limits.validate()?;
    if options.spill_pending_txns && limits.target_memtable_encoded_bytes != usize::MAX {
        return Err(MidgeError::InvalidArgument(
            "spooled WAL transactions require unbounded replay memtable limits".into(),
        ));
    }
    let started = std::time::Instant::now();
    let paths = collect_replay_paths(storage, wal_dir)?;
    let (frontiers, had_corruption) =
        discover_frontiers(storage, &paths, replay_policy, limits, options.deadline)?;
    let mut state = ReplayState {
        stats: RecoveryStats {
            max_epoch_seen: frontiers.max_epoch_seen(),
            had_corruption,
            ..RecoveryStats::new()
        },
        memtables,
        open_txns: HashMap::new(),
        pending_bytes: 0,
        committed_sequence: None,
        limits,
        options,
        should_apply,
        checkpoint,
    };
    replay_paths(storage, &paths, replay_policy, &frontiers, &mut state)?;
    state.stats.total_replay_ns = started.elapsed().as_nanos();
    tracing::info!(
        dir = %wal_dir,
        records = state.stats.record_count,
        bytes = state.stats.bytes,
        max_sequence = ?state.stats.max_sequence,
        max_epoch = state.stats.max_epoch_seen,
        stale_skipped = state.stats.stale_records_skipped,
        had_corruption = state.stats.had_corruption,
        "wal replay completed"
    );
    Ok(state.stats)
}

fn ensure_deadline(deadline: Option<&crate::common::OperationDeadline>) -> MidgeResult<()> {
    if deadline.is_some_and(crate::common::OperationDeadline::is_expired) {
        return Err(MidgeError::Timeout("WAL replay deadline expired".into()));
    }
    Ok(())
}

/// Highest sequence among verified frames beginning at `offset`. Used only to
/// keep new writes above sequences that salvage set aside unreplayed.
fn max_verified_sequence_from_offset(
    storage: &dyn Fs,
    path: &FsPath,
    limits: StreamingReplayLimits,
    offset: u64,
) -> Option<u64> {
    let mut read_ns = 0;
    let file = open_wal_replay_file(storage, path, &mut read_ns).ok()??;
    let source = frame_reader::source(&*file, path, limits);
    let mut pos = offset;
    let mut max_sequence = None;
    while let Ok(NextWalFrame::Frame(frame)) =
        frame_reader::next_frame(&source, path, pos, limits, &mut read_ns)
    {
        max_sequence = max_sequence.max(Some(frame.record.seq));
        pos = frame.next_pos;
    }
    max_sequence
}

/// Inspect one stable WAL file with the same tail and epoch contracts as the
/// existing active-byte inspector, without reading the complete object.
pub(crate) fn inspect_wal_file(
    file: &dyn crate::io::File,
    path: &FsPath,
    limits: StreamingReplayLimits,
) -> Result<super::VerifiedWalPrefix, super::WalPrefixInspectionFailure> {
    inspect_file(file, path, limits, EpochPolicy::SkipStale, &mut |_| Ok(()))
}

/// A local sealed file has the same epoch rule as a local active file: it may
/// span a restart or contain a late append from a fenced writer.
pub(crate) fn inspect_local_sealed_wal_file(
    file: &dyn crate::io::File,
    path: &FsPath,
    limits: StreamingReplayLimits,
) -> MidgeResult<super::VerifiedWalPrefix> {
    let prefix =
        inspect_wal_file(file, path, limits).map_err(|failure| failure.failure.into_error())?;
    if prefix.record_count == 0 {
        return Err(MidgeError::Corruption("sealed WAL segment is empty".into()));
    }
    Ok(prefix)
}

/// Sealed cloud segments must contain complete frames from exactly one epoch.
pub(crate) fn inspect_sealed_wal_file(
    file: &dyn crate::io::File,
    path: &FsPath,
    limits: StreamingReplayLimits,
) -> MidgeResult<super::VerifiedWalPrefix> {
    visit_sealed_wal_records(file, path, limits, &mut |_| Ok(()))
}

/// Visit one validated record at a time without retaining a complete segment.
/// The caller must keep the file identity pinned until the returned proof is used.
pub(crate) fn visit_sealed_wal_records(
    file: &dyn crate::io::File,
    path: &FsPath,
    limits: StreamingReplayLimits,
    visitor: &mut dyn FnMut(&WalRecord) -> MidgeResult<()>,
) -> MidgeResult<super::VerifiedWalPrefix> {
    let prefix = inspect_file(file, path, limits, EpochPolicy::SingleEpoch, visitor)
        .map_err(|failure| failure.failure.into_error())?;
    if prefix.record_count == 0 {
        return Err(MidgeError::Corruption("sealed WAL segment is empty".into()));
    }
    Ok(prefix)
}

fn inspect_file(
    file: &dyn crate::io::File,
    path: &FsPath,
    limits: StreamingReplayLimits,
    epoch_policy: EpochPolicy,
    visitor: &mut dyn FnMut(&WalRecord) -> MidgeResult<()>,
) -> Result<super::VerifiedWalPrefix, super::WalPrefixInspectionFailure> {
    inspect_file_from(
        file,
        path,
        limits,
        epoch_policy,
        super::VerifiedWalPrefix::default(),
        visitor,
        &mut || Ok(()),
    )
}

pub(crate) fn visit_sealed_wal_records_from(
    file: &dyn crate::io::File,
    path: &FsPath,
    limits: StreamingReplayLimits,
    progress: &mut super::VerifiedWalPrefix,
    visitor: &mut dyn FnMut(&WalRecord) -> MidgeResult<()>,
    checkpoint: &mut dyn FnMut() -> MidgeResult<()>,
) -> MidgeResult<()> {
    match inspect_file_from(
        file,
        path,
        limits,
        EpochPolicy::SingleEpoch,
        *progress,
        visitor,
        checkpoint,
    ) {
        Ok(prefix) => {
            *progress = prefix;
            if prefix.record_count == 0 {
                return Err(MidgeError::Corruption("sealed WAL segment is empty".into()));
            }
            Ok(())
        }
        Err(failure) => {
            *progress = failure.verified_prefix;
            Err(failure.failure.into_error())
        }
    }
}

fn inspect_file_from(
    file: &dyn crate::io::File,
    path: &FsPath,
    limits: StreamingReplayLimits,
    epoch_policy: EpochPolicy,
    mut prefix: super::VerifiedWalPrefix,
    visitor: &mut dyn FnMut(&WalRecord) -> MidgeResult<()>,
    checkpoint: &mut dyn FnMut() -> MidgeResult<()>,
) -> Result<super::VerifiedWalPrefix, super::WalPrefixInspectionFailure> {
    limits.validate().map_err(|error| {
        super::wal_prefix_failure(super::VerifiedWalPrefix::default(), error.into())
    })?;
    let mut read_ns = 0;
    let source = frame_reader::source(file, path, limits);
    loop {
        let next = frame_reader::next_frame(
            &source,
            path,
            prefix.valid_bytes as u64,
            limits,
            &mut read_ns,
        )
        .map_err(|failure| super::wal_prefix_failure(prefix, failure))?;
        let NextWalFrame::Frame(frame) = next else {
            return Ok(prefix);
        };
        let newest = (prefix.record_count > 0).then_some(prefix.writer_epoch);
        let writer_epoch = epoch_policy
            .admit(newest, frame.record.writer_epoch)
            .map_err(|error| {
                super::wal_prefix_failure(
                    prefix,
                    super::ReplayFailure::Error(MidgeError::Corruption(format!(
                        "sealed WAL segment {error}"
                    ))),
                )
            })?;
        validate_record_contents(&frame.record, limits.max_pending_txn_bytes)
            .map_err(|error| super::wal_prefix_failure(prefix, error.into()))?;
        visitor(&frame.record).map_err(|error| super::wal_prefix_failure(prefix, error.into()))?;
        prefix.writer_epoch = writer_epoch;
        prefix.max_sequence = prefix.max_sequence.max(frame.record.seq);
        prefix.record_count = prefix.record_count.saturating_add(1);
        prefix.valid_bytes = usize::try_from(frame.next_pos).map_err(|_| {
            super::wal_prefix_failure(
                prefix,
                super::ReplayFailure::Error(MidgeError::ResourceLimit(
                    "WAL offset exceeds platform address space".into(),
                )),
            )
        })?;
        checkpoint().map_err(|error| super::wal_prefix_failure(prefix, error.into()))?;
    }
}

fn validate_record_contents(record: &WalRecord, max_bytes: usize) -> MidgeResult<()> {
    if record.op.is_transaction_batch() {
        let payload = record
            .value
            .as_ref()
            .ok_or_else(|| MidgeError::Corruption("transaction batch payload missing".into()))?;
        let batch =
            crate::wal::encoding::decode_txn_batch_payload_bounded(record, payload, max_bytes)?;
        if batch
            .records
            .iter()
            .any(|op| matches!(op.op.role(), WalOpRole::RangeDelete) && op.range_end.is_none())
        {
            return Err(MidgeError::Corruption(
                "WAL batch delete range missing range_end".into(),
            ));
        }
    } else if matches!(record.op.role(), WalOpRole::RangeDelete) && record.range_end.is_none() {
        return Err(MidgeError::Corruption(
            "WAL delete range missing range_end".into(),
        ));
    }
    Ok(())
}

pub(super) fn discover_frontiers(
    storage: &dyn Fs,
    paths: &[ReplayFile],
    policy: ReplayPolicy,
    limits: StreamingReplayLimits,
    deadline: Option<&crate::common::OperationDeadline>,
) -> MidgeResult<(WriterEpochFrontiers, bool)> {
    let mut frontiers = WriterEpochFrontiers::default();
    let mut ordinal = 0_u64;
    let mut read_ns = 0;
    for path in paths {
        let Some(file) = open_wal_replay_file(storage, &path.path, &mut read_ns)? else {
            continue;
        };
        let source = frame_reader::source(&*file, &path.path, limits);
        let mut pos = 0;
        loop {
            ensure_deadline(deadline)?;
            match frame_reader::next_frame(&source, &path.path, pos, limits, &mut read_ns) {
                Ok(NextWalFrame::Eof) => break,
                Ok(NextWalFrame::Frame(frame)) => {
                    frontiers.record(&frame.record, ordinal);
                    ordinal = ordinal.saturating_add(1);
                    pos = frame.next_pos;
                }
                Err(failure) => {
                    return match replay_error_action(path, policy, &failure) {
                        ReplayErrorAction::TolerateFinalActiveTail => Ok((frontiers, false)),
                        ReplayErrorAction::SalvageVerifiedPrefix => Ok((frontiers, true)),
                        ReplayErrorAction::Fail => Err(failure.into_error()),
                    }
                }
            }
        }
    }
    Ok((frontiers, false))
}

fn replay_paths(
    storage: &dyn Fs,
    paths: &[ReplayFile],
    policy: ReplayPolicy,
    frontiers: &WriterEpochFrontiers,
    state: &mut ReplayState<'_>,
) -> MidgeResult<()> {
    let mut ordinal = 0_u64;
    let mut max_seen_sequence = None;
    for (index, path) in paths.iter().enumerate() {
        let Some(file) = open_wal_replay_file(storage, &path.path, &mut state.stats.wal_read_ns)?
        else {
            continue;
        };
        let source = frame_reader::source(&*file, &path.path, state.limits);
        // End of the last frame this file replayed: the verified prefix.
        let mut pos = 0;
        loop {
            ensure_deadline(state.options.deadline)?;
            let frame = match frame_reader::next_frame(
                &source,
                &path.path,
                pos,
                state.limits,
                &mut state.stats.wal_read_ns,
            ) {
                Ok(NextWalFrame::Eof) => break,
                Ok(NextWalFrame::Frame(frame)) => frame,
                Err(failure) => {
                    return state.stop_at(storage, paths, index, pos, policy, failure);
                }
            };
            let record_ordinal = ordinal;
            ordinal = ordinal.saturating_add(1);
            // A stale record is skipped whether or not an earlier file carried
            // it, and it can never conflict, so it needs no rescan. It also
            // must not raise the high-water mark: a fenced writer's high
            // sequence would push every later fresh record into the rescan.
            // For one epoch and sequence, staleness only grows with ordinal,
            // so a fresh copy's earlier copy was fresh and still counts.
            if frontiers.is_stale(&frame.record, record_ordinal) {
                state.stats.stale_records_skipped += 1;
                pos = frame.next_pos;
                continue;
            }
            let may_repeat = max_seen_sequence.is_some_and(|sequence| frame.record.seq <= sequence);
            max_seen_sequence = Some(max_seen_sequence.unwrap_or(0).max(frame.record.seq));
            let replayed = if may_repeat {
                duplicate_before(
                    storage,
                    &paths[..=index],
                    pos,
                    (&frame.record, record_ordinal),
                    frontiers,
                    state.limits,
                    state.options.deadline,
                    &mut state.stats.wal_read_ns,
                )
            } else {
                Ok(false)
            }
            .and_then(|duplicate| {
                if duplicate {
                    Ok(())
                } else {
                    state.process(frame.record)
                }
            });
            if let Err(error) = replayed {
                if state.options.salvage_record_errors {
                    return state.stop_at(
                        storage,
                        paths,
                        index,
                        pos,
                        policy,
                        ReplayFailure::Record(error),
                    );
                }
                return Err(error);
            }
            pos = frame.next_pos;
        }
    }
    Ok(())
}

impl ReplayState<'_> {
    /// Stop replay at `valid_bytes` into `paths[index]` after `failure`:
    /// tolerate a torn final active tail, salvage the verified prefix, or fail.
    fn stop_at(
        &mut self,
        storage: &dyn Fs,
        paths: &[ReplayFile],
        index: usize,
        valid_bytes: u64,
        policy: ReplayPolicy,
        failure: ReplayFailure,
    ) -> MidgeResult<()> {
        let path = &paths[index];
        match replay_error_action(path, policy, &failure) {
            ReplayErrorAction::TolerateFinalActiveTail => {
                tracing::info!(
                    path = %path.path,
                    error = %failure.error(),
                    valid_bytes,
                    "wal replay dropped an incomplete final active tail"
                );
                self.stats.tolerated_active_tail = Some(ToleratedActiveTail {
                    path: path.path.clone(),
                    valid_bytes,
                });
                Ok(())
            }
            ReplayErrorAction::SalvageVerifiedPrefix => {
                self.stats.mark_corruption();
                tracing::warn!(
                    path = %path.path,
                    error = %failure.error(),
                    valid_bytes,
                    "wal replay stopped at corrupt verified-prefix boundary"
                );
                let unreplayed_paths: Vec<FsPath> = paths[index + 1..]
                    .iter()
                    .map(|file| file.path.clone())
                    .collect();
                let mut max_unreplayed_sequence = unreplayed_paths
                    .iter()
                    .filter_map(|path| {
                        max_verified_sequence_from_offset(storage, path, self.limits, 0)
                    })
                    .max();
                if failure.is_record_failure() {
                    // The failing record was decoded, so later frames in this
                    // file can still be read. Keep their sequence range above
                    // the next writer even though the file is quarantined.
                    max_unreplayed_sequence =
                        max_unreplayed_sequence.max(max_verified_sequence_from_offset(
                            storage,
                            &path.path,
                            self.limits,
                            valid_bytes,
                        ));
                }
                self.stats.salvage_stop = Some(WalSalvageStop {
                    path: path.path.clone(),
                    valid_bytes,
                    unreplayed_paths,
                    max_unreplayed_sequence,
                });
                Ok(())
            }
            ReplayErrorAction::Fail => Err(failure.into_error()),
        }
    }
}

/// Normal monotonically sequenced files never enter this slow path. Rotation
/// overlap is resolved by exact comparisons against earlier immutable bytes,
/// avoiding a record/value-sized deduplication index for the entire backlog.
#[allow(clippy::too_many_arguments)] // The rescan needs the replay position and its bounds.
fn duplicate_before(
    storage: &dyn Fs,
    paths: &[ReplayFile],
    current_pos: u64,
    record_with_ordinal: (&WalRecord, u64),
    frontiers: &WriterEpochFrontiers,
    limits: StreamingReplayLimits,
    deadline: Option<&crate::common::OperationDeadline>,
    read_ns: &mut u128,
) -> MidgeResult<bool> {
    #[cfg(test)]
    DUPLICATE_RESCANS.set(DUPLICATE_RESCANS.get().saturating_add(1));
    let (record, record_ordinal) = record_with_ordinal;
    let mut prior_ordinal = 0_u64;
    for (index, path) in paths.iter().enumerate() {
        let current_file = index + 1 == paths.len();
        let Some(file) = open_wal_replay_file(storage, &path.path, read_ns)? else {
            continue;
        };
        let source = frame_reader::source(&*file, &path.path, limits);
        let mut pos = 0;
        while !current_file || pos < current_pos {
            ensure_deadline(deadline)?;
            let frame = match frame_reader::next_frame(&source, &path.path, pos, limits, read_ns)
                .map_err(super::ReplayFailure::into_error)?
            {
                NextWalFrame::Eof => break,
                NextWalFrame::Frame(frame) => frame,
            };
            if frame.record == *record && !current_file {
                return Ok(true);
            }
            if !frontiers.is_stale(record, record_ordinal)
                && !frontiers.is_stale(&frame.record, prior_ordinal)
                && frame.record.seq == record.seq
                && ((frame.record.cf_id == record.cf_id
                    && frame.record.key == record.key
                    && point_record(record)
                    && point_record(&frame.record))
                    || (record.op.is_transaction_batch() && frame.record.op.is_transaction_batch()))
            {
                return Err(MidgeError::Corruption(format!(
                    "conflicting or repeated WAL sequence {} across replay checkpoints",
                    record.seq
                )));
            }
            pos = frame.next_pos;
            prior_ordinal = prior_ordinal.saturating_add(1);
        }
    }
    Ok(false)
}

fn point_record(record: &WalRecord) -> bool {
    matches!(
        record.op.role(),
        WalOpRole::ValueWrite | WalOpRole::PointDelete
    )
}

impl ReplayState<'_> {
    fn process(&mut self, record: WalRecord) -> MidgeResult<()> {
        self.stats.record(&record);
        match record.op.role() {
            WalOpRole::TransactionBatch => {
                let payload = record.value.as_ref().ok_or_else(|| {
                    MidgeError::Corruption("transaction batch payload missing".into())
                })?;
                let batch = crate::wal::encoding::decode_txn_batch_payload_bounded(
                    &record,
                    payload,
                    self.limits.max_pending_txn_bytes,
                )?;
                let records: Vec<_> = batch
                    .records
                    .into_iter()
                    .map(|op| WalRecord {
                        cf_id: op.cf_id,
                        op: op.op,
                        key: op.key,
                        value: op.value,
                        seq: op.seq,
                        expiration: op.expiration,
                        range_end: op.range_end,
                        txn_id: Some(batch.txn_id),
                        writer_epoch: batch.writer_epoch,
                    })
                    .collect();
                self.apply_atomic(&records)?;
            }
            WalOpRole::TransactionBegin => {
                if let Some(txn_id) = record.txn_id {
                    let key = (record.writer_epoch, txn_id);
                    if self.open_txns.contains_key(&key) {
                        return Err(MidgeError::Corruption(
                            "duplicate transaction begin during streaming replay".into(),
                        ));
                    }
                    let pending = if self.options.spill_pending_txns {
                        PendingTxn {
                            records: PendingRecords::Spool(TxnSpool::new()?),
                            bytes: 0,
                        }
                    } else {
                        let bytes = pending_txn_overhead_bytes();
                        self.reserve_pending(bytes)?;
                        PendingTxn {
                            records: PendingRecords::Memory(Vec::new()),
                            bytes,
                        }
                    };
                    self.open_txns.insert(key, pending);
                }
            }
            WalOpRole::TransactionCommit => {
                if let Some(txn_id) = record.txn_id {
                    if let Some(pending) = self.open_txns.remove(&(record.writer_epoch, txn_id)) {
                        self.pending_bytes = self.pending_bytes.saturating_sub(pending.bytes);
                        match pending.records {
                            PendingRecords::Memory(records) => self.apply_atomic(&records)?,
                            PendingRecords::Spool(spool) => self.apply_spooled(spool)?,
                        }
                    }
                }
            }
            WalOpRole::ValueWrite | WalOpRole::PointDelete | WalOpRole::RangeDelete => {
                let key = record.txn_id.map(|id| (record.writer_epoch, id));
                if let Some(key) = key.filter(|key| self.open_txns.contains_key(key)) {
                    let spooled = matches!(
                        self.open_txns.get(&key).map(|pending| &pending.records),
                        Some(PendingRecords::Spool(_))
                    );
                    let bytes = if spooled { 0 } else { record_bytes(&record) };
                    self.reserve_pending(bytes)?;
                    let pending = self
                        .open_txns
                        .get_mut(&key)
                        .expect("checked pending transaction");
                    pending.bytes = pending.bytes.saturating_add(bytes);
                    match &mut pending.records {
                        PendingRecords::Memory(records) => records.push(record),
                        PendingRecords::Spool(spool) => spool.append(&record)?,
                    }
                } else {
                    self.apply_atomic(std::slice::from_ref(&record))?;
                }
            }
        }
        if self.open_txns.is_empty() {
            self.committed_sequence = self.stats.max_sequence;
        }
        Ok(())
    }

    fn reserve_pending(&mut self, bytes: usize) -> MidgeResult<()> {
        if bytes
            > self
                .limits
                .max_pending_txn_bytes
                .saturating_sub(self.pending_bytes)
        {
            return Err(MidgeError::ResourceLimit(
                "uncommitted WAL transactions exceed configured replay buffer limit".into(),
            ));
        }
        self.pending_bytes += bytes;
        Ok(())
    }

    fn apply_atomic(&mut self, records: &[WalRecord]) -> MidgeResult<()> {
        if self.limits.max_memtable_encoded_bytes == usize::MAX
            && self.limits.target_memtable_encoded_bytes == usize::MAX
        {
            // Unbounded (local) replay never checkpoints: skip the accounting.
            return self.apply_unchecked(records);
        }
        let mut growth = HashMap::<u32, usize>::new();
        for record in records
            .iter()
            .filter(|record| self.should_apply.is_none_or(|filter| filter(record)))
        {
            let bytes = match record.op.role() {
                WalOpRole::RangeDelete => size_bound::range_bytes(
                    record.key.len(),
                    record.range_end.as_ref().map_or(0, bytes::Bytes::len),
                ),
                _ => size_bound::point_bytes(
                    record.key.len(),
                    record.value.as_ref().map_or(0, bytes::Bytes::len),
                ),
            };
            let entry = growth.entry(record.cf_id).or_default();
            *entry = entry.saturating_add(bytes);
        }
        let standalone = growth.values().fold(0_usize, |total, bytes| {
            total
                .saturating_add(*bytes)
                .saturating_add(size_bound::FIXED_SST_BYTES)
        });
        if standalone > self.limits.max_memtable_encoded_bytes {
            return Err(MidgeError::NoSpace(format!("atomic WAL transaction needs {standalone} encoded memtable bytes, exceeding configured replay checkpoint limit {}", self.limits.max_memtable_encoded_bytes)));
        }
        let checkpoint_limit = self
            .limits
            .target_memtable_encoded_bytes
            .min(self.limits.max_memtable_encoded_bytes)
            .max(standalone);
        if self.projected_bytes(&growth) > checkpoint_limit {
            if !self.open_txns.is_empty() {
                return Err(MidgeError::ResourceLimit(
                    "cannot checkpoint while split WAL transactions remain open".into(),
                ));
            }
            let mut checkpoint_stats = self.stats.clone();
            checkpoint_stats.max_sequence = self.committed_sequence;
            (self.checkpoint)(self.memtables, &checkpoint_stats)?;
            if self.projected_bytes(&growth) > checkpoint_limit {
                return Err(MidgeError::ResourceLimit(
                    "WAL checkpoint did not release enough recovered memtable capacity".into(),
                ));
            }
        }
        let started = std::time::Instant::now();
        for record in records
            .iter()
            .filter(|record| self.should_apply.is_none_or(|filter| filter(record)))
        {
            apply_record(record, self.memtables)?;
        }
        self.stats.apply_ns = self
            .stats
            .apply_ns
            .saturating_add(started.elapsed().as_nanos());
        Ok(())
    }

    fn apply_unchecked(&mut self, records: &[WalRecord]) -> MidgeResult<()> {
        let started = std::time::Instant::now();
        for record in records
            .iter()
            .filter(|record| self.should_apply.is_none_or(|filter| filter(record)))
        {
            apply_record(record, self.memtables)?;
        }
        self.stats.apply_ns = self
            .stats
            .apply_ns
            .saturating_add(started.elapsed().as_nanos());
        Ok(())
    }

    /// Apply a spooled transaction record by record. Spooling requires
    /// unbounded memtable limits, so no checkpoint can fall inside it.
    fn apply_spooled(&mut self, spool: TxnSpool) -> MidgeResult<()> {
        let started = std::time::Instant::now();
        let should_apply = self.should_apply;
        let memtables = &mut *self.memtables;
        spool.replay(|record| {
            if should_apply.is_none_or(|filter| filter(&record)) {
                apply_record(&record, memtables)?;
            }
            Ok(())
        })?;
        self.stats.apply_ns = self
            .stats
            .apply_ns
            .saturating_add(started.elapsed().as_nanos());
        Ok(())
    }

    fn projected_bytes(&self, growth: &HashMap<u32, usize>) -> usize {
        let resident = self.memtables.values().fold(0_usize, |total, table| {
            total.saturating_add(table.encoded_size_upper_bound())
        });
        growth.iter().fold(resident, |total, (cf, bytes)| {
            total
                .saturating_add(*bytes)
                .saturating_add(if self.memtables.contains_key(cf) {
                    0
                } else {
                    size_bound::FIXED_SST_BYTES
                })
        })
    }
}

fn record_bytes(record: &WalRecord) -> usize {
    pending_record_bytes(
        record.key.len(),
        record.value.as_ref().map_or(0, bytes::Bytes::len),
        record.range_end.as_ref().map_or(0, bytes::Bytes::len),
    )
}

/// Replay buffer charge for one record of a split-marker transaction. The
/// writer uses the same accounting to reject transactions replay cannot hold.
pub(crate) fn pending_record_bytes(
    key_len: usize,
    value_len: usize,
    range_end_len: usize,
) -> usize {
    size_of::<WalRecord>()
        .saturating_mul(2)
        .saturating_add(key_len)
        .saturating_add(value_len)
        .saturating_add(range_end_len)
}

/// Replay buffer charge for opening one split-marker transaction.
pub(crate) fn pending_txn_overhead_bytes() -> usize {
    size_of::<PendingTxn>()
        .saturating_add(size_of::<(u64, u64)>())
        .saturating_mul(2)
}
