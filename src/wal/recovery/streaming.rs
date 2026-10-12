//! Cloud replay with bounded reads, transaction buffers, and checkpointable memtables.

use super::{
    apply_record, open_wal_replay_file, replay_error_action, Expectation, NextWalFrame,
    RecoveryStats, ReplayErrorAction, ReplayFailure, ReplayFile, ReplayFileKind, ReplayPolicy,
    ReplaySegments, ToleratedActiveTail, WalSalvageStop,
};
use crate::common::{MidgeError, MidgeResult};
use crate::io::{FsPath, ReadFs};
use crate::memtable::{size_bound, SkipListMemtable};
use crate::wal::format::{Chain, SegmentScan, HEADER_LEN};
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
    file: &dyn crate::io::ReadFile,
    path: &FsPath,
    start: u64,
    limits: StreamingReplayLimits,
) -> MidgeResult<Option<u64>> {
    use crate::wal::frame::FrameBytes;

    let source = crate::wal::frame::FileFrames::new(file, path);
    let file_len = source.len()?;
    let overlap = crate::wal::frame::WAL_FRAME_HEADER_LEN + 2;
    let chunk_size = limits.max_frame_bytes.min(1024 * 1024).max(overlap + 1);
    let mut pos = start.max(HEADER_LEN as u64);
    let mut max_sequence = None;
    let mut progress = crate::telemetry::recovery_progress::WorkProgress::new("suffix_scan");
    while file_len.saturating_sub(pos) > overlap as u64 {
        let len = usize::try_from((file_len - pos).min(chunk_size as u64)).map_err(|_| {
            MidgeError::ResourceLimit("WAL suffix read length exceeds usize".into())
        })?;
        let bytes = source.read(pos, len as u64)?;
        for payload_offset in crate::wal::frame::WAL_FRAME_HEADER_LEN..=len.saturating_sub(3) {
            if !crate::wal::encoding::has_segment_frame_prefix(&bytes[payload_offset..]) {
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
        progress.completed((len - overlap) as u64, 0);
    }
    progress.finish();
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

/// How a replay admits units, and what local recovery adds: a deadline or
/// scope, salvage of record errors, and spilled transactions. Every replay
/// names its selection; see [`ReplayOptions::new`].
#[derive(Clone, Copy)]
pub(crate) struct ReplayOptions<'a> {
    /// Stop with `Timeout` between frames once this expires.
    pub deadline: Option<&'a crate::common::OperationDeadline>,
    pub scope: Option<&'a crate::common::DeadlineScope>,
    pub selection: crate::wal::replay::ReplaySelection<'a>,
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

impl<'a> ReplayOptions<'a> {
    /// Admit units by `selection`, with no deadline and neither salvage nor
    /// spill behaviour.
    pub(crate) const fn new(selection: crate::wal::replay::ReplaySelection<'a>) -> Self {
        Self {
            deadline: None,
            scope: None,
            selection,
            salvage_record_errors: false,
            spill_pending_txns: false,
        }
    }

    fn check(self) -> MidgeResult<()> {
        ensure_deadline(self.deadline)?;
        self.scope.map_or(Ok(()), |scope| scope.check("WAL replay"))
    }

    fn should_apply_unit(self, cf_id: u32, commit_sequence: u64) -> MidgeResult<bool> {
        self.check()?;
        let result = self.selection.should_apply(cf_id, commit_sequence)?;
        self.check()?;
        Ok(result)
    }
}

/// The last complete transaction boundary before admission needs a checkpoint.
#[derive(Debug, Clone, Copy)]
pub(crate) struct ReplayCheckpoint {
    pub(crate) through_sequence: u64,
}

type Memtables = HashMap<u32, Arc<SkipListMemtable>>;
type Checkpoint<'a> = dyn FnMut(&mut Memtables, ReplayCheckpoint) -> MidgeResult<()> + 'a;

struct PendingTxn {
    records: PendingRecords,
    bytes: usize,
    /// The column family the transaction's begin frame names.
    cf_id: u32,
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
        let mut progress =
            crate::telemetry::recovery_progress::WorkProgress::new("transaction_spool");
        for _ in 0..self.record_count {
            let mut header = [0_u8; crate::wal::frame::WAL_FRAME_HEADER_LEN];
            self.file.read_exact(&mut header)?;
            let (payload_len, expected_crc) = crate::wal::frame::decode_frame_header(&header)?;
            let mut payload = vec![0_u8; payload_len];
            self.file.read_exact(&mut payload)?;
            crate::wal::frame::verify_frame_crc(&payload, expected_crc)?;
            visitor(crate::wal::encoding::decode(payload.as_slice())?)?;
            progress.completed((payload_len + header.len()) as u64, 1);
        }
        let mut trailing = [0_u8; 1];
        if self.file.read(&mut trailing)? != 0 {
            return Err(MidgeError::Corruption(
                "transaction recovery spool has trailing bytes".to_string(),
            ));
        }
        progress.finish();
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
    checkpoint: &'a mut Checkpoint<'a>,
    operation_progress: crate::telemetry::recovery_progress::WorkProgress,
}

/// The caller must expose stable, immutable input views across both passes and
/// publish checkpoints durably before removing entries from the memtable map.
#[cfg(test)]
pub(crate) fn replay_wal_with_checkpoint(
    storage: &dyn ReadFs,
    segments: &ReplaySegments,
    memtables: &mut Memtables,
    replay_policy: ReplayPolicy,
    limits: StreamingReplayLimits,
    checkpoint: &mut Checkpoint<'_>,
) -> MidgeResult<RecoveryStats> {
    replay_wal_with_options(
        storage,
        segments,
        memtables,
        replay_policy,
        limits,
        ReplayOptions::new(crate::wal::replay::ReplaySelection::All),
        checkpoint,
    )
}

/// `replay_wal_with_checkpoint` with local-recovery behaviour selected.
#[allow(clippy::too_many_arguments)] // Each input is a distinct replay contract.
pub(crate) fn replay_wal_with_options(
    storage: &dyn ReadFs,
    segments: &ReplaySegments,
    memtables: &mut Memtables,
    replay_policy: ReplayPolicy,
    limits: StreamingReplayLimits,
    options: ReplayOptions<'_>,
    checkpoint: &mut Checkpoint<'_>,
) -> MidgeResult<RecoveryStats> {
    limits.validate()?;
    options.check()?;
    if options.spill_pending_txns && limits.target_memtable_encoded_bytes != usize::MAX {
        return Err(MidgeError::InvalidArgument(
            "spooled WAL transactions require unbounded replay memtable limits".into(),
        ));
    }
    let started = std::time::Instant::now();
    let mut state = ReplayState {
        stats: RecoveryStats::new(),
        memtables,
        open_txns: HashMap::new(),
        pending_bytes: 0,
        committed_sequence: None,
        limits,
        options,
        checkpoint,
        operation_progress: crate::telemetry::recovery_progress::WorkProgress::new(
            "replay_decision",
        ),
    };
    replay_paths(storage, segments, replay_policy, &mut state)?;
    options.check()?;
    state.operation_progress.finish();
    state.stats.total_replay_ns = started.elapsed().as_nanos();
    tracing::info!(
        segments = segments.len(),
        records = state.stats.record_count,
        bytes = state.stats.bytes,
        max_sequence = ?state.stats.max_sequence,
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
    storage: &dyn ReadFs,
    path: &FsPath,
    limits: StreamingReplayLimits,
    offset: u64,
    options: ReplayOptions<'_>,
) -> MidgeResult<Option<u64>> {
    let mut read_ns = 0;
    options.check()?;
    let file = match open_wal_replay_file(storage, path, &mut read_ns) {
        Ok(Some(file)) => file,
        Err(error @ MidgeError::Timeout(_)) => return Err(error),
        _ => return Ok(None),
    };
    let source = frame_reader::source(&*file, path, limits);
    let mut pos = offset.max(HEADER_LEN as u64);
    let mut max_sequence = None;
    let mut progress = crate::telemetry::recovery_progress::WorkProgress::new("salvage_frontier");
    loop {
        options.check()?;
        let next_pos = match frame_reader::next_frame(&source, path, pos, limits, &mut read_ns) {
            Ok(NextWalFrame::Frame(frame)) => {
                max_sequence = max_sequence.max(Some(frame.record.seq));
                frame.next_pos
            }
            Ok(NextWalFrame::Footer { next_pos, .. }) => next_pos,
            Err(failure) if matches!(failure.error(), MidgeError::Timeout(_)) => {
                return Err(failure.into_error())
            }
            _ => break,
        };
        let bytes = next_pos.saturating_sub(pos);
        pos = next_pos;
        progress.completed(bytes, 1);
    }
    progress.finish();
    Ok(max_sequence)
}

/// Inspect one stable WAL segment, open or sealed, without reading the
/// complete object: its header, then every frame, accepting a footer.
pub(crate) fn inspect_wal_file(
    file: &dyn crate::io::ReadFile,
    path: &FsPath,
    limits: StreamingReplayLimits,
) -> Result<super::VerifiedWalPrefix, super::WalPrefixInspectionFailure> {
    inspect_file(file, path, limits, &mut |_| Ok(()))
}

/// A sealed segment must hold whole frames and at least one record, and end
/// in the footer that matches them.
pub(crate) fn inspect_sealed_wal_file(
    file: &dyn crate::io::ReadFile,
    path: &FsPath,
    limits: StreamingReplayLimits,
) -> MidgeResult<super::VerifiedWalPrefix> {
    visit_sealed_wal_records(file, path, limits, &mut |_| Ok(()))
}

/// Visit one validated record at a time without retaining a complete segment.
/// The caller must keep the file identity pinned until the returned proof is used.
pub(crate) fn visit_sealed_wal_records(
    file: &dyn crate::io::ReadFile,
    path: &FsPath,
    limits: StreamingReplayLimits,
    visitor: &mut dyn FnMut(&WalRecord) -> MidgeResult<()>,
) -> MidgeResult<super::VerifiedWalPrefix> {
    let prefix = inspect_file(file, path, limits, visitor)
        .map_err(|failure| failure.failure.into_error())?;
    require_sealed_records(path, &prefix)?;
    Ok(prefix)
}

fn require_sealed_records(path: &FsPath, prefix: &super::VerifiedWalPrefix) -> MidgeResult<()> {
    if !prefix.sealed() {
        return Err(MidgeError::Corruption(format!(
            "sealed WAL segment {path} ends without its footer"
        )));
    }
    if prefix.record_count() == 0 {
        return Err(MidgeError::Corruption("sealed WAL segment is empty".into()));
    }
    Ok(())
}

fn inspect_file(
    file: &dyn crate::io::ReadFile,
    path: &FsPath,
    limits: StreamingReplayLimits,
    visitor: &mut dyn FnMut(&WalRecord) -> MidgeResult<()>,
) -> Result<super::VerifiedWalPrefix, super::WalPrefixInspectionFailure> {
    inspect_file_from(
        file,
        path,
        limits,
        super::VerifiedWalPrefix::default(),
        visitor,
        &mut || Ok(()),
    )
}

/// Read segment `path` with one [`SegmentScan`]: its header, then each
/// record, through `until` when given (a frame boundary) or to its end or
/// footer. Any torn or invalid frame is an error.
///
/// # Errors
///
/// The read, frame, decode or scan error.
pub(crate) fn scan_segment_file(
    file: &dyn crate::io::ReadFile,
    path: &FsPath,
    until: Option<u64>,
) -> MidgeResult<SegmentScan> {
    let limits = StreamingReplayLimits::local();
    let mut read_ns = 0;
    let source = frame_reader::source(file, path, limits);
    let header = read_header(&source)?;
    let mut scan = SegmentScan::start(&header)?;
    let mut pos = HEADER_LEN as u64;
    while until.is_none_or(|until| pos < until) {
        match frame_reader::next_frame(&source, path, pos, limits, &mut read_ns)
            .map_err(ReplayFailure::into_error)?
        {
            NextWalFrame::Eof => break,
            NextWalFrame::Frame(frame) => {
                scan.frame(&frame.payload)?;
                scan.observe(frame.record.seq, frame.record.cf_id)?;
                pos = frame.next_pos;
            }
            NextWalFrame::Footer { payload, .. } => {
                scan.frame(&payload)?;
                break;
            }
        }
    }
    if until.is_some_and(|until| pos != until) {
        return Err(MidgeError::Corruption(format!(
            "WAL segment {path} has no frame boundary at byte {}",
            until.unwrap_or_default()
        )));
    }
    scan.finish(match until {
        Some(until) => until,
        None => crate::wal::frame::FrameBytes::len(&source)?,
    })?;
    Ok(scan)
}

/// The header bytes at the start of a segment.
fn read_header(source: &crate::wal::frame::FileFrames<'_>) -> MidgeResult<bytes::Bytes> {
    use crate::wal::frame::FrameBytes;
    let len = source.len()?;
    if len < HEADER_LEN as u64 {
        return Err(MidgeError::Corruption(format!(
            "WAL segment holds {len} bytes, shorter than its {HEADER_LEN}-byte header"
        )));
    }
    source.read(0, HEADER_LEN as u64)
}

fn inspect_file_from(
    file: &dyn crate::io::ReadFile,
    path: &FsPath,
    limits: StreamingReplayLimits,
    prefix: super::VerifiedWalPrefix,
    visitor: &mut dyn FnMut(&WalRecord) -> MidgeResult<()>,
    checkpoint: &mut dyn FnMut() -> MidgeResult<()>,
) -> Result<super::VerifiedWalPrefix, super::WalPrefixInspectionFailure> {
    limits.validate().map_err(|error| {
        super::wal_prefix_failure(super::VerifiedWalPrefix::default(), error.into())
    })?;
    let mut read_ns = 0;
    let source = frame_reader::source(file, path, limits);
    let super::VerifiedWalPrefix {
        mut valid_bytes,
        scan,
    } = prefix;
    let mut scan = match scan {
        Some(scan) => scan,
        None => match read_header(&source).and_then(|header| SegmentScan::start(&header)) {
            Ok(scan) => {
                valid_bytes = HEADER_LEN;
                Box::new(scan)
            }
            Err(error) => {
                return Err(super::wal_prefix_failure(
                    super::VerifiedWalPrefix {
                        valid_bytes,
                        scan: None,
                    },
                    error.into(),
                ))
            }
        },
    };
    let mut progress = crate::telemetry::recovery_progress::WorkProgress::new("sealed_inspection");
    while scan.footer().is_none() {
        let next =
            match frame_reader::next_frame(&source, path, valid_bytes as u64, limits, &mut read_ns)
            {
                Ok(next) => next,
                Err(failure) => return Err(stopped(valid_bytes, scan, failure)),
            };
        let (next_pos, records) = match next {
            NextWalFrame::Eof => break,
            NextWalFrame::Footer { payload, next_pos } => {
                // The walk takes the footer only once nothing follows it, so
                // a stopped walk never reads as sealed.
                let mut sealed = (*scan).clone();
                let finished = sealed
                    .frame(&payload)
                    .and_then(|_| crate::wal::frame::FrameBytes::len(&source))
                    .and_then(|len| sealed.finish(len));
                if let Err(error) = finished {
                    return Err(stopped(valid_bytes, scan, error.into()));
                }
                *scan = sealed;
                (next_pos, 0)
            }
            NextWalFrame::Frame(frame) => {
                // The scan takes the frame only once the visitor accepted
                // it: a visitor that stops (a deadline, a budget) leaves the
                // prefix where a resumed inspection reads the frame again.
                let admitted = scan
                    .check_record(frame.record.seq)
                    .and_then(|()| {
                        validate_record_contents(&frame.record, limits.max_pending_txn_bytes)
                    })
                    .and_then(|()| visitor(&frame.record))
                    .and_then(|()| {
                        scan.frame(&frame.payload)?;
                        scan.observe(frame.record.seq, frame.record.cf_id)
                    });
                if let Err(error) = admitted {
                    return Err(stopped(valid_bytes, scan, error.into()));
                }
                (frame.next_pos, 1)
            }
        };
        let bytes = next_pos.saturating_sub(valid_bytes as u64);
        let Ok(next_valid) = usize::try_from(next_pos) else {
            return Err(stopped(
                valid_bytes,
                scan,
                super::ReplayFailure::Error(MidgeError::ResourceLimit(
                    "WAL offset exceeds platform address space".into(),
                )),
            ));
        };
        valid_bytes = next_valid;
        if let Err(error) = checkpoint() {
            return Err(stopped(valid_bytes, scan, error.into()));
        }
        progress.completed(bytes, records);
    }
    progress.finish();
    Ok(super::VerifiedWalPrefix {
        valid_bytes,
        scan: Some(scan),
    })
}

/// A walk that stopped after verifying `valid_bytes` with `scan`.
fn stopped(
    valid_bytes: usize,
    scan: Box<SegmentScan>,
    failure: super::ReplayFailure,
) -> super::WalPrefixInspectionFailure {
    super::wal_prefix_failure(
        super::VerifiedWalPrefix {
            valid_bytes,
            scan: Some(scan),
        },
        failure,
    )
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

fn replay_paths(
    storage: &dyn ReadFs,
    segments: &ReplaySegments,
    policy: ReplayPolicy,
    state: &mut ReplayState<'_>,
) -> MidgeResult<()> {
    let paths = segments.files();
    let mut chain = Chain::new(segments.anchor);
    let mut progress = crate::telemetry::recovery_progress::WorkProgress::new("replay");
    for (index, path) in paths.iter().enumerate() {
        let Some(file) = open_wal_replay_file(storage, &path.path, &mut state.stats.wal_read_ns)?
        else {
            // Every planned file is history: one that is gone lost its
            // records, so nothing after it may replay (#842).
            return state.stop_at(
                storage,
                paths,
                (index, 0),
                policy,
                ReplayFailure::Record(MidgeError::Corruption(format!(
                    "WAL segment {} is history but its file is missing",
                    path.path
                ))),
            );
        };
        let source = frame_reader::source(&*file, &path.path, state.limits);
        let mut scan = match admit_segment(&source, path, &mut chain) {
            Ok(scan) => scan,
            Err(error) => {
                return state.stop_at(storage, paths, (index, 0), policy, error.into());
            }
        };
        let epoch = scan.header().epoch;
        // End of the last frame this file replayed: the verified prefix.
        let mut pos = HEADER_LEN as u64;
        loop {
            state.options.check()?;
            let next = frame_reader::next_frame(
                &source,
                &path.path,
                pos,
                state.limits,
                &mut state.stats.wal_read_ns,
            );
            let frame = match next {
                Ok(NextWalFrame::Eof) => break,
                Ok(NextWalFrame::Footer { payload, next_pos }) => {
                    let sealed = scan
                        .frame(&payload)
                        .and_then(|_| crate::wal::frame::FrameBytes::len(&source))
                        .and_then(|len| scan.finish(len))
                        .and_then(|()| expect_footer(path, &scan));
                    if let Err(error) = sealed {
                        return state.stop_at(storage, paths, (index, pos), policy, error.into());
                    }
                    progress.completed(next_pos.saturating_sub(pos), 0);
                    break;
                }
                Ok(NextWalFrame::Frame(frame)) => frame,
                Err(failure) => {
                    return state.stop_at(storage, paths, (index, pos), policy, failure);
                }
            };
            let bytes = frame.next_pos.saturating_sub(pos);
            if let Err(error) = scan.frame(&frame.payload) {
                return state.stop_at(storage, paths, (index, pos), policy, error.into());
            }
            // Authority failures cannot be classified as salvageable WAL damage.
            state
                .options
                .selection
                .validate_family(frame.record.cf_id)?;
            let replayed = scan
                .observe(frame.record.seq, frame.record.cf_id)
                .and_then(|()| state.process(frame.record, epoch, scan.header().first_seq));
            if let Err(error) = replayed {
                if state.options.salvage_record_errors {
                    return state.stop_at(
                        storage,
                        paths,
                        (index, pos),
                        policy,
                        ReplayFailure::Record(error),
                    );
                }
                return Err(error);
            }
            pos = frame.next_pos;
            progress.completed(bytes, 1);
        }
        if let Some(stopped) =
            state.end_segment(storage, paths, (index, pos), (&scan, &mut chain), policy)
        {
            return stopped;
        }
    }
    progress.finish();
    Ok(())
}

/// Read the header of `path`'s segment and admit it to `chain`, checking it
/// against what the caller expects of the file.
fn admit_segment(
    source: &crate::wal::frame::FileFrames<'_>,
    path: &ReplayFile,
    chain: &mut Chain,
) -> MidgeResult<SegmentScan> {
    let scan = SegmentScan::start(&read_header(source)?)?;
    let header = *scan.header();
    chain.admit(&header, path.id)?;
    let expected = match &path.expect {
        Expectation::Member { member, holder } => {
            let actual = header.open_facts();
            if header.holder != *holder {
                Err(format!(
                    "its header names holder {:02x?}, not this ledger's {holder:02x?}",
                    header.holder
                ))
            } else if actual != member.open {
                Err(format!(
                    "its header {actual:?} is not the ledger's {:?}",
                    member.open
                ))
            } else {
                Ok(())
            }
        }
        Expectation::Published(entry) => {
            if header.epoch != entry.epoch() {
                Err(format!(
                    "its header names writer epoch {}, not the catalog's {}",
                    header.epoch,
                    entry.epoch()
                ))
            } else if header.prev != entry.prev() {
                Err(format!(
                    "its header links segment {} (last sequence {}), not the catalog's segment {} (last sequence {})",
                    header.prev.id,
                    header.prev.last_seq,
                    entry.prev().id,
                    entry.prev().last_seq
                ))
            } else {
                Ok(())
            }
        }
    };
    expected.map_err(|reason| {
        MidgeError::Corruption(format!(
            "WAL segment {} disagrees with its history: {reason}",
            path.path
        ))
    })?;
    Ok(scan)
}

/// A sealed segment's footer must seal what its history recorded: the
/// ledger's facts for a member, the catalog's facts for a published object.
fn expect_footer(path: &ReplayFile, scan: &SegmentScan) -> MidgeResult<()> {
    let actual = scan.footer().map(crate::wal::format::Footer::sealed_facts);
    let disagreement = match &path.expect {
        Expectation::Member { member, .. } => match &member.sealed {
            Some(expected) if actual.as_ref() != Some(expected) => {
                Some(format!("the ledger's {expected:?}"))
            }
            _ => None,
        },
        Expectation::Published(entry) => (actual.as_ref() != Some(entry.facts()))
            .then(|| format!("the catalog's {:?}", entry.facts())),
    };
    match disagreement {
        None => Ok(()),
        Some(expected) => Err(MidgeError::Corruption(format!(
            "WAL segment {} seals {actual:?}, not {expected}",
            path.path
        ))),
    }
}

impl ReplayState<'_> {
    /// Finish `paths[index]`, read through `pos`: link the chain to a sealed
    /// file's footer. `Some` when replay stops here: a sealed file must end
    /// in its footer.
    fn end_segment(
        &mut self,
        storage: &dyn ReadFs,
        paths: &[ReplayFile],
        (index, pos): (usize, u64),
        (scan, chain): (&SegmentScan, &mut Chain),
        policy: ReplayPolicy,
    ) -> Option<MidgeResult<()>> {
        let path = &paths[index];
        match scan.footer() {
            Some(footer) => {
                chain.seal(footer.sealed_facts().link);
                None
            }
            None if path.kind == ReplayFileKind::Sealed => Some(self.stop_at(
                storage,
                paths,
                (index, pos),
                policy,
                ReplayFailure::Error(MidgeError::Corruption(format!(
                    "sealed WAL segment {} ends without its footer",
                    path.path
                ))),
            )),
            None => None,
        }
    }

    /// Stop replay at `valid_bytes` into `paths[index]` after `failure`:
    /// tolerate a torn tail of the open segment, salvage the verified prefix,
    /// or fail.
    fn stop_at(
        &mut self,
        storage: &dyn ReadFs,
        paths: &[ReplayFile],
        (index, valid_bytes): (usize, u64),
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
                let mut max_unreplayed_sequence = None;
                for unreplayed in &paths[index + 1..] {
                    max_unreplayed_sequence =
                        max_unreplayed_sequence.max(max_verified_sequence_from_offset(
                            storage,
                            &unreplayed.path,
                            self.limits,
                            0,
                            self.options,
                        )?);
                }
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
                            self.options,
                        )?);
                }
                self.stats.salvage_stop = Some(WalSalvageStop {
                    segment: path.id,
                    valid_bytes,
                    max_unreplayed_sequence,
                });
                Ok(())
            }
            ReplayErrorAction::Fail => Err(failure.into_error()),
        }
    }
}

impl ReplayState<'_> {
    /// Apply `record`, read from a segment written by `epoch`. Transaction
    /// ids restart in every process, so a split transaction is the pair of
    /// its writer's epoch and its id.
    fn process(
        &mut self,
        record: WalRecord,
        epoch: u64,
        segment_first_sequence: u64,
    ) -> MidgeResult<()> {
        let previous_sequence = self.stats.max_sequence;
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
                // The outer frame carries only the commit sequence. Its
                // complete reserved span must also follow earlier frames and
                // remain inside this segment's sequence range, even when the
                // family frontier will skip the unit. Validate before a
                // checkpoint can clear prior state or any member can apply.
                if batch.begin_seq < segment_first_sequence
                    || previous_sequence.is_some_and(|previous| batch.begin_seq <= previous)
                {
                    return Err(MidgeError::Corruption(format!(
                        "transaction batch beginning at sequence {} overlaps earlier WAL history (segment begins at {segment_first_sequence}, previous frame {previous_sequence:?})",
                        batch.begin_seq
                    )));
                }
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
                    })
                    .collect();
                self.apply_atomic(&records, record.cf_id, record.seq)?;
            }
            WalOpRole::TransactionBegin => {
                if let Some(txn_id) = record.txn_id {
                    self.begin_txn((epoch, txn_id), record.cf_id)?;
                }
            }
            WalOpRole::TransactionCommit => {
                if let Some(txn_id) = record.txn_id {
                    self.commit_txn((epoch, txn_id), record.cf_id, record.seq)?;
                }
            }
            WalOpRole::ValueWrite | WalOpRole::PointDelete | WalOpRole::RangeDelete => {
                let key = record.txn_id.map(|id| (epoch, id));
                if let Some(key) = key.filter(|key| self.open_txns.contains_key(key)) {
                    let family = self.open_txns.get(&key).map(|pending| pending.cf_id);
                    if family != Some(record.cf_id) {
                        return Err(MidgeError::Corruption(format!(
                            "transaction {} member at sequence {} belongs to column family {}, not the transaction's {}",
                            key.1,
                            record.seq,
                            record.cf_id,
                            family.unwrap_or_default()
                        )));
                    }
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
                    self.apply_atomic(std::slice::from_ref(&record), record.cf_id, record.seq)?;
                }
            }
        }
        if self.open_txns.is_empty() {
            self.committed_sequence = self.stats.max_sequence;
        }
        Ok(())
    }

    /// Open split transaction `key`, which writes family `cf_id`.
    fn begin_txn(&mut self, key: (u64, u64), cf_id: u32) -> MidgeResult<()> {
        if self.open_txns.contains_key(&key) {
            return Err(MidgeError::Corruption(
                "duplicate transaction begin during streaming replay".into(),
            ));
        }
        let pending = if self.options.spill_pending_txns {
            PendingTxn {
                records: PendingRecords::Spool(TxnSpool::new()?),
                bytes: 0,
                cf_id,
            }
        } else {
            let bytes = pending_txn_overhead_bytes();
            self.reserve_pending(bytes)?;
            PendingTxn {
                records: PendingRecords::Memory(Vec::new()),
                bytes,
                cf_id,
            }
        };
        self.open_txns.insert(key, pending);
        Ok(())
    }

    /// Apply split transaction `key` as its commit marker in family `cf_id`
    /// arrives; a commit without a begin replays nothing.
    fn commit_txn(&mut self, key: (u64, u64), cf_id: u32, commit_sequence: u64) -> MidgeResult<()> {
        let Some(pending) = self.open_txns.remove(&key) else {
            return Ok(());
        };
        if pending.cf_id != cf_id {
            return Err(MidgeError::Corruption(format!(
                "transaction {} began in column family {} and commits in {cf_id}",
                key.1, pending.cf_id
            )));
        }
        self.pending_bytes = self.pending_bytes.saturating_sub(pending.bytes);
        match pending.records {
            PendingRecords::Memory(records) => self.apply_atomic(&records, cf_id, commit_sequence),
            PendingRecords::Spool(spool) => self.apply_spooled(spool, cf_id, commit_sequence),
        }
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

    fn apply_atomic(
        &mut self,
        records: &[WalRecord],
        cf_id: u32,
        commit_sequence: u64,
    ) -> MidgeResult<()> {
        if !self.options.should_apply_unit(cf_id, commit_sequence)? {
            return Ok(());
        }
        if self.limits.max_memtable_encoded_bytes == usize::MAX
            && self.limits.target_memtable_encoded_bytes == usize::MAX
        {
            // Unbounded (local) replay never checkpoints: skip the accounting.
            return self.apply_unchecked(records);
        }
        let mut growth = HashMap::<u32, usize>::new();
        let progress = &mut self.operation_progress;
        for record in records {
            self.options.check()?;
            progress.completed_operation();
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
            (self.checkpoint)(
                self.memtables,
                ReplayCheckpoint {
                    through_sequence: self.committed_sequence.unwrap_or(0),
                },
            )?;
            if self.projected_bytes(&growth) > checkpoint_limit {
                return Err(MidgeError::ResourceLimit(
                    "WAL checkpoint did not release enough recovered memtable capacity".into(),
                ));
            }
        }
        let started = std::time::Instant::now();
        let progress = &mut self.operation_progress;
        for record in records {
            self.options.check()?;
            progress.completed_operation();
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
        let progress = &mut self.operation_progress;
        for record in records {
            self.options.check()?;
            progress.completed_operation();
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
    fn apply_spooled(
        &mut self,
        spool: TxnSpool,
        cf_id: u32,
        commit_sequence: u64,
    ) -> MidgeResult<()> {
        let started = std::time::Instant::now();
        let apply = self.options.should_apply_unit(cf_id, commit_sequence)?;
        let memtables = &mut *self.memtables;
        let progress = &mut self.operation_progress;
        spool.replay(|record| {
            self.options.check()?;
            if apply {
                apply_record(&record, memtables)?;
            }
            progress.completed_operation();
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
