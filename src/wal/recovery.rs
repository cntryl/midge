//! WAL recovery - replay WAL files to restore state after crash
//!
//! On startup, persistent WAL files are replayed to reconstruct the
//! memtables for each column family. Callers say which files to replay as
//! [`ReplaySegments`]: sealed segments in order, then the active file.

use super::types::{ColumnFamilyId, WalOpRole, WalRecord};
use crate::common::{MidgeError, MidgeResult};
use crate::io::{FsError, FsPath, ReadFs};
use crate::memtable::SkipListMemtable;
use std::collections::HashMap;
use std::hash::BuildHasher;
use std::sync::Arc;
use tracing::instrument;

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

pub(crate) use crate::wal::planner::Expectation;

#[derive(Debug, Clone)]
struct ReplayFile {
    id: crate::wal::ledger::SegmentId,
    path: FsPath,
    kind: ReplayFileKind,
    expect: Expectation,
}

/// The WAL files one replay reads: sealed segments, oldest first, every
/// frame of which must be whole and which end in their footer, then at most
/// one open segment, replayed last, whose incomplete tail is a write the
/// crash interrupted. The first segment links `anchor`, unchecked when it is
/// unknown; each later one links the footer of the one before.
#[derive(Debug, Default, Clone)]
pub(crate) struct ReplaySegments {
    anchor: Option<crate::wal::format::ChainLink>,
    files: Vec<ReplayFile>,
}

impl ReplaySegments {
    /// Segments replayed after `anchor`, or after any link when `None`.
    pub(crate) fn new(anchor: Option<crate::wal::format::ChainLink>) -> Self {
        Self {
            anchor,
            files: Vec::new(),
        }
    }

    /// Every member of `ledger` at its path, checked against the facts the
    /// ledger holds, after the ledger's anchor: the local plan
    /// ([`crate::wal::planner::local`]).
    pub(crate) fn from_ledger(
        ledger: &crate::wal::ledger::WalLedger,
        path: impl Fn(crate::wal::ledger::SegmentId) -> FsPath,
    ) -> Self {
        let plan = crate::wal::planner::local(ledger);
        Self::from_plan(plan.anchor, &plan.replay, path)
    }

    /// The segments a recovery plan replays, in its order, at their paths,
    /// after `anchor`.
    pub(crate) fn from_plan(
        anchor: crate::wal::format::ChainLink,
        planned: &[crate::wal::planner::PlannedSegment],
        path: impl Fn(crate::wal::ledger::SegmentId) -> FsPath,
    ) -> Self {
        let mut segments = Self::new(Some(anchor));
        for segment in planned {
            if segment.sealed {
                segments.push_sealed(segment.id, path(segment.id), segment.expect.clone());
            } else {
                segments.push_open(segment.id, path(segment.id), segment.expect.clone());
            }
        }
        segments
    }

    /// Replay sealed segment `id` from `path` next.
    pub(crate) fn push_sealed(
        &mut self,
        id: crate::wal::ledger::SegmentId,
        path: FsPath,
        expect: Expectation,
    ) {
        self.files.push(ReplayFile {
            id,
            path,
            kind: ReplayFileKind::Sealed,
            expect,
        });
    }

    /// Replay open segment `id` from `path` last.
    pub(crate) fn push_open(
        &mut self,
        id: crate::wal::ledger::SegmentId,
        path: FsPath,
        expect: Expectation,
    ) {
        self.files.push(ReplayFile {
            id,
            path,
            kind: ReplayFileKind::FinalActive,
            expect,
        });
    }

    fn files(&self) -> &[ReplayFile] {
        &self.files
    }

    /// How many files the replay reads.
    pub(crate) fn len(&self) -> usize {
        self.files.len()
    }

    /// The open segment the replay reads last, if any.
    #[cfg(test)]
    pub(crate) fn open_segment(&self) -> Option<crate::wal::ledger::SegmentId> {
        self.files
            .last()
            .filter(|file| file.kind == ReplayFileKind::FinalActive)
            .map(|file| file.id)
    }
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
    Record(MidgeError),
}

impl From<crate::wal::frame::FrameError> for ReplayFailure {
    fn from(error: crate::wal::frame::FrameError) -> Self {
        if error.is_incomplete_tail() {
            Self::IncompleteTail(error.into_error())
        } else {
            Self::Error(error.into_error())
        }
    }
}

impl ReplayFailure {
    fn error(&self) -> &MidgeError {
        match self {
            Self::IncompleteTail(error) | Self::Error(error) | Self::Record(error) => error,
        }
    }

    fn into_error(self) -> MidgeError {
        match self {
            Self::IncompleteTail(error) | Self::Error(error) | Self::Record(error) => error,
        }
    }

    fn is_incomplete_tail(&self) -> bool {
        matches!(self, Self::IncompleteTail(_))
    }

    fn is_record_failure(&self) -> bool {
        matches!(self, Self::Record(_))
    }
}

impl From<MidgeError> for ReplayFailure {
    fn from(error: MidgeError) -> Self {
        Self::Error(error)
    }
}

/// What one segment's walk verified so far, resumable: the scan of its
/// header and records, and where the next frame starts.
#[derive(Debug, Clone, Default)]
pub(crate) struct VerifiedWalPrefix {
    pub(crate) valid_bytes: usize,
    /// The scan the walk resumes, once the header was read. Boxed: its
    /// digest state is large, and failures carry the prefix.
    pub(crate) scan: Option<Box<crate::wal::format::SegmentScan>>,
}

impl VerifiedWalPrefix {
    /// The greatest sequence the verified records hold, or zero.
    pub(crate) fn max_sequence(&self) -> u64 {
        self.scan
            .as_ref()
            .and_then(|scan| scan.tail().last_seq)
            .unwrap_or(0)
    }

    /// The records verified so far.
    pub(crate) fn record_count(&self) -> u64 {
        self.scan
            .as_ref()
            .map_or(0, |scan| scan.tail().record_count)
    }

    /// Whether the segment's footer was verified, nothing following it.
    pub(crate) fn sealed(&self) -> bool {
        self.scan
            .as_ref()
            .is_some_and(|scan| scan.footer().is_some())
    }

    /// What the segment says it is, once its footer sealed it.
    pub(crate) fn identity(&self) -> Option<crate::wal::format::SegmentIdentity> {
        let scan = self.scan.as_ref()?;
        let footer = scan.footer()?;
        Some(crate::wal::format::SegmentIdentity {
            epoch: scan.header().epoch,
            prev: scan.header().prev,
            link: footer.sealed_facts().link,
        })
    }
}

#[derive(Debug)]
pub(crate) struct WalPrefixInspectionFailure {
    verified_prefix: VerifiedWalPrefix,
    failure: ReplayFailure,
}

impl WalPrefixInspectionFailure {
    pub(crate) fn verified_prefix(&self) -> VerifiedWalPrefix {
        self.verified_prefix.clone()
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

fn replay_error_action(
    replay_file: &ReplayFile,
    replay_policy: ReplayPolicy,
    failure: &ReplayFailure,
) -> ReplayErrorAction {
    if replay_file.kind == ReplayFileKind::FinalActive && failure.is_incomplete_tail() {
        ReplayErrorAction::TolerateFinalActiveTail
    } else if replay_policy == ReplayPolicy::SalvageValidPrefix && failure.error().is_salvageable()
    {
        ReplayErrorAction::SalvageVerifiedPrefix
    } else {
        ReplayErrorAction::Fail
    }
}

fn map_fs_error(err: FsError) -> MidgeError {
    err.into_midge()
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

    /// Final active WAL whose incomplete tail replay tolerated. Replay stays
    /// read-only; an owner that reopens the file for append must first
    /// truncate it to `valid_bytes`, or new frames land after the torn bytes.
    pub(crate) tolerated_active_tail: Option<ToleratedActiveTail>,
    /// Where salvage replay stopped. Replay stays read-only; an owner that
    /// keeps writing must first move everything past this point aside, or
    /// new records land behind corruption and reuse on-disk sequences.
    pub(crate) salvage_stop: Option<WalSalvageStop>,
}

/// Point at which salvage replay stopped, and the WAL it never reached.
#[derive(Debug, Clone, PartialEq, Eq)]
pub(crate) struct WalSalvageStop {
    /// Segment holding the first corrupt frame.
    pub(crate) segment: crate::wal::ledger::SegmentId,
    /// Verified prefix of `path` that replay applied.
    pub(crate) valid_bytes: u64,
    /// Highest verified sequence in the unreplayed files and, for a decoded
    /// record failure, in the readable remainder of the stop file.
    pub(crate) max_unreplayed_sequence: Option<u64>,
}

/// Verified prefix of a final active WAL whose incomplete tail was dropped.
#[derive(Debug, Clone, PartialEq, Eq)]
pub(crate) struct ToleratedActiveTail {
    pub(crate) path: FsPath,
    pub(crate) valid_bytes: u64,
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
            tolerated_active_tail: None,
            salvage_stop: None,
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

/// Replay `segments` through the one streaming engine (#524) as storage
/// verification does.
///
/// Local recovery keeps two behaviours the cloud path does not need: a
/// split-marker transaction spills to disk, since local writers spill
/// transactions too large to hold in memory, and under salvage a record that
/// fails to replay stops at its frame like a corrupt one. Limits are
/// unbounded, so no checkpoint is needed.
fn replay_local(
    storage: &dyn ReadFs,
    segments: &ReplaySegments,
    memtables: &mut HashMap<ColumnFamilyId, Arc<SkipListMemtable>>,
    replay_policy: ReplayPolicy,
    selection: crate::wal::replay::ReplaySelection<'_>,
    deadline: Option<&crate::common::OperationDeadline>,
) -> MidgeResult<RecoveryStats> {
    streaming::replay_wal_with_options(
        storage,
        segments,
        memtables,
        replay_policy,
        streaming::StreamingReplayLimits::local(),
        streaming::ReplayOptions {
            deadline,
            salvage_record_errors: true,
            spill_pending_txns: true,
            ..streaming::ReplayOptions::new(selection)
        },
        &mut |_, _| Ok(()),
    )
}

/// Verify every WAL frame and count records and bytes as strict replay
/// would, without materializing memtables or retaining replayed records.
/// Storage verification uses this: it needs only the stats, and a full
/// replay would allocate another copy of every unflushed write.
///
/// # Errors
///
/// Returns the error replay would return, or `Timeout` once `deadline`
/// expires between frames.
pub(crate) fn validate_wal_with_policy(
    storage: &dyn ReadFs,
    segments: &ReplaySegments,
    replay_policy: ReplayPolicy,
    deadline: Option<&crate::common::OperationDeadline>,
) -> MidgeResult<RecoveryStats> {
    let mut no_memtables = HashMap::new();
    replay_local(
        storage,
        segments,
        &mut no_memtables,
        replay_policy,
        crate::wal::replay::ReplaySelection::ValidateOnly,
        deadline,
    )
}

pub(crate) fn replay_wal_with_frontiers_within(
    storage: &dyn ReadFs,
    segments: &ReplaySegments,
    memtables: &mut HashMap<ColumnFamilyId, Arc<SkipListMemtable>>,
    replay_policy: ReplayPolicy,
    frontiers: &crate::metadata::frontiers::FlushFrontiers,
    scope: Option<&crate::common::DeadlineScope>,
) -> MidgeResult<RecoveryStats> {
    streaming::replay_wal_with_options(
        storage,
        segments,
        memtables,
        replay_policy,
        streaming::StreamingReplayLimits::local(),
        streaming::ReplayOptions {
            scope,
            salvage_record_errors: true,
            spill_pending_txns: true,
            ..streaming::ReplayOptions::new(crate::wal::replay::ReplaySelection::Frontiers(
                frontiers,
            ))
        },
        &mut |_, _| Ok(()),
    )
}

struct ReplayedWalFrame {
    record: WalRecord,
    /// The frame's verified payload, which the segment digest covers.
    payload: bytes::Bytes,
    next_pos: u64,
}

enum NextWalFrame {
    Eof,
    Frame(ReplayedWalFrame),
    /// The segment's footer frame, and the offset after it.
    Footer {
        payload: bytes::Bytes,
        next_pos: u64,
    },
}

fn open_wal_replay_file<'a>(
    storage: &'a dyn ReadFs,
    file_path: &FsPath,
    file_read_ns: &mut u128,
) -> MidgeResult<Option<Box<dyn crate::io::ReadFile + 'a>>> {
    if !storage.exists(file_path).map_err(map_fs_error)? {
        return Ok(None);
    }

    let open_start = std::time::Instant::now();
    let file = match storage.open_read(file_path) {
        Ok(file) => file,
        Err(FsError::NotFound(_)) => return Ok(None),
        Err(e) => return Err(map_fs_error(e)),
    };
    *file_read_ns = file_read_ns.saturating_add(open_start.elapsed().as_nanos());
    Ok(Some(file))
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
            // VALUE is required for Put/Insert; skipping a record without it
            // would silently drop an acknowledged write.
            let Some(value) = &record.value else {
                return Err(MidgeError::Corruption(format!(
                    "{:?} WAL record at seq {} is missing VALUE",
                    record.op, record.seq
                )));
            };
            memtable.put_with_seq(
                record.key.to_vec(),
                value.to_vec(),
                record.seq,
                record.expiration,
            )?;
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
mod fixture;
#[cfg(test)]
mod tests;
