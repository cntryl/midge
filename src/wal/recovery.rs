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
use crate::memtable::SkipListMemtable;
use std::collections::HashMap;
use std::hash::BuildHasher;
use std::sync::Arc;
use tracing::instrument;

#[cfg(test)]
thread_local! {
    static WAL_REPLAY_FILE_OPEN_COUNT: std::cell::Cell<usize> = const { std::cell::Cell::new(0) };
}

#[cfg(test)]
fn reset_wal_replay_file_open_count() {
    WAL_REPLAY_FILE_OPEN_COUNT.set(0);
}

#[cfg(test)]
fn wal_replay_file_open_count() -> usize {
    WAL_REPLAY_FILE_OPEN_COUNT.get()
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

/// Which writer epochs one WAL file may hold.
///
/// Every WAL record carries the epoch of the writer that appended it. A file
/// written in place (the active WAL, a locally rotated segment) can span a
/// restart, and a paused, fenced writer can still append a lower-epoch record
/// after its successor. Replay skips such records as stale, so every walker
/// over a local file must accept them too, or validation would reject or
/// truncate a file that replay recovers in full (#487).
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(crate) enum EpochPolicy {
    /// A published cloud segment object: one writer sealed every record.
    SingleEpoch,
    /// A local file: epochs may rise, and a lower-epoch record after a newer
    /// one is stale. It is still validated but does not lower the newest epoch.
    SkipStale,
}

impl EpochPolicy {
    /// Checks `record_epoch` against the newest epoch seen so far in the file
    /// and returns the newest epoch after this record.
    pub(crate) fn admit(self, newest: Option<u64>, record_epoch: u64) -> Result<u64, String> {
        match (self, newest) {
            (_, None) => Ok(record_epoch),
            (Self::SingleEpoch, Some(epoch)) if epoch != record_epoch => {
                Err(format!("mixes writer epochs {epoch} and {record_epoch}"))
            }
            (_, Some(epoch)) => Ok(epoch.max(record_epoch)),
        }
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
    /// File holding the first corrupt frame.
    pub(crate) path: FsPath,
    /// Verified prefix of `path` that replay applied.
    pub(crate) valid_bytes: u64,
    /// Files after `path` in replay order, never replayed.
    pub(crate) unreplayed_paths: Vec<FsPath>,
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
            max_epoch_seen: 0,
            stale_records_skipped: 0,
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

/// Replay WAL files under `wal_dir`, rebuilding memtables per column family.
///
/// Returns aggregated recovery statistics. Caller is responsible for attaching
/// the recovered memtables to the runtime state.
#[instrument(level = "info", skip(storage, memtables), fields(wal_dir = ?wal_dir))]
///
/// # Errors
///
/// Returns an error if WAL enumeration, decoding, or record application fails.
#[cfg(test)]
pub fn replay_wal(
    storage: &dyn Fs,
    wal_dir: &FsPath,
    memtables: &mut HashMap<ColumnFamilyId, Arc<SkipListMemtable>>,
) -> MidgeResult<RecoveryStats> {
    replay_wal_with_policy(storage, wal_dir, memtables, ReplayPolicy::Strict)
}

#[instrument(level = "info", skip(storage, memtables), fields(wal_dir = ?wal_dir, replay_policy = ?replay_policy))]
///
/// # Errors
///
/// Returns an error if WAL enumeration, decoding, or record application fails
/// according to the selected replay policy.
#[cfg(test)]
pub fn replay_wal_with_policy(
    storage: &dyn Fs,
    wal_dir: &FsPath,
    memtables: &mut HashMap<ColumnFamilyId, Arc<SkipListMemtable>>,
    replay_policy: ReplayPolicy,
) -> MidgeResult<RecoveryStats> {
    replay_local(storage, wal_dir, memtables, replay_policy, None, None)
}

/// Replay a local WAL directory through the one streaming engine (#524).
///
/// Local recovery keeps two behaviours the cloud path does not need: a
/// split-marker transaction spills to disk, since local writers spill
/// transactions too large to hold in memory, and under salvage a record that
/// fails to replay stops at its frame like a corrupt one. Limits are
/// unbounded, so no checkpoint is needed.
fn replay_local(
    storage: &dyn Fs,
    wal_dir: &FsPath,
    memtables: &mut HashMap<ColumnFamilyId, Arc<SkipListMemtable>>,
    replay_policy: ReplayPolicy,
    should_apply: Option<&dyn Fn(&WalRecord) -> bool>,
    deadline: Option<&crate::common::OperationDeadline>,
) -> MidgeResult<RecoveryStats> {
    streaming::replay_wal_with_options(
        storage,
        wal_dir,
        memtables,
        replay_policy,
        should_apply,
        streaming::StreamingReplayLimits::local(),
        streaming::ReplayOptions {
            deadline,
            salvage_record_errors: true,
            spill_pending_txns: true,
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
    storage: &dyn Fs,
    wal_dir: &FsPath,
    replay_policy: ReplayPolicy,
    deadline: Option<&crate::common::OperationDeadline>,
) -> MidgeResult<RecoveryStats> {
    let mut no_memtables = HashMap::new();
    replay_local(
        storage,
        wal_dir,
        &mut no_memtables,
        replay_policy,
        Some(&|_| false),
        deadline,
    )
}

pub(crate) fn replay_wal_with_manifest_filter(
    storage: &dyn Fs,
    wal_dir: &FsPath,
    memtables: &mut HashMap<ColumnFamilyId, Arc<SkipListMemtable>>,
    replay_policy: ReplayPolicy,
    should_apply: &dyn Fn(&WalRecord) -> bool,
) -> MidgeResult<RecoveryStats> {
    replay_local(
        storage,
        wal_dir,
        memtables,
        replay_policy,
        Some(should_apply),
        None,
    )
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

/// Highest writer epoch recorded in the WAL files under `wal_dir`.
///
/// Reads the same files and applies the same corruption policy as replay,
/// without applying any records, so startup can supply it as the lease epoch
/// floor (`format/lease.md` §4 step 4) before acquiring leadership. Under
/// salvage, discovery stops at the same corrupt-prefix boundary replay does.
///
/// # Errors
///
/// Returns an error if WAL enumeration or reading fails, or if a frame is
/// corrupt and `replay_policy` does not tolerate it.
pub(crate) fn max_writer_epoch(
    storage: &dyn Fs,
    wal_dir: &FsPath,
    replay_policy: ReplayPolicy,
) -> MidgeResult<u64> {
    let replay_paths = collect_replay_paths(storage, wal_dir)?;
    let (frontiers, _had_corruption) = streaming::discover_frontiers(
        storage,
        &replay_paths,
        replay_policy,
        streaming::StreamingReplayLimits::local(),
        None,
    )?;
    Ok(frontiers.max_epoch_seen())
}

struct ReplayedWalFrame {
    record: WalRecord,
    next_pos: u64,
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
mod tests;
