//! Cloud replay with bounded reads, transaction buffers, and checkpointable memtables.

use super::{
    apply_record, collect_replay_paths, open_wal_replay_file, replay_error_action, NextWalFrame,
    RecoveryStats, ReplayErrorAction, ReplayFile, ReplayPolicy, WriterEpochFrontiers,
};
use crate::common::{MidgeError, MidgeResult};
use crate::io::{Fs, FsPath};
use crate::sst::{size_bound, SkipListMemtable};
use crate::wal::{types::WalOpRole, WalRecord};
use std::collections::HashMap;
use std::sync::Arc;

mod frame_reader;
#[cfg(test)]
mod tests;

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

type Memtables = HashMap<u32, Arc<SkipListMemtable>>;
type Checkpoint<'a> = dyn FnMut(&mut Memtables, &RecoveryStats) -> MidgeResult<()> + 'a;

struct PendingTxn {
    records: Vec<WalRecord>,
    bytes: usize,
}

struct ReplayState<'a> {
    stats: RecoveryStats,
    memtables: &'a mut Memtables,
    open_txns: HashMap<(u64, u64), PendingTxn>,
    pending_bytes: usize,
    committed_sequence: Option<u64>,
    limits: StreamingReplayLimits,
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
    limits.validate()?;
    let started = std::time::Instant::now();
    let paths = collect_replay_paths(storage, wal_dir)?;
    let (frontiers, had_corruption) = discover_frontiers(storage, &paths, replay_policy, limits)?;
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
        should_apply,
        checkpoint,
    };
    replay_paths(storage, &paths, replay_policy, &frontiers, &mut state)?;
    state.stats.total_replay_ns = started.elapsed().as_nanos();
    Ok(state.stats)
}

/// Inspect one stable WAL file with the same tail and epoch contracts as the
/// existing active-byte inspector, without reading the complete object.
pub(crate) fn inspect_wal_file(
    file: &dyn crate::io::File,
    path: &FsPath,
    limits: StreamingReplayLimits,
) -> Result<super::VerifiedWalPrefix, super::WalPrefixInspectionFailure> {
    inspect_file(file, path, limits, false, &mut |_| Ok(()))
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
    let prefix = inspect_file(file, path, limits, true, visitor)
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
    sealed: bool,
    visitor: &mut dyn FnMut(&WalRecord) -> MidgeResult<()>,
) -> Result<super::VerifiedWalPrefix, super::WalPrefixInspectionFailure> {
    inspect_file_from(
        file,
        path,
        limits,
        sealed,
        super::VerifiedWalPrefix::default(),
        visitor,
    )
}

pub(crate) fn visit_sealed_wal_records_from(
    file: &dyn crate::io::File,
    path: &FsPath,
    limits: StreamingReplayLimits,
    progress: &mut super::VerifiedWalPrefix,
    visitor: &mut dyn FnMut(&WalRecord) -> MidgeResult<()>,
) -> MidgeResult<()> {
    match inspect_file_from(file, path, limits, true, *progress, visitor) {
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
    sealed: bool,
    mut prefix: super::VerifiedWalPrefix,
    visitor: &mut dyn FnMut(&WalRecord) -> MidgeResult<()>,
) -> Result<super::VerifiedWalPrefix, super::WalPrefixInspectionFailure> {
    limits.validate().map_err(|error| {
        super::wal_prefix_failure(super::VerifiedWalPrefix::default(), error.into())
    })?;
    let mut read_ns = 0;
    loop {
        let next =
            frame_reader::next_frame(file, path, prefix.valid_bytes as u64, limits, &mut read_ns)
                .map_err(|failure| super::wal_prefix_failure(prefix, failure))?;
        let NextWalFrame::Frame(frame) = next else {
            return Ok(prefix);
        };
        if prefix.record_count > 0
            && (frame.record.writer_epoch < prefix.writer_epoch
                || (sealed && frame.record.writer_epoch != prefix.writer_epoch))
        {
            return Err(super::wal_prefix_failure(
                prefix,
                super::ReplayFailure::Error(MidgeError::Corruption(
                    "active WAL writer epoch regressed".into(),
                )),
            ));
        }
        validate_record_contents(&frame.record, limits.max_pending_txn_bytes)
            .map_err(|error| super::wal_prefix_failure(prefix, error.into()))?;
        visitor(&frame.record).map_err(|error| super::wal_prefix_failure(prefix, error.into()))?;
        prefix.writer_epoch = frame.record.writer_epoch;
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

fn discover_frontiers(
    storage: &dyn Fs,
    paths: &[ReplayFile],
    policy: ReplayPolicy,
    limits: StreamingReplayLimits,
) -> MidgeResult<(WriterEpochFrontiers, bool)> {
    let mut frontiers = WriterEpochFrontiers::default();
    let mut ordinal = 0_u64;
    let mut read_ns = 0;
    for path in paths {
        let Some(file) = open_wal_replay_file(storage, &path.path, &mut read_ns)? else {
            continue;
        };
        let mut pos = 0;
        loop {
            match frame_reader::next_frame(&*file, &path.path, pos, limits, &mut read_ns) {
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
        let mut pos = 0;
        loop {
            let frame = match frame_reader::next_frame(
                &*file,
                &path.path,
                pos,
                state.limits,
                &mut state.stats.wal_read_ns,
            ) {
                Ok(NextWalFrame::Eof) => break,
                Ok(NextWalFrame::Frame(frame)) => frame,
                Err(failure) => {
                    return match replay_error_action(path, policy, &failure) {
                        ReplayErrorAction::TolerateFinalActiveTail => Ok(()),
                        ReplayErrorAction::SalvageVerifiedPrefix => {
                            state.stats.mark_corruption();
                            Ok(())
                        }
                        ReplayErrorAction::Fail => Err(failure.into_error()),
                    }
                }
            };
            let record_ordinal = ordinal;
            ordinal = ordinal.saturating_add(1);
            let duplicate = max_seen_sequence.is_some_and(|sequence| frame.record.seq <= sequence)
                && duplicate_before(
                    storage,
                    &paths[..=index],
                    pos,
                    (&frame.record, record_ordinal),
                    frontiers,
                    state.limits,
                    &mut state.stats.wal_read_ns,
                )?;
            max_seen_sequence = Some(max_seen_sequence.unwrap_or(0).max(frame.record.seq));
            pos = frame.next_pos;
            if duplicate {
                continue;
            }
            if frontiers.is_stale(&frame.record, record_ordinal) {
                state.stats.stale_records_skipped += 1;
                continue;
            }
            state.process(frame.record)?;
        }
    }
    Ok(())
}

/// Normal monotonically sequenced files never enter this slow path. Rotation
/// overlap is resolved by exact comparisons against earlier immutable bytes,
/// avoiding a record/value-sized deduplication index for the entire backlog.
fn duplicate_before(
    storage: &dyn Fs,
    paths: &[ReplayFile],
    current_pos: u64,
    record_with_ordinal: (&WalRecord, u64),
    frontiers: &WriterEpochFrontiers,
    limits: StreamingReplayLimits,
    read_ns: &mut u128,
) -> MidgeResult<bool> {
    let (record, record_ordinal) = record_with_ordinal;
    let mut prior_ordinal = 0_u64;
    for (index, path) in paths.iter().enumerate() {
        let current_file = index + 1 == paths.len();
        let Some(file) = open_wal_replay_file(storage, &path.path, read_ns)? else {
            continue;
        };
        let mut pos = 0;
        while !current_file || pos < current_pos {
            let frame = match frame_reader::next_frame(&*file, &path.path, pos, limits, read_ns)
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
                        compression: None,
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
                    let bytes = size_of::<PendingTxn>()
                        .saturating_add(size_of::<(u64, u64)>())
                        .saturating_mul(2);
                    self.reserve_pending(bytes)?;
                    self.open_txns.insert(
                        key,
                        PendingTxn {
                            records: Vec::new(),
                            bytes,
                        },
                    );
                }
            }
            WalOpRole::TransactionCommit => {
                if let Some(txn_id) = record.txn_id {
                    if let Some(pending) = self.open_txns.remove(&(record.writer_epoch, txn_id)) {
                        self.pending_bytes = self.pending_bytes.saturating_sub(pending.bytes);
                        self.apply_atomic(&pending.records)?;
                    }
                }
            }
            WalOpRole::ValueWrite | WalOpRole::PointDelete | WalOpRole::RangeDelete => {
                let key = record.txn_id.map(|id| (record.writer_epoch, id));
                if let Some(key) = key.filter(|key| self.open_txns.contains_key(key)) {
                    let bytes = record_bytes(&record);
                    self.reserve_pending(bytes)?;
                    let pending = self
                        .open_txns
                        .get_mut(&key)
                        .expect("checked pending transaction");
                    pending.bytes = pending.bytes.saturating_add(bytes);
                    pending.records.push(record);
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
    size_of::<WalRecord>()
        .saturating_mul(2)
        .saturating_add(record.key.len())
        .saturating_add(record.value.as_ref().map_or(0, bytes::Bytes::len))
        .saturating_add(record.range_end.as_ref().map_or(0, bytes::Bytes::len))
}
