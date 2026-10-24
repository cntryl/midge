use super::ensure_compaction_not_aborted;
#[cfg(test)]
use super::merge::{collect_reader_input, CompactionEventCursor};
use super::merge::{
    CompactionEvent, CompactionStreamInputs, EventMergeIterator, TombstoneGcPolicy,
};
use super::{tombstone_is_obsolete, CompactionVersion};
use crate::common::MidgeResult;
use crate::io::FsError;
#[cfg(test)]
use crate::sst::traits::RawSstVersionCursor;
use crate::sst::traits::SstFactory;
use crate::types::RangeTombstone;
#[cfg(test)]
use crate::types::{EntryType, KeyState};
use std::path::Path;

struct OutputSetCleanup {
    fs: std::sync::Arc<dyn crate::io::Fs>,
    paths: Vec<std::path::PathBuf>,
    armed: bool,
}

struct FinishedPartition {
    name: String,
    path: std::path::PathBuf,
    summary: Option<crate::sst::traits::FinalizedSst>,
}

impl OutputSetCleanup {
    fn new(fs: std::sync::Arc<dyn crate::io::Fs>) -> Self {
        Self {
            fs,
            paths: Vec::new(),
            armed: true,
        }
    }

    fn record(&mut self, path: std::path::PathBuf) {
        self.paths.push(path);
    }

    fn record_finished_partition(
        &mut self,
        finished: Option<FinishedPartition>,
        names: &mut Vec<String>,
        budget: &crate::common::resource_budget::ResourceBudget,
        sink: Option<&crate::compaction::CompactionOutputSink<'_>>,
    ) -> MidgeResult<()> {
        if let Some(FinishedPartition {
            name,
            path,
            summary,
        }) = finished
        {
            // Record cleanup ownership before a fallible sink can publish or
            // reject this finalized non-authoritative partition.
            self.record(path.clone());
            if let Some(sink) = sink {
                sink(&name, &path, budget, summary.as_ref())?;
            }
            names.push(name);
        }
        Ok(())
    }

    fn disarm(&mut self) {
        self.armed = false;
    }
}

impl Drop for OutputSetCleanup {
    fn drop(&mut self) {
        if !self.armed {
            return;
        }
        for path in &self.paths {
            let result = crate::sst::fs::fs_relative_sst_path(&self.fs, path)
                .and_then(|fs_path| self.fs.remove_file(&fs_path).map_err(FsError::into_midge));
            match result {
                Ok(()) | Err(crate::common::MidgeError::NotFound) => {}
                Err(error) => tracing::warn!(
                    file = %path.display(),
                    %error,
                    "retaining non-authoritative compaction residue after cleanup failure"
                ),
            }
        }
    }
}

fn add_partition_range_tombstones(
    writer: &mut dyn crate::sst::traits::DynSstWriter,
    tombstones: &[&RangeTombstone],
    lower_bound: Option<&[u8]>,
    upper_bound: Option<&[u8]>,
    abort_check: Option<&dyn Fn() -> bool>,
) -> MidgeResult<usize> {
    let mut added = 0usize;
    for (index, tombstone) in tombstones.iter().enumerate() {
        if index.is_multiple_of(1024) {
            ensure_compaction_not_aborted(abort_check)?;
        }
        let start = lower_bound.map_or(tombstone.start.as_slice(), |lower| {
            tombstone.start.as_slice().max(lower)
        });
        let end = upper_bound.map_or(tombstone.end.as_slice(), |upper| {
            tombstone.end.as_slice().min(upper)
        });
        if start >= end {
            continue;
        }
        writer.add_range_tombstone(start, end, tombstone.seq)?;
        added = added.saturating_add(1);
    }
    Ok(added)
}

#[derive(Clone, Copy)]
struct PartitionIdentity {
    cf_id: u32,
    target_level: u32,
    generation: u64,
    ordinal: u32,
}

#[allow(clippy::too_many_arguments)]
fn finish_partition(
    mut writer: Box<dyn crate::sst::traits::DynSstWriter>,
    point_count: usize,
    retained_range_tombstones: &[&RangeTombstone],
    lower_bound: Option<&[u8]>,
    upper_bound: Option<&[u8]>,
    identity: PartitionIdentity,
    output_dir: &Path,
    abort_check: Option<&dyn Fn() -> bool>,
    output_size_limit: Option<usize>,
    output_fs: &std::sync::Arc<dyn crate::io::Fs>,
) -> MidgeResult<Option<FinishedPartition>> {
    ensure_compaction_not_aborted(abort_check)?;
    let tombstone_count = add_partition_range_tombstones(
        writer.as_mut(),
        retained_range_tombstones,
        lower_bound,
        upper_bound,
        abort_check,
    )?;
    if point_count == 0 && tombstone_count == 0 {
        return Ok(None);
    }
    ensure_output_fits_local_staging(writer.as_ref(), output_size_limit)?;
    ensure_compaction_not_aborted(abort_check)?;
    let name = crate::cloud_layout::compaction_file_name(
        identity.cf_id,
        identity.target_level,
        identity.generation,
        identity.ordinal,
    );
    let path = output_dir.join(&name);
    let summary = writer.finish_to_path_with_summary(&path)?;
    let fs_path = crate::sst::fs::fs_relative_sst_path(output_fs, &path)?;
    let size = match output_fs.metadata(&fs_path) {
        Ok(metadata) => metadata.len,
        Err(error) => {
            if let Err(cleanup_error) = output_fs.remove_file(&fs_path) {
                tracing::warn!(file = %path.display(), %cleanup_error, "retaining unverified compaction output");
            }
            return Err(error.into_midge());
        }
    };
    if output_size_limit.is_some_and(|limit| size > limit as u64) {
        output_fs
            .remove_file(&fs_path)
            .map_err(FsError::into_midge)?;
        return Err(crate::common::MidgeError::ResourceLimit(
            "encoded compaction partition exceeds its local staging limit".into(),
        ));
    }
    Ok(Some(FinishedPartition {
        name,
        path,
        summary,
    }))
}

struct BudgetedEvent {
    event: CompactionEvent,
    _reservation: crate::common::resource_budget::ResourceReservation,
}

impl BudgetedEvent {
    fn new(
        event: CompactionEvent,
        budget: &crate::common::resource_budget::ResourceBudget,
    ) -> MidgeResult<Self> {
        let reservation = budget.reserve(event.retained_bytes(), "same-key event group")?;
        Ok(Self {
            event,
            _reservation: reservation,
        })
    }
}

fn reserve_tombstone(
    tombstone: &RangeTombstone,
    budget: &crate::common::resource_budget::ResourceBudget,
    label: &'static str,
) -> MidgeResult<crate::common::resource_budget::ResourceReservation> {
    let retained_bytes = std::mem::size_of::<RangeTombstone>()
        .saturating_add(tombstone.start.capacity())
        .saturating_add(tombstone.end.capacity());
    budget.reserve(retained_bytes, label)
}

/// A retained range tombstone ordered by `(start, end, seq)`.
///
/// The key owns the only copy of the tombstone, so indexing it costs no extra
/// key bytes, and the ordering keeps a partition's tombstones sorted by the
/// bound the writer encodes first.
#[derive(Clone, PartialEq, Eq)]
struct PartitionTombstoneKey(RangeTombstone);

impl Ord for PartitionTombstoneKey {
    fn cmp(&self, other: &Self) -> std::cmp::Ordering {
        self.0
            .start
            .cmp(&other.0.start)
            .then_with(|| self.0.end.cmp(&other.0.end))
            .then_with(|| self.0.seq.cmp(&other.0.seq))
    }
}

impl PartialOrd for PartitionTombstoneKey {
    fn partial_cmp(&self, other: &Self) -> Option<std::cmp::Ordering> {
        Some(self.cmp(other))
    }
}

/// An active range tombstone ordered by `(seq, start, end)`.
///
/// Sequence-major ordering makes the highest-sequence cover the last entry, so
/// obsolete coverage is a single lookup rather than a scan of every active
/// tombstone for every merged key group.
#[derive(Clone, PartialEq, Eq)]
struct ActiveTombstoneKey(RangeTombstone);

impl Ord for ActiveTombstoneKey {
    fn cmp(&self, other: &Self) -> std::cmp::Ordering {
        self.0
            .seq
            .cmp(&other.0.seq)
            .then_with(|| self.0.start.cmp(&other.0.start))
            .then_with(|| self.0.end.cmp(&other.0.end))
    }
}

impl PartialOrd for ActiveTombstoneKey {
    fn partial_cmp(&self, other: &Self) -> Option<std::cmp::Ordering> {
        Some(self.cmp(other))
    }
}

/// Writer context that bounds how much a pending range tombstone would add to
/// the partition currently being encoded.
#[derive(Clone, Copy)]
struct PendingTombstoneBound<'a> {
    writer: &'a dyn crate::sst::traits::DynSstWriter,
    lower_bound: Option<&'a [u8]>,
}

/// Range tombstones retained for the partition being written.
///
/// Membership is logarithmic in the partition's tombstone count, and both size
/// bounds the merge loop consults per key group are maintained incrementally
/// as tombstones are added, so neither costs a scan of the whole set.
struct PartitionTombstones {
    entries: std::collections::BTreeMap<
        PartitionTombstoneKey,
        crate::common::resource_budget::ResourceReservation,
    >,
    encoded_bytes: usize,
    pending_bytes: usize,
}

impl PartitionTombstones {
    fn new() -> Self {
        Self {
            entries: std::collections::BTreeMap::new(),
            encoded_bytes: 0,
            pending_bytes: 0,
        }
    }

    fn is_empty(&self) -> bool {
        self.entries.is_empty()
    }

    fn contains(&self, tombstone: &RangeTombstone) -> bool {
        self.entries
            .contains_key(&PartitionTombstoneKey(tombstone.clone()))
    }

    /// Upper bound on the bytes this partition's tombstones encode to, kept in
    /// step with the set instead of refolded for every key group.
    fn encoded_bytes(&self) -> usize {
        self.encoded_bytes
    }

    /// Upper bound on the growth these tombstones add to the writer's own
    /// encoded bound. Only tracked while a staging limit is enforced, since a
    /// writer that cannot bound a pending tombstone must not be asked to.
    fn pending_bytes(&self) -> usize {
        self.pending_bytes
    }

    fn tombstones(&self) -> impl Iterator<Item = &RangeTombstone> + Clone {
        self.entries.keys().map(|key| &key.0)
    }

    fn clear(&mut self) {
        self.entries.clear();
        self.encoded_bytes = 0;
        self.pending_bytes = 0;
    }

    fn insert(
        &mut self,
        tombstone: &RangeTombstone,
        budget: &crate::common::resource_budget::ResourceBudget,
        label: &'static str,
        pending: Option<PendingTombstoneBound<'_>>,
    ) -> MidgeResult<()> {
        let key = PartitionTombstoneKey(tombstone.clone());
        if self.entries.contains_key(&key) {
            return Ok(());
        }
        let pending_growth = match pending {
            Some(bound) => {
                pending_tombstone_size(bound.writer, std::iter::once(tombstone), bound.lower_bound)?
            }
            None => 0,
        };
        let reservation = reserve_tombstone(tombstone, budget, label)?;
        self.encoded_bytes = self
            .encoded_bytes
            .saturating_add(encoded_tombstone_upper_bound(tombstone));
        self.pending_bytes = self.pending_bytes.saturating_add(pending_growth);
        self.entries.insert(key, reservation);
        Ok(())
    }
}

/// Range tombstones covering the key group being merged, split by whether
/// tombstone GC may drop the points they cover.
struct ActiveTombstones {
    live: std::collections::BTreeMap<
        ActiveTombstoneKey,
        crate::common::resource_budget::ResourceReservation,
    >,
    obsolete: std::collections::BTreeMap<
        ActiveTombstoneKey,
        crate::common::resource_budget::ResourceReservation,
    >,
}

impl ActiveTombstones {
    fn new() -> Self {
        Self {
            live: std::collections::BTreeMap::new(),
            obsolete: std::collections::BTreeMap::new(),
        }
    }

    fn contains(&self, tombstone: &RangeTombstone) -> bool {
        let key = ActiveTombstoneKey(tombstone.clone());
        self.live.contains_key(&key) || self.obsolete.contains_key(&key)
    }

    fn remove(&mut self, tombstone: &RangeTombstone) {
        let key = ActiveTombstoneKey(tombstone.clone());
        self.live.remove(&key);
        self.obsolete.remove(&key);
    }

    fn insert(
        &mut self,
        tombstone: &RangeTombstone,
        obsolete: bool,
        budget: &crate::common::resource_budget::ResourceBudget,
    ) -> MidgeResult<()> {
        if self.contains(tombstone) {
            return Ok(());
        }
        let reservation = reserve_tombstone(tombstone, budget, "active range tombstone")?;
        let key = ActiveTombstoneKey(tombstone.clone());
        if obsolete {
            self.obsolete.insert(key, reservation);
        } else {
            self.live.insert(key, reservation);
        }
        Ok(())
    }

    /// The obsolete cover with the highest sequence, or `None` when tombstone
    /// GC may not drop anything covering the current key group.
    fn highest_obsolete_cover(&self) -> Option<&RangeTombstone> {
        self.obsolete.last_key_value().map(|(key, _)| &key.0)
    }

    fn live_tombstones(&self) -> impl Iterator<Item = &RangeTombstone> {
        self.live.keys().map(|key| &key.0)
    }
}

/// The range-tombstone state that crosses key groups and output partitions.
///
/// Keeping the active and current-partition collections together makes the
/// carry rule explicit: only live ranges cross a partition boundary, while an
/// obsolete range remains available solely for the current key group's GC
/// decision. The contained collections retain their existing ordering and
/// byte accounting.
struct RangeTombstoneTracker {
    active: ActiveTombstones,
    partition: PartitionTombstones,
}

impl RangeTombstoneTracker {
    fn new() -> Self {
        Self {
            active: ActiveTombstones::new(),
            partition: PartitionTombstones::new(),
        }
    }

    /// Retire ranges whose end event sorts before the key-group reduction.
    fn advance_to_key<'a>(&mut self, events: impl Iterator<Item = &'a CompactionEvent>) {
        for event in events {
            if let CompactionEvent::RangeEnd(tombstone) = event {
                self.active.remove(tombstone);
            }
        }
    }

    fn highest_obsolete_cover(&self) -> Option<&RangeTombstone> {
        self.active.highest_obsolete_cover()
    }

    fn partition_tombstones(&self) -> impl Iterator<Item = &RangeTombstone> + Clone {
        self.partition.tombstones()
    }

    fn partition_contains(&self, tombstone: &RangeTombstone) -> bool {
        self.partition.contains(tombstone)
    }

    fn partition_encoded_bytes(&self) -> usize {
        self.partition.encoded_bytes()
    }

    fn partition_pending_bytes(&self) -> usize {
        self.partition.pending_bytes()
    }

    fn has_partition_tombstones(&self) -> bool {
        !self.partition.is_empty()
    }

    /// Observe the ranges opening at this key group after any preceding
    /// partition roll has carried the still-live ranges into the new writer.
    fn observe_starts<'a>(
        &mut self,
        tombstones: impl Iterator<Item = &'a RangeTombstone>,
        policy: TombstoneGcPolicy,
        budget: &crate::common::resource_budget::ResourceBudget,
        pending: Option<PendingTombstoneBound<'_>>,
    ) -> MidgeResult<()> {
        for tombstone in tombstones {
            if tombstone.start >= tombstone.end {
                return Err(crate::common::MidgeError::Corruption(
                    "compaction observed an empty or inverted range tombstone".to_string(),
                ));
            }
            let obsolete = range_tombstone_is_obsolete(tombstone, policy);
            if !obsolete {
                self.partition
                    .insert(tombstone, budget, "partition range tombstone", pending)?;
            }
            self.active.insert(tombstone, obsolete, budget)?;
        }
        Ok(())
    }

    /// Start a fresh output partition and retain only ranges that are still
    /// live at its lower bound. This deliberately uses the existing map
    /// insertion path so ordering, deduplication, reservations, and pending
    /// size accounting remain unchanged.
    fn carry_live_tombstones(
        &mut self,
        budget: &crate::common::resource_budget::ResourceBudget,
        pending: Option<PendingTombstoneBound<'_>>,
    ) -> MidgeResult<()> {
        let (active, partition) = (&self.active, &mut self.partition);
        partition.clear();
        for tombstone in active.live_tombstones() {
            partition.insert(tombstone, budget, "carried range tombstone", pending)?;
        }
        Ok(())
    }
}

fn range_tombstone_is_obsolete(tombstone: &RangeTombstone, policy: TombstoneGcPolicy) -> bool {
    policy.range_eligible
        && (policy.retained_snapshot_sources
            || tombstone_is_obsolete(tombstone.seq, policy.snapshot_horizon))
}

/// Whether the partition being built should be closed at this key group
/// because it has reached the target size.
///
/// A partition rolls when a surviving version would land in a partition that is
/// already at the target. A span of range tombstones with no surviving points
/// must roll too: local deployments have no hard limit, and every retained
/// tombstone holds budget until its partition finishes; any event key is a
/// valid fragment boundary. The range-only case is measured by the tombstones
/// alone, because an empty writer's estimate includes fixed overhead and would
/// otherwise roll on every event.
fn soft_roll_due(
    has_selected_version: bool,
    partition_point_count: usize,
    partition_size: usize,
    partition_tombstone_bytes: usize,
    has_partition_tombstones: bool,
    target_sst_size: usize,
) -> bool {
    let target = target_sst_size.max(1);
    let range_only_roll = partition_point_count == 0
        && has_partition_tombstones
        && partition_tombstone_bytes >= target;
    (has_selected_version && partition_point_count > 0 && partition_size >= target)
        || range_only_roll
}

/// Newest version in one key group, ignoring range events.
///
/// Inputs do not guarantee sequence order within a key: a chained level cursor
/// can yield an older version from one file before a newer one from the next.
/// Versions are therefore sorted here rather than trusted to arrive newest
/// first.
///
/// Two versions at the same sequence must be identical, including shadowed
/// ones. If they differ, the inputs disagree about what was written at that
/// sequence, and compaction must not paper over that by picking one.
fn select_newest_version<'a>(
    events: impl Iterator<Item = &'a CompactionEvent>,
) -> MidgeResult<Option<&'a CompactionVersion>> {
    let mut versions: Vec<&CompactionVersion> = events
        .filter_map(|event| match event {
            CompactionEvent::Version(version) => Some(version),
            CompactionEvent::RangeStart(_) | CompactionEvent::RangeEnd(_) => None,
        })
        .collect();
    versions.sort_by_key(|version| std::cmp::Reverse(version.seq));
    for pair in versions.windows(2) {
        let (newer, older) = (pair[0], pair[1]);
        if newer.seq != older.seq {
            continue;
        }
        crate::types::resolve_same_sequence(
            crate::types::VersionContent {
                is_tombstone: newer.is_tombstone,
                value: newer.value.as_deref(),
                expiration: newer.expiration,
            },
            crate::types::VersionContent {
                is_tombstone: older.is_tombstone,
                value: older.value.as_deref(),
                expiration: older.expiration,
            },
        )
        .map_err(|()| {
            crate::common::MidgeError::Corruption(format!(
                "conflicting compaction versions for key {:?} at sequence {}",
                String::from_utf8_lossy(&older.key),
                older.seq
            ))
        })?;
    }
    Ok(versions.first().copied())
}

/// Whether a key group's newest version survives tombstone GC.
///
/// `obsolete_active_cover` is the highest-sequence obsolete range tombstone
/// still open at this key group and `range_starts` are the tombstones that open
/// inside it. Every tombstone still active opened at or before this key group
/// and has not reached its `RangeEnd`, so it covers the key. A cover that
/// unexpectedly does not contain the key only retains the version, which is the
/// safe direction.
fn survives_tombstone_gc<'a>(
    version: &CompactionVersion,
    obsolete_active_cover: Option<&RangeTombstone>,
    mut range_starts: impl Iterator<Item = &'a RangeTombstone>,
    gc: TombstoneGcPolicy,
) -> bool {
    let covered_by_active = obsolete_active_cover
        .is_some_and(|active| active.covers(&version.key) && active.seq >= version.seq);
    let covered_by_start = range_starts.any(|tombstone| {
        range_tombstone_is_obsolete(tombstone, gc)
            && tombstone.covers(&version.key)
            && tombstone.seq >= version.seq
    });
    !(covered_by_active
        || covered_by_start
        || (gc.point_eligible
            && version.is_tombstone
            && tombstone_is_obsolete(version.seq, gc.snapshot_horizon)))
}

fn encoded_tombstone_upper_bound(tombstone: &RangeTombstone) -> usize {
    std::mem::size_of::<u32>()
        .saturating_mul(2)
        .saturating_add(std::mem::size_of::<u64>())
        .saturating_add(tombstone.start.len())
        .saturating_add(tombstone.end.len())
}

fn ensure_output_fits_local_staging(
    writer: &dyn crate::sst::traits::DynSstWriter,
    limit: Option<usize>,
) -> MidgeResult<()> {
    if let Some(limit) = limit {
        let encoded_bound = writer.encoded_size_upper_bound().ok_or_else(|| {
            crate::common::MidgeError::ResourceLimit(
                "compaction writer cannot bound its local output size".into(),
            )
        })?;
        if encoded_bound > limit {
            return Err(crate::common::MidgeError::ResourceLimit(format!(
                "compaction partition requires {encoded_bound} encoded bytes, exceeding local staging limit {limit}"
            )));
        }
    }
    Ok(())
}

fn prospective_partition_size(
    writer: &dyn crate::sst::traits::DynSstWriter,
    next_version: Option<&CompactionVersion>,
    pending_tombstone_bytes: usize,
) -> MidgeResult<usize> {
    let bound = match next_version {
        Some(version) => writer
            .encoded_size_upper_bound_after_sorted_entry(&version.key, version.value.as_deref()),
        None => writer.encoded_size_upper_bound(),
    }
    .ok_or_else(|| {
        crate::common::MidgeError::ResourceLimit(
            "compaction writer cannot bound its next local output".into(),
        )
    })?;
    Ok(bound.saturating_add(pending_tombstone_bytes))
}

fn pending_tombstone_size<'a>(
    writer: &dyn crate::sst::traits::DynSstWriter,
    tombstones: impl Iterator<Item = &'a RangeTombstone>,
    lower_bound: Option<&[u8]>,
) -> MidgeResult<usize> {
    let mut bound = 0usize;
    for tombstone in tombstones {
        let start = lower_bound.map_or(tombstone.start.as_slice(), |lower| {
            tombstone.start.as_slice().max(lower)
        });
        if start >= tombstone.end.as_slice() {
            continue;
        }
        let growth = writer
            .additional_range_tombstone_size_upper_bound(start, &tombstone.end)
            .ok_or_else(|| {
                crate::common::MidgeError::ResourceLimit(
                    "compaction writer cannot bound pending range tombstones".into(),
                )
            })?;
        bound = bound.saturating_add(growth);
    }
    Ok(bound)
}

/// Owns the one output writer currently receiving compacted key groups.
///
/// The roller centralizes the existing soft/hard boundary decision and the
/// state transition that follows it. It intentionally leaves output cleanup
/// and publication to the executor so the caller retains the same cleanup and
/// sink ordering around every completed file.
struct PartitionRoller<'a> {
    sst_factory: &'a dyn SstFactory,
    output_dir: &'a Path,
    identity: PartitionIdentity,
    target_sst_size: usize,
    budget: &'a crate::common::resource_budget::ResourceBudget,
    output_size_limit: Option<usize>,
    writer: Option<Box<dyn crate::sst::traits::DynSstWriter>>,
    partition_lower_bound: Option<(Vec<u8>, crate::common::resource_budget::ResourceReservation)>,
    partition_point_count: usize,
}

impl<'a> PartitionRoller<'a> {
    fn new(
        sst_factory: &'a dyn SstFactory,
        output_dir: &'a Path,
        identity: PartitionIdentity,
        target_sst_size: usize,
        budget: &'a crate::common::resource_budget::ResourceBudget,
        output_size_limit: Option<usize>,
    ) -> MidgeResult<Self> {
        Ok(Self {
            sst_factory,
            output_dir,
            identity,
            target_sst_size,
            budget,
            output_size_limit,
            writer: Some(sst_factory.create_for_compaction(budget.clone())?),
            partition_lower_bound: None,
            partition_point_count: 0,
        })
    }

    fn writer(&self) -> &dyn crate::sst::traits::DynSstWriter {
        self.writer
            .as_deref()
            .expect("partition roller always owns a writer before finalization")
    }

    fn lower_bound(&self) -> Option<&[u8]> {
        self.partition_lower_bound
            .as_ref()
            .map(|(key, _reservation)| key.as_slice())
    }

    fn pending_tombstone_bound(&self) -> Option<PendingTombstoneBound<'_>> {
        self.output_size_limit.map(|_| PendingTombstoneBound {
            writer: self.writer(),
            lower_bound: self.lower_bound(),
        })
    }

    /// Preserve the existing soft target and local staging limit decisions.
    fn should_roll<'b>(
        &self,
        selected_version: Option<&CompactionVersion>,
        starts: impl Iterator<Item = &'b RangeTombstone> + Clone,
        tombstones: &RangeTombstoneTracker,
        tombstone_gc: TombstoneGcPolicy,
    ) -> MidgeResult<bool> {
        self.should_roll_with_soft_target(selected_version, starts, tombstones, tombstone_gc, true)
    }

    fn should_roll_with_soft_target<'b>(
        &self,
        selected_version: Option<&CompactionVersion>,
        starts: impl Iterator<Item = &'b RangeTombstone> + Clone,
        tombstones: &RangeTombstoneTracker,
        tombstone_gc: TombstoneGcPolicy,
        allow_soft_roll: bool,
    ) -> MidgeResult<bool> {
        let partition_tombstone_bytes = tombstones.partition_encoded_bytes();
        let partition_size = self
            .writer()
            .estimated_size_bytes()
            .saturating_add(partition_tombstone_bytes);
        let soft_roll = allow_soft_roll
            && soft_roll_due(
                selected_version.is_some(),
                self.partition_point_count,
                partition_size,
                partition_tombstone_bytes,
                tombstones.has_partition_tombstones(),
                self.target_sst_size,
            );
        let hard_roll = if let Some(limit) = self.output_size_limit {
            let mut bound = prospective_partition_size(
                self.writer(),
                selected_version,
                tombstones.partition_pending_bytes(),
            )?;
            for tombstone in starts {
                if range_tombstone_is_obsolete(tombstone, tombstone_gc)
                    || tombstones.partition_contains(tombstone)
                {
                    continue;
                }
                let growth = pending_tombstone_size(
                    self.writer(),
                    std::iter::once(tombstone),
                    self.lower_bound(),
                )?;
                bound = bound.saturating_add(growth);
            }
            (self.partition_point_count > 0 || tombstones.has_partition_tombstones())
                && bound > limit
        } else {
            false
        };
        Ok(soft_roll || hard_roll)
    }

    fn write_selected(&mut self, version: &CompactionVersion) -> MidgeResult<()> {
        self.writer
            .as_deref_mut()
            .expect("partition roller always owns a writer before finalization")
            .add_sorted_with_meta(
                &version.key,
                version.value.as_deref(),
                version.seq,
                if version.is_tombstone {
                    crate::types::EntryType::Delete
                } else {
                    crate::types::EntryType::Put
                },
                version.expiration,
            )?;
        self.partition_point_count = self.partition_point_count.saturating_add(1);
        Ok(())
    }

    fn ensure_current_fits(&self, tombstones: &RangeTombstoneTracker) -> MidgeResult<()> {
        if let Some(limit) = self
            .output_size_limit
            .filter(|_| self.partition_point_count > 0 || tombstones.has_partition_tombstones())
        {
            let bound = prospective_partition_size(
                self.writer(),
                None,
                tombstones.partition_pending_bytes(),
            )?;
            if bound > limit {
                return Err(crate::common::MidgeError::ResourceLimit(format!(
                    "indivisible compaction key group requires {bound} encoded bytes, exceeding local staging limit {limit}",
                )));
            }
        }
        Ok(())
    }

    fn finish_current(
        &mut self,
        tombstones: &RangeTombstoneTracker,
        upper_bound: Option<&[u8]>,
        abort_check: Option<&dyn Fn() -> bool>,
    ) -> MidgeResult<Option<FinishedPartition>> {
        let writer = self
            .writer
            .take()
            .expect("partition roller always owns a writer before finalization");
        let retained = tombstones.partition_tombstones().collect::<Vec<_>>();
        let output_fs = self.sst_factory.output_fs();
        finish_partition(
            writer,
            self.partition_point_count,
            &retained,
            self.lower_bound(),
            upper_bound,
            self.identity,
            self.output_dir,
            abort_check,
            self.output_size_limit,
            &output_fs,
        )
    }

    /// Finish the current partition and retain its boundary reservation until
    /// the caller has recorded and staged the returned output. Keeping the
    /// later state transition separate preserves the existing cleanup/sink
    /// ordering when staging fails.
    fn finish_for_roll(
        &mut self,
        boundary_key: &[u8],
        tombstones: &RangeTombstoneTracker,
        abort_check: Option<&dyn Fn() -> bool>,
    ) -> MidgeResult<(
        Option<FinishedPartition>,
        crate::common::resource_budget::ResourceReservation,
    )> {
        let boundary_reservation = self
            .budget
            .reserve(boundary_key.len(), "output partition boundary")?;
        let finished = self.finish_current(tombstones, Some(boundary_key), abort_check)?;
        Ok((finished, boundary_reservation))
    }

    /// Begin the partition after an already staged boundary. The executor
    /// invokes this only after it has recorded the completed path and called
    /// its publication sink, matching the original failure ordering.
    fn start_next_partition(
        &mut self,
        boundary_key: Vec<u8>,
        boundary_reservation: crate::common::resource_budget::ResourceReservation,
        tombstones: &mut RangeTombstoneTracker,
        partition_emitted: bool,
    ) -> MidgeResult<()> {
        if partition_emitted {
            self.identity.ordinal = self.identity.ordinal.checked_add(1).ok_or_else(|| {
                crate::common::MidgeError::ResourceLimit(
                    "compaction partition ordinal space exhausted".to_string(),
                )
            })?;
        }
        self.partition_lower_bound = Some((boundary_key, boundary_reservation));
        self.writer = Some(
            self.sst_factory
                .create_for_compaction(self.budget.clone())?,
        );
        self.partition_point_count = 0;
        let pending = self.pending_tombstone_bound();
        tombstones.carry_live_tombstones(self.budget, pending)?;
        Ok(())
    }

    fn finish(
        mut self,
        tombstones: &RangeTombstoneTracker,
        abort_check: Option<&dyn Fn() -> bool>,
    ) -> MidgeResult<Option<FinishedPartition>> {
        self.finish_current(tombstones, None, abort_check)
    }
}

fn collect_fragment_key_events(
    first: CompactionEvent,
    merged: &mut EventMergeIterator<'_>,
    budget: &crate::common::resource_budget::ResourceBudget,
) -> MidgeResult<Vec<BudgetedEvent>> {
    let mut events = vec![BudgetedEvent::new(first, budget)?];
    while merged
        .peek_event()
        .is_some_and(|candidate| candidate.key() == events[0].event.key())
    {
        let event = merged.next_event()?.expect("peeked fragment event exists");
        events.push(BudgetedEvent::new(event, budget)?);
    }
    Ok(events)
}

#[allow(clippy::too_many_arguments)]
fn write_fragment_key_events(
    events: &[BudgetedEvent],
    roller: &mut PartitionRoller<'_>,
    tombstones: &mut RangeTombstoneTracker,
    cleanup: &mut OutputSetCleanup,
    names: &mut Vec<String>,
    forced_roll: bool,
    final_group: bool,
    abort_check: Option<&dyn Fn() -> bool>,
    sink: Option<&crate::compaction::CompactionOutputSink<'_>>,
) -> MidgeResult<()> {
    tombstones.advance_to_key(events.iter().map(|event| &event.event));
    // Keep the ordinary equal-sequence corruption check, while retaining every
    // version instead of applying the returned newest-version selection.
    let newest = select_newest_version(events.iter().map(|event| &event.event))?;
    let starts = events.iter().filter_map(|event| match &event.event {
        CompactionEvent::RangeStart(tombstone) => Some(tombstone),
        CompactionEvent::RangeEnd(_) | CompactionEvent::Version(_) => None,
    });
    let retain_all = TombstoneGcPolicy {
        snapshot_horizon: None,
        point_eligible: false,
        range_eligible: false,
        retained_snapshot_sources: false,
    };
    let key = events[0].event.key();
    // Splitting off an outer endpoint already retained by a range would add
    // two files at that endpoint, in addition to any unchanged level neighbor.
    let keep_ending_range = final_group
        && tombstones
            .partition_tombstones()
            .any(|range| range.end == key);
    let allow_soft_roll = !forced_roll && !keep_ending_range;
    // A hard roll across a gap can still discharge carried ranges. Rolling
    // again at the same boundary cannot reduce the indivisible key's budget.
    let can_roll = !forced_roll || roller.lower_bound().is_some_and(|lower| lower < key);
    if can_roll
        && roller.should_roll_with_soft_target(
            newest,
            starts.clone(),
            tombstones,
            retain_all,
            allow_soft_roll,
        )?
    {
        let (finished, boundary) = roller.finish_for_roll(key, tombstones, abort_check)?;
        let partition_emitted = finished.is_some();
        cleanup.record_finished_partition(finished, names, roller.budget, sink)?;
        roller.start_next_partition(key.to_vec(), boundary, tombstones, partition_emitted)?;
    }
    tombstones.observe_starts(
        starts,
        retain_all,
        roller.budget,
        roller.pending_tombstone_bound(),
    )?;
    for event in events {
        if let CompactionEvent::Version(version) = &event.event {
            roller.write_selected(version)?;
        }
    }
    roller.ensure_current_fits(tombstones)
}

/// Rewrite one source without reducing its raw history, cutting ranges and
/// point-key groups at the planned boundary before ordinary size partitioning.
#[allow(clippy::too_many_arguments)]
pub(crate) fn write_fragmented_compaction_outputs(
    sst_factory: &dyn SstFactory,
    output_dir: &Path,
    plan: &crate::compaction::CompactionPlan,
    target_sst_size: usize,
    inputs: CompactionStreamInputs<'_>,
    budget: &crate::common::resource_budget::ResourceBudget,
    abort_check: Option<&dyn Fn() -> bool>,
    output_sink: Option<&crate::compaction::CompactionOutputSink<'_>>,
    output_size_limit: Option<usize>,
) -> MidgeResult<Vec<String>> {
    let crate::compaction::strategy::CompactionPurpose::Fragment { split_key } = &plan.purpose
    else {
        return Err(crate::common::MidgeError::Corruption(
            "fragmentation requires a split boundary".into(),
        ));
    };
    let _split_reservation = budget.reserve(split_key.len(), "fragment split boundary")?;
    let CompactionStreamInputs {
        cursors,
        _cursor_reservation,
    } = inputs;
    let mut merged = EventMergeIterator::new(cursors, budget.clone(), abort_check)?;
    if merged
        .peek_event()
        .is_none_or(|first| first.key() >= split_key.as_slice())
    {
        return Err(crate::common::MidgeError::Corruption(
            "fragment boundary is not inside its source bounds".into(),
        ));
    }
    let mut roller = PartitionRoller::new(
        sst_factory,
        output_dir,
        PartitionIdentity {
            cf_id: plan.cf_id,
            target_level: plan.target_level,
            generation: plan.output_seq,
            ordinal: 0,
        },
        target_sst_size,
        budget,
        output_size_limit,
    )?;
    let mut tombstones = RangeTombstoneTracker::new();
    let mut cleanup = OutputSetCleanup::new(sst_factory.output_fs());
    let mut names = Vec::new();
    let mut cut_pending = true;
    let mut has_key_after_cut = false;
    while let Some(first) = merged.next_event()? {
        ensure_compaction_not_aborted(abort_check)?;
        let events = collect_fragment_key_events(first, &mut merged, budget)?;
        let key = events[0].event.key();
        has_key_after_cut |= key > split_key.as_slice();
        let forced_roll = cut_pending && key >= split_key.as_slice();
        if forced_roll {
            // At an exact endpoint, retire ending ranges before carrying;
            // across a gap they remain live until the later endpoint event.
            if key == split_key.as_slice() {
                tombstones.advance_to_key(events.iter().map(|event| &event.event));
            }
            let (finished, boundary) =
                roller.finish_for_roll(split_key, &tombstones, abort_check)?;
            let partition_emitted = finished.is_some();
            cleanup.record_finished_partition(finished, &mut names, budget, output_sink)?;
            roller.start_next_partition(
                split_key.clone(),
                boundary,
                &mut tombstones,
                partition_emitted,
            )?;
            cut_pending = false;
        }
        write_fragment_key_events(
            &events,
            &mut roller,
            &mut tombstones,
            &mut cleanup,
            &mut names,
            forced_roll,
            merged.peek_event().is_none(),
            abort_check,
            output_sink,
        )?;
    }
    if !has_key_after_cut {
        return Err(crate::common::MidgeError::Corruption(
            "fragment boundary is not inside its source bounds".into(),
        ));
    }
    cleanup.record_finished_partition(
        roller.finish(&tombstones, abort_check)?,
        &mut names,
        budget,
        output_sink,
    )?;
    ensure_compaction_not_aborted(abort_check)?;
    cleanup.disarm();
    Ok(names)
}

/// Merge, normalize, deduplicate, and write target-sized compaction partitions
/// without materializing a second deduplicated result vector.
#[allow(clippy::too_many_arguments)]
pub(crate) fn write_partitioned_compaction_outputs(
    sst_factory: &dyn SstFactory,
    output_dir: &Path,
    cf_id: u32,
    target_level: u32,
    generation: u64,
    target_sst_size: usize,
    inputs: CompactionStreamInputs<'_>,
    budget: &crate::common::resource_budget::ResourceBudget,
    tombstone_gc: TombstoneGcPolicy,
    abort_check: Option<&dyn Fn() -> bool>,
    output_sink: Option<&crate::compaction::CompactionOutputSink<'_>>,
    output_size_limit: Option<usize>,
) -> MidgeResult<Vec<String>> {
    ensure_compaction_not_aborted(abort_check)?;
    let CompactionStreamInputs {
        cursors,
        _cursor_reservation,
    } = inputs;
    let mut roller = PartitionRoller::new(
        sst_factory,
        output_dir,
        PartitionIdentity {
            cf_id,
            target_level,
            generation,
            ordinal: 0,
        },
        target_sst_size,
        budget,
        output_size_limit,
    )?;
    let mut output_names = Vec::new();
    let mut cleanup = OutputSetCleanup::new(sst_factory.output_fs());
    let mut tombstones = RangeTombstoneTracker::new();

    let mut merged = EventMergeIterator::new(cursors, budget.clone(), abort_check)?;
    if merged.peek_event().is_none() {
        return Err(crate::common::MidgeError::Internal(
            "compaction produced no output; inputs were not replaced".to_string(),
        ));
    }
    let mut seen = 0usize;
    while let Some(first_event) = merged.next_event()? {
        if seen.is_multiple_of(1024) {
            ensure_compaction_not_aborted(abort_check)?;
        }
        seen = seen.saturating_add(1);
        let event_key = first_event.key().to_vec();
        let mut key_events = vec![BudgetedEvent::new(first_event, budget)?];
        while merged
            .peek_event()
            .is_some_and(|candidate| candidate.key() == event_key)
        {
            let event = merged
                .next_event()?
                .expect("peeked compaction event exists");
            key_events.push(BudgetedEvent::new(event, budget)?);
            seen = seen.saturating_add(1);
            if seen.is_multiple_of(1024) {
                ensure_compaction_not_aborted(abort_check)?;
            }
        }

        tombstones.advance_to_key(key_events.iter().map(|event| &event.event));

        let selected_version = select_newest_version(key_events.iter().map(|event| &event.event))?;

        let starts = key_events.iter().filter_map(|event| match &event.event {
            CompactionEvent::RangeStart(tombstone) => Some(tombstone),
            CompactionEvent::RangeEnd(_) | CompactionEvent::Version(_) => None,
        });
        let selected_version = selected_version.filter(|version| {
            survives_tombstone_gc(
                version,
                tombstones.highest_obsolete_cover(),
                starts.clone(),
                tombstone_gc,
            )
        });

        if roller.should_roll(selected_version, starts.clone(), &tombstones, tombstone_gc)? {
            let (finished, boundary_reservation) =
                roller.finish_for_roll(&event_key, &tombstones, abort_check)?;
            let partition_emitted = finished.is_some();
            cleanup.record_finished_partition(finished, &mut output_names, budget, output_sink)?;
            ensure_compaction_not_aborted(abort_check)?;
            roller.start_next_partition(
                event_key.clone(),
                boundary_reservation,
                &mut tombstones,
                partition_emitted,
            )?;
        }

        tombstones.observe_starts(
            starts,
            tombstone_gc,
            budget,
            roller.pending_tombstone_bound(),
        )?;

        if let Some(version) = selected_version {
            roller.write_selected(version)?;
        }
        roller.ensure_current_fits(&tombstones)?;
    }

    ensure_compaction_not_aborted(abort_check)?;
    cleanup.record_finished_partition(
        roller.finish(&tombstones, abort_check)?,
        &mut output_names,
        budget,
        output_sink,
    )?;
    ensure_compaction_not_aborted(abort_check)?;
    cleanup.disarm();
    Ok(output_names)
}

#[cfg(test)]
struct VersionMergeIterator<'a>(EventMergeIterator<'a>);

#[cfg(test)]
impl VersionMergeIterator<'_> {
    fn new(
        cursors: Vec<RawSstVersionCursor>,
        budget: crate::common::resource_budget::ResourceBudget,
    ) -> MidgeResult<Self> {
        let cursors = cursors
            .into_iter()
            .map(|cursor| {
                Box::new(cursor.map(|version| version.map(CompactionEvent::Version)))
                    as CompactionEventCursor<'static>
            })
            .collect();
        EventMergeIterator::new(cursors, budget, None).map(Self)
    }

    fn next_version(&mut self) -> MidgeResult<Option<CompactionVersion>> {
        match self.0.next_event()? {
            Some(CompactionEvent::Version(version)) => Ok(Some(version)),
            Some(CompactionEvent::RangeEnd(_) | CompactionEvent::RangeStart(_)) => {
                Err(crate::common::MidgeError::Internal(
                    "version-only test merge observed a range event".to_string(),
                ))
            }
            None => Ok(None),
        }
    }
}

#[cfg(test)]
fn normalize_range_tombstones(mut tombstones: Vec<RangeTombstone>) -> Vec<RangeTombstone> {
    tombstones.sort_by(|left, right| {
        left.start
            .cmp(&right.start)
            .then_with(|| left.end.cmp(&right.end))
            .then_with(|| right.seq.cmp(&left.seq))
    });
    tombstones.dedup();
    tombstones
}

#[cfg(test)]
#[path = "tests.rs"]
mod tests;
