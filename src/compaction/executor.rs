//! Compaction execution: lazy version merging and output
//!
//! This module implements a K-way compaction pipeline:
//!   1. Open one block-at-a-time logical-version cursor per input SST.
//!   2. Merge their heads into a sorted stream (key ascending, seq descending).
//!   3. Deduplicate one key at a time (newest version first).
//!   4. Preserve raw TTL values; read snapshots alone interpret expiration.
//!   5. Feed the result directly to the `SstFactory` writer.
//!
//! This keeps one merge head per input and avoids materializing either an input
//! SST, the aggregate plan, or a second deduplicated output vector.

use crate::common::MidgeResult;
use crate::sst::traits::{RawSstVersion, RawSstVersionCursor, SstFactory};
#[cfg(test)]
use crate::sst::types::KeyState;
use crate::sst::types::RangeTombstone;
use std::cmp::Ordering;
use std::collections::BinaryHeap;
use std::path::Path;

/// A single logical version of a key observed during compaction.
///
/// Compaction consumers treat this as the "flattened" key history:
///   - `seq` is strictly monotonic per write.
///   - Higher `seq` means "newer".
///   - Tombstones represent deletions.
///   - TTL is expressed as an absolute expiry timestamp (milliseconds since epoch).
pub type CompactionVersion = RawSstVersion;

fn tombstone_is_obsolete(sequence: u64, snapshot_horizon: Option<u64>) -> bool {
    snapshot_horizon.is_none_or(|horizon| sequence <= horizon)
}

fn ensure_compaction_not_aborted(abort_check: Option<&dyn Fn() -> bool>) -> MidgeResult<()> {
    if abort_check.is_some_and(|check| check()) {
        return Err(crate::common::MidgeError::Aborted(
            "compaction aborted due to ingest epoch change".to_string(),
        ));
    }
    Ok(())
}

#[cfg(test)]
#[derive(Debug, Clone, Default, PartialEq, Eq)]
pub struct SstCompactionInput {
    pub versions: Vec<CompactionVersion>,
    pub range_tombstones: Vec<RangeTombstone>,
}

#[derive(Debug, Clone, Copy)]
pub(crate) struct TombstoneGcPolicy {
    pub(crate) snapshot_horizon: Option<u64>,
    pub(crate) point_eligible: bool,
    pub(crate) range_eligible: bool,
}

/// One sorted input and its current merge head.
struct VersionMergeInput {
    cursor: RawSstVersionCursor,
    current: Option<RetainedVersion>,
}

struct RetainedVersion {
    version: CompactionVersion,
    _reservation: crate::common::resource_budget::ResourceReservation,
}

impl RetainedVersion {
    fn new(
        version: CompactionVersion,
        budget: &crate::common::resource_budget::ResourceBudget,
    ) -> MidgeResult<Self> {
        // The filesystem cursor keeps a yielded-version reservation until its
        // next advance. This deliberately overlaps the merge reservation: the
        // merge head outlives that advance, and its key is also cloned into the
        // heap. The conservative handoff prevents either allocation from ever
        // becoming unaccounted while input advancement can allocate again.
        let retained_bytes = std::mem::size_of::<CompactionVersion>()
            .saturating_add(version.key.capacity().saturating_mul(2))
            .saturating_add(version.value.as_ref().map_or(0, Vec::capacity));
        let reservation = budget.reserve(retained_bytes, "merge head")?;
        Ok(Self {
            version,
            _reservation: reservation,
        })
    }
}

/// Heap item that orders compaction versions by key ascending and sequence
/// descending. `BinaryHeap` is a max heap, so the key ordering is inverted.
#[derive(Debug, Clone)]
struct VersionHeapItem {
    key: Vec<u8>,
    seq: u64,
    input_idx: usize,
}

impl PartialEq for VersionHeapItem {
    fn eq(&self, other: &Self) -> bool {
        self.key == other.key && self.seq == other.seq && self.input_idx == other.input_idx
    }
}

impl Eq for VersionHeapItem {}

impl PartialOrd for VersionHeapItem {
    fn partial_cmp(&self, other: &Self) -> Option<Ordering> {
        Some(self.cmp(other))
    }
}

impl Ord for VersionHeapItem {
    fn cmp(&self, other: &Self) -> Ordering {
        match self.key.cmp(&other.key) {
            Ordering::Less => Ordering::Greater,
            Ordering::Greater => Ordering::Less,
            Ordering::Equal => match self.seq.cmp(&other.seq) {
                Ordering::Less => Ordering::Less,
                Ordering::Greater => Ordering::Greater,
                Ordering::Equal => self.input_idx.cmp(&other.input_idx).reverse(),
            },
        }
    }
}

/// K-way merge over the per-SST version vectors. It emits one input head at a
/// time, so deduplication and SST writing never need an additional global
/// version vector or key map.
struct VersionMergeIterator {
    inputs: Vec<VersionMergeInput>,
    heap: BinaryHeap<VersionHeapItem>,
    budget: crate::common::resource_budget::ResourceBudget,
    _container_reservation: crate::common::resource_budget::ResourceReservation,
}

impl VersionMergeIterator {
    fn new(
        mut cursors: Vec<RawSstVersionCursor>,
        budget: crate::common::resource_budget::ResourceBudget,
    ) -> MidgeResult<Self> {
        let container_bytes = cursors.len().saturating_mul(
            std::mem::size_of::<VersionMergeInput>()
                .saturating_add(std::mem::size_of::<VersionHeapItem>()),
        );
        let container_reservation =
            budget.reserve(container_bytes, "merge cursor and heap containers")?;
        let mut inputs = Vec::with_capacity(cursors.len());
        let mut heap = BinaryHeap::new();

        for (input_idx, mut cursor) in cursors.drain(..).enumerate() {
            let current = cursor
                .next()
                .transpose()?
                .map(|version| RetainedVersion::new(version, &budget))
                .transpose()?;
            if let Some(entry) = &current {
                heap.push(VersionHeapItem {
                    key: entry.version.key.clone(),
                    seq: entry.version.seq,
                    input_idx,
                });
            }
            inputs.push(VersionMergeInput { cursor, current });
        }

        Ok(Self {
            inputs,
            heap,
            budget,
            _container_reservation: container_reservation,
        })
    }

    fn next_version(&mut self) -> MidgeResult<Option<CompactionVersion>> {
        let Some(head) = self.heap.pop() else {
            return Ok(None);
        };
        let input = self.inputs.get_mut(head.input_idx).ok_or_else(|| {
            crate::common::MidgeError::Internal("compaction merge input is missing".to_string())
        })?;
        let current = input.current.take().ok_or_else(|| {
            crate::common::MidgeError::Internal("compaction merge head is missing".to_string())
        })?;

        if let Some(next) = input.cursor.next().transpose()? {
            let next = RetainedVersion::new(next, &self.budget)?;
            self.heap.push(VersionHeapItem {
                key: next.version.key.clone(),
                seq: next.version.seq,
                input_idx: head.input_idx,
            });
            input.current = Some(next);
        }

        Ok(Some(current.version))
    }

    fn peek_version(&self) -> Option<&CompactionVersion> {
        let head = self.heap.peek()?;
        self.inputs
            .get(head.input_idx)?
            .current
            .as_ref()
            .map(|retained| &retained.version)
    }
}

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
fn collect_reader_input(
    reader: &dyn crate::sst::traits::SstReaderExt,
) -> MidgeResult<SstCompactionInput> {
    let versions = reader
        .scan_range_raw_state(None, None)?
        .into_iter()
        .filter_map(|(key, state)| match state {
            KeyState::Absent => None,
            KeyState::Tombstone(seq) => Some(CompactionVersion {
                key: key.to_vec(),
                seq,
                is_tombstone: true,
                value: None,
                expiration: None,
            }),
            KeyState::Value(value, seq, expiration, _op_type) => Some(CompactionVersion {
                key: key.to_vec(),
                seq,
                is_tombstone: false,
                value: Some(value.to_vec()),
                expiration,
            }),
        })
        .collect();

    Ok(SstCompactionInput {
        versions,
        range_tombstones: reader.range_tombstones(),
    })
}

pub(crate) struct CompactionStreamInputs {
    cursors: Vec<RawSstVersionCursor>,
    range_tombstones: Vec<RangeTombstone>,
    _cursor_reservation: crate::common::resource_budget::ResourceReservation,
    _range_tombstone_reservations: Vec<crate::common::resource_budget::ResourceReservation>,
}

/// Open every selected SST without advancing any input beyond its first merge
/// head. Each production filesystem cursor retains at most one decoded block.
pub(crate) fn collect_compaction_stream_inputs(
    sst_factory: &dyn SstFactory,
    input_files: &[String],
    budget: &crate::common::resource_budget::ResourceBudget,
    abort_check: Option<&dyn Fn() -> bool>,
) -> MidgeResult<CompactionStreamInputs> {
    let cursor_bytes = input_files.len().saturating_mul(
        std::mem::size_of::<RawSstVersionCursor>().saturating_add(std::mem::size_of::<
            crate::common::resource_budget::ResourceReservation,
        >()),
    );
    let cursor_reservation = budget.reserve(cursor_bytes, "raw cursor containers")?;
    let mut cursors = Vec::with_capacity(input_files.len());
    let mut range_tombstones = Vec::new();
    let mut range_tombstone_reservations = Vec::new();

    for filename in input_files {
        // Periodically check whether we should abort (cooperative cancellation)
        if let Some(check) = abort_check {
            if check() {
                tracing::info!(file = %filename, "compaction aborting due to ingest epoch change");
                return Err(crate::common::MidgeError::Aborted(
                    "compaction aborted due to ingest epoch change".to_string(),
                ));
            }
        }

        let path = Path::new(filename);

        let reader = sst_factory.open_for_compaction(path, budget.clone())?;
        // The reader remains Arc-owned by the raw cursor, so its parsed
        // tombstones stay live. Reserve the distinct clone collected here as
        // aggregate compaction metadata before requesting it from the reader.
        let tombstone_bytes = reader.range_tombstone_memory_usage();
        range_tombstone_reservations
            .push(budget.reserve(tombstone_bytes, "range tombstone metadata")?);
        let input_range_tombstones = reader.range_tombstones();
        if !input_range_tombstones.is_empty() {
            tracing::debug!(
                file = %filename,
                count = input_range_tombstones.len(),
                "compaction observed SST range tombstones"
            );
        }
        range_tombstones.extend(input_range_tombstones);
        cursors.push(reader.raw_version_cursor_with_budget(None, None, Some(budget.clone()))?);
    }

    Ok(CompactionStreamInputs {
        cursors,
        range_tombstones: normalize_range_tombstones(range_tombstones),
        _cursor_reservation: cursor_reservation,
        _range_tombstone_reservations: range_tombstone_reservations,
    })
}

struct OutputSetCleanup {
    paths: Vec<std::path::PathBuf>,
    armed: bool,
}

impl OutputSetCleanup {
    fn new() -> Self {
        Self {
            paths: Vec::new(),
            armed: true,
        }
    }

    fn record(&mut self, path: std::path::PathBuf) {
        self.paths.push(path);
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
            match std::fs::remove_file(path) {
                Ok(()) => {}
                Err(error) if error.kind() == std::io::ErrorKind::NotFound => {}
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

struct PartitionRangeTombstoneSizeIndex<'a> {
    by_start: &'a [&'a RangeTombstone],
    by_end: Vec<&'a RangeTombstone>,
    start_length_prefix: Vec<usize>,
    end_length_prefix: Vec<usize>,
    _reservation: crate::common::resource_budget::ResourceReservation,
}

impl<'a> PartitionRangeTombstoneSizeIndex<'a> {
    fn new(
        tombstones: &'a [&'a RangeTombstone],
        budget: &crate::common::resource_budget::ResourceBudget,
    ) -> MidgeResult<Self> {
        let prefix_len = tombstones.len().saturating_add(1);
        let retained_bytes = tombstones
            .len()
            .saturating_mul(std::mem::size_of::<&RangeTombstone>())
            .saturating_add(
                prefix_len
                    .saturating_mul(std::mem::size_of::<usize>())
                    .saturating_mul(2),
            );
        let reservation = budget.reserve(retained_bytes, "range tombstone size index")?;
        let mut by_end = Vec::with_capacity(tombstones.len());
        by_end.extend_from_slice(tombstones);
        by_end.sort_by(|left, right| {
            left.end
                .cmp(&right.end)
                .then_with(|| left.start.cmp(&right.start))
                .then_with(|| right.seq.cmp(&left.seq))
        });

        let mut start_length_prefix = Vec::with_capacity(prefix_len);
        start_length_prefix.push(0usize);
        for tombstone in tombstones {
            start_length_prefix.push(
                start_length_prefix
                    .last()
                    .copied()
                    .unwrap_or(0)
                    .saturating_add(tombstone.start.len()),
            );
        }
        let mut end_length_prefix = Vec::with_capacity(prefix_len);
        end_length_prefix.push(0usize);
        for tombstone in &by_end {
            end_length_prefix.push(
                end_length_prefix
                    .last()
                    .copied()
                    .unwrap_or(0)
                    .saturating_add(tombstone.end.len()),
            );
        }

        Ok(Self {
            by_start: tombstones,
            by_end,
            start_length_prefix,
            end_length_prefix,
            _reservation: reservation,
        })
    }

    fn estimate(&self, lower_bound: Option<&[u8]>, upper_bound: &[u8]) -> usize {
        let start_upper = self
            .by_start
            .partition_point(|tombstone| tombstone.start.as_slice() < upper_bound);
        let start_lower = lower_bound.map_or(0, |lower| {
            self.by_start
                .partition_point(|tombstone| tombstone.start.as_slice() < lower)
        });
        let end_lower = lower_bound.map_or(0, |lower| {
            self.by_end
                .partition_point(|tombstone| tombstone.end.as_slice() <= lower)
        });
        let end_upper = self
            .by_end
            .partition_point(|tombstone| tombstone.end.as_slice() <= upper_bound);
        let intersecting = start_upper.saturating_sub(end_lower);
        if intersecting == 0 {
            return 0;
        }

        let spanning = start_lower.saturating_sub(end_lower);
        let starts_within_partition = self.start_length_prefix[start_upper]
            .saturating_sub(self.start_length_prefix[start_lower]);
        let ends_within_partition =
            self.end_length_prefix[end_upper].saturating_sub(self.end_length_prefix[end_lower]);
        let spanning_upper = start_upper.saturating_sub(end_upper);
        let lower_length = lower_bound.map_or(0, <[u8]>::len);
        let encoded_bytes = std::mem::size_of::<u32>()
            .saturating_add(
                intersecting.saturating_mul(
                    std::mem::size_of::<u32>()
                        .saturating_mul(2)
                        .saturating_add(std::mem::size_of::<u64>()),
                ),
            )
            .saturating_add(spanning.saturating_mul(lower_length))
            .saturating_add(starts_within_partition)
            .saturating_add(ends_within_partition)
            .saturating_add(spanning_upper.saturating_mul(upper_bound.len()));
        // SST blocks add a four-byte length prefix and a five-byte
        // codec/checksum trailer. This remains a soft target for codecs that
        // can expand incompressible input slightly.
        encoded_bytes.saturating_add(9)
    }
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
) -> MidgeResult<Option<(String, std::path::PathBuf)>> {
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
    ensure_compaction_not_aborted(abort_check)?;
    let name = crate::sst::compaction_file_name(
        identity.cf_id,
        identity.target_level,
        identity.generation,
        identity.ordinal,
    );
    let path = output_dir.join(&name);
    writer.finish_to_path(&path)?;
    Ok(Some((name, path)))
}

/// Merge, normalize, deduplicate, and write target-sized compaction partitions
/// without materializing a second deduplicated result vector.
#[allow(clippy::too_many_arguments, clippy::too_many_lines)]
pub(crate) fn write_partitioned_compaction_outputs(
    sst_factory: &dyn SstFactory,
    output_dir: &Path,
    cf_id: u32,
    target_level: u32,
    generation: u64,
    target_sst_size: usize,
    inputs: CompactionStreamInputs,
    budget: &crate::common::resource_budget::ResourceBudget,
    tombstone_gc: TombstoneGcPolicy,
    abort_check: Option<&dyn Fn() -> bool>,
) -> MidgeResult<Vec<String>> {
    let CompactionStreamInputs {
        cursors,
        range_tombstones,
        _cursor_reservation,
        _range_tombstone_reservations,
    } = inputs;
    let mut writer = sst_factory.create_for_compaction(budget.clone())?;
    let mut last_key: Option<(Vec<u8>, crate::common::resource_budget::ResourceReservation)> = None;
    let mut partition_lower_bound: Option<(
        Vec<u8>,
        crate::common::resource_budget::ResourceReservation,
    )> = None;
    let mut partition_point_count = 0usize;
    let mut partition_ordinal = 0u32;
    let mut output_names = Vec::new();
    let mut cleanup = OutputSetCleanup::new();
    let had_range_tombstones = !range_tombstones.is_empty();
    let (obsolete_range_tombstones, retained_range_tombstones): (Vec<_>, Vec<_>) =
        range_tombstones.iter().partition(|tombstone| {
            tombstone_gc.range_eligible
                && tombstone_is_obsolete(tombstone.seq, tombstone_gc.snapshot_horizon)
        });
    let tombstone_size_index =
        PartitionRangeTombstoneSizeIndex::new(&retained_range_tombstones, budget)?;

    let mut merged = VersionMergeIterator::new(cursors, budget.clone())?;
    if merged.peek_version().is_none() && !had_range_tombstones {
        return Err(crate::common::MidgeError::Internal(
            "compaction produced no output; inputs were not replaced".to_string(),
        ));
    }
    let mut seen = 0usize;
    while let Some(version) = merged.next_version()? {
        if seen.is_multiple_of(1024) {
            ensure_compaction_not_aborted(abort_check)?;
        }
        seen = seen.saturating_add(1);

        while merged
            .peek_version()
            .is_some_and(|candidate| candidate.key == version.key && candidate.seq == version.seq)
        {
            let duplicate = merged
                .next_version()?
                .expect("peeked compaction version exists");
            seen = seen.saturating_add(1);
            if seen.is_multiple_of(1024) {
                ensure_compaction_not_aborted(abort_check)?;
            }
            if duplicate != version {
                return Err(crate::common::MidgeError::Corruption(format!(
                    "conflicting compaction versions for key {:?} at sequence {}",
                    String::from_utf8_lossy(&version.key),
                    version.seq
                )));
            }
        }

        if last_key
            .as_ref()
            .is_some_and(|(key, _reservation)| key.as_slice() == version.key.as_slice())
        {
            continue;
        }
        let new_last_key_reservation = budget.reserve(version.key.len(), "last merged user key")?;
        last_key = Some((version.key.clone(), new_last_key_reservation));

        if obsolete_range_tombstones
            .iter()
            .any(|tombstone| tombstone.covers(&version.key) && tombstone.seq >= version.seq)
        {
            continue;
        }
        if tombstone_gc.point_eligible
            && version.is_tombstone
            && tombstone_is_obsolete(version.seq, tombstone_gc.snapshot_horizon)
        {
            continue;
        }

        let estimated_tombstone_bytes = tombstone_size_index.estimate(
            partition_lower_bound
                .as_ref()
                .map(|(key, _reservation)| key.as_slice()),
            &version.key,
        );
        if partition_point_count > 0
            && writer
                .estimated_size_bytes()
                .saturating_add(estimated_tombstone_bytes)
                >= target_sst_size.max(1)
        {
            let boundary_reservation =
                budget.reserve(version.key.len(), "output partition boundary")?;
            let boundary = version.key.clone();
            if let Some((name, path)) = finish_partition(
                writer,
                partition_point_count,
                &retained_range_tombstones,
                partition_lower_bound
                    .as_ref()
                    .map(|(key, _reservation)| key.as_slice()),
                Some(&boundary),
                PartitionIdentity {
                    cf_id,
                    target_level,
                    generation,
                    ordinal: partition_ordinal,
                },
                output_dir,
                abort_check,
            )? {
                cleanup.record(path);
                output_names.push(name);
            }
            ensure_compaction_not_aborted(abort_check)?;
            partition_ordinal = partition_ordinal.checked_add(1).ok_or_else(|| {
                crate::common::MidgeError::ResourceLimit(
                    "compaction partition ordinal space exhausted".to_string(),
                )
            })?;
            partition_lower_bound = Some((boundary, boundary_reservation));
            writer = sst_factory.create_for_compaction(budget.clone())?;
            partition_point_count = 0;
        }

        writer.add_sorted_with_meta(
            &version.key,
            version.value.as_deref(),
            version.seq,
            u8::from(version.is_tombstone) * 2,
            version.expiration,
        )?;
        partition_point_count = partition_point_count.saturating_add(1);
    }

    ensure_compaction_not_aborted(abort_check)?;
    if let Some((name, path)) = finish_partition(
        writer,
        partition_point_count,
        &retained_range_tombstones,
        partition_lower_bound
            .as_ref()
            .map(|(key, _reservation)| key.as_slice()),
        None,
        PartitionIdentity {
            cf_id,
            target_level,
            generation,
            ordinal: partition_ordinal,
        },
        output_dir,
        abort_check,
    )? {
        cleanup.record(path);
        output_names.push(name);
    }
    ensure_compaction_not_aborted(abort_check)?;
    drop(last_key);
    cleanup.disarm();
    Ok(output_names)
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn should_initialize_merge_with_one_raw_version_per_input() {
        use crate::sst::traits::RawSstVersionCursor;
        use std::sync::atomic::{AtomicUsize, Ordering};
        use std::sync::Arc;

        // Arrange
        struct InstrumentedCursor {
            versions: std::vec::IntoIter<CompactionVersion>,
            advances: Arc<AtomicUsize>,
        }

        impl Iterator for InstrumentedCursor {
            type Item = MidgeResult<CompactionVersion>;

            fn next(&mut self) -> Option<Self::Item> {
                self.versions.next().map(|version| {
                    self.advances.fetch_add(1, Ordering::SeqCst);
                    Ok(version)
                })
            }
        }

        let left_advances = Arc::new(AtomicUsize::new(0));
        let right_advances = Arc::new(AtomicUsize::new(0));
        let cursors: Vec<RawSstVersionCursor> = vec![
            Box::new(InstrumentedCursor {
                versions: vec![
                    mk_version("a", 3, false, Some("a3"), None),
                    mk_version("c", 1, false, Some("c1"), None),
                ]
                .into_iter(),
                advances: Arc::clone(&left_advances),
            }),
            Box::new(InstrumentedCursor {
                versions: vec![
                    mk_version("b", 2, false, Some("b2"), None),
                    mk_version("d", 1, false, Some("d1"), None),
                ]
                .into_iter(),
                advances: Arc::clone(&right_advances),
            }),
        ];

        // Act
        let _merge = VersionMergeIterator::new(
            cursors,
            crate::common::resource_budget::ResourceBudget::new(1024 * 1024),
        )
        .expect("initialize lazy merge");

        // Assert
        assert_eq!(left_advances.load(Ordering::SeqCst), 1);
        assert_eq!(right_advances.load(Ordering::SeqCst), 1);
    }

    fn mk_version<K: AsRef<[u8]>, V: AsRef<[u8]>>(
        key: K,
        seq: u64,
        is_tombstone: bool,
        value: Option<V>,
        expiration: Option<u64>,
    ) -> CompactionVersion {
        CompactionVersion {
            key: key.as_ref().to_vec(),
            seq,
            is_tombstone,
            value: value.map(|v| v.as_ref().to_vec()),
            expiration,
        }
    }

    fn cursor_from_versions(mut versions: Vec<CompactionVersion>) -> RawSstVersionCursor {
        versions.sort_by(|left, right| {
            left.key
                .cmp(&right.key)
                .then_with(|| right.seq.cmp(&left.seq))
        });
        Box::new(versions.into_iter().map(Ok))
    }

    #[test]
    fn should_merge_version_streams_by_key_then_descending_sequence() {
        // Arrange
        let left = vec![
            mk_version("a", 3, false, Some("a3"), None),
            mk_version("c", 1, false, Some("c1"), None),
        ];
        let right = vec![
            mk_version("a", 4, false, Some("a4"), None),
            mk_version("b", 2, false, Some("b2"), None),
        ];

        // Act
        let mut merge = VersionMergeIterator::new(
            vec![cursor_from_versions(left), cursor_from_versions(right)],
            crate::common::resource_budget::ResourceBudget::new(1024 * 1024),
        )
        .expect("initialize merge");
        let mut merged = Vec::new();
        while let Some(version) = merge.next_version().expect("advance merge") {
            merged.push(version);
        }

        // Assert
        assert_eq!(
            merged
                .iter()
                .map(|version| (version.key.clone(), version.seq))
                .collect::<Vec<_>>(),
            vec![
                (b"a".to_vec(), 4),
                (b"a".to_vec(), 3),
                (b"b".to_vec(), 2),
                (b"c".to_vec(), 1),
            ]
        );
    }

    #[test]
    fn should_use_input_index_given_equal_key_sequence_when_merging() {
        // Arrange
        let first = vec![mk_version("same", 7, false, Some("first"), None)];
        let second = vec![mk_version("same", 7, false, Some("second"), None)];

        // Act
        let mut merge = VersionMergeIterator::new(
            vec![cursor_from_versions(first), cursor_from_versions(second)],
            crate::common::resource_budget::ResourceBudget::new(1024 * 1024),
        )
        .expect("initialize merge");
        let mut merged = Vec::new();
        while let Some(version) = merge.next_version().expect("advance merge") {
            merged.push(version);
        }

        // Assert
        assert_eq!(merged.len(), 2);
        assert_eq!(merged[0].value.as_deref(), Some(b"first".as_ref()));
        assert_eq!(merged[1].value.as_deref(), Some(b"second".as_ref()));
    }

    #[test]
    fn should_collect_tombstones_given_stateful_reader_input() {
        use crate::sst::traits::{SstReader, SstStateReader};

        // Arrange
        struct FakeReader;

        impl SstReader for FakeReader {
            fn get(&self, _key: &[u8]) -> MidgeResult<Option<bytes::Bytes>> {
                Ok(None)
            }

            fn scan_range(
                &self,
                _start: Option<&[u8]>,
                _end: Option<&[u8]>,
            ) -> MidgeResult<Vec<(bytes::Bytes, bytes::Bytes)>> {
                Ok(Vec::new())
            }
        }

        impl SstStateReader for FakeReader {
            fn get_state(&self, _key: &[u8]) -> MidgeResult<KeyState> {
                Ok(KeyState::Absent)
            }

            fn scan_range_state(
                &self,
                _start: Option<&[u8]>,
                _end: Option<&[u8]>,
            ) -> MidgeResult<Vec<(bytes::Bytes, KeyState)>> {
                Ok(vec![
                    (
                        bytes::Bytes::from_static(b"alpha"),
                        KeyState::Value(bytes::Bytes::from_static(b"v1"), 42, Some(900), 0),
                    ),
                    (bytes::Bytes::from_static(b"beta"), KeyState::Tombstone(41)),
                ])
            }

            fn scan_range_raw_state(
                &self,
                start: Option<&[u8]>,
                end: Option<&[u8]>,
            ) -> MidgeResult<Vec<(bytes::Bytes, KeyState)>> {
                self.scan_range_state(start, end)
            }

            fn range_tombstones(&self) -> Vec<RangeTombstone> {
                vec![RangeTombstone::new(b"c".to_vec(), b"f".to_vec(), 40)]
            }
        }

        // Act
        let input = collect_reader_input(&FakeReader).expect("collect stateful input");

        // Assert
        assert_eq!(input.versions.len(), 2);
        assert_eq!(input.versions[0].key, b"alpha".to_vec());
        assert_eq!(input.versions[0].seq, 42);
        assert_eq!(input.versions[0].expiration, Some(900));
        assert!(input.versions[1].is_tombstone);
        assert_eq!(input.range_tombstones.len(), 1);
        assert_eq!(input.range_tombstones[0].start, b"c".to_vec());
    }

    #[test]
    fn should_normalize_duplicate_range_tombstones_when_collecting_input() {
        // Arrange
        let tombstones = vec![
            RangeTombstone::new(b"m".to_vec(), b"z".to_vec(), 7),
            RangeTombstone::new(b"a".to_vec(), b"f".to_vec(), 9),
            RangeTombstone::new(b"a".to_vec(), b"f".to_vec(), 9),
            RangeTombstone::new(b"a".to_vec(), b"f".to_vec(), 5),
        ];

        // Act
        let normalized = normalize_range_tombstones(tombstones);

        // Assert
        assert_eq!(normalized.len(), 3);
        assert_eq!(normalized[0].start, b"a".to_vec());
        assert_eq!(normalized[0].seq, 9);
        assert_eq!(normalized[1].seq, 5);
        assert_eq!(normalized[2].start, b"m".to_vec());
    }

    #[test]
    fn should_treat_all_tombstones_as_obsolete_when_no_snapshot_horizon() {
        // Arrange
        let horizon = None;

        // Act
        let zero_is_obsolete = tombstone_is_obsolete(0, horizon);
        let large_is_obsolete = tombstone_is_obsolete(1_000_000, horizon);

        // Assert: with no snapshot horizon, every sequence is
        // eligible for GC (legacy "drop all tombstones" behavior).
        assert!(zero_is_obsolete);
        assert!(large_is_obsolete);
    }

    #[test]
    fn should_treat_tombstone_as_obsolete_when_sequence_is_at_or_below_horizon() {
        // Arrange
        let horizon = Some(150);

        // Act
        let at_horizon = tombstone_is_obsolete(150, horizon);
        let below_horizon = tombstone_is_obsolete(149, horizon);

        // Assert: sequences at or below the horizon are obsolete, since
        // no live snapshot can observe them being resurrected.
        assert!(at_horizon);
        assert!(below_horizon);
    }

    #[test]
    fn should_retain_tombstone_when_sequence_is_above_horizon() {
        // Arrange
        let horizon = Some(150);

        // Act
        let above_horizon = tombstone_is_obsolete(151, horizon);
        let maximum_sequence = tombstone_is_obsolete(u64::MAX, horizon);

        // Assert: a tombstone newer than the horizon must be preserved
        // so a snapshot reading at or below the horizon does not observe a
        // resurrected key.
        assert!(!above_horizon);
        assert!(!maximum_sequence);
    }
}
