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
        let retained_bytes = std::mem::size_of::<CompactionVersion>()
            .saturating_add(version.key.capacity())
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
}

impl VersionMergeIterator {
    fn new(
        mut cursors: Vec<RawSstVersionCursor>,
        budget: crate::common::resource_budget::ResourceBudget,
    ) -> MidgeResult<Self> {
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

        let reader = sst_factory.open(path)?;
        let input_range_tombstones = reader.range_tombstones();
        let tombstone_bytes = input_range_tombstones.iter().fold(
            input_range_tombstones
                .len()
                .saturating_mul(std::mem::size_of::<RangeTombstone>()),
            |total, tombstone| {
                total
                    .saturating_add(tombstone.start.capacity())
                    .saturating_add(tombstone.end.capacity())
            },
        );
        range_tombstone_reservations
            .push(budget.reserve(tombstone_bytes, "range tombstone metadata")?);
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
        _range_tombstone_reservations: range_tombstone_reservations,
    })
}

/// Merge, normalize, deduplicate, and write compaction versions without
/// materializing a second deduplicated result vector.
pub(crate) fn write_merged_compaction_output_to_sst(
    sst_factory: &dyn SstFactory,
    output_filename: &str,
    inputs: CompactionStreamInputs,
    budget: &crate::common::resource_budget::ResourceBudget,
    tombstone_gc: TombstoneGcPolicy,
    abort_check: Option<&dyn Fn() -> bool>,
) -> MidgeResult<bool> {
    let CompactionStreamInputs {
        cursors,
        range_tombstones,
        _range_tombstone_reservations,
    } = inputs;
    let mut writer = sst_factory.create()?;
    let mut last_key: Option<Vec<u8>> = None;
    let mut written = 0usize;
    let (obsolete_range_tombstones, retained_range_tombstones): (Vec<_>, Vec<_>) =
        range_tombstones.iter().partition(|tombstone| {
            tombstone_gc.range_eligible
                && tombstone_is_obsolete(tombstone.seq, tombstone_gc.snapshot_horizon)
        });

    let mut merged = VersionMergeIterator::new(cursors, budget.clone())?;
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

        if last_key.as_deref() == Some(version.key.as_slice()) {
            continue;
        }
        last_key = Some(version.key.clone());

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

        writer.add_sorted_with_meta(
            &version.key,
            version.value.as_deref(),
            version.seq,
            u8::from(version.is_tombstone) * 2,
            version.expiration,
        )?;
        written = written.saturating_add(1);
    }

    for (index, tombstone) in retained_range_tombstones.iter().enumerate() {
        if index % 1024 == 0 {
            ensure_compaction_not_aborted(abort_check)?;
        }
        writer.add_range_tombstone(&tombstone.start, &tombstone.end, tombstone.seq)?;
    }

    ensure_compaction_not_aborted(abort_check)?;

    if written == 0 && retained_range_tombstones.is_empty() {
        return Ok(false);
    }

    let output_path = Path::new(output_filename);
    crate::sst::fs::finish_writer_to_path(writer, output_path)?;

    // Cancellation can race with finalization. Once an output exists, it is
    // still only staged: remove it before reporting the abort so a later
    // compaction cannot mistake it for a publishable replacement.
    if abort_check.is_some_and(|check| check()) {
        match std::fs::remove_file(output_path) {
            Ok(()) => {}
            Err(error) if error.kind() == std::io::ErrorKind::NotFound => {}
            Err(error) => return Err(crate::common::MidgeError::Io(error)),
        }
        return Err(crate::common::MidgeError::Aborted(
            "compaction aborted due to ingest epoch change".to_string(),
        ));
    }

    Ok(true)
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
