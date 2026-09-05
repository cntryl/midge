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

#[derive(Debug, Clone, PartialEq, Eq)]
enum CompactionEvent {
    RangeEnd(RangeTombstone),
    RangeStart(RangeTombstone),
    Version(CompactionVersion),
}

impl CompactionEvent {
    fn key(&self) -> &[u8] {
        match self {
            Self::RangeEnd(tombstone) => &tombstone.end,
            Self::RangeStart(tombstone) => &tombstone.start,
            Self::Version(version) => &version.key,
        }
    }

    fn sequence(&self) -> u64 {
        match self {
            Self::RangeEnd(tombstone) | Self::RangeStart(tombstone) => tombstone.seq,
            Self::Version(version) => version.seq,
        }
    }

    fn kind_rank(&self) -> u8 {
        match self {
            Self::RangeEnd(_) => 0,
            Self::RangeStart(_) => 1,
            Self::Version(_) => 2,
        }
    }

    fn retained_bytes(&self) -> usize {
        match self {
            Self::RangeEnd(tombstone) | Self::RangeStart(tombstone) => std::mem::size_of::<Self>()
                .saturating_add(tombstone.start.capacity())
                .saturating_add(tombstone.end.capacity()),
            Self::Version(version) => std::mem::size_of::<Self>()
                .saturating_add(version.key.capacity().saturating_mul(2))
                .saturating_add(version.value.as_ref().map_or(0, Vec::capacity)),
        }
    }
}

type CompactionEventCursor<'a> = Box<dyn Iterator<Item = MidgeResult<CompactionEvent>> + 'a>;

/// One sorted input and its current merge head.
struct EventMergeInput<'a> {
    cursor: CompactionEventCursor<'a>,
    current: Option<RetainedEvent>,
}

struct RetainedEvent {
    event: CompactionEvent,
    _reservation: crate::common::resource_budget::ResourceReservation,
}

impl RetainedEvent {
    fn new(
        event: CompactionEvent,
        budget: &crate::common::resource_budget::ResourceBudget,
    ) -> MidgeResult<Self> {
        // The filesystem cursor keeps a yielded-version reservation until its
        // next advance. This deliberately overlaps the merge reservation: the
        // merge head outlives that advance, and its key is also cloned into the
        // heap. The conservative handoff prevents either allocation from ever
        // becoming unaccounted while input advancement can allocate again.
        let reservation = budget.reserve(event.retained_bytes(), "merge head")?;
        Ok(Self {
            event,
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
    kind_rank: u8,
    input_idx: usize,
}

impl PartialEq for VersionHeapItem {
    fn eq(&self, other: &Self) -> bool {
        self.key == other.key
            && self.seq == other.seq
            && self.kind_rank == other.kind_rank
            && self.input_idx == other.input_idx
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
            Ordering::Equal => match other.kind_rank.cmp(&self.kind_rank) {
                Ordering::Equal => match self.seq.cmp(&other.seq) {
                    Ordering::Less => Ordering::Less,
                    Ordering::Greater => Ordering::Greater,
                    Ordering::Equal => self.input_idx.cmp(&other.input_idx).reverse(),
                },
                ordering => ordering,
            },
        }
    }
}

/// K-way merge over the per-SST version vectors. It emits one input head at a
/// time, so deduplication and SST writing never need an additional global
/// version vector or key map.
struct EventMergeIterator<'a> {
    inputs: Vec<EventMergeInput<'a>>,
    heap: BinaryHeap<VersionHeapItem>,
    budget: crate::common::resource_budget::ResourceBudget,
    _container_reservation: crate::common::resource_budget::ResourceReservation,
}

impl<'a> EventMergeIterator<'a> {
    fn new(
        mut cursors: Vec<CompactionEventCursor<'a>>,
        budget: crate::common::resource_budget::ResourceBudget,
    ) -> MidgeResult<Self> {
        let container_bytes = cursors.len().saturating_mul(
            std::mem::size_of::<EventMergeInput<'a>>()
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
                .map(|event| RetainedEvent::new(event, &budget))
                .transpose()?;
            if let Some(entry) = &current {
                heap.push(VersionHeapItem {
                    key: entry.event.key().to_vec(),
                    seq: entry.event.sequence(),
                    kind_rank: entry.event.kind_rank(),
                    input_idx,
                });
            }
            inputs.push(EventMergeInput { cursor, current });
        }

        Ok(Self {
            inputs,
            heap,
            budget,
            _container_reservation: container_reservation,
        })
    }

    fn next_event(&mut self) -> MidgeResult<Option<CompactionEvent>> {
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
            let next = RetainedEvent::new(next, &self.budget)?;
            self.heap.push(VersionHeapItem {
                key: next.event.key().to_vec(),
                seq: next.event.sequence(),
                kind_rank: next.event.kind_rank(),
                input_idx: head.input_idx,
            });
            input.current = Some(next);
        }

        Ok(Some(current.event))
    }

    fn peek_event(&self) -> Option<&CompactionEvent> {
        let head = self.heap.peek()?;
        self.inputs
            .get(head.input_idx)?
            .current
            .as_ref()
            .map(|retained| &retained.event)
    }
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

struct SstEventCursor {
    versions: std::iter::Peekable<RawSstVersionCursor>,
    range_events: std::iter::Peekable<std::vec::IntoIter<CompactionEvent>>,
    _range_event_reservation: crate::common::resource_budget::ResourceReservation,
}

impl SstEventCursor {
    fn open(
        sst_factory: &dyn SstFactory,
        filename: &str,
        budget: &crate::common::resource_budget::ResourceBudget,
    ) -> MidgeResult<Self> {
        let reader = sst_factory.open_for_compaction(Path::new(filename), budget.clone())?;
        let tombstone_bytes = reader.range_tombstone_memory_usage();
        let event_bytes = tombstone_bytes.saturating_mul(2);
        let range_event_reservation = budget.reserve(event_bytes, "range tombstone events")?;
        let tombstones = reader.range_tombstones();
        let mut range_events = Vec::with_capacity(tombstones.len().saturating_mul(2));
        for tombstone in tombstones {
            range_events.push(CompactionEvent::RangeStart(tombstone.clone()));
            range_events.push(CompactionEvent::RangeEnd(tombstone));
        }
        range_events.sort_by(|left, right| {
            left.key()
                .cmp(right.key())
                .then_with(|| left.kind_rank().cmp(&right.kind_rank()))
                .then_with(|| right.sequence().cmp(&left.sequence()))
        });
        let versions = reader
            .raw_version_cursor_with_budget(None, None, Some(budget.clone()))?
            .peekable();
        Ok(Self {
            versions,
            range_events: range_events.into_iter().peekable(),
            _range_event_reservation: range_event_reservation,
        })
    }
}

impl Iterator for SstEventCursor {
    type Item = MidgeResult<CompactionEvent>;

    fn next(&mut self) -> Option<Self::Item> {
        match (self.range_events.peek(), self.versions.peek()) {
            (None, None) => None,
            (Some(_), None) => self.range_events.next().map(Ok),
            (None, Some(_)) => self
                .versions
                .next()
                .map(|version| version.map(CompactionEvent::Version)),
            (Some(event), Some(Ok(version))) => {
                let event_precedes = event.key() < version.key.as_slice()
                    || (event.key() == version.key.as_slice() && event.kind_rank() < 2);
                if event_precedes {
                    self.range_events.next().map(Ok)
                } else {
                    self.versions
                        .next()
                        .map(|version| version.map(CompactionEvent::Version))
                }
            }
            (Some(_), Some(Err(_))) => self
                .versions
                .next()
                .map(|version| version.map(CompactionEvent::Version)),
        }
    }
}

struct ChainedSstEventCursor<'a> {
    sst_factory: &'a dyn SstFactory,
    files: std::slice::Iter<'a, String>,
    current: Option<SstEventCursor>,
    budget: crate::common::resource_budget::ResourceBudget,
    abort_check: Option<&'a dyn Fn() -> bool>,
    last_key: Option<(Vec<u8>, crate::common::resource_budget::ResourceReservation)>,
}

impl<'a> ChainedSstEventCursor<'a> {
    fn new(
        sst_factory: &'a dyn SstFactory,
        files: &'a [String],
        budget: crate::common::resource_budget::ResourceBudget,
        abort_check: Option<&'a dyn Fn() -> bool>,
    ) -> Self {
        Self {
            sst_factory,
            files: files.iter(),
            current: None,
            budget,
            abort_check,
            last_key: None,
        }
    }
}

impl Iterator for ChainedSstEventCursor<'_> {
    type Item = MidgeResult<CompactionEvent>;

    fn next(&mut self) -> Option<Self::Item> {
        loop {
            if let Some(current) = &mut self.current {
                if let Some(event) = current.next() {
                    return Some(event.and_then(|event| {
                        if self
                            .last_key
                            .as_ref()
                            .is_some_and(|(last, _reservation)| last.as_slice() > event.key())
                        {
                            return Err(crate::common::MidgeError::Corruption(
                                "chained compaction level is not key ordered".to_string(),
                            ));
                        }
                        let reservation = self
                            .budget
                            .reserve(event.key().len(), "chained cursor boundary key")?;
                        self.last_key = Some((event.key().to_vec(), reservation));
                        Ok(event)
                    }));
                }
                self.current = None;
            }
            let filename = self.files.next()?;
            if self.abort_check.is_some_and(|check| check()) {
                return Some(Err(crate::common::MidgeError::Aborted(
                    "compaction aborted while transitioning target files".to_string(),
                )));
            }
            match SstEventCursor::open(self.sst_factory, filename, &self.budget) {
                Ok(cursor) => self.current = Some(cursor),
                Err(error) => return Some(Err(error)),
            }
        }
    }
}

pub(crate) struct CompactionStreamInputs<'a> {
    cursors: Vec<CompactionEventCursor<'a>>,
    _cursor_reservation: crate::common::resource_budget::ResourceReservation,
}

#[cfg(test)]
impl CompactionStreamInputs<'_> {
    pub(crate) fn merge_head_count(&self) -> usize {
        self.cursors.len()
    }
}

/// Open every selected SST without advancing any input beyond its first merge
/// head. Each production filesystem cursor retains at most one decoded block.
pub(crate) fn collect_compaction_stream_inputs<'a>(
    sst_factory: &'a dyn SstFactory,
    source_files: &'a [String],
    target_files: &'a [String],
    source_level: u32,
    budget: &crate::common::resource_budget::ResourceBudget,
    abort_check: Option<&'a dyn Fn() -> bool>,
) -> MidgeResult<CompactionStreamInputs<'a>> {
    let source_streams = if source_level == 0 {
        source_files.len()
    } else {
        usize::from(!source_files.is_empty())
    };
    let cursor_count = source_streams.saturating_add(usize::from(!target_files.is_empty()));
    let cursor_bytes = cursor_count.saturating_mul(
        std::mem::size_of::<CompactionEventCursor<'a>>().saturating_add(std::mem::size_of::<
            crate::common::resource_budget::ResourceReservation,
        >()),
    );
    let cursor_reservation = budget.reserve(cursor_bytes, "raw cursor containers")?;
    let mut cursors: Vec<CompactionEventCursor<'a>> = Vec::with_capacity(cursor_count);

    if source_level == 0 {
        for filename in source_files {
            ensure_compaction_not_aborted(abort_check)?;
            cursors.push(Box::new(SstEventCursor::open(
                sst_factory,
                filename,
                budget,
            )?));
        }
    } else if !source_files.is_empty() {
        cursors.push(Box::new(ChainedSstEventCursor::new(
            sst_factory,
            source_files,
            budget.clone(),
            abort_check,
        )));
    }
    if !target_files.is_empty() {
        cursors.push(Box::new(ChainedSstEventCursor::new(
            sst_factory,
            target_files,
            budget.clone(),
            abort_check,
        )));
    }

    Ok(CompactionStreamInputs {
        cursors,
        _cursor_reservation: cursor_reservation,
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
    ensure_output_fits_local_staging(writer.as_ref(), output_size_limit)?;
    ensure_compaction_not_aborted(abort_check)?;
    let name = crate::sst::compaction_file_name(
        identity.cf_id,
        identity.target_level,
        identity.generation,
        identity.ordinal,
    );
    let path = output_dir.join(&name);
    writer.finish_to_path(&path)?;
    if output_size_limit.is_some_and(|limit| {
        std::fs::metadata(&path).is_ok_and(|metadata| metadata.len() > limit as u64)
    }) {
        std::fs::remove_file(&path)?;
        return Err(crate::common::MidgeError::ResourceLimit(
            "encoded compaction partition exceeds its local staging limit".into(),
        ));
    }
    Ok(Some((name, path)))
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

struct BudgetedTombstone {
    tombstone: RangeTombstone,
    obsolete: bool,
    _reservation: crate::common::resource_budget::ResourceReservation,
}

impl BudgetedTombstone {
    fn new(
        tombstone: &RangeTombstone,
        obsolete: bool,
        budget: &crate::common::resource_budget::ResourceBudget,
        label: &'static str,
    ) -> MidgeResult<Self> {
        let retained_bytes = std::mem::size_of::<RangeTombstone>()
            .saturating_add(tombstone.start.capacity())
            .saturating_add(tombstone.end.capacity());
        let reservation = budget.reserve(retained_bytes, label)?;
        Ok(Self {
            tombstone: tombstone.clone(),
            obsolete,
            _reservation: reservation,
        })
    }
}

fn push_tombstone_if_absent(
    tombstones: &mut Vec<BudgetedTombstone>,
    tombstone: &RangeTombstone,
    budget: &crate::common::resource_budget::ResourceBudget,
    label: &'static str,
) -> MidgeResult<()> {
    if !tombstones
        .iter()
        .any(|retained| retained.tombstone == *tombstone)
    {
        tombstones.push(BudgetedTombstone::new(tombstone, false, budget, label)?);
    }
    Ok(())
}

fn range_tombstone_is_obsolete(tombstone: &RangeTombstone, policy: TombstoneGcPolicy) -> bool {
    policy.range_eligible && tombstone_is_obsolete(tombstone.seq, policy.snapshot_horizon)
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

fn prospective_partition_size<'a>(
    writer: &dyn crate::sst::traits::DynSstWriter,
    next_version: Option<&CompactionVersion>,
    tombstones: impl Iterator<Item = &'a RangeTombstone>,
    lower_bound: Option<&[u8]>,
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
    Ok(bound.saturating_add(pending_tombstone_size(writer, tombstones, lower_bound)?))
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
    inputs: CompactionStreamInputs<'_>,
    budget: &crate::common::resource_budget::ResourceBudget,
    tombstone_gc: TombstoneGcPolicy,
    abort_check: Option<&dyn Fn() -> bool>,
    output_sink: Option<&super::CompactionOutputSink<'_>>,
    output_size_limit: Option<usize>,
) -> MidgeResult<Vec<String>> {
    let CompactionStreamInputs {
        cursors,
        _cursor_reservation,
    } = inputs;
    let mut writer = sst_factory.create_for_compaction(budget.clone())?;
    let mut partition_lower_bound: Option<(
        Vec<u8>,
        crate::common::resource_budget::ResourceReservation,
    )> = None;
    let mut partition_point_count = 0usize;
    let mut partition_ordinal = 0u32;
    let mut output_names = Vec::new();
    let mut cleanup = OutputSetCleanup::new();
    let mut active_tombstones: Vec<BudgetedTombstone> = Vec::new();
    let mut partition_tombstones: Vec<BudgetedTombstone> = Vec::new();

    let mut merged = EventMergeIterator::new(cursors, budget.clone())?;
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

        for event in &key_events {
            if let CompactionEvent::RangeEnd(tombstone) = &event.event {
                active_tombstones.retain(|active| active.tombstone != *tombstone);
            }
        }

        let mut selected_version: Option<&CompactionVersion> = None;
        for event in &key_events {
            let CompactionEvent::Version(version) = &event.event else {
                continue;
            };
            match selected_version {
                None => selected_version = Some(version),
                Some(selected) if selected.seq == version.seq && selected != version => {
                    return Err(crate::common::MidgeError::Corruption(format!(
                        "conflicting compaction versions for key {:?} at sequence {}",
                        String::from_utf8_lossy(&version.key),
                        version.seq
                    )));
                }
                Some(selected) if version.seq > selected.seq => selected_version = Some(version),
                Some(_) => {}
            }
        }

        let starts = key_events.iter().filter_map(|event| match &event.event {
            CompactionEvent::RangeStart(tombstone) => Some(tombstone),
            CompactionEvent::RangeEnd(_) | CompactionEvent::Version(_) => None,
        });
        let selected_version = selected_version.filter(|version| {
            let covered_by_active = active_tombstones.iter().any(|active| {
                active.obsolete
                    && active.tombstone.covers(&version.key)
                    && active.tombstone.seq >= version.seq
            });
            let covered_by_start = starts.clone().any(|tombstone| {
                range_tombstone_is_obsolete(tombstone, tombstone_gc)
                    && tombstone.covers(&version.key)
                    && tombstone.seq >= version.seq
            });
            !(covered_by_active
                || covered_by_start
                || (tombstone_gc.point_eligible
                    && version.is_tombstone
                    && tombstone_is_obsolete(version.seq, tombstone_gc.snapshot_horizon)))
        });

        let partition_tombstone_bytes = partition_tombstones.iter().fold(0usize, |total, item| {
            total.saturating_add(encoded_tombstone_upper_bound(&item.tombstone))
        });
        let soft_roll = selected_version.is_some()
            && partition_point_count > 0
            && writer
                .estimated_size_bytes()
                .saturating_add(partition_tombstone_bytes)
                >= target_sst_size.max(1);
        let hard_roll = if let Some(limit) = output_size_limit {
            let lower_bound = partition_lower_bound
                .as_ref()
                .map(|(key, _)| key.as_slice());
            let mut bound = prospective_partition_size(
                writer.as_ref(),
                selected_version,
                partition_tombstones.iter().map(|item| &item.tombstone),
                lower_bound,
            )?;
            for tombstone in starts.clone() {
                if range_tombstone_is_obsolete(tombstone, tombstone_gc)
                    || partition_tombstones
                        .iter()
                        .any(|item| item.tombstone == *tombstone)
                {
                    continue;
                }
                let growth = pending_tombstone_size(
                    writer.as_ref(),
                    std::iter::once(tombstone),
                    lower_bound,
                )?;
                bound = bound.saturating_add(growth);
            }
            (partition_point_count > 0 || !partition_tombstones.is_empty()) && bound > limit
        } else {
            false
        };
        let should_roll = soft_roll || hard_roll;
        if should_roll {
            let boundary_reservation =
                budget.reserve(event_key.len(), "output partition boundary")?;
            let retained = partition_tombstones
                .iter()
                .map(|item| &item.tombstone)
                .collect::<Vec<_>>();
            if let Some((name, path)) = finish_partition(
                writer,
                partition_point_count,
                &retained,
                partition_lower_bound
                    .as_ref()
                    .map(|(key, _reservation)| key.as_slice()),
                Some(&event_key),
                PartitionIdentity {
                    cf_id,
                    target_level,
                    generation,
                    ordinal: partition_ordinal,
                },
                output_dir,
                abort_check,
                output_size_limit,
            )? {
                cleanup.record(path.clone());
                if let Some(sink) = output_sink {
                    sink(&name, &path, budget)?;
                }
                output_names.push(name);
            }
            ensure_compaction_not_aborted(abort_check)?;
            partition_ordinal = partition_ordinal.checked_add(1).ok_or_else(|| {
                crate::common::MidgeError::ResourceLimit(
                    "compaction partition ordinal space exhausted".to_string(),
                )
            })?;
            partition_lower_bound = Some((event_key.clone(), boundary_reservation));
            writer = sst_factory.create_for_compaction(budget.clone())?;
            partition_point_count = 0;
            partition_tombstones.clear();
            for active in &active_tombstones {
                if !active.obsolete {
                    push_tombstone_if_absent(
                        &mut partition_tombstones,
                        &active.tombstone,
                        budget,
                        "carried range tombstone",
                    )?;
                }
            }
        }

        for tombstone in starts {
            if tombstone.start >= tombstone.end {
                return Err(crate::common::MidgeError::Corruption(
                    "compaction observed an empty or inverted range tombstone".to_string(),
                ));
            }
            let obsolete = range_tombstone_is_obsolete(tombstone, tombstone_gc);
            if !obsolete {
                push_tombstone_if_absent(
                    &mut partition_tombstones,
                    tombstone,
                    budget,
                    "partition range tombstone",
                )?;
            }
            if !active_tombstones
                .iter()
                .any(|active| active.tombstone == *tombstone)
            {
                active_tombstones.push(BudgetedTombstone::new(
                    tombstone,
                    obsolete,
                    budget,
                    "active range tombstone",
                )?);
            }
        }

        if let Some(version) = selected_version {
            writer.add_sorted_with_meta(
                &version.key,
                version.value.as_deref(),
                version.seq,
                u8::from(version.is_tombstone) * 2,
                version.expiration,
            )?;
            partition_point_count = partition_point_count.saturating_add(1);
        }
        if let Some(limit) = output_size_limit
            .filter(|_| partition_point_count > 0 || !partition_tombstones.is_empty())
        {
            let bound = prospective_partition_size(
                writer.as_ref(),
                None,
                partition_tombstones.iter().map(|item| &item.tombstone),
                partition_lower_bound
                    .as_ref()
                    .map(|(key, _)| key.as_slice()),
            )?;
            if bound > limit {
                return Err(crate::common::MidgeError::ResourceLimit(format!(
                    "indivisible compaction key group requires {bound} encoded bytes, exceeding local staging limit {limit}",
                )));
            }
        }
    }

    ensure_compaction_not_aborted(abort_check)?;
    let retained = partition_tombstones
        .iter()
        .map(|item| &item.tombstone)
        .collect::<Vec<_>>();
    if let Some((name, path)) = finish_partition(
        writer,
        partition_point_count,
        &retained,
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
        output_size_limit,
    )? {
        cleanup.record(path.clone());
        if let Some(sink) = output_sink {
            sink(&name, &path, budget)?;
        }
        output_names.push(name);
    }
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
        EventMergeIterator::new(cursors, budget).map(Self)
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
