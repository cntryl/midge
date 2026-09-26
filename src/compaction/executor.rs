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
use crate::types::EntryType;
#[cfg(test)]
use crate::types::KeyState;
use crate::types::RangeTombstone;
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
    fs: std::sync::Arc<dyn crate::io::Fs>,
    paths: Vec<std::path::PathBuf>,
    armed: bool,
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
                .and_then(|fs_path| self.fs.remove_file(&fs_path).map_err(Into::into));
            match result {
                Ok(()) => {}
                Err(crate::common::MidgeError::NotFound) => {}
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
    let name = crate::cloud_layout::compaction_file_name(
        identity.cf_id,
        identity.target_level,
        identity.generation,
        identity.ordinal,
    );
    let path = output_dir.join(&name);
    writer.finish_to_path(&path)?;
    let fs_path = crate::sst::fs::fs_relative_sst_path(output_fs, &path)?;
    let size = match output_fs.metadata(&fs_path) {
        Ok(metadata) => metadata.len,
        Err(error) => {
            if let Err(cleanup_error) = output_fs.remove_file(&fs_path) {
                tracing::warn!(file = %path.display(), %cleanup_error, "retaining unverified compaction output");
            }
            return Err(error.into());
        }
    };
    if output_size_limit.is_some_and(|limit| size > limit as u64) {
        output_fs.remove_file(&fs_path)?;
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
    policy.range_eligible && tombstone_is_obsolete(tombstone.seq, policy.snapshot_horizon)
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
        let partition_tombstone_bytes = tombstones.partition_encoded_bytes();
        let partition_size = self
            .writer()
            .estimated_size_bytes()
            .saturating_add(partition_tombstone_bytes);
        let soft_roll = soft_roll_due(
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
    ) -> MidgeResult<Option<(String, std::path::PathBuf)>> {
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
        Option<(String, std::path::PathBuf)>,
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
    ) -> MidgeResult<()> {
        self.identity.ordinal = self.identity.ordinal.checked_add(1).ok_or_else(|| {
            crate::common::MidgeError::ResourceLimit(
                "compaction partition ordinal space exhausted".to_string(),
            )
        })?;
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
    ) -> MidgeResult<Option<(String, std::path::PathBuf)>> {
        self.finish_current(tombstones, None, abort_check)
    }
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
    output_sink: Option<&super::CompactionOutputSink<'_>>,
    output_size_limit: Option<usize>,
) -> MidgeResult<Vec<String>> {
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
            if let Some((name, path)) = finished {
                cleanup.record(path.clone());
                if let Some(sink) = output_sink {
                    sink(&name, &path, budget)?;
                }
                output_names.push(name);
            }
            ensure_compaction_not_aborted(abort_check)?;
            roller.start_next_partition(
                event_key.clone(),
                boundary_reservation,
                &mut tombstones,
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
    if let Some((name, path)) = roller.finish(&tombstones, abort_check)? {
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

    struct MetadataFailFs {
        inner: crate::io::MockFs,
    }

    impl crate::io::Fs for MetadataFailFs {
        fn open(
            &self,
            path: &crate::io::FsPath,
            opts: crate::io::OpenOptions,
        ) -> crate::io::FsResult<Box<dyn crate::io::File + '_>> {
            self.inner.open(path, opts)
        }
        fn remove_file(&self, path: &crate::io::FsPath) -> crate::io::FsResult<()> {
            self.inner.remove_file(path)
        }
        fn exists(&self, path: &crate::io::FsPath) -> crate::io::FsResult<bool> {
            self.inner.exists(path)
        }
        fn metadata(
            &self,
            _path: &crate::io::FsPath,
        ) -> crate::io::FsResult<crate::io::traits::Metadata> {
            Err(crate::io::FsError::Io(
                "injected output metadata failure".into(),
            ))
        }
        fn create_dir_all(&self, path: &crate::io::FsPath) -> crate::io::FsResult<()> {
            self.inner.create_dir_all(path)
        }
        fn list_dir(
            &self,
            path: &crate::io::FsPath,
        ) -> crate::io::FsResult<Vec<crate::io::traits::DirEntry>> {
            self.inner.list_dir(path)
        }
        fn remove_dir_all(&self, path: &crate::io::FsPath) -> crate::io::FsResult<()> {
            self.inner.remove_dir_all(path)
        }
        fn sync_dir(
            &self,
            path: &crate::io::FsPath,
            durability: crate::io::Durability,
        ) -> crate::io::FsResult<()> {
            self.inner.sync_dir(path, durability)
        }
        fn rename_atomic(
            &self,
            from: &crate::io::FsPath,
            to: &crate::io::FsPath,
        ) -> crate::io::FsResult<()> {
            self.inner.rename_atomic(from, to)
        }
    }

    #[test]
    fn should_fail_partition_when_output_metadata_cannot_be_read() -> MidgeResult<()> {
        // Arrange
        let mock = crate::io::MockFs::new();
        let factory = crate::sst::FsSstFactoryIo::new(
            std::sync::Arc::new(MetadataFailFs {
                inner: mock.clone(),
            }),
            4096,
        );
        let mut writer = factory.create()?;
        writer.add_with_meta(
            b"key",
            Some(b"value"),
            1,
            crate::types::EntryType::Put,
            None,
        )?;
        let fs = factory.output_fs();

        // Act
        let result = finish_partition(
            writer,
            1,
            &[],
            None,
            None,
            PartitionIdentity {
                cf_id: 0,
                target_level: 1,
                generation: 1,
                ordinal: 0,
            },
            std::path::Path::new("output"),
            None,
            Some(usize::MAX),
            &fs,
        );

        // Assert
        assert!(matches!(result, Err(crate::common::MidgeError::Io(_))));
        let name = crate::cloud_layout::compaction_file_name(0, 1, 1, 0);
        assert!(mock.get_file(&format!("output/{name}")).is_none());
        Ok(())
    }

    #[test]
    fn should_remove_partial_output_through_injected_fs_when_compaction_aborts() -> MidgeResult<()>
    {
        // Arrange
        let mock = std::sync::Arc::new(crate::io::MockFs::new());
        let factory = crate::sst::FsSstFactoryIo::new(mock.clone(), 4096);
        let mut writer = factory.create()?;
        writer.add_with_meta(
            b"key",
            Some(b"value"),
            1,
            crate::types::EntryType::Put,
            None,
        )?;
        writer.finish_to_path(std::path::Path::new("input.sst"))?;
        let mut plan = crate::compaction::CompactionPlan::new(0, 0, 1).with_output_seq(2);
        plan.add_test_source("input.sst");
        let sink = |_name: &str,
                    _path: &std::path::Path,
                    _budget: &crate::common::resource_budget::ResourceBudget| {
            Err(crate::common::MidgeError::Aborted(
                "injected after first output".into(),
            ))
        };

        // Act
        let result = crate::compaction::execute_compaction_with_output_sink(
            &plan,
            &factory,
            std::path::Path::new("output"),
            None,
            Some(&sink),
            None,
        );

        // Assert
        assert!(matches!(result, Err(crate::common::MidgeError::Aborted(_))));
        assert!(mock.get_file("input.sst").is_some());
        let output = crate::cloud_layout::compaction_file_name(0, 1, 2, 0);
        assert!(mock.get_file(&format!("output/{output}")).is_none());
        Ok(())
    }

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
        use crate::sst::traits::SstStateReader;

        // Arrange
        struct FakeReader;

        impl SstStateReader for FakeReader {
            crate::sst::traits::test_reader_required_methods!();

            fn range_tombstone_memory_usage(&self) -> usize {
                self.range_tombstones()
                    .iter()
                    .map(|range| {
                        range.start.capacity()
                            + range.end.capacity()
                            + std::mem::size_of::<RangeTombstone>()
                    })
                    .sum()
            }

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
                        KeyState::Value(
                            bytes::Bytes::from_static(b"v1"),
                            42,
                            Some(900),
                            crate::types::EntryType::Put,
                        ),
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

    fn mk_tombstone(index: usize, seq: u64) -> RangeTombstone {
        RangeTombstone::new(
            format!("key{index:06}a").into_bytes(),
            format!("key{index:06}b").into_bytes(),
            seq,
        )
    }

    #[test]
    fn should_keep_compaction_work_linear_when_partition_holds_many_range_tombstones(
    ) -> MidgeResult<()> {
        use std::sync::atomic::{AtomicUsize, Ordering};

        // Arrange
        struct CountingWriter {
            bounds: AtomicUsize,
        }

        impl crate::sst::traits::DynSstWriter for CountingWriter {
            fn estimated_size_bytes(&self) -> usize {
                0
            }

            fn finish_to_path(self: Box<Self>, _path: &std::path::Path) -> MidgeResult<()> {
                Err(crate::common::MidgeError::NotSupported(
                    "counting writer has no filesystem".into(),
                ))
            }

            fn encoded_size_upper_bound(&self) -> Option<usize> {
                Some(0)
            }

            fn encoded_size_upper_bound_after_sorted_entry(
                &self,
                _key: &[u8],
                _value: Option<&[u8]>,
            ) -> Option<usize> {
                None
            }

            fn additional_range_tombstone_size_upper_bound(
                &self,
                start: &[u8],
                end: &[u8],
            ) -> Option<usize> {
                self.bounds.fetch_add(1, Ordering::SeqCst);
                Some(start.len().saturating_add(end.len()))
            }

            fn add_with_meta(
                &mut self,
                _key: &[u8],
                _value: Option<&[u8]>,
                _seq: u64,
                _op_type: EntryType,
                _expiration: Option<u64>,
            ) -> MidgeResult<()> {
                Ok(())
            }

            fn add_sorted_with_meta(
                &mut self,
                _key: &[u8],
                _value: Option<&[u8]>,
                _seq: u64,
                _op_type: EntryType,
                _expiration: Option<u64>,
            ) -> MidgeResult<()> {
                Ok(())
            }

            fn add_range_tombstone(
                &mut self,
                _start: &[u8],
                _end: &[u8],
                _seq: u64,
            ) -> MidgeResult<()> {
                Ok(())
            }

            fn finish_bytes(self: Box<Self>) -> MidgeResult<Vec<u8>> {
                Ok(Vec::new())
            }
        }

        let budget = crate::common::resource_budget::ResourceBudget::new(16 * 1024 * 1024);
        let writer = CountingWriter {
            bounds: AtomicUsize::new(0),
        };
        let tombstone_count = 2_000usize;
        let key_groups = 2_000usize;

        // Act
        let mut partition = PartitionTombstones::new();
        for index in 0..tombstone_count {
            let seq = u64::try_from(index).expect("sequence fits in u64") + 1;
            partition.insert(
                &mk_tombstone(index, seq),
                &budget,
                "partition range tombstone",
                Some(PendingTombstoneBound {
                    writer: &writer,
                    lower_bound: None,
                }),
            )?;
        }
        let bounds_after_inserts = writer.bounds.load(Ordering::SeqCst);
        for _ in 0..key_groups {
            let _bound = prospective_partition_size(&writer, None, partition.pending_bytes())?;
        }
        let bounds_after_key_groups = writer.bounds.load(Ordering::SeqCst);

        // Assert: each tombstone is bounded once when it joins the partition,
        // and the per-key-group size checks never revisit the retained set, so
        // the work stays linear in tombstones instead of keys x tombstones.
        assert_eq!(bounds_after_inserts, tombstone_count);
        assert_eq!(bounds_after_key_groups, tombstone_count);
        assert_eq!(
            partition.pending_bytes(),
            pending_tombstone_size(&writer, partition.tombstones(), None)?
        );
        Ok(())
    }

    #[test]
    fn should_track_encoded_bytes_when_partition_tombstones_are_added_and_cleared(
    ) -> MidgeResult<()> {
        // Arrange
        let budget = crate::common::resource_budget::ResourceBudget::new(1024 * 1024);
        let mut partition = PartitionTombstones::new();
        let first = mk_tombstone(1, 7);
        let second = mk_tombstone(2, 9);

        // Act
        partition.insert(&first, &budget, "partition range tombstone", None)?;
        partition.insert(&second, &budget, "partition range tombstone", None)?;
        partition.insert(&first, &budget, "partition range tombstone", None)?;

        // Assert: the duplicate is dropped and the tracked byte bound matches
        // a full fold over the retained set.
        assert_eq!(partition.tombstones().count(), 2);
        assert!(partition.contains(&first));
        assert_eq!(
            partition.encoded_bytes(),
            encoded_tombstone_upper_bound(&first)
                .saturating_add(encoded_tombstone_upper_bound(&second))
        );
        partition.clear();
        assert!(partition.is_empty());
        assert_eq!(partition.encoded_bytes(), 0);
        assert_eq!(partition.pending_bytes(), 0);
        Ok(())
    }

    #[test]
    fn should_report_highest_sequence_cover_when_many_obsolete_tombstones_are_active(
    ) -> MidgeResult<()> {
        // Arrange
        let budget = crate::common::resource_budget::ResourceBudget::new(1024 * 1024);
        let mut active = ActiveTombstones::new();
        let live = RangeTombstone::new(b"a".to_vec(), b"z".to_vec(), 400);
        let highest = RangeTombstone::new(b"a".to_vec(), b"z".to_vec(), 300);

        // Act
        for seq in 1..=200u64 {
            active.insert(
                &RangeTombstone::new(b"a".to_vec(), b"z".to_vec(), seq),
                true,
                &budget,
            )?;
        }
        active.insert(&highest, true, &budget)?;
        active.insert(&live, false, &budget)?;
        active.insert(&live, false, &budget)?;

        // Assert
        assert_eq!(active.highest_obsolete_cover(), Some(&highest));
        assert_eq!(active.live_tombstones().collect::<Vec<_>>(), vec![&live]);
        assert!(active.contains(&live));
        active.remove(&highest);
        active.remove(&live);
        assert!(!active.contains(&live));
        assert_eq!(
            active
                .highest_obsolete_cover()
                .map(|tombstone| tombstone.seq),
            Some(200)
        );
        Ok(())
    }

    #[test]
    fn should_carry_live_ranges_after_retiring_obsolete_ranges_between_partitions(
    ) -> MidgeResult<()> {
        // Arrange
        let budget = crate::common::resource_budget::ResourceBudget::new(1024 * 1024);
        let mut tracker = RangeTombstoneTracker::new();
        let obsolete = RangeTombstone::new(b"a".to_vec(), b"m".to_vec(), 4);
        let live = RangeTombstone::new(b"b".to_vec(), b"z".to_vec(), 12);
        let policy = TombstoneGcPolicy {
            snapshot_horizon: Some(10),
            point_eligible: true,
            range_eligible: true,
        };

        // Act: the obsolete range covers GC decisions but never reaches an
        // output partition; the live range is carried across a roll until its
        // end event is observed.
        tracker.observe_starts([&obsolete, &live].into_iter(), policy, &budget, None)?;
        let first_partition = tracker.partition_tombstones().cloned().collect::<Vec<_>>();
        tracker.carry_live_tombstones(&budget, None)?;
        let carried_partition = tracker.partition_tombstones().cloned().collect::<Vec<_>>();
        let events = [
            CompactionEvent::RangeEnd(obsolete.clone()),
            CompactionEvent::RangeEnd(live.clone()),
        ];
        tracker.advance_to_key(events[..1].iter());
        let after_obsolete_end = tracker.highest_obsolete_cover().cloned();
        tracker.advance_to_key(events[1..].iter());
        tracker.carry_live_tombstones(&budget, None)?;

        // Assert
        assert_eq!(tracker.highest_obsolete_cover(), None);
        assert_eq!(after_obsolete_end, None);
        assert_eq!(first_partition, vec![live.clone()]);
        assert_eq!(carried_partition, vec![live]);
        assert!(tracker.partition_tombstones().next().is_none());
        Ok(())
    }

    #[test]
    fn should_roll_only_after_a_written_partition_reaches_its_boundary() -> MidgeResult<()> {
        // Arrange
        let directory = tempfile::tempdir()?;
        let factory = crate::sst::FsSstFactoryIo::new(
            std::sync::Arc::new(crate::io::RealFs::new(directory.path())?),
            4096,
        );
        let budget = crate::common::resource_budget::ResourceBudget::new(1024 * 1024);
        let mut roller = PartitionRoller::new(
            &factory,
            directory.path(),
            PartitionIdentity {
                cf_id: 0,
                target_level: 1,
                generation: 1,
                ordinal: 0,
            },
            1,
            &budget,
            None,
        )?;
        let tracker = RangeTombstoneTracker::new();
        let first = mk_version("a", 1, false, Some("first"), None);
        let next = mk_version("b", 2, false, Some("next"), None);
        let policy = TombstoneGcPolicy {
            snapshot_horizon: None,
            point_eligible: false,
            range_eligible: false,
        };

        // Act
        let before_write = roller.should_roll(Some(&next), std::iter::empty(), &tracker, policy)?;
        roller.write_selected(&first)?;
        let after_write = roller.should_roll(Some(&next), std::iter::empty(), &tracker, policy)?;

        // Assert: writer overhead alone cannot create an empty partition, but
        // a later key group can roll the partition once it contains a point.
        assert!(!before_write);
        assert!(after_write);
        Ok(())
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

    mod key_group_reduction {
        use super::*;

        fn version_event(key: &str, seq: u64, is_tombstone: bool, value: &str) -> CompactionEvent {
            CompactionEvent::Version(mk_version(
                key,
                seq,
                is_tombstone,
                (!is_tombstone).then_some(value),
                None,
            ))
        }

        fn cover(start: &str, end: &str, seq: u64) -> RangeTombstone {
            RangeTombstone::new(start.as_bytes().to_vec(), end.as_bytes().to_vec(), seq)
        }

        fn policy(horizon: Option<u64>, point: bool, range: bool) -> TombstoneGcPolicy {
            TombstoneGcPolicy {
                snapshot_horizon: horizon,
                point_eligible: point,
                range_eligible: range,
            }
        }

        fn selected_seq(events: &[CompactionEvent]) -> MidgeResult<Option<u64>> {
            select_newest_version(events.iter()).map(|version| version.map(|v| v.seq))
        }

        #[test]
        fn should_select_highest_sequence_when_key_group_has_several_versions() {
            // Arrange
            let events = [
                version_event("k", 3, false, "old"),
                version_event("k", 9, false, "new"),
                version_event("k", 5, false, "mid"),
            ];

            // Act
            let selected = selected_seq(&events);

            // Assert
            assert_eq!(selected.unwrap(), Some(9));
        }

        #[test]
        fn should_keep_one_version_when_equal_sequence_versions_are_identical() {
            // Arrange
            let events = [
                version_event("k", 4, false, "same"),
                version_event("k", 4, false, "same"),
            ];

            // Act
            let selected = selected_seq(&events);

            // Assert
            assert_eq!(selected.unwrap(), Some(4));
        }

        #[test]
        fn should_reject_key_group_when_equal_sequence_versions_disagree() {
            // Arrange
            let events = [
                version_event("k", 4, false, "left"),
                version_event("k", 4, false, "right"),
            ];

            // Act
            let selected = selected_seq(&events);

            // Assert
            assert!(matches!(
                selected,
                Err(crate::common::MidgeError::Corruption(message))
                    if message.contains("conflicting compaction versions")
            ));
        }

        #[test]
        fn should_reject_shadowed_equal_sequence_conflict_during_compaction() {
            // Arrange
            let events = [
                version_event("k", 10, false, "newest"),
                version_event("k", 5, false, "left"),
                version_event("k", 5, false, "right"),
            ];

            // Act
            let selected = selected_seq(&events);

            // Assert
            assert!(matches!(
                selected,
                Err(crate::common::MidgeError::Corruption(_))
            ));
        }

        #[test]
        fn should_keep_newest_version_when_shadowed_equal_sequence_copies_are_identical() {
            // Arrange
            let events = [
                version_event("k", 10, false, "newest"),
                version_event("k", 5, false, "same"),
                version_event("k", 5, false, "same"),
            ];

            // Act
            let selected = selected_seq(&events);

            // Assert
            assert_eq!(selected.unwrap(), Some(10));
        }

        #[test]
        fn should_select_newest_version_when_chained_files_yield_older_version_first() {
            // Arrange
            let events = [
                version_event("k", 3, false, "source"),
                version_event("k", 1, false, "left-target"),
                version_event("k", 5, false, "right-target"),
            ];

            // Act
            let selected = selected_seq(&events);

            // Assert
            assert_eq!(selected.unwrap(), Some(5));
        }

        #[test]
        fn should_reject_shadowed_equal_sequence_conflict_when_versions_arrive_unordered() {
            // Arrange
            let events = [
                version_event("k", 5, false, "left"),
                version_event("k", 10, false, "newest"),
                version_event("k", 5, false, "right"),
            ];

            // Act
            let selected = selected_seq(&events);

            // Assert
            assert!(matches!(
                selected,
                Err(crate::common::MidgeError::Corruption(_))
            ));
        }

        #[test]
        fn should_select_nothing_when_key_group_holds_only_range_events() {
            // Arrange
            let events = [
                CompactionEvent::RangeStart(cover("a", "m", 7)),
                CompactionEvent::RangeEnd(cover("a", "m", 7)),
            ];

            // Act
            let selected = selected_seq(&events);

            // Assert
            assert_eq!(selected.unwrap(), None);
        }

        #[test]
        fn should_drop_point_tombstone_when_it_is_at_or_below_the_horizon() {
            // Arrange
            let version = mk_version("k", 5, true, None::<&str>, None);

            // Act
            let survives = survives_tombstone_gc(
                &version,
                None,
                std::iter::empty(),
                policy(Some(5), true, true),
            );

            // Assert
            assert!(!survives);
        }

        #[test]
        fn should_keep_point_tombstone_when_a_snapshot_can_still_see_it() {
            // Arrange
            let version = mk_version("k", 6, true, None::<&str>, None);

            // Act
            let survives = survives_tombstone_gc(
                &version,
                None,
                std::iter::empty(),
                policy(Some(5), true, true),
            );

            // Assert
            assert!(survives);
        }

        #[test]
        fn should_keep_point_tombstone_when_point_gc_is_not_eligible() {
            // Arrange
            let version = mk_version("k", 1, true, None::<&str>, None);

            // Act
            let survives = survives_tombstone_gc(
                &version,
                None,
                std::iter::empty(),
                policy(Some(100), false, true),
            );

            // Assert
            assert!(survives);
        }

        #[test]
        fn should_drop_value_when_an_obsolete_active_cover_is_at_least_as_new() {
            // Arrange
            let version = mk_version("k", 5, false, Some("v"), None);
            let active = cover("a", "z", 5);

            // Act
            let survives = survives_tombstone_gc(
                &version,
                Some(&active),
                std::iter::empty(),
                policy(Some(10), true, true),
            );

            // Assert
            assert!(!survives);
        }

        #[test]
        fn should_keep_value_when_the_active_cover_is_older_than_the_value() {
            // Arrange
            let version = mk_version("k", 6, false, Some("v"), None);
            let active = cover("a", "z", 5);

            // Act
            let survives = survives_tombstone_gc(
                &version,
                Some(&active),
                std::iter::empty(),
                policy(Some(10), true, true),
            );

            // Assert
            assert!(survives);
        }

        #[test]
        fn should_keep_value_when_the_active_cover_does_not_contain_the_key() {
            // Arrange: retaining is the safe direction if the cover is misplaced.
            let version = mk_version("k", 5, false, Some("v"), None);
            let active = cover("m", "z", 9);

            // Act
            let survives = survives_tombstone_gc(
                &version,
                Some(&active),
                std::iter::empty(),
                policy(Some(10), true, true),
            );

            // Assert
            assert!(survives);
        }

        #[test]
        fn should_drop_value_when_an_obsolete_range_start_in_the_group_covers_it() {
            // Arrange
            let version = mk_version("k", 5, false, Some("v"), None);
            let starts = [cover("a", "z", 8)];

            // Act
            let survives =
                survives_tombstone_gc(&version, None, starts.iter(), policy(Some(10), true, true));

            // Assert
            assert!(!survives);
        }

        #[test]
        fn should_keep_value_when_the_covering_range_start_is_not_yet_obsolete() {
            // Arrange: sequence 11 is above the horizon, so a snapshot may need the value.
            let version = mk_version("k", 5, false, Some("v"), None);
            let starts = [cover("a", "z", 11)];

            // Act
            let survives =
                survives_tombstone_gc(&version, None, starts.iter(), policy(Some(10), true, true));

            // Assert
            assert!(survives);
        }

        #[test]
        fn should_keep_value_when_no_tombstone_covers_it() {
            // Arrange
            let version = mk_version("k", 5, false, Some("v"), None);

            // Act
            let survives = survives_tombstone_gc(
                &version,
                None,
                std::iter::empty(),
                policy(Some(10), true, true),
            );

            // Assert
            assert!(survives);
        }
    }

    mod soft_roll {
        use super::*;

        struct Case {
            has_selected_version: bool,
            point_count: usize,
            partition_size: usize,
            tombstone_bytes: usize,
            has_tombstones: bool,
            target: usize,
        }

        fn due(case: &Case) -> bool {
            soft_roll_due(
                case.has_selected_version,
                case.point_count,
                case.partition_size,
                case.tombstone_bytes,
                case.has_tombstones,
                case.target,
            )
        }

        fn case() -> Case {
            Case {
                has_selected_version: true,
                point_count: 3,
                partition_size: 100,
                tombstone_bytes: 0,
                has_tombstones: false,
                target: 100,
            }
        }

        #[test]
        fn should_roll_when_the_partition_reaches_the_target_size() {
            // Arrange
            let case = case();

            // Act
            let roll = due(&case);

            // Assert
            assert!(roll);
        }

        #[test]
        fn should_not_roll_when_the_partition_is_below_the_target_size() {
            // Arrange
            let case = Case {
                partition_size: 99,
                ..case()
            };

            // Act
            let roll = due(&case);

            // Assert
            assert!(!roll);
        }

        #[test]
        fn should_not_roll_at_a_group_that_drops_its_only_version() {
            // Arrange: no surviving version means this key group adds no point.
            let case = Case {
                has_selected_version: false,
                ..case()
            };

            // Act
            let roll = due(&case);

            // Assert
            assert!(!roll);
        }

        #[test]
        fn should_not_roll_an_empty_partition_on_writer_overhead_alone() {
            // Arrange: an empty writer's estimate includes fixed overhead, so a
            // large size with no points and no tombstones must not roll.
            let case = Case {
                point_count: 0,
                partition_size: 10_000,
                ..case()
            };

            // Act
            let roll = due(&case);

            // Assert
            assert!(!roll);
        }

        #[test]
        fn should_roll_a_range_only_partition_when_tombstones_reach_the_target() {
            // Arrange: no surviving points, but retained tombstones hold budget.
            let case = Case {
                has_selected_version: false,
                point_count: 0,
                partition_size: 0,
                tombstone_bytes: 100,
                has_tombstones: true,
                target: 100,
            };

            // Act
            let roll = due(&case);

            // Assert
            assert!(roll);
        }

        #[test]
        fn should_not_roll_a_range_only_partition_below_the_target() {
            // Arrange
            let case = Case {
                has_selected_version: false,
                point_count: 0,
                partition_size: 0,
                tombstone_bytes: 99,
                has_tombstones: true,
                target: 100,
            };

            // Act
            let roll = due(&case);

            // Assert
            assert!(!roll);
        }

        #[test]
        fn should_not_roll_a_range_only_partition_without_tombstones() {
            // Arrange
            let case = Case {
                has_selected_version: false,
                point_count: 0,
                partition_size: 0,
                tombstone_bytes: 100,
                has_tombstones: false,
                target: 100,
            };

            // Act
            let roll = due(&case);

            // Assert
            assert!(!roll);
        }

        #[test]
        fn should_treat_a_zero_target_as_one_byte() {
            // Arrange
            let case = Case {
                partition_size: 1,
                target: 0,
                ..case()
            };

            // Act
            let roll = due(&case);

            // Assert
            assert!(roll);
        }
    }
}
