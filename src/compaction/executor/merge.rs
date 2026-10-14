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
    abort_check: Option<&'a dyn Fn() -> bool>,
}

impl<'a> EventMergeIterator<'a> {
    fn new(
        mut cursors: Vec<CompactionEventCursor<'a>>,
        budget: crate::common::resource_budget::ResourceBudget,
        abort_check: Option<&'a dyn Fn() -> bool>,
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
            ensure_compaction_not_aborted(abort_check)?;
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
            abort_check,
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

        ensure_compaction_not_aborted(self.abort_check)?;
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
    same_level_repair: bool,
    budget: &crate::common::resource_budget::ResourceBudget,
    abort_check: Option<&'a dyn Fn() -> bool>,
) -> MidgeResult<CompactionStreamInputs<'a>> {
    let source_streams = if source_level == 0 || same_level_repair {
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

    if source_level == 0 || same_level_repair {
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

/// Make a non-authoritative raw run from at most one admitted fan-in batch.
/// No version selection or tombstone GC is allowed here: a later pass must
/// still see every input to validate equal-sequence conflicts and snapshots.
pub(crate) fn merge_repair_inputs_to_run(
    sst_factory: &dyn SstFactory,
    sources: &[String],
    path: &Path,
    budget: &crate::common::resource_budget::ResourceBudget,
    byte_limit: usize,
    abort_check: Option<&dyn Fn() -> bool>,
) -> MidgeResult<()> {
    let inputs =
        collect_compaction_stream_inputs(sst_factory, sources, &[], 1, true, budget, abort_check)?;
    ensure_compaction_not_aborted(abort_check)?;
    let CompactionStreamInputs {
        cursors,
        _cursor_reservation,
    } = inputs;
    let mut merged = EventMergeIterator::new(cursors, budget.clone(), abort_check)?;
    ensure_compaction_not_aborted(abort_check)?;
    let mut writer = sst_factory.create_for_compaction(budget.clone())?;
    let mut seen = 0usize;
    while let Some(event) = merged.next_event()? {
        if seen.is_multiple_of(1024) {
            ensure_compaction_not_aborted(abort_check)?;
        }
        seen = seen.saturating_add(1);
        match event {
            CompactionEvent::Version(version) => writer.add_sorted_with_meta(
                &version.key,
                version.value.as_deref(),
                version.seq,
                if version.is_tombstone {
                    EntryType::Delete
                } else {
                    EntryType::Put
                },
                version.expiration,
            )?,
            CompactionEvent::RangeStart(tombstone) => {
                writer.add_range_tombstone(&tombstone.start, &tombstone.end, tombstone.seq)?;
            }
            CompactionEvent::RangeEnd(_) => {}
        }
        if writer
            .encoded_size_upper_bound()
            .is_none_or(|bytes| bytes > byte_limit)
        {
            return Err(crate::common::MidgeError::ResourceLimit(
                "repair scratch run exceeds admitted local capacity".into(),
            ));
        }
    }
    if seen == 0 {
        return Err(crate::common::MidgeError::Corruption(
            "overlap repair input has no versions or tombstones".into(),
        ));
    }
    ensure_compaction_not_aborted(abort_check)?;
    writer.finish_to_path(path)
}
