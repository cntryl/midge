use super::format::{previous_sparse_offset, read_op_primary_key_frame, RunFile, RunHeader};
use super::range::{read_ordered_range, RangeEntry, RangeHeader, RangeOrder};
use super::{lookup_run_points, IntentLookup, SpillRun, TransactionWriteSet, RUN_HEADER_LEN};
use crate::common::{MidgeError, MidgeResult};
use crate::runtime::range_cover::RangeCoverSweep;
use bytes::Bytes;
use std::sync::Arc;

enum ReverseChunkSource {
    Materialized {
        chunks: Vec<(u64, u64)>,
        next: usize,
        pool: Arc<super::TransactionMemoryPool>,
        charge: usize,
    },
    Streaming {
        header: RunHeader,
        next_end: u64,
    },
}

enum RunKeyDirection {
    Forward,
    Reverse {
        chunks: ReverseChunkSource,
        keys: std::vec::IntoIter<Bytes>,
    },
}

pub(super) struct RunKeyCursor {
    path: std::path::PathBuf,
    cursor: u64,
    data_end: u64,
    start: Option<Vec<u8>>,
    end: Option<Vec<u8>>,
    direction: RunKeyDirection,
    previous_key: Option<Bytes>,
    exhausted: bool,
}

impl RunKeyCursor {
    pub(super) fn new(
        run: &SpillRun,
        start: Option<&[u8]>,
        end: Option<&[u8]>,
        reverse: bool,
    ) -> MidgeResult<Self> {
        // The reader cache supplies the header and any admitted sparse index.
        // Reverse scans separately reserve their chunk table, then stream the
        // sparse frames when the transaction pool refuses that reservation.
        let header = run.header()?;
        let (cursor, direction) = if reverse {
            let charge = header
                .sparse_count
                .checked_mul(size_of::<(u64, u64)>())
                .ok_or_else(|| {
                    MidgeError::Corruption(
                        "transaction spill sparse chunk reservation overflow".to_string(),
                    )
                })?;
            let chunks = if run.pool.try_reserve(charge) {
                match run.sparse_chunks() {
                    Ok(chunks) => ReverseChunkSource::Materialized {
                        next: chunks.len(),
                        chunks,
                        pool: Arc::clone(&run.pool),
                        charge,
                    },
                    Err(error) => {
                        run.pool.release(charge);
                        return Err(error);
                    }
                }
            } else {
                ReverseChunkSource::Streaming {
                    header,
                    next_end: header.ordinal_table_offset,
                }
            };
            (
                RUN_HEADER_LEN as u64,
                RunKeyDirection::Reverse {
                    chunks,
                    keys: Vec::new().into_iter(),
                },
            )
        } else {
            (run.sparse_start(start)?, RunKeyDirection::Forward)
        };

        Ok(Self {
            path: run.path.clone(),
            cursor,
            data_end: header.ordinal_table_offset,
            start: start.map(<[u8]>::to_vec),
            end: end.map(<[u8]>::to_vec),
            direction,
            previous_key: None,
            exhausted: header.record_count == 0,
        })
    }

    fn key_in_bounds(&self, key: &[u8]) -> bool {
        self.start.as_deref().is_none_or(|start| key >= start)
            && self.end.as_deref().is_none_or(|end| key < end)
    }

    fn next_forward_key(&mut self) -> MidgeResult<Option<Bytes>> {
        let mut file = RunFile::open(&self.path)?;
        while self.cursor < self.data_end {
            file.seek_to(self.cursor)?;
            let (key, next_cursor) = read_op_primary_key_frame(&mut file)?;
            if next_cursor > self.data_end || next_cursor <= self.cursor {
                return Err(MidgeError::Corruption(
                    "transaction spill data frame exceeds its data section".to_string(),
                ));
            }
            self.cursor = next_cursor;
            if self.end.as_deref().is_some_and(|end| key.as_ref() >= end) {
                self.exhausted = true;
                return Ok(None);
            }
            if !self.key_in_bounds(&key)
                || self
                    .previous_key
                    .as_ref()
                    .is_some_and(|previous| previous == &key)
            {
                continue;
            }
            self.previous_key = Some(key.clone());
            return Ok(Some(key));
        }
        self.exhausted = true;
        Ok(None)
    }

    fn load_reverse_chunk(&mut self) -> MidgeResult<bool> {
        let mut file = RunFile::open(&self.path)?;
        let (chunk_start, chunk_end) = {
            let RunKeyDirection::Reverse { chunks, .. } = &mut self.direction else {
                return Ok(false);
            };
            match chunks {
                ReverseChunkSource::Materialized { chunks, next, .. } => {
                    if *next == 0 {
                        self.exhausted = true;
                        return Ok(false);
                    }
                    *next -= 1;
                    chunks[*next]
                }
                ReverseChunkSource::Streaming { header, next_end } => {
                    let Some(chunk_start) = previous_sparse_offset(&mut file, header, *next_end)?
                    else {
                        self.exhausted = true;
                        return Ok(false);
                    };
                    let chunk_end = *next_end;
                    *next_end = chunk_start;
                    (chunk_start, chunk_end)
                }
            }
        };
        let mut cursor = chunk_start;
        let mut chunk_keys: Vec<Bytes> = Vec::new();
        while cursor < chunk_end {
            file.seek_to(cursor)?;
            let (key, next_cursor) = read_op_primary_key_frame(&mut file)?;
            if next_cursor > chunk_end || next_cursor <= cursor {
                return Err(MidgeError::Corruption(
                    "transaction spill sparse chunk does not align to operation frames".to_string(),
                ));
            }
            cursor = next_cursor;
            if self.key_in_bounds(&key) {
                chunk_keys.push(key);
            }
        }
        chunk_keys.dedup();
        chunk_keys.reverse();
        if let RunKeyDirection::Reverse { keys, .. } = &mut self.direction {
            *keys = chunk_keys.into_iter();
        }
        Ok(true)
    }

    fn next_reverse_key(&mut self) -> MidgeResult<Option<Bytes>> {
        loop {
            let key = match &mut self.direction {
                RunKeyDirection::Reverse { keys, .. } => keys.next(),
                RunKeyDirection::Forward => None,
            };
            if let Some(key) = key {
                if self
                    .previous_key
                    .as_ref()
                    .is_some_and(|previous| previous == &key)
                {
                    continue;
                }
                self.previous_key = Some(key.clone());
                return Ok(Some(key));
            }
            if !self.load_reverse_chunk()? {
                return Ok(None);
            }
        }
    }
}

impl Drop for RunKeyCursor {
    fn drop(&mut self) {
        if let RunKeyDirection::Reverse {
            chunks: ReverseChunkSource::Materialized { pool, charge, .. },
            ..
        } = &mut self.direction
        {
            pool.release(*charge);
            *charge = 0;
        }
    }
}

impl std::iter::Iterator for RunKeyCursor {
    type Item = MidgeResult<Bytes>;

    fn next(&mut self) -> Option<Self::Item> {
        if self.exhausted {
            return None;
        }
        let result = match self.direction {
            RunKeyDirection::Forward => self.next_forward_key(),
            RunKeyDirection::Reverse { .. } => self.next_reverse_key(),
        };
        match result {
            Ok(Some(key)) => Some(Ok(key)),
            Ok(None) => None,
            Err(error) => {
                self.exhausted = true;
                Some(Err(error))
            }
        }
    }
}

/// Newest point (put or delete) intent for a key, with its ordinal.
pub(crate) type IntentPoint = (u64, IntentLookup);

/// One distinct intent key with its newest point intent. Range deletes are
/// resolved separately by [`IntentKeyScan::resolve`], because they also hide
/// keys that only the snapshot holds.
pub(crate) struct IntentEntry {
    pub(crate) key: Bytes,
    pub(crate) point: Option<IntentPoint>,
}

/// Heap entry ordered so the scan's next key pops first.
struct HeadEntry {
    key: Bytes,
    source: usize,
    reverse: bool,
}

impl PartialEq for HeadEntry {
    fn eq(&self, other: &Self) -> bool {
        self.cmp(other) == std::cmp::Ordering::Equal
    }
}
impl Eq for HeadEntry {}
impl PartialOrd for HeadEntry {
    fn partial_cmp(&self, other: &Self) -> Option<std::cmp::Ordering> {
        Some(self.cmp(other))
    }
}
impl Ord for HeadEntry {
    fn cmp(&self, other: &Self) -> std::cmp::Ordering {
        let order = self
            .key
            .cmp(&other.key)
            .then_with(|| self.source.cmp(&other.source));
        if self.reverse {
            order
        } else {
            order.reverse()
        }
    }
}

/// Streams one run's range deletes in the order a scan reaches them: by
/// ascending start for forward scans, by descending end for reverse scans.
/// It holds a position and nothing else, so it costs no pool memory.
struct RunRangeCursor {
    path: std::path::PathBuf,
    header: RangeHeader,
    order: RangeOrder,
    /// Forward cursors count up from `0`; reverse cursors count down from
    /// `node_count`.
    position: usize,
    scan_start: Option<Vec<u8>>,
    scan_end: Option<Vec<u8>>,
}

impl RunRangeCursor {
    fn new(
        run: &SpillRun,
        start: Option<&[u8]>,
        end: Option<&[u8]>,
        reverse: bool,
    ) -> MidgeResult<Self> {
        let header = run.with_reader(|reader| Ok(reader.range_header()))?;
        Ok(Self {
            path: run.range_path.clone(),
            header,
            order: if reverse {
                RangeOrder::End
            } else {
                RangeOrder::Start
            },
            position: if reverse { header.node_count() } else { 0 },
            scan_start: start.map(<[u8]>::to_vec),
            scan_end: end.map(<[u8]>::to_vec),
        })
    }

    /// Next range overlapping the scan bounds. Ranges that start at or after
    /// the scan end (forward) or end at or before the scan start (reverse)
    /// end the stream: every later range is further out still.
    fn next(&mut self) -> MidgeResult<Option<RangeEntry>> {
        let mut file = None;
        loop {
            let position = if self.order == RangeOrder::End {
                if self.position == 0 {
                    return Ok(None);
                }
                self.position - 1
            } else {
                if self.position >= self.header.node_count() {
                    return Ok(None);
                }
                self.position
            };
            if file.is_none() {
                file = Some(RunFile::open(&self.path)?);
            }
            let file = file.as_mut().expect("opened above");
            let entry = read_ordered_range(file, &self.header, self.order, position)?;
            if self.order == RangeOrder::End {
                self.position -= 1;
            } else {
                self.position += 1;
            }
            let (past_scan, outside) = if self.order == RangeOrder::End {
                (
                    self.scan_start
                        .as_deref()
                        .is_some_and(|start| entry.end.as_ref() <= start),
                    self.scan_end
                        .as_deref()
                        .is_some_and(|end| entry.start.as_ref() >= end),
                )
            } else {
                (
                    self.scan_end
                        .as_deref()
                        .is_some_and(|end| entry.start.as_ref() >= end),
                    self.scan_start
                        .as_deref()
                        .is_some_and(|start| entry.end.as_ref() <= start),
                )
            };
            if past_scan {
                self.position = if self.order == RangeOrder::End {
                    0
                } else {
                    self.header.node_count()
                };
                return Ok(None);
            }
            if !outside {
                return Ok(Some(entry));
            }
        }
    }
}

/// A run's next unreached range, ordered so the scan's next range pops first.
struct RangeHead {
    activation: Bytes,
    entry: RangeEntry,
    cursor: usize,
    reverse: bool,
}

impl PartialEq for RangeHead {
    fn eq(&self, other: &Self) -> bool {
        self.cmp(other) == std::cmp::Ordering::Equal
    }
}
impl Eq for RangeHead {}
impl PartialOrd for RangeHead {
    fn partial_cmp(&self, other: &Self) -> Option<std::cmp::Ordering> {
        Some(self.cmp(other))
    }
}
impl Ord for RangeHead {
    fn cmp(&self, other: &Self) -> std::cmp::Ordering {
        let order = self.activation.cmp(&other.activation);
        if self.reverse {
            order
        } else {
            order.reverse()
        }
    }
}

/// K-way unique-key merge over resident intents and private spill runs.
///
/// Source `0` is the resident index; source `n + 1` is spill run `n`. A heap
/// of source heads makes each key cost `O(log R)` plus one point probe per
/// run that actually holds the key, instead of comparing every run per key.
/// Range deletes stream lazily from every run into a sweep over the ranges
/// covering the current key, so they cost `O((K + D) log D)` rather than one
/// index stab per run per key.
pub(crate) struct IntentKeyScan {
    reverse: bool,
    resident: std::vec::IntoIter<(Bytes, IntentPoint)>,
    resident_head: Option<IntentPoint>,
    cursors: Vec<RunKeyCursor>,
    heap: std::collections::BinaryHeap<HeadEntry>,
    consumed: Vec<usize>,
    primed: bool,
    range_cursors: Vec<RunRangeCursor>,
    range_heap: std::collections::BinaryHeap<RangeHead>,
    covering: RangeCoverSweep,
    exhausted: bool,
}

impl IntentKeyScan {
    pub(super) fn new(
        write_set: &TransactionWriteSet,
        start: Option<&[u8]>,
        end: Option<&[u8]>,
        reverse: bool,
    ) -> MidgeResult<Self> {
        let mut resident = write_set.resident_index.points_in(start, end);
        if reverse {
            resident.reverse();
        }
        let mut cursors = Vec::with_capacity(write_set.runs.len());
        let mut range_cursors = Vec::new();
        for run in &write_set.runs {
            cursors.push(RunKeyCursor::new(run, start, end, reverse)?);
            if run.range_count != 0 {
                range_cursors.push(RunRangeCursor::new(run, start, end, reverse)?);
            }
        }
        let mut scan = Self {
            reverse,
            resident: resident.into_iter(),
            resident_head: None,
            cursors,
            heap: std::collections::BinaryHeap::new(),
            consumed: Vec::new(),
            primed: false,
            range_cursors,
            range_heap: std::collections::BinaryHeap::new(),
            covering: RangeCoverSweep::new(reverse),
            exhausted: false,
        };
        for cursor in 0..scan.range_cursors.len() {
            scan.advance_range_cursor(write_set, cursor)?;
        }
        Ok(scan)
    }

    /// Pulls the next key of `source` into the heap.
    fn advance(&mut self, write_set: &TransactionWriteSet, source: usize) -> MidgeResult<()> {
        write_set.record_work(1);
        let key = if source == 0 {
            self.resident.next().map(|(key, point)| {
                self.resident_head = Some(point);
                key
            })
        } else {
            self.cursors[source - 1].next().transpose()?
        };
        if let Some(key) = key {
            self.heap.push(HeadEntry {
                key,
                source,
                reverse: self.reverse,
            });
        }
        Ok(())
    }

    /// Pulls the next unreached range of one run into the range heap.
    fn advance_range_cursor(
        &mut self,
        write_set: &TransactionWriteSet,
        cursor: usize,
    ) -> MidgeResult<()> {
        write_set.record_work(1);
        if let Some(entry) = self.range_cursors[cursor].next()? {
            let activation = if self.reverse {
                entry.end.clone()
            } else {
                entry.start.clone()
            };
            self.range_heap.push(RangeHead {
                activation,
                entry,
                cursor,
                reverse: self.reverse,
            });
        }
        Ok(())
    }

    fn next_entry_inner(
        &mut self,
        write_set: &TransactionWriteSet,
    ) -> MidgeResult<Option<IntentEntry>> {
        if self.primed {
            for source in std::mem::take(&mut self.consumed) {
                self.advance(write_set, source)?;
            }
        } else {
            self.primed = true;
            for source in 0..=self.cursors.len() {
                self.advance(write_set, source)?;
            }
        }

        let Some(first) = self.heap.pop() else {
            return Ok(None);
        };
        let key = first.key;
        let mut sources = vec![first.source];
        while self.heap.peek().is_some_and(|head| head.key == key) {
            if let Some(head) = self.heap.pop() {
                sources.push(head.source);
            }
        }

        let mut point = None;
        for source in sources {
            if source == 0 {
                point = self.resident_head.take();
            } else {
                write_set.record_work(1);
                lookup_run_points(&write_set.runs[source - 1], &key, u64::MAX, &mut point)?;
            }
            self.consumed.push(source);
        }
        Ok(Some(IntentEntry { key, point }))
    }

    /// Next distinct intent key in scan order.
    pub(crate) fn next_entry(
        &mut self,
        write_set: &TransactionWriteSet,
    ) -> Option<MidgeResult<IntentEntry>> {
        if self.exhausted {
            return None;
        }
        match self.next_entry_inner(write_set) {
            Ok(Some(entry)) => Some(Ok(entry)),
            Ok(None) => {
                self.exhausted = true;
                None
            }
            Err(error) => {
                self.exhausted = true;
                Some(Err(error))
            }
        }
    }

    /// Moves every spilled range the scan has reached into the sweep.
    fn reach_ranges(&mut self, write_set: &TransactionWriteSet, key: &[u8]) -> MidgeResult<()> {
        while self.range_heap.peek().is_some_and(|head| {
            if self.reverse {
                head.activation.as_ref() > key
            } else {
                head.activation.as_ref() <= key
            }
        }) {
            let Some(head) = self.range_heap.pop() else {
                break;
            };
            self.covering.insert(
                head.entry.start.to_vec(),
                head.entry.end.to_vec(),
                head.entry.ordinal,
            );
            self.advance_range_cursor(write_set, head.cursor)?;
        }
        Ok(())
    }

    /// Newest intent for `key` given its newest point intent: a covering range
    /// delete with a higher ordinal turns it into a deletion. `key` may come
    /// from the snapshot alone, in which case `point` is `None`. Keys must be
    /// resolved in scan order.
    pub(crate) fn resolve(
        &mut self,
        write_set: &TransactionWriteSet,
        key: &[u8],
        point: Option<IntentPoint>,
    ) -> MidgeResult<Option<IntentLookup>> {
        write_set.record_work(1);
        let mut latest = point;
        if let Some(ordinal) = write_set.resident_index.covering_range(key) {
            if latest
                .as_ref()
                .is_none_or(|(point_ordinal, _)| ordinal > *point_ordinal)
            {
                latest = Some((ordinal, IntentLookup::Deleted));
            }
        }
        self.reach_ranges(write_set, key)?;
        if let Some(ordinal) = self.covering.max_cover_seq(key) {
            if latest
                .as_ref()
                .is_none_or(|(point_ordinal, _)| ordinal > *point_ordinal)
            {
                latest = Some((ordinal, IntentLookup::Deleted));
            }
        }
        Ok(latest.map(|(_, lookup)| lookup))
    }
}
