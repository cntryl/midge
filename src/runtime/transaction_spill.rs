//! Bounded transaction intent storage and CRC-framed spill runs.
//!
//! Runs are key-sorted for lookup and carry a separate ordinal table. The
//! runtime can therefore replay a spilled transaction in caller order without
//! reconstructing its complete write set in memory.

use super::TransactionOp;
use crate::common::{MidgeError, MidgeResult};
use bytes::Bytes;
use std::fs::{self, File, OpenOptions};
use std::io::{Read, Seek, SeekFrom, Write};
use std::mem::size_of;
use std::path::{Path, PathBuf};
use std::sync::atomic::{AtomicUsize, Ordering};
use std::sync::Arc;

const RUN_MAGIC: &[u8; 8] = b"MDGTXN01";
const RUN_VERSION: u32 = 1;
const RUN_HEADER_LEN: usize = 48;
const SPARSE_INDEX_STRIDE: usize = 16;
const MAX_FRAME_BYTES: usize = crate::wal::frame::WAL_MAX_RECORD_LEN;
const RANGE_MAGIC: &[u8; 8] = b"MDGRNG01";
const RANGE_VERSION: u32 = 1;
const RANGE_HEADER_LEN: usize = 32;
const RANGE_TABLE_ENTRY_LEN: usize = 12;
const NO_RANGE_CHILD: u64 = u64::MAX;
// Covers resident Vec capacity plus temporary ordinal, sparse-key, and range
// interval-tree metadata built while freezing a run. Key/value bytes and the
// enum allocation itself are charged separately below.
const INTENT_ACCOUNTING_OVERHEAD: usize = 256;

/// One bounded pool shared by every transaction opened by an engine.
#[derive(Debug)]
pub(crate) struct TransactionMemoryPool {
    capacity: usize,
    resident: AtomicUsize,
}

impl TransactionMemoryPool {
    #[must_use]
    pub(crate) fn new(capacity: usize) -> Self {
        Self {
            capacity,
            resident: AtomicUsize::new(0),
        }
    }

    fn try_reserve(&self, bytes: usize) -> bool {
        let mut current = self.resident.load(Ordering::Acquire);
        loop {
            let Some(next) = current.checked_add(bytes) else {
                return false;
            };
            if next > self.capacity {
                return false;
            }
            match self.resident.compare_exchange_weak(
                current,
                next,
                Ordering::AcqRel,
                Ordering::Acquire,
            ) {
                Ok(_) => return true,
                Err(observed) => current = observed,
            }
        }
    }

    fn release(&self, bytes: usize) {
        if bytes == 0 {
            return;
        }
        let previous = self.resident.fetch_sub(bytes, Ordering::AcqRel);
        debug_assert!(previous >= bytes, "transaction pool accounting underflow");
    }
}

#[derive(Debug, Clone)]
struct OrdinalOp {
    ordinal: u64,
    op: TransactionOp,
}

impl OrdinalOp {
    fn primary_key(&self) -> &[u8] {
        op_primary_key(&self.op)
    }

    fn estimated_resident_bytes(&self) -> usize {
        size_of::<Self>()
            .saturating_add(op_heap_bytes(&self.op))
            .saturating_add(INTENT_ACCOUNTING_OVERHEAD)
    }
}

#[derive(Debug, Clone)]
struct SpillRun {
    path: PathBuf,
    range_path: PathBuf,
    record_count: usize,
}

#[derive(Debug)]
pub(crate) enum IntentLookup {
    Present(Bytes),
    Deleted,
}

/// Mutable transaction-local write set.
#[derive(Debug)]
pub(crate) struct TransactionWriteSet {
    pool: Arc<TransactionMemoryPool>,
    spill_dir: Option<PathBuf>,
    txn_id: u64,
    resident: Vec<OrdinalOp>,
    resident_bytes: usize,
    runs: Vec<SpillRun>,
    next_ordinal: u64,
}

impl TransactionWriteSet {
    #[must_use]
    pub(crate) fn new(
        pool: Arc<TransactionMemoryPool>,
        db_path: &Path,
        memory_mode: bool,
        txn_id: u64,
    ) -> Self {
        Self {
            pool,
            spill_dir: (!memory_mode).then(|| db_path.join("txn")),
            txn_id,
            resident: Vec::new(),
            resident_bytes: 0,
            runs: Vec::new(),
            next_ordinal: 0,
        }
    }

    #[must_use]
    pub(crate) fn is_empty(&self) -> bool {
        self.next_ordinal == 0
    }

    #[must_use]
    pub(crate) fn has_spills(&self) -> bool {
        !self.runs.is_empty()
    }

    pub(crate) fn push(&mut self, op: TransactionOp) -> MidgeResult<()> {
        let ordinal_op = OrdinalOp {
            ordinal: self.next_ordinal,
            op,
        };
        let bytes = ordinal_op.estimated_resident_bytes();
        if self.pool.try_reserve(bytes) {
            self.admit_resident(ordinal_op, bytes);
            return Ok(());
        }

        let Some(spill_dir) = self.spill_dir.clone() else {
            return Err(MidgeError::ResourceLimit(format!(
                "transaction memory pool cannot admit {bytes} additional bytes"
            )));
        };
        self.spill_resident(&spill_dir)?;
        if self.pool.try_reserve(bytes) {
            self.admit_resident(ordinal_op, bytes);
        } else {
            let mut direct = vec![ordinal_op];
            let run = write_run(
                &spill_dir,
                self.txn_id,
                self.runs.len(),
                direct.as_mut_slice(),
            )?;
            self.runs.push(run);
            self.next_ordinal = self.next_ordinal.saturating_add(1);
        }
        Ok(())
    }

    fn admit_resident(&mut self, ordinal_op: OrdinalOp, bytes: usize) {
        self.resident_bytes = self.resident_bytes.saturating_add(bytes);
        self.resident.push(ordinal_op);
        self.next_ordinal = self.next_ordinal.saturating_add(1);
    }

    fn spill_resident(&mut self, spill_dir: &Path) -> MidgeResult<()> {
        if self.resident.is_empty() {
            return Ok(());
        }
        let run = write_run(
            spill_dir,
            self.txn_id,
            self.runs.len(),
            self.resident.as_mut_slice(),
        )?;
        self.runs.push(run);
        self.resident.clear();
        self.pool.release(self.resident_bytes);
        self.resident_bytes = 0;
        Ok(())
    }

    pub(crate) fn latest_for_key(&self, key: &[u8]) -> MidgeResult<Option<IntentLookup>> {
        let mut latest = None;
        for ordinal_op in &self.resident {
            consider_lookup(ordinal_op, key, &mut latest);
        }
        for run in &self.runs {
            lookup_run_key(run, key, u64::MAX, &mut latest)?;
        }
        Ok(latest.map(|(_, lookup)| lookup))
    }

    pub(crate) fn key_scan(
        &self,
        start: Option<&[u8]>,
        end: Option<&[u8]>,
        reverse: bool,
    ) -> MidgeResult<IntentKeyScan> {
        IntentKeyScan::new(self, start, end, reverse)
    }

    pub(crate) fn take_in_memory_ops(&mut self) -> Vec<TransactionOp> {
        debug_assert!(self.runs.is_empty());
        self.resident.sort_unstable_by_key(|entry| entry.ordinal);
        let ops = std::mem::take(&mut self.resident)
            .into_iter()
            .map(|entry| entry.op)
            .collect();
        self.pool.release(self.resident_bytes);
        self.resident_bytes = 0;
        self.next_ordinal = 0;
        ops
    }

    pub(crate) fn take_source(&mut self) -> TransactionOpSource {
        let source = TransactionOpSource {
            runs: std::mem::take(&mut self.runs),
            resident: std::mem::take(&mut self.resident),
            resident_pool: Arc::clone(&self.pool),
            resident_bytes: std::mem::take(&mut self.resident_bytes),
            op_count: usize::try_from(self.next_ordinal).unwrap_or(usize::MAX),
        };
        self.next_ordinal = 0;
        source
    }

    fn cleanup(&mut self) {
        self.pool.release(self.resident_bytes);
        self.resident_bytes = 0;
        self.resident.clear();
        for run in self.runs.drain(..) {
            remove_run(&run.path);
            remove_run(&run.range_path);
        }
        self.next_ordinal = 0;
    }
}

impl Drop for TransactionWriteSet {
    fn drop(&mut self) {
        self.cleanup();
    }
}

enum RunKeyDirection {
    Forward,
    Reverse {
        chunks: Vec<(u64, u64)>,
        next_chunk: usize,
        keys: std::vec::IntoIter<Bytes>,
    },
}

struct RunKeyCursor {
    file: File,
    cursor: u64,
    data_end: u64,
    start: Option<Vec<u8>>,
    end: Option<Vec<u8>>,
    direction: RunKeyDirection,
    previous_key: Option<Bytes>,
    exhausted: bool,
}

impl RunKeyCursor {
    fn new(
        run: &SpillRun,
        start: Option<&[u8]>,
        end: Option<&[u8]>,
        reverse: bool,
    ) -> MidgeResult<Self> {
        let mut file = File::open(&run.path)?;
        let header = read_header(&mut file)?;
        if header.record_count != run.record_count {
            return Err(MidgeError::Corruption(
                "transaction spill record count changed".to_string(),
            ));
        }

        let (cursor, direction) = if reverse {
            let starts = read_sparse_offsets(&mut file, &header)?;
            let chunks = starts
                .iter()
                .enumerate()
                .map(|(index, chunk_start)| {
                    let chunk_end = starts
                        .get(index + 1)
                        .copied()
                        .unwrap_or(header.ordinal_table_offset);
                    (*chunk_start, chunk_end)
                })
                .collect::<Vec<_>>();
            let next_chunk = chunks.len();
            (
                RUN_HEADER_LEN as u64,
                RunKeyDirection::Reverse {
                    chunks,
                    next_chunk,
                    keys: Vec::new().into_iter(),
                },
            )
        } else {
            (
                sparse_start_for_key(&mut file, &header, start)?,
                RunKeyDirection::Forward,
            )
        };

        Ok(Self {
            file,
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
        while self.cursor < self.data_end {
            self.file.seek(SeekFrom::Start(self.cursor))?;
            let (key, next_cursor) = read_op_primary_key_frame(&mut self.file)?;
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
        let (chunk_start, chunk_end) = {
            let RunKeyDirection::Reverse {
                chunks, next_chunk, ..
            } = &mut self.direction
            else {
                return Ok(false);
            };
            if *next_chunk == 0 {
                self.exhausted = true;
                return Ok(false);
            }
            *next_chunk -= 1;
            chunks[*next_chunk]
        };
        let mut cursor = chunk_start;
        let mut chunk_keys = Vec::new();
        while cursor < chunk_end {
            self.file.seek(SeekFrom::Start(cursor))?;
            let (key, next_cursor) = read_op_primary_key_frame(&mut self.file)?;
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

enum IntentKeyIterator {
    Resident(std::vec::IntoIter<Bytes>),
    Run(RunKeyCursor),
}

impl IntentKeyIterator {
    fn next(&mut self) -> Option<MidgeResult<Bytes>> {
        match self {
            Self::Resident(keys) => keys.next().map(Ok),
            Self::Run(keys) => keys.next(),
        }
    }
}

struct IntentKeySource {
    iterator: IntentKeyIterator,
    head: Option<MidgeResult<Bytes>>,
    primed: bool,
    needs_advance: bool,
}

impl IntentKeySource {
    fn new(iterator: IntentKeyIterator) -> Self {
        Self {
            iterator,
            head: None,
            primed: false,
            needs_advance: false,
        }
    }

    fn prime(&mut self) {
        if !self.primed {
            self.head = self.iterator.next();
            self.primed = true;
        }
    }

    fn advance_if_needed(&mut self) {
        if self.needs_advance {
            self.head = self.iterator.next();
            self.needs_advance = false;
        }
    }

    fn consume(&mut self) {
        self.head = None;
        self.needs_advance = true;
    }
}

/// K-way unique-key merge over resident intents and private spill runs.
pub(crate) struct IntentKeyScan {
    reverse: bool,
    sources: Vec<IntentKeySource>,
    exhausted: bool,
}

impl IntentKeyScan {
    fn new(
        write_set: &TransactionWriteSet,
        start: Option<&[u8]>,
        end: Option<&[u8]>,
        reverse: bool,
    ) -> MidgeResult<Self> {
        let mut resident = write_set
            .resident
            .iter()
            .map(|entry| op_primary_key_bytes(&entry.op))
            .filter(|key| {
                start.is_none_or(|start| key.as_ref() >= start)
                    && end.is_none_or(|end| key.as_ref() < end)
            })
            .collect::<Vec<_>>();
        resident.sort_unstable();
        resident.dedup();
        if reverse {
            resident.reverse();
        }

        let mut sources = Vec::with_capacity(write_set.runs.len() + 1);
        sources.push(IntentKeySource::new(IntentKeyIterator::Resident(
            resident.into_iter(),
        )));
        for run in &write_set.runs {
            sources.push(IntentKeySource::new(IntentKeyIterator::Run(
                RunKeyCursor::new(run, start, end, reverse)?,
            )));
        }
        Ok(Self {
            reverse,
            sources,
            exhausted: false,
        })
    }

    fn next_key(&mut self) -> MidgeResult<Option<Bytes>> {
        for source in &mut self.sources {
            source.prime();
            source.advance_if_needed();
        }
        if let Some(error) = self.sources.iter_mut().find_map(|source| {
            if source.head.as_ref().is_some_and(Result::is_err) {
                source.head.take().and_then(Result::err)
            } else {
                None
            }
        }) {
            return Err(error);
        }

        let Some(selected) = self
            .sources
            .iter()
            .filter_map(|source| source.head.as_ref()?.as_ref().ok())
            .cloned()
            .reduce(|selected, candidate| {
                let candidate_wins = if self.reverse {
                    candidate > selected
                } else {
                    candidate < selected
                };
                if candidate_wins {
                    candidate
                } else {
                    selected
                }
            })
        else {
            return Ok(None);
        };

        for source in &mut self.sources {
            if source
                .head
                .as_ref()
                .and_then(|head| head.as_ref().ok())
                .is_some_and(|key| key == &selected)
            {
                source.consume();
            }
        }
        Ok(Some(selected))
    }
}

impl std::iter::Iterator for IntentKeyScan {
    type Item = MidgeResult<Bytes>;

    fn next(&mut self) -> Option<Self::Item> {
        if self.exhausted {
            return None;
        }
        match self.next_key() {
            Ok(Some(key)) => Some(Ok(key)),
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
}

/// Reopenable source transferred to the event-loop thread for commit.
#[derive(Debug)]
pub(crate) struct TransactionOpSource {
    runs: Vec<SpillRun>,
    resident: Vec<OrdinalOp>,
    resident_pool: Arc<TransactionMemoryPool>,
    resident_bytes: usize,
    op_count: usize,
}

impl TransactionOpSource {
    #[must_use]
    pub(crate) fn len(&self) -> usize {
        self.op_count
    }

    #[must_use]
    pub(crate) fn is_empty(&self) -> bool {
        self.op_count == 0
    }

    pub(crate) fn for_each<F>(&self, mut visitor: F) -> MidgeResult<()>
    where
        F: FnMut(u64, TransactionOp) -> MidgeResult<()>,
    {
        for run in &self.runs {
            for_each_run_ordinal(run, |ordinal_op| visitor(ordinal_op.ordinal, ordinal_op.op))?;
        }
        let mut resident = self.resident.iter().collect::<Vec<_>>();
        resident.sort_unstable_by_key(|entry| entry.ordinal);
        for ordinal_op in resident {
            visitor(ordinal_op.ordinal, ordinal_op.op.clone())?;
        }
        Ok(())
    }

    pub(crate) fn latest_before(
        &self,
        ordinal: u64,
        key: &[u8],
    ) -> MidgeResult<Option<IntentLookup>> {
        let mut latest = None;
        for run in &self.runs {
            lookup_run_key(run, key, ordinal, &mut latest)?;
        }
        for candidate in &self.resident {
            if candidate.ordinal < ordinal {
                consider_lookup(candidate, key, &mut latest);
            }
        }
        Ok(latest.map(|(_, lookup)| lookup))
    }

    pub(crate) fn touched_cf_ids(&self) -> MidgeResult<Vec<crate::types::ColumnFamilyId>> {
        let mut cfs = Vec::new();
        self.for_each(|_, op| {
            let cf_id = op_cf_id(&op);
            if !cfs.contains(&cf_id) {
                cfs.push(cf_id);
            }
            Ok(())
        })?;
        Ok(cfs)
    }
}

impl Drop for TransactionOpSource {
    fn drop(&mut self) {
        self.resident_pool.release(self.resident_bytes);
        self.resident_bytes = 0;
        for run in self.runs.drain(..) {
            remove_run(&run.path);
            remove_run(&run.range_path);
        }
    }
}

pub(crate) fn cleanup_orphaned_runs(db_path: &Path) -> MidgeResult<()> {
    let txn_dir = db_path.join("txn");
    match fs::remove_dir_all(txn_dir) {
        Ok(()) => Ok(()),
        Err(error) if error.kind() == std::io::ErrorKind::NotFound => Ok(()),
        Err(error) => Err(MidgeError::Io(error)),
    }
}

fn consider_lookup(ordinal_op: &OrdinalOp, key: &[u8], latest: &mut Option<(u64, IntentLookup)>) {
    let lookup = match &ordinal_op.op {
        TransactionOp::Put {
            key: candidate,
            value,
            ..
        } if candidate.as_ref() == key => Some(IntentLookup::Present(value.clone())),
        TransactionOp::Delete { key: candidate, .. } if candidate.as_ref() == key => {
            Some(IntentLookup::Deleted)
        }
        TransactionOp::DeleteRange {
            start_key, end_key, ..
        } if key >= start_key.as_ref() && key < end_key.as_ref() => Some(IntentLookup::Deleted),
        _ => None,
    };
    if let Some(lookup) = lookup {
        if latest
            .as_ref()
            .is_none_or(|(ordinal, _)| ordinal_op.ordinal > *ordinal)
        {
            *latest = Some((ordinal_op.ordinal, lookup));
        }
    }
}

pub(crate) fn op_cf_id(op: &TransactionOp) -> crate::types::ColumnFamilyId {
    match op {
        TransactionOp::Put { cf_id, .. }
        | TransactionOp::Delete { cf_id, .. }
        | TransactionOp::DeleteRange { cf_id, .. } => *cf_id,
    }
}

fn op_primary_key(op: &TransactionOp) -> &[u8] {
    match op {
        TransactionOp::Put { key, .. } | TransactionOp::Delete { key, .. } => key.as_ref(),
        TransactionOp::DeleteRange { start_key, .. } => start_key.as_ref(),
    }
}

fn op_primary_key_bytes(op: &TransactionOp) -> Bytes {
    match op {
        TransactionOp::Put { key, .. } | TransactionOp::Delete { key, .. } => key.clone(),
        TransactionOp::DeleteRange { start_key, .. } => start_key.clone(),
    }
}

fn op_heap_bytes(op: &TransactionOp) -> usize {
    match op {
        TransactionOp::Put { key, value, .. } => key.len().saturating_add(value.len()),
        TransactionOp::Delete { key, .. } => key.len(),
        TransactionOp::DeleteRange {
            start_key, end_key, ..
        } => start_key.len().saturating_add(end_key.len()),
    }
}

#[derive(Debug)]
struct RangeNodeMeta {
    op_index: usize,
    left: u64,
    right: u64,
    max_end: Bytes,
}

fn build_range_nodes(ops: &[OrdinalOp]) -> MidgeResult<Vec<RangeNodeMeta>> {
    let range_indices = ops
        .iter()
        .enumerate()
        .filter_map(|(index, op)| {
            matches!(op.op, TransactionOp::DeleteRange { .. }).then_some(index)
        })
        .collect::<Vec<_>>();
    let mut nodes = Vec::with_capacity(range_indices.len());
    let _ = build_range_subtree(&range_indices, ops, &mut nodes)?;
    Ok(nodes)
}

fn build_range_subtree(
    indices: &[usize],
    ops: &[OrdinalOp],
    nodes: &mut Vec<RangeNodeMeta>,
) -> MidgeResult<Option<u64>> {
    if indices.is_empty() {
        return Ok(None);
    }
    let middle = indices.len() / 2;
    let op_index = indices[middle];
    let (_, end_key) = range_keys(&ops[op_index].op).ok_or_else(|| {
        MidgeError::Internal("range index selected a non-range operation".to_string())
    })?;
    let node_index = nodes.len();
    nodes.push(RangeNodeMeta {
        op_index,
        left: NO_RANGE_CHILD,
        right: NO_RANGE_CHILD,
        max_end: end_key.clone(),
    });
    let left = build_range_subtree(&indices[..middle], ops, nodes)?;
    let right = build_range_subtree(&indices[middle + 1..], ops, nodes)?;
    let mut max_end = end_key.clone();
    for child in [left, right].into_iter().flatten() {
        let child_index = u64_to_usize(child)?;
        if nodes[child_index].max_end > max_end {
            max_end = nodes[child_index].max_end.clone();
        }
    }
    nodes[node_index] = RangeNodeMeta {
        op_index,
        left: left.unwrap_or(NO_RANGE_CHILD),
        right: right.unwrap_or(NO_RANGE_CHILD),
        max_end,
    };
    Ok(Some(usize_to_u64(node_index)?))
}

fn range_keys(op: &TransactionOp) -> Option<(&Bytes, &Bytes)> {
    match op {
        TransactionOp::DeleteRange {
            start_key, end_key, ..
        } => Some((start_key, end_key)),
        TransactionOp::Put { .. } | TransactionOp::Delete { .. } => None,
    }
}

fn write_range_index_file(path: &Path, ops: &[OrdinalOp]) -> MidgeResult<()> {
    let nodes = build_range_nodes(ops)?;
    let mut file = OpenOptions::new().write(true).create_new(true).open(path)?;
    file.write_all(&[0; RANGE_HEADER_LEN])?;
    let table_bytes = nodes
        .len()
        .checked_mul(RANGE_TABLE_ENTRY_LEN)
        .ok_or_else(|| {
            MidgeError::ResourceLimit("transaction range index table overflow".to_string())
        })?;
    write_zeroes(&mut file, table_bytes)?;
    let node_section_offset = file.stream_position()?;
    let mut node_offsets = Vec::with_capacity(nodes.len());
    for node in &nodes {
        node_offsets.push(file.stream_position()?);
        write_range_node_frame(&mut file, node, ops)?;
    }
    let end = file.stream_position()?;
    file.seek(SeekFrom::Start(RANGE_HEADER_LEN as u64))?;
    for offset in node_offsets {
        let bytes = offset.to_le_bytes();
        file.write_all(&bytes)?;
        file.write_all(&crc32c::crc32c(&bytes).to_le_bytes())?;
    }
    let header = encode_range_header(nodes.len(), node_section_offset)?;
    file.seek(SeekFrom::Start(0))?;
    file.write_all(&header)?;
    file.seek(SeekFrom::Start(end))?;
    file.sync_all()?;
    Ok(())
}

fn write_zeroes(file: &mut File, mut bytes: usize) -> MidgeResult<()> {
    const ZEROES: [u8; 4096] = [0; 4096];
    while bytes != 0 {
        let chunk = bytes.min(ZEROES.len());
        file.write_all(&ZEROES[..chunk])?;
        bytes -= chunk;
    }
    Ok(())
}

fn encode_range_header(
    node_count: usize,
    node_section_offset: u64,
) -> MidgeResult<[u8; RANGE_HEADER_LEN]> {
    let mut header = [0_u8; RANGE_HEADER_LEN];
    header[..8].copy_from_slice(RANGE_MAGIC);
    header[8..12].copy_from_slice(&RANGE_VERSION.to_le_bytes());
    header[12..20].copy_from_slice(&usize_to_u64(node_count)?.to_le_bytes());
    header[20..28].copy_from_slice(&node_section_offset.to_le_bytes());
    let crc = crc32c::crc32c(&header[..28]);
    header[28..32].copy_from_slice(&crc.to_le_bytes());
    Ok(header)
}

fn write_range_node_frame(
    file: &mut File,
    node: &RangeNodeMeta,
    ops: &[OrdinalOp],
) -> MidgeResult<()> {
    let ordinal_op = &ops[node.op_index];
    let (start_key, end_key) = range_keys(&ordinal_op.op).ok_or_else(|| {
        MidgeError::Internal("range node references a non-range operation".to_string())
    })?;
    let ordinal = ordinal_op.ordinal.to_le_bytes();
    let left = node.left.to_le_bytes();
    let right = node.right.to_le_bytes();
    let start_len = field_len_bytes(start_key.len())?;
    let end_len = field_len_bytes(end_key.len())?;
    let max_end_len = field_len_bytes(node.max_end.len())?;
    write_frame_parts(
        file,
        &[
            &ordinal,
            &left,
            &right,
            &start_len,
            start_key,
            &end_len,
            end_key,
            &max_end_len,
            &node.max_end,
        ],
    )
}

fn write_run(
    spill_dir: &Path,
    txn_id: u64,
    run_number: usize,
    ops: &mut [OrdinalOp],
) -> MidgeResult<SpillRun> {
    fs::create_dir_all(spill_dir)?;
    let path = spill_dir.join(format!("{txn_id:016x}-{run_number:08x}.run"));
    let temp_path = path.with_extension("run.tmp");
    let range_path = path.with_extension("ranges");
    let range_temp_path = path.with_extension("ranges.tmp");
    let result = write_run_file(&temp_path, ops)
        .and_then(|()| write_range_index_file(&range_temp_path, ops))
        .and_then(|()| {
            fs::rename(&range_temp_path, &range_path)?;
            fs::rename(&temp_path, &path)?;
            // Spill files are private pre-commit scratch. WAL commit is their
            // durability boundary, so no parent-directory fsync is required.
            Ok(())
        });
    if let Err(error) = result {
        let _ = fs::remove_file(&temp_path);
        let _ = fs::remove_file(&range_temp_path);
        let _ = fs::remove_file(&path);
        let _ = fs::remove_file(&range_path);
        return Err(error);
    }
    Ok(SpillRun {
        path,
        range_path,
        record_count: ops.len(),
    })
}

fn write_run_file(path: &Path, ops: &mut [OrdinalOp]) -> MidgeResult<()> {
    ops.sort_unstable_by(|left, right| {
        left.primary_key()
            .cmp(right.primary_key())
            .then_with(|| left.ordinal.cmp(&right.ordinal))
    });
    let mut file = OpenOptions::new().write(true).create_new(true).open(path)?;
    file.write_all(&[0; RUN_HEADER_LEN])?;

    let mut ordinal_offsets = Vec::with_capacity(ops.len());
    let mut sparse_entries = Vec::with_capacity(ops.len().div_ceil(SPARSE_INDEX_STRIDE));
    for (index, ordinal_op) in ops.iter().enumerate() {
        let offset = file.stream_position()?;
        write_op_frame(&mut file, ordinal_op)?;
        ordinal_offsets.push((ordinal_op.ordinal, offset));
        if index % SPARSE_INDEX_STRIDE == 0 {
            sparse_entries.push((op_primary_key_bytes(&ordinal_op.op), offset));
        }
    }

    let ordinal_table_offset = file.stream_position()?;
    ordinal_offsets.sort_unstable_by_key(|(ordinal, _)| *ordinal);
    for (ordinal, offset) in ordinal_offsets {
        let mut payload = Vec::with_capacity(16);
        payload.extend_from_slice(&ordinal.to_le_bytes());
        payload.extend_from_slice(&offset.to_le_bytes());
        write_frame(&mut file, &payload)?;
    }

    let sparse_index_offset = file.stream_position()?;
    for (key, offset) in &sparse_entries {
        write_sparse_frame(&mut file, key, *offset)?;
    }

    let header = encode_header(
        ops.len(),
        ordinal_table_offset,
        sparse_index_offset,
        sparse_entries.len(),
    )?;
    file.seek(SeekFrom::Start(0))?;
    file.write_all(&header)?;
    file.sync_all()?;
    Ok(())
}

fn encode_header(
    record_count: usize,
    ordinal_table_offset: u64,
    sparse_index_offset: u64,
    sparse_count: usize,
) -> MidgeResult<[u8; RUN_HEADER_LEN]> {
    let mut header = [0_u8; RUN_HEADER_LEN];
    header[..8].copy_from_slice(RUN_MAGIC);
    header[8..12].copy_from_slice(&RUN_VERSION.to_le_bytes());
    header[12..20].copy_from_slice(&usize_to_u64(record_count)?.to_le_bytes());
    header[20..28].copy_from_slice(&ordinal_table_offset.to_le_bytes());
    header[28..36].copy_from_slice(&sparse_index_offset.to_le_bytes());
    header[36..44].copy_from_slice(&usize_to_u64(sparse_count)?.to_le_bytes());
    let crc = crc32c::crc32c(&header[..44]);
    header[44..48].copy_from_slice(&crc.to_le_bytes());
    Ok(header)
}

struct RunHeader {
    record_count: usize,
    ordinal_table_offset: u64,
    sparse_index_offset: u64,
    sparse_count: usize,
}

fn read_header(file: &mut File) -> MidgeResult<RunHeader> {
    let mut header = [0_u8; RUN_HEADER_LEN];
    file.seek(SeekFrom::Start(0))?;
    file.read_exact(&mut header)?;
    if &header[..8] != RUN_MAGIC {
        return Err(MidgeError::Corruption(
            "transaction spill run has invalid magic".to_string(),
        ));
    }
    let version = read_u32_at(&header, 8)?;
    if version != RUN_VERSION {
        return Err(MidgeError::Corruption(format!(
            "unsupported transaction spill version {version}"
        )));
    }
    let expected_crc = read_u32_at(&header, 44)?;
    if crc32c::crc32c(&header[..44]) != expected_crc {
        return Err(MidgeError::Corruption(
            "transaction spill header checksum mismatch".to_string(),
        ));
    }
    Ok(RunHeader {
        record_count: u64_to_usize(read_u64_at(&header, 12)?)?,
        ordinal_table_offset: read_u64_at(&header, 20)?,
        sparse_index_offset: read_u64_at(&header, 28)?,
        sparse_count: u64_to_usize(read_u64_at(&header, 36)?)?,
    })
}

fn for_each_run_ordinal<F>(run: &SpillRun, mut visitor: F) -> MidgeResult<()>
where
    F: FnMut(OrdinalOp) -> MidgeResult<()>,
{
    let mut file = File::open(&run.path)?;
    let header = read_header(&mut file)?;
    if header.record_count != run.record_count {
        return Err(MidgeError::Corruption(
            "transaction spill record count changed".to_string(),
        ));
    }
    let mut table_cursor = header.ordinal_table_offset;
    for _ in 0..header.record_count {
        file.seek(SeekFrom::Start(table_cursor))?;
        let (payload, next_cursor) = read_frame(&mut file)?;
        table_cursor = next_cursor;
        if payload.len() != 16 {
            return Err(MidgeError::Corruption(
                "transaction spill ordinal entry has invalid length".to_string(),
            ));
        }
        let ordinal = read_u64_at(&payload, 0)?;
        let record_offset = read_u64_at(&payload, 8)?;
        if record_offset < RUN_HEADER_LEN as u64 || record_offset >= header.ordinal_table_offset {
            return Err(MidgeError::Corruption(
                "transaction spill ordinal offset is out of bounds".to_string(),
            ));
        }
        file.seek(SeekFrom::Start(record_offset))?;
        let (ordinal_op, _) = read_op_frame(&mut file)?;
        if ordinal_op.ordinal != ordinal {
            return Err(MidgeError::Corruption(
                "transaction spill ordinal index does not match record".to_string(),
            ));
        }
        visitor(ordinal_op)?;
    }
    validate_sparse_index(&mut file, &header)?;
    validate_range_index(&run.range_path)
}

fn validate_sparse_index(file: &mut File, header: &RunHeader) -> MidgeResult<()> {
    sparse_start_for_key(file, header, None).map(|_| ())
}

fn read_sparse_offsets(file: &mut File, header: &RunHeader) -> MidgeResult<Vec<u64>> {
    let mut cursor = header.sparse_index_offset;
    let mut previous_key: Option<Vec<u8>> = None;
    let mut offsets = Vec::with_capacity(header.sparse_count);
    for _ in 0..header.sparse_count {
        file.seek(SeekFrom::Start(cursor))?;
        let (payload, next_cursor) = read_frame(file)?;
        cursor = next_cursor;
        if payload.len() < 12 {
            return Err(MidgeError::Corruption(
                "transaction spill sparse index entry is truncated".to_string(),
            ));
        }
        let key_len = read_u32_at(&payload, 0)? as usize;
        let key_end = 4_usize.checked_add(key_len).ok_or_else(|| {
            MidgeError::Corruption("transaction spill sparse key length overflow".to_string())
        })?;
        if key_end.checked_add(8) != Some(payload.len()) {
            return Err(MidgeError::Corruption(
                "transaction spill sparse index entry has invalid length".to_string(),
            ));
        }
        let key = payload[4..key_end].to_vec();
        if previous_key
            .as_ref()
            .is_some_and(|previous| previous > &key)
        {
            return Err(MidgeError::Corruption(
                "transaction spill sparse index is not sorted".to_string(),
            ));
        }
        let record_offset = read_u64_at(&payload, key_end)?;
        if record_offset < RUN_HEADER_LEN as u64 || record_offset >= header.ordinal_table_offset {
            return Err(MidgeError::Corruption(
                "transaction spill sparse index offset is out of bounds".to_string(),
            ));
        }
        if offsets
            .last()
            .is_some_and(|previous| *previous >= record_offset)
        {
            return Err(MidgeError::Corruption(
                "transaction spill sparse offsets are not increasing".to_string(),
            ));
        }
        offsets.push(record_offset);
        previous_key = Some(key);
    }
    if header.record_count != 0 && offsets.first().copied() != Some(RUN_HEADER_LEN as u64) {
        return Err(MidgeError::Corruption(
            "transaction spill sparse index does not cover the first record".to_string(),
        ));
    }
    Ok(offsets)
}

fn sparse_start_for_key(
    file: &mut File,
    header: &RunHeader,
    target: Option<&[u8]>,
) -> MidgeResult<u64> {
    let mut cursor = header.sparse_index_offset;
    let mut previous_key: Option<Vec<u8>> = None;
    let mut previous_offset = RUN_HEADER_LEN as u64;
    let mut selected_offset = RUN_HEADER_LEN as u64;
    for _ in 0..header.sparse_count {
        file.seek(SeekFrom::Start(cursor))?;
        let (payload, next_cursor) = read_frame(file)?;
        cursor = next_cursor;
        if payload.len() < 12 {
            return Err(MidgeError::Corruption(
                "transaction spill sparse index entry is truncated".to_string(),
            ));
        }
        let key_len = read_u32_at(&payload, 0)? as usize;
        let key_end = 4_usize.checked_add(key_len).ok_or_else(|| {
            MidgeError::Corruption("transaction spill sparse key length overflow".to_string())
        })?;
        if key_end.checked_add(8) != Some(payload.len()) {
            return Err(MidgeError::Corruption(
                "transaction spill sparse index entry has invalid length".to_string(),
            ));
        }
        let key = payload[4..key_end].to_vec();
        if previous_key
            .as_ref()
            .is_some_and(|previous| previous > &key)
        {
            return Err(MidgeError::Corruption(
                "transaction spill sparse index is not sorted".to_string(),
            ));
        }
        let record_offset = read_u64_at(&payload, key_end)?;
        if record_offset < RUN_HEADER_LEN as u64 || record_offset >= header.ordinal_table_offset {
            return Err(MidgeError::Corruption(
                "transaction spill sparse index offset is out of bounds".to_string(),
            ));
        }
        if target.is_some_and(|target| key.as_slice() <= target) {
            selected_offset = previous_offset;
            previous_offset = record_offset;
        }
        previous_key = Some(key);
    }
    Ok(selected_offset)
}

fn lookup_run_key(
    run: &SpillRun,
    key: &[u8],
    ordinal_ceiling: u64,
    latest: &mut Option<(u64, IntentLookup)>,
) -> MidgeResult<()> {
    let mut file = File::open(&run.path)?;
    let header = read_header(&mut file)?;
    if header.record_count != run.record_count {
        return Err(MidgeError::Corruption(
            "transaction spill record count changed".to_string(),
        ));
    }

    let point_start = sparse_start_for_key(&mut file, &header, Some(key))?;
    let mut cursor = point_start;
    while cursor < header.ordinal_table_offset {
        file.seek(SeekFrom::Start(cursor))?;
        let (ordinal_op, next_cursor) = read_op_frame(&mut file)?;
        if next_cursor > header.ordinal_table_offset {
            return Err(MidgeError::Corruption(
                "transaction spill data frame overlaps ordinal table".to_string(),
            ));
        }
        cursor = next_cursor;
        match ordinal_op.primary_key().cmp(key) {
            std::cmp::Ordering::Less => {}
            std::cmp::Ordering::Equal => match &ordinal_op.op {
                TransactionOp::Put { .. } | TransactionOp::Delete { .. } => {
                    if ordinal_op.ordinal < ordinal_ceiling {
                        consider_lookup(&ordinal_op, key, latest);
                    }
                }
                TransactionOp::DeleteRange { .. } => {}
            },
            std::cmp::Ordering::Greater => break,
        }
    }

    lookup_range_index(&run.range_path, key, ordinal_ceiling, latest)
}

struct RangeHeader {
    node_count: usize,
    node_section_offset: u64,
}

struct RangeNode {
    ordinal: u64,
    left: u64,
    right: u64,
    start_key: Bytes,
    end_key: Bytes,
    max_end: Bytes,
}

fn read_range_header(file: &mut File) -> MidgeResult<RangeHeader> {
    let mut header = [0_u8; RANGE_HEADER_LEN];
    file.seek(SeekFrom::Start(0))?;
    file.read_exact(&mut header)?;
    if &header[..8] != RANGE_MAGIC {
        return Err(MidgeError::Corruption(
            "transaction range index has invalid magic".to_string(),
        ));
    }
    let version = read_u32_at(&header, 8)?;
    if version != RANGE_VERSION {
        return Err(MidgeError::Corruption(format!(
            "unsupported transaction range index version {version}"
        )));
    }
    if crc32c::crc32c(&header[..28]) != read_u32_at(&header, 28)? {
        return Err(MidgeError::Corruption(
            "transaction range index header checksum mismatch".to_string(),
        ));
    }
    let node_count = u64_to_usize(read_u64_at(&header, 12)?)?;
    let node_section_offset = read_u64_at(&header, 20)?;
    let expected_offset = RANGE_HEADER_LEN
        .checked_add(
            node_count
                .checked_mul(RANGE_TABLE_ENTRY_LEN)
                .ok_or_else(|| {
                    MidgeError::Corruption("transaction range table length overflow".to_string())
                })?,
        )
        .ok_or_else(|| {
            MidgeError::Corruption("transaction range node offset overflow".to_string())
        })?;
    if node_section_offset != usize_to_u64(expected_offset)? {
        return Err(MidgeError::Corruption(
            "transaction range node section offset is invalid".to_string(),
        ));
    }
    Ok(RangeHeader {
        node_count,
        node_section_offset,
    })
}

fn read_range_node(
    file: &mut File,
    header: &RangeHeader,
    node_index: u64,
) -> MidgeResult<RangeNode> {
    let index = u64_to_usize(node_index)?;
    if index >= header.node_count {
        return Err(MidgeError::Corruption(format!(
            "transaction range child {node_index} is out of bounds"
        )));
    }
    let table_offset = RANGE_HEADER_LEN
        .checked_add(index.checked_mul(RANGE_TABLE_ENTRY_LEN).ok_or_else(|| {
            MidgeError::Corruption("transaction range table index overflow".to_string())
        })?)
        .ok_or_else(|| {
            MidgeError::Corruption("transaction range table offset overflow".to_string())
        })?;
    file.seek(SeekFrom::Start(usize_to_u64(table_offset)?))?;
    let mut entry = [0_u8; RANGE_TABLE_ENTRY_LEN];
    file.read_exact(&mut entry)?;
    let expected_crc = read_u32_at(&entry, 8)?;
    if crc32c::crc32c(&entry[..8]) != expected_crc {
        return Err(MidgeError::Corruption(
            "transaction range offset checksum mismatch".to_string(),
        ));
    }
    let node_offset = read_u64_at(&entry, 0)?;
    if node_offset < header.node_section_offset {
        return Err(MidgeError::Corruption(
            "transaction range node offset is out of bounds".to_string(),
        ));
    }
    file.seek(SeekFrom::Start(node_offset))?;
    read_range_node_frame(file)
}

fn read_range_node_frame(file: &mut File) -> MidgeResult<RangeNode> {
    let (payload_len, expected_crc) = read_frame_header(file)?;
    if payload_len < 36 {
        return Err(MidgeError::Corruption(
            "transaction range node is truncated".to_string(),
        ));
    }
    let mut crc = 0_u32;
    let mut fixed = [0_u8; 24];
    read_crc_exact(file, &mut fixed, &mut crc)?;
    let ordinal = read_u64_at(&fixed, 0)?;
    let left = read_u64_at(&fixed, 8)?;
    let right = read_u64_at(&fixed, 16)?;
    let mut consumed = 24_usize;
    let start_key = read_crc_field(file, payload_len, &mut consumed, &mut crc)?;
    let end_key = read_crc_field(file, payload_len, &mut consumed, &mut crc)?;
    let max_end = read_crc_field(file, payload_len, &mut consumed, &mut crc)?;
    if consumed != payload_len {
        return Err(MidgeError::Corruption(
            "transaction range node has trailing bytes".to_string(),
        ));
    }
    if crc != expected_crc {
        return Err(MidgeError::Corruption(
            "transaction range node checksum mismatch".to_string(),
        ));
    }
    Ok(RangeNode {
        ordinal,
        left,
        right,
        start_key: Bytes::from(start_key),
        end_key: Bytes::from(end_key),
        max_end: Bytes::from(max_end),
    })
}

fn read_crc_field(
    file: &mut File,
    payload_len: usize,
    consumed: &mut usize,
    crc: &mut u32,
) -> MidgeResult<Vec<u8>> {
    let mut length_bytes = [0_u8; 4];
    read_crc_exact(file, &mut length_bytes, crc)?;
    *consumed = consumed.saturating_add(4);
    let length = read_u32_at(&length_bytes, 0)? as usize;
    if consumed.saturating_add(length) > payload_len {
        return Err(MidgeError::Corruption(
            "transaction range field exceeds its frame".to_string(),
        ));
    }
    let value = read_crc_vec(file, length, crc)?;
    *consumed = consumed.saturating_add(length);
    Ok(value)
}

fn lookup_range_index(
    path: &Path,
    key: &[u8],
    ordinal_ceiling: u64,
    latest: &mut Option<(u64, IntentLookup)>,
) -> MidgeResult<()> {
    let mut file = File::open(path)?;
    let header = read_range_header(&mut file)?;
    if header.node_count == 0 {
        return Ok(());
    }
    let mut lookup = RangeLookup {
        file: &mut file,
        header: &header,
        key,
        ordinal_ceiling,
        latest,
    };
    lookup_range_subtree(&mut lookup, 0, None, header.node_count)
}

struct RangeLookup<'a> {
    file: &'a mut File,
    header: &'a RangeHeader,
    key: &'a [u8],
    ordinal_ceiling: u64,
    latest: &'a mut Option<(u64, IntentLookup)>,
}

fn lookup_range_subtree(
    lookup: &mut RangeLookup<'_>,
    node_index: u64,
    loaded: Option<RangeNode>,
    remaining_nodes: usize,
) -> MidgeResult<()> {
    if remaining_nodes == 0 {
        return Err(MidgeError::Corruption(
            "transaction range index contains a child cycle".to_string(),
        ));
    }
    let node = match loaded {
        Some(node) => node,
        None => read_range_node(lookup.file, lookup.header, node_index)?,
    };
    if node.left != NO_RANGE_CHILD {
        let left = read_range_node(lookup.file, lookup.header, node.left)?;
        if lookup.key < left.max_end.as_ref() {
            lookup_range_subtree(lookup, node.left, Some(left), remaining_nodes - 1)?;
        }
    }
    if node.ordinal < lookup.ordinal_ceiling
        && node.start_key.as_ref() <= lookup.key
        && lookup.key < node.end_key.as_ref()
        && lookup
            .latest
            .as_ref()
            .is_none_or(|(ordinal, _)| node.ordinal > *ordinal)
    {
        *lookup.latest = Some((node.ordinal, IntentLookup::Deleted));
    }
    if node.right != NO_RANGE_CHILD && node.start_key.as_ref() <= lookup.key {
        lookup_range_subtree(lookup, node.right, None, remaining_nodes - 1)?;
    }
    Ok(())
}

fn validate_range_index(path: &Path) -> MidgeResult<()> {
    let mut file = File::open(path)?;
    let header = read_range_header(&mut file)?;
    for index in 0..header.node_count {
        let node = read_range_node(&mut file, &header, usize_to_u64(index)?)?;
        for child in [node.left, node.right] {
            if child != NO_RANGE_CHILD && u64_to_usize(child)? >= header.node_count {
                return Err(MidgeError::Corruption(
                    "transaction range child is out of bounds".to_string(),
                ));
            }
        }
        if node.max_end < node.end_key {
            return Err(MidgeError::Corruption(
                "transaction range subtree maximum is invalid".to_string(),
            ));
        }
    }
    Ok(())
}

fn write_op_frame(file: &mut File, ordinal_op: &OrdinalOp) -> MidgeResult<()> {
    let (tag, cf_id, ttl, key, second) = match &ordinal_op.op {
        TransactionOp::Put {
            cf_id,
            key,
            value,
            ttl_seconds,
            insert_only,
        } => (
            u8::from(*insert_only),
            *cf_id,
            ttl_seconds.unwrap_or(u64::MAX),
            key.as_ref(),
            value.as_ref(),
        ),
        TransactionOp::Delete { cf_id, key } => (2, *cf_id, u64::MAX, key.as_ref(), &[][..]),
        TransactionOp::DeleteRange {
            cf_id,
            start_key,
            end_key,
        } => (3, *cf_id, u64::MAX, start_key.as_ref(), end_key.as_ref()),
    };
    let ordinal = ordinal_op.ordinal.to_le_bytes();
    let tag = [tag];
    let cf_id = cf_id.to_le_bytes();
    let ttl = ttl.to_le_bytes();
    let key_len = field_len_bytes(key.len())?;
    let second_len = field_len_bytes(second.len())?;
    write_frame_parts(
        file,
        &[
            &ordinal,
            &tag,
            &cf_id,
            &ttl,
            &key_len,
            key,
            &second_len,
            second,
        ],
    )
}

fn read_op_frame(file: &mut File) -> MidgeResult<(OrdinalOp, u64)> {
    let (payload_len, expected_crc) = read_frame_header(file)?;
    if payload_len < 29 {
        return Err(MidgeError::Corruption(
            "transaction spill operation is truncated".to_string(),
        ));
    }
    let mut crc = 0_u32;
    let mut fixed = [0_u8; 21];
    read_crc_exact(file, &mut fixed, &mut crc)?;
    let ordinal = read_u64_at(&fixed, 0)?;
    let tag = fixed[8];
    let cf_id = read_u32_at(&fixed, 9)?;
    let ttl = read_u64_at(&fixed, 13)?;

    let mut key_len_bytes = [0_u8; 4];
    read_crc_exact(file, &mut key_len_bytes, &mut crc)?;
    let key_len = read_u32_at(&key_len_bytes, 0)? as usize;
    let mut consumed = 25_usize;
    if consumed.saturating_add(key_len).saturating_add(4) > payload_len {
        return Err(MidgeError::Corruption(
            "transaction spill key length exceeds its frame".to_string(),
        ));
    }
    let key = read_crc_vec(file, key_len, &mut crc)?;
    consumed = consumed.saturating_add(key_len);

    let mut second_len_bytes = [0_u8; 4];
    read_crc_exact(file, &mut second_len_bytes, &mut crc)?;
    consumed = consumed.saturating_add(4);
    let second_len = read_u32_at(&second_len_bytes, 0)? as usize;
    if consumed.saturating_add(second_len) != payload_len {
        return Err(MidgeError::Corruption(
            "transaction spill value length does not match its frame".to_string(),
        ));
    }
    let second = read_crc_vec(file, second_len, &mut crc)?;
    if crc != expected_crc {
        return Err(MidgeError::Corruption(
            "transaction spill frame checksum mismatch".to_string(),
        ));
    }
    let op = match tag {
        0 | 1 => TransactionOp::Put {
            cf_id,
            key: Bytes::from(key),
            value: Bytes::from(second),
            ttl_seconds: (ttl != u64::MAX).then_some(ttl),
            insert_only: tag == 1,
        },
        2 if second.is_empty() => TransactionOp::Delete {
            cf_id,
            key: Bytes::from(key),
        },
        3 => TransactionOp::DeleteRange {
            cf_id,
            start_key: Bytes::from(key),
            end_key: Bytes::from(second),
        },
        _ => {
            return Err(MidgeError::Corruption(format!(
                "transaction spill operation has invalid tag {tag}"
            )))
        }
    };
    Ok((OrdinalOp { ordinal, op }, file.stream_position()?))
}

/// Read and checksum one key-sorted operation while retaining only its key.
///
/// A spilled value may be as large as a WAL frame. Scan cursors must not
/// reconstruct that value merely to merge intent keys, so the second field is
/// checksummed through a fixed scratch buffer and discarded.
fn read_op_primary_key_frame(file: &mut File) -> MidgeResult<(Bytes, u64)> {
    let (payload_len, expected_crc) = read_frame_header(file)?;
    if payload_len < 29 {
        return Err(MidgeError::Corruption(
            "transaction spill operation is truncated".to_string(),
        ));
    }

    let mut crc = 0_u32;
    let mut fixed = [0_u8; 21];
    read_crc_exact(file, &mut fixed, &mut crc)?;
    let tag = fixed[8];
    if tag > 3 {
        return Err(MidgeError::Corruption(format!(
            "transaction spill operation has invalid tag {tag}"
        )));
    }

    let mut key_len_bytes = [0_u8; 4];
    read_crc_exact(file, &mut key_len_bytes, &mut crc)?;
    let key_len = read_u32_at(&key_len_bytes, 0)? as usize;
    let mut consumed = 25_usize;
    if consumed.saturating_add(key_len).saturating_add(4) > payload_len {
        return Err(MidgeError::Corruption(
            "transaction spill key length exceeds its frame".to_string(),
        ));
    }
    let key = read_crc_vec(file, key_len, &mut crc)?;
    consumed = consumed.saturating_add(key_len);

    let mut second_len_bytes = [0_u8; 4];
    read_crc_exact(file, &mut second_len_bytes, &mut crc)?;
    consumed = consumed.saturating_add(4);
    let second_len = read_u32_at(&second_len_bytes, 0)? as usize;
    if consumed.saturating_add(second_len) != payload_len {
        return Err(MidgeError::Corruption(
            "transaction spill value length does not match its frame".to_string(),
        ));
    }
    if tag == 2 && second_len != 0 {
        return Err(MidgeError::Corruption(
            "transaction spill delete has a value".to_string(),
        ));
    }

    let mut remaining = second_len;
    let mut scratch = [0_u8; 8192];
    while remaining != 0 {
        let chunk_len = remaining.min(scratch.len());
        read_crc_exact(file, &mut scratch[..chunk_len], &mut crc)?;
        remaining -= chunk_len;
    }
    if crc != expected_crc {
        return Err(MidgeError::Corruption(
            "transaction spill frame checksum mismatch".to_string(),
        ));
    }
    Ok((Bytes::from(key), file.stream_position()?))
}

fn write_frame(file: &mut File, payload: &[u8]) -> MidgeResult<()> {
    let length = u32::try_from(payload.len()).map_err(|_| {
        MidgeError::ResourceLimit("transaction spill frame exceeds u32 length".to_string())
    })?;
    file.write_all(&length.to_le_bytes())?;
    file.write_all(&crc32c::crc32c(payload).to_le_bytes())?;
    file.write_all(payload)?;
    Ok(())
}

fn write_frame_parts(file: &mut File, parts: &[&[u8]]) -> MidgeResult<()> {
    let payload_len = parts.iter().try_fold(0_usize, |total, part| {
        total.checked_add(part.len()).ok_or_else(|| {
            MidgeError::ResourceLimit("transaction spill frame length overflow".to_string())
        })
    })?;
    if payload_len > MAX_FRAME_BYTES {
        return Err(MidgeError::ResourceLimit(format!(
            "transaction spill frame length {payload_len} exceeds WAL frame limit"
        )));
    }
    let payload_len_u32 = u32::try_from(payload_len).map_err(|_| {
        MidgeError::ResourceLimit("transaction spill frame exceeds u32 length".to_string())
    })?;
    let crc = parts
        .iter()
        .fold(0_u32, |crc, part| crc32c::crc32c_append(crc, part));
    file.write_all(&payload_len_u32.to_le_bytes())?;
    file.write_all(&crc.to_le_bytes())?;
    for part in parts {
        file.write_all(part)?;
    }
    Ok(())
}

fn write_sparse_frame(file: &mut File, key: &[u8], offset: u64) -> MidgeResult<()> {
    let key_len = field_len_bytes(key.len())?;
    let offset = offset.to_le_bytes();
    write_frame_parts(file, &[&key_len, key, &offset])
}

fn field_len_bytes(length: usize) -> MidgeResult<[u8; 4]> {
    u32::try_from(length)
        .map(u32::to_le_bytes)
        .map_err(|_| MidgeError::ResourceLimit("transaction spill field is too large".to_string()))
}

fn read_frame_header(file: &mut File) -> MidgeResult<(usize, u32)> {
    let mut frame_header = [0_u8; 8];
    file.read_exact(&mut frame_header)?;
    let length = read_u32_at(&frame_header, 0)? as usize;
    if length > MAX_FRAME_BYTES {
        return Err(MidgeError::Corruption(format!(
            "transaction spill frame length {length} exceeds limit"
        )));
    }
    Ok((length, read_u32_at(&frame_header, 4)?))
}

fn read_crc_exact(file: &mut File, dst: &mut [u8], crc: &mut u32) -> MidgeResult<()> {
    file.read_exact(dst)?;
    *crc = crc32c::crc32c_append(*crc, dst);
    Ok(())
}

fn read_crc_vec(file: &mut File, length: usize, crc: &mut u32) -> MidgeResult<Vec<u8>> {
    let mut bytes = Vec::new();
    bytes.try_reserve_exact(length).map_err(|_| {
        MidgeError::ResourceLimit(format!(
            "transaction spill cannot allocate {length} bytes for one operation field"
        ))
    })?;
    bytes.resize(length, 0);
    read_crc_exact(file, &mut bytes, crc)?;
    Ok(bytes)
}

fn read_frame(file: &mut File) -> MidgeResult<(Vec<u8>, u64)> {
    let (length, expected_crc) = read_frame_header(file)?;
    let mut payload = Vec::new();
    payload.try_reserve_exact(length).map_err(|_| {
        MidgeError::ResourceLimit(format!(
            "transaction spill cannot allocate {length} bytes for an index frame"
        ))
    })?;
    payload.resize(length, 0);
    file.read_exact(&mut payload)?;
    if crc32c::crc32c(&payload) != expected_crc {
        return Err(MidgeError::Corruption(
            "transaction spill frame checksum mismatch".to_string(),
        ));
    }
    Ok((payload, file.stream_position()?))
}

fn read_u32_at(bytes: &[u8], offset: usize) -> MidgeResult<u32> {
    let end = offset.checked_add(4).ok_or_else(|| {
        MidgeError::Corruption("transaction spill u32 offset overflow".to_string())
    })?;
    let slice = bytes
        .get(offset..end)
        .ok_or_else(|| MidgeError::Corruption("transaction spill u32 is truncated".to_string()))?;
    Ok(u32::from_le_bytes(
        slice.try_into().expect("validated u32 slice"),
    ))
}

fn read_u64_at(bytes: &[u8], offset: usize) -> MidgeResult<u64> {
    let end = offset.checked_add(8).ok_or_else(|| {
        MidgeError::Corruption("transaction spill u64 offset overflow".to_string())
    })?;
    let slice = bytes
        .get(offset..end)
        .ok_or_else(|| MidgeError::Corruption("transaction spill u64 is truncated".to_string()))?;
    Ok(u64::from_le_bytes(
        slice.try_into().expect("validated u64 slice"),
    ))
}

fn usize_to_u64(value: usize) -> MidgeResult<u64> {
    u64::try_from(value)
        .map_err(|_| MidgeError::ResourceLimit("transaction spill count exceeds u64".to_string()))
}

fn u64_to_usize(value: u64) -> MidgeResult<usize> {
    usize::try_from(value).map_err(|_| {
        MidgeError::Corruption("transaction spill count exceeds platform size".to_string())
    })
}

fn remove_run(path: &Path) {
    match fs::remove_file(path) {
        Ok(()) => {}
        Err(error) if error.kind() == std::io::ErrorKind::NotFound => {}
        Err(error) => {
            tracing::warn!(path = %path.display(), %error, "failed to remove transaction spill run");
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn put(key: &[u8], value: &[u8]) -> TransactionOp {
        TransactionOp::Put {
            cf_id: 0,
            key: Bytes::copy_from_slice(key),
            value: Bytes::copy_from_slice(value),
            ttl_seconds: None,
            insert_only: false,
        }
    }

    fn delete_range(start: &[u8], end: &[u8]) -> TransactionOp {
        TransactionOp::DeleteRange {
            cf_id: 0,
            start_key: Bytes::copy_from_slice(start),
            end_key: Bytes::copy_from_slice(end),
        }
    }

    #[test]
    fn should_reject_corrupt_data_frame_when_reading_spill_run() -> MidgeResult<()> {
        // Arrange
        let temp = tempfile::tempdir()?;
        let mut ops = vec![OrdinalOp {
            ordinal: 0,
            op: put(b"key", b"value"),
        }];
        let run = write_run(temp.path(), 1, 0, &mut ops)?;
        let mut bytes = fs::read(&run.path)?;
        bytes[RUN_HEADER_LEN + 8] ^= 0x55;
        fs::write(&run.path, bytes)?;

        // Act
        let result = for_each_run_ordinal(&run, |_| Ok(()));

        // Assert
        assert!(matches!(result, Err(MidgeError::Corruption(_))));
        Ok(())
    }

    #[test]
    fn should_reject_corrupt_sparse_index_when_reading_spill_run() -> MidgeResult<()> {
        // Arrange
        let temp = tempfile::tempdir()?;
        let mut ops = vec![OrdinalOp {
            ordinal: 0,
            op: put(b"key", b"value"),
        }];
        let run = write_run(temp.path(), 1, 0, &mut ops)?;
        let mut file = File::open(&run.path)?;
        let header = read_header(&mut file)?;
        drop(file);
        let mut bytes = fs::read(&run.path)?;
        let corrupt_at = usize::try_from(header.sparse_index_offset)
            .map_err(|_| MidgeError::Corruption("index offset exceeds usize".to_string()))?
            + 8;
        bytes[corrupt_at] ^= 0x55;
        fs::write(&run.path, bytes)?;

        // Act
        let result = for_each_run_ordinal(&run, |_| Ok(()));

        // Assert
        assert!(matches!(result, Err(MidgeError::Corruption(_))));
        Ok(())
    }

    #[test]
    fn should_resolve_range_tombstone_before_ordinal_ceiling() -> MidgeResult<()> {
        // Arrange
        let temp = tempfile::tempdir()?;
        let mut ops = vec![
            OrdinalOp {
                ordinal: 0,
                op: put(b"middle", b"before"),
            },
            OrdinalOp {
                ordinal: 1,
                op: delete_range(b"alpha", b"omega"),
            },
            OrdinalOp {
                ordinal: 2,
                op: put(b"middle", b"after"),
            },
        ];
        let run = write_run(temp.path(), 1, 0, &mut ops)?;

        // Act
        let mut before_final_put = None;
        lookup_run_key(&run, b"middle", 2, &mut before_final_put)?;
        let mut after_final_put = None;
        lookup_run_key(&run, b"middle", 3, &mut after_final_put)?;

        // Assert
        assert!(matches!(before_final_put, Some((1, IntentLookup::Deleted))));
        assert!(matches!(
            after_final_put,
            Some((2, IntentLookup::Present(value))) if value == Bytes::from_static(b"after")
        ));
        Ok(())
    }

    #[test]
    fn should_reject_corrupt_range_index_when_reading_spill_run() -> MidgeResult<()> {
        // Arrange
        let temp = tempfile::tempdir()?;
        let mut ops = vec![OrdinalOp {
            ordinal: 0,
            op: delete_range(b"alpha", b"omega"),
        }];
        let run = write_run(temp.path(), 1, 0, &mut ops)?;
        let mut file = File::open(&run.range_path)?;
        let header = read_range_header(&mut file)?;
        drop(file);
        let mut bytes = fs::read(&run.range_path)?;
        let corrupt_at = usize::try_from(header.node_section_offset)
            .map_err(|_| MidgeError::Corruption("range offset exceeds usize".to_string()))?
            + 8;
        bytes[corrupt_at] ^= 0x55;
        fs::write(&run.range_path, bytes)?;

        // Act
        let result = lookup_run_key(&run, b"middle", u64::MAX, &mut None);

        // Assert
        assert!(matches!(result, Err(MidgeError::Corruption(_))));
        Ok(())
    }

    #[test]
    fn should_stream_large_spill_record_with_wal_sized_bound() -> MidgeResult<()> {
        // Arrange
        let temp = tempfile::tempdir()?;
        let value = vec![b'x'; 2 * 1024 * 1024];
        let mut ops = vec![OrdinalOp {
            ordinal: 0,
            op: put(b"large", &value),
        }];

        // Act
        let run = write_run(temp.path(), 1, 0, &mut ops)?;
        let mut read_value = None;
        for_each_run_ordinal(&run, |ordinal_op| {
            if let TransactionOp::Put { value, .. } = ordinal_op.op {
                read_value = Some(value);
            }
            Ok(())
        })?;

        // Assert
        assert_eq!(read_value.as_ref().map(Bytes::len), Some(value.len()));
        Ok(())
    }

    #[test]
    fn should_scan_spill_keys_in_both_directions_across_sparse_chunks() -> MidgeResult<()> {
        // Arrange
        let temp = tempfile::tempdir()?;
        let mut ops = (0_u64..40)
            .map(|ordinal| OrdinalOp {
                ordinal,
                op: put(format!("key-{ordinal:02}").as_bytes(), b"value"),
            })
            .collect::<Vec<_>>();
        let run = write_run(temp.path(), 1, 0, &mut ops)?;

        // Act
        let forward = RunKeyCursor::new(&run, Some(b"key-10"), Some(b"key-20"), false)?
            .collect::<MidgeResult<Vec<_>>>()?;
        let reverse = RunKeyCursor::new(&run, Some(b"key-10"), Some(b"key-20"), true)?
            .collect::<MidgeResult<Vec<_>>>()?;

        // Assert
        let expected = (10_u64..20)
            .map(|ordinal| Bytes::from(format!("key-{ordinal:02}")))
            .collect::<Vec<_>>();
        assert_eq!(forward, expected);
        assert_eq!(reverse, expected.into_iter().rev().collect::<Vec<_>>());
        Ok(())
    }

    #[test]
    fn should_surface_late_spill_corruption_from_key_cursor_item() -> MidgeResult<()> {
        // Arrange
        let temp = tempfile::tempdir()?;
        let mut ops = vec![
            OrdinalOp {
                ordinal: 0,
                op: put(b"alpha", b"one"),
            },
            OrdinalOp {
                ordinal: 1,
                op: put(b"bravo", b"two"),
            },
        ];
        let run = write_run(temp.path(), 1, 0, &mut ops)?;
        let mut file = File::open(&run.path)?;
        file.seek(SeekFrom::Start(RUN_HEADER_LEN as u64))?;
        let (_, second_offset) = read_op_frame(&mut file)?;
        drop(file);
        let mut bytes = fs::read(&run.path)?;
        let corrupt_at = u64_to_usize(second_offset)?.saturating_add(8);
        bytes[corrupt_at] ^= 0x55;
        fs::write(&run.path, bytes)?;
        let mut scan = RunKeyCursor::new(&run, None, None, false)?;

        // Act
        let first = scan.next().transpose()?;
        let late = scan.next().expect("second item must report corruption");

        // Assert
        assert_eq!(first, Some(Bytes::from_static(b"alpha")));
        assert!(matches!(late, Err(MidgeError::Corruption(_))));
        Ok(())
    }
}
