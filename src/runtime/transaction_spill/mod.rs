//! Bounded transaction intent storage and CRC-framed spill runs.
//!
//! Runs are key-sorted for lookup and carry a separate ordinal table. The
//! runtime can therefore replay a spilled transaction in caller order without
//! reconstructing its complete write set in memory.
use super::TransactionOp;
use crate::common::{MidgeError, MidgeResult};
use bytes::Bytes;
use std::fs;
use std::path::{Path, PathBuf};
use std::sync::atomic::{AtomicUsize, Ordering};
use std::sync::Arc;

mod format;
mod range;
mod scan;

#[cfg(test)]
use format::u64_to_usize;
use format::{for_each_run_ordinal, lookup_run_key, remove_run, write_run};
#[cfg(test)]
use format::{read_header, read_op_frame};
#[cfg(test)]
use range::read_range_header;
pub(crate) use scan::IntentKeyScan;
#[cfg(test)]
use scan::RunKeyCursor;
#[cfg(test)]
use std::fs::File;
#[cfg(test)]
use std::io::{Seek, SeekFrom};
const RUN_MAGIC: &[u8; 8] = b"MDGTXN01";
const RUN_VERSION: u32 = 2;
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

    pub(crate) fn try_reserve(&self, bytes: usize) -> bool {
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

    pub(crate) fn release(&self, bytes: usize) {
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
        // Reject unflushable entries before allocating reservations or writing
        // spill files, so failed admission leaves the transaction unchanged.
        match &op {
            TransactionOp::Put { key, value, .. } => {
                crate::sst::encoding::validate_entry_size(key.len(), value.len())?;
            }
            TransactionOp::Delete { key, .. } => {
                crate::sst::encoding::validate_entry_size(key.len(), 0)?;
            }
            TransactionOp::DeleteRange {
                start_key, end_key, ..
            } => {
                crate::sst::types::validate_range_tombstone_size(start_key.len(), end_key.len())?;
            }
        }
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

pub(super) fn op_primary_key_bytes(op: &TransactionOp) -> Bytes {
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

#[cfg(test)]
mod tests;
