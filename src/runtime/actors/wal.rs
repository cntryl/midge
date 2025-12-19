//! WAL Actor - handles write-ahead log operations
//!
//! Responsible for:
//! - Appending records to WAL
//! - Syncing WAL to disk
//! - Rotating WAL segments
//! - Coordinating with cloud actor for WAL uploads
//! - Tracking local_durable_seq and cloud_durable_seq frontiers
//! - Managing pending requests waiting for cloud durability
//!
//! ARCHITECTURAL RULES:
//! - ALWAYS uses FsWalWriter (never creates new backends)
//! - Assigns global sequence numbers via state.next_sequence()
//! - Tracks two durability frontiers for CloudFirst mode
//! - Does NOT block event loop waiting for cloud
//! - Queues pending requests and completes them when cloud_durable_seq advances
//!
//! NOTE: `DurabilityPolicy::CloudFirst` is wired end-to-end.
//! In CloudFirst mode, `append()` queues writes in `pending_cloud_writes` and
//! defers visibility/response until `handle_cloud_upload_complete()` advances
//! `cloud_durable_seq` on CloudAck.
//!
//! CLOUD-DURABLE MEMTABLE RULE:
//! In Durability::CloudFirst mode, writes are NOT visible in memtable
//! until cloud storage acknowledges durability. Local WAL is ephemeral;
//! cloud WAL is the source of truth. Therefore:
//! - Append to local WAL immediately (fast path)
//! - Do NOT update memtable yet
//! - Do NOT respond to request yet
//! - Queue as PendingCloudWrite
//! - When cloud ACKs: apply to memtable + respond to request

use super::super::state::RuntimeState;
use crate::common::MidgeError;
use crate::common::MidgeResult;
use crate::io::{Fs, FsPath, RealFs};
use crate::runtime::IntentLogEntry;
use crate::sst::Memtable;
use crate::wal::{DurabilityPolicy, FsWalFactoryIo, WalOpKind, WalRecord, WalWriter};
use bytes::Bytes;
use std::collections::VecDeque;
use std::path::PathBuf;
use std::sync::Arc;
use std::time::{Duration, Instant};
/// === CloudFirst Backpressure Configuration ===
/// These constants prevent memory exhaustion when cloud uploads are slow or stalled.
///
/// Maximum number of pending cloud writes before returning WriteStall
const MAX_PENDING_CLOUD_WRITES: usize = 100_000;

/// Approximate memory threshold for pending cloud writes (100MB)
/// Each write is tracked in pending_cloud_writes; assume ~1KB average per write
const MAX_PENDING_CLOUD_WRITE_BYTES: usize = 100 * 1024 * 1024;

/// Maximum time to wait for cloud upload acknowledgment (30 seconds)
const CLOUD_UPLOAD_TIMEOUT: Duration = Duration::from_secs(30);

#[derive(Debug)]
enum PendingCloudWrite {
    Single {
        cf_id: u32,
        key: Vec<u8>,
        value: Option<Vec<u8>>,
        sequence: u64,
        expiration: Option<u64>,
        enqueued_at: Instant,
    },
    Merge {
        cf_id: u32,
        key: Vec<u8>,
        operand: Vec<u8>,
        sequence: u64,
        enqueued_at: Instant,
    },
    Batch {
        commit_sequence: u64,
        ops: Vec<BatchApplyOp>,
        enqueued_at: Instant,
    },
}

#[derive(Debug)]
enum BatchApplyOp {
    Put {
        cf_id: u32,
        key: Vec<u8>,
        value: Vec<u8>,
        expiration: Option<u64>,
        sequence: u64,
    },
    Delete {
        cf_id: u32,
        key: Vec<u8>,
        sequence: u64,
    },
}

/// Actor handling WAL operations
pub struct WalActor {
    /// WAL writer (owned by this actor)
    writer: Option<Box<dyn WalWriter>>,
    /// Filesystem backend for WAL files (io::Fs abstraction)
    wal_fs: Option<Arc<dyn Fs>>,
    /// Buffered writes pending sync
    pending_sync_count: usize,
    /// Durability policy (determines sync behavior)
    durability_policy: DurabilityPolicy,
    /// Pending writes waiting for cloud durability (CloudFirst mode only)
    /// These writes are in local WAL but NOT in memtable yet
    pending_cloud_writes: VecDeque<PendingCloudWrite>,
    /// Approximate bytes in pending_cloud_writes queue (for backpressure)
    /// Each write costs ~1KB; used to enforce MAX_PENDING_CLOUD_WRITE_BYTES
    pending_cloud_write_bytes: usize,
    /// Bytes written since last sync (for batched mode)
    bytes_since_sync: usize,
    /// Flush generation for batched/local durability group commit
    flush_generation: u64,

    // === Optional instrumentation ===
    sync_calls: u64,
    sync_total: Duration,
}

impl WalActor {
    pub fn new(
        wal_dir: PathBuf,
        durability_policy: DurabilityPolicy,
        memory_mode: bool,
    ) -> MidgeResult<Self> {
        let (wal_fs, writer) = if memory_mode {
            (None, None)
        } else {
            let fs: Arc<dyn Fs> = Arc::new(RealFs::new(&wal_dir)?);
            let factory = FsWalFactoryIo::new(Arc::clone(&fs));
            let writer = Some(factory.create_writer("wal.log")?);
            (Some(fs), writer)
        };

        Ok(Self {
            writer,
            wal_fs,
            pending_sync_count: 0,
            durability_policy,
            pending_cloud_writes: VecDeque::new(),
            pending_cloud_write_bytes: 0,
            bytes_since_sync: 0,
            flush_generation: 0,
            sync_calls: 0,
            sync_total: Duration::from_secs(0),
        })
    }

    pub fn durability_policy(&self) -> DurabilityPolicy {
        self.durability_policy
    }

    pub fn is_cloud_first(&self) -> bool {
        matches!(self.durability_policy, DurabilityPolicy::CloudFirst)
    }

    pub fn has_pending_cloud_writes(&self) -> bool {
        !self.pending_cloud_writes.is_empty()
    }

    pub fn pending_cloud_writes_len(&self) -> usize {
        self.pending_cloud_writes.len()
    }

    pub fn pending_cloud_write_bytes(&self) -> usize {
        self.pending_cloud_write_bytes
    }

    pub fn bytes_since_sync(&self) -> usize {
        self.bytes_since_sync
    }

    pub fn pending_sync_count(&self) -> usize {
        self.pending_sync_count
    }

    pub fn current_flush_generation(&self) -> u64 {
        self.flush_generation
    }

    /// Check if cloud write queue has hit backpressure limits
    /// Returns true if we should reject new writes to prevent memory exhaustion
    pub fn should_apply_backpressure(&self) -> bool {
        self.pending_cloud_writes.len() >= MAX_PENDING_CLOUD_WRITES
            || self.pending_cloud_write_bytes >= MAX_PENDING_CLOUD_WRITE_BYTES
    }

    /// Check for timed-out pending cloud writes
    /// Returns number of writes that have exceeded CLOUD_UPLOAD_TIMEOUT
    pub fn count_timed_out_writes(&self) -> usize {
        let now = Instant::now();
        self.pending_cloud_writes
            .iter()
            .filter(|pw| {
                let enqueued_at = match pw {
                    PendingCloudWrite::Single { enqueued_at, .. }
                    | PendingCloudWrite::Merge { enqueued_at, .. }
                    | PendingCloudWrite::Batch { enqueued_at, .. } => *enqueued_at,
                };
                now.duration_since(enqueued_at) > CLOUD_UPLOAD_TIMEOUT
            })
            .count()
    }

    /// Append a record to the WAL
    ///
    /// - Strict: fsync immediately + apply to memtable + respond
    /// - Batched: batch writes + apply to memtable immediately + respond
    /// - CloudMirrored: fsync + apply to memtable + schedule cloud upload + respond
    /// - CloudFirst: local write (cache) + queue for cloud + DO NOT apply to memtable yet
    ///
    /// In CloudFirst mode, writes are NOT visible until cloud acknowledges.
    /// Returns the assigned sequence number.
    ///
    /// IDEMPOTENCY: Uses request_id to detect retries. If the same request_id is seen twice,
    /// returns the same sequence number instead of allocating a new one.
    #[allow(clippy::too_many_arguments)]
    pub fn append(
        &mut self,
        state: &mut RuntimeState,
        request_id: u64,
        cf_id: u32,
        key: Bytes,
        value: Option<Bytes>,
        insert_only: bool,
        ttl_seconds: Option<u64>,
    ) -> MidgeResult<(u64, bool)> {
        // Enforce insert-only if requested by checking in-memory state
        if insert_only && self.key_exists_or_pending(state, cf_id, &key) {
            return Err(MidgeError::InvalidArgument(
                "key already exists".to_string(),
            ));
        }
        
        // 🔑 CRITICAL: Allocate sequence idempotently using request_id.
        // If this request_id was already allocated, return the same sequence.
        // Otherwise, allocate a new sequence and cache it.
        let (first_seq, _count) = state.allocate_sequences_idempotent(request_id, 1);
        let sequence = first_seq;

        // Determine operation kind: Delete if value is None, Put otherwise
        let op_kind = if value.is_none() {
            WalOpKind::Delete
        } else {
            WalOpKind::Put
        };

        // Create WAL record (with expiration if provided)
        let record = match ttl_seconds {
            Some(ttl) if ttl > 0 => {
                WalRecord::new_with_ttl(cf_id, op_kind, key.clone(), value.clone(), sequence, ttl)
            }
            _ => WalRecord::new_cf(cf_id, op_kind, key.clone(), value.clone(), sequence),
        };

        // Calculate record size for batching
        let record_size = record.estimated_size();

        // ALWAYS append to local WAL first (FsWalWriter)
        if let Some(writer) = &mut self.writer {
            writer.append_record(&record)?;
        }

        // Update state tracking
        state.wal.pending_writes += 1;
        self.pending_sync_count += 1;
        self.bytes_since_sync += record_size;

        // Apply durability policy
        match self.durability_policy {
            DurabilityPolicy::Strict => {
                // Fsync immediately, then apply to memtable
                self.sync_internal(state)?;
                state.wal.local_durable_seq = sequence;

                // Apply to memtable - write is now visible
                self.apply_to_memtable(state, sequence, cf_id, &key, &value, record.expiration)?;
            }
            DurabilityPolicy::Batched => {
                // Apply to memtable immediately, but defer response until fsync completes.
                // Caller joins the group commit waiter queue; sync completion notifies all.
                self.apply_to_memtable(state, sequence, cf_id, &key, &value, record.expiration)?;
                // Return deferred=true so caller joins group commit
            }
            DurabilityPolicy::CloudMirrored => {
                // Fsync locally, apply to memtable, schedule cloud upload in background
                self.sync_internal(state)?;
                state.wal.local_durable_seq = sequence;

                // Apply to memtable - local durability sufficient
                self.apply_to_memtable(state, sequence, cf_id, &key, &value, record.expiration)?;

                // TODO: Send CloudUploadWal message to CloudActor
            }
            DurabilityPolicy::CloudFirst => {
                // === CRITICAL: Check backpressure before queueing ===
                // If cloud upload is stalled, pending queue grows without bound.
                // Prevent memory exhaustion by rejecting writes when queue is full.
                if self.should_apply_backpressure() {
                    tracing::warn!(
                        pending_count = self.pending_cloud_writes.len(),
                        pending_bytes = self.pending_cloud_write_bytes,
                        "CloudFirst write stall: pending queue at capacity"
                    );
                    return Err(MidgeError::WriteStall(
                        "CloudFirst pending queue at capacity; cloud upload too slow".to_string(),
                    ));
                }

                // Check for timed-out writes
                let timed_out = self.count_timed_out_writes();
                if timed_out > 0 {
                    tracing::error!(
                        timed_out_count = timed_out,
                        timeout_secs = CLOUD_UPLOAD_TIMEOUT.as_secs(),
                        "CloudFirst timeout: pending writes not acknowledged by cloud"
                    );
                    return Err(MidgeError::Internal(
                        format!("{} pending writes exceeded cloud upload timeout", timed_out)
                    ));
                }

                // DO NOT apply to memtable yet!
                // Queue write for cloud durability confirmation.
                // Memtable update + response happen in handle_cloud_upload_complete.
                self.queue_cloud_write(
                    cf_id,
                    key.to_vec(),
                    value.as_ref().map(|v| v.to_vec()),
                    sequence,
                    record.expiration,
                );
            }
        }

        tracing::trace!(cf_id, sequence, policy = ?self.durability_policy, "WAL append");

        // Return deferred=true if using group commit (Batched or CloudFirst modes)
        let deferred = matches!(
            self.durability_policy,
            DurabilityPolicy::Batched | DurabilityPolicy::CloudFirst
        );
        Ok((sequence, deferred))
    }

    /// Append a batch of operations to the WAL as a single atomic unit.
    ///
    /// This method:
    /// - allocates a single transaction id
    /// - writes TxnBegin marker
    /// - writes all operation records (in order)
    /// - writes TxnCommit marker
    /// - applies all operations to memtables (in order)
    ///
    /// Returns the last allocated sequence number for the batch.
    pub fn append_batch(
        &mut self,
        state: &mut RuntimeState,
        _request_id: u64,
        ops: Vec<crate::runtime::WriteBatchOp>,
    ) -> MidgeResult<(u64, usize, bool)> {
        if ops.is_empty() {
            return Ok((state.sequence, 0, false));
        }

        let txn_id = state.next_txn_id();

        // Marker key is unused by semantics but required by the record format.
        let marker_key = Bytes::from_static(b"txn");

        // Preallocate sequence range for batch: begin, per-op, commit
        let ops_count = ops.len();
        let ops_count_u64 = ops_count as u64;
        // sequences: begin_seq, op_seqs[0..ops_count-1], commit_seq
        let begin_seq = state.sequence + 1;
        let first_op_seq = begin_seq + 1;
        let commit_seq = begin_seq + 1 + ops_count_u64;

        // Advance global sequence to commit_seq
        state.sequence = commit_seq;

        // Log seqno allocations for intent tracing
        // Begin seq (cf_id 0)
        state.append_intent(IntentLogEntry::SeqnoAllocated {
            seqno: begin_seq,
            cf_id: 0,
        })?;
        // Per-op seqs will be logged below when we know cf_id
        // Commit seq
        state.append_intent(IntentLogEntry::SeqnoAllocated {
            seqno: commit_seq,
            cf_id: 0,
        })?;

        // Create and write begin record
        let mut begin_record =
            WalRecord::new_cf(0, WalOpKind::TxnBegin, marker_key.clone(), None, begin_seq);
        begin_record.txn_id = Some(txn_id);
        if let Some(writer) = &mut self.writer {
            writer.append_record(&begin_record)?;
        }

        state.wal.pending_writes += 1;
        self.pending_sync_count += 1;
        self.bytes_since_sync += begin_record.estimated_size();

        let mut apply_ops: Vec<BatchApplyOp> = Vec::with_capacity(ops_count);

        // Now write op records using deterministic sequences
        for (i, op) in ops.into_iter().enumerate() {
            let seq = first_op_seq + i as u64;
            match op {
                crate::runtime::WriteBatchOp::Put {
                    cf_id,
                    key,
                    value,
                    ttl_seconds,
                } => {
                    let key_b = Bytes::from(key);
                    let value_b = Bytes::from(value);

                    let mut record = match ttl_seconds {
                        Some(ttl) if ttl > 0 => WalRecord::new_with_ttl(
                            cf_id,
                            WalOpKind::Put,
                            key_b.clone(),
                            Some(value_b.clone()),
                            seq,
                            ttl,
                        ),
                        _ => WalRecord::new_cf(
                            cf_id,
                            WalOpKind::Put,
                            key_b.clone(),
                            Some(value_b.clone()),
                            seq,
                        ),
                    };
                    record.txn_id = Some(txn_id);

                    if let Some(writer) = &mut self.writer {
                        writer.append_record(&record)?;
                    }

                    // Log seq allocation for this CF
                    state.append_intent(IntentLogEntry::SeqnoAllocated { seqno: seq, cf_id })?;

                    state.wal.pending_writes += 1;
                    self.pending_sync_count += 1;
                    self.bytes_since_sync += record.estimated_size();

                    apply_ops.push(BatchApplyOp::Put {
                        cf_id,
                        key: key_b.to_vec(),
                        value: value_b.to_vec(),
                        expiration: record.expiration,
                        sequence: seq,
                    });
                }
                crate::runtime::WriteBatchOp::Delete { cf_id, key } => {
                    let key_b = Bytes::from(key);

                    let mut record =
                        WalRecord::new_cf(cf_id, WalOpKind::Delete, key_b.clone(), None, seq);
                    record.txn_id = Some(txn_id);

                    if let Some(writer) = &mut self.writer {
                        writer.append_record(&record)?;
                    }

                    state.append_intent(IntentLogEntry::SeqnoAllocated { seqno: seq, cf_id })?;

                    state.wal.pending_writes += 1;
                    self.pending_sync_count += 1;
                    self.bytes_since_sync += record.estimated_size();

                    apply_ops.push(BatchApplyOp::Delete {
                        cf_id,
                        key: key_b.to_vec(),
                        sequence: seq,
                    });
                }
            }
        }

        // Write commit record
        let last_sequence = commit_seq;
        let mut commit_record =
            WalRecord::new_cf(0, WalOpKind::TxnCommit, marker_key, None, commit_seq);
        commit_record.txn_id = Some(txn_id);
        if let Some(writer) = &mut self.writer {
            writer.append_record(&commit_record)?;
        }

        state.wal.pending_writes += 1;
        self.pending_sync_count += 1;
        self.bytes_since_sync += commit_record.estimated_size();

        // Apply durability policy (single sync for the whole batch, where relevant).
        match self.durability_policy {
            DurabilityPolicy::Strict | DurabilityPolicy::CloudMirrored => {
                self.sync_internal(state)?;
                state.wal.local_durable_seq = last_sequence;
            }
            DurabilityPolicy::Batched => {
                // no-op; background/timer can sync later
            }
            DurabilityPolicy::CloudFirst => {
                // no local durability boundary; visibility gated on cloud ack.
            }
        }

        let op_count = apply_ops.len();

        // For batched mode, mark the sequence range as pending atomicity
        // Reads at sequences >= begin_seq must wait for the batch to become durable
        if matches!(self.durability_policy, DurabilityPolicy::Batched) {
            state.pending_batch_min_seq = Some(begin_seq);
        }

        if self.is_cloud_first() {
            // === CRITICAL: Check backpressure before queueing batch ===
            if self.should_apply_backpressure() {
                tracing::warn!(
                    op_count,
                    pending_count = self.pending_cloud_writes.len(),
                    pending_bytes = self.pending_cloud_write_bytes,
                    "CloudFirst batch stall: pending queue at capacity"
                );
                return Err(MidgeError::WriteStall(
                    "CloudFirst pending queue at capacity; cloud upload too slow".to_string(),
                ));
            }

            // Check for timed-out writes
            let timed_out = self.count_timed_out_writes();
            if timed_out > 0 {
                tracing::error!(
                    timed_out_count = timed_out,
                    timeout_secs = CLOUD_UPLOAD_TIMEOUT.as_secs(),
                    "CloudFirst batch timeout: pending writes not acknowledged by cloud"
                );
                return Err(MidgeError::Internal(
                    format!("{} pending writes exceeded cloud upload timeout", timed_out)
                ));
            }

            // Atomic visibility after cloud durability (gate on commit seq).
            let batch_estimated_bytes: usize = apply_ops
                .iter()
                .map(|op| match op {
                    BatchApplyOp::Put {
                        key, value, ..
                    } => key.len() + value.len() + 64,
                    BatchApplyOp::Delete { key, .. } => key.len() + 64,
                })
                .sum();
            self.pending_cloud_write_bytes += batch_estimated_bytes;

            self.pending_cloud_writes
                .push_back(PendingCloudWrite::Batch {
                    commit_sequence: last_sequence,
                    ops: apply_ops,
                    enqueued_at: Instant::now(),
                });

            tracing::trace!(
                commit_sequence = last_sequence,
                op_count,
                pending_count = self.pending_cloud_writes.len(),
                pending_bytes = self.pending_cloud_write_bytes,
                "Queued batch for cloud durability"
            );
        } else {
            // Apply to memtables in-order (atomic visibility within the actor).
            for apply_op in apply_ops {
                match apply_op {
                    BatchApplyOp::Put {
                        cf_id,
                        key,
                        value,
                        expiration,
                        sequence,
                    } => {
                        self.apply_to_memtable(
                            state,
                            sequence,
                            cf_id,
                            &key,
                            &Some(Bytes::from(value)),
                            expiration,
                        )?;
                    }
                    BatchApplyOp::Delete { cf_id, key, sequence } => {
                        self.apply_to_memtable(state, sequence, cf_id, &key, &None, None)?;
                    }
                }
            }
        }

        tracing::trace!(txn_id, last_sequence, op_count, "WAL batch append");

        // Return deferred=true if using group commit (Batched or CloudFirst modes)
        let deferred = matches!(
            self.durability_policy,
            DurabilityPolicy::Batched | DurabilityPolicy::CloudFirst
        );
        Ok((last_sequence, op_count, deferred))
    }

    /// Append a merge operand to the WAL
    pub fn append_merge(
        &mut self,
        state: &mut RuntimeState,
        request_id: u64,
        cf_id: u32,
        key: Bytes,
        operand: Bytes,
    ) -> MidgeResult<(u64, bool)> {
        // 🔑 CRITICAL: Allocate sequence idempotently using request_id.
        let (first_seq, _count) = state.allocate_sequences_idempotent(request_id, 1);
        let sequence = first_seq;

        // Create WAL record for merge
        let record = WalRecord::new_cf(
            cf_id,
            WalOpKind::Merge,
            key.clone(),
            Some(operand.clone()),
            sequence,
        );

        // Calculate record size for batching
        let record_size = record.key.len() + record.value.as_ref().map_or(0, |v| v.len());

        // ALWAYS append to local WAL first (FsWalWriter)
        if let Some(writer) = &self.writer {
            writer.append_record(&record)?;
        }

        // Update state tracking
        state.wal.pending_writes += 1;
        self.pending_sync_count += 1;
        self.bytes_since_sync += record_size;

        // Apply durability policy (same as regular append)
        match self.durability_policy {
            DurabilityPolicy::Strict => {
                // Fsync immediately, then apply to memtable
                self.sync_internal(state)?;
                state.wal.local_durable_seq = sequence;

                // Apply merge operand to memtable
                self.apply_merge_to_memtable(state, cf_id, &key, &operand, sequence)?;
            }
            DurabilityPolicy::Batched => {
                // Apply to memtable immediately, but defer response until fsync completes.
                // Caller joins the group commit waiter queue; sync completion notifies all.
                self.apply_merge_to_memtable(state, cf_id, &key, &operand, sequence)?;
                // Return deferred=true so caller joins group commit
            }
            DurabilityPolicy::CloudMirrored => {
                // Fsync locally, apply to memtable, schedule cloud upload in background
                self.sync_internal(state)?;
                state.wal.local_durable_seq = sequence;

                // Apply merge operand to memtable
                self.apply_merge_to_memtable(state, cf_id, &key, &operand, sequence)?;

                // TODO: mirror to cloud (not via CloudFirst pending queue)
            }
            DurabilityPolicy::CloudFirst => {}
        }

        if self.is_cloud_first() {
            // CloudFirst: gate visibility (and response) on cloud durability.
            self.queue_cloud_merge(cf_id, key.to_vec(), operand.to_vec(), sequence);
        }

        tracing::trace!(cf_id, sequence, policy = ?self.durability_policy, "WAL merge append");

        // Return deferred=true if using group commit (Batched or CloudFirst modes)
        let deferred = matches!(
            self.durability_policy,
            DurabilityPolicy::Batched | DurabilityPolicy::CloudFirst
        );
        Ok((sequence, deferred))
    }

    /// Checks current in-memory view (active + immutable memtables) for existence
    fn key_exists(&self, state: &RuntimeState, cf_id: u32, key: &[u8]) -> bool {
        if let Some(cf_state) = state.column_families.get(&cf_id) {
            if let Ok(Some(_)) = cf_state.memtable.get(key) {
                return true;
            }
            for imm in cf_state.immutable_memtables.iter().rev() {
                if let Ok(Some(_)) = imm.get(key) {
                    return true;
                }
            }
        }
        false
    }

    fn key_exists_or_pending(&self, state: &RuntimeState, cf_id: u32, key: &[u8]) -> bool {
        if self.key_exists(state, cf_id, key) {
            return true;
        }

        if !self.is_cloud_first() {
            return false;
        }

        for pending in &self.pending_cloud_writes {
            match pending {
                PendingCloudWrite::Single {
                    cf_id: p_cf,
                    key: p_key,
                    ..
                } => {
                    if *p_cf == cf_id && p_key.as_slice() == key {
                        return true;
                    }
                }
                PendingCloudWrite::Merge {
                    cf_id: p_cf,
                    key: p_key,
                    ..
                } => {
                    if *p_cf == cf_id && p_key.as_slice() == key {
                        return true;
                    }
                }
                PendingCloudWrite::Batch { ops, .. } => {
                    for op in ops {
                        match op {
                            BatchApplyOp::Put {
                                cf_id: p_cf,
                                key: p_key,
                                ..
                            }
                            | BatchApplyOp::Delete {
                                cf_id: p_cf,
                                key: p_key,
                                sequence: _
                            } => {
                                if *p_cf == cf_id && p_key.as_slice() == key {
                                    return true;
                                }
                            }
                        }
                    }
                }
            }
        }

        false
    }

    /// Apply a write to the memtable
    fn apply_to_memtable(
        &self,
        state: &RuntimeState,
        sequence: u64,
        cf_id: u32,
        key: &[u8],
        value: &Option<Bytes>,
        expiration: Option<u64>,
    ) -> MidgeResult<()> {
        if let Some(cf_state) = state.column_families.get(&cf_id) {
            if let Some(val) = value {
                cf_state
                    .memtable
                    .as_ref()
                    .put_with_seq(key.to_vec(), val.to_vec(), sequence, expiration)?;
            } else {
                cf_state
                    .memtable
                    .as_ref()
                    .delete_with_seq(key.to_vec(), sequence)?;
            }
        }
        Ok(())
    }

    /// Apply a merge operand to the memtable
    fn apply_merge_to_memtable(
        &self,
        state: &RuntimeState,
        cf_id: u32,
        key: &[u8],
        operand: &Bytes,
        sequence: u64,
    ) -> MidgeResult<()> {
        if let Some(cf_state) = state.column_families.get(&cf_id) {
            cf_state
                .memtable
                .as_ref()
                .merge_with_seq(key.to_vec(), operand.to_vec(), sequence)?;
        }
        Ok(())
    }

    /// Internal sync helper - fsyncs the writer
    fn sync_internal(&mut self, state: &mut RuntimeState) -> MidgeResult<()> {
        if let Some(writer) = &mut self.writer {
            let start = Instant::now();
            writer.sync()?;
            let elapsed = start.elapsed();

            self.sync_calls += 1;
            self.sync_total += elapsed;

            if std::env::var_os("MIDGE_TRACE_WAL_SYNC").is_some()
                && self.sync_calls.is_multiple_of(1000)
            {
                let avg_ms = (self.sync_total.as_secs_f64() * 1000.0) / (self.sync_calls as f64);
                eprintln!(
                    "[midge] wal.sync: calls={} total_ms={:.2} avg_ms={:.3}",
                    self.sync_calls,
                    self.sync_total.as_secs_f64() * 1000.0,
                    avg_ms
                );
            }
        }

        state.wal.last_synced_seq = state.sequence;
        state.wal.local_durable_seq = state.sequence;
        state.wal.pending_writes = 0;
        self.pending_sync_count = 0;
        self.bytes_since_sync = 0;

        Ok(())
    }

    /// Sync WAL to disk (public interface).
    ///
    /// Returns the sealed flush generation. In group commit modes (Batched/CloudFirst),
    /// all pending writes at sync time are grouped under this generation.
    pub fn sync(&mut self, state: &mut RuntimeState) -> MidgeResult<u64> {
        let pending = state.wal.pending_writes;
        let sealed_generation = self.flush_generation;

        self.sync_internal(state)?;

        // Advance to next generation for next batch
        self.flush_generation += 1;

        tracing::debug!(
            pending_writes = pending,
            synced_seq = state.wal.last_synced_seq,
            local_durable = state.wal.local_durable_seq,
            sealed_generation,
            "WAL sync"
        );

        Ok(sealed_generation)
    }

    /// Flush WAL buffers without fsync.
    ///
    /// CloudFirst durability uses local WAL as a staging file for upload.
    /// We avoid fsync on every write, but do a flush+fsync only when sealing
    /// a segment right before upload so the uploader reads a complete file.
    pub fn flush_for_cloud_upload(&mut self, state: &mut RuntimeState) -> MidgeResult<()> {
        let pending = state.wal.pending_writes;

        if let Some(writer) = &mut self.writer {
            writer.flush()?;
        }

        // Treat everything appended so far as ready-to-ship.
        state.wal.last_synced_seq = state.sequence;
        state.wal.local_durable_seq = state.sequence;
        state.wal.pending_writes = 0;
        self.pending_sync_count = 0;
        self.bytes_since_sync = 0;

        tracing::debug!(
            pending_writes = pending,
            flushed_seq = state.wal.last_synced_seq,
            "WAL flush (CloudFirst upload)"
        );

        Ok(())
    }

    /// Check if batched sync should trigger
    pub fn should_sync_batch(&self) -> bool {
        // TODO: Add time-based check (max_delay_ms)
        const MAX_BATCH_BYTES: usize = 64 * 1024; // 64KB
        self.bytes_since_sync >= MAX_BATCH_BYTES
    }

    /// Rotate to a new WAL segment
    pub fn rotate(&mut self, state: &mut RuntimeState) -> MidgeResult<()> {
        let old_segment = state.wal.current_segment_id;

        // Close the current writer before renaming
        let _ = self.writer.take();

        if let Some(fs) = &self.wal_fs {
            // Rename wal.log to {old_segment}.wal
            let old_path = FsPath::new("wal.log");
            let new_path = FsPath::new(format!("{old_segment}.wal"));

            // Rename may fail if file doesn't exist (e.g., in memory mode)
            let _ = fs.rename_atomic(&old_path, &new_path);

            // Create new writer for the next segment
            let factory = FsWalFactoryIo::new(Arc::clone(fs));
            self.writer = Some(factory.create_writer("wal.log")?);
        }

        state.wal.current_segment_id += 1;

        tracing::info!(
            old_segment,
            new_segment = state.wal.current_segment_id,
            "WAL rotate"
        );

        Ok(())
    }

    /// Handle cloud upload completion (CloudFirst durability)
    ///
    /// Updates cloud_durable_seq and completes any pending writes
    /// by applying them to memtable.
    pub fn handle_cloud_upload_complete(
        &mut self,
        state: &mut RuntimeState,
        segment_id: u64,
        max_seq_in_segment: u64,
    ) -> MidgeResult<()> {
        // Update cloud durability frontier
        state.wal.cloud_durable_seq = state.wal.cloud_durable_seq.max(max_seq_in_segment);

        tracing::debug!(
            segment_id,
            cloud_durable_seq = state.wal.cloud_durable_seq,
            "Cloud upload complete"
        );

        // Apply pending writes to memtable.
        while let Some(pending) = self.pending_cloud_writes.front() {
            let gate_seq = match pending {
                PendingCloudWrite::Single { sequence, .. } => *sequence,
                PendingCloudWrite::Merge { sequence, .. } => *sequence,
                PendingCloudWrite::Batch {
                    commit_sequence, ..
                } => *commit_sequence,
            };

            if gate_seq > state.wal.cloud_durable_seq {
                break;
            }

            let pending = self
                .pending_cloud_writes
                .pop_front()
                .expect("pending write exists after front() check");

            // Decrement pending cloud write bytes as we dequeue
            let dequeued_bytes = match &pending {
                PendingCloudWrite::Single { key, value, .. } => {
                    key.len() + value.as_ref().map_or(0, |v| v.len()) + 64
                }
                PendingCloudWrite::Merge {
                    key, operand, ..
                } => key.len() + operand.len() + 64,
                PendingCloudWrite::Batch { ops, .. } => {
                    ops.iter()
                        .map(|op| match op {
                            BatchApplyOp::Put { key, value, .. } => key.len() + value.len() + 64,
                            BatchApplyOp::Delete { key, .. } => key.len() + 64,
                        })
                        .sum()
                }
            };
            self.pending_cloud_write_bytes = self.pending_cloud_write_bytes.saturating_sub(dequeued_bytes);

            match pending {
                PendingCloudWrite::Single {
                    cf_id,
                    key,
                    value,
                    sequence,
                    expiration,
                    enqueued_at,
                } => {
                    let wait_time = Instant::now().duration_since(enqueued_at);
                    tracing::debug!(
                        sequence,
                        wait_ms = wait_time.as_millis(),
                        "Applying pending single write after cloud durability"
                    );
                    let key_bytes = Bytes::from(key);
                    let value_bytes = value.map(Bytes::from);
                    self.apply_to_memtable(state, sequence, cf_id, &key_bytes, &value_bytes, expiration)?;
                }
                PendingCloudWrite::Merge {
                    cf_id,
                    key,
                    operand,
                    sequence,
                    enqueued_at,
                } => {
                    let wait_time = Instant::now().duration_since(enqueued_at);
                    tracing::debug!(
                        sequence,
                        wait_ms = wait_time.as_millis(),
                        "Applying pending merge after cloud durability"
                    );
                    let operand_bytes = Bytes::from(operand);
                    self.apply_merge_to_memtable(state, cf_id, &key, &operand_bytes, sequence)?;
                }
                PendingCloudWrite::Batch {
                    commit_sequence,
                    ops,
                    enqueued_at,
                } => {
                    let wait_time = Instant::now().duration_since(enqueued_at);
                    let op_count = ops.len();
                    tracing::debug!(
                        commit_sequence,
                        op_count,
                        wait_ms = wait_time.as_millis(),
                        "Applying pending batch after cloud durability"
                    );
                    for op in ops {
                        match op {
                            BatchApplyOp::Put {
                                cf_id,
                                key,
                                value,
                                expiration,
                                sequence,
                            } => {
                                self.apply_to_memtable(
                                    state,
                                    sequence,
                                    cf_id,
                                    &key,
                                    &Some(Bytes::from(value)),
                                    expiration,
                                )?;
                            }
                            BatchApplyOp::Delete { cf_id, key, sequence } => {
                                self.apply_to_memtable(state, sequence, cf_id, &key, &None, None)?;
                            }
                        }
                    }
                }
            }
        }

        Ok(())
    }

    pub fn handle_cloud_upload_failed(&mut self, segment_id: u64, error: &str) {
        tracing::error!(segment_id, error, "Cloud upload failed");

        // Conservative behavior: fail all pending CloudFirst requests.
        // We cannot claim durability for any queued writes.
        while let Some(pending) = self.pending_cloud_writes.pop_front() {
            match pending {
                PendingCloudWrite::Single { .. }
                | PendingCloudWrite::Merge { .. }
                | PendingCloudWrite::Batch { .. } => {}
            }
        }
    }

    /// Queue a write waiting for cloud durability (CloudFirst mode)
    pub fn queue_cloud_write(
        &mut self,
        cf_id: u32,
        key: Vec<u8>,
        value: Option<Vec<u8>>,
        sequence: u64,
        expiration: Option<u64>,
    ) {
        let estimated_bytes = key.len() + value.as_ref().map_or(0, |v| v.len()) + 64; // 64 for overhead
        self.pending_cloud_write_bytes += estimated_bytes;

        self.pending_cloud_writes
            .push_back(PendingCloudWrite::Single {
                cf_id,
                key,
                value,
                sequence,
                expiration,
                enqueued_at: Instant::now(),
            });

        tracing::trace!(
            sequence,
            pending_count = self.pending_cloud_writes.len(),
            pending_bytes = self.pending_cloud_write_bytes,
            "Queued write for cloud durability"
        );
    }

    /// Queue a merge operand waiting for cloud durability (CloudFirst mode)
    pub fn queue_cloud_merge(&mut self, cf_id: u32, key: Vec<u8>, operand: Vec<u8>, sequence: u64) {
        let estimated_bytes = key.len() + operand.len() + 64; // 64 for overhead
        self.pending_cloud_write_bytes += estimated_bytes;

        self.pending_cloud_writes
            .push_back(PendingCloudWrite::Merge {
                cf_id,
                key,
                operand,
                sequence,
                enqueued_at: Instant::now(),
            });

        tracing::trace!(
            sequence,
            pending_count = self.pending_cloud_writes.len(),
            pending_bytes = self.pending_cloud_write_bytes,
            "Queued merge for cloud durability"
        );
    }

    /// Handle sync completion notification
    pub fn handle_sync_complete(&mut self, state: &mut RuntimeState, segment_id: u64) {
        tracing::debug!(segment_id, "WAL sync complete");

        // Update last synced info if this is newer
        if segment_id >= state.wal.current_segment_id {
            // This sync covers the current segment
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::runtime::RuntimeState;
    use bytes::Bytes;
    use std::path::PathBuf;

    #[test]
    fn should_apply_wal_sequence_to_memtable() -> MidgeResult<()> {
        // Arrange: start with a large sequence so memtable's local seq would differ
        let mut state = RuntimeState::new(PathBuf::from("/tmp/test"), true);
        state.sequence = 100; // ensure WAL sequence is distinct from memtable's internal counter

        let mut wal_actor = WalActor::new(PathBuf::from("/tmp/test"), DurabilityPolicy::Strict, true)?;

        // Act: append a single put
        let (seq, deferred) = wal_actor.append(
            &mut state,
            1, // request_id
            0, // cf_id
            Bytes::from("k"),
            Some(Bytes::from("v")),
            false,
            None,
        )?;

        // Assert: memtable contains one entry and its seq equals WAL seq
        assert!(!deferred);
        let cf_state = state.get_cf(0).expect("cf exists");
        let entries = cf_state.memtable.iter_all(u64::MAX);
        assert_eq!(entries.len(), 1);
        let (key, value, m_seq) = &entries[0];
        assert_eq!(key.as_slice(), b"k");
        assert_eq!(value.as_ref().unwrap().as_slice(), b"v");
        assert_eq!(*m_seq, seq);

        Ok(())
    }

    #[test]
    fn should_apply_merge_sequence_to_memtable() -> MidgeResult<()> {
        // Arrange
        let mut state = RuntimeState::new(PathBuf::from("/tmp/test"), true);
        state.sequence = 50;

        let mut wal_actor = WalActor::new(PathBuf::from("/tmp/test"), DurabilityPolicy::Strict, true)?;

        // Act: append a merge operand
        let (seq, deferred) = wal_actor.append_merge(
            &mut state,
            2, // request_id
            0, // cf_id
            Bytes::from("mk"),
            Bytes::from("op"),
        )?;

        // Assert
        assert!(!deferred);
        let cf_state = state.get_cf(0).expect("cf exists");
        let entries = cf_state.memtable.iter_all(u64::MAX);
        // Merge operand should exist as an entry
        assert_eq!(entries.len(), 1);
        let (key, value, m_seq) = &entries[0];
        assert_eq!(key.as_slice(), b"mk");
        assert_eq!(value.as_ref().unwrap().as_slice(), b"op");
        assert_eq!(*m_seq, seq);

        Ok(())
    }
}

impl Default for WalActor {
    fn default() -> Self {
        // Cannot create with default since we need a WAL directory
        panic!("WalActor::default() should not be called, use WalActor::new(wal_dir)")
    }
}
