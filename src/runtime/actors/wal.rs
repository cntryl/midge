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
use crate::sst::Memtable;
use crate::wal::{DurabilityPolicy, FsWalFactory, WalFactory, WalOpKind, WalRecord, WalWriter};
use bytes::Bytes;
use std::collections::VecDeque;
use std::path::PathBuf;
use std::time::{Duration, Instant};
use crate::runtime::IntentLogEntry;

#[derive(Debug)]
enum PendingCloudWrite {
    Single {
        cf_id: u32,
        key: Vec<u8>,
        value: Option<Vec<u8>>,
        sequence: u64,
        expiration: Option<u64>,
    },
    Merge {
        cf_id: u32,
        key: Vec<u8>,
        operand: Vec<u8>,
        sequence: u64,
    },
    Batch {
        commit_sequence: u64,
        ops: Vec<BatchApplyOp>,
    },
}

#[derive(Debug)]
enum BatchApplyOp {
    Put {
        cf_id: u32,
        key: Vec<u8>,
        value: Vec<u8>,
        expiration: Option<u64>,
    },
    Delete {
        cf_id: u32,
        key: Vec<u8>,
    },
}

/// Actor handling WAL operations
pub struct WalActor {
    /// WAL writer (owned by this actor)
    writer: Option<Box<dyn WalWriter>>,
    /// WAL directory
    wal_dir: PathBuf,
    /// Buffered writes pending sync
    pending_sync_count: usize,
    /// Durability policy (determines sync behavior)
    durability_policy: DurabilityPolicy,
    /// Pending writes waiting for cloud durability (CloudFirst mode only)
    /// These writes are in local WAL but NOT in memtable yet
    pending_cloud_writes: VecDeque<PendingCloudWrite>,
    /// Bytes written since last sync (for batched mode)
    bytes_since_sync: usize,

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
        let writer = if memory_mode {
            None
        } else {
            let factory = FsWalFactory;
            Some(factory.create_writer(&wal_dir)?)
        };

        Ok(Self {
            writer,
            wal_dir,
            pending_sync_count: 0,
            durability_policy,
            pending_cloud_writes: VecDeque::new(),
            bytes_since_sync: 0,
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

    pub fn bytes_since_sync(&self) -> usize {
        self.bytes_since_sync
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
    #[allow(clippy::too_many_arguments)]
    pub fn append(
        &mut self,
        state: &mut RuntimeState,
        _request_id: u64,
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
        // Allocate sequence number at append time to preserve a total order under concurrency.
        let sequence = state.next_sequence();

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
                self.apply_to_memtable(state, cf_id, &key, &value, record.expiration)?;
            }
            DurabilityPolicy::Batched => {
                // Apply to memtable immediately (no cloud wait)
                self.apply_to_memtable(state, cf_id, &key, &value, record.expiration)?;
                // Sync if batch thresholds exceeded (handled by caller/timer)
            }
            DurabilityPolicy::CloudMirrored => {
                // Fsync locally, apply to memtable, schedule cloud upload in background
                self.sync_internal(state)?;
                state.wal.local_durable_seq = sequence;

                // Apply to memtable - local durability sufficient
                self.apply_to_memtable(state, cf_id, &key, &value, record.expiration)?;

                // TODO: Send CloudUploadWal message to CloudActor
            }
            DurabilityPolicy::CloudFirst => {
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

        Ok((sequence, self.is_cloud_first()))
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
        state.append_intent(IntentLogEntry::SeqnoAllocated { seqno: begin_seq, cf_id: 0 })?;
        // Per-op seqs will be logged below when we know cf_id
        // Commit seq
        state.append_intent(IntentLogEntry::SeqnoAllocated { seqno: commit_seq, cf_id: 0 })?;

        // Create and write begin record
        let mut begin_record = WalRecord::new_cf(0, WalOpKind::TxnBegin, marker_key.clone(), None, begin_seq);
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
                crate::runtime::WriteBatchOp::Put { cf_id, key, value, ttl_seconds } => {
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
                        _ => WalRecord::new_cf(cf_id, WalOpKind::Put, key_b.clone(), Some(value_b.clone()), seq),
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
                    });
                }
                crate::runtime::WriteBatchOp::Delete { cf_id, key } => {
                    let key_b = Bytes::from(key);

                    let mut record = WalRecord::new_cf(cf_id, WalOpKind::Delete, key_b.clone(), None, seq);
                    record.txn_id = Some(txn_id);

                    if let Some(writer) = &mut self.writer {
                        writer.append_record(&record)?;
                    }

                    state.append_intent(IntentLogEntry::SeqnoAllocated { seqno: seq, cf_id })?;

                    state.wal.pending_writes += 1;
                    self.pending_sync_count += 1;
                    self.bytes_since_sync += record.estimated_size();

                    apply_ops.push(BatchApplyOp::Delete { cf_id, key: key_b.to_vec() });
                }
            }
        }

        // Write commit record
        let last_sequence = commit_seq;
        let mut commit_record = WalRecord::new_cf(0, WalOpKind::TxnCommit, marker_key, None, commit_seq);
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

        if self.is_cloud_first() {
            // Atomic visibility after cloud durability (gate on commit seq).
            self.pending_cloud_writes
                .push_back(PendingCloudWrite::Batch {
                    commit_sequence: last_sequence,
                    ops: apply_ops,
                });
        } else {
            // Apply to memtables in-order (atomic visibility within the actor).
            for apply_op in apply_ops {
                match apply_op {
                    BatchApplyOp::Put {
                        cf_id,
                        key,
                        value,
                        expiration,
                    } => {
                        self.apply_to_memtable(
                            state,
                            cf_id,
                            &key,
                            &Some(Bytes::from(value)),
                            expiration,
                        )?;
                    }
                    BatchApplyOp::Delete { cf_id, key } => {
                        self.apply_to_memtable(state, cf_id, &key, &None, None)?;
                    }
                }
            }
        }

        tracing::trace!(txn_id, last_sequence, op_count, "WAL batch append");

        Ok((last_sequence, op_count, self.is_cloud_first()))
    }

    /// Append a merge operand to the WAL
    pub fn append_merge(
        &mut self,
        state: &mut RuntimeState,
        cf_id: u32,
        key: Bytes,
        operand: Bytes,
    ) -> MidgeResult<(u64, bool)> {
        // Allocate sequence number at append time to preserve a total order under concurrency.
        let sequence = state.next_sequence();

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
                self.apply_merge_to_memtable(state, cf_id, &key, &operand)?;
            }
            DurabilityPolicy::Batched => {
                // Apply to memtable immediately (no cloud wait)
                self.apply_merge_to_memtable(state, cf_id, &key, &operand)?;
                // Sync if batch thresholds exceeded (handled by caller/timer)
            }
            DurabilityPolicy::CloudMirrored => {
                // Fsync locally, apply to memtable, schedule cloud upload in background
                self.sync_internal(state)?;
                state.wal.local_durable_seq = sequence;

                // Apply merge operand to memtable
                self.apply_merge_to_memtable(state, cf_id, &key, &operand)?;

                // TODO: mirror to cloud (not via CloudFirst pending queue)
            }
            DurabilityPolicy::CloudFirst => {}
        }

        if self.is_cloud_first() {
            // CloudFirst: gate visibility (and response) on cloud durability.
            self.queue_cloud_merge(cf_id, key.to_vec(), operand.to_vec(), sequence);
        }

        tracing::trace!(cf_id, sequence, policy = ?self.durability_policy, "WAL merge append");

        Ok((sequence, self.is_cloud_first()))
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
                    .put_with_exp(key.to_vec(), val.to_vec(), expiration)?;
            } else {
                cf_state.memtable.as_ref().delete(key.to_vec())?;
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
    ) -> MidgeResult<()> {
        if let Some(cf_state) = state.column_families.get(&cf_id) {
            cf_state
                .memtable
                .as_ref()
                .merge(key.to_vec(), operand.to_vec())?;
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

            if std::env::var_os("MIDGE_TRACE_WAL_SYNC").is_some() && self.sync_calls % 1000 == 0 {
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

    /// Sync WAL to disk (public interface)
    pub fn sync(&mut self, state: &mut RuntimeState) -> MidgeResult<()> {
        let pending = state.wal.pending_writes;

        self.sync_internal(state)?;

        tracing::debug!(
            pending_writes = pending,
            synced_seq = state.wal.last_synced_seq,
            local_durable = state.wal.local_durable_seq,
            "WAL sync"
        );

        Ok(())
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

        // IMPORTANT (Windows): the active `wal.log` must be closed before `fs::rename`.
        // `FsWalFactory::rotate_writer` renames `wal.log` to `{segment_id}.wal`.
        // Renaming an open file is denied on Windows, so drop the writer first.
        let _ = self.writer.take();

        // Rotate via factory
        let factory = FsWalFactory;
        self.writer = Some(factory.rotate_writer(&self.wal_dir, old_segment)?);

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

            match pending {
                PendingCloudWrite::Single {
                    cf_id,
                    key,
                    value,
                    sequence,
                    expiration,
                } => {
                    let key_bytes = Bytes::from(key);
                    let value_bytes = value.map(Bytes::from);
                    self.apply_to_memtable(state, cf_id, &key_bytes, &value_bytes, expiration)?;
                    let _ = sequence;
                }
                PendingCloudWrite::Merge {
                    cf_id,
                    key,
                    operand,
                    sequence,
                } => {
                    let operand_bytes = Bytes::from(operand);
                    self.apply_merge_to_memtable(state, cf_id, &key, &operand_bytes)?;
                    let _ = sequence;
                }
                PendingCloudWrite::Batch {
                    commit_sequence,
                    ops,
                } => {
                    for op in ops {
                        match op {
                            BatchApplyOp::Put {
                                cf_id,
                                key,
                                value,
                                expiration,
                            } => {
                                self.apply_to_memtable(
                                    state,
                                    cf_id,
                                    &key,
                                    &Some(Bytes::from(value)),
                                    expiration,
                                )?;
                            }
                            BatchApplyOp::Delete { cf_id, key } => {
                                self.apply_to_memtable(state, cf_id, &key, &None, None)?;
                            }
                        }
                    }
                    let _ = commit_sequence;
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
        self.pending_cloud_writes
            .push_back(PendingCloudWrite::Single {
                cf_id,
                key,
                value,
                sequence,
                expiration,
            });

        tracing::trace!(sequence, "Queued write for cloud durability");
    }

    /// Queue a merge operand waiting for cloud durability (CloudFirst mode)
    pub fn queue_cloud_merge(&mut self, cf_id: u32, key: Vec<u8>, operand: Vec<u8>, sequence: u64) {
        self.pending_cloud_writes
            .push_back(PendingCloudWrite::Merge {
                cf_id,
                key,
                operand,
                sequence,
            });

        tracing::trace!(sequence, "Queued merge for cloud durability");
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

impl Default for WalActor {
    fn default() -> Self {
        // Cannot create with default since we need a WAL directory
        panic!("WalActor::default() should not be called, use WalActor::new(wal_dir)")
    }
}
