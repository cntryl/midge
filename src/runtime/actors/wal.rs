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
use super::cloud_write_queue::{
    CloudWriteQueue, PendingCloudWrite, TransactionApplyOp, CLOUD_UPLOAD_TIMEOUT,
};
use crate::common::MidgeError;
use crate::common::MidgeResult;
use crate::io::{Fs, FsPath, RealFs};
use crate::runtime::IntentLogEntry;
use crate::sst::Memtable;
use crate::wal::policy::BatchConfig;
use crate::wal::{DurabilityPolicy, FsWalFactoryIo, WalOpKind, WalRecord, WalWriter};
use bytes::Bytes;
use std::path::PathBuf;
use std::sync::Arc;
use std::time::{Duration, Instant};

/// Parameters for WAL append operation
pub struct AppendParams {
    pub request_id: u64,
    pub cf_id: crate::engine::ColumnFamilyId,
    pub key: Bytes,
    pub value: Option<Bytes>,
    pub insert_only: bool,
    pub ttl_seconds: Option<u64>,
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
    cloud_write_queue: CloudWriteQueue,
    /// Bytes written since last sync (for batched mode)
    bytes_since_sync: usize,
    /// Flush generation for batched/local durability group commit
    flush_generation: u64,

    /// Batch configuration governing max_delay_ms and max_bytes
    batch_config: BatchConfig,
    /// Last wall-clock time we performed a WAL fsync
    last_sync_instant: Instant,

    /// Fencing epoch assigned when this writer acquired leadership.
    /// Stamped on every WAL record so stale writers can be detected.
    current_epoch: u64,

    /// Optional leader store for epoch validation at sync boundaries.
    /// When set, each fsync checks that our epoch is still current.
    leader_store: Option<Arc<dyn crate::lease::LeaderStore>>,

    // === Optional instrumentation ===
    sync_calls: u64,
    sync_total: Duration,

    // WAL append instrumentation
    append_calls: u64,
    append_total: Duration,
}

impl WalActor {
    pub fn new(
        wal_dir: PathBuf,
        durability_policy: DurabilityPolicy,
        batch_config: BatchConfig,
        memory_mode: bool,
        writer_epoch: u64,
    ) -> MidgeResult<Self> {
        let (wal_fs, writer) = if memory_mode {
            (None, None)
        } else {
            let fs: Arc<dyn Fs> = Arc::new(RealFs::new(&wal_dir)?);
            let factory = FsWalFactoryIo::new(Arc::clone(&fs));
            let writer = Some(factory.create_writer("wal.log")?);
            (Some(fs), writer)
        };

        let actor = Self {
            writer,
            wal_fs,
            pending_sync_count: 0,
            durability_policy,
            cloud_write_queue: CloudWriteQueue::new(),
            bytes_since_sync: 0,
            flush_generation: 0,
            sync_calls: 0,
            sync_total: Duration::from_secs(0),
            append_calls: 0,
            append_total: Duration::from_secs(0),
            batch_config,
            last_sync_instant: Instant::now(),
            current_epoch: writer_epoch,
            leader_store: None,
        };

        // Log resolved WAL mode for diagnostics
        tracing::info!(
            wal_policy = ?actor.durability_policy,
            batching_enabled = matches!(actor.durability_policy, DurabilityPolicy::Batched) || matches!(actor.durability_policy, DurabilityPolicy::CloudFirst),
            max_delay_ms = actor.batch_config.max_delay_ms,
            max_bytes = actor.batch_config.max_bytes,
            "WAL actor initialized"
        );

        Ok(actor)
    }

    /// Helper that wraps writer.append_record with local counters and telemetry
    fn append_record_instrumented(
        &mut self,
        writer: &mut Box<dyn WalWriter>,
        record: &WalRecord,
    ) -> MidgeResult<()> {
        let a_start = Instant::now();
        writer.append_record(record)?;
        let a_elapsed = a_start.elapsed();
        self.append_calls += 1;
        self.append_total += a_elapsed;
        if let Some(t) = crate::telemetry::Telemetry::global() {
            t.metrics()
                .record_wal_append(record.estimated_size() as u64);
            t.metrics().record_wal_append_count();
            t.metrics()
                .record_wal_append_ns(a_elapsed.as_nanos() as u64);
        }
        Ok(())
    }

    pub fn durability_policy(&self) -> DurabilityPolicy {
        self.durability_policy
    }

    /// Attach a leader store for epoch validation at sync boundaries.
    pub fn set_leader_store(&mut self, store: Arc<dyn crate::lease::LeaderStore>) {
        self.leader_store = Some(store);
    }

    pub fn batch_config(&self) -> crate::wal::policy::BatchConfig {
        self.batch_config
    }

    /// Set durability policy and optional batch config at runtime
    pub fn set_durability(
        &mut self,
        policy: DurabilityPolicy,
        batch_config: crate::wal::policy::BatchConfig,
    ) -> MidgeResult<()> {
        // Update batch config and policy atomically
        self.batch_config = batch_config;
        self.durability_policy = policy;
        Ok(())
    }

    pub fn is_cloud_first(&self) -> bool {
        matches!(self.durability_policy, DurabilityPolicy::CloudFirst)
    }

    pub fn has_pending_cloud_writes(&self) -> bool {
        self.cloud_write_queue.has_pending_writes()
    }

    pub fn pending_cloud_writes_len(&self) -> usize {
        self.cloud_write_queue.len()
    }

    pub fn pending_cloud_write_bytes(&self) -> usize {
        self.cloud_write_queue.pending_bytes()
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

    /// Number of times sync_internal has been called (for tests/diagnostics)
    pub fn sync_calls(&self) -> u64 {
        self.sync_calls
    }

    /// Check if cloud write queue has hit backpressure limits
    /// Returns true if we should reject new writes to prevent memory exhaustion
    pub fn should_apply_backpressure(&self) -> bool {
        self.cloud_write_queue.should_apply_backpressure()
    }

    /// Check for timed-out pending cloud writes
    /// Returns number of writes that have exceeded CLOUD_UPLOAD_TIMEOUT
    pub fn count_timed_out_writes(&self) -> usize {
        self.cloud_write_queue.count_timed_out_writes()
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
    pub fn append(
        &mut self,
        state: &mut RuntimeState,
        params: AppendParams,
    ) -> MidgeResult<(u64, bool)> {
        let AppendParams {
            request_id,
            cf_id,
            key,
            value,
            insert_only,
            ttl_seconds,
        } = params;

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

        // If this request_id has already been confirmed as durable, we can
        // short-circuit and return the previously-allocated sequence without
        // writing another WAL record or queuing another pending cloud write.
        if let Some((_first, _cnt, confirmed_at)) =
            state.sequence_idempotency_cache.get(&request_id)
        {
            if *confirmed_at > 0 {
                tracing::debug!(
                    request_id = request_id,
                    sequence = sequence,
                    "idempotent request already confirmed; returning existing allocation"
                );
                // Not deferred — the sequence is already durable
                return Ok((sequence, false));
            }
        }

        // Determine operation kind: Delete if value is None, Put otherwise
        let op_kind = if value.is_none() {
            WalOpKind::Delete
        } else {
            WalOpKind::Put
        };

        // Create WAL record (with expiration if provided)
        let record = match ttl_seconds {
            Some(ttl) if ttl > 0 => WalRecord::new_with_ttl(
                cf_id,
                op_kind,
                key.clone(),
                value.clone(),
                sequence,
                ttl,
                self.current_epoch,
            ),
            _ => WalRecord::new_cf(
                cf_id,
                op_kind,
                key.clone(),
                value.clone(),
                sequence,
                self.current_epoch,
            ),
        };

        // Calculate record size for batching
        let record_size = record.estimated_size();

        // ALWAYS append to local WAL first (FsWalWriter) EXCEPT for BestEffort mode
        if let Some(writer) = &mut self.writer {
            if !matches!(self.durability_policy, DurabilityPolicy::BestEffort) {
                let a_start = Instant::now();
                writer.append_record(&record)?;
                let a_elapsed = a_start.elapsed();
                self.append_calls += 1;
                self.append_total += a_elapsed;
                // Instrumentation: record wal append bytes/count/latency
                if let Some(t) = crate::telemetry::Telemetry::global() {
                    t.metrics()
                        .record_wal_append(record.estimated_size() as u64);
                    t.metrics().record_wal_append_count();
                    t.metrics()
                        .record_wal_append_ns(a_elapsed.as_nanos() as u64);
                }
            }
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
                self.apply_to_memtable(
                    state,
                    sequence,
                    cf_id,
                    key.clone(),
                    value.clone(),
                    record.expiration,
                )?;
            }
            DurabilityPolicy::Batched => {
                // Apply to memtable immediately, but defer response until fsync completes.
                // Caller joins the group commit waiter queue; sync completion notifies all.
                self.apply_to_memtable(
                    state,
                    sequence,
                    cf_id,
                    key.clone(),
                    value.clone(),
                    record.expiration,
                )?;
                // Return deferred=true so caller joins group commit
            }
            DurabilityPolicy::CloudMirrored => {
                // Fsync locally, apply to memtable, schedule cloud upload in background
                self.sync_internal(state)?;
                state.wal.local_durable_seq = sequence;

                // Apply to memtable - local durability sufficient
                self.apply_to_memtable(
                    state,
                    sequence,
                    cf_id,
                    key.clone(),
                    value.clone(),
                    record.expiration,
                )?;

                // Schedule cloud upload of the current WAL segment
                // The segment has been synced locally; now background-upload to cloud.
                self.cloud_write_queue.enqueue_write(
                    cf_id,
                    key.to_vec(),
                    value.as_ref().map(|v| v.to_vec()),
                    sequence,
                    record.expiration,
                );
            }
            DurabilityPolicy::CloudFirst => {
                // === CRITICAL: Check backpressure before queueing ===
                // If cloud upload is stalled, pending queue grows without bound.
                // Prevent memory exhaustion by rejecting writes when queue is full.
                if self.should_apply_backpressure() {
                    tracing::warn!(
                        pending_count = self.cloud_write_queue.len(),
                        pending_bytes = self.cloud_write_queue.pending_bytes(),
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
                    return Err(MidgeError::Internal(format!(
                        "{} pending writes exceeded cloud upload timeout",
                        timed_out
                    )));
                }

                // CRITICAL: Apply to memtable immediately for background CloudFirst.
                // Cloud upload runs asynchronously; data must be visible for reads
                // without waiting for upload completion. This is the key difference
                // from the old CloudFirst behavior which blocked on cloud confirmation.
                self.apply_to_memtable(
                    state,
                    sequence,
                    cf_id,
                    key.clone(),
                    value.clone(),
                    record.expiration,
                )?;

                // Queue write for cloud durability confirmation (used for telemetry/monitoring).
                // Memtable update happened above; reads don't wait for cloud upload.
                self.cloud_write_queue.enqueue_write(
                    cf_id,
                    key.to_vec(),
                    value.as_ref().map(|v| v.to_vec()),
                    sequence,
                    record.expiration,
                );
            }
            DurabilityPolicy::BestEffort => {
                // Best-effort persistence: write directly to memtable only.
                // No fsync, no cloud upload, no group commit.
                // Data is visible for reads and can be flushed to SST, but not durable on crash before flush.
                // Safe for bulk loads where re-load is acceptable.
                self.apply_to_memtable(state, sequence, cf_id, key, value, record.expiration)?;
            }
        }

        tracing::trace!(cf_id = cf_id, sequence, policy = ?self.durability_policy, "WAL append");

        // Optional tracing for append averages
        if std::env::var_os("MIDGE_TRACE_WAL_APPEND").is_some()
            && self.append_calls.is_multiple_of(1000)
        {
            let avg_ms = (self.append_total.as_secs_f64() * 1000.0) / (self.append_calls as f64);
            eprintln!(
                "[midge] wal.append: calls={} total_ms={:.2} avg_ms={:.3}",
                self.append_calls,
                self.append_total.as_secs_f64() * 1000.0,
                avg_ms
            );
        }

        // Return deferred=true if using group commit (Batched or CloudFirst modes)
        let deferred = matches!(
            self.durability_policy,
            DurabilityPolicy::Batched | DurabilityPolicy::CloudFirst
        );
        Ok((sequence, deferred))
    }

    /// Append a delete range tombstone to WAL.
    ///
    /// This writes a single DeleteRange record covering [start_key, end_key).
    /// Much more efficient than scanning and deleting each key individually.
    pub fn append_delete_range(
        &mut self,
        state: &mut RuntimeState,
        request_id: u64,
        cf_id: u32,
        start_key: Bytes,
        end_key: Bytes,
    ) -> MidgeResult<(u64, bool)> {
        // Allocate sequence idempotently
        let (first_seq, _count) = state.allocate_sequences_idempotent(request_id, 1);
        let sequence = first_seq;

        // Check for idempotent retry that's already confirmed
        if let Some((_first, _cnt, confirmed_at)) =
            state.sequence_idempotency_cache.get(&request_id)
        {
            if *confirmed_at > 0 {
                tracing::debug!(
                    request_id = request_id,
                    sequence = sequence,
                    "idempotent delete_range already confirmed; returning existing allocation"
                );
                return Ok((sequence, false));
            }
        }

        // Create DeleteRange WAL record
        let record = WalRecord {
            cf_id,
            op: WalOpKind::DeleteRange,
            key: start_key.clone(),
            value: None,
            seq: sequence,
            expiration: None,
            range_end: Some(end_key.clone()),
            txn_id: None,
            writer_epoch: self.current_epoch,
            compression: None,
        };

        let record_size = record.estimated_size();

        // Append to local WAL
        if let Some(writer) = &mut self.writer {
            let a_start = Instant::now();
            writer.append_record(&record)?;
            let a_elapsed = a_start.elapsed();
            self.append_calls += 1;
            self.append_total += a_elapsed;
            if let Some(t) = crate::telemetry::Telemetry::global() {
                t.metrics()
                    .record_wal_append(record.estimated_size() as u64);
                t.metrics().record_wal_append_count();
                t.metrics()
                    .record_wal_append_ns(a_elapsed.as_nanos() as u64);
            }
        }

        // Update state tracking
        state.wal.pending_writes += 1;
        self.pending_sync_count += 1;
        self.bytes_since_sync += record_size;

        // Apply durability policy
        match self.durability_policy {
            DurabilityPolicy::Strict => {
                self.sync_internal(state)?;
                state.wal.local_durable_seq = sequence;
                if let Some(range_end) = record.range_end.as_ref() {
                    self.apply_delete_range_to_memtable(
                        state,
                        sequence,
                        cf_id,
                        record.key.as_ref(),
                        range_end.as_ref(),
                    )?;
                }
            }
            DurabilityPolicy::Batched => {
                // Apply to memtable immediately, but defer response until fsync completes
                if let Some(range_end) = record.range_end.as_ref() {
                    self.apply_delete_range_to_memtable(
                        state,
                        sequence,
                        cf_id,
                        record.key.as_ref(),
                        range_end.as_ref(),
                    )?;
                }
                // Return deferred=true so caller joins group commit
            }
            DurabilityPolicy::CloudMirrored => {
                self.sync_internal(state)?;
                state.wal.local_durable_seq = sequence;
                if let Some(range_end) = record.range_end.as_ref() {
                    self.apply_delete_range_to_memtable(
                        state,
                        sequence,
                        cf_id,
                        record.key.as_ref(),
                        range_end.as_ref(),
                    )?;
                }
            }
            DurabilityPolicy::CloudFirst => {
                if self.should_apply_backpressure() {
                    tracing::warn!(
                        pending_count = self.cloud_write_queue.len(),
                        pending_bytes = self.cloud_write_queue.pending_bytes(),
                        "CloudFirst write stall on delete_range"
                    );
                    return Err(MidgeError::WriteStall(
                        "CloudFirst pending queue at capacity".to_string(),
                    ));
                }

                let timed_out = self.count_timed_out_writes();
                if timed_out > 0 {
                    return Err(MidgeError::Internal(format!(
                        "{} pending writes exceeded cloud upload timeout",
                        timed_out
                    )));
                }

                // CRITICAL: Apply to memtable immediately for background CloudFirst.
                // Cloud upload runs asynchronously; deletions must be visible immediately.
                if let Some(range_end) = record.range_end.as_ref() {
                    self.apply_delete_range_to_memtable(
                        state,
                        sequence,
                        cf_id,
                        record.key.as_ref(),
                        range_end.as_ref(),
                    )?;
                }

                // Queue for cloud confirmation (used for telemetry/monitoring).
                self.cloud_write_queue.enqueue_delete_range(
                    cf_id,
                    record.key.to_vec(),
                    record
                        .range_end
                        .as_ref()
                        .map(|b| b.to_vec())
                        .unwrap_or_default(),
                    sequence,
                );
            }
            DurabilityPolicy::BestEffort => {
                // Skip WAL for delete range - apply to memtable only
                if let Some(range_end) = record.range_end.as_ref() {
                    self.apply_delete_range_to_memtable(
                        state,
                        sequence,
                        cf_id,
                        record.key.as_ref(),
                        range_end.as_ref(),
                    )?;
                }
            }
        }

        tracing::trace!(cf_id = cf_id, sequence, policy = ?self.durability_policy, "WAL append_delete_range");

        let deferred = matches!(
            self.durability_policy,
            DurabilityPolicy::Batched | DurabilityPolicy::CloudFirst
        );
        Ok((sequence, deferred))
    }

    /// Apply a transaction's operations to the WAL as a single atomic unit.
    ///
    /// This method:
    /// - allocates a single transaction id
    /// - writes TxnBegin marker (unless BestEffort)
    /// - writes all operation records (in order, unless BestEffort)
    /// - writes TxnCommit marker at the end of the transaction path (unless BestEffort)
    /// - applies all operations to memtables (in order)
    ///
    /// The `durability_policy` parameter allows per-request durability control.
    /// If Some(DurabilityPolicy::BestEffort), WAL writes are skipped entirely
    /// (only memtable updates happen) for maximum bulk-load performance.
    /// If None, uses the actor's configured durability policy.
    ///
    /// Returns the last allocated sequence number for the batch.
    pub fn append_transaction(
        &mut self,
        state: &mut RuntimeState,
        _request_id: u64,
        ops: Vec<crate::runtime::TransactionOp>,
        durability_policy: Option<DurabilityPolicy>,
    ) -> MidgeResult<(u64, usize, bool)> {
        if ops.is_empty() {
            return Ok((state.sequence, 0, false));
        }

        // Preflight insert-only operations so we can fail without writing any WAL records.
        for op in ops.iter() {
            if let crate::runtime::TransactionOp::Put {
                cf_id,
                key,
                insert_only: true,
                ..
            } = op
            {
                if self.key_exists_or_pending(state, *cf_id, &key[..]) {
                    return Err(MidgeError::InvalidArgument(
                        "key already exists".to_string(),
                    ));
                }
            }
        }

        let txn_id = state.next_txn_id();

        // Marker key is unused by semantics but required by the record format.
        let marker_key = Bytes::from_static(b"txn");

        // Preallocate sequence range for batch: begin, per-op, commit.
        let ops_count = ops.len();
        debug_assert!(
            ops_count > 0,
            "append_transaction requires at least one operation"
        );
        let ops_count_u64 = ops_count as u64;
        // sequences: begin_seq, op_seqs[0..ops_count-1], commit_seq
        let begin_seq = state.sequence + 1;
        let first_op_seq = begin_seq + 1;
        let commit_seq = begin_seq + 1 + ops_count_u64;

        // Advance global sequence to commit_seq
        state.sequence = commit_seq;

        // Log seqno allocations for intent tracing (deferred - single persist at end)
        // Begin seq (cf_id 0)
        state.append_intent_deferred(IntentLogEntry::SeqnoAllocated {
            seqno: begin_seq,
            cf_id: 0,
        });
        // Commit seq
        state.append_intent_deferred(IntentLogEntry::SeqnoAllocated {
            seqno: commit_seq,
            cf_id: 0,
        });

        // Determine effective durability policy: use provided one, or fall back to actor's default
        let effective_durability = durability_policy.unwrap_or(self.durability_policy);

        // For BestEffort mode, skip WAL entirely - only update memtable
        let skip_wal = matches!(effective_durability, DurabilityPolicy::BestEffort);
        let mut total_wal_bytes: usize = 0;

        // Build begin + op records first. The commit marker is appended only at the
        // end of the transaction path so recovery cannot observe a "committed"
        // transaction if the process crashes before append_transaction returns.
        let mut wal_records: Vec<WalRecord> = if skip_wal {
            Vec::new() // Don't allocate WAL records for BestEffort
        } else {
            let mut records = Vec::with_capacity(ops_count + 1);

            // Create and write begin record
            let mut begin_record = WalRecord::new_cf(
                0,
                WalOpKind::TxnBegin,
                marker_key.clone(),
                None,
                begin_seq,
                self.current_epoch,
            );
            begin_record.txn_id = Some(txn_id);
            records.push(begin_record);
            records
        };

        let apply_ops =
            self.build_apply_ops(ops, first_op_seq, txn_id, state, skip_wal, &mut wal_records);

        let mut commit_record = None;
        if !skip_wal {
            let mut record = WalRecord::new_cf(
                0,
                WalOpKind::TxnCommit,
                marker_key,
                None,
                commit_seq,
                self.current_epoch,
            );
            record.txn_id = Some(txn_id);
            commit_record = Some(record);

            for r in &wal_records {
                total_wal_bytes += r.estimated_size();
            }
        }

        // Single batched WAL write — one writer lock acquisition, one buffer flush
        // Skip entirely for BestEffort mode
        if !skip_wal {
            if let Some(writer) = &mut self.writer {
                let a_start = Instant::now();
                writer.append_batch(&wal_records)?;
                fail::fail_point!("midge::wal::after_append_batch_before_sync");
                let a_elapsed = a_start.elapsed();
                self.append_calls += 1;
                self.append_total += a_elapsed;
                if let Some(t) = crate::telemetry::Telemetry::global() {
                    t.metrics().record_wal_append(total_wal_bytes as u64);
                    t.metrics().record_wal_append_count();
                    t.metrics()
                        .record_wal_append_ns(a_elapsed.as_nanos() as u64);
                }
            }

            // Update bookkeeping (single update for entire batch)
            let record_count = wal_records.len();
            state.wal.pending_writes += record_count;
            self.pending_sync_count += record_count;
            self.bytes_since_sync += total_wal_bytes;
        }

        let last_sequence = commit_seq;

        // Apply durability policy (single sync for the whole batch, where relevant).
        // Use effective_durability which may be per-request BestEffort
        match effective_durability {
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
            DurabilityPolicy::BestEffort => {
                // no-op; data is already in memtable, no WAL to sync
            }
        }

        let op_count = apply_ops.len();

        // For batched mode, mark the sequence range as pending atomicity
        // Reads at sequences >= begin_seq must wait for the batch to become durable
        if matches!(effective_durability, DurabilityPolicy::Batched) {
            state.pending_txn_min_seq = Some(begin_seq);
            state.pending_txn_start_time = Some(Instant::now());

            // Record metric
            if let Some(t) = crate::telemetry::Telemetry::global() {
                t.metrics().record_pending_txn_started();
            }
        }

        if matches!(effective_durability, DurabilityPolicy::CloudFirst) {
            // === CRITICAL: Check backpressure before queueing batch ===
            if self.should_apply_backpressure() {
                tracing::warn!(
                    op_count,
                    pending_count = self.cloud_write_queue.len(),
                    pending_bytes = self.cloud_write_queue.pending_bytes(),
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
                return Err(MidgeError::Internal(format!(
                    "{} pending writes exceeded cloud upload timeout",
                    timed_out
                )));
            }

            // CRITICAL: Apply to memtable immediately for CloudFirst.
            // Cloud upload runs asynchronously; data must be visible for reads
            // without waiting for upload completion. This matches append() behavior.
            // We consume apply_ops here (same as non-CloudFirst path) to avoid cloning.
            for apply_op in apply_ops {
                match apply_op {
                    TransactionApplyOp::Put {
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
                            key,
                            Some(value),
                            expiration,
                        )?;
                    }
                    TransactionApplyOp::Delete {
                        cf_id,
                        key,
                        sequence,
                    } => {
                        self.apply_to_memtable(state, sequence, cf_id, key, None, None)?;
                    }
                }
            }

            // Note: We no longer queue to cloud_write_queue for batched transactions
            // since memtable apply already happened. Cloud WAL upload is handled
            // separately via segment rotation. The queue is primarily for single-write
            // CloudFirst tracking which still uses append() -> enqueue_write().

            tracing::trace!(
                commit_sequence = last_sequence,
                op_count,
                "Applied batch to memtable (CloudFirst)"
            );
        } else {
            self.apply_ops_to_memtables(state, apply_ops)?;
        }

        if let Some(commit_record) = commit_record {
            if let Some(writer) = &mut self.writer {
                let a_start = Instant::now();
                writer.append_record(&commit_record)?;
                let a_elapsed = a_start.elapsed();
                self.append_calls += 1;
                self.append_total += a_elapsed;
                if let Some(t) = crate::telemetry::Telemetry::global() {
                    t.metrics().record_wal_append(commit_record.estimated_size() as u64);
                    t.metrics().record_wal_append_count();
                    t.metrics()
                        .record_wal_append_ns(a_elapsed.as_nanos() as u64);
                }
            }

            state.wal.pending_writes += 1;
            self.pending_sync_count += 1;
            self.bytes_since_sync += commit_record.estimated_size();
        }

        tracing::trace!(txn_id, last_sequence, op_count, "WAL transaction apply");

        // Return deferred=true if using group commit (Batched or CloudFirst modes)
        let deferred = matches!(
            effective_durability,
            DurabilityPolicy::Batched | DurabilityPolicy::CloudFirst
        );
        Ok((last_sequence, op_count, deferred))
    }

    fn build_apply_ops(
        &mut self,
        ops: Vec<crate::runtime::TransactionOp>,
        first_op_seq: u64,
        txn_id: u64,
        state: &mut RuntimeState,
        skip_wal: bool,
        wal_records: &mut Vec<WalRecord>,
    ) -> Vec<TransactionApplyOp> {
        let mut apply_ops = Vec::with_capacity(ops.len());

        for (i, op) in ops.into_iter().enumerate() {
            let seq = first_op_seq + i as u64;
            match op {
                crate::runtime::TransactionOp::Put {
                    cf_id,
                    key,
                    value,
                    ttl_seconds,
                    insert_only,
                } => {
                    let op_kind = if insert_only {
                        WalOpKind::Insert
                    } else {
                        WalOpKind::Put
                    };

                    let mut record = match ttl_seconds {
                        Some(ttl) if ttl > 0 => WalRecord::new_with_ttl(
                            cf_id,
                            op_kind,
                            key.clone(),
                            Some(value.clone()),
                            seq,
                            ttl,
                            self.current_epoch,
                        ),
                        _ => WalRecord::new_cf(
                            cf_id,
                            op_kind,
                            key.clone(),
                            Some(value.clone()),
                            seq,
                            self.current_epoch,
                        ),
                    };
                    record.txn_id = Some(txn_id);

                    state.append_intent_deferred(IntentLogEntry::SeqnoAllocated {
                        seqno: seq,
                        cf_id,
                    });

                    apply_ops.push(TransactionApplyOp::Put {
                        cf_id,
                        key,
                        value,
                        expiration: record.expiration,
                        sequence: seq,
                    });

                    if !skip_wal {
                        wal_records.push(record);
                    }
                }
                crate::runtime::TransactionOp::Delete { cf_id, key } => {
                    let mut record = WalRecord::new_cf(
                        cf_id,
                        WalOpKind::Delete,
                        key.clone(),
                        None,
                        seq,
                        self.current_epoch,
                    );
                    record.txn_id = Some(txn_id);

                    state.append_intent_deferred(IntentLogEntry::SeqnoAllocated {
                        seqno: seq,
                        cf_id,
                    });

                    apply_ops.push(TransactionApplyOp::Delete {
                        cf_id,
                        key,
                        sequence: seq,
                    });

                    if !skip_wal {
                        wal_records.push(record);
                    }
                }
            }
        }

        apply_ops
    }

    fn apply_ops_to_memtables(
        &mut self,
        state: &mut RuntimeState,
        apply_ops: Vec<TransactionApplyOp>,
    ) -> MidgeResult<()> {
        for apply_op in apply_ops {
            match apply_op {
                TransactionApplyOp::Put {
                    cf_id,
                    key,
                    value,
                    expiration,
                    sequence,
                } => {
                    self.apply_to_memtable(state, sequence, cf_id, key, Some(value), expiration)?;
                }
                TransactionApplyOp::Delete {
                    cf_id,
                    key,
                    sequence,
                } => {
                    self.apply_to_memtable(state, sequence, cf_id, key, None, None)?;
                }
            }
        }
        Ok(())
    }

    /// Checks current in-memory view (active + immutable memtables) for existence
    fn key_exists(
        &self,
        state: &RuntimeState,
        cf_id: crate::engine::ColumnFamilyId,
        key: &[u8],
    ) -> bool {
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

    fn key_exists_or_pending(
        &self,
        state: &RuntimeState,
        cf_id: crate::engine::ColumnFamilyId,
        key: &[u8],
    ) -> bool {
        if self.key_exists(state, cf_id, key) {
            return true;
        }

        if !self.is_cloud_first() {
            return false;
        }

        self.cloud_write_queue.contains_key(cf_id, key)
    }

    /// Apply a write to the memtable
    fn apply_to_memtable(
        &self,
        state: &mut RuntimeState,
        sequence: u64,
        cf_id: crate::engine::ColumnFamilyId,
        key: Bytes,
        value: Option<Bytes>,
        expiration: Option<u64>,
    ) -> MidgeResult<()> {
        if let Some(cf_state) = state.column_families.get(&cf_id) {
            let prev = cf_state.memtable.size_bytes();
            if let Some(val) = value {
                cf_state
                    .memtable
                    .as_ref()
                    .put_bytes_with_seq(key, val, sequence, expiration)?;
            } else {
                cf_state
                    .memtable
                    .as_ref()
                    .delete_bytes_with_seq(key, sequence)?;
            }
            let new = cf_state.memtable.size_bytes();
            let delta = new.saturating_sub(prev);
            state.total_memtable_bytes = state.total_memtable_bytes.saturating_add(delta);
        }
        Ok(())
    }

    /// Apply a delete_range operation to the memtable
    fn apply_delete_range_to_memtable(
        &self,
        state: &mut RuntimeState,
        sequence: u64,
        cf_id: u32,
        start_key: &[u8],
        end_key: &[u8],
    ) -> MidgeResult<()> {
        if let Some(cf_state) = state.column_families.get(&cf_id) {
            let prev = cf_state.memtable.size_bytes();
            cf_state
                .memtable
                .as_ref()
                .delete_range_with_seq(start_key, end_key, sequence)?;
            let new = cf_state.memtable.size_bytes();
            let delta = new.saturating_sub(prev);
            state.total_memtable_bytes = state.total_memtable_bytes.saturating_add(delta);
        }
        Ok(())
    }

    /// Internal sync helper - fsyncs the writer
    fn sync_internal(&mut self, state: &mut RuntimeState) -> MidgeResult<()> {
        // Epoch fencing check: verify our epoch is still current before making
        // data durable.  If a newer writer has taken over, we must stop.
        if let Some(store) = &self.leader_store {
            store.validate_epoch(self.current_epoch).map_err(|e| {
                tracing::error!(epoch = self.current_epoch, err = %e, "fenced at sync boundary");
                e
            })?;
        }

        if let Some(writer) = &mut self.writer {
            // CRITICAL: Phase 2.3 - WAL fsync timeout protection
            // Wraps fsync in a timeout to prevent event loop starvation.
            // If fsync blocks >5s (unlikely except on severely degraded storage),
            // we still update state to allow progress and log a warning.
            // This prioritizes liveness over perfect durability in extreme cases.
            let start = Instant::now();
            let fsync_timeout = Duration::from_secs(5);

            let sync_result =
                std::panic::catch_unwind(std::panic::AssertUnwindSafe(|| writer.sync()));

            let elapsed = start.elapsed();

            // Check if sync took too long (but still succeeded)
            if elapsed > fsync_timeout {
                tracing::warn!(
                    elapsed_ms = elapsed.as_millis(),
                    "WAL fsync exceeded timeout threshold (5s); storage may be degraded or stalled"
                );
            }

            // Handle panic or error from sync
            match sync_result {
                Ok(Ok(())) => {
                    // Success
                }
                Ok(Err(e)) => {
                    return Err(e);
                }
                Err(panic_info) => {
                    tracing::error!(
                        panic_info = ?panic_info,
                        "WAL fsync panic; returning error to unblock event loop"
                    );
                    return Err(crate::common::MidgeError::Internal(
                        "WAL fsync panicked".to_string(),
                    ));
                }
            }

            self.sync_calls += 1;
            self.sync_total += elapsed;
            self.last_sync_instant = Instant::now();

            // Record telemetry metric for WAL syncs and fsync latency
            if let Some(t) = crate::telemetry::Telemetry::global() {
                t.metrics().record_wal_sync();
                t.metrics().record_wal_fsync_count();
                t.metrics().record_wal_fsync_ns(elapsed.as_nanos() as u64);
            }

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

            fail::fail_point!("midge::wal::after_fsync_before_durable_frontier");
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
            if let Some(t) = crate::telemetry::Telemetry::global() {
                t.metrics().record_wal_flush();
            }
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
        // Time-based check (max_delay_ms) OR byte-count threshold
        let by_bytes = self.bytes_since_sync >= self.batch_config.max_bytes;
        let by_time = self.last_sync_instant.elapsed().as_millis()
            >= (self.batch_config.max_delay_ms as u128);
        by_bytes || by_time
    }

    /// Returns true if there is any buffered data awaiting sync.
    pub fn has_pending_data(&self) -> bool {
        self.bytes_since_sync > 0 || self.pending_sync_count > 0
    }

    /// Reset the sync timer without performing a sync.
    ///
    /// Used when the time-based threshold fires but there is nothing to sync,
    /// so the timer doesn't immediately re-trigger on the next tick.
    pub fn reset_sync_timer(&mut self) {
        self.last_sync_instant = Instant::now();
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
        let drained_writes = self
            .cloud_write_queue
            .drain_until(state.wal.cloud_durable_seq);

        for pending in drained_writes {
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
                    self.apply_to_memtable(
                        state,
                        sequence,
                        cf_id,
                        key_bytes,
                        value_bytes,
                        expiration,
                    )?;
                }
                PendingCloudWrite::DeleteRange {
                    cf_id,
                    start_key,
                    end_key,
                    sequence,
                    enqueued_at,
                } => {
                    let wait_time = Instant::now().duration_since(enqueued_at);
                    tracing::debug!(
                        cf_id,
                        sequence,
                        wait_ms = wait_time.as_millis(),
                        start_len = start_key.len(),
                        end_len = end_key.len(),
                        "Applying pending delete_range after cloud durability"
                    );
                    self.apply_delete_range_to_memtable(
                        state, sequence, cf_id, &start_key, &end_key,
                    )?;
                }
                PendingCloudWrite::Transaction {
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
                        "Applying pending transaction after cloud durability"
                    );
                    for op in ops {
                        match op {
                            TransactionApplyOp::Put {
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
                                    key,
                                    Some(value),
                                    expiration,
                                )?;
                            }
                            TransactionApplyOp::Delete {
                                cf_id,
                                key,
                                sequence,
                            } => {
                                self.apply_to_memtable(state, sequence, cf_id, key, None, None)?;
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
        self.cloud_write_queue.clear();
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
        // INTENTIONAL: Cannot create with default since we need a WAL directory.
        // WalActor must be created via WalActor::new(wal_dir) to ensure proper initialization.
        // This prevents misuse while maintaining the Default trait for generic contexts.
        panic!("WalActor::default() is not supported. Use WalActor::new(wal_dir) instead.")
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

        let mut wal_actor = WalActor::new(
            PathBuf::from("/tmp/test"),
            DurabilityPolicy::Strict,
            BatchConfig::default(),
            true,
            1,
        )?;

        // Act: append a single put
        let (seq, deferred) = wal_actor.append(
            &mut state,
            AppendParams {
                request_id: 1,
                cf_id: 0,
                key: Bytes::from("k"),
                value: Some(Bytes::from("v")),
                insert_only: false,
                ttl_seconds: None,
            },
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
    fn should_not_force_sync_immediately() -> MidgeResult<()> {
        // Arrange: WAL actor with a long batch window so batching is possible
        let temp = tempfile::tempdir().map_err(crate::common::MidgeError::Io)?;
        let wal_dir = temp.path().to_path_buf();
        let batch_cfg = BatchConfig {
            max_delay_ms: 10_000,
            max_bytes: 1024 * 1024,
        };
        let mut wal_actor = WalActor::new(
            wal_dir.clone(),
            DurabilityPolicy::Batched,
            batch_cfg,
            false,
            1,
        )?;

        // Prepare a runtime state
        let mut state = RuntimeState::new(wal_dir, false);

        // Act: append a small batch (deferred in Batched mode)
        let ops = vec![
            crate::runtime::TransactionOp::Put {
                cf_id: 0,
                key: Bytes::from_static(b"k1"),
                value: Bytes::from_static(b"v1"),
                ttl_seconds: None,
                insert_only: false,
            },
            crate::runtime::TransactionOp::Put {
                cf_id: 0,
                key: Bytes::from_static(b"k2"),
                value: Bytes::from_static(b"v2"),
                ttl_seconds: None,
                insert_only: false,
            },
        ];

        let (_last_seq, _count, deferred) =
            wal_actor.append_transaction(&mut state, 1, ops, None)?;
        assert!(deferred);

        // Assert
        // Should not request an immediate sync (time/bytes thresholds not met)
        assert!(
            !wal_actor.should_sync_batch(),
            "should_sync_batch should be false immediately after small append"
        );
        assert_eq!(
            wal_actor.sync_calls(),
            0,
            "no syncs should have been performed yet"
        );

        Ok(())
    }
}
