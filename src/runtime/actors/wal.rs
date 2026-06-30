//! WAL Actor - handles write-ahead log operations
//!
//! Responsible for:
//! - Appending records to WAL
//! - Syncing WAL to disk
//! - Rotating WAL segments
//! - Coordinating with cloud actor for WAL uploads
//! - Tracking `local_durable_seq` and `cloud_durable_seq` frontiers
//! - Managing pending requests waiting for cloud durability
//!
//! ARCHITECTURAL RULES:
//! - ALWAYS uses `FsWalWriter` (never creates new backends)
//! - Assigns global sequence numbers via `state.next_sequence()`
//! - Tracks two durability frontiers for `CloudAsync` mode
//! - Does NOT block event loop waiting for cloud
//! - Queues pending requests and completes them when `cloud_durable_seq` advances
//!
//! NOTE: `DurabilityPolicy::CloudAsync` is wired as an async cloud-backed mode.
//! Local WAL append is the immediate visibility barrier; cloud durability advances
//! independently via `cloud_durable_seq`.
//!
//! CLOUD-BACKED VISIBILITY RULE:
//! In `DurabilityPolicy::CloudAsync`, writes become visible after the local WAL
//! append barrier succeeds and the memtable is updated. Cloud upload remains
//! asynchronous unless the caller explicitly waits on the cloud durability frontier.

use super::super::state::RuntimeState;
use super::cloud_write_queue::{
    CloudWriteQueue, PendingCloudWrite, TransactionApplyOp, CLOUD_UPLOAD_TIMEOUT,
};
use crate::common::MidgeError;
use crate::common::MidgeResult;
use crate::io::{Fs, FsPath, RealFs};
use crate::sst::Memtable;
use crate::wal::policy::BatchConfig;
use crate::wal::{DurabilityPolicy, FsWalFactoryIo, WalOpKind, WalRecord, WalWriter};
use bytes::Bytes;
use std::collections::HashSet;
use std::path::PathBuf;
use std::sync::Arc;

struct TxnSequencePlan {
    txn_id: u64,
    begin_seq: u64,
    first_op_seq: u64,
    commit_seq: u64,
}

struct TxnWalBatch {
    wal_records: Vec<WalRecord>,
    commit_record: Option<WalRecord>,
    total_wal_bytes: usize,
    skip_wal: bool,
}
use std::time::{Duration, Instant};

/// Parameters for WAL append operation
pub struct AppendParams {
    pub request_id: u64,
    pub cf_id: crate::types::ColumnFamilyId,
    pub key: Bytes,
    pub value: Option<Bytes>,
    pub insert_only: bool,
    pub ttl_seconds: Option<u64>,
}

/// Actor handling WAL operations
pub struct WalActor {
    /// WAL writer (owned by this actor)
    writer: Option<Box<dyn WalWriter>>,
    /// Filesystem backend for WAL files (`io::Fs` abstraction)
    wal_fs: Option<Arc<dyn Fs>>,
    /// Buffered writes pending sync
    pending_sync_count: usize,
    /// Durability policy (determines sync behavior)
    durability_policy: DurabilityPolicy,
    /// Pending writes waiting for cloud durability (`CloudAsync` mode only)
    /// These writes are in local WAL but NOT in memtable yet
    cloud_write_queue: CloudWriteQueue,
    /// Bytes written since last sync (for batched mode)
    bytes_since_sync: usize,
    /// Highest sequence actually appended to the current WAL segment.
    segment_max_sequence: u64,
    /// Flush generation for batched/local durability group commit
    flush_generation: u64,

    /// Batch configuration governing `max_delay_ms` and `max_bytes`
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
    fn duration_nanos_u64(duration: Duration) -> u64 {
        u64::try_from(duration.as_nanos()).unwrap_or(u64::MAX)
    }

    fn u64_to_f64(value: u64) -> f64 {
        let upper = u32::try_from(value >> 32).unwrap_or(u32::MAX);
        let lower = u32::try_from(value & u64::from(u32::MAX)).unwrap_or(u32::MAX);
        f64::from(upper) * 4_294_967_296.0 + f64::from(lower)
    }

    fn record_no_space_event() {
        if let Some(t) = crate::telemetry::Telemetry::global() {
            t.metrics().record_no_space_event();
            t.metrics().record_write_stall_no_space();
        }
    }

    fn record_write_conflict_point() {
        if let Some(t) = crate::telemetry::Telemetry::global() {
            t.metrics().record_write_conflict_point();
        }
    }

    fn record_write_conflict_range() {
        if let Some(t) = crate::telemetry::Telemetry::global() {
            t.metrics().record_write_conflict_range();
        }
    }

    fn finish_append_instrumentation(&mut self, bytes_written: u64, elapsed: Duration) {
        self.append_calls += 1;
        self.append_total += elapsed;
        if let Some(t) = crate::telemetry::Telemetry::global() {
            t.metrics().record_wal_append(bytes_written);
            t.metrics().record_wal_append_count();
            t.metrics()
                .record_wal_append_ns(Self::duration_nanos_u64(elapsed));
        }
    }

    fn record_segment_sequence(&mut self, sequence: u64) {
        self.segment_max_sequence = self.segment_max_sequence.max(sequence);
    }

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
            let fs: Arc<dyn Fs> = Arc::new(RealFs::new(wal_dir)?);
            let factory = FsWalFactoryIo::new(Arc::clone(&fs));
            let writer = Some(factory.create_writer(crate::wal::ACTIVE_FILE_NAME)?);
            (Some(fs), writer)
        };

        let actor = Self {
            writer,
            wal_fs,
            pending_sync_count: 0,
            durability_policy,
            cloud_write_queue: CloudWriteQueue::new(),
            bytes_since_sync: 0,
            segment_max_sequence: 0,
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
            batching_enabled = matches!(actor.durability_policy, DurabilityPolicy::Batched) || matches!(actor.durability_policy, DurabilityPolicy::CloudAsync),
            max_delay_ms = actor.batch_config.max_delay_ms,
            max_bytes = actor.batch_config.max_bytes,
            "WAL actor initialized"
        );

        Ok(actor)
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
    ) {
        // Update batch config and policy atomically
        self.batch_config = batch_config;
        self.durability_policy = policy;
    }

    pub fn is_cloud_async(&self) -> bool {
        matches!(self.durability_policy, DurabilityPolicy::CloudAsync)
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

    /// Number of times `sync_internal` has been called (for tests/diagnostics)
    pub fn sync_calls(&self) -> u64 {
        self.sync_calls
    }

    /// Check if cloud write queue has hit backpressure limits
    /// Returns true if we should reject new writes to prevent memory exhaustion
    pub fn should_apply_backpressure(&self) -> bool {
        self.cloud_write_queue.should_apply_backpressure()
    }

    /// Check for timed-out pending cloud writes
    /// Returns number of writes that have exceeded `CLOUD_UPLOAD_TIMEOUT`
    pub fn count_timed_out_writes(&self) -> usize {
        self.cloud_write_queue.count_timed_out_writes()
    }

    fn ensure_cloud_async_ready(&self) -> MidgeResult<()> {
        if self.should_apply_backpressure() {
            if let Some(t) = crate::telemetry::Telemetry::global() {
                t.metrics().record_write_stall_cloud();
            }
            tracing::warn!(
                pending_count = self.cloud_write_queue.len(),
                pending_bytes = self.cloud_write_queue.pending_bytes(),
                "CloudAsync write stall: pending queue at capacity"
            );
            return Err(MidgeError::WriteStall(
                "CloudAsync pending queue at capacity; cloud upload too slow".to_string(),
            ));
        }

        let timed_out = self.count_timed_out_writes();
        if timed_out > 0 {
            tracing::error!(
                timed_out_count = timed_out,
                timeout_secs = CLOUD_UPLOAD_TIMEOUT.as_secs(),
                "CloudAsync timeout: pending writes not acknowledged by cloud"
            );
            return Err(MidgeError::Internal(format!(
                "{timed_out} pending writes exceeded cloud upload timeout"
            )));
        }

        Ok(())
    }

    fn apply_append_policy(
        &mut self,
        state: &mut RuntimeState,
        sequence: u64,
        cf_id: crate::types::ColumnFamilyId,
        key: &Bytes,
        value: Option<&Bytes>,
        expiration: Option<u64>,
    ) -> MidgeResult<()> {
        match self.durability_policy {
            DurabilityPolicy::Strict | DurabilityPolicy::CloudMirrored => {
                self.sync_internal(state)?;
                state.wal.local_durable_seq = sequence;
                Self::apply_to_memtable(
                    state,
                    sequence,
                    cf_id,
                    key.clone(),
                    value.cloned(),
                    expiration,
                )?;

                if matches!(self.durability_policy, DurabilityPolicy::CloudMirrored) {
                    self.cloud_write_queue.enqueue_write(
                        cf_id,
                        key.to_vec(),
                        value.map(|bytes| bytes.as_ref().to_vec()),
                        sequence,
                        expiration,
                    );
                }
            }
            DurabilityPolicy::Batched | DurabilityPolicy::BestEffort => {
                Self::apply_to_memtable(
                    state,
                    sequence,
                    cf_id,
                    key.clone(),
                    value.cloned(),
                    expiration,
                )?;
            }
            DurabilityPolicy::CloudAsync => {
                self.ensure_cloud_async_ready()?;
                Self::apply_to_memtable(
                    state,
                    sequence,
                    cf_id,
                    key.clone(),
                    value.cloned(),
                    expiration,
                )?;
                self.cloud_write_queue.enqueue_write(
                    cf_id,
                    key.to_vec(),
                    value.map(|bytes| bytes.as_ref().to_vec()),
                    sequence,
                    expiration,
                );
            }
        }

        Ok(())
    }

    fn apply_delete_range_policy(
        &mut self,
        state: &mut RuntimeState,
        sequence: u64,
        cf_id: u32,
        start_key: &[u8],
        end_key: &[u8],
        effective_durability: DurabilityPolicy,
    ) -> MidgeResult<()> {
        match effective_durability {
            DurabilityPolicy::Strict | DurabilityPolicy::CloudMirrored => {
                self.sync_internal(state)?;
                state.wal.local_durable_seq = sequence;
                Self::apply_delete_range_to_memtable(state, sequence, cf_id, start_key, end_key)?;
            }
            DurabilityPolicy::Batched | DurabilityPolicy::BestEffort => {
                Self::apply_delete_range_to_memtable(state, sequence, cf_id, start_key, end_key)?;
            }
            DurabilityPolicy::CloudAsync => {
                self.ensure_cloud_async_ready()?;
                Self::apply_delete_range_to_memtable(state, sequence, cf_id, start_key, end_key)?;
                self.cloud_write_queue.enqueue_delete_range(
                    cf_id,
                    start_key.to_vec(),
                    end_key.to_vec(),
                    sequence,
                );
            }
        }

        Ok(())
    }

    /// Append a record to the WAL
    ///
    /// - Strict: fsync immediately + apply to memtable + respond
    /// - Batched: batch writes + apply to memtable immediately + respond
    /// - `CloudMirrored`: fsync + apply to memtable + schedule cloud upload + respond
    /// - `CloudAsync`: local append barrier + apply to memtable + queue for cloud confirmation
    ///
    /// Returns the assigned sequence number.
    ///
    /// IDEMPOTENCY: Uses `request_id` to detect retries. If the same `request_id` is seen twice,
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
                if let Err(error) = writer.append_record(&record) {
                    if matches!(error, MidgeError::NoSpace(_)) {
                        Self::record_no_space_event();
                    }
                    return Err(error);
                }
                self.finish_append_instrumentation(
                    record.estimated_size() as u64,
                    a_start.elapsed(),
                );
                self.record_segment_sequence(sequence);
            }
        }

        // Update state tracking
        state.wal.pending_writes += 1;
        self.pending_sync_count += 1;
        self.bytes_since_sync += record_size;

        self.apply_append_policy(
            state,
            sequence,
            cf_id,
            &key,
            value.as_ref(),
            record.expiration,
        )?;

        tracing::trace!(cf_id = cf_id, sequence, policy = ?self.durability_policy, "WAL append");

        // Optional tracing for append averages
        if std::env::var_os("MIDGE_TRACE_WAL_APPEND").is_some()
            && self.append_calls.is_multiple_of(1000)
        {
            let avg_ms =
                (self.append_total.as_secs_f64() * 1000.0) / Self::u64_to_f64(self.append_calls);
            eprintln!(
                "[midge] wal.append: calls={} total_ms={:.2} avg_ms={:.3}",
                self.append_calls,
                self.append_total.as_secs_f64() * 1000.0,
                avg_ms
            );
        }

        // Return deferred=true if using group commit (Batched or CloudAsync modes)
        let deferred = matches!(
            self.durability_policy,
            DurabilityPolicy::Batched | DurabilityPolicy::CloudAsync
        );
        Ok((sequence, deferred))
    }

    /// Append a delete range tombstone to WAL.
    ///
    /// This writes a single `DeleteRange` record covering [`start_key`, `end_key`).
    /// Much more efficient than scanning and deleting each key individually.
    pub fn append_delete_range(
        &mut self,
        state: &mut RuntimeState,
        request_id: u64,
        cf_id: u32,
        start_key: Bytes,
        end_key: Bytes,
        durability_policy: Option<DurabilityPolicy>,
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
            key: start_key,
            value: None,
            seq: sequence,
            expiration: None,
            range_end: Some(end_key),
            txn_id: None,
            writer_epoch: self.current_epoch,
            compression: None,
        };

        let record_size = record.estimated_size();
        let effective_durability = durability_policy.unwrap_or(self.durability_policy);
        let skip_wal = matches!(effective_durability, DurabilityPolicy::BestEffort);

        // Append to local WAL unless the caller explicitly requested best effort.
        if !skip_wal {
            if let Some(writer) = &mut self.writer {
                fail::fail_point!("midge::wal::inject_no_space_on_delete_range_append", |_| {
                    Err(MidgeError::NoSpace(
                        "failpoint: no space on delete_range append".to_string(),
                    ))
                });
                let a_start = Instant::now();
                if let Err(error) = writer.append_record(&record) {
                    if matches!(error, MidgeError::NoSpace(_)) {
                        Self::record_no_space_event();
                    }
                    return Err(error);
                }
                self.finish_append_instrumentation(
                    record.estimated_size() as u64,
                    a_start.elapsed(),
                );
                self.record_segment_sequence(sequence);
            }

            // Update state tracking
            state.wal.pending_writes += 1;
            self.pending_sync_count += 1;
            self.bytes_since_sync += record_size;
        }

        if let Some(range_end) = record.range_end.as_ref() {
            self.apply_delete_range_policy(
                state,
                sequence,
                cf_id,
                record.key.as_ref(),
                range_end.as_ref(),
                effective_durability,
            )?;
        }

        tracing::trace!(cf_id = cf_id, sequence, policy = ?self.durability_policy, "WAL append_delete_range");

        let deferred = matches!(
            effective_durability,
            DurabilityPolicy::Batched | DurabilityPolicy::CloudAsync
        );
        Ok((sequence, deferred))
    }

    /// Apply a transaction's operations to the WAL as a single atomic unit.
    ///
    /// This method:
    /// - allocates a single transaction id
    /// - writes `TxnBegin` marker (unless `BestEffort`)
    /// - writes all operation records (in order, unless `BestEffort`)
    /// - writes `TxnCommit` marker at the end of the transaction path (unless `BestEffort`)
    /// - applies all operations to memtables (in order)
    ///
    /// The `durability_policy` parameter allows per-request durability control.
    /// If `Some(DurabilityPolicy::BestEffort)`, WAL writes are skipped entirely
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
        start_sequence: Option<u64>,
        isolation_policy: crate::runtime::TransactionIsolationPolicy,
    ) -> MidgeResult<(u64, usize, bool)> {
        if ops.is_empty() {
            return Ok((state.sequence, 0, false));
        }

        self.validate_transaction_preconditions(state, &ops, start_sequence, isolation_policy)?;
        let effective_durability = durability_policy.unwrap_or(self.durability_policy);
        let sequence_plan = Self::allocate_transaction_sequences(state, ops.len());
        let (apply_ops, wal_batch) =
            self.build_transaction_wal_batch(state, ops, &sequence_plan, effective_durability);
        let last_sequence = sequence_plan.commit_seq;
        let apply_op_count = apply_ops.len();

        self.append_transaction_begin_and_ops(state, &wal_batch)?;
        self.append_transaction_commit(state, wal_batch.commit_record.as_ref())?;
        self.apply_transaction_durability(
            state,
            effective_durability,
            last_sequence,
            sequence_plan.begin_seq,
        )?;
        self.apply_transaction_ops(
            state,
            apply_ops,
            effective_durability,
            last_sequence,
            apply_op_count,
        )?;

        tracing::trace!(
            txn_id = sequence_plan.txn_id,
            last_sequence,
            apply_op_count,
            "WAL transaction apply"
        );

        // Return deferred=true if using group commit (Batched or CloudAsync modes)
        let deferred = matches!(
            effective_durability,
            DurabilityPolicy::Batched | DurabilityPolicy::CloudAsync
        );
        Ok((last_sequence, apply_op_count, deferred))
    }

    fn validate_transaction_preconditions(
        &self,
        state: &RuntimeState,
        ops: &[crate::runtime::TransactionOp],
        start_sequence: Option<u64>,
        isolation_policy: crate::runtime::TransactionIsolationPolicy,
    ) -> MidgeResult<()> {
        if matches!(
            isolation_policy,
            crate::runtime::TransactionIsolationPolicy::AbortOnWriteConflict
        ) {
            let start_sequence = start_sequence.ok_or_else(|| {
                MidgeError::InvalidArgument(
                    "AbortOnWriteConflict requires transaction start_sequence".to_string(),
                )
            })?;
            Self::ensure_no_write_conflicts(state, ops, start_sequence)?;
        }

        for op in ops {
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
        Ok(())
    }

    fn allocate_transaction_sequences(
        state: &mut RuntimeState,
        ops_count: usize,
    ) -> TxnSequencePlan {
        debug_assert!(
            ops_count > 0,
            "append_transaction requires at least one operation"
        );
        let ops_count_u64 = u64::try_from(ops_count).unwrap_or(u64::MAX);
        let begin_seq = state.sequence + 1;
        let first_op_seq = begin_seq + 1;
        let commit_seq = begin_seq + 1 + ops_count_u64;
        state.sequence = commit_seq;
        TxnSequencePlan {
            txn_id: state.next_txn_id(),
            begin_seq,
            first_op_seq,
            commit_seq,
        }
    }

    fn build_transaction_wal_batch(
        &mut self,
        state: &mut RuntimeState,
        ops: Vec<crate::runtime::TransactionOp>,
        sequence_plan: &TxnSequencePlan,
        effective_durability: DurabilityPolicy,
    ) -> (Vec<TransactionApplyOp>, TxnWalBatch) {
        let skip_wal = matches!(effective_durability, DurabilityPolicy::BestEffort);
        let mut wal_records = self.begin_transaction_records(sequence_plan, ops.len(), skip_wal);
        let apply_ops = self.build_apply_ops(
            ops,
            sequence_plan.first_op_seq,
            sequence_plan.txn_id,
            state,
            skip_wal,
            &mut wal_records,
        );
        let commit_record = self.build_transaction_commit_record(sequence_plan, skip_wal);
        let total_wal_bytes = wal_records.iter().map(WalRecord::estimated_size).sum();
        (
            apply_ops,
            TxnWalBatch {
                wal_records,
                commit_record,
                total_wal_bytes,
                skip_wal,
            },
        )
    }

    fn begin_transaction_records(
        &self,
        sequence_plan: &TxnSequencePlan,
        ops_count: usize,
        skip_wal: bool,
    ) -> Vec<WalRecord> {
        if skip_wal {
            return Vec::new();
        }
        let mut begin_record = WalRecord::new_cf(
            0,
            WalOpKind::TxnBegin,
            Bytes::from_static(b"txn"),
            None,
            sequence_plan.begin_seq,
            self.current_epoch,
        );
        begin_record.txn_id = Some(sequence_plan.txn_id);
        let mut records = Vec::with_capacity(ops_count + 1);
        records.push(begin_record);
        records
    }

    fn build_transaction_commit_record(
        &self,
        sequence_plan: &TxnSequencePlan,
        skip_wal: bool,
    ) -> Option<WalRecord> {
        if skip_wal {
            return None;
        }
        let mut record = WalRecord::new_cf(
            0,
            WalOpKind::TxnCommit,
            Bytes::from_static(b"txn"),
            None,
            sequence_plan.commit_seq,
            self.current_epoch,
        );
        record.txn_id = Some(sequence_plan.txn_id);
        Some(record)
    }

    fn append_transaction_begin_and_ops(
        &mut self,
        state: &mut RuntimeState,
        wal_batch: &TxnWalBatch,
    ) -> MidgeResult<()> {
        if wal_batch.skip_wal {
            return Ok(());
        }
        if let Some(writer) = &mut self.writer {
            fail::fail_point!("midge::wal::inject_no_space_on_txn_append_batch", |_| Err(
                MidgeError::NoSpace("failpoint: no space on transaction batch append".to_string())
            ));
            let append_start = Instant::now();
            if let Err(error) = writer.append_batch(&wal_batch.wal_records) {
                if matches!(error, MidgeError::NoSpace(_)) {
                    Self::record_no_space_event();
                }
                return Err(error);
            }
            self.finish_append_instrumentation(
                wal_batch.total_wal_bytes as u64,
                append_start.elapsed(),
            );
            if let Some(max_sequence) = wal_batch.wal_records.iter().map(|record| record.seq).max()
            {
                self.record_segment_sequence(max_sequence);
            }
            fail::fail_point!("midge::wal::after_append_batch_before_sync");
        }
        let record_count = wal_batch.wal_records.len();
        state.wal.pending_writes += record_count;
        self.pending_sync_count += record_count;
        self.bytes_since_sync += wal_batch.total_wal_bytes;
        Ok(())
    }

    fn append_transaction_commit(
        &mut self,
        state: &mut RuntimeState,
        commit_record: Option<&WalRecord>,
    ) -> MidgeResult<()> {
        let Some(commit_record) = commit_record else {
            return Ok(());
        };
        fail::fail_point!("midge::wal::txn_after_ops_append_before_commit");
        if let Some(writer) = &mut self.writer {
            fail::fail_point!("midge::wal::inject_no_space_on_txn_commit_append", |_| Err(
                MidgeError::NoSpace("failpoint: no space on transaction commit append".to_string())
            ));
            let append_start = Instant::now();
            if let Err(error) = writer.append_record(commit_record) {
                if matches!(error, MidgeError::NoSpace(_)) {
                    Self::record_no_space_event();
                }
                return Err(error);
            }
            self.finish_append_instrumentation(
                commit_record.estimated_size() as u64,
                append_start.elapsed(),
            );
            self.record_segment_sequence(commit_record.seq);
        }
        state.wal.pending_writes += 1;
        self.pending_sync_count += 1;
        self.bytes_since_sync += commit_record.estimated_size();
        Ok(())
    }

    fn apply_transaction_durability(
        &mut self,
        state: &mut RuntimeState,
        effective_durability: DurabilityPolicy,
        last_sequence: u64,
        begin_seq: u64,
    ) -> MidgeResult<()> {
        match effective_durability {
            DurabilityPolicy::Strict | DurabilityPolicy::CloudMirrored => {
                fail::fail_point!("midge::wal::txn_after_commit_append_before_sync");
                self.sync_internal(state)?;
                state.wal.local_durable_seq = last_sequence;
                fail::fail_point!("midge::wal::txn_after_sync_before_ack");
            }
            DurabilityPolicy::Batched => {
                state.pending_txn_min_seq = Some(begin_seq);
                state.pending_txn_start_time = Some(Instant::now());
                if let Some(telemetry) = crate::telemetry::Telemetry::global() {
                    telemetry.metrics().record_pending_txn_started();
                }
            }
            DurabilityPolicy::CloudAsync | DurabilityPolicy::BestEffort => {}
        }
        Ok(())
    }

    fn apply_transaction_ops(
        &mut self,
        state: &mut RuntimeState,
        apply_ops: Vec<TransactionApplyOp>,
        effective_durability: DurabilityPolicy,
        last_sequence: u64,
        apply_op_count: usize,
    ) -> MidgeResult<()> {
        if !matches!(effective_durability, DurabilityPolicy::CloudAsync) {
            return Self::apply_ops_to_memtables(state, apply_ops);
        }
        self.validate_cloud_async_transaction_queue(apply_op_count)?;
        for apply_op in apply_ops {
            Self::apply_transaction_op_to_memtable(state, apply_op)?;
        }
        tracing::trace!(
            commit_sequence = last_sequence,
            apply_op_count,
            "Applied batch to memtable (CloudAsync)"
        );
        Ok(())
    }

    fn validate_cloud_async_transaction_queue(&self, apply_op_count: usize) -> MidgeResult<()> {
        if self.should_apply_backpressure() {
            if let Some(telemetry) = crate::telemetry::Telemetry::global() {
                telemetry.metrics().record_write_stall_cloud();
            }
            tracing::warn!(
                apply_op_count,
                pending_count = self.cloud_write_queue.len(),
                pending_bytes = self.cloud_write_queue.pending_bytes(),
                "CloudAsync batch stall: pending queue at capacity"
            );
            return Err(MidgeError::WriteStall(
                "CloudAsync pending queue at capacity; cloud upload too slow".to_string(),
            ));
        }
        let timed_out = self.count_timed_out_writes();
        if timed_out > 0 {
            tracing::error!(
                timed_out_count = timed_out,
                timeout_secs = CLOUD_UPLOAD_TIMEOUT.as_secs(),
                "CloudAsync batch timeout: pending writes not acknowledged by cloud"
            );
            return Err(MidgeError::Internal(format!(
                "{timed_out} pending writes exceeded cloud upload timeout"
            )));
        }
        Ok(())
    }

    fn apply_transaction_op_to_memtable(
        state: &mut RuntimeState,
        apply_op: TransactionApplyOp,
    ) -> MidgeResult<()> {
        match apply_op {
            TransactionApplyOp::Put {
                cf_id,
                key,
                value,
                expiration,
                sequence,
            } => Self::apply_to_memtable(state, sequence, cf_id, key, Some(value), expiration),
            TransactionApplyOp::Delete {
                cf_id,
                key,
                sequence,
            } => Self::apply_to_memtable(state, sequence, cf_id, key, None, None),
            TransactionApplyOp::DeleteRange {
                cf_id,
                start_key,
                end_key,
                sequence,
            } => Self::apply_delete_range_to_memtable(state, sequence, cf_id, &start_key, &end_key),
        }
    }

    fn ensure_no_write_conflicts(
        state: &RuntimeState,
        ops: &[crate::runtime::TransactionOp],
        start_sequence: u64,
    ) -> MidgeResult<()> {
        let mut checked_keys: HashSet<(crate::types::ColumnFamilyId, Vec<u8>)> = HashSet::new();

        for op in ops {
            match op {
                crate::runtime::TransactionOp::Put { cf_id, key, .. }
                | crate::runtime::TransactionOp::Delete { cf_id, key } => {
                    let dedupe_key = (*cf_id, key.to_vec());
                    if !checked_keys.insert(dedupe_key) {
                        continue;
                    }

                    if let Some(latest_seq) = Self::latest_key_sequence(state, *cf_id, key) {
                        if latest_seq > start_sequence {
                            Self::record_write_conflict_point();
                            return Err(MidgeError::WriteConflict(format!(
                                "key conflict on cf {cf_id} for key {key:?}: latest seq {latest_seq} > tx start {start_sequence}"
                            )));
                        }
                    }

                    if let Some(range_seq) =
                        state.latest_covering_delete_range_sequence(*cf_id, key)
                    {
                        if range_seq > start_sequence {
                            Self::record_write_conflict_point();
                            return Err(MidgeError::WriteConflict(format!(
                                "key conflict on cf {cf_id} for key {key:?}: covered by delete-range seq {range_seq} > tx start {start_sequence}"
                            )));
                        }
                    }
                }
                crate::runtime::TransactionOp::DeleteRange { .. } => {
                    let (cf_id, start_key, end_key) = match op {
                        crate::runtime::TransactionOp::DeleteRange {
                            cf_id,
                            start_key,
                            end_key,
                        } => (*cf_id, start_key, end_key),
                        _ => unreachable!(),
                    };

                    if let Some(latest_seq) = Self::latest_range_sequence(
                        state,
                        cf_id,
                        start_key.as_ref(),
                        end_key.as_ref(),
                    ) {
                        if latest_seq > start_sequence {
                            Self::record_write_conflict_range();
                            return Err(MidgeError::WriteConflict(format!(
                                "range conflict on cf {cf_id} for [{start_key:?}, {end_key:?}): latest seq {latest_seq} > tx start {start_sequence}"
                            )));
                        }
                    }

                    if let Some(range_seq) = state.latest_overlapping_delete_range_sequence(
                        cf_id,
                        start_key.as_ref(),
                        end_key.as_ref(),
                    ) {
                        if range_seq > start_sequence {
                            Self::record_write_conflict_range();
                            return Err(MidgeError::WriteConflict(format!(
                                "range conflict on cf {cf_id} for [{start_key:?}, {end_key:?}): overlaps delete-range seq {range_seq} > tx start {start_sequence}"
                            )));
                        }
                    }
                }
            }
        }

        Ok(())
    }

    fn latest_key_sequence(
        state: &RuntimeState,
        cf_id: crate::types::ColumnFamilyId,
        key: &[u8],
    ) -> Option<u64> {
        let cf_state = state.column_families.get(&cf_id)?;
        let sst_files: Vec<_> = state
            .manifest
            .files
            .iter()
            .filter(|f| f.cf_id == cf_id)
            .cloned()
            .collect();

        let sst_path_prefix = state
            .sst_dir
            .strip_prefix(&state.db_path)
            .unwrap_or_else(|_| std::path::Path::new("sst"))
            .to_path_buf();

        let mut snapshot = crate::runtime::ReadSnapshot::new(
            cf_state.memtable.clone(),
            cf_state.immutable_memtables.clone(),
            sst_files,
            Arc::clone(&state.fs),
            sst_path_prefix,
            state.is_memory_mode(),
        );
        snapshot.cf_id = cf_id;
        snapshot.latest_state_sequence(key)
    }

    fn latest_range_sequence(
        state: &RuntimeState,
        cf_id: crate::types::ColumnFamilyId,
        start_key: &[u8],
        end_key: &[u8],
    ) -> Option<u64> {
        let cf_state = state.column_families.get(&cf_id)?;
        let sst_files: Vec<_> = state
            .manifest
            .files
            .iter()
            .filter(|f| f.cf_id == cf_id)
            .cloned()
            .collect();

        let sst_path_prefix = state
            .sst_dir
            .strip_prefix(&state.db_path)
            .unwrap_or_else(|_| std::path::Path::new("sst"))
            .to_path_buf();

        let mut snapshot = crate::runtime::ReadSnapshot::new(
            cf_state.memtable.clone(),
            cf_state.immutable_memtables.clone(),
            sst_files,
            Arc::clone(&state.fs),
            sst_path_prefix,
            state.is_memory_mode(),
        );
        snapshot.cf_id = cf_id;
        snapshot.latest_sequence_in_range(start_key, end_key)
    }

    fn build_apply_ops(
        &mut self,
        ops: Vec<crate::runtime::TransactionOp>,
        first_op_seq: u64,
        txn_id: u64,
        _state: &mut RuntimeState,
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

                    apply_ops.push(TransactionApplyOp::Delete {
                        cf_id,
                        key,
                        sequence: seq,
                    });

                    if !skip_wal {
                        wal_records.push(record);
                    }
                }
                crate::runtime::TransactionOp::DeleteRange {
                    cf_id,
                    start_key,
                    end_key,
                } => {
                    let mut record = WalRecord::new_cf(
                        cf_id,
                        WalOpKind::DeleteRange,
                        start_key.clone(),
                        None,
                        seq,
                        self.current_epoch,
                    );
                    record.range_end = Some(end_key.clone());
                    record.txn_id = Some(txn_id);

                    apply_ops.push(TransactionApplyOp::DeleteRange {
                        cf_id,
                        start_key,
                        end_key,
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
                    Self::apply_to_memtable(state, sequence, cf_id, key, Some(value), expiration)?;
                }
                TransactionApplyOp::Delete {
                    cf_id,
                    key,
                    sequence,
                } => {
                    Self::apply_to_memtable(state, sequence, cf_id, key, None, None)?;
                }
                TransactionApplyOp::DeleteRange {
                    cf_id,
                    start_key,
                    end_key,
                    sequence,
                } => {
                    Self::apply_delete_range_to_memtable(
                        state, sequence, cf_id, &start_key, &end_key,
                    )?;
                }
            }
        }
        Ok(())
    }

    /// Checks current in-memory view (active + immutable memtables) for existence
    fn key_exists(state: &RuntimeState, cf_id: crate::types::ColumnFamilyId, key: &[u8]) -> bool {
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
        cf_id: crate::types::ColumnFamilyId,
        key: &[u8],
    ) -> bool {
        if Self::key_exists(state, cf_id, key) {
            return true;
        }

        if !self.is_cloud_async() {
            return false;
        }

        self.cloud_write_queue.contains_key(cf_id, key)
    }

    /// Apply a write to the memtable
    fn apply_to_memtable(
        state: &mut RuntimeState,
        sequence: u64,
        cf_id: crate::types::ColumnFamilyId,
        key: Bytes,
        value: Option<Bytes>,
        expiration: Option<u64>,
    ) -> MidgeResult<()> {
        let current_segment_id = state.wal.current_segment_id;
        if let Some(cf_state) = state.column_families.get_mut(&cf_id) {
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
            if prev == 0 && new > 0 {
                cf_state.active_memtable_started_in_segment = current_segment_id;
            }
            let delta = new.saturating_sub(prev);
            state.total_memtable_bytes = state.total_memtable_bytes.saturating_add(delta);
        }
        Ok(())
    }

    /// Apply a `delete_range` operation to the memtable
    fn apply_delete_range_to_memtable(
        state: &mut RuntimeState,
        sequence: u64,
        cf_id: u32,
        start_key: &[u8],
        end_key: &[u8],
    ) -> MidgeResult<()> {
        let current_segment_id = state.wal.current_segment_id;
        if let Some(cf_state) = state.column_families.get_mut(&cf_id) {
            let memtable = Arc::clone(&cf_state.memtable);
            let prev = memtable.size_bytes();
            memtable
                .as_ref()
                .delete_range_with_seq(start_key, end_key, sequence)?;
            let new = memtable.size_bytes();
            if prev == 0 && new > 0 {
                cf_state.active_memtable_started_in_segment = current_segment_id;
            }
            let delta = new.saturating_sub(prev);
            state.total_memtable_bytes = state.total_memtable_bytes.saturating_add(delta);
        }
        state.record_delete_range(cf_id, start_key, end_key, sequence);
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
                    if matches!(e, MidgeError::NoSpace(_)) {
                        Self::record_no_space_event();
                    }
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
                t.metrics()
                    .record_wal_fsync_ns(Self::duration_nanos_u64(elapsed));
            }

            if std::env::var_os("MIDGE_TRACE_WAL_SYNC").is_some()
                && self.sync_calls.is_multiple_of(1000)
            {
                let avg_ms =
                    (self.sync_total.as_secs_f64() * 1000.0) / Self::u64_to_f64(self.sync_calls);
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
    /// Returns the sealed flush generation. In group commit modes (Batched/CloudAsync),
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
    /// `CloudAsync` durability uses local WAL as a staging file for upload.
    /// We avoid fsync on every write, but do a flush+fsync only when sealing
    /// a segment right before upload so the uploader reads a complete file.
    pub fn flush_for_cloud_upload(&mut self, state: &mut RuntimeState) -> MidgeResult<u64> {
        let pending = state.wal.pending_writes;
        let segment_max_sequence = self.segment_max_sequence;

        if let Some(writer) = &mut self.writer {
            writer.flush()?;
            if let Some(t) = crate::telemetry::Telemetry::global() {
                t.metrics().record_wal_flush();
            }
        }

        // Treat everything appended so far as ready-to-ship.
        state.wal.last_synced_seq = segment_max_sequence;
        state.wal.local_durable_seq = segment_max_sequence;

        tracing::debug!(
            pending_writes = pending,
            flushed_seq = state.wal.last_synced_seq,
            "WAL flush (CloudAsync upload)"
        );

        Ok(segment_max_sequence)
    }

    /// Clear buffered WAL accounting after a `CloudAsync` segment is successfully
    /// sealed, rotated, and enqueued for upload.
    pub fn complete_cloud_upload_seal(&mut self, state: &mut RuntimeState) {
        state.wal.pending_writes = 0;
        self.pending_sync_count = 0;
        self.bytes_since_sync = 0;
        self.segment_max_sequence = 0;
        self.last_sync_instant = Instant::now();
    }

    /// Check if batched sync should trigger
    pub fn should_sync_batch(&self) -> bool {
        // Time-based check (max_delay_ms) OR byte-count threshold
        let by_bytes = self.bytes_since_sync >= self.batch_config.max_bytes;
        let by_time = self.last_sync_instant.elapsed().as_millis()
            >= u128::from(self.batch_config.max_delay_ms);
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
            // Rename the mutable active WAL to its immutable sealed segment name.
            let old_path = FsPath::new(crate::wal::ACTIVE_FILE_NAME);
            let new_path = FsPath::new(crate::wal::segment_file_name(old_segment));

            // Rename may fail if file doesn't exist (e.g., in memory mode)
            let _ = fs.rename_atomic(&old_path, &new_path);

            // Create new writer for the next segment
            let factory = FsWalFactoryIo::new(Arc::clone(fs));
            self.writer = Some(factory.create_writer(crate::wal::ACTIVE_FILE_NAME)?);
        }

        state.wal.current_segment_id += 1;
        self.segment_max_sequence = 0;

        tracing::info!(
            old_segment,
            new_segment = state.wal.current_segment_id,
            "WAL rotate"
        );

        Ok(())
    }

    /// Handle cloud upload completion (`CloudAsync` durability)
    ///
    /// Updates `cloud_durable_seq` and completes any pending writes
    /// by applying them to memtable.
    fn apply_pending_cloud_write(
        state: &mut RuntimeState,
        pending: PendingCloudWrite,
    ) -> MidgeResult<()> {
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
                Self::apply_to_memtable(
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
                Self::apply_delete_range_to_memtable(state, sequence, cf_id, &start_key, &end_key)?;
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
                Self::apply_ops_to_memtables(state, ops)?;
            }
        }

        Ok(())
    }

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
            Self::apply_pending_cloud_write(state, pending)?;
        }

        Ok(())
    }

    pub fn handle_cloud_upload_failed(&mut self, segment_id: u64, error: &str) {
        tracing::error!(segment_id, error, "Cloud upload failed");

        // Conservative behavior: fail all pending CloudAsync requests.
        // We cannot claim durability for any queued writes.
        self.cloud_write_queue.clear();
    }

    /// Handle sync completion notification
    pub fn handle_sync_complete(state: &mut RuntimeState, segment_id: u64) {
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

        let (_last_seq, _count, deferred) = wal_actor.append_transaction(
            &mut state,
            1,
            ops,
            None,
            None,
            crate::runtime::TransactionIsolationPolicy::LastWriteWins,
        )?;
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

    #[test]
    fn should_rotate_to_lex_sortable_wal_segment_name() -> MidgeResult<()> {
        let temp = tempfile::tempdir().map_err(crate::common::MidgeError::Io)?;
        let db_path = temp.path().to_path_buf();
        let wal_dir = db_path.join("wal");
        let mut state = RuntimeState::new(db_path, false);
        let mut wal_actor = WalActor::new(
            wal_dir.clone(),
            DurabilityPolicy::Strict,
            BatchConfig::default(),
            false,
            1,
        )?;

        wal_actor.rotate(&mut state)?;

        assert!(
            wal_dir
                .join(crate::wal::cloud_segment_file_name(1))
                .exists(),
            "rotated WAL segments should use the canonical lex-sortable segment name"
        );
        assert!(
            !wal_dir.join("1.wal").exists(),
            "newly rotated WAL segments should not use the legacy non-padded name"
        );

        Ok(())
    }
}
