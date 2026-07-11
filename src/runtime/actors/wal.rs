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
use super::cloud_write_queue::TransactionApplyOp;
#[cfg(test)]
use super::cloud_write_queue::{CloudWriteQueue, PendingCloudWrite, CLOUD_UPLOAD_TIMEOUT};
use crate::common::MidgeError;
use crate::common::MidgeResult;
use crate::io::traits::FsError;
use crate::io::{Fs, FsPath, RealFs};
use crate::sst::Memtable;
use crate::wal::policy::BatchConfig;
use crate::wal::{DurabilityPolicy, FsWalFactoryIo, WalOpKind, WalRecord, WalWriter};
use bytes::Bytes;
use std::collections::HashSet;
use std::path::PathBuf;
#[cfg(all(test, feature = "failpoints"))]
use std::sync::atomic::{AtomicU64, Ordering};
use std::sync::Arc;

#[cfg(all(test, feature = "failpoints"))]
const TXN_APPEND_BATCH_NO_SPACE_FAILPOINT_DISABLED_REQUEST_ID: u64 = u64::MAX;

#[cfg(all(test, feature = "failpoints"))]
static TXN_APPEND_BATCH_NO_SPACE_FAILPOINT_REQUEST_ID: AtomicU64 =
    AtomicU64::new(TXN_APPEND_BATCH_NO_SPACE_FAILPOINT_DISABLED_REQUEST_ID);

struct TxnSequencePlan {
    txn_id: u64,
    begin_seq: u64,
    first_op_seq: u64,
    commit_seq: u64,
}

struct TxnWalBatch {
    wal_record: Option<WalRecord>,
    total_wal_bytes: usize,
    pending_wal_records: usize,
}

/// Inputs needed to prepare one transaction append.
pub(crate) struct TransactionAppendParams {
    pub request_id: u64,
    pub ops: Vec<crate::runtime::TransactionOp>,
    pub durability_policy: Option<DurabilityPolicy>,
    pub start_sequence: Option<u64>,
    pub conflict_policy: crate::runtime::ConflictPolicy,
}

/// Prepared transaction state that has allocated sequences but has not yet
/// published memtable updates.
pub(crate) struct PreparedTransactionAppend {
    request_id: u64,
    sequence_plan: TxnSequencePlan,
    apply_ops: Vec<TransactionApplyOp>,
    wal_batch: TxnWalBatch,
    effective_durability: DurabilityPolicy,
}

/// Result returned after a prepared transaction has been appended and applied.
pub(crate) struct TransactionAppendResult {
    pub request_id: u64,
    pub last_sequence: u64,
    pub op_count: usize,
    pub deferred: bool,
}

enum TransactionIntent<'a> {
    Point {
        cf_id: crate::types::ColumnFamilyId,
        key: &'a [u8],
        exists: bool,
    },
    Range {
        cf_id: crate::types::ColumnFamilyId,
        start: &'a [u8],
        end: &'a [u8],
    },
}
use std::time::{Duration, Instant};

#[cfg(all(test, feature = "failpoints"))]
pub(crate) fn set_txn_append_batch_no_space_failpoint_request_id(request_id: Option<u64>) {
    TXN_APPEND_BATCH_NO_SPACE_FAILPOINT_REQUEST_ID.store(
        request_id.unwrap_or(TXN_APPEND_BATCH_NO_SPACE_FAILPOINT_DISABLED_REQUEST_ID),
        Ordering::SeqCst,
    );
}

/// Parameters for WAL append operation
#[cfg(test)]
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
    #[cfg(test)]
    cloud_write_queue: CloudWriteQueue,
    #[cfg(not(test))]
    cloud_pending_writes: bool,
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
    /// Maximum wait for storage append, flush, and sync acknowledgements.
    storage_io_timeout: Duration,

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
        storage_io_timeout: Duration,
    ) -> MidgeResult<Self> {
        let (wal_fs, writer) = if memory_mode {
            (None, None)
        } else {
            let fs: Arc<dyn Fs> = Arc::new(RealFs::new(wal_dir)?);
            let factory = FsWalFactoryIo::new(Arc::clone(&fs)).with_io_timeout(storage_io_timeout);
            let writer = Some(factory.create_writer(crate::wal::ACTIVE_FILE_NAME)?);
            (Some(fs), writer)
        };

        let actor = Self {
            writer,
            wal_fs,
            pending_sync_count: 0,
            durability_policy,
            #[cfg(test)]
            cloud_write_queue: CloudWriteQueue::new(),
            #[cfg(not(test))]
            cloud_pending_writes: false,
            bytes_since_sync: 0,
            segment_max_sequence: 0,
            flush_generation: 0,
            sync_calls: 0,
            sync_total: Duration::from_secs(0),
            append_calls: 0,
            append_total: Duration::from_secs(0),
            batch_config,
            last_sync_instant: Instant::now(),
            storage_io_timeout,
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

    pub(crate) fn can_coalesce_transaction_append(
        &self,
        durability_policy: Option<DurabilityPolicy>,
    ) -> bool {
        let effective_durability = durability_policy.unwrap_or(self.durability_policy);
        matches!(effective_durability, DurabilityPolicy::Batched)
            && !self.is_cloud_async()
            && self.writer.is_some()
    }

    pub fn has_pending_cloud_writes(&self) -> bool {
        #[cfg(test)]
        {
            self.cloud_write_queue.has_pending_writes()
        }
        #[cfg(not(test))]
        {
            self.cloud_pending_writes
        }
    }

    #[cfg(test)]
    pub fn pending_cloud_writes_len(&self) -> usize {
        self.cloud_write_queue.len()
    }

    pub fn bytes_since_sync(&self) -> usize {
        self.bytes_since_sync
    }

    #[cfg(test)]
    pub fn pending_sync_count(&self) -> usize {
        self.pending_sync_count
    }

    pub fn current_flush_generation(&self) -> u64 {
        self.flush_generation
    }

    /// Number of times `sync_internal` has been called (for tests/diagnostics)
    #[cfg(test)]
    pub fn sync_calls(&self) -> u64 {
        self.sync_calls
    }

    /// Number of physical append calls made by this actor (for tests/diagnostics).
    #[cfg(test)]
    pub fn append_calls(&self) -> u64 {
        self.append_calls
    }

    /// Check if cloud write queue has hit backpressure limits
    /// Returns true if we should reject new writes to prevent memory exhaustion
    #[cfg(test)]
    pub fn should_apply_backpressure(&self) -> bool {
        self.cloud_write_queue.should_apply_backpressure()
    }

    /// Check for timed-out pending cloud writes
    /// Returns number of writes that have exceeded `CLOUD_UPLOAD_TIMEOUT`
    #[cfg(test)]
    pub fn count_timed_out_writes(&self) -> usize {
        self.cloud_write_queue.count_timed_out_writes()
    }

    #[cfg(test)]
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

    #[cfg(test)]
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

    #[cfg(test)]
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
    #[cfg(test)]
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
    #[cfg(test)]
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
                crate::failpoints::fail_point!(
                    "midge::wal::inject_no_space_on_delete_range_append",
                    |_| {
                        Err(MidgeError::NoSpace(
                            "failpoint: no space on delete_range append".to_string(),
                        ))
                    }
                );
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
    /// - writes one `TxnBatch` frame containing the logical transaction (unless `BestEffort`)
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
        request_id: u64,
        ops: Vec<crate::runtime::TransactionOp>,
        durability_policy: Option<DurabilityPolicy>,
        start_sequence: Option<u64>,
        conflict_policy: crate::runtime::ConflictPolicy,
    ) -> MidgeResult<(u64, usize, bool)> {
        if ops.is_empty() {
            return Ok((state.sequence, 0, false));
        }

        for op in &ops {
            let cf_id = match op {
                crate::runtime::TransactionOp::Put { cf_id, .. }
                | crate::runtime::TransactionOp::Delete { cf_id, .. }
                | crate::runtime::TransactionOp::DeleteRange { cf_id, .. } => *cf_id,
            };
            if !state.column_families.contains_key(&cf_id) {
                return Err(MidgeError::InvalidArgument(format!(
                    "column family {cf_id} does not exist"
                )));
            }
        }

        let prepared = self.prepare_transaction_append(
            state,
            TransactionAppendParams {
                request_id,
                ops,
                durability_policy,
                start_sequence,
                conflict_policy,
            },
        )?;
        let last_sequence = prepared.sequence_plan.commit_seq;
        let apply_op_count = prepared.apply_ops.len();
        let txn_id = prepared.sequence_plan.txn_id;

        let results = self.append_prepared_transactions(state, vec![prepared])?;
        let deferred = results
            .into_iter()
            .next()
            .is_some_and(|result| result.deferred);

        tracing::trace!(
            txn_id,
            last_sequence,
            apply_op_count,
            "WAL transaction apply"
        );

        Ok((last_sequence, apply_op_count, deferred))
    }

    /// Stream a spilled transaction through split WAL markers without
    /// reconstructing its complete write set in memory.
    pub(crate) fn append_spilled_transaction(
        &mut self,
        state: &mut RuntimeState,
        request_id: u64,
        source: &crate::runtime::transaction_spill::TransactionOpSource,
        durability_policy: Option<DurabilityPolicy>,
        start_sequence: u64,
        conflict_policy: crate::runtime::ConflictPolicy,
    ) -> MidgeResult<(u64, usize, bool)> {
        if source.is_empty() {
            return Ok((state.sequence, 0, false));
        }
        self.validate_spilled_transaction(state, source, start_sequence, conflict_policy)?;

        let effective_durability = durability_policy.unwrap_or(self.durability_policy);
        let sequence_plan = Self::allocate_transaction_sequences(state, source.len());
        let commit_time_millis = crate::common::time::unix_time_millis();
        let (wal_bytes, wal_records) = self.append_spilled_wal_records(
            request_id,
            source,
            &sequence_plan,
            effective_durability,
            commit_time_millis,
        )?;
        state.wal.pending_writes = state.wal.pending_writes.saturating_add(wal_records);
        self.pending_sync_count = self.pending_sync_count.saturating_add(wal_records);
        self.bytes_since_sync = self.bytes_since_sync.saturating_add(wal_bytes);

        self.apply_transaction_durability(
            state,
            effective_durability,
            sequence_plan.commit_seq,
            sequence_plan.begin_seq,
        )?;
        self.apply_spilled_transaction_ops(
            state,
            source,
            &sequence_plan,
            effective_durability,
            commit_time_millis,
        )?;

        let deferred = matches!(
            effective_durability,
            DurabilityPolicy::Batched | DurabilityPolicy::CloudAsync
        );
        tracing::trace!(
            txn_id = sequence_plan.txn_id,
            last_sequence = sequence_plan.commit_seq,
            apply_op_count = source.len(),
            "streamed spilled WAL transaction apply"
        );
        Ok((sequence_plan.commit_seq, source.len(), deferred))
    }

    fn validate_spilled_transaction(
        &self,
        state: &RuntimeState,
        source: &crate::runtime::transaction_spill::TransactionOpSource,
        start_sequence: u64,
        conflict_policy: crate::runtime::ConflictPolicy,
    ) -> MidgeResult<()> {
        let mut expected_ordinal = 0_u64;
        source.for_each(|ordinal, op| {
            if ordinal != expected_ordinal {
                return Err(MidgeError::Corruption(format!(
                    "transaction spill ordinal {ordinal} is out of order; expected {expected_ordinal}"
                )));
            }
            expected_ordinal = expected_ordinal.saturating_add(1);
            let cf_id = crate::runtime::transaction_spill::op_cf_id(&op);
            if !state.column_families.contains_key(&cf_id) {
                return Err(MidgeError::InvalidArgument(format!(
                    "column family {cf_id} does not exist"
                )));
            }

            if matches!(
                conflict_policy,
                crate::runtime::ConflictPolicy::AbortOnWriteConflict
            ) {
                Self::ensure_no_write_conflict_for_op(state, &op, start_sequence)?;
            }

            if let crate::runtime::TransactionOp::Put {
                cf_id,
                key,
                insert_only: true,
                ..
            } = &op
            {
                let exists = match source.latest_before(ordinal, key)? {
                    Some(crate::runtime::transaction_spill::IntentLookup::Present(_)) => true,
                    Some(crate::runtime::transaction_spill::IntentLookup::Deleted) => false,
                    None => self.key_exists_or_pending(state, *cf_id, key),
                };
                if exists {
                    return Err(MidgeError::InvalidArgument(
                        "key already exists".to_string(),
                    ));
                }
            }
            Ok(())
        })?;
        if usize::try_from(expected_ordinal).unwrap_or(usize::MAX) != source.len() {
            return Err(MidgeError::Corruption(
                "transaction spill operation count does not match its header".to_string(),
            ));
        }
        Ok(())
    }

    fn ensure_no_write_conflict_for_op(
        state: &RuntimeState,
        op: &crate::runtime::TransactionOp,
        start_sequence: u64,
    ) -> MidgeResult<()> {
        match op {
            crate::runtime::TransactionOp::Put { cf_id, key, .. }
            | crate::runtime::TransactionOp::Delete { cf_id, key } => {
                if Self::latest_key_sequence(state, *cf_id, key)?
                    .is_some_and(|sequence| sequence > start_sequence)
                    || state
                        .latest_covering_delete_range_sequence(*cf_id, key)
                        .is_some_and(|sequence| sequence > start_sequence)
                {
                    Self::record_write_conflict_point();
                    return Err(MidgeError::WriteConflict(format!(
                        "key conflict on cf {cf_id} for key {key:?} after tx start {start_sequence}"
                    )));
                }
            }
            crate::runtime::TransactionOp::DeleteRange {
                cf_id,
                start_key,
                end_key,
            } => {
                if Self::latest_range_sequence(state, *cf_id, start_key, end_key)?
                    .is_some_and(|sequence| sequence > start_sequence)
                    || state
                        .latest_overlapping_delete_range_sequence(*cf_id, start_key, end_key)
                        .is_some_and(|sequence| sequence > start_sequence)
                {
                    Self::record_write_conflict_range();
                    return Err(MidgeError::WriteConflict(format!(
                        "range conflict on cf {cf_id} for [{start_key:?}, {end_key:?}) after tx start {start_sequence}"
                    )));
                }
            }
        }
        Ok(())
    }

    fn append_spilled_wal_records(
        &mut self,
        request_id: u64,
        source: &crate::runtime::transaction_spill::TransactionOpSource,
        sequence_plan: &TxnSequencePlan,
        effective_durability: DurabilityPolicy,
        commit_time_millis: u64,
    ) -> MidgeResult<(usize, usize)> {
        if matches!(effective_durability, DurabilityPolicy::BestEffort) || self.writer.is_none() {
            return Ok((0, 0));
        }

        let mut begin = WalRecord::new_cf(
            0,
            WalOpKind::TxnBegin,
            Bytes::from_static(b"txn"),
            None,
            sequence_plan.begin_seq,
            self.current_epoch,
        );
        begin.txn_id = Some(sequence_plan.txn_id);
        let mut total_bytes = begin.estimated_size();
        let append_started_at = Instant::now();
        self.append_spilled_record(&begin)?;

        let epoch = self.current_epoch;
        source.for_each(|ordinal, op| {
            let sequence = sequence_plan
                .first_op_seq
                .checked_add(ordinal)
                .ok_or_else(|| {
                    MidgeError::ResourceLimit(
                        "transaction spill sequence allocation overflow".to_string(),
                    )
                })?;
            let record = Self::wal_record_for_transaction_op(
                op,
                sequence,
                sequence_plan.txn_id,
                epoch,
                commit_time_millis,
            );
            total_bytes = total_bytes.saturating_add(record.estimated_size());
            self.append_spilled_record(&record)
        })?;

        crate::failpoints::fail_point!("midge::wal::txn_after_ops_append_before_commit");
        crate::failpoints::fail_point!(
            "midge::wal::inject_no_space_on_txn_commit_append",
            |_| Err(MidgeError::NoSpace(
                "failpoint: no space on transaction commit append".to_string()
            ))
        );
        let mut commit = WalRecord::new_cf(
            0,
            WalOpKind::TxnCommit,
            Bytes::from_static(b"txn"),
            None,
            sequence_plan.commit_seq,
            self.current_epoch,
        );
        commit.txn_id = Some(sequence_plan.txn_id);
        total_bytes = total_bytes.saturating_add(commit.estimated_size());
        self.append_spilled_record(&commit)?;
        self.finish_append_instrumentation(total_bytes as u64, append_started_at.elapsed());
        self.record_segment_sequence(sequence_plan.commit_seq);
        crate::failpoints::fail_point!("midge::wal::after_append_batch_before_sync");
        tracing::trace!(request_id, "appended split transaction WAL frames");
        Ok((total_bytes, source.len().saturating_add(2)))
    }

    fn append_spilled_record(&mut self, record: &WalRecord) -> MidgeResult<()> {
        let writer = self.writer.as_mut().ok_or_else(|| {
            MidgeError::Internal("transaction WAL writer disappeared during append".to_string())
        })?;
        if let Err(error) = writer.append_record(record) {
            if matches!(error, MidgeError::NoSpace(_)) {
                Self::record_no_space_event();
            }
            return Err(error);
        }
        Ok(())
    }

    fn wal_record_for_transaction_op(
        op: crate::runtime::TransactionOp,
        sequence: u64,
        txn_id: u64,
        writer_epoch: u64,
        commit_time_millis: u64,
    ) -> WalRecord {
        let mut record = match op {
            crate::runtime::TransactionOp::Put {
                cf_id,
                key,
                value,
                ttl_seconds,
                insert_only,
            } => {
                let mut record = WalRecord::new_cf(
                    cf_id,
                    if insert_only {
                        WalOpKind::Insert
                    } else {
                        WalOpKind::Put
                    },
                    key,
                    Some(value),
                    sequence,
                    writer_epoch,
                );
                record.expiration = ttl_seconds
                    .filter(|ttl| *ttl > 0)
                    .map(|ttl| commit_time_millis.saturating_add(ttl.saturating_mul(1_000)));
                record
            }
            crate::runtime::TransactionOp::Delete { cf_id, key } => {
                WalRecord::new_cf(cf_id, WalOpKind::Delete, key, None, sequence, writer_epoch)
            }
            crate::runtime::TransactionOp::DeleteRange {
                cf_id,
                start_key,
                end_key,
            } => {
                let mut record = WalRecord::new_cf(
                    cf_id,
                    WalOpKind::DeleteRange,
                    start_key,
                    None,
                    sequence,
                    writer_epoch,
                );
                record.range_end = Some(end_key);
                record
            }
        };
        record.txn_id = Some(txn_id);
        record
    }

    fn apply_spilled_transaction_ops(
        &mut self,
        state: &mut RuntimeState,
        source: &crate::runtime::transaction_spill::TransactionOpSource,
        sequence_plan: &TxnSequencePlan,
        effective_durability: DurabilityPolicy,
        commit_time_millis: u64,
    ) -> MidgeResult<()> {
        #[cfg(test)]
        if matches!(effective_durability, DurabilityPolicy::CloudAsync) {
            self.validate_cloud_async_transaction_queue(source.len())?;
        }
        let epoch = self.current_epoch;
        source.for_each(|ordinal, op| {
            let sequence = sequence_plan.first_op_seq.saturating_add(ordinal);
            let record = Self::wal_record_for_transaction_op(
                op,
                sequence,
                sequence_plan.txn_id,
                epoch,
                commit_time_millis,
            );
            let apply_op = match record.op {
                WalOpKind::Put | WalOpKind::Insert => TransactionApplyOp::Put {
                    op: record.op,
                    cf_id: record.cf_id,
                    key: record.key,
                    value: record.value.ok_or_else(|| {
                        MidgeError::Corruption("spilled transaction put lost its value".to_string())
                    })?,
                    expiration: record.expiration,
                    sequence,
                },
                WalOpKind::Delete => TransactionApplyOp::Delete {
                    cf_id: record.cf_id,
                    key: record.key,
                    sequence,
                },
                WalOpKind::DeleteRange => TransactionApplyOp::DeleteRange {
                    cf_id: record.cf_id,
                    start_key: record.key,
                    end_key: record.range_end.ok_or_else(|| {
                        MidgeError::Corruption(
                            "spilled transaction range delete lost its end key".to_string(),
                        )
                    })?,
                    sequence,
                },
                WalOpKind::TxnBegin | WalOpKind::TxnCommit | WalOpKind::TxnBatch => {
                    return Err(MidgeError::Internal(
                        "transaction source produced a marker operation".to_string(),
                    ));
                }
            };
            Self::apply_transaction_op_to_memtable(state, apply_op)
        })?;
        tracing::trace!(
            commit_sequence = sequence_plan.commit_seq,
            apply_op_count = source.len(),
            ?effective_durability,
            "applied streamed transaction to memtables"
        );
        Ok(())
    }

    pub(crate) fn prepare_transaction_append(
        &mut self,
        state: &mut RuntimeState,
        params: TransactionAppendParams,
    ) -> MidgeResult<PreparedTransactionAppend> {
        let TransactionAppendParams {
            request_id,
            ops,
            durability_policy,
            start_sequence,
            conflict_policy,
        } = params;

        if ops.is_empty() {
            return Err(MidgeError::InvalidArgument(
                "transaction append requires at least one operation".to_string(),
            ));
        }

        for op in &ops {
            let cf_id = match op {
                crate::runtime::TransactionOp::Put { cf_id, .. }
                | crate::runtime::TransactionOp::Delete { cf_id, .. }
                | crate::runtime::TransactionOp::DeleteRange { cf_id, .. } => *cf_id,
            };
            if !state.column_families.contains_key(&cf_id) {
                return Err(MidgeError::InvalidArgument(format!(
                    "column family {cf_id} does not exist"
                )));
            }
        }

        self.validate_transaction_preconditions(state, &ops, start_sequence, conflict_policy)?;
        let effective_durability = durability_policy.unwrap_or(self.durability_policy);
        let sequence_plan = Self::allocate_transaction_sequences(state, ops.len());
        let (apply_ops, wal_batch) =
            self.build_transaction_wal_batch(ops, &sequence_plan, effective_durability)?;

        Ok(PreparedTransactionAppend {
            request_id,
            sequence_plan,
            apply_ops,
            wal_batch,
            effective_durability,
        })
    }

    pub(crate) fn append_prepared_transactions(
        &mut self,
        state: &mut RuntimeState,
        prepared_transactions: Vec<PreparedTransactionAppend>,
    ) -> MidgeResult<Vec<TransactionAppendResult>> {
        if prepared_transactions.is_empty() {
            return Ok(Vec::new());
        }

        self.append_prepared_transaction_batches(state, &prepared_transactions)?;

        let mut results = Vec::with_capacity(prepared_transactions.len());
        for prepared in prepared_transactions {
            let PreparedTransactionAppend {
                request_id,
                sequence_plan,
                apply_ops,
                effective_durability,
                ..
            } = prepared;
            let last_sequence = sequence_plan.commit_seq;
            let apply_op_count = apply_ops.len();

            self.apply_transaction_durability(
                state,
                effective_durability,
                last_sequence,
                sequence_plan.begin_seq,
            )?;
            #[cfg(test)]
            self.apply_transaction_ops(
                state,
                apply_ops,
                effective_durability,
                last_sequence,
                apply_op_count,
            )?;
            #[cfg(not(test))]
            Self::apply_transaction_ops(
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

            let deferred = matches!(
                effective_durability,
                DurabilityPolicy::Batched | DurabilityPolicy::CloudAsync
            );
            results.push(TransactionAppendResult {
                request_id,
                last_sequence,
                op_count: apply_op_count,
                deferred,
            });
        }

        Ok(results)
    }

    fn validate_transaction_preconditions(
        &self,
        state: &RuntimeState,
        ops: &[crate::runtime::TransactionOp],
        start_sequence: Option<u64>,
        conflict_policy: crate::runtime::ConflictPolicy,
    ) -> MidgeResult<()> {
        if matches!(
            conflict_policy,
            crate::runtime::ConflictPolicy::AbortOnWriteConflict
        ) {
            let start_sequence = start_sequence.ok_or_else(|| {
                MidgeError::InvalidArgument(
                    "AbortOnWriteConflict requires transaction start_sequence".to_string(),
                )
            })?;
            Self::ensure_no_write_conflicts(state, ops, start_sequence)?;
        }

        let mut intents = Vec::with_capacity(ops.len());
        let key_exists_after_intents =
            |cf_id: crate::types::ColumnFamilyId, key: &[u8], intents: &[TransactionIntent<'_>]| {
                for intent in intents.iter().rev() {
                    match intent {
                        TransactionIntent::Point {
                            cf_id: intent_cf,
                            key: intent_key,
                            exists,
                        } if *intent_cf == cf_id && *intent_key == key => return *exists,
                        TransactionIntent::Range {
                            cf_id: intent_cf,
                            start,
                            end,
                        } if *intent_cf == cf_id && *start <= key && key < *end => return false,
                        _ => {}
                    }
                }
                self.key_exists_or_pending(state, cf_id, key)
            };

        for op in ops {
            match op {
                crate::runtime::TransactionOp::Put {
                    cf_id,
                    key,
                    insert_only: false,
                    ..
                } => intents.push(TransactionIntent::Point {
                    cf_id: *cf_id,
                    key: key.as_ref(),
                    exists: true,
                }),
                crate::runtime::TransactionOp::Delete { cf_id, key } => {
                    intents.push(TransactionIntent::Point {
                        cf_id: *cf_id,
                        key: key.as_ref(),
                        exists: false,
                    });
                }
                crate::runtime::TransactionOp::DeleteRange {
                    cf_id,
                    start_key,
                    end_key,
                } => intents.push(TransactionIntent::Range {
                    cf_id: *cf_id,
                    start: start_key.as_ref(),
                    end: end_key.as_ref(),
                }),
                crate::runtime::TransactionOp::Put {
                    cf_id,
                    key,
                    insert_only: true,
                    ..
                } => {
                    if key_exists_after_intents(*cf_id, key, &intents) {
                        return Err(MidgeError::InvalidArgument(
                            "key already exists".to_string(),
                        ));
                    }
                    intents.push(TransactionIntent::Point {
                        cf_id: *cf_id,
                        key: key.as_ref(),
                        exists: true,
                    });
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
        ops: Vec<crate::runtime::TransactionOp>,
        sequence_plan: &TxnSequencePlan,
        effective_durability: DurabilityPolicy,
    ) -> MidgeResult<(Vec<TransactionApplyOp>, TxnWalBatch)> {
        let skip_wal = matches!(effective_durability, DurabilityPolicy::BestEffort);
        let encode_wal = !skip_wal && self.writer.is_some();
        let apply_ops = Self::build_apply_ops(
            ops,
            sequence_plan.first_op_seq,
            sequence_plan.txn_id,
            self.current_epoch,
        );
        let wal_record =
            self.build_transaction_batch_record(sequence_plan, &apply_ops, encode_wal)?;
        let total_wal_bytes = wal_record.as_ref().map_or(0, WalRecord::estimated_size);
        let pending_wal_records = usize::from(!skip_wal);
        Ok((
            apply_ops,
            TxnWalBatch {
                wal_record,
                total_wal_bytes,
                pending_wal_records,
            },
        ))
    }

    fn build_transaction_batch_record(
        &self,
        sequence_plan: &TxnSequencePlan,
        apply_ops: &[TransactionApplyOp],
        encode_wal: bool,
    ) -> MidgeResult<Option<WalRecord>> {
        if !encode_wal {
            return Ok(None);
        }
        let mut nested_records = Vec::with_capacity(apply_ops.len());
        for apply_op in apply_ops {
            let record = match apply_op {
                TransactionApplyOp::Put {
                    op,
                    cf_id,
                    key,
                    value,
                    expiration,
                    sequence,
                } => crate::wal::encoding::TxnBatchEncodeRecord {
                    cf_id: *cf_id,
                    op: *op,
                    key: key.as_ref(),
                    value: Some(value.as_ref()),
                    seq: *sequence,
                    expiration: *expiration,
                    range_end: None,
                    txn_id: Some(sequence_plan.txn_id),
                    writer_epoch: self.current_epoch,
                },
                TransactionApplyOp::Delete {
                    cf_id,
                    key,
                    sequence,
                } => crate::wal::encoding::TxnBatchEncodeRecord {
                    cf_id: *cf_id,
                    op: WalOpKind::Delete,
                    key: key.as_ref(),
                    value: None,
                    seq: *sequence,
                    expiration: None,
                    range_end: None,
                    txn_id: Some(sequence_plan.txn_id),
                    writer_epoch: self.current_epoch,
                },
                TransactionApplyOp::DeleteRange {
                    cf_id,
                    start_key,
                    end_key,
                    sequence,
                } => crate::wal::encoding::TxnBatchEncodeRecord {
                    cf_id: *cf_id,
                    op: WalOpKind::DeleteRange,
                    key: start_key.as_ref(),
                    value: None,
                    seq: *sequence,
                    expiration: None,
                    range_end: Some(end_key.as_ref()),
                    txn_id: Some(sequence_plan.txn_id),
                    writer_epoch: self.current_epoch,
                },
            };
            nested_records.push(record);
        }

        let payload = crate::wal::encoding::encode_txn_batch_payload_records(
            sequence_plan.txn_id,
            sequence_plan.begin_seq,
            sequence_plan.commit_seq,
            self.current_epoch,
            &nested_records,
        )
        .map_err(|error| {
            MidgeError::Corruption(format!(
                "failed to encode transaction batch {}: {error}",
                sequence_plan.txn_id
            ))
        })?;

        let mut batch_record = WalRecord::new_cf(
            0,
            WalOpKind::TxnBatch,
            Bytes::from_static(b"txn"),
            Some(payload),
            sequence_plan.commit_seq,
            self.current_epoch,
        );
        batch_record.txn_id = Some(sequence_plan.txn_id);
        Ok(Some(batch_record))
    }

    fn append_prepared_transaction_batches(
        &mut self,
        state: &mut RuntimeState,
        prepared_transactions: &[PreparedTransactionAppend],
    ) -> MidgeResult<()> {
        let mut records = Vec::with_capacity(prepared_transactions.len());
        let mut total_wal_bytes = 0usize;
        let mut pending_wal_records = 0usize;

        for prepared in prepared_transactions {
            if let Some(record) = prepared.wal_batch.wal_record.as_ref() {
                records.push(record.clone());
            }
            total_wal_bytes += prepared.wal_batch.total_wal_bytes;
            pending_wal_records += prepared.wal_batch.pending_wal_records;
        }

        if records.is_empty() {
            state.wal.pending_writes += pending_wal_records;
            self.pending_sync_count += pending_wal_records;
            return Ok(());
        }

        if let Some(writer) = &mut self.writer {
            crate::failpoints::fail_point!(
                "midge::wal::inject_no_space_on_txn_append_batch",
                Self::should_inject_txn_append_batch_no_space(prepared_transactions),
                |_| Err(MidgeError::NoSpace(
                    "failpoint: no space on transaction batch append".to_string()
                ))
            );
            crate::failpoints::fail_point!(
                "midge::wal::inject_no_space_on_txn_commit_append",
                |_| Err(MidgeError::NoSpace(
                    "failpoint: no space on transaction batch append".to_string()
                ))
            );
            crate::failpoints::fail_point!("midge::wal::txn_after_ops_append_before_commit");
            let append_start = Instant::now();
            if let Err(error) = writer.append_batch(&records) {
                if matches!(error, MidgeError::NoSpace(_)) {
                    Self::record_no_space_event();
                }
                return Err(error);
            }
            self.finish_append_instrumentation(total_wal_bytes as u64, append_start.elapsed());
            if let Some(max_sequence) = records.iter().map(|record| record.seq).max() {
                self.record_segment_sequence(max_sequence);
            }
            crate::failpoints::fail_point!("midge::wal::after_append_batch_before_sync");
        }

        state.wal.pending_writes += pending_wal_records;
        self.pending_sync_count += pending_wal_records;
        self.bytes_since_sync += total_wal_bytes;
        Ok(())
    }

    #[cfg(all(test, feature = "failpoints"))]
    fn should_inject_txn_append_batch_no_space(
        prepared_transactions: &[PreparedTransactionAppend],
    ) -> bool {
        let target_request_id =
            TXN_APPEND_BATCH_NO_SPACE_FAILPOINT_REQUEST_ID.load(Ordering::SeqCst);
        target_request_id != TXN_APPEND_BATCH_NO_SPACE_FAILPOINT_DISABLED_REQUEST_ID
            && prepared_transactions
                .iter()
                .any(|prepared| prepared.request_id == target_request_id)
    }

    #[cfg(all(not(test), feature = "failpoints"))]
    fn should_inject_txn_append_batch_no_space(
        _prepared_transactions: &[PreparedTransactionAppend],
    ) -> bool {
        true
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
                crate::failpoints::fail_point!("midge::wal::txn_after_commit_append_before_sync");
                self.sync_internal(state)?;
                state.wal.local_durable_seq = last_sequence;
                crate::failpoints::fail_point!("midge::wal::txn_after_sync_before_ack");
            }
            DurabilityPolicy::Batched => {
                state.pending_txn_min_seq = Some(
                    state
                        .pending_txn_min_seq
                        .map_or(begin_seq, |pending| pending.min(begin_seq)),
                );
                if state.pending_txn_start_time.is_none() {
                    state.pending_txn_start_time = Some(Instant::now());
                }
                if let Some(telemetry) = crate::telemetry::Telemetry::global() {
                    telemetry.metrics().record_pending_txn_started();
                }
            }
            DurabilityPolicy::CloudAsync | DurabilityPolicy::BestEffort => {}
        }
        Ok(())
    }

    #[cfg(test)]
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
        #[cfg(test)]
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

    #[cfg(not(test))]
    fn apply_transaction_ops(
        state: &mut RuntimeState,
        apply_ops: Vec<TransactionApplyOp>,
        effective_durability: DurabilityPolicy,
        last_sequence: u64,
        apply_op_count: usize,
    ) -> MidgeResult<()> {
        if !matches!(effective_durability, DurabilityPolicy::CloudAsync) {
            return Self::apply_ops_to_memtables(state, apply_ops);
        }
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

    #[cfg(test)]
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
                ..
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

                    if let Some(latest_seq) = Self::latest_key_sequence(state, *cf_id, key)? {
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
                crate::runtime::TransactionOp::DeleteRange {
                    cf_id,
                    start_key,
                    end_key,
                } => {
                    if let Some(latest_seq) = Self::latest_range_sequence(
                        state,
                        *cf_id,
                        start_key.as_ref(),
                        end_key.as_ref(),
                    )? {
                        if latest_seq > start_sequence {
                            Self::record_write_conflict_range();
                            return Err(MidgeError::WriteConflict(format!(
                                "range conflict on cf {cf_id} for [{start_key:?}, {end_key:?}): latest seq {latest_seq} > tx start {start_sequence}"
                            )));
                        }
                    }

                    if let Some(range_seq) = state.latest_overlapping_delete_range_sequence(
                        *cf_id,
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
    ) -> MidgeResult<Option<u64>> {
        let Some(cf_state) = state.column_families.get(&cf_id) else {
            return Ok(None);
        };
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
    ) -> MidgeResult<Option<u64>> {
        let Some(cf_state) = state.column_families.get(&cf_id) else {
            return Ok(None);
        };
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
        ops: Vec<crate::runtime::TransactionOp>,
        first_op_seq: u64,
        _txn_id: u64,
        current_epoch: u64,
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

                    let expiration = match ttl_seconds {
                        Some(ttl) if ttl > 0 => {
                            WalRecord::new_with_ttl(
                                cf_id,
                                op_kind,
                                key.clone(),
                                Some(value.clone()),
                                seq,
                                ttl,
                                current_epoch,
                            )
                            .expiration
                        }
                        _ => None,
                    };

                    apply_ops.push(TransactionApplyOp::Put {
                        op: op_kind,
                        cf_id,
                        key,
                        value,
                        expiration,
                        sequence: seq,
                    });
                }
                crate::runtime::TransactionOp::Delete { cf_id, key } => {
                    apply_ops.push(TransactionApplyOp::Delete {
                        cf_id,
                        key,
                        sequence: seq,
                    });
                }
                crate::runtime::TransactionOp::DeleteRange {
                    cf_id,
                    start_key,
                    end_key,
                } => {
                    apply_ops.push(TransactionApplyOp::DeleteRange {
                        cf_id,
                        start_key,
                        end_key,
                        sequence: seq,
                    });
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
                    ..
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
        let Some(cf_state) = state.column_families.get(&cf_id) else {
            return false;
        };
        let sst_files = state
            .manifest
            .files
            .iter()
            .filter(|file| file.cf_id == cf_id)
            .cloned()
            .collect();
        let sst_path_prefix = state
            .sst_dir
            .strip_prefix(&state.db_path)
            .unwrap_or_else(|_| std::path::Path::new("sst"))
            .to_path_buf();
        let snapshot = crate::runtime::ReadSnapshot::new(
            cf_state.memtable.clone(),
            cf_state.immutable_memtables.clone(),
            sst_files,
            Arc::clone(&state.fs),
            sst_path_prefix,
            state.is_memory_mode(),
        );
        matches!(snapshot.get(key, u64::MAX), Ok(Some(_)))
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

        #[cfg(test)]
        {
            self.cloud_write_queue.contains_key(cf_id, key)
        }
        #[cfg(not(test))]
        {
            false
        }
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
            state.recompute_total_memtable_bytes();
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
            state.recompute_total_memtable_bytes();
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
            crate::failpoints::fail_point!("midge::wal::inject_no_space_on_sync", |_| Err(
                MidgeError::NoSpace("failpoint: no space on WAL sync".to_string())
            ));

            // Bound the acknowledgement wait so a degraded storage device cannot
            // starve the event loop indefinitely.
            let start = Instant::now();
            let fsync_timeout = self.storage_io_timeout;

            let sync_result = std::panic::catch_unwind(std::panic::AssertUnwindSafe(|| {
                writer.sync_with_timeout(fsync_timeout)
            }));

            let elapsed = start.elapsed();

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

            // Record the logical WAL sync here. The writer runner owns the
            // physical fsync count and latency metrics at the filesystem call
            // boundary so one barrier is never counted at multiple layers.
            if let Some(t) = crate::telemetry::Telemetry::global() {
                t.metrics().record_wal_sync();
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

            crate::failpoints::fail_point!("midge::wal::after_fsync_before_durable_frontier");
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

    /// Return how long the event loop can sleep before the batched sync deadline.
    #[must_use]
    pub fn sync_deadline_timeout(&self) -> Option<Duration> {
        if !self.has_pending_data() {
            return None;
        }

        let max_delay = Duration::from_millis(self.batch_config.max_delay_ms);
        Some(max_delay.saturating_sub(self.last_sync_instant.elapsed()))
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

        if let Some(fs) = self.wal_fs.as_ref().map(Arc::clone) {
            // Rename the mutable active WAL to its immutable sealed segment name.
            let old_path = FsPath::new(crate::wal::ACTIVE_FILE_NAME);
            let new_path = FsPath::new(crate::wal::segment_file_name(old_segment));

            match fs.rename_atomic(&old_path, &new_path) {
                Ok(()) => {}
                Err(FsError::NotFound(_)) if self.can_ignore_missing_active_segment() => {
                    tracing::debug!(
                        old_segment,
                        "WAL rotate ignored missing empty active segment"
                    );
                }
                Err(error) => {
                    self.restore_active_writer_after_failed_rotate(&fs);
                    tracing::error!(
                        old_segment,
                        error = ?error,
                        "WAL rotate failed while sealing active segment"
                    );
                    return Err(MidgeError::from(error));
                }
            }

            // Create new writer for the next segment
            let factory =
                FsWalFactoryIo::new(Arc::clone(&fs)).with_io_timeout(self.storage_io_timeout);
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

    fn can_ignore_missing_active_segment(&self) -> bool {
        self.segment_max_sequence == 0 && !self.has_pending_data()
    }

    fn restore_active_writer_after_failed_rotate(&mut self, fs: &Arc<dyn Fs>) {
        let factory = FsWalFactoryIo::new(Arc::clone(fs)).with_io_timeout(self.storage_io_timeout);
        match factory.create_writer(crate::wal::ACTIVE_FILE_NAME) {
            Ok(writer) => self.writer = Some(writer),
            Err(error) => {
                tracing::error!(
                    error = ?error,
                    "failed to reopen active WAL writer after rotate failure"
                );
            }
        }
    }

    /// Handle cloud upload completion (`CloudAsync` durability)
    ///
    /// Updates `cloud_durable_seq` and completes any pending writes
    /// by applying them to memtable.
    #[cfg(test)]
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
        }

        Ok(())
    }

    #[cfg(test)]
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
        #[cfg(test)]
        {
            let drained_writes = self
                .cloud_write_queue
                .drain_until(state.wal.cloud_durable_seq);

            for pending in drained_writes {
                Self::apply_pending_cloud_write(state, pending)?;
            }
        }

        Ok(())
    }

    #[cfg(not(test))]
    pub fn handle_cloud_upload_complete(
        &mut self,
        state: &mut RuntimeState,
        segment_id: u64,
        max_seq_in_segment: u64,
    ) {
        state.wal.cloud_durable_seq = state.wal.cloud_durable_seq.max(max_seq_in_segment);
        self.cloud_pending_writes = false;

        tracing::debug!(
            segment_id,
            cloud_durable_seq = state.wal.cloud_durable_seq,
            "Cloud upload complete"
        );
    }

    pub fn handle_cloud_upload_failed(&mut self, segment_id: u64, error: &str) {
        tracing::error!(segment_id, error, "Cloud upload failed");

        #[cfg(test)]
        self.cloud_write_queue.clear();
        #[cfg(not(test))]
        {
            self.cloud_pending_writes = false;
        }
    }

    /// Handle sync completion notification
    #[cfg(test)]
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
    use crate::io::traits::{DirEntry, FsError, Metadata};
    use crate::io::{
        Durability as FsDurability, File, Fs, FsPath, FsResult, MockFs,
        OpenOptions as FsOpenOptions,
    };
    use crate::runtime::RuntimeState;
    use bytes::Bytes;
    use std::path::PathBuf;
    #[cfg(feature = "failpoints")]
    use std::sync::{Mutex, OnceLock};

    #[cfg(feature = "failpoints")]
    static FAILPOINT_TEST_LOCK: OnceLock<Mutex<()>> = OnceLock::new();

    #[cfg(feature = "failpoints")]
    fn failpoint_test_lock() -> &'static Mutex<()> {
        FAILPOINT_TEST_LOCK.get_or_init(|| Mutex::new(()))
    }

    #[cfg(feature = "failpoints")]
    struct TxnAppendBatchNoSpaceFailpointGuard;

    #[cfg(feature = "failpoints")]
    impl TxnAppendBatchNoSpaceFailpointGuard {
        fn setup(request_id: u64) -> Self {
            set_txn_append_batch_no_space_failpoint_request_id(Some(request_id));
            fail::cfg("midge::wal::inject_no_space_on_txn_append_batch", "return")
                .expect("configure txn append batch no-space failpoint");
            Self
        }
    }

    #[cfg(feature = "failpoints")]
    impl Drop for TxnAppendBatchNoSpaceFailpointGuard {
        fn drop(&mut self) {
            fail::remove("midge::wal::inject_no_space_on_txn_append_batch");
            set_txn_append_batch_no_space_failpoint_request_id(None);
        }
    }

    struct RenameFailingFs {
        inner: MockFs,
    }

    impl RenameFailingFs {
        fn new() -> Self {
            Self {
                inner: MockFs::new(),
            }
        }
    }

    impl Fs for RenameFailingFs {
        fn open(&self, path: &FsPath, opts: FsOpenOptions) -> FsResult<Box<dyn File + '_>> {
            self.inner.open(path, opts)
        }

        fn open_persistent_handle(
            &self,
            path: &FsPath,
            opts: FsOpenOptions,
        ) -> FsResult<Box<dyn File>> {
            self.inner.open_persistent_handle(path, opts)
        }

        fn remove_file(&self, path: &FsPath) -> FsResult<()> {
            self.inner.remove_file(path)
        }

        fn exists(&self, path: &FsPath) -> FsResult<bool> {
            self.inner.exists(path)
        }

        fn metadata(&self, path: &FsPath) -> FsResult<Metadata> {
            self.inner.metadata(path)
        }

        fn create_dir_all(&self, path: &FsPath) -> FsResult<()> {
            self.inner.create_dir_all(path)
        }

        fn list_dir(&self, path: &FsPath) -> FsResult<Vec<DirEntry>> {
            self.inner.list_dir(path)
        }

        fn remove_dir_all(&self, path: &FsPath) -> FsResult<()> {
            self.inner.remove_dir_all(path)
        }

        fn sync_dir(&self, path: &FsPath, dur: FsDurability) -> FsResult<()> {
            self.inner.sync_dir(path, dur)
        }

        fn rename_atomic(&self, from: &FsPath, _to: &FsPath) -> FsResult<()> {
            Err(FsError::Unavailable(format!(
                "rename failed for {}",
                from.0
            )))
        }
    }

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
            crate::config::DEFAULT_STORAGE_IO_TIMEOUT,
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
            crate::config::DEFAULT_STORAGE_IO_TIMEOUT,
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
            crate::runtime::ConflictPolicy::LastWriteWins,
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
        assert!(
            wal_actor.sync_deadline_timeout().is_some(),
            "pending data should expose a sync deadline timeout"
        );

        Ok(())
    }

    #[test]
    fn should_append_multiple_prepared_transactions_with_one_physical_call() -> MidgeResult<()> {
        // Arrange
        let temp = tempfile::tempdir().map_err(crate::common::MidgeError::Io)?;
        let db_path = temp.path().to_path_buf();
        let wal_dir = db_path.join("wal");
        let mut state = RuntimeState::new(db_path, false);
        let mut wal_actor = WalActor::new(
            wal_dir,
            DurabilityPolicy::Batched,
            BatchConfig::default(),
            false,
            1,
            crate::config::DEFAULT_STORAGE_IO_TIMEOUT,
        )?;

        let first = wal_actor.prepare_transaction_append(
            &mut state,
            TransactionAppendParams {
                request_id: 10,
                ops: vec![crate::runtime::TransactionOp::Put {
                    cf_id: 0,
                    key: Bytes::from_static(b"coalesce-a"),
                    value: Bytes::from_static(b"value-a"),
                    ttl_seconds: None,
                    insert_only: false,
                }],
                durability_policy: Some(DurabilityPolicy::Batched),
                start_sequence: None,
                conflict_policy: crate::runtime::ConflictPolicy::LastWriteWins,
            },
        )?;
        let second = wal_actor.prepare_transaction_append(
            &mut state,
            TransactionAppendParams {
                request_id: 11,
                ops: vec![crate::runtime::TransactionOp::Put {
                    cf_id: 0,
                    key: Bytes::from_static(b"coalesce-b"),
                    value: Bytes::from_static(b"value-b"),
                    ttl_seconds: Some(60),
                    insert_only: true,
                }],
                durability_policy: Some(DurabilityPolicy::Batched),
                start_sequence: None,
                conflict_policy: crate::runtime::ConflictPolicy::LastWriteWins,
            },
        )?;

        // Act
        let results = wal_actor.append_prepared_transactions(&mut state, vec![first, second])?;

        // Assert
        assert_eq!(results.len(), 2);
        assert_eq!(wal_actor.append_calls(), 1);
        assert_eq!(state.wal.pending_writes, 2);
        assert_eq!(wal_actor.pending_sync_count(), 2);
        assert!(state.pending_txn_min_seq.is_some());

        let cf_state = state.get_cf(0).expect("cf exists");
        let entries = cf_state.memtable.iter_all(u64::MAX);
        assert!(entries.iter().any(|(key, value, _sequence)| {
            key.as_slice() == b"coalesce-a"
                && value
                    .as_ref()
                    .is_some_and(|bytes| bytes.as_slice() == b"value-a")
        }));
        assert!(entries.iter().any(|(key, value, _sequence)| {
            key.as_slice() == b"coalesce-b"
                && value
                    .as_ref()
                    .is_some_and(|bytes| bytes.as_slice() == b"value-b")
        }));

        Ok(())
    }

    #[cfg(feature = "failpoints")]
    #[test]
    fn should_fail_all_prepared_transactions_when_batch_append_hits_no_space() -> MidgeResult<()> {
        // Arrange
        let _guard = failpoint_test_lock().lock().expect("lock failpoint tests");
        let temp = tempfile::tempdir().map_err(crate::common::MidgeError::Io)?;
        let db_path = temp.path().to_path_buf();
        let wal_dir = db_path.join("wal");
        let mut state = RuntimeState::new(db_path, false);
        let mut wal_actor = WalActor::new(
            wal_dir,
            DurabilityPolicy::Batched,
            BatchConfig::default(),
            false,
            1,
            crate::config::DEFAULT_STORAGE_IO_TIMEOUT,
        )?;

        let first = wal_actor.prepare_transaction_append(
            &mut state,
            TransactionAppendParams {
                request_id: 20,
                ops: vec![crate::runtime::TransactionOp::Put {
                    cf_id: 0,
                    key: Bytes::from_static(b"failed-a"),
                    value: Bytes::from_static(b"value-a"),
                    ttl_seconds: None,
                    insert_only: false,
                }],
                durability_policy: Some(DurabilityPolicy::Batched),
                start_sequence: None,
                conflict_policy: crate::runtime::ConflictPolicy::LastWriteWins,
            },
        )?;
        let second = wal_actor.prepare_transaction_append(
            &mut state,
            TransactionAppendParams {
                request_id: 21,
                ops: vec![crate::runtime::TransactionOp::Delete {
                    cf_id: 0,
                    key: Bytes::from_static(b"failed-b"),
                }],
                durability_policy: Some(DurabilityPolicy::Batched),
                start_sequence: None,
                conflict_policy: crate::runtime::ConflictPolicy::LastWriteWins,
            },
        )?;
        assert!(
            state.sequence > 0,
            "preparation should preserve existing sequence allocation behavior"
        );

        {
            let scenario = fail::FailScenario::setup();
            let failpoint_guard = TxnAppendBatchNoSpaceFailpointGuard::setup(20);

            // Act
            let result = wal_actor.append_prepared_transactions(&mut state, vec![first, second]);

            // Assert
            assert!(result.is_err(), "coalesced append should fail");
            let error = result.err().expect("coalesced append error");
            assert!(matches!(error, MidgeError::NoSpace(_)));
            assert_eq!(wal_actor.append_calls(), 0);
            assert_eq!(state.wal.pending_writes, 0);
            assert_eq!(wal_actor.pending_sync_count(), 0);
            assert_eq!(wal_actor.bytes_since_sync(), 0);
            assert_eq!(state.pending_txn_min_seq, None);
            assert!(
                state
                    .get_cf(0)
                    .expect("cf exists")
                    .memtable
                    .iter_all(u64::MAX)
                    .is_empty(),
                "failed coalesced append must not publish partial memtable state"
            );

            drop(failpoint_guard);
            scenario.teardown();
        }

        let recovery = wal_actor.prepare_transaction_append(
            &mut state,
            TransactionAppendParams {
                request_id: 22,
                ops: vec![crate::runtime::TransactionOp::Put {
                    cf_id: 0,
                    key: Bytes::from_static(b"recovered"),
                    value: Bytes::from_static(b"value"),
                    ttl_seconds: None,
                    insert_only: false,
                }],
                durability_policy: Some(DurabilityPolicy::Batched),
                start_sequence: None,
                conflict_policy: crate::runtime::ConflictPolicy::LastWriteWins,
            },
        )?;
        wal_actor.append_prepared_transactions(&mut state, vec![recovery])?;
        assert!(state
            .get_cf(0)
            .expect("cf exists")
            .memtable
            .iter_all(u64::MAX)
            .iter()
            .any(|(key, value, _sequence)| {
                key.as_slice() == b"recovered"
                    && value
                        .as_ref()
                        .is_some_and(|bytes| bytes.as_slice() == b"value")
            }));

        Ok(())
    }

    #[test]
    fn should_not_report_sync_deadline_without_pending_data() -> MidgeResult<()> {
        // Arrange
        let temp = tempfile::tempdir().map_err(crate::common::MidgeError::Io)?;
        let wal_actor = WalActor::new(
            temp.path().to_path_buf(),
            DurabilityPolicy::Batched,
            BatchConfig::default(),
            false,
            1,
            crate::config::DEFAULT_STORAGE_IO_TIMEOUT,
        )?;

        // Act
        let deadline = wal_actor.sync_deadline_timeout();

        // Assert
        assert_eq!(deadline, None);
        Ok(())
    }

    #[test]
    fn should_only_coalesce_local_batched_transaction_appends() -> MidgeResult<()> {
        // Arrange
        let temp = tempfile::tempdir().map_err(crate::common::MidgeError::Io)?;
        let batched_actor = WalActor::new(
            temp.path().join("batched"),
            DurabilityPolicy::Batched,
            BatchConfig::default(),
            false,
            1,
            crate::config::DEFAULT_STORAGE_IO_TIMEOUT,
        )?;
        let strict_actor = WalActor::new(
            temp.path().join("strict"),
            DurabilityPolicy::Strict,
            BatchConfig::default(),
            false,
            1,
            crate::config::DEFAULT_STORAGE_IO_TIMEOUT,
        )?;
        let cloud_actor = WalActor::new(
            temp.path().join("cloud"),
            DurabilityPolicy::CloudAsync,
            BatchConfig::default(),
            false,
            1,
            crate::config::DEFAULT_STORAGE_IO_TIMEOUT,
        )?;
        let memory_actor = WalActor::new(
            temp.path().join("memory"),
            DurabilityPolicy::Batched,
            BatchConfig::default(),
            true,
            1,
            crate::config::DEFAULT_STORAGE_IO_TIMEOUT,
        )?;

        // Act
        // Assert
        assert!(batched_actor.can_coalesce_transaction_append(Some(DurabilityPolicy::Batched)));
        assert!(!batched_actor.can_coalesce_transaction_append(Some(DurabilityPolicy::Strict)));
        assert!(!batched_actor.can_coalesce_transaction_append(Some(DurabilityPolicy::BestEffort)));
        assert!(!batched_actor.can_coalesce_transaction_append(Some(DurabilityPolicy::CloudAsync)));
        assert!(!strict_actor.can_coalesce_transaction_append(None));
        assert!(!cloud_actor.can_coalesce_transaction_append(Some(DurabilityPolicy::Batched)));
        assert!(!memory_actor.can_coalesce_transaction_append(Some(DurabilityPolicy::Batched)));

        Ok(())
    }

    #[test]
    fn should_rotate_to_lex_sortable_wal_segment_name() -> MidgeResult<()> {
        // Arrange
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
            crate::config::DEFAULT_STORAGE_IO_TIMEOUT,
        )?;

        // Act
        wal_actor.rotate(&mut state)?;

        // Assert
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

    #[test]
    fn should_fail_rotate_without_advancing_segment_when_rename_fails() -> MidgeResult<()> {
        // Arrange
        let temp = tempfile::tempdir().map_err(crate::common::MidgeError::Io)?;
        let db_path = temp.path().to_path_buf();
        let fs: Arc<dyn Fs> = Arc::new(RenameFailingFs::new());
        let writer =
            FsWalFactoryIo::new(Arc::clone(&fs)).create_writer(crate::wal::ACTIVE_FILE_NAME)?;
        let mut state = RuntimeState::new(db_path.clone(), true);
        state.wal.current_segment_id = 7;

        let mut wal_actor = WalActor::new(
            db_path.join("unused-wal"),
            DurabilityPolicy::Strict,
            BatchConfig::default(),
            true,
            1,
            crate::config::DEFAULT_STORAGE_IO_TIMEOUT,
        )?;
        wal_actor.wal_fs = Some(fs);
        wal_actor.writer = Some(writer);
        wal_actor.segment_max_sequence = 42;

        // Act
        let result = wal_actor.rotate(&mut state);

        // Assert
        assert!(result.is_err());
        assert_eq!(state.wal.current_segment_id, 7);
        assert_eq!(wal_actor.segment_max_sequence, 42);
        assert!(wal_actor.writer.is_some());

        Ok(())
    }
}
