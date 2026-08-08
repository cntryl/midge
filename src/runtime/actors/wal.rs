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

#[cfg(test)]
use super::super::state::RuntimeState;
use super::cloud_write_queue::TransactionApplyOp;
use crate::common::MidgeResult;
use crate::io::{Fs, RealFs};
use crate::wal::policy::BatchConfig;
use crate::wal::{DurabilityPolicy, FsWalFactoryIo, WalRecord, WalWriter};
#[cfg(test)]
use crate::{common::MidgeError, wal::WalOpKind};
#[cfg(test)]
use bytes::Bytes;
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
    pub assertions: Vec<crate::runtime::KeyAssertion>,
    pub durability_policy: Option<DurabilityPolicy>,
    pub start_sequence: Option<u64>,
    pub conflict_policy: crate::runtime::ConflictPolicy,
}

/// Inputs needed to append one spilled transaction. Mirrors
/// `TransactionAppendParams` for the streamed-source variant.
pub(crate) struct SpilledTransactionAppendParams {
    pub request_id: u64,
    pub assertions: Vec<crate::runtime::KeyAssertion>,
    pub durability_policy: Option<DurabilityPolicy>,
    pub start_sequence: u64,
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

mod durability;
mod memtable;
mod rotation;
mod spill;
mod transaction;

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
        self.coalesced_transaction_durability(durability_policy)
            .is_some()
    }

    pub(crate) fn coalesced_transaction_durability(
        &self,
        durability_policy: Option<DurabilityPolicy>,
    ) -> Option<DurabilityPolicy> {
        let effective_durability = durability_policy.unwrap_or(self.durability_policy);
        (matches!(
            effective_durability,
            DurabilityPolicy::Strict | DurabilityPolicy::Batched
        ) && !self.is_cloud_async()
            && self.writer.is_some())
        .then_some(effective_durability)
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
            }
            DurabilityPolicy::Batched
            | DurabilityPolicy::BestEffort
            | DurabilityPolicy::CloudAsync => {
                Self::apply_to_memtable(
                    state,
                    sequence,
                    cf_id,
                    key.clone(),
                    value.cloned(),
                    expiration,
                )?;
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
            DurabilityPolicy::Batched
            | DurabilityPolicy::BestEffort
            | DurabilityPolicy::CloudAsync => {
                Self::apply_delete_range_to_memtable(state, sequence, cf_id, start_key, end_key)?;
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
        if insert_only && Self::key_exists(state, cf_id, &key) {
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
mod tests;
