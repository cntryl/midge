//! WAL Actor - handles write-ahead log operations
//!
//! Responsible for:
//! - Appending records to WAL
//! - Syncing WAL to disk
//! - Rotating WAL segments
//! - Handing sealed WAL segments to hybrid storage for cloud upload
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
//!
//! TRANSITION INVARIANT (see `crate::runtime::wal_transition`):
//! A durability transition (fsync, rotate, cloud seal, cloud ack) is committed
//! only when the actor, the durability coordinator, runtime frontiers, sealed
//! segment ownership, and durability waiters all moved together. The actor
//! enforces its part with `WalIoState`:
//!
//! - durable work (any policy other than explicit `BestEffort`) is accepted only
//!   in `Open`, before any sequence is allocated or memtable mutated;
//! - every transition installs `Transitioning` before its first irreversible or
//!   ambiguous I/O step and returns to `Open` only when it fully completed;
//! - any failure after an irreversible step lands in `Fenced`, which rejects
//!   durable work, degrades health, and records a sealed segment that needs
//!   restart recovery. A transition never returns while still `Transitioning`;
//! - the frontier-moving entry points (`begin_sync_transition`,
//!   `commit_sync_transition`, `flush_for_cloud_upload_within`, `rotate`,
//!   `complete_cloud_upload_seal`, `cancel_reversible_cloud_flush`) require a
//!   protocol ticket or rotation receipt, so no caller can run half of a
//!   paired transition.

use super::super::state::RuntimeState;
use crate::common::{MidgeError, MidgeResult};
use crate::io::FsError;
use crate::io::{Fs, RealFs};
use crate::runtime::wal_transition::WalSealTicket;
use crate::wal::policy::BatchConfig;
use crate::wal::{DurabilityPolicy, FsWalFactoryIo, WalRecord, WalWriter};
use apply_op::TransactionApplyOp;
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

#[cfg(test)]
pub(crate) use test_append::AppendParams;

/// Operational state of the filesystem WAL.
///
/// A filesystem-backed actor may accept durable work only in `Open`. Rotation
/// and fsync install `Transitioning` before their first irreversible or
/// potentially ambiguous I/O step. Any failure from that point is represented
/// as `Fenced`; the actor cannot accidentally behave as though its old writer
/// were still usable.
enum WalIoState {
    Memory,
    Open {
        fs: Arc<dyn Fs>,
        writer: Box<dyn WalWriter>,
    },
    Transitioning {
        fs: Arc<dyn Fs>,
        writer: Option<Box<dyn WalWriter>>,
        operation: WalTransitionOperation,
        sealed_segment: Option<u64>,
    },
    Fenced {
        fs: Arc<dyn Fs>,
        reason: String,
        sealed_segment: Option<u64>,
        /// This writer may be stale (lease or epoch check failed), as opposed
        /// to a local I/O failure that poisoned the WAL.
        authority_lost: bool,
    },
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
enum WalTransitionOperation {
    Sync,
    CloudFlush,
    Rotate,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(crate) struct WalSyncReceipt {
    durable_sequence: u64,
    pending_writes: usize,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(crate) struct WalRotationReceipt {
    sealed_segment: u64,
    next_segment: u64,
    max_sequence: u64,
}

impl WalRotationReceipt {
    #[must_use]
    pub(crate) fn sealed_segment(self) -> u64 {
        self.sealed_segment
    }

    #[must_use]
    pub(crate) fn next_segment(self) -> u64 {
        self.next_segment
    }

    #[must_use]
    pub(crate) fn max_sequence(self) -> u64 {
        self.max_sequence
    }

    #[cfg(test)]
    pub(crate) fn for_test(sealed_segment: u64, next_segment: u64, max_sequence: u64) -> Self {
        Self {
            sealed_segment,
            next_segment,
            max_sequence,
        }
    }
}

/// Actor handling WAL operations
pub struct WalActor {
    io: WalIoState,
    /// Operational counters of the owning engine; shared with every writer.
    counters: crate::telemetry::CounterSink,
    storage_budget: Option<Arc<crate::storage::HybridStorage>>,
    /// The event loop's read resources (reader and block caches), shared by
    /// transaction validation so its SST reads reuse cached readers (#492).
    read_resources: Option<Arc<crate::runtime::read_resources::ReadResources>>,
    /// Buffered writes pending sync
    pending_sync_count: usize,
    /// Durability policy (determines sync behavior)
    durability_policy: DurabilityPolicy,
    /// Bytes written since last sync (for batched mode)
    bytes_since_sync: usize,
    /// Highest sequence actually appended to the current WAL segment.
    segment_max_sequence: u64,
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
    /// This writer's own lease holder identity, checked alongside epoch at
    /// each fencing validation.
    leader_holder_id: String,
    /// Largest transaction WAL footprint recovery can replay, when bounded.
    max_replayable_txn_bytes: Option<usize>,

    // === Optional instrumentation ===
    sync_calls: u64,
    sync_total: Duration,

    // WAL append instrumentation
    append_calls: u64,
    append_total: Duration,
}

mod apply_op;
mod durability;
mod rotation;
mod spill;
#[cfg(test)]
mod test_append;
mod transaction;
mod transaction_state;

impl WalActor {
    #[cfg(test)]
    pub(crate) fn replace_writer_for_test(&mut self, writer: Box<dyn WalWriter>) {
        match &mut self.io {
            WalIoState::Open {
                writer: current, ..
            } => *current = writer,
            WalIoState::Memory => {
                self.io = WalIoState::Open {
                    fs: Arc::new(crate::io::MockFs::new()),
                    writer,
                };
            }
            WalIoState::Transitioning { .. } | WalIoState::Fenced { .. } => {
                panic!("cannot replace a WAL writer while the actor is not open")
            }
        }
    }

    #[cfg(test)]
    pub(crate) fn install_filesystem_for_test(
        &mut self,
        fs: Arc<dyn Fs>,
        writer: Box<dyn WalWriter>,
    ) {
        self.io = WalIoState::Open { fs, writer };
    }

    fn writer(&self) -> Option<&dyn WalWriter> {
        match &self.io {
            WalIoState::Open { writer, .. }
            | WalIoState::Transitioning {
                writer: Some(writer),
                ..
            } => Some(writer.as_ref()),
            WalIoState::Memory
            | WalIoState::Transitioning { writer: None, .. }
            | WalIoState::Fenced { .. } => None,
        }
    }

    fn writer_mut(&mut self) -> Option<&mut (dyn WalWriter + '_)> {
        match &mut self.io {
            WalIoState::Open { writer, .. }
            | WalIoState::Transitioning {
                writer: Some(writer),
                ..
            } => Some(writer.as_mut()),
            WalIoState::Memory
            | WalIoState::Transitioning { writer: None, .. }
            | WalIoState::Fenced { .. } => None,
        }
    }

    fn filesystem(&self) -> Option<Arc<dyn Fs>> {
        match &self.io {
            WalIoState::Open { fs, .. }
            | WalIoState::Transitioning { fs, .. }
            | WalIoState::Fenced { fs, .. } => Some(Arc::clone(fs)),
            WalIoState::Memory => None,
        }
    }

    /// Whether the actor can accept durable work right now.
    pub(crate) fn is_open(&self) -> bool {
        matches!(self.io, WalIoState::Memory | WalIoState::Open { .. })
    }

    pub(crate) fn is_fenced(&self) -> bool {
        matches!(self.io, WalIoState::Fenced { .. })
    }

    /// Whether the actor is fenced because writer authority was lost or could
    /// not be proven, as opposed to a local failure poisoning the WAL.
    pub(crate) fn authority_lost(&self) -> bool {
        matches!(
            self.io,
            WalIoState::Fenced {
                authority_lost: true,
                ..
            }
        )
    }

    pub(crate) fn fenced_sealed_segment(&self) -> Option<u64> {
        match self.io {
            WalIoState::Transitioning { sealed_segment, .. }
            | WalIoState::Fenced { sealed_segment, .. } => sealed_segment,
            WalIoState::Memory | WalIoState::Open { .. } => None,
        }
    }

    /// Roll back a `CloudFlush` transition that has not yet renamed anything.
    ///
    /// Requires the seal ticket of the transition being cancelled so the
    /// rollback cannot be issued outside the protocol-owned seal.
    pub(crate) fn cancel_reversible_cloud_flush(
        &mut self,
        _ticket: &WalSealTicket,
    ) -> MidgeResult<()> {
        match self.io {
            WalIoState::Transitioning {
                operation: WalTransitionOperation::CloudFlush,
                sealed_segment: None,
                ..
            } => self.finish_io_transition(),
            WalIoState::Memory | WalIoState::Open { .. } => Ok(()),
            _ => Err(self.io_error().unwrap_or_else(|| {
                MidgeError::Internal("WAL cloud flush cannot be rolled back".to_string())
            })),
        }
    }

    fn begin_io_transition(&mut self, operation: WalTransitionOperation) -> MidgeResult<()> {
        let previous = std::mem::replace(&mut self.io, WalIoState::Memory);
        match previous {
            WalIoState::Memory => {
                self.io = WalIoState::Memory;
                Ok(())
            }
            WalIoState::Open { fs, writer } => {
                self.io = WalIoState::Transitioning {
                    fs,
                    writer: Some(writer),
                    operation,
                    sealed_segment: None,
                };
                Ok(())
            }
            WalIoState::Transitioning {
                fs,
                writer,
                operation: WalTransitionOperation::CloudFlush,
                sealed_segment: None,
            } if operation == WalTransitionOperation::Rotate => {
                self.io = WalIoState::Transitioning {
                    fs,
                    writer,
                    operation,
                    sealed_segment: None,
                };
                Ok(())
            }
            unavailable => {
                self.io = unavailable;
                Err(self.io_error().unwrap_or_else(|| {
                    MidgeError::Internal("WAL transition is unavailable".to_string())
                }))
            }
        }
    }

    fn finish_io_transition(&mut self) -> MidgeResult<()> {
        let previous = std::mem::replace(&mut self.io, WalIoState::Memory);
        match previous {
            WalIoState::Memory => Ok(()),
            WalIoState::Transitioning {
                fs,
                writer: Some(writer),
                ..
            } => {
                self.io = WalIoState::Open { fs, writer };
                Ok(())
            }
            unavailable => {
                self.io = unavailable;
                Err(self.io_error().unwrap_or_else(|| {
                    MidgeError::Internal("WAL transition lost its writer".to_string())
                }))
            }
        }
    }

    fn mark_transition_sealed(&mut self, segment_id: u64) {
        if let WalIoState::Transitioning { sealed_segment, .. } = &mut self.io {
            *sealed_segment = Some(segment_id);
        }
    }

    fn take_transition_writer(&mut self) -> Option<Box<dyn WalWriter>> {
        match &mut self.io {
            WalIoState::Transitioning { writer, .. } => writer.take(),
            WalIoState::Memory | WalIoState::Open { .. } | WalIoState::Fenced { .. } => None,
        }
    }

    fn install_transition_writer(&mut self, writer: Box<dyn WalWriter>) -> MidgeResult<()> {
        match &mut self.io {
            WalIoState::Transitioning {
                writer: current, ..
            } => {
                *current = Some(writer);
                Ok(())
            }
            _ => Err(MidgeError::Internal(
                "replacement WAL writer arrived outside rotation transition".to_string(),
            )),
        }
    }

    fn io_error(&self) -> Option<MidgeError> {
        match &self.io {
            WalIoState::Memory | WalIoState::Open { .. } => None,
            WalIoState::Transitioning {
                operation,
                sealed_segment,
                ..
            } => Some(MidgeError::RecoveryFailed(format!(
                "WAL {operation:?} transition did not complete{}; restart is required",
                sealed_segment.map_or_else(String::new, |segment| format!(
                    " after sealing segment {segment}"
                ))
            ))),
            WalIoState::Fenced {
                reason,
                sealed_segment,
                authority_lost,
                ..
            } => {
                let message = format!(
                    "{reason}{}",
                    sealed_segment.map_or_else(String::new, |segment| format!(
                        "; sealed segment {segment} requires restart recovery"
                    ))
                );
                // Only lost authority is `Fenced`. A WAL poisoned by local
                // I/O needs a restart, which is what `RecoveryFailed` says.
                Some(if *authority_lost {
                    MidgeError::Fenced(message)
                } else {
                    MidgeError::RecoveryFailed(format!("local WAL is poisoned: {message}"))
                })
            }
        }
    }

    /// Stop the WAL after a local failure it cannot recover from in place.
    pub(crate) fn fence_transition(&mut self, state: &mut RuntimeState, reason: impl Into<String>) {
        self.fence_with_cause(state, reason, false);
    }

    /// Stop the WAL, recording whether writer authority was lost (lease or
    /// epoch) or the WAL was poisoned by I/O. Once lost, authority stays lost.
    pub(crate) fn fence_with_cause(
        &mut self,
        state: &mut RuntimeState,
        reason: impl Into<String>,
        authority_lost: bool,
    ) {
        let reason = reason.into();
        let previous = std::mem::replace(&mut self.io, WalIoState::Memory);
        self.io = match previous {
            WalIoState::Memory => WalIoState::Memory,
            WalIoState::Open { fs, .. } => WalIoState::Fenced {
                fs,
                reason,
                sealed_segment: None,
                authority_lost,
            },
            WalIoState::Transitioning {
                fs, sealed_segment, ..
            } => WalIoState::Fenced {
                fs,
                reason,
                sealed_segment,
                authority_lost,
            },
            WalIoState::Fenced {
                fs,
                sealed_segment,
                authority_lost: already_lost,
                ..
            } => WalIoState::Fenced {
                fs,
                reason,
                sealed_segment,
                authority_lost: authority_lost || already_lost,
            },
        };
        if !matches!(self.io, WalIoState::Memory) {
            state.mark_persistence_anomaly();
        }
    }

    fn duration_nanos_u64(duration: Duration) -> u64 {
        u64::try_from(duration.as_nanos()).unwrap_or(u64::MAX)
    }

    fn u64_to_f64(value: u64) -> f64 {
        let upper = u32::try_from(value >> 32).unwrap_or(u32::MAX);
        let lower = u32::try_from(value & u64::from(u32::MAX)).unwrap_or(u32::MAX);
        f64::from(upper) * 4_294_967_296.0 + f64::from(lower)
    }

    /// Route this actor's (and its writers') events to an engine's counters.
    pub(crate) fn attach_counters(&self, diagnostics: &crate::diagnostics::RuntimeDiagnostics) {
        diagnostics.attach(&self.counters);
    }

    fn record_no_space_event(counters: &crate::telemetry::CounterSink) {
        counters.record(|m| {
            m.record_no_space_event();
            m.record_write_stall_no_space();
        });
    }

    fn finish_append_instrumentation(&mut self, bytes_written: u64, elapsed: Duration) {
        self.append_calls += 1;
        self.append_total += elapsed;
        self.counters.record(|m| {
            m.record_wal_append(bytes_written);
            m.record_wal_append_count();
            m.record_wal_append_ns(Self::duration_nanos_u64(elapsed));
        });
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
        let counters = crate::telemetry::CounterSink::default();
        let io = if memory_mode {
            WalIoState::Memory
        } else {
            let fs: Arc<dyn Fs> = Arc::new(RealFs::new(wal_dir).map_err(FsError::into_midge)?);
            let factory = FsWalFactoryIo::new(Arc::clone(&fs))
                .with_io_timeout(storage_io_timeout)
                .with_counters(counters.clone());
            let writer = factory.create_writer(crate::wal::ACTIVE_FILE_NAME)?;
            WalIoState::Open { fs, writer }
        };

        let actor = Self {
            io,
            counters,
            storage_budget: None,
            read_resources: None,
            pending_sync_count: 0,
            durability_policy,
            bytes_since_sync: 0,
            segment_max_sequence: 0,
            sync_calls: 0,
            sync_total: Duration::from_secs(0),
            append_calls: 0,
            append_total: Duration::from_secs(0),
            batch_config,
            last_sync_instant: Instant::now(),
            storage_io_timeout,
            current_epoch: writer_epoch,
            leader_store: None,
            leader_holder_id: String::new(),
            max_replayable_txn_bytes: None,
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

    pub(crate) fn set_max_replayable_txn_bytes(&mut self, limit: Option<usize>) {
        self.max_replayable_txn_bytes = limit;
    }

    /// Reject a transaction recovery could not replay, before any byte of it
    /// is written.
    fn ensure_replayable_transaction(&self, wal_bytes: usize) -> MidgeResult<()> {
        match self.max_replayable_txn_bytes {
            Some(limit) if wal_bytes > limit => Err(MidgeError::ResourceLimit(format!(
                "transaction needs {wal_bytes} WAL replay bytes; recovery replays at most {limit} bytes with the configured memory budget"
            ))),
            _ => Ok(()),
        }
    }

    pub(crate) fn set_storage_budget(&mut self, storage: Arc<crate::storage::HybridStorage>) {
        self.storage_budget = Some(storage);
    }

    #[cfg(test)]
    pub(crate) fn has_read_resources(&self) -> bool {
        self.read_resources.is_some()
    }

    /// Lets transaction validation read through the event loop's reader and
    /// block caches instead of opening SSTs per validated op (#492).
    pub(crate) fn set_read_resources(
        &mut self,
        resources: Option<Arc<crate::runtime::read_resources::ReadResources>>,
    ) {
        self.read_resources = resources;
    }

    fn admit_wal_records(&self, records: &[WalRecord]) -> MidgeResult<u64> {
        let Some(storage) = self
            .storage_budget
            .as_ref()
            .filter(|_| self.writer().is_some())
        else {
            return Ok(0);
        };
        if !storage.ephemeral_sst_cache_enabled() {
            return Ok(0);
        }
        let bytes = records.iter().try_fold(0_u64, |total, record| {
            let frame = crate::wal::encoding::record_frame_size_bound(record)?;
            total
                .checked_add(u64::try_from(frame).unwrap_or(u64::MAX))
                .ok_or_else(|| {
                    crate::common::MidgeError::ResourceLimit("WAL batch size overflow".into())
                })
        })?;
        storage.admit_local_wal_bytes(bytes)?;
        Ok(bytes)
    }

    fn settle_wal_append(&self, admitted: u64, previous_position: u64) {
        if admitted == 0 {
            return;
        }
        let Some(storage) = &self.storage_budget else {
            return;
        };
        let actual = self
            .writer()
            .map_or(previous_position, crate::wal::WalWriter::current_pos)
            .saturating_sub(previous_position);
        storage.settle_local_wal_admission(admitted, actual);
    }

    fn settle_failed_wal_append(
        &self,
        admitted: u64,
        failure: crate::wal::traits::WalAppendError,
    ) -> crate::common::MidgeError {
        if failure.unchanged {
            if let Some(storage) = &self.storage_budget {
                storage.settle_local_wal_admission(admitted, 0);
            }
        }
        if matches!(failure.error, crate::common::MidgeError::NoSpace(_)) {
            Self::record_no_space_event(&self.counters);
        }
        failure.error
    }

    /// Attach a leader store for epoch validation at sync boundaries, along
    /// with this writer's own holder identity.
    pub fn set_leader_store(
        &mut self,
        store: Arc<dyn crate::lease::LeaderStore>,
        holder_id: String,
    ) {
        self.leader_store = Some(store);
        self.leader_holder_id = holder_id;
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

    fn ensure_filesystem_wal_available(&self, state: &mut RuntimeState) -> MidgeResult<()> {
        if let Some(error) = self.io_error() {
            state.mark_persistence_anomaly();
            return Err(error);
        }
        Ok(())
    }

    fn ensure_write_durability_available(
        &self,
        state: &mut RuntimeState,
        durability_policy: DurabilityPolicy,
    ) -> MidgeResult<()> {
        if matches!(durability_policy, DurabilityPolicy::BestEffort) {
            return Ok(());
        }
        self.ensure_filesystem_wal_available(state)
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
            && self.writer().is_some())
        .then_some(effective_durability)
    }

    pub fn bytes_since_sync(&self) -> usize {
        self.bytes_since_sync
    }

    pub(crate) fn current_segment_max_sequence(&self) -> u64 {
        self.segment_max_sequence
    }

    #[cfg(test)]
    pub fn pending_sync_count(&self) -> usize {
        self.pending_sync_count
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
