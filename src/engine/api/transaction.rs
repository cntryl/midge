//! Transaction API for multi-key ACID operations
//!
//! Provides transaction support with:
//! - Multi-key atomic operations
//! - Snapshot isolation with repeatable reads
//! - Rollback and commit semantics
//! - Column-family scoped transactions

use crate::common::{MidgeError, MidgeResult};
use crate::engine::api::write_options::effective_wal_durability_policy;
use crate::engine::ingest::IngestCoordinator;
use crate::engine::ColumnFamilyId;
use crate::runtime::read_snapshot::SnapshotScan;
use crate::runtime::transaction_spill::{
    IntentKeyScan, IntentLookup, TransactionMemoryPool, TransactionWriteSet,
};
use crate::runtime::{KeyAssertion, RuntimeHandle};
use crate::types::ConflictPolicy;
use std::path::PathBuf;
use std::sync::atomic::{AtomicU64, Ordering};
use std::sync::Arc;
use std::time::{Duration, Instant};

const ASSERTION_ACCOUNTING_OVERHEAD: usize = 256;

/// Transaction mode controls read/write capabilities
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum TransactionMode {
    /// Read-only transaction; writes forbidden
    ReadOnly,
    /// Read-write transaction; point writes are allowed
    ReadWrite,
}

/// Transaction for multi-key ACID operations
///
/// Collects read and write intents, validates them, and commits atomically.
/// Thread-safe; multiple transactions can be in flight simultaneously.
pub struct Transaction {
    runtime_handle: RuntimeHandle,
    coordinator: Arc<IngestCoordinator>,
    sequence_publisher: Arc<AtomicU64>,
    /// Column family this transaction is bound to
    cf_id: ColumnFamilyId,
    /// Transaction mode (`ReadOnly` or `ReadWrite`)
    mode: TransactionMode,
    /// Isolation behavior used during commit conflict handling.
    conflict_policy: ConflictPolicy,
    /// Bounded resident write set with optional durable spill runs.
    write_set: TransactionWriteSet,
    /// Value assertions checked before a transaction is submitted. Assertions
    /// never enter the write set and therefore do not create write conflicts.
    assertions: Vec<(Vec<u8>, Option<Vec<u8>>)>,
    assertion_bytes: usize,
    memory_pool: Arc<TransactionMemoryPool>,
    /// Start sequence number (snapshot point)
    start_sequence: u64,
    /// Immutable snapshot for direct read execution (bypasses event loop)
    read_snapshot: Option<Arc<crate::runtime::ReadSnapshot>>,
    /// True when opened against cloud-backed storage.
    cloud_mode: bool,
    /// Owns the active snapshot registration for this transaction.
    snapshot_pin: SnapshotPinGuard,
    /// Keeps the runtime alive until this transaction is fully dropped.
    _runtime_transaction_guard: crate::runtime::RuntimeTransactionGuard,
}

struct SnapshotPinGuard {
    runtime_handle: RuntimeHandle,
    snapshot_id: u64,
    active: bool,
}

impl SnapshotPinGuard {
    #[must_use]
    fn new(runtime_handle: RuntimeHandle, snapshot_id: u64) -> Self {
        Self {
            runtime_handle,
            snapshot_id,
            active: true,
        }
    }

    fn unregister(&mut self) -> bool {
        if !self.active {
            return false;
        }

        let _ = self
            .runtime_handle
            .unregister_snapshot_pin(self.snapshot_id);
        self.runtime_handle.diagnostics.record_snapshot_unregister();
        self.active = false;
        true
    }
}

impl Drop for SnapshotPinGuard {
    fn drop(&mut self) {
        let _ = self.unregister();
    }
}

struct CommitTiming {
    started_at: Instant,
    sample: crate::diagnostics::TransactionCommitTimingSample,
}

impl CommitTiming {
    fn maybe_start() -> Option<Self> {
        crate::diagnostics::transaction_commit_timing_enabled().then(|| {
            crate::diagnostics::clear_current_transaction_submit_timing();
            Self {
                started_at: Instant::now(),
                sample: crate::diagnostics::TransactionCommitTimingSample::default(),
            }
        })
    }

    fn phase_start(timing: Option<&Self>) -> Option<Instant> {
        timing.map(|_| Instant::now())
    }

    fn record_submit(timing: &mut Option<Self>, started_at: Option<Instant>) {
        if let (Some(timing), Some(started_at)) = (timing.as_mut(), started_at) {
            let submit_timing = crate::diagnostics::take_current_transaction_submit_timing();
            timing.sample.submit_apply_transaction_ns = duration_as_nanos(started_at.elapsed());
            timing.sample.runtime_apply_ns = submit_timing.runtime_apply;
        }
    }

    fn record_durability(timing: &mut Option<Self>, started_at: Option<Instant>) {
        if let (Some(timing), Some(started_at)) = (timing.as_mut(), started_at) {
            timing.sample.durability_finalize_ns = duration_as_nanos(started_at.elapsed());
        }
    }

    fn record_unregister(timing: &mut Option<Self>, started_at: Option<Instant>) {
        if let (Some(timing), Some(started_at)) = (timing.as_mut(), started_at) {
            timing.sample.unregister_snapshot_ns = duration_as_nanos(started_at.elapsed());
        }
    }

    fn finish(timing: Option<Self>, succeeded: bool) {
        if let Some(mut timing) = timing {
            timing.sample.commit_total_ns = duration_as_nanos(timing.started_at.elapsed());
            timing.sample.succeeded = succeeded;
            crate::diagnostics::record_transaction_commit_timing(timing.sample);
        }
    }
}

fn duration_as_nanos(duration: Duration) -> u64 {
    u64::try_from(duration.as_nanos()).unwrap_or(u64::MAX)
}

pub(crate) struct TransactionInit {
    pub(crate) runtime_handle: RuntimeHandle,
    pub(crate) coordinator: Arc<IngestCoordinator>,
    pub(crate) sequence_publisher: Arc<AtomicU64>,
    pub(crate) id: u64,
    pub(crate) cf_id: ColumnFamilyId,
    pub(crate) mode: TransactionMode,
    pub(crate) start_sequence: u64,
    pub(crate) read_snapshot: Option<Arc<crate::runtime::ReadSnapshot>>,
    pub(crate) cloud_mode: bool,
    pub(crate) db_path: PathBuf,
    pub(crate) memory_mode: bool,
    pub(crate) transaction_memory_pool: Arc<TransactionMemoryPool>,
    pub(crate) runtime_transaction_guard: crate::runtime::RuntimeTransactionGuard,
}

enum ScanLifecycle {
    Uninitialized,
    Active,
    Exhausted,
    Failed(MidgeError),
}

#[derive(Clone, Copy, PartialEq, Eq)]
enum SourceAdvance {
    None,
    Snapshot,
    Intent,
    Both,
}

struct TransactionScan<'a> {
    snapshot: SnapshotScan,
    intents: IntentKeyScan,
    write_set: &'a TransactionWriteSet,
    snapshot_head: Option<MidgeResult<(bytes::Bytes, bytes::Bytes)>>,
    intent_head: Option<MidgeResult<bytes::Bytes>>,
    direction: super::iterator::Direction,
    prefix: Option<bytes::Bytes>,
    remaining: Option<usize>,
    lifecycle: ScanLifecycle,
    pending_advance: SourceAdvance,
}

impl<'a> TransactionScan<'a> {
    fn new(
        snapshot: SnapshotScan,
        intents: IntentKeyScan,
        write_set: &'a TransactionWriteSet,
        query: &super::query::Query,
    ) -> Self {
        Self {
            snapshot,
            intents,
            write_set,
            snapshot_head: None,
            intent_head: None,
            direction: query.direction,
            prefix: query.prefix.clone(),
            remaining: query.limit,
            lifecycle: ScanLifecycle::Uninitialized,
            pending_advance: SourceAdvance::None,
        }
    }

    fn initialize(&mut self) {
        if matches!(self.lifecycle, ScanLifecycle::Uninitialized) {
            self.snapshot_head = self.snapshot.next();
            self.intent_head = self.intents.next();
            self.lifecycle = ScanLifecycle::Active;
        }
    }

    fn take_source_error(&mut self) -> Option<MidgeError> {
        if self.snapshot_head.as_ref().is_some_and(Result::is_err) {
            return self.snapshot_head.take().and_then(Result::err);
        }
        if self.intent_head.as_ref().is_some_and(Result::is_err) {
            return self.intent_head.take().and_then(Result::err);
        }
        None
    }

    fn advance_consumed_sources(&mut self) {
        match self.pending_advance {
            SourceAdvance::Snapshot => self.snapshot_head = self.snapshot.next(),
            SourceAdvance::Intent => self.intent_head = self.intents.next(),
            SourceAdvance::Both => {
                self.snapshot_head = self.snapshot.next();
                self.intent_head = self.intents.next();
            }
            SourceAdvance::None => {}
        }
        self.pending_advance = SourceAdvance::None;
    }

    fn selected_key(&self) -> Option<bytes::Bytes> {
        let snapshot_key = self
            .snapshot_head
            .as_ref()
            .and_then(|row| row.as_ref().ok())
            .map(|(key, _)| key);
        let intent_key = self.intent_head.as_ref().and_then(|key| key.as_ref().ok());
        match (snapshot_key, intent_key) {
            (Some(snapshot), Some(intent)) => {
                let intent_wins = if self.direction == super::iterator::Direction::Reverse {
                    intent > snapshot
                } else {
                    intent < snapshot
                };
                Some(if intent_wins {
                    intent.clone()
                } else {
                    snapshot.clone()
                })
            }
            (Some(snapshot), None) => Some(snapshot.clone()),
            (None, Some(intent)) => Some(intent.clone()),
            (None, None) => None,
        }
    }

    fn next_visible(&mut self) -> MidgeResult<Option<(bytes::Bytes, bytes::Bytes)>> {
        if self.remaining == Some(0) {
            return Ok(None);
        }
        self.initialize();

        loop {
            self.advance_consumed_sources();
            if let Some(error) = self.take_source_error() {
                return Err(error);
            }
            let Some(key) = self.selected_key() else {
                return Ok(None);
            };

            let mut base_value = None;
            if self
                .snapshot_head
                .as_ref()
                .and_then(|row| row.as_ref().ok())
                .is_some_and(|(candidate, _)| candidate == &key)
            {
                base_value = self
                    .snapshot_head
                    .take()
                    .and_then(Result::ok)
                    .map(|(_, value)| value);
                self.pending_advance = match self.pending_advance {
                    SourceAdvance::None | SourceAdvance::Intent => SourceAdvance::Snapshot,
                    SourceAdvance::Snapshot | SourceAdvance::Both => SourceAdvance::Both,
                };
            }
            if self
                .intent_head
                .as_ref()
                .and_then(|candidate| candidate.as_ref().ok())
                .is_some_and(|candidate| candidate == &key)
            {
                self.intent_head = None;
                self.pending_advance = match self.pending_advance {
                    SourceAdvance::None | SourceAdvance::Snapshot => SourceAdvance::Intent,
                    SourceAdvance::Intent | SourceAdvance::Both => SourceAdvance::Both,
                };
            }

            let value = match self.write_set.latest_for_key(&key)? {
                Some(IntentLookup::Present(value)) => Some(value),
                Some(IntentLookup::Deleted) => None,
                None => base_value,
            };
            let Some(value) = value else {
                continue;
            };
            if self
                .prefix
                .as_ref()
                .is_some_and(|prefix| !key.starts_with(prefix))
            {
                continue;
            }

            if let Some(remaining) = &mut self.remaining {
                *remaining = remaining.saturating_sub(1);
            }
            return Ok(Some((key, value)));
        }
    }
}

impl std::iter::Iterator for TransactionScan<'_> {
    type Item = MidgeResult<(bytes::Bytes, bytes::Bytes)>;

    fn next(&mut self) -> Option<Self::Item> {
        match &self.lifecycle {
            ScanLifecycle::Failed(error) => return Some(Err(error.replay())),
            ScanLifecycle::Exhausted => return None,
            ScanLifecycle::Uninitialized | ScanLifecycle::Active => {}
        }
        match self.next_visible() {
            Ok(Some(row)) => Some(Ok(row)),
            Ok(None) => {
                self.lifecycle = ScanLifecycle::Exhausted;
                None
            }
            Err(error) => {
                self.lifecycle = ScanLifecycle::Failed(error);
                match &self.lifecycle {
                    ScanLifecycle::Failed(error) => Some(Err(error.replay())),
                    ScanLifecycle::Uninitialized
                    | ScanLifecycle::Active
                    | ScanLifecycle::Exhausted => unreachable!(),
                }
            }
        }
    }
}

impl Transaction {
    /// Create a new transaction with the given ID and mode.
    pub(crate) fn new(init: TransactionInit) -> Self {
        let runtime_handle = init.runtime_handle;
        Self {
            runtime_handle: runtime_handle.clone(),
            coordinator: init.coordinator,
            sequence_publisher: init.sequence_publisher,
            cf_id: init.cf_id,
            mode: init.mode,
            conflict_policy: ConflictPolicy::LastWriteWins,
            write_set: TransactionWriteSet::new(
                Arc::clone(&init.transaction_memory_pool),
                &init.db_path,
                init.memory_mode,
                init.id,
            ),
            assertions: Vec::new(),
            assertion_bytes: 0,
            memory_pool: init.transaction_memory_pool,
            start_sequence: init.start_sequence,
            read_snapshot: init.read_snapshot,
            cloud_mode: init.cloud_mode,
            snapshot_pin: SnapshotPinGuard::new(runtime_handle, init.id),
            _runtime_transaction_guard: init.runtime_transaction_guard,
        }
    }

    /// Add a put (upsert) to the transaction's write set
    ///
    /// # Errors
    ///
    /// Returns `MidgeError::InvalidArgument` when called on a read-only transaction.
    pub fn put(
        &mut self,
        key: Vec<u8>,
        value: Vec<u8>,
        ttl_seconds: Option<u64>,
    ) -> MidgeResult<()> {
        if self.is_read_only() {
            return Err(MidgeError::InvalidArgument(
                "Cannot write in ReadOnly transaction".to_string(),
            ));
        }
        self.write_set.push(crate::runtime::TransactionOp::Put {
            cf_id: self.cf_id,
            key: bytes::Bytes::from(key),
            value: bytes::Bytes::from(value),
            ttl_seconds,
            insert_only: false,
        })
    }

    /// Add an insert (error if exists) to the transaction's write set
    ///
    /// # Errors
    ///
    /// Returns `MidgeError::InvalidArgument` when called on a read-only transaction.
    pub fn insert(
        &mut self,
        key: Vec<u8>,
        value: Vec<u8>,
        ttl_seconds: Option<u64>,
    ) -> MidgeResult<()> {
        if self.is_read_only() {
            return Err(MidgeError::InvalidArgument(
                "Cannot write in ReadOnly transaction".to_string(),
            ));
        }
        self.write_set.push(crate::runtime::TransactionOp::Put {
            cf_id: self.cf_id,
            key: bytes::Bytes::from(key),
            value: bytes::Bytes::from(value),
            ttl_seconds,
            insert_only: true,
        })
    }

    /// Add a delete to the transaction's write set
    ///
    /// # Errors
    ///
    /// Returns `MidgeError::InvalidArgument` when called on a read-only transaction.
    pub fn delete(&mut self, key: Vec<u8>) -> MidgeResult<()> {
        if self.is_read_only() {
            return Err(MidgeError::InvalidArgument(
                "Cannot write in ReadOnly transaction".to_string(),
            ));
        }
        self.write_set.push(crate::runtime::TransactionOp::Delete {
            cf_id: self.cf_id,
            key: bytes::Bytes::from(key),
        })
    }

    /// Add a delete range (atomic tombstone) to the transaction's write set
    ///
    /// Deletes all keys in the range [`start_key`, `end_key`) atomically as part of
    /// the transaction commit. This is atomic with any puts/deletes in the same transaction.
    ///
    /// # Errors
    ///
    /// Returns `MidgeError::InvalidArgument` when called on a read-only transaction
    /// or when `start_key > end_key`.
    pub fn delete_range(&mut self, start_key: Vec<u8>, end_key: Vec<u8>) -> MidgeResult<()> {
        if self.is_read_only() {
            return Err(MidgeError::InvalidArgument(
                "Cannot write in ReadOnly transaction".to_string(),
            ));
        }
        if start_key.as_slice() > end_key.as_slice() {
            return Err(MidgeError::InvalidArgument(
                "delete_range requires start_key <= end_key".to_string(),
            ));
        }
        self.write_set
            .push(crate::runtime::TransactionOp::DeleteRange {
                cf_id: self.cf_id,
                start_key: bytes::Bytes::from(start_key),
                end_key: bytes::Bytes::from(end_key),
            })
    }

    /// Assert that `key` has the expected value in this transaction's frozen
    /// start snapshot. Pass `None` to assert that the key is absent.
    ///
    /// Unlike [`Transaction::get`], this check deliberately does not observe
    /// pending writes from the same transaction. This permits the common
    /// compare-then-update pattern: assert the original value, then stage its
    /// replacement in the same transaction.
    ///
    /// The assertion is registered only when this method returns `Ok(())`.
    /// Ignoring an error leaves no guard for `commit` to enforce. Assertion
    /// reservations share the engine's transaction memory pool with write-set
    /// intents, so unrelated transactions can cause `ResourceLimit` under
    /// joint pressure. The assertion is not itself a write and is excluded
    /// from the transaction write set.
    ///
    /// # Errors
    ///
    /// Returns `MidgeError::InvalidArgument` when called on a read-only
    /// transaction or `MidgeError::ResourceLimit` when the transaction memory
    /// pool cannot admit the assertion.
    #[must_use = "the value assertion is registered only when this result is Ok"]
    pub fn assert_value(&mut self, key: Vec<u8>, expected: Option<Vec<u8>>) -> MidgeResult<()> {
        if self.is_read_only() {
            return Err(MidgeError::InvalidArgument(
                "Cannot assert in ReadOnly transaction".to_string(),
            ));
        }
        let expected_bytes = expected.as_ref().map_or(0, Vec::len);
        let bytes = key
            .len()
            .saturating_add(expected_bytes)
            .saturating_add(ASSERTION_ACCOUNTING_OVERHEAD);
        if !self.memory_pool.try_reserve(bytes) {
            return Err(MidgeError::ResourceLimit(format!(
                "transaction memory pool cannot admit {bytes} assertion bytes"
            )));
        }
        if let Err(error) = self.assertions.try_reserve(1) {
            self.memory_pool.release(bytes);
            return Err(MidgeError::ResourceLimit(format!(
                "cannot allocate transaction assertion: {error}"
            )));
        }
        self.assertions.push((key, expected));
        self.assertion_bytes = self.assertion_bytes.saturating_add(bytes);
        Ok(())
    }

    // === Internal helpers for engine/mod.rs commit logic ===

    pub(crate) fn is_read_only(&self) -> bool {
        matches!(self.mode, TransactionMode::ReadOnly)
    }

    pub(crate) fn has_writes(&self) -> bool {
        !self.write_set.is_empty()
    }

    #[must_use]
    pub fn conflict_policy(&self) -> ConflictPolicy {
        self.conflict_policy
    }

    pub fn set_conflict_policy(&mut self, conflict_policy: ConflictPolicy) {
        self.conflict_policy = conflict_policy;
    }

    /// Commit this transaction with the provided durability options.
    ///
    /// # Errors
    ///
    /// Returns an error when commit coordination, WAL durability, or cloud durability
    /// confirmation fails.
    pub fn commit(mut self, opts: crate::engine::api::WriteOptions) -> MidgeResult<()> {
        let mut timing = CommitTiming::maybe_start();

        self.validate_assertions()?;

        if self.is_read_only() {
            let unregister_started_at = CommitTiming::phase_start(timing.as_ref());
            self.unregister_snapshot();
            CommitTiming::record_unregister(&mut timing, unregister_started_at);
            CommitTiming::finish(timing, true);
            return Ok(());
        }

        let had_writes = self.has_writes();
        let key_assertions = self.deduplicated_key_assertions();

        if !had_writes && key_assertions.is_empty() {
            let durability_started_at = CommitTiming::phase_start(timing.as_ref());
            let sync_result = effective_wal_durability_policy(self.cloud_mode, opts)
                .and_then(|_| if opts.is_sync() { self.sync() } else { Ok(()) });
            CommitTiming::record_durability(&mut timing, durability_started_at);
            let unregister_started_at = CommitTiming::phase_start(timing.as_ref());
            self.unregister_snapshot();
            CommitTiming::record_unregister(&mut timing, unregister_started_at);
            CommitTiming::finish(timing, sync_result.is_ok());
            return sync_result;
        }

        let conflict_policy = match self.conflict_policy() {
            ConflictPolicy::LastWriteWins => crate::runtime::ConflictPolicy::LastWriteWins,
            ConflictPolicy::AbortOnWriteConflict => {
                crate::runtime::ConflictPolicy::AbortOnWriteConflict
            }
        };

        let durability_policy = Some(effective_wal_durability_policy(self.cloud_mode, opts)?);
        let submit_started_at = CommitTiming::phase_start(timing.as_ref());
        let collect_submit_timing = timing.is_some();
        // An assertion-only commit (no writes) still carries a non-empty
        // `key_assertions` here, and is validated server-side — at the same
        // pre-sequence-allocation serialization point as write-conflict
        // detection — without ever allocating a sequence, appending to the
        // WAL, or touching a memtable.
        let commit_result = if self.write_set.has_spills() {
            let source = self.write_set.take_source();
            self.coordinator.submit_spilled_ops(
                &self.runtime_handle,
                crate::runtime::SpilledTransactionSubmission {
                    source,
                    assertions: key_assertions,
                    durability_policy,
                    start_sequence: self.start_sequence(),
                    conflict_policy,
                },
                collect_submit_timing,
            )
        } else {
            let runtime_ops = self.write_set.take_in_memory_ops();
            self.coordinator.submit_ops(
                &self.runtime_handle,
                crate::runtime::TransactionSubmission {
                    ops: runtime_ops,
                    assertions: key_assertions,
                    durability_policy,
                    start_sequence: Some(self.start_sequence()),
                    conflict_policy,
                },
                collect_submit_timing,
            )
        };
        CommitTiming::record_submit(&mut timing, submit_started_at);

        let result = match commit_result {
            Ok(sequence) => {
                self.sequence_publisher.store(sequence, Ordering::SeqCst);
                let durability_started_at = CommitTiming::phase_start(timing.as_ref());
                let result = self.finalize_write_durability(sequence, opts, had_writes);
                CommitTiming::record_durability(&mut timing, durability_started_at);
                result
            }
            Err(error) => Err(error),
        };

        let unregister_started_at = CommitTiming::phase_start(timing.as_ref());
        self.unregister_snapshot();
        CommitTiming::record_unregister(&mut timing, unregister_started_at);
        CommitTiming::finish(timing, result.is_ok());
        result
    }

    fn validate_assertions(&self) -> MidgeResult<()> {
        let snapshot = self.read_snapshot.as_ref().ok_or_else(|| {
            MidgeError::Internal("read snapshot not available - this is a bug".to_string())
        })?;
        for (key, expected) in &self.assertions {
            let actual = snapshot.get_bytes(key, self.start_sequence)?;
            if actual.as_deref() != expected.as_deref() {
                return Err(MidgeError::WriteConflict(format!(
                    "value assertion failed for key {key:?}"
                )));
            }
        }
        Ok(())
    }

    /// Deduplicated keys for the commit-time precondition check.
    ///
    /// Only the key crosses into the runtime — expected values are already
    /// validated client-side, against the frozen start snapshot, in
    /// `validate_assertions`. What the runtime checks is narrower: has this
    /// key's sequence advanced past `start_sequence` since then. See
    /// `KeyAssertion` for why that's the right (ABA-safe) check.
    fn deduplicated_key_assertions(&self) -> Vec<KeyAssertion> {
        let mut seen = std::collections::HashSet::with_capacity(self.assertions.len());
        self.assertions
            .iter()
            .filter(|(key, _)| seen.insert(key.clone()))
            .map(|(key, _)| KeyAssertion {
                cf_id: self.cf_id,
                key: bytes::Bytes::from(key.clone()),
            })
            .collect()
    }

    /// Roll back this transaction and unregister its snapshot.
    ///
    /// # Errors
    ///
    /// Returns an error if snapshot cleanup fails.
    pub fn rollback(mut self) -> MidgeResult<()> {
        self.unregister_snapshot();
        Ok(())
    }

    pub(crate) fn unregister_snapshot(&mut self) {
        let _ = self.snapshot_pin.unregister();
    }

    #[must_use]
    pub(crate) fn start_sequence(&self) -> u64 {
        self.start_sequence
    }

    fn sync(&self) -> MidgeResult<()> {
        let response = self
            .runtime_handle
            .send_and_wait(crate::runtime::RuntimeMsg::WalSync {
                request_id: crate::runtime::next_request_id()?,
            })?;

        match response {
            crate::runtime::RuntimeResponse::Ok { .. } => Ok(()),
            crate::runtime::RuntimeResponse::Error { error, .. } => Err(error),
            _ => Err(MidgeError::Internal(
                "Unexpected response to sync".to_string(),
            )),
        }
    }

    fn finalize_write_durability(
        &self,
        sequence: u64,
        opts: crate::engine::api::WriteOptions,
        had_writes: bool,
    ) -> MidgeResult<()> {
        if self.cloud_mode {
            if opts.is_sync() || opts.is_cloud_strict() {
                let response = self.runtime_handle.send_and_wait(
                    crate::runtime::RuntimeMsg::SealWalForCloud {
                        request_id: crate::runtime::next_request_id()?,
                        sequence,
                        wait_for_ack: opts.is_cloud_strict(),
                    },
                )?;

                return match response {
                    crate::runtime::RuntimeResponse::Ok { .. } => Ok(()),
                    crate::runtime::RuntimeResponse::Error { error, .. } => Err(error),
                    _ => Err(MidgeError::Internal(
                        "Unexpected response to SealWalForCloud".to_string(),
                    )),
                };
            }

            return Ok(());
        }

        if !had_writes && opts.is_sync() {
            return self.sync();
        }

        // A local synchronous commit with writes is already fsynced by the WAL
        // actor before it acknowledges `submit_ops`. Issuing `WalSync` here
        // would add a second physical durability barrier. Assertion-only
        // commits have no WAL append, so they take the explicit barrier above.
        Ok(())
    }

    /// Get a value within this transaction (read-your-own-writes semantics)
    ///
    /// This is the primary way to read data. Checks the transaction's write set first,
    /// then falls back to the engine state at the transaction's snapshot sequence.
    ///
    /// Executes directly against the immutable snapshot (no message passing).
    ///
    /// # Errors
    ///
    /// Returns an error when the transaction snapshot is unavailable.
    pub fn get(&self, key: &[u8]) -> MidgeResult<Option<bytes::Bytes>> {
        // Check transaction's write set first (read-your-own-writes)
        if let Some(value) = self.write_set.latest_for_key(key)? {
            return Ok(match value {
                IntentLookup::Present(bytes) => Some(bytes),
                IntentLookup::Deleted => None,
            });
        }

        // Execute directly against snapshot (bypasses event loop)
        let snapshot = self.read_snapshot.as_ref().ok_or_else(|| {
            MidgeError::Internal("read snapshot not available - this is a bug".to_string())
        })?;
        snapshot.get_bytes(key, self.start_sequence)
    }

    /// Range scan within this transaction
    ///
    /// Returns an iterator over all key-value pairs matching the query,
    /// visible at this transaction's snapshot sequence.
    ///
    /// Query must explicitly specify all scan parameters (start, end, direction, limit).
    /// Executes directly against the immutable snapshot (no message passing).
    ///
    /// # Errors
    ///
    /// Returns [`MidgeError::InvalidArgument`] when both explicit bounds are
    /// present and `start > end`. Equal bounds and valid bounds whose prefix
    /// intersection is empty produce an empty iterator. Also returns an error
    /// when the transaction snapshot is unavailable.
    pub fn scan(&self, query: &super::query::Query) -> MidgeResult<super::iterator::Iterator<'_>> {
        if let (Some(start), Some(end)) = (query.start.as_deref(), query.end.as_deref()) {
            if start > end {
                return Err(MidgeError::InvalidArgument(
                    "scan requires start_key <= end_key".to_string(),
                ));
            }
        }
        let snapshot = self.read_snapshot.as_ref().ok_or_else(|| {
            MidgeError::Internal("read snapshot not available - this is a bug".to_string())
        })?;
        let start = query.effective_start().map(<[u8]>::to_vec);
        let end = query.effective_end();
        let reverse = query.direction == super::iterator::Direction::Reverse;
        let snapshot_scan =
            snapshot.state_scan(start.clone(), end.clone(), reverse, self.start_sequence);
        let intent_scan = self
            .write_set
            .key_scan(start.as_deref(), end.as_deref(), reverse)?;
        Ok(super::iterator::Iterator::from_iter(
            TransactionScan::new(snapshot_scan, intent_scan, &self.write_set, query),
            query.direction,
        ))
    }
}

impl Drop for Transaction {
    fn drop(&mut self) {
        self.unregister_snapshot();
        self.memory_pool.release(self.assertion_bytes);
    }
}
