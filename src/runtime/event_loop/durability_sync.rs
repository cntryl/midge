//! Durability synchronization — WAL sync, group commit, durability waiter completion
//!
//! Contains the ack policy logic, WAL sync triggers, and the shared
//! `complete_durability_waiters` helper that deduplicates the waiter-completion
//! pattern used by cloud ack, WAL sync, and forced sync code paths.

use super::EventLoop;
use super::super::durability::DurabilityWaiter;
use super::super::RuntimeResponse;
use crossbeam::channel::Receiver;
use super::super::RuntimeMsg;

/// Describes the source of a durability completion so the shared helper
/// can apply the correct side-effects (confirm variant, barrier clearing,
/// idempotency cleanup).
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(crate) enum CompletionSource {
    /// Cloud upload acknowledged — uses cloud frontier for confirm.
    CloudAck,
    /// Local WAL sync completed (group commit).
    WalSync,
    /// Sealed generation completed (forced sync / DDL barrier).
    SealedGeneration,
}

impl EventLoop {
    #[inline]
    pub(super) fn should_ack_immediately(&self, deferred: bool) -> bool {
        // Ack policy:
        // - CloudFirst (background mode): ack immediately after local WAL write.
        //   Cloud upload runs asynchronously; commits never block on upload.
        // - CloudStrict (explicit): never ack until cloud confirms (blocking durability).
        // - Batched/Strict: ack immediately; durability is enforced via explicit sync/flush barriers
        //   and via read-path durability frontiers when requested.
        //
        // In Batched mode, deferring the ack until fsync would serialize callers and
        // defeat group commit (and can make tests look hung).
        //
        // CRITICAL: Background CloudFirst must NOT wait for cloud confirmation.
        // Only CloudStrict policy (explicit opt-in) blocks on cloud upload.
        if self.wal_actor.is_cloud_first() {
            // TODO: Check if WriteOptions::cloud_strict() was used.
            // For now, always ack immediately (background mode).
            // CloudStrict implementation requires passing write options through to runtime.
            return true;
        }

        // Non-CloudFirst always acks immediately.
        // `deferred` still matters for whether we queue confirm-only waiters.
        let _ = deferred;
        true
    }

    #[inline]
    pub(super) fn maybe_queue_confirm_only_waiter(
        &self,
        deferred: bool,
        request_id: u64,
        is_transaction: bool,
    ) {
        // If we already waited for durability (deferred==false for local; or cloud-first with non-immediate ack)
        // then the request will be confirmed at response time.
        if !deferred {
            return;
        }

        // Only queue confirm-only waiters when we are acknowledging before durability.
        if !self.should_ack_immediately(deferred) {
            return;
        }

        if is_transaction {
            self.durability
                .queue_waiter(DurabilityWaiter::ConfirmTransactionApply { request_id });
        } else {
            self.durability
                .queue_waiter(DurabilityWaiter::ConfirmWalAppend { request_id });
        }
    }

    /// Shared helper: complete a batch of durability waiters.
    ///
    /// This replaces three near-identical match blocks that were duplicated across
    /// cloud ack handling, WAL sync completion, and forced sync paths. The
    /// `source` parameter controls which side-effects apply:
    ///
    /// - `CloudAck`: uses `confirm_sequences_at(cloud_durable_seq)`, no transaction barrier clear
    /// - `WalSync`: uses `confirm_sequences(request_id)`, clears transaction barrier
    /// - `SealedGeneration`: same as `WalSync` plus `cleanup_old_idempotency_entries()`
    pub(super) fn complete_durability_waiters(
        &mut self,
        waiters: Vec<DurabilityWaiter>,
        source: CompletionSource,
    ) {
        for w in waiters {
            match w {
                DurabilityWaiter::WalAppend {
                    request_id,
                    sequence,
                } => {
                    self.confirm_for_source(request_id, source);
                    self.respond(
                        request_id,
                        RuntimeResponse::WalAppended {
                            request_id,
                            sequence,
                        },
                    );
                }
                DurabilityWaiter::ConfirmWalAppend { request_id } => {
                    self.confirm_for_source(request_id, source);
                }
                DurabilityWaiter::TransactionApply {
                    request_id,
                    last_sequence,
                    op_count,
                } => {
                    if source != CompletionSource::CloudAck {
                        self.state.clear_pending_transaction_barrier();
                    }
                    self.confirm_for_source(request_id, source);
                    self.respond(
                        request_id,
                        RuntimeResponse::TransactionApplied {
                            request_id,
                            last_sequence,
                            op_count,
                            write_stall_hint: self.state.should_stall_writes(0),
                        },
                    );
                }
                DurabilityWaiter::ConfirmTransactionApply { request_id } => {
                    if source != CompletionSource::CloudAck {
                        self.state.clear_pending_transaction_barrier();
                    }
                    self.confirm_for_source(request_id, source);
                }
                DurabilityWaiter::Read {
                    request_id,
                    cf_id,
                    key,
                    sequence,
                    requested_durability: _,
                } => {
                    let value = self.handle_read(cf_id, &key, sequence);
                    self.respond(
                        request_id,
                        RuntimeResponse::ReadValue { request_id, value },
                    );
                }
                DurabilityWaiter::RangeScan {
                    request_id,
                    cf_id,
                    start,
                    end,
                    sequence,
                    requested_durability: _,
                } => {
                    let results = self.handle_range_scan(cf_id, &start, &end, sequence);
                    self.respond(
                        request_id,
                        RuntimeResponse::RangeScanResults {
                            request_id,
                            results,
                        },
                    );
                }
            }

            if source == CompletionSource::SealedGeneration {
                self.state.cleanup_old_idempotency_entries();
            }
        }
    }

    /// Apply the correct sequence-confirmation call for the given source.
    #[inline]
    fn confirm_for_source(&mut self, request_id: u64, source: CompletionSource) {
        match source {
            CompletionSource::CloudAck => {
                self.state
                    .confirm_sequences_at(request_id, self.state.wal.cloud_durable_seq);
            }
            CompletionSource::WalSync | CompletionSource::SealedGeneration => {
                self.state.confirm_sequences(request_id);
            }
        }
    }

    /// Sync batched WAL if threshold exceeded or if there are pending writes.
    /// In group commit mode, this completes all waiters for the sealed generation.
    pub(super) fn sync_batched_wal_if_needed(&mut self, msg_rx: &Receiver<RuntimeMsg>) {
        if self.wal_actor.is_cloud_first() {
            return; // CloudFirst has separate logic
        }

        // Sync if any of these conditions are true:
        // 1. Byte threshold exceeded
        // 2. Time threshold exceeded
        // NOTE: Do NOT unconditionally sync just because there are pending writes; that
        // defeats group commit—let the batch window (time/bytes) determine when to sync.
        // Durable waiters will be satisfied when the batch window elapses.
        let _has_pending_waiters = self.durability.has_pending_waiters();

        let should_sync = self.wal_actor.should_sync_batch();

        if !should_sync {
            return;
        }

        // 🔑 CRITICAL INVARIANT: If we have pending waiters, we MUST seal a generation.
        // Even with zero bytes, the durability guarantee requires advancing the generation.
        // Drain any available writes to maximize group commit.
        const MAX_DRAIN_WRITES_BEFORE_SYNC: usize = 4096;
        let _ = self.drain_pending_writes(msg_rx, MAX_DRAIN_WRITES_BEFORE_SYNC);

        // Always call sync - it advances the generation even with zero bytes
        match self.wal_actor.sync(&mut self.state) {
            Ok(sealed_gen) => {
                // Rotate group commit to new generation and complete old one
                self.durability.rotate_to(sealed_gen + 1);
                let completed = self.durability.complete_waiters_at(sealed_gen);
                self.complete_durability_waiters(completed, CompletionSource::WalSync);
            }
            Err(e) => {
                tracing::warn!(error = %e, "failed to sync batched WAL");
            }
        }
    }

    /// Sync WAL if any durable waiters exist (safety valve for forward-progress).
    /// This ensures that operations observing durability (reads, ranges, deletes)
    /// always see a stable state with durability guarantees.
    /// Without this, tests with patterns like "write → read/range/delete" hang forever.
    #[allow(dead_code)]
    pub(super) fn sync_if_waiters_exist(&mut self, msg_rx: &Receiver<RuntimeMsg>) {
        if self.wal_actor.is_cloud_first() {
            return; // CloudFirst has separate logic
        }

        let has_waiters = self.durability.has_pending_waiters();

        if has_waiters {
            self.force_wal_sync(msg_rx);
        }
    }

    /// Force WAL sync even if no pending writes (for DDL durability barriers).
    /// Required before CF metadata mutations to guarantee durability fences.
    /// CRITICAL: Must drain pending writes first so they are included in the sync.
    pub(super) fn force_wal_sync(&mut self, msg_rx: &Receiver<RuntimeMsg>) {
        if self.wal_actor.is_cloud_first() {
            return; // CloudFirst has separate logic
        }

        // 🔑 Drain any pending writes so they are included in this sync
        const MAX_DRAIN: usize = 4096;
        let _ = self.drain_pending_writes(msg_rx, MAX_DRAIN);

        // Always sync to establish durability barrier
        // (even if no pending writes or waiters - we're being asked to guarantee durability)
        match self.wal_actor.sync(&mut self.state) {
            Ok(sealed_gen) => {
                // Rotate and complete any pending waiters
                self.durability.rotate_to(sealed_gen + 1);
                let completed = self.durability.complete_waiters_at(sealed_gen);
                self.complete_durability_waiters(completed, CompletionSource::SealedGeneration);
            }
            Err(e) => {
                tracing::warn!(error = %e, "failed to force WAL sync for durability barrier");
            }
        }
    }
}
