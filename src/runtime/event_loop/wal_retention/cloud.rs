//! Retire cloud WAL authority using committed shared family frontiers.

use super::super::EventLoop;
use crate::runtime::hybrid_persistence::{CloudPersistence, SharedMetadataSnapshot};

// Bound each catalog retirement turn so maintenance yields to foreground work.
const CLOUD_WAL_PRUNE_BATCH_SIZE: usize = 32;

#[cfg(test)]
std::thread_local! {
    static PRUNE_JOIN_HOOK: std::cell::RefCell<Option<Box<dyn FnOnce()>>> =
        std::cell::RefCell::new(None);
}

fn run_cloud_wal_retirement(
    storage: &CloudPersistence,
    candidates: &[(u64, u64)],
    candidate_ids: Vec<u64>,
    metadata_snapshot: &SharedMetadataSnapshot,
    writer_epoch: u64,
    attempt_budget: std::time::Duration,
) {
    let attempt = std::panic::catch_unwind(std::panic::AssertUnwindSafe(|| {
        let deadline = crate::common::OperationDeadline::from_budget(attempt_budget);
        let authority = metadata_snapshot.committed_retirement_authority_within(&deadline)?;
        storage.prune_cloud_wal_segments_within(candidates, &authority, writer_epoch, &deadline)
    }))
    .unwrap_or_else(|_| {
        Err(crate::common::MidgeError::Internal(
            "cloud WAL retirement panicked".into(),
        ))
    });
    match attempt {
        Ok(results) => {
            for (id, result) in results {
                match result {
                    Ok(()) => storage
                        .queue_cloud_wal_prune_complete(id, crate::storage::StorageOutcome::Ok(())),
                    Err(error) => {
                        storage.queue_cloud_wal_prune_attempt_failed(id, error.to_string());
                    }
                }
            }
        }
        Err(error) => {
            for id in candidate_ids {
                storage.queue_cloud_wal_prune_attempt_failed(id, error.to_string());
            }
        }
    }
}

impl EventLoop {
    fn cloud_wal_prune_layout_publication_active(&self) -> bool {
        self.state
            .active_compactions
            .load(std::sync::atomic::Ordering::Acquire)
            > 0
            || !self.state.compaction.compacting_ssts.is_empty()
            || self.flush_actor.is_inflight()
    }

    pub(crate) fn prune_cloud_wal_segments_covered_by_manifest(&mut self) {
        // A cleanup omission retains native WAL authority for recovery fixtures.
        crate::failpoints::fail_point!("midge::cloud::defer_wal_prune_admission", |_| ());
        // Shutdown must settle admitted durability work without admitting a
        // fresh optional retirement from flush/upload completion or maintenance.
        // Retain recovery authority for a later engine lifetime instead.
        if self.shutting_down {
            return;
        }
        self.reap_cloud_wal_prune_worker();
        if self.cloud_maintenance_enabled() && !self.cloud_coordinator.cloud_maintenance.dispatching
        {
            self.schedule_cloud_maintenance();
            return;
        }
        // Reaping may restore a control request that was deferred behind the
        // publication gate. Give the run loop a chance to dispatch it before
        // another maintenance prune reacquires the gate; otherwise a steady
        // stream of eligible WAL segments can defer the same request forever.
        if self.pending_msg.is_some() || !self.publication_gate.deferred_messages_is_empty() {
            return;
        }
        if !self.wal_actor.is_cloud_async() || self.state.is_memory_mode() {
            return;
        }

        // A flush publication mutates the local control files on its worker.
        // Start only between publication phases, then own the gate until the
        // worker has verified metadata and retired catalog authority.
        if self.cloud_coordinator.cloud_wal_prune_worker.is_some()
            || self.publication_gate.is_active()
            || self.cloud_wal_prune_layout_publication_active()
        {
            return;
        }

        let Some(storage) = self.cloud_coordinator.hybrid_storage.clone() else {
            return;
        };
        storage.configure_maintenance_memory(self.compaction_actor.compaction_memory_limit());
        let candidates = self.next_cloud_wal_prune_candidates();
        if candidates.is_empty() {
            return;
        }
        let metadata_snapshot = match self.cloud_metadata_snapshot_for_wal_cleanup() {
            Ok(snapshot) => snapshot,
            Err(error) => {
                tracing::debug!(%error, "deferring WAL cleanup admission");
                return;
            }
        };
        let writer_epoch = self.state.writer_epoch;
        // This callerless attempt has retry ownership, but shutdown must still
        // be able to join it within the cloud drain window. Starting the budget
        // here also bounds the whole retirement sequence rather than granting
        // each provider callback a fresh timeout.
        let attempt_budget = self
            .runtime_response_timeout
            .min(self.shutdown_cloud_drain_timeout);
        let publication_owner =
            crate::runtime::event_loop::coordination::ManifestPublicationOwner::WalPrune;
        if !self.publication_gate.try_acquire(publication_owner.clone()) {
            return;
        }
        let candidate_ids = candidates
            .iter()
            .map(|(segment_id, _)| *segment_id)
            .collect::<Vec<_>>();
        self.cloud_coordinator
            .cloud_wal
            .admit_prunes(candidate_ids.iter().copied());
        let worker_name = format!(
            "midge-wal-retirement-{}-{}",
            candidate_ids.first().copied().unwrap_or_default(),
            candidate_ids.last().copied().unwrap_or_default()
        );
        let worker_candidate_ids = candidate_ids.clone();
        // `EventLoop::drop` joins this worker, possibly inside a failpoint test.
        let failpoint_gate = crate::failpoints::worker_gate();
        let worker = std::thread::Builder::new()
            .name(worker_name)
            .spawn(move || {
                failpoint_gate.run(|| {
                    run_cloud_wal_retirement(
                        &CloudPersistence::new(storage),
                        &candidates,
                        worker_candidate_ids,
                        &metadata_snapshot,
                        writer_epoch,
                        attempt_budget,
                    );
                });
            });

        match worker {
            Ok(worker) => self.cloud_coordinator.set_prune_worker(worker),
            Err(error) => {
                for segment_id in candidate_ids {
                    self.cloud_coordinator.cloud_wal.release_prune(segment_id);
                }
                self.publication_gate.release(&publication_owner);
                self.state.mark_persistence_anomaly();
                tracing::warn!(
                    %error,
                    "Failed to start cloud WAL retirement worker"
                );
            }
        }
    }

    fn next_cloud_wal_prune_candidates(&self) -> Vec<(u64, u64)> {
        let tracker = &self.cloud_coordinator.cloud_wal;
        // Retirement is a prefix the committed shared generation covers. That
        // generation mirrors an earlier local manifest, and family frontiers
        // only advance, so a segment the local frontiers leave unflushed is
        // unflushed there too. Checking it here never grants retirement; it
        // only spares a worker that would hold the gate to answer Busy.
        let Some(oldest) = tracker.oldest_published_facts() else {
            return Vec::new();
        };
        if self
            .state
            .oldest_wal_blocking_family(Some(oldest))
            .is_some()
        {
            return Vec::new();
        }
        tracker
            .acks()
            .published()
            .take_while(|&(segment_id, max_sequence)| {
                max_sequence <= self.state.wal.frontiers.cloud_durable()
                    && !tracker.prunes().contains(segment_id)
            })
            .take(CLOUD_WAL_PRUNE_BATCH_SIZE)
            .collect()
    }

    pub(in crate::runtime::event_loop) fn reap_cloud_wal_prune_worker(&mut self) {
        if self
            .cloud_coordinator
            .cloud_wal_prune_worker
            .as_ref()
            .is_none_or(std::thread::JoinHandle::is_finished)
        {
            self.join_cloud_wal_prune_worker();
            self.restore_publication_deferred_message();
            if !self.cloud_maintenance_enabled() {
                self.schedule_next_flush_worker();
            }
        }
    }

    pub(in crate::runtime::event_loop) fn join_cloud_wal_prune_worker(&mut self) {
        if let Some(worker) = self.cloud_coordinator.take_prune_worker() {
            #[cfg(test)]
            PRUNE_JOIN_HOOK.with(|hook| {
                let hook = hook.borrow_mut().take();
                if let Some(hook) = hook {
                    hook();
                }
            });
            if worker.join().is_err() {
                self.state.mark_persistence_anomaly();
                tracing::warn!("cloud WAL retirement worker panicked during join");
            }
            self.publication_gate.release(
                &crate::runtime::event_loop::coordination::ManifestPublicationOwner::WalPrune,
            );
        }
    }

    #[cfg(test)]
    pub(in crate::runtime::event_loop) fn set_cloud_wal_prune_join_hook_for_test(
        hook: impl FnOnce() + 'static,
    ) {
        PRUNE_JOIN_HOOK.with(|pending| *pending.borrow_mut() = Some(Box::new(hook)));
    }
}

impl Drop for EventLoop {
    fn drop(&mut self) {
        self.join_cloud_wal_prune_worker();
    }
}

impl EventLoop {
    /// Queue `segment_id`'s local copy for retirement once cloud durability
    /// or catalog pruning makes it unneeded locally, then retire the leading
    /// run of queued segments (#842). A segment still needed locally holds
    /// every later one: only a prefix of the ledger retires. Each retired
    /// segment then releases its runtime obligations.
    ///
    /// # Errors
    ///
    /// Returns the transition protocol's refusal to release an obligation;
    /// the caller fences. A ledger that cannot retire only retains WAL.
    pub(in crate::runtime::event_loop) fn retire_cloud_local_wal(
        &mut self,
        segment_id: u64,
        release: crate::runtime::event_loop::coordination::LocalRetirement,
    ) -> crate::common::MidgeResult<()> {
        self.cloud_coordinator
            .cloud_wal
            .queue_local_retirement(segment_id, release);
        let ledger = self.state.wal.history.ledger();
        let to = self.cloud_coordinator.cloud_wal.local_retire_point(ledger);
        if to > ledger.log_start() {
            self.retire_wal_below_or_retain(to);
        }
        // Memory mode keeps no local copies, so everything queued is retired.
        let log_start = if self.state.wal.history.segment_files().is_some() {
            self.state.wal.history.ledger().log_start()
        } else {
            u64::MAX
        };
        let released = self
            .cloud_coordinator
            .cloud_wal
            .drain_local_retirements_below(log_start);
        for (segment_id, release) in released {
            if release.cloud_durable {
                self.wal_transition.retire_cloud_durable(segment_id)?;
            }
        }
        Ok(())
    }
}
