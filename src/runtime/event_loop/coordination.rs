use super::RuntimeMsg;
use std::collections::{BTreeMap, HashMap, HashSet, VecDeque};
use std::time::Duration;

#[cfg(not(test))]
const CLOUD_WAL_RUNTIME_RETRY_DELAY: Duration = Duration::from_secs(1);
#[cfg(test)]
const CLOUD_WAL_RUNTIME_RETRY_DELAY: Duration = Duration::from_millis(10);

/// Owns cloud WAL admission, acknowledgement, pruning, and cleanup proof state.
pub(crate) struct CloudWalUploadTracker {
    /// Acknowledged segments, each with the last sequence it holds: verified
    /// uploads, and segments recovery found published.
    acked_segments: BTreeMap<u64, u64>,
    /// The verified uploads among `acked_segments` the catalog does not
    /// publish yet, each as publication adds it. An upload waits here until
    /// every earlier segment is uploaded too, so they publish as one
    /// contiguous run (#841).
    uploaded: BTreeMap<u64, crate::wal::cloud_catalog::PublishedWalSegment>,
    pub(super) upload_backlog: BTreeMap<u64, u64>,
    /// Earliest time at which runtime-owned WAL upload obligations may be
    /// resubmitted: a completed upload whose readback or publication failed,
    /// or one deferred for memory. A failed upload attempt never comes back
    /// here; `HybridStorage` retries it itself.
    upload_retry: crate::runtime::retry_schedule::RetrySchedule,
    pub(super) prune_inflight: HashSet<u64>,
    /// Sealed segments whose local copies may go (#842).
    pub(super) local_retirement: LocalRetirementQueue,
}

/// Sealed segments whose local copies may go, with what each releases once
/// it falls below the ledger's log start. Only a prefix of the ledger
/// retires, so a queued segment can wait for an earlier one.
#[derive(Debug, Default)]
pub(crate) struct LocalRetirementQueue {
    queued: BTreeMap<u64, LocalRetirement>,
}

impl LocalRetirementQueue {
    /// Queue `segment_id`, adding `release` to what it already releases.
    pub(crate) fn queue(&mut self, segment_id: u64, release: LocalRetirement) {
        let entry = self.queued.entry(segment_id).or_default();
        *entry = entry.merge(release);
    }

    /// One past the leading run of `ledger`'s sealed members that are
    /// queued: the log start that run retires to.
    pub(crate) fn retire_point(&self, ledger: &crate::wal::ledger::WalLedger) -> u64 {
        ledger
            .sealed()
            .find(|segment_id| !self.queued.contains_key(segment_id))
            .unwrap_or_else(|| ledger.open_segment())
    }

    /// Remove and return every queued segment below `log_start`.
    pub(crate) fn drain_below(&mut self, log_start: u64) -> Vec<(u64, LocalRetirement)> {
        let kept = self.queued.split_off(&log_start);
        std::mem::replace(&mut self.queued, kept)
            .into_iter()
            .collect()
    }
}

/// What a segment's local retirement releases.
#[derive(Debug, Default, Clone, Copy, PartialEq, Eq)]
pub(crate) struct LocalRetirement {
    /// Its cloud-durable obligation in the transition protocol.
    pub(crate) cloud_durable: bool,
    /// Its tombstone GC pin, once the catalog pruned it.
    pub(crate) gc_pin: bool,
}

impl LocalRetirement {
    pub(crate) const CLOUD_DURABLE: Self = Self {
        cloud_durable: true,
        gc_pin: false,
    };
    pub(crate) const GC_PIN: Self = Self {
        cloud_durable: false,
        gc_pin: true,
    };

    fn merge(self, other: Self) -> Self {
        Self {
            cloud_durable: self.cloud_durable || other.cloud_durable,
            gc_pin: self.gc_pin || other.gc_pin,
        }
    }
}

impl CloudWalUploadTracker {
    pub(super) fn new(acked_segments: BTreeMap<u64, u64>) -> Self {
        Self {
            acked_segments,
            uploaded: BTreeMap::new(),
            upload_backlog: BTreeMap::new(),
            upload_retry: crate::runtime::retry_schedule::RetrySchedule::new(
                CLOUD_WAL_RUNTIME_RETRY_DELAY,
            ),
            prune_inflight: HashSet::new(),
            local_retirement: LocalRetirementQueue::default(),
        }
    }

    /// Acknowledged segments, each with the last sequence it holds.
    pub(super) fn acked(&self) -> &BTreeMap<u64, u64> {
        &self.acked_segments
    }

    /// Acknowledge `segment_id` as published, as recovery would.
    #[cfg(test)]
    pub(super) fn acknowledge_for_test(&mut self, segment_id: u64, max_sequence: u64) {
        self.acked_segments.insert(segment_id, max_sequence);
    }

    /// Acknowledge a verified upload. It is not published yet.
    pub(super) fn record_upload(&mut self, upload: crate::wal::cloud_catalog::PublishedWalSegment) {
        self.acked_segments
            .insert(upload.segment_id, upload.max_sequence);
        self.uploaded.insert(upload.segment_id, upload);
    }

    /// Withdraw a segment's acknowledgement, so no run publishes past it
    /// until it is verified again.
    pub(super) fn forget_upload(&mut self, segment_id: u64) {
        self.acked_segments.remove(&segment_id);
        self.uploaded.remove(&segment_id);
    }

    /// The catalog no longer publishes `segment_id`: pruning retired it.
    pub(super) fn retire_pruned(&mut self, segment_id: u64) {
        self.acked_segments.remove(&segment_id);
    }

    /// What publishing `run`, a contiguous run of acknowledged segments,
    /// adds: its verified uploads, oldest first. Segments recovery found
    /// published add nothing.
    pub(super) fn unpublished_uploads(
        &self,
        run: &[crate::wal::publish::OwedSegment],
    ) -> Vec<crate::wal::cloud_catalog::PublishedWalSegment> {
        run.iter()
            .filter_map(|segment| self.uploaded.get(&segment.segment_id).cloned())
            .collect()
    }

    /// The catalog now publishes `run`.
    pub(super) fn mark_published(&mut self, run: &[crate::wal::publish::OwedSegment]) {
        for segment in run {
            self.uploaded.remove(&segment.segment_id);
        }
    }

    /// The verified uploads still waiting to be published: they hold local
    /// WAL the writer owes the cloud, and count against the upload queue's
    /// limits.
    pub(super) fn held_uploads(&self) -> crate::storage::hybrid::backend::HeldUploads {
        crate::storage::hybrid::backend::HeldUploads {
            entries: self.uploaded.len(),
            bytes: self
                .uploaded
                .values()
                .map(|upload| upload.size_bytes)
                .fold(0, u64::saturating_add),
        }
    }

    pub(super) fn has_pending_uploads(&self) -> bool {
        !self.upload_backlog.is_empty()
    }

    pub(super) fn uploads_ready(&self) -> bool {
        !self.upload_backlog.is_empty() && self.upload_retry.is_ready()
    }

    pub(super) fn defer_upload_retry(&mut self) {
        self.upload_retry.defer();
    }

    pub(super) fn begin_upload_attempt(&mut self) {
        self.upload_retry.clear();
    }

    pub(super) fn upload_retry_deadline_timeout(&self) -> Option<Duration> {
        (!self.upload_backlog.is_empty())
            .then(|| self.upload_retry.remaining())
            .flatten()
    }
}

/// Identity of the runtime operation that currently owns manifest publication.
#[derive(Clone, Debug, PartialEq, Eq)]
pub(crate) enum ManifestPublicationOwner {
    Flush {
        flush_id: u64,
    },
    WalPrune,
    Compaction {
        request_id: u64,
        output_generation: u64,
    },
}

/// Serializes manifest-authority changes and retains deferred messages in FIFO order.
#[derive(Default)]
pub(crate) struct ManifestPublicationGate {
    deferred_messages: VecDeque<RuntimeMsg>,
    owner: Option<ManifestPublicationOwner>,
}

impl ManifestPublicationGate {
    pub(super) fn is_active(&self) -> bool {
        self.owner.is_some()
    }

    pub(super) fn is_owned_by(&self, owner: &ManifestPublicationOwner) -> bool {
        self.owner.as_ref() == Some(owner)
    }

    pub(super) fn try_acquire(&mut self, owner: ManifestPublicationOwner) -> bool {
        if self.owner.is_some() {
            return false;
        }
        self.owner = Some(owner);
        true
    }

    /// Release only the current owner's token. A stale completion cannot
    /// reopen the gate while another publication is in flight.
    pub(super) fn release(&mut self, owner: &ManifestPublicationOwner) -> bool {
        if self.owner.as_ref() != Some(owner) {
            return false;
        }
        self.owner = None;
        true
    }

    pub(super) fn defer(&mut self, message: RuntimeMsg) {
        self.deferred_messages.push_back(message);
    }

    pub(super) fn deferred_messages_is_empty(&self) -> bool {
        self.deferred_messages.is_empty()
    }

    #[cfg(test)]
    pub(super) fn deferred_messages_len(&self) -> usize {
        self.deferred_messages.len()
    }

    pub(super) fn take_deferred(&mut self) -> Vec<RuntimeMsg> {
        self.deferred_messages.drain(..).collect()
    }

    #[cfg(test)]
    pub(super) fn deferred_request_ids(&self) -> Vec<Option<u64>> {
        self.deferred_messages
            .iter()
            .map(RuntimeMsg::request_id)
            .collect()
    }

    /// Index of the next deferred message that may run. A drop waits until
    /// its column family's flush and compaction pipeline drains, but the
    /// messages that drain it (`CompactionComplete` in particular) must not
    /// queue behind it, or neither ever runs. Column-family DDL still keeps
    /// FIFO order so a create never overtakes a pending drop.
    pub(super) fn next_restorable_index(
        &self,
        pipeline_active: impl Fn(crate::types::ColumnFamilyId) -> bool,
    ) -> Option<usize> {
        let mut ddl_blocked = false;
        for (index, message) in self.deferred_messages.iter().enumerate() {
            match message {
                RuntimeMsg::ManifestDropColumnFamily { cf_id, .. } => {
                    if !ddl_blocked && !pipeline_active(*cf_id) {
                        return Some(index);
                    }
                    ddl_blocked = true;
                }
                RuntimeMsg::ManifestCreateColumnFamily { .. } => {
                    if !ddl_blocked {
                        return Some(index);
                    }
                }
                _ => return Some(index),
            }
        }
        None
    }

    /// End the gate and take the deferred message at `index` to run next.
    pub(super) fn finish_at(&mut self, index: usize) -> Option<RuntimeMsg> {
        debug_assert!(
            self.owner.is_none(),
            "publication owner must release the gate"
        );
        self.deferred_messages.remove(index)
    }
}

/// Owns the runtime-wide online-verification barrier and its deferred messages.
#[derive(Default)]
pub(crate) struct VerificationBarrier {
    pub(super) token: Option<u64>,
    pub(super) deferred_messages: VecDeque<RuntimeMsg>,
    /// Cloud-acknowledged segments whose local WAL retirement waits for the
    /// barrier, because verification is replaying those files.
    deferred_wal_retirements: Vec<u64>,
    /// Storage events that would change the layout being verified.
    deferred_storage_events: VecDeque<crate::storage::StorageEvent>,
}

impl VerificationBarrier {
    pub(super) fn is_active(&self) -> bool {
        self.token.is_some()
    }

    pub(super) fn activate(&mut self, token: u64) -> bool {
        if self.token.is_some() {
            return false;
        }
        self.token = Some(token);
        true
    }

    pub(super) fn defer_wal_retirements(&mut self, segment_ids: &[u64]) {
        for segment_id in segment_ids {
            if !self.deferred_wal_retirements.contains(segment_id) {
                self.deferred_wal_retirements.push(*segment_id);
            }
        }
    }

    pub(super) fn defer_storage_event(&mut self, event: crate::storage::StorageEvent) {
        self.deferred_storage_events.push_back(event);
    }

    /// Take the storage events deferred while the barrier was held.
    pub(super) fn take_deferred_storage_events(
        &mut self,
    ) -> VecDeque<crate::storage::StorageEvent> {
        std::mem::take(&mut self.deferred_storage_events)
    }

    /// Take the retirements deferred while the barrier was held.
    pub(super) fn take_deferred_wal_retirements(&mut self) -> Vec<u64> {
        std::mem::take(&mut self.deferred_wal_retirements)
    }

    pub(super) fn release(&mut self, token: u64) -> Option<RuntimeMsg> {
        if self.token != Some(token) {
            return None;
        }
        self.token = None;
        self.deferred_messages.pop_front()
    }
}

/// Owns both indexes for requests waiting on per-column-family write pressure.
#[derive(Default)]
pub(crate) struct WriteStallWaiters {
    by_request: HashMap<u64, crate::types::ColumnFamilyId>,
    by_column_family: HashMap<crate::types::ColumnFamilyId, VecDeque<u64>>,
}

impl WriteStallWaiters {
    pub(super) fn register(&mut self, request_id: u64, cf_id: crate::types::ColumnFamilyId) {
        if let Some(previous_cf) = self.by_request.insert(request_id, cf_id) {
            self.remove_from_queue(previous_cf, request_id);
        }
        self.by_column_family
            .entry(cf_id)
            .or_default()
            .push_back(request_id);
    }

    pub(super) fn cancel(&mut self, request_id: u64) -> bool {
        let Some(cf_id) = self.by_request.remove(&request_id) else {
            return false;
        };
        self.remove_from_queue(cf_id, request_id);
        true
    }

    pub(super) fn column_families(&self) -> Vec<crate::types::ColumnFamilyId> {
        self.by_column_family.keys().copied().collect()
    }

    pub(super) fn take_column_family(&mut self, cf_id: crate::types::ColumnFamilyId) -> Vec<u64> {
        self.by_column_family
            .remove(&cf_id)
            .into_iter()
            .flatten()
            .filter(|request_id| self.by_request.remove(request_id).is_some())
            .collect()
    }

    pub(super) fn drain(&mut self) -> Vec<u64> {
        self.by_column_family.clear();
        self.by_request
            .drain()
            .map(|(request_id, _)| request_id)
            .collect()
    }

    fn remove_from_queue(&mut self, cf_id: crate::types::ColumnFamilyId, request_id: u64) {
        let remove_queue = if let Some(queue) = self.by_column_family.get_mut(&cf_id) {
            queue.retain(|queued| *queued != request_id);
            queue.is_empty()
        } else {
            false
        };
        if remove_queue {
            self.by_column_family.remove(&cf_id);
        }
    }

    #[cfg(test)]
    pub(super) fn contains(&self, request_id: u64) -> bool {
        self.by_request.contains_key(&request_id)
    }

    #[cfg(test)]
    pub(super) fn column_family_for(
        &self,
        request_id: u64,
    ) -> Option<crate::types::ColumnFamilyId> {
        self.by_request.get(&request_id).copied()
    }

    #[cfg(test)]
    pub(super) fn column_family_queue(
        &self,
        cf_id: crate::types::ColumnFamilyId,
    ) -> Option<&VecDeque<u64>> {
        self.by_column_family.get(&cf_id)
    }

    pub(super) fn is_empty(&self) -> bool {
        self.by_request.is_empty() && self.by_column_family.is_empty()
    }
}

#[cfg(test)]
mod tests {
    use super::{
        CloudWalUploadTracker, ManifestPublicationGate, VerificationBarrier, WriteStallWaiters,
    };
    use crate::runtime::RuntimeMsg;
    use std::collections::BTreeMap;

    fn shutdown_message(request_id: u64) -> RuntimeMsg {
        RuntimeMsg::ShutdownWithResponse { request_id }
    }

    #[test]
    fn should_retain_cloud_wal_state_when_constructed() {
        // Arrange
        let recovered = BTreeMap::from([(3, 41)]);

        // Act
        let mut tracker = CloudWalUploadTracker::new(recovered);
        tracker.upload_backlog.insert(4, 52);

        // Assert
        assert_eq!(tracker.acked().get(&3), Some(&41));
        assert!(tracker.has_pending_uploads());
    }

    #[test]
    fn should_release_manifest_publication_messages_in_fifo_order_when_finished() {
        // Arrange
        let mut gate = ManifestPublicationGate::default();
        let owner = super::ManifestPublicationOwner::WalPrune;
        assert!(gate.try_acquire(owner.clone()));
        gate.defer(shutdown_message(7));
        gate.defer(shutdown_message(8));

        // Act
        assert!(gate.release(&owner));
        let released = gate.finish_at(0);

        // Assert
        assert!(!gate.is_active());
        assert!(matches!(
            released,
            Some(RuntimeMsg::ShutdownWithResponse { request_id: 7 })
        ));
        assert_eq!(gate.deferred_messages_len(), 1);
    }

    #[test]
    fn should_keep_publication_gate_closed_when_non_owner_releases() {
        // Arrange
        let mut gate = ManifestPublicationGate::default();
        let owner = super::ManifestPublicationOwner::WalPrune;
        let other_owner = super::ManifestPublicationOwner::Flush { flush_id: 7 };
        assert!(gate.try_acquire(owner));

        // Act
        let released = gate.release(&other_owner);

        // Assert
        assert!(!released);
        assert!(gate.is_active());
    }

    #[test]
    fn should_release_verification_messages_only_for_matching_token() {
        // Arrange
        let mut barrier = VerificationBarrier::default();
        assert!(barrier.activate(11));
        barrier.deferred_messages.push_back(shutdown_message(9));

        // Act
        let wrong = barrier.release(12);
        let released = barrier.release(11);

        // Assert
        assert!(wrong.is_none());
        assert!(matches!(
            released,
            Some(RuntimeMsg::ShutdownWithResponse { request_id: 9 })
        ));
        assert!(barrier.token.is_none());
    }

    #[test]
    fn should_remove_write_stall_request_from_both_indexes_when_cancelled() {
        // Arrange
        let mut waiters = WriteStallWaiters::default();
        waiters.register(7, 3);

        // Act
        let removed = waiters.cancel(7);

        // Assert
        assert!(removed);
        assert!(waiters.is_empty());
    }
}
