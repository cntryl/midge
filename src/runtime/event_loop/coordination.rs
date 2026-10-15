use super::RuntimeMsg;
use crate::wal::cloud_catalog::CatalogEntry;
use crate::wal::publish::{AckSet, OwedSegment};
use std::collections::{BTreeMap, HashMap, HashSet, VecDeque};
use std::time::Duration;

/// Owns cloud WAL admission, acknowledgement, retirement, and local cleanup.
pub(crate) struct CloudWalUploadTracker {
    /// Acknowledged segments: verified uploads, and segments the catalog
    /// publishes. An upload waits there until every earlier segment is
    /// acknowledged too, so they publish as one contiguous run (#841).
    acks: AckSet,
    /// Sealed segments the runtime still has to hand to the upload queue.
    backlog: UploadBacklog,
    /// Catalog prunes in flight.
    prunes: PruneTracker,
    /// Sealed segments whose local copies may go (#842).
    local_retirement: LocalRetirementQueue,
}

/// Sealed segments the runtime owes the upload queue, each with the last
/// sequence it holds, oldest first, and when the next attempt may run.
///
/// A segment arrives sealed, or comes back for a fresh upload after its
/// readback or publication failed or shared memory ran short. A failed
/// upload attempt never comes back here; `HybridStorage` retries it itself.
pub(crate) struct UploadBacklog {
    queued: BTreeMap<u64, u64>,
    /// Earliest time the backlog may be resubmitted.
    retry: crate::runtime::retry_schedule::RetrySchedule,
}

/// A segment the WAL ledger does not hold sealed, so it has no history to
/// upload again.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub(crate) struct NotMember {
    /// The refused segment.
    pub(crate) segment_id: u64,
}

impl std::fmt::Display for NotMember {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(
            f,
            "cloud WAL segment {} is not a sealed member of the WAL ledger",
            self.segment_id
        )
    }
}

impl std::error::Error for NotMember {}

impl UploadBacklog {
    #[cfg(test)]
    fn new() -> Self {
        Self::with_retry_delay(crate::runtime::RuntimeRetryPolicy::default().cloud_wal)
    }

    fn with_retry_delay(delay: Duration) -> Self {
        Self {
            queued: BTreeMap::new(),
            retry: crate::runtime::retry_schedule::RetrySchedule::new(delay),
        }
    }

    /// Queue a freshly sealed segment for its first upload.
    pub(crate) fn queue_sealed(&mut self, segment_id: u64, max_sequence: u64) {
        self.queued.insert(segment_id, max_sequence);
    }

    /// Take back `segment_id` for a fresh upload after the retry delay.
    ///
    /// # Errors
    ///
    /// Refuses a segment `ledger` does not hold sealed: it has no history
    /// to upload again.
    pub(crate) fn requeue(
        &mut self,
        segment_id: u64,
        max_sequence: u64,
        ledger: &crate::wal::ledger::WalLedger,
    ) -> Result<(), NotMember> {
        if !ledger.is_member(segment_id) || segment_id == ledger.open_segment() {
            return Err(NotMember { segment_id });
        }
        self.queued.insert(segment_id, max_sequence);
        self.retry.defer();
        Ok(())
    }

    /// Hold the next attempt for the retry delay: the upload queue has no
    /// room for the backlog's next segment.
    pub(crate) fn defer_for_memory(&mut self) {
        self.retry.defer();
    }

    /// Hold the next attempt for the retry delay after an attempt failed.
    pub(crate) fn defer_after_failure(&mut self) {
        self.retry.defer();
    }

    /// The oldest queued segment, with the last sequence it holds.
    pub(crate) fn first(&self) -> Option<(u64, u64)> {
        self.queued
            .first_key_value()
            .map(|(segment_id, max_sequence)| (*segment_id, *max_sequence))
    }

    /// Stop owing `segment_id` to the upload queue, returning the last
    /// sequence it holds.
    pub(crate) fn remove(&mut self, segment_id: u64) -> Option<u64> {
        self.queued.remove(&segment_id)
    }

    /// The last sequence queued segment `segment_id` holds.
    #[cfg(test)]
    pub(crate) fn get(&self, segment_id: u64) -> Option<u64> {
        self.queued.get(&segment_id).copied()
    }

    /// How many segments are queued.
    pub(crate) fn len(&self) -> usize {
        self.queued.len()
    }

    /// Whether no segment is queued.
    pub(crate) fn is_empty(&self) -> bool {
        self.queued.is_empty()
    }

    /// Whether a queued segment may be submitted now.
    pub(crate) fn ready(&self) -> bool {
        !self.queued.is_empty() && self.retry.is_ready()
    }

    /// Start an attempt to submit the backlog.
    pub(crate) fn begin_attempt(&mut self) {
        self.retry.clear();
    }

    /// How long until a queued segment may be submitted, if one is waiting
    /// for its retry delay.
    pub(crate) fn retry_timeout(&self) -> Option<Duration> {
        (!self.queued.is_empty())
            .then(|| self.retry.remaining())
            .flatten()
    }

    #[cfg(test)]
    pub(crate) fn retry_delay(&self) -> Duration {
        self.retry.configured_delay()
    }
}

/// Catalog prunes in flight. A completion counts only for a prune that was
/// admitted.
#[derive(Debug, Default)]
pub(crate) struct PruneTracker {
    inflight: HashSet<u64>,
}

impl PruneTracker {
    /// Mark the prunes of `segment_ids` in flight.
    pub(crate) fn admit(&mut self, segment_ids: impl IntoIterator<Item = u64>) {
        self.inflight.extend(segment_ids);
    }

    /// End `segment_id`'s prune. Returns false when it was never admitted,
    /// so its completion is ignored.
    pub(crate) fn release(&mut self, segment_id: u64) -> bool {
        self.inflight.remove(&segment_id)
    }

    /// Whether `segment_id`'s prune is in flight.
    pub(crate) fn contains(&self, segment_id: u64) -> bool {
        self.inflight.contains(&segment_id)
    }

    /// How many prunes are in flight.
    pub(crate) fn len(&self) -> usize {
        self.inflight.len()
    }
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
}

impl LocalRetirement {
    pub(crate) const CLOUD_DURABLE: Self = Self {
        cloud_durable: true,
    };

    fn merge(self, other: Self) -> Self {
        Self {
            cloud_durable: self.cloud_durable || other.cloud_durable,
        }
    }
}

impl CloudWalUploadTracker {
    #[cfg(test)]
    /// A tracker acknowledging the segments recovery found published, each
    /// with the last sequence it holds.
    pub(super) fn new(recovered: BTreeMap<u64, CatalogEntry>) -> Self {
        Self::with_retry_policy(recovered, crate::runtime::RuntimeRetryPolicy::default())
    }

    pub(super) fn with_retry_policy(
        recovered: BTreeMap<u64, CatalogEntry>,
        retry_policy: crate::runtime::RuntimeRetryPolicy,
    ) -> Self {
        Self {
            acks: AckSet::recovered(recovered),
            backlog: UploadBacklog::with_retry_delay(retry_policy.cloud_wal),
            prunes: PruneTracker::default(),
            local_retirement: LocalRetirementQueue::default(),
        }
    }

    /// The acknowledged segments.
    pub(super) fn acks(&self) -> &AckSet {
        &self.acks
    }

    pub(super) fn oldest_published_facts(&self) -> Option<&crate::wal::format::SealedFacts> {
        self.acks.oldest_published_facts()
    }

    /// The sealed segments the runtime owes the upload queue.
    pub(super) fn backlog(&self) -> &UploadBacklog {
        &self.backlog
    }

    /// The catalog prunes in flight.
    pub(super) fn prunes(&self) -> &PruneTracker {
        &self.prunes
    }

    /// Acknowledge `segment_id` as published, as recovery would.
    #[cfg(test)]
    pub(super) fn acknowledge_for_test(&mut self, segment_id: u64, max_sequence: u64) {
        self.acks.acknowledge(segment_id, max_sequence);
    }

    /// Acknowledge a verified upload. It is not published yet.
    pub(super) fn record_upload(&mut self, upload: CatalogEntry) {
        self.acks.record_upload(upload);
    }

    /// Withdraw `segment_id`'s verified upload, so no run publishes past it
    /// until it is verified again. A published segment stays published.
    pub(super) fn withdraw_upload(&mut self, segment_id: u64) {
        self.acks.withdraw(segment_id);
    }

    /// The catalog no longer publishes `segment_id`: pruning retired it.
    pub(super) fn retire_pruned(&mut self, segment_id: u64) {
        self.acks.retire_pruned(segment_id);
    }

    /// The catalog now publishes `run`.
    pub(super) fn mark_published(&mut self, run: &[OwedSegment]) {
        self.acks.mark_published(run);
    }

    /// The verified uploads still waiting to be published: they hold local
    /// WAL the writer owes the cloud, and count against the upload queue's
    /// limits.
    pub(super) fn held_uploads(&self) -> crate::storage::hybrid::backend::HeldUploads {
        let (entries, bytes) = self.acks.held();
        crate::storage::hybrid::backend::HeldUploads { entries, bytes }
    }

    /// Queue a freshly sealed segment for its first upload.
    pub(super) fn queue_sealed(&mut self, segment_id: u64, max_sequence: u64) {
        self.backlog.queue_sealed(segment_id, max_sequence);
    }

    /// Take back `segment_id` for a fresh upload after the retry delay.
    ///
    /// # Errors
    ///
    /// Refuses a segment `ledger` does not hold sealed.
    pub(super) fn requeue(
        &mut self,
        segment_id: u64,
        max_sequence: u64,
        ledger: &crate::wal::ledger::WalLedger,
    ) -> Result<(), NotMember> {
        self.backlog.requeue(segment_id, max_sequence, ledger)
    }

    /// Hold the next backlog attempt: the upload queue has no room.
    pub(super) fn defer_uploads_for_memory(&mut self) {
        self.backlog.defer_for_memory();
    }

    /// Hold the next backlog attempt after an attempt failed.
    pub(super) fn defer_uploads_after_failure(&mut self) {
        self.backlog.defer_after_failure();
    }

    /// Start an attempt to submit the backlog.
    pub(super) fn begin_upload_attempt(&mut self) {
        self.backlog.begin_attempt();
    }

    /// The upload queue admitted `segment_id`: the backlog no longer owes it.
    pub(super) fn admitted_upload(&mut self, segment_id: u64) {
        self.backlog.remove(segment_id);
    }

    /// Mark the prunes of `segment_ids` in flight.
    pub(super) fn admit_prunes(&mut self, segment_ids: impl IntoIterator<Item = u64>) {
        self.prunes.admit(segment_ids);
    }

    /// End `segment_id`'s prune. Returns false when it was never admitted.
    pub(super) fn release_prune(&mut self, segment_id: u64) -> bool {
        self.prunes.release(segment_id)
    }

    /// Queue `segment_id`'s local copy for retirement, adding `release` to
    /// what it already releases.
    pub(super) fn queue_local_retirement(&mut self, segment_id: u64, release: LocalRetirement) {
        self.local_retirement.queue(segment_id, release);
    }

    /// The log start the leading run of queued local retirements retires
    /// `ledger` to.
    pub(super) fn local_retire_point(&self, ledger: &crate::wal::ledger::WalLedger) -> u64 {
        self.local_retirement.retire_point(ledger)
    }

    /// Remove and return every queued local retirement below `log_start`.
    pub(super) fn drain_local_retirements_below(
        &mut self,
        log_start: u64,
    ) -> Vec<(u64, LocalRetirement)> {
        self.local_retirement.drain_below(log_start)
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
        CloudWalUploadTracker, ManifestPublicationGate, NotMember, PruneTracker, UploadBacklog,
        VerificationBarrier, WriteStallWaiters,
    };
    use crate::runtime::wal_history::WalHistory;
    use crate::runtime::RuntimeMsg;
    use std::collections::BTreeMap;

    fn shutdown_message(request_id: u64) -> RuntimeMsg {
        RuntimeMsg::ShutdownWithResponse { request_id }
    }

    #[test]
    fn should_retain_cloud_wal_state_when_constructed() {
        // Arrange
        let record = crate::wal::WalRecord::new(
            crate::wal::WalOpKind::Put,
            bytes::Bytes::from_static(b"key"),
            Some(bytes::Bytes::from_static(b"value")),
            41,
        );
        let (bytes, _) = crate::wal::format::segment::SegmentBuilder::new(
            3,
            1,
            crate::wal::format::ChainLink::GENESIS,
        )
        .record(&record)
        .seal();
        let entry = crate::wal::cloud_catalog::CatalogEntry::from_sealed_bytes(&bytes).unwrap();
        let recovered = BTreeMap::from([(3, entry)]);

        // Act
        let mut tracker = CloudWalUploadTracker::new(recovered);
        tracker.queue_sealed(4, 52);

        // Assert
        assert_eq!(tracker.acks().published().collect::<Vec<_>>(), [(3, 41)]);
        assert_eq!(tracker.backlog().first(), Some((4, 52)));
        assert!(tracker.backlog().ready());
    }

    #[test]
    fn should_refuse_requeue_of_a_segment_outside_the_ledger() {
        // Arrange: segment 1 is retired, 2 sealed and 3 open.
        let mut history = WalHistory::in_memory(1);
        crate::runtime::wal_history::fixture::seal_through(&mut history, 2)
            .expect("seal the ledger through segment 2");
        let mut backlog = UploadBacklog::new();

        // Act
        let retired = backlog.requeue(1, 10, history.ledger());
        let open = backlog.requeue(3, 30, history.ledger());
        let sealed = backlog.requeue(2, 20, history.ledger());

        // Assert
        assert_eq!(retired, Err(NotMember { segment_id: 1 }));
        assert_eq!(open, Err(NotMember { segment_id: 3 }));
        assert_eq!(sealed, Ok(()));
        assert_eq!(backlog.len(), 1);
        assert_eq!(backlog.get(2), Some(20));
        assert!(
            backlog.retry_timeout().is_some(),
            "a requeued segment waits for its retry"
        );
    }

    #[test]
    fn should_ignore_prune_completion_without_admission() {
        // Arrange
        let mut prunes = PruneTracker::default();
        prunes.admit([4]);

        // Act
        let unadmitted = prunes.release(5);
        let admitted = prunes.release(4);
        let repeated = prunes.release(4);

        // Assert
        assert!(!unadmitted);
        assert!(admitted);
        assert!(!repeated);
        assert_eq!(prunes.len(), 0);
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
