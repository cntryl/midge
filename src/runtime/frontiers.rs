//! WAL durability frontiers that only move forward.

use crate::types::DurabilityFrontiersSnapshot;
use std::hint::spin_loop;
use std::sync::atomic::{AtomicU64, AtomicUsize, Ordering};

/// Event-loop-published coherent frontier values for nonblocking readers.
///
/// The event loop is the sole writer. The revision counter makes the three
/// atomic values a coherent snapshot for concurrent readers without a lock.
#[derive(Debug)]
pub(crate) struct ObservedDurabilityFrontiers {
    revision: AtomicU64,
    current_sequence: AtomicU64,
    wal_local_durable_seq: AtomicU64,
    wal_cloud_durable_seq: AtomicU64,
}

impl ObservedDurabilityFrontiers {
    pub(crate) fn new(snapshot: DurabilityFrontiersSnapshot) -> Self {
        Self {
            revision: AtomicU64::new(0),
            current_sequence: AtomicU64::new(snapshot.current_sequence),
            wal_local_durable_seq: AtomicU64::new(snapshot.wal_local_durable_seq),
            wal_cloud_durable_seq: AtomicU64::new(snapshot.wal_cloud_durable_seq),
        }
    }

    /// Publish a coherent value. Callers must serialize writers on the runtime
    /// event loop.
    pub(crate) fn publish(&self, snapshot: DurabilityFrontiersSnapshot) {
        self.revision.fetch_add(1, Ordering::SeqCst);
        self.current_sequence
            .store(snapshot.current_sequence, Ordering::SeqCst);
        self.wal_local_durable_seq
            .store(snapshot.wal_local_durable_seq, Ordering::SeqCst);
        self.wal_cloud_durable_seq
            .store(snapshot.wal_cloud_durable_seq, Ordering::SeqCst);
        self.revision.fetch_add(1, Ordering::SeqCst);
    }

    /// Read a coherent value without waiting for a runtime request.
    #[must_use]
    pub(crate) fn load(&self) -> DurabilityFrontiersSnapshot {
        loop {
            let revision_before = self.revision.load(Ordering::SeqCst);
            if revision_before % 2 == 1 {
                spin_loop();
                continue;
            }

            let snapshot = DurabilityFrontiersSnapshot {
                current_sequence: self.current_sequence.load(Ordering::SeqCst),
                wal_local_durable_seq: self.wal_local_durable_seq.load(Ordering::SeqCst),
                wal_cloud_durable_seq: self.wal_cloud_durable_seq.load(Ordering::SeqCst),
            };
            if self.revision.load(Ordering::SeqCst) == revision_before {
                return snapshot;
            }
        }
    }
}

/// Event-loop-published queue gauge sampled alongside durability lag.
#[derive(Debug)]
pub(crate) struct ObservedPendingCloudUploads(AtomicUsize);

impl ObservedPendingCloudUploads {
    pub(crate) fn new(value: usize) -> Self {
        Self(AtomicUsize::new(value))
    }

    pub(crate) fn publish(&self, value: usize) {
        self.0.store(value, Ordering::Release);
    }

    #[must_use]
    pub(crate) fn load(&self) -> usize {
        self.0.load(Ordering::Acquire)
    }
}

/// The highest sequences known synced, durable locally, and durable in cloud
/// storage.
///
/// Every advance is monotonic, so a receipt for older data cannot lower a
/// frontier. The only way down is the named recovery reset.
#[derive(Debug, Default, Clone, Copy, PartialEq, Eq)]
pub struct WalFrontiers {
    last_synced: u64,
    local_durable: u64,
    cloud_durable: u64,
}

impl WalFrontiers {
    /// Frontiers after recovery replayed the WAL through `sequence`: that
    /// much is durable locally and, until startup re-proves it, in the cloud.
    #[must_use]
    pub(crate) fn recovered_at(sequence: u64) -> Self {
        Self {
            last_synced: 0,
            local_durable: sequence,
            cloud_durable: sequence,
        }
    }

    /// Last sequence the WAL writer synced.
    #[must_use]
    pub fn last_synced(&self) -> u64 {
        self.last_synced
    }

    /// Highest sequence fsynced locally.
    #[must_use]
    pub fn local_durable(&self) -> u64 {
        self.local_durable
    }

    /// Highest sequence confirmed durable in cloud storage.
    #[must_use]
    pub fn cloud_durable(&self) -> u64 {
        self.cloud_durable
    }

    /// Record a local sync through `sequence`. An older sync leaves both
    /// frontiers where they are.
    pub(crate) fn advance_synced_to(&mut self, sequence: u64) {
        self.last_synced = self.last_synced.max(sequence);
        self.local_durable = self.local_durable.max(sequence);
    }

    /// Record local durability through `sequence` without a writer sync.
    pub(crate) fn advance_local_to(&mut self, sequence: u64) {
        self.local_durable = self.local_durable.max(sequence);
    }

    /// Record cloud durability through `sequence`.
    #[cfg(test)]
    pub(crate) fn advance_cloud_to(&mut self, sequence: u64) {
        self.cloud_durable = self.cloud_durable.max(sequence);
    }

    /// Record cloud durability through `sequence`, then over the sequences
    /// no WAL record holds (#843): those after `open_prev_last`, the last
    /// record before the open segment, and below `open_first_seq`, which an
    /// earlier process reserved or no record took before a rotation. Once
    /// the cloud holds every record through the segment before the open one,
    /// it holds every record below the open one.
    pub(crate) fn advance_cloud_over(
        &mut self,
        sequence: u64,
        open_prev_last: u64,
        open_first_seq: u64,
    ) {
        self.cloud_durable = self.cloud_durable.max(sequence);
        if self.cloud_durable >= open_prev_last {
            self.cloud_durable = self.cloud_durable.max(open_first_seq.saturating_sub(1));
        }
    }

    /// Recovery only: restart the cloud frontier at what the manifest proves,
    /// before startup re-advances it through validated remote segments.
    pub(crate) fn reset_cloud_for_recovery(&mut self, sequence: u64) {
        self.cloud_durable = sequence;
    }

    #[cfg(test)]
    pub(crate) fn set_last_synced_for_test(&mut self, sequence: u64) {
        self.last_synced = sequence;
    }

    #[cfg(test)]
    pub(crate) fn set_local_durable_for_test(&mut self, sequence: u64) {
        self.local_durable = sequence;
    }

    #[cfg(test)]
    pub(crate) fn set_cloud_durable_for_test(&mut self, sequence: u64) {
        self.cloud_durable = sequence;
    }
}

#[cfg(test)]
mod tests {
    use super::{ObservedDurabilityFrontiers, ObservedPendingCloudUploads, WalFrontiers};
    use crate::types::DurabilityFrontiersSnapshot;
    use std::sync::atomic::{AtomicBool, Ordering};
    use std::sync::Arc;

    #[test]
    fn should_not_lower_frontiers_when_an_older_sequence_arrives() {
        // Arrange
        let mut frontiers = WalFrontiers::default();
        frontiers.advance_synced_to(20);
        frontiers.advance_cloud_to(15);

        // Act
        frontiers.advance_synced_to(10);
        frontiers.advance_local_to(5);
        frontiers.advance_cloud_to(3);

        // Assert
        assert_eq!(frontiers.last_synced(), 20);
        assert_eq!(frontiers.local_durable(), 20);
        assert_eq!(frontiers.cloud_durable(), 15);
    }

    #[test]
    fn should_extend_cloud_frontier_over_unwritten_sequences_once_their_prefix_is_durable() {
        // Arrange: the open segment starts at 1001, after a record at 3.
        let mut frontiers = WalFrontiers::default();

        // Act
        frontiers.advance_cloud_over(2, 3, 1001);
        let before = frontiers.cloud_durable();
        frontiers.advance_cloud_over(3, 3, 1001);

        // Assert
        assert_eq!(before, 2);
        assert_eq!(frontiers.cloud_durable(), 1000);
    }

    #[test]
    fn should_lower_cloud_frontier_only_through_recovery_reset() {
        // Arrange
        let mut frontiers = WalFrontiers::default();
        frontiers.advance_cloud_to(15);

        // Act
        frontiers.reset_cloud_for_recovery(7);

        // Assert
        assert_eq!(frontiers.cloud_durable(), 7);
    }

    #[test]
    fn should_read_coherent_frontier_values_during_publication() {
        // Arrange
        let published = Arc::new(ObservedDurabilityFrontiers::new(
            DurabilityFrontiersSnapshot::default(),
        ));
        let writer_snapshot = Arc::clone(&published);
        let writer_finished = Arc::new(AtomicBool::new(false));
        let writer_finished_thread = Arc::clone(&writer_finished);

        // Act
        let writer = std::thread::spawn(move || {
            for sequence in 1..=10_000 {
                writer_snapshot.publish(DurabilityFrontiersSnapshot {
                    current_sequence: sequence,
                    wal_local_durable_seq: sequence,
                    wal_cloud_durable_seq: sequence,
                });
            }
            writer_finished_thread.store(true, Ordering::Release);
        });
        let mut previous_sequence = 0;
        while !writer_finished.load(Ordering::Acquire) {
            let snapshot = published.load();
            assert_eq!(snapshot.current_sequence, snapshot.wal_local_durable_seq);
            assert_eq!(
                snapshot.wal_local_durable_seq,
                snapshot.wal_cloud_durable_seq
            );
            assert!(snapshot.current_sequence >= previous_sequence);
            previous_sequence = snapshot.current_sequence;
        }
        writer.join().expect("frontier publisher thread");

        // Assert
        assert_eq!(published.load().current_sequence, 10_000);
    }

    #[test]
    fn should_publish_pending_upload_counts_for_nonblocking_readers() {
        // Arrange
        let pending_uploads = ObservedPendingCloudUploads::new(2);

        // Act
        pending_uploads.publish(5);

        // Assert
        assert_eq!(pending_uploads.load(), 5);
    }
}
