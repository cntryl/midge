use super::RuntimeMsg;
use std::collections::{BTreeMap, HashMap, HashSet, VecDeque};
use std::time::Instant;

#[derive(Debug, Clone, PartialEq, Eq)]
pub(crate) struct MetadataCleanupProof {
    pub(super) len: u64,
    pub(super) crc32c: u32,
    pub(super) remote: crate::storage::StorageObjectMetadata,
}

/// Owns cloud WAL admission, acknowledgement, pruning, and cleanup proof state.
pub(crate) struct CloudWalUploadTracker {
    pub(super) acked_segments: BTreeMap<u64, u64>,
    pub(super) upload_backlog: BTreeMap<u64, u64>,
    pub(super) prune_inflight: HashSet<u64>,
    pub(super) prune_retries: HashMap<u64, (u32, Instant)>,
    pub(super) metadata_cleanup_proofs: HashMap<String, MetadataCleanupProof>,
}

impl CloudWalUploadTracker {
    pub(super) fn new(acked_segments: BTreeMap<u64, u64>) -> Self {
        Self {
            acked_segments,
            upload_backlog: BTreeMap::new(),
            prune_inflight: HashSet::new(),
            prune_retries: HashMap::new(),
            metadata_cleanup_proofs: HashMap::new(),
        }
    }

    pub(super) fn has_pending_uploads(&self) -> bool {
        !self.upload_backlog.is_empty()
    }
}

/// Serializes manifest-authority changes and retains deferred messages in FIFO order.
#[derive(Default)]
pub(crate) struct ManifestPublicationGate {
    pub(super) deferred_messages: VecDeque<RuntimeMsg>,
    pub(super) active: bool,
}

impl ManifestPublicationGate {
    pub(super) fn defer(&mut self, message: RuntimeMsg) {
        self.deferred_messages.push_back(message);
    }

    pub(super) fn finish(&mut self) -> Option<RuntimeMsg> {
        self.active = false;
        self.deferred_messages.pop_front()
    }
}

/// Owns the runtime-wide online-verification barrier and its deferred messages.
#[derive(Default)]
pub(crate) struct VerificationBarrier {
    pub(super) token: Option<u64>,
    pub(super) deferred_messages: VecDeque<RuntimeMsg>,
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

    pub(super) fn release(&mut self, token: u64) -> Option<RuntimeMsg> {
        if self.token != Some(token) {
            return None;
        }
        self.token = None;
        self.deferred_messages.pop_front()
    }
}

#[cfg(test)]
mod tests {
    use super::{CloudWalUploadTracker, ManifestPublicationGate, VerificationBarrier};
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
        assert_eq!(tracker.acked_segments.get(&3), Some(&41));
        assert!(tracker.has_pending_uploads());
    }

    #[test]
    fn should_release_manifest_publication_messages_in_fifo_order_when_finished() {
        // Arrange
        let mut gate = ManifestPublicationGate {
            active: true,
            ..ManifestPublicationGate::default()
        };
        gate.defer(shutdown_message(7));
        gate.defer(shutdown_message(8));

        // Act
        let released = gate.finish();

        // Assert
        assert!(!gate.active);
        assert!(matches!(
            released,
            Some(RuntimeMsg::ShutdownWithResponse { request_id: 7 })
        ));
        assert_eq!(gate.deferred_messages.len(), 1);
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
}
