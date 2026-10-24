//! Runtime ownership of cloud metadata-publication serialization.
//!
//! Local capture turns and reclamation share one lock. Immutable publications
//! may perform provider I/O concurrently; exact epoch-owned pointer CAS and
//! typed capture lineage prevent an older task from rolling authority back.

use parking_lot::{Mutex, MutexGuard};
use std::sync::{Arc, OnceLock};

mod cut;
pub(crate) use cut::{CapturedMetadataCut, MetadataCutProof};

#[derive(Clone, Default)]
pub(crate) struct MetadataPublicationLock {
    state: Arc<Mutex<MetadataPublicationState>>,
    cuts: Arc<Mutex<cut::CutRegistry>>,
    capture_memory: Arc<OnceLock<crate::common::resource_budget::ResourceBudget>>,
    capture_slots: Arc<OnceLock<crate::common::resource_budget::ResourceBudget>>,
}

#[derive(Default)]
pub(crate) struct MetadataPublicationState {
    pub(crate) cached: Option<crate::runtime::hybrid_persistence::MetadataMirrorCache>,
    pub(crate) retention: crate::runtime::hybrid_persistence::MetadataRetentionState,
}

impl MetadataPublicationLock {
    /// Bind the runtime's existing shared maintenance pool before admission.
    /// Existing ownership must never move to a different accounting ledger.
    pub(crate) fn configure_capture_memory(
        &self,
        memory: crate::common::resource_budget::ResourceBudget,
    ) {
        let _ = self.capture_memory.set(memory.with_contention_errors());
    }
    #[cfg(test)]
    pub(crate) fn capture_contention(&self, error: &crate::common::MidgeError) -> bool {
        crate::common::resource_budget::ResourceContention::from_error(error).is_some_and(
            |contention| {
                self.capture_memory
                    .get()
                    .is_some_and(|memory| contention.is_blocked_by(memory))
            },
        )
    }
    pub(crate) fn capture_admission_retryable(&self, error: &crate::common::MidgeError) -> bool {
        crate::common::resource_budget::ResourceContention::from_error(error).is_some_and(
            |contention| {
                self.capture_memory
                    .get()
                    .is_some_and(|memory| contention.is_for_pool(memory))
                    || self
                        .capture_slots
                        .get()
                        .is_some_and(|slots| contention.is_for_pool(slots))
            },
        )
    }
    pub(crate) fn lock_for(
        &self,
        timeout: std::time::Duration,
    ) -> Option<MutexGuard<'_, MetadataPublicationState>> {
        self.state.try_lock_for(timeout)
    }

    #[cfg(test)]
    pub(crate) fn try_lock(&self) -> Option<MutexGuard<'_, MetadataPublicationState>> {
        self.state.try_lock()
    }

    #[cfg(test)]
    pub(crate) fn lock(&self) -> MutexGuard<'_, MetadataPublicationState> {
        self.state.lock()
    }
}
