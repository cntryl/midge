//! Runtime ownership of cloud metadata-publication serialization.
//!
//! Local capture turns and reclamation share one lock. Immutable publications
//! may perform provider I/O concurrently; exact epoch-owned pointer CAS and
//! typed capture lineage prevent an older task from rolling authority back.

use parking_lot::{Mutex, MutexGuard};
use std::sync::{Arc, OnceLock};

mod cut;
pub(crate) use cut::{CapturedMetadataCut, MetadataCutProof, SelectedTerminalWitnesses};

#[derive(Clone, Default)]
pub(crate) struct MetadataPublicationLock {
    state: Arc<Mutex<MetadataPublicationState>>,
    format_10_writer: Arc<Mutex<()>>,
    cuts: Arc<Mutex<cut::CutRegistry>>,
    capture_memory: Arc<OnceLock<cntryl_midge_core::common::resource_budget::ResourceBudget>>,
    capture_slots: Arc<OnceLock<cntryl_midge_core::common::resource_budget::ResourceBudget>>,
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
        memory: cntryl_midge_core::common::resource_budget::ResourceBudget,
    ) {
        let _ = self.capture_memory.set(memory.with_contention_errors());
    }
    #[cfg(test)]
    pub(crate) fn capture_contention(&self, error: &cntryl_midge_core::common::MidgeError) -> bool {
        cntryl_midge_core::common::resource_budget::ResourceContention::from_error(error)
            .is_some_and(|contention| {
                self.capture_memory
                    .get()
                    .is_some_and(|memory| contention.is_blocked_by(memory))
            })
    }
    pub(crate) fn capture_admission_retryable(
        &self,
        error: &cntryl_midge_core::common::MidgeError,
    ) -> bool {
        cntryl_midge_core::common::resource_budget::ResourceContention::from_error(error)
            .is_some_and(|contention| {
                self.capture_memory
                    .get()
                    .is_some_and(|memory| contention.is_for_pool(memory))
                    || self
                        .capture_slots
                        .get()
                        .is_some_and(|slots| contention.is_for_pool(slots))
            })
    }
    pub(crate) fn lock_for(
        &self,
        timeout: std::time::Duration,
    ) -> Option<MutexGuard<'_, MetadataPublicationState>> {
        self.state.try_lock_for(timeout)
    }

    /// FORMAT 10 staging can recycle a terminal predecessor while this turn
    /// owns the read-to-CAS interval. FORMAT 9 keeps its optimistic pipeline.
    pub(crate) fn lock_format_10_writer(
        &self,
        timeout: std::time::Duration,
    ) -> cntryl_midge_core::common::MidgeResult<MutexGuard<'_, ()>> {
        self.format_10_writer.try_lock_for(timeout).ok_or_else(|| {
            cntryl_midge_core::common::MidgeError::Timeout(
                "FORMAT 10 publication waits for staging authority".into(),
            )
        })
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
