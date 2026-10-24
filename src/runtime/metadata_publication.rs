//! Runtime ownership of cloud metadata-publication serialization.
//!
//! Cloud dispatchers are transport adapters. The runtime owns the invariant
//! that a flush publication, foreground mirror, and cleanup proof cannot
//! change metadata authority concurrently, even if they use separate
//! dispatchers for the same control location.

use parking_lot::{Mutex, MutexGuard};
use std::sync::Arc;

mod cut;
pub(crate) use cut::{CapturedMetadataCut, MetadataCutProof};

#[derive(Clone, Default)]
pub(crate) struct MetadataPublicationLock {
    state: Arc<Mutex<MetadataPublicationState>>,
    cuts: Arc<Mutex<cut::CutRegistry>>,
}

#[derive(Default)]
pub(crate) struct MetadataPublicationState {
    pub(crate) cached: Option<crate::runtime::hybrid_persistence::MetadataMirrorCache>,
    pub(crate) retention: crate::runtime::hybrid_persistence::MetadataRetentionState,
}

impl MetadataPublicationLock {
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
