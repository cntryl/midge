//! Runtime ownership of cloud metadata-publication serialization.
//!
//! Cloud dispatchers are transport adapters. The runtime owns the invariant
//! that a flush publication, foreground mirror, and cleanup proof cannot
//! change metadata authority concurrently, even if they use separate
//! dispatchers for the same control location.

use parking_lot::{Mutex, MutexGuard};
use std::sync::Arc;

#[derive(Clone, Default)]
pub(crate) struct MetadataPublicationLock(Arc<Mutex<()>>);

impl MetadataPublicationLock {
    pub(crate) fn try_lock(&self) -> Option<MutexGuard<'_, ()>> {
        self.0.try_lock()
    }

    pub(crate) fn lock_for(&self, timeout: std::time::Duration) -> Option<MutexGuard<'_, ()>> {
        self.0.try_lock_for(timeout)
    }

    pub(crate) fn lock(&self) -> MutexGuard<'_, ()> {
        self.0.lock()
    }
}
