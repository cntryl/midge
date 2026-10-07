//! Per-owner publication accounting with fixed immutable origins.

use super::RuntimeState;
#[cfg(feature = "internal-testing")]
use crate::metadata::accounting::MetricsHandle;
use crate::metadata::accounting::{Medium, Origin, Owner};
use std::time::Instant;

/// An immutable generation keeps its original origin through retry/shutdown.
#[derive(Clone, Copy)]
pub(crate) struct FlushPublicationAccounting {
    pub(crate) origin: Origin,
    pub(crate) started: Option<Instant>,
    pub(crate) attempt_started: Option<Instant>,
}

impl FlushPublicationAccounting {
    pub(crate) const fn new(origin: Origin) -> Self {
        Self {
            origin,
            started: None,
            attempt_started: None,
        }
    }

    /// Call only when `submit_publish` accepted this exact immutable.
    /// False reports incomplete telemetry; it does not reject engine work.
    pub(crate) fn accept_attempt(&mut self, started: Instant) -> bool {
        self.started.get_or_insert(started);
        self.attempt_started.replace(started).is_none()
    }
}

impl RuntimeState {
    pub(crate) fn metadata_accounting(&self) -> &Owner {
        self.manifest_store.accounting_owner()
    }

    pub(crate) fn metadata_medium(&self) -> Medium {
        // Bound once when ManifestStore is created. Do not inspect a wrapper's
        // host_addressing or an ambient shutdown/recovery flag here.
        self.manifest_store.accounting_medium()
    }

    #[cfg(feature = "internal-testing")]
    pub(crate) fn checkpoint_metrics(&self) -> MetricsHandle {
        self.manifest_store.accounting_handle()
    }

    pub(crate) fn accept_flush_publication_attempt(&mut self, flush_id: u64, started: Instant) {
        let accepted = self
            .immutable_flush_by_id_mut(flush_id)
            .is_some_and(|(_, flush)| flush.accounting.accept_attempt(started));
        if !accepted {
            self.metadata_accounting()
                .invalidate_missing_publication_start();
        }
    }

    /// Returns false when no accepted attempt is active. Build/admission
    /// failures have no publication attempt and should not manufacture one.
    pub(crate) fn finish_flush_publication_attempt(&mut self, flush_id: u64, failed: bool) -> bool {
        let observation = self
            .immutable_flush_by_id_mut(flush_id)
            .and_then(|(_, flush)| {
                flush
                    .accounting
                    .attempt_started
                    .take()
                    .map(|started| (flush.accounting.origin, started))
            });
        if let Some((origin, started)) = observation {
            self.metadata_accounting().publication_attempt(
                origin,
                self.metadata_medium(),
                started.elapsed(),
                failed,
            );
            true
        } else {
            false
        }
    }

    /// Called after actual owned shutdown joins, before state is discarded.
    /// Mark retained accepted publication evidence rather than pretending a
    /// clock that never reached installation represents a successful attempt.
    pub(crate) fn invalidate_unsettled_flush_accounting(&self) {
        if self.column_families.values().any(|family| {
            family
                .immutable_flushes
                .iter()
                .any(|flush| flush.accounting.started.is_some())
        }) {
            self.metadata_accounting()
                .invalidate_missing_publication_start();
        }
    }
}
