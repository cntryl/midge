use super::EventLoop;
#[cfg(test)]
use crate::runtime::RuntimeResponse;

impl EventLoop {
    #[cfg(test)]
    pub(super) fn handle_get_ingest_state(&self, request_id: u64) {
        let active = self
            .state
            .ingest_active
            .load(std::sync::atomic::Ordering::SeqCst);
        self.respond(
            request_id,
            RuntimeResponse::IngestState {
                request_id,
                ingest_active: active,
            },
        );
    }

    #[cfg(test)]
    pub(super) fn handle_begin_ingest(&mut self, request_id: u64) {
        let active = self
            .state
            .active_compactions
            .load(std::sync::atomic::Ordering::SeqCst);
        let current_epoch = self
            .state
            .ingest_epoch
            .load(std::sync::atomic::Ordering::SeqCst);

        tracing::info!(
            component = "ingest",
            invariant = "begin_ingest_barrier",
            ingest_epoch = current_epoch,
            "ingest: begin_ingest requested"
        );
        tracing::info!(
            component = "ingest",
            invariant = "begin_ingest_barrier",
            active_compactions = active,
            ingest_epoch = current_epoch,
            "ingest: active_compactions={} (will wait for drain)",
            active
        );

        self.state.set_compaction_enabled(false);
        self.state
            .ingest_active
            .store(true, std::sync::atomic::Ordering::SeqCst);

        let new_epoch = self
            .state
            .ingest_epoch
            .fetch_add(1, std::sync::atomic::Ordering::SeqCst)
            + 1;

        let active = self
            .state
            .active_compactions
            .load(std::sync::atomic::Ordering::SeqCst);

        if active == 0 {
            tracing::info!(
                component = "ingest",
                invariant = "begin_ingest_barrier",
                ingest_epoch = new_epoch,
                "ingest: ingestion barrier enabled - no active compactions to drain"
            );
            self.respond(request_id, RuntimeResponse::Ok { request_id });
        } else {
            let mut pending = self.state.pending_compaction_waits.lock();
            pending.insert(request_id, format!("BeginIngest(epoch={new_epoch})"));
            tracing::debug!(
                component = "ingest",
                "begin_ingest queued; waiting for {} active compactions to drain (epoch={})",
                active,
                new_epoch
            );
        }
    }

    #[cfg(test)]
    pub(super) fn handle_end_ingest(&mut self, request_id: u64) {
        tracing::info!("EndIngest: flushing memtables and restoring scheduling");

        let cf_ids: Vec<u32> = self.state.column_families.keys().copied().collect();
        for cf_id in cf_ids {
            let _ = self.freeze_active_memtable(cf_id);
        }
        self.schedule_next_flush_worker();
        self.drain_inline_flush_worker();
        self.wake_write_stall_waiters();

        let new_epoch = self
            .state
            .ingest_epoch
            .fetch_add(1, std::sync::atomic::Ordering::SeqCst)
            + 1;
        tracing::info!(
            ingest_epoch = new_epoch,
            "EndIngest: ingestion barrier lifted"
        );

        self.state
            .ingest_active
            .store(false, std::sync::atomic::Ordering::SeqCst);

        let followup = self
            .compaction_actor
            .check_compaction(&self.state)
            .and_then(|plan| plan.map_or(Ok(()), |plan| self.launch_compaction(plan)));
        if let Err(error) = followup {
            self.state.mark_persistence_anomaly();
            tracing::warn!(%error, "EndIngest: failed to schedule follow-up compaction");
        }

        self.respond(request_id, RuntimeResponse::Ok { request_id });
    }
}
