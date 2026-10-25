//! Idle scheduling keeps bounded cleanup independent of future metadata writes.

use super::EventLoop;
use std::time::{Duration, Instant};

#[cfg(test)]
mod tests;

impl EventLoop {
    pub(in crate::runtime::event_loop) fn metadata_retention_timeout(&self) -> Option<Duration> {
        if self
            .actors
            .cloud_coordinator
            .cloud_metadata_storage
            .is_none()
            || self.cloud_maintenance_control_blocked()
        {
            return None;
        }
        Some(
            self.actors
                .cloud_coordinator
                .metadata_mirror
                .retention_due
                .saturating_duration_since(Instant::now()),
        )
    }

    pub(in crate::runtime::event_loop) fn schedule_metadata_retention_if_due(&mut self) {
        if self.metadata_retention_timeout() != Some(Duration::ZERO) {
            return;
        }
        let deadline = self.event_loop_cloud_deadline();
        let continuation = self.periodic_retention_continuation();
        let result = self.start_metadata_mirror(continuation, deadline);
        let pipeline = &mut self.actors.cloud_coordinator.metadata_mirror;
        let delay = if let Err(error) = result {
            tracing::debug!(%error, "cloud metadata retention remains scheduled");
            pipeline.retention_interval.min(Duration::from_secs(1))
        } else {
            pipeline.retention_interval
        };
        pipeline.retention_due = Instant::now() + delay;
    }
}
