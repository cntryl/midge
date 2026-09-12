//! Event-loop side of the WAL durability transition protocol.
//!
//! The protocol itself lives in `crate::runtime::wal_transition`. This module
//! owns the only two ways the event loop may leave a transition:
//! [`EventLoop::fence_wal_transition`] fences the actor and the protocol
//! together, and [`EventLoop::settle_failed_seal_step`] either fences or rolls
//! back a seal depending on whether the actor crossed an irreversible step.
//! Every seal and sync path in the event loop routes its failures through
//! these two helpers so the actor and protocol views cannot diverge.

use crate::common::{MidgeError, MidgeResult};
use crate::runtime::actors::wal::WalRotationReceipt;
use crate::runtime::wal_transition::WalSealTicket;

impl super::EventLoop {
    /// Fence the WAL actor and the transition protocol as one unit.
    ///
    /// This is the only place the event loop fences either of them, so a
    /// fenced protocol always implies a fenced actor and vice versa. A sealed
    /// segment that lost its runtime owner is recorded from either argument or
    /// the actor's own transition bookkeeping.
    pub(super) fn fence_wal_transition(&mut self, error: &MidgeError, sealed_segment: Option<u64>) {
        let sealed_segment = sealed_segment.or_else(|| self.wal_actor.fenced_sealed_segment());
        self.wal_actor
            .fence_transition(&mut self.state, error.to_string());
        self.wal_transition.fence(error.to_string(), sealed_segment);
        self.state.mark_persistence_anomaly();
    }

    /// Resolve a failed actor-side seal step.
    ///
    /// If the actor fenced itself, the step crossed an irreversible boundary
    /// and the protocol must fence with it. Otherwise nothing irreversible
    /// happened and the prepared obligation is rolled back so the seal can be
    /// retried. Consuming the ticket prevents the caller from continuing the
    /// actor side of a transition the protocol no longer tracks.
    pub(super) fn settle_failed_seal_step(&mut self, ticket: WalSealTicket, error: &MidgeError) {
        if self.wal_actor.is_fenced() {
            // The actor records a sealed segment only once the rename happened,
            // so `None` here defers to its bookkeeping instead of guessing.
            self.fence_wal_transition(error, None);
            drop(ticket);
        } else {
            self.wal_transition.abandon_prepared_seal(ticket);
        }
    }

    /// Rotate a local WAL only through the paired lifecycle protocol.
    pub(super) fn rotate_local_wal_transition(&mut self) -> MidgeResult<WalRotationReceipt> {
        self.wal_transition.ensure_ready()?;
        let segment_id = self.state.wal.current_segment_id;
        let next_segment_id = segment_id.checked_add(1).ok_or_else(|| {
            MidgeError::ResourceLimit("WAL segment identity space exhausted".to_string())
        })?;
        let max_sequence = self.wal_actor.current_segment_max_sequence();
        let ticket = self
            .wal_transition
            .begin_seal(segment_id, next_segment_id, max_sequence)?;

        let receipt = match self.wal_actor.rotate(&mut self.state, &ticket) {
            Ok(receipt) => receipt,
            Err(error) => {
                self.settle_failed_seal_step(ticket, &error);
                return Err(error);
            }
        };
        if let Err(error) = self.wal_transition.note_sealed(&ticket, receipt) {
            self.fence_wal_transition(&error, Some(segment_id));
            return Err(error);
        }
        if let Err(error) = self.wal_transition.finish_seal(ticket, receipt) {
            self.fence_wal_transition(&error, Some(segment_id));
            return Err(error);
        }
        if let Err(error) = self.wal_transition.retire_local_seal(segment_id) {
            self.fence_wal_transition(&error, Some(segment_id));
            return Err(error);
        }
        Ok(receipt)
    }

    /// Paired `CloudAsync` seal used by acknowledgement fixtures that must
    /// control remote publication explicitly without bypassing lifecycle
    /// bookkeeping.
    #[cfg(test)]
    pub(super) fn seal_cloud_wal_without_enqueue_for_test(&mut self) -> MidgeResult<(u64, u64)> {
        self.wal_transition.ensure_ready()?;
        let segment_id = self.state.wal.current_segment_id;
        let next_segment_id = segment_id.checked_add(1).ok_or_else(|| {
            MidgeError::ResourceLimit("WAL segment identity space exhausted".to_string())
        })?;
        let max_sequence = self.wal_actor.current_segment_max_sequence();
        let ticket = self
            .wal_transition
            .begin_seal(segment_id, next_segment_id, max_sequence)?;
        let flushed = match self
            .wal_actor
            .flush_for_cloud_upload(&mut self.state, &ticket)
        {
            Ok(flushed) => flushed,
            Err(error) => {
                self.settle_failed_seal_step(ticket, &error);
                return Err(error);
            }
        };
        if flushed != max_sequence {
            let error = MidgeError::Fenced(
                "cloud WAL fixture observed inconsistent seal accounting".to_string(),
            );
            self.fence_wal_transition(&error, None);
            return Err(error);
        }
        let receipt = match self.wal_actor.rotate(&mut self.state, &ticket) {
            Ok(receipt) => receipt,
            Err(error) => {
                self.settle_failed_seal_step(ticket, &error);
                return Err(error);
            }
        };
        if let Err(error) = self.wal_transition.note_sealed(&ticket, receipt) {
            self.finish_failed_cloud_seal_transition(receipt, &error);
            return Err(error);
        }
        if let Err(error) = self.durability.rotate_from_to(segment_id, next_segment_id) {
            self.finish_failed_cloud_seal_transition(receipt, &error);
            return Err(error);
        }
        self.durability
            .record_cloud_segment_inflight(segment_id, max_sequence);
        if let Err(error) = self.wal_transition.note_queued(segment_id) {
            self.finish_failed_cloud_seal_transition(receipt, &error);
            return Err(error);
        }
        self.wal_actor
            .complete_cloud_upload_seal(&mut self.state, receipt);
        self.durability.record_cloud_flush();
        if let Err(error) = self.wal_transition.finish_seal(ticket, receipt) {
            self.finish_failed_cloud_seal_transition(receipt, &error);
            return Err(error);
        }
        Ok((segment_id, max_sequence))
    }
}

#[cfg(test)]
mod tests {
    use super::super::tests::create_test_state;
    use super::super::EventLoop;
    use crate::common::MidgeError;
    use crate::runtime::{ResponseRouter, RuntimeConfig};
    use std::sync::Arc;

    fn create_local_event_loop() -> EventLoop {
        let state = create_test_state();
        let router = Arc::new(ResponseRouter::new());
        let config = RuntimeConfig {
            wal_durability_policy: crate::wal::DurabilityPolicy::Batched,
            ..RuntimeConfig::default()
        };
        EventLoop::new(state, false, router, config, None).expect("create event loop")
    }

    #[test]
    fn should_fence_actor_and_protocol_together_from_a_single_helper() {
        // Arrange
        let mut event_loop = create_local_event_loop();
        let error = MidgeError::Internal("injected".to_string());

        // Act
        event_loop.fence_wal_transition(&error, Some(3));

        // Assert
        assert!(event_loop.wal_actor.is_fenced());
        assert!(event_loop.wal_transition.is_fenced());
        assert_eq!(event_loop.wal_transition.fenced_sealed_segment(), Some(3));
        assert!(event_loop.state.persistence_anomaly_detected());
        assert!(matches!(
            event_loop.rotate_local_wal_transition(),
            Err(MidgeError::Fenced(_))
        ));
    }

    #[test]
    fn should_roll_back_prepared_seal_when_actor_stayed_operational() {
        // Arrange
        let mut event_loop = create_local_event_loop();
        let ticket = event_loop
            .wal_transition
            .begin_seal(1, 2, 0)
            .expect("prepare seal");
        let error = MidgeError::Internal("reversible".to_string());

        // Act
        event_loop.settle_failed_seal_step(ticket, &error);

        // Assert
        assert!(!event_loop.wal_actor.is_fenced());
        assert!(!event_loop.wal_transition.is_fenced());
        assert!(!event_loop.wal_transition.segment_is_tracked(1));
        event_loop
            .wal_transition
            .ensure_ready()
            .expect("rolled back seal must leave the protocol ready");
    }

    #[test]
    fn should_advance_segment_and_retire_local_seal_when_rotation_commits() {
        // Arrange
        let mut event_loop = create_local_event_loop();
        let segment_id = event_loop.state.wal.current_segment_id;

        // Act
        let receipt = event_loop
            .rotate_local_wal_transition()
            .expect("local rotation");

        // Assert
        assert_eq!(receipt.sealed_segment, segment_id);
        assert_eq!(event_loop.state.wal.current_segment_id, segment_id + 1);
        assert!(!event_loop.wal_transition.segment_is_tracked(segment_id));
        event_loop
            .wal_transition
            .ensure_ready()
            .expect("committed rotation must leave the protocol ready");
        assert!(!event_loop.state.persistence_anomaly_detected());
    }

    #[cfg(feature = "failpoints")]
    #[test]
    fn should_keep_protocol_and_actor_consistent_at_every_local_rotation_boundary() {
        // Arrange
        let _test_guard = crate::failpoints::test_failpoint_guard();
        let scenario = fail::FailScenario::setup();
        use crate::runtime::wal_transition_boundary::WalTransitionBoundary;

        for boundary in WalTransitionBoundary::LOCAL_ROTATION_BOUNDARIES {
            let mut event_loop = create_local_event_loop();
            let segment_id = event_loop.state.wal.current_segment_id;
            fail::cfg(boundary.failpoint_name(), "return")
                .expect("configure local rotation boundary");

            // Act
            let error = event_loop
                .rotate_local_wal_transition()
                .expect_err("configured boundary must interrupt rotation");
            fail::remove(boundary.failpoint_name());

            // Assert
            assert!(matches!(error, MidgeError::Internal(_)), "{boundary:?}");
            assert_eq!(event_loop.state.wal.current_segment_id, segment_id);
            let irreversible = boundary != WalTransitionBoundary::BeforeRename;
            assert_eq!(
                event_loop.wal_actor.is_fenced(),
                irreversible,
                "{boundary:?}"
            );
            assert_eq!(
                event_loop.wal_transition.is_fenced(),
                irreversible,
                "{boundary:?}"
            );
            assert_eq!(
                event_loop.state.persistence_anomaly_detected(),
                irreversible,
                "{boundary:?}"
            );
            if irreversible {
                assert_eq!(
                    event_loop.wal_transition.fenced_sealed_segment(),
                    Some(segment_id),
                    "{boundary:?}: the sealed file must be recorded for restart recovery"
                );
                assert!(event_loop.wal_transition.segment_is_tracked(segment_id));
                assert!(matches!(
                    event_loop.rotate_local_wal_transition(),
                    Err(MidgeError::Fenced(_))
                ));
            } else {
                assert!(!event_loop.wal_transition.segment_is_tracked(segment_id));
                event_loop
                    .rotate_local_wal_transition()
                    .expect("reversible failure must be retryable");
                assert_eq!(event_loop.state.wal.current_segment_id, segment_id + 1);
            }
        }

        scenario.teardown();
    }
}
