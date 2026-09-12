//! WAL durability transition protocol.
//!
//! # The invariant this module enforces
//!
//! A WAL generation or durability transition is not committed until every
//! component that observes it agrees:
//!
//! - the filesystem actor (`WalActor`): writer ownership, fsync, rename;
//! - the durability coordinator: the generation or segment key that new
//!   waiters join and the inflight `CloudAsync` frontier;
//! - runtime state: `current_segment_id`, `local_durable_seq`,
//!   `last_synced_seq`, `pending_writes`;
//! - cloud upload tracking: the upload backlog and acknowledged segments;
//! - durability waiters: every waiter receives exactly one terminal outcome.
//!
//! An irreversible filesystem step (fsync, rename, replacement writer creation,
//! local segment deletion) must never leave the runtime behaving as though the
//! previous operational state still exists. The protocol therefore has exactly
//! three outcomes for any transition attempt:
//!
//! 1. **committed**: every component above moved together and the phase is
//!    `Ready` again;
//! 2. **abandoned before any irreversible step**: nothing moved and the phase
//!    is `Ready` again, so the operation is safely retryable;
//! 3. **fenced**: an irreversible step happened but the transition could not
//!    complete. The actor and this protocol are both `Fenced`, durable work is
//!    rejected, health is degraded, and any sealed segment that lost its
//!    runtime owner is recorded for restart recovery.
//!
//! There is no fourth outcome. "Partially transitioned but still accepting
//! work" is unrepresentable because:
//!
//! - the actor's own I/O state machine rejects durable work unless it is
//!   `Open` with a writer (see `WalIoState`);
//! - the actor entry points that perform a transition require a
//!   [`WalSyncTicket`] or [`WalSealTicket`], which only this protocol can mint
//!   after installing a non-`Ready` phase. No other code path can fsync,
//!   rotate, or transfer accounting on the actor;
//! - the event loop fences the actor and this protocol through one helper
//!   (`EventLoop::fence_wal_transition`), so the two views cannot diverge;
//! - a sealed segment is an obligation tracked here from `begin_seal` until
//!   it is retired after cloud durability (or immediately for a local seal),
//!   so it cannot fall out of ownership between components.

use crate::common::{MidgeError, MidgeResult};
use crate::runtime::actors::wal::WalRotationReceipt;
use std::collections::BTreeMap;

/// Proof that the transition protocol installed `Syncing` for `generation`.
///
/// Only [`WalTransitionProtocol::begin_sync`] can construct one. The WAL actor
/// requires it for `begin_sync_transition` and `commit_sync_transition`, so an
/// fsync that advances the runtime durability frontier cannot happen outside
/// the paired event-loop operation.
#[derive(Debug)]
pub(crate) struct WalSyncTicket {
    generation: u64,
}

impl WalSyncTicket {
    #[must_use]
    pub(crate) fn generation(&self) -> u64 {
        self.generation
    }

    #[cfg(test)]
    pub(crate) fn for_test(generation: u64) -> Self {
        Self { generation }
    }
}

/// Proof that the transition protocol installed `Sealing` and registered the
/// segment obligation for `segment_id`.
///
/// Only [`WalTransitionProtocol::begin_seal`] can construct one. The WAL actor
/// requires it for flush, rotate, cancel, and accounting transfer, so a
/// segment cannot be sealed on disk without a runtime owner.
#[derive(Debug)]
pub(crate) struct WalSealTicket {
    segment_id: u64,
    next_segment_id: u64,
    max_sequence: u64,
}

impl WalSealTicket {
    #[must_use]
    pub(crate) fn segment_id(&self) -> u64 {
        self.segment_id
    }

    #[must_use]
    pub(crate) fn next_segment_id(&self) -> u64 {
        self.next_segment_id
    }

    #[must_use]
    pub(crate) fn max_sequence(&self) -> u64 {
        self.max_sequence
    }

    #[must_use]
    pub(crate) fn matches(&self, receipt: WalRotationReceipt) -> bool {
        self.segment_id == receipt.sealed_segment
            && self.next_segment_id == receipt.next_segment
            && self.max_sequence == receipt.max_sequence
    }

    #[cfg(test)]
    pub(crate) fn for_test(segment_id: u64, next_segment_id: u64, max_sequence: u64) -> Self {
        Self {
            segment_id,
            next_segment_id,
            max_sequence,
        }
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
enum SegmentObligationState {
    /// `begin_seal` ran; nothing irreversible has happened yet.
    Prepared,
    /// The actor renamed the active file. The segment exists on disk.
    Sealed,
    /// The segment is owned by the upload backlog or inflight frontier.
    Queued,
    /// Cloud storage acknowledged the upload; the frontier has not moved yet.
    Acknowledged,
    /// The cloud durability frontier covers the segment.
    CloudDurable,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
struct SegmentObligation {
    max_sequence: u64,
    state: SegmentObligationState,
}

#[derive(Debug, Clone, PartialEq, Eq)]
enum WalLifecyclePhase {
    Ready,
    Syncing {
        generation: u64,
    },
    Sealing {
        segment_id: u64,
        next_segment_id: u64,
        max_sequence: u64,
    },
    Acknowledging {
        through_segment_id: u64,
    },
    Fenced {
        reason: String,
        sealed_segment: Option<u64>,
    },
}

/// Cross-component WAL transition state and authoritative sealed obligations.
pub(crate) struct WalTransitionProtocol {
    phase: WalLifecyclePhase,
    segments: BTreeMap<u64, SegmentObligation>,
}

impl WalTransitionProtocol {
    pub(crate) fn new() -> Self {
        Self {
            phase: WalLifecyclePhase::Ready,
            segments: BTreeMap::new(),
        }
    }

    pub(crate) fn ensure_ready(&self) -> MidgeResult<()> {
        match &self.phase {
            WalLifecyclePhase::Ready => Ok(()),
            WalLifecyclePhase::Fenced {
                reason,
                sealed_segment,
            } => Err(MidgeError::Fenced(format!(
                "{reason}{}",
                sealed_segment.map_or_else(String::new, |segment| {
                    format!("; sealed segment {segment} requires restart recovery")
                })
            ))),
            phase => Err(MidgeError::Fenced(format!(
                "WAL durability transition {phase:?} is incomplete"
            ))),
        }
    }

    pub(crate) fn begin_sync(&mut self, generation: u64) -> MidgeResult<WalSyncTicket> {
        self.ensure_ready()?;
        self.phase = WalLifecyclePhase::Syncing { generation };
        Ok(WalSyncTicket { generation })
    }

    pub(crate) fn begin_seal(
        &mut self,
        segment_id: u64,
        next_segment_id: u64,
        max_sequence: u64,
    ) -> MidgeResult<WalSealTicket> {
        self.ensure_ready()?;
        if self.segments.contains_key(&segment_id) {
            return Err(MidgeError::Fenced(format!(
                "WAL segment {segment_id} already has a transition obligation"
            )));
        }
        self.segments.insert(
            segment_id,
            SegmentObligation {
                max_sequence,
                state: SegmentObligationState::Prepared,
            },
        );
        self.phase = WalLifecyclePhase::Sealing {
            segment_id,
            next_segment_id,
            max_sequence,
        };
        Ok(WalSealTicket {
            segment_id,
            next_segment_id,
            max_sequence,
        })
    }

    pub(crate) fn note_sealed(
        &mut self,
        ticket: &WalSealTicket,
        receipt: WalRotationReceipt,
    ) -> MidgeResult<()> {
        if !ticket.matches(receipt) || !self.is_sealing(ticket) {
            return Err(MidgeError::Fenced(
                "WAL rotation receipt does not match the active seal transition".to_string(),
            ));
        }
        if let Some(obligation) = self.segments.get_mut(&ticket.segment_id) {
            obligation.state = SegmentObligationState::Sealed;
        }
        Ok(())
    }

    pub(crate) fn note_queued(&mut self, segment_id: u64) -> MidgeResult<()> {
        let obligation = self.segments.get_mut(&segment_id).ok_or_else(|| {
            MidgeError::Fenced(format!(
                "sealed WAL segment {segment_id} was queued without an obligation"
            ))
        })?;
        match obligation.state {
            SegmentObligationState::Sealed | SegmentObligationState::Queued => {
                obligation.state = SegmentObligationState::Queued;
                Ok(())
            }
            state => Err(MidgeError::Fenced(format!(
                "WAL segment {segment_id} cannot enter the upload queue from {state:?}"
            ))),
        }
    }

    pub(crate) fn note_acknowledged(&mut self, segment_id: u64) -> MidgeResult<()> {
        let obligation = self.segments.get_mut(&segment_id).ok_or_else(|| {
            MidgeError::Fenced(format!(
                "cloud acknowledged unknown WAL segment {segment_id}"
            ))
        })?;
        match obligation.state {
            SegmentObligationState::Queued | SegmentObligationState::Acknowledged => {
                obligation.state = SegmentObligationState::Acknowledged;
                Ok(())
            }
            state => Err(MidgeError::Fenced(format!(
                "WAL segment {segment_id} cannot be acknowledged from {state:?}"
            ))),
        }
    }

    pub(crate) fn note_requeued(&mut self, segment_id: u64) -> MidgeResult<()> {
        let obligation = self.segments.get_mut(&segment_id).ok_or_else(|| {
            MidgeError::Fenced(format!(
                "cloud failure referenced unknown WAL segment {segment_id}"
            ))
        })?;
        match obligation.state {
            SegmentObligationState::Queued | SegmentObligationState::Acknowledged => {
                obligation.state = SegmentObligationState::Queued;
                Ok(())
            }
            state => Err(MidgeError::Fenced(format!(
                "WAL segment {segment_id} cannot be requeued from {state:?}"
            ))),
        }
    }

    /// Whether `segment_id` still has an unretired upload obligation.
    #[must_use]
    pub(crate) fn owns_segment(&self, segment_id: u64) -> bool {
        self.segments.contains_key(&segment_id)
    }

    pub(crate) fn begin_ack(&mut self, through_segment_id: u64) -> MidgeResult<()> {
        self.ensure_ready()?;
        self.phase = WalLifecyclePhase::Acknowledging { through_segment_id };
        Ok(())
    }

    pub(crate) fn note_cloud_durable(&mut self, segment_id: u64) -> MidgeResult<()> {
        let obligation = self.segments.get_mut(&segment_id).ok_or_else(|| {
            MidgeError::Fenced(format!(
                "cloud durability advanced through unknown WAL segment {segment_id}"
            ))
        })?;
        match obligation.state {
            SegmentObligationState::Acknowledged | SegmentObligationState::CloudDurable => {
                obligation.state = SegmentObligationState::CloudDurable;
                Ok(())
            }
            state => Err(MidgeError::Fenced(format!(
                "WAL segment {segment_id} cannot become cloud durable from {state:?}"
            ))),
        }
    }

    pub(crate) fn finish_sync(&mut self, ticket: WalSyncTicket) -> MidgeResult<()> {
        match self.phase {
            WalLifecyclePhase::Syncing { generation } if generation == ticket.generation => {
                self.phase = WalLifecyclePhase::Ready;
                Ok(())
            }
            _ => Err(MidgeError::Fenced(format!(
                "WAL sync generation {} cannot commit from phase {:?}",
                ticket.generation, self.phase
            ))),
        }
    }

    pub(crate) fn finish_seal(
        &mut self,
        ticket: WalSealTicket,
        receipt: WalRotationReceipt,
    ) -> MidgeResult<()> {
        if !ticket.matches(receipt) || !self.is_sealing(&ticket) {
            return Err(MidgeError::Fenced(format!(
                "WAL seal receipt cannot commit from phase {:?}",
                self.phase
            )));
        }
        self.phase = WalLifecyclePhase::Ready;
        Ok(())
    }

    pub(crate) fn finish_ack(&mut self, through_segment_id: u64) -> MidgeResult<()> {
        match self.phase {
            WalLifecyclePhase::Acknowledging {
                through_segment_id: active,
            } if active == through_segment_id => {
                self.phase = WalLifecyclePhase::Ready;
                Ok(())
            }
            _ => Err(MidgeError::Fenced(format!(
                "cloud WAL acknowledgement through segment {through_segment_id} cannot commit from phase {:?}",
                self.phase
            ))),
        }
    }

    /// Roll back a seal that failed before any irreversible step.
    ///
    /// Consuming the ticket guarantees the caller cannot continue the actor
    /// side of a transition the protocol no longer tracks. The rollback is
    /// only honored while the obligation is still `Prepared`; once the actor
    /// reported a rename the obligation must survive into `Fenced`.
    pub(crate) fn abandon_prepared_seal(&mut self, ticket: WalSealTicket) {
        if self
            .segments
            .get(&ticket.segment_id)
            .is_some_and(|obligation| obligation.state == SegmentObligationState::Prepared)
        {
            self.segments.remove(&ticket.segment_id);
            if self.is_sealing(&ticket) {
                self.phase = WalLifecyclePhase::Ready;
            }
        }
    }

    pub(crate) fn fence(&mut self, reason: impl Into<String>, sealed_segment: Option<u64>) {
        let sealed_segment = sealed_segment.or(match self.phase {
            WalLifecyclePhase::Fenced { sealed_segment, .. } => sealed_segment,
            _ => None,
        });
        self.phase = WalLifecyclePhase::Fenced {
            reason: reason.into(),
            sealed_segment,
        };
    }

    pub(crate) fn register_recovered(
        &mut self,
        segment_id: u64,
        max_sequence: u64,
        acknowledged: bool,
    ) {
        self.segments.insert(
            segment_id,
            SegmentObligation {
                max_sequence,
                state: if acknowledged {
                    SegmentObligationState::Acknowledged
                } else {
                    SegmentObligationState::Queued
                },
            },
        );
    }

    pub(crate) fn retire_cloud_durable(&mut self, segment_id: u64) -> MidgeResult<()> {
        match self.segments.get(&segment_id) {
            Some(SegmentObligation {
                state: SegmentObligationState::CloudDurable,
                ..
            }) => {
                self.segments.remove(&segment_id);
                Ok(())
            }
            state => Err(MidgeError::Fenced(format!(
                "WAL segment {segment_id} cannot retire from {state:?}"
            ))),
        }
    }

    /// A local (non-cloud) seal has no upload obligation: once the paired
    /// rotation committed, the fsynced sealed file needs no further owner.
    pub(crate) fn retire_local_seal(&mut self, segment_id: u64) -> MidgeResult<()> {
        match self.segments.get(&segment_id) {
            Some(SegmentObligation {
                state: SegmentObligationState::Sealed,
                ..
            }) => {
                self.segments.remove(&segment_id);
                Ok(())
            }
            state => Err(MidgeError::Fenced(format!(
                "local WAL segment {segment_id} cannot retire from {state:?}"
            ))),
        }
    }

    fn is_sealing(&self, ticket: &WalSealTicket) -> bool {
        matches!(
            self.phase,
            WalLifecyclePhase::Sealing {
                segment_id,
                next_segment_id,
                max_sequence,
            } if segment_id == ticket.segment_id
                && next_segment_id == ticket.next_segment_id
                && max_sequence == ticket.max_sequence
        )
    }

    #[cfg(test)]
    pub(crate) fn segment_is_tracked(&self, segment_id: u64) -> bool {
        self.segments.contains_key(&segment_id)
    }

    #[cfg(test)]
    pub(crate) fn is_fenced(&self) -> bool {
        matches!(self.phase, WalLifecyclePhase::Fenced { .. })
    }

    #[cfg(test)]
    pub(crate) fn fenced_sealed_segment(&self) -> Option<u64> {
        match self.phase {
            WalLifecyclePhase::Fenced { sealed_segment, .. } => sealed_segment,
            _ => None,
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn receipt(sealed: u64, next: u64, max_sequence: u64) -> WalRotationReceipt {
        WalRotationReceipt {
            sealed_segment: sealed,
            next_segment: next,
            max_sequence,
            sealed_file_created: true,
        }
    }

    #[test]
    fn should_retain_segment_obligation_when_seal_is_fenced_after_rename() -> MidgeResult<()> {
        // Arrange
        let mut protocol = WalTransitionProtocol::new();
        let ticket = protocol.begin_seal(7, 8, 42)?;
        protocol.note_sealed(&ticket, receipt(7, 8, 42))?;

        // Act
        protocol.fence("coordinator mismatch", Some(7));
        protocol.abandon_prepared_seal(ticket);

        // Assert
        assert!(protocol.is_fenced());
        assert!(protocol.segment_is_tracked(7));
        assert_eq!(protocol.fenced_sealed_segment(), Some(7));
        assert!(protocol.ensure_ready().is_err());
        Ok(())
    }

    #[test]
    fn should_reject_out_of_order_protocol_commits() -> MidgeResult<()> {
        // Arrange
        let mut protocol = WalTransitionProtocol::new();
        let sync = protocol.begin_sync(4)?;

        // Act
        let wrong_sync = protocol.finish_sync(WalSyncTicket::for_test(5));
        let overlapping_seal = protocol.begin_seal(4, 5, 12);
        protocol.finish_sync(sync)?;
        let seal = protocol.begin_seal(4, 5, 12)?;
        let wrong_seal = protocol.finish_seal(WalSealTicket::for_test(4, 6, 12), receipt(4, 6, 12));

        // Assert
        assert!(matches!(wrong_sync, Err(MidgeError::Fenced(_))));
        assert!(matches!(overlapping_seal, Err(MidgeError::Fenced(_))));
        assert!(matches!(wrong_seal, Err(MidgeError::Fenced(_))));
        assert!(protocol.ensure_ready().is_err());
        drop(seal);
        Ok(())
    }

    #[test]
    fn should_release_prepared_seal_without_touching_a_sealed_obligation() -> MidgeResult<()> {
        // Arrange
        let mut protocol = WalTransitionProtocol::new();
        let prepared = protocol.begin_seal(1, 2, 5)?;
        protocol.abandon_prepared_seal(prepared);
        let sealed = protocol.begin_seal(1, 2, 5)?;
        protocol.note_sealed(&sealed, receipt(1, 2, 5))?;

        // Act
        protocol.abandon_prepared_seal(sealed);

        // Assert
        assert!(protocol.segment_is_tracked(1));
        assert!(
            protocol.ensure_ready().is_err(),
            "a sealed obligation must not silently return the protocol to Ready"
        );
        Ok(())
    }

    #[test]
    fn should_reject_a_sync_commit_once_the_protocol_is_fenced() -> MidgeResult<()> {
        // Arrange
        let mut protocol = WalTransitionProtocol::new();
        let ticket = protocol.begin_sync(9)?;
        protocol.fence("fsync failed", None);

        // Act
        let result = protocol.finish_sync(ticket);

        // Assert
        assert!(matches!(result, Err(MidgeError::Fenced(_))));
        assert!(protocol.is_fenced());
        Ok(())
    }

    #[test]
    fn should_walk_cloud_obligation_through_every_state_exactly_once() -> MidgeResult<()> {
        // Arrange
        let mut protocol = WalTransitionProtocol::new();
        let ticket = protocol.begin_seal(3, 4, 30)?;
        protocol.note_sealed(&ticket, receipt(3, 4, 30))?;
        protocol.note_queued(3)?;
        protocol.finish_seal(ticket, receipt(3, 4, 30))?;

        // Act
        let early_retire = protocol.retire_cloud_durable(3);
        protocol.note_acknowledged(3)?;
        protocol.note_requeued(3)?;
        protocol.note_acknowledged(3)?;
        protocol.begin_ack(3)?;
        protocol.note_cloud_durable(3)?;
        protocol.finish_ack(3)?;
        protocol.retire_cloud_durable(3)?;
        let stale_ack = protocol.note_acknowledged(3);

        // Assert
        assert!(matches!(early_retire, Err(MidgeError::Fenced(_))));
        assert!(!protocol.segment_is_tracked(3));
        assert!(matches!(stale_ack, Err(MidgeError::Fenced(_))));
        protocol.ensure_ready()
    }
}
