// Responsibilities for this WAL actor slice stay within the actor namespace.
//
// Rotation step classification (see the module docs in `wal.rs`):
//
// | step                                         | reversible? | failure outcome        |
// |----------------------------------------------|-------------|------------------------|
// | ticket / ledger agreement                    | no          | `Fenced`, sealed none  |
// | create the next segment, empty and durable   | yes         | `Open`, retryable      |
// | drop the writer of the open segment          | no          | `Fenced`, sealed none  |
// | record `Rotate` in the segment ledger        | no          | `Fenced`, sealed = N   |
// | open a writer on the next segment            | no          | `Fenced`, sealed = N   |
// | return to `Open` + advance segment id        | no          | `Fenced`, sealed = N   |
//
// Nothing is appended to the next segment until the ledger records it, so a
// crash before that leaves at most an empty file no history names, which the
// next rotation adopts. A failed ledger write may still have become durable,
// so it counts as sealing segment N.
use super::{WalActor, WalRotationReceipt, WalTransitionOperation};
use crate::common::{MidgeError, MidgeResult};
use crate::io::Fs;
use crate::runtime::state::RuntimeState;
use crate::runtime::wal_transition::WalSealTicket;
use crate::runtime::wal_transition_boundary::WalTransitionBoundary;
use crate::wal::ledger::{LedgerEdit, SegmentId};
use crate::wal::FsWalFactoryIo;
use std::sync::Arc;

impl WalActor {
    /// Rotate to a new WAL segment and return the immutable receipt that the
    /// event-loop transition protocol must register before it can accept more
    /// durable work.
    ///
    /// Requires a [`WalSealTicket`], which only the transition protocol can
    /// mint, so a segment cannot be sealed on disk without a runtime owner.
    /// On return the actor is either `Open` (nothing irreversible happened) or
    /// `Fenced` (an irreversible step happened and the rotation did not
    /// complete); it is never left mid-transition.
    pub(crate) fn rotate(
        &mut self,
        state: &mut RuntimeState,
        ticket: &WalSealTicket,
    ) -> MidgeResult<WalRotationReceipt> {
        let result = self.rotate_with_ticket(state, ticket);
        debug_assert!(
            self.is_open() || self.is_fenced(),
            "WAL rotation must settle in Open or Fenced"
        );
        result
    }

    fn rotate_with_ticket(
        &mut self,
        state: &mut RuntimeState,
        ticket: &WalSealTicket,
    ) -> MidgeResult<WalRotationReceipt> {
        self.ensure_transition_authority(state)?;
        let old_segment = state.wal.current_segment_id;
        let next_segment = old_segment.checked_add(1).ok_or_else(|| {
            MidgeError::ResourceLimit("WAL segment identity space exhausted".to_string())
        })?;
        let max_sequence = self.segment_max_sequence;
        if ticket.segment_id() != old_segment
            || ticket.next_segment_id() != next_segment
            || ticket.max_sequence() != max_sequence
        {
            let error = MidgeError::Internal(format!(
                "WAL seal ticket (segment {}, next {}, max sequence {}) disagrees with the actor (segment {old_segment}, next {next_segment}, max sequence {max_sequence})",
                ticket.segment_id(),
                ticket.next_segment_id(),
                ticket.max_sequence()
            ));
            self.fence_transition(state, error.to_string());
            return Err(error);
        }

        let Some(fs) = self.filesystem() else {
            if !self.is_open() {
                return Err(self.io_error().unwrap_or_else(|| {
                    MidgeError::Internal("memory WAL transition is unavailable".to_string())
                }));
            }
            state.wal.current_segment_id = next_segment;
            self.segment_max_sequence = 0;
            return Ok(WalRotationReceipt {
                sealed_segment: old_segment,
                next_segment,
                max_sequence,
            });
        };
        if let Err(error) = self.ensure_ledger_agrees(old_segment, next_segment) {
            self.fence_transition(state, error.to_string());
            return Err(error);
        }

        self.begin_io_transition(WalTransitionOperation::Rotate)?;
        if let Err(error) = self.create_next_segment(next_segment) {
            if let Err(rollback_error) = self.finish_io_transition() {
                self.fence_transition(
                    state,
                    format!(
                        "WAL segment creation failed and the reversible transition could not be rolled back: {rollback_error}"
                    ),
                );
            }
            return Err(error);
        }
        self.record_rotation(state, old_segment, next_segment)?;
        self.install_next_writer(state, &fs, old_segment, next_segment)?;
        if let Err(error) = self.finish_io_transition() {
            self.fence_transition(
                state,
                format!("WAL rotation could not return to the open state: {error}"),
            );
            return Err(error);
        }
        state.wal.current_segment_id = next_segment;
        self.segment_max_sequence = 0;

        tracing::info!(old_segment, new_segment = next_segment, "WAL rotate");
        Ok(WalRotationReceipt {
            sealed_segment: old_segment,
            next_segment,
            max_sequence,
        })
    }

    fn ensure_ledger_agrees(&self, old_segment: SegmentId, next: SegmentId) -> MidgeResult<()> {
        let history = self.history.as_ref().ok_or_else(|| {
            MidgeError::Internal("a file-backed WAL rotated without its segment ledger".to_string())
        })?;
        let ledger = history.ledger();
        if ledger.open_segment() != old_segment || ledger.next_segment_id() != next {
            return Err(MidgeError::Internal(format!(
                "the WAL ledger (open segment {}, next {}) disagrees with the actor (segment {old_segment}, next {next})",
                ledger.open_segment(),
                ledger.next_segment_id()
            )));
        }
        Ok(())
    }

    /// The reversible step: create the next segment, empty and durable. An
    /// empty file an earlier attempt left is adopted.
    fn create_next_segment(&self, next_segment: SegmentId) -> MidgeResult<()> {
        WalTransitionBoundary::BeforeSegmentCreate.check()?;
        Self::before_segment_create_boundary()?;
        self.ensure_lease_validity()?;
        self.history
            .as_ref()
            .ok_or_else(|| {
                MidgeError::Internal("a file-backed WAL rotated without its segment ledger".into())
            })?
            .create_segment(next_segment)?;
        WalTransitionBoundary::AfterSegmentCreate.check()
    }

    /// The irreversible step: stop writing the open segment, then make the
    /// ledger record it sealed and the next segment open.
    fn record_rotation(
        &mut self,
        state: &mut RuntimeState,
        old_segment: SegmentId,
        next_segment: SegmentId,
    ) -> MidgeResult<()> {
        drop(self.take_transition_writer());
        self.ensure_transition_authority(state)?;
        let edit = LedgerEdit::Rotate {
            seal: old_segment,
            open: next_segment,
        };
        let committed = match self.history.as_mut() {
            Some(history) => history.commit(&edit),
            None => Err(MidgeError::Internal(
                "a file-backed WAL rotated without its segment ledger".to_string(),
            )),
        };
        self.mark_transition_sealed(old_segment);
        if let Err(error) = committed {
            self.fence_transition(
                state,
                format!("the WAL ledger could not record the rotation: {error}"),
            );
            tracing::error!(old_segment, error = %error, "WAL rotate could not record the sealed segment");
            return Err(error);
        }
        if let Err(error) = WalTransitionBoundary::AfterLedgerRotate.check() {
            self.fence_transition(state, error.to_string());
            return Err(error);
        }
        Ok(())
    }

    fn install_next_writer(
        &mut self,
        state: &mut RuntimeState,
        fs: &Arc<dyn Fs>,
        old_segment: SegmentId,
        next_segment: SegmentId,
    ) -> MidgeResult<()> {
        if let Err(error) = Self::after_ledger_rotate_boundary() {
            self.fence_transition(state, error.to_string());
            return Err(error);
        }
        let writer = match self.segment_writer(fs, next_segment) {
            Ok(writer) => writer,
            Err(error) => {
                self.fence_transition(
                    state,
                    format!(
                        "WAL segment {old_segment} was sealed but no writer could open segment {next_segment}: {error}"
                    ),
                );
                tracing::error!(old_segment, error = %error, "WAL rotate could not install the next writer");
                return Err(error);
            }
        };
        if let Err(error) = self.install_transition_writer(writer) {
            self.fence_transition(state, error.to_string());
            return Err(error);
        }
        if let Err(error) = Self::after_writer_create_boundary() {
            self.fence_transition(state, error.to_string());
            return Err(error);
        }
        self.ensure_transition_authority(state)
    }

    fn ensure_transition_authority(&mut self, state: &mut RuntimeState) -> MidgeResult<()> {
        if let Err(error) = self.ensure_lease_validity() {
            self.fence_with_cause(
                state,
                error.to_string(),
                matches!(error, MidgeError::Fenced(_)),
            );
            return Err(error);
        }
        Ok(())
    }

    fn segment_writer(
        &self,
        fs: &Arc<dyn Fs>,
        segment: SegmentId,
    ) -> MidgeResult<Box<dyn crate::wal::WalWriter>> {
        FsWalFactoryIo::new(Arc::clone(fs))
            .with_io_timeout(self.storage_io_timeout)
            .with_counters(self.counters.clone())
            .with_write_authority(self.write_authority())
            .open_segment(segment)
    }

    // The failpoint expands to an early return only with the `failpoints`
    // feature; the Result is the production-shaped boundary contract.
    #[allow(clippy::unnecessary_wraps)]
    fn before_segment_create_boundary() -> MidgeResult<()> {
        crate::failpoints::fail_point!("midge::wal::inject_fail_before_segment_create", |_| Err(
            MidgeError::Internal(
                "failpoint: WAL rotation failed before creating the next segment".to_string(),
            )
        ));
        Ok(())
    }

    // The failpoint expands to an early return only with the `failpoints`
    // feature; the Result is the production-shaped boundary contract.
    #[allow(clippy::unnecessary_wraps)]
    fn after_ledger_rotate_boundary() -> MidgeResult<()> {
        crate::failpoints::fail_point!(
            "midge::wal::inject_fail_after_ledger_rotate_before_writer_create",
            |_| Err(MidgeError::Internal(
                "failpoint: the next WAL writer failed after the ledger recorded the rotation"
                    .to_string(),
            ))
        );
        Ok(())
    }

    // The failpoint expands to an early return only with the `failpoints`
    // feature; the Result is the production-shaped boundary contract.
    #[allow(clippy::unnecessary_wraps)]
    fn after_writer_create_boundary() -> MidgeResult<()> {
        crate::failpoints::fail_point!(
            "midge::wal::inject_fail_after_writer_create_before_commit",
            |_| Err(MidgeError::Internal(
                "failpoint: replacement WAL writer created before transition commit".to_string(),
            ))
        );
        Ok(())
    }
}
