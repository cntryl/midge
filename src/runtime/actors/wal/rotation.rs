// Responsibilities for this WAL actor slice stay within the actor namespace.
//
// Rotation step classification (see the module docs in `wal.rs`):
//
// | step                                         | reversible? | failure outcome        |
// |----------------------------------------------|-------------|------------------------|
// | ticket agreement with the ledger             | no          | `Fenced`, sealed none  |
// | seal facts: the open segment's digest (no IO)| no          | `Fenced`, sealed none  |
// | heal the ledger, create the next segment     | yes         | `Open`, retryable      |
// |   linked to the seal facts                   |             |                        |
// | open segment missing, next one holds records | no          | `Fenced`, sealed none  |
// | write and sync the footer of the open segment| no          | `Fenced`, sealed = N   |
// | record `Rotate` in the segment ledger        | no          | `Fenced`, sealed = N   |
// | open a writer on the next segment            | no          | `Fenced`, sealed = N   |
// | return to `Open`                             | no          | `Fenced`, sealed = N   |
//
// The digest comes first (#843): the next segment's header names it, so the
// segment is created before the footer, while a failed create can still be
// undone. Nothing is appended to the next segment until the ledger records
// it, so a crash before that leaves at most a header-only file no history
// names, which the next open sweeps. Once the footer write starts, segment N
// takes no more records: a crash leaves its footer torn (truncated and
// resealed on the next open) or whole (recorded as the seal on the next
// open). A failed ledger write may still have become durable, so it counts
// as sealing segment N.
use super::{WalActor, WalRotationReceipt, WalTransitionOperation};
use crate::common::{MidgeError, MidgeResult};
use crate::io::Fs;
use crate::runtime::state::RuntimeState;
use crate::runtime::wal_history::PreparedRotation;
use crate::runtime::wal_transition::WalSealTicket;
use crate::runtime::wal_transition_boundary::WalTransitionBoundary;
use crate::wal::ledger::SegmentId;
use crate::wal::FsWalFactoryIo;
use std::sync::Arc;

impl WalActor {
    /// Prepare the reversible rotation steps and submit the footer append to
    /// the writer worker. The event loop retains both the returned pending
    /// effect and the prepared ledger edit until the footer append and its
    /// separate sync barrier complete.
    pub(crate) fn submit_cloud_rotation_footer(
        &mut self,
        state: &mut RuntimeState,
        deadline: &crate::common::OperationDeadline,
        ticket: &WalSealTicket,
    ) -> MidgeResult<(
        crate::wal::footer_completion::WalPendingFooter,
        PreparedRotation,
    )> {
        self.ensure_transition_authority(state)?;
        if ticket.segment_id() != state.wal.open_segment()
            || ticket.next_segment_id() != state.wal.history.ledger().next_segment_id()
            || ticket.max_sequence() != self.open_segment_max_sequence()
        {
            let error = MidgeError::Internal(
                "CloudAsync rotation ticket no longer matches the open WAL prefix".into(),
            );
            self.fence_transition(state, error.to_string());
            return Err(error);
        }
        let first_seq = match state.sequence.checked_add(1) {
            Some(first_seq) => first_seq,
            None => {
                self.cancel_reversible_cloud_flush(ticket)?;
                return Err(MidgeError::ResourceLimit("sequence space exhausted".into()));
            }
        };
        self.begin_io_transition(WalTransitionOperation::Rotate)?;
        let facts = match self.seal_facts() {
            Ok(facts) => facts,
            Err(error) => {
                self.fence_transition(
                    state,
                    format!("the open WAL segment could not be sealed: {error}"),
                );
                return Err(error);
            }
        };
        if let Err(error) = WalTransitionBoundary::AfterSealDigest.check() {
            return Err(self.roll_back_rotation(state, error));
        }
        let prepared = match self.prepare_next_segment(state, facts.clone(), first_seq) {
            Ok(prepared) => prepared,
            Err(error @ (MidgeError::RecoveryFailed(_) | MidgeError::Corruption(_))) => {
                self.fence_transition(state, error.to_string());
                return Err(error);
            }
            Err(error) => return Err(self.roll_back_rotation(state, error)),
        };
        if let Err(error) = self.ensure_transition_authority(state) {
            return Err(error);
        }
        let Some(io_timeout) = deadline.clamp_nonzero(self.storage_io_timeout) else {
            return Err(self.roll_back_rotation(
                state,
                MidgeError::Timeout(
                    "operation deadline expired before WAL footer submission".into(),
                ),
            ));
        };
        let pending = match self
            .writer()
            .ok_or_else(|| MidgeError::Internal("WAL rotation lost its writer".into()))
            .and_then(|writer| {
                writer
                    .submit_footer(ticket.footer_operation_id(), &facts, io_timeout, None)
                    .ok_or_else(|| {
                        MidgeError::Internal("WAL writer lost footer submission capability".into())
                    })?
            }) {
            Ok(pending) => pending,
            Err(error)
                if matches!(error, MidgeError::Fenced(_) | MidgeError::RecoveryFailed(_)) =>
            {
                self.fence_with_cause(state, error.to_string(), true);
                return Err(error);
            }
            Err(error) => return Err(self.roll_back_rotation(state, error)),
        };
        self.mark_transition_sealed(ticket.segment_id());
        Ok((pending, prepared))
    }

    /// Submit the footer durability barrier after its frame append completed.
    /// This remains a distinct barrier from the pre-rotation prefix sync.
    pub(crate) fn submit_cloud_footer_sync(
        &mut self,
        state: &mut RuntimeState,
        deadline: &crate::common::OperationDeadline,
        ticket: &WalSealTicket,
        footer: crate::wal::footer_completion::WalFooterPrefix,
    ) -> MidgeResult<(
        crate::wal::traits::WalPendingSync,
        crate::wal::traits::WalSyncPrefix,
    )> {
        self.ensure_transition_authority(state)?;
        let writer = self
            .writer()
            .ok_or_else(|| MidgeError::Internal("WAL rotation lost its writer".into()))?;
        let tail = writer.tail();
        let expected = crate::wal::traits::WalSyncPrefix {
            barrier_id: footer.operation_id,
            writer_epoch: footer.writer_epoch,
            segment_id: footer.segment_id,
            first_sequence: tail.first_seq.unwrap_or(0),
            last_sequence: tail.last_seq.unwrap_or(0),
            end_offset: footer.end_offset,
        };
        if footer.segment_id != ticket.segment_id()
            || footer.operation_id != ticket.footer_operation_id()
            || footer.writer_epoch != self.current_epoch
            || writer.current_pos() != footer.end_offset
        {
            let error = MidgeError::Internal(
                "WAL footer completion does not match the transitioning segment".into(),
            );
            self.fence_transition(state, error.to_string());
            return Err(error);
        }
        let Some(io_timeout) = deadline.clamp_nonzero(self.storage_io_timeout) else {
            let error = MidgeError::OutcomeUnknown(
                "operation deadline expired after footer append; footer durability is unresolved"
                    .into(),
            );
            self.fence_transition(state, error.to_string());
            return Err(error);
        };
        let submission = Self::sync_failure_boundary().and_then(|()| {
            WalTransitionBoundary::BeforeFsync.check()?;
            writer
                .submit_sync_prefix(expected, io_timeout, None)
                .ok_or_else(|| {
                    MidgeError::Internal("WAL writer lost asynchronous sync capability".into())
                })?
        });
        match submission {
            Ok(pending) => Ok((pending, expected)),
            Err(error) => {
                self.fence_transition(state, format!("WAL footer sync submission failed: {error}"));
                Err(error)
            }
        }
    }

    /// Finish a previously admitted footer only after its append and separate
    /// footer fsync have both completed. Ledger changes and replacement writer
    /// installation remain owned by the event loop's actor call.
    pub(crate) fn finish_cloud_rotation_footer(
        &mut self,
        state: &mut RuntimeState,
        ticket: &WalSealTicket,
        prepared: PreparedRotation,
        completion: MidgeResult<crate::wal::traits::WalSyncCompletion>,
        expected: crate::wal::traits::WalSyncPrefix,
    ) -> MidgeResult<WalRotationReceipt> {
        let completed = completion.and_then(|completion| {
            if completion.prefix != expected {
                return Err(MidgeError::Internal(
                    "CloudAsync footer sync did not match its admitted prefix".into(),
                ));
            }
            self.ensure_lease_validity()?;
            WalTransitionBoundary::AfterFooter.check()?;
            Ok(())
        });
        if let Err(error) = completed {
            self.fence_transition(state, error.to_string());
            return Err(error);
        }
        let old_segment = ticket.segment_id();
        let next_segment = ticket.next_segment_id();
        let max_sequence = ticket.max_sequence();
        let fs = self
            .filesystem()
            .ok_or_else(|| MidgeError::Internal("WAL rotation lost its filesystem".into()))?;
        self.record_rotation(state, prepared, old_segment)?;
        self.install_next_writer(state, &fs, old_segment, next_segment)?;
        if let Err(error) = self.finish_io_transition() {
            self.fence_transition(
                state,
                format!("WAL rotation could not return to the open state: {error}"),
            );
            return Err(error);
        }
        tracing::info!(old_segment, new_segment = next_segment, "WAL rotate");
        Ok(WalRotationReceipt {
            sealed_segment: old_segment,
            next_segment,
            max_sequence,
        })
    }

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
        // A fenced WAL refuses with its fence before comparing the ticket:
        // the ledger may already have recorded the rotation that fenced it.
        if self.is_fenced() {
            return Err(self
                .io_error()
                .unwrap_or_else(|| MidgeError::RecoveryFailed("the WAL is fenced".to_string())));
        }
        let old_segment = state.wal.open_segment();
        let next_segment = state.wal.history.ledger().next_segment_id();
        let max_sequence = self.open_segment_max_sequence();
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

        // The next segment starts past every sequence handed out.
        let first_seq = state
            .sequence
            .checked_add(1)
            .ok_or_else(|| MidgeError::ResourceLimit("sequence space exhausted".to_string()))?;

        let Some(fs) = self.filesystem() else {
            if !self.is_open() {
                return Err(self.io_error().unwrap_or_else(|| {
                    MidgeError::Internal("memory WAL transition is unavailable".to_string())
                }));
            }
            // Memory mode keeps the ledger without files or records: the
            // rotation is only the ledger's.
            let open = state.wal.history.ledger().open_member().open;
            let facts = crate::wal::format::SealedFacts {
                link: crate::wal::format::ChainLink {
                    id: open.id,
                    last_seq: open.prev.last_seq,
                    digest: [0; 16],
                },
                record_count: 0,
                data_len: 0,
                max_seq_per_cf: Vec::new(),
            };
            let prepared = state.wal.history.prepare_rotation(facts, first_seq)?;
            state.wal.history.record_rotation(prepared)?;
            return Ok(WalRotationReceipt {
                sealed_segment: old_segment,
                next_segment,
                max_sequence,
            });
        };
        self.begin_io_transition(WalTransitionOperation::Rotate)?;
        let facts = match self.seal_facts() {
            Ok(facts) => facts,
            Err(error) => {
                self.fence_transition(
                    state,
                    format!("the open WAL segment could not be sealed: {error}"),
                );
                return Err(error);
            }
        };
        if let Err(error) = WalTransitionBoundary::AfterSealDigest.check() {
            return Err(self.roll_back_rotation(state, error));
        }
        let prepared = match self.prepare_next_segment(state, facts.clone(), first_seq) {
            Ok(prepared) => prepared,
            // History is damaged: the open segment's records are gone, or
            // the next segment holds data no ledger edit accounts for.
            Err(error @ (MidgeError::RecoveryFailed(_) | MidgeError::Corruption(_))) => {
                self.fence_transition(state, error.to_string());
                return Err(error);
            }
            Err(error) => return Err(self.roll_back_rotation(state, error)),
        };
        self.write_footer(state, &facts, old_segment)?;
        self.record_rotation(state, prepared, old_segment)?;
        self.install_next_writer(state, &fs, old_segment, next_segment)?;
        if let Err(error) = self.finish_io_transition() {
            self.fence_transition(
                state,
                format!("WAL rotation could not return to the open state: {error}"),
            );
            return Err(error);
        }

        tracing::info!(old_segment, new_segment = next_segment, "WAL rotate");
        Ok(WalRotationReceipt {
            sealed_segment: old_segment,
            next_segment,
            max_sequence,
        })
    }

    /// Undo a rotation that did nothing irreversible: the writer stays on
    /// the open segment. Returns `error`, the reason it stopped.
    fn roll_back_rotation(&mut self, state: &mut RuntimeState, error: MidgeError) -> MidgeError {
        if let Err(rollback_error) = self.finish_io_transition() {
            self.fence_transition(
                state,
                format!(
                    "WAL rotation stopped ({error}) and the reversible transition could not be rolled back: {rollback_error}"
                ),
            );
        }
        error
    }

    /// The facts that seal the open segment: its digest and records, which
    /// the writer holds. No I/O.
    fn seal_facts(&self) -> MidgeResult<crate::wal::format::SealedFacts> {
        self.writer()
            .ok_or_else(|| MidgeError::Internal("WAL rotation lost its writer".to_string()))?
            .seal_facts(self.storage_io_timeout)
    }

    /// The reversible step: make the ledger writable again after an earlier
    /// failed write, then create the next segment, linked to `facts` and
    /// starting at `first_seq`, with its header durable. A header-only file
    /// an earlier attempt left is rewritten.
    fn prepare_next_segment(
        &self,
        state: &mut RuntimeState,
        facts: crate::wal::format::SealedFacts,
        first_seq: u64,
    ) -> MidgeResult<PreparedRotation> {
        WalTransitionBoundary::BeforeSegmentCreate.check()?;
        Self::before_segment_create_boundary()?;
        self.ensure_lease_validity()?;
        let prepared = state.wal.history.prepare_rotation(facts, first_seq)?;
        WalTransitionBoundary::AfterSegmentCreate.check()?;
        Ok(prepared)
    }

    /// The first irreversible step: seal the open segment with the footer
    /// `facts` describe and sync it. The writer appends nothing afterwards.
    fn write_footer(
        &mut self,
        state: &mut RuntimeState,
        facts: &crate::wal::format::SealedFacts,
        old_segment: SegmentId,
    ) -> MidgeResult<()> {
        self.ensure_transition_authority(state)?;
        self.mark_transition_sealed(old_segment);
        let written = self
            .writer()
            .ok_or_else(|| MidgeError::Internal("WAL rotation lost its writer".to_string()))
            .and_then(|writer| writer.write_footer(facts, self.storage_io_timeout));
        if let Err(error) = written {
            self.fence_transition(
                state,
                format!("the footer of WAL segment {old_segment} could not be written: {error}"),
            );
            tracing::error!(old_segment, error = %error, "WAL rotate could not seal the open segment");
            return Err(error);
        }
        if let Err(error) = WalTransitionBoundary::AfterFooter.check() {
            self.fence_transition(state, error.to_string());
            return Err(error);
        }
        Ok(())
    }

    /// The irreversible step: stop writing the open segment, then make the
    /// ledger record it sealed and the next segment open.
    fn record_rotation(
        &mut self,
        state: &mut RuntimeState,
        prepared: PreparedRotation,
        old_segment: SegmentId,
    ) -> MidgeResult<()> {
        drop(self.take_transition_writer());
        self.ensure_transition_authority(state)?;
        let recorded = state.wal.history.record_rotation(prepared);
        self.mark_transition_sealed(old_segment);
        if let Err(error) = recorded {
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
