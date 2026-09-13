// Responsibilities for this WAL actor slice stay within the actor namespace.
//
// Rotation step classification (see the module docs in `wal.rs`):
//
// | step                                   | reversible? | failure outcome        |
// |----------------------------------------|-------------|------------------------|
// | ticket / existing-file preflight       | yes         | `Open`, retryable      |
// | drop active writer                     | yes*        | reopened or `Fenced`   |
// | rename `wal.log` -> `wal-N`            | no          | `Fenced`, sealed = N   |
// | directory sync after rename            | no          | `Fenced`, sealed = N   |
// | create replacement writer              | no          | `Fenced`, sealed = N   |
// | directory sync after writer creation   | no          | `Fenced`, sealed = N   |
// | return to `Open` + advance segment id  | no          | `Fenced`, sealed = N   |
//
// * Dropping the writer closes the file. If the rename then fails the writer
//   is reopened on the same path; if that reopen fails the actor fences.
use super::{WalActor, WalRotationReceipt, WalTransitionOperation};
use crate::common::{MidgeError, MidgeResult};
use crate::io::{Durability, Fs, FsError, FsPath};
use crate::runtime::state::RuntimeState;
use crate::runtime::wal_transition::WalSealTicket;
use crate::runtime::wal_transition_boundary::WalTransitionBoundary;
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
        let old_segment = state.wal.current_segment_id;
        let next_segment = old_segment.checked_add(1).ok_or_else(|| {
            MidgeError::ResourceLimit("WAL segment identity space exhausted".to_string())
        })?;
        let max_sequence = self.segment_max_sequence;
        if ticket.segment_id() != old_segment
            || ticket.next_segment_id() != next_segment
            || ticket.max_sequence() != max_sequence
        {
            let error = MidgeError::Fenced(format!(
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
                    MidgeError::Fenced("memory WAL transition is unavailable".to_string())
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

        self.seal_active_segment(state, &fs, old_segment)?;
        self.install_replacement_writer_after_seal(state, &fs, old_segment)?;
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

    fn seal_active_segment(
        &mut self,
        state: &mut RuntimeState,
        fs: &Arc<dyn Fs>,
        old_segment: u64,
    ) -> MidgeResult<bool> {
        let old_path = FsPath::new(crate::wal::ACTIVE_FILE_NAME);
        let new_path = FsPath::new(crate::wal::segment_file_name(old_segment));
        self.begin_io_transition(WalTransitionOperation::Rotate)?;
        let sealed_segment_exists = match fs.exists(&new_path) {
            Ok(exists) => exists,
            Err(error) => {
                let error = MidgeError::from(error);
                if let Err(rollback_error) = self.finish_io_transition() {
                    self.fence_transition(
                        state,
                        format!(
                            "WAL rotation preflight failed and the reversible transition could not be rolled back: {rollback_error}"
                        ),
                    );
                }
                return Err(error);
            }
        };
        if sealed_segment_exists {
            let error = MidgeError::Fenced(format!(
                "refusing to overwrite existing sealed WAL segment {old_segment}"
            ));
            self.fence_transition(state, error.to_string());
            return Err(error);
        }

        if let Err(error) = WalTransitionBoundary::BeforeRename.check() {
            self.finish_io_transition()?;
            return Err(error);
        }
        if let Err(error) = Self::before_rename_boundary() {
            self.finish_io_transition()?;
            return Err(error);
        }

        drop(self.take_transition_writer());
        let sealed_file_created = match fs.rename_atomic(&old_path, &new_path) {
            Ok(()) => true,
            Err(FsError::NotFound(_)) if self.can_ignore_missing_active_segment() => {
                tracing::debug!(
                    old_segment,
                    "WAL rotate ignored missing empty active segment"
                );
                false
            }
            Err(FsError::NotFound(_)) => {
                // Records were appended to a file that no longer exists. A
                // later fsync could not make them durable, so the actor must
                // not continue as though the accepted writes were intact.
                let error = MidgeError::Fenced(format!(
                    "active WAL segment disappeared while {} buffered records (through sequence {}) were pending",
                    self.pending_sync_count, self.segment_max_sequence
                ));
                self.fence_transition(state, error.to_string());
                tracing::error!(
                    old_segment,
                    "WAL rotate found no active segment for pending records"
                );
                return Err(error);
            }
            Err(error) => {
                let original = MidgeError::from(error);
                if let Err(reopen_error) = self.restore_active_writer_after_failed_rotate(fs) {
                    self.fence_transition(
                        state,
                        format!(
                            "WAL rename failed and the active writer could not be restored: {reopen_error}"
                        ),
                    );
                }
                tracing::error!(old_segment, error = %original, "WAL rotate failed before sealing active segment");
                return Err(original);
            }
        };

        if sealed_file_created {
            self.mark_transition_sealed(old_segment);
            if let Err(error) = WalTransitionBoundary::AfterRename.check() {
                self.fence_transition(state, error.to_string());
                return Err(error);
            }
            if let Err(error) = fs.sync_dir(&FsPath::new("."), Durability::Durable) {
                let error = MidgeError::from(error);
                self.fence_transition(state, format!("sealed WAL directory sync failed: {error}"));
                return Err(error);
            }
        }
        Ok(sealed_file_created)
    }

    fn install_replacement_writer_after_seal(
        &mut self,
        state: &mut RuntimeState,
        fs: &Arc<dyn Fs>,
        old_segment: u64,
    ) -> MidgeResult<()> {
        if let Err(error) = Self::after_rename_boundary() {
            self.fence_transition(state, error.to_string());
            return Err(error);
        }
        if let Err(error) = WalTransitionBoundary::BeforeWriterCreate.check() {
            self.fence_transition(state, error.to_string());
            return Err(error);
        }

        match self.create_replacement_writer(fs) {
            Ok(writer) => {
                if let Err(error) = self.install_transition_writer(writer) {
                    self.fence_transition(state, error.to_string());
                    return Err(error);
                }
                if let Err(error) = Self::after_writer_create_boundary() {
                    self.fence_transition(state, error.to_string());
                    return Err(error);
                }
                if let Err(error) = WalTransitionBoundary::AfterWriterCreate.check() {
                    self.fence_transition(state, error.to_string());
                    return Err(error);
                }
                if let Err(error) = fs.sync_dir(&FsPath::new("."), Durability::Durable) {
                    let error = MidgeError::from(error);
                    self.fence_transition(
                        state,
                        format!("replacement WAL directory sync failed: {error}"),
                    );
                    return Err(error);
                }
            }
            Err(error) => {
                self.fence_transition(
                    state,
                    format!(
                        "WAL segment was sealed but replacement writer creation failed: {error}"
                    ),
                );
                tracing::error!(old_segment, error = %error, "WAL rotate could not install replacement writer");
                return Err(error);
            }
        }

        if let Err(error) = WalTransitionBoundary::AfterReplacementDirectorySync.check() {
            self.fence_transition(state, error.to_string());
            return Err(error);
        }
        Ok(())
    }

    fn can_ignore_missing_active_segment(&self) -> bool {
        self.segment_max_sequence == 0 && !self.has_pending_data()
    }

    fn create_replacement_writer(
        &self,
        fs: &Arc<dyn Fs>,
    ) -> MidgeResult<Box<dyn crate::wal::WalWriter>> {
        let factory = FsWalFactoryIo::new(Arc::clone(fs)).with_io_timeout(self.storage_io_timeout);
        factory.create_writer(crate::wal::ACTIVE_FILE_NAME)
    }

    fn restore_active_writer_after_failed_rotate(&mut self, fs: &Arc<dyn Fs>) -> MidgeResult<()> {
        let factory = FsWalFactoryIo::new(Arc::clone(fs)).with_io_timeout(self.storage_io_timeout);
        match factory.create_writer(crate::wal::ACTIVE_FILE_NAME) {
            Ok(writer) => {
                self.install_transition_writer(writer)?;
                self.finish_io_transition()
            }
            Err(error) => {
                tracing::error!(
                    error = ?error,
                    "failed to reopen active WAL writer after rotate failure"
                );
                Err(error)
            }
        }
    }

    // The failpoint expands to an early return only with the `failpoints`
    // feature; the Result is the production-shaped boundary contract.
    #[allow(clippy::unnecessary_wraps)]
    fn before_rename_boundary() -> MidgeResult<()> {
        crate::failpoints::fail_point!("midge::wal::inject_fail_before_rename", |_| Err(
            MidgeError::Internal("failpoint: WAL rotation failed before rename".to_string(),)
        ));
        Ok(())
    }

    // The failpoint expands to an early return only with the `failpoints`
    // feature; the Result is the production-shaped boundary contract.
    #[allow(clippy::unnecessary_wraps)]
    fn after_rename_boundary() -> MidgeResult<()> {
        crate::failpoints::fail_point!(
            "midge::wal::inject_fail_after_rename_before_writer_create",
            |_| Err(MidgeError::Internal(
                "failpoint: replacement WAL writer creation failed after rename".to_string(),
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
