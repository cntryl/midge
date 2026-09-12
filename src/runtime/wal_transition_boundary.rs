//! Stable failure boundaries for the WAL durability commit protocol.

use crate::common::{MidgeError, MidgeResult};

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(crate) enum WalTransitionBoundary {
    BeforeFsync,
    AfterFsync,
    AfterAppendBeforeAccounting,
    BeforeRename,
    AfterRename,
    BeforeWriterCreate,
    AfterWriterCreate,
    BeforeCoordinatorCommit,
    AfterCoordinatorCommit,
    BeforeSegmentRegistration,
    AfterSegmentRegistration,
    BeforeAccountingTransfer,
    AfterAccountingTransfer,
    BeforeWaiterCompletion,
    AfterCommitBeforeReturn,
}

impl WalTransitionBoundary {
    #[cfg(test)]
    pub(crate) const ALL: [Self; 15] = [
        Self::BeforeFsync,
        Self::AfterFsync,
        Self::AfterAppendBeforeAccounting,
        Self::BeforeRename,
        Self::AfterRename,
        Self::BeforeWriterCreate,
        Self::AfterWriterCreate,
        Self::BeforeCoordinatorCommit,
        Self::AfterCoordinatorCommit,
        Self::BeforeSegmentRegistration,
        Self::AfterSegmentRegistration,
        Self::BeforeAccountingTransfer,
        Self::AfterAccountingTransfer,
        Self::BeforeWaiterCompletion,
        Self::AfterCommitBeforeReturn,
    ];

    pub(crate) const fn failpoint_name(self) -> &'static str {
        match self {
            Self::BeforeFsync => "midge::wal_transition::before_fsync",
            Self::AfterFsync => "midge::wal_transition::after_fsync",
            Self::AfterAppendBeforeAccounting => {
                "midge::wal_transition::after_append_before_accounting"
            }
            Self::BeforeRename => "midge::wal_transition::before_rename",
            Self::AfterRename => "midge::wal_transition::after_rename",
            Self::BeforeWriterCreate => "midge::wal_transition::before_writer_create",
            Self::AfterWriterCreate => "midge::wal_transition::after_writer_create",
            Self::BeforeCoordinatorCommit => "midge::wal_transition::before_coordinator_commit",
            Self::AfterCoordinatorCommit => "midge::wal_transition::after_coordinator_commit",
            Self::BeforeSegmentRegistration => "midge::wal_transition::before_segment_registration",
            Self::AfterSegmentRegistration => "midge::wal_transition::after_segment_registration",
            Self::BeforeAccountingTransfer => "midge::wal_transition::before_accounting_transfer",
            Self::AfterAccountingTransfer => "midge::wal_transition::after_accounting_transfer",
            Self::BeforeWaiterCompletion => "midge::wal_transition::before_waiter_completion",
            Self::AfterCommitBeforeReturn => "midge::wal_transition::after_commit_before_return",
        }
    }

    pub(crate) fn check(self) -> MidgeResult<()> {
        if crate::failpoints::is_active(self.failpoint_name()) {
            return Err(MidgeError::Internal(format!(
                "failpoint: WAL transition stopped at {self:?}"
            )));
        }
        Ok(())
    }
}
