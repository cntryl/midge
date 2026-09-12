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
    /// Boundaries exercised by the local paired sync matrix
    /// (`event_loop::durability_sync`).
    #[cfg(all(test, feature = "failpoints"))]
    pub(crate) const LOCAL_SYNC_BOUNDARIES: [Self; 6] = [
        Self::BeforeFsync,
        Self::AfterFsync,
        Self::BeforeCoordinatorCommit,
        Self::AfterCoordinatorCommit,
        Self::BeforeWaiterCompletion,
        Self::AfterCommitBeforeReturn,
    ];

    /// Boundaries exercised by the local rotation matrices (actor and
    /// event-loop level).
    #[cfg(all(test, feature = "failpoints"))]
    pub(crate) const LOCAL_ROTATION_BOUNDARIES: [Self; 4] = [
        Self::BeforeRename,
        Self::AfterRename,
        Self::BeforeWriterCreate,
        Self::AfterWriterCreate,
    ];

    /// Boundaries exercised by the durable append matrix.
    #[cfg(all(test, feature = "failpoints"))]
    pub(crate) const APPEND_BOUNDARIES: [Self; 1] = [Self::AfterAppendBeforeAccounting];

    /// Boundaries exercised by the `CloudAsync` seal matrix
    /// (`event_loop::cloud_integration::tests`).
    #[cfg(all(test, feature = "failpoints"))]
    pub(crate) const CLOUD_SEAL_BOUNDARIES: [Self; 10] = [
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
    ];

    /// Boundaries exercised by the `CloudAsync` acknowledgement matrix.
    #[cfg(all(test, feature = "failpoints"))]
    pub(crate) const CLOUD_ACK_BOUNDARIES: [Self; 4] = [
        Self::BeforeAccountingTransfer,
        Self::AfterAccountingTransfer,
        Self::BeforeWaiterCompletion,
        Self::AfterCommitBeforeReturn,
    ];

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

#[cfg(test)]
mod tests {
    use super::WalTransitionBoundary;

    #[test]
    fn should_enumerate_every_declared_wal_transition_boundary() {
        // Arrange
        let expected = 15;

        // Act
        let boundaries = WalTransitionBoundary::ALL;

        // Assert
        assert_eq!(boundaries.len(), expected);
        let names = boundaries.map(WalTransitionBoundary::failpoint_name);
        let unique = names.into_iter().collect::<std::collections::BTreeSet<_>>();
        assert_eq!(unique.len(), boundaries.len());
        assert!(names
            .into_iter()
            .all(|name| name.starts_with("midge::wal_transition::")));
    }

    /// Every boundary must belong to at least one failure-injection matrix.
    /// Adding a boundary to `ALL` without adding it to a matrix list fails
    /// here, and each matrix iterates its list, so a new transition step
    /// automatically receives failure-path coverage.
    #[cfg(feature = "failpoints")]
    #[test]
    fn should_cover_every_wal_transition_boundary_with_a_failure_matrix() {
        // Arrange
        let covered = WalTransitionBoundary::LOCAL_SYNC_BOUNDARIES
            .iter()
            .chain(WalTransitionBoundary::LOCAL_ROTATION_BOUNDARIES.iter())
            .chain(WalTransitionBoundary::APPEND_BOUNDARIES.iter())
            .chain(WalTransitionBoundary::CLOUD_SEAL_BOUNDARIES.iter())
            .chain(WalTransitionBoundary::CLOUD_ACK_BOUNDARIES.iter())
            .map(|boundary| boundary.failpoint_name())
            .collect::<std::collections::BTreeSet<_>>();

        // Act
        let uncovered = WalTransitionBoundary::ALL
            .iter()
            .filter(|boundary| !covered.contains(boundary.failpoint_name()))
            .copied()
            .collect::<Vec<_>>();

        // Assert
        assert!(
            uncovered.is_empty(),
            "WAL transition boundaries without a failure matrix: {uncovered:?}"
        );
    }
}
