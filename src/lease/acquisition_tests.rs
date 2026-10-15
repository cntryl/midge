use super::*;
use crate::common::{DeadlineScope, OperationDeadline};
use crate::lease::fs_leader_store::FsLeaderStore;
use std::sync::atomic::{AtomicBool, Ordering};

struct CancelAfterCommitStore {
    inner: FsLeaderStore,
    scope: DeadlineScope,
    cancellation_was_ambiguous: AtomicBool,
    lose_result: bool,
}

impl CancelAfterCommitStore {
    fn new(scope: DeadlineScope, lose_result: bool) -> Self {
        Self {
            inner: FsLeaderStore::new(
                Arc::new(crate::io::MockFs::new()),
                crate::lease::LeaseClock::default(),
            ),
            scope,
            cancellation_was_ambiguous: AtomicBool::new(false),
            lose_result,
        }
    }
}

impl LeaderStore for CancelAfterCommitStore {
    fn acquire_leadership(&self, holder_id: &str) -> Result<LeaderRecord, LeaseError> {
        let record = self.inner.acquire_leadership(holder_id)?;
        self.cancellation_was_ambiguous
            .store(self.scope.cancel(), Ordering::Release);
        if self.lose_result {
            return Err(LeaseError::IoError("acquisition response lost".into()));
        }
        Ok(record)
    }

    fn read_current(&self) -> Result<Option<LeaderRecord>, LeaseError> {
        self.inner.read_current()
    }
}

#[test]
fn should_track_default_acquisition_owner_when_open_is_cancelled_after_commit() {
    // Arrange: only the production filesystem store performs the mutation.
    // The wrapper cancels the caller at the committed-but-not-returned boundary.
    let scope = DeadlineScope::new(OperationDeadline::from_budget(Duration::from_secs(5)));
    let store = CancelAfterCommitStore::new(scope.clone(), false);
    let mut cleanup_epoch = None;

    // Act
    let result = acquire_leadership_with_minimum_epoch_within(
        &store,
        "cancelled-owner",
        0,
        &scope,
        &mut |epoch| cleanup_epoch = Some(epoch),
    );
    let current = store.read_current().unwrap().unwrap();

    // Assert: uncertainty was visible while the call could still commit.
    assert!(store.cancellation_was_ambiguous.load(Ordering::Acquire));
    assert!(matches!(result, Err(LeaseError::Indeterminate(_))));
    assert_eq!(cleanup_epoch, Some(current.epoch));
    assert!(store
        .inner
        .release_if_owner("cancelled-owner", current.epoch)
        .unwrap());
    assert_eq!(
        store.read_current().unwrap().unwrap().acquired_at,
        crate::lease::fs_leader_store::RELEASED_TIMESTAMP
    );
}

#[test]
fn should_keep_default_acquisition_uncertain_when_cancelled_call_loses_commit_result() {
    // Arrange
    let scope = DeadlineScope::new(OperationDeadline::from_budget(Duration::from_secs(5)));
    let store = CancelAfterCommitStore::new(scope.clone(), true);
    let mut cleanup_epoch = None;

    // Act
    let result = acquire_leadership_with_minimum_epoch_within(
        &store,
        "uncertain-owner",
        0,
        &scope,
        &mut |epoch| cleanup_epoch = Some(epoch),
    );

    // Assert: a missing result cannot prove the persisted mutation did not happen.
    assert!(store.cancellation_was_ambiguous.load(Ordering::Acquire));
    assert!(matches!(result, Err(LeaseError::Indeterminate(_))));
    assert_eq!(cleanup_epoch, None);
    assert_eq!(
        store.read_current().unwrap().unwrap().holder_id,
        "uncertain-owner"
    );
    assert!(scope.cancel());
}
