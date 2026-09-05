//! Bounded ownership of in-flight immutable reader opens.

use super::{ReaderCacheKey, SstFileIo};
use crate::common::resource_budget::{ResourceBudget, ResourceReservation};
use crate::common::{MidgeError, MidgeResult};
use parking_lot::{Condvar, Mutex};
use std::collections::HashMap;
use std::sync::Arc;

type OpenResult = MidgeResult<Arc<SstFileIo>>;

pub(super) struct OpenAdmission {
    pending: Mutex<HashMap<ReaderCacheKey, Arc<PendingOpen>>>,
    available: Condvar,
    max_owners: usize,
    max_owner_bytes: usize,
}

pub(super) struct PendingOpen {
    result: Mutex<Option<OpenResult>>,
    ready: Condvar,
    _reservation: ResourceReservation,
}

pub(super) enum OpenAttempt<'a> {
    Owner(OpenOwner<'a>),
    Shared(Arc<PendingOpen>),
}

pub(super) struct OpenOwner<'a> {
    admission: &'a OpenAdmission,
    key: ReaderCacheKey,
    pending: Arc<PendingOpen>,
    finished: bool,
}

fn copy_result(result: &OpenResult) -> OpenResult {
    result.as_ref().map(Arc::clone).map_err(MidgeError::replay)
}

impl OpenAdmission {
    pub(super) fn new(metadata_bytes: usize) -> Self {
        // Keep provider fan-out finite and leave at least seven eighths of the
        // pool for actual reader metadata, including concurrently opened files.
        let max_owners = (metadata_bytes / (8 * 1024)).clamp(1, 8);
        Self {
            pending: Mutex::new(HashMap::new()),
            available: Condvar::new(),
            max_owners,
            max_owner_bytes: metadata_bytes / 8 / max_owners,
        }
    }

    pub(super) fn begin<'a>(
        &'a self,
        key: &ReaderCacheKey,
        budget: &ResourceBudget,
    ) -> MidgeResult<OpenAttempt<'a>> {
        let mut pending = self.pending.lock();
        loop {
            if let Some(shared) = pending.get(key) {
                return Ok(OpenAttempt::Shared(Arc::clone(shared)));
            }
            if pending.len() < self.max_owners {
                break;
            }
            // Callers already own their threads. Do not create tasks, queues,
            // or per-waiter engine allocations while the owner slots are full.
            self.available.wait(&mut pending);
        }
        let bytes = std::mem::size_of::<PendingOpen>()
            .saturating_add(std::mem::size_of::<OpenOwner<'_>>())
            .saturating_add(std::mem::size_of::<ReaderCacheKey>())
            .saturating_add(key.name.len().saturating_mul(2))
            .saturating_add(128);
        if bytes > self.max_owner_bytes {
            return Err(MidgeError::ResourceLimit(
                "SST open coordination exceeds its metadata allowance".into(),
            ));
        }
        let flight = Arc::new(PendingOpen {
            result: Mutex::new(None),
            ready: Condvar::new(),
            _reservation: budget.reserve(bytes, "SST open coordination")?,
        });
        pending.insert(key.clone(), Arc::clone(&flight));
        Ok(OpenAttempt::Owner(OpenOwner {
            admission: self,
            key: key.clone(),
            pending: flight,
            finished: false,
        }))
    }
}

impl PendingOpen {
    pub(super) fn wait(&self) -> OpenResult {
        let mut result = self.result.lock();
        loop {
            if let Some(result) = result.as_ref() {
                return copy_result(result);
            }
            self.ready.wait(&mut result);
        }
    }
}

impl OpenOwner<'_> {
    pub(super) fn complete(mut self, result: &OpenResult) {
        self.publish(copy_result(result));
    }

    fn publish(&mut self, result: OpenResult) {
        *self.pending.result.lock() = Some(result);
        self.pending.ready.notify_all();
        self.admission.pending.lock().remove(&self.key);
        self.finished = true;
        self.admission.available.notify_all();
    }
}

impl Drop for OpenOwner<'_> {
    fn drop(&mut self) {
        if !self.finished {
            self.publish(Err(MidgeError::Aborted(
                "SST reader open owner was canceled".into(),
            )));
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn key(id: u64) -> ReaderCacheKey {
        ReaderCacheKey {
            name: format!("{id}.sst"),
            sst_id: id,
        }
    }

    #[test]
    fn should_complete_failed_open_when_owner_finishes() -> MidgeResult<()> {
        // Arrange
        let budget = ResourceBudget::new(16 * 1024);
        let admission = OpenAdmission::new(budget.limit());
        let OpenAttempt::Owner(owner) = admission.begin(&key(1), &budget)? else {
            panic!("owner")
        };
        let OpenAttempt::Shared(shared) = admission.begin(&key(1), &budget)? else {
            panic!("shared")
        };
        // Act
        owner.complete(&Err(MidgeError::Timeout(
            "one failed provider request".into(),
        )));
        let failure = shared.wait();
        let retry = admission.begin(&key(1), &budget)?;
        // Assert
        assert!(
            matches!(failure, Err(MidgeError::Timeout(message)) if message == "one failed provider request")
        );
        assert!(matches!(retry, OpenAttempt::Owner(_)));
        Ok(())
    }

    #[test]
    fn should_clean_up_pending_open_when_owner_is_dropped() -> MidgeResult<()> {
        // Arrange
        let budget = ResourceBudget::new(16 * 1024);
        let admission = OpenAdmission::new(budget.limit());
        let OpenAttempt::Owner(owner) = admission.begin(&key(1), &budget)? else {
            panic!("owner")
        };
        let OpenAttempt::Shared(shared) = admission.begin(&key(1), &budget)? else {
            panic!("shared")
        };
        // Act
        drop(owner);
        let canceled = shared.wait();
        drop(shared);
        // Assert
        assert!(matches!(canceled, Err(MidgeError::Aborted(_))));
        assert!(admission.pending.lock().is_empty());
        assert!(budget
            .reserve(budget.limit(), "all released metadata")
            .is_ok());
        Ok(())
    }

    #[test]
    fn should_leave_metadata_workspace_when_open_coordination_reaches_its_cap() -> MidgeResult<()> {
        // Arrange
        let budget = ResourceBudget::new(16 * 1024);
        let admission = OpenAdmission::new(budget.limit());
        let owners: Vec<_> = (0..admission.max_owners)
            .map(|id| admission.begin(&key(id as u64), &budget))
            .collect::<MidgeResult<_>>()?;
        // Act
        let workspace = budget.reserve(budget.limit() * 7 / 8, "actual reader metadata")?;
        let OpenAttempt::Shared(shared) = admission.begin(&key(0), &budget)? else {
            panic!("same-object waiter must not need another slot")
        };
        // Assert
        assert_eq!(admission.pending.lock().len(), admission.max_owners);
        assert!(budget.peak() <= budget.limit());
        drop(owners);
        assert!(matches!(shared.wait(), Err(MidgeError::Aborted(_))));
        drop(shared);
        drop(workspace);
        assert!(budget.reserve(budget.limit(), "released pool").is_ok());
        Ok(())
    }
}
