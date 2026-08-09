use crate::common::MidgeResult;
use std::sync::{Arc, Mutex};

pub(super) type FencingResources = (
    Option<Mutex<crate::lease::LeaseHeartbeat>>,
    Option<Arc<dyn crate::lease::PrimaryLease>>,
    Option<crate::lease::LeaseGuard>,
);

pub(super) enum PendingFencingCleanup {
    Known {
        completion: crossbeam::channel::Receiver<()>,
        terminal_result: MidgeResult<()>,
    },
    Runtime {
        completion: crossbeam::channel::Receiver<MidgeResult<()>>,
    },
}

/// Owns the writer-fencing resources retained for the engine lifetime.
pub(super) struct LeaseState {
    pub(super) lease: Option<Arc<dyn crate::lease::PrimaryLease>>,
    pub(super) guard: Option<crate::lease::LeaseGuard>,
    pub(super) heartbeat: Option<Mutex<crate::lease::LeaseHeartbeat>>,
    pub(super) pending_cleanup: Option<PendingFencingCleanup>,
}

impl LeaseState {
    pub(super) fn new(
        lease: Arc<dyn crate::lease::PrimaryLease>,
        guard: crate::lease::LeaseGuard,
        heartbeat: crate::lease::LeaseHeartbeat,
    ) -> Self {
        Self {
            lease: Some(lease),
            guard: Some(guard),
            heartbeat: Some(Mutex::new(heartbeat)),
            pending_cleanup: None,
        }
    }

    pub(super) fn has_resources(&self) -> bool {
        self.heartbeat.is_some() || self.lease.is_some() || self.guard.is_some()
    }

    pub(super) fn take_resources(&mut self) -> FencingResources {
        (self.heartbeat.take(), self.lease.take(), self.guard.take())
    }

    pub(super) fn restore_resources(&mut self, resources: FencingResources) {
        self.heartbeat = resources.0;
        self.lease = resources.1;
        self.guard = resources.2;
    }
}

#[cfg(test)]
mod tests {
    use super::LeaseState;
    use crate::lease::PrimaryLease;
    use std::sync::Arc;

    struct TestLease;

    impl PrimaryLease for TestLease {
        fn try_acquire(
            self: Arc<Self>,
        ) -> Result<crate::lease::LeaseGuard, crate::lease::LeaseError> {
            Ok(crate::lease::LeaseGuard::token())
        }

        fn renew(&self) -> Result<(), crate::lease::LeaseError> {
            Ok(())
        }

        fn release(&self) -> Result<(), crate::lease::LeaseError> {
            Ok(())
        }

        fn ttl(&self) -> std::time::Duration {
            std::time::Duration::from_secs(30)
        }

        fn holder_id(&self) -> String {
            "test-holder".to_string()
        }

        fn epoch(&self) -> u64 {
            1
        }
    }

    #[test]
    fn should_move_fencing_resources_when_cleanup_begins() {
        // Arrange
        let lease: Arc<dyn PrimaryLease> = Arc::new(TestLease);
        let heartbeat = crate::lease::LeaseHeartbeat::new(Arc::clone(&lease));
        let mut state = LeaseState::new(lease, crate::lease::LeaseGuard::token(), heartbeat);

        // Act
        let resources = state.take_resources();

        // Assert
        assert!(!state.has_resources());
        assert!(resources.0.is_some());
        assert!(resources.1.is_some());
        assert!(resources.2.is_some());
    }
}
