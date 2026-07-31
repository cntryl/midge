//! Lease heartbeat and monitoring.
//!
//! Manages periodic lease renewal in a background thread.
//! Monitors lease health and triggers shutdown if renewal fails.

use super::traits::{LeaseValidityState, PrimaryLease};
use std::sync::atomic::{AtomicBool, Ordering};
use std::sync::Arc;
use std::thread::{self, JoinHandle};
use std::time::Duration;

/// Heartbeat manager for lease renewal.
///
/// Runs a background thread that periodically renews the lease.
/// If renewal fails, sets a flag that the engine can monitor.
pub struct LeaseHeartbeat {
    lease: Arc<dyn PrimaryLease>,
    validity: Option<Arc<super::traits::LeaseValidity>>,
    running: Arc<AtomicBool>,
    healthy: Arc<AtomicBool>,
    renewal_handle: Option<JoinHandle<()>>,
    watchdog_handle: Option<JoinHandle<()>>,
    spawn_failed: bool,
}

fn run_renewal_worker(
    lease: &dyn PrimaryLease,
    validity: Option<&super::traits::LeaseValidity>,
    running: &AtomicBool,
    healthy: &AtomicBool,
    ttl: Duration,
) {
    let renewal_interval = ttl / 3;
    tracing::info!(
        ttl_secs = ttl.as_secs(),
        renewal_interval_secs = renewal_interval.as_secs(),
        "lease heartbeat started"
    );

    while running.load(Ordering::Acquire) {
        if let Some(validity) = validity {
            let observed = validity.snapshot();
            match observed {
                LeaseValidityState::Inactive => {
                    let _ = validity.wait_for_change(observed, renewal_interval, running);
                    continue;
                }
                LeaseValidityState::Fenced { .. } => {
                    healthy.store(false, Ordering::Release);
                    break;
                }
                LeaseValidityState::Active { valid_until, .. } => {
                    let renewal_at = valid_until
                        .checked_sub(ttl.saturating_mul(2) / 3)
                        .unwrap_or(valid_until);
                    let wait = renewal_at.saturating_duration_since(std::time::Instant::now());
                    if !wait.is_zero()
                        && validity.wait_for_change(observed, wait, running) != observed
                    {
                        continue;
                    }
                }
            }
        } else {
            thread::park_timeout(renewal_interval);
        }

        if !running.load(Ordering::Acquire) {
            break;
        }
        match lease.renew() {
            Ok(()) => {
                if validity.is_some_and(|validity| {
                    matches!(validity.snapshot(), LeaseValidityState::Fenced { .. })
                }) {
                    healthy.store(false, Ordering::Release);
                    break;
                }
                tracing::trace!("lease renewed successfully");
            }
            Err(error) => {
                tracing::error!(%error, "lease renewal failed; marking unhealthy");
                healthy.store(false, Ordering::Release);
                if let Some(validity) = validity {
                    if let LeaseValidityState::Active { epoch, .. } = validity.snapshot() {
                        validity.fence(epoch);
                    }
                }
                break;
            }
        }
    }
    tracing::info!("lease heartbeat stopped");
}

fn run_watchdog_worker(
    validity: &super::traits::LeaseValidity,
    running: &AtomicBool,
    healthy: &AtomicBool,
) {
    while running.load(Ordering::Acquire) {
        let observed = validity.snapshot();
        match observed {
            LeaseValidityState::Inactive => {
                let _ = validity.wait_for_change(observed, Duration::from_secs(30), running);
            }
            LeaseValidityState::Fenced { .. } => {
                healthy.store(false, Ordering::Release);
                break;
            }
            LeaseValidityState::Active { epoch, valid_until } => {
                let wait = valid_until.saturating_duration_since(std::time::Instant::now());
                if wait.is_zero() {
                    validity.fence(epoch);
                    healthy.store(false, Ordering::Release);
                    break;
                }
                let current = validity.wait_for_change(observed, wait, running);
                if current == observed && std::time::Instant::now() >= valid_until {
                    validity.fence(epoch);
                    healthy.store(false, Ordering::Release);
                    break;
                }
            }
        }
    }
    tracing::info!("lease expiry watchdog stopped");
}

impl LeaseHeartbeat {
    /// Create a new heartbeat manager.
    ///
    /// Does not start the heartbeat loop automatically.
    /// Call `start()` to begin renewal.
    #[cfg(test)]
    pub fn new(lease: Arc<dyn PrimaryLease>) -> Self {
        Self::new_with_healthy_and_validity(lease, Arc::new(AtomicBool::new(true)), None)
    }

    #[cfg(test)]
    fn new_with_validity(
        lease: Arc<dyn PrimaryLease>,
        validity: Arc<super::traits::LeaseValidity>,
    ) -> Self {
        Self::new_with_healthy_and_validity(lease, Arc::new(AtomicBool::new(true)), Some(validity))
    }

    pub(crate) fn new_with_healthy_and_validity(
        lease: Arc<dyn PrimaryLease>,
        healthy: Arc<AtomicBool>,
        validity: Option<Arc<super::traits::LeaseValidity>>,
    ) -> Self {
        Self {
            lease,
            validity,
            running: Arc::new(AtomicBool::new(false)),
            healthy,
            renewal_handle: None,
            watchdog_handle: None,
            spawn_failed: false,
        }
    }

    /// Return a shared reference to the healthy flag.
    /// Start the heartbeat loop.
    ///
    /// Spawns a background thread that renews the lease at intervals
    /// of `ttl / 3` (conservative to handle clock skew and latency).
    pub fn start(&mut self) {
        if self.running.load(Ordering::Acquire) {
            tracing::warn!("lease heartbeat already running");
            return;
        }

        let lease = Arc::clone(&self.lease);
        let running = Arc::clone(&self.running);
        let healthy = Arc::clone(&self.healthy);
        let validity = self.validity.as_ref().map(Arc::clone);

        running.store(true, Ordering::Release);
        healthy.store(true, Ordering::Release);

        let ttl = lease.ttl();

        let renewal_validity = validity.as_ref().map(Arc::clone);
        let handle = thread::Builder::new()
            .name("midge-lease-renewal".to_string())
            .spawn(move || {
                run_renewal_worker(
                    lease.as_ref(),
                    renewal_validity.as_deref(),
                    &running,
                    &healthy,
                    ttl,
                );
            });

        match handle {
            Ok(h) => {
                self.renewal_handle = Some(h);
                self.spawn_failed = false;
            }
            Err(e) => {
                tracing::error!("Failed to spawn lease heartbeat thread: {}", e);
                self.healthy.store(false, Ordering::Release);
                self.spawn_failed = true;
                self.running.store(false, Ordering::Release);
                return;
            }
        }

        let Some(validity) = validity else {
            return;
        };
        let running = Arc::clone(&self.running);
        let healthy = Arc::clone(&self.healthy);
        let watchdog_validity = Arc::clone(&validity);
        match thread::Builder::new()
            .name("midge-lease-watchdog".to_string())
            .spawn(move || run_watchdog_worker(&watchdog_validity, &running, &healthy))
        {
            Ok(handle) => self.watchdog_handle = Some(handle),
            Err(error) => {
                tracing::error!(%error, "failed to spawn lease expiry watchdog");
                self.healthy.store(false, Ordering::Release);
                self.running.store(false, Ordering::Release);
                validity.notify_all();
                self.spawn_failed = true;
            }
        }
    }

    /// Stop the heartbeat loop.
    ///
    /// Waits for the background thread to exit.
    pub fn stop(&mut self) {
        if !self.running.load(Ordering::Acquire)
            && self.renewal_handle.is_none()
            && self.watchdog_handle.is_none()
        {
            return;
        }

        tracing::info!("stopping lease heartbeat");
        self.running.store(false, Ordering::Release);
        if let Some(validity) = self.validity.as_ref() {
            validity.notify_all();
        }

        if let Some(handle) = self.renewal_handle.take() {
            handle.thread().unpark();
            let _ = handle.join();
        }
        if let Some(handle) = self.watchdog_handle.take() {
            handle.thread().unpark();
            let _ = handle.join();
        }
    }

    /// Check if the lease is healthy (renewal succeeding).
    ///
    /// Returns `false` if renewal has failed, indicating the instance
    /// should stop accepting writes.
    pub fn is_healthy(&self) -> bool {
        self.healthy.load(Ordering::Acquire)
    }

    #[cfg(test)]
    pub(crate) fn healthy_flag(&self) -> Arc<AtomicBool> {
        Arc::clone(&self.healthy)
    }
}

impl Drop for LeaseHeartbeat {
    fn drop(&mut self) {
        self.stop();
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::lease::traits::{LeaseError, LeaseGuard, PrimaryLease};
    use std::sync::atomic::AtomicUsize;
    use std::time::Duration;

    struct BlockingRenewalLease {
        validity: Arc<crate::lease::traits::LeaseValidity>,
        ttl: Duration,
        renewal: std::sync::Mutex<(bool, bool, bool)>,
        changed: std::sync::Condvar,
    }

    impl BlockingRenewalLease {
        fn new(ttl: Duration) -> Self {
            Self {
                validity: Arc::new(crate::lease::traits::LeaseValidity::new()),
                ttl,
                renewal: std::sync::Mutex::new((false, false, false)),
                changed: std::sync::Condvar::new(),
            }
        }

        fn wait_until_renewal_started(&self, timeout: Duration) -> bool {
            let state = self
                .renewal
                .lock()
                .unwrap_or_else(std::sync::PoisonError::into_inner);
            let (state, _) = self
                .changed
                .wait_timeout_while(state, timeout, |state| !state.0)
                .unwrap_or_else(std::sync::PoisonError::into_inner);
            state.0
        }

        fn allow_renewal(&self) {
            let mut state = self
                .renewal
                .lock()
                .unwrap_or_else(std::sync::PoisonError::into_inner);
            state.1 = true;
            self.changed.notify_all();
        }

        fn wait_until_renewal_completed(&self, timeout: Duration) -> bool {
            let state = self
                .renewal
                .lock()
                .unwrap_or_else(std::sync::PoisonError::into_inner);
            let (state, _) = self
                .changed
                .wait_timeout_while(state, timeout, |state| !state.2)
                .unwrap_or_else(std::sync::PoisonError::into_inner);
            state.2
        }
    }

    impl PrimaryLease for BlockingRenewalLease {
        fn try_acquire(self: Arc<Self>) -> Result<LeaseGuard, LeaseError> {
            self.validity
                .activate(1, std::time::Instant::now() + self.ttl)?;
            Ok(LeaseGuard::token())
        }

        fn renew(&self) -> Result<(), LeaseError> {
            let candidate_until = std::time::Instant::now() + self.ttl;
            let mut state = self
                .renewal
                .lock()
                .unwrap_or_else(std::sync::PoisonError::into_inner);
            state.0 = true;
            self.changed.notify_all();
            state = self
                .changed
                .wait_while(state, |state| !state.1)
                .unwrap_or_else(std::sync::PoisonError::into_inner);
            drop(state);
            let result = self.validity.advance(1, candidate_until);
            let mut state = self
                .renewal
                .lock()
                .unwrap_or_else(std::sync::PoisonError::into_inner);
            state.2 = true;
            self.changed.notify_all();
            result
        }

        fn release(&self) -> Result<(), LeaseError> {
            self.validity.deactivate(1);
            Ok(())
        }

        fn ttl(&self) -> Duration {
            self.ttl
        }

        fn holder_id(&self) -> String {
            "blocking-renewal".to_string()
        }

        fn epoch(&self) -> u64 {
            1
        }
    }

    struct AdvancingLease {
        validity: Arc<crate::lease::traits::LeaseValidity>,
        ttl: Duration,
        renewals: AtomicUsize,
    }

    impl PrimaryLease for AdvancingLease {
        fn try_acquire(self: Arc<Self>) -> Result<LeaseGuard, LeaseError> {
            self.validity
                .activate(1, std::time::Instant::now() + self.ttl)?;
            Ok(LeaseGuard::token())
        }

        fn renew(&self) -> Result<(), LeaseError> {
            let valid_until = std::time::Instant::now() + self.ttl;
            self.validity.advance(1, valid_until)?;
            self.renewals.fetch_add(1, Ordering::Release);
            Ok(())
        }

        fn release(&self) -> Result<(), LeaseError> {
            self.validity.deactivate(1);
            Ok(())
        }

        fn ttl(&self) -> Duration {
            self.ttl
        }

        fn holder_id(&self) -> String {
            "advancing".to_string()
        }

        fn epoch(&self) -> u64 {
            1
        }
    }

    struct MockLease {
        renewal_count: Arc<AtomicUsize>,
        should_fail: Arc<AtomicBool>,
    }

    impl MockLease {
        fn new() -> Self {
            Self {
                renewal_count: Arc::new(AtomicUsize::new(0)),
                should_fail: Arc::new(AtomicBool::new(false)),
            }
        }

        fn get_renewal_count(&self) -> usize {
            self.renewal_count.load(Ordering::Acquire)
        }

        fn set_should_fail(&self, fail: bool) {
            self.should_fail.store(fail, Ordering::Release);
        }
    }

    impl PrimaryLease for MockLease {
        fn try_acquire(self: std::sync::Arc<Self>) -> Result<LeaseGuard, LeaseError> {
            // Token-style guard for the mock (no-op on Drop)
            Ok(LeaseGuard::token())
        }

        fn renew(&self) -> Result<(), LeaseError> {
            if self.should_fail.load(Ordering::Acquire) {
                return Err(LeaseError::RenewalFailed("mock failure".to_string()));
            }
            self.renewal_count.fetch_add(1, Ordering::Release);
            Ok(())
        }

        fn release(&self) -> Result<(), LeaseError> {
            Ok(())
        }

        fn ttl(&self) -> Duration {
            Duration::from_millis(300) // Short TTL for fast tests
        }

        fn holder_id(&self) -> String {
            "mock".to_string()
        }

        fn epoch(&self) -> u64 {
            1
        }
    }

    #[test]
    fn should_renew_lease_periodically_when_healthy() {
        // Arrange
        let mock = Arc::new(MockLease::new());
        let mut heartbeat = LeaseHeartbeat::new(mock.clone() as Arc<dyn PrimaryLease>);

        // Act
        heartbeat.start();

        // Wait up to a reasonable timeout for several renewals to happen.
        // Using a loop with a timeout reduces flakiness on slower or busy runners.
        let deadline = std::time::Instant::now() + mock.ttl() * 5;
        while mock.get_renewal_count() < 3 && std::time::Instant::now() < deadline {
            std::thread::sleep(Duration::from_millis(10));
        }

        heartbeat.stop();

        let renewal_count = mock.get_renewal_count();

        let is_healthy = heartbeat.is_healthy();

        // Assert
        assert!(
            renewal_count >= 3,
            "expected at least 3 renewals within timeout, got {renewal_count}"
        );
        assert!(is_healthy);
    }

    #[test]
    fn should_mark_heartbeat_unhealthy_given_renewal_failure_when_running() {
        // Arrange
        let mock = Arc::new(MockLease::new());
        let mut heartbeat = LeaseHeartbeat::new(mock.clone() as Arc<dyn PrimaryLease>);

        // Act
        heartbeat.start();
        std::thread::sleep(Duration::from_millis(200)); // Let it succeed once

        // Trigger failure
        mock.set_should_fail(true);

        // Wait up to a timeout for unhealthy state to be set.
        let deadline = std::time::Instant::now() + Duration::from_millis(500);
        while heartbeat.is_healthy() && std::time::Instant::now() < deadline {
            std::thread::sleep(Duration::from_millis(10));
        }

        let is_healthy = heartbeat.is_healthy();

        // Assert

        assert!(
            !is_healthy,
            "expected heartbeat to be unhealthy after renewal failure"
        );
        heartbeat.stop();
    }

    #[test]
    fn should_stop_heartbeat_given_renewal_failure_when_running() {
        // Arrange
        let mock = Arc::new(MockLease::new());
        mock.set_should_fail(true);
        let mut heartbeat = LeaseHeartbeat::new(mock as Arc<dyn PrimaryLease>);

        // Act
        heartbeat.start();
        std::thread::sleep(Duration::from_millis(200));
        heartbeat.stop();

        // Assert
        assert!(!heartbeat.is_healthy());
    }

    #[test]
    fn should_remain_single_threaded_given_start_called_twice_when_heartbeat_is_running() {
        // Arrange
        let mock = Arc::new(MockLease::new());
        let mut heartbeat = LeaseHeartbeat::new(mock);

        // Act
        heartbeat.start();
        heartbeat.start();
        heartbeat.stop();

        // Assert
        assert!(heartbeat.renewal_handle.is_none());
        assert!(heartbeat.watchdog_handle.is_none());
    }

    #[test]
    fn should_fence_at_monotonic_expiry_while_renewal_is_blocked() {
        // Arrange
        let lease = Arc::new(BlockingRenewalLease::new(Duration::from_secs(2)));
        let _guard = Arc::clone(&lease).try_acquire().expect("acquire lease");
        let mut heartbeat =
            LeaseHeartbeat::new_with_validity(lease.clone(), Arc::clone(&lease.validity));

        // Act
        heartbeat.start();
        assert!(lease.wait_until_renewal_started(Duration::from_secs(5)));
        let deadline = std::time::Instant::now() + Duration::from_secs(5);
        while heartbeat.is_healthy() && std::time::Instant::now() < deadline {
            std::thread::sleep(Duration::from_millis(5));
        }

        // Assert
        assert!(!heartbeat.is_healthy());
        assert!(matches!(
            lease.validity.snapshot(),
            LeaseValidityState::Fenced { epoch: 1 }
        ));
        lease.allow_renewal();
        assert!(lease.wait_until_renewal_completed(Duration::from_secs(1)));
        assert!(
            !heartbeat.is_healthy(),
            "late renewal must not restore health"
        );
        heartbeat.stop();
    }

    #[test]
    fn should_remain_healthy_past_prior_deadline_when_renewal_advances_in_time() {
        // Arrange
        let ttl = Duration::from_secs(2);
        let lease = Arc::new(AdvancingLease {
            validity: Arc::new(crate::lease::traits::LeaseValidity::new()),
            ttl,
            renewals: AtomicUsize::new(0),
        });
        let _guard = Arc::clone(&lease).try_acquire().expect("acquire lease");
        let prior_deadline = match lease.validity.snapshot() {
            LeaseValidityState::Active { valid_until, .. } => valid_until,
            state => panic!("expected active validity, got {state:?}"),
        };
        let mut heartbeat =
            LeaseHeartbeat::new_with_validity(lease.clone(), Arc::clone(&lease.validity));

        // Act
        heartbeat.start();
        while lease.renewals.load(Ordering::Acquire) == 0
            && std::time::Instant::now() < prior_deadline
        {
            std::thread::sleep(Duration::from_millis(5));
        }
        while std::time::Instant::now() <= prior_deadline + Duration::from_millis(20) {
            std::thread::sleep(Duration::from_millis(5));
        }

        // Assert
        assert!(lease.renewals.load(Ordering::Acquire) > 0);
        assert!(heartbeat.is_healthy());
        assert!(matches!(
            lease.validity.snapshot(),
            LeaseValidityState::Active { valid_until, .. } if valid_until > prior_deadline
        ));
        heartbeat.stop();
    }
}
