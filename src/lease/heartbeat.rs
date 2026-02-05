//! Lease heartbeat and monitoring.
//!
//! Manages periodic lease renewal in a background thread.
//! Monitors lease health and triggers shutdown if renewal fails.

use super::traits::PrimaryLease;
use std::sync::atomic::{AtomicBool, Ordering};
use std::sync::Arc;
use std::thread::{self, JoinHandle};

/// Heartbeat manager for lease renewal.
///
/// Runs a background thread that periodically renews the lease.
/// If renewal fails, sets a flag that the engine can monitor.
pub struct LeaseHeartbeat {
    lease: Arc<dyn PrimaryLease>,
    running: Arc<AtomicBool>,
    healthy: Arc<AtomicBool>,
    thread_handle: Option<JoinHandle<()>>,
}

impl LeaseHeartbeat {
    /// Create a new heartbeat manager.
    ///
    /// Does not start the heartbeat loop automatically.
    /// Call `start()` to begin renewal.
    pub fn new(lease: Arc<dyn PrimaryLease>) -> Self {
        Self {
            lease,
            running: Arc::new(AtomicBool::new(false)),
            healthy: Arc::new(AtomicBool::new(true)),
            thread_handle: None,
        }
    }

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

        running.store(true, Ordering::Release);
        healthy.store(true, Ordering::Release);

        let ttl = lease.ttl();
        let renewal_interval = ttl / 3; // Renew 3x per TTL period

        let handle = thread::Builder::new()
            .name("midge-lease-heartbeat".to_string())
            .spawn(move || {
                tracing::info!(
                    ttl_secs = ttl.as_secs(),
                    renewal_interval_secs = renewal_interval.as_secs(),
                    "lease heartbeat started"
                );

                while running.load(Ordering::Acquire) {
                    // Use park_timeout so `stop()` can wake us immediately via `unpark()`.
                    // `sleep()` would otherwise delay shutdown by up to `renewal_interval`.
                    thread::park_timeout(renewal_interval);

                    if !running.load(Ordering::Acquire) {
                        break;
                    }

                    match lease.renew() {
                        Ok(()) => {
                            tracing::trace!("lease renewed successfully");
                        }
                        Err(e) => {
                            tracing::error!(
                                error = %e,
                                "lease renewal failed; marking unhealthy"
                            );
                            healthy.store(false, Ordering::Release);

                            // Stop heartbeat on renewal failure
                            // Engine should monitor `is_healthy()` and shut down
                            break;
                        }
                    }
                }

                tracing::info!("lease heartbeat stopped");
            })
            .expect("failed to spawn lease heartbeat thread");

        self.thread_handle = Some(handle);
    }

    /// Stop the heartbeat loop.
    ///
    /// Waits for the background thread to exit.
    pub fn stop(&mut self) {
        if !self.running.load(Ordering::Acquire) {
            return;
        }

        tracing::info!("stopping lease heartbeat");
        self.running.store(false, Ordering::Release);

        if let Some(handle) = self.thread_handle.take() {
            // Wake the parked heartbeat thread so shutdown doesn't block.
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

    /// Get the lease holder ID.
    pub fn holder_id(&self) -> String {
        self.lease.holder_id()
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
        fn try_acquire(&self) -> Result<LeaseGuard, LeaseError> {
            Ok(LeaseGuard::new(|| {}))
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
            "expected at least 3 renewals within timeout, got {}",
            renewal_count
        );
        assert!(is_healthy);
    }

    #[test]
    fn should_mark_unhealthy_when_renewal_fails() {
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
}
