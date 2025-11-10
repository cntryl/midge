//! Common utilities for lock renewal and lifecycle management.

use parking_lot::Mutex;
use std::sync::Arc;
use std::thread::{self, JoinHandle};
use std::time::Duration;

/// Shared renewal thread infrastructure for both local and cloud locks.
///
/// Handles:
/// - Background thread spawning
/// - Stop signal coordination
/// - Automatic TTL/2 renewal interval
pub(super) struct RenewalThread {
    /// Renewal thread handle
    handle: Option<JoinHandle<()>>,
    /// Signal to stop renewal thread
    stop_signal: Arc<Mutex<bool>>,
}

impl RenewalThread {
    /// Create a new renewal thread infrastructure (not yet started).
    pub(super) fn new() -> Self {
        Self {
            handle: None,
            stop_signal: Arc::new(Mutex::new(false)),
        }
    }

    /// Start the renewal thread with the given interval and renewal callback.
    ///
    /// The callback will be invoked every `renewal_interval` until stop is signaled.
    pub(super) fn start<F>(&mut self, renewal_interval: Duration, mut renewal_fn: F)
    where
        F: FnMut() + Send + 'static,
    {
        let stop_signal = Arc::clone(&self.stop_signal);

        let handle = thread::spawn(move || {
            loop {
                // Check stop signal
                {
                    let stop = stop_signal.lock();
                    if *stop {
                        break;
                    }
                }

                // Sleep for renewal interval
                thread::sleep(renewal_interval);

                // Execute renewal callback
                renewal_fn();
            }
        });

        self.handle = Some(handle);
    }

    /// Signal the renewal thread to stop and wait for it to finish.
    pub(super) fn stop(&mut self) {
        // Signal stop
        {
            let mut stop = self.stop_signal.lock();
            *stop = true;
        }

        // Wait for thread to finish
        if let Some(handle) = self.handle.take() {
            let _ = handle.join(); // Best effort
        }
    }
}

impl Drop for RenewalThread {
    fn drop(&mut self) {
        self.stop();
    }
}

/// Helper to compute renewal interval from TTL (TTL / 2).
pub(super) fn renewal_interval_from_ttl(ttl_ms: u32) -> Duration {
    Duration::from_millis((ttl_ms as u64) / 2)
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::sync::atomic::{AtomicUsize, Ordering};

    #[test]
    fn should_execute_renewal_callback_periodically() {
        // Arrange
        let counter = Arc::new(AtomicUsize::new(0));
        let counter_clone = Arc::clone(&counter);
        let mut renewal = RenewalThread::new();

        // Act
        renewal.start(Duration::from_millis(50), move || {
            counter_clone.fetch_add(1, Ordering::SeqCst);
        });

        std::thread::sleep(Duration::from_millis(200));

        // Assert
        let count = counter.load(Ordering::SeqCst);
        assert!((2..=5).contains(&count), "Expected 2-5 renewals, got {}", count);

        // Cleanup
        renewal.stop();
    }

    #[test]
    fn should_stop_renewal_thread_when_signaled() {
        // Arrange
        let counter = Arc::new(AtomicUsize::new(0));
        let counter_clone = Arc::clone(&counter);
        let mut renewal = RenewalThread::new();

        // Act
        renewal.start(Duration::from_millis(50), move || {
            counter_clone.fetch_add(1, Ordering::SeqCst);
        });

        std::thread::sleep(Duration::from_millis(100));
        let count_before_stop = counter.load(Ordering::SeqCst);
        
        renewal.stop();
        std::thread::sleep(Duration::from_millis(150));
        let count_after_stop = counter.load(Ordering::SeqCst);

        // Assert
        assert!(count_before_stop > 0, "Should have renewed before stop");
        // Allow at most one additional renewal due to race condition
        // (thread might execute once more before checking stop signal)
        assert!(
            count_after_stop <= count_before_stop + 1,
            "Should stop renewal quickly after signal (before: {}, after: {})",
            count_before_stop,
            count_after_stop
        );
    }

    #[test]
    fn should_compute_renewal_interval_as_half_ttl() {
        // Arrange & Act
        let interval = renewal_interval_from_ttl(10000);

        // Assert
        assert_eq!(interval, Duration::from_millis(5000));
    }
}
