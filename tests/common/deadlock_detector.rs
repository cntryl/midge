//! Deadlock Detection Utility for Integration Tests
//!
//! Provides utilities to detect potential deadlocks and hanging tests at runtime.
//!
//! # Usage
//!
//! ```rust
//! use common::deadlock_detector::DeadlockDetector;
//! use std::time::Duration;
//!
//! #[test]
//! fn my_concurrent_test() {
//!     let _detector = DeadlockDetector::new("my_concurrent_test", Duration::from_secs(10));
//!     
//!     // Your test code here...
//!     // If test takes >10s, detector will print warning
//! }
//! ```

use std::sync::atomic::{AtomicBool, Ordering};
use std::sync::Arc;
use std::time::{Duration, Instant};

/// Deadlock detector that monitors test execution time
pub struct DeadlockDetector {
    test_name: String,
    start: Instant,
    timeout: Duration,
    shutdown: Arc<AtomicBool>,
    handle: Option<std::thread::JoinHandle<()>>,
}

impl DeadlockDetector {
    /// Create a new deadlock detector for a test
    ///
    /// # Arguments
    /// * `test_name` - Name of the test being monitored
    /// * `timeout` - Maximum allowed duration before warning
    pub fn new(test_name: impl Into<String>, timeout: Duration) -> Self {
        let test_name = test_name.into();
        let start = Instant::now();
        let shutdown = Arc::new(AtomicBool::new(false));

        let test_name_clone = test_name.clone();
        let shutdown_clone = shutdown.clone();

        let handle = std::thread::spawn(move || {
            std::thread::sleep(timeout);

            if !shutdown_clone.load(Ordering::Relaxed) {
                eprintln!("\n⚠️  DEADLOCK WARNING ⚠️");
                eprintln!(
                    "Test '{}' has been running for >{:?}",
                    test_name_clone, timeout
                );
                eprintln!("Possible deadlock or infinite loop detected!");
                eprintln!("\nCommon causes:");
                eprintln!("  • Condvar.wait() without proper condition check");
                eprintln!("  • Missing notify_all() after state change");
                eprintln!("  • Spin loop without parking fallback");
                eprintln!("  • Lock acquired twice in same thread");
                eprintln!("\nSee docs/DEADLOCK_DETECTION.md for debugging tips\n");
            }
        });

        Self {
            test_name,
            start,
            timeout,
            shutdown,
            handle: Some(handle),
        }
    }

    /// Create with a default 30-second timeout
    pub fn with_default_timeout(test_name: impl Into<String>) -> Self {
        Self::new(test_name, Duration::from_secs(30))
    }

    /// Check if test has exceeded timeout (non-blocking)
    pub fn check_timeout(&self) -> bool {
        self.start.elapsed() > self.timeout
    }

    /// Get elapsed time since test started
    pub fn elapsed(&self) -> Duration {
        self.start.elapsed()
    }
}

impl Drop for DeadlockDetector {
    fn drop(&mut self) {
        // Signal watchdog to shut down
        self.shutdown.store(true, Ordering::Relaxed);

        // Wait for watchdog thread to finish
        if let Some(handle) = self.handle.take() {
            let _ = handle.join();
        }

        // Log if test took suspiciously long (but didn't timeout)
        let elapsed = self.elapsed();
        if elapsed > self.timeout * 3 / 4 {
            eprintln!(
                "⚠️  Test '{}' took {:?} (warning threshold: {:?})",
                self.test_name, elapsed, self.timeout
            );
        }
    }
}

/// Run a closure with deadlock detection
///
/// # Example
/// ```
/// with_deadlock_detection("my_test", Duration::from_secs(5), || {
///     // test code
/// });
/// ```
pub fn with_deadlock_detection<F, R>(test_name: &str, timeout: Duration, f: F) -> R
where
    F: FnOnce() -> R,
{
    let _detector = DeadlockDetector::new(test_name, timeout);
    f()
}

/// Assert that a closure completes within a timeout
///
/// # Panics
/// Panics if the operation takes longer than the timeout
///
/// # Example
/// ```
/// assert_completes_within(Duration::from_secs(1), || {
///     expensive_operation();
/// });
/// ```
pub fn assert_completes_within<F, R>(timeout: Duration, f: F) -> R
where
    F: FnOnce() -> R + Send + 'static,
    R: Send + 'static,
{
    use std::sync::mpsc;

    let (tx, rx) = mpsc::channel();

    let handle = std::thread::spawn(move || {
        let result = f();
        let _ = tx.send(result);
    });

    match rx.recv_timeout(timeout) {
        Ok(result) => {
            let _ = handle.join();
            result
        }
        Err(_) => {
            panic!(
                "Operation did not complete within {:?} - possible deadlock or hang",
                timeout
            );
        }
    }
}

/// Stress test coordinator that runs multiple concurrent operations
/// and detects deadlocks
pub struct StressTestCoordinator {
    barrier: Arc<std::sync::Barrier>,
    detector: DeadlockDetector,
}

impl StressTestCoordinator {
    /// Create a new stress test coordinator
    ///
    /// # Arguments
    /// * `test_name` - Name of the stress test
    /// * `num_threads` - Number of concurrent threads
    /// * `timeout` - Maximum allowed duration
    pub fn new(test_name: impl Into<String>, num_threads: usize, timeout: Duration) -> Self {
        Self {
            barrier: Arc::new(std::sync::Barrier::new(num_threads)),
            detector: DeadlockDetector::new(test_name, timeout),
        }
    }

    /// Run a closure concurrently on multiple threads
    pub fn run_concurrent<F>(&self, num_threads: usize, f: F)
    where
        F: Fn() + Send + Sync + 'static,
    {
        let f = Arc::new(f);
        let mut handles = Vec::new();

        for _ in 0..num_threads {
            let barrier = self.barrier.clone();
            let f = f.clone();

            let handle = std::thread::spawn(move || {
                barrier.wait(); // All threads start simultaneously
                f();
            });

            handles.push(handle);
        }

        for handle in handles {
            handle.join().expect("Thread panicked");
        }
    }

    /// Get elapsed time
    pub fn elapsed(&self) -> Duration {
        self.detector.elapsed()
    }
}
