//! Mutex-based batched sync mechanism for WAL synchronization
//!
//! Allows multiple concurrent threads to share a single fsync operation,
//! dramatically improving throughput under concurrent write workloads.
//!
//! ## How it works
//! 1. Multiple threads append to WAL (buffered, no fsync)
//! 2. When a thread calls sync(), it checks `in_progress` under a mutex
//! 3. First caller becomes leader, others become followers waiting on condvar
//! 4. Leader performs ONE fsync for all waiting threads (without holding lock)
//! 5. Leader increments epoch after sync completes, waking all followers
//!
//! This can batch 100+ fsync requests into a single syscall, increasing
//! throughput from ~1K writes/sec to >100K writes/sec.
//!
//! ## Coordination
//! - All state is protected by a single `Mutex<State>`
//! - Leader releases lock while performing fsync (avoids blocking new waiters)
//! - Followers wait on condvar until `epoch` changes
//! - No lock-free primitives needed - simpler and more reliable

use crate::error::{MidgeError, MidgeResult};
use crossbeam::channel;
use parking_lot::{Condvar, Mutex};
use std::time::Duration;

/// Configuration for batched synchronization behavior
#[derive(Debug, Clone)]
pub struct BatchedSyncConfig {
    /// Delay to accumulate waiters (microseconds). Set to 0 to disable.
    /// Default: 100µs
    pub wait_micros: u64,
}

impl Default for BatchedSyncConfig {
    fn default() -> Self {
        let wait_micros = std::env::var("SHALE_BATCHED_SYNC_WAIT_US")
            .ok()
            .and_then(|s| s.parse::<u64>().ok())
            .unwrap_or(100);

        Self { wait_micros }
    }
}

/// Internal shared state guarded by a single mutex.
#[derive(Debug)]
struct State {
    /// Monotonically increasing batch epoch; incremented after each completed sync.
    epoch: u64,
    /// True while a leader is currently doing work for this epoch.
    in_progress: bool,
    /// Number of callers that joined the current batch (including leader).
    waiters: u64,
    /// Whether the last completed batch failed (followers only see generic error).
    last_failed: bool,
}

impl State {
    fn new() -> Self {
        Self {
            epoch: 0,
            in_progress: false,
            waiters: 0,
            last_failed: false,
        }
    }
}

/// Batched-sync coordinator implemented with a single mutex + condvar.
///
/// This is much easier to reason about than the lock-free variant:
/// - All coordination state is behind one `Mutex<State>`.
/// - The leader runs `sync_fn` *without* holding the lock.
/// - Followers simply wait for `epoch` to change.
pub struct BatchedSyncCoordinator {
    state: Mutex<State>,
    cv: Condvar,
    config: BatchedSyncConfig,

    // Optional test hooks (used only in tests, but kept here for API compatibility).
    test_leader_ready: Mutex<Option<channel::Sender<()>>>,
    test_leader_continue: Mutex<Option<channel::Receiver<()>>>,
}

impl BatchedSyncCoordinator {
    pub fn new(config: BatchedSyncConfig) -> Self {
        Self {
            state: Mutex::new(State::new()),
            cv: Condvar::new(),
            config,
            test_leader_ready: Mutex::new(None),
            test_leader_continue: Mutex::new(None),
        }
    }

    pub fn set_test_leader_sync(
        &self,
        ready: Option<channel::Sender<()>>,
        continue_rx: Option<channel::Receiver<()>>,
    ) {
        *self.test_leader_ready.lock() = ready;
        *self.test_leader_continue.lock() = continue_rx;
    }

    /// Exposed only for tests that previously touched `epoch` / `in_progress` directly.
    #[cfg(test)]
    fn epoch(&self) -> u64 {
        self.state.lock().epoch
    }

    #[cfg(test)]
    fn in_progress(&self) -> bool {
        self.state.lock().in_progress
    }

    /// Wait for the next sync to complete, batching with other concurrent callers.
    ///
    /// The first caller that finds `in_progress == false` for this epoch becomes the leader
    /// and performs the actual durable sync. All other callers wait until `epoch` advances.
    pub fn wait_for_sync<F>(&self, sync_fn: F) -> MidgeResult<()>
    where
        F: FnOnce() -> MidgeResult<()>,
    {
        // Fast path: try to become leader under the mutex.
        let mut state = self.state.lock();
        let my_epoch = state.epoch;

        if state.in_progress {
            // Already a leader for this epoch -> become follower.
            state.waiters += 1;
            return self.wait_as_follower(state, my_epoch);
        }

        // We are the leader for this epoch.
        state.in_progress = true;
        state.waiters += 1;
        drop(state); // Do not hold the lock while doing fsync.

        // Optional small sleep to accumulate followers into this batch.
        if self.config.wait_micros > 0 {
            std::thread::sleep(Duration::from_micros(self.config.wait_micros));
        }

        // Test hook: signal that leader is ready and optionally wait for test to continue.
        #[cfg(test)]
        {
            if let Some(tx) = self.test_leader_ready.lock().clone() {
                let _ = tx.send(());
            }
            if let Some(rx) = self.test_leader_continue.lock().clone() {
                let _ = rx.recv();
            }
        }

        // Perform the actual durable sync.
        let res = sync_fn();
        let success = res.is_ok();

        // Reacquire lock and complete the batch.
        let mut state = self.state.lock();
        let batch_size = state.waiters;
        state.waiters = 0;
        state.in_progress = false;
        state.last_failed = !success;
        state.epoch = state.epoch.wrapping_add(1);

        // Record batch metrics while still holding the state (for consistency).
        if batch_size > 0 {
            #[allow(unused_imports)]
            {
                crate::metrics::global_performance_metrics()
                    .wal
                    .record_batched_sync(batch_size);
            }
        }

        // Wake up all followers waiting on the previous epoch.
        self.cv.notify_all();
        drop(state);

        // Leader returns the original result.
        res
    }

    fn wait_as_follower(
        &self,
        mut state: parking_lot::MutexGuard<'_, State>,
        my_epoch: u64,
    ) -> MidgeResult<()> {
        // Wait until epoch changes. We never block if the leader already completed.
        self.cv.wait_while(&mut state, |s| s.epoch == my_epoch);

        // Now we're observing a completed batch.
        if state.last_failed {
            Err(MidgeError::internal("batched-sync leader fsync failed"))
        } else {
            Ok(())
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::sync::atomic::{AtomicUsize, Ordering};
    use std::sync::Arc;

    #[test]
    fn should_create_coordinator_with_default_config() {
        // Arrange
        let config = BatchedSyncConfig::default();

        // Act
        let coordinator = BatchedSyncCoordinator::new(config);

        // Assert
        assert!(!coordinator.in_progress());
        assert_eq!(coordinator.epoch(), 0);
    }

    #[test]
    fn should_batch_multiple_sync_requests() {
        // Arrange
        // Use a small wait to allow followers to accumulate
        let coordinator = Arc::new(BatchedSyncCoordinator::new(BatchedSyncConfig {
            wait_micros: 100,
        }));
        let sync_count = Arc::new(AtomicUsize::new(0));
        let barrier = Arc::new(std::sync::Barrier::new(10));

        // Act - spawn multiple threads requesting sync concurrently
        let mut handles = vec![];
        for _ in 0..10 {
            let coord = coordinator.clone();
            let count = sync_count.clone();
            let bar = barrier.clone();
            let handle = std::thread::spawn(move || {
                bar.wait(); // Ensure all threads start simultaneously
                coord
                    .wait_for_sync(|| {
                        count.fetch_add(1, Ordering::SeqCst);
                        Ok(())
                    })
                    .unwrap();
            });
            handles.push(handle);
        }

        for handle in handles {
            handle.join().unwrap();
        }

        // Assert - sync should be called fewer times than threads (batching)
        let actual_syncs = sync_count.load(Ordering::SeqCst);
        assert!(
            actual_syncs < 10,
            "Expected batching, got {} syncs for 10 threads",
            actual_syncs
        );
    }

    #[test]
    fn should_handle_single_sync_request() {
        // Arrange
        let coordinator = BatchedSyncCoordinator::new(BatchedSyncConfig::default());
        let mut sync_called = false;

        // Act
        let result = coordinator.wait_for_sync(|| {
            sync_called = true;
            Ok(())
        });

        // Assert
        assert!(result.is_ok());
        assert!(sync_called);
    }

    #[test]
    fn should_propagate_sync_errors_to_leader() {
        // Arrange
        let coordinator = BatchedSyncCoordinator::new(BatchedSyncConfig::default());

        // Act
        let result =
            coordinator.wait_for_sync(|| Err(crate::error::MidgeError::internal("sync failed")));

        // Assert
        assert!(result.is_err());
    }

    #[test]
    fn should_complete_followers_when_leader_succeeds() {
        // Arrange
        // Use fast config to avoid timing-related stalls in CI
        let coordinator = Arc::new(BatchedSyncCoordinator::new(BatchedSyncConfig {
            wait_micros: 0,
        }));
        let barrier = Arc::new(std::sync::Barrier::new(5));

        // Act - spawn multiple threads, all should succeed
        let mut handles = vec![];
        for _ in 0..5 {
            let coord = coordinator.clone();
            let bar = barrier.clone();
            let handle = std::thread::spawn(move || {
                bar.wait();
                coord.wait_for_sync(|| Ok(()))
            });
            handles.push(handle);
        }

        let results: Vec<_> = handles.into_iter().map(|h| h.join().unwrap()).collect();

        // Assert - all threads should succeed
        assert!(results.iter().all(|r| r.is_ok()));
    }

    #[test]
    fn should_propagate_error_given_leader_fails_when_followers_wait() {
        // Arrange
        let coordinator = Arc::new(BatchedSyncCoordinator::new(BatchedSyncConfig {
            wait_micros: 10,
        }));
        let barrier = Arc::new(std::sync::Barrier::new(6));

        // Act
        let mut handles = vec![];
        for _ in 0..6 {
            let coord = coordinator.clone();
            let bar = barrier.clone();
            let handle = std::thread::spawn(move || {
                bar.wait();
                // The leader will run this closure and return Err; followers will
                // observe the leader's error via result flag and return an Internal error.
                coord.wait_for_sync(|| Err(crate::error::MidgeError::internal("leader_err")))
            });
            handles.push(handle);
        }

        let results: Vec<_> = handles.into_iter().map(|h| h.join().unwrap()).collect();

        // Assert - all should fail (leader sees original error, followers see generic error)
        for res in results {
            assert!(res.is_err());
        }
    }

    #[test]
    fn should_clear_error_after_failed_batch_when_new_batch_starts() {
        // Arrange
        let coordinator = Arc::new(BatchedSyncCoordinator::new(BatchedSyncConfig {
            wait_micros: 10,
        }));
        let barrier = Arc::new(std::sync::Barrier::new(4));

        // Act - produce an error from the leader while several followers wait
        let mut handles = vec![];
        for _ in 0..4 {
            let coord = coordinator.clone();
            let bar = barrier.clone();
            let handle = std::thread::spawn(move || {
                bar.wait();
                coord.wait_for_sync(|| Err(crate::error::MidgeError::internal("boom")))
            });
            handles.push(handle);
        }

        for h in handles {
            let _ = h.join().unwrap();
        }

        // Assert - after batch completes, a new sync should work fine
        let next_result = coordinator.wait_for_sync(|| Ok(()));
        assert!(next_result.is_ok());
    }

    #[test]
    fn should_increment_batch_id_given_new_leader_starts_when_previous_finished() {
        // Arrange
        let coordinator = BatchedSyncCoordinator::new(BatchedSyncConfig { wait_micros: 1 });

        // Act - perform two sequential syncs and observe the epoch monotonicity
        coordinator.wait_for_sync(|| Ok(())).unwrap();
        let first_epoch = coordinator.epoch();

        coordinator.wait_for_sync(|| Ok(())).unwrap();
        let second_epoch = coordinator.epoch();

        // Assert
        assert!(
            second_epoch > first_epoch,
            "epoch should increase after a new leader starts"
        );
    }

    #[test]
    fn should_allow_back_to_back_syncs_given_multiple_rounds_when_reused() {
        // Arrange
        let coordinator = Arc::new(BatchedSyncCoordinator::new(BatchedSyncConfig {
            wait_micros: 0,
        }));

        // Act - run several back-to-back rounds of concurrent syncs
        for _round in 0..5 {
            let barrier = Arc::new(std::sync::Barrier::new(4));
            let mut handles = vec![];

            for _ in 0..4 {
                let coord = coordinator.clone();
                let bar = barrier.clone();
                let handle = std::thread::spawn(move || {
                    bar.wait();
                    coord.wait_for_sync(|| Ok(()))
                });
                handles.push(handle);
            }

            for h in handles {
                let res = h.join().unwrap();
                assert!(res.is_ok());
            }
        }

        // Assert - coordinator survived repeated reuse; final state is sane
        assert!(!coordinator.in_progress());
    }

    #[test]
    fn should_handle_heavy_concurrency_given_many_threads_when_batched_repeatedly() {
        // Arrange
        let coordinator = Arc::new(BatchedSyncCoordinator::new(BatchedSyncConfig {
            wait_micros: 5,
        }));
        let sync_count = Arc::new(AtomicUsize::new(0));
        let threads = 50usize;
        let rounds = 20usize;

        // Act - spawn many threads that repeatedly call wait_for_sync
        let mut handles = vec![];
        for _ in 0..threads {
            let coord = coordinator.clone();
            let count = sync_count.clone();
            let handle = std::thread::spawn(move || {
                for _ in 0..rounds {
                    coord
                        .wait_for_sync(|| {
                            count.fetch_add(1, Ordering::SeqCst);
                            Ok(())
                        })
                        .unwrap();
                }
            });
            handles.push(handle);
        }

        for h in handles {
            h.join().unwrap();
        }

        // Assert - ensure we had fewer actual sync invocations than callers (batching did something)
        let actual_syncs = sync_count.load(Ordering::SeqCst);
        let total_calls = threads * rounds;
        assert!(actual_syncs <= total_calls);
        assert!(
            actual_syncs < total_calls,
            "expected some batching to occur"
        );
    }
}
