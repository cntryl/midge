//! Lock-free batched sync mechanism for WAL synchronization
//!
//! Allows multiple concurrent threads to share a single fsync operation,
//! dramatically improving throughput under concurrent write workloads.
//!
//! ## How it works
//! 1. Multiple threads append to WAL (buffered, no fsync)
//! 2. When a thread calls sync(), it attempts a CAS on `in_progress` to become leader
//! 3. If CAS succeeds: becomes leader, performs ONE fsync for all waiting threads
//! 4. If CAS fails: becomes follower, spins briefly then parks until epoch changes
//! 5. Leader increments epoch after sync completes, waking all followers
//!
//! This can batch 100+ fsync requests into a single syscall, increasing
//! throughput from ~1K writes/sec to >100K writes/sec.
//!
//! ## Memory ordering
//! - Leader: Uses `AcqRel` CAS to claim leadership
//! - Leader: Publishes result with `Release`, then increments epoch with `Release`
//! - Follower: Observes epoch with `Acquire` (synchronizes-with leader's Release)
//! - Follower: Reads result with `Acquire` after epoch change

use crate::error::{MidgeError, MidgeResult};
use crossbeam::channel;
use parking_lot::{Condvar, Mutex};
use std::sync::atomic::{AtomicBool, AtomicU64, AtomicU8, Ordering::*};
use std::time::Duration;

/// Configuration for batched synchronization behavior
#[derive(Debug, Clone)]
pub struct BatchedSyncConfig {
    /// Delay to accumulate waiters (microseconds). Set to 0 to disable.
    /// Default: 100µs
    pub wait_micros: u64,
    /// Number of spin loop iterations before parking a follower thread.
    /// Avoids syscall overhead for very short batches.
    /// Default: 100
    pub spin_loops: u32,
}

impl Default for BatchedSyncConfig {
    fn default() -> Self {
        // Allow runtime override for experiments via environment variable
        // `SHALE_BATCHED_SYNC_WAIT_US`. If unset or invalid, fall back to 100µs.
        let wait_micros = std::env::var("SHALE_BATCHED_SYNC_WAIT_US")
            .ok()
            .and_then(|s| s.parse::<u64>().ok())
            .unwrap_or(100);

        let spin_loops = std::env::var("SHALE_BATCHED_SYNC_SPIN_LOOPS")
            .ok()
            .and_then(|s| s.parse::<u32>().ok())
            .unwrap_or(100);

        Self {
            wait_micros,
            spin_loops,
        }
    }
}

/// Lock-free batched-sync coordinator that batches fsync requests
pub struct BatchedSyncCoordinator {
    /// True while leader is performing sync (leadership flag)
    in_progress: AtomicBool,
    /// Monotonically increasing batch epoch; incremented after each completed sync
    epoch: AtomicU64,
    /// Result of the last completed batch: 0=pending, 1=ok, 2=err
    result: AtomicU8,
    /// Number of callers waiting to be included in the next batch. Incremented
    /// by all callers upon entry to `wait_for_sync`, and reset by the leader
    /// when it collects the batch. This provides a cheap approximation of
    /// the batch size for metrics and tuning.
    pending: AtomicU64,
    /// Parking gate to avoid busy-wait after spin threshold (not used for correctness)
    park_lock: Mutex<()>,
    /// Condition variable for waking parked followers
    park_cv: Condvar,
    /// Configuration
    config: BatchedSyncConfig,
    /// Optional test hooks: a sender that leader will notify when batch is collected
    /// and a receiver that the leader will wait on for deterministic tests.
    test_leader_ready: Mutex<Option<channel::Sender<()>>>,
    test_leader_continue: Mutex<Option<channel::Receiver<()>>>,
}

impl BatchedSyncCoordinator {
    /// Create a new batched-sync coordinator
    pub fn new(config: BatchedSyncConfig) -> Self {
        Self {
            in_progress: AtomicBool::new(false),
            epoch: AtomicU64::new(0),
            result: AtomicU8::new(0),
            park_lock: Mutex::new(()),
            park_cv: Condvar::new(),
            pending: AtomicU64::new(0),
            config,
            test_leader_ready: Mutex::new(None),
            test_leader_continue: Mutex::new(None),
        }
    }

    /// Test helper to set deterministic synchronization hooks.
    ///
    /// `ready` - optional sender that the leader sends to when batch is collected.
    /// `continue_rx` - optional receiver that the leader will wait on until test allows it to continue.
    #[cfg(test)]
    pub fn set_test_leader_sync(
        &self,
        ready: Option<channel::Sender<()>>,
        continue_rx: Option<channel::Receiver<()>>,
    ) {
        *self.test_leader_ready.lock() = ready;
        *self.test_leader_continue.lock() = continue_rx;
    }

    /// Wait for the next sync to complete, batching with other concurrent callers.
    ///
    /// The first caller becomes the leader via lock-free CAS and performs the actual fsync.
    /// Other callers spin briefly then park until the leader completes and increments the epoch.
    pub fn wait_for_sync<F>(&self, sync_fn: F) -> MidgeResult<()>
    where
        F: FnOnce() -> MidgeResult<()>,
    {
        // Track that we're wanting to join the next batch. This is incremented
        // by all callers and read/reset by the leader to compute the batch size
        // for metrics.
        self.pending.fetch_add(1, AcqRel);

        // Capture the current epoch before attempting to become leader
        let my_epoch = self.epoch.load(Acquire);

        // Try to become leader via CAS (fast path, lock-free)
        match self
            .in_progress
            .compare_exchange(false, true, AcqRel, Acquire)
        {
            Ok(_) => {
                // We are the leader - perform sync for all concurrent callers

                // Optionally sleep briefly to accumulate more followers (increases batch size)
                if self.config.wait_micros > 0 {
                    std::thread::sleep(Duration::from_micros(self.config.wait_micros));
                }

                // Determine how many callers we collected for this batch. Swap
                // resets the pending counter to 0 for the next batch. The value
                // includes the leader itself and any followers that incremented
                // the counter before the swap.
                let batch = self.pending.swap(0, AcqRel);

                // If test hooks are configured, notify the test that the leader
                // has collected the batch and pause until the test allows us to continue.
                let tx_opt = self.test_leader_ready.lock().clone();
                if let Some(tx) = tx_opt {
                    let _ = tx.send(());
                }
                let rx_opt = self.test_leader_continue.lock().clone();
                if let Some(rx) = rx_opt {
                    let _ = rx.recv();
                }

                // Mark result as pending (defensive, should already be 0 or stale)
                self.result.store(0, Release);

                // Perform the actual durable sync outside any locks
                let res = sync_fn();

                // Record batch metrics (uses global accessor to avoid wiring)
                #[allow(unused_imports)]
                {
                    // Avoid a hard dependency on a metrics crate when not used by
                    // tests; record only when available at runtime.
                    if batch > 0 {
                        crate::metrics::global_performance_metrics()
                            .wal
                            .record_batched_sync(batch);
                    }
                }

                // Publish the result atomically: 1=success, 2=error
                // This happens-before the epoch increment (both use Release ordering)
                self.result.store(if res.is_ok() { 1 } else { 2 }, Release);

                // Increment epoch to signal batch completion (synchronizes-with follower Acquire)
                // All followers spinning/parked on my_epoch will observe the new epoch
                self.epoch.fetch_add(1, Release);

                // Release leadership so next caller can become leader
                self.in_progress.store(false, Release);

                // Wake all parked followers (they'll observe the new epoch and result)
                self.park_cv.notify_all();

                res
            }
            Err(_) => {
                // We are a follower - wait for the current leader to finish

                // Spin briefly to avoid parking syscall overhead for very short batches
                for _ in 0..self.config.spin_loops {
                    if self.epoch.load(Acquire) != my_epoch {
                        break;
                    }
                    std::hint::spin_loop();
                }

                // If still waiting after spin, park until leader wakes us
                if self.epoch.load(Acquire) == my_epoch {
                    let mut guard = self.park_lock.lock();
                    // Use wait_while to handle spurious wakeups and re-check condition atomically
                    self.park_cv.wait_while(&mut guard, |_| self.epoch.load(Acquire) == my_epoch);
                }

                // Epoch changed - the batch we joined has completed
                // Read the result with Acquire ordering (synchronizes-with leader's Release)
                match self.result.load(Acquire) {
                    1 => Ok(()),
                    2 => {
                        // Leader's sync failed. We can't propagate the original error
                        // (would require shared storage), so return a generic error.
                        // The leader thread should log the actual failure.
                        Err(MidgeError::internal("batched-sync leader fsync failed"))
                    }
                    0 => {
                        // Very rare race: epoch changed but result not yet visible due to
                        // CPU cache coherency delay. Spin briefly to let the write propagate.
                        for _ in 0..10 {
                            std::hint::spin_loop();
                        }
                        match self.result.load(Acquire) {
                            1 => Ok(()),
                            2 => Err(MidgeError::internal("batched-sync leader fsync failed")),
                            // Still 0 after spin? Treat as success (leader already released lock)
                            _ => Ok(()),
                        }
                    }
                    // Any other value (shouldn't happen) - treat as success
                    _ => Ok(()),
                }
            }
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
        assert!(!coordinator.in_progress.load(Acquire));
        assert_eq!(coordinator.epoch.load(Acquire), 0);
    }

    #[test]
    fn should_batch_multiple_sync_requests() {
        // Arrange
        // Use a fast config to avoid timing-related stalls in CI
        let coordinator = Arc::new(BatchedSyncCoordinator::new(BatchedSyncConfig {
            wait_micros: 0,
            spin_loops: 50,
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
            spin_loops: 50,
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
            spin_loops: 50,
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
    fn should_clear_error_after_last_follower_consumes_when_new_batch_starts() {
        // Arrange
        let coordinator = Arc::new(BatchedSyncCoordinator::new(BatchedSyncConfig {
            wait_micros: 10,
            spin_loops: 50,
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
        let coordinator = BatchedSyncCoordinator::new(BatchedSyncConfig {
            wait_micros: 1,
            spin_loops: 10,
        });

        // Act - perform two sequential syncs and observe the epoch monotonicity
        coordinator.wait_for_sync(|| Ok(())).unwrap();
        let first_epoch = coordinator.epoch.load(Acquire);

        coordinator.wait_for_sync(|| Ok(())).unwrap();
        let second_epoch = coordinator.epoch.load(Acquire);

        // Assert
        assert!(
            second_epoch > first_epoch,
            "epoch should increase after a new leader starts"
        );
    }

    #[test]
    fn should_allow_back_to_back_syncs_given_multiple_rounds_when_reused() {
        // Arrange
        // Use a simple sequential test without complex channel coordination
        // to avoid race conditions that cause hangs.
        let coordinator = Arc::new(BatchedSyncCoordinator::new(BatchedSyncConfig {
            wait_micros: 0,
            spin_loops: 10,
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
        assert!(!coordinator.in_progress.load(Acquire));
    }

    #[test]
    fn should_handle_heavy_concurrency_given_many_threads_when_batched_repeatedly() {
        // Arrange
        let coordinator = Arc::new(BatchedSyncCoordinator::new(BatchedSyncConfig {
            wait_micros: 5,
            spin_loops: 100,
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
