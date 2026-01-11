use parking_lot::Mutex;
use std::collections::HashMap;
use std::hash::Hash;

/// A keyed group-commit accumulator.
///
/// This is the same core idea as `Accumulator`, but instead of returning
/// per-submitter channels, it tracks *waiter payloads* grouped by a key.
///
/// Intended use:
/// - Call `join(waiter)` to register a waiter under the current key.
/// - When the current key is sealed (e.g. WAL segment rotation), call `rotate_to(new_key)`
///   to move all pending waiters for the sealed key into an inflight bucket.
/// - When the keyed work completes (e.g. CloudAck for that WAL segment), call
///   `complete(key)` to drain the waiters and notify them externally.
pub struct KeyedGroupCommit<K, W> {
    state: Mutex<KeyedState<K, W>>,
}

struct KeyedState<K, W> {
    current_key: K,
    pending: Vec<W>,
    inflight: HashMap<K, Vec<W>>,
}

impl<K, W> KeyedGroupCommit<K, W>
where
    K: Eq + Hash + Clone,
{
    pub fn new(current_key: K) -> Self {
        Self {
            state: Mutex::new(KeyedState {
                current_key,
                pending: Vec::new(),
                inflight: HashMap::new(),
            }),
        }
    }

    /// Join the current generation for the current key.
    pub fn join(&self, waiter: W) {
        let mut state = self.state.lock();
        state.pending.push(waiter);
    }

    /// Seal the current key and begin a new one.
    ///
    /// Returns the sealed key + number of waiters moved to inflight, if any.
    pub fn rotate_to(&self, new_key: K) -> Option<(K, usize)> {
        let mut state = self.state.lock();

        let old_key = state.current_key.clone();
        state.current_key = new_key;

        if state.pending.is_empty() {
            return None;
        }

        let waiters = std::mem::take(&mut state.pending);
        let moved = waiters.len();
        state.inflight.insert(old_key.clone(), waiters);
        Some((old_key, moved))
    }

    /// Drain all waiters for the given key.
    pub fn complete(&self, key: &K) -> Vec<W> {
        let mut state = self.state.lock();
        state.inflight.remove(key).unwrap_or_default()
    }

    /// Drain all pending + inflight waiters.
    pub fn drain_all(&self) -> Vec<W> {
        let mut state = self.state.lock();
        let mut out = Vec::new();

        out.append(&mut state.pending);
        for (_, mut ws) in state.inflight.drain() {
            out.append(&mut ws);
        }
        out
    }

    pub fn pending_len(&self) -> usize {
        let state = self.state.lock();
        state.pending.len()
    }

    pub fn inflight_len(&self) -> usize {
        let state = self.state.lock();
        state.inflight.len()
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::sync::atomic::{AtomicUsize, Ordering};
    use std::sync::Arc;

    #[test]
    fn should_fan_out_one_flush_result_to_many_waiters() {
        // Arrange
        let acc: Arc<Accumulator<u64, u64>> = Arc::new(Accumulator::new());

        let threads = 8;
        let per_thread = 50;
        let total = threads * per_thread;

        // Act: submitters enqueue work without blocking.
        let (rx_tx, rx_rx) = crossbeam::channel::unbounded::<crossbeam::channel::Receiver<u64>>();
        let mut join = Vec::new();
        for t in 0..threads {
            let acc = acc.clone();
            let rx_tx = rx_tx.clone();
            join.push(std::thread::spawn(move || {
                for i in 0..per_thread {
                    let rx = acc.submit_async((t * 1_000 + i) as u64);
                    rx_tx.send(rx).expect("send receiver");
                }
            }));
        }
        drop(rx_tx);

        // Collect all receivers before flushing.
        let receivers: Vec<_> = rx_rx.iter().take(total).collect();
        assert_eq!(receivers.len(), total);

        let ran = acc.flush_now(|batch| batch.len() as u64);

        for r in receivers {
            assert_eq!(r.recv().unwrap(), total as u64);
        }

        for j in join {
            j.join().expect("thread panicked");
        }

        // Assert
        assert!(ran);
        assert_eq!(acc.pending_len(), 0);
    }

    #[test]
    fn should_not_flush_when_policy_not_triggered() {
        // Arrange
        let acc: Accumulator<u32, u32> = Accumulator::new();
        let policy = LenFlushPolicy::new(3);

        let _rx1 = acc.submit_async(1);
        let _rx2 = acc.submit_async(2);

        // Act
        let ran = acc.flush_if_needed(&policy, |batch| batch.len() as u32);

        // Assert
        assert!(!ran);
        assert_eq!(acc.pending_len(), 2);
    }

    #[test]
    fn should_flush_when_policy_triggers() {
        // Arrange
        let acc: Accumulator<u32, u32> = Accumulator::new();
        let policy = LenFlushPolicy::new(3);

        let rx1 = acc.submit_async(1);
        let rx2 = acc.submit_async(2);
        let rx3 = acc.submit_async(3);

        // Act
        let ran = acc.flush_if_needed(&policy, |batch| batch.len() as u32);

        // Assert
        assert!(ran);
        assert_eq!(rx1.recv().unwrap(), 3);
        assert_eq!(rx2.recv().unwrap(), 3);
        assert_eq!(rx3.recv().unwrap(), 3);
    }

    #[test]
    fn should_run_flush_once_for_many_submitters() {
        // Arrange
        let acc: Accumulator<u32, u32> = Accumulator::new();
        let policy = LenFlushPolicy::new(1);

        let rx1 = acc.submit_async(1);
        let rx2 = acc.submit_async(2);

        // Act: flush should run only once
        let flushes = AtomicUsize::new(0);
        let ran = acc.flush_if_needed(&policy, |batch| {
            flushes.fetch_add(1, Ordering::Relaxed);
            batch.len() as u32
        });

        // Assert
        assert!(ran);
        assert_eq!(flushes.load(Ordering::Relaxed), 1);
        assert_eq!(rx1.recv().unwrap(), 2);
        assert_eq!(rx2.recv().unwrap(), 2);
    }

    #[test]
    fn should_drain_waiters_when_key_completed() {
        // This test verifies that waiters grouped per key are drained when that key is completed.

        // Arrange
        let gc: KeyedGroupCommit<u64, u64> = KeyedGroupCommit::new(10);

        // Join under current key (10)
        gc.join(1);
        gc.join(2);

        // Act: Seal key=10 and start key=11
        let sealed = gc.rotate_to(11);
        assert_eq!(sealed, Some((10, 2)));

        // Arrange (join under new key 11)
        gc.join(3);

        // Act: complete key=10 and collect waiters
        let w10 = gc.complete(&10);

        // Assert: Key=10's waiters drained
        assert_eq!(w10, vec![1, 2]);

        // Assert: Key=11 is still pending (not sealed yet)
        assert_eq!(gc.complete(&11), Vec::<u64>::new());
        assert_eq!(gc.pending_len(), 1);
    }
}
