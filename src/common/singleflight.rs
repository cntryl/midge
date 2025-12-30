use parking_lot::Mutex;
use std::collections::HashMap;
use std::hash::Hash;

/// Policy for deciding when accumulated work should be flushed.
///
/// This is intentionally generic and can be driven by:
/// - size thresholds
/// - timers
/// - explicit external signals
pub trait FlushPolicy<T>: Send + Sync + 'static {
    fn should_flush(&self, pending: &[T]) -> bool;
}

struct Generation<R> {
    waiters: Vec<crossbeam::channel::Sender<R>>,
}

struct State<T, R> {
    pending: Vec<T>,
    generation: Option<Generation<R>>,
}

/// A generic accumulator that coalesces many submitters onto a single flush.
///
/// Key idea:
/// - Callers `submit()` items and block waiting for the *next* flush result.
/// - A flush runs at most once per "generation" and fans the result out
///   to all waiters.
/// - The accumulator does not force a flush timing model; you can either:
///   - call `flush_now()` explicitly (e.g. from a tick loop), or
///   - call `flush_if_needed()` with a policy.
pub struct Accumulator<T, R> {
    state: Mutex<State<T, R>>,
}

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

impl<T, R> Default for Accumulator<T, R> {
    fn default() -> Self {
        Self::new()
    }
}

impl<T, R> Accumulator<T, R> {
    pub fn new() -> Self {
        Self {
            state: Mutex::new(State {
                pending: Vec::new(),
                generation: None,
            }),
        }
    }

    /// Submit an item and block until the next flush completes.
    ///
    /// This does NOT necessarily trigger a flush by itself.
    /// Use `flush_if_needed()` (policy-driven) or `flush_now()` (explicit)
    /// from your scheduler/event loop.
    pub fn submit(&self, item: T) -> R
    where
        R: Send + Clone + 'static,
    {
        let rx = self.submit_async(item);
        rx.recv().expect("flush result")
    }

    /// Submit an item and obtain a one-shot receiver for the next flush result.
    pub fn submit_async(&self, item: T) -> crossbeam::channel::Receiver<R>
    where
        R: Send + Clone + 'static,
    {
        let (tx, rx) = crossbeam::channel::bounded::<R>(1);

        let mut state = self.state.lock();
        state.pending.push(item);

        // A pending item always belongs to a generation.
        let gen = state.generation.get_or_insert_with(|| Generation {
            waiters: Vec::new(),
        });
        gen.waiters.push(tx);

        rx
    }

    /// Flush pending items if the policy requests it.
    ///
    /// Returns true if a flush ran.
    pub fn flush_if_needed<P, F>(&self, policy: &P, flush_fn: F) -> bool
    where
        P: FlushPolicy<T>,
        R: Send + Clone + 'static,
        F: FnOnce(Vec<T>) -> R,
    {
        // Intentional: policy check and flush are separated to keep `flush_fn`
        // execution out of the lock.
        let should_flush = {
            let state = self.state.lock();
            !state.pending.is_empty() && policy.should_flush(&state.pending)
        };

        if should_flush {
            self.flush_now(flush_fn)
        } else {
            false
        }
    }

    /// Flush immediately (if there is an inflight generation).
    ///
    /// Returns true if a flush ran.
    pub fn flush_now<F>(&self, flush_fn: F) -> bool
    where
        R: Send + Clone + 'static,
        F: FnOnce(Vec<T>) -> R,
    {
        // Take the batch and the waiter list out under the lock.
        let (batch, waiters) = {
            let mut state = self.state.lock();

            if state.pending.is_empty() {
                // Invariant: no pending work => no generation.
                state.generation = None;
                return false;
            }

            let gen = match state.generation.take() {
                Some(gen) => gen,
                None => {
                    // No waiters yet; treat as no-op.
                    state.pending.clear();
                    return false;
                }
            };

            let batch = std::mem::take(&mut state.pending);
            (batch, gen.waiters)
        };

        let result = flush_fn(batch);

        // Fan-out. Ignore send failures (waiter dropped).
        // Note: flush result is cloned once per waiter.
        for w in waiters {
            let _ = w.send(result.clone());
        }

        true
    }

    pub fn pending_len(&self) -> usize {
        let state = self.state.lock();
        state.pending.len()
    }

    pub fn has_inflight(&self) -> bool {
        let state = self.state.lock();
        state.generation.is_some()
    }
}

/// A simple policy that flushes when `pending.len() >= n`.
pub struct LenFlushPolicy {
    n: usize,
}

impl LenFlushPolicy {
    pub fn new(n: usize) -> Self {
        Self { n }
    }
}

impl<T> FlushPolicy<T> for LenFlushPolicy {
    fn should_flush(&self, pending: &[T]) -> bool {
        pending.len() >= self.n
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
