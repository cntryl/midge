use parking_lot::Mutex;
use std::collections::HashMap;
use std::hash::Hash;

/// A keyed group-commit accumulator.
///
/// Tracks waiter payloads by the durability generation that will complete them.
///
/// Intended use:
/// - Call `join(waiter)` to register a waiter under the current key.
/// - When the current key is sealed (e.g. WAL segment rotation), call `rotate_from_to`
///   to move all pending waiters for the sealed key into an inflight bucket.
/// - When the keyed work completes (e.g. `CloudAck` for that WAL segment), call
///   `complete(key)` to drain the waiters and notify them externally.
pub struct KeyedGroupCommit<K, W> {
    state: Mutex<KeyedState<K, W>>,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct KeyTransitionMismatch;

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

    /// Join an already-sealed key directly.
    pub fn join_for_key(&self, key: K, waiter: W) {
        let mut state = self.state.lock();
        state.inflight.entry(key).or_default().push(waiter);
    }

    fn rotate_locked(state: &mut KeyedState<K, W>, new_key: K) -> Option<(K, usize)> {
        let old_key = state.current_key.clone();
        state.current_key = new_key;

        if state.pending.is_empty() {
            return None;
        }

        let waiters = std::mem::take(&mut state.pending);
        let moved = waiters.len();
        state
            .inflight
            .entry(old_key.clone())
            .or_default()
            .extend(waiters);
        Some((old_key, moved))
    }

    /// Seal `expected_current` and begin `new_key`, failing without mutation
    /// when the caller's external generation has drifted from this primitive.
    pub fn rotate_from_to(
        &self,
        expected_current: &K,
        new_key: K,
    ) -> Result<Option<(K, usize)>, KeyTransitionMismatch> {
        let mut state = self.state.lock();
        if &state.current_key != expected_current {
            return Err(KeyTransitionMismatch);
        }
        Ok(Self::rotate_locked(&mut state, new_key))
    }

    /// Inspect the waiters queued at `key` without completing them.
    ///
    /// Lets a caller derive a shared budget from the waiters it is about to
    /// serve, which requires reading them before the work runs rather than
    /// after.
    pub fn inspect_key<T>(&self, key: &K, project: impl Fn(&W) -> T) -> Vec<T> {
        let state = self.state.lock();
        state
            .inflight
            .get(key)
            .map(|waiters| waiters.iter().map(project).collect())
            .unwrap_or_default()
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

    /// Atomically discard every existing generation and install `new_key`.
    ///
    /// This is the fail-closed recovery path for an external generation drift:
    /// callers must terminally fail the returned waiters before accepting work
    /// under the replacement key.
    pub fn drain_all_and_reset(&self, new_key: K) -> Vec<W> {
        let mut state = self.state.lock();
        let mut out = Vec::new();
        out.append(&mut state.pending);
        for (_, mut waiters) in state.inflight.drain() {
            out.append(&mut waiters);
        }
        state.current_key = new_key;
        out
    }

    pub fn pending_len(&self) -> usize {
        let state = self.state.lock();
        state.pending.len()
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn should_drain_waiters_when_key_completed() {
        // This test verifies that waiters grouped per key are drained when that key is completed.

        // Arrange
        let gc: KeyedGroupCommit<u64, u64> = KeyedGroupCommit::new(10);

        // Join under current key (10)
        gc.join(1);
        gc.join(2);

        // Act: Seal key=10 and start key=11
        let sealed = gc.rotate_from_to(&10, 11).expect("rotate generation");
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

    #[test]
    fn should_preserve_join_for_key_waiters_given_pending_batch_when_rotating_same_key() {
        // Arrange
        let gc: KeyedGroupCommit<u64, u64> = KeyedGroupCommit::new(1);
        gc.join_for_key(1, 100);
        gc.join(200);

        // Act
        assert_eq!(gc.rotate_from_to(&1, 2), Ok(Some((1, 1))));

        // Assert
        assert_eq!(gc.complete(&1), vec![100, 200]);
    }

    #[test]
    fn should_reject_transition_without_orphaning_waiters_when_expected_key_drifted() {
        // Arrange
        let gc: KeyedGroupCommit<u64, u64> = KeyedGroupCommit::new(7);
        gc.join(9);

        // Act
        let result = gc.rotate_from_to(&6, 8);

        // Assert
        assert_eq!(result, Err(KeyTransitionMismatch));
        assert_eq!(gc.rotate_from_to(&7, 8), Ok(Some((7, 1))));
        assert_eq!(gc.complete(&7), vec![9]);
    }

    #[test]
    fn should_reset_generation_after_draining_waiters_given_external_drift() {
        // Arrange
        let gc: KeyedGroupCommit<u64, u64> = KeyedGroupCommit::new(7);
        gc.join_for_key(7, 1);
        gc.join(2);

        // Act
        let drained = gc.drain_all_and_reset(9);
        gc.join(3);

        // Assert
        assert_eq!(drained, vec![2, 1]);
        assert_eq!(gc.rotate_from_to(&9, 10), Ok(Some((9, 1))));
        assert_eq!(gc.complete(&9), vec![3]);
    }
}
