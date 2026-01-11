use parking_lot::Mutex;
use std::sync::mpsc::{channel, Receiver, Sender};

/// Simple accumulator used by benches for hotpath measurement.
///
/// Generic over submitted value `V` (batched inputs) and response `R` (value sent to waiters
/// when the batch flushes). This is intentionally minimal: it stores pending waiters and
/// replies to each waiter with the result of the provided closure passed to `flush_now`.
#[derive(Debug)]
pub struct Accumulator<V, R> {
    state: Mutex<State<V, R>>,
}

#[derive(Debug)]
struct State<V, R> {
    pending: Vec<(V, Sender<R>)>,
}

impl<V, R> Accumulator<V, R>
where
    V: Clone,
    R: Clone,
{
    /// Create a new accumulator
    pub fn new() -> Self {
        Self {
            state: Mutex::new(State { pending: Vec::new() }),
        }
    }

    /// Submit a value and receive a channel that will be notified when the batch flushes.
    pub fn submit_async(&self, v: V) -> Receiver<R> {
        let (tx, rx) = channel();
        let mut s = self.state.lock();
        s.pending.push((v, tx));
        rx
    }

    /// Flush the current pending batch now and notify waiters with the result of `f`.
    /// Returns the value produced by the closure.
    pub fn flush_now<F>(&self, f: F) -> R
    where
        F: FnOnce(&[V]) -> R,
    {
        let mut s = self.state.lock();
        if s.pending.is_empty() {
            // Call the closure on an empty slice and return its result
            return f(&[]);
        }

        let pending = std::mem::take(&mut s.pending);
        let values: Vec<V> = pending.iter().map(|(v, _)| v.clone()).collect();
        let res = f(&values);

        for (_, tx) in pending.into_iter() {
            // Best-effort send; ignore errors (receiver may have been dropped)
            let _ = tx.send(res.clone());
        }

        res
    }

    /// Drain all pending submissions and return their values (test/utility helper)
    pub fn drain_all(&self) -> Vec<V> {
        let mut s = self.state.lock();
        let mut out = Vec::new();
        for (v, _) in s.pending.drain(..) {
            out.push(v)
        }
        out
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn should_notify_waiters_on_flush() {
        let acc: Accumulator<u64, u64> = Accumulator::new();
        let rx1 = acc.submit_async(1);
        let rx2 = acc.submit_async(2);
        let ran = acc.flush_now(|batch| batch.len() as u64);
        assert_eq!(ran, 2);
        assert_eq!(rx1.recv().unwrap(), 2);
        assert_eq!(rx2.recv().unwrap(), 2);
    }
}