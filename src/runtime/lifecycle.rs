//! Runtime lifecycle state and transaction/shutdown guards.

use super::RuntimeResponse;
use crate::common::{MidgeError, MidgeResult};
use crossbeam::channel::Receiver;
use std::sync::{
    atomic::{AtomicU8, Ordering},
    Arc,
};
use std::time::Duration;

pub(crate) enum ShutdownTerminal {
    Success,
    Error(MidgeError),
}

impl ShutdownTerminal {
    pub(crate) fn replay(&self) -> MidgeResult<()> {
        match self {
            Self::Success => Ok(()),
            Self::Error(error) => Err(error.replay()),
        }
    }
}

pub(crate) enum ShutdownState {
    Idle,
    Sending,
    Ready {
        request_id: u64,
        response_rx: Receiver<RuntimeResponse>,
    },
    Receiving {
        request_id: u64,
    },
    ResponseDisconnected,
    Terminal(ShutdownTerminal),
}

pub(crate) struct ShutdownCoordinator {
    pub(crate) state: std::sync::Mutex<ShutdownState>,
    pub(crate) changed: std::sync::Condvar,
}

impl ShutdownCoordinator {
    fn new() -> Self {
        Self {
            state: std::sync::Mutex::new(ShutdownState::Idle),
            changed: std::sync::Condvar::new(),
        }
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
#[repr(u8)]
pub(crate) enum RuntimeLifecycleState {
    Open = 0,
    Closing = 1,
    Closed = 2,
    Fenced = 3,
}

impl RuntimeLifecycleState {
    pub(crate) fn load(state: &AtomicU8) -> Self {
        match state.load(Ordering::Acquire) {
            0 => Self::Open,
            1 => Self::Closing,
            2 => Self::Closed,
            _ => Self::Fenced,
        }
    }
}

pub(crate) struct RuntimeLifecycle {
    pub(crate) state: AtomicU8,
    pub(crate) running: std::sync::atomic::AtomicBool,
    pub(crate) active_transactions: std::sync::atomic::AtomicUsize,
    pub(crate) submission_gate: std::sync::RwLock<()>,
    pub(crate) wait_lock: std::sync::Mutex<()>,
    pub(crate) wait_cv: std::sync::Condvar,
    pub(crate) shutdown: ShutdownCoordinator,
}

impl RuntimeLifecycle {
    pub(crate) fn new() -> Self {
        Self {
            state: AtomicU8::new(RuntimeLifecycleState::Open as u8),
            running: std::sync::atomic::AtomicBool::new(false),
            active_transactions: std::sync::atomic::AtomicUsize::new(0),
            submission_gate: std::sync::RwLock::new(()),
            wait_lock: std::sync::Mutex::new(()),
            wait_cv: std::sync::Condvar::new(),
            shutdown: ShutdownCoordinator::new(),
        }
    }

    pub(crate) fn acquire(self: &Arc<Self>) -> MidgeResult<RuntimeTransactionGuard> {
        self.ensure_open()?;
        self.active_transactions.fetch_add(1, Ordering::AcqRel);
        if let Err(error) = self.ensure_open() {
            self.release();
            return Err(error);
        }
        Ok(RuntimeTransactionGuard {
            lifecycle: Arc::clone(self),
        })
    }

    pub(crate) fn release(&self) {
        if self.active_transactions.fetch_sub(1, Ordering::AcqRel) == 1 {
            // Only the final release takes the wait mutex. A waiter either
            // observes zero, or atomically releases this mutex when sleeping
            // before we can acquire it and notify. Intermediate releases need
            // no notification and never contend on the mutex.
            let _wait_guard = self
                .wait_lock
                .lock()
                .unwrap_or_else(std::sync::PoisonError::into_inner);
            self.wait_cv.notify_all();
        }
    }

    pub(crate) fn state(&self) -> RuntimeLifecycleState {
        RuntimeLifecycleState::load(&self.state)
    }

    pub(crate) fn ensure_open(&self) -> MidgeResult<()> {
        match self.state() {
            RuntimeLifecycleState::Open => Ok(()),
            RuntimeLifecycleState::Fenced => Err(MidgeError::Fenced(
                "engine was dropped after its transaction wait expired".into(),
            )),
            RuntimeLifecycleState::Closing | RuntimeLifecycleState::Closed => {
                Err(MidgeError::Busy("engine is shutting down".into()))
            }
        }
    }

    pub(crate) fn ensure_readable(&self) -> MidgeResult<()> {
        if matches!(
            self.state(),
            RuntimeLifecycleState::Fenced | RuntimeLifecycleState::Closed
        ) {
            Err(MidgeError::Fenced(
                "transaction outlived its runtime".into(),
            ))
        } else {
            Ok(())
        }
    }

    pub(crate) fn fence_transactions(&self) {
        self.state
            .store(RuntimeLifecycleState::Fenced as u8, Ordering::Release);
        self.wait_cv.notify_all();
    }

    pub(crate) fn begin_submission(&self) -> MidgeResult<std::sync::RwLockReadGuard<'_, ()>> {
        let guard = self
            .submission_gate
            .read()
            .unwrap_or_else(std::sync::PoisonError::into_inner);
        self.ensure_open()?;
        Ok(guard)
    }

    pub(crate) fn begin_shutdown(&self) {
        let _ = self.state.compare_exchange(
            RuntimeLifecycleState::Open as u8,
            RuntimeLifecycleState::Closing as u8,
            Ordering::AcqRel,
            Ordering::Acquire,
        );
        // Wait for a submission that observed Open to finish its non-blocking
        // queue send before the shutdown marker is enqueued. New submissions
        // now observe Closing and fail immediately.
        drop(
            self.submission_gate
                .write()
                .unwrap_or_else(std::sync::PoisonError::into_inner),
        );
        self.wait_cv.notify_all();
    }

    pub(crate) fn mark_running(&self) {
        self.running.store(true, Ordering::Release);
    }

    pub(crate) fn mark_closed(&self) {
        let _wait_guard = self
            .wait_lock
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner);
        self.running.store(false, Ordering::Release);
        if self.state() != RuntimeLifecycleState::Fenced {
            self.state
                .store(RuntimeLifecycleState::Closed as u8, Ordering::Release);
        }
        self.wait_cv.notify_all();
    }

    pub(crate) fn active_transaction_count(&self) -> usize {
        self.active_transactions.load(Ordering::Acquire)
    }

    pub(crate) fn wait_for_transactions(&self) {
        let mut guard = self
            .wait_lock
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner);
        while self.active_transactions.load(Ordering::Acquire) != 0 {
            guard = self
                .wait_cv
                .wait(guard)
                .unwrap_or_else(std::sync::PoisonError::into_inner);
        }
    }

    pub(crate) fn wait_for_transactions_within(&self, timeout: Duration) -> bool {
        let started = std::time::Instant::now();
        let mut guard = self
            .wait_lock
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner);
        while self.active_transaction_count() != 0 {
            let Some(remaining) = timeout.checked_sub(started.elapsed()) else {
                return false;
            };
            let (next, _) = self
                .wait_cv
                .wait_timeout(guard, remaining)
                .unwrap_or_else(std::sync::PoisonError::into_inner);
            guard = next;
        }
        true
    }

    pub(crate) fn wait_until_stopped(&self, timeout: Duration) -> bool {
        if !self.running.load(Ordering::Acquire) {
            return true;
        }

        let started = std::time::Instant::now();
        let mut guard = self
            .wait_lock
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner);
        loop {
            if !self.running.load(Ordering::Acquire) {
                return true;
            }
            let Some(remaining) = timeout.checked_sub(started.elapsed()) else {
                return false;
            };
            let (next_guard, wait_result) = self
                .wait_cv
                .wait_timeout(guard, remaining)
                .unwrap_or_else(std::sync::PoisonError::into_inner);
            guard = next_guard;
            if wait_result.timed_out() && self.running.load(Ordering::Acquire) {
                return false;
            }
        }
    }

    #[cfg(test)]
    pub(crate) fn shutdown_response_is_available(&self) -> bool {
        matches!(
            *self
                .shutdown
                .state
                .lock()
                .unwrap_or_else(std::sync::PoisonError::into_inner),
            ShutdownState::Ready { .. }
        )
    }
}

pub(crate) struct RuntimeTransactionGuard {
    lifecycle: Arc<RuntimeLifecycle>,
}

impl Drop for RuntimeTransactionGuard {
    fn drop(&mut self) {
        self.lifecycle.release();
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn should_release_nonfinal_transaction_while_shutdown_wait_mutex_is_held() {
        // Arrange
        let lifecycle = Arc::new(RuntimeLifecycle::new());
        let first = lifecycle.acquire().expect("first transaction");
        let last = lifecycle.acquire().expect("last transaction");
        let wait_guard = lifecycle.wait_lock.lock().unwrap();
        let (completed, receiver) = std::sync::mpsc::channel();

        // Act
        let worker = std::thread::spawn(move || {
            drop(first);
            completed.send(()).expect("report release");
        });
        let result = receiver.recv_timeout(Duration::from_secs(1));
        drop(wait_guard);
        worker.join().expect("release worker");

        // Assert
        assert!(result.is_ok(), "nonfinal release waited on global mutex");
        assert_eq!(lifecycle.active_transaction_count(), 1);
        drop(last);
        lifecycle.begin_shutdown();
        lifecycle.wait_for_transactions();
    }

    #[test]
    fn should_wake_shutdown_when_final_release_races_wait_setup() {
        for _ in 0..100 {
            // Arrange
            let lifecycle = Arc::new(RuntimeLifecycle::new());
            let transaction = lifecycle.acquire().expect("transaction");
            lifecycle.begin_shutdown();
            let waiting = Arc::clone(&lifecycle);
            let (completed, receiver) = std::sync::mpsc::channel();

            // Act
            let worker = std::thread::spawn(move || {
                waiting.wait_for_transactions();
                completed.send(()).expect("report drained transactions");
            });
            drop(transaction);

            // Assert
            receiver
                .recv_timeout(Duration::from_secs(1))
                .expect("final release must wake waiter");
            worker.join().expect("shutdown waiter");
            assert_eq!(lifecycle.active_transaction_count(), 0);
        }
    }
}
