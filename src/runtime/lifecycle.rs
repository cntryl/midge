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
            Self::Error(error) => Err(clone_shutdown_error(error)),
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
}

impl RuntimeLifecycleState {
    pub(crate) fn load(state: &AtomicU8) -> Self {
        match state.load(Ordering::Acquire) {
            0 => Self::Open,
            1 => Self::Closing,
            _ => Self::Closed,
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
        if self.state() != RuntimeLifecycleState::Open {
            return Err(MidgeError::Busy("engine is shutting down".to_string()));
        }
        self.active_transactions.fetch_add(1, Ordering::AcqRel);
        if self.state() != RuntimeLifecycleState::Open {
            self.release();
            return Err(MidgeError::Busy("engine is shutting down".to_string()));
        }
        Ok(RuntimeTransactionGuard {
            lifecycle: Arc::clone(self),
        })
    }

    pub(crate) fn release(&self) {
        // Pair the counter transition with the same mutex used by the waiter.
        // Without this lock, the final transaction can notify between the
        // waiter's condition check and `Condvar::wait`, losing the wakeup.
        let _wait_guard = self
            .wait_lock
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner);
        if self.active_transactions.fetch_sub(1, Ordering::AcqRel) == 1 {
            self.wait_cv.notify_all();
        }
    }

    pub(crate) fn state(&self) -> RuntimeLifecycleState {
        RuntimeLifecycleState::load(&self.state)
    }

    pub(crate) fn ensure_open(&self) -> MidgeResult<()> {
        if self.state() == RuntimeLifecycleState::Open {
            Ok(())
        } else {
            Err(MidgeError::Busy("engine is shutting down".to_string()))
        }
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
        self.state
            .store(RuntimeLifecycleState::Closed as u8, Ordering::Release);
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

fn clone_shutdown_error(error: &MidgeError) -> MidgeError {
    match error {
        MidgeError::Io(error) => MidgeError::Io(error.raw_os_error().map_or_else(
            || std::io::Error::new(error.kind(), error.to_string()),
            std::io::Error::from_raw_os_error,
        )),
        MidgeError::NotFound => MidgeError::NotFound,
        MidgeError::InvalidArgument(message) => MidgeError::InvalidArgument(message.clone()),
        MidgeError::Corruption(message) => MidgeError::Corruption(message.clone()),
        MidgeError::NotSupported(message) => MidgeError::NotSupported(message.clone()),
        MidgeError::Internal(message) => MidgeError::Internal(message.clone()),
        MidgeError::InvalidPath => MidgeError::InvalidPath,
        MidgeError::NoSpace(message) => MidgeError::NoSpace(message.clone()),
        MidgeError::RecoveryFailed(message) => MidgeError::RecoveryFailed(message.clone()),
        MidgeError::CompatibilityError(message) => MidgeError::CompatibilityError(message.clone()),
        MidgeError::WriteStall(message) => MidgeError::WriteStall(message.clone()),
        MidgeError::MemoryModeViolation(message) => {
            MidgeError::MemoryModeViolation(message.clone())
        }
        MidgeError::Fenced(message) => MidgeError::Fenced(message.clone()),
        MidgeError::LeaseHeld(message) => MidgeError::LeaseHeld(message.clone()),
        MidgeError::LeaseUnavailable(message) => MidgeError::LeaseUnavailable(message.clone()),
        MidgeError::LeaseIndeterminate(message) => MidgeError::LeaseIndeterminate(message.clone()),
        MidgeError::LeaseEpochExhausted => MidgeError::LeaseEpochExhausted,
        MidgeError::WriteConflict(message) => MidgeError::WriteConflict(message.clone()),
        MidgeError::Aborted(message) => MidgeError::Aborted(message.clone()),
        MidgeError::Busy(message) => MidgeError::Busy(message.clone()),
        MidgeError::Timeout(message) => MidgeError::Timeout(message.clone()),
        MidgeError::ResourceLimit(message) => MidgeError::ResourceLimit(message.clone()),
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
