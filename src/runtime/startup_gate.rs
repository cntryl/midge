//! Dormant runtime acceptance under the caller's original startup budget.

use super::{RuntimeConfig, RuntimeLifecycle, StartupEvent, StartupObserver};
use crate::common::{DeadlineScope, MidgeError, MidgeResult};
use std::sync::{atomic::Ordering, Arc, Condvar, Mutex};
use std::time::Duration;

#[derive(Clone, Copy, PartialEq, Eq)]
enum Phase {
    Preparing,
    Prepared,
    Accepted,
    Cancelled,
}

struct Inner {
    scope: DeadlineScope,
    phase: Mutex<Phase>,
    changed: Condvar,
    lifecycle: Arc<RuntimeLifecycle>,
    lease_healthy: Option<Arc<std::sync::atomic::AtomicBool>>,
    lease_validity: Option<Arc<crate::lease::LeaseValidity>>,
    writer_epoch: u64,
    observer: Option<Arc<dyn StartupObserver>>,
}

#[derive(Clone)]
pub(crate) struct StartupAdmission(Arc<Inner>);

impl StartupAdmission {
    pub(super) fn new(
        scope: DeadlineScope,
        lifecycle: Arc<RuntimeLifecycle>,
        config: &RuntimeConfig,
    ) -> Self {
        Self(Arc::new(Inner {
            scope,
            phase: Mutex::new(Phase::Preparing),
            changed: Condvar::new(),
            lifecycle,
            lease_healthy: config.lease_healthy.clone(),
            lease_validity: config.lease_validity.clone(),
            writer_epoch: config.writer_epoch,
            observer: config.startup_observer.clone(),
        }))
    }

    pub(crate) fn accept(&self) -> MidgeResult<()> {
        {
            let mut phase = self
                .0
                .phase
                .lock()
                .unwrap_or_else(std::sync::PoisonError::into_inner);
            match *phase {
                Phase::Accepted => return Ok(()),
                Phase::Cancelled => {
                    return Err(MidgeError::Timeout("runtime startup was cancelled".into()));
                }
                Phase::Preparing => {
                    return Err(MidgeError::Internal("runtime is not prepared".into()));
                }
                Phase::Prepared => {}
            }
            self.check_authority()?;
            self.0.scope.complete()?;
            *phase = Phase::Accepted;
            self.0.lifecycle.mark_running();
            self.0.changed.notify_all();
        }
        self.observe(StartupEvent::RuntimeAdmitted);
        Ok(())
    }

    pub(crate) fn cancel(&self) -> bool {
        let mut phase = self
            .0
            .phase
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner);
        if *phase == Phase::Accepted {
            return false;
        }
        let ambiguous = self.0.scope.cancel();
        *phase = Phase::Cancelled;
        self.0.changed.notify_all();
        ambiguous
    }

    pub(super) fn prepared(&self) -> MidgeResult<()> {
        {
            let mut phase = self
                .0
                .phase
                .lock()
                .unwrap_or_else(std::sync::PoisonError::into_inner);
            self.0.scope.check("runtime preparation")?;
            if *phase != Phase::Preparing {
                return Err(MidgeError::Timeout("runtime startup was cancelled".into()));
            }
            *phase = Phase::Prepared;
        }
        self.observe(StartupEvent::RuntimePrepared);
        Ok(())
    }

    pub(super) fn wait_until_accepted(&self) -> bool {
        let mut phase = self
            .0
            .phase
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner);
        loop {
            match *phase {
                Phase::Accepted => return true,
                Phase::Cancelled => return false,
                Phase::Preparing | Phase::Prepared => {}
            }
            let remaining = self.0.scope.deadline().remaining();
            if remaining.is_zero() {
                self.0.scope.cancel();
                *phase = Phase::Cancelled;
                self.0.changed.notify_all();
                return false;
            }
            // Observe an early outer-owner cancellation without admitting work.
            let (next, _) = self
                .0
                .changed
                .wait_timeout(phase, remaining.min(Duration::from_millis(100)))
                .unwrap_or_else(std::sync::PoisonError::into_inner);
            phase = next;
        }
    }

    pub(super) fn aborted(&self) {
        self.cancel();
        self.observe(StartupEvent::RuntimeAborted);
    }

    fn check_authority(&self) -> MidgeResult<()> {
        if self
            .0
            .lease_healthy
            .as_ref()
            .is_some_and(|healthy| !healthy.load(Ordering::Acquire))
        {
            return Err(MidgeError::Fenced(
                "lease heartbeat is unhealthy at runtime admission".into(),
            ));
        }
        if let Some(validity) = &self.0.lease_validity {
            validity.remaining(self.0.writer_epoch).map_err(|error| {
                MidgeError::Fenced(format!(
                    "lease validity was lost at runtime admission: {error}"
                ))
            })?;
        }
        Ok(())
    }

    fn observe(&self, event: StartupEvent) {
        if let Some(observer) = &self.0.observer {
            observer.observe(event);
        }
    }
}

#[cfg(test)]
mod tests;
