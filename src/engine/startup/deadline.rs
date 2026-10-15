//! Timed callers leave startup resources with their worker until safe cleanup.

use super::super::{Engine, OpenOptions};
use super::EngineStartup;
use crate::common::{DeadlineScope, MidgeError, MidgeResult, OperationDeadline};
use crate::runtime::{StartupAdmission, StartupEvent, StartupObserver};
use crossbeam::channel::{self, Receiver, Sender};
use std::sync::{Arc, Mutex};
use std::time::{Duration, Instant};

pub(super) struct PreparedEngine {
    pub engine: Engine,
    pub admission: StartupAdmission,
}

type ReadySlot = Arc<Mutex<Option<MidgeResult<PreparedEngine>>>>;

pub(super) fn open(opts: OpenOptions, budget: Duration) -> MidgeResult<Engine> {
    let start = Instant::now();
    let deadline = OperationDeadline::from_start(start, budget);
    if !deadline.is_bounded() {
        return Err(MidgeError::InvalidArgument(
            "open timeout cannot be represented at startup".into(),
        ));
    }
    #[cfg(test)]
    let mut test_control = super::test_control::take();
    #[cfg(test)]
    let deadline = if test_control
        .as_ref()
        .is_some_and(|control| control.expire.is_some())
    {
        OperationDeadline::unbounded()
    } else {
        deadline
    };
    let scope = DeadlineScope::new(deadline);
    #[cfg(test)]
    let test_expire = test_control
        .as_mut()
        .and_then(|control| control.expire.take());
    let slot: ReadySlot = Arc::new(Mutex::new(None));
    let (ready_tx, ready_rx) = channel::bounded(1);
    let (decision_tx, decision_rx) = channel::bounded(1);
    let worker_slot = Arc::clone(&slot);
    let worker_scope = scope.clone();
    let caller_dispatch = tracing::dispatcher::get_default(Clone::clone);
    let caller_span = tracing::Span::current();
    std::thread::Builder::new()
        .name("midge-startup".into())
        .spawn(move || {
            #[cfg(test)]
            let _observation = super::test_control::enter_worker(test_control);
            tracing::dispatcher::with_default(&caller_dispatch, || {
                caller_span.in_scope(|| {
                    run_worker(
                        &opts,
                        &worker_scope,
                        start,
                        &worker_slot,
                        &ready_tx,
                        &decision_rx,
                    );
                });
            });
        })
        .map_err(|error| {
            MidgeError::ResourceLimit(format!("cannot spawn startup worker: {error}"))
        })?;

    #[cfg(test)]
    let readiness = match test_expire {
        Some(expire) => super::test_control::controlled_readiness(&ready_rx, &expire, &scope)?,
        None => ready_rx.recv_timeout(scope.clamp(Duration::MAX)),
    };
    #[cfg(not(test))]
    let readiness = ready_rx.recv_timeout(scope.clamp(Duration::MAX));
    match readiness {
        Ok(()) => accept_ready(&slot, &scope, &decision_tx, start),
        Err(channel::RecvTimeoutError::Timeout) => Err(cancelled_error(&scope)),
        Err(channel::RecvTimeoutError::Disconnected) => {
            if scope.deadline().is_expired() {
                Err(cancelled_error(&scope))
            } else {
                scope.cancel();
                Err(MidgeError::Internal(
                    "startup worker exited before readiness".into(),
                ))
            }
        }
    }
    // Dropping decision_tx wakes the resource owner on every failure path.
}

#[cfg(test)]
mod tests;

fn accept_ready(
    slot: &ReadySlot,
    scope: &DeadlineScope,
    decision: &Sender<()>,
    start: Instant,
) -> MidgeResult<Engine> {
    let mut ready = slot
        .lock()
        .unwrap_or_else(std::sync::PoisonError::into_inner);
    match ready.as_ref() {
        Some(Ok(prepared)) => {
            if let Err(error) = prepared.admission.accept() {
                scope.cancel();
                return Err(error);
            }
        }
        Some(Err(_)) => {}
        None => {
            return Err(MidgeError::Internal(
                "startup readiness has no payload".into(),
            ))
        }
    }
    let result = ready
        .take()
        .expect("readiness payload checked under its lock");
    let _ = decision.send(());
    result.map(|prepared| {
        tracing::info!(
            db_path = %prepared.engine.db_path.display(),
            open_ms = start.elapsed().as_secs_f64() * 1000.0,
            "engine open completed"
        );
        prepared.engine
    })
}

fn cancelled_error(scope: &DeadlineScope) -> MidgeError {
    if scope.cancel() {
        MidgeError::LeaseIndeterminate(
            "startup deadline expired while lease acquisition was unresolved".into(),
        )
    } else {
        MidgeError::Timeout("aggregate engine open deadline exhausted".into())
    }
}

fn run_worker(
    opts: &OpenOptions,
    scope: &DeadlineScope,
    start: Instant,
    slot: &ReadySlot,
    ready: &Sender<()>,
    decision: &Receiver<()>,
) {
    let observer = opts.startup_observer();
    let result = EngineStartup::prepare_within(opts, scope, start);
    *slot
        .lock()
        .unwrap_or_else(std::sync::PoisonError::into_inner) = Some(result);
    if ready.send(()).is_err() {
        scope.cancel();
    } else {
        let _ = decision.recv();
    }
    let remaining = slot
        .lock()
        .unwrap_or_else(std::sync::PoisonError::into_inner)
        .take();
    if let Some(Ok(prepared)) = remaining {
        scope.cancel();
        cleanup_prepared(prepared, observer.as_ref());
    }
}

fn cleanup_prepared(mut prepared: PreparedEngine, observer: Option<&Arc<dyn StartupObserver>>) {
    prepared.admission.cancel();
    // Runtime Drop cancels the prepared worker and joins every owned actor.
    drop(prepared.engine.runtime.take());
    let cleanup = prepared.engine.lease_state.release_owned_synchronously();
    if let Some(observer) = observer {
        observer.observe(StartupEvent::CleanupFinished {
            successful: cleanup.is_ok(),
        });
    }
}
