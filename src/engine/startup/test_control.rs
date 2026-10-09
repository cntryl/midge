//! Scoped scheduling and owner observations for public timed-open unit tests.

use crate::common::{DeadlineScope, MidgeError, MidgeResult};
use crossbeam::channel::{self, Receiver, Sender};
use std::cell::RefCell;
use std::time::Duration;

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub(super) enum OwnerExit {
    Returned,
    Panicked,
}

pub(super) struct OpenTestControl {
    pub expire: Option<Receiver<()>>,
    pub owner_finished: Sender<OwnerExit>,
    pub checkpoint: Option<CheckpointTestBoundaries>,
}

pub(super) struct CheckpointTestBoundaries {
    pub before_publish_receive: Option<OneShotBarrier>,
    pub actor_join_started: Option<Sender<()>>,
}

pub(super) struct OneShotBarrier {
    pub entered: Sender<()>,
    pub release: Receiver<()>,
}

#[derive(Default)]
struct CallerSlot {
    installed: bool,
    control: Option<OpenTestControl>,
}

thread_local! {
    static CALLER: RefCell<CallerSlot> = RefCell::new(CallerSlot::default());
    static WORKER: RefCell<Option<CheckpointTestBoundaries>> = const { RefCell::new(None) };
}

pub(super) struct Installation(std::marker::PhantomData<*mut ()>);

pub(super) fn install(control: OpenTestControl) -> Installation {
    CALLER.with(|slot| {
        let mut slot = slot.borrow_mut();
        assert!(!slot.installed, "nested timed-open test control");
        slot.installed = true;
        slot.control = Some(control);
    });
    Installation(std::marker::PhantomData)
}

impl Drop for Installation {
    fn drop(&mut self) {
        CALLER.with(|slot| *slot.borrow_mut() = CallerSlot::default());
    }
}

pub(super) fn take() -> Option<OpenTestControl> {
    CALLER.with(|slot| slot.borrow_mut().control.take())
}

pub(super) struct WorkerObservation(Option<Sender<OwnerExit>>);

pub(super) fn enter_worker(control: Option<OpenTestControl>) -> WorkerObservation {
    let Some(control) = control else {
        return WorkerObservation(None);
    };
    WORKER.with(|slot| {
        assert!(
            slot.borrow().is_none(),
            "nested startup worker test control"
        );
        *slot.borrow_mut() = control.checkpoint;
    });
    WorkerObservation(Some(control.owner_finished))
}

impl Drop for WorkerObservation {
    fn drop(&mut self) {
        WORKER.with(|slot| *slot.borrow_mut() = None);
        if let Some(finished) = self.0.take() {
            let exit = if std::thread::panicking() {
                OwnerExit::Panicked
            } else {
                OwnerExit::Returned
            };
            let _ = finished.try_send(exit);
        }
    }
}

pub(super) fn controlled_readiness(
    ready: &Receiver<()>,
    expire: &Receiver<()>,
    scope: &DeadlineScope,
) -> MidgeResult<Result<(), channel::RecvTimeoutError>> {
    crossbeam::select! {
        recv(ready) -> result => Ok(result.map_err(|_| channel::RecvTimeoutError::Disconnected)),
        recv(expire) -> result => if result.is_ok() {
            Ok(Err(channel::RecvTimeoutError::Timeout))
        } else {
            scope.cancel();
            Err(MidgeError::Internal("timed-open test controller disconnected without expiry".into()))
        }
    }
}

pub(super) fn before_publish_receive() {
    let barrier = WORKER.with(|slot| {
        slot.borrow_mut()
            .as_mut()
            .and_then(|control| control.before_publish_receive.take())
    });
    if let Some(barrier) = barrier {
        barrier
            .entered
            .send(())
            .expect("checkpoint controller present");
        barrier
            .release
            .recv_timeout(Duration::from_secs(10))
            .expect("checkpoint controller must release pre-receive barrier");
    }
}

pub(super) fn actor_join_started() {
    let entered = WORKER.with(|slot| {
        slot.borrow_mut()
            .as_mut()
            .and_then(|control| control.actor_join_started.take())
    });
    if let Some(entered) = entered {
        entered.send(()).expect("actor-join controller present");
    }
}
