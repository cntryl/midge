use crate::common::{MidgeError, MidgeResult};
use crate::runtime::actors::wal::{MemtableApplyAuthority, MemtableApplyJob};
use crate::runtime::RuntimeMsg;
use crossbeam::channel::{Receiver, Sender, TrySendError};
use std::sync::atomic::{AtomicBool, Ordering};
use std::sync::Arc;
use std::thread::JoinHandle;

pub(super) struct ApplyCompletion {
    pub(super) ordinal: usize,
    pub(super) result: MidgeResult<()>,
}

pub(super) struct ApplyWork {
    pub(super) job: MemtableApplyJob,
    pub(super) authority: MemtableApplyAuthority,
    pub(super) completed: Sender<ApplyCompletion>,
    pub(super) cancelled: Arc<AtomicBool>,
    pub(super) wake_queued: Arc<AtomicBool>,
    pub(super) wake: Option<Sender<RuntimeMsg>>,
}

pub(super) struct ApplyPool {
    requests: Option<Sender<ApplyWork>>,
    workers: Vec<JoinHandle<()>>,
}

impl ApplyPool {
    pub(super) fn new() -> MidgeResult<Self> {
        let (requests, incoming) = crossbeam::channel::bounded::<ApplyWork>(2);
        let mut pool = Self {
            requests: Some(requests),
            workers: Vec::new(),
        };
        for ordinal in 0..2 {
            let incoming = incoming.clone();
            let gate = crate::failpoints::worker_gate();
            let handle = std::thread::Builder::new()
                .name(format!("midge-memtable-apply-{ordinal}"))
                .spawn(move || gate.run(|| Self::run(&incoming)))
                .map_err(MidgeError::Io)?;
            pool.workers.push(handle);
        }
        Ok(pool)
    }

    fn run(incoming: &Receiver<ApplyWork>) {
        for work in incoming {
            let ordinal = work.job.ordinal;
            let result = std::panic::catch_unwind(std::panic::AssertUnwindSafe(|| {
                work.job.apply(&work.authority)
            }))
            .unwrap_or_else(|_| {
                Err(MidgeError::Internal(
                    "admitted memtable apply panicked".into(),
                ))
            });
            if result.is_err() {
                work.cancelled.store(true, Ordering::Release);
            }
            let _ = work.completed.send(ApplyCompletion { ordinal, result });
            if !work.wake_queued.swap(true, Ordering::AcqRel) {
                if let Some(wake) = work.wake {
                    let _ = wake.send(RuntimeMsg::MemtableApplyProgress);
                }
            }
        }
    }

    pub(super) fn try_submit(&self, work: ApplyWork) -> Result<(), TrySendError<ApplyWork>> {
        self.requests
            .as_ref()
            .expect("apply pool admits while owned")
            .try_send(work)
    }
}

impl Drop for ApplyPool {
    fn drop(&mut self) {
        self.requests.take();
        for worker in self.workers.drain(..) {
            let _ = worker.join();
        }
    }
}
