//! Bounded reusable compute workers. Tickets retain owned work through join.
use crate::common::{MidgeError, MidgeResult};
use crossbeam::channel::{Receiver, Sender};
use std::sync::Arc;
use std::thread::JoinHandle;

type Compute = Box<dyn FnOnce() -> Vec<String> + Send + 'static>;
type Completion = std::thread::Result<Vec<String>>;

struct Work {
    compute: Compute,
    completed: Sender<Completion>,
    gate: crate::failpoints::WorkerGate,
}

pub(super) struct CompactionPool {
    requests: Option<Sender<Work>>,
    workers: Vec<JoinHandle<()>>,
}

pub(super) struct CompactionTask {
    completed: Receiver<Completion>,
}

impl CompactionPool {
    pub(super) fn new(limit: usize) -> MidgeResult<Arc<Self>> {
        let limit = limit.max(1);
        let (requests, incoming) = crossbeam::channel::bounded::<Work>(limit);
        let mut pool = Self {
            requests: Some(requests),
            workers: Vec::new(),
        };
        for index in 0..limit {
            let incoming = incoming.clone();
            let worker = std::thread::Builder::new()
                .name(format!("midge-compaction-worker-{index}"))
                .spawn(move || {
                    while let Ok(work) = incoming.recv() {
                        let result = work.gate.run(|| {
                            std::panic::catch_unwind(std::panic::AssertUnwindSafe(work.compute))
                        });
                        let _ = work.completed.send(result);
                    }
                })
                .map_err(|error| {
                    MidgeError::ResourceLimit(format!("start compaction worker: {error}"))
                })?;
            pool.workers.push(worker);
        }
        Ok(Arc::new(pool))
    }

    pub(super) fn submit(
        &self,
        compute: impl FnOnce() -> Vec<String> + Send + 'static,
    ) -> MidgeResult<CompactionTask> {
        let (completed, result) = crossbeam::channel::bounded(1);
        let work = Work {
            compute: Box::new(compute),
            completed,
            gate: crate::failpoints::worker_gate(),
        };
        self.requests
            .as_ref()
            .ok_or_else(|| MidgeError::Busy("compaction worker pool is closed".into()))?
            .try_send(work)
            .map_err(|error| match error {
                crossbeam::channel::TrySendError::Full(_) => {
                    MidgeError::Busy("compaction worker queue is full".into())
                }
                crossbeam::channel::TrySendError::Disconnected(_) => {
                    MidgeError::Internal("compaction worker pool terminated".into())
                }
            })?;
        Ok(CompactionTask { completed: result })
    }
}

impl CompactionTask {
    pub(super) fn is_ready(&self) -> bool {
        !self.completed.is_empty()
    }

    pub(super) fn try_join(&self) -> Option<Completion> {
        match self.completed.try_recv() {
            Ok(result) => Some(result),
            Err(crossbeam::channel::TryRecvError::Empty) => None,
            Err(crossbeam::channel::TryRecvError::Disconnected) => Some(Err(Box::new(
                "compaction completion disconnected".to_string(),
            ))),
        }
    }

    pub(super) fn join(self) -> Completion {
        self.completed.recv().unwrap_or_else(|_| {
            Err(Box::new(
                "compaction worker completion disconnected".to_string(),
            ))
        })
    }
}

impl Drop for CompactionPool {
    fn drop(&mut self) {
        // Close admission, then join every worker; never detach admitted work.
        self.requests.take();
        for worker in self.workers.drain(..) {
            let _ = worker.join();
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::time::Duration;

    #[cfg(feature = "failpoints")]
    #[test]
    fn should_inherit_current_callers_failpoint_gate_when_reusing_compute_worker() -> MidgeResult<()>
    {
        // Arrange: the reusable thread predates the caller's isolated scope.
        let pool = CompactionPool::new(1)?;
        let guard = crate::failpoints::test_failpoint_guard();
        let scenario = fail::FailScenario::setup();
        let (observed, reached) = crossbeam::channel::bounded(1);
        fail::cfg_callback("midge::compaction_pool::caller_gate", move || {
            observed.send(()).unwrap();
        })
        .unwrap();

        // Act: actual submitted work evaluates a configured failpoint.
        let ticket = pool.submit(|| {
            crate::failpoints::fail_point!("midge::compaction_pool::caller_gate");
            Vec::new()
        })?;
        let inherited = reached.recv_timeout(Duration::from_secs(5));
        // Release before joining even on failure so the regression terminates.
        scenario.teardown();
        drop(guard);
        ticket.join().expect("submitted compute completed");

        // Assert: the callback ran while the caller still owned its gate.
        assert!(inherited.is_ok(), "worker blocked on its caller's gate");
        Ok(())
    }

    #[test]
    fn should_reuse_compute_thread_for_sequential_compaction_jobs() -> MidgeResult<()> {
        // Arrange
        let pool = CompactionPool::new(1)?;
        let (identities, observed) = crossbeam::channel::bounded(2);

        // Act
        for _ in 0..2 {
            let identities = identities.clone();
            pool.submit(move || {
                identities
                    .send(std::thread::current().id())
                    .expect("report worker");
                vec!["finished.sst".into()]
            })?
            .join()
            .expect("compute succeeded");
        }

        // Assert
        assert_eq!(observed.recv().unwrap(), observed.recv().unwrap());
        Ok(())
    }

    #[test]
    fn should_bound_queued_work_while_compute_is_held() -> MidgeResult<()> {
        // Arrange
        let pool = CompactionPool::new(1)?;
        let (entered, waiting) = crossbeam::channel::bounded(1);
        let (release, held) = crossbeam::channel::bounded(1);
        let first = pool.submit(move || {
            entered.send(()).unwrap();
            held.recv().unwrap();
            Vec::new()
        })?;
        waiting
            .recv_timeout(Duration::from_secs(1))
            .expect("worker held");

        // Act
        let second = pool.submit(Vec::new)?;
        let rejected = pool.submit(Vec::new);
        release.send(()).expect("release worker before asserting");

        // Assert
        assert!(matches!(rejected, Err(MidgeError::Busy(_))));
        first.join().expect("first compute");
        second.join().expect("queued compute");
        Ok(())
    }

    #[test]
    fn should_preserve_worker_after_a_compute_panics() -> MidgeResult<()> {
        // Arrange
        let pool = CompactionPool::new(1)?;

        // Act
        let failed = pool.submit(|| panic!("injected compute panic"))?.join();
        let next = pool.submit(|| vec!["next.sst".into()])?.join();

        // Assert
        assert!(failed.is_err());
        assert_eq!(next.expect("worker survived"), vec!["next.sst"]);
        Ok(())
    }
}
