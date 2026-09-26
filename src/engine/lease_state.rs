use crate::common::{MidgeError, MidgeResult};
use crate::runtime::{Runtime, RuntimeHandle};
use std::sync::{Arc, Mutex};
use std::thread::JoinHandle;
use std::time::Duration;

type ReaperTask = Box<dyn FnOnce() + Send>;

fn spawn_retained<T, F, S>(
    name: &str,
    payload: T,
    operation: F,
    spawner: S,
) -> Result<(), (std::io::Error, T)>
where
    T: Send + 'static,
    F: FnOnce(T) + Send + 'static,
    S: FnOnce(&str, ReaperTask) -> std::io::Result<JoinHandle<()>>,
{
    let retained = Arc::new(Mutex::new(Some(payload)));
    let worker_retained = Arc::clone(&retained);
    let task: ReaperTask = Box::new(move || {
        if let Some(payload) = worker_retained
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner)
            .take()
        {
            operation(payload);
        }
    });
    match spawner(name, task) {
        Ok(worker) => {
            drop(worker);
            Ok(())
        }
        Err(error) => {
            let payload = retained
                .lock()
                .unwrap_or_else(std::sync::PoisonError::into_inner)
                .take()
                .expect("reaper spawn failed before task began");
            Err((error, payload))
        }
    }
}

fn spawn_thread(name: &str, task: ReaperTask) -> std::io::Result<JoinHandle<()>> {
    std::thread::Builder::new()
        .name(name.to_string())
        .spawn(task)
}

pub(super) type FencingResources = (
    Option<Mutex<crate::lease::LeaseHeartbeat>>,
    Option<Arc<dyn crate::lease::PrimaryLease>>,
    Option<crate::lease::LeaseGuard>,
);

pub(super) enum PendingFencingCleanup {
    Known {
        completion: crossbeam::channel::Receiver<MidgeResult<()>>,
        terminal_result: MidgeResult<()>,
    },
    Runtime {
        completion: crossbeam::channel::Receiver<MidgeResult<()>>,
    },
}

/// Owns the writer-fencing resources retained for the engine lifetime.
pub(super) struct LeaseState {
    pub(super) lease: Option<Arc<dyn crate::lease::PrimaryLease>>,
    pub(super) guard: Option<crate::lease::LeaseGuard>,
    pub(super) heartbeat: Option<Mutex<crate::lease::LeaseHeartbeat>>,
    pub(super) pending_cleanup: Option<PendingFencingCleanup>,
}

impl LeaseState {
    pub(super) fn new(
        lease: Arc<dyn crate::lease::PrimaryLease>,
        guard: crate::lease::LeaseGuard,
        heartbeat: crate::lease::LeaseHeartbeat,
    ) -> Self {
        Self {
            lease: Some(lease),
            guard: Some(guard),
            heartbeat: Some(Mutex::new(heartbeat)),
            pending_cleanup: None,
        }
    }

    pub(super) fn has_resources(&self) -> bool {
        self.heartbeat.is_some() || self.lease.is_some() || self.guard.is_some()
    }

    pub(super) fn take_resources(&mut self) -> FencingResources {
        (self.heartbeat.take(), self.lease.take(), self.guard.take())
    }

    pub(super) fn restore_resources(&mut self, resources: FencingResources) {
        self.heartbeat = resources.0;
        self.lease = resources.1;
        self.guard = resources.2;
    }

    pub(super) fn release_fencing_parts(
        lease_heartbeat: Option<Mutex<crate::lease::LeaseHeartbeat>>,
        lease: Option<Arc<dyn crate::lease::PrimaryLease>>,
        lease_guard: Option<crate::lease::LeaseGuard>,
    ) -> MidgeResult<()> {
        if let Some(heartbeat_mutex) = lease_heartbeat {
            let mut heartbeat = heartbeat_mutex
                .lock()
                .unwrap_or_else(std::sync::PoisonError::into_inner);
            heartbeat.stop();
            tracing::trace!("Engine: lease heartbeat stopped");
        }
        let result = lease.map_or(Ok(()), |lease| Self::release_lease_bounded(lease.as_ref()));
        drop(lease_guard);
        result
    }

    /// Retry release for no longer than the lease TTL. An unreleased lease
    /// expires after its heartbeat stops.
    fn release_lease_bounded(lease: &dyn crate::lease::PrimaryLease) -> MidgeResult<()> {
        const MAX_BACKOFF: Duration = Duration::from_secs(1);
        let deadline = std::time::Instant::now() + lease.ttl();
        let mut backoff = Duration::from_millis(1);
        loop {
            match lease.release() {
                Ok(()) => return Ok(()),
                Err(error) => {
                    let remaining = deadline.saturating_duration_since(std::time::Instant::now());
                    if remaining.is_zero() || matches!(error, crate::lease::LeaseError::Internal(_))
                    {
                        tracing::warn!(%error, "primary lease release failed; it will expire at its TTL");
                        return Err(MidgeError::LeaseUnavailable(format!(
                            "primary lease release failed; the lease expires at its TTL: {error}"
                        )));
                    }
                    std::thread::sleep(backoff.min(remaining));
                    backoff = (backoff * 2).min(MAX_BACKOFF);
                }
            }
        }
    }

    pub(super) fn schedule_cleanup(&mut self, terminal_result: MidgeResult<()>) -> MidgeResult<()> {
        self.schedule_cleanup_with_spawner(terminal_result, spawn_thread)
    }

    fn schedule_cleanup_with_spawner<S>(
        &mut self,
        terminal_result: MidgeResult<()>,
        spawner: S,
    ) -> MidgeResult<()>
    where
        S: FnOnce(&str, ReaperTask) -> std::io::Result<JoinHandle<()>>,
    {
        debug_assert!(self.pending_cleanup.is_none());
        let resources = self.take_resources();
        let (completion_tx, completion_rx) = crossbeam::channel::bounded(1);
        match spawn_retained(
            "midge-fencing-reaper",
            resources,
            move |(heartbeat, lease, guard)| {
                let _ = completion_tx.send(Self::release_fencing_parts(heartbeat, lease, guard));
                tracing::debug!("Engine fencing reaper cleanup complete");
            },
            spawner,
        ) {
            Ok(()) => {
                self.pending_cleanup = Some(PendingFencingCleanup::Known {
                    completion: completion_rx,
                    terminal_result,
                });
                Ok(())
            }
            Err((error, resources)) => {
                self.restore_resources(resources);
                tracing::error!(%error, "failed to spawn fencing cleanup reaper");
                match terminal_result {
                    Ok(()) => Err(MidgeError::ResourceLimit(format!(
                        "failed to spawn fencing cleanup reaper: {error}"
                    ))),
                    Err(terminal_error) => Err(terminal_error),
                }
            }
        }
    }

    pub(super) fn schedule_runtime_cleanup(
        &mut self,
        runtime: Runtime,
        runtime_handle: RuntimeHandle,
    ) -> Result<(), (MidgeError, Runtime)> {
        debug_assert!(self.pending_cleanup.is_none());
        let resources = self.take_resources();
        let (completion_tx, completion_rx) = crossbeam::channel::bounded(1);
        match spawn_retained(
            "midge-runtime-fencing-reaper",
            (runtime, resources),
            move |(runtime, (heartbeat, lease, guard))| {
                let terminal_result = runtime_handle.shutdown(Duration::MAX);
                drop(runtime);
                let released = Self::release_fencing_parts(heartbeat, lease, guard);
                let _ = completion_tx.send(terminal_result.and(released));
                tracing::debug!("Engine runtime and fencing reaper cleanup complete");
            },
            spawn_thread,
        ) {
            Ok(()) => {
                self.pending_cleanup = Some(PendingFencingCleanup::Runtime {
                    completion: completion_rx,
                });
                Ok(())
            }
            Err((error, (runtime, resources))) => {
                self.restore_resources(resources);
                Err((
                    MidgeError::ResourceLimit(format!(
                        "failed to spawn runtime fencing cleanup reaper: {error}"
                    )),
                    runtime,
                ))
            }
        }
    }

    pub(super) fn detach_reaper(&mut self, runtime: Option<Runtime>) {
        let resources = self.take_resources();
        if runtime.is_none()
            && resources.0.is_none()
            && resources.1.is_none()
            && resources.2.is_none()
        {
            return;
        }
        if let Err((error, (runtime, (heartbeat, lease, guard)))) = spawn_retained(
            "midge-engine-reaper",
            (runtime, resources),
            move |(runtime, (heartbeat, lease, guard))| {
                drop(runtime);
                if let Err(error) = Self::release_fencing_parts(heartbeat, lease, guard) {
                    tracing::debug!(%error, "Engine reaper finished without releasing the lease");
                }
                tracing::debug!("Engine reaper cleanup complete");
            },
            spawn_thread,
        ) {
            tracing::error!(%error, "failed to spawn engine cleanup reaper");
            drop(runtime);
            let _ = Self::release_fencing_parts(heartbeat, lease, guard);
        }
    }
    pub(super) fn wait_for_cleanup(&mut self, timeout: Duration) -> MidgeResult<()> {
        enum CleanupWait {
            KnownComplete(MidgeResult<()>),
            RuntimeComplete(MidgeResult<()>),
            Timeout,
            Disconnected,
        }

        let cleanup = self
            .pending_cleanup
            .as_ref()
            .ok_or_else(|| MidgeError::Internal("fencing cleanup was not scheduled".to_string()))?;
        let wait_result = match cleanup {
            PendingFencingCleanup::Known { completion, .. } => {
                match completion.recv_timeout(timeout) {
                    Ok(released) => CleanupWait::KnownComplete(released),
                    Err(crossbeam::channel::RecvTimeoutError::Timeout) => CleanupWait::Timeout,
                    Err(crossbeam::channel::RecvTimeoutError::Disconnected) => {
                        CleanupWait::Disconnected
                    }
                }
            }
            PendingFencingCleanup::Runtime { completion } => {
                match completion.recv_timeout(timeout) {
                    Ok(result) => CleanupWait::RuntimeComplete(result),
                    Err(crossbeam::channel::RecvTimeoutError::Timeout) => CleanupWait::Timeout,
                    Err(crossbeam::channel::RecvTimeoutError::Disconnected) => {
                        CleanupWait::Disconnected
                    }
                }
            }
        };

        match wait_result {
            CleanupWait::KnownComplete(released) => {
                let cleanup = self.pending_cleanup.take().ok_or_else(|| {
                    MidgeError::Internal("fencing cleanup result was lost".to_string())
                })?;
                let PendingFencingCleanup::Known {
                    terminal_result, ..
                } = cleanup
                else {
                    return Err(MidgeError::Internal(
                        "fencing cleanup result kind changed while waiting".to_string(),
                    ));
                };
                terminal_result.and(released)
            }
            CleanupWait::RuntimeComplete(result) => {
                self.pending_cleanup.take();
                result
            }
            CleanupWait::Timeout => Err(MidgeError::Timeout(
                "fencing cleanup did not complete before shutdown deadline".to_string(),
            )),
            CleanupWait::Disconnected => {
                self.pending_cleanup.take();
                Err(MidgeError::Internal(
                    "fencing cleanup reaper terminated without reporting completion".to_string(),
                ))
            }
        }
    }
}

#[cfg(test)]
mod tests {
    use super::LeaseState;
    use crate::lease::PrimaryLease;
    use std::sync::Arc;

    struct TestLease;

    impl PrimaryLease for TestLease {
        fn try_acquire(
            self: Arc<Self>,
        ) -> Result<crate::lease::LeaseGuard, crate::lease::LeaseError> {
            Ok(crate::lease::LeaseGuard::token())
        }

        fn renew(&self) -> Result<(), crate::lease::LeaseError> {
            Ok(())
        }

        fn release(&self) -> Result<(), crate::lease::LeaseError> {
            Ok(())
        }

        fn ttl(&self) -> std::time::Duration {
            std::time::Duration::from_secs(30)
        }

        fn holder_id(&self) -> String {
            "test-holder".to_string()
        }

        fn epoch(&self) -> u64 {
            1
        }
    }

    #[test]
    fn should_move_fencing_resources_when_cleanup_begins() {
        // Arrange
        let lease: Arc<dyn PrimaryLease> = Arc::new(TestLease);
        let heartbeat = crate::lease::LeaseHeartbeat::new(Arc::clone(&lease));
        let mut state = LeaseState::new(lease, crate::lease::LeaseGuard::token(), heartbeat);

        // Act
        let resources = state.take_resources();

        // Assert
        assert!(!state.has_resources());
        assert!(resources.0.is_some());
        assert!(resources.1.is_some());
        assert!(resources.2.is_some());
    }

    #[test]
    fn should_restore_fencing_resources_when_reaper_spawn_fails() {
        // Arrange
        let lease: Arc<dyn PrimaryLease> = Arc::new(TestLease);
        let heartbeat = crate::lease::LeaseHeartbeat::new(Arc::clone(&lease));
        let mut state = LeaseState::new(lease, crate::lease::LeaseGuard::token(), heartbeat);

        // Act
        let result = state.schedule_cleanup_with_spawner(Ok(()), |_, _| {
            Err(std::io::Error::other("injected spawn failure"))
        });

        // Assert
        assert!(result.is_err());
        assert!(state.has_resources());
        assert!(state.pending_cleanup.is_none());
        let (heartbeat, lease, guard) = state.take_resources();
        LeaseState::release_fencing_parts(heartbeat, lease, guard)
            .expect("restored lease can be released");
    }
}
