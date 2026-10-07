//! Runtime worker ownership and startup/shutdown lifecycle.

use super::{
    snapshot_cache, snapshot_pins, EventLoop, ResponseRouter, RuntimeConfig, RuntimeHandle,
    RuntimeLifecycle, RuntimeMsg, RuntimeState, StartupAdmission,
};
use crate::common::{DeadlineScope, MidgeError, MidgeResult, OperationDeadline};
use crossbeam::channel::{self, Receiver, Sender};
use std::sync::{atomic::Ordering, Arc};
use std::thread::{self, JoinHandle};
use std::time::Duration;

pub(crate) const RUNTIME_QUEUE_CAPACITY: usize = 1000;

/// Main runtime for background operations.
///
/// Owns all mutable engine state and coordinates actors via message passing.
/// All background work flows through this runtime and its event loop thread.
pub struct Runtime {
    /// Message channel sender (for handle).
    msg_tx: Sender<RuntimeMsg>,
    /// Message channel receiver (for event loop).
    msg_rx: Receiver<RuntimeMsg>,
    /// Event loop thread handle.
    event_loop_handle: Option<JoinHandle<()>>,
    /// Whether tracing is enabled.
    trace_enabled: bool,
    /// Response router shared between handle and event loop.
    router: Arc<ResponseRouter>,
    diagnostics: Arc<crate::diagnostics::RuntimeDiagnostics>,
    lifecycle: Arc<RuntimeLifecycle>,
    startup_admission: Option<StartupAdmission>,
}

impl Runtime {
    /// Create a new runtime and a corresponding handle for submitting work.
    pub fn new() -> (Self, RuntimeHandle) {
        Self::create(crate::config::DEFAULT_RUNTIME_RESPONSE_TIMEOUT)
    }

    #[cfg(test)]
    pub(crate) fn new_with_response_timeout(
        runtime_response_timeout: Duration,
    ) -> (Self, RuntimeHandle) {
        Self::create(runtime_response_timeout)
    }

    fn create(runtime_response_timeout: Duration) -> (Self, RuntimeHandle) {
        let (msg_tx, msg_rx) = channel::bounded(RUNTIME_QUEUE_CAPACITY);
        let router = Arc::new(ResponseRouter::new());

        let trace_enabled = std::env::var("MIDGE_TRACE_RUNTIME")
            .is_ok_and(|v| matches!(v.to_ascii_lowercase().as_str(), "1" | "true" | "yes"));

        let snapshot_cache = Arc::new(snapshot_cache::SnapshotCache::new());
        let snapshot_pins = Arc::new(snapshot_pins::SnapshotPinRegistry::default());
        let diagnostics = Arc::new(crate::diagnostics::RuntimeDiagnostics::default());
        let lifecycle = Arc::new(RuntimeLifecycle::new());

        let handle = RuntimeHandle {
            msg_tx: msg_tx.clone(),
            router: router.clone(),
            snapshot_cache,
            snapshot_pins,
            diagnostics: Arc::clone(&diagnostics),
            storage_budget: None,
            sst_read_fs: None,
            read_authority: None,
            lifecycle: Arc::clone(&lifecycle),
            runtime_response_timeout,
        };

        let runtime = Self {
            msg_tx,
            msg_rx,
            event_loop_handle: None,
            trace_enabled,
            router,
            diagnostics,
            lifecycle,
            startup_admission: None,
        };

        (runtime, handle)
    }

    /// Start the runtime event loop with explicit configuration.
    ///
    /// Returns the Runtime (which owns the thread) and a handle for submitting work.
    pub fn start_with_config(
        self,
        state: RuntimeState,
        config: RuntimeConfig,
    ) -> MidgeResult<(Self, RuntimeHandle)> {
        let (runtime, handle, admission) = self.prepare_with_config(
            state,
            config,
            DeadlineScope::new(OperationDeadline::unbounded()),
        )?;
        admission.accept()?;
        Ok((runtime, handle))
    }

    /// Initialize actors while their owner retains the admission decision.
    pub(crate) fn prepare_with_config(
        mut self,
        state: RuntimeState,
        mut config: RuntimeConfig,
        scope: DeadlineScope,
    ) -> MidgeResult<(Self, RuntimeHandle, StartupAdmission)> {
        scope.check("runtime creation")?;
        config.startup_scope = Some(scope.clone());
        let trace_enabled = self.trace_enabled;
        let router = self.router.clone();
        let runtime_response_timeout = config.runtime_response_timeout;

        // Channel to signal successful event loop initialization
        let (init_tx, init_rx) = channel::bounded::<MidgeResult<()>>(1);

        let snapshot_cache = Arc::new(snapshot_cache::SnapshotCache::new());
        let snapshot_pins = Arc::clone(&state.snapshot_pins);
        // Startup recovery has already recorded counters into this diagnostics
        // instance. Hand the same instance to runtime handles and actors.
        self.diagnostics = Arc::clone(&state.diagnostics);
        let lifecycle = Arc::clone(&self.lifecycle);
        let lifecycle_for_thread = Arc::clone(&lifecycle);
        let preparation_scope = scope.clone();
        let admission = StartupAdmission::new(scope, Arc::clone(&lifecycle), &config);
        self.startup_admission = Some(admission.clone());

        // Handle for callers to use.
        let handle = RuntimeHandle {
            msg_tx: self.msg_tx.clone(),
            router: router.clone(),
            snapshot_cache: snapshot_cache.clone(),
            snapshot_pins,
            diagnostics: Arc::clone(&self.diagnostics),
            storage_budget: config.hybrid_storage.clone(),
            sst_read_fs: config.sst_read_fs.clone(),
            read_authority: super::handle::CloudReadAuthority::from_config(&config),
            lifecycle,
            runtime_response_timeout,
        };

        let (worker_msg_tx, worker_msg_rx) = channel::unbounded();
        let msg_rx =
            std::mem::replace(&mut self.msg_rx, channel::bounded(RUNTIME_QUEUE_CAPACITY).1);
        let router_for_thread = router.clone();

        let startup = WorkerStartup {
            state,
            config,
            trace_enabled,
            router,
            router_for_thread,
            snapshot_cache,
            lifecycle: lifecycle_for_thread,
            worker_msg_tx,
            worker_msg_rx,
            msg_rx,
            init_tx,
            admission: admission.clone(),
        };
        let event_loop_handle = thread::Builder::new()
            .name("midge-runtime".to_string())
            .spawn(move || startup.run())
            .map_err(|e| MidgeError::Internal(format!("Failed to spawn runtime thread: {e}")))?;

        self.event_loop_handle = Some(event_loop_handle);

        wait_for_preparation(&init_rx, &preparation_scope)?;
        preparation_scope.check("runtime preparation receipt")?;
        Ok((self, handle, admission))
    }

    fn join_until(&mut self, timeout: Duration, context: &str) -> bool {
        let Some(handle) = self.event_loop_handle.as_ref() else {
            self.lifecycle.mark_closed();
            return true;
        };
        let started = std::time::Instant::now();
        while !handle.is_finished() {
            if started.elapsed() >= timeout {
                return false;
            }
            std::thread::sleep(Duration::from_millis(1));
        }

        if let Some(handle) = self.event_loop_handle.take() {
            if handle.join().is_ok() {
                tracing::debug!("Runtime {} completed cleanly", context);
            } else {
                tracing::warn!("Runtime thread panicked during {}", context);
            }
        }
        self.lifecycle.mark_closed();
        self.router
            .fail_all("runtime event loop terminated before responding");
        true
    }

    pub(crate) fn wait_for_exit(&mut self, timeout: Duration) -> bool {
        self.join_until(timeout, "shutdown")
    }

    fn shutdown_inner(&mut self, context: &str) {
        if let Some(admission) = &self.startup_admission {
            admission.cancel();
        }
        self.lifecycle.begin_shutdown();
        self.lifecycle.wait_for_transactions();
        if self.event_loop_handle.is_some()
            && self.lifecycle.running.load(Ordering::Acquire)
            && self.msg_tx.send(RuntimeMsg::Shutdown).is_err()
        {
            tracing::debug!("Runtime {}: shutdown message send failed", context);
        }
        let _ = self.join_until(Duration::MAX, context);
    }

    /// Shutdown the runtime and wait for completion.
    #[cfg(test)]
    pub fn shutdown(mut self) {
        self.shutdown_inner("shutdown");
    }
}

impl Drop for Runtime {
    fn drop(&mut self) {
        self.shutdown_inner("drop");
    }
}

struct WorkerStartup {
    state: RuntimeState,
    config: RuntimeConfig,
    trace_enabled: bool,
    router: Arc<ResponseRouter>,
    router_for_thread: Arc<ResponseRouter>,
    snapshot_cache: Arc<snapshot_cache::SnapshotCache>,
    lifecycle: Arc<RuntimeLifecycle>,
    worker_msg_tx: Sender<RuntimeMsg>,
    worker_msg_rx: Receiver<RuntimeMsg>,
    msg_rx: Receiver<RuntimeMsg>,
    init_tx: Sender<MidgeResult<()>>,
    admission: StartupAdmission,
}

impl WorkerStartup {
    fn run(self) {
        let Self {
            state,
            config,
            trace_enabled,
            router,
            router_for_thread,
            snapshot_cache,
            lifecycle,
            worker_msg_tx,
            worker_msg_rx,
            msg_rx,
            init_tx,
            admission,
        } = self;
        match EventLoop::new(
            state,
            trace_enabled,
            router,
            config,
            crate::runtime::event_loop::FlushWorkerMode::Background(worker_msg_tx),
        ) {
            Ok(mut event_loop) => {
                event_loop.set_snapshot_cache(snapshot_cache);
                match admission.prepared() {
                    Ok(()) => {
                        let _ = init_tx.send(Ok(()));
                        if admission.wait_until_accepted() {
                            event_loop.schedule_background_compaction_on_startup();
                            run_event_loop(
                                &mut event_loop,
                                &msg_rx,
                                &worker_msg_rx,
                                &lifecycle,
                                &router_for_thread,
                            );
                        } else {
                            admission.aborted();
                        }
                    }
                    Err(error) => {
                        let _ = init_tx.send(Err(error));
                        admission.aborted();
                    }
                }
                drop(event_loop);
            }
            Err(error) => {
                tracing::error!(%error, "Failed to create event loop");
                let _ = init_tx.send(Err(error));
                admission.aborted();
            }
        }
        lifecycle.mark_closed();
    }
}

fn wait_for_preparation(
    init_rx: &Receiver<MidgeResult<()>>,
    scope: &DeadlineScope,
) -> MidgeResult<()> {
    let deadline = scope.deadline();
    let ready = if deadline.is_bounded() {
        init_rx.recv_timeout(deadline.remaining()).map_err(|error| {
            if error.is_timeout() {
                MidgeError::Timeout("runtime preparation exceeded the open deadline".into())
            } else {
                MidgeError::Internal("runtime initialization channel closed unexpectedly".into())
            }
        })
    } else {
        init_rx.recv().map_err(|_| {
            MidgeError::Internal("runtime initialization channel closed unexpectedly".into())
        })
    };
    ready?
}

fn run_event_loop(
    event_loop: &mut EventLoop,
    msg_rx: &Receiver<RuntimeMsg>,
    worker_msg_rx: &Receiver<RuntimeMsg>,
    lifecycle: &RuntimeLifecycle,
    router: &ResponseRouter,
) {
    let result = std::panic::catch_unwind(std::panic::AssertUnwindSafe(|| {
        event_loop.run(msg_rx, worker_msg_rx);
    }));
    if result.is_err() {
        tracing::error!("Runtime event loop panicked");
        // Close submissions before snapshotting and failing pending routes.
        lifecycle.begin_shutdown();
        router.fail_all("runtime event loop panicked before responding");
    }
}

// Note: Runtime does not implement Default because it returns (Runtime, RuntimeHandle).
// Use Runtime::new() directly.
