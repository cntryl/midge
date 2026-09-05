//! Runtime worker ownership and startup/shutdown lifecycle.

use super::{
    snapshot_cache, snapshot_pins, EventLoop, ResponseRouter, RuntimeConfig, RuntimeHandle,
    RuntimeLifecycle, RuntimeMsg, RuntimeState,
};
use crate::common::{MidgeError, MidgeResult};
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
            ingest_active: Arc::new(std::sync::atomic::AtomicBool::new(false)),
            snapshot_cache,
            snapshot_pins,
            diagnostics: Arc::clone(&diagnostics),
            storage_budget: None,
            sst_read_fs: None,
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
        };

        (runtime, handle)
    }

    /// Start the runtime event loop with explicit configuration.
    ///
    /// Returns the Runtime (which owns the thread) and a handle for submitting work.
    pub fn start_with_config(
        mut self,
        mut state: RuntimeState,
        config: RuntimeConfig,
    ) -> MidgeResult<(Self, RuntimeHandle)> {
        let trace_enabled = self.trace_enabled;
        let router = self.router.clone();
        let runtime_response_timeout = config.runtime_response_timeout;

        // Channel to signal successful event loop initialization
        let (init_tx, init_rx) = channel::bounded::<Result<(), String>>(1);

        let snapshot_cache = Arc::new(snapshot_cache::SnapshotCache::new());
        let ingest_active = Arc::clone(&state.ingest_active);
        let snapshot_pins = Arc::clone(&state.snapshot_pins);
        state.diagnostics = Arc::clone(&self.diagnostics);
        let lifecycle = Arc::clone(&self.lifecycle);
        let lifecycle_for_thread = Arc::clone(&lifecycle);

        // Handle for callers to use.
        let handle = RuntimeHandle {
            msg_tx: self.msg_tx.clone(),
            router: router.clone(),
            ingest_active,
            snapshot_cache: snapshot_cache.clone(),
            snapshot_pins,
            diagnostics: Arc::clone(&self.diagnostics),
            storage_budget: config.hybrid_storage.clone(),
            sst_read_fs: config.sst_read_fs.clone(),
            lifecycle,
            runtime_response_timeout,
        };

        let msg_tx_for_loop = self.msg_tx.clone();
        let msg_rx =
            std::mem::replace(&mut self.msg_rx, channel::bounded(RUNTIME_QUEUE_CAPACITY).1);
        let router_for_thread = router.clone();

        let event_loop_handle = thread::Builder::new()
            .name("midge-runtime".to_string())
            .spawn(move || {
                match EventLoop::new(state, trace_enabled, router, config, Some(msg_tx_for_loop)) {
                    Ok(mut event_loop) => {
                        // Share the snapshot cache with the event loop
                        event_loop.set_snapshot_cache(snapshot_cache);
                        event_loop.schedule_background_compaction_on_startup();
                        lifecycle_for_thread.mark_running();
                        // Signal successful initialization
                        let _ = init_tx.send(Ok(()));
                        let run_result =
                            std::panic::catch_unwind(std::panic::AssertUnwindSafe(|| {
                                event_loop.run(&msg_rx);
                            }));
                        if run_result.is_err() {
                            tracing::error!("Runtime event loop panicked");
                            // Close the submission gate before failing pending
                            // requests. On the clean shutdown path `begin_shutdown`
                            // already drains in-flight submissions before
                            // `fail_all` runs, but a spontaneous panic leaves the
                            // lifecycle Open, so a caller could register after
                            // `fail_all` snapshotted the pending table and then
                            // wait out its full response timeout.
                            lifecycle_for_thread.begin_shutdown();
                            router_for_thread
                                .fail_all("runtime event loop panicked before responding");
                        }
                        // Drop actors and their worker handles before publishing Closed.
                        // Engine fencing resources are retained until this point.
                        drop(event_loop);
                    }
                    Err(e) => {
                        let msg = format!("Failed to create event loop: {e}");
                        tracing::error!("{}", msg);
                        // Signal initialization failure
                        let _ = init_tx.send(Err(msg));
                    }
                }
                lifecycle_for_thread.mark_closed();
            })
            .map_err(|e| MidgeError::Internal(format!("Failed to spawn runtime thread: {e}")))?;

        self.event_loop_handle = Some(event_loop_handle);

        // Wait for event loop initialization to complete
        match init_rx.recv() {
            Ok(Ok(())) => Ok((self, handle)),
            Ok(Err(e)) => Err(MidgeError::Internal(e)),
            Err(_) => Err(MidgeError::Internal(
                "Runtime initialization channel closed unexpectedly".to_string(),
            )),
        }
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

// Note: Runtime does not implement Default because it returns (Runtime, RuntimeHandle).
// Use Runtime::new() directly.
