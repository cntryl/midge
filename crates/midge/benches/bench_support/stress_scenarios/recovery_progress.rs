//! Bind completed recovery tracing work to its current stress workload.

use cntryl_stress::ProgressHandle;
use std::cell::RefCell;
use std::marker::PhantomData;
use std::rc::Rc;
use std::sync::atomic::{AtomicU64, Ordering};
use std::sync::Arc;
use tracing::field::{Field, Visit};
use tracing::{Event, Metadata, Subscriber};
use tracing_subscriber::layer::{Context, Layer};

const WORK_TARGET: &str = "midge::recovery::work";

struct ActiveRecovery {
    progress: Arc<dyn Fn() + Send + Sync>,
    phase: Arc<AtomicU64>,
    generation: u64,
}

thread_local! {
    static ACTIVE_RECOVERY: RefCell<Option<ActiveRecovery>> = const { RefCell::new(None) };
}

/// A scope follows the recovery caller, never its background workers. Phase
/// generation changes invalidate it even before the caller returns.
pub(super) struct RecoveryScope {
    previous: Option<ActiveRecovery>,
    _caller_thread: PhantomData<Rc<()>>,
}

impl RecoveryScope {
    pub(super) fn enter(
        progress: &ProgressHandle,
        phase: &Arc<AtomicU64>,
        phase_name: &str,
    ) -> Self {
        let progress = progress.clone();
        Self::enter_with_completion(Arc::new(move || progress.advance()), phase, phase_name)
    }

    pub(super) fn enter_with_completion(
        progress: Arc<dyn Fn() + Send + Sync>,
        phase: &Arc<AtomicU64>,
        phase_name: &str,
    ) -> Self {
        let generation = phase.load(Ordering::Acquire);
        let active = (phase_name == "recovery" && generation & 1 != 0).then(|| ActiveRecovery {
            progress,
            phase: Arc::clone(phase),
            generation,
        });
        let previous = ACTIVE_RECOVERY.with(|current| current.replace(active));
        Self {
            previous,
            _caller_thread: PhantomData,
        }
    }
}

impl Drop for RecoveryScope {
    fn drop(&mut self) {
        ACTIVE_RECOVERY.with(|current| current.replace(self.previous.take()));
    }
}

pub(super) struct RecoveryProgressLayer;

pub(super) fn is_recovery_work(metadata: &Metadata<'_>) -> bool {
    metadata.target() == WORK_TARGET
}

#[derive(Default)]
struct CompletedWork {
    marked: bool,
    failed: Option<bool>,
    bytes: u64,
    frames: u64,
    operations: u64,
}

impl CompletedWork {
    fn successful(&self) -> bool {
        self.marked
            && self.failed == Some(false)
            && (self.bytes > 0 || self.frames > 0 || self.operations > 0)
    }
}

impl Visit for CompletedWork {
    fn record_bool(&mut self, field: &Field, value: bool) {
        match field.name() {
            "recovery_work_completed" => self.marked = value,
            "failed" => self.failed = Some(value),
            _ => {}
        }
    }

    fn record_u64(&mut self, field: &Field, value: u64) {
        match field.name() {
            "completed_bytes" => self.bytes = value,
            "completed_frames" => self.frames = value,
            "completed_operations" => self.operations = value,
            _ => {}
        }
    }

    fn record_debug(&mut self, _field: &Field, _value: &dyn std::fmt::Debug) {}
}

impl<S: Subscriber> Layer<S> for RecoveryProgressLayer {
    fn on_event(&self, event: &Event<'_>, _context: Context<'_, S>) {
        if !is_recovery_work(event.metadata()) {
            return;
        }
        let mut work = CompletedWork::default();
        event.record(&mut work);
        if !work.successful() {
            return;
        }
        let progress = ACTIVE_RECOVERY.with(|current| {
            current.borrow().as_ref().and_then(|active| {
                (active.phase.load(Ordering::Acquire) == active.generation)
                    .then(|| active.progress.clone())
            })
        });
        if let Some(progress) = progress {
            progress();
        }
    }
}
