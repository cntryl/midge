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
    progress: ProgressHandle,
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
        let generation = phase.load(Ordering::Acquire);
        let active = (phase_name == "recovery" && generation & 1 != 0).then(|| ActiveRecovery {
            progress: progress.clone(),
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
            progress.advance();
        }
    }
}

#[cfg(test)]
fn emit_work(bytes: u64, frames: u64, operations: u64, marked: bool, failed: bool) {
    tracing::debug!(
        target: WORK_TARGET,
        recovery_work_completed = marked,
        stage = "fixture",
        completed_bytes = bytes,
        completed_frames = frames,
        completed_operations = operations,
        failed,
        "fixture recovery work"
    );
}

#[cfg(test)]
fn assert_event_filtering(progress: &ProgressHandle) {
    // Arrange: the independent progress layer stays active with fmt disabled.
    let phase = Arc::new(AtomicU64::new(3));
    let before = progress.completed_units();
    let _scope = RecoveryScope::enter(progress, &phase, "recovery");

    // Act: bytes, frames, and operations are each meaningful completed work.
    emit_work(1, 0, 0, true, false);
    emit_work(0, 1, 0, true, false);
    emit_work(0, 0, 1, true, false);

    // Assert
    assert_eq!(progress.completed_units(), before + 3);

    // Act: incomplete, failed, zero and unrelated events are diagnostics.
    emit_work(1, 0, 0, false, false);
    emit_work(1, 0, 0, true, true);
    emit_work(0, 0, 0, true, false);
    tracing::info!(target: WORK_TARGET, completed_bytes = 1u64, failed = false, "phase entry");
    tracing::info!(target: WORK_TARGET, recovery_work_completed = true,
        completed_bytes = 1u64, "missing success classification");
    tracing::info!(target: "midge::recovery", recovery_work_completed = true,
        completed_bytes = 1u64, failed = false, "phase completion");
    tracing::info!(target: "midge::lease", recovery_work_completed = true,
        completed_bytes = 1u64, failed = false, "lease retry");

    // Assert
    assert_eq!(progress.completed_units(), before + 3);
}

#[cfg(test)]
fn assert_scope_isolation(progress: &ProgressHandle) {
    // Arrange
    let before = progress.completed_units();
    let phase = Arc::new(AtomicU64::new(3));

    // Act / Assert: no active scope, a client phase, and an ended scope ignore work.
    emit_work(1, 0, 0, true, false);
    assert_eq!(progress.completed_units(), before);
    {
        let _scope = RecoveryScope::enter(progress, &phase, "workload");
        emit_work(1, 0, 0, true, false);
    }
    assert_eq!(progress.completed_units(), before);
    {
        let _scope = RecoveryScope::enter(progress, &phase, "recovery");
        emit_work(1, 0, 0, true, false);
    }
    emit_work(1, 0, 0, true, false);
    assert_eq!(progress.completed_units(), before + 1);

    // Act / Assert: changing the recorded phase invalidates its old scope.
    {
        let _scope = RecoveryScope::enter(progress, &phase, "recovery");
        phase.store(5, Ordering::Release);
        emit_work(1, 0, 0, true, false);
    }
    assert_eq!(progress.completed_units(), before + 1);

    // Act / Assert: nested phases restore the previous recovery caller.
    {
        let _outer = RecoveryScope::enter(progress, &phase, "recovery");
        {
            let _inner = RecoveryScope::enter(progress, &phase, "workload");
            emit_work(1, 0, 0, true, false);
        }
        emit_work(1, 0, 0, true, false);
        let dispatch = tracing::dispatcher::get_default(Clone::clone);
        std::thread::spawn(move || {
            tracing::dispatcher::with_default(&dispatch, || emit_work(1, 0, 0, true, false));
        })
        .join()
        .expect("join background recovery event fixture");
    }
    assert_eq!(progress.completed_units(), before + 2);
}

#[cfg(test)]
pub(super) fn assert_listener_isolation(progress: &ProgressHandle) {
    use tracing_subscriber::layer::SubscriberExt;

    // Arrange: logging is fully disabled, while the separately filtered layer
    // accepts genuine completed work from the active recovery caller.
    let subscriber = tracing_subscriber::registry()
        .with(
            RecoveryProgressLayer
                .with_filter(tracing_subscriber::filter::filter_fn(is_recovery_work)),
        )
        .with(
            tracing_subscriber::fmt::layer()
                .with_writer(std::io::sink)
                .with_filter(tracing_subscriber::EnvFilter::new("off")),
        );

    // Act / Assert: use the real event visitor, real scopes and real handle.
    tracing::subscriber::with_default(subscriber, || {
        assert_event_filtering(progress);
        assert_scope_isolation(progress);
    });
}
