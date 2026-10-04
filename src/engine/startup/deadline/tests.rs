use super::*;
use crate::engine::{TransactionMode, WriteOptions};
use std::sync::atomic::{AtomicUsize, Ordering};
use tracing_subscriber::layer::{Context, SubscriberExt};
use tracing_subscriber::registry::LookupSpan;
use tracing_subscriber::Layer;

#[derive(Debug, Default)]
struct Events {
    prepared: AtomicUsize,
    admitted: AtomicUsize,
    aborted: AtomicUsize,
    successful_cleanup: AtomicUsize,
}

impl StartupObserver for Events {
    fn observe(&self, event: StartupEvent) {
        let counter = match event {
            StartupEvent::RuntimePrepared => &self.prepared,
            StartupEvent::RuntimeAdmitted => &self.admitted,
            StartupEvent::RuntimeAborted => &self.aborted,
            StartupEvent::CleanupFinished { successful: true } => &self.successful_cleanup,
            StartupEvent::LeaseAcquired { .. }
            | StartupEvent::CleanupFinished { successful: false } => return,
        };
        counter.fetch_add(1, Ordering::AcqRel);
    }
}

struct ReadyWorker {
    directory: tempfile::TempDir,
    options: OpenOptions,
    slot: ReadySlot,
    scope: DeadlineScope,
    start: Instant,
    decision: Option<Sender<()>>,
    worker: Option<std::thread::JoinHandle<()>>,
    events: Arc<Events>,
}

impl ReadyWorker {
    fn start() -> Self {
        let directory = tempfile::tempdir().unwrap();
        let events = Arc::new(Events::default());
        let options = OpenOptions::local(directory.path())
            .background_compaction(false)
            .startup_observer_for_testing(events.clone())
            .build()
            .unwrap();
        let start = Instant::now();
        let scope =
            DeadlineScope::new(OperationDeadline::from_start(start, Duration::from_secs(5)));
        let slot: ReadySlot = Arc::new(Mutex::new(None));
        let (ready_tx, ready_rx) = channel::bounded(1);
        let (decision, decision_rx) = channel::bounded(1);
        let worker_options = options.clone();
        let worker_slot = Arc::clone(&slot);
        let worker_scope = scope.clone();
        let worker = std::thread::spawn(move || {
            run_worker(
                &worker_options,
                &worker_scope,
                start,
                &worker_slot,
                &ready_tx,
                &decision_rx,
            );
        });
        let mut fixture = Self {
            directory,
            options,
            slot,
            scope,
            start,
            decision: Some(decision),
            worker: Some(worker),
            events,
        };
        if let Err(error) = ready_rx.recv_timeout(Duration::from_secs(5)) {
            fixture.finish();
            panic!("actual startup owner failed to report readiness: {error}");
        }
        fixture
    }

    fn lease(&self) -> Arc<dyn crate::lease::PrimaryLease> {
        let slot = self.slot.lock().unwrap();
        match slot.as_ref() {
            Some(Ok(prepared)) => Arc::clone(prepared.engine.lease_state.lease.as_ref().unwrap()),
            Some(Err(error)) => panic!("genuine startup preparation failed: {error}"),
            None => panic!("actual prepared slot was empty"),
        }
    }

    fn finish(&mut self) {
        drop(self.decision.take());
        if let Some(worker) = self.worker.take() {
            worker.join().unwrap();
        }
    }
}

impl Drop for ReadyWorker {
    fn drop(&mut self) {
        self.finish();
    }
}

fn assert_released_owner(
    record: Option<&crate::lease::LeaderRecord>,
    owner: &crate::lease::LeaderRecord,
) {
    assert_eq!(
        record,
        Some(&crate::lease::LeaderRecord {
            acquired_at: "1970-01-01T00:00:00Z".into(),
            ..owner.clone()
        }),
        "conditional release must preserve the original epoch floor"
    );
}

#[test]
fn should_clean_actual_ready_engine_when_cancellation_wins_before_acceptance() {
    // Arrange: the real startup owner assembled an Engine and awaits a decision.
    let mut fixture = ReadyWorker::start();
    let lease = fixture.lease();
    let store = lease.get_leader_store().unwrap();
    let held_owner = store.read_current().unwrap().unwrap();

    // Act: a cancelled caller cannot take a genuine fully assembled payload.
    fixture.scope.cancel();
    let result = accept_ready(
        &fixture.slot,
        &fixture.scope,
        fixture.decision.as_ref().unwrap(),
        fixture.start,
    );
    let retained = fixture.slot.lock().unwrap().is_some();
    let timeout = match result {
        Err(MidgeError::Timeout(_)) => true,
        Ok(mut engine) => {
            engine.shutdown(Duration::from_secs(5)).unwrap();
            false
        }
        Err(error) => panic!("ready cancellation lost its timeout type: {error}"),
    };
    fixture.finish();
    let released_owner = store.read_current().unwrap();

    // Assert: the owned Runtime is joined before conditional lease cleanup ends.
    assert!(timeout);
    assert!(retained);
    assert_released_owner(released_owner.as_ref(), &held_owner);
    assert_eq!(lease.epoch(), 0);
    assert!(fixture.slot.lock().unwrap().is_none());
    assert_eq!(fixture.events.prepared.load(Ordering::Acquire), 1);
    assert_eq!(fixture.events.admitted.load(Ordering::Acquire), 0);
    assert_eq!(fixture.events.aborted.load(Ordering::Acquire), 1);
    assert_eq!(fixture.events.successful_cleanup.load(Ordering::Acquire), 1);
    let mut reopened = Engine::open(fixture.options.clone()).unwrap();
    let reacquired_owner = store.read_current().unwrap().unwrap();
    reopened.shutdown(Duration::from_secs(5)).unwrap();
    assert!(reacquired_owner.epoch > held_owner.epoch);
    assert_ne!(reacquired_owner.acquired_at, "1970-01-01T00:00:00Z");
    assert_released_owner(store.read_current().unwrap().as_ref(), &reacquired_owner);
    assert!(fixture.directory.path().join("FORMAT").is_file());
}

#[test]
fn should_keep_actual_ready_engine_when_acceptance_wins_before_cancellation() {
    // Arrange
    let mut fixture = ReadyWorker::start();
    let lease = fixture.lease();
    let store = lease.get_leader_store().unwrap();

    // Act: the caller takes ownership; the worker finds no payload to clean.
    let mut engine = accept_ready(
        &fixture.slot,
        &fixture.scope,
        fixture.decision.as_ref().unwrap(),
        fixture.start,
    )
    .unwrap();
    fixture.scope.cancel();
    fixture.finish();
    let cleanup_before_shutdown = fixture.events.successful_cleanup.load(Ordering::Acquire);
    let owner_before_shutdown = store.read_current().unwrap().unwrap();
    let cf = engine.get_column_family("default").unwrap();
    let mut write = engine
        .begin_tx(cf.id(), TransactionMode::ReadWrite)
        .unwrap();
    write
        .put(b"acknowledged".to_vec(), b"value".to_vec(), None)
        .unwrap();
    write.commit(WriteOptions::sync()).unwrap();
    engine.flush_cf(&cf).unwrap();
    engine.shutdown(Duration::from_secs(5)).unwrap();
    let released_first_owner = store.read_current().unwrap();
    let mut reopened = Engine::open(fixture.options.clone()).unwrap();
    let reacquired_owner = store.read_current().unwrap().unwrap();
    let cf = reopened.get_column_family("default").unwrap();
    let read = reopened
        .begin_tx(cf.id(), TransactionMode::ReadOnly)
        .unwrap();
    let value = read.get(b"acknowledged").unwrap();
    drop(read);
    reopened.shutdown(Duration::from_secs(5)).unwrap();

    // Assert: late cancellation leaves normal operations and exact rows intact.
    assert!(!fixture.scope.deadline().is_bounded());
    assert!(fixture.slot.lock().unwrap().is_none());
    assert_released_owner(released_first_owner.as_ref(), &owner_before_shutdown);
    assert!(reacquired_owner.epoch > owner_before_shutdown.epoch);
    assert_ne!(reacquired_owner.acquired_at, "1970-01-01T00:00:00Z");
    assert_eq!(cleanup_before_shutdown, 0);
    assert_eq!(fixture.events.aborted.load(Ordering::Acquire), 0);
    assert_eq!(fixture.events.admitted.load(Ordering::Acquire), 2);
    assert_eq!(value, Some(bytes::Bytes::from_static(b"value")));
    assert_released_owner(store.read_current().unwrap().as_ref(), &reacquired_owner);
}

#[derive(Default)]
struct PhaseFields {
    phase: Option<String>,
}

impl tracing::field::Visit for PhaseFields {
    fn record_str(&mut self, field: &tracing::field::Field, value: &str) {
        if field.name() == "phase" {
            self.phase = Some(value.to_string());
        }
    }

    fn record_debug(&mut self, _: &tracing::field::Field, _: &dyn std::fmt::Debug) {}
}

struct ObservedPhase {
    thread: std::thread::ThreadId,
    caller_parent: bool,
}

#[derive(Clone)]
struct PhaseSubscriber(Arc<Mutex<Vec<ObservedPhase>>>);

impl<S> Layer<S> for PhaseSubscriber
where
    S: tracing::Subscriber + for<'span> LookupSpan<'span>,
{
    fn on_event(&self, event: &tracing::Event<'_>, context: Context<'_, S>) {
        if event.metadata().target() != "midge::recovery" {
            return;
        }
        let mut fields = PhaseFields::default();
        event.record(&mut fields);
        if fields.phase.as_deref() != Some("storage_materialization") {
            return;
        }
        let caller_parent = context.event_scope(event).is_some_and(|scope| {
            scope
                .from_root()
                .any(|span| span.name() == "startup-original-caller")
        });
        self.0.lock().unwrap().push(ObservedPhase {
            thread: std::thread::current().id(),
            caller_parent,
        });
    }
}

#[test]
fn should_inherit_actual_caller_tracing_context_when_timed_startup_uses_worker() {
    // Arrange: install only a scoped subscriber; no process default is changed.
    let directory = tempfile::tempdir().unwrap();
    let observations = Arc::new(Mutex::new(Vec::new()));
    let subscriber =
        tracing_subscriber::registry().with(PhaseSubscriber(Arc::clone(&observations)));
    let options = OpenOptions::local(directory.path())
        .background_compaction(false)
        .open_timeout(Duration::from_secs(5))
        .build()
        .unwrap();
    let caller = std::thread::current().id();

    // Act: the actual storage phases execute on the owned startup thread.
    let mut engine = tracing::subscriber::with_default(subscriber, || {
        tracing::info_span!("startup-original-caller").in_scope(|| Engine::open(options))
    })
    .unwrap();
    engine.shutdown(Duration::from_secs(5)).unwrap();

    // Assert: positive phase evidence came from the worker through this caller.
    let observed = observations.lock().unwrap();
    assert!(
        observed.len() >= 2,
        "actual phase start/end must reach the scoped subscriber"
    );
    assert!(observed.iter().all(|phase| phase.thread != caller));
    assert!(observed.iter().all(|phase| phase.caller_parent));
}
