//! Native aggregate startup deadlines through an explicitly required Sqrzl endpoint.

use super::*;
use cntryl_midge::__internal::startup::{StartupEvent, StartupObserver};
use cntryl_midge::MidgeError;
use std::net::SocketAddr;
use std::sync::atomic::{AtomicUsize, Ordering};
use std::sync::{Arc, Condvar, Mutex};
use std::time::Instant;

#[path = "native_startup_proxy.rs"]
pub(crate) mod proxy;
use proxy::{Control, DelayMode, LeaseCondition, NativeStartupProxy};

const OPEN_BUDGET: Duration = Duration::from_millis(600);
const IO_BUDGET: Duration = Duration::from_secs(2);
const CLEANUP_WAIT: Duration = Duration::from_secs(6);
static CASE_ID: AtomicUsize = AtomicUsize::new(0);

struct StartupTrace {
    control: Arc<Control>,
    prepared: AtomicUsize,
    admitted: AtomicUsize,
    aborted: AtomicUsize,
    cleanup_result: Mutex<Option<bool>>,
    cleanup_changed: Condvar,
    prepared_hold: Mutex<bool>,
    prepared_changed: Condvar,
}

impl StartupTrace {
    fn new(control: Arc<Control>) -> Arc<Self> {
        Arc::new(Self {
            control,
            prepared: AtomicUsize::new(0),
            admitted: AtomicUsize::new(0),
            aborted: AtomicUsize::new(0),
            cleanup_result: Mutex::new(None),
            cleanup_changed: Condvar::new(),
            prepared_hold: Mutex::new(false),
            prepared_changed: Condvar::new(),
        })
    }

    fn wait_for_successful_cleanup(&self) -> bool {
        let (result, _) = self
            .cleanup_changed
            .wait_timeout_while(
                self.cleanup_result.lock().unwrap(),
                CLEANUP_WAIT,
                |result| result.is_none(),
            )
            .unwrap();
        *result == Some(true)
    }

    fn assert_no_runtime_admission(&self) {
        assert_eq!(self.prepared.load(Ordering::Acquire), 0);
        assert_eq!(self.admitted.load(Ordering::Acquire), 0);
    }

    fn release_prepared(&self) {
        *self.prepared_hold.lock().unwrap() = false;
        self.prepared_changed.notify_all();
    }

    fn observe_prepared(&self) {
        self.prepared.fetch_add(1, Ordering::AcqRel);
        self.prepared_changed.notify_all();
        // A finite test-only hold also makes an incorrect baseline return,
        // while the RAII owner releases early on every completed caller path.
        let (mut held, _) = self
            .prepared_changed
            .wait_timeout_while(
                self.prepared_hold.lock().unwrap(),
                Duration::from_secs(2),
                |held| *held,
            )
            .unwrap();
        *held = false;
    }
}

struct PreparedHold(Arc<StartupTrace>);

impl PreparedHold {
    fn new(trace: Arc<StartupTrace>) -> Self {
        *trace.prepared_hold.lock().unwrap() = true;
        Self(trace)
    }
}

impl Drop for PreparedHold {
    fn drop(&mut self) {
        self.0.release_prepared();
    }
}

struct CommittedResponseHold(Arc<Control>);

impl Drop for CommittedResponseHold {
    fn drop(&mut self) {
        self.0.release_committed_response();
    }
}

impl std::fmt::Debug for StartupTrace {
    fn fmt(&self, formatter: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        formatter
            .debug_struct("StartupTrace")
            .finish_non_exhaustive()
    }
}

impl StartupObserver for StartupTrace {
    fn observe(&self, event: StartupEvent) {
        match event {
            StartupEvent::LeaseAcquired { epoch } => {
                self.control
                    .confirmed_lease_epoch
                    .store(epoch, Ordering::Release);
            }
            StartupEvent::RuntimePrepared => {
                self.observe_prepared();
            }
            StartupEvent::RuntimeAdmitted => {
                self.admitted.fetch_add(1, Ordering::AcqRel);
            }
            StartupEvent::RuntimeAborted => {
                self.aborted.fetch_add(1, Ordering::AcqRel);
            }
            StartupEvent::CleanupFinished { successful } => {
                *self.cleanup_result.lock().unwrap() = Some(successful);
                self.cleanup_changed.notify_all();
            }
        }
    }
}

struct Fixture {
    directory: tempfile::TempDir,
    prefix: String,
    provider: CloudProviderConfig,
    proxy: NativeStartupProxy,
}

impl Fixture {
    fn new(mode: DelayMode) -> Self {
        let endpoint = std::env::var("MIDGE_SQRZL_ENDPOINT")
            .unwrap_or_else(|_| "http://127.0.0.1:9000".to_string());
        // Fail when selected without prerequisites. Do not silently return.
        let address = endpoint
            .strip_prefix("http://")
            .expect("test Sqrzl endpoint must be plain loopback HTTP")
            .parse::<SocketAddr>()
            .expect("test Sqrzl endpoint must be a loopback socket");
        assert!(address.ip().is_loopback(), "test must use local Sqrzl");
        std::net::TcpStream::connect_timeout(&address, Duration::from_millis(200))
            .expect("selected native startup qualification requires running Sqrzl");
        let secret = std::env::var("SQRZL_SECRET_ACCESS_KEY")
            .expect("selected native startup qualification requires Sqrzl credential");
        let case = CASE_ID.fetch_add(1, Ordering::AcqRel);
        let bucket = format!("midge-open-budget-{}-{case}", std::process::id());
        super::sqrzl::ensure_sqrzl_s3_bucket_at(&bucket, &endpoint)
            .expect("create exact native fixture namespace");
        let proxy = NativeStartupProxy::start(mode, address).expect("native S3 proxy");
        let provider =
            CloudProviderConfig::s3_compatible_static(&bucket, &proxy.endpoint, "admin", secret);
        Self {
            directory: tempfile::tempdir().unwrap(),
            prefix: "db".into(),
            provider,
            proxy,
        }
    }

    fn options(&self, budget: Option<Duration>, trace: Arc<StartupTrace>) -> OpenOptions {
        let mut builder = OpenOptions::cloud(
            self.directory.path(),
            CloudStorageLocation::new(self.provider.clone(), &self.prefix),
        )
        .background_compaction(false)
        .storage_io_timeout(IO_BUDGET)
        .runtime_response_timeout(Duration::from_secs(10))
        .startup_observer_for_testing(trace);
        if let Some(budget) = budget {
            builder = builder.open_timeout(budget);
        }
        builder.build().expect("explicit startup option contract")
    }

    fn trace(&self) -> Arc<StartupTrace> {
        StartupTrace::new(Arc::clone(&self.proxy.control))
    }
}

fn observed_open(
    fixture: &Fixture,
    budget: Option<Duration>,
    trace: Arc<StartupTrace>,
) -> (Result<Engine, MidgeError>, Duration) {
    let started = Instant::now();
    let result = Engine::open(fixture.options(budget, trace));
    (result, started.elapsed())
}

fn record_rows_and_verify(engine: &Engine) {
    let cf = default_cf(engine);
    let mut write = engine
        .begin_tx(cf.id(), TransactionMode::ReadWrite)
        .unwrap();
    for index in 0..8 {
        write
            .put(
                format!("deadline-key-{index}").into_bytes(),
                format!("deadline-value-{index}").into_bytes(),
                None,
            )
            .unwrap();
    }
    write.commit(WriteOptions::cloud_strict()).unwrap();
    engine.flush_cf(&cf).unwrap();
    verify_rows(engine);
}

fn verify_rows(engine: &Engine) {
    let cf = default_cf(engine);
    let read = engine.begin_tx(cf.id(), TransactionMode::ReadOnly).unwrap();
    for index in 0..8 {
        assert_eq!(
            read.get(format!("deadline-key-{index}").as_bytes())
                .unwrap(),
            Some(Bytes::from(format!("deadline-value-{index}"))),
        );
    }
}

fn assert_fast_same_path_reopens(fixture: &Fixture) {
    fixture.proxy.control.set_mode(DelayMode::Fast);
    let bounded_trace = fixture.trace();
    let (bounded, _) = observed_open(
        fixture,
        Some(Duration::from_secs(5)),
        Arc::clone(&bounded_trace),
    );
    let mut engine =
        bounded.expect("same endpoint/cache/prefix must admit healthy subsequent open");
    assert_eq!(bounded_trace.admitted.load(Ordering::Acquire), 1);
    record_rows_and_verify(&engine);
    engine.shutdown(Duration::from_secs(5)).unwrap();

    // Reopen the small now-committed database under600ms, then make one real
    // ordinary WAL PUT last800ms <the2s runtime provider cap. Successful strict
    // commit proves the aggregate startup deadline is not retained at runtime.
    let runtime_trace = fixture.trace();
    let (runtime, _) = observed_open(fixture, Some(OPEN_BUDGET), Arc::clone(&runtime_trace));
    let mut engine = runtime.expect("small committed fast reopen under explicit deadline");
    assert_eq!(runtime_trace.admitted.load(Ordering::Acquire), 1);
    fixture.proxy.control.set_mode(DelayMode::PostOpenWalOnce);
    record_rows_and_verify(&engine);
    let requests = fixture.proxy.control.observations();
    assert!(requests.iter().all(|request| request.error.is_none()));
    assert!(requests.iter().any(|request| {
        request.delayed
            && request.method == "PUT"
            && std::path::Path::new(&request.path).extension() == Some(std::ffi::OsStr::new("wal"))
            && request.response_written
            && request.status == Some(200)
            && request.completed.duration_since(request.started) > OPEN_BUDGET
    }));
    engine.shutdown(Duration::from_secs(5)).unwrap();

    fixture.proxy.control.set_mode(DelayMode::Fast);
    let default_trace = fixture.trace();
    let options = fixture.options(None, Arc::clone(&default_trace));
    assert_eq!(
        options.open_timeout(),
        None,
        "compatibility default must remain unbounded"
    );
    let mut engine = Engine::open(options).expect("defaultNone same-path reopen");
    assert_eq!(default_trace.admitted.load(Ordering::Acquire), 1);
    verify_rows(&engine);
    engine.flush_cf(&default_cf(&engine)).unwrap();
    engine.shutdown(Duration::from_secs(5)).unwrap();
}

#[test]
#[ignore = "requires Sqrzl; selected cloud-integration qualification asserts prerequisites"]
fn should_open_through_native_startup_proxy_when_aggregate_deadline_is_absent() {
    // Arrange: qualify signed forwarding and actual namespace operations first.
    let fixture = Fixture::new(DelayMode::Fast);

    // Act: actual bounded reopen, post-open800ms WAL I/O and defaultNone reopen.
    assert_fast_same_path_reopens(&fixture);

    // Assert: the helper proves exact rows and one observed runtime admission
    // per successful open. No timeout primitive or provider response is faked.
}

#[test]
#[ignore = "requires Sqrzl; selected cloud-integration qualification asserts prerequisites"]
fn should_bound_native_epoch_discovery_when_catalog_heads_exceed_total_open_budget() {
    // Arrange: two actual absent-catalog HEADs, each350ms <2s native I/O cap.
    // Their cumulative700ms exceeds one600ms deadline captured before discovery.
    let fixture = Fixture::new(DelayMode::DiscoveryPair);
    let trace = fixture.trace();

    // Act: always shut down an unexpected baseline Engine before asserting RED.
    let (result, elapsed) = observed_open(&fixture, Some(OPEN_BUDGET), Arc::clone(&trace));
    let timeout = match result {
        Err(MidgeError::Timeout(_)) => true,
        Ok(mut engine) => {
            engine.shutdown(Duration::from_secs(5)).unwrap();
            false
        }
        Err(error) => panic!("aggregate deadline lost its type: {error:?}"),
    };
    let observed = fixture.proxy.control.wait_for_delayed_completion(2);
    let requests = fixture.proxy.control.observations();

    // Assert: native evidence, typed timeout, no lease mutation/runtime admission.
    assert!(
        timeout,
        "aggregate deadline was ignored: elapsed={elapsed:?}, native={requests:?}, admitted={}",
        trace.admitted.load(Ordering::Acquire)
    );
    assert!(
        observed,
        "both individually fast native catalog HEADs must enter"
    );
    let delayed: Vec<_> = requests.iter().filter(|request| request.delayed).collect();
    assert_eq!(delayed.len(), 2);
    assert!(delayed.iter().all(|request| request.method == "HEAD"));
    assert_eq!(delayed[0].status, Some(404));
    assert!(delayed[0].response_written);
    assert!(
        delayed[1].client_cancelled,
        "second native request must close at remaining deadline"
    );
    assert!(delayed
        .iter()
        .all(|request| request.completed.duration_since(request.started) < IO_BUDGET));
    assert!(requests.iter().all(|request| request.method != "PUT"));
    assert_eq!(
        fixture
            .proxy
            .control
            .confirmed_lease_epoch
            .load(Ordering::Acquire),
        0
    );
    trace.assert_no_runtime_admission();
    // Cooperative native fixture envelope, not a preemption promise for arbitrary local syscalls.
    assert!(
        elapsed <= OPEN_BUDGET + Duration::from_millis(400),
        "{elapsed:?}"
    );
    assert_fast_same_path_reopens(&fixture);
}

#[test]
#[ignore = "requires Sqrzl; selected cloud-integration qualification asserts prerequisites"]
fn should_keep_startup_deadline_when_mandatory_native_catalog_read_follows_acquisition() {
    // Arrange: discovery/acquisition are real and fast. A scoped lease event
    // arms only a subsequent mandatory HEAD; its1.1s delay is below the2s I/O cap.
    let fixture = Fixture::new(DelayMode::ConfirmedRecovery);
    let trace = fixture.trace();

    // Act: timeout a read after confirmed acquisition, retaining conditional cleanup.
    let (result, elapsed) = observed_open(&fixture, Some(OPEN_BUDGET), Arc::clone(&trace));
    let timeout = match result {
        Err(MidgeError::Timeout(_)) => true,
        Ok(mut engine) => {
            engine.shutdown(Duration::from_secs(5)).unwrap();
            false
        }
        // A genuinely uncertain acquisition remains LeaseIndeterminate. This
        // controlled path must establish confirmed acquisition before the held read.
        Err(error) => panic!("confirmed-acquisition read timeout lost its type: {error:?}"),
    };
    let observed = fixture.proxy.control.wait_for_delayed_completion(1);
    let clean = !timeout || trace.wait_for_successful_cleanup();
    let requests = fixture.proxy.control.observations();

    // Assert: actual owner established, held read cancelled, no runtime admission,
    // then cleanup/reopen prove no leaked owner or expired deadline in runtime.
    assert!(
        timeout,
        "aggregate deadline was ignored after acquisition: elapsed={elapsed:?}, native={requests:?}, admitted={}",
        trace.admitted.load(Ordering::Acquire)
    );
    assert!(
        observed,
        "confirmed owner never entered selected mandatory native HEAD"
    );
    assert!(
        fixture
            .proxy
            .control
            .confirmed_lease_epoch
            .load(Ordering::Acquire)
            > 0
    );
    let held: Vec<_> = requests.iter().filter(|request| request.delayed).collect();
    assert_eq!(held.len(), 1);
    assert_eq!(held[0].method, "HEAD");
    assert!(held[0].client_cancelled);
    trace.assert_no_runtime_admission();
    assert!(
        elapsed <= OPEN_BUDGET + Duration::from_millis(400),
        "{elapsed:?}"
    );
    assert!(
        clean,
        "actual owned startup cleanup must finish successfully before reopen"
    );
    assert_fast_same_path_reopens(&fixture);
}

#[test]
#[ignore = "requires Sqrzl; selected cloud-integration qualification asserts prerequisites"]
fn should_abort_late_actual_runtime_preparation_when_native_open_caller_expires() {
    // Arrange: bootstrap and durably acknowledge actual rows without an open
    // deadline. The timed recovered reopen then targets a real dormant runtime,
    // rather than spending its phase budget on fresh namespace initialization.
    let fixture = Fixture::new(DelayMode::Fast);
    let mut seeded = Engine::open(fixture.options(None, fixture.trace()))
        .expect("defaultNone native bootstrap before preparation hold");
    record_rows_and_verify(&seeded);
    seeded.shutdown(Duration::from_secs(5)).unwrap();
    let trace = fixture.trace();
    let hold = PreparedHold::new(Arc::clone(&trace));

    // Act: the owned startup envelope must return while the worker is held.
    let (result, elapsed) = observed_open(&fixture, Some(OPEN_BUDGET), Arc::clone(&trace));
    let held_at_return = *trace.prepared_hold.lock().unwrap();
    let admitted_at_return = trace.admitted.load(Ordering::Acquire);
    drop(hold);
    let timeout = match result {
        Err(MidgeError::Timeout(_)) => true,
        Ok(mut engine) => {
            engine.shutdown(Duration::from_secs(5)).unwrap();
            false
        }
        Err(error) => panic!("late preparation lost its timeout type: {error}"),
    };
    let clean = !timeout || trace.wait_for_successful_cleanup();

    // Assert: release/join precedes assertions; late readiness never admits.
    assert!(
        timeout,
        "prepared worker ignored aggregate budget: {elapsed:?}"
    );
    assert!(
        held_at_return,
        "caller returned only after held worker was released"
    );
    assert!(
        elapsed <= OPEN_BUDGET + Duration::from_millis(400),
        "{elapsed:?}"
    );
    assert_eq!(trace.prepared.load(Ordering::Acquire), 1);
    assert_eq!(admitted_at_return, 0);
    assert_eq!(trace.admitted.load(Ordering::Acquire), 0);
    assert_eq!(trace.aborted.load(Ordering::Acquire), 1);
    assert!(
        clean,
        "actual aborted runtime and lease cleanup must finish"
    );
    // Check the acknowledged seed before the healthy helper writes these keys.
    let mut recovered = Engine::open(fixture.options(None, fixture.trace()))
        .expect("defaultNone reopen after aborted runtime cleanup");
    verify_rows(&recovered);
    recovered.shutdown(Duration::from_secs(5)).unwrap();
    assert_fast_same_path_reopens(&fixture);
}

#[test]
#[ignore = "requires Sqrzl; selected cloud-integration qualification asserts prerequisites"]
fn should_retain_native_cleanup_when_committed_lease_cas_response_exceeds_open_budget() {
    // Arrange: forward the first signed lease CAS to real Sqrzl and buffer its
    // actual 200 response. Hold that response and any scoped reconciliation GET
    // until the outer caller returns; never manufacture a provider outcome.
    let fixture = Fixture::new(DelayMode::CommittedLeaseResponse);
    let trace = fixture.trace();
    let hold = CommittedResponseHold(Arc::clone(&fixture.proxy.control));

    // Act: retain the response gate until its finite hold settles the exact
    // committed CAS. An outer caller timeout can precede native socket teardown;
    // releasing here would let the real 200 race that cancellation. Then release
    // every gate before assertions and wait for exact conditional expiration.
    let (result, elapsed) = observed_open(&fixture, Some(OPEN_BUDGET), Arc::clone(&trace));
    let held_at_return = fixture.proxy.control.committed_response_is_held();
    let committed = fixture.proxy.control.committed_lease();
    let completed = fixture
        .proxy
        .control
        .wait_for_committed_response_completion();
    drop(hold);
    let error = match result {
        Err(error) => Some(error),
        Ok(mut engine) => {
            engine.shutdown(Duration::from_secs(5)).unwrap();
            None
        }
    };
    let cleanup = committed
        .as_ref()
        .is_some_and(|proof| fixture.proxy.control.wait_for_conditional_cleanup(proof));
    let requests = fixture.proxy.control.observations();
    let creates: Vec<_> = requests
        .iter()
        .filter(|request| {
            request.method == "PUT"
                && request.path.ends_with("/midge_primary_lease.json")
                && request.condition == LeaseCondition::Create
        })
        .collect();

    // Assert: endpoint commitment is positive evidence of ambiguity, while a
    // separate conditional expiration proves cleanup rather than a no-op Ok.
    assert!(
        matches!(error, Some(MidgeError::LeaseIndeterminate(_))),
        "unacknowledged committed CAS lost uncertainty: {error:?}, elapsed={elapsed:?}"
    );
    assert!(
        held_at_return,
        "the actual committed response was never held"
    );
    assert!(completed, "held native CAS response never settled");
    assert_eq!(creates.len(), 1, "uncertain acquisition CAS was replayed");
    assert_eq!(
        creates[0].status,
        Some(200),
        "Sqrzl must commit the real CAS"
    );
    assert!(creates[0].client_cancelled);
    assert!(!creates[0].response_written);
    assert!(creates[0]
        .lease_put
        .as_ref()
        .is_some_and(|proof| proof.epoch > 0));
    trace.assert_no_runtime_admission();
    assert_eq!(
        fixture
            .proxy
            .control
            .confirmed_lease_epoch
            .load(Ordering::Acquire),
        0
    );
    assert!(
        elapsed <= OPEN_BUDGET + Duration::from_millis(400),
        "{elapsed:?}"
    );
    assert!(
        cleanup,
        "retained uncertain owner must perform an exact If-Match expiration, native={requests:?}"
    );
    // Actual rows, defaultNone compatibility, and an800ms WAL operation under
    // the ordinary2s cap prove the former600ms scope is disarmed on acceptance.
    assert_fast_same_path_reopens(&fixture);
}
