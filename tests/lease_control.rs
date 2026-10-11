//! Lease control for campaigns (#835): a test takes a lease over by advancing
//! the lease clock a process reads, never by rewriting the lease document,
//! and the next holder acquires the next epoch through the normal path.

use cntryl_midge::__internal::io::Fs;
use cntryl_midge::__internal::simulation::{CrashDevice, SimulatedClock, SimulatedObjectStore};
use cntryl_midge::__internal::startup::{StartupEvent, StartupObserver};
use cntryl_midge::{
    CloudProviderConfig, CloudStorageLocation, Engine, MidgeError, OpenOptions, TransactionMode,
    WriteOptions,
};
use std::path::Path;
use std::sync::atomic::{AtomicBool, AtomicU64, Ordering};
use std::sync::Arc;
use std::time::{Duration, Instant};

/// Long enough that no lease expires by itself while a test runs; cloud
/// leases need two thirds of it to exceed the provider write margin.
const LEASE_TTL: Duration = Duration::from_secs(30);
/// Past the TTL and the default skew allowance, half the TTL.
const PAST_EXPIRY: Duration = Duration::from_secs(46);
/// Within the TTL and the default skew allowance.
const SHORT_OF_EXPIRY: Duration = Duration::from_secs(44);

#[derive(Debug, Clone, Copy)]
enum Mode {
    Local,
    SimulatedCloud,
    ProviderCloud,
}

/// The epoch each open acquired.
#[derive(Debug, Default)]
struct Epoch(AtomicU64);

impl StartupObserver for Epoch {
    fn observe(&self, event: StartupEvent) {
        if let StartupEvent::LeaseAcquired { epoch } = event {
            self.0.store(epoch, Ordering::SeqCst);
        }
    }
}

/// One process: its cache device, its view of the shared store, its lease
/// clock, and what it observed.
struct Process {
    device: CrashDevice,
    store: SimulatedObjectStore,
    clock: SimulatedClock,
    epoch: Arc<Epoch>,
    lost_lease: Arc<AtomicBool>,
}

impl Process {
    fn new(device: CrashDevice, store: &SimulatedObjectStore, clock: SimulatedClock) -> Self {
        Self {
            device,
            store: store.process(),
            clock,
            epoch: Arc::default(),
            lost_lease: Arc::default(),
        }
    }

    /// The process dies: it reaches neither its cache nor the store again.
    fn crash(&self) {
        self.device.process_crash();
        self.store.process_crash();
    }

    fn options(&self, mode: Mode, named: &Path) -> OpenOptions {
        let builder = match mode {
            Mode::Local => OpenOptions::local(named),
            Mode::SimulatedCloud => OpenOptions::cloud_simulated(named, "bucket", "lease"),
            Mode::ProviderCloud => OpenOptions::cloud(
                named,
                CloudStorageLocation::new(
                    CloudProviderConfig::aws_s3_static("bucket", "us-east-1", "access", "secret"),
                    "lease/",
                ),
            ),
        };
        let builder = match mode {
            Mode::Local => builder,
            Mode::SimulatedCloud | Mode::ProviderCloud => {
                builder.object_store_for_testing(Arc::new(self.store.clone()))
            }
        };
        let root: Arc<dyn Fs> = Arc::new(self.device.fs());
        let lost_lease = Arc::clone(&self.lost_lease);
        builder
            .background_compaction(false)
            .lease_ttl(LEASE_TTL)
            .lease_clock_for_testing(Arc::new(self.clock.clone()))
            .startup_observer_for_testing(self.epoch.clone())
            .on_lease_loss(move || lost_lease.store(true, Ordering::SeqCst))
            .filesystem_for_testing(root)
            .build()
            .expect("open options")
    }

    fn epoch(&self) -> u64 {
        self.epoch.0.load(Ordering::SeqCst)
    }
}

impl Mode {
    /// A write that survives a process crash.
    fn durable(self) -> WriteOptions {
        match self {
            Self::Local => WriteOptions::sync(),
            Self::SimulatedCloud | Self::ProviderCloud => WriteOptions::cloud_strict(),
        }
    }
}

fn put(engine: &Engine, key: &[u8], options: WriteOptions) -> Result<(), MidgeError> {
    let cf = engine.get_column_family("default").expect("default");
    let mut tx = engine.begin_tx(cf.id(), TransactionMode::ReadWrite)?;
    tx.put(key.to_vec(), b"v".to_vec(), None)?;
    tx.commit(options)
}

fn get(engine: &Engine, key: &[u8]) -> Option<Vec<u8>> {
    let cf = engine.get_column_family("default").expect("default");
    let tx = engine
        .begin_tx(cf.id(), TransactionMode::ReadOnly)
        .expect("read");
    tx.get(key).expect("get").map(|value| value.to_vec())
}

/// What a restart after a process crash observed.
struct Takeover {
    first_epoch: u64,
    restarted: Result<(u64, Option<Vec<u8>>), MidgeError>,
    waited: Duration,
}

/// Crash a holder, advance the restarted process's lease clock by
/// `advance`, and reopen.
fn restart_after_crash(mode: Mode, advance: Duration) -> Takeover {
    let named = tempfile::tempdir().expect("named directory");
    let store = SimulatedObjectStore::eager();
    let first = Process::new(CrashDevice::new(), &store, SimulatedClock::new());
    let engine = Engine::open(first.options(mode, named.path())).expect("first open");
    put(&engine, b"k", mode.durable()).expect("put");
    first.crash();
    drop(engine);
    let restarted = Process::new(first.device.clone(), &store, first.clock.clone());
    restarted.clock.advance(advance);
    let started = Instant::now();
    let reopened = Engine::open(restarted.options(mode, named.path()));
    let waited = started.elapsed();
    Takeover {
        first_epoch: first.epoch(),
        restarted: reopened.map(|mut engine| {
            let value = get(&engine, b"k");
            engine
                .shutdown(Duration::from_secs(10))
                .expect("shutdown restarted engine");
            (restarted.epoch(), value)
        }),
        waited,
    }
}

fn assert_taken_over_without_waiting(takeover: &Takeover) {
    let (epoch, value) = takeover
        .restarted
        .as_ref()
        .expect("the restarted process takes the lease over");
    assert_eq!(*epoch, takeover.first_epoch + 1);
    assert_eq!(value.as_deref(), Some(b"v".as_slice()));
    assert!(
        takeover.waited < LEASE_TTL / 3,
        "the takeover waited {:?}",
        takeover.waited
    );
}

#[test]
fn should_take_over_crashed_local_holder_without_waiting_when_lease_clock_advances() {
    // Arrange
    let mode = Mode::Local;

    // Act
    let takeover = restart_after_crash(mode, PAST_EXPIRY);

    // Assert
    assert_taken_over_without_waiting(&takeover);
}

#[test]
fn should_take_over_crashed_simulated_cloud_holder_without_waiting_when_lease_clock_advances() {
    // Arrange
    let mode = Mode::SimulatedCloud;

    // Act
    let takeover = restart_after_crash(mode, PAST_EXPIRY);

    // Assert
    assert_taken_over_without_waiting(&takeover);
}

#[test]
fn should_take_over_crashed_provider_holder_without_waiting_when_lease_clock_advances() {
    // Arrange
    let mode = Mode::ProviderCloud;

    // Act
    let takeover = restart_after_crash(mode, PAST_EXPIRY);

    // Assert
    assert_taken_over_without_waiting(&takeover);
}

#[test]
fn should_refuse_crashed_holder_lease_when_lease_clock_stops_short_of_expiry() {
    // Arrange: inside the TTL plus the skew allowance, the dead holder's
    // lease still stands.
    let modes = [Mode::Local, Mode::SimulatedCloud, Mode::ProviderCloud];

    // Act
    let takeovers: Vec<_> = modes
        .iter()
        .map(|mode| (mode, restart_after_crash(*mode, SHORT_OF_EXPIRY)))
        .collect();

    // Assert
    for (mode, takeover) in takeovers {
        assert!(
            matches!(takeover.restarted, Err(MidgeError::LeaseHeld(_))),
            "{mode:?}: {:?}",
            takeover.restarted.as_ref().map(|(epoch, _)| epoch)
        );
    }
}

#[test]
fn should_fence_running_holder_when_a_process_with_a_later_clock_takes_over() {
    // Arrange: two hosts share the provider store, each with its own cache;
    // the second host's clock runs past the first holder's expiry.
    let named = tempfile::tempdir().expect("named directory");
    let store = SimulatedObjectStore::eager();
    let old = Process::new(CrashDevice::new(), &store, SimulatedClock::new());
    let engine =
        Engine::open(old.options(Mode::ProviderCloud, named.path())).expect("old holder opens");
    put(&engine, b"before", WriteOptions::cloud_async()).expect("old holder writes");
    let competitor = Process::new(CrashDevice::new(), &store, old.clock.fork());
    competitor.clock.advance(PAST_EXPIRY);

    // Act: the competitor takes over while the old holder still runs; the
    // old holder accepts a write on its own validity, then renews.
    let mut successor = Engine::open(competitor.options(Mode::ProviderCloud, named.path()))
        .expect("competitor takes over");
    let zombie = put(&engine, b"zombie", WriteOptions::cloud_async());
    let deadline = Instant::now() + LEASE_TTL;
    while !old.lost_lease.load(Ordering::SeqCst) && Instant::now() < deadline {
        std::thread::sleep(Duration::from_millis(50));
    }
    let after_loss = put(&engine, b"after", WriteOptions::cloud_async());

    // Assert
    assert_eq!(competitor.epoch(), old.epoch() + 1);
    assert!(
        zombie.is_ok(),
        "the old holder's write before renewal: {zombie:?}"
    );
    assert!(
        old.lost_lease.load(Ordering::SeqCst),
        "the old holder's renewal was not fenced"
    );
    assert!(after_loss.is_err(), "a fenced holder accepted a write");
    successor
        .shutdown(Duration::from_secs(10))
        .expect("successor shutdown");
}
