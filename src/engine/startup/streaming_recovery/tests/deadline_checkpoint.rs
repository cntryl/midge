//! Separate real aggregate deadline and accepted-publication cancellation proofs.

use super::*;
use crate::engine::startup::test_control::{
    self, CheckpointTestBoundaries, OneShotBarrier, OpenTestControl, OwnerExit,
};
use crate::lease::PrimaryLease;
use crate::runtime::{StartupEvent, StartupObserver};
use crossbeam::channel::{self, Receiver, Sender};
use std::path::{Path, PathBuf};
use std::sync::atomic::{AtomicBool, AtomicUsize, Ordering};
use std::sync::{Arc, Condvar, Mutex};
use std::time::{Duration, Instant};

const CHILD: &str = "MIDGE_STARTUP_CHECKPOINT_OWNER_CHILD";
const MODE: &str = "MIDGE_STARTUP_CHECKPOINT_MODE";
const CHILD_TEST: &str = "engine::startup::streaming_recovery::tests::deadline_checkpoint::should_exercise_checkpoint_deadline_scenario_in_child";
const HOLD_POINT: &str = "midge::flush_worker::after_cloud_sst_upload";
const DELAY_POINT: &str = "midge::recovery::before_name_reservation";
const BUDGET: u64 = 256 * 1024;
const RECORDS: u64 = 384;
const SETUP_BOUND: Duration = Duration::from_secs(5);

#[derive(Debug, Default)]
struct OwnershipTrace {
    admitted: AtomicUsize,
    cleanup: Mutex<Option<bool>>,
    changed: Condvar,
}

impl StartupObserver for OwnershipTrace {
    fn observe(&self, event: StartupEvent) {
        match event {
            StartupEvent::RuntimeAdmitted => {
                self.admitted.fetch_add(1, Ordering::AcqRel);
            }
            StartupEvent::CleanupFinished { successful } => {
                *self.cleanup.lock().unwrap() = Some(successful);
                self.changed.notify_all();
            }
            _ => {}
        }
    }
}

impl OwnershipTrace {
    fn wait_for_cleanup(&self) -> bool {
        let (result, _) = self
            .changed
            .wait_timeout_while(self.cleanup.lock().unwrap(), SETUP_BOUND, |result| {
                result.is_none()
            })
            .unwrap();
        *result == Some(true)
    }
}

#[derive(Default)]
struct PublicationGate {
    entered: AtomicUsize,
    escaped: AtomicBool,
    released: Mutex<bool>,
    changed: Condvar,
}

impl PublicationGate {
    fn observe(&self) {
        let released = self.released.lock().unwrap();
        self.entered.fetch_add(1, Ordering::AcqRel);
        self.changed.notify_all();
        // A completed real upload is held; a finite escape is always rejected.
        let (released, _) = self
            .changed
            .wait_timeout_while(released, Duration::from_secs(3), |released| !*released)
            .unwrap();
        if !*released {
            self.escaped.store(true, Ordering::Release);
        }
    }

    fn wait_until_entered(&self) -> bool {
        let (_released, _) = self
            .changed
            .wait_timeout_while(self.released.lock().unwrap(), SETUP_BOUND, |_| {
                self.entered.load(Ordering::Acquire) == 0
            })
            .unwrap();
        self.entered.load(Ordering::Acquire) > 0
    }

    fn release(&self) {
        *self.released.lock().unwrap() = true;
        self.changed.notify_all();
    }

    fn assert_not_escaped(&self) {
        assert!(
            !self.escaped.load(Ordering::Acquire),
            "completed-upload gate used its finite escape"
        );
    }
}

struct HeldPublication(Arc<PublicationGate>);

impl HeldPublication {
    fn install(delayed: bool, reject_upload: bool) -> Self {
        let gate = Arc::new(PublicationGate::default());
        let observed = Arc::clone(&gate);
        fail::cfg_callback(HOLD_POINT, move || observed.observe()).unwrap();
        if delayed || reject_upload {
            let first = AtomicBool::new(true);
            fail::cfg_callback(DELAY_POINT, move || {
                if first.swap(false, Ordering::AcqRel) {
                    assert!(
                        !reject_upload,
                        "controlled permanent rejection before upload"
                    );
                    // Deliberately longer than the unchanged public 600 ms budget.
                    std::thread::sleep(Duration::from_millis(900));
                }
            })
            .unwrap();
        }
        Self(gate)
    }
}

impl Drop for HeldPublication {
    fn drop(&mut self) {
        self.0.release();
        fail::remove(HOLD_POINT);
        fail::remove(DELAY_POINT);
    }
}

struct AttemptOwner {
    gate: Arc<PublicationGate>,
    expire: Option<Sender<()>>,
    before_receive: Option<Sender<()>>,
    finished: Receiver<OwnerExit>,
    observed_exit: Option<OwnerExit>,
    caller: Option<std::thread::JoinHandle<()>>,
    settled: bool,
}

impl AttemptOwner {
    fn release_before_receive(&mut self) {
        if let Some(release) = self.before_receive.take() {
            let _ = release.try_send(());
        }
    }

    fn release_all(&mut self) {
        if let Some(expire) = &self.expire {
            let _ = expire.try_send(());
        }
        self.release_before_receive();
        self.gate.release();
    }

    fn settle(&mut self) -> Result<OwnerExit, String> {
        self.release_all();
        let exit = match self.observed_exit {
            Some(exit) => exit,
            None => self
                .finished
                .recv_timeout(Duration::from_secs(10))
                .map_err(|error| format!("actual startup owner did not settle: {error}"))?,
        };
        self.observed_exit = Some(exit);
        if let Some(caller) = self.caller.take() {
            let until = Instant::now() + SETUP_BOUND;
            while !caller.is_finished() && Instant::now() < until {
                std::thread::sleep(Duration::from_millis(10));
            }
            if !caller.is_finished() {
                return Err("public caller did not settle after cancellation/release".into());
            }
            caller.join().map_err(|_| "public caller panicked")?;
        }
        self.settled = true;
        Ok(exit)
    }

    fn assert_returned(&mut self) {
        assert_eq!(self.settle().unwrap(), OwnerExit::Returned);
    }
}

impl Drop for AttemptOwner {
    fn drop(&mut self) {
        if !self.settled {
            let outcome = self.settle();
            eprintln!("failure-path owner settlement: {outcome:?}");
            assert!(
                std::thread::panicking() || outcome == Ok(OwnerExit::Returned),
                "fixture could not settle owned startup: {outcome:?}"
            );
        }
    }
}

fn bounded_options(path: &Path, trace: Arc<OwnershipTrace>) -> OpenOptions {
    OpenOptions::cloud_simulated(path, "bucket", "streaming-recovery")
        .local_storage_budget(BUDGET)
        .with_memtable_size_limit(64 * 1024)
        .background_compaction(false)
        .open_timeout(Duration::from_millis(600))
        .startup_observer_for_testing(trace)
        .build()
        .unwrap()
}

fn authoritative_catalog(path: &Path) -> crate::wal::cloud_catalog::WalPublicationCatalog {
    crate::wal::cloud_catalog::WalPublicationCatalog::decode(
        &std::fs::read(path.join("cloud_store/wal/publication-catalog.v1.json")).unwrap(),
    )
    .unwrap()
}

struct Seed {
    path: PathBuf,
    source: Vec<u8>,
    catalog: crate::wal::cloud_catalog::WalPublicationCatalog,
}

impl Seed {
    fn new(path: PathBuf) -> Self {
        let mut initial = Engine::open(options(&path, BUDGET)).unwrap();
        initial.shutdown(Duration::from_secs(10)).unwrap();
        drop(initial);
        let source = publish_wal(&path, RECORDS);
        assert!(source.len() as u64 > BUDGET);
        let catalog = authoritative_catalog(&path);
        Self {
            path,
            source,
            catalog,
        }
    }

    fn assert_history(&self) {
        let catalog = authoritative_catalog(&self.path);
        assert_eq!(self.catalog.segments, catalog.segments);
        let key = &catalog.segments[&1].object_key;
        assert_eq!(
            std::fs::read(self.path.join("cloud_store").join(key)).unwrap(),
            self.source
        );
    }

    fn recover(&self, reservation: Option<(std::ffi::OsString, u64)>) {
        let mut recovered = Engine::open(options(&self.path, BUDGET)).unwrap();
        let cf = recovered.get_column_family("default").unwrap();
        let tx = recovered
            .begin_tx(cf.id(), TransactionMode::ReadOnly)
            .unwrap();
        for sequence in 1..=RECORDS {
            assert_eq!(
                tx.get(&sequence.to_be_bytes()).unwrap().as_deref(),
                Some(value(sequence).as_slice())
            );
        }
        drop(tx);
        if let Some((name, frontier)) = reservation {
            let after = crate::metadata::ManifestPersistence::load(&self.path).unwrap();
            assert!(after.next_sst_seqs.get(&0).copied().unwrap() >= frontier);
            assert!(!after
                .files
                .iter()
                .any(|file| std::ffi::OsStr::new(&file.name) == name.as_os_str()));
        }
        recovered.shutdown(Duration::from_secs(10)).unwrap();
    }
}

fn timeout_error(result: crate::common::MidgeResult<Engine>) -> crate::common::MidgeError {
    match result {
        Err(error) => {
            assert!(
                matches!(error, crate::common::MidgeError::Timeout(_)),
                "actual public result: {error:?}"
            );
            error
        }
        Ok(mut engine) => {
            engine.shutdown(Duration::from_secs(10)).unwrap();
            panic!("held recovery incorrectly admitted runtime");
        }
    }
}

fn aggregate_deadline(seed: &Seed, delayed: bool) {
    let trace = Arc::new(OwnershipTrace::default());
    let hold = HeldPublication::install(delayed, false);
    let (finished_tx, finished) = channel::bounded(1);
    let installation = test_control::install(OpenTestControl {
        expire: None,
        owner_finished: finished_tx,
        checkpoint: None,
    });
    let mut owner = AttemptOwner {
        gate: Arc::clone(&hold.0),
        expire: None,
        before_receive: None,
        finished,
        observed_exit: None,
        caller: None,
        settled: false,
    };
    let start = Instant::now();
    let result = Engine::open(bounded_options(&seed.path, Arc::clone(&trace)));
    let elapsed = start.elapsed();
    let entered = hold.0.entered.load(Ordering::Acquire);
    let raw_result = result
        .as_ref()
        .map(|_| "Engine")
        .map_err(|error| format!("{error:?}"));
    eprintln!("aggregate600ms elapsed={elapsed:?}, result={raw_result:?}, gate={entered}, cleanup={trace:?}");
    drop(installation);
    owner.assert_returned();
    timeout_error(result);
    assert!(
        elapsed >= Duration::from_millis(550) && elapsed < Duration::from_millis(1200),
        "{elapsed:?}"
    );
    assert_eq!(trace.admitted.load(Ordering::Acquire), 0);
    hold.0.assert_not_escaped();
    if delayed {
        assert_eq!(
            entered, 0,
            "pre-upload delay must prevent acceptance before timeout"
        );
    }
    drop(hold);
    seed.assert_history();
    seed.recover(None);
}

fn real_contender_is_denied(path: &Path) -> bool {
    let contender = Arc::new(crate::lease::CloudStorageLease::new(
        crate::lease::CloudLeaseConfig {
            bucket: "bucket".into(),
            prefix: "streaming-recovery".into(),
        },
        path,
    ));
    let contention = contender.try_acquire();
    let denied = matches!(
        contention,
        Err(crate::lease::LeaseError::AcquisitionFailed(_))
    );
    if let Ok(guard) = contention {
        guard.release();
    }
    denied
}

fn held_evidence(
    seed: &Seed,
    trace: &OwnershipTrace,
    gate: &PublicationGate,
) -> (std::ffi::OsString, u64) {
    assert_eq!(gate.entered.load(Ordering::Acquire), 1);
    assert_eq!(*trace.cleanup.lock().unwrap(), None);
    assert_eq!(trace.admitted.load(Ordering::Acquire), 0);
    assert!(
        real_contender_is_denied(&seed.path),
        "real contender acquired while accepted publisher was held"
    );
    let uploaded: Vec<_> = std::fs::read_dir(seed.path.join("cloud_store/sst"))
        .expect("positively observed completed upload must have its real SST directory")
        .map(|entry| entry.unwrap().path())
        .collect();
    assert_eq!(
        uploaded.len(),
        1,
        "one actual completed SST upload required"
    );
    assert!(std::fs::metadata(&uploaded[0]).unwrap().len() > 0);
    let manifest = crate::metadata::ManifestPersistence::load(&seed.path).unwrap();
    assert!(manifest.files.is_empty());
    seed.assert_history();
    // Check after ALL contender/filesystem observations, not only before them.
    gate.assert_not_escaped();
    assert_eq!(*trace.cleanup.lock().unwrap(), None);
    assert_eq!(trace.admitted.load(Ordering::Acquire), 0);
    assert_eq!(gate.entered.load(Ordering::Acquire), 1);
    (
        uploaded[0].file_name().unwrap().to_owned(),
        manifest.next_sst_seqs[&0],
    )
}

struct ControlledCaller {
    owner: AttemptOwner,
    result: Receiver<crate::common::MidgeResult<Engine>>,
    before_receive: Receiver<()>,
    join_started: Receiver<()>,
}

fn controlled_caller(
    seed: &Seed,
    trace: &Arc<OwnershipTrace>,
    gate: &Arc<PublicationGate>,
) -> ControlledCaller {
    let (expire_tx, expire) = channel::bounded(1);
    let (finished_tx, finished) = channel::bounded(1);
    let (entered, before_receive) = channel::bounded(1);
    let (release_tx, release) = channel::bounded(1);
    let (join_tx, join_started) = channel::bounded(1);
    let (result_tx, result) = channel::bounded(1);
    let path = seed.path.clone();
    let trace = Arc::clone(trace);
    let caller = std::thread::spawn(move || {
        let _installation = test_control::install(OpenTestControl {
            expire: Some(expire),
            owner_finished: finished_tx,
            checkpoint: Some(CheckpointTestBoundaries {
                before_publish_receive: Some(OneShotBarrier { entered, release }),
                actor_join_started: Some(join_tx),
            }),
        });
        let _ = result_tx.send(Engine::open(bounded_options(&path, trace)));
    });
    ControlledCaller {
        owner: AttemptOwner {
            gate: Arc::clone(gate),
            expire: Some(expire_tx),
            before_receive: Some(release_tx),
            finished,
            observed_exit: None,
            caller: Some(caller),
            settled: false,
        },
        result,
        before_receive,
        join_started,
    }
}

fn accepted_checkpoint(seed: &Seed, delayed: bool, reject_upload: bool, disconnect: bool) {
    let trace = Arc::new(OwnershipTrace::default());
    let hold = HeldPublication::install(delayed, reject_upload);
    let mut caller = controlled_caller(seed, &trace, &hold.0);
    let uploaded = hold.0.wait_until_entered();
    if !uploaded {
        let result = caller.result.try_recv().map(|result| match result {
            Err(error) => format!("{error:?}"),
            Ok(mut engine) => {
                let _ = engine.shutdown(Duration::from_secs(5));
                "unexpected Engine".into()
            }
        });
        panic!("accepted-checkpoint setup failed: no genuine upload; result={result:?}, trace={trace:?}");
    }
    caller
        .before_receive
        .recv_timeout(SETUP_BOUND)
        .expect("real worker reached pre-receive barrier");
    assert_eq!(hold.0.entered.load(Ordering::Acquire), 1);
    let expired = Instant::now();
    if disconnect {
        drop(caller.owner.expire.take());
    } else {
        caller.owner.expire.as_ref().unwrap().try_send(()).unwrap();
    }
    let result = caller
        .result
        .recv_timeout(Duration::from_millis(1200))
        .expect("public caller must respond to controlled expiry");
    timeout_error(result);
    assert!(
        expired.elapsed() < Duration::from_millis(1200),
        "controlled cancellation response exceeded envelope"
    );
    // The public Timeout has cancelled the real shared scope. Release only
    // startup's pre-receive barrier; the genuine completed upload stays held.
    caller.owner.release_before_receive();
    caller
        .join_started
        .recv_timeout(SETUP_BOUND)
        .expect("cancelled real startup must enter actor shutdown/join");
    let reservation = held_evidence(seed, &trace, &hold.0);
    caller.owner.assert_returned();
    assert!(
        trace.wait_for_cleanup(),
        "accepted owner must join publisher then conditionally release lease"
    );
    assert_eq!(trace.admitted.load(Ordering::Acquire), 0);
    hold.0.assert_not_escaped();
    drop(hold);
    seed.assert_history();
    seed.recover(Some(reservation));
}

#[test]
fn should_exercise_checkpoint_deadline_scenario_in_child() {
    // Arrange: global upload/delay callbacks are confined to this child process.
    let Some(path) = std::env::var_os(CHILD) else {
        return;
    };
    let mode = std::env::var(MODE).unwrap();
    let seed = Seed::new(PathBuf::from(path));
    // Act: public aggregate clock and controlled accepted ownership are distinct.
    match mode.as_str() {
        "aggregate600ms" => aggregate_deadline(&seed, false),
        "aggregate600ms-delayed" => aggregate_deadline(&seed, true),
        "accepted-checkpoint-cancellation" => accepted_checkpoint(&seed, false, false, false),
        "accepted-checkpoint-cancellation-delayed" => {
            accepted_checkpoint(&seed, true, false, false);
        }
        "reject-missing-upload" => accepted_checkpoint(&seed, false, true, false),
        "reject-controller-disconnect" => accepted_checkpoint(&seed, false, false, true),
        _ => panic!("unknown checkpoint deadline scenario: {mode}"),
    }
    // Assert: parent requires this mode's unique marker after complete recovery.
    std::fs::write(seed.path.join("checkpoint-deadline-proof"), mode).unwrap();
}

fn run_child(mode: &str) -> (tempfile::TempDir, std::process::Output, bool) {
    use std::process::{Command, Stdio};
    let directory = tempfile::tempdir().unwrap();
    let stdout_path = directory.path().join("child.stdout");
    let stderr_path = directory.path().join("child.stderr");
    let mut child = Command::new(std::env::current_exe().unwrap())
        .args(["--exact", CHILD_TEST, "--nocapture", "--test-threads=1"])
        .env(CHILD, directory.path())
        .env(MODE, mode)
        .stdout(Stdio::from(std::fs::File::create(&stdout_path).unwrap()))
        .stderr(Stdio::from(std::fs::File::create(&stderr_path).unwrap()))
        .spawn()
        .unwrap();
    let deadline = Instant::now() + Duration::from_secs(30);
    let timed_out = loop {
        if child.try_wait().unwrap().is_some() {
            break false;
        }
        if Instant::now() >= deadline {
            child.kill().unwrap();
            break true;
        }
        std::thread::sleep(Duration::from_millis(10));
    };
    let output = std::process::Output {
        status: child.wait().unwrap(),
        stdout: std::fs::read(stdout_path).unwrap(),
        stderr: std::fs::read(stderr_path).unwrap(),
    };
    (directory, output, timed_out)
}

fn assert_child_passes(mode: &str) {
    let (directory, output, timed_out) = run_child(mode);
    assert!(
        !timed_out && output.status.success(),
        "scenario={mode}, timed_out={timed_out}, status={}\n{}\n{}",
        output.status,
        String::from_utf8_lossy(&output.stdout),
        String::from_utf8_lossy(&output.stderr)
    );
    eprintln!(
        "passed checkpoint scenario={mode}:\n{}",
        String::from_utf8_lossy(&output.stderr)
    );
    assert_eq!(
        std::fs::read(directory.path().join("checkpoint-deadline-proof")).unwrap(),
        mode.as_bytes()
    );
}

#[test]
fn should_honor_aggregate_open_deadline_without_assuming_checkpoint_acceptance() {
    // Arrange: each isolated child uses the unchanged real 600 ms public budget.
    // Act: run ordinary and deliberately delayed-before-upload startup.
    // Assert: both children must settle and recover every row.
    assert_child_passes("aggregate600ms");
    assert_child_passes("aggregate600ms-delayed");
}

#[test]
fn should_retain_accepted_checkpoint_ownership_when_public_open_is_cancelled() {
    // Arrange: expiration is controlled only after both real stages are observed.
    // Act: run ordinary and deliberately delayed preparation.
    // Assert: neither child can bypass genuine upload/lease proof.
    assert_child_passes("accepted-checkpoint-cancellation");
    assert_child_passes("accepted-checkpoint-cancellation-delayed");
}

#[test]
fn should_reject_ownership_claim_when_upload_or_expiry_signal_is_missing() {
    // Arrange: deliberately invalid controllers must fail in isolated children.
    for (mode, diagnostic) in [
        (
            "reject-missing-upload",
            "accepted-checkpoint setup failed: no genuine upload",
        ),
        (
            "reject-controller-disconnect",
            "timed-open test controller disconnected without expiry",
        ),
    ] {
        // Act
        let (directory, output, timed_out) = run_child(mode);
        eprintln!(
            "negative checkpoint scenario={mode}:\n{}",
            String::from_utf8_lossy(&output.stderr)
        );
        // Assert: neither missing evidence nor disconnection is a timeout success.
        assert!(
            !timed_out,
            "negative control must settle through fixture cleanup"
        );
        assert!(!output.status.success());
        assert!(
            String::from_utf8_lossy(&output.stderr).contains(diagnostic),
            "{}",
            String::from_utf8_lossy(&output.stderr)
        );
        assert!(!directory.path().join("checkpoint-deadline-proof").exists());
    }
}
