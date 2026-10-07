//! Actual startup checkpoint ownership after the caller deadline.
//! Actual accepted recovery publication, genuine filesystem-backed cloud SST,
//! and real primary-lease contender. No simulated completion or provider claim.

use super::*;
use crate::lease::PrimaryLease;
use crate::runtime::{StartupEvent, StartupObserver};
use std::sync::atomic::{AtomicUsize, Ordering};
use std::sync::{Arc, Condvar, Mutex};
use std::time::{Duration, Instant};

const CHILD: &str = "MIDGE_STARTUP_CHECKPOINT_OWNER_CHILD";
const HOLD_POINT: &str = "midge::flush_worker::after_cloud_sst_upload";
const BUDGET: u64 = 256 * 1024;
const RECORDS: u64 = 384;

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
            .wait_timeout_while(
                self.cleanup.lock().unwrap(),
                Duration::from_secs(4),
                |result| result.is_none(),
            )
            .unwrap();
        *result == Some(true)
    }
}

#[derive(Default)]
struct PublicationGate {
    entered: AtomicUsize,
    released: Mutex<bool>,
    changed: Condvar,
}

impl PublicationGate {
    fn observe(&self) {
        self.entered.fetch_add(1, Ordering::AcqRel);
        // Finite escape makes a broken baseline return; RAII releases sooner
        // on every caller path. This is a completed genuine SST upload.
        let (mut released, _) = self
            .changed
            .wait_timeout_while(
                self.released.lock().unwrap(),
                Duration::from_secs(3),
                |released| !*released,
            )
            .unwrap();
        *released = true;
    }
    fn release(&self) {
        *self.released.lock().unwrap() = true;
        self.changed.notify_all();
    }
}

struct HeldPublication(Arc<PublicationGate>);
impl Drop for HeldPublication {
    fn drop(&mut self) {
        self.0.release();
        fail::remove(HOLD_POINT);
    }
}

fn bounded_options(path: &std::path::Path, trace: Arc<OwnershipTrace>) -> OpenOptions {
    OpenOptions::cloud_simulated(path, "bucket", "streaming-recovery")
        .local_storage_budget(BUDGET)
        .with_memtable_size_limit(64 * 1024)
        .background_compaction(false)
        .open_timeout(Duration::from_millis(600))
        .startup_observer_for_testing(trace)
        .build()
        .unwrap()
}

fn authoritative_catalog(
    path: &std::path::Path,
) -> crate::wal::cloud_catalog::WalPublicationCatalog {
    crate::wal::cloud_catalog::WalPublicationCatalog::decode(
        &std::fs::read(path.join("cloud_store/wal/publication-catalog.v1.json")).unwrap(),
    )
    .unwrap()
}

fn verify_rows(engine: &Engine) {
    let cf = engine.get_column_family("default").unwrap();
    let tx = engine.begin_tx(cf.id(), TransactionMode::ReadOnly).unwrap();
    for sequence in 1..=RECORDS {
        assert_eq!(
            tx.get(&sequence.to_be_bytes()).unwrap().as_deref(),
            Some(value(sequence).as_slice())
        );
    }
}

struct HeldAttempt {
    result: crate::common::MidgeResult<Engine>,
    elapsed: Duration,
    held_entries: usize,
    premature_cleanup: bool,
    admitted: usize,
    denied: bool,
    uploaded: Vec<std::path::PathBuf>,
    catalog_held: crate::wal::cloud_catalog::WalPublicationCatalog,
    manifest_held: crate::metadata::Manifest,
}

fn observe_held_attempt(
    path: &std::path::Path,
    trace: &Arc<OwnershipTrace>,
    gate: &Arc<PublicationGate>,
) -> HeldAttempt {
    let start = Instant::now();
    let result = Engine::open(bounded_options(path, Arc::clone(trace)));
    let elapsed = start.elapsed();
    let held_entries = gate.entered.load(Ordering::Acquire);
    let premature_cleanup = trace.cleanup.lock().unwrap().is_some();
    let admitted = trace.admitted.load(Ordering::Acquire);
    let contender = Arc::new(crate::lease::CloudStorageLease::new(
        crate::lease::CloudLeaseConfig {
            bucket: "bucket".into(),
            prefix: "streaming-recovery".into(),
        },
        path,
    ));
    let contention = Arc::clone(&contender).try_acquire();
    let denied = matches!(
        &contention,
        Err(crate::lease::LeaseError::AcquisitionFailed(_))
    );
    if let Ok(guard) = contention {
        guard.release();
    }
    let uploaded: Vec<_> = std::fs::read_dir(path.join("cloud_store/sst"))
        .unwrap()
        .map(|entry| entry.unwrap().path())
        .collect();
    let catalog_held = authoritative_catalog(path);
    let manifest_held = crate::metadata::ManifestPersistence::load(path).unwrap();
    HeldAttempt {
        result,
        elapsed,
        held_entries,
        premature_cleanup,
        admitted,
        denied,
        uploaded,
        catalog_held,
        manifest_held,
    }
}

#[test]
fn should_retain_startup_ownership_when_open_deadline_expires_in_child() {
    // Arrange: isolated process means the unguarded callback cannot pause
    // another engine's worker. Real seed helpers provide all catalog/WAL bytes.
    let Some(path) = std::env::var_os(CHILD) else {
        return;
    };
    let path = std::path::PathBuf::from(path);
    let mut initial = Engine::open(options(&path, BUDGET)).unwrap();
    initial.shutdown(Duration::from_secs(10)).unwrap();
    drop(initial);
    let source = publish_wal(&path, RECORDS);
    assert!(source.len() as u64 > BUDGET);
    let catalog_before = authoritative_catalog(&path);
    let trace = Arc::new(OwnershipTrace::default());
    let gate = Arc::new(PublicationGate::default());
    let observed = Arc::clone(&gate);
    fail::cfg_callback(HOLD_POINT, move || observed.observe()).unwrap();
    let hold = HeldPublication(Arc::clone(&gate));

    // Act: the actual worker has finalized and uploaded an SST before it is
    // held. The timed caller must return without releasing that worker's lease.
    let attempt = observe_held_attempt(&path, &trace, &gate);
    // Mandatory release precedes all failure assertions and orderly cleanup.
    drop(hold);
    let cleaned = trace.wait_for_cleanup();
    let error = match attempt.result {
        Err(error) => error,
        Ok(mut engine) => {
            engine.shutdown(Duration::from_secs(10)).unwrap();
            panic!("held recovery incorrectly admitted runtime");
        }
    };

    // Assert: elapsed budget is bounded while the lease covers the actual
    // accepted publisher until its join; uploaded output is not committed.
    assert!(
        matches!(error, crate::common::MidgeError::Timeout(_)),
        "{error}"
    );
    assert!(
        attempt.elapsed >= Duration::from_millis(550)
            && attempt.elapsed < Duration::from_millis(1200),
        "{:?}",
        attempt.elapsed
    );
    assert_eq!(attempt.held_entries, 1);
    assert!(!attempt.premature_cleanup);
    assert_eq!(attempt.admitted, 0);
    assert!(
        attempt.denied,
        "real contender acquired while accepted SST publisher was held"
    );
    assert!(
        cleaned,
        "owner must finish actor join and conditional lease release"
    );
    assert_eq!(
        attempt.uploaded.len(),
        1,
        "one actual accepted SST upload is required"
    );
    assert!(std::fs::metadata(&attempt.uploaded[0]).unwrap().len() > 0);
    assert!(attempt.manifest_held.files.is_empty());
    assert_eq!(catalog_before.segments, attempt.catalog_held.segments);
    let key = &attempt.catalog_held.segments[&1].object_key;
    assert_eq!(
        std::fs::read(path.join("cloud_store").join(key)).unwrap(),
        source
    );
    let reserved_name = attempt.uploaded[0].file_name().unwrap().to_owned();
    let reserved_frontier = attempt
        .manifest_held
        .next_sst_seqs
        .get(&0)
        .copied()
        .unwrap();

    // A same-path healthy open must recover every cataloged row using
    // preserved history, reserve later names, and avoid reusing held output.
    let mut recovered = Engine::open(options(&path, BUDGET)).unwrap();
    verify_rows(&recovered);
    let after = crate::metadata::ManifestPersistence::load(&path).unwrap();
    assert!(after.next_sst_seqs.get(&0).copied().unwrap() >= reserved_frontier);
    assert!(!after
        .files
        .iter()
        .any(|file| std::ffi::OsStr::new(&file.name) == reserved_name.as_os_str()));
    recovered.shutdown(Duration::from_secs(10)).unwrap();
    std::fs::write(
        path.join("checkpoint-deadline-proof"),
        b"actual upload, retained lease, exact rows",
    )
    .unwrap();
}

#[test]
fn should_retain_accepted_checkpoint_ownership_when_public_open_times_out() {
    // Arrange: child isolation preserves unrelated worker/failpoint state.
    let directory = tempfile::tempdir().unwrap();
    // Act: actual public Engine::open, real source WAL and actual upload worker.
    let output = std::process::Command::new(std::env::current_exe().unwrap())
        .args(["--exact", "engine::startup::streaming_recovery::tests::deadline_checkpoint::should_retain_startup_ownership_when_open_deadline_expires_in_child", "--nocapture"])
        .env(CHILD, directory.path()).output().unwrap();
    // Assert: child assertions include typed caller result and complete recovery.
    assert!(
        output.status.success(),
        "{}\n{}",
        String::from_utf8_lossy(&output.stdout),
        String::from_utf8_lossy(&output.stderr)
    );
    assert_eq!(
        std::fs::read(directory.path().join("checkpoint-deadline-proof")).unwrap(),
        b"actual upload, retained lease, exact rows"
    );
}
