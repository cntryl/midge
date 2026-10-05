//! Public Engine ownership and recovery at a genuine compaction boundary.
//!
//! The callback holds an accepted publication worker after its actual output
//! proof succeeds. It does not simulate a provider response or socket cancel.
//! The fault-injection target runs serially because failpoints are global.

use cntryl_midge::{
    Bytes, Engine, MidgeError, MidgeResult, OpenOptions, Query, RecoveryPolicy, TransactionMode,
    WriteOptions,
};
use std::collections::BTreeSet;
use std::path::Path;
use std::sync::atomic::{AtomicBool, AtomicUsize, Ordering};
use std::sync::{mpsc, Arc, Condvar, Mutex};
use std::time::{Duration, Instant};

const OUTPUT_PROVED: &str = "slice7::after_compaction_output_durable_before_manifest_publish";
const CALLER_BUDGET: Duration = Duration::from_secs(5);
const OBSERVATION_WAIT: Duration = Duration::from_secs(10);
const HOLD_LIMIT: Duration = Duration::from_secs(30);
const CLEANUP_WAIT: Duration = Duration::from_secs(15);
const SHORT_SHUTDOWN: Duration = Duration::from_millis(100);
const BATCHES: usize = 4;
const RECORDS_PER_BATCH: usize = 16;

type Row = (Bytes, Bytes);

struct ReadSnapshot {
    points: Vec<(Bytes, Option<Bytes>)>,
    scan: Vec<Row>,
}

#[derive(Default)]
struct PublicationHold {
    released: Mutex<bool>,
    changed: Condvar,
    entered: AtomicBool,
    exits: AtomicUsize,
    exhausted: AtomicBool,
}

impl PublicationHold {
    fn wait(&self) {
        let released = self
            .released
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner);
        let (released, waited) = self
            .changed
            .wait_timeout_while(released, HOLD_LIMIT, |released| !*released)
            .unwrap_or_else(std::sync::PoisonError::into_inner);
        if waited.timed_out() && !*released {
            self.exhausted.store(true, Ordering::Release);
        }
        self.exits.fetch_add(1, Ordering::AcqRel);
    }

    fn release(&self) {
        *self
            .released
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner) = true;
        self.changed.notify_all();
    }
}

struct ReleasePublication(Arc<PublicationHold>);

impl Drop for ReleasePublication {
    fn drop(&mut self) {
        self.0.release();
        fail::remove(OUTPUT_PROVED);
    }
}

fn install_publication_hold(hold: &Arc<PublicationHold>) -> mpsc::Receiver<()> {
    let callback_hold = Arc::clone(hold);
    let (entered_tx, entered_rx) = mpsc::sync_channel(1);
    fail::cfg_callback(OUTPUT_PROVED, move || {
        if !callback_hold.entered.swap(true, Ordering::AcqRel) {
            let _ = entered_tx.try_send(());
            callback_hold.wait();
        }
    })
    .expect("hold the actual output-proved publication worker");
    entered_rx
}

fn options(path: &Path) -> OpenOptions {
    OpenOptions::cloud_simulated(path, "compaction-deadline", "owned-publication")
        .background_compaction(false)
        .recovery_policy(RecoveryPolicy::Strict)
        .storage_io_timeout(Duration::from_secs(4))
        .runtime_response_timeout(CALLER_BUDGET)
        .build()
        .expect("build strict filesystem-backed cloud options")
}

fn acknowledge_flushed_batches(engine: &Engine) -> MidgeResult<Vec<Row>> {
    let cf = engine
        .get_column_family("default")
        .ok_or_else(|| MidgeError::Internal("fixture default column family is missing".into()))?;
    let mut acknowledged = Vec::new();
    for batch in 0..BATCHES {
        let mut write = engine.begin_tx(cf.id(), TransactionMode::ReadWrite)?;
        let mut rows = Vec::new();
        for index in 0..RECORDS_PER_BATCH {
            let key = Bytes::from(format!("deadline:{batch:02}:{index:03}"));
            let value = Bytes::from(format!("strict-acknowledged-value:{batch:02}:{index:03}"));
            write.put(key.to_vec(), value.to_vec(), None)?;
            rows.push((key, value));
        }
        write.commit(WriteOptions::cloud_strict())?;
        acknowledged.extend(rows);
        engine.flush_cf(&cf)?;
    }
    Ok(acknowledged)
}

fn read_acknowledged(engine: &Engine, acknowledged: &[Row]) -> MidgeResult<ReadSnapshot> {
    let cf = engine
        .get_column_family("default")
        .ok_or_else(|| MidgeError::Internal("fixture default column family is missing".into()))?;
    let read = engine.begin_tx(cf.id(), TransactionMode::ReadOnly)?;
    let points = acknowledged
        .iter()
        .map(|(key, _)| read.get(key).map(|value| (key.clone(), value)))
        .collect::<MidgeResult<Vec<_>>>()?;
    let scan = read.scan(&Query::new())?.try_collect()?;
    Ok(ReadSnapshot { points, scan })
}

fn manifest_names(engine: &Engine) -> MidgeResult<BTreeSet<String>> {
    Ok(engine
        .metrics()
        .get_storage_layout()?
        .levels
        .into_iter()
        .flat_map(|level| level.files.into_iter().map(|file| file.name))
        .collect())
}

fn assert_exact_rows(observed: MidgeResult<ReadSnapshot>, acknowledged: &[Row]) {
    let observed = observed.expect("read every acknowledged row and the full keyset");
    let expected_points = acknowledged
        .iter()
        .map(|(key, value)| (key.clone(), Some(value.clone())))
        .collect::<Vec<_>>();
    assert_eq!(observed.points, expected_points);
    assert_eq!(observed.scan, acknowledged);
    assert_eq!(observed.scan.len(), BATCHES * RECORDS_PER_BATCH);
}

#[test]
fn should_retain_acknowledged_inputs_when_expired_publication_remains_owned_during_shutdown() {
    // Arrange: each real CloudStrict acknowledgement is followed by a flush.
    let directory = tempfile::tempdir().expect("create database directory");
    let options = options(directory.path());
    let mut engine = Engine::open(options.clone()).expect("open simulated cloud engine");
    let acknowledged = acknowledge_flushed_batches(&engine).expect("acknowledge four real batches");
    let inputs = manifest_names(&engine).expect("capture authoritative input names");
    let scenario = fail::FailScenario::setup();
    let hold = Arc::new(PublicationHold::default());
    let release = ReleasePublication(Arc::clone(&hold));
    let entered_rx = install_publication_hold(&hold);

    // Act: abandoning the compact response does not cancel the owned worker.
    let (entered, compact_result, caller_elapsed) = std::thread::scope(|scope| {
        let compact = scope.spawn(|| {
            let started = Instant::now();
            (engine.compact_all(), started.elapsed())
        });
        let entered = entered_rx.recv_timeout(OBSERVATION_WAIT);
        let (result, elapsed) = compact.join().expect("join bounded CompactAll caller");
        (entered, result, elapsed)
    });
    let held_layout = manifest_names(&engine);
    let held_rows = read_acknowledged(&engine, &acknowledged);
    let still_owned = hold.exits.load(Ordering::Acquire);
    let shutdown_started = Instant::now();
    let first_shutdown = engine.shutdown(SHORT_SHUTDOWN);
    let shutdown_elapsed = shutdown_started.elapsed();
    let mut contender = Engine::open(options.clone());
    hold.release();
    let final_shutdown = engine.shutdown(CLEANUP_WAIT);
    if let Ok(competing) = &mut contender {
        let _ = competing.shutdown(CLEANUP_WAIT);
    }
    let contender_error = contender.err();
    drop(release);
    scenario.teardown();
    drop(engine);
    let mut reopened =
        Engine::open(options).expect("reopen after actual worker and fencing cleanup");
    let recovered_names = manifest_names(&reopened);
    let recovered_rows = read_acknowledged(&reopened, &acknowledged);
    let reopened_shutdown = reopened.shutdown(CLEANUP_WAIT);

    // Assert: expiry starts no new manifest switch, and every ACK survives.
    entered.expect("actual output proof completed before caller expiry");
    assert!(
        matches!(compact_result, Err(MidgeError::Timeout(_))),
        "{compact_result:?}"
    );
    assert!(caller_elapsed >= CALLER_BUDGET);
    assert!(caller_elapsed < OBSERVATION_WAIT, "{caller_elapsed:?}");
    assert_eq!(inputs.len(), BATCHES);
    assert_eq!(still_owned, 0, "publisher must still own the held callback");
    assert!(
        matches!(first_shutdown, Err(MidgeError::Timeout(_))),
        "{first_shutdown:?}"
    );
    assert!(
        shutdown_elapsed < Duration::from_secs(1),
        "{shutdown_elapsed:?}"
    );
    assert!(
        matches!(contender_error, Some(MidgeError::LeaseHeld(_))),
        "{contender_error:?}"
    );
    final_shutdown.expect("released publisher must join before writer fencing releases");
    assert_eq!(hold.exits.load(Ordering::Acquire), 1);
    assert!(
        !hold.exhausted.load(Ordering::Acquire),
        "finite safety release was not used"
    );
    assert_eq!(held_layout.expect("read held manifest"), inputs);
    assert_eq!(recovered_names.expect("read recovered manifest"), inputs);
    assert_exact_rows(held_rows, &acknowledged);
    assert_exact_rows(recovered_rows, &acknowledged);
    reopened_shutdown.expect("shutdown exact recovered dataset");
}

#[test]
fn should_recover_exact_acknowledged_rows_when_manual_compaction_finishes_within_budget() {
    // Arrange
    let directory = tempfile::tempdir().expect("create database directory");
    let options = options(directory.path());
    let mut engine = Engine::open(options.clone()).expect("open healthy simulated cloud engine");
    let acknowledged = acknowledge_flushed_batches(&engine).expect("acknowledge four real batches");
    let inputs = manifest_names(&engine).expect("capture healthy input names");

    // Act
    let compact_result = engine.compact_all();
    let compacted_names = manifest_names(&engine);
    let metrics = engine.metrics().get_runtime_metrics();
    let compacted_rows = read_acknowledged(&engine, &acknowledged);
    let shutdown = engine.shutdown(CLEANUP_WAIT);
    drop(engine);
    let mut reopened = Engine::open(options).expect("reopen normally compacted dataset");
    let recovered_rows = read_acknowledged(&reopened, &acknowledged);
    let recovered_names = manifest_names(&reopened);
    let reopened_shutdown = reopened.shutdown(CLEANUP_WAIT);

    // Assert
    compact_result.expect("healthy manual compaction must complete inside its original budget");
    shutdown.expect("shutdown healthy compacted engine");
    let compacted_names = compacted_names.expect("read actual replacement manifest");
    assert_eq!(inputs.len(), BATCHES);
    assert!(!compacted_names.is_empty());
    assert!(compacted_names.is_disjoint(&inputs));
    assert_eq!(
        recovered_names.expect("read healthy recovered manifest"),
        compacted_names
    );
    let metrics = metrics.expect("read actual compaction completion metrics");
    assert!(metrics.compactions_run > 0);
    assert_eq!(metrics.compaction_failures, 0);
    assert_eq!(metrics.active_compactions, 0);
    assert_exact_rows(compacted_rows, &acknowledged);
    assert_exact_rows(recovered_rows, &acknowledged);
    reopened_shutdown.expect("shutdown healthy recovered engine");
}
