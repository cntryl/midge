//! Actual startup measurements through a private per-thread tracing subscriber.

use super::{Recorder, Snapshot};
use cntryl_midge::{Engine, MemoryBudget, OpenOptions, Query, TransactionMode, WriteOptions};
use std::time::Duration;
use tracing_subscriber::{layer::SubscriberExt, Layer};

const TIMEOUT: Duration = Duration::from_secs(30);

fn recorded<T>(operation: impl FnOnce() -> T) -> (T, Snapshot) {
    let recorder = Recorder::default();
    let subscriber = tracing_subscriber::registry().with(recorder.clone().with_filter(
        tracing_subscriber::filter::filter_fn(|metadata| {
            matches!(metadata.target(), "midge::cloud_io" | "midge::recovery")
        }),
    ));
    let dispatch = tracing::Dispatch::new(subscriber);
    // The public timed open captures this caller dispatcher for its owned worker.
    let result = tracing::dispatcher::with_default(&dispatch, operation);
    (result, recorder.snapshot())
}

fn recorded_open(options: &OpenOptions) -> (Engine, Snapshot) {
    recorded(|| Engine::open(options.clone()).expect("actual timed engine open"))
}

fn acknowledge_rows(engine: &Engine, rows: &[(Vec<u8>, Vec<u8>)]) {
    let cf = engine.get_column_family("default").expect("default family");
    let mut transaction = engine
        .begin_tx(cf.id(), TransactionMode::ReadWrite)
        .expect("actual write transaction");
    for (key, value) in rows {
        transaction
            .put(key.clone(), value.clone(), None)
            .expect("write acknowledged row");
    }
    transaction
        .commit(WriteOptions::cloud_strict())
        .expect("actual strict cloud acknowledgment");
    engine.flush_cf(&cf).expect("actual publication");
}

fn recovered_rows(engine: &Engine) -> Vec<(Vec<u8>, Vec<u8>)> {
    let cf = engine.get_column_family("default").expect("default family");
    let transaction = engine
        .begin_tx(cf.id(), TransactionMode::ReadOnly)
        .expect("actual reopened read transaction");
    transaction
        .scan(&Query::new())
        .expect("complete reopened keyset")
        .map(|entry| {
            let (key, value) = entry.expect("reopened row");
            assert_eq!(
                transaction.get(&key).expect("reopened point read"),
                Some(value.clone())
            );
            (key.to_vec(), value.to_vec())
        })
        .collect()
}

fn assert_one_completed_phase(snapshot: &Snapshot) {
    for phase in [
        "wal_replay",
        "open_preparation",
        "lease_epoch_floor",
        "lease_acquisition",
        "storage_materialization",
        "wal_plan",
        "replay_and_repair",
        "runtime_start",
    ] {
        let totals = snapshot
            .recovery_phases
            .get(phase)
            .expect("actual startup phase recorded");
        assert_eq!(totals.get("count"), Some(&1), "{phase}: {totals:?}");
        assert_eq!(totals.get("started_count"), Some(&1), "{phase}: {totals:?}");
        assert_eq!(totals.get("failed"), Some(&0), "{phase}: {totals:?}");
        assert!(totals.contains_key("elapsed_ns"), "{phase}: {totals:?}");
    }
}

#[test]
fn should_count_one_completed_replay_when_timed_engine_reopens_acknowledged_rows() {
    // Arrange: real filesystem-backed cloud recovery without a global subscriber.
    let directory = tempfile::tempdir().expect("actual recovery directory");
    let options = OpenOptions::cloud_simulated(directory.path(), "telemetry", "replay")
        .memory_budget(MemoryBudget::Bytes(64 * 1024 * 1024))
        .background_compaction(false)
        .open_timeout(TIMEOUT)
        .build()
        .expect("actual timed cloud options");
    let expected: Vec<_> = (0..32)
        .map(|index| {
            (
                format!("key-{index:02}").into_bytes(),
                format!("acknowledged-value-{index:02}").into_bytes(),
            )
        })
        .collect();

    // Act: each owned startup worker inherits only its caller's private recorder.
    let (mut writer, initial) = recorded_open(&options);
    acknowledge_rows(&writer, &expected);
    writer.shutdown(TIMEOUT).expect("close acknowledged engine");
    drop(writer);
    let (mut reopened, recovered) = recorded_open(&options);
    let actual = recovered_rows(&reopened);
    reopened.shutdown(TIMEOUT).expect("close recovered engine");
    drop(reopened);

    // Assert: exact acknowledged state is verified before the phase-count contract.
    assert_eq!(actual, expected);
    assert_one_completed_phase(&initial);
    assert_one_completed_phase(&recovered);
}

#[test]
fn should_count_failed_lease_acquisition_when_actual_owner_rejects_another_timed_open() {
    // Arrange: a genuine live owner retains the filesystem-backed cloud lease.
    let directory = tempfile::tempdir().expect("actual held lease directory");
    let options = OpenOptions::cloud_simulated(directory.path(), "telemetry", "held")
        .memory_budget(MemoryBudget::Bytes(64 * 1024 * 1024))
        .background_compaction(false)
        .open_timeout(TIMEOUT)
        .build()
        .expect("actual timed cloud options");
    let (mut owner, _) = recorded_open(&options);

    // Act: the second real startup must fail before entering storage/replay.
    let (outcome, rejected) = recorded(|| Engine::open(options));
    owner.shutdown(TIMEOUT).expect("close actual lease owner");
    drop(owner);

    // Assert: failure remains one completed attempt plus one diagnostic start.
    assert!(matches!(
        outcome,
        Err(cntryl_midge::MidgeError::LeaseHeld(_))
    ));
    for phase in ["lease_acquisition", "open_preparation"] {
        let totals = rejected
            .recovery_phases
            .get(phase)
            .expect("actual rejected startup phase");
        assert_eq!(totals.get("count"), Some(&1), "{phase}: {totals:?}");
        assert_eq!(totals.get("started_count"), Some(&1), "{phase}: {totals:?}");
        assert_eq!(totals.get("failed"), Some(&1), "{phase}: {totals:?}");
        assert!(totals.contains_key("elapsed_ns"), "{phase}: {totals:?}");
    }
    assert!(!rejected.recovery_phases.contains_key("wal_replay"));
    assert!(!rejected
        .recovery_phases
        .contains_key("storage_materialization"));
}
