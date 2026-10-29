//! Differential testing of the public engine API against a `BTreeMap` model
//! (#951): random multi-key histories with snapshots, TTL, range deletes,
//! buffered and synced commits, flushes, compactions, reopens and power-loss
//! crashes, and a concurrent writer mode reconciled by commit sequence. The
//! libFuzzer target `engine_differential` shares the driver.

#[path = "support/differential.rs"]
mod differential;

use cntryl_midge::__internal::simulation::CrashDevice;
use cntryl_midge::{Clock, Engine, MidgeError, OpenOptions, Query, TransactionMode, WriteOptions};
use differential::{decode, key, rows, run_history, FrozenClock, Model, ScanSpec, Step, WriteOp};
use proptest::prelude::*;
use std::collections::BTreeMap;
use std::sync::atomic::{AtomicUsize, Ordering};
use std::sync::{Arc, Barrier};
use std::time::Duration;

fn cases() -> u32 {
    std::env::var("PROPTEST_CASES")
        .ok()
        .and_then(|cases| cases.parse().ok())
        .unwrap_or(256)
}

proptest! {
    #![proptest_config(ProptestConfig::with_cases(cases()))]

    #[test]
    fn should_match_btreemap_model_given_random_multi_key_history_when_replayed_through_crashes(
        data in proptest::collection::vec(any::<u8>(), 0..320),
    ) {
        // Arrange: the history is decoded from the bytes.
        // Act: every step runs against the engine and the model.
        // Assert: every read, every recovery and the final state agree with
        // the model.
        run_history(&data);
    }
}

#[test]
fn should_preserve_compaction_contract_when_replaying_expiration_regression_seed() {
    // Arrange: preserved proptest seed ed843532a3dc372428651b6e436d64226.
    let data = [
        117, 0, 0, 0, 208, 0, 0, 7, 0, 0, 53, 0, 0, 0, 0, 0, 0, 0, 0, 0, 76, 0, 0, 0, 0, 0, 217,
        33, 0, 154, 0, 55, 7, 0, 7, 0, 15, 0, 180, 0, 0, 98, 0, 45, 7, 1, 7, 1, 76, 0, 0, 0, 0, 0,
        70, 0, 28, 87, 1, 123, 34, 0, 0, 0, 0, 0, 94, 1, 160, 0, 0, 0, 0, 0, 25, 68,
    ];

    // Act: replay every real commit, compaction and recovery.
    // Assert: the driver checks the BTreeMap history at each step; the
    // production plan validator remains enabled.
    run_history(&data);
}

/// The fuzz corpus seeds decode into histories that between them reach every
/// kind of step, and each runs clean.
#[test]
fn should_reach_every_step_kind_given_the_corpus_seeds_when_replayed() {
    // Arrange
    let corpus =
        std::path::Path::new(env!("CARGO_MANIFEST_DIR")).join("fuzz/corpus/engine_differential");
    let mut seeds: Vec<_> = std::fs::read_dir(&corpus)
        .expect("corpus directory")
        .map(|entry| entry.expect("corpus entry").path())
        .collect();
    seeds.sort();
    assert!(
        !seeds.is_empty(),
        "no corpus seeds under {}",
        corpus.display()
    );

    // Act
    let mut kinds = std::collections::BTreeSet::new();
    for seed in &seeds {
        let data = std::fs::read(seed).expect("read seed");
        for step in decode(&data) {
            kinds.insert(step_kind(&step));
        }
        run_history(&data);
    }

    // Assert
    let expected: std::collections::BTreeSet<&str> = [
        "commit",
        "buffered_commit",
        "get",
        "scan",
        "snapshot",
        "snapshot_get",
        "snapshot_scan",
        "advance_clock",
        "flush",
        "compact",
        "reopen",
        "crash",
    ]
    .into_iter()
    .collect();
    assert_eq!(kinds, expected, "step kinds the seeds reach");
}

fn step_kind(step: &Step) -> &'static str {
    match step {
        Step::Commit { sync: true, .. } => "commit",
        Step::Commit { sync: false, .. } => "buffered_commit",
        Step::Get(_) => "get",
        Step::Scan(_) => "scan",
        Step::Snapshot => "snapshot",
        Step::SnapshotGet { .. } => "snapshot_get",
        Step::SnapshotScan { .. } => "snapshot_scan",
        Step::AdvanceClock(_) => "advance_clock",
        Step::Flush => "flush",
        Step::Compact => "compact",
        Step::Reopen => "reopen",
        Step::Crash(_) => "crash",
    }
}

/// The driver swaps out-of-order scan bounds because the engine rejects them;
/// this pins that contract so the swap stays justified.
#[test]
fn should_reject_scan_given_start_above_end_when_queried() {
    // Arrange
    let directory = tempfile::tempdir().expect("database directory");
    let mut engine = Engine::open(
        OpenOptions::local(directory.path())
            .background_compaction(false)
            .build()
            .expect("options"),
    )
    .expect("open engine");
    let cf_id = engine
        .get_column_family("default")
        .expect("default family")
        .id();
    let tx = engine
        .begin_tx(cf_id, TransactionMode::ReadOnly)
        .expect("read transaction");

    // Act
    let rejected = tx
        .scan(
            &Query::new()
                .start_key(bytes::Bytes::from_static(b"b"))
                .end_key(bytes::Bytes::from_static(b"a")),
        )
        .err();

    // Assert
    assert!(
        matches!(rejected, Some(MidgeError::InvalidArgument(_))),
        "scan with start above end: {rejected:?}"
    );
    drop(tx);
    engine.shutdown(Duration::from_secs(30)).expect("shutdown");
}

/// A writer thread: `commits` single-operation sync commits on pseudo-random
/// keys, each returned with the sequence the engine assigned it. Writers
/// start together and count their commits so the test can snapshot midway.
fn spawn_writer(
    engine: Arc<Engine>,
    cf_id: u32,
    writer: u64,
    commits: u64,
    start: Arc<Barrier>,
    completed: Arc<AtomicUsize>,
) -> std::thread::JoinHandle<Vec<(u64, WriteOp)>> {
    std::thread::spawn(move || {
        start.wait();
        let mut committed = Vec::new();
        let mut state = writer.wrapping_mul(0x9e37_79b9_7f4a_7c15) | 1;
        for round in 0..commits {
            state ^= state << 13;
            state ^= state >> 7;
            state ^= state << 17;
            let subject = key(u8::try_from(state % 32).expect("byte"));
            let op = if state.is_multiple_of(5) {
                WriteOp::Delete { key: subject }
            } else {
                WriteOp::Put {
                    key: subject,
                    value: vec![
                        b'w',
                        u8::try_from(writer).expect("byte"),
                        u8::try_from(round).expect("byte"),
                    ],
                    ttl_seconds: None,
                }
            };
            let mut tx = engine
                .begin_tx(cf_id, TransactionMode::ReadWrite)
                .expect("write transaction");
            match &op {
                WriteOp::Put { key, value, .. } => {
                    tx.put(key.clone(), value.clone(), None).expect("put");
                }
                WriteOp::Delete { key } => tx.delete(key.clone()).expect("delete"),
                WriteOp::DeleteRange { .. } => unreachable!("no range deletes here"),
            }
            let sequence = tx
                .commit_with_sequence(WriteOptions::sync())
                .expect("sync commit");
            committed.push((sequence, op));
            completed.fetch_add(1, Ordering::SeqCst);
        }
        committed
    })
}

/// Writers, each committing one operation at a time through
/// `commit_with_sequence`; the model replays all of them in sequence order.
#[test]
fn should_match_sequence_ordered_model_given_concurrent_writers_when_reads_observe_a_snapshot() {
    // Arrange
    const WRITERS: u64 = 4;
    const COMMITS_PER_WRITER: u64 = 24;
    const TOTAL: usize = 4 * 24;
    let directory = tempfile::tempdir().expect("database directory");
    let device = CrashDevice::new();
    let ttl_clock = FrozenClock::new();
    let root: Arc<dyn cntryl_midge::__internal::io::Fs> = Arc::new(device.fs());
    let engine = Arc::new(
        Engine::open(
            OpenOptions::local(directory.path())
                .background_compaction(false)
                .ttl_clock(Arc::clone(&ttl_clock) as Arc<dyn Clock>)
                .filesystem_for_testing(root)
                .build()
                .expect("options"),
        )
        .expect("open engine"),
    );
    let cf_id = engine
        .get_column_family("default")
        .expect("default family")
        .id();
    let start = Arc::new(Barrier::new(usize::try_from(WRITERS).expect("writers")));
    let completed = Arc::new(AtomicUsize::new(0));

    // Act: the writers race; the main thread opens a snapshot once half the
    // commits are in.
    let writers: Vec<_> = (0..WRITERS)
        .map(|writer| {
            spawn_writer(
                Arc::clone(&engine),
                cf_id,
                writer,
                COMMITS_PER_WRITER,
                Arc::clone(&start),
                Arc::clone(&completed),
            )
        })
        .collect();
    while completed.load(Ordering::SeqCst) < TOTAL / 2 {
        std::thread::yield_now();
    }
    let snapshot = engine
        .begin_tx(cf_id, TransactionMode::ReadOnly)
        .expect("snapshot");
    let snapshot_rows = rows(&snapshot, &ScanSpec::EVERYTHING);
    let snapshot_sequence = snapshot
        .commit_with_sequence(WriteOptions::sync())
        .expect("read-only commit reports its snapshot sequence");
    let mut committed: Vec<(u64, WriteOp)> = writers
        .into_iter()
        .flat_map(|writer| writer.join().expect("writer"))
        .collect();
    committed.sort_by_key(|(sequence, _)| *sequence);

    // Assert: sequences are distinct, the snapshot fell strictly inside the
    // run and equals the model at its sequence, and the final state equals
    // the model after every commit.
    assert_eq!(committed.len(), TOTAL);
    assert!(
        committed.windows(2).all(|pair| pair[0].0 < pair[1].0),
        "commit sequences repeat"
    );
    let mut model = Model::default();
    let mut at_snapshot = None;
    for (sequence, op) in &committed {
        if at_snapshot.is_none() && *sequence > snapshot_sequence {
            at_snapshot = Some(model.commits());
        }
        model.commit(ttl_clock.now_millis(), vec![op.clone()], true);
    }
    let at_snapshot = at_snapshot.unwrap_or(model.commits());
    assert!(
        at_snapshot > 0 && at_snapshot < TOTAL,
        "snapshot at sequence {snapshot_sequence} fell at commit {at_snapshot} of {TOTAL}"
    );
    let expected_snapshot: BTreeMap<Vec<u8>, Vec<u8>> =
        model.state_at(at_snapshot, ttl_clock.now_millis());
    assert_eq!(
        snapshot_rows,
        Model::scan(&expected_snapshot, &ScanSpec::EVERYTHING),
        "snapshot at sequence {snapshot_sequence}"
    );
    let final_state = model.state_at(model.commits(), ttl_clock.now_millis());
    let tx = engine
        .begin_tx(cf_id, TransactionMode::ReadOnly)
        .expect("final read");
    assert_eq!(
        rows(&tx, &ScanSpec::EVERYTHING),
        Model::scan(&final_state, &ScanSpec::EVERYTHING),
        "final state"
    );
    drop(tx);
    let Ok(mut engine) = Arc::try_unwrap(engine) else {
        panic!("writers released the engine");
    };
    engine.shutdown(Duration::from_secs(30)).expect("shutdown");
}
