//! Scripted boundary sampling policies; real Engine evidence is identified separately.
#![cfg(feature = "internal-testing")]

#[path = "../benches/bench_support/checkpoint_boundary.rs"]
mod checkpoint_boundary;

use checkpoint_boundary::{capture_metadata_boundary, Observation};
use cntryl_midge::__internal::checkpoint::{
    metrics_handle, Bucket, Counters, LatencyHistogram, Medium, Origin, Snapshot,
};
use cntryl_midge::{Engine, MidgeError, OpenOptions, Query, TransactionMode, WriteOptions};
use std::cell::Cell;
use std::time::{Duration, Instant};

fn constructed_snapshot(active: u64, medium: Medium) -> Snapshot {
    // One synthetic gauge bucket is sufficient for this helper, never a cadence gate input.
    Snapshot {
        owner_id: 1,
        buckets: vec![Bucket {
            origin: Origin::CompactionBeforeGc,
            medium,
            counters: Counters::default(),
            checkpoint_latency: LatencyHistogram::default(),
            full_publication_latency: LatencyHistogram::default(),
            committed_sst_size_log2: vec![0; 64],
            active_operations: active,
        }],
        overflow: false,
        late_operation_writes: 0,
        incomplete_observations: 0,
    }
}

fn exact_rows(engine: &Engine) {
    let family = engine.get_column_family("default").unwrap();
    let transaction = engine
        .begin_tx(family.id(), TransactionMode::ReadOnly)
        .unwrap();
    let rows = transaction
        .scan(&Query::new())
        .unwrap()
        .collect::<Result<Vec<_>, _>>()
        .unwrap();
    assert_eq!(rows.len(), 32);
    for (index, (key, value)) in rows.into_iter().enumerate() {
        assert_eq!(key.as_ref(), format!("row-{index:04}").as_bytes());
        assert_eq!(
            value.as_ref(),
            vec![u8::try_from(index).unwrap(); 128].as_slice()
        );
    }
}

#[test]
fn should_capture_real_engine_after_scripted_active_metadata_boundary() {
    // Arrange: actual local strict ACK and genuine flush; first gauge alone is scripted.
    let directory = tempfile::tempdir().unwrap();
    let options = OpenOptions::local(directory.path())
        .background_compaction(false)
        .build()
        .unwrap();
    let mut engine = Engine::open(options.clone()).unwrap();
    let family = engine.get_column_family("default").unwrap();
    let mut transaction = engine
        .begin_tx(family.id(), TransactionMode::ReadWrite)
        .unwrap();
    for index in 0..32 {
        transaction
            .put(
                format!("row-{index:04}").into_bytes(),
                vec![u8::try_from(index).unwrap(); 128],
                None,
            )
            .unwrap();
    }
    transaction.commit(WriteOptions::sync()).unwrap();
    engine.flush_cf(&family).unwrap();
    let handle = metrics_handle(&engine);
    let started = Instant::now();
    let clock = Cell::new(started);
    let queries = Cell::new(0_u64);
    let mut observation = Observation::default();

    // Act: every sample executes the genuine runtime query before the same-owner snapshot.
    let result = capture_metadata_boundary(
        started + Duration::from_secs(30),
        Duration::from_secs(30),
        &mut observation,
        || clock.get(),
        |remaining| {
            let runtime = engine
                .metrics()
                .get_runtime_metrics_with_timeout(remaining)?;
            let mut snapshot = handle.snapshot();
            queries.set(queries.get() + 1);
            if queries.get() == 1 {
                snapshot
                    .buckets
                    .iter_mut()
                    .find(|bucket| {
                        bucket.origin == Origin::CompactionBeforeGc
                            && bucket.medium == Medium::Persistent
                    })
                    .unwrap()
                    .active_operations = 1;
            }
            Ok((runtime, snapshot))
        },
        |duration| clock.set(clock.get() + duration),
    );
    engine.shutdown(Duration::from_secs(30)).unwrap();
    drop(engine);
    let mut reopened = Engine::open(options).unwrap();
    exact_rows(&reopened);
    reopened.shutdown(Duration::from_secs(30)).unwrap();

    // Assert: no synthetic idle receipt, duplicate ACK or ignored active gauge.
    let captured = result.expect("bounded capture must reach the actual idle snapshot");
    assert_eq!(queries.get(), 2);
    assert_eq!(captured.snapshot.owner_id, handle.snapshot().owner_id);
    assert_eq!(captured.runtime.compactions_run, 0);
    assert_eq!(captured.query_started, started + Duration::from_millis(1));
    assert_eq!(observation.samples, 2);
    assert_eq!(observation.active_samples, 1);
    assert_eq!(observation.pause_calls, 1);
    assert_eq!(observation.final_persistent_active, 0);
    assert!(observation.complete);
}

#[test]
fn should_admit_no_sample_when_original_cell_deadline_is_expired() {
    // Arrange: constructed already-expired immutable clock.
    let started = Instant::now();
    let samples = Cell::new(0);
    let pauses = Cell::new(0);
    let mut observation = Observation::default();
    // Act.
    let result = capture_metadata_boundary(
        started,
        Duration::from_secs(30),
        &mut observation,
        || started,
        |_| {
            samples.set(samples.get() + 1);
            Ok(((), constructed_snapshot(0, Medium::Persistent)))
        },
        |_| pauses.set(pauses.get() + 1),
    );
    // Assert.
    assert!(matches!(result, Err(MidgeError::Timeout(_))));
    assert_eq!(samples.get(), 0);
    assert_eq!(pauses.get(), 0);
    assert!(!observation.complete);
}

#[test]
fn should_end_permanent_active_metadata_when_original_boundary_allowance_expires() {
    // Arrange: constructed activity and clock; no completed workload is fabricated.
    let started = Instant::now();
    let clock = Cell::new(started);
    let mut observation = Observation::default();
    // Act: successful active samples and pauses never reset the three-ms allowance.
    let result = capture_metadata_boundary(
        started + Duration::from_mins(15),
        Duration::from_millis(3),
        &mut observation,
        || clock.get(),
        |_| Ok(((), constructed_snapshot(1, Medium::Persistent))),
        |duration| clock.set(clock.get() + duration),
    );
    // Assert.
    assert!(matches!(result, Err(MidgeError::Timeout(_))));
    assert_eq!(
        clock.get().duration_since(started),
        Duration::from_millis(3)
    );
    assert_eq!(observation.samples, 3);
    assert_eq!(observation.pause_calls, 3);
    assert_eq!(observation.paused_ns, 3_000_000);
    assert_eq!(observation.max_pause_requested_ns, 1_000_000);
    assert!(!observation.complete);
}

#[test]
fn should_cap_sampling_allowance_when_requested_boundary_budget_exceeds_thirty_seconds() {
    // Arrange: constructed expensive successful query, still carrying an active gauge.
    let started = Instant::now();
    let clock = Cell::new(started);
    let mut observation = Observation::default();
    let mut allowances = Vec::new();
    // Act.
    let result = capture_metadata_boundary(
        started + Duration::from_mins(15),
        Duration::from_secs(60),
        &mut observation,
        || clock.get(),
        |remaining| {
            allowances.push(remaining);
            clock.set(clock.get() + remaining.min(Duration::from_secs(10)));
            Ok(((), constructed_snapshot(1, Medium::Persistent)))
        },
        |duration| clock.set(clock.get() + duration),
    );
    // Assert: capped immutable wall deadline, rather than a fresh allowance per query.
    assert!(matches!(result, Err(MidgeError::Timeout(_))));
    assert_eq!(clock.get().duration_since(started), Duration::from_secs(30));
    assert_eq!(allowances.len(), 3);
    assert_eq!(allowances[0], Duration::from_secs(30));
    assert!(allowances.windows(2).all(|pair| pair[1] < pair[0]));
    assert!(!observation.complete);
}

#[test]
fn should_clamp_boundary_sampling_when_original_cell_remainder_is_shorter() {
    // Arrange: constructed cell remainder below the one-ms pause cap.
    let started = Instant::now();
    let clock = Cell::new(started);
    let mut observation = Observation::default();
    let mut allowances = Vec::new();
    let mut pauses = Vec::new();
    // Act.
    let result = capture_metadata_boundary(
        started + Duration::from_micros(250),
        Duration::from_secs(30),
        &mut observation,
        || clock.get(),
        |remaining| {
            allowances.push(remaining);
            Ok(((), constructed_snapshot(1, Medium::Persistent)))
        },
        |duration| {
            pauses.push(duration);
            clock.set(clock.get() + duration);
        },
    );
    // Assert.
    assert!(matches!(result, Err(MidgeError::Timeout(_))));
    assert_eq!(allowances, vec![Duration::from_micros(250)]);
    assert_eq!(pauses, vec![Duration::from_micros(250)]);
    assert_eq!(observation.paused_ns, 250_000);
}

#[test]
fn should_reject_late_idle_sample_when_query_crosses_the_original_deadline() {
    // Arrange: constructed late query success is not permission to accept an expired boundary.
    let started = Instant::now();
    let clock = Cell::new(started);
    let mut observation = Observation::default();
    let pauses = Cell::new(0);
    // Act.
    let result = capture_metadata_boundary(
        started + Duration::from_millis(2),
        Duration::from_secs(30),
        &mut observation,
        || clock.get(),
        |_| {
            clock.set(started + Duration::from_millis(3));
            Ok(((), constructed_snapshot(0, Medium::Persistent)))
        },
        |_| pauses.set(pauses.get() + 1),
    );
    // Assert.
    assert!(matches!(result, Err(MidgeError::Timeout(_))));
    assert_eq!(observation.samples, 1);
    assert_eq!(observation.final_persistent_active, 0);
    assert_eq!(pauses.get(), 0);
    assert!(!observation.complete);
}

#[test]
fn should_preserve_terminal_query_error_when_boundary_cannot_be_observed() {
    // Arrange: constructed proven terminal authority failure.
    let started = Instant::now();
    let mut observation = Observation::default();
    let samples = Cell::new(0);
    let pauses = Cell::new(0);
    // Act.
    let result = capture_metadata_boundary::<()>(
        started + Duration::from_mins(15),
        Duration::from_secs(30),
        &mut observation,
        || started,
        |_| {
            samples.set(samples.get() + 1);
            Err(MidgeError::Fenced("original authority loss".into()))
        },
        |_| pauses.set(pauses.get() + 1),
    );
    // Assert: no retries or reclassification conceal the original query outcome.
    assert!(
        matches!(result, Err(MidgeError::Fenced(ref detail)) if detail == "original authority loss")
    );
    assert_eq!(samples.get(), 1);
    assert_eq!(pauses.get(), 0);
    assert!(!observation.complete);
}

#[test]
fn should_capture_persistent_boundary_when_only_memory_metadata_is_active() {
    // Arrange: explicitly constructed MemoryOnly activity, not global runtime idle evidence.
    let started = Instant::now();
    let mut observation = Observation::default();
    let pauses = Cell::new(0);
    // Act.
    let result = capture_metadata_boundary(
        started + Duration::from_mins(15),
        Duration::from_secs(30),
        &mut observation,
        || started,
        |_| Ok(((), constructed_snapshot(1, Medium::MemoryOnly))),
        |_| pauses.set(pauses.get() + 1),
    );
    // Assert: existing persistent gate remains the boundary contract.
    assert!(result.is_ok());
    assert_eq!(observation.samples, 1);
    assert_eq!(observation.final_persistent_active, 0);
    assert_eq!(pauses.get(), 0);
    assert!(observation.complete);
}

fn scripted_timed_boundary(
    original_deadline: Instant,
    clock: &Cell<Instant>,
    observation: &mut Observation,
) -> checkpoint_boundary::Boundary<usize> {
    let candidates = Cell::new(0);
    capture_metadata_boundary(
        original_deadline,
        Duration::from_secs(30),
        observation,
        || clock.get(),
        |_| {
            candidates.set(candidates.get() + 1);
            let active = candidates.get() == 1;
            clock.set(clock.get() + Duration::from_millis(if active { 5 } else { 2 }));
            Ok((
                candidates.get(),
                constructed_snapshot(u64::from(active), Medium::Persistent),
            ))
        },
        |duration| clock.set(clock.get() + duration),
    )
    .unwrap()
}

#[test]
fn should_return_final_query_clock_when_warmup_candidates_precede_measured_ingestion() {
    // Arrange: constructed timed candidates; no wall-clock I/O or Engine proof is claimed.
    let original_started = Instant::now();
    let original_deadline = original_started + Duration::from_mins(15);
    let clock = Cell::new(original_started);
    let mut before_observation = Observation::default();
    let mut after_observation = Observation::default();

    // Act: same helper, then the actual benchmark's start/finish clock formulas.
    let before = scripted_timed_boundary(original_deadline, &clock, &mut before_observation);
    let measured_started = before.query_started;
    let after = scripted_timed_boundary(original_deadline, &clock, &mut after_observation);
    let measured_elapsed = clock.get().duration_since(measured_started);

    // Assert: prior warmup query+pause excluded, final accepted query and all end work included.
    assert_eq!(before.runtime, 2);
    assert_eq!(after.runtime, 2);
    assert_eq!(
        measured_started.duration_since(original_started),
        Duration::from_millis(6)
    );
    assert_eq!(before_observation.elapsed_ns, 8_000_000);
    assert_eq!(before_observation.paused_ns, 1_000_000);
    assert_eq!(after_observation.elapsed_ns, 8_000_000);
    assert_eq!(after_observation.paused_ns, 1_000_000);
    assert_eq!(measured_elapsed, Duration::from_millis(10));
    assert!(u128::from(after_observation.elapsed_ns) <= measured_elapsed.as_nanos());
}
