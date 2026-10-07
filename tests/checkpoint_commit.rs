//! Scripted backpressure policy controls with separately identified real ACK work.

#[path = "../benches/bench_support/checkpoint_commit.rs"]
mod checkpoint_commit;

use checkpoint_commit::{commit_with_backpressure, Observations};
use cntryl_midge::{Engine, MidgeError, OpenOptions, Query, TransactionMode, WriteOptions};
use std::cell::Cell;
use std::time::{Duration, Instant};

const ROWS: usize = 32;

fn strict_rows(engine: &Engine) -> cntryl_midge::MidgeResult<()> {
    let family = engine.get_column_family("default").unwrap();
    let mut transaction = engine.begin_tx(family.id(), TransactionMode::ReadWrite)?;
    for row in 0..ROWS {
        transaction.put(
            format!("row-{row:04}").into_bytes(),
            vec![u8::try_from(row).unwrap(); 128],
            None,
        )?;
    }
    transaction.commit(WriteOptions::sync())
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
    assert_eq!(rows.len(), ROWS);
    for (index, (key, value)) in rows.into_iter().enumerate() {
        assert_eq!(key.as_ref(), format!("row-{index:04}").as_bytes());
        assert_eq!(
            value.as_ref(),
            vec![u8::try_from(index).unwrap(); 128].as_slice()
        );
    }
}

#[test]
fn should_retry_scripted_stall_when_next_attempt_receives_actual_strict_ack() {
    // Arrange: the first rejection is scripted; only the second performs real Engine work.
    let directory = tempfile::tempdir().unwrap();
    let options = OpenOptions::local(directory.path())
        .background_compaction(false)
        .build()
        .unwrap();
    let mut engine = Engine::open(options.clone()).unwrap();
    let family = engine.get_column_family("default").unwrap();
    let attempts = Cell::new(0);
    let actual_acks = Cell::new(0);
    let mut observations = Observations::default();

    // Act: same shared helper as the benchmark, real public stall wait, real strict transaction.
    let result = commit_with_backpressure(
        Instant::now() + Duration::from_secs(30),
        Duration::from_secs(30),
        &mut observations,
        Instant::now,
        || {
            attempts.set(attempts.get() + 1);
            if attempts.get() == 1 {
                return Err(MidgeError::WriteStall(
                    "scripted admission rejection".into(),
                ));
            }
            strict_rows(&engine)?;
            actual_acks.set(actual_acks.get() + 1);
            Ok(())
        },
        |timeout| engine.wait_for_write_stall_clear(family.id(), timeout),
    );
    engine.shutdown(Duration::from_secs(30)).unwrap();
    drop(engine);
    let mut reopened = Engine::open(options).unwrap();
    if actual_acks.get() == 1 {
        exact_rows(&reopened);
    }
    reopened.shutdown(Duration::from_secs(30)).unwrap();

    // Assert: no scripted success, duplicate logical commit or false ACK credit.
    assert!(result.is_ok(), "shared commit policy returned {result:?}");
    assert_eq!(actual_acks.get(), 1);
    assert_eq!(observations.attempts, 2);
    assert_eq!(observations.successful_commits, 1);
    assert_eq!(observations.write_stalls, 1);
    assert_eq!(observations.wait_calls, 1);
    assert_eq!(
        observations.last_stall.as_deref(),
        Some("scripted admission rejection")
    );
}

#[test]
fn should_invoke_neither_commit_nor_wait_when_original_cell_budget_is_expired() {
    // Arrange
    let start = Instant::now();
    let attempts = Cell::new(0);
    let waits = Cell::new(0);
    let mut observations = Observations::default();

    // Act
    let result = commit_with_backpressure(
        start,
        Duration::from_secs(30),
        &mut observations,
        || start,
        || {
            attempts.set(attempts.get() + 1);
            Ok(())
        },
        |_| {
            waits.set(waits.get() + 1);
            Ok(true)
        },
    );

    // Assert: constructed-clock policy control only.
    assert!(matches!(result, Err(MidgeError::Timeout(_))), "{result:?}");
    assert_eq!(attempts.get(), 0);
    assert_eq!(waits.get(), 0);
    assert_eq!(observations.attempts, 0);
    assert_eq!(observations.successful_commits, 0);
}

#[test]
fn should_end_permanent_stall_when_original_logical_budget_expires() {
    // Arrange: virtual time advances only by the requested real-wait allowance.
    let start = Instant::now();
    let clock = Cell::new(start);
    let mut observations = Observations::default();
    let mut slices = Vec::new();

    // Act
    let result = commit_with_backpressure(
        start + Duration::from_mins(1),
        Duration::from_secs(3),
        &mut observations,
        || clock.get(),
        || {
            Err(MidgeError::WriteStall(
                "permanent constructed pressure".into(),
            ))
        },
        |timeout| {
            slices.push(timeout);
            clock.set(clock.get() + timeout);
            Ok(false)
        },
    );

    // Assert
    assert!(matches!(result, Err(MidgeError::Timeout(_))), "{result:?}");
    assert_eq!(clock.get().duration_since(start), Duration::from_secs(3));
    assert_eq!(slices, vec![Duration::from_secs(1); 3]);
    assert_eq!(observations.attempts, 1);
    assert_eq!(observations.successful_commits, 0);
    assert_eq!(observations.wait_calls, 3);
    assert_eq!(observations.wait_timeouts, 3);
    assert_eq!(observations.wait_elapsed_ns, 3_000_000_000);
}

#[test]
fn should_preserve_original_allowance_when_clear_signals_lead_to_more_stalls() {
    // Arrange
    let start = Instant::now();
    let clock = Cell::new(start);
    let mut observations = Observations::default();

    // Act: clear signals are scripted; every commit remains rejected.
    let result = commit_with_backpressure(
        start + Duration::from_mins(1),
        Duration::from_secs(2),
        &mut observations,
        || clock.get(),
        || Err(MidgeError::WriteStall("still rejected".into())),
        |timeout| {
            clock.set(clock.get() + timeout);
            Ok(true)
        },
    );

    // Assert: clear never means success and later attempts never refresh the clock.
    assert!(matches!(result, Err(MidgeError::Timeout(_))), "{result:?}");
    assert_eq!(clock.get().duration_since(start), Duration::from_secs(2));
    assert_eq!(observations.attempts, 2);
    assert_eq!(observations.successful_commits, 0);
    assert_eq!(observations.wait_calls, 2);
    assert_eq!(observations.wait_timeouts, 0);
    assert_eq!(observations.write_stalls, 2);
}

#[test]
fn should_cap_stall_allowance_when_requested_budget_exceeds_thirty_seconds() {
    // Arrange
    let start = Instant::now();
    let clock = Cell::new(start);
    let mut observations = Observations::default();

    // Act
    let result = commit_with_backpressure(
        start + Duration::from_secs(90),
        Duration::from_mins(1),
        &mut observations,
        || clock.get(),
        || Err(MidgeError::WriteStall("pressure".into())),
        |timeout| {
            clock.set(clock.get() + timeout);
            Ok(false)
        },
    );

    // Assert: constructed clock proves the immutable policy cap, not a wall-clock syscall bound.
    assert!(matches!(result, Err(MidgeError::Timeout(_))), "{result:?}");
    assert_eq!(clock.get().duration_since(start), Duration::from_secs(30));
    assert_eq!(observations.wait_calls, 30);
    assert_eq!(observations.successful_commits, 0);
}

#[test]
fn should_clamp_wait_to_original_cell_budget_when_it_is_shorter_than_stall_allowance() {
    // Arrange
    let start = Instant::now();
    let clock = Cell::new(start);
    let remaining = Duration::from_millis(250);
    let mut observations = Observations::default();
    let mut slices = Vec::new();

    // Act
    let result = commit_with_backpressure(
        start + remaining,
        Duration::from_secs(30),
        &mut observations,
        || clock.get(),
        || Err(MidgeError::WriteStall("pressure".into())),
        |timeout| {
            slices.push(timeout);
            clock.set(clock.get() + timeout);
            Ok(false)
        },
    );

    // Assert
    assert!(matches!(result, Err(MidgeError::Timeout(_))), "{result:?}");
    assert_eq!(slices, vec![remaining]);
    assert_eq!(clock.get().duration_since(start), remaining);
    assert_eq!(observations.successful_commits, 0);
}

#[test]
fn should_preserve_terminal_commit_errors_when_reconstruction_would_be_ambiguous() {
    // Arrange: constructed terminal taxonomy; no Engine/provider outcome is fabricated.
    for error in [
        MidgeError::Timeout("possibly accepted".into()),
        MidgeError::Busy("not a rejected L0 admission".into()),
        MidgeError::NoSpace("storage exhaustion".into()),
        MidgeError::Fenced("terminal authority loss".into()),
        MidgeError::Corruption("bad data".into()),
        MidgeError::ResourceLimit("permanent bound".into()),
    ] {
        let expected = error.to_string();
        let expected_kind = std::mem::discriminant(&error);
        let mut error = Some(error);
        let mut observations = Observations::default();
        let waits = Cell::new(0);

        // Act
        let result = commit_with_backpressure(
            Instant::now() + Duration::from_secs(30),
            Duration::from_secs(30),
            &mut observations,
            Instant::now,
            || Err(error.take().expect("terminal operation must not retry")),
            |_| {
                waits.set(waits.get() + 1);
                Ok(true)
            },
        );

        // Assert
        let reported = result.unwrap_err();
        assert_eq!(std::mem::discriminant(&reported), expected_kind);
        assert_eq!(reported.to_string(), expected);
        assert_eq!(observations.attempts, 1);
        assert_eq!(observations.successful_commits, 0);
        assert_eq!(waits.get(), 0);
    }
}

#[test]
fn should_preserve_terminal_wait_error_when_rejected_commit_has_not_succeeded() {
    // Arrange
    let mut observations = Observations::default();

    // Act
    let result = commit_with_backpressure(
        Instant::now() + Duration::from_secs(30),
        Duration::from_secs(30),
        &mut observations,
        Instant::now,
        || Err(MidgeError::WriteStall("rejected".into())),
        |_| Err(MidgeError::Fenced("lost while waiting".into())),
    );

    // Assert
    assert!(
        matches!(result, Err(MidgeError::Fenced(ref detail)) if detail == "lost while waiting")
    );
    assert_eq!(observations.attempts, 1);
    assert_eq!(observations.wait_calls, 1);
    assert_eq!(observations.successful_commits, 0);
}
