use cntryl_midge::WriteBackpressureConfig;
use cntryl_midge::{Engine, MidgeError, MidgeResult, OpenOptions, TransactionMode, WriteOptions};
use std::time::{Duration, Instant};

fn write(engine: &Engine, ordinal: u32) -> MidgeResult<()> {
    let family = engine.get_column_family("default").unwrap();
    let mut tx = engine.begin_tx(family.id(), TransactionMode::ReadWrite)?;
    tx.put(ordinal.to_be_bytes().to_vec(), b"value".to_vec(), None)?;
    tx.commit(WriteOptions::sync())
}

#[test]
fn should_delay_successful_admission_when_published_l0_reaches_soft_band() -> MidgeResult<()> {
    // Arrange: no concurrent compaction can remove the controlled L0 debt.
    let directory = tempfile::tempdir().unwrap();
    let options = OpenOptions::local(directory.path())
        .background_compaction(false)
        .write_backpressure(WriteBackpressureConfig {
            l0_soft_limit: Some(1),
            max_admission_delay: Duration::from_millis(20),
            ..Default::default()
        })
        .build()?;
    let mut engine = Engine::open(options.clone())?;
    let family = engine.get_column_family("default").unwrap();
    write(&engine, 0)?;
    engine.flush_cf(&family)?;
    let before = engine.metrics().get_runtime_metrics()?;

    // Act
    let started = Instant::now();
    write(&engine, 1)?;
    let elapsed = started.elapsed();
    let after = engine.metrics().get_runtime_metrics()?;

    // Assert: admission delay is separately observable; the accepted write
    // remains recoverable and ordinary hard pressure has not been reached.
    assert!(elapsed >= Duration::from_millis(15));
    assert_eq!(after.write_slowdown_count, before.write_slowdown_count + 1);
    assert!(after.write_slowdown_ns_total > before.write_slowdown_ns_total);
    assert!(!after.write_stalled);
    engine.shutdown(Duration::from_secs(10))?;
    drop(engine);
    let mut reopened = Engine::open(options)?;
    let read = reopened.begin_tx(family.id(), TransactionMode::ReadOnly)?;
    assert_eq!(read.get(&1_u32.to_be_bytes())?.unwrap().as_ref(), b"value");
    drop(read);
    reopened.shutdown(Duration::from_secs(10))
}

#[test]
fn should_reject_at_hard_l0_ceiling_when_slowdown_is_enabled() -> MidgeResult<()> {
    // Arrange
    let directory = tempfile::tempdir().unwrap();
    let mut engine = Engine::open(
        OpenOptions::local(directory.path())
            .background_compaction(false)
            .write_backpressure(WriteBackpressureConfig {
                l0_soft_limit: Some(1),
                max_admission_delay: Duration::from_millis(1),
                ..Default::default()
            })
            .build()?,
    )?;
    let family = engine.get_column_family("default").unwrap();
    let mut admitted = 0;

    // Act: explicit flushes consume real published generations.
    let rejected = loop {
        assert!(admitted < 64, "hard admission must remain bounded");
        match write(&engine, admitted) {
            Ok(()) => {
                admitted += 1;
                engine.flush_cf(&family)?;
            }
            Err(error) => break error,
        }
    };

    // Assert
    assert!(admitted > 1);
    assert!(
        matches!(rejected, MidgeError::WriteStall(_)),
        "{rejected:?}"
    );
    let metrics = engine.metrics().get_runtime_metrics()?;
    assert!(metrics.write_stalls_compaction_total > 0, "{metrics:?}");
    engine.shutdown(Duration::from_secs(10))
}

#[test]
fn should_preserve_immediate_admission_when_no_soft_limits_are_configured() -> MidgeResult<()> {
    // Arrange
    let directory = tempfile::tempdir().unwrap();
    let mut engine = Engine::open(
        OpenOptions::local(directory.path())
            .background_compaction(false)
            .build()?,
    )?;
    let family = engine.get_column_family("default").unwrap();
    write(&engine, 0)?;
    engine.flush_cf(&family)?;

    // Act
    write(&engine, 1)?;
    let metrics = engine.metrics().get_runtime_metrics()?;

    // Assert
    assert_eq!(metrics.write_slowdown_count, 0);
    assert_eq!(metrics.write_slowdown_ns_total, 0);
    assert_eq!(metrics.background_rate_limited_bytes, 0);
    engine.shutdown(Duration::from_secs(10))
}

#[test]
fn should_enforce_pending_byte_limit_before_the_l0_hard_ceiling() -> MidgeResult<()> {
    // Arrange: four tiny real SSTs exceed the count trigger but not the L0
    // hard ceiling. The estimate excludes the terminal level from debt.
    let directory = tempfile::tempdir().unwrap();
    let mut engine = Engine::open(
        OpenOptions::local(directory.path())
            .goal(cntryl_midge::Goal::Economy)
            .background_compaction(false)
            .write_backpressure(WriteBackpressureConfig {
                pending_compaction_soft_limit: Some(1),
                pending_compaction_hard_limit: Some(2),
                max_admission_delay: Duration::from_millis(1),
                ..Default::default()
            })
            .build()?,
    )?;
    let family = engine.get_column_family("default").unwrap();
    for ordinal in 0..4 {
        write(&engine, ordinal)?;
        engine.flush_cf(&family)?;
    }
    let before = engine.metrics().get_runtime_metrics()?.current_sequence;

    // Act
    let rejected = write(&engine, 5);
    let metrics = engine.metrics().get_runtime_metrics()?;

    // Assert
    assert!(
        matches!(&rejected, Err(MidgeError::WriteStall(message))
            if message.contains("pending compaction")),
        "{rejected:?}"
    );
    assert_eq!(metrics.sst_count, 4);
    assert!(metrics.estimated_pending_compaction_bytes >= 2);
    assert_eq!(
        metrics.current_sequence, before,
        "rejection precedes WAL allocation"
    );
    let read = engine.begin_tx(family.id(), TransactionMode::ReadOnly)?;
    assert!(read.get(&5_u32.to_be_bytes())?.is_none());
    drop(read);
    engine.compact_all()?;
    write(&engine, 5)?;
    engine.shutdown(Duration::from_secs(10))?;
    drop(engine);
    let mut reopened = Engine::open(OpenOptions::local(directory.path()).build()?)?;
    let read = reopened.begin_tx(family.id(), TransactionMode::ReadOnly)?;
    assert_eq!(read.get(&5_u32.to_be_bytes())?.unwrap().as_ref(), b"value");
    drop(read);
    reopened.shutdown(Duration::from_secs(10))
}
