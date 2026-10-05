//! Public caller and background worker response ownership.

use cntryl_midge::{Engine, OpenOptions, TransactionMode, WriteOptions};
use std::time::{Duration, Instant};

fn write_generations(engine: &Engine) {
    let cf = engine.get_column_family("default").expect("default family");
    for generation in 0..4 {
        let mut tx = engine
            .begin_tx(cf.id(), TransactionMode::ReadWrite)
            .expect("begin generation");
        for key in 0..16 {
            tx.put(
                format!("key-{key:02}").into_bytes(),
                format!("generation-{generation}").into_bytes(),
                None,
            )
            .expect("stage exact value");
        }
        tx.commit(WriteOptions::sync()).expect("commit generation");
        engine.flush_cf(&cf).expect("publish generation");
    }
}

fn assert_latest_values(engine: &Engine) {
    let cf = engine.get_column_family("default").expect("default family");
    let tx = engine
        .begin_tx(cf.id(), TransactionMode::ReadOnly)
        .expect("read compacted values");
    for key in 0..16 {
        let value = tx
            .get(format!("key-{key:02}").as_bytes())
            .expect("read key");
        assert_eq!(value.as_deref(), Some(b"generation-3".as_slice()));
    }
}

#[test]
fn should_complete_manual_compaction_without_false_late_worker_response() {
    // Arrange
    let directory = tempfile::tempdir().expect("database directory");
    let options = OpenOptions::local(directory.path())
        .background_compaction(false)
        .build()
        .expect("options");
    let mut engine = Engine::open(options.clone()).expect("open engine");
    write_generations(&engine);
    let before = engine
        .metrics()
        .get_runtime_metrics()
        .expect("before metrics");

    // Act
    engine
        .compact_all()
        .expect("actual manual caller completes");
    let after = engine
        .metrics()
        .get_runtime_metrics()
        .expect("after metrics");

    // Assert: actual publication and public data checks precede the diagnostic gate.
    assert!(after.compactions_run > before.compactions_run);
    assert_latest_values(&engine);
    engine
        .shutdown(Duration::from_secs(10))
        .expect("shutdown writer");
    let mut reopened = Engine::open(options).expect("reopen published data");
    assert_latest_values(&reopened);
    reopened
        .shutdown(Duration::from_secs(10))
        .expect("shutdown reopened engine");
    assert_eq!(
        after.abandoned_runtime_requests_total,
        before.abandoned_runtime_requests_total
    );
    assert_eq!(
        after.late_runtime_responses_total, before.late_runtime_responses_total,
        "a background worker notification has no caller response obligation"
    );
}

#[test]
fn should_complete_automatic_compaction_without_false_late_caller_response() {
    // Arrange
    let directory = tempfile::tempdir().expect("database directory");
    let mut engine = Engine::open(
        OpenOptions::local(directory.path())
            .background_compaction(true)
            .build()
            .expect("options"),
    )
    .expect("open engine");
    let before = engine
        .metrics()
        .get_runtime_metrics()
        .expect("before metrics");

    // Act: no public compact_all call, only normal flush-driven maintenance.
    write_generations(&engine);
    let deadline = Instant::now() + Duration::from_secs(10);
    let after = loop {
        let metrics = engine
            .metrics()
            .get_runtime_metrics()
            .expect("maintenance metrics");
        if metrics.compactions_run > before.compactions_run {
            break metrics;
        }
        assert!(
            Instant::now() < deadline,
            "automatic compaction did not publish: {metrics:?}"
        );
        std::thread::sleep(Duration::from_millis(5));
    };

    // Assert
    assert_latest_values(&engine);
    engine
        .shutdown(Duration::from_secs(10))
        .expect("shutdown maintenance engine");
    assert_eq!(
        after.abandoned_runtime_requests_total,
        before.abandoned_runtime_requests_total
    );
    assert_eq!(
        after.late_runtime_responses_total, before.late_runtime_responses_total,
        "successful automatic compaction must not be counted as a late caller response"
    );
}
