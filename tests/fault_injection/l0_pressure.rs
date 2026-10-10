//! Deterministic public admission rejection while real pressure recovery is held.
use cntryl_midge::{Engine, MidgeError, OpenOptions, TransactionMode, WriteOptions};
use std::time::Duration;

#[test]
fn should_reject_before_wal_when_l0_pressure_recovery_is_held() {
    // Arrange: the hook defers pressure-compaction admission without blocking
    // the event loop, so every public write and metrics request can complete.
    let scenario = fail::FailScenario::setup();
    let held = "midge::compaction::defer_pressure_recovery";
    fail::cfg(held, "return").unwrap();
    let dir = tempfile::tempdir().unwrap();
    let options = || {
        OpenOptions::local(dir.path())
            .background_compaction(false)
            .build()
            .unwrap()
    };
    let mut engine = Engine::open(options()).unwrap();
    let cf = engine.create_column_family("held-l0-pressure").unwrap();
    let mut rejected = None;

    // Act: grow genuine L0 debt until admission deterministically rejects.
    for index in 0_u32..64 {
        let before = engine.metrics().get_runtime_metrics().unwrap();
        let mut tx = engine
            .begin_tx(cf.id(), TransactionMode::ReadWrite)
            .unwrap();
        tx.put(index.to_be_bytes().to_vec(), b"value".to_vec(), None)
            .unwrap();
        match tx.commit(WriteOptions::sync()) {
            Ok(()) => engine.flush_cf(&cf).unwrap(),
            Err(MidgeError::WriteStall(_)) => {
                let after = engine.metrics().get_runtime_metrics().unwrap();
                assert_eq!(after.wal_append_count, before.wal_append_count);
                assert_eq!(after.current_sequence, before.current_sequence);
                let read = engine.begin_tx(cf.id(), TransactionMode::ReadOnly).unwrap();
                assert_eq!(read.get(&index.to_be_bytes()).unwrap(), None);
                rejected = Some(index);
                break;
            }
            Err(error) => panic!("unexpected write rejection: {error}"),
        }
    }
    let index = rejected.expect("held recovery must force public WriteStall coverage on every run");
    let counts = cntryl_midge::__internal::diagnostics::write_admission_snapshot(&engine);
    assert_eq!(counts.commit_write_stall_total, 1);
    assert_eq!(counts.l0_total + counts.ingest_hint_total, 1);
    assert_eq!(
        counts.queue_total + counts.cloud_generation_total + counts.cloud_wal_total,
        0
    );
    let before_recovery = engine.metrics().get_runtime_metrics().unwrap();
    assert_eq!(before_recovery.compactions_run, 0);
    fail::remove(held);
    assert!(engine
        .wait_for_write_stall_clear(cf.id(), Duration::from_secs(10))
        .unwrap());
    let mut retry = engine
        .begin_tx(cf.id(), TransactionMode::ReadWrite)
        .unwrap();
    retry
        .put(index.to_be_bytes().to_vec(), b"value".to_vec(), None)
        .unwrap();
    retry.commit(WriteOptions::sync()).unwrap();
    engine.flush_cf(&cf).unwrap();

    // Assert: release allows actual compaction publication, then durable retry.
    let recovered = engine.metrics().get_runtime_metrics().unwrap();
    assert!(recovered.compactions_run > 0);
    assert!(recovered.compaction_bytes_rewritten > 0);
    engine.shutdown(Duration::from_secs(10)).unwrap();
    drop(engine);
    scenario.teardown();
    let mut reopened = Engine::open(options()).unwrap();
    let cf = reopened.get_column_family("held-l0-pressure").unwrap();
    let read = reopened
        .begin_tx(cf.id(), TransactionMode::ReadOnly)
        .unwrap();
    for acknowledged in 0..=index {
        assert_eq!(
            read.get(&acknowledged.to_be_bytes()).unwrap().as_deref(),
            Some(b"value".as_slice())
        );
    }
    drop(read);
    reopened.shutdown(Duration::from_secs(10)).unwrap();
}
