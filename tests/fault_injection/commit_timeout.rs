//! A commit that the runtime admitted and then answered too late may still be
//! applied and made durable. Its error must say the outcome is unknown rather
//! than invite a retry (#901).
use cntryl_midge::{Engine, MidgeError, OpenOptions, Severity, TransactionMode, WriteOptions};
use std::time::Duration;

#[test]
fn should_classify_commit_timeout_as_outcome_unknown_given_admitted_write_when_response_arrives_late(
) {
    // Arrange: the strict durability step pauses on the event loop for longer
    // than the caller's response budget. The record is already appended and is
    // synced after the pause, so the caller stops waiting on a write that lands.
    let scenario = fail::FailScenario::setup();
    let paused = "midge::wal::txn_after_commit_append_before_sync";
    fail::cfg(paused, "1*sleep(2500)->off").unwrap();
    let dir = tempfile::tempdir().unwrap();
    let options = || {
        OpenOptions::local(dir.path())
            .background_compaction(false)
            // The fsync budget starts after the pause, so it only has to cover
            // one real fsync; keep it as wide as the response budget allows.
            .storage_io_timeout(Duration::from_millis(900))
            .runtime_response_timeout(Duration::from_millis(1000))
            .build()
            .unwrap()
    };
    let mut engine = Engine::open(options()).unwrap();
    let cf = engine.create_column_family("commit-timeout").unwrap();

    // Act
    let mut tx = engine
        .begin_tx(cf.id(), TransactionMode::ReadWrite)
        .unwrap();
    tx.put(b"key".to_vec(), b"value".to_vec(), None).unwrap();
    let error = tx
        .commit(WriteOptions::sync())
        .expect_err("the response must arrive after the caller's budget");

    // Assert: the caller is told the outcome is unknown, never to retry, and
    // the write it must reconcile against is durable after reopen.
    assert_eq!(error.severity(), Severity::OutcomeUnknown, "{error}");
    assert!(matches!(error, MidgeError::OutcomeUnknown(_)), "{error}");
    fail::remove(paused);
    // Shutdown waits for the paused event loop to finish the sync and release
    // the lease; a plain drop would hand that wait to a detached reaper.
    engine.shutdown(Duration::from_secs(10)).unwrap();
    drop(engine);
    let reopened = Engine::open(options()).unwrap();
    let cf = reopened.get_column_family("commit-timeout").unwrap();
    let read = reopened
        .begin_tx(cf.id(), TransactionMode::ReadOnly)
        .unwrap();
    assert_eq!(read.get(b"key").unwrap().as_deref(), Some(&b"value"[..]));
    drop(read);
    drop(reopened);
    drop(scenario);
}

#[test]
fn should_keep_backpressure_given_runtime_timeout_when_no_write_was_admitted() {
    // Arrange: the same variant outside an admitted commit still means
    // "retry later", so the mapping is specific to writes that may have landed.
    let error = MidgeError::Timeout("runtime request".into());

    // Act
    let severity = error.severity();

    // Assert
    assert_eq!(severity, Severity::Backpressure);
}
