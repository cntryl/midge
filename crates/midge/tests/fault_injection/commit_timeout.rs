//! A commit that the runtime admitted and then answered too late may still be
//! applied and made durable. Its error must say the outcome is unknown rather
//! than invite a retry (#901).
use cntryl_midge::{Engine, MidgeError, OpenOptions, Severity, TransactionMode, WriteOptions};
use std::time::Duration;

#[test]
fn should_keep_timeout_given_provably_unchanged_failure_when_commit_is_submitted() {
    for pool_size in [1024, 16 * 1024] {
        // Arrange: the small pool exercises the spilled route; both requests
        // traverse Transaction, RuntimeHandle, the actor and the real writer.
        let scenario = fail::FailScenario::setup();
        let directory = tempfile::tempdir().unwrap();
        let options = || {
            OpenOptions::local(directory.path())
                .transaction_memory_pool_size(pool_size)
                .background_compaction(false)
                .build()
                .unwrap()
        };
        let mut engine = Engine::open(options()).unwrap();
        let cf = engine.get_column_family("default").unwrap();
        let point = "midge::wal::timeout_before_enqueue";
        fail::cfg(point, "1*return->off").unwrap();
        let mut transaction = engine
            .begin_tx(cf.id(), TransactionMode::ReadWrite)
            .unwrap();
        transaction
            .put(b"unchanged-timeout".to_vec(), vec![7; 4096], None)
            .unwrap();

        // Act
        let result = transaction.commit(WriteOptions::sync());
        fail::remove(point);

        // Assert: this is a loop-reported timeout with proof, not an expired
        // caller wait. Restart must find no record to replay.
        assert!(matches!(result, Err(MidgeError::Timeout(_))), "{result:?}");
        if let Err(error) = engine.shutdown(Duration::from_secs(5)) {
            assert!(matches!(error, MidgeError::RecoveryFailed(_)), "{error}");
        }
        drop(engine);
        let mut reopened = Engine::open(options()).unwrap();
        let read = reopened
            .begin_tx(cf.id(), TransactionMode::ReadOnly)
            .unwrap();
        assert_eq!(read.get(b"unchanged-timeout").unwrap(), None);
        drop(read);
        reopened.shutdown(Duration::from_secs(5)).unwrap();
        drop(scenario);
    }
}

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

#[test]
fn should_report_outcome_unknown_when_event_loop_panics_after_transaction_wal_sync() {
    // Arrange: the event loop panics after the strict WAL sync and before it
    // answers, so the record is durable while the caller is still waiting.
    let scenario = fail::FailScenario::setup();
    let point = "midge::wal::txn_after_sync_before_ack";
    fail::cfg(point, "1*panic->off").unwrap();
    let dir = tempfile::tempdir().unwrap();
    let options = || {
        OpenOptions::local(dir.path())
            .background_compaction(false)
            .build()
            .unwrap()
    };
    let mut engine = Engine::open(options()).unwrap();
    let cf = engine.get_column_family("default").unwrap();
    let mut tx = engine
        .begin_tx(cf.id(), TransactionMode::ReadWrite)
        .unwrap();
    tx.put(b"panic-after-sync".to_vec(), b"value".to_vec(), None)
        .unwrap();

    // Act
    let result = tx.commit(WriteOptions::sync());
    fail::remove(point);
    let _ = engine.shutdown(Duration::from_secs(5));
    drop(engine);
    drop(scenario);
    let deadline = std::time::Instant::now() + Duration::from_secs(30);
    let mut reopened = loop {
        match Engine::open(options()) {
            Ok(reopened) => break reopened,
            Err(MidgeError::LeaseHeld(_)) if std::time::Instant::now() < deadline => {
                std::thread::sleep(Duration::from_millis(10));
            }
            Err(error) => panic!("reopen after runtime exit: {error:?}"),
        }
    };

    // Assert: the synced record replays, so the caller must not have been told
    // the commit definitely failed.
    let read = reopened
        .begin_tx(cf.id(), TransactionMode::ReadOnly)
        .unwrap();
    assert_eq!(
        read.get(b"panic-after-sync").unwrap().as_deref(),
        Some(&b"value"[..]),
        "the synced transaction must replay on reopen"
    );
    drop(read);
    reopened.shutdown(Duration::from_secs(5)).unwrap();
    assert!(
        matches!(&result, Err(MidgeError::OutcomeUnknown(message))
            if message.contains("runtime event loop panicked before responding")),
        "a commit lost to an event-loop panic after the WAL sync must be uncertain: {result:?}"
    );
}
