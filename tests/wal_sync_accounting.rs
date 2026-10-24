use cntryl_midge::{Engine, MidgeResult, OpenOptions, TransactionMode, WriteOptions};
use std::time::Duration;

#[test]
fn should_account_strict_sync_boundaries_through_restart() -> MidgeResult<()> {
    // Arrange: warm the sequence reservation before measuring one Strict group.
    let directory = tempfile::tempdir().unwrap();
    let options = OpenOptions::local(directory.path())
        .background_compaction(false)
        .build()?;
    let mut engine = Engine::open(options.clone())?;
    let family = engine.get_column_family("default").unwrap();
    let mut warmup = engine.begin_tx(family.id(), TransactionMode::ReadWrite)?;
    warmup.put(b"first".to_vec(), b"one".to_vec(), None)?;
    warmup.commit(WriteOptions::sync())?;
    let before = engine.metrics().get_runtime_metrics()?;

    // Act
    let mut measured = engine.begin_tx(family.id(), TransactionMode::ReadWrite)?;
    measured.put(b"second".to_vec(), b"two".to_vec(), None)?;
    measured.commit(WriteOptions::sync())?;
    let after = engine.metrics().get_runtime_metrics()?;

    // Assert: retaining exact ledger evidence still costs its independent sync.
    assert_eq!(after.wal_fsync_count - before.wal_fsync_count, 1);
    assert_eq!(
        after.wal_ledger_file_syncs - before.wal_ledger_file_syncs,
        1
    );
    assert_eq!(
        after.wal_ledger_file_sync_attempts - before.wal_ledger_file_sync_attempts,
        1
    );
    assert_eq!(
        after.wal_ledger_directory_syncs,
        before.wal_ledger_directory_syncs
    );
    assert_eq!(after.wal_accounted_syncs - before.wal_accounted_syncs, 2);
    engine.shutdown(Duration::from_secs(10))?;
    drop(engine);
    let mut reopened = Engine::open(options)?;
    let read = reopened.begin_tx(family.id(), TransactionMode::ReadOnly)?;
    assert_eq!(read.get(b"first")?.as_deref(), Some(b"one".as_slice()));
    assert_eq!(read.get(b"second")?.as_deref(), Some(b"two".as_slice()));
    drop(read);
    reopened.shutdown(Duration::from_secs(10))
}
