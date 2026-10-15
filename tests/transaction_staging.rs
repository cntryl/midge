use cntryl_midge::{
    ColumnFamilyHandle, Engine, MidgeError, MidgeResult, OpenOptions, Query, TransactionMode,
    WriteOptions,
};
use std::path::Path;
use std::sync::Arc;
use std::time::Duration;

const VALUE_BYTES: usize = 128;

fn options(path: &Path) -> OpenOptions {
    OpenOptions::cloud_simulated(path, "staging-tests", "transactions")
        .local_storage_budget(16 * 1024 * 1024)
        .with_memtable_size_limit(4 * 1024 * 1024)
        .with_memtable_flush_threshold(4 * 1024 * 1024)
        .background_compaction(false)
        .build()
        .unwrap()
}

fn key(index: usize) -> Vec<u8> {
    let mut key = vec![0; 16];
    key[8..].copy_from_slice(&u64::try_from(index).unwrap().to_be_bytes());
    key
}

fn value(index: usize) -> Vec<u8> {
    vec![u8::try_from(index % 251).unwrap(); VALUE_BYTES]
}

fn write_rows(
    engine: &Engine,
    family: &ColumnFamilyHandle,
    range: std::ops::Range<usize>,
    write: WriteOptions,
) -> MidgeResult<()> {
    let mut transaction = engine.begin_tx(family.id(), TransactionMode::ReadWrite)?;
    for index in range {
        transaction.put(key(index), value(index), None)?;
    }
    transaction.commit(write)
}

fn assert_rows(engine: &Engine, family: &ColumnFamilyHandle, count: usize) {
    let read = engine
        .begin_tx(family.id(), TransactionMode::ReadOnly)
        .unwrap();
    let rows = read
        .scan(&Query::new())
        .unwrap()
        .collect::<MidgeResult<Vec<_>>>()
        .unwrap();
    assert_eq!(rows.len(), count);
    for (index, (actual_key, actual_value)) in rows.iter().enumerate() {
        assert_eq!(actual_key.as_ref(), key(index));
        assert_eq!(actual_value.as_ref(), value(index));
    }
}

#[test]
fn should_preserve_durable_rows_when_oversized_batch_is_rejected() {
    // Arrange: persist a small batch before testing rejection of a larger one.
    let directory = tempfile::tempdir().unwrap();
    let options = options(directory.path());
    let mut engine = Engine::open(options.clone()).unwrap();
    let family = engine.get_column_family("default").unwrap();
    let accepted_count = 64;
    write_rows(
        &engine,
        &family,
        0..accepted_count,
        WriteOptions::best_effort(),
    )
    .unwrap();
    engine.flush_cf(&family).unwrap();
    let before = engine.metrics().get_runtime_metrics().unwrap();

    // Act: the oversized transaction includes overwrites and new keys.
    let mut transaction = engine
        .begin_tx(family.id(), TransactionMode::ReadWrite)
        .unwrap();
    for index in 0..12_500 {
        transaction
            .put(key(index), vec![b'r'; VALUE_BYTES], None)
            .unwrap();
    }
    let rejected = transaction.commit(WriteOptions::best_effort());
    let after = engine.metrics().get_runtime_metrics().unwrap();

    // Assert: rejection preserves sequence, WAL, existing values, and durable state.
    assert!(
        matches!(rejected, Err(MidgeError::NoSpace(_))),
        "{rejected:?}"
    );
    assert_eq!(after.current_sequence, before.current_sequence);
    assert_eq!(after.wal_append_count, before.wal_append_count);
    assert_rows(&engine, &family, accepted_count);
    engine.shutdown(Duration::from_secs(30)).unwrap();
    drop(engine);
    let mut reopened = Engine::open(options).unwrap();
    let recovered_family = reopened.get_column_family("default").unwrap();
    assert_rows(&reopened, &recovered_family, accepted_count);
    reopened.shutdown(Duration::from_secs(30)).unwrap();
}

#[test]
fn should_reject_oversized_value_without_publishing_any_part_of_transaction() {
    // Arrange
    let directory = tempfile::tempdir().unwrap();
    let mut engine = Engine::open(options(directory.path())).unwrap();
    let family = engine.get_column_family("default").unwrap();
    let before = engine.metrics().get_runtime_metrics().unwrap();
    let mut transaction = engine
        .begin_tx(family.id(), TransactionMode::ReadWrite)
        .unwrap();
    transaction.put(key(0), value(0), None).unwrap();
    transaction
        .put(key(1), vec![1; 2 * 1024 * 1024], None)
        .unwrap();

    // Act
    let result = transaction.commit(WriteOptions::cloud_strict());
    let after = engine.metrics().get_runtime_metrics().unwrap();

    // Assert
    assert!(matches!(result, Err(MidgeError::NoSpace(_))), "{result:?}");
    assert_eq!(after.current_sequence, before.current_sequence);
    assert_eq!(after.wal_append_count, before.wal_append_count);
    assert_rows(&engine, &family, 0);
    engine.shutdown(Duration::from_secs(30)).unwrap();
}

#[test]
fn should_recover_every_concurrent_best_effort_write_after_completed_family_flush() {
    // Arrange
    const WRITERS: usize = 4;
    const ROWS_PER_WRITER: usize = 64;
    let directory = tempfile::tempdir().unwrap();
    let options = options(directory.path());
    let engine = Arc::new(Engine::open(options.clone()).unwrap());
    let family = engine.get_column_family("default").unwrap();
    let mut workers = Vec::new();

    // Act: each worker must acknowledge its entire disjoint batch.
    for writer in 0..WRITERS {
        let engine = Arc::clone(&engine);
        let family = family.clone();
        workers.push(std::thread::spawn(move || {
            let start = writer * ROWS_PER_WRITER;
            write_rows(
                &engine,
                &family,
                start..start + ROWS_PER_WRITER,
                WriteOptions::best_effort(),
            )
        }));
    }
    for worker in workers {
        worker
            .join()
            .expect("writer panicked")
            .expect("commit failed");
    }
    engine.flush_cf(&family).unwrap();

    // Assert: one completed flush promises every acknowledged row on restart.
    assert_rows(&engine, &family, WRITERS * ROWS_PER_WRITER);
    let mut engine = Arc::try_unwrap(engine).unwrap_or_else(|_| panic!("writer retained engine"));
    engine.shutdown(Duration::from_secs(30)).unwrap();
    drop(engine);
    let mut reopened = Engine::open(options).unwrap();
    let recovered_family = reopened.get_column_family("default").unwrap();
    assert_rows(&reopened, &recovered_family, WRITERS * ROWS_PER_WRITER);
    reopened.shutdown(Duration::from_secs(30)).unwrap();
}
