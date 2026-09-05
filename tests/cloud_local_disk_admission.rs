use cntryl_midge::{Engine, MidgeError, OpenOptions, TransactionMode, WriteOptions};
use std::path::Path;
use std::time::Duration;

fn random_value(length: usize, random: &mut u32) -> Vec<u8> {
    (0..length)
        .map(|_| {
            *random ^= *random << 13;
            *random ^= *random >> 17;
            *random ^= *random << 5;
            random.to_le_bytes()[0]
        })
        .collect()
}

fn wal_bytes(directory: &Path) -> u64 {
    std::fs::read_dir(directory.join("wal"))
        .expect("WAL directory")
        .map(|entry| {
            entry
                .expect("WAL entry")
                .metadata()
                .expect("WAL metadata")
                .len()
        })
        .sum()
}

#[test]
fn should_keep_ordinary_cloud_commits_flushable_when_local_budget_is_smaller_than_memtable_limit() {
    for (lengths, spill_pool) in [
        ([96, 96, 96, 96], None),
        ([40, 96, 96, 96], None),
        ([40, 96, 96, 96], Some(1)),
    ] {
        // Arrange
        let directory = tempfile::tempdir().expect("database directory");
        let mut builder =
            OpenOptions::cloud_simulated(directory.path(), "bucket", "bounded-commits")
                .local_storage_budget(1024 * 1024)
                .background_compaction(false);
        if let Some(pool) = spill_pool {
            builder = builder.transaction_memory_pool_size(pool);
        }
        let options = builder.build().expect("options");
        let mut engine = Engine::open(options.clone()).expect("open");
        let cf = engine.create_column_family("data").expect("column family");
        let mut random = 0x9e37_79b9_u32;
        let values: Vec<_> = lengths
            .into_iter()
            .map(|length| random_value(length * 1024, &mut random))
            .collect();

        // Act
        for (index, value) in values.iter().enumerate() {
            let mut tx = engine
                .begin_tx(cf.id(), TransactionMode::ReadWrite)
                .expect("transaction");
            tx.put(index.to_be_bytes().to_vec(), value.clone(), None)
                .expect("put");
            tx.commit(WriteOptions::cloud_strict())
                .expect("cloud commit");
        }
        let flush = engine.flush_cf(&cf);
        let shutdown = engine.shutdown(Duration::from_secs(10));

        // Assert
        assert!(
            flush.is_ok(),
            "accepted commits must remain flushable: {flush:?}"
        );
        shutdown.expect("shutdown");
        drop(engine);
        let mut reopened = Engine::open(options).expect("reopen");
        let cf = reopened
            .get_column_family("data")
            .expect("recovered column family");
        let tx = reopened
            .begin_tx(cf.id(), TransactionMode::ReadOnly)
            .expect("reader");
        for (index, value) in values.iter().enumerate() {
            assert_eq!(
                tx.get(&index.to_be_bytes())
                    .expect("recovered value")
                    .as_deref(),
                Some(value.as_slice())
            );
        }
        drop(tx);
        reopened
            .shutdown(Duration::from_secs(10))
            .expect("recovered shutdown");
    }
}

#[test]
fn should_reject_atomic_cloud_work_before_wal_append_when_its_flush_cannot_fit() {
    for (value_lengths, spill_pool) in [
        (vec![160 * 1024], None),
        (vec![96 * 1024, 96 * 1024], None),
        (vec![96 * 1024, 96 * 1024], Some(1)),
    ] {
        // Arrange
        let directory = tempfile::tempdir().expect("database directory");
        let mut builder =
            OpenOptions::cloud_simulated(directory.path(), "bucket", "oversized-atomic")
                .local_storage_budget(1024 * 1024)
                .background_compaction(false);
        if let Some(pool) = spill_pool {
            builder = builder.transaction_memory_pool_size(pool);
        }
        let mut engine = Engine::open(builder.build().expect("options")).expect("open");
        let cf = engine.create_column_family("data").expect("column family");
        let mut tx = engine
            .begin_tx(cf.id(), TransactionMode::ReadWrite)
            .expect("transaction");
        let mut random = 0x9e37_79b9_u32;
        for (index, length) in value_lengths.iter().enumerate() {
            tx.put(
                index.to_be_bytes().to_vec(),
                random_value(*length, &mut random),
                None,
            )
            .expect("put");
        }
        let before_wal_bytes = wal_bytes(directory.path());

        // Act
        let result = tx.commit(WriteOptions::cloud_strict());
        let after_wal_bytes = wal_bytes(directory.path());
        let reader = engine
            .begin_tx(cf.id(), TransactionMode::ReadOnly)
            .expect("reader");
        let visible = reader
            .get(&0_usize.to_be_bytes())
            .expect("read after rejected commit");
        drop(reader);
        let shutdown = engine.shutdown(Duration::from_secs(10));

        // Assert
        assert!(
            matches!(result, Err(MidgeError::NoSpace(_))),
            "atomic work must be rejected before commit: {result:?}; spill={spill_pool:?}"
        );
        assert_eq!(after_wal_bytes, before_wal_bytes);
        assert!(visible.is_none());
        shutdown.expect("shutdown after rejected work");
    }
}

#[test]
fn should_reject_oversized_cloud_tombstones_before_wal_append() {
    for range in [false, true] {
        // Arrange
        let directory = tempfile::tempdir().expect("database directory");
        let options = OpenOptions::cloud_simulated(directory.path(), "bucket", "oversized-delete")
            .local_storage_budget(1024 * 1024)
            .build()
            .expect("options");
        let mut engine = Engine::open(options).expect("open");
        let cf = engine.create_column_family("data").expect("column family");
        let mut tx = engine
            .begin_tx(cf.id(), TransactionMode::ReadWrite)
            .expect("transaction");
        if range {
            tx.delete_range(vec![b'a'; 24 * 1024], vec![b'z'; 24 * 1024])
                .expect("range tombstone");
        } else {
            tx.delete(vec![b'a'; 32 * 1024]).expect("point tombstone");
        }
        let before = wal_bytes(directory.path());

        // Act
        let result = tx.commit(WriteOptions::cloud_strict());
        let after = wal_bytes(directory.path());
        engine.shutdown(Duration::from_secs(10)).expect("shutdown");

        // Assert
        assert!(
            matches!(result, Err(MidgeError::NoSpace(_))),
            "oversized tombstone must be rejected: {result:?}"
        );
        assert_eq!(before, after);
    }
}
