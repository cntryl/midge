//! Shared staging must keep accepted work flushable across small column families.

use cntryl_midge::{Engine, MidgeError, OpenOptions, TransactionMode, WriteOptions};
use std::path::Path;
use std::time::Duration;

fn file_bytes(path: &Path) -> u64 {
    std::fs::read_dir(path)
        .into_iter()
        .flatten()
        .flatten()
        .map(|entry| {
            let path = entry.path();
            if path.is_dir() {
                file_bytes(&path)
            } else {
                entry.metadata().map_or(0, |metadata| metadata.len())
            }
        })
        .sum()
}

fn working_bytes(path: &Path) -> u64 {
    ["wal", "sst", "staging", "hybrid_local"]
        .iter()
        .map(|name| file_bytes(&path.join(name)))
        .sum()
}

#[test]
fn should_flush_many_small_cloud_families_without_exhausting_shared_wal_staging() {
    // Arrange
    let directory = tempfile::tempdir().expect("database directory");
    let limit = 256 * 1024;
    let options = OpenOptions::cloud_simulated(directory.path(), "bucket", "many-small-families")
        .local_storage_budget(limit)
        .background_compaction(false)
        .build()
        .expect("options");
    let mut engine = Engine::open(options.clone()).expect("engine");
    let families: Vec<_> = (0..160)
        .map(|index| {
            engine
                .create_column_family(&format!("cf-{index}"))
                .expect("family")
        })
        .collect();
    let mut random = 0x1f37_9945_u32;
    let values: Vec<Vec<u8>> = families
        .iter()
        .map(|_| {
            (0..2048)
                .map(|_| {
                    random ^= random << 13;
                    random ^= random >> 17;
                    random ^= random << 5;
                    random.to_le_bytes()[0]
                })
                .collect()
        })
        .collect();

    let wal_before = file_bytes(&directory.path().join("wal"));
    let mut oversized = engine
        .begin_tx(families[0].id(), TransactionMode::ReadWrite)
        .expect("oversized transaction");
    oversized
        .put(b"too-large".to_vec(), vec![1; 64 * 1024], None)
        .expect("buffer put");
    assert!(matches!(
        oversized.commit(WriteOptions::cloud_strict()),
        Err(MidgeError::NoSpace(_))
    ));
    assert_eq!(
        file_bytes(&directory.path().join("wal")),
        wal_before,
        "unflushable indivisible transactions must fail before WAL growth"
    );

    // Act
    for (index, (family, value)) in families.iter().zip(&values).enumerate() {
        let mut committed = false;
        for _ in 0..3 {
            let mut tx = engine
                .begin_tx(family.id(), TransactionMode::ReadWrite)
                .expect("transaction");
            tx.put(b"key".to_vec(), value.clone(), None).expect("put");
            match tx.commit(WriteOptions::cloud_strict()) {
                Ok(()) => {
                    committed = true;
                    break;
                }
                Err(MidgeError::NoSpace(_) | MidgeError::WriteStall(_)) => {
                    for accepted in &families[..index] {
                        engine
                            .flush_cf(accepted)
                            .expect("accepted work must retain flush capacity");
                    }
                }
                Err(error) => panic!("cloud commit: {error}"),
            }
        }
        assert!(
            committed,
            "family {index} must make progress after flushing accepted work"
        );
        assert!(
            working_bytes(directory.path()) <= limit,
            "physical working files must stay inside shared budget"
        );
    }
    for family in &families {
        engine.flush_cf(family).expect("flush accepted family");
    }
    engine.shutdown(Duration::from_secs(30)).expect("shutdown");
    drop(engine);
    let mut reopened = Engine::open(options).expect("reopen");

    // Assert
    for (index, value) in values.iter().enumerate() {
        let family = reopened
            .get_column_family(&format!("cf-{index}"))
            .expect("recovered family");
        let tx = reopened
            .begin_tx(family.id(), TransactionMode::ReadOnly)
            .expect("read");
        assert_eq!(
            tx.get(b"key").expect("get").as_deref(),
            Some(value.as_slice())
        );
    }
    assert!(working_bytes(directory.path()) <= limit);
    assert!(file_bytes(&directory.path().join("cloud_store/sst")) > limit);
    reopened
        .shutdown(Duration::from_secs(30))
        .expect("reopened shutdown");
}
