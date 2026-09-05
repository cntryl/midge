//! Cloud SST reads must not turn the ephemeral disk into a database replica.

use cntryl_midge::{Engine, OpenOptions, TransactionMode, WriteOptions};
use std::path::Path;
use std::time::Duration;

fn sst_bytes(path: &Path) -> u64 {
    std::fs::read_dir(path)
        .into_iter()
        .flatten()
        .flatten()
        .filter(|entry| entry.path().extension().is_some_and(|ext| ext == "sst"))
        .map(|entry| entry.metadata().expect("SST metadata").len())
        .sum()
}

#[test]
fn should_operate_cloud_database_when_inventory_exceeds_local_cache() {
    // Arrange
    let dir = tempfile::tempdir().expect("database directory");
    let local_budget = 1024 * 1024;
    let options = OpenOptions::cloud_simulated(dir.path(), "bucket", "remote-sst")
        .local_storage_budget(local_budget)
        .background_compaction(false)
        .build()
        .expect("options");
    let mut engine = Engine::open(options.clone()).expect("open");
    let cf = engine.create_column_family("data").expect("create CF");
    // Distinct values prevent repeated-value compression from hiding the
    // relationship between cloud inventory and the ephemeral disk budget.
    let mut random = 0x9e37_79b9_u32;
    let values: Vec<Vec<u8>> = (0..12)
        .map(|_| {
            (0..96 * 1024)
                .map(|_| {
                    random ^= random << 13;
                    random ^= random >> 17;
                    random ^= random << 5;
                    random.to_le_bytes()[0]
                })
                .collect()
        })
        .collect();

    // Act
    for (index, value) in values.iter().enumerate() {
        let mut tx = engine
            .begin_tx(cf.id(), TransactionMode::ReadWrite)
            .expect("transaction");
        tx.put(format!("key-{index}").into_bytes(), value.clone(), None)
            .expect("put");
        tx.commit(WriteOptions::cloud_strict())
            .expect("cloud commit");
        engine.flush_cf(&cf).expect("flush");
        assert_eq!(
            sst_bytes(&dir.path().join("sst")),
            0,
            "published SSTs must be evicted"
        );
    }
    engine.shutdown(Duration::from_secs(30)).expect("shutdown");
    drop(engine);
    assert!(sst_bytes(&dir.path().join("cloud_store/sst")) > local_budget);
    let mut reopened = Engine::open(options).expect("cold reopen");
    assert_eq!(
        sst_bytes(&dir.path().join("sst")),
        0,
        "startup must not hydrate SSTs"
    );
    let cf = reopened.get_column_family("data").expect("recovered CF");
    {
        let tx = reopened
            .begin_tx(cf.id(), TransactionMode::ReadOnly)
            .expect("read transaction");
        for (index, value) in values.iter().enumerate() {
            assert_eq!(
                tx.get(format!("key-{index}").as_bytes())
                    .expect("remote read")
                    .as_deref(),
                Some(value.as_slice())
            );
        }
    }
    reopened.compact_all().expect("remote input compaction");

    // Assert
    assert_eq!(
        sst_bytes(&dir.path().join("sst")),
        0,
        "reads and compaction must leave published SSTs remote"
    );
    assert_eq!(
        sst_bytes(&dir.path().join("hybrid_local/sst")),
        0,
        "no duplicate full-file cache"
    );
    let tx = reopened
        .begin_tx(cf.id(), TransactionMode::ReadOnly)
        .expect("read transaction");
    for (index, value) in values.iter().enumerate() {
        assert_eq!(
            tx.get(format!("key-{index}").as_bytes())
                .expect("compacted read")
                .as_deref(),
            Some(value.as_slice())
        );
    }
    drop(tx);
    reopened
        .verify_storage(Duration::from_secs(30))
        .expect("verify remote-only SSTs");
    reopened
        .shutdown(Duration::from_secs(30))
        .expect("shutdown reopened engine");
}

#[test]
fn should_report_corrupt_remote_data_block_when_read_after_metadata_only_startup() {
    // Arrange
    let dir = tempfile::tempdir().expect("database directory");
    let options = OpenOptions::cloud_simulated(dir.path(), "bucket", "corrupt-block")
        .background_compaction(false)
        .build()
        .expect("options");
    let mut engine = Engine::open(options.clone()).expect("open");
    let cf = engine.create_column_family("data").expect("CF");
    let mut tx = engine
        .begin_tx(cf.id(), TransactionMode::ReadWrite)
        .expect("transaction");
    tx.put(b"key".to_vec(), b"value".to_vec(), None)
        .expect("put");
    tx.commit(WriteOptions::cloud_strict()).expect("commit");
    engine.flush_cf(&cf).expect("flush");
    engine.shutdown(Duration::from_secs(30)).expect("shutdown");
    drop(engine);
    let path = std::fs::read_dir(dir.path().join("cloud_store/sst"))
        .expect("cloud inventory")
        .flatten()
        .find(|entry| entry.path().extension().is_some_and(|ext| ext == "sst"))
        .expect("published SST")
        .path();
    let mut bytes = std::fs::read(&path).expect("fixture SST");
    bytes[4] ^= 0x80;
    std::fs::write(path, bytes).expect("inject data-block corruption");

    // Act
    let mut reopened = Engine::open(options).expect("startup only validates metadata");
    let cf = reopened.get_column_family("data").expect("CF");
    let tx = reopened
        .begin_tx(cf.id(), TransactionMode::ReadOnly)
        .expect("read transaction");
    let result = tx.get(b"key");

    // Assert
    assert!(
        result.is_err(),
        "corruption must not be returned as a missing key"
    );
    assert_eq!(sst_bytes(&dir.path().join("sst")), 0);
    drop(tx);
    reopened
        .shutdown(Duration::from_secs(30))
        .expect("shutdown");
}
