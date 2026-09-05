//! Small public memory configurations must still provide bounded cold reads.

use cntryl_midge::{Engine, MemoryBudget, OpenOptions, TransactionMode, WriteOptions};
use std::time::Duration;

#[test]
fn should_read_cold_cloud_sst_when_default_memory_budget_is_eight_mebibytes() {
    // Arrange
    let directory = tempfile::tempdir().expect("database directory");
    let database = directory.path().join("nested-cache-".repeat(12));
    let budget = 8 * 1024 * 1024;
    let options = OpenOptions::cloud_simulated(database, "bucket", "small-memory")
        .memory_budget(MemoryBudget::Bytes(budget))
        .background_compaction(false)
        .build()
        .expect("small cloud options");
    let mut engine = Engine::open(options.clone()).expect("seed engine");
    let cf = engine.create_column_family("data").expect("create CF");
    let mut transaction = engine
        .begin_tx(cf.id(), TransactionMode::ReadWrite)
        .expect("write transaction");
    transaction
        .put(b"key".to_vec(), b"value".to_vec(), None)
        .expect("put");
    transaction
        .commit(WriteOptions::cloud_strict())
        .expect("cloud acknowledgement");
    engine.flush_cf(&cf).expect("publish SST");
    engine.shutdown(Duration::from_secs(30)).expect("shutdown");
    drop(engine);

    // Act
    let mut reopened = Engine::open(options.clone()).expect("cold engine");
    let cf = reopened.get_column_family("data").expect("recovered CF");
    let transaction = reopened
        .begin_tx(cf.id(), TransactionMode::ReadOnly)
        .expect("read transaction");
    let value = transaction.get(b"key").expect("bounded cold SST read");
    let metrics = reopened.get_runtime_metrics().expect("read metrics");

    // Assert
    assert_eq!(value.as_deref(), Some(b"value".as_slice()));
    assert!(metrics.remote_range_requests_total > 0);
    assert!(options.block_cache_size() > 0);
    let public_pools = options
        .memtable_size_limit()
        .saturating_mul(2)
        .saturating_add(options.transaction_memory_pool_size())
        .saturating_add(options.block_cache_size());
    assert!(public_pools <= budget);
    drop(transaction);
    reopened
        .shutdown(Duration::from_secs(30))
        .expect("shutdown reader");
}
