// Long-running stress tests for manual runs only.
// This file lives under `tests/stress/` so it won't be accidentally executed in CI.

#[test]
#[ignore = "Long-running stress test: creates many SSTs. Run manually when needed."]
fn should_scan_across_1000_ssts_given_large_database() {
    // Arrange
    let opts = midge::MidgeOptions {
        storage_mode: midge::StorageMode::Memory,
        memtable_size: 64, // tiny to force many flushes
        ..Default::default()
    };
    let engine = midge::MidgeEngine::open(opts).unwrap();

    // Act - produce ~1000 SSTs by flushing frequently with a single key each
    for i in 0..1000 {
        let key = format!("stress_k{:05}", i);
        engine.put(bytes::Bytes::from(key), bytes::Bytes::from("v")).unwrap();
        engine.flush().unwrap();
    }

    // Assert - scanning returns all keys
    let results = engine.scan(midge::Query::new()).unwrap();
    assert_eq!(results.len(), 1000);
}
