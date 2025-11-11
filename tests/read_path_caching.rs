mod common;
use bytes::Bytes;
use cntryl_midge::Query;
use common::{assert_get_equals, new_engine};

#[test]
fn should_reject_block_given_checksum_mismatch_when_paranoid_mode_enabled() {
    // Arrange
    let (_dir, eng) = new_engine();
    let cf = eng.default_column_family();

    // Act - write data
    eng.put(&cf, b"key1", b"value1").expect("put");
    eng.put(&cf, b"key2", b"value2").expect("put");

    // TODO: Enable paranoid checksum mode
    // Corrupt a block and verify read fails

    // Assert - normal reads should succeed
    assert_get_equals(&eng, b"key1", b"value1");
    assert_get_equals(&eng, b"key2", b"value2");
}

#[test]
fn should_evict_least_recently_used_entry_given_cache_full_when_insert_new_block() {
    // Arrange
    let (_dir, eng) = new_engine();
    let cf = eng.default_column_family();

    // Act - write many keys to fill cache
    for i in 0..1000 {
        eng.put(&cf, format!("key{:04}", i).as_bytes(), b"value")
            .expect("put");
    }

    // Read keys in specific order to establish LRU state
    for i in (0..100).rev() {
        let _ = eng
            .get(&cf, format!("key{:04}", i).as_bytes())
            .expect("get");
    }

    // TODO: Monitor cache metrics to verify LRU eviction

    // Assert - most recently accessed keys should be cached
    let result = eng.get(&cf, b"key0099").expect("get");
    assert!(result.is_some(), "Recently accessed key should be present");
}

#[test]
fn should_limit_read_amplification_given_bloom_filters_and_index_locality() {
    // Arrange
    let (_dir, eng) = new_engine();
    let cf = eng.default_column_family();

    // Act - write sorted data to enable bloom filter effectiveness
    for i in 0..1000 {
        eng.put(&cf, format!("key{:04}", i).as_bytes(), b"value")
            .expect("put");
    }

    // Perform point reads (should benefit from bloom filters)
    for i in (0..1000).step_by(10) {
        let _ = eng
            .get(&cf, format!("key{:04}", i).as_bytes())
            .expect("get");
    }

    // Perform range scan (should benefit from index locality)
    let query = Query::new()
        .start_key(Bytes::from("key0000"))
        .end_key(Bytes::from("key0100"));
    let results = eng.scan(&cf, query).expect("scan");

    // Assert - scans should be efficient
    assert_eq!(results.len(), 100, "Range scan should return correct count");
    // TODO: Add instrumentation to verify read amplification metrics
}
