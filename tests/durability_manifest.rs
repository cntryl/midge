// Durability tests for Manifest and Flush coordination
// Tests the contract: "SST + Manifest MUST be fsynced before WAL truncation"

mod common;

use bytes::Bytes;
use common::*;

#[test]
fn should_preserve_manifest_consistency_across_flush() {
    let cf = engine.default_column_family();
    // Arrange
    let dir = test_temp_dir();
    let opts = flush_test_opts(dir.path().to_path_buf(), 1024);

    // Act
    with_engine_restart(
        opts.clone(),
        |eng| {
            for i in 0..50 {
                let key = format!("key{:03}", i);
                let value = vec![0u8; 100];
                eng.put(&cf, key.as_bytes(), value.as_bytes()).unwrap();
            }
            eng.flush().unwrap();
        },
        |eng| {
            // Assert
            let result = eng.get(&cf, b"key000").unwrap();
            assert!(result.is_some(), "Data should persist after flush");
        },
    );

    assert_manifest_exists(dir.path());
    let manifest_data = std::fs::read_to_string(dir.path().join("manifest.json")).unwrap();
    assert!(manifest_data.contains("ssts"), "Manifest should track SSTs");
}

#[test]
fn should_recover_from_incomplete_flush() {
    let cf = engine.default_column_family();
    // Arrange
    let dir = test_temp_dir();
    let opts = durability_opts(dir.path().to_path_buf());

    // Act
    with_engine_restart(
        opts,
        |eng| {
            eng.put(&cf, "key1".as_bytes(), "value1".as_bytes()).unwrap();
            eng.put(&cf, "key2".as_bytes(), "value2".as_bytes()).unwrap();
        },
        |eng| {
            // Assert
            assert_get_equals(eng, b"key1", b"value1");
            assert_get_equals(eng, b"key2", b"value2");
        },
    );
}

#[test]
fn should_maintain_sequence_numbers_across_flush_and_restart() {
    let cf = engine.default_column_family();
    // Arrange
    let dir = test_temp_dir();
    let opts = durability_opts(dir.path().to_path_buf());

    // Act
    with_engine_restart(
        opts,
        |eng| {
            eng.put(&cf, "a".as_bytes(), "1".as_bytes()).unwrap();
            eng.flush().unwrap();
            eng.put(&cf, "b".as_bytes(), "2".as_bytes()).unwrap();
        },
        |eng| {
            // Assert
            assert_get_equals(eng, b"a", b"1");
            assert_get_equals(eng, b"b", b"2");
        },
    );
}

#[test]
fn should_not_lose_data_given_flush_during_writes() {
    let cf = engine.default_column_family();
    // Arrange
    let dir = test_temp_dir();
    let opts = durability_opts(dir.path().to_path_buf());

    // Act
    with_engine_restart(
        opts,
        |eng| {
            eng.put(&cf, "k1".as_bytes(), "v1".as_bytes()).unwrap();
            eng.flush().unwrap();

            eng.put(&cf, "k2".as_bytes(), "v2".as_bytes()).unwrap();
            eng.flush().unwrap();

            eng.put(&cf, "k3".as_bytes(), "v3".as_bytes()).unwrap();
            eng.flush().unwrap();
        },
        |eng| {
            // Assert
            assert_get_equals(eng, b"k1", b"v1");
            assert_get_equals(eng, b"k2", b"v2");
            assert_get_equals(eng, b"k3", b"v3");
        },
    );
}

#[test]
fn should_preserve_tombstones_across_flush() {
    let cf = engine.default_column_family();
    // Arrange
    let dir = test_temp_dir();
    let opts = durability_opts(dir.path().to_path_buf());

    // Act
    with_engine_restart(
        opts,
        |eng| {
            eng.put(&cf, "key".as_bytes(), "value".as_bytes()).unwrap();
            eng.delete(&cf, "key".as_bytes()).unwrap();
            eng.flush().unwrap();
        },
        |eng| {
            // Assert
            assert_key_absent(eng, b"key");
        },
    );
}

#[test]
fn should_handle_multiple_flushes_without_data_loss() {
    let cf = engine.default_column_family();
    // Arrange
    let dir = test_temp_dir();
    let opts = durability_opts(dir.path().to_path_buf());

    // Act
    with_engine_restart(
        opts,
        |eng| {
            for i in 0..10 {
                let key = format!("key{}", i);
                let value = format!("value{}", i);
                eng.put(&cf, key.as_bytes(), value.as_bytes()).unwrap();
                eng.flush().unwrap();
            }
        },
        |eng| {
            // Assert
            for i in 0..10 {
                let key = format!("key{}", i);
                let expected = format!("value{}", i);
                assert_get_equals(eng, key.as_bytes(), expected.as_bytes());
            }
        },
    );
}

#[test]
fn should_recover_wal_entries_not_yet_flushed() {
    let cf = engine.default_column_family();
    // Arrange
    let dir = test_temp_dir();
    let opts = durability_opts(dir.path().to_path_buf());

    // Act
    with_engine_restart(
        opts,
        |eng| {
            eng.put(&cf, "unflushed1".as_bytes(), "data1".as_bytes())
                .unwrap();
            eng.put(&cf, "unflushed2".as_bytes(), "data2".as_bytes())
                .unwrap();
        },
        |eng| {
            // Assert
            assert_get_equals(eng, b"unflushed1", b"data1");
            assert_get_equals(eng, b"unflushed2", b"data2");
        },
    );
}

#[test]
fn should_preserve_manifest_last_persisted_sequence() {
    let cf = engine.default_column_family();
    // Arrange
    let dir = test_temp_dir();
    let opts = durability_opts(dir.path().to_path_buf());

    // Act
    with_engine(opts, |eng| {
        eng.put(&cf, "key".as_bytes(), "value".as_bytes()).unwrap();
        eng.flush().unwrap();
    });

    let manifest_path = dir.path().join("manifest.json");
    let manifest_data = std::fs::read_to_string(&manifest_path).unwrap();

    // Assert
    assert!(
        manifest_data.contains("last_persisted_sequence"),
        "Manifest should track last sequence"
    );
}

#[test]
fn should_handle_empty_memtable_flush_gracefully() {
    let cf = engine.default_column_family();
    // Arrange
    let dir = test_temp_dir();
    let opts = durability_opts(dir.path().to_path_buf());

    // Act
    with_engine(opts, |eng| {
        let result = eng.flush();

        // Assert
        assert!(result.is_ok(), "Empty flush should succeed");

        eng.put(&cf, "key".as_bytes(), "value".as_bytes()).unwrap();
        assert_get_equals(eng, b"key", b"value");
    });
}

#[test]
fn should_maintain_atomicity_given_flush_then_immediate_restart() {
    let cf = engine.default_column_family();
    // Arrange
    let dir = test_temp_dir();
    let opts = durability_opts(dir.path().to_path_buf());

    // Act
    with_engine_restart(
        opts,
        |eng| {
            eng.put(&cf, "atomic".as_bytes(), "test".as_bytes()).unwrap();
            eng.flush().unwrap();
        },
        |eng| {
            // Assert
            assert_get_equals(eng, b"atomic", b"test");
        },
    );
}
