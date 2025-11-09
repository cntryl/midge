// Durability tests for WAL and Transaction coordination
// Tests the durability contract: "Transaction commit MUST fsync WAL before returning success"

mod common;

use bytes::Bytes;
use common::*;

#[test]
fn should_persist_committed_transaction_across_restart() {
    // Arrange
    let dir = test_temp_dir();
    let opts = durability_opts(dir.path().to_path_buf());

    // Act
    with_engine_restart(
        opts,
        |eng| {
            let cf = eng.default_column_family();
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
fn should_recover_wal_entries_into_memtable_given_restart() {
    // Arrange
    let dir = test_temp_dir();
    let opts = durability_opts(dir.path().to_path_buf());

    // Act
    with_engine_restart(
        opts,
        |eng| {
            let cf = eng.default_column_family();
            eng.put(&cf, "a".as_bytes(), "1".as_bytes()).unwrap();
            eng.put(&cf, "b".as_bytes(), "2".as_bytes()).unwrap();
            eng.delete(&cf, "a".as_bytes()).unwrap();
            eng.put(&cf, "c".as_bytes(), "3".as_bytes()).unwrap();
        },
        |eng| {
            // Assert
            assert_key_absent(eng, b"a");
            assert_get_equals(eng, b"b", b"2");
            assert_get_equals(eng, b"c", b"3");
        },
    );
}

#[test]
fn should_preserve_write_order_across_restart() {
    // Arrange
    let dir = test_temp_dir();
    let opts = durability_opts(dir.path().to_path_buf());

    // Act
    with_engine_restart(
        opts,
        |eng| {
            let cf = eng.default_column_family();
            eng.put(&cf, "key".as_bytes(), "v1".as_bytes()).unwrap();
            eng.put(&cf, "key".as_bytes(), "v2".as_bytes()).unwrap();
            eng.put(&cf, "key".as_bytes(), "v3".as_bytes()).unwrap();
        },
        |eng| {
            // Assert
            assert_get_equals(eng, b"key", b"v3");
        },
    );
}

#[test]
fn should_maintain_durability_given_large_batch() {
    // Arrange
    let dir = test_temp_dir();
    let opts = durability_opts(dir.path().to_path_buf());

    // Act
    with_engine_restart(
        opts,
        |eng| {
            let cf = eng.default_column_family();
            for i in 0..100 {
                let key = format!("key{:03}", i);
                let value = format!("value{:03}", i);
                eng.put(&cf, key.as_bytes(), value.as_bytes()).unwrap();
            }
        },
        |eng| {
            // Assert
            for i in 0..100 {
                let key = format!("key{:03}", i);
                let expected = format!("value{:03}", i);
                assert_get_equals(eng, key.as_bytes(), expected.as_bytes());
            }
        },
    );
}

#[test]
fn should_preserve_deletes_across_restart() {
    // Arrange
    let dir = test_temp_dir();
    let opts = durability_opts(dir.path().to_path_buf());

    // Act
    with_engine_restart(
        opts,
        |eng| {
            let cf = eng.default_column_family();
            eng.put(&cf, "temp".as_bytes(), "value".as_bytes()).unwrap();
            eng.delete(&cf, "temp".as_bytes()).unwrap();
        },
        |eng| {
            // Assert
            assert_key_absent(eng, b"temp");
        },
    );
}

#[test]
fn should_replay_operations_in_correct_sequence() {
    // Arrange
    let dir = test_temp_dir();
    let opts = durability_opts(dir.path().to_path_buf());

    // Act
    with_engine_restart(
        opts,
        |eng| {
            let cf = eng.default_column_family();
            eng.put(&cf, "x".as_bytes(), "1".as_bytes()).unwrap();
            eng.put(&cf, "y".as_bytes(), "1".as_bytes()).unwrap();
            eng.put(&cf, "x".as_bytes(), "2".as_bytes()).unwrap();
            eng.delete(&cf, "y".as_bytes()).unwrap();
            eng.put(&cf, "z".as_bytes(), "1".as_bytes()).unwrap();
        },
        |eng| {
            // Assert
            assert_get_equals(eng, b"x", b"2");
            assert_key_absent(eng, b"y");
            assert_get_equals(eng, b"z", b"1");
        },
    );
}
