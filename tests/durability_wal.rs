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
            eng.put(Bytes::from("key1"), Bytes::from("value1")).unwrap();
            eng.put(Bytes::from("key2"), Bytes::from("value2")).unwrap();
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
            eng.put(Bytes::from("a"), Bytes::from("1")).unwrap();
            eng.put(Bytes::from("b"), Bytes::from("2")).unwrap();
            eng.delete(Bytes::from("a")).unwrap();
            eng.put(Bytes::from("c"), Bytes::from("3")).unwrap();
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
            eng.put(Bytes::from("key"), Bytes::from("v1")).unwrap();
            eng.put(Bytes::from("key"), Bytes::from("v2")).unwrap();
            eng.put(Bytes::from("key"), Bytes::from("v3")).unwrap();
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
            for i in 0..100 {
                let key = format!("key{:03}", i);
                let value = format!("value{:03}", i);
                eng.put(Bytes::from(key), Bytes::from(value)).unwrap();
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
            eng.put(Bytes::from("temp"), Bytes::from("value")).unwrap();
            eng.delete(Bytes::from("temp")).unwrap();
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
            eng.put(Bytes::from("x"), Bytes::from("1")).unwrap();
            eng.put(Bytes::from("y"), Bytes::from("1")).unwrap();
            eng.put(Bytes::from("x"), Bytes::from("2")).unwrap();
            eng.delete(Bytes::from("y")).unwrap();
            eng.put(Bytes::from("z"), Bytes::from("1")).unwrap();
        },
        |eng| {
            // Assert
            assert_get_equals(eng, b"x", b"2");
            assert_key_absent(eng, b"y");
            assert_get_equals(eng, b"z", b"1");
        },
    );
}
