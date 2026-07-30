//! Regression coverage for transaction visibility and lifecycle boundaries.

use bytes::Bytes;
use cntryl_midge::{MidgeError, Query, TransactionMode, WriteOptions};

mod common;
use common::{open_with_mode, opts_for_mode};

#[test]
fn should_reject_insert_when_key_exists_only_in_sst() {
    // Arrange
    let engine = open_with_mode(&opts_for_mode("local"), "local");
    let cf = engine.create_column_family("sst").expect("create cf");
    let mut seed = engine
        .begin_tx(cf.id(), TransactionMode::ReadWrite)
        .expect("begin seed transaction");
    seed.put(b"sst-only".to_vec(), b"value".to_vec(), None)
        .expect("put seed value");
    seed.commit(WriteOptions::buffered()).expect("commit seed");
    engine.flush_cf(&cf).expect("flush seed");

    // Act
    let mut tx = engine
        .begin_tx(cf.id(), TransactionMode::ReadWrite)
        .expect("begin insert transaction");
    tx.insert(b"sst-only".to_vec(), b"replacement".to_vec(), None)
        .expect("queue insert");
    let result = tx.commit(WriteOptions::buffered());

    // Assert
    assert!(matches!(
        result,
        Err(MidgeError::InvalidArgument(message)) if message.contains("already exists")
    ));
}

#[test]
fn should_apply_last_intent_given_duplicate_operations_on_same_key_when_committing() {
    // Arrange
    let engine = open_with_mode(&opts_for_mode("memory"), "memory");
    let cf = engine.create_column_family("intents").expect("create cf");
    let mut seed = engine
        .begin_tx(cf.id(), TransactionMode::ReadWrite)
        .expect("begin seed transaction");
    seed.put(b"existing".to_vec(), b"old".to_vec(), None)
        .expect("put seed value");
    seed.commit(WriteOptions::best_effort())
        .expect("commit seed");

    // Act: the second insert must see the first insert; delete then insert is
    // valid because the delete changes the transaction-local existence state.
    let mut duplicate = engine
        .begin_tx(cf.id(), TransactionMode::ReadWrite)
        .expect("begin duplicate transaction");
    duplicate
        .insert(b"new".to_vec(), b"first".to_vec(), None)
        .expect("queue first insert");
    duplicate
        .insert(b"new".to_vec(), b"second".to_vec(), None)
        .expect("queue second insert");
    let duplicate_result = duplicate.commit(WriteOptions::best_effort());

    let mut replace = engine
        .begin_tx(cf.id(), TransactionMode::ReadWrite)
        .expect("begin replace transaction");
    replace.delete(b"existing".to_vec()).expect("queue delete");
    replace
        .insert(b"existing".to_vec(), b"new".to_vec(), None)
        .expect("queue replacement insert");
    replace
        .commit(WriteOptions::best_effort())
        .expect("delete then insert should commit");

    // Assert
    assert!(matches!(
        duplicate_result,
        Err(MidgeError::InvalidArgument(message)) if message.contains("already exists")
    ));
    let read = engine
        .begin_tx(cf.id(), TransactionMode::ReadOnly)
        .expect("begin read transaction");
    assert_eq!(
        read.get(b"existing").expect("read replacement").as_deref(),
        Some(&b"new"[..])
    );
}

#[test]
fn should_honor_prefix_upper_bound_given_prefix_ending_in_ff_when_scanning() {
    // Arrange
    let engine = open_with_mode(&opts_for_mode("memory"), "memory");
    let cf = engine.create_column_family("binary").expect("create cf");
    let mut tx = engine
        .begin_tx(cf.id(), TransactionMode::ReadWrite)
        .expect("begin transaction");
    tx.put(vec![0x10, 0xff], b"prefix".to_vec(), None)
        .expect("put prefix");
    tx.put(vec![0x10, 0xff, 0x00], b"child".to_vec(), None)
        .expect("put child");
    tx.put(vec![0x10, 0xfe], b"sibling".to_vec(), None)
        .expect("put sibling");
    tx.commit(WriteOptions::best_effort())
        .expect("commit values");

    // Act
    let read = engine
        .begin_tx(cf.id(), TransactionMode::ReadOnly)
        .expect("begin read transaction");
    let query = Query::new().prefix(Bytes::from(vec![0x10, 0xff]));
    let results = read
        .scan(&query)
        .expect("scan prefix")
        .try_collect()
        .expect("collect prefix scan");

    // Assert
    assert_eq!(results.len(), 2);
    assert!(results
        .iter()
        .all(|(key, _)| key.starts_with(&[0x10, 0xff])));
}

#[test]
fn should_reject_commit_after_column_family_drop_without_acknowledging_write() {
    // Arrange
    let engine = open_with_mode(&opts_for_mode("local"), "local");
    let cf = engine.create_column_family("stale").expect("create cf");
    let cf_id = cf.id();
    let mut tx = engine
        .begin_tx(cf_id, TransactionMode::ReadWrite)
        .expect("begin transaction");
    tx.put(b"must-not-commit".to_vec(), b"value".to_vec(), None)
        .expect("queue write");
    engine.drop_column_family(cf_id).expect("drop cf");

    // Act
    let result = tx.commit(WriteOptions::buffered());

    // Assert
    assert!(matches!(
        result,
        Err(MidgeError::InvalidArgument(message))
            if message.contains("column family") && message.contains("does not exist")
    ));
    assert!(matches!(
        engine.begin_tx(cf_id, TransactionMode::ReadOnly),
        Err(MidgeError::InvalidArgument(_))
    ));
}

#[test]
fn should_use_transaction_snapshot_time_for_ttl_visibility() {
    // Arrange
    let engine = open_with_mode(&opts_for_mode("local"), "local");
    let cf = engine.create_column_family("ttl").expect("create cf");
    let mut writer = engine
        .begin_tx(cf.id(), TransactionMode::ReadWrite)
        .expect("begin writer");
    writer
        .put(b"ttl-key".to_vec(), b"value".to_vec(), Some(1))
        .expect("put ttl value");
    writer
        .commit(WriteOptions::buffered())
        .expect("commit ttl value");
    let snapshot = engine
        .begin_tx(cf.id(), TransactionMode::ReadOnly)
        .expect("begin snapshot");

    // Act
    std::thread::sleep(std::time::Duration::from_millis(1_100));

    // Assert: the old snapshot remains stable while a new snapshot observes expiry.
    assert_eq!(
        snapshot.get(b"ttl-key").expect("snapshot read").as_deref(),
        Some(&b"value"[..])
    );
    drop(snapshot);
    let current = engine
        .begin_tx(cf.id(), TransactionMode::ReadOnly)
        .expect("begin current read");
    assert_eq!(current.get(b"ttl-key").expect("current read"), None);
}

#[test]
fn should_persist_range_tombstone_when_flush_survives_restart() {
    // Arrange
    let opts = opts_for_mode("local");
    {
        let mut engine = open_with_mode(&opts, "local");
        let cf = engine.create_column_family("ranges").expect("create cf");
        let mut writer = engine
            .begin_tx(cf.id(), TransactionMode::ReadWrite)
            .expect("begin writer");
        for key in [b"a".as_slice(), b"b".as_slice(), b"c".as_slice()] {
            writer
                .put(key.to_vec(), key.to_vec(), None)
                .expect("put value");
        }
        writer
            .commit(WriteOptions::buffered())
            .expect("commit values");
        engine.flush_cf(&cf).expect("flush values");

        let mut deleter = engine
            .begin_tx(cf.id(), TransactionMode::ReadWrite)
            .expect("begin delete transaction");
        deleter
            .delete_range(b"b".to_vec(), b"d".to_vec())
            .expect("delete range");
        deleter
            .commit(WriteOptions::buffered())
            .expect("commit range tombstone");
        engine.flush_cf(&cf).expect("flush range tombstone");
        engine
            .shutdown(std::time::Duration::from_secs(2))
            .expect("shutdown before immediate reopen");
    }

    // Act
    let engine = open_with_mode(&opts, "local");
    let cf = engine.get_column_family("ranges").expect("reopen cf");
    let read = engine
        .begin_tx(cf.id(), TransactionMode::ReadOnly)
        .expect("begin read transaction");

    // Assert
    assert_eq!(read.get(b"a").expect("read a").as_deref(), Some(&b"a"[..]));
    assert_eq!(read.get(b"b").expect("read b"), None);
    assert_eq!(read.get(b"c").expect("read c"), None);
}

#[test]
fn should_apply_flushed_range_tombstone_outside_point_key_bounds() {
    // Arrange: the second flush has a point key above the range tombstone.
    // Its manifest bounds must still include the tombstone's [a, c) interval.
    let engine = open_with_mode(&opts_for_mode("local"), "local");
    let cf = engine
        .create_column_family("range-bounds")
        .expect("create cf");

    let mut seed = engine
        .begin_tx(cf.id(), TransactionMode::ReadWrite)
        .expect("begin seed transaction");
    seed.put(b"b".to_vec(), b"old".to_vec(), None)
        .expect("put seed value");
    seed.commit(WriteOptions::buffered()).expect("commit seed");
    engine.flush_cf(&cf).expect("flush seed");

    let mut delete = engine
        .begin_tx(cf.id(), TransactionMode::ReadWrite)
        .expect("begin range transaction");
    delete
        .put(b"m".to_vec(), b"unrelated".to_vec(), None)
        .expect("put unrelated value");
    delete
        .delete_range(b"a".to_vec(), b"c".to_vec())
        .expect("queue range tombstone");
    delete
        .commit(WriteOptions::buffered())
        .expect("commit range tombstone");
    engine.flush_cf(&cf).expect("flush mixed contents");

    // Act
    let read = engine
        .begin_tx(cf.id(), TransactionMode::ReadOnly)
        .expect("begin read transaction");

    // Assert: `b` is below the point key `m`, but still covered by [a, c).
    assert_eq!(read.get(b"b").expect("read covered key"), None);
}
