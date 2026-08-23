// Copyright (c) 2025 Cntryl, Inc.
// SPDX-License-Identifier: Apache-2.0 WITH LLVM-exception

//! Transaction conflict tests - validates LWW semantics, write conflict handling, and concurrent transaction behavior.
//!
//! Tests ensure that concurrent transactions follow Last-Write-Wins semantics and handle conflicts appropriately.
//! These tests validate logical transaction behavior across all storage modes (Memory, FS, Cloud).

use bytes::Bytes;
mod common;
use common::*;
use std::sync::Arc;
use std::time::{Duration, Instant};

#[test]
fn should_assert_expected_value_without_creating_a_write_conflict() {
    // Arrange
    let engine = Arc::new(open_with_mode(&MidgeOptions::default(), "memory"));
    let cf = engine
        .create_column_family("assertions")
        .expect("create cf");
    let mut seed = engine
        .begin_tx(cf.id(), cntryl_midge::TransactionMode::ReadWrite)
        .expect("begin seed tx");
    seed.put(b"key".to_vec(), b"value".to_vec(), None)
        .expect("seed value");
    seed.commit(cntryl_midge::WriteOptions::buffered())
        .expect("seed commit");

    // Act
    let mut txn = engine
        .begin_tx(cf.id(), cntryl_midge::TransactionMode::ReadWrite)
        .expect("begin assertion tx");
    txn.assert_value(b"key".to_vec(), Some(b"value".to_vec()))
        .expect("register assertion");
    txn.commit(cntryl_midge::WriteOptions::buffered())
        .expect("assertion commit");

    // Assert
    let mut missing = engine
        .begin_tx(cf.id(), cntryl_midge::TransactionMode::ReadWrite)
        .expect("begin missing assertion tx");
    missing
        .assert_value(b"absent".to_vec(), None)
        .expect("register missing assertion");
    missing
        .commit(cntryl_midge::WriteOptions::buffered())
        .expect("missing assertion commit");
}

#[test]
fn should_reject_assertion_when_snapshot_value_differs() {
    // Arrange
    let engine = Arc::new(open_with_mode(&MidgeOptions::default(), "memory"));
    let cf = engine
        .create_column_family("assertions")
        .expect("create cf");
    let mut seed = engine
        .begin_tx(cf.id(), cntryl_midge::TransactionMode::ReadWrite)
        .expect("begin seed tx");
    seed.put(b"key".to_vec(), b"actual".to_vec(), None)
        .expect("seed value");
    seed.commit(cntryl_midge::WriteOptions::buffered())
        .expect("seed commit");

    // Act
    let mut txn = engine
        .begin_tx(cf.id(), cntryl_midge::TransactionMode::ReadWrite)
        .expect("begin assertion tx");
    txn.assert_value(b"key".to_vec(), Some(b"expected".to_vec()))
        .expect("register assertion");
    let result = txn.commit(cntryl_midge::WriteOptions::buffered());

    // Assert
    assert!(matches!(
        result,
        Err(cntryl_midge::MidgeError::WriteConflict(_))
    ));
}

#[test]
fn should_validate_assertion_against_snapshot_when_key_is_written() {
    // Arrange
    let engine = Arc::new(open_with_mode(&MidgeOptions::default(), "memory"));
    let cf = engine
        .create_column_family("assertion_snapshot")
        .expect("create cf");
    let mut seed = engine
        .begin_tx(cf.id(), cntryl_midge::TransactionMode::ReadWrite)
        .expect("begin seed tx");
    seed.put(b"key".to_vec(), b"old".to_vec(), None)
        .expect("seed value");
    seed.commit(cntryl_midge::WriteOptions::buffered())
        .expect("seed commit");

    let mut txn = engine
        .begin_tx(cf.id(), cntryl_midge::TransactionMode::ReadWrite)
        .expect("begin assertion tx");
    txn.put(b"key".to_vec(), b"new".to_vec(), None)
        .expect("pending write");
    txn.assert_value(b"key".to_vec(), Some(b"old".to_vec()))
        .expect("register assertion");

    // Act
    let result = txn.commit(cntryl_midge::WriteOptions::buffered());

    // Assert
    assert!(result.is_ok());

    let read_tx = engine
        .begin_tx(cf.id(), cntryl_midge::TransactionMode::ReadOnly)
        .expect("begin read tx");
    assert_eq!(
        read_tx.get(b"key").expect("read value"),
        Some(Bytes::from_static(b"new"))
    );
}

#[test]
fn should_bound_assertion_memory_by_transaction_pool() {
    // Arrange
    let opts = cntryl_midge::OpenOptions::in_memory()
        .transaction_memory_pool_size(512)
        .build()
        .expect("build options");
    let engine = cntryl_midge::Engine::open(opts).expect("open engine");
    let cf = engine
        .create_column_family("assertion_limit")
        .expect("create cf");
    let mut txn = engine
        .begin_tx(cf.id(), cntryl_midge::TransactionMode::ReadWrite)
        .expect("begin transaction");

    // Act
    let first = txn.assert_value(vec![b'a'; 128], None);
    let second = txn.assert_value(vec![b'b'; 128], None);

    // Assert
    assert!(first.is_ok());
    assert!(matches!(
        second,
        Err(cntryl_midge::MidgeError::ResourceLimit(_))
    ));
}

#[test]
fn should_reject_assertion_when_write_intent_consumes_shared_transaction_pool() {
    // Arrange
    let opts = cntryl_midge::OpenOptions::in_memory()
        .transaction_memory_pool_size(1_024)
        .build()
        .expect("build options");
    let engine = cntryl_midge::Engine::open(opts).expect("open engine");
    let cf = engine
        .create_column_family("write-pressure")
        .expect("create cf");
    let mut writer = engine
        .begin_tx(cf.id(), cntryl_midge::TransactionMode::ReadWrite)
        .expect("begin writer");
    writer
        .put(b"held".to_vec(), b"value".to_vec(), None)
        .expect("reserve write intent memory");
    let mut asserting = engine
        .begin_tx(cf.id(), cntryl_midge::TransactionMode::ReadWrite)
        .expect("begin asserting transaction");

    // Act
    let pressured = asserting.assert_value(vec![b'a'; 512], None);
    drop(writer);
    let admitted_after_release = asserting.assert_value(vec![b'b'; 512], None);

    // Assert
    assert!(matches!(
        pressured,
        Err(cntryl_midge::MidgeError::ResourceLimit(_))
    ));
    assert!(
        admitted_after_release.is_ok(),
        "dropping the writer must release its shared pool reservation"
    );
}

#[test]
fn should_reject_write_intent_when_assertion_consumes_shared_transaction_pool() {
    // Arrange
    let opts = cntryl_midge::OpenOptions::in_memory()
        .transaction_memory_pool_size(1_024)
        .build()
        .expect("build options");
    let engine = cntryl_midge::Engine::open(opts).expect("open engine");
    let cf = engine
        .create_column_family("assertion-pressure")
        .expect("create cf");
    let mut asserting = engine
        .begin_tx(cf.id(), cntryl_midge::TransactionMode::ReadWrite)
        .expect("begin asserting transaction");
    asserting
        .assert_value(vec![b'a'; 512], None)
        .expect("reserve assertion memory");
    let mut writer = engine
        .begin_tx(cf.id(), cntryl_midge::TransactionMode::ReadWrite)
        .expect("begin writer");

    // Act
    let pressured = writer.put(b"held".to_vec(), b"value".to_vec(), None);
    drop(asserting);
    let admitted_after_release = writer.put(b"released".to_vec(), b"value".to_vec(), None);

    // Assert
    assert!(matches!(
        pressured,
        Err(cntryl_midge::MidgeError::ResourceLimit(_))
    ));
    assert!(
        admitted_after_release.is_ok(),
        "dropping the assertion owner must release its shared pool reservation"
    );
}

#[test]
fn should_spill_write_intent_when_assertion_consumes_shared_pool_in_local_mode() {
    // Arrange
    let temp_dir = tempfile::tempdir().expect("create database directory");
    let opts = cntryl_midge::OpenOptions::local(temp_dir.path())
        .transaction_memory_pool_size(1_024)
        .build()
        .expect("build options");
    let engine = cntryl_midge::Engine::open(opts).expect("open engine");
    let cf = engine
        .create_column_family("assertion-spill-pressure")
        .expect("create cf");
    let mut asserting = engine
        .begin_tx(cf.id(), cntryl_midge::TransactionMode::ReadWrite)
        .expect("begin asserting transaction");
    asserting
        .assert_value(vec![b'a'; 512], None)
        .expect("reserve assertion memory");
    let mut writer = engine
        .begin_tx(cf.id(), cntryl_midge::TransactionMode::ReadWrite)
        .expect("begin writer");

    // Act
    let admitted = writer.put(b"spilled".to_vec(), b"value".to_vec(), None);
    let spill_runs = std::fs::read_dir(temp_dir.path().join("txn"))
        .expect("open transaction spill directory")
        .filter_map(Result::ok)
        .filter(|entry| entry.path().extension().is_some_and(|ext| ext == "run"))
        .count();
    let committed = writer.commit(cntryl_midge::WriteOptions::buffered());

    // Assert
    assert!(
        admitted.is_ok(),
        "local transactions must spill when assertion pressure prevents resident admission"
    );
    assert!(
        spill_runs > 0,
        "assertion pressure must create a transaction spill run before commit"
    );
    assert!(
        committed.is_ok(),
        "the directly spilled write must remain committable: {committed:?}"
    );
    let current = engine
        .begin_tx(cf.id(), cntryl_midge::TransactionMode::ReadOnly)
        .expect("begin verification transaction");
    assert_eq!(
        current.get(b"spilled").expect("read spilled value"),
        Some(Bytes::from_static(b"value"))
    );
    drop(asserting);
}

#[test]
fn should_use_transaction_snapshot_time_when_asserted_value_expires_before_commit() {
    // Arrange
    let engine = Arc::new(open_with_mode(&MidgeOptions::default(), "memory"));
    let cf = engine
        .create_column_family("assertion-ttl")
        .expect("create cf");
    let mut seed = engine
        .begin_tx(cf.id(), cntryl_midge::TransactionMode::ReadWrite)
        .expect("begin seed tx");
    seed.put(b"ttl-key".to_vec(), b"value".to_vec(), Some(1))
        .expect("seed expiring value");
    seed.commit(cntryl_midge::WriteOptions::buffered())
        .expect("commit expiring value");
    let mut asserting = engine
        .begin_tx(cf.id(), cntryl_midge::TransactionMode::ReadWrite)
        .expect("begin assertion snapshot");
    asserting
        .assert_value(b"ttl-key".to_vec(), Some(b"value".to_vec()))
        .expect("register ttl assertion");

    // Act
    std::thread::sleep(Duration::from_millis(1_100));
    let result = asserting.commit(cntryl_midge::WriteOptions::buffered());

    // Assert
    assert!(
        result.is_ok(),
        "assertion must use the TTL clock frozen with its transaction snapshot: {result:?}"
    );
    let current = engine
        .begin_tx(cf.id(), cntryl_midge::TransactionMode::ReadOnly)
        .expect("begin current snapshot");
    assert_eq!(
        current.get(b"ttl-key").expect("read current value"),
        None,
        "a new snapshot must observe the value as expired"
    );
}

#[test]
fn should_isolate_assertion_conflicts_between_column_families() {
    // Arrange
    let engine = Arc::new(open_with_mode(&MidgeOptions::default(), "memory"));
    let first_cf = engine
        .create_column_family("assertion-cf-first")
        .expect("create first cf");
    let second_cf = engine
        .create_column_family("assertion-cf-second")
        .expect("create second cf");
    for (cf_id, value) in [
        (first_cf.id(), b"first".as_slice()),
        (second_cf.id(), b"second".as_slice()),
    ] {
        let mut seed = engine
            .begin_tx(cf_id, cntryl_midge::TransactionMode::ReadWrite)
            .expect("begin seed transaction");
        seed.put(b"shared-key".to_vec(), value.to_vec(), None)
            .expect("seed shared key");
        seed.commit(cntryl_midge::WriteOptions::buffered())
            .expect("commit shared key");
    }
    let mut asserting = engine
        .begin_tx(first_cf.id(), cntryl_midge::TransactionMode::ReadWrite)
        .expect("begin first-cf assertion");
    asserting
        .assert_value(b"shared-key".to_vec(), Some(b"first".to_vec()))
        .expect("register first-cf assertion");
    let mut concurrent = engine
        .begin_tx(second_cf.id(), cntryl_midge::TransactionMode::ReadWrite)
        .expect("begin second-cf writer");
    concurrent
        .put(b"shared-key".to_vec(), b"updated".to_vec(), None)
        .expect("update second-cf key");
    concurrent
        .commit(cntryl_midge::WriteOptions::buffered())
        .expect("commit second-cf update");

    // Act
    let result = asserting.commit(cntryl_midge::WriteOptions::buffered());

    // Assert
    assert!(
        result.is_ok(),
        "a same-named key in another column family must not conflict: {result:?}"
    );
}

#[test]
fn should_reject_conflicting_duplicate_assertions_for_one_key() {
    // Arrange
    let engine = Arc::new(open_with_mode(&MidgeOptions::default(), "memory"));
    let cf = engine
        .create_column_family("duplicate-assertion")
        .expect("create cf");
    let mut seed = engine
        .begin_tx(cf.id(), cntryl_midge::TransactionMode::ReadWrite)
        .expect("begin seed transaction");
    seed.put(b"key".to_vec(), b"value".to_vec(), None)
        .expect("seed value");
    seed.commit(cntryl_midge::WriteOptions::buffered())
        .expect("commit seed value");
    let mut asserting = engine
        .begin_tx(cf.id(), cntryl_midge::TransactionMode::ReadWrite)
        .expect("begin asserting transaction");
    asserting
        .assert_value(b"key".to_vec(), Some(b"value".to_vec()))
        .expect("register matching assertion");
    asserting
        .assert_value(b"key".to_vec(), Some(b"different".to_vec()))
        .expect("register conflicting assertion");

    // Act
    let result = asserting.commit(cntryl_midge::WriteOptions::buffered());

    // Assert
    assert!(matches!(
        result,
        Err(cntryl_midge::MidgeError::WriteConflict(_))
    ));
}

// ============================================================================
// COMMIT-TIME ASSERTION ENFORCEMENT
//
// assert_value validates against the transaction's start snapshot client-side
// (above), but that alone is a TOCTOU gap: a concurrent commit to the
// asserted key between validation and this transaction's own commit is
// invisible to a purely client-side check. These tests cover the server-side
// enforcement that closes it.
// ============================================================================

fn sequence_metric(engine: &cntryl_midge::Engine) -> u64 {
    engine
        .get_runtime_metrics()
        .expect("runtime metrics")
        .current_sequence
}

#[test]
fn should_reject_assertion_when_concurrent_put_lands_after_start_sequence() {
    // Arrange
    let engine = Arc::new(open_with_mode(&MidgeOptions::default(), "memory"));
    let cf = engine
        .create_column_family("assert-put")
        .expect("create cf");
    let mut seed = engine
        .begin_tx(cf.id(), cntryl_midge::TransactionMode::ReadWrite)
        .expect("begin seed tx");
    seed.put(b"key".to_vec(), b"v1".to_vec(), None)
        .expect("seed value");
    seed.commit(cntryl_midge::WriteOptions::buffered())
        .expect("seed commit");

    let mut asserting = engine
        .begin_tx(cf.id(), cntryl_midge::TransactionMode::ReadWrite)
        .expect("begin asserting tx");
    asserting
        .assert_value(b"key".to_vec(), Some(b"v1".to_vec()))
        .expect("register assertion");
    asserting
        .put(b"other".to_vec(), b"unrelated".to_vec(), None)
        .expect("unrelated write so this isn't an assert-only commit");

    // Act: a concurrent transaction commits a new value to the asserted key
    // before the asserting transaction commits.
    let mut concurrent = engine
        .begin_tx(cf.id(), cntryl_midge::TransactionMode::ReadWrite)
        .expect("begin concurrent tx");
    concurrent
        .put(b"key".to_vec(), b"v2".to_vec(), None)
        .expect("concurrent write");
    concurrent
        .commit(cntryl_midge::WriteOptions::buffered())
        .expect("concurrent commit");

    let sequence_before = sequence_metric(&engine);
    let result = asserting.commit(cntryl_midge::WriteOptions::buffered());

    // Assert
    assert!(matches!(
        result,
        Err(cntryl_midge::MidgeError::WriteConflict(_))
    ));
    assert_eq!(
        sequence_metric(&engine),
        sequence_before,
        "a rejected commit must not advance the sequence"
    );
    let read_tx = engine
        .begin_tx(cf.id(), cntryl_midge::TransactionMode::ReadOnly)
        .expect("begin read tx");
    assert_eq!(
        read_tx.get(b"other").expect("read unrelated key"),
        None,
        "a rejected commit must not apply any of its writes"
    );
}

#[test]
fn should_reject_assertion_when_concurrent_delete_lands_after_start_sequence() {
    // Arrange
    let engine = Arc::new(open_with_mode(&MidgeOptions::default(), "memory"));
    let cf = engine
        .create_column_family("assert-delete")
        .expect("create cf");
    let mut seed = engine
        .begin_tx(cf.id(), cntryl_midge::TransactionMode::ReadWrite)
        .expect("begin seed tx");
    seed.put(b"key".to_vec(), b"v1".to_vec(), None)
        .expect("seed value");
    seed.commit(cntryl_midge::WriteOptions::buffered())
        .expect("seed commit");

    let mut asserting = engine
        .begin_tx(cf.id(), cntryl_midge::TransactionMode::ReadWrite)
        .expect("begin asserting tx");
    asserting
        .assert_value(b"key".to_vec(), Some(b"v1".to_vec()))
        .expect("register assertion");

    // Act
    let mut concurrent = engine
        .begin_tx(cf.id(), cntryl_midge::TransactionMode::ReadWrite)
        .expect("begin concurrent tx");
    concurrent
        .delete(b"key".to_vec())
        .expect("concurrent delete");
    concurrent
        .commit(cntryl_midge::WriteOptions::buffered())
        .expect("concurrent commit");

    let result = asserting.commit(cntryl_midge::WriteOptions::buffered());

    // Assert
    assert!(matches!(
        result,
        Err(cntryl_midge::MidgeError::WriteConflict(_))
    ));
}

#[test]
fn should_reject_assertion_when_concurrent_range_delete_covers_the_key() {
    // Arrange
    let engine = Arc::new(open_with_mode(&MidgeOptions::default(), "memory"));
    let cf = engine
        .create_column_family("assert-range-delete")
        .expect("create cf");
    let mut seed = engine
        .begin_tx(cf.id(), cntryl_midge::TransactionMode::ReadWrite)
        .expect("begin seed tx");
    seed.put(b"key-b".to_vec(), b"v1".to_vec(), None)
        .expect("seed value");
    seed.commit(cntryl_midge::WriteOptions::buffered())
        .expect("seed commit");

    let mut asserting = engine
        .begin_tx(cf.id(), cntryl_midge::TransactionMode::ReadWrite)
        .expect("begin asserting tx");
    asserting
        .assert_value(b"key-b".to_vec(), Some(b"v1".to_vec()))
        .expect("register assertion");

    // Act: the concurrent range delete never touches "key-b" directly, only
    // covers it — the assertion check must consult covering range deletes,
    // not just point mutations.
    let mut concurrent = engine
        .begin_tx(cf.id(), cntryl_midge::TransactionMode::ReadWrite)
        .expect("begin concurrent tx");
    concurrent
        .delete_range(b"key-a".to_vec(), b"key-z".to_vec())
        .expect("concurrent range delete");
    concurrent
        .commit(cntryl_midge::WriteOptions::buffered())
        .expect("concurrent commit");

    let result = asserting.commit(cntryl_midge::WriteOptions::buffered());

    // Assert
    assert!(matches!(
        result,
        Err(cntryl_midge::MidgeError::WriteConflict(_))
    ));
}

#[test]
fn should_reject_absent_assertion_when_key_is_inserted_concurrently() {
    // Arrange
    let engine = Arc::new(open_with_mode(&MidgeOptions::default(), "memory"));
    let cf = engine
        .create_column_family("assert-absent-insert")
        .expect("create cf");

    let mut asserting = engine
        .begin_tx(cf.id(), cntryl_midge::TransactionMode::ReadWrite)
        .expect("begin asserting tx");
    asserting
        .assert_value(b"key".to_vec(), None)
        .expect("register absent assertion");

    // Act: the key is still absent as of `asserting`'s frozen start
    // snapshot, so client-side validation (checked at commit()) would still
    // pass — only the server-side sequence check catches this.
    let mut concurrent = engine
        .begin_tx(cf.id(), cntryl_midge::TransactionMode::ReadWrite)
        .expect("begin concurrent tx");
    concurrent
        .put(b"key".to_vec(), b"inserted".to_vec(), None)
        .expect("concurrent insert");
    concurrent
        .commit(cntryl_midge::WriteOptions::buffered())
        .expect("concurrent commit");

    let result = asserting.commit(cntryl_midge::WriteOptions::buffered());

    // Assert
    assert!(matches!(
        result,
        Err(cntryl_midge::MidgeError::WriteConflict(_))
    ));
}

#[test]
fn should_reject_assertion_given_aba_value_restored_after_intervening_write() {
    // Arrange: value goes v1 -> v2 -> v1. A value re-read at commit time
    // would see v1 and wrongly pass; the sequence-based check must still
    // reject because the key's sequence advanced twice after start_sequence.
    let engine = Arc::new(open_with_mode(&MidgeOptions::default(), "memory"));
    let cf = engine
        .create_column_family("assert-aba")
        .expect("create cf");
    let mut seed = engine
        .begin_tx(cf.id(), cntryl_midge::TransactionMode::ReadWrite)
        .expect("begin seed tx");
    seed.put(b"key".to_vec(), b"v1".to_vec(), None)
        .expect("seed value");
    seed.commit(cntryl_midge::WriteOptions::buffered())
        .expect("seed commit");

    let mut asserting = engine
        .begin_tx(cf.id(), cntryl_midge::TransactionMode::ReadWrite)
        .expect("begin asserting tx");
    asserting
        .assert_value(b"key".to_vec(), Some(b"v1".to_vec()))
        .expect("register assertion");

    // Act
    let mut to_v2 = engine
        .begin_tx(cf.id(), cntryl_midge::TransactionMode::ReadWrite)
        .expect("begin tx to v2");
    to_v2
        .put(b"key".to_vec(), b"v2".to_vec(), None)
        .expect("write v2");
    to_v2
        .commit(cntryl_midge::WriteOptions::buffered())
        .expect("commit v2");

    let mut back_to_v1 = engine
        .begin_tx(cf.id(), cntryl_midge::TransactionMode::ReadWrite)
        .expect("begin tx back to v1");
    back_to_v1
        .put(b"key".to_vec(), b"v1".to_vec(), None)
        .expect("restore v1");
    back_to_v1
        .commit(cntryl_midge::WriteOptions::buffered())
        .expect("commit restored v1");

    let result = asserting.commit(cntryl_midge::WriteOptions::buffered());

    // Assert
    assert!(
        matches!(result, Err(cntryl_midge::MidgeError::WriteConflict(_))),
        "ABA-restoring the value must not defeat the assertion, got: {result:?}"
    );
}

#[test]
fn should_allow_writing_asserted_key_in_same_transaction() {
    // Arrange
    let engine = Arc::new(open_with_mode(&MidgeOptions::default(), "memory"));
    let cf = engine
        .create_column_family("assert-self")
        .expect("create cf");
    let mut seed = engine
        .begin_tx(cf.id(), cntryl_midge::TransactionMode::ReadWrite)
        .expect("begin seed tx");
    seed.put(b"key".to_vec(), b"v1".to_vec(), None)
        .expect("seed value");
    seed.commit(cntryl_midge::WriteOptions::buffered())
        .expect("seed commit");

    // Act: a compare-and-swap pattern — assert the current value, then
    // write a new one, all in the same transaction. The transaction's own
    // pending write must not be mistaken for an external conflict.
    let mut txn = engine
        .begin_tx(cf.id(), cntryl_midge::TransactionMode::ReadWrite)
        .expect("begin cas tx");
    txn.assert_value(b"key".to_vec(), Some(b"v1".to_vec()))
        .expect("register assertion");
    txn.put(b"key".to_vec(), b"v2".to_vec(), None)
        .expect("cas write");
    let result = txn.commit(cntryl_midge::WriteOptions::buffered());

    // Assert
    assert!(result.is_ok(), "expected CAS commit to succeed: {result:?}");
    let read_tx = engine
        .begin_tx(cf.id(), cntryl_midge::TransactionMode::ReadOnly)
        .expect("begin read tx");
    assert_eq!(
        read_tx.get(b"key").expect("read value"),
        Some(Bytes::from_static(b"v2"))
    );
}

#[test]
fn should_commit_disjoint_assertion_with_write() {
    // Arrange
    let engine = Arc::new(open_with_mode(&MidgeOptions::default(), "memory"));
    let cf = engine
        .create_column_family("assert-disjoint")
        .expect("create cf");
    let mut seed = engine
        .begin_tx(cf.id(), cntryl_midge::TransactionMode::ReadWrite)
        .expect("begin seed tx");
    seed.put(b"guard".to_vec(), b"unchanged".to_vec(), None)
        .expect("seed guard value");
    seed.commit(cntryl_midge::WriteOptions::buffered())
        .expect("seed commit");

    // Act: assert an unrelated key while writing a different one; neither
    // should interfere with the other.
    let mut txn = engine
        .begin_tx(cf.id(), cntryl_midge::TransactionMode::ReadWrite)
        .expect("begin tx");
    txn.assert_value(b"guard".to_vec(), Some(b"unchanged".to_vec()))
        .expect("register assertion");
    txn.put(b"data".to_vec(), b"value".to_vec(), None)
        .expect("unrelated write");
    let result = txn.commit(cntryl_midge::WriteOptions::buffered());

    // Assert
    assert!(result.is_ok());
    let read_tx = engine
        .begin_tx(cf.id(), cntryl_midge::TransactionMode::ReadOnly)
        .expect("begin read tx");
    assert_eq!(
        read_tx.get(b"data").expect("read data"),
        Some(Bytes::from_static(b"value"))
    );
}

#[test]
fn should_enforce_assertion_conflict_even_under_last_write_wins_policy() {
    // Arrange: LastWriteWins is the default and normally lets a later
    // committer silently overwrite an earlier reader's view. An explicit
    // assertion is a stronger, opt-in guarantee and must still be enforced.
    let engine = Arc::new(open_with_mode(&MidgeOptions::default(), "memory"));
    let cf = engine
        .create_column_family("assert-lww")
        .expect("create cf");
    let mut seed = engine
        .begin_tx(cf.id(), cntryl_midge::TransactionMode::ReadWrite)
        .expect("begin seed tx");
    seed.put(b"key".to_vec(), b"v1".to_vec(), None)
        .expect("seed value");
    seed.commit(cntryl_midge::WriteOptions::buffered())
        .expect("seed commit");

    let mut asserting = engine
        .begin_tx(cf.id(), cntryl_midge::TransactionMode::ReadWrite)
        .expect("begin asserting tx");
    assert_eq!(
        asserting.conflict_policy(),
        cntryl_midge::ConflictPolicy::LastWriteWins,
        "this test exercises the default policy"
    );
    asserting
        .assert_value(b"key".to_vec(), Some(b"v1".to_vec()))
        .expect("register assertion");
    asserting
        .put(b"other".to_vec(), b"value".to_vec(), None)
        .expect("unrelated write");

    // Act
    let mut concurrent = engine
        .begin_tx(cf.id(), cntryl_midge::TransactionMode::ReadWrite)
        .expect("begin concurrent tx");
    concurrent
        .put(b"key".to_vec(), b"v2".to_vec(), None)
        .expect("concurrent write");
    concurrent
        .commit(cntryl_midge::WriteOptions::buffered())
        .expect("concurrent commit");

    let result = asserting.commit(cntryl_midge::WriteOptions::buffered());

    // Assert
    assert!(matches!(
        result,
        Err(cntryl_midge::MidgeError::WriteConflict(_))
    ));
}

#[test]
fn should_validate_assertion_only_commit_without_allocating_a_sequence() {
    // Arrange
    let engine = Arc::new(open_with_mode(&MidgeOptions::default(), "memory"));
    let cf = engine
        .create_column_family("assert-only-commit")
        .expect("create cf");
    let mut seed = engine
        .begin_tx(cf.id(), cntryl_midge::TransactionMode::ReadWrite)
        .expect("begin seed tx");
    seed.put(b"key".to_vec(), b"v1".to_vec(), None)
        .expect("seed value");
    seed.commit(cntryl_midge::WriteOptions::buffered())
        .expect("seed commit");

    // Act: no put/delete/delete_range calls at all — only an assertion.
    let mut txn = engine
        .begin_tx(cf.id(), cntryl_midge::TransactionMode::ReadWrite)
        .expect("begin assert-only tx");
    txn.assert_value(b"key".to_vec(), Some(b"v1".to_vec()))
        .expect("register assertion");

    let sequence_before = sequence_metric(&engine);
    let result = txn.commit(cntryl_midge::WriteOptions::buffered());

    // Assert
    assert!(
        result.is_ok(),
        "expected assert-only commit to succeed: {result:?}"
    );
    assert_eq!(
        sequence_metric(&engine),
        sequence_before,
        "an assert-only commit must not allocate a sequence"
    );
}

#[test]
fn should_reject_assertion_only_commit_when_key_changed_concurrently() {
    // Arrange
    let engine = Arc::new(open_with_mode(&MidgeOptions::default(), "memory"));
    let cf = engine
        .create_column_family("assert-only-reject")
        .expect("create cf");
    let mut seed = engine
        .begin_tx(cf.id(), cntryl_midge::TransactionMode::ReadWrite)
        .expect("begin seed tx");
    seed.put(b"key".to_vec(), b"v1".to_vec(), None)
        .expect("seed value");
    seed.commit(cntryl_midge::WriteOptions::buffered())
        .expect("seed commit");

    let mut asserting = engine
        .begin_tx(cf.id(), cntryl_midge::TransactionMode::ReadWrite)
        .expect("begin assert-only tx");
    asserting
        .assert_value(b"key".to_vec(), Some(b"v1".to_vec()))
        .expect("register assertion");

    // Act
    let mut concurrent = engine
        .begin_tx(cf.id(), cntryl_midge::TransactionMode::ReadWrite)
        .expect("begin concurrent tx");
    concurrent
        .put(b"key".to_vec(), b"v2".to_vec(), None)
        .expect("concurrent write");
    concurrent
        .commit(cntryl_midge::WriteOptions::buffered())
        .expect("concurrent commit");

    let sequence_before = sequence_metric(&engine);
    let result = asserting.commit(cntryl_midge::WriteOptions::buffered());

    // Assert
    assert!(matches!(
        result,
        Err(cntryl_midge::MidgeError::WriteConflict(_))
    ));
    assert_eq!(
        sequence_metric(&engine),
        sequence_before,
        "a rejected assert-only commit must not allocate a sequence"
    );
}

#[test]
fn should_reject_assertion_conflict_in_a_spilled_transaction() {
    for_each_storage_mode(durable_storage_modes(), |mode, opts| {
        // Arrange: a small memory budget forces the write set to spill to
        // disk, exercising validate_spilled_transaction's assertion check
        // rather than the in-memory path.
        let mut opts = opts;
        opts = opts.memory_budget(256 * 1024);
        let engine = open_with_mode(&opts, mode);
        let cf = engine
            .create_column_family("assert-spill")
            .expect("create cf");

        let mut seed = engine
            .begin_tx(cf.id(), cntryl_midge::TransactionMode::ReadWrite)
            .expect("begin seed tx");
        seed.put(b"guard".to_vec(), b"v1".to_vec(), None)
            .expect("seed value");
        seed.commit(buffered_write_options(mode))
            .expect("seed commit");

        let mut asserting = engine
            .begin_tx(cf.id(), cntryl_midge::TransactionMode::ReadWrite)
            .expect("begin asserting tx");
        asserting
            .assert_value(b"guard".to_vec(), Some(b"v1".to_vec()))
            .expect("register assertion");
        for i in 0..200 {
            let key = format!("spill-key{i:04}");
            let value = format!("spill-value_{i:04}");
            asserting
                .put(key.as_bytes().to_vec(), value.as_bytes().to_vec(), None)
                .expect("put");
        }

        // Act
        let mut concurrent = engine
            .begin_tx(cf.id(), cntryl_midge::TransactionMode::ReadWrite)
            .expect("begin concurrent tx");
        concurrent
            .put(b"guard".to_vec(), b"v2".to_vec(), None)
            .expect("concurrent write");
        concurrent
            .commit(buffered_write_options(mode))
            .expect("concurrent commit");

        let result = asserting.commit(buffered_write_options(mode));

        // Assert
        assert!(
            matches!(result, Err(cntryl_midge::MidgeError::WriteConflict(_))),
            "expected spilled commit to reject the stale assertion in mode {mode}, got: {result:?}"
        );

        let read_tx = engine
            .begin_tx(cf.id(), cntryl_midge::TransactionMode::ReadOnly)
            .expect("begin read tx");
        assert_eq!(
            read_tx.get(b"spill-key0000").expect("read spilled key"),
            None,
            "a rejected spilled commit must not apply any of its writes in mode {mode}"
        );
    });
}

// ============================================================================
// BASIC LWW SEMANTICS TESTS
// ============================================================================

#[test]
fn should_allow_concurrent_puts_to_same_key_given_lww_semantics() {
    for_each_storage_mode(&all_storage_modes_new(), |mode, opts| {
        // Arrange
        let engine = Arc::new(open_with_mode(&opts, mode));
        let cf = engine.create_column_family("test").expect("create cf");

        // Act
        let mut txn1 = engine
            .begin_tx(cf.id(), cntryl_midge::TransactionMode::ReadWrite)
            .unwrap();
        let mut txn2 = engine
            .begin_tx(cf.id(), cntryl_midge::TransactionMode::ReadWrite)
            .unwrap();

        txn1.put(b"key".to_vec(), b"value1".to_vec(), None).unwrap();
        txn2.put(b"key".to_vec(), b"value2".to_vec(), None).unwrap();

        txn1.commit(buffered_write_options(mode)).unwrap();
        txn2.commit(buffered_write_options(mode)).unwrap();

        // Assert - last committed wins
        let read_tx = engine
            .begin_tx(cf.id(), cntryl_midge::TransactionMode::ReadOnly)
            .unwrap();
        let value = read_tx.get(b"key").unwrap();
        assert_eq!(value, Some(Bytes::from_static(b"value2")));
    });
}

#[test]
fn should_accept_both_committers_given_concurrent_puts_when_lww() {
    for_each_storage_mode(&all_storage_modes_new(), |mode, opts| {
        // Arrange
        let engine = Arc::new(open_with_mode(&opts, mode));
        let cf = engine.create_column_family("test").expect("create cf");
        let cf_id = cf.id();
        let engine1 = Arc::clone(&engine);
        let engine2 = Arc::clone(&engine);

        // Act
        let write_options = buffered_write_options(mode);
        let handle1 = std::thread::spawn(move || {
            let mut txn = engine1
                .begin_tx(cf_id, cntryl_midge::TransactionMode::ReadWrite)
                .unwrap();
            txn.put(b"key".to_vec(), b"value1".to_vec(), None).unwrap();
            txn.commit(write_options)
        });

        let write_options = buffered_write_options(mode);
        let handle2 = std::thread::spawn(move || {
            let mut txn = engine2
                .begin_tx(cf_id, cntryl_midge::TransactionMode::ReadWrite)
                .unwrap();
            txn.put(b"key".to_vec(), b"value2".to_vec(), None).unwrap();
            txn.commit(write_options)
        });

        // Assert - both commits succeed
        assert!(handle1.join().unwrap().is_ok());
        assert!(handle2.join().unwrap().is_ok());
    });
}

#[test]
fn should_finish_concurrent_local_same_key_lww_buffered_commits_within_timeout() {
    // Arrange
    let mode = "local";
    let opts = opts_for_mode(mode);
    let engine = Arc::new(open_with_mode(&opts, mode));
    let cf = engine.create_column_family("test").expect("create cf");
    let cf_id = cf.id();
    let worker_count = 16usize;
    let barrier = Arc::new(std::sync::Barrier::new(worker_count + 1));
    let (result_tx, result_rx) = std::sync::mpsc::channel();
    let mut handles = Vec::with_capacity(worker_count);

    for worker_id in 0..worker_count {
        let worker_engine = Arc::clone(&engine);
        let worker_barrier = Arc::clone(&barrier);
        let worker_result_tx = result_tx.clone();
        handles.push(std::thread::spawn(move || {
            let result = (|| -> cntryl_midge::MidgeResult<()> {
                let mut txn =
                    worker_engine.begin_tx(cf_id, cntryl_midge::TransactionMode::ReadWrite)?;
                txn.put(
                    b"shared-key".to_vec(),
                    format!("value-{worker_id:02}").into_bytes(),
                    None,
                )?;
                worker_barrier.wait();
                txn.commit(buffered_write_options(mode))
            })();

            let _ = worker_result_tx.send((worker_id, result));
        }));
    }
    drop(result_tx);

    // Act
    barrier.wait();

    // Assert
    let deadline = Instant::now() + Duration::from_secs(5);
    let mut seen = vec![false; worker_count];
    for _ in 0..worker_count {
        let remaining = deadline
            .checked_duration_since(Instant::now())
            .unwrap_or_else(|| Duration::from_secs(0));
        let (worker_id, result) = result_rx.recv_timeout(remaining).unwrap_or_else(|error| {
            panic!("timed out waiting for concurrent commit result: {error:?}");
        });
        assert!(worker_id < worker_count, "invalid worker id {worker_id}");
        assert!(
            !std::mem::replace(&mut seen[worker_id], true),
            "duplicate result from worker {worker_id}"
        );
        result.unwrap_or_else(|error| panic!("worker {worker_id} commit failed: {error:?}"));
    }

    for handle in handles {
        handle
            .join()
            .expect("worker should not panic after reporting result");
    }
    assert!(
        seen.into_iter().all(|received| received),
        "every worker should report exactly one result"
    );

    let read_tx = engine
        .begin_tx(cf_id, cntryl_midge::TransactionMode::ReadOnly)
        .expect("begin read tx");
    let value = read_tx
        .get(b"shared-key")
        .expect("read shared key")
        .expect("shared key should exist");
    let final_value = std::str::from_utf8(value.as_ref()).expect("final value should be utf8");
    assert!(
        final_value.starts_with("value-"),
        "final value should be one of the committed worker values, got {final_value}"
    );
}

#[test]
fn should_preserve_first_commit_given_write_conflict_when_second_aborts() {
    for_each_storage_mode(&all_storage_modes_new(), |mode, opts| {
        // Arrange
        let engine = Arc::new(open_with_mode(&opts, mode));
        let cf = engine.create_column_family("test").expect("create cf");

        // Act
        let mut txn1 = engine
            .begin_tx(cf.id(), cntryl_midge::TransactionMode::ReadWrite)
            .unwrap();
        txn1.put(b"key".to_vec(), b"value1".to_vec(), None).unwrap();
        txn1.commit(buffered_write_options(mode)).unwrap();

        let mut txn2 = engine
            .begin_tx(cf.id(), cntryl_midge::TransactionMode::ReadWrite)
            .unwrap();
        txn2.put(b"key".to_vec(), b"value2".to_vec(), None).unwrap();
        drop(txn2); // Rollback

        // Assert
        let read_tx = engine
            .begin_tx(cf.id(), cntryl_midge::TransactionMode::ReadOnly)
            .unwrap();
        let value = read_tx.get(b"key").unwrap();
        assert_eq!(value, Some(Bytes::from_static(b"value1")));
    });
}

#[test]
fn should_allow_concurrent_delete_put_operations_given_lww_semantics() {
    for_each_storage_mode(&all_storage_modes_new(), |mode, opts| {
        // Arrange
        let engine = Arc::new(open_with_mode(&opts, mode));
        let cf = engine.create_column_family("test").expect("create cf");
        let mut setup_tx = engine
            .begin_tx(cf.id(), cntryl_midge::TransactionMode::ReadWrite)
            .unwrap();
        setup_tx
            .put(b"key".to_vec(), b"initial".to_vec(), None)
            .unwrap();
        setup_tx.commit(buffered_write_options(mode)).unwrap();

        // Act
        let mut txn1 = engine
            .begin_tx(cf.id(), cntryl_midge::TransactionMode::ReadWrite)
            .unwrap();
        let mut txn2 = engine
            .begin_tx(cf.id(), cntryl_midge::TransactionMode::ReadWrite)
            .unwrap();

        txn1.delete(b"key".to_vec()).unwrap();
        txn2.put(b"key".to_vec(), b"value".to_vec(), None).unwrap();

        txn1.commit(buffered_write_options(mode)).unwrap();
        txn2.commit(buffered_write_options(mode)).unwrap();

        // Assert - last operation wins
        let read_tx = engine
            .begin_tx(cf.id(), cntryl_midge::TransactionMode::ReadOnly)
            .unwrap();
        let value = read_tx.get(b"key").unwrap();
        assert_eq!(value, Some(Bytes::from_static(b"value")));
    });
}

#[test]
fn should_allow_overlapping_put_after_delete_range_given_lww_semantics() {
    for_each_storage_mode(&all_storage_modes_new(), |mode, opts| {
        // Arrange
        let engine = Arc::new(open_with_mode(&opts, mode));
        let cf = engine.create_column_family("test").expect("create cf");
        let cf_id = cf.id();
        let mut setup_tx = engine
            .begin_tx(cf_id, cntryl_midge::TransactionMode::ReadWrite)
            .unwrap();
        setup_tx
            .put(b"key1".to_vec(), b"value1".to_vec(), None)
            .unwrap();
        setup_tx
            .put(b"key2".to_vec(), b"value2".to_vec(), None)
            .unwrap();
        setup_tx.commit(buffered_write_options(mode)).unwrap();

        // Act
        let mut txn2 = engine
            .begin_tx(cf_id, cntryl_midge::TransactionMode::ReadWrite)
            .unwrap();

        txn2.put(b"key2".to_vec(), b"newvalue".to_vec(), None)
            .unwrap();

        let mut delete_tx = engine
            .begin_tx(cf.id(), cntryl_midge::TransactionMode::ReadWrite)
            .unwrap();
        delete_tx
            .delete_range(b"key1".to_vec(), b"key3".to_vec())
            .unwrap();
        delete_tx.commit(buffered_write_options(mode)).unwrap();
        txn2.commit(buffered_write_options(mode)).unwrap();

        // Assert
        let read_tx = engine
            .begin_tx(cf_id, cntryl_midge::TransactionMode::ReadOnly)
            .unwrap();
        let value = read_tx.get(b"key2").unwrap();
        assert_eq!(value, Some(Bytes::from_static(b"newvalue")));
    });
}

#[test]
fn should_allow_put_then_delete_range_given_lww_semantics() {
    for_each_storage_mode(&all_storage_modes_new(), |mode, opts| {
        // Arrange
        let engine = Arc::new(open_with_mode(&opts, mode));
        let cf = engine.create_column_family("test").expect("create cf");

        // Act
        let mut txn1 = engine
            .begin_tx(cf.id(), cntryl_midge::TransactionMode::ReadWrite)
            .unwrap();

        txn1.put(b"key".to_vec(), b"value".to_vec(), None).unwrap();

        txn1.commit(buffered_write_options(mode)).unwrap();
        let mut tx = engine
            .begin_tx(cf.id(), cntryl_midge::TransactionMode::ReadWrite)
            .unwrap();
        tx.delete_range(b"key".to_vec(), b"keyz".to_vec()).unwrap();
        tx.commit(buffered_write_options(mode)).unwrap();

        // Assert
        let read_tx = engine
            .begin_tx(cf.id(), cntryl_midge::TransactionMode::ReadOnly)
            .unwrap();
        let value = read_tx.get(b"key").unwrap();
        assert_eq!(value, None);
    });
}

#[test]
fn should_allow_concurrent_delete_ranges_given_lww_semantics() {
    for_each_storage_mode(&all_storage_modes_new(), |mode, opts| {
        // Arrange
        let engine = Arc::new(open_with_mode(&opts, mode));
        let cf = engine.create_column_family("test").expect("create cf");
        let cf_id = cf.id();
        let mut setup_tx = engine
            .begin_tx(cf_id, cntryl_midge::TransactionMode::ReadWrite)
            .unwrap();
        setup_tx
            .put(b"key1".to_vec(), b"value1".to_vec(), None)
            .unwrap();
        setup_tx
            .put(b"key2".to_vec(), b"value2".to_vec(), None)
            .unwrap();
        setup_tx.commit(buffered_write_options(mode)).unwrap();

        // Act
        let mut tx = engine
            .begin_tx(cf.id(), cntryl_midge::TransactionMode::ReadWrite)
            .unwrap();
        tx.delete_range(b"key1".to_vec(), b"key3".to_vec()).unwrap();
        tx.commit(buffered_write_options(mode)).unwrap();
        let mut tx = engine
            .begin_tx(cf.id(), cntryl_midge::TransactionMode::ReadWrite)
            .unwrap();
        tx.delete_range(b"key1".to_vec(), b"key3".to_vec()).unwrap();
        tx.commit(buffered_write_options(mode)).unwrap();

        // Assert - both succeed
        let read_tx = engine
            .begin_tx(cf_id, cntryl_midge::TransactionMode::ReadOnly)
            .unwrap();
        assert!(read_tx.get(b"key1").unwrap().is_none());
    });
}

#[test]
fn should_allow_delete_range_delete_operations_given_lww_semantics() {
    for_each_storage_mode(&all_storage_modes_new(), |mode, opts| {
        // Arrange
        let engine = Arc::new(open_with_mode(&opts, mode));
        let cf = engine.create_column_family("test").expect("create cf");
        let mut setup_tx = engine
            .begin_tx(cf.id(), cntryl_midge::TransactionMode::ReadWrite)
            .unwrap();
        setup_tx
            .put(b"key".to_vec(), b"value".to_vec(), None)
            .unwrap();
        setup_tx.commit(buffered_write_options(mode)).unwrap();

        // Act
        let mut txn2 = engine
            .begin_tx(cf.id(), cntryl_midge::TransactionMode::ReadWrite)
            .unwrap();

        txn2.delete(b"key".to_vec()).unwrap();

        let mut delete_tx = engine
            .begin_tx(cf.id(), cntryl_midge::TransactionMode::ReadWrite)
            .unwrap();
        delete_tx
            .delete_range(b"key1".to_vec(), b"key3".to_vec())
            .unwrap();
        delete_tx.commit(buffered_write_options(mode)).unwrap();
        txn2.commit(buffered_write_options(mode)).unwrap();

        // Assert
        let read_tx = engine
            .begin_tx(cf.id(), cntryl_midge::TransactionMode::ReadOnly)
            .unwrap();
        assert!(read_tx.get(b"key").unwrap().is_none());
    });
}

// ============================================================================
// INSERT CONFLICT TESTS
// ============================================================================

#[test]
fn should_overwrite_existing_value_given_put_on_existing_key_when_committed() {
    for_each_storage_mode(&all_storage_modes_new(), |mode, opts| {
        // Arrange
        let engine = Arc::new(open_with_mode(&opts, mode));
        let cf = engine.create_column_family("test").expect("create cf");
        let mut setup_tx = engine
            .begin_tx(cf.id(), cntryl_midge::TransactionMode::ReadWrite)
            .unwrap();
        setup_tx
            .put(b"key".to_vec(), b"existing".to_vec(), None)
            .unwrap();
        setup_tx.commit(buffered_write_options(mode)).unwrap();

        // Act - transaction attempts put on existing key
        let mut txn = engine
            .begin_tx(cf.id(), cntryl_midge::TransactionMode::ReadWrite)
            .unwrap();
        txn.put(b"key".to_vec(), b"newvalue".to_vec(), None)
            .unwrap();
        let result = txn.commit(buffered_write_options(mode));

        // Assert - put succeeds (LWW semantics, not insert semantics)
        assert!(result.is_ok());
        let read_tx = engine
            .begin_tx(cf.id(), cntryl_midge::TransactionMode::ReadOnly)
            .unwrap();
        let value = read_tx.get(b"key").unwrap();
        assert_eq!(value, Some(Bytes::from_static(b"newvalue")));
    });
}

// ============================================================================
// LOST UPDATE TESTS
// ============================================================================

#[test]
fn should_allow_lost_update_given_put_read_modify_write_when_concurrent() {
    for_each_storage_mode(&all_storage_modes_new(), |mode, opts| {
        // Arrange
        let engine = Arc::new(open_with_mode(&opts, mode));
        let cf = engine.create_column_family("test").expect("create cf");
        let mut setup_tx = engine
            .begin_tx(cf.id(), cntryl_midge::TransactionMode::ReadWrite)
            .unwrap();
        setup_tx
            .put(b"counter".to_vec(), b"0".to_vec(), None)
            .unwrap();
        setup_tx.commit(buffered_write_options(mode)).unwrap();

        // Act - simulate lost update with LWW semantics
        let read_tx1 = engine
            .begin_tx(cf.id(), cntryl_midge::TransactionMode::ReadOnly)
            .unwrap();
        let _val1 = read_tx1.get(b"counter").unwrap().unwrap();
        let read_tx2 = engine
            .begin_tx(cf.id(), cntryl_midge::TransactionMode::ReadOnly)
            .unwrap();
        let _val2 = read_tx2.get(b"counter").unwrap().unwrap();

        let mut txn1 = engine
            .begin_tx(cf.id(), cntryl_midge::TransactionMode::ReadWrite)
            .unwrap();
        txn1.put(b"counter".to_vec(), b"1".to_vec(), None).unwrap();
        txn1.commit(buffered_write_options(mode)).unwrap();

        let mut txn2 = engine
            .begin_tx(cf.id(), cntryl_midge::TransactionMode::ReadWrite)
            .unwrap();
        txn2.put(b"counter".to_vec(), b"1".to_vec(), None).unwrap();
        txn2.commit(buffered_write_options(mode)).unwrap();

        // Assert - lost update allowed with LWW
        let read_tx = engine
            .begin_tx(cf.id(), cntryl_midge::TransactionMode::ReadOnly)
            .unwrap();
        let final_value = read_tx.get(b"counter").unwrap();
        assert_eq!(final_value, Some(Bytes::from_static(b"1")));
    });
}

#[test]
fn should_detect_lost_update_given_cas_pattern_when_value_changed() {
    for_each_storage_mode(&all_storage_modes_new(), |mode, opts| {
        // Arrange
        let engine = Arc::new(open_with_mode(&opts, mode));
        let cf = engine.create_column_family("test").expect("create cf");
        let mut setup_tx = engine
            .begin_tx(cf.id(), cntryl_midge::TransactionMode::ReadWrite)
            .unwrap();
        setup_tx
            .put(b"counter".to_vec(), b"0".to_vec(), None)
            .unwrap();
        setup_tx.commit(buffered_write_options(mode)).unwrap();

        // Act - compare-and-swap pattern: read the counter, then commit a write
        // guarded by an assertion that the value has not changed since the read.
        let read_tx = engine
            .begin_tx(cf.id(), cntryl_midge::TransactionMode::ReadOnly)
            .unwrap();
        let original = read_tx.get(b"counter").unwrap().unwrap();
        assert_eq!(original, Bytes::from_static(b"0"));

        let mut txn = engine
            .begin_tx(cf.id(), cntryl_midge::TransactionMode::ReadWrite)
            .unwrap();
        txn.put(b"counter".to_vec(), b"1".to_vec(), None).unwrap();
        txn.assert_value(b"counter".to_vec(), Some(original.to_vec()))
            .expect("register CAS assertion");

        // Concurrent transaction modifies the counter before the CAS transaction commits.
        let mut txn_concurrent = engine
            .begin_tx(cf.id(), cntryl_midge::TransactionMode::ReadWrite)
            .unwrap();
        txn_concurrent
            .put(b"counter".to_vec(), b"2".to_vec(), None)
            .unwrap();
        txn_concurrent.commit(buffered_write_options(mode)).unwrap();

        // The stale CAS transaction now tries to commit its read-modify-write.
        let result = txn.commit(buffered_write_options(mode));

        // Assert - the CAS assertion detects that the value changed underneath it
        // and rejects the commit as a write conflict, so the update is not lost.
        assert!(
            matches!(result, Err(cntryl_midge::MidgeError::WriteConflict(_))),
            "expected stale CAS commit to be rejected in mode {mode}, got: {result:?}"
        );

        // The winning value is the concurrent transaction's write, not the stale
        // transaction's "1" - the lost update was successfully prevented.
        let read_tx2 = engine
            .begin_tx(cf.id(), cntryl_midge::TransactionMode::ReadOnly)
            .unwrap();
        let value = read_tx2.get(b"counter").unwrap();
        assert_eq!(
            value,
            Some(Bytes::from_static(b"2")),
            "concurrent writer's value must win when the CAS commit is rejected in mode {mode}"
        );
    });
}

// ============================================================================
// NON-CONFLICTING TRANSACTION TESTS
// ============================================================================

#[test]
fn should_preserve_both_updates_given_non_overlapping_keys_when_concurrent_commits() {
    for_each_storage_mode(&all_storage_modes_new(), |mode, opts| {
        // Arrange
        let engine = Arc::new(open_with_mode(&opts, mode));
        let cf = engine.create_column_family("test").expect("create cf");

        // Act
        let mut txn1 = engine
            .begin_tx(cf.id(), cntryl_midge::TransactionMode::ReadWrite)
            .unwrap();
        let mut txn2 = engine
            .begin_tx(cf.id(), cntryl_midge::TransactionMode::ReadWrite)
            .unwrap();

        txn1.put(b"key1".to_vec(), b"value1".to_vec(), None)
            .unwrap();
        txn2.put(b"key2".to_vec(), b"value2".to_vec(), None)
            .unwrap();

        txn1.commit(buffered_write_options(mode)).unwrap();
        txn2.commit(buffered_write_options(mode)).unwrap();

        // Assert
        let read_tx = engine
            .begin_tx(cf.id(), cntryl_midge::TransactionMode::ReadOnly)
            .unwrap();
        assert_eq!(
            read_tx.get(b"key1").unwrap(),
            Some(Bytes::from_static(b"value1"))
        );
        assert_eq!(
            read_tx.get(b"key2").unwrap(),
            Some(Bytes::from_static(b"value2"))
        );
    });
}

#[test]
fn should_commit_transaction_given_no_conflicts() {
    for_each_storage_mode(&all_storage_modes_new(), |mode, opts| {
        // Arrange
        let engine = Arc::new(open_with_mode(&opts, mode));
        let cf = engine.create_column_family("test").expect("create cf");

        // Act
        let mut txn = engine
            .begin_tx(cf.id(), cntryl_midge::TransactionMode::ReadWrite)
            .unwrap();
        txn.put(b"key".to_vec(), b"value".to_vec(), None).unwrap();
        let result = txn.commit(buffered_write_options(mode));

        // Assert
        assert!(result.is_ok());
        let read_tx = engine
            .begin_tx(cf.id(), cntryl_midge::TransactionMode::ReadOnly)
            .unwrap();
        assert_eq!(
            read_tx.get(b"key").unwrap(),
            Some(Bytes::from_static(b"value"))
        );
    });
}

#[test]
fn should_commit_transaction_given_concurrent_modifications_to_different_keys() {
    for_each_storage_mode(&all_storage_modes_new(), |mode, opts| {
        // Arrange
        let engine = Arc::new(open_with_mode(&opts, mode));
        let cf = engine.create_column_family("test").expect("create cf");
        let cf_id = cf.id();
        let engine1 = Arc::clone(&engine);
        let engine2 = Arc::clone(&engine);

        // Act
        let write_options = buffered_write_options(mode);
        let handle1 = std::thread::spawn(move || {
            let mut txn = engine1
                .begin_tx(cf_id, cntryl_midge::TransactionMode::ReadWrite)
                .unwrap();
            txn.put(b"key1".to_vec(), b"value1".to_vec(), None).unwrap();
            txn.commit(write_options)
        });

        let write_options = buffered_write_options(mode);
        let handle2 = std::thread::spawn(move || {
            let mut txn = engine2
                .begin_tx(cf_id, cntryl_midge::TransactionMode::ReadWrite)
                .unwrap();
            txn.put(b"key2".to_vec(), b"value2".to_vec(), None).unwrap();
            txn.commit(write_options)
        });

        // Assert
        assert!(handle1.join().unwrap().is_ok());
        assert!(handle2.join().unwrap().is_ok());
        let read_tx = engine
            .begin_tx(cf.id(), cntryl_midge::TransactionMode::ReadOnly)
            .unwrap();
        assert_eq!(
            read_tx.get(b"key1").unwrap(),
            Some(Bytes::from_static(b"value1"))
        );
        assert_eq!(
            read_tx.get(b"key2").unwrap(),
            Some(Bytes::from_static(b"value2"))
        );
    });
}

#[test]
fn should_read_values_within_transaction() {
    for_each_storage_mode(&all_storage_modes_new(), |mode, opts| {
        // Arrange
        let engine = Arc::new(open_with_mode(&opts, mode));
        let cf = engine.create_column_family("test").expect("create cf");
        let mut setup_tx = engine
            .begin_tx(cf.id(), cntryl_midge::TransactionMode::ReadWrite)
            .unwrap();
        setup_tx
            .put(b"key".to_vec(), b"value".to_vec(), None)
            .unwrap();
        setup_tx.commit(buffered_write_options(mode)).unwrap();

        // Act
        let txn = engine
            .begin_tx(cf.id(), cntryl_midge::TransactionMode::ReadWrite)
            .unwrap();
        let value = txn.get(b"key").unwrap();

        // Assert - should read committed value at transaction start
        assert_eq!(value, Some(Bytes::from_static(b"value")));
    });
}

#[test]
fn should_commit_new_key_given_clean_transaction() {
    for_each_storage_mode(&all_storage_modes_new(), |mode, opts| {
        // Arrange
        let engine = Arc::new(open_with_mode(&opts, mode));
        let cf = engine.create_column_family("test").expect("create cf");

        // Act
        let mut txn = engine
            .begin_tx(cf.id(), cntryl_midge::TransactionMode::ReadWrite)
            .unwrap();
        txn.put(b"newkey".to_vec(), b"newvalue".to_vec(), None)
            .unwrap();
        txn.commit(buffered_write_options(mode)).unwrap();

        // Assert
        let read_tx = engine
            .begin_tx(cf.id(), cntryl_midge::TransactionMode::ReadOnly)
            .unwrap();
        assert_eq!(
            read_tx.get(b"newkey").unwrap(),
            Some(Bytes::from_static(b"newvalue"))
        );
    });
}

// ============================================================================
// STRESS TESTS
// ============================================================================

#[test]
fn should_allow_concurrent_writes_to_different_keys() {
    for_each_storage_mode(&all_storage_modes_new(), |mode, opts| {
        // Arrange
        let engine = Arc::new(open_with_mode(&opts, mode));
        let cf = engine.create_column_family("test").expect("create cf");
        let cf_id = cf.id();
        let mut handles = vec![];

        // Act - spawn 10 threads writing different keys
        for i in 0..10 {
            let engine_clone = Arc::clone(&engine);
            let write_options = buffered_write_options(mode);
            let handle = std::thread::spawn(move || {
                let mut txn = engine_clone
                    .begin_tx(cf_id, cntryl_midge::TransactionMode::ReadWrite)
                    .unwrap();
                let key = format!("key{i}");
                let value = format!("value{i}");
                txn.put(key.as_bytes().to_vec(), value.as_bytes().to_vec(), None)
                    .unwrap();
                txn.commit(write_options)
            });
            handles.push(handle);
        }

        // Assert - all commits succeed
        for handle in handles {
            assert!(handle.join().unwrap().is_ok());
        }

        let read_tx = engine
            .begin_tx(cf.id(), cntryl_midge::TransactionMode::ReadOnly)
            .unwrap();
        for i in 0..10 {
            let key = format!("key{i}");
            let expected = format!("value{i}");
            assert_eq!(
                read_tx.get(key.as_bytes()).unwrap(),
                Some(Bytes::from(expected.as_bytes().to_vec()))
            );
        }
    });
}

#[test]
fn should_handle_high_contention_writes_without_panic() {
    for_each_storage_mode(&all_storage_modes_new(), |mode, opts| {
        // Arrange
        let engine = Arc::new(open_with_mode(&opts, mode));
        let cf = engine.create_column_family("test").expect("create cf");
        let cf_id = cf.id();
        let mut handles = vec![];

        // Act - multiple threads writing to same key
        for i in 0..8 {
            let engine_clone = Arc::clone(&engine);
            let write_options = buffered_write_options(mode);
            let handle = std::thread::spawn(move || {
                let mut txn = engine_clone
                    .begin_tx(cf_id, cntryl_midge::TransactionMode::ReadWrite)
                    .unwrap();
                let value = format!("value{i}");
                txn.put(b"hotkey".to_vec(), value.as_bytes().to_vec(), None)
                    .unwrap();
                txn.commit(write_options)
            });
            handles.push(handle);
        }

        // Assert - all commits succeed (LWW semantics)
        for handle in handles {
            assert!(handle.join().unwrap().is_ok());
        }

        // One of the values should win
        let read_tx = engine
            .begin_tx(cf.id(), cntryl_midge::TransactionMode::ReadOnly)
            .unwrap();
        assert!(read_tx.get(b"hotkey").unwrap().is_some());
    });
}

#[test]
fn should_handle_concurrent_read_modify_writes_without_panic() {
    for_each_storage_mode(&all_storage_modes_new(), |mode, opts| {
        // Arrange
        let engine = Arc::new(open_with_mode(&opts, mode));
        let cf = engine.create_column_family("test").expect("create cf");
        let cf_id = cf.id();
        let mut setup_tx = engine
            .begin_tx(cf.id(), cntryl_midge::TransactionMode::ReadWrite)
            .unwrap();
        setup_tx
            .put(b"counter".to_vec(), b"0".to_vec(), None)
            .unwrap();
        setup_tx.commit(buffered_write_options(mode)).unwrap();
        let mut handles = vec![];

        // Act - 10 threads incrementing counter
        for i in 0..10 {
            let engine_clone = Arc::clone(&engine);
            let write_options = buffered_write_options(mode);
            let handle = std::thread::spawn(move || {
                let read_tx = engine_clone
                    .begin_tx(cf_id, cntryl_midge::TransactionMode::ReadOnly)
                    .unwrap();
                let _value = read_tx.get(b"counter").unwrap();
                let mut txn = engine_clone
                    .begin_tx(cf_id, cntryl_midge::TransactionMode::ReadWrite)
                    .unwrap();
                let new_value = format!("{i}");
                txn.put(b"counter".to_vec(), new_value.as_bytes().to_vec(), None)
                    .unwrap();
                txn.commit(write_options)
            });
            handles.push(handle);
        }

        // Assert - all commits succeed
        for handle in handles {
            assert!(handle.join().unwrap().is_ok());
        }
    });
}

#[test]
fn should_handle_high_concurrency_optimistic_locking() {
    for_each_storage_mode(&all_storage_modes_new(), |mode, opts| {
        // Arrange
        let engine = Arc::new(open_with_mode(&opts, mode));
        let cf = engine.create_column_family("test").expect("create cf");
        let cf_id = cf.id();
        let barrier = Arc::new(std::sync::Barrier::new(50));
        let mut handles = vec![];

        // Act - 50 threads performing optimistic lock pattern (read then write)
        for i in 0..50 {
            let engine_clone = Arc::clone(&engine);
            let barrier_clone = Arc::clone(&barrier);
            let write_options = buffered_write_options(mode);
            let handle = std::thread::spawn(move || {
                // Wait for all threads to be ready before starting
                barrier_clone.wait();

                // Optimistic lock pattern: read first
                let read_tx = engine_clone
                    .begin_tx(cf_id, cntryl_midge::TransactionMode::ReadOnly)
                    .unwrap();
                let _current = read_tx.get(b"value").unwrap();

                // Then write in transaction
                let mut txn = engine_clone
                    .begin_tx(cf_id, cntryl_midge::TransactionMode::ReadWrite)
                    .unwrap();
                let write_val = format!("{i}");
                txn.put(b"value".to_vec(), write_val.as_bytes().to_vec(), None)
                    .unwrap();
                txn.commit(write_options)
            });
            handles.push(handle);
        }

        // Assert - all transactions succeed
        for handle in handles {
            assert!(handle.join().unwrap().is_ok());
        }

        // Final value should be one of the writes
        let read_tx = engine
            .begin_tx(cf.id(), cntryl_midge::TransactionMode::ReadOnly)
            .unwrap();
        assert!(read_tx.get(b"value").unwrap().is_some());
    });
}

#[test]
fn should_maintain_transaction_isolation_under_stress() {
    for_each_storage_mode(&all_storage_modes_new(), |mode, opts| {
        // Arrange
        let engine = Arc::new(open_with_mode(&opts, mode));
        let cf = engine.create_column_family("test").expect("create cf");
        let cf_id = cf.id();
        let mut setup_tx = engine
            .begin_tx(cf.id(), cntryl_midge::TransactionMode::ReadWrite)
            .unwrap();
        setup_tx
            .put(b"isolated".to_vec(), b"initial".to_vec(), None)
            .unwrap();
        setup_tx.commit(buffered_write_options(mode)).unwrap();

        // Take a read-only snapshot before any concurrent stress writers start.
        let snapshot_tx = engine
            .begin_tx(cf.id(), cntryl_midge::TransactionMode::ReadOnly)
            .unwrap();

        // Act - many threads hammering the same key with concurrent read-modify-writes
        // while the snapshot transaction above stays open.
        let mut handles = vec![];
        for i in 0..20 {
            let engine_clone = Arc::clone(&engine);
            let write_options = buffered_write_options(mode);
            let handle = std::thread::spawn(move || {
                let read_tx = engine_clone
                    .begin_tx(cf_id, cntryl_midge::TransactionMode::ReadOnly)
                    .unwrap();
                let _current = read_tx.get(b"isolated").unwrap();
                let mut txn = engine_clone
                    .begin_tx(cf_id, cntryl_midge::TransactionMode::ReadWrite)
                    .unwrap();
                let value = format!("stress{i}");
                txn.put(b"isolated".to_vec(), value.as_bytes().to_vec(), None)
                    .unwrap();
                txn.commit(write_options)
            });
            handles.push(handle);
        }

        // Assert - all concurrent commits succeed without panicking
        for handle in handles {
            assert!(handle.join().unwrap().is_ok());
        }

        // The long-lived snapshot must still observe the pre-stress value: isolation
        // means none of the concurrent stress commits are visible to it.
        assert_eq!(
            snapshot_tx.get(b"isolated").unwrap(),
            Some(Bytes::from_static(b"initial")),
            "snapshot transaction leaked a concurrent stress write in mode {mode}"
        );

        // The committed state, on the other hand, must reflect one of the stress
        // writes - proving the stress writers actually raced and mutated the key.
        let read_tx = engine
            .begin_tx(cf.id(), cntryl_midge::TransactionMode::ReadOnly)
            .unwrap();
        let final_value = read_tx.get(b"isolated").unwrap();
        assert_ne!(
            final_value,
            Some(Bytes::from_static(b"initial")),
            "expected concurrent stress writers to update the key in mode {mode}"
        );
        assert!(final_value.is_some());
    });
}

// ============================================================================
// RECOVERY TESTS (FS + CLOUD ONLY)
// ============================================================================

#[test]
fn should_recover_conflict_state_after_engine_restart() {
    for_each_storage_mode(&["local", "cloud"], |mode, opts| {
        // Arrange
        // Act (Phase 1) - create conflicts and commit
        {
            let mut engine = open_with_mode(&opts, mode);
            let cf = engine.create_column_family("test").expect("create cf");

            // Create conflicting transactions where last-write wins
            let mut txn1 = engine
                .begin_tx(cf.id(), cntryl_midge::TransactionMode::ReadWrite)
                .unwrap();
            txn1.put(b"conflict_key".to_vec(), b"value1".to_vec(), None)
                .unwrap();
            txn1.commit(buffered_write_options(mode)).unwrap();

            let mut txn2 = engine
                .begin_tx(cf.id(), cntryl_midge::TransactionMode::ReadWrite)
                .unwrap();
            txn2.put(b"conflict_key".to_vec(), b"value2".to_vec(), None)
                .unwrap();
            txn2.commit(buffered_write_options(mode)).unwrap();
            engine
                .shutdown(Duration::from_secs(5))
                .expect("shutdown before restart");
        }

        // Assert (Phase 2) - restart and verify
        {
            let engine = open_with_mode(&opts, mode);
            let cf = engine.get_column_family("test").expect("get cf");

            // Assert - last written value persists
            let read_tx = engine
                .begin_tx(cf.id(), cntryl_midge::TransactionMode::ReadOnly)
                .unwrap();
            let value = read_tx.get(b"conflict_key").unwrap();
            assert_eq!(value, Some(Bytes::from_static(b"value2")));
        }
    });
}

#[test]
fn should_persist_lost_update_prevention_after_restart() {
    for_each_storage_mode(&["local", "cloud"], |mode, opts| {
        // Arrange
        // Act (Phase 1) - set up concurrent updates
        {
            let mut engine = open_with_mode(&opts, mode);
            let cf = engine.create_column_family("test").expect("create cf");

            // Initial value
            let mut setup_tx = engine
                .begin_tx(cf.id(), cntryl_midge::TransactionMode::ReadWrite)
                .unwrap();
            setup_tx
                .put(b"counter".to_vec(), b"0".to_vec(), None)
                .unwrap();
            setup_tx.commit(buffered_write_options(mode)).unwrap();

            // Two transactions attempt concurrent increment
            let mut txn1 = engine
                .begin_tx(cf.id(), cntryl_midge::TransactionMode::ReadWrite)
                .unwrap();
            txn1.put(b"counter".to_vec(), b"1".to_vec(), None).unwrap();
            txn1.commit(buffered_write_options(mode)).unwrap();

            let mut txn2 = engine
                .begin_tx(cf.id(), cntryl_midge::TransactionMode::ReadWrite)
                .unwrap();
            txn2.put(b"counter".to_vec(), b"2".to_vec(), None).unwrap();
            txn2.commit(buffered_write_options(mode)).unwrap();
            engine
                .shutdown(Duration::from_secs(5))
                .expect("shutdown before restart");
        }

        // Assert (Phase 2) - restart and verify
        {
            let engine = open_with_mode(&opts, mode);
            let cf = engine.get_column_family("test").expect("get cf");

            // Assert - last written value (2) persists
            let read_tx = engine
                .begin_tx(cf.id(), cntryl_midge::TransactionMode::ReadOnly)
                .unwrap();
            let value = read_tx.get(b"counter").unwrap();
            assert_eq!(value, Some(Bytes::from_static(b"2")));
        }
    });
}
// ============================================================================
// BASELINE CONFLICT PREVENTION (Negative Tests)
// ============================================================================

#[test]
fn should_preserve_both_writes_when_non_overlapping_keys_given_concurrent_commits() {
    for_each_storage_mode(&all_storage_modes_new(), |mode, opts| {
        // Verify that non-conflicting concurrent writes are both visible
        // Arrange
        let engine = Arc::new(open_with_mode(&opts, mode));
        let cf = engine.create_column_family("test").expect("create cf");

        // Pre-populate
        let mut setup_tx = engine
            .begin_tx(cf.id(), cntryl_midge::TransactionMode::ReadWrite)
            .unwrap();
        setup_tx
            .put(b"key1".to_vec(), b"old1".to_vec(), None)
            .unwrap();
        setup_tx
            .put(b"key2".to_vec(), b"old2".to_vec(), None)
            .unwrap();
        setup_tx.commit(buffered_write_options(mode)).unwrap();

        // Act: Two concurrent updates to different keys
        let mut txn1 = engine
            .begin_tx(cf.id(), cntryl_midge::TransactionMode::ReadWrite)
            .unwrap();
        let mut txn2 = engine
            .begin_tx(cf.id(), cntryl_midge::TransactionMode::ReadWrite)
            .unwrap();

        txn1.put(b"key1".to_vec(), b"new1".to_vec(), None).unwrap();
        txn2.put(b"key2".to_vec(), b"new2".to_vec(), None).unwrap();

        txn1.commit(buffered_write_options(mode))
            .expect("commit first disjoint update");
        txn2.commit(buffered_write_options(mode))
            .expect("commit second disjoint update");

        // Assert: Both updates must be visible
        let read_tx = engine
            .begin_tx(cf.id(), cntryl_midge::TransactionMode::ReadOnly)
            .unwrap();
        let v1 = read_tx.get(b"key1").unwrap();
        let v2 = read_tx.get(b"key2").unwrap();

        assert_eq!(
            v1,
            Some(Bytes::from_static(b"new1")),
            "key1 update lost in {mode}"
        );
        assert_eq!(
            v2,
            Some(Bytes::from_static(b"new2")),
            "key2 update lost in {mode}"
        );
    });
}
