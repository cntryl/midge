//! Trust-critical smoke suite for early external evaluation.
//!
//! These tests are intentionally small and local-disk only. They are the
//! minimum gate for saying Midge is "safe enough to try" in a controlled
//! environment.

use bytes::Bytes;
mod common;
use cntryl_midge::{Query, TransactionMode, WriteOptions};
use common::*;
use std::sync::{Mutex, OnceLock};
use std::time::Duration;

#[test]
fn should_recover_wal_backed_commit_when_reopening_local_engine() {
    // Arrange
    let _guard = lock_smoke_tests();
    let opts = opts_for_mode("local");

    // Act
    {
        let mut engine = open_with_mode(&opts, "local");
        let cf = engine.create_column_family("smoke").expect("create cf");
        let mut tx = engine
            .begin_tx(cf.id(), TransactionMode::ReadWrite)
            .expect("begin write tx");
        tx.put(b"wal-key".to_vec(), b"wal-value".to_vec(), None)
            .expect("put wal value");
        tx.commit(WriteOptions::sync()).expect("sync commit");
        engine
            .shutdown(Duration::from_secs(2))
            .expect("shutdown before reopen");
    }

    let engine = open_with_mode(&opts, "local");
    let cf = engine.get_column_family("smoke").expect("get smoke cf");
    let tx = engine
        .begin_tx(cf.id(), TransactionMode::ReadOnly)
        .expect("begin read tx");

    // Assert
    assert_eq!(
        tx.get(b"wal-key").expect("get wal key"),
        Some(Bytes::from_static(b"wal-value"))
    );
}

#[cfg(feature = "failpoints")]
#[test]
fn should_keep_data_recoverable_when_flush_publication_is_interrupted() {
    // Arrange
    let _guard = lock_smoke_tests();
    let temp_dir = tempfile::TempDir::new().expect("temp dir");
    let db_path = temp_dir.path();
    let mut engine = cntryl_midge::Engine::open(
        cntryl_midge::OpenOptions::local(db_path)
            .build()
            .expect("build options"),
    )
    .expect("open engine");
    let cf = engine.get_column_family("default").expect("default cf");

    for index in 0..8 {
        let key = format!("flush-{index:02}");
        let value = format!("value-{index:02}");
        let mut tx = engine
            .begin_tx(cf.id(), TransactionMode::ReadWrite)
            .expect("begin write tx");
        tx.put(key.into_bytes(), value.into_bytes(), None)
            .expect("put flush seed");
        tx.commit(WriteOptions::sync()).expect("commit flush seed");
    }

    // Act
    let scenario = fail::FailScenario::setup();
    fail::cfg("midge::manifest::inject_no_space_on_add_sst_edit", "return")
        .expect("configure manifest append failpoint");
    let flush_error = engine
        .flush_cf(&cf)
        .expect_err("flush should fail before publish");
    fail::remove("midge::manifest::inject_no_space_on_add_sst_edit");
    scenario.teardown();
    assert!(
        flush_error
            .to_string()
            .to_ascii_lowercase()
            .contains("space"),
        "unexpected flush error: {flush_error}"
    );
    engine
        .shutdown(Duration::from_secs(2))
        .expect("shutdown before reopen");

    let reopened = cntryl_midge::Engine::open(
        cntryl_midge::OpenOptions::local(db_path)
            .build()
            .expect("build options"),
    )
    .expect("reopen engine");
    let cf = reopened.get_column_family("default").expect("default cf");
    let tx = reopened
        .begin_tx(cf.id(), TransactionMode::ReadOnly)
        .expect("begin read tx");

    // Assert
    assert_eq!(
        tx.get(b"flush-00").expect("get recovered key"),
        Some(Bytes::from_static(b"value-00"))
    );
}

#[cfg(feature = "failpoints")]
#[test]
fn should_keep_compacted_data_visible_when_compaction_crashes_before_publish() {
    // Arrange
    let _guard = lock_smoke_tests();
    let temp_dir = tempfile::TempDir::new().expect("temp dir");
    let db_path = temp_dir.path();
    let mut engine = cntryl_midge::Engine::open(
        cntryl_midge::OpenOptions::local(db_path)
            .build()
            .expect("build options"),
    )
    .expect("open engine");
    let cf = engine.get_column_family("default").expect("default cf");

    for batch in 0..4 {
        for index in 0..20 {
            let key = format!("cmp-b{batch}-k{index:02}");
            let value = format!("value-{batch}-{index}");
            let mut tx = engine
                .begin_tx(cf.id(), TransactionMode::ReadWrite)
                .expect("begin write tx");
            tx.put(key.into_bytes(), value.into_bytes(), None)
                .expect("put compaction seed");
            tx.commit(WriteOptions::sync())
                .expect("commit compaction seed");
        }
        engine.flush_cf(&cf).expect("flush compaction seed");
    }

    // Act
    let scenario = fail::FailScenario::setup();
    fail::cfg(
        "midge::manifest::inject_no_space_on_compaction_batch_edit",
        "return",
    )
    .expect("configure compaction publish failpoint");
    engine
        .compact_all()
        .expect_err("compact_all must report the injected publication failure");
    fail::remove("midge::manifest::inject_no_space_on_compaction_batch_edit");
    scenario.teardown();
    engine
        .shutdown(Duration::from_secs(2))
        .expect("shutdown before reopen");

    let reopened = cntryl_midge::Engine::open(
        cntryl_midge::OpenOptions::local(db_path)
            .build()
            .expect("build options"),
    )
    .expect("reopen engine");
    let cf = reopened.get_column_family("default").expect("default cf");
    let tx = reopened
        .begin_tx(cf.id(), TransactionMode::ReadOnly)
        .expect("begin read tx");

    // Assert
    assert_eq!(
        tx.get(b"cmp-b0-k00").expect("get oldest compacted key"),
        Some(Bytes::from_static(b"value-0-0"))
    );
    assert_eq!(
        tx.get(b"cmp-b3-k19").expect("get newest compacted key"),
        Some(Bytes::from_static(b"value-3-19"))
    );
}

#[test]
fn should_filter_point_delete_tombstones_during_cross_sst_iteration() {
    // Arrange
    let _guard = lock_smoke_tests();
    let engine = open_with_mode(&opts_for_mode("local"), "local");
    let cf = engine.create_column_family("smoke").expect("create cf");

    for batch in 0..2 {
        for i in 0..20 {
            let key = format!("k{:03}", batch * 20 + i);
            let value = format!("v{:03}", batch * 20 + i);
            let mut tx = engine
                .begin_tx(cf.id(), TransactionMode::ReadWrite)
                .expect("begin write tx");
            tx.put(key.into_bytes(), value.into_bytes(), None)
                .expect("put iterator seed");
            tx.commit(WriteOptions::buffered())
                .expect("commit iterator seed");
        }
        engine.flush_cf(&cf).expect("flush iterator batch");
    }

    // Act
    let mut tx = engine
        .begin_tx(cf.id(), TransactionMode::ReadWrite)
        .expect("begin delete tx");
    for deleted in [b"k010", b"k011", b"k024"] {
        tx.delete(deleted.to_vec()).expect("delete iterator key");
    }
    tx.commit(WriteOptions::buffered())
        .expect("commit iterator deletes");
    engine.flush_cf(&cf).expect("flush iterator tombstones");

    let tx = engine
        .begin_tx(cf.id(), TransactionMode::ReadOnly)
        .expect("begin read tx");
    let results = tx
        .scan(&Query::new())
        .expect("scan")
        .try_collect()
        .expect("collect scan");

    // Assert
    for deleted in [b"k010", b"k011", b"k024"] {
        assert!(
            !results.iter().any(|(k, _)| k.as_ref() == deleted),
            "deleted key {:?} should stay hidden",
            String::from_utf8_lossy(deleted)
        );
    }
    assert!(results.iter().any(|(k, _)| k.as_ref() == b"k000"));
    assert!(results.iter().any(|(k, _)| k.as_ref() == b"k039"));
}

fn smoke_test_lock() -> &'static Mutex<()> {
    static LOCK: OnceLock<Mutex<()>> = OnceLock::new();
    LOCK.get_or_init(|| Mutex::new(()))
}

fn lock_smoke_tests() -> std::sync::MutexGuard<'static, ()> {
    match smoke_test_lock().lock() {
        Ok(guard) => guard,
        Err(poisoned) => poisoned.into_inner(),
    }
}
