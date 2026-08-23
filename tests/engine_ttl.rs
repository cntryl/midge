//! Integration tests for TTL (Time-To-Live) support

use std::sync::atomic::{AtomicU64, Ordering};
use std::sync::Arc;
use std::thread;
use std::time::Duration;
use std::{fs, path::Path};

use bytes::Bytes;
mod common;
use cntryl_midge::common::time::Clock;
use cntryl_midge::{MemoryBudget, MidgeEngine, OpenOptions, Query, TransactionMode, WriteOptions};
use common::*;

#[derive(Debug)]
struct ManualClock(AtomicU64);

impl ManualClock {
    fn new(now: u64) -> Self {
        Self(AtomicU64::new(now))
    }

    fn set(&self, now: u64) {
        self.0.store(now, Ordering::Release);
    }
}

impl Clock for ManualClock {
    fn now_millis(&self) -> u64 {
        self.0.load(Ordering::Acquire)
    }
}

fn open_with_clock(clock: Arc<ManualClock>) -> MidgeEngine {
    MidgeEngine::open(
        OpenOptions::in_memory()
            .ttl_clock(clock)
            .build()
            .expect("build options"),
    )
    .expect("open engine")
}

#[test]
fn should_not_reexpose_expired_key_given_clock_steps_backward_when_reading() {
    // Arrange
    let clock = Arc::new(ManualClock::new(10_000));
    let engine = open_with_clock(Arc::clone(&clock));
    let mut write = engine
        .begin_tx(0, TransactionMode::ReadWrite)
        .expect("begin write");
    write
        .put(b"key".to_vec(), b"value".to_vec(), Some(1))
        .expect("put");
    write.commit(WriteOptions::buffered()).expect("commit");
    clock.set(11_000);
    assert_eq!(
        engine
            .begin_tx(0, TransactionMode::ReadOnly)
            .expect("read expired")
            .get(b"key")
            .expect("get"),
        None
    );

    // Act
    clock.set(10_500);
    let value = engine
        .begin_tx(0, TransactionMode::ReadOnly)
        .expect("read after skew")
        .get(b"key")
        .expect("get");

    // Assert
    assert_eq!(value, None);
}

#[test]
fn should_use_one_commit_time_given_multiple_ttl_puts_when_committing() {
    // Arrange
    let clock = Arc::new(ManualClock::new(20_000));
    let engine = open_with_clock(Arc::clone(&clock));
    let mut transaction = engine
        .begin_tx(0, TransactionMode::ReadWrite)
        .expect("begin write");
    transaction
        .put(b"a".to_vec(), b"a".to_vec(), Some(1))
        .expect("put a");
    transaction
        .put(b"b".to_vec(), b"b".to_vec(), Some(1))
        .expect("put b");

    // Act
    transaction
        .commit(WriteOptions::buffered())
        .expect("commit");
    clock.set(21_000);
    let read = engine
        .begin_tx(0, TransactionMode::ReadOnly)
        .expect("begin read");

    // Assert
    assert_eq!(read.get(b"a").expect("get a"), None);
    assert_eq!(read.get(b"b").expect("get b"), None);
}

#[test]
fn should_keep_pending_ttl_visible_given_read_your_own_write_before_commit() {
    // Arrange
    let clock = Arc::new(ManualClock::new(30_000));
    let engine = open_with_clock(Arc::clone(&clock));
    let mut transaction = engine
        .begin_tx(0, TransactionMode::ReadWrite)
        .expect("begin write");
    transaction
        .put(b"key".to_vec(), b"value".to_vec(), Some(1))
        .expect("put");

    // Act
    clock.set(40_000);
    let value = transaction.get(b"key").expect("read own write");

    // Assert
    assert_eq!(value, Some(Bytes::from_static(b"value")));
}

#[test]
fn should_use_fixed_snapshot_time_given_range_scan_crosses_ttl_boundary() {
    // Arrange
    let clock = Arc::new(ManualClock::new(50_000));
    let engine = open_with_clock(Arc::clone(&clock));
    let mut write = engine
        .begin_tx(0, TransactionMode::ReadWrite)
        .expect("begin write");
    write
        .put(b"short".to_vec(), b"value".to_vec(), Some(1))
        .expect("put short");
    write
        .put(b"stable".to_vec(), b"value".to_vec(), None)
        .expect("put stable");
    write.commit(WriteOptions::buffered()).expect("commit");
    clock.set(50_999);
    let read = engine
        .begin_tx(0, TransactionMode::ReadOnly)
        .expect("begin snapshot");

    // Act
    clock.set(51_001);
    let entries = read
        .scan(&Query::new())
        .expect("scan")
        .try_collect()
        .expect("collect scan");

    // Assert
    assert_eq!(entries.len(), 2);
}

#[test]
fn should_match_expiration_given_resident_and_spilled_transaction_paths() {
    // Arrange
    let clock = Arc::new(ManualClock::new(60_000));
    let resident = open_with_clock(Arc::clone(&clock));
    let spill_dir = tempfile::TempDir::new().expect("spill directory");
    let spilled = MidgeEngine::open(
        OpenOptions::local(spill_dir.path())
            .memory_budget(MemoryBudget::Bytes(128 * 1024))
            .ttl_clock(clock.clone())
            .build()
            .expect("build spill options"),
    )
    .expect("open spill engine");
    for (engine, count) in [(&resident, 1), (&spilled, 100)] {
        let mut transaction = engine
            .begin_tx(0, TransactionMode::ReadWrite)
            .expect("begin write");
        for index in 0..count {
            transaction
                .put(
                    format!("key-{index:03}").into_bytes(),
                    vec![b'x'; 1024],
                    Some(1),
                )
                .expect("put");
        }
        transaction
            .commit(WriteOptions::buffered())
            .expect("commit");
    }

    // Act
    clock.set(61_000);
    let resident_value = resident
        .begin_tx(0, TransactionMode::ReadOnly)
        .expect("resident read")
        .get(b"key-000")
        .expect("resident get");
    let spilled_value = spilled
        .begin_tx(0, TransactionMode::ReadOnly)
        .expect("spilled read")
        .get(b"key-000")
        .expect("spilled get");

    // Assert
    assert_eq!(resident_value, None);
    assert_eq!(spilled_value, None);
}

#[test]
fn should_preserve_maximum_expiration_given_resident_spilled_transactions() {
    // Arrange
    let clock = Arc::new(ManualClock::new(u64::MAX - 500));
    let resident = open_with_clock(Arc::clone(&clock));
    let spill_dir = tempfile::TempDir::new().expect("spill directory");
    let spilled = MidgeEngine::open(
        OpenOptions::local(spill_dir.path())
            .memory_budget(MemoryBudget::Bytes(64 * 1024 * 1024))
            .transaction_memory_pool_size(32 * 1024)
            .ttl_clock(clock.clone())
            .build()
            .expect("build spill options"),
    )
    .expect("open spill engine");
    let mut resident_write = resident
        .begin_tx(0, TransactionMode::ReadWrite)
        .expect("begin resident write");
    resident_write
        .put(b"max-ttl".to_vec(), b"resident".to_vec(), Some(1))
        .expect("put resident TTL");
    resident_write
        .put(b"no-ttl".to_vec(), b"resident-stable".to_vec(), None)
        .expect("put resident control");
    resident_write
        .commit(WriteOptions::sync())
        .expect("commit resident write");

    let mut spilled_write = spilled
        .begin_tx(0, TransactionMode::ReadWrite)
        .expect("begin spilled write");
    spilled_write
        .put(b"max-ttl".to_vec(), vec![b't'; 4096], Some(1))
        .expect("put spilled TTL");
    spilled_write
        .put(b"no-ttl".to_vec(), b"spilled-stable".to_vec(), None)
        .expect("put spilled control");
    for index in 0..64 {
        spilled_write
            .put(
                format!("padding-{index:03}").into_bytes(),
                vec![b'p'; 4096],
                None,
            )
            .expect("put spill pressure");
    }
    assert!(contains_spill_run(spill_dir.path()));
    spilled_write
        .commit(WriteOptions::sync())
        .expect("commit spilled write");

    // Act
    clock.set(u64::MAX);
    let resident_read = resident
        .begin_tx(0, TransactionMode::ReadOnly)
        .expect("begin resident read");
    let spilled_read = spilled
        .begin_tx(0, TransactionMode::ReadOnly)
        .expect("begin spilled read");

    // Assert
    assert_eq!(
        resident_read.get(b"max-ttl").expect("resident TTL read"),
        None
    );
    assert_eq!(
        spilled_read.get(b"max-ttl").expect("spilled TTL read"),
        None
    );
    assert_eq!(
        resident_read.get(b"no-ttl").expect("resident control read"),
        Some(Bytes::from_static(b"resident-stable"))
    );
    assert_eq!(
        spilled_read.get(b"no-ttl").expect("spilled control read"),
        Some(Bytes::from_static(b"spilled-stable"))
    );
}

fn contains_spill_run(path: &Path) -> bool {
    fs::read_dir(path).is_ok_and(|entries| {
        entries.filter_map(Result::ok).any(|entry| {
            let path = entry.path();
            if path.is_dir() {
                contains_spill_run(&path)
            } else {
                path.extension().is_some_and(|extension| extension == "run")
            }
        })
    })
}

#[test]
fn should_round_trip_maximum_expiration_value_given_sst_flush_when_ttl_saturates_to_u64_max() {
    // Arrange
    let directory = tempfile::TempDir::new().expect("database directory");
    let clock = Arc::new(ManualClock::new(u64::MAX - 500));
    let mut engine = MidgeEngine::open(
        OpenOptions::local(directory.path())
            .ttl_clock(clock.clone())
            .background_compaction(false)
            .build()
            .expect("build options"),
    )
    .expect("open engine");
    let cf = engine.create_column_family("max-ttl").expect("create CF");
    for index in 0..4 {
        let mut write = engine
            .begin_tx(cf.id(), TransactionMode::ReadWrite)
            .expect("begin write");
        write
            .put(
                if index == 0 {
                    b"max-ttl".to_vec()
                } else {
                    format!("padding-{index}").into_bytes()
                },
                b"value".to_vec(),
                (index == 0).then_some(1),
            )
            .expect("put TTL generation");
        if index == 0 {
            write
                .put(b"no-ttl".to_vec(), b"stable".to_vec(), None)
                .expect("put control");
        }
        write.commit(WriteOptions::sync()).expect("commit");
        engine.flush_cf(&cf).expect("flush V4 SST");
    }
    engine.compact_all().expect("compact V4 SSTs");
    clock.set(u64::MAX);
    let before_restart = engine
        .begin_tx(cf.id(), TransactionMode::ReadOnly)
        .expect("begin pre-restart read");
    assert_eq!(before_restart.get(b"max-ttl").expect("read TTL"), None);
    assert_eq!(
        before_restart.get(b"no-ttl").expect("read control"),
        Some(Bytes::from_static(b"stable"))
    );
    drop(before_restart);
    engine.shutdown(Duration::from_secs(5)).expect("shutdown");

    // Act
    let reopened = MidgeEngine::open(
        OpenOptions::local(directory.path())
            .ttl_clock(clock)
            .background_compaction(false)
            .build()
            .expect("build reopen options"),
    )
    .expect("reopen engine");
    let reopened_cf = reopened.get_column_family("max-ttl").expect("reopen CF");
    let after_restart = reopened
        .begin_tx(reopened_cf.id(), TransactionMode::ReadOnly)
        .expect("begin reopened read");

    // Assert
    assert_eq!(after_restart.get(b"max-ttl").expect("reopened TTL"), None);
    assert_eq!(
        after_restart.get(b"no-ttl").expect("reopened control"),
        Some(Bytes::from_static(b"stable"))
    );
}

#[test]
fn should_preserve_raw_ttl_value_given_forward_skew_during_flush_and_compaction() {
    // Arrange
    let directory = tempfile::TempDir::new().expect("database directory");
    let clock = Arc::new(ManualClock::new(1_000));
    let mut engine = MidgeEngine::open(
        OpenOptions::local(directory.path())
            .ttl_clock(clock.clone())
            .build()
            .expect("build options"),
    )
    .expect("open engine");
    let cf = engine.create_column_family("ttl").expect("create cf");
    for index in 0..4 {
        let mut transaction = engine
            .begin_tx(cf.id(), TransactionMode::ReadWrite)
            .expect("begin write");
        transaction
            .put(
                if index == 0 {
                    b"ttl-key".to_vec()
                } else {
                    format!("padding-{index}").into_bytes()
                },
                b"value".to_vec(),
                (index == 0).then_some(100),
            )
            .expect("put");
        transaction
            .commit(WriteOptions::buffered())
            .expect("commit");
        engine.flush_cf(&cf).expect("flush");
    }
    clock.set(200_000);

    // Act
    engine.compact_all().expect("compact");
    engine.shutdown(Duration::from_secs(5)).expect("shutdown");
    clock.set(50_000);
    let reopened = MidgeEngine::open(
        OpenOptions::local(directory.path())
            .ttl_clock(clock)
            .build()
            .expect("build reopen options"),
    )
    .expect("reopen engine");
    let reopened_cf = reopened.get_column_family("ttl").expect("reopened cf");
    let value = reopened
        .begin_tx(reopened_cf.id(), TransactionMode::ReadOnly)
        .expect("begin read")
        .get(b"ttl-key")
        .expect("get");

    // Assert
    assert_eq!(value, Some(Bytes::from_static(b"value")));
}

// ============================================================================
// Basic TTL Behavior
// ============================================================================

#[test]
fn should_return_value_given_ttl_not_elapsed_when_reading() {
    for_each_storage_mode(&all_storage_modes_new(), |mode, opts| {
        // Arrange
        let engine = Arc::new(open_with_mode(&opts, mode));
        let cf = engine.create_column_family("test").expect("create cf");
        let mut tx = engine
            .begin_tx(cf.id(), TransactionMode::ReadWrite)
            .unwrap();
        tx.put(b"key1".to_vec(), b"value1".to_vec(), Some(3600))
            .unwrap(); // 1 hour TTL
        tx.commit(buffered_write_options(mode)).unwrap();

        // Act
        let read_tx = engine.begin_tx(cf.id(), TransactionMode::ReadOnly).unwrap();
        let result = read_tx.get(b"key1").unwrap();

        // Assert
        assert_eq!(result, Some(Bytes::from_static(b"value1")));
    });
}

#[test]
fn should_return_none_given_ttl_elapsed_when_reading() {
    for_each_storage_mode(&all_storage_modes_new(), |mode, opts| {
        // Arrange
        let engine = Arc::new(open_with_mode(&opts, mode));
        let cf = engine.create_column_family("test").expect("create cf");
        let mut tx = engine
            .begin_tx(cf.id(), TransactionMode::ReadWrite)
            .unwrap();
        tx.put(b"key1".to_vec(), b"value1".to_vec(), Some(1))
            .unwrap(); // 1 second TTL
        tx.commit(buffered_write_options(mode)).unwrap();

        // Act
        thread::sleep(Duration::from_millis(1100)); // Wait for expiration
        let read_tx = engine.begin_tx(cf.id(), TransactionMode::ReadOnly).unwrap();
        let result = read_tx.get(b"key1").unwrap();

        // Assert
        assert_eq!(result, None);
    });
}

#[test]
fn should_not_expire_key_given_zero_ttl_when_zero_means_infinite() {
    for_each_storage_mode(&all_storage_modes_new(), |mode, opts| {
        // Arrange
        let engine = Arc::new(open_with_mode(&opts, mode));
        let cf = engine.create_column_family("test").expect("create cf");
        let mut tx = engine
            .begin_tx(cf.id(), TransactionMode::ReadWrite)
            .unwrap();
        tx.put(b"key1".to_vec(), b"value1".to_vec(), Some(0))
            .unwrap(); // 0 = no expiration (infinite)
        tx.commit(buffered_write_options(mode)).unwrap();

        // Act
        thread::sleep(Duration::from_millis(100));
        let read_tx = engine.begin_tx(cf.id(), TransactionMode::ReadOnly).unwrap();
        let result = read_tx.get(b"key1").unwrap();

        // Assert - TTL of 0 means never expires
        assert_eq!(result, Some(Bytes::from_static(b"value1")));
    });
}

// ============================================================================
// Persistence & Recovery
// ============================================================================

#[test]
fn should_persist_ttl_metadata_given_restart_when_reopening() {
    for_each_storage_mode(&["local", "cloud"], |mode, opts| {
        // Arrange
        // Act (Phase 1)
        {
            let mut engine = open_with_mode(&opts, mode);
            let cf = engine.create_column_family("test").expect("create cf");
            let mut tx = engine
                .begin_tx(cf.id(), TransactionMode::ReadWrite)
                .unwrap();
            tx.put(b"key1".to_vec(), b"value1".to_vec(), Some(3600))
                .unwrap(); // 1 hour
            tx.commit(buffered_write_options(mode)).unwrap();
            engine
                .shutdown(Duration::from_secs(5))
                .expect("shutdown before reopen");
        }

        // Assert (Phase 2)
        {
            let engine = open_with_mode(&opts, mode);
            let cf = engine.create_column_family("test").expect("create cf");
            let read_tx = engine.begin_tx(cf.id(), TransactionMode::ReadOnly).unwrap();
            let result = read_tx.get(b"key1").unwrap();
            assert_eq!(result, Some(Bytes::from_static(b"value1")));
        }
    });
}

#[test]
fn should_persist_ttl_metadata_given_flush_and_restart_when_reopening() {
    for_each_storage_mode(&["local", "cloud"], |mode, opts| {
        // Arrange
        {
            let mut engine = open_with_mode(&opts, mode);
            let cf = engine.create_column_family("test").expect("create cf");
            let mut tx = engine
                .begin_tx(cf.id(), TransactionMode::ReadWrite)
                .unwrap();
            tx.put(b"key1".to_vec(), b"value1".to_vec(), Some(3600))
                .unwrap();
            tx.commit(buffered_write_options(mode)).unwrap();
            engine.flush_cf(&cf).unwrap();
            engine
                .shutdown(Duration::from_secs(5))
                .expect("shutdown before reopen");
        }

        // Act
        {
            let engine = open_with_mode(&opts, mode);
            let cf = engine
                .get_column_family("test")
                .unwrap_or_else(|| engine.create_column_family("test").expect("create cf"));
            let read_tx = engine.begin_tx(cf.id(), TransactionMode::ReadOnly).unwrap();
            let result = read_tx.get(b"key1").unwrap();

            // Assert
            assert_eq!(result, Some(Bytes::from_static(b"value1")));
        }
    });
}

#[test]
fn should_expire_after_restart_given_ttl_elapsed_during_shutdown_when_reopening() {
    for_each_storage_mode(&["local", "cloud"], |mode, opts| {
        // Arrange
        // Act (Phase 1)
        {
            let mut engine = open_with_mode(&opts, mode);
            let cf = engine
                .get_column_family("test")
                .unwrap_or_else(|| engine.create_column_family("test").expect("create cf"));
            let mut tx = engine
                .begin_tx(cf.id(), TransactionMode::ReadWrite)
                .unwrap();
            tx.put(b"key1".to_vec(), b"value1".to_vec(), Some(1))
                .unwrap(); // 1 second
            tx.commit(buffered_write_options(mode)).unwrap();
            thread::sleep(Duration::from_millis(1100)); // Wait for expiration
            engine
                .shutdown(Duration::from_secs(5))
                .expect("shutdown before reopen");
        }

        // Assert (Phase 2)
        {
            let engine = open_with_mode(&opts, mode);
            let cf = engine
                .get_column_family("test")
                .unwrap_or_else(|| engine.create_column_family("test").expect("create cf"));
            let read_tx = engine.begin_tx(cf.id(), TransactionMode::ReadOnly).unwrap();
            let result = read_tx.get(b"key1").unwrap();
            assert_eq!(result, None);
        }
    });
}

// ============================================================================
// Compaction Interaction
// ============================================================================

#[test]
fn should_remove_expired_entries_given_compaction_when_ttl_exceeded() {
    for_each_storage_mode(&all_storage_modes_new(), |mode, opts| {
        // Arrange: four separate L0 generations so `compact_all()` crosses
        // the default L0 file-count trigger and performs a real merge,
        // rather than flush_cf() alone (the test name promises compaction
        // specifically). The TTL'd key sits alongside unrelated filler
        // keys so a genuine multi-file merge is required to resolve it.
        let engine = Arc::new(open_with_mode(&opts, mode));
        let cf = engine.create_column_family("test").expect("create cf");
        for batch in 0..3 {
            let mut tx = engine
                .begin_tx(cf.id(), TransactionMode::ReadWrite)
                .unwrap();
            let key = format!("compaction_ttl_filler_{batch}");
            tx.put(key.into_bytes(), b"filler".to_vec(), None).unwrap();
            tx.commit(buffered_write_options(mode)).unwrap();
            engine.flush_cf(&cf).unwrap();
        }
        let mut tx = engine
            .begin_tx(cf.id(), TransactionMode::ReadWrite)
            .unwrap();
        tx.put(b"key1".to_vec(), b"value1".to_vec(), Some(1))
            .unwrap(); // 1 second
        tx.commit(buffered_write_options(mode)).unwrap();
        engine.flush_cf(&cf).unwrap();
        thread::sleep(Duration::from_millis(1100));

        // Act - trigger a real compaction (not just a flush)
        engine.compact_all().unwrap();

        // Assert - expired entry should be removed, filler keys survive
        let read_tx = engine.begin_tx(cf.id(), TransactionMode::ReadOnly).unwrap();
        let result = read_tx.get(b"key1").unwrap();
        assert_eq!(
            result, None,
            "TTL-expired entry should be dropped by compaction in mode: {mode}"
        );
        for batch in 0..3 {
            let key = format!("compaction_ttl_filler_{batch}");
            assert!(
                read_tx.get(key.as_bytes()).unwrap().is_some(),
                "unrelated key {key} should survive compaction in mode: {mode}"
            );
        }
    });
}

#[test]
fn should_preserve_non_expired_entries_given_compaction_when_ttl_not_exceeded() {
    for_each_storage_mode(&all_storage_modes_new(), |mode, opts| {
        // Arrange: same reasoning as the sibling expiry test — four L0
        // generations so `compact_all()` performs a genuine merge instead
        // of a silent no-op.
        let engine = Arc::new(open_with_mode(&opts, mode));
        let cf = engine.create_column_family("test").expect("create cf");
        for batch in 0..3 {
            let mut tx = engine
                .begin_tx(cf.id(), TransactionMode::ReadWrite)
                .unwrap();
            let key = format!("compaction_ttl_filler_{batch}");
            tx.put(key.into_bytes(), b"filler".to_vec(), None).unwrap();
            tx.commit(buffered_write_options(mode)).unwrap();
            engine.flush_cf(&cf).unwrap();
        }
        let mut tx = engine
            .begin_tx(cf.id(), TransactionMode::ReadWrite)
            .unwrap();
        tx.put(b"key1".to_vec(), b"value1".to_vec(), Some(3600))
            .unwrap(); // 1 hour
        tx.commit(buffered_write_options(mode)).unwrap();
        engine.flush_cf(&cf).unwrap();

        // Act - trigger a real compaction (not just a flush)
        engine.compact_all().unwrap();

        // Assert - non-expired entry preserved
        let read_tx = engine.begin_tx(cf.id(), TransactionMode::ReadOnly).unwrap();
        let result = read_tx.get(b"key1").unwrap();
        assert_eq!(
            result,
            Some(Bytes::from_static(b"value1")),
            "non-expired entry should survive compaction in mode: {mode}"
        );
    });
}

// ============================================================================
// Mixed TTL Keys
// ============================================================================

#[test]
fn should_handle_mixed_ttl_keys_given_some_expire_when_reading() {
    for_each_storage_mode(&all_storage_modes_new(), |mode, opts| {
        // Arrange
        let engine = Arc::new(open_with_mode(&opts, mode));
        let cf = engine.create_column_family("test").expect("create cf");
        let mut tx1 = engine
            .begin_tx(cf.id(), TransactionMode::ReadWrite)
            .unwrap();
        tx1.put(b"key1".to_vec(), b"value1".to_vec(), Some(1))
            .unwrap(); // Expires
        tx1.commit(buffered_write_options(mode)).unwrap();
        let mut tx2 = engine
            .begin_tx(cf.id(), TransactionMode::ReadWrite)
            .unwrap();
        tx2.put(b"key2".to_vec(), b"value2".to_vec(), Some(0))
            .unwrap(); // Never expires
        tx2.commit(buffered_write_options(mode)).unwrap();
        let mut tx3 = engine
            .begin_tx(cf.id(), TransactionMode::ReadWrite)
            .unwrap();
        tx3.put(b"key3".to_vec(), b"value3".to_vec(), Some(3600))
            .unwrap(); // Long TTL
        tx3.commit(buffered_write_options(mode)).unwrap();

        // Act
        thread::sleep(Duration::from_millis(1100));
        let read_tx1 = engine.begin_tx(cf.id(), TransactionMode::ReadOnly).unwrap();
        let result1 = read_tx1.get(b"key1").unwrap();
        let read_tx2 = engine.begin_tx(cf.id(), TransactionMode::ReadOnly).unwrap();
        let result2 = read_tx2.get(b"key2").unwrap();
        let read_tx3 = engine.begin_tx(cf.id(), TransactionMode::ReadOnly).unwrap();
        let result3 = read_tx3.get(b"key3").unwrap();

        // Assert
        assert_eq!(result1, None); // Expired
        assert_eq!(result2, Some(Bytes::from_static(b"value2"))); // Never expires
        assert_eq!(result3, Some(Bytes::from_static(b"value3"))); // Still valid
    });
}

// ============================================================================
// TTL Update
// ============================================================================

#[test]
fn should_update_ttl_given_overwrite_with_new_ttl_when_writing() {
    for_each_storage_mode(&all_storage_modes_new(), |mode, opts| {
        // Arrange
        let engine = Arc::new(open_with_mode(&opts, mode));
        let cf = engine.create_column_family("test").expect("create cf");
        let mut tx1 = engine
            .begin_tx(cf.id(), TransactionMode::ReadWrite)
            .unwrap();
        tx1.put(b"key1".to_vec(), b"value1".to_vec(), Some(1))
            .unwrap(); // 1 second
        tx1.commit(buffered_write_options(mode)).unwrap();
        thread::sleep(Duration::from_millis(500));

        // Act - overwrite with longer TTL
        let mut tx2 = engine
            .begin_tx(cf.id(), TransactionMode::ReadWrite)
            .unwrap();
        tx2.put(b"key1".to_vec(), b"value2".to_vec(), Some(3600))
            .unwrap(); // 1 hour
        tx2.commit(buffered_write_options(mode)).unwrap();
        thread::sleep(Duration::from_millis(700)); // Original would have expired

        // Assert - should still be readable with new TTL
        let read_tx = engine.begin_tx(cf.id(), TransactionMode::ReadOnly).unwrap();
        let result = read_tx.get(b"key1").unwrap();
        assert_eq!(result, Some(Bytes::from_static(b"value2")));
    });
}

// ============================================================================
// TTL & Range Tombstone Interactions
// ============================================================================

#[test]
fn should_expire_keys_covered_by_range_tombstone_during_compaction() {
    for_each_storage_mode(&all_storage_modes_new(), |mode, opts| {
        eprintln!("\n=== TTL: Expire Keys Covered by Range Tombstone (mode: {mode}) ===");

        // Arrange: Write keys with TTL in range [k3, k8)
        let engine = Arc::new(open_with_mode(&opts, mode));
        let cf = engine.create_column_family("test").expect("create cf");

        // Write keys k1..k10 with 1 second TTL
        let mut tx = engine
            .begin_tx(cf.id(), TransactionMode::ReadWrite)
            .unwrap();
        for i in 1..=10 {
            let key = format!("k{i}");
            tx.put(key.as_bytes().to_vec(), b"ttl_value".to_vec(), Some(1))
                .unwrap();
        }
        tx.commit(buffered_write_options(mode)).unwrap();
        engine.flush_cf(&cf).expect("flush");

        // Write range tombstone [k3, k8) - covers k3-k7
        let mut delete_tx = engine
            .begin_tx(cf.id(), TransactionMode::ReadWrite)
            .unwrap();
        delete_tx
            .delete_range(b"k3".to_vec(), b"k8".to_vec())
            .unwrap();
        delete_tx.commit(buffered_write_options(mode)).unwrap();
        engine.flush_cf(&cf).expect("flush");

        // Wait for TTL expiry
        thread::sleep(Duration::from_millis(1100));

        // Act: Trigger compaction
        engine.compact_all().expect("compact TTL levels");

        // Assert: All keys expired and/or tombstoned, compaction cleaned them up
        let tx = engine.begin_tx(cf.id(), TransactionMode::ReadOnly).unwrap();

        // k1, k2 should be expired (TTL)
        assert_eq!(tx.get(b"k1").unwrap(), None);
        assert_eq!(tx.get(b"k2").unwrap(), None);

        // k3-k7 should be gone (range tombstone)
        assert_eq!(tx.get(b"k3").unwrap(), None);
        assert_eq!(tx.get(b"k5").unwrap(), None);
        assert_eq!(tx.get(b"k7").unwrap(), None);

        // k8-k10 should be expired (TTL)
        assert_eq!(tx.get(b"k8").unwrap(), None);
        assert_eq!(tx.get(b"k10").unwrap(), None);

        eprintln!("âœ“ TTL and range tombstone both cleaned during compaction");
    });
}

#[test]
fn should_handle_ttl_expiry_during_multi_level_compaction() {
    for_each_storage_mode(&all_storage_modes_new(), |mode, opts| {
        eprintln!("\n=== TTL: Multi-Level Compaction with Expiry (mode: {mode}) ===");

        // Arrange: Build multi-level LSM with different TTLs
        let engine = Arc::new(open_with_mode(&opts, mode));
        let cf = engine.create_column_family("test").expect("create cf");

        // L0: Write keys with 1-second TTL
        let mut tx = engine
            .begin_tx(cf.id(), TransactionMode::ReadWrite)
            .unwrap();
        for i in 0..50 {
            let key = format!("level0_key_{i:04}");
            tx.put(key.as_bytes().to_vec(), b"l0_value".to_vec(), Some(1))
                .unwrap();
        }
        tx.commit(buffered_write_options(mode)).unwrap();
        engine.flush_cf(&cf).expect("flush L0");

        // L1: Write keys with longer TTL
        let mut tx = engine
            .begin_tx(cf.id(), TransactionMode::ReadWrite)
            .unwrap();
        for i in 50..100 {
            let key = format!("level1_key_{i:04}");
            tx.put(key.as_bytes().to_vec(), b"l1_value".to_vec(), Some(3600))
                .unwrap();
        }
        tx.commit(buffered_write_options(mode)).unwrap();
        engine.flush_cf(&cf).expect("flush L1");

        // Wait for L0 TTL to expire
        thread::sleep(Duration::from_millis(1100));

        // Act: Trigger compaction (L0â†’L1)
        engine.compact_all().ok();

        // Assert: L0 expired keys removed; L1 keys unchanged
        let tx = engine.begin_tx(cf.id(), TransactionMode::ReadOnly).unwrap();

        // L0 keys should be gone (expired)
        let l0_found = (0..50)
            .filter(|i| {
                let key = format!("level0_key_{i:04}");
                tx.get(key.as_bytes())
                    .expect("read expired L0 key")
                    .is_some()
            })
            .count();
        assert_eq!(l0_found, 0, "L0 expired keys should be removed");

        // L1 keys should remain (not expired)
        let l1_found = (50..100)
            .filter(|i| {
                let key = format!("level1_key_{i:04}");
                tx.get(key.as_bytes())
                    .expect("read retained L1 key")
                    .is_some()
            })
            .count();
        assert!(l1_found >= 40, "L1 keys should remain");

        eprintln!("âœ“ Multi-level compaction handled TTL correctly; L1 retained: {l1_found}");
    });
}

#[test]
fn should_not_expose_ttl_expired_key_covered_by_range_tombstone() {
    for_each_storage_mode(&all_storage_modes_new(), |mode, opts| {
        eprintln!("\n=== TTL: Don't Expose TTL+Tombstone (mode: {mode}) ===");

        // Arrange: Create scenario with both TTL and tombstone covering same key
        let engine = Arc::new(open_with_mode(&opts, mode));
        let cf = engine.create_column_family("test").expect("create cf");

        // Write k5 with 1-second TTL (not flushed)
        let mut tx = engine
            .begin_tx(cf.id(), TransactionMode::ReadWrite)
            .unwrap();
        tx.put(b"k5".to_vec(), b"ttl_tombstone_value".to_vec(), Some(1))
            .unwrap();
        tx.commit(buffered_write_options(mode)).unwrap();

        // Immediately write range tombstone [k1, k10)
        let mut tx = engine
            .begin_tx(cf.id(), TransactionMode::ReadWrite)
            .unwrap();
        tx.delete_range(b"k1".to_vec(), b"k10".to_vec()).unwrap();
        tx.commit(buffered_write_options(mode)).unwrap();

        // Act: Read k5 after TTL expiry
        thread::sleep(Duration::from_millis(1100));
        let tx = engine.begin_tx(cf.id(), TransactionMode::ReadOnly).unwrap();
        let result = tx.get(b"k5").unwrap();

        // Assert: k5 not exposed (TTL expired AND tombstone covers it)
        assert_eq!(result, None);

        eprintln!("âœ“ TTL-expired + tombstone-covered key not exposed");
    });
}
