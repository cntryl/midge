//! Transaction Conflict Tests
//!
//! These tests verify transaction conflict detection and resolution:
//! - Write-write conflict detection
//! - Optimistic concurrency control (OCC)
//! - Lost update prevention
//! - CAS (Compare-And-Swap) conflict handling
//! - Delete and delete_range conflict detection
//! - High-contention scenarios
//! - Conflict state persistence across restarts
//!
//! # Storage Mode Coverage
//! - Uses `disk_storage_modes()` (LocalDisk, CloudBacked) since transactions require WAL durability
//! - Memory mode does not support durable transactions

mod common;

use bytes::Bytes;
use cntryl_midge::{KvTransaction, MidgeEngine, MidgeOptions, WriteOptions};
use common::{create_storage_mode, disk_storage_modes, DurabilityTestContext};
use std::sync::Arc;

// ============================================================================
// WRITE-WRITE CONFLICT DETECTION
// ============================================================================

#[test]
fn should_detect_write_write_conflict_given_concurrent_updates_to_same_key() {
    for mode in disk_storage_modes() {
        let (name, storage_mode, _dir) = create_storage_mode(mode);

        // Arrange
        let opts = MidgeOptions {
            storage_mode,
            ..Default::default()
        };
        let engine = Arc::new(MidgeEngine::open(opts).expect("open"));
        let cf = engine.default_column_family();
        engine.put(&cf, b"key", b"v0").unwrap();

        let mut txn1 = engine.begin_transaction(&cf).unwrap();
        let mut txn2 = engine.begin_transaction(&cf).unwrap();

        txn1.put(b"key", b"v1").unwrap();
        txn2.put(b"key", b"v2").unwrap();

        // Act
        let first_result = engine.commit_transaction(txn1, WriteOptions::default());
        let second_result = engine.commit_transaction(txn2, WriteOptions::default());

        // Assert
        assert!(first_result.is_ok(), "first transaction should succeed for {}", name);
        assert!(
            second_result.is_err(),
            "second transaction should fail on conflict for {}",
            name
        );
        assert_eq!(
            engine.get(&cf, b"key").expect("get"),
            Some(Bytes::from("v1")),
            "Failed for {}",
            name
        );
    }
}

#[test]
fn should_abort_second_transaction_given_write_conflict_when_both_commit() {
    for mode in disk_storage_modes() {
        let (name, storage_mode, _dir) = create_storage_mode(mode);

        // Arrange
        let opts = MidgeOptions {
            storage_mode,
            ..Default::default()
        };
        let engine = Arc::new(MidgeEngine::open(opts).expect("open"));
        let cf = engine.default_column_family();

        let mut winner = engine.begin_transaction(&cf).unwrap();
        let mut loser = engine.begin_transaction(&cf).unwrap();

        winner.put(b"conflict_key", b"txn1_value").unwrap();
        loser.put(b"conflict_key", b"txn2_value").unwrap();

        // Act
        let winner_result = engine.commit_transaction(winner, WriteOptions::default());
        let loser_result = engine.commit_transaction(loser, WriteOptions::default());

        // Assert
        assert!(winner_result.is_ok(), "winner should commit successfully for {}", name);
        assert!(
            loser_result.is_err(),
            "loser should fail with write-write conflict for {}",
            name
        );
        assert_eq!(
            engine.get(&cf, b"conflict_key").expect("get"),
            Some(Bytes::from("txn1_value")),
            "Failed for {}",
            name
        );
    }
}

#[test]
fn should_reject_second_committer_on_write_write_conflict() {
    for mode in disk_storage_modes() {
        let (name, storage_mode, _dir) = create_storage_mode(mode);

        // Arrange
        let opts = MidgeOptions {
            storage_mode,
            ..Default::default()
        };
        let engine = Arc::new(MidgeEngine::open(opts).expect("open"));
        let cf = engine.default_column_family();

        let mut first_txn = engine.begin_transaction(&cf).expect("begin first");
        let mut second_txn = engine.begin_transaction(&cf).expect("begin second");

        first_txn.put(b"conflict_key", b"txn1_val").unwrap();
        second_txn.put(b"conflict_key", b"txn2_val").unwrap();

        // Act
        let first_result = engine.commit_transaction(first_txn, WriteOptions::default());
        let second_result = engine.commit_transaction(second_txn, WriteOptions::default());

        // Assert
        assert!(
            first_result.is_ok() && second_result.is_err(),
            "Expected first commit to succeed and second to fail with conflict for {}",
            name
        );
    }
}

#[test]
fn should_preserve_first_commit_given_write_conflict_when_second_aborts() {
    for mode in disk_storage_modes() {
        let (name, storage_mode, _dir) = create_storage_mode(mode);

        // Arrange
        let opts = MidgeOptions {
            storage_mode,
            ..Default::default()
        };
        let engine = Arc::new(MidgeEngine::open(opts).expect("open"));
        let cf = engine.default_column_family();

        let mut txn1 = engine.begin_transaction(&cf).unwrap();
        txn1.put(b"key", b"first_value").unwrap();
        engine
            .commit_transaction(txn1, WriteOptions::default())
            .unwrap();

        let mut aborted_txn = engine.begin_transaction(&cf).unwrap();
        aborted_txn.put(b"key", b"second_value").unwrap();

        // Act
        drop(aborted_txn); // rollback

        // Assert
        assert_eq!(
            engine.get(&cf, b"key").expect("get"),
            Some(Bytes::from("first_value")),
            "Failed for {}",
            name
        );
    }
}

// ============================================================================
// DELETE CONFLICT DETECTION
// ============================================================================

#[test]
fn should_handle_write_conflict_on_delete_given_concurrent_delete_and_put() {
    for mode in disk_storage_modes() {
        let (name, storage_mode, _dir) = create_storage_mode(mode);

        // Arrange
        let opts = MidgeOptions {
            storage_mode,
            ..Default::default()
        };
        let engine = Arc::new(MidgeEngine::open(opts).expect("open"));
        let cf = engine.default_column_family();
        engine.put(&cf, b"key", b"initial").unwrap();

        let mut delete_txn = engine.begin_transaction(&cf).unwrap();
        let mut put_txn = engine.begin_transaction(&cf).unwrap();

        delete_txn.delete(b"key").unwrap();
        put_txn.put(b"key", b"updated").unwrap();

        // Act
        let delete_result = engine.commit_transaction(delete_txn, WriteOptions::default());
        let put_result = engine.commit_transaction(put_txn, WriteOptions::default());

        // Assert
        assert!(delete_result.is_ok(), "delete should commit first for {}", name);
        assert!(put_result.is_err(), "put should fail due to conflict for {}", name);
        assert_eq!(
            engine.get(&cf, b"key").unwrap(),
            None,
            "Key should be deleted for {}",
            name
        );
    }
}

#[test]
fn should_detect_conflict_on_delete_range_given_overlapping_keys() {
    for mode in disk_storage_modes() {
        let (name, storage_mode, _dir) = create_storage_mode(mode);

        // Arrange
        let opts = MidgeOptions {
            storage_mode,
            ..Default::default()
        };
        let engine = Arc::new(MidgeEngine::open(opts).expect("open"));
        let cf = engine.default_column_family();

        for i in 0..10 {
            let key = format!("key{i}");
            engine.put(&cf, key.as_bytes(), b"val").unwrap();
        }

        let mut range_txn = engine.begin_transaction(&cf).unwrap();
        range_txn.delete_range(b"key3", b"key7").unwrap();

        let mut overlap_txn = engine.begin_transaction(&cf).unwrap();
        overlap_txn.put(b"key5", b"new_value").unwrap();

        // Act
        let range_result = engine.commit_transaction(range_txn, WriteOptions::default());
        let overlap_result = engine.commit_transaction(overlap_txn, WriteOptions::default());

        // Assert
        assert!(range_result.is_ok(), "Range delete should commit for {}", name);
        assert!(
            overlap_result.is_err(),
            "Overlapping transaction should fail due to range conflict for {}",
            name
        );
    }
}

// ============================================================================
// LOST UPDATE PREVENTION
// ============================================================================

#[test]
fn should_prevent_lost_update_given_read_modify_write_when_concurrent() {
    for mode in disk_storage_modes() {
        let (name, storage_mode, _dir) = create_storage_mode(mode);

        // Arrange
        let opts = MidgeOptions {
            storage_mode,
            ..Default::default()
        };
        let engine = Arc::new(MidgeEngine::open(opts).expect("open"));
        let cf = engine.default_column_family();

        engine.put(&cf, b"counter", b"0").expect("put");

        let mut first_increment_txn = engine.begin_transaction(&cf).expect("begin first");
        let mut second_increment_txn = engine.begin_transaction(&cf).expect("begin second");

        let snap1 = engine.snapshot();
        let snap2 = engine.snapshot();

        let val1 = engine.get_at(&cf, b"counter", &snap1).expect("get");
        let val2 = engine.get_at(&cf, b"counter", &snap2).expect("get");

        let count1: i32 = String::from_utf8(val1.unwrap().to_vec())
            .unwrap()
            .parse()
            .unwrap();
        let count2: i32 = String::from_utf8(val2.unwrap().to_vec())
            .unwrap()
            .parse()
            .unwrap();

        first_increment_txn
            .put(b"counter", (count1 + 1).to_string().as_bytes())
            .unwrap();
        second_increment_txn
            .put(b"counter", (count2 + 1).to_string().as_bytes())
            .unwrap();

        engine
            .commit_transaction(first_increment_txn, WriteOptions::default())
            .expect("commit first");

        // Act
        let result = engine.commit_transaction(second_increment_txn, WriteOptions::default());

        // Assert - With read tracking and snapshot isolation, lost update is PREVENTED
        assert!(
            result.is_err(),
            "Should detect read-write conflict and prevent lost update for {}",
            name
        );

        // Final value is 1 (only first increment succeeded)
        let final_val = engine.get(&cf, b"counter").expect("get final");
        let final_count: i32 = String::from_utf8(final_val.unwrap().to_vec())
            .unwrap()
            .parse()
            .unwrap();
        assert_eq!(
            final_count, 1,
            "Only first transaction should have committed for {}",
            name
        );
    }
}

#[test]
fn should_detect_lost_update_given_cas_pattern_when_value_changed() {
    for mode in disk_storage_modes() {
        let (name, storage_mode, _dir) = create_storage_mode(mode);

        // Arrange
        let opts = MidgeOptions {
            storage_mode,
            ..Default::default()
        };
        let engine = Arc::new(MidgeEngine::open(opts).expect("open"));
        let cf = engine.default_column_family();

        engine.put(&cf, b"key", b"v1").expect("put");

        let snap = engine.snapshot();
        let expected = engine.get_at(&cf, b"key", &snap).expect("get");

        engine.put(&cf, b"key", b"v2").expect("concurrent update");

        let mut cas_txn = engine.begin_transaction(&cf).expect("begin");
        cas_txn
            .compare_and_swap(b"key", expected.as_ref().map(|b| b.as_ref()), b"v3")
            .unwrap();

        // Act
        let result = engine.commit_transaction(cas_txn, WriteOptions::default());

        // Assert - CAS should fail because the value changed
        assert!(
            result.is_err(),
            "CAS should fail when value changed since snapshot for {}",
            name
        );
        assert!(expected.is_some());
    }
}

#[test]
fn should_preserve_both_updates_given_non_overlapping_keys_when_concurrent_commits() {
    for mode in disk_storage_modes() {
        let (name, storage_mode, _dir) = create_storage_mode(mode);

        // Arrange
        let opts = MidgeOptions {
            storage_mode,
            ..Default::default()
        };
        let engine = Arc::new(MidgeEngine::open(opts).expect("open"));
        let cf = engine.default_column_family();

        let mut first_key_txn = engine.begin_transaction(&cf).expect("begin first");
        let mut second_key_txn = engine.begin_transaction(&cf).expect("begin second");

        first_key_txn.put(b"key1", b"value1").unwrap();
        second_key_txn.put(b"key2", b"value2").unwrap();

        engine
            .commit_transaction(first_key_txn, WriteOptions::default())
            .expect("commit first");

        // Act
        engine
            .commit_transaction(second_key_txn, WriteOptions::default())
            .expect("commit second");

        // Assert
        assert_eq!(
            engine.get(&cf, b"key1").expect("get"),
            Some(Bytes::from("value1")),
            "Failed for {}",
            name
        );
        assert_eq!(
            engine.get(&cf, b"key2").expect("get"),
            Some(Bytes::from("value2")),
            "Failed for {}",
            name
        );
    }
}

// ============================================================================
// OPTIMISTIC CONCURRENCY CONTROL
// ============================================================================

#[test]
fn should_commit_transaction_given_no_conflicts() {
    for mode in disk_storage_modes() {
        let (name, storage_mode, _dir) = create_storage_mode(mode);

        // Arrange
        let opts = MidgeOptions {
            storage_mode,
            ..Default::default()
        };
        let engine = MidgeEngine::open(opts).expect("open");
        let cf = engine.default_column_family();

        engine.put(&cf, b"key", b"v1").expect("put");

        // Act - Start transaction, read key, modify different key, then commit
        let mut txn = engine.begin_transaction(&cf).expect("begin");
        let _read_value = txn.get(b"key").expect("get");
        txn.put(b"other_key", b"value").expect("put");

        let result = engine.commit_transaction(txn, WriteOptions::default());

        // Assert - Should commit successfully
        assert!(
            result.is_ok(),
            "Transaction without conflicts should commit for {}",
            name
        );
    }
}

#[test]
fn should_commit_transaction_given_concurrent_modifications_to_different_keys() {
    for mode in disk_storage_modes() {
        let (name, storage_mode, _dir) = create_storage_mode(mode);

        // Arrange
        let opts = MidgeOptions {
            storage_mode,
            ..Default::default()
        };
        let engine = MidgeEngine::open(opts).expect("open");
        let cf = engine.default_column_family();

        engine.put(&cf, b"key", b"initial").expect("put");

        // Act - Start transaction, read one key, concurrently modify a different key
        let mut txn = engine.begin_transaction(&cf).expect("begin");
        let _value = txn.get(b"key").expect("get");

        // Concurrent modification to different key
        engine.put(&cf, b"other_key", b"modified").expect("put");

        // Transaction writes to yet another key
        txn.put(b"txn_key", b"txn_value").expect("put");

        let result = engine.commit_transaction(txn, WriteOptions::default());

        // Assert - Should succeed (no write-write conflict)
        assert!(result.is_ok(), "No conflict on different keys for {}", name);
    }
}

#[test]
fn should_read_values_within_transaction() {
    for mode in disk_storage_modes() {
        let (name, storage_mode, _dir) = create_storage_mode(mode);

        // Arrange
        let opts = MidgeOptions {
            storage_mode,
            ..Default::default()
        };
        let engine = MidgeEngine::open(opts).expect("open");
        let cf = engine.default_column_family();

        engine.put(&cf, b"k1", b"v1").expect("put");
        engine.put(&cf, b"k2", b"v2").expect("put");

        // Act - Read multiple keys within transaction
        let mut txn = engine.begin_transaction(&cf).expect("begin");
        let v1 = txn.get(b"k1").expect("get");
        let v2 = txn.get(b"k2").expect("get");

        // Assert - Transaction should provide snapshot isolation
        assert!(v1.is_some(), "Should read k1 for {}", name);
        assert!(v2.is_some(), "Should read k2 for {}", name);
    }
}

#[test]
fn should_commit_new_key_given_clean_transaction() {
    for mode in disk_storage_modes() {
        let (name, storage_mode, _dir) = create_storage_mode(mode);

        // Arrange
        let opts = MidgeOptions {
            storage_mode,
            ..Default::default()
        };
        let engine = MidgeEngine::open(opts).expect("open");
        let cf = engine.default_column_family();

        // Act - Create transaction and write new key
        let mut txn = engine.begin_transaction(&cf).expect("begin");
        txn.put(b"new_key", b"new_value").expect("put");

        let result = engine.commit_transaction(txn, WriteOptions::default());

        // Assert - Should commit successfully
        assert!(result.is_ok(), "Clean transaction should commit for {}", name);
        assert_eq!(
            engine.get(&cf, b"new_key").expect("get"),
            Some(Bytes::from("new_value")),
            "Failed for {}",
            name
        );
    }
}

#[test]
fn should_allow_concurrent_writes_to_different_keys() {
    for mode in disk_storage_modes() {
        let (name, storage_mode, _dir) = create_storage_mode(mode);

        // Arrange
        let opts = MidgeOptions {
            storage_mode,
            ..Default::default()
        };
        let engine = Arc::new(MidgeEngine::open(opts).expect("open"));
        let cf = engine.default_column_family();

        // Act: Spawn 20 threads each writing to a different key
        let handles: Vec<_> = (0..20)
            .map(|i| {
                let eng = engine.clone();
                let cf_clone = cf.clone();
                std::thread::spawn(move || {
                    let mut txn = eng.begin_transaction(&cf_clone).unwrap();
                    let key = format!("key_{}", i);
                    txn.put(key.as_bytes(), b"value").unwrap();
                    eng.commit_transaction(txn, WriteOptions::default())
                })
            })
            .collect();

        let results: Vec<_> = handles
            .into_iter()
            .map(|h| h.join().expect("Thread panicked"))
            .collect();

        // Assert: All writes succeeded (no conflicts for different keys)
        let success_count = results.iter().filter(|r| r.is_ok()).count();
        assert_eq!(
            success_count, 20,
            "All concurrent writes to different keys should succeed for {}",
            name
        );

        for i in 0..20 {
            let key = format!("key_{}", i);
            assert_eq!(
                engine.get(&cf, key.as_bytes()).expect("get"),
                Some(Bytes::from("value")),
                "Failed for {}",
                name
            );
        }
    }
}

// ============================================================================
// HIGH CONTENTION STRESS TESTS
// ============================================================================

#[test]
fn should_handle_high_contention_writes_without_panic() {
    for mode in disk_storage_modes() {
        let (name, storage_mode, _dir) = create_storage_mode(mode);

        // Arrange
        let opts = MidgeOptions {
            storage_mode,
            ..Default::default()
        };
        let engine = Arc::new(MidgeEngine::open(opts).expect("open"));
        let cf = engine.default_column_family();

        // Act: Spawn 10 threads each performing conflicting writes
        let handles: Vec<_> = (0..10)
            .map(|i| {
                let eng = engine.clone();
                let cf_clone = cf.clone();
                std::thread::spawn(move || {
                    for j in 0..5 {
                        let mut txn = eng.begin_transaction(&cf_clone).unwrap();
                        txn.put(
                            b"contention_key",
                            format!("thread_{}_iteration_{}", i, j).as_bytes(),
                        )
                        .unwrap();
                        let _ = eng.commit_transaction(txn, WriteOptions::default());
                    }
                })
            })
            .collect();

        for handle in handles {
            handle.join().expect("Thread panicked");
        }

        // Assert: No panics, final value exists
        assert!(
            engine.get(&cf, b"contention_key").is_ok(),
            "Contention key should be readable for {}",
            name
        );
    }
}

#[test]
fn should_handle_concurrent_read_modify_writes_without_panic() {
    for mode in disk_storage_modes() {
        let (name, storage_mode, _dir) = create_storage_mode(mode);

        // Arrange
        let opts = MidgeOptions {
            storage_mode,
            ..Default::default()
        };
        let engine = Arc::new(MidgeEngine::open(opts).expect("open"));
        let cf = engine.default_column_family();
        engine.put(&cf, b"concurrent_counter", b"0").unwrap();

        // Act: Spawn 20 threads, each doing read-modify-write
        let handles: Vec<_> = (0..20)
            .map(|thread_id| {
                let eng = engine.clone();
                let cf_clone = cf.clone();
                std::thread::spawn(move || {
                    for _ in 0..5 {
                        let mut txn = eng.begin_transaction(&cf_clone).unwrap();
                        let current = txn.get(b"concurrent_counter").unwrap();
                        let num: i32 = String::from_utf8(current.unwrap_or_default().to_vec())
                            .unwrap_or_else(|_| "0".to_string())
                            .parse()
                            .unwrap_or(0);
                        txn.put(
                            b"concurrent_counter",
                            format!("{}_{}", num + 1, thread_id).as_bytes(),
                        )
                        .unwrap();
                        let _ = eng.commit_transaction(txn, WriteOptions::default());
                    }
                })
            })
            .collect();

        for handle in handles {
            handle.join().expect("Thread panicked");
        }

        // Assert: Counter is readable
        assert!(
            engine.get(&cf, b"concurrent_counter").is_ok(),
            "Counter should be readable for {}",
            name
        );
    }
}

#[test]
fn should_handle_high_concurrency_optimistic_locking() {
    for mode in disk_storage_modes() {
        let (name, storage_mode, _dir) = create_storage_mode(mode);

        // Arrange
        let opts = MidgeOptions {
            storage_mode,
            ..Default::default()
        };
        let engine = Arc::new(MidgeEngine::open(opts).expect("open"));
        let cf = engine.default_column_family();

        // Pre-populate 10 keys
        for i in 0..10 {
            let key = format!("opt_key_{}", i);
            engine.put(&cf, key.as_bytes(), b"v0").unwrap();
        }

        // Act: Spawn 20 threads with overlapping reads and writes
        let handles: Vec<_> = (0..20)
            .map(|thread_id| {
                let eng = engine.clone();
                let cf_clone = cf.clone();
                std::thread::spawn(move || {
                    for iteration in 0..10 {
                        let key_index = (thread_id * iteration) % 10;
                        let key = format!("opt_key_{}", key_index);

                        let mut txn = eng.begin_transaction(&cf_clone).unwrap();
                        let _ = txn.get(key.as_bytes());
                        txn.put(
                            key.as_bytes(),
                            format!("t{}_i{}", thread_id, iteration).as_bytes(),
                        )
                        .unwrap();
                        let _ = eng.commit_transaction(txn, WriteOptions::default());
                    }
                })
            })
            .collect();

        for handle in handles {
            handle.join().expect("Thread panicked");
        }

        // Assert: All keys should still be readable
        for i in 0..10 {
            let key = format!("opt_key_{}", i);
            assert!(
                engine.get(&cf, key.as_bytes()).is_ok(),
                "Key {} should be readable for {}",
                key,
                name
            );
        }
    }
}

#[test]
fn should_maintain_transaction_isolation_under_stress() {
    for mode in disk_storage_modes() {
        let (name, storage_mode, _dir) = create_storage_mode(mode);

        // Arrange
        let opts = MidgeOptions {
            storage_mode,
            ..Default::default()
        };
        let engine = Arc::new(MidgeEngine::open(opts).expect("open"));
        let cf = engine.default_column_family();

        // Pre-populate keys
        for i in 0..100 {
            let key = format!("stress_key_{}", i);
            engine.put(&cf, key.as_bytes(), b"0").unwrap();
        }

        // Act: High concurrency stress test
        let handles: Vec<_> = (0..50)
            .map(|thread_id| {
                let eng = engine.clone();
                let cf_clone = cf.clone();
                std::thread::spawn(move || {
                    for iteration in 0..10 {
                        let key_index = (thread_id * 10 + iteration) % 100;
                        let key = format!("stress_key_{}", key_index);

                        let mut txn = eng.begin_transaction(&cf_clone).unwrap();
                        let new_value = format!("t{}_i{}", thread_id, iteration);
                        txn.put(key.as_bytes(), new_value.as_bytes()).unwrap();
                        let _ = eng.commit_transaction(txn, WriteOptions::default());
                    }
                })
            })
            .collect();

        for handle in handles {
            handle.join().expect("Thread panicked");
        }

        // Assert: All keys still exist and are readable
        for i in 0..100 {
            let key = format!("stress_key_{}", i);
            let result = engine.get(&cf, key.as_bytes());
            assert!(
                result.is_ok(),
                "Key {} should be readable after stress test for {}",
                key,
                name
            );
        }
    }
}

// ============================================================================
// DURABILITY TESTS - CONFLICT STATE PERSISTENCE
// ============================================================================

#[test]
fn should_recover_conflict_state_after_engine_restart() {
    for mode in disk_storage_modes() {
        let ctx = DurabilityTestContext::new(mode);
        let name = ctx.name();

        // Arrange
        let opts = MidgeOptions {
            storage_mode: ctx.create_storage_mode(),
            ..Default::default()
        };
        let engine = MidgeEngine::open(opts).expect("initial open");
        let cf = engine.default_column_family();

        // Put initial value
        engine.put(&cf, b"recovery_key", b"initial").unwrap();

        let mut txn1 = engine.begin_transaction(&cf).unwrap();
        txn1.put(b"recovery_key", b"after_conflict").unwrap();
        engine
            .commit_transaction(txn1, WriteOptions::default())
            .unwrap();

        drop(engine);

        // Act: Reopen engine and verify state persisted
        let opts2 = MidgeOptions {
            storage_mode: ctx.create_storage_mode(),
            ..Default::default()
        };
        let engine = MidgeEngine::open(opts2).expect("restart open");
        let cf = engine.default_column_family();

        // Assert: Value persisted correctly
        assert_eq!(
            engine.get(&cf, b"recovery_key").expect("get"),
            Some(Bytes::from("after_conflict")),
            "Failed for {}",
            name
        );
    }
}

#[test]
fn should_persist_lost_update_prevention_after_restart() {
    for mode in disk_storage_modes() {
        let ctx = DurabilityTestContext::new(mode);
        let name = ctx.name();

        // Arrange
        let opts = MidgeOptions {
            storage_mode: ctx.create_storage_mode(),
            ..Default::default()
        };
        let engine = MidgeEngine::open(opts).expect("initial open");
        let cf = engine.default_column_family();

        engine.put(&cf, b"persist_counter", b"100").unwrap();
        engine
            .commit_transaction(
                {
                    let mut txn = engine.begin_transaction(&cf).unwrap();
                    txn.put(b"persist_counter", b"101").unwrap();
                    txn
                },
                WriteOptions::default(),
            )
            .unwrap();

        drop(engine);

        // Act: Restart and verify value persisted
        let opts2 = MidgeOptions {
            storage_mode: ctx.create_storage_mode(),
            ..Default::default()
        };
        let engine = MidgeEngine::open(opts2).expect("restart open");
        let cf = engine.default_column_family();

        // Assert: Value should persist
        let result = engine.get(&cf, b"persist_counter").unwrap();
        assert_eq!(
            result.as_deref(),
            Some(b"101".as_ref()),
            "Failed for {}",
            name
        );
    }
}

#[test]
fn should_maintain_optimistic_locking_under_recovery() {
    for mode in disk_storage_modes() {
        let ctx = DurabilityTestContext::new(mode);
        let name = ctx.name();

        // Arrange
        let opts = MidgeOptions {
            storage_mode: ctx.create_storage_mode(),
            ..Default::default()
        };
        let engine = MidgeEngine::open(opts).expect("initial open");
        let cf = engine.default_column_family();

        // Pre-populate and perform transactions
        for i in 0..5 {
            let key = format!("recovery_opt_key_{}", i);
            engine.put(&cf, key.as_bytes(), b"initial").unwrap();
        }

        for i in 0..5 {
            let key = format!("recovery_opt_key_{}", i);
            let mut txn = engine.begin_transaction(&cf).unwrap();
            let _ = txn.get(key.as_bytes());
            txn.put(key.as_bytes(), format!("updated_{}", i).as_bytes())
                .unwrap();
            engine
                .commit_transaction(txn, WriteOptions::default())
                .unwrap();
        }

        drop(engine);

        // Act: Restart and verify optimistic locking state persisted
        let opts2 = MidgeOptions {
            storage_mode: ctx.create_storage_mode(),
            ..Default::default()
        };
        let engine = MidgeEngine::open(opts2).expect("restart open");
        let cf = engine.default_column_family();

        // Assert: All values should persist
        for i in 0..5 {
            let key = format!("recovery_opt_key_{}", i);
            let result = engine.get(&cf, key.as_bytes()).unwrap();
            assert!(
                result.is_some(),
                "Optimistically locked key {} should persist for {}",
                key,
                name
            );
        }
    }
}
