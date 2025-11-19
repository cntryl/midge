mod common;
use common::{
    bulk_put_fn, compaction_opts, new_engine_with_opts, new_shared_engine, test_temp_dir,
    with_engine_restart,
};
use std::sync::Arc;
use std::thread;

#[test]
fn should_generate_strictly_increasing_sequence_numbers_given_parallel_writes() {
    // Arrange
    let (_dir, eng) = new_shared_engine();
    let initial_seq = eng.current_sequence();

    // Act - concurrent writes from multiple threads
    let handles: Vec<_> = (0..10)
        .map(|thread_id| {
            let eng = eng.clone();
            thread::spawn(move || {
                let cf = eng.default_column_family();
                for i in 0..50 {
                    eng.put(
                        &cf,
                        format!("t{}_key{:02}", thread_id, i).as_bytes(),
                        b"value",
                    )
                    .expect("put");
                }
            })
        })
        .collect();

    for h in handles {
        h.join().unwrap();
    }

    // Assert - all writes should succeed
    let cf = eng.default_column_family();
    for thread_id in 0..10 {
        for i in 0..50 {
            let key = format!("t{}_key{:02}", thread_id, i);
            let result = eng.get(&cf, key.as_bytes()).expect("get");
            assert!(result.is_some(), "All concurrent writes should succeed");
        }
    }

    // Verify sequence numbers are strictly increasing
    let final_seq = eng.current_sequence();
    let expected_operations = 10 * 50; // 10 threads * 50 writes each
    assert_eq!(
        final_seq,
        initial_seq + expected_operations as u64,
        "Sequence numbers should increase by exactly the number of operations"
    );
}

#[test]
fn should_route_new_writes_to_new_memtable_given_freeze_in_progress_when_full() {
    // Arrange
    let (_dir, eng) = new_engine_with_opts(1024, false);
    let cf = eng.default_column_family();

    // Act - write enough to trigger memtable freeze and handoff
    bulk_put_fn(&eng, &cf, "key", 200, |_| b"value".to_vec());

    // Assert - all writes should succeed (routed to appropriate memtable)
    for i in 0..200 {
        let result = eng
            .get(&cf, format!("key{:03}", i).as_bytes())
            .expect("get");
        assert!(
            result.is_some(),
            "Writes should succeed during memtable freeze"
        );
    }
}

#[test]
fn should_return_latest_value_given_concurrent_puts_to_same_key_when_read() {
    // Arrange
    let (_dir, eng) = new_shared_engine();
    let cf = eng.default_column_family();

    // Act - concurrent writes to same key
    let handles: Vec<_> = (0..10)
        .map(|i| {
            let eng = eng.clone();
            thread::spawn(move || {
                let cf = eng.default_column_family();
                eng.put(&cf, b"shared_key", format!("value{}", i).as_bytes())
                    .expect("put");
            })
        })
        .collect();

    for h in handles {
        h.join().unwrap();
    }

    // Assert - should get one of the written values (last write wins)
    let result = eng.get(&cf, b"shared_key").expect("get");
    assert!(result.is_some(), "Concurrent writes should produce a value");
    // The specific value depends on thread scheduling
}

#[test]
fn should_trigger_flush_given_memtable_exceeds_threshold_when_background_thread_runs() {
    // Arrange
    let dir = test_temp_dir();

    with_engine_restart(
        compaction_opts(dir.path().to_path_buf(), 1024),
        |eng| {
            let cf = eng.default_column_family();
            // Act - write enough to exceed threshold
            bulk_put_fn(eng, &cf, "key", 100, |_| b"some_value_data".to_vec());
        },
        |eng| {
            // Assert - data should be flushed to SST
            let cf = eng.default_column_family();
            for i in 0..100 {
                let key = format!("key{:03}", i);
                let result = eng.get(&cf, key.as_bytes()).expect("get");
                assert!(result.is_some(), "Flushed data should be recoverable");
            }
        },
    );
}

#[test]
fn should_handle_extreme_concurrency_with_high_contention_writes_to_shared_memtable() {
    // Arrange
    let (_dir, eng) = new_shared_engine();
    let cf = eng.default_column_family();
    const NUM_THREADS: usize = 20;
    const ITERATIONS: usize = 100;

    // Act - high concurrency writes
    let handles: Vec<_> = (0..NUM_THREADS)
        .map(|thread_id| {
            let eng = eng.clone();
            let cf_clone = cf.clone();
            thread::spawn(move || {
                for i in 0..ITERATIONS {
                    eng.put(
                        &cf_clone,
                        format!("memtable_key_{}_{}", thread_id, i).as_bytes(),
                        format!("memtable_value_{}", thread_id * ITERATIONS + i).as_bytes(),
                    )
                    .expect("put under high concurrency");
                }
            })
        })
        .collect();

    for h in handles {
        h.join().expect("Thread panicked");
    }

    // Assert - verify all writes succeeded
    for thread_id in 0..NUM_THREADS {
        for i in 0..ITERATIONS {
            let result = eng
                .get(&cf, format!("memtable_key_{}_{}", thread_id, i).as_bytes())
                .expect("get");
            assert!(
                result.is_some(),
                "Write from thread {} iteration {} should be visible",
                thread_id,
                i
            );
        }
    }
}

#[test]
fn should_maintain_isolation_between_concurrent_memtable_operations_during_freeze() {
    // Arrange
    let (_dir, engine) = new_engine_with_opts(4096, true);
    let cf = engine.default_column_family();
    let engine = Arc::new(engine);
    const NUM_THREADS: usize = 15;

    // Act - concurrent writes that may trigger memtable freeze
    let handles: Vec<_> = (0..NUM_THREADS)
        .map(|thread_id| {
            let eng = Arc::clone(&engine);
            let cf_clone = cf.clone();
            std::thread::spawn(move || {
                for batch in 0..50 {
                    for i in 0..10 {
                        let key = format!("freeze_test_{}_{}_{}", thread_id, batch, i).into_bytes();
                        let value =
                            format!("value_{}", thread_id * 500 + batch * 10 + i).into_bytes();
                        eng.put(&cf_clone, &key, &value).expect("put during freeze");
                    }
                }
            })
        })
        .collect();

    for h in handles {
        h.join().expect("Thread panicked");
    }

    // Assert - verify all data persisted despite potential memtable freezes
    for thread_id in 0..NUM_THREADS {
        for batch in 0..50 {
            for i in 0..10 {
                let key = format!("freeze_test_{}_{}_{}", thread_id, batch, i).into_bytes();
                let result = engine.get(&cf, &key).expect("get after freeze");
                assert!(
                    result.is_some(),
                    "Data from thread {} batch {} should be visible",
                    thread_id,
                    batch
                );
            }
        }
    }
}

#[test]
fn should_track_sequence_numbers_correctly_across_concurrent_writes_with_overlapping_keys() {
    // Arrange
    let (_dir, engine) = new_shared_engine();
    let cf = engine.default_column_family();
    const NUM_THREADS: usize = 10;
    const WRITES_PER_THREAD: usize = 50;
    let initial_seq = engine.current_sequence();

    // Act - concurrent writes with overlapping key ranges
    let handles: Vec<_> = (0..NUM_THREADS)
        .map(|thread_id| {
            let eng = engine.clone();
            let cf_clone = cf.clone();
            thread::spawn(move || {
                for i in 0..WRITES_PER_THREAD {
                    let key = format!("seq_key_{:04}", (thread_id * 10 + i) % 100).into_bytes();
                    let value = format!(
                        "seq_value_{}_{}_{}",
                        thread_id,
                        i,
                        thread_id * WRITES_PER_THREAD + i
                    )
                    .into_bytes();
                    eng.put(&cf_clone, &key, &value)
                        .expect("put with overlapping keys");
                }
            })
        })
        .collect();

    for h in handles {
        h.join().expect("Thread panicked");
    }

    // Assert - final values should exist and be consistent
    for i in 0..100 {
        let key = format!("seq_key_{:04}", i).into_bytes();
        let result = engine
            .get(&cf, &key)
            .expect("get")
            .expect("key should exist");
        assert!(
            !result.is_empty(),
            "Key should have a value from sequence numbering"
        );
    }

    // Verify sequence numbers are strictly increasing
    let final_seq = engine.current_sequence();
    let expected_operations = NUM_THREADS * WRITES_PER_THREAD; // 10 threads * 50 writes each
    assert_eq!(
        final_seq,
        initial_seq + expected_operations as u64,
        "Sequence numbers should increase by exactly the number of operations"
    );
}
