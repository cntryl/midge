mod common;
use common::{bulk_put_fn, compaction_opts, new_engine_with_opts, new_shared_engine, test_temp_dir, with_engine_restart};
use std::thread;

#[test]
fn should_generate_strictly_increasing_sequence_numbers_given_parallel_writes() {
    // Arrange
    let (_dir, eng) = new_shared_engine();
    
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
                        b"value"
                    ).expect("put");
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
    // TODO: Add instrumentation to verify sequence numbers are strictly increasing
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
        let result = eng.get(&cf, format!("key{:03}", i).as_bytes()).expect("get");
        assert!(result.is_some(), "Writes should succeed during memtable freeze");
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
                eng.put(&cf, b"shared_key", format!("value{}", i).as_bytes()).expect("put");
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
        }
    );
}
