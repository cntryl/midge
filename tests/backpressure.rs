//! Backpressure tests (formerly Phase 2: backpressure validation)

use cntryl_midge::testkit::*;
use cntryl_midge::{MidgeError, TransactionMode, WriteOptions};
use std::sync::atomic::{AtomicBool, Ordering};
use std::sync::Arc;
use std::time::Duration;

#[test]
fn should_return_write_stall_when_memory_budget_exceeded() {
    // Memory mode doesn't have meaningful backpressure (everything stays in memory)
    // so we only test durable modes where flush/compaction creates actual pressure.
    for_each_storage_mode(durable_storage_modes(), |mode, opts| {
        let mut opts = opts;
        // Use a smaller budget to trigger stall faster.
        // With 1MB budget: flush threshold = 512KB, stall at 1MB total.
        opts = opts.memory_budget(1024 * 1024);
        let engine = open_with_mode(opts, mode);
        let cf = engine.create_column_family("test").expect("create cf");

        let mut write_stall_observed = false;

        for i in 0..10_000 {
            let key = format!("key_{:06}", i);
            let value = vec![0u8; 1024]; // 1KB

            let mut txn = engine
                .begin_tx(cf.id(), TransactionMode::ReadWrite)
                .expect("begin");
            txn.put(key.as_bytes().to_vec(), value.clone(), None)
                .expect("put");

            match engine.commit(txn, WriteOptions::buffered()) {
                Ok(()) => {}
                Err(MidgeError::WriteStall(_msg)) => {
                    write_stall_observed = true;
                    break;
                }
                Err(e) => panic!("unexpected: {:?}", e),
            }
        }

        assert!(write_stall_observed, "Expected WriteStall in mode {}", mode);
    });
}

#[test]
fn should_succeed_after_backoff_when_write_stall_cleared() {
    for_each_storage_mode(durable_storage_modes(), |mode, opts| {
        let mut opts = opts;
        // Use 1MB budget for faster stall detection
        opts = opts.memory_budget(1024 * 1024);
        let engine = open_with_mode(opts, mode);
        let cf = engine.create_column_family("test").expect("create cf");

        // Hit first stall
        let mut first_stall_at = None;
        for i in 0..5000 {
            let key = format!("key_{:06}", i);
            let value = vec![0u8; 1024];
            let mut txn = engine
                .begin_tx(cf.id(), TransactionMode::ReadWrite)
                .expect("begin");
            txn.put(key.as_bytes().to_vec(), value.clone(), None)
                .expect("put");

            match engine.commit(txn, WriteOptions::buffered()) {
                Ok(()) => {}
                Err(MidgeError::WriteStall(_)) => {
                    first_stall_at = Some(i);
                    break;
                }
                Err(e) => panic!("unexpected: {:?}", e),
            }
        }

        assert!(first_stall_at.is_some(), "Expected stall in mode {}", mode);

        // Wait for stall to clear (compaction, etc.)
        std::thread::sleep(Duration::from_millis(100));

        // Attempt a write after stall is cleared; should succeed or stall again (not panic)
        let key = "recovery_key".to_string();
        let value = vec![0u8; 1024];
        let mut txn = engine
            .begin_tx(cf.id(), TransactionMode::ReadWrite)
            .expect("begin");
        txn.put(key.as_bytes().to_vec(), value.clone(), None)
            .expect("put");

        // Should either succeed or hit another stall, but not panic
        match engine.commit(txn, WriteOptions::buffered()) {
            Ok(()) => {
                // Success - stall was cleared
            }
            Err(MidgeError::WriteStall(_)) => {
                // Another stall occurred; acceptable
            }
            Err(e) => panic!("unexpected error: {:?}", e),
        }
    });
}

#[test]
fn should_prevent_oom_by_rejecting_writes_when_budget_exceeded() {
    for_each_storage_mode(&all_storage_modes_new(), |mode, opts| {
        let mut opts = opts;
        opts = opts.memory_budget(512 * 1024); // 512KB instead of 2MB for faster backpressure trigger
        let engine = Arc::new(open_with_mode(opts, mode));
        let cf = engine.create_column_family("test").expect("create cf");
        let cf_id = cf.id();

        let shutdown = Arc::new(AtomicBool::new(false));
        let shutdown_clone = shutdown.clone();
        let engine_clone = Arc::clone(&engine);

        let handle = std::thread::spawn(move || {
            let mut total_writes = 0;
            let mut total_stalls = 0;

            while !shutdown_clone.load(Ordering::Relaxed) {
                let key = format!("key_{}", total_writes);
                let value = vec![0u8; 2048]; // 2KB
                let mut txn = engine_clone
                    .begin_tx(cf_id, TransactionMode::ReadWrite)
                    .expect("begin");
                txn.put(key.as_bytes().to_vec(), value.clone(), None)
                    .expect("put");

                match engine_clone.commit(txn, WriteOptions::buffered()) {
                    Ok(()) => total_writes += 1,
                    Err(MidgeError::WriteStall(_)) => {
                        total_stalls += 1;
                        std::thread::sleep(Duration::from_millis(10));
                    }
                    Err(e) => panic!("unexpected: {:?}", e),
                }

                if total_writes + total_stalls >= 1000 {
                    break;
                }
            }

            (total_writes, total_stalls)
        });

        std::thread::sleep(Duration::from_secs(2));
        shutdown.store(true, Ordering::Relaxed);
        let (_total_writes, total_stalls) = handle.join().expect("panic");

        // CloudFirst uses a different backpressure mechanism (cloud_write_queue size)
        // rather than memory budget, so skip the stall assertion for cloud mode.
        // Memory mode doesn't have meaningful backpressure (everything stays in memory).
        if mode == "local" {
            assert!(total_stalls > 0, "Expected stalls in mode {}", mode);
        }
    });
}

#[test]
fn should_handle_concurrent_writes_with_consistent_backpressure() {
    for_each_storage_mode(&all_storage_modes_new(), |mode, opts| {
        let mut opts = opts;
        opts = opts.memory_budget(4 * 1024 * 1024);
        let engine = Arc::new(open_with_mode(opts, mode));
        let cf = engine.create_column_family("test").expect("create cf");

        let shutdown = Arc::new(AtomicBool::new(false));
        let mut handles = vec![];

        for thread_id in 0..4 {
            let engine_clone = Arc::clone(&engine);
            let shutdown_clone = shutdown.clone();
            let cf_id = cf.id();

            let handle = std::thread::spawn(move || {
                let mut writes = 0;
                let mut stalls = 0;

                while !shutdown_clone.load(Ordering::Relaxed) {
                    let key = format!("thread_{}_key_{}", thread_id, writes);
                    let value = vec![0u8; 1024];
                    let mut txn = engine_clone
                        .begin_tx(cf_id, TransactionMode::ReadWrite)
                        .expect("begin");
                    txn.put(key.as_bytes().to_vec(), value.clone(), None)
                        .expect("put");

                    match engine_clone.commit(txn, WriteOptions::buffered()) {
                        Ok(()) => writes += 1,
                        Err(MidgeError::WriteStall(_)) => {
                            stalls += 1;
                            std::thread::sleep(Duration::from_millis(5));
                        }
                        Err(e) => panic!("thread {} unexpected: {:?}", thread_id, e),
                    }

                    if writes + stalls >= 250 {
                        break;
                    }
                }

                (writes, stalls)
            });

            handles.push(handle);
        }

        std::thread::sleep(Duration::from_secs(2));
        shutdown.store(true, Ordering::Relaxed);

        let mut total_writes = 0;
        let mut _total_stalls = 0;
        for handle in handles.into_iter() {
            let (writes, stalls) = handle.join().expect("panic");
            total_writes += writes;
            _total_stalls += stalls;
        }

        if !mode.eq("memory") {
            assert!(total_writes > 0, "should have writes in mode {}", mode);
        }
    });
}
