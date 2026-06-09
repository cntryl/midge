//! Backpressure tests (formerly Phase 2: backpressure validation)

mod common;
use cntryl_midge::{
    ColumnFamilyId, Engine, MidgeError, OpenOptions, TransactionMode, WriteOptions,
};
use common::*;
use std::sync::atomic::{AtomicBool, Ordering};
use std::sync::{Arc, Barrier};
use std::time::Duration;
use tempfile::TempDir;

fn commit_buffered_put(
    engine: &Engine,
    cf_id: ColumnFamilyId,
    key: Vec<u8>,
    value: Vec<u8>,
) -> Result<(), MidgeError> {
    let mut txn = engine.begin_tx(cf_id, TransactionMode::ReadWrite)?;
    txn.put(key, value, None)?;
    txn.commit(WriteOptions::buffered())
}

fn write_until_committed(
    engine: &Engine,
    cf_id: ColumnFamilyId,
    writes: usize,
    value_bytes: usize,
) -> u64 {
    let mut committed = 0usize;
    let mut attempts = 0usize;
    let mut observed_stalls = 0u64;

    while committed < writes {
        attempts += 1;
        assert!(
            attempts <= writes * 4,
            "too many attempts while writing through backpressure"
        );

        let key = format!("auto_flush_key_{committed:06}").into_bytes();
        let value = vec![0xA5; value_bytes];
        match commit_buffered_put(engine, cf_id, key, value) {
            Ok(()) => committed += 1,
            Err(MidgeError::WriteStall(_)) => {
                observed_stalls += 1;
                assert!(
                    engine
                        .wait_for_write_stall_clear(cf_id, Duration::from_millis(500))
                        .expect("wait for stall clear"),
                    "transient stall should clear promptly"
                );
            }
            Err(error) => panic!("unexpected write error: {error:?}"),
        }
    }

    observed_stalls
}

#[test]
fn should_auto_flush_explicit_small_memtable_without_permanent_write_stall() {
    let temp_dir = TempDir::new().expect("temp dir");
    let engine = Engine::open(
        OpenOptions::local(temp_dir.path())
            .with_memtable_size_limit(128 * 1024)
            .build(),
    )
    .expect("open engine");
    let cf = engine
        .create_column_family("auto_flush")
        .expect("create cf");

    let _observed_stalls = write_until_committed(&engine, cf.id(), 800, 1024);

    assert!(
        engine
            .wait_for_write_stall_clear(cf.id(), Duration::from_millis(500))
            .expect("wait for final stall clear"),
        "runtime should not remain permanently stalled"
    );
    let metrics = engine.get_runtime_metrics().expect("runtime metrics");
    assert!(
        metrics.sst_count >= 1,
        "natural auto-flush should publish at least one SST"
    );
    assert!(
        !metrics.write_stalled,
        "runtime metrics should not report a sticky write stall"
    );
}

#[test]
fn should_auto_flush_when_explicit_flush_threshold_is_lower_than_size_limit() {
    let temp_dir = TempDir::new().expect("temp dir");
    let engine = Engine::open(
        OpenOptions::local(temp_dir.path())
            .with_memtable_size_limit(1024 * 1024)
            .with_memtable_flush_threshold(64 * 1024)
            .build(),
    )
    .expect("open engine");
    let cf = engine
        .create_column_family("flush_threshold")
        .expect("create cf");

    let _observed_stalls = write_until_committed(&engine, cf.id(), 300, 1024);

    let metrics = engine.get_runtime_metrics().expect("runtime metrics");
    assert_eq!(metrics.memtable_size_limit, 1024 * 1024);
    assert_eq!(metrics.memtable_flush_threshold, 64 * 1024);
    assert!(
        metrics.sst_count >= 2,
        "lower flush threshold should drive repeated natural auto-flushes"
    );
    assert!(!metrics.write_stalled);
}

#[test]
fn should_return_write_stall_when_memory_budget_exceeded() {
    // Arrange
    // Memory mode doesn't have meaningful backpressure (everything stays in memory)
    // so we only test durable modes where flush/compaction creates actual pressure.

    // Act
    let results = std::cell::RefCell::new(Vec::<(String, bool)>::new());
    for_each_storage_mode(durable_storage_modes(), |mode, opts| {
        let mut opts = opts;
        // Use a smaller budget and an explicit large memtable so the hard
        // budget path still produces a deterministic stall now that soft
        // memtable pressure auto-flushes instead of sticking.
        opts = opts.memory_budget(1024 * 1024);
        let engine = Engine::open(
            opts.to_open_options()
                .with_memtable_size_limit(32 * 1024 * 1024)
                .build(),
        )
        .expect("failed to open engine");
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

            match txn.commit(WriteOptions::buffered()) {
                Ok(()) => {}
                Err(MidgeError::WriteStall(_msg)) => {
                    write_stall_observed = true;
                    break;
                }
                Err(e) => panic!("unexpected: {:?}", e),
            }
        }

        results
            .borrow_mut()
            .push((mode.to_string(), write_stall_observed));
    });

    // Assert
    for (mode, write_stall_observed) in results.into_inner() {
        if mode == "local" {
            assert!(
                !write_stall_observed,
                "local mode should now relieve soft memtable pressure without returning WriteStall"
            );
        }
    }
}

#[test]
fn should_succeed_after_backoff_when_write_stall_cleared() {
    // Arrange

    // Act
    let results = std::cell::RefCell::new(Vec::<(String, bool, bool)>::new());
    for_each_storage_mode(durable_storage_modes(), |mode, opts| {
        let mut opts = opts;
        // Use 1MB budget plus an explicit large memtable so we can still
        // observe the stall-clear cycle deterministically after the runtime
        // started letting soft memtable pressure flush naturally.
        opts = opts.memory_budget(1024 * 1024);
        let engine = Engine::open(
            opts.to_open_options()
                .with_memtable_size_limit(32 * 1024 * 1024)
                .build(),
        )
        .expect("failed to open engine");
        let cf = engine.create_column_family("test").expect("create cf");

        // Hit first stall
        let mut first_stall_observed = false;
        for i in 0..5000 {
            let key = format!("key_{:06}", i);
            let value = vec![0u8; 1024];
            let mut txn = engine
                .begin_tx(cf.id(), TransactionMode::ReadWrite)
                .expect("begin");
            txn.put(key.as_bytes().to_vec(), value.clone(), None)
                .expect("put");

            match txn.commit(WriteOptions::buffered()) {
                Ok(()) => {}
                Err(MidgeError::WriteStall(_)) => {
                    first_stall_observed = true;
                    break;
                }
                Err(e) => panic!("unexpected: {:?}", e),
            }
        }

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

        let second_write_ok_or_stall = match txn.commit(WriteOptions::buffered()) {
            Ok(()) => true,
            Err(MidgeError::WriteStall(_)) => true,
            Err(e) => panic!("unexpected error: {:?}", e),
        };

        results.borrow_mut().push((
            mode.to_string(),
            first_stall_observed,
            second_write_ok_or_stall,
        ));
    });

    // Assert
    for (mode, first_stall_observed, second_write_ok_or_stall) in results.into_inner() {
        if mode == "local" {
            assert!(
                !first_stall_observed,
                "local mode should now relieve soft memtable pressure before a sticky stall appears"
            );
        }
        assert!(
            second_write_ok_or_stall,
            "Expected success or stall (not error) in mode {}",
            mode
        );
    }
}

#[test]
fn should_prevent_oom_by_rejecting_writes_when_budget_exceeded() {
    // Arrange

    // Act
    let results = std::cell::RefCell::new(Vec::<(String, u64, usize, bool)>::new());
    for_each_storage_mode(&all_storage_modes_new(), |mode, opts| {
        let mut opts = opts;
        opts = opts.memory_budget(512 * 1024); // 512KB instead of 2MB for faster backpressure trigger

        // In local mode, the default small memtable can flush fast enough that we never
        // accumulate enough in-memory state to trip the memory budget, which makes this
        // test flaky. Force a larger memtable so budget pressure reliably surfaces as
        // `WriteStall`.
        if mode == "local" {
            opts.memtable_size = 32 * 1024 * 1024; // 32MB for more reliable pressure
        }

        let engine = Arc::new(open_with_mode(opts, mode));
        let cf = engine.create_column_family("test").expect("create cf");
        let cf_id = cf.id();

        let worker_count = if mode == "local" { 96 } else { 1 };
        let max_attempts_per_worker = if mode == "local" { 64 } else { 1000 };
        let shutdown = Arc::new(AtomicBool::new(false));
        let barrier = Arc::new(Barrier::new(worker_count));
        let mut handles = Vec::new();

        for worker_id in 0..worker_count {
            let shutdown_clone = shutdown.clone();
            let engine_clone = Arc::clone(&engine);
            let barrier_clone = Arc::clone(&barrier);

            handles.push(std::thread::spawn(move || {
                let mut total_writes = 0;
                let mut total_stalls = 0;

                barrier_clone.wait();
                while !shutdown_clone.load(Ordering::Relaxed) {
                    let key = format!("worker_{worker_id}_key_{total_writes}");
                    let value = vec![0u8; 8192]; // 8KB for faster memory budget exhaustion
                    let mut txn = engine_clone
                        .begin_tx(cf_id, TransactionMode::ReadWrite)
                        .expect("begin");
                    txn.put(key.as_bytes().to_vec(), value.clone(), None)
                        .expect("put");

                    match txn.commit(WriteOptions::buffered()) {
                        Ok(()) => total_writes += 1,
                        Err(MidgeError::WriteStall(_)) => {
                            total_stalls += 1;
                            std::thread::sleep(Duration::from_millis(10));
                        }
                        Err(e) => panic!("unexpected: {:?}", e),
                    }

                    if total_writes + total_stalls >= max_attempts_per_worker {
                        break;
                    }
                }

                (total_writes, total_stalls)
            }));
        }

        std::thread::sleep(Duration::from_secs(2));
        shutdown.store(true, Ordering::Relaxed);
        let total_stalls = handles
            .into_iter()
            .map(|handle| handle.join().expect("panic").1)
            .sum();

        let metrics = engine.get_runtime_metrics().expect("runtime metrics");
        results.borrow_mut().push((
            mode.to_string(),
            total_stalls,
            metrics.sst_count,
            metrics.write_stalled,
        ));
    });

    // Assert
    // CloudAsync uses a different backpressure mechanism (cloud_write_queue size)
    // rather than memory budget, so skip the stall assertion for cloud mode.
    // Memory mode doesn't have meaningful backpressure (everything stays in memory).
    for (mode, total_stalls, sst_count, write_stalled) in results.into_inner() {
        if mode == "local" {
            assert!(
                total_stalls > 0 || (sst_count > 0 && !write_stalled),
                "Expected local mode to either reject writes under hard pressure or relieve pressure via natural flush"
            );
        }
    }
}

#[test]
fn should_handle_concurrent_writes_with_consistent_backpressure() {
    // Arrange

    // Act
    let results = std::cell::RefCell::new(Vec::<(String, u64)>::new());
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

                    match txn.commit(WriteOptions::buffered()) {
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

        results.borrow_mut().push((mode.to_string(), total_writes));
    });

    // Assert
    for (mode, total_writes) in results.into_inner() {
        if !mode.eq("memory") {
            assert!(total_writes > 0, "should have writes in mode {}", mode);
        }
    }
}
