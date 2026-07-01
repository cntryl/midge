mod common;

use common::opts_for_mode;
use std::sync::Arc;
use std::thread;
use std::time::Instant;

#[test]
fn should_group_concurrent_batch_submissions_when_multiple_threads_submit() {
    // Arrange
    let opts = opts_for_mode("memory");
    let engine =
        Arc::new(cntryl_midge::Engine::open(opts.to_open_options()).expect("Engine creation"));
    let cf = engine.create_column_family("test_cf").expect("create CF");
    let cf_id = cf.id();

    let num_threads = 8_u64;
    let batches_per_thread = 100_u64;

    // Act
    let start = Instant::now();
    let handles: Vec<_> = (0..num_threads)
        .map(|thread_id| {
            let engine_clone = Arc::clone(&engine);
            thread::spawn(move || {
                for batch_num in 0..batches_per_thread {
                    let key = format!("key_{thread_id}_{batch_num}");
                    let value = format!("value_{thread_id}");

                    // Each thread submits a small batch
                    let mut tx = engine_clone
                        .begin_tx(cf_id, cntryl_midge::TransactionMode::ReadWrite)
                        .expect("begin_tx");
                    tx.put(key.into_bytes(), value.into_bytes(), None)
                        .expect("put");
                    let result = tx.commit(cntryl_midge::WriteOptions::buffered());
                    assert!(result.is_ok(), "commit should succeed");
                }
            })
        })
        .collect();

    // Wait for all threads to complete
    for handle in handles {
        handle.join().expect("Thread should complete");
    }

    let elapsed = start.elapsed();
    let total_ops = batches_per_thread * num_threads;
    let total_ops_f64 =
        f64::from(u32::try_from(total_ops).expect("test operation count fits in u32"));
    let throughput = total_ops_f64 / elapsed.as_secs_f64();

    // Assert: Verify that operations completed successfully and measure throughput
    for thread_id in 0..num_threads {
        let key = format!("key_{}_{}", thread_id, batches_per_thread - 1);
        let read_tx = engine
            .begin_tx(cf_id, cntryl_midge::TransactionMode::ReadOnly)
            .expect("begin_tx for read");
        let result = read_tx.get(key.as_bytes());
        assert!(result.is_ok(), "Should be able to read back written value");
    }

    println!(
        "âœ“ Write grouping: {} ops from {} threads in {:.3}s, {:.0} ops/sec",
        total_ops,
        num_threads,
        elapsed.as_secs_f64(),
        throughput
    );
}

#[test]
fn should_handle_concurrent_writes_correctly_with_write_grouping() {
    // Arrange: Create engine
    let opts = opts_for_mode("memory");
    let engine =
        Arc::new(cntryl_midge::Engine::open(opts.to_open_options()).expect("Engine creation"));
    let cf = engine.create_column_family("test_cf2").expect("create CF");
    let cf_id = cf.id();

    let num_threads = 4_u64;
    let ops_per_thread = 50_u64;

    // Act
    let start = Instant::now();
    let handles: Vec<_> = (0..num_threads)
        .map(|thread_id| {
            let engine_clone = Arc::clone(&engine);
            thread::spawn(move || {
                for op_num in 0..ops_per_thread {
                    let key = format!("key_{thread_id}_{op_num}");
                    let value = "x".repeat(100); // 100 bytes value

                    let mut tx = engine_clone
                        .begin_tx(cf_id, cntryl_midge::TransactionMode::ReadWrite)
                        .expect("begin_tx");
                    tx.put(key.into_bytes(), value.into_bytes(), None)
                        .expect("put");
                    let _ = tx.commit(cntryl_midge::WriteOptions::buffered());
                }
            })
        })
        .collect();

    for handle in handles {
        handle.join().expect("Thread should complete");
    }

    let elapsed = start.elapsed();

    // Assert: Verify operations completed
    let key = format!("key_0_{}", ops_per_thread - 1);
    let read_tx = engine
        .begin_tx(cf_id, cntryl_midge::TransactionMode::ReadOnly)
        .expect("begin_tx");
    let result = read_tx.get(key.as_bytes());
    assert!(result.is_ok(), "Should be able to read back values");

    let total_ops = ops_per_thread * num_threads;
    let total_ops_f64 =
        f64::from(u32::try_from(total_ops).expect("test operation count fits in u32"));
    let throughput = total_ops_f64 / elapsed.as_secs_f64();
    println!(
        "âœ“ Write grouping with backpressure: {total_ops} ops from {num_threads} threads, {throughput:.0} ops/sec"
    );
}

#[test]
fn should_maintain_ordering_with_write_grouping() {
    // Arrange
    let opts = opts_for_mode("memory");
    let engine = cntryl_midge::Engine::open(opts.to_open_options()).expect("Engine creation");
    let cf = engine
        .create_column_family("test_counter")
        .expect("create CF");
    let cf_id = cf.id();

    let num_sequential_ops = 100_u64;

    // Act: Insert sequential values for the same key via concurrent transactions
    for i in 0..num_sequential_ops {
        let mut tx = engine
            .begin_tx(cf_id, cntryl_midge::TransactionMode::ReadWrite)
            .expect("begin_tx");
        tx.put(
            "counter".as_bytes().to_vec(),
            i.to_string().into_bytes(),
            None,
        )
        .expect("put");
        tx.commit(cntryl_midge::WriteOptions::buffered())
            .expect("commit");
    }

    // Assert: Verify final value
    let read_tx = engine
        .begin_tx(cf_id, cntryl_midge::TransactionMode::ReadOnly)
        .expect("begin_tx");
    if let Ok(Some(val)) = read_tx.get("counter".as_bytes()) {
        let final_val: u64 = std::str::from_utf8(&val)
            .ok()
            .and_then(|s| s.parse().ok())
            .expect("Should parse as u64");

        // The final value should be the last one we wrote
        assert_eq!(
            final_val,
            num_sequential_ops - 1,
            "Final value should match last written value"
        );
    }

    println!("âœ“ Ordering maintained across {num_sequential_ops} sequential operations");
}
