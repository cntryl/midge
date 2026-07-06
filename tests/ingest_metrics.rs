mod common;

// Concurrent write test to measure ingest metrics
use common::opts_for_mode;
use std::sync::Arc;
use std::thread;
use std::time::Duration;

#[test]
fn should_measure_concurrent_ingest_coordinator_metrics_when_multiple_threads_write() {
    // Arrange
    let mut opts = opts_for_mode("memory");
    opts.memtable_size = 64 * 1024 * 1024;
    let engine = Arc::new(cntryl_midge::Engine::open(opts.to_open_options()).unwrap());
    let cf = engine.create_column_family("test_cf").unwrap();
    let cf_id = cf.id();

    let num_threads = 8;
    let ops_per_thread = 10000;

    println!("\n=== Running concurrent write test ===");
    println!("Threads: {num_threads}");
    println!("Operations per thread: {ops_per_thread}");

    // Act
    let start = std::time::Instant::now();

    let mut handles = vec![];
    for thread_id in 0..num_threads {
        let engine_clone = Arc::clone(&engine);
        let handle = thread::spawn(move || {
            for op_id in 0..ops_per_thread {
                let key = format!("key-t{thread_id:02}-o{op_id:06}");
                let value = format!("val-{op_id}");

                let mut tx = engine_clone
                    .begin_tx(cf_id, cntryl_midge::TransactionMode::ReadWrite)
                    .expect("begin");
                tx.put(key.into_bytes(), value.into_bytes(), None)
                    .expect("put");
                tx.commit(cntryl_midge::WriteOptions::buffered())
                    .expect("commit");
            }
        });
        handles.push(handle);
    }

    // Wait for all threads
    for handle in handles {
        handle.join().expect("thread join");
    }

    // Assert
    let elapsed = start.elapsed();
    let total_ops = f64::from(num_threads * ops_per_thread);
    let throughput = total_ops / elapsed.as_secs_f64();

    println!("\nCompleted in {:.2}s", elapsed.as_secs_f64());
    println!("Throughput: {throughput:.0} ops/sec");

    // Give ingest threads time to log final stats
    thread::sleep(Duration::from_millis(100));
}
