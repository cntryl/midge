mod common;

// Concurrent public writes exercise runtime-side transaction coalescing.
use cntryl_midge::Query;
use common::opts_for_mode;
use std::sync::Arc;
use std::thread;

#[test]
fn should_preserve_runtime_coalescing_when_threads_write_concurrently() {
    // Arrange
    let mut opts = opts_for_mode("memory");
    opts.memtable_size = 64 * 1024 * 1024;
    let engine = Arc::new(cntryl_midge::Engine::open(opts.to_open_options()).unwrap());
    let cf = engine.create_column_family("test_cf").unwrap();
    let cf_id = cf.id();

    let num_threads = 8_usize;
    let ops_per_thread = 500_usize;

    // Act
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

    // Assert: caller submissions stay distinct, while the runtime coalesces
    // their WAL frames and preserves every logical write.
    let metrics = engine.get_runtime_metrics().expect("runtime metrics");
    let read = engine
        .begin_tx(cf_id, cntryl_midge::TransactionMode::ReadOnly)
        .expect("begin read transaction");
    let rows = read
        .scan(&Query::new())
        .expect("scan writes")
        .try_collect()
        .expect("collect writes");
    let total_ops = u64::try_from(num_threads * ops_per_thread).expect("operation count fits u64");
    assert_eq!(rows.len() as u64, total_ops);
    assert!(
        metrics.wal_append_count < total_ops,
        "runtime write draining should coalesce logical transactions"
    );
}
