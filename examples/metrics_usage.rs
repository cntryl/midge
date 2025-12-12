use cntryl_midge::metrics::EngineMetrics;

fn main() {
    let metrics = EngineMetrics::new();

    // Simulate some operations
    for i in 0..100 {
        metrics.record_read(1_000_000 + (i * 10_000) as u64);
        metrics.record_write(512, 500_000 + (i * 5_000) as u64);
    }

    // Record some compaction
    {
        let _guard = metrics.record_compaction_start();
        metrics.record_compaction_bytes(50_000_000, 40_000_000);
    }

    // Record cache activity
    for i in 0..100 {
        if i % 3 == 0 {
            metrics.record_cache_hit();
        } else {
            metrics.record_cache_miss();
        }
    }

    // Record WAL activity
    for _ in 0..50 {
        metrics.record_wal_write(1024);
    }
    for _ in 0..5 {
        metrics.record_wal_sync();
    }

    // Print summary
    println!("=== Operation Metrics ===");
    println!("Total Operations: {}", metrics.total_ops());
    println!("Read Operations: {}", metrics.read_ops.load(std::sync::atomic::Ordering::Relaxed));
    println!("Write Operations: {}", metrics.write_ops.load(std::sync::atomic::Ordering::Relaxed));
    println!("Delete Operations: {}", metrics.delete_ops.load(std::sync::atomic::Ordering::Relaxed));

    println!("\n=== Latency Metrics ===");
    println!("Read Latency (avg): {} ns", metrics.read_latency_ns.avg_nanos());
    println!("Write Latency (avg): {} ns", metrics.write_latency_ns.avg_nanos());
    println!("Read Latency (max): {} ns", metrics.read_latency_ns.max_nanos());

    println!("\n=== Storage Metrics ===");
    println!("Total Bytes Written: {}", metrics.total_bytes_written.load(std::sync::atomic::Ordering::Relaxed));
    println!("Total Bytes Read: {}", metrics.total_bytes_read.load(std::sync::atomic::Ordering::Relaxed));

    println!("\n=== Compaction Metrics ===");
    println!("Compaction Runs: {}", metrics.compaction_runs.load(std::sync::atomic::Ordering::Relaxed));
    println!("Compaction Bytes Read: {}", metrics.compaction_bytes_read.load(std::sync::atomic::Ordering::Relaxed));
    println!("Compaction Bytes Written: {}", metrics.compaction_bytes_written.load(std::sync::atomic::Ordering::Relaxed));
    println!("Compaction Ratio: {:.2}", metrics.compaction_ratio());
    println!("Compaction Duration (samples): {}", metrics.compaction_duration_ns.count());

    println!("\n=== Cache Metrics ===");
    println!("Cache Hits: {}", metrics.cache_hits.load(std::sync::atomic::Ordering::Relaxed));
    println!("Cache Misses: {}", metrics.cache_misses.load(std::sync::atomic::Ordering::Relaxed));
    println!("Cache Hit Rate: {:.2}%", metrics.cache_hit_rate() * 100.0);

    println!("\n=== WAL Metrics ===");
    println!("WAL Writes: {}", metrics.wal_writes.load(std::sync::atomic::Ordering::Relaxed));
    println!("WAL Syncs: {}", metrics.wal_syncs.load(std::sync::atomic::Ordering::Relaxed));
    println!("WAL Bytes Written: {}", metrics.wal_bytes_written.load(std::sync::atomic::Ordering::Relaxed));
}
