//! Smart Configuration Example
//!
//! Demonstrates the intelligent configuration system with automatic parameter derivation.

use cntryl_midge::{Durability, Goal, MemoryBudget, MidgeResult, OpenOptions, WorkloadProfile};

fn main() -> MidgeResult<()> {
    println!("=== Midge Smart Configuration Examples ===\n");

    // Example 1: Simple latency-optimized config
    println!("1. Latency-optimized (just 2 knobs):");
    let opts = OpenOptions::new()
        .path("./latency_db")
        .goal(Goal::Latency)
        .build();

    println!("   Block size: {} KB", opts.block_size() / 1024);
    println!(
        "   Memtable size: {} MB",
        opts.memtable_size_limit() / 1024 / 1024
    );
    println!(
        "   Cache size: {} MB",
        opts.block_cache_size() / 1024 / 1024
    );
    println!("   WAL sync: {}", opts.wal_sync_on_write());
    println!();

    // Example 2: Throughput-optimized with write-heavy workload
    println!("2. Throughput-optimized, write-heavy:");
    let opts = OpenOptions::new()
        .path("./throughput_db")
        .goal(Goal::Throughput)
        .workload(WorkloadProfile::WriteHeavy)
        .build();

    println!("   Block size: {} KB", opts.block_size() / 1024);
    println!(
        "   Memtable size: {} MB",
        opts.memtable_size_limit() / 1024 / 1024
    );
    println!(
        "   Cache size: {} MB",
        opts.block_cache_size() / 1024 / 1024
    );
    println!("   L0 compaction trigger: {}", opts.l0_compaction_trigger());
    println!();

    // Example 3: Cost-optimized with strict durability
    println!("3. Cost-optimized, strict durability:");
    let opts = OpenOptions::new()
        .path("./cost_db")
        .goal(Goal::Cost)
        .durability(Durability::Strict)
        .build();

    println!("   Block size: {} KB", opts.block_size() / 1024);
    println!(
        "   Memtable size: {} MB",
        opts.memtable_size_limit() / 1024 / 1024
    );
    println!(
        "   Cache size: {} MB",
        opts.block_cache_size() / 1024 / 1024
    );
    println!("   WAL sync: {} (every write!)", opts.wal_sync_on_write());
    println!();

    // Example 4: Read-mostly workload with explicit memory budget
    println!("4. Read-mostly, 2GB memory budget:");
    let opts = OpenOptions::new()
        .path("./read_db")
        .goal(Goal::Latency)
        .memory_budget(MemoryBudget::Bytes(2 * 1024 * 1024 * 1024))
        .workload(WorkloadProfile::ReadMostly)
        .build();

    println!("   Block size: {} KB", opts.block_size() / 1024);
    println!(
        "   Memtable size: {} MB",
        opts.memtable_size_limit() / 1024 / 1024
    );
    println!(
        "   Cache size: {} MB (70% of budget for reads)",
        opts.block_cache_size() / 1024 / 1024
    );
    println!();

    // Example 5: Range scan workload
    println!("5. Range scan optimized:");
    let opts = OpenOptions::new()
        .path("./scan_db")
        .goal(Goal::Throughput)
        .workload(WorkloadProfile::RangeScan)
        .build();

    println!(
        "   Block size: {} KB (large for sequential reads)",
        opts.block_size() / 1024
    );
    println!(
        "   Memtable size: {} MB",
        opts.memtable_size_limit() / 1024 / 1024
    );
    println!();

    println!("Key Insight: Only 3 questions needed!");
    println!("  1. Goal: Latency | Throughput | Cost");
    println!("  2. Durability: Strict | Steady | CloudReplicated");
    println!("  3. Memory: Auto | Bytes(n)");
    println!();
    println!("All other parameters derived automatically!");

    Ok(())
}
