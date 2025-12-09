# Phase 8.4: Performance Benchmarking & Validation

## Overview

This document describes the comprehensive benchmarking suite for validating Midge's performance characteristics against the Phase 8 baselines and ensuring no regression from Phases 5-7.

---

## Benchmark Tier Structure

Midge benchmarks are organized into 6 tiers, each validating specific performance aspects:

### Tier 1: Hotpath (Core Operations) - ~30 seconds

**Focus**: Microsecond-level performance of fundamental operations.

**Benchmarks**:

```rust
// benches/tier1_hotpath/point_operations.rs

#[bench]
fn write_single_key_memory(b: &mut Bencher) {
    let engine = setup_memory_engine(4MB_memtable);
    let cf = engine.default_column_family();
    
    b.iter_with_setup(
        || random_key_value(100),
        |(key, value)| {
            engine.put(&cf, &key, &value)
        }
    );
}

#[bench]
fn read_cached_key(b: &mut Bencher) {
    let engine = setup_engine_with_cache(256MB);
    let cf = engine.default_column_family();
    
    // Warm cache
    for i in 0..10000 {
        engine.put(&cf, &format!("key_{}", i).into_bytes(), b"value")
            .ok();
    }
    engine.flush();
    
    b.iter(|| {
        engine.get(&cf, b"key_5000")
    });
}

#[bench]
fn sequential_write_throughput(b: &mut Bencher) {
    let engine = setup_memory_engine(64MB_memtable);
    let cf = engine.default_column_family();
    
    b.iter_with_setup(
        || Vec::with_capacity(1000),
        |mut batch| {
            for i in 0..1000 {
                let key = format!("key_{:06}", i);
                engine.put(&cf, key.as_bytes(), b"value")
                    .expect("write failed");
            }
        }
    );
}
```

**Target Results**:
- Single key write: < 1µs
- Cached key read: < 2µs
- Sequential write: > 1M ops/sec

**Running**: `cargo bench --bench tier1_hotpath`

---

### Tier 2: Subsystem (Flush, Compaction) - ~2 minutes

**Focus**: Flush and compaction performance at subsystem level.

**Benchmarks**:

```rust
// benches/tier2_subsystem/flush_performance.rs

#[bench]
fn flush_1mb_memtable(b: &mut Bencher) {
    b.iter_with_setup(
        || setup_engine_with_data(1024 * 1024),  // 1MB data
        |engine| {
            let cf = engine.default_column_family();
            engine.flush()
                .expect("flush failed")
        }
    );
}

#[bench]
fn flush_64mb_memtable(b: &mut Bencher) {
    b.iter_with_setup(
        || setup_engine_with_data(64 * 1024 * 1024),  // 64MB data
        |engine| {
            let cf = engine.default_column_family();
            engine.flush()
                .expect("flush failed")
        }
    );
}

#[bench]
fn compaction_l0_to_l1(b: &mut Bencher) {
    b.iter_with_setup(
        || {
            let engine = setup_engine();
            // Create multiple L0 files
            for batch in 0..5 {
                for i in 0..10000 {
                    let key = format!("key_{:06}", batch * 10000 + i);
                    engine.put(&cf, key.as_bytes(), b"value").ok();
                }
                engine.flush();
            }
            engine
        },
        |engine| {
            let cf = engine.default_column_family();
            engine.compact_level(&cf, 0)
                .expect("compaction failed")
        }
    );
}

#[bench]
fn write_amplification_validation(b: &mut Bencher) {
    // Verify write amplification doesn't exceed expected bounds
    // Phase 5 (segments) should improve this
    
    let engine = setup_engine();
    let cf = engine.default_column_family();
    
    let disk_before = total_sst_bytes(&engine);
    
    b.iter(|| {
        for i in 0..100000 {
            let key = format!("key_{:06}", i);
            engine.put(&cf, key.as_bytes(), b"value")
                .expect("write failed");
        }
        engine.flush();
    });
    
    let disk_after = total_sst_bytes(&engine);
    let bytes_written = 100000 * (key_size + value_size);
    let write_amp = (disk_after - disk_before) as f64 / bytes_written as f64;
    
    // Expected: 10-20x (Phase 5 segments should reduce this)
    assert!(write_amp < 20.0, 
        "Write amplification {} exceeds expected bounds",
        write_amp
    );
}
```

**Target Results**:
- Flush 1MB: < 50ms
- Flush 64MB: < 500ms
- Compaction L0→L1: < 2 seconds
- Write amplification: 10-20x

**Running**: `cargo bench --bench tier2_subsystem`

---

### Tier 3: System Integration - ~5 minutes

**Focus**: Integrated system behavior under mixed workloads.

**Benchmarks**:

```rust
// benches/tier3_system/mixed_workload.rs

#[bench]
fn read_heavy_workload(b: &mut Bencher) {
    // 95% reads, 5% writes - typical OLTP pattern
    let engine = setup_populated_engine(1M_keys);
    let cf = engine.default_column_family();
    
    b.iter(|| {
        for _ in 0..10000 {
            // 95% reads
            for _ in 0..95 {
                let key = random_key(1M_keys);
                engine.get(&cf, &key).ok();
            }
            
            // 5% writes
            for _ in 0..5 {
                let key = random_key(1M_keys);
                let value = random_value(100);
                engine.put(&cf, &key, &value).ok();
            }
        }
    });
}

#[bench]
fn write_heavy_workload(b: &mut Bencher) {
    // 5% reads, 95% writes - ingest pattern
    let engine = setup_engine();
    let cf = engine.default_column_family();
    
    b.iter(|| {
        for batch in 0..100 {
            for i in 0..1000 {
                let key = format!("batch_{:05}_key_{:06}", batch, i);
                let value = random_value(1000);
                engine.put(&cf, key.as_bytes(), &value)
                    .expect("write failed");
            }
            
            // Occasional reads
            if batch % 10 == 0 {
                for _ in 0..50 {
                    engine.get(&cf, &random_key).ok();
                }
            }
        }
    });
}

#[bench]
fn concurrent_memtable_writes(b: &mut Bencher) {
    // Verify multiple concurrent memtables work efficiently
    let engine = Arc::new(setup_engine());
    
    b.iter(|| {
        let mut handles = vec![];
        
        for thread_id in 0..4 {
            let engine = Arc::clone(&engine);
            let handle = std::thread::spawn(move || {
                let cf = engine.default_column_family();
                for i in 0..10000 {
                    let key = format!("t{}_{:06}", thread_id, i);
                    engine.put(&cf, key.as_bytes(), b"value").ok();
                }
            });
            handles.push(handle);
        }
        
        for h in handles {
            h.join().ok();
        }
    });
}

#[bench]
fn range_scan_performance(b: &mut Bencher) {
    // Phase 3: Prefix trie should enable fast range scans
    let engine = setup_populated_engine(100K_keys);
    let cf = engine.default_column_family();
    
    b.iter(|| {
        let start = b"key_10000";
        let end = b"key_20000";
        let count: usize = engine.scan(&cf, Some(start), Some(end))
            .unwrap()
            .count();
        count
    });
}
```

**Target Results**:
- Read-heavy: > 500K ops/sec
- Write-heavy: > 50K ops/sec
- Concurrent writes: scales with thread count
- Range scan 10K keys: < 100ms

**Running**: `cargo bench --bench tier3_system`

---

### Tier 4: Integration & Cloud - ~10 minutes

**Focus**: Full system integration including cloud backend.

**Benchmarks**:

```rust
// benches/tier4_integration/cloud_backed.rs

#[bench]
fn local_write_with_cloud_upload(b: &mut Bencher) {
    let engine = setup_cloud_backed_engine();
    let cf = engine.default_column_family();
    
    b.iter(|| {
        for i in 0..10000 {
            let key = format!("key_{:06}", i);
            engine.put(&cf, key.as_bytes(), b"value")
                .expect("write failed");
        }
        // Cloud uploads scheduled (async, non-blocking)
        engine.flush();
    });
}

#[bench]
fn cloud_read_miss_recovery(b: &mut Bencher) {
    // Cache miss - must read from cloud (simulated latency)
    let engine = setup_cloud_backed_engine_with_simulated_latency(10ms);
    let cf = engine.default_column_family();
    
    // Populate cloud only
    for i in 0..100000 {
        let key = format!("key_{:06}", i);
        engine.put(&cf, key.as_bytes(), b"value").ok();
    }
    engine.flush();
    
    // Clear local cache to force cloud reads
    engine.clear_block_cache();
    
    b.iter(|| {
        let random_key = format!("key_{:06}", rand::random::<usize>() % 100000);
        engine.get(&cf, random_key.as_bytes()).ok()
    });
}

#[bench]
fn multi_cf_isolation(b: &mut Bencher) {
    let engine = setup_engine();
    
    let cf_default = engine.default_column_family();
    let cf_orders = engine.create_column_family("orders", Default::default()).unwrap();
    let cf_users = engine.create_column_family("users", Default::default()).unwrap();
    
    b.iter(|| {
        for i in 0..10000 {
            engine.put(&cf_default, format!("d_{}", i).as_bytes(), b"v1").ok();
            engine.put(&cf_orders, format!("o_{}", i).as_bytes(), b"v2").ok();
            engine.put(&cf_users, format!("u_{}", i).as_bytes(), b"v3").ok();
        }
    });
}
```

**Target Results**:
- Cloud write (non-blocking): < 1ms per op
- Cloud read miss: < 50ms (depends on simulated latency)
- Multi-CF throughput: > 100K ops/sec combined

**Running**: `cargo bench --bench tier4_integration`

---

### Tier 5: Soak Testing - ~30 minutes

**Focus**: Long-running stability and resource usage patterns.

**Benchmarks**:

```rust
// benches/tier5_soak/stability.rs

#[bench]
fn sustained_write_load(b: &mut Bencher) {
    let engine = setup_engine();
    let cf = engine.default_column_family();
    
    // 1 hour of sustained writes
    b.iter(|| {
        for second in 0..3600 {
            for i in 0..1000 {
                let key = format!("t_{}_k_{}", second, i);
                engine.put(&cf, key.as_bytes(), &random_value(1KB))
                    .expect("write failed");
            }
            
            // Metrics check every 60 seconds
            if second % 60 == 0 {
                verify_health(&engine);
            }
        }
    });
}

#[bench]
fn memory_stability(b: &mut Bencher) {
    let engine = setup_engine();
    let cf = engine.default_column_family();
    
    // Verify memory doesn't leak during repeated cycles
    let initial_memory = get_memory_usage();
    
    for cycle in 0..100 {
        // Write cycle
        for i in 0..10000 {
            let key = format!("c_{}_k_{}", cycle, i);
            engine.put(&cf, key.as_bytes(), b"value").ok();
        }
        engine.flush();
        
        // Read cycle
        for i in 0..10000 {
            let key = format!("c_{}_k_{}", cycle, i);
            engine.get(&cf, key.as_bytes()).ok();
        }
        
        // Memory check
        let current_memory = get_memory_usage();
        let growth = current_memory - initial_memory;
        
        // Should not grow unbounded
        assert!(growth < 100MB, 
            "Memory growth {} MB after cycle {}",
            growth / (1024 * 1024), cycle
        );
    }
}

#[bench]
fn compaction_stability(b: &mut Bencher) {
    let engine = setup_engine();
    let cf = engine.default_column_family();
    
    // Create fragmented state and verify compaction succeeds repeatedly
    for round in 0..50 {
        // Create L0 files
        for batch in 0..10 {
            for i in 0..5000 {
                let key = format!("r_{:03}_b_{}_k_{:05}", round, batch, i);
                engine.put(&cf, key.as_bytes(), &random_value(500))
                    .expect("write failed");
            }
            engine.flush();
        }
        
        // Trigger compaction
        engine.compact_level(&cf, 0)
            .expect("compaction failed");
        
        // Verify structure
        verify_lsm_invariants(&engine);
    }
}
```

**Target Results**:
- Sustained: > 1M ops/hour without errors
- Memory: < 10MB growth per 100K ops
- Compaction: Success rate 100%, LSM invariants maintained

**Running**: `cargo bench --bench tier5_soak`

---

### Tier 6: Capacity Testing - ~60 minutes

**Focus**: Behavior under extreme conditions.

**Benchmarks**:

```rust
// benches/tier6_capacity/limits.rs

#[bench]
fn large_dataset_throughput(b: &mut Bencher) {
    // 1GB dataset
    let engine = setup_disk_backed_engine();
    let cf = engine.default_column_family();
    
    b.iter(|| {
        for batch in 0..1000 {  // 1000 batches * ~1MB = 1GB
            for i in 0..100000 {
                let key = format!("b_{}_k_{}", batch, i);
                engine.put(&cf, key.as_bytes(), &random_value(10KB))
                    .expect("write failed");
            }
            
            // Flush periodically
            if batch % 10 == 0 {
                engine.flush();
            }
        }
    });
}

#[bench]
fn high_cardinality_keys(b: &mut Bencher) {
    // Very large key space
    let engine = setup_engine();
    let cf = engine.default_column_family();
    
    b.iter(|| {
        // 100M unique keys
        for i in 0..100_000_000 {
            let key = format!("key_{:08}", i);
            engine.put(&cf, key.as_bytes(), b"v").ok();
        }
    });
}

#[bench]
fn many_column_families(b: &mut Bencher) {
    let engine = setup_engine();
    
    // Create 100 column families
    let mut cfs = vec![];
    for i in 0..100 {
        let cf = engine.create_column_family(
            &format!("cf_{:03}", i),
            Default::default()
        ).unwrap();
        cfs.push(cf);
    }
    
    b.iter(|| {
        for cf in &cfs {
            for i in 0..1000 {
                let key = format!("key_{}", i);
                engine.put(cf, key.as_bytes(), b"value").ok();
            }
        }
    });
}

#[bench]
fn cache_under_pressure(b: &mut Bencher) {
    // Workload larger than cache
    let engine = setup_engine_with_cache(256MB);
    let cf = engine.default_column_family();
    
    // 500M working set
    b.iter(|| {
        for batch in 0..50 {
            for i in 0..10_000_000 {
                let key = format!("b_{}_k_{}", batch, i);
                let value = random_value(1KB);
                engine.put(&cf, key.as_bytes(), &value).ok();
            }
            
            // Hit cache at all offsets
            for offset in 0..10 {
                let read_key = format!("b_{}_k_{}", batch - offset % batch, i);
                engine.get(&cf, read_key.as_bytes()).ok();
            }
        }
    });
}
```

**Target Results**:
- 1GB throughput: > 10MB/sec
- 100M keys: Completes successfully
- 100 CFs: Linear throughput scaling
- Pressure workload: Graceful performance degradation

**Running**: `cargo bench --bench tier6_capacity`

---

## Phase 8.4 Validation Checklist

### Pre-Benchmarking

- [ ] Clean build: `cargo clean && cargo build --release`
- [ ] System baseline: Close other applications
- [ ] Environment: Consistent CPU frequency (disable turbo if inconsistent)
- [ ] Storage: Dedicated disk for benchmark I/O

### Tier 1 (Hotpath)

- [ ] Point write < 1µs
- [ ] Cached read < 2µs
- [ ] Sequential write > 1M ops/sec
- [ ] No regressions vs Phase 4 baseline

### Tier 2 (Subsystem)

- [ ] Flush 1MB < 50ms
- [ ] Flush 64MB < 500ms
- [ ] Compaction < 2 seconds
- [ ] Write amplification 10-20x (Phase 5 segments should reduce this)

### Tier 3 (System)

- [ ] Read-heavy > 500K ops/sec
- [ ] Write-heavy > 50K ops/sec
- [ ] Range scan 10K keys < 100ms
- [ ] Concurrent: Linear scaling to 4 threads

### Tier 4 (Integration)

- [ ] Cloud write non-blocking < 1ms
- [ ] Multi-CF throughput > 100K ops/sec
- [ ] No interference between CFs

### Tier 5 (Soak)

- [ ] 1M+ ops completed without errors
- [ ] Memory growth < 10MB per 100K ops
- [ ] Compaction 100% success rate

### Tier 6 (Capacity)

- [ ] 1GB throughput > 10MB/sec
- [ ] 100M keys: Successful completion
- [ ] 100 CFs: Linear scaling
- [ ] Graceful degradation under cache pressure

---

## Running All Benchmarks

```bash
# Run all tiers sequentially (expect ~2 hours total)
./scripts/run_all_benchmarks.sh

# Or individually
cargo bench --bench tier1_hotpath    # 30s
cargo bench --bench tier2_subsystem  # 2min
cargo bench --bench tier3_system     # 5min
cargo bench --bench tier4_integration # 10min
cargo bench --bench tier5_soak       # 30min
cargo bench --bench tier6_capacity   # 60min

# Generate comparison report vs baseline
./scripts/compare_baselines.sh
```

---

## Regression Detection

Performance regressions are detected by:

```rust
// Automatic comparison with previous run
if current_throughput < baseline_throughput * 0.95 {
    eprintln!("REGRESSION: {} reduced by {:.1}%",
        name,
        ((baseline - current) / baseline) * 100.0
    );
}
```

---

## Phase Baselines

### Phase 4 Baseline (Before Phase 5-7)

- Single write: ~2µs
- Cached read: ~3µs
- Sequential throughput: 500K ops/sec
- Flush 64MB: ~600ms

### Phase 5 Improvements (Mutable Segments)

- Expected improvement: 10-15% reduction in write amplification
- Flush slightly faster due to better memtable organization

### Phase 6 Improvements (Runtime Unification)

- Deterministic ordering may add <1% overhead
- Compaction coordination more predictable

### Phase 8 Baselines (After All Phases)

After running full benchmark suite, expected improvements:

- Write amplification: Reduced 15-20% vs Phase 4
- Flush latency: ±5% variance (determinism trade-off)
- Read throughput: Maintained or improved (cache benefits)
- Stability: 100% success on soak/capacity tests

---

## Benchmark Results Format

```json
{
  "phase": 8,
  "timestamp": "2025-01-16T12:30:00Z",
  "system": {
    "cpu": "Intel i7-9700K",
    "memory_gb": 32,
    "storage": "Samsung 970 EVO NVMe"
  },
  "tiers": {
    "tier1_hotpath": {
      "point_write_us": 0.95,
      "cached_read_us": 1.85,
      "sequential_write_ops_sec": 1_050_000
    },
    "tier2_subsystem": {
      "flush_1mb_ms": 45,
      "flush_64mb_ms": 480,
      "write_amplification": 15.2
    },
    // ... more tiers
  },
  "regression_check": "PASS"
}
```

---

## Continuous Integration

Add to CI pipeline:

```yaml
benchmark:
  runs-on: ubuntu-latest
  steps:
    - uses: actions/checkout@v3
    - run: cargo bench --bench tier1_hotpath
    - run: cargo bench --bench tier2_subsystem
    - uses: benchmark-action/github-action@v1
      with:
        tool: 'cargo'
        output-file-path: target/criterion/output.txt
        github-token: ${{ secrets.GITHUB_TOKEN }}
        auto-push: true
```

---

## Summary

Phase 8.4 benchmarking validates:

✅ **No regression** from Phases 5-7  
✅ **Determinism goals** met  
✅ **Performance targets** achieved  
✅ **Stability** under sustained load  
✅ **Scalability** with data/concurrency  
✅ **Production readiness**  

---

*Last Updated: December 2025 | Midge Phase 8.4 Benchmarking Guide*
