# Benchmarks

This repo uses [Criterion](https://bheisler.github.io/criterion.rs/book/) for micro- and subsystem-level benchmarking, and [cntryl-stress](https://github.com/cntryl/cntryl-stress) for system-level and workload stress tests.

## Running Benchmarks

### Criterion Benchmarks (Tier 1-2)

- Run the whole suite:

  ```bash
  cargo bench
  ```

- Run a single benchmark target:

  ```bash
  cargo bench --bench tier1_hotpath_api
  ```

- Run with filters (Criterion):

  ```bash
  cargo bench --bench tier1_hotpath_api -- "get"
  ```

- Compare results against baseline:

  ```bash
  # Save baseline before changes
  cargo bench --bench tier1_hotpath_api -- --save-baseline before

  # Make changes...

  # Compare against baseline
  cargo bench --bench tier1_hotpath_api -- --baseline before
  ```

### Stress Tests (Tier 3-4)

Tier 3 and 4 benchmarks are stress tests that measure system behavior under realistic workloads. They use the `cntryl-stress` harness for automated test discovery and execution.

**Install stress tool:**

```bash
cargo install cntryl-stress
```

**Run all stress tests:**

```bash
cargo stress
```

**Run with verbose output:**

```bash
cargo stress -v
```

**Run specific stress test (by function name):**

```bash
# YCSB workload A with 4 clients
cargo stress tier4_ycsb_a_mem_4_clients

# YCSB workload F (read-modify-write) with 8 clients
cargo stress tier4_ycsb_f_local_8_clients

# All YCSB workload tests
cargo stress tier4_ycsb

# All tier 3 system tests
cargo stress tier3_system

# Durability tests
cargo stress tier4_system_durability
```

**Stress test duration:**

Tier 3 and 4 tests are longer-running than Tier 1-2:

- Tier 3 system tests: ~5-10 minutes each
- Tier 4 workload tests: ~10-30 minutes each

## Benchmark Organization

Benchmarks live in `benches/` and are organized by "tiers":

### Tier 1: Hotpath Microbenchmarks

Tight inner-loop microbenchmarks of critical components. Measure individual operations in isolation.

**Examples:**
- `tier1_hotpath_api.rs` — Get, Put, Delete operations
- `tier1_hotpath_memtable.rs` — MemTable writes and reads
- `tier1_hotpath_bloom.rs` — Bloom filter operations
- `tier1_hotpath_sst.rs` — SST reads and seeks
- `tier1_hotpath_compression.rs` — Compression/decompression
- `tier1_hotpath_wal.rs` — WAL writes

**Typical runtime:** 1-10 seconds each

### Tier 2: Subsystem Benchmarks

End-to-end behavior for a subsystem. Measure how components interact.

**Examples:**
- `tier2_subsystem_block_cache.rs` — Cache performance under load
- `tier2_subsystem_memtable_rotate.rs` — Flush behavior
- `tier2_subsystem_iterator_multi_sst.rs` — Range scans across SSTs
- `tier2_subsystem_bloom_build.rs` — Bloom filter construction
- `tier2_subsystem_read_amplification.rs` — Read efficiency

**Typical runtime:** 5-30 seconds each

### Tier 3: System Tests

Larger scenarios measuring engine behavior under realistic conditions. Stress tests using cntryl-stress.

**Examples:**
- `tier3_system_engine.rs` — Basic engine operations under sustained load
- `tier3_system_compaction.rs` — Compaction behavior and efficiency
- `tier3_system_mvcc.rs` — Snapshot isolation under concurrent load
- `tier3_system_recovery.rs` — Recovery time and correctness
- `tier3_system_durability.rs` — Durability guarantees validation

**Typical runtime:** 5-10 minutes each

**Purpose:** Validate engine behavior doesn't degrade under sustained realistic workloads.

### Tier 4: Workload Tests

Full system benchmarks with realistic access patterns. Standard workload suites (YCSB).

**Examples:**
- `tier4_ycsb_workload_a.rs` — 50% read, 50% update (balanced)
- `tier4_ycsb_workload_b.rs` — 95% read, 5% update (read-heavy)
- `tier4_ycsb_workload_c.rs` — 100% read (cache-heavy)
- `tier4_ycsb_workload_d.rs` — Skewed read/update distribution
- `tier4_ycsb_workload_e.rs` — Scan-heavy (95% scan, 5% insert)
- `tier4_ycsb_workload_f.rs` — Read-modify-write operations
- `tier4_system_engine_batch_throughput.rs` — Batch insert throughput
- `tier4_streaming_workload.rs` — Streaming data patterns
- `tier4_system_durability_cloud.rs` — Cloud mode durability

**Typical runtime:** 10-30 minutes each

**Purpose:** Compare performance against standard benchmarks and track regression over time.

## Tier 1-2 Microbenchmark Rules (Critical)

To keep results stable and meaningful:

1. **Precompute all data outside `b.iter(|| ...)`**
   - File system initialization
   - Data generation with deterministic RNG
   - Index/lookup table construction

2. **No allocations inside hot loop**
   - Pre-allocate buffers before `b.iter()`
   - Reuse data structures across iterations
   - Use `black_box` to prevent compiler optimizations from skewing results

3. **No I/O or network in hot loop**
   - Cold paths (filesystem, network) go outside `b.iter()`
   - Hot paths measure only in-memory operations
   - File reads should be cached before timing

4. **Use deterministic randomness**
   - Fixed seed for RNG (not current time)
   - Same seed every run produces same data
   - Enables baseline comparisons

5. **Use `black_box()` on inputs/outputs**
   - Prevents compiler from optimizing based on constant propagation
   - Ensures benchmark measures actual code path, not compile-time evaluation
   - Example: `let result = black_box(engine.get(key))?;`

6. **Configure sampling correctly**
   - Use `SamplingMode::Flat` for variable execution times
   - Set appropriate `throughput()` for I/O operations
   - Example: `group.throughput(Throughput::Bytes(block_size as u64))`

### Criterion Configuration

Common Criterion configuration helpers live in `benches/criterion_helper.rs`:

```rust
// Criterion setup example
let mut criterion = Criterion::default();

// Configure sampling for flat distribution (preferred for variable workloads)
criterion.configure_from_args()
    .sampling_mode(SamplingMode::Flat)
    .measurement_time(Duration::from_secs(30))
    .warm_up_time(Duration::from_secs(5))
    .sample_size(100);

// Measure throughput for I/O-bound operations
group.throughput(Throughput::Bytes(data_size as u64));
```

## Tier 3-4 Stress Test Rules

Stress tests measure behavior under load and have different rules than microbenchmarks:

1. **Realistic workload patterns**
   - Use YCSB or actual operation distributions
   - Include proper transaction boundaries
   - Measure end-to-end latency including all overhead

2. **Deterministic scenarios**
   - Fixed operation counts (not time-based)
   - Reproducible data patterns
   - Same initial state across runs

3. **Validate correctness during benchmark**
   - Verify data consistency
   - Check recovery works
   - Validate durability guarantees

4. **Measure realistic metrics**
   - Throughput (ops/sec)
   - Latency percentiles (p50, p95, p99)
   - Read/write amplification
   - Resource usage (memory, CPU)

## Interpreting Results

### Criterion Microbenchmarks

Criterion reports:

- **time**: Mean execution time per iteration
- **std. dev**: Standard deviation (lower is better, indicates stable behavior)
- **throughput**: Operations/bytes per second
- **confidence interval**: 95% CI around the mean

**Healthy indicators:**
- Low standard deviation (<5% of mean)
- Stable across runs
- Throughput matches expected capacity

**Concerns:**
- High std. dev (>10%): Unstable, check for system load or cache effects
- Results drift run-to-run: May indicate memory fragmentation or GC pressure
- Throughput much lower than expected: Possible regression

### Stress Test Results (Tier 3-4)

Stress tests report:

- **throughput**: Operations per second under sustained load
- **latency percentiles**: p50, p95, p99, p99.9 in milliseconds
- **resource usage**: Memory, CPU utilization
- **compaction stats**: Files created, levels touched
- **correctness validation**: Pass/fail on durability checks

**Healthy indicators:**
- Throughput stable over time (no degradation under load)
- Latency percentiles within acceptable bounds
- Compaction efficiency (few levels, reasonable file counts)
- All correctness checks pass

**Concerns:**
- Throughput degradation over time: Possible compaction backlog
- High p99 latency: Long-tail pauses, possible GC or I/O stalls
- Read amplification increasing: Level structure imbalanced
- Correctness failures: Data loss or recovery bugs

## Common Workflows

### Validate Before Benchmarking

Always verify correctness first:

```bash
# Run full test suite
cargo test

# Run integration tests specifically
cargo test --test '*'

# Validate test structure (required)
python ./scripts/validate_tests.py --summary
```

### Benchmark Comparisons

When comparing benchmark results:

1. **Same machine**: Comparisons should be run on the same hardware. Avoid comparing results from different machines or cloud instances.

2. **Control environment**: Close other applications, disable frequency scaling, minimize background load.

3. **Multiple runs**: Run benchmarks at least 3 times, report mean + std. dev.

4. **Look for systematic differences**:
   - Warmup effects (first run slower due to cold caches)
   - Filesystem cache effects (repeat runs cached, first run not)
   - Memory fragmentation (longer running benchmarks)
   - System load variations

5. **Statistical significance**: Criterion shows confidence intervals. Changes <5% may be noise, not real regression.

### Stress Test Comparisons

For Tier 3-4 stress tests:

- **Compare on identical hardware**: Cloud instance type, disk speed, CPU generation
- **Same background load**: Run benchmarks with similar system state each time
- **Consistent warmup**: Cold start vs warm cache can show 2-5x differences
- **Multiple iterations of same workload**: Report median + range across runs

Example comparison script:

```bash
#!/bin/bash
# Compare before/after for stress test

echo "Before optimization:"
for i in {1..3}; do
    echo "  Run $i:"
    cargo stress tier3_system_engine 2>&1 | grep -E "(throughput|latency|p99)"
done

# Make optimization...

echo "After optimization:"
for i in {1..3}; do
    echo "  Run $i:"
    cargo stress tier3_system_engine 2>&1 | grep -E "(throughput|latency|p99)"
done
```

### Baseline Comparisons for Criterion

```bash
# Benchmark memtable operations
cargo bench --bench tier1_hotpath_memtable

# Save baseline
cargo bench --bench tier1_hotpath_memtable -- --save-baseline before

# Make optimization...

# Compare
cargo bench --bench tier1_hotpath_memtable -- --baseline before
```

### Validate System Performance

```bash
# Run tier 2 subsystem tests
cargo bench --bench tier2_subsystem_block_cache
cargo bench --bench tier2_subsystem_memtable_rotate

# Run individual tier 3 system tests
cargo stress tier3_system_engine
cargo stress tier3_system_compaction
cargo stress tier3_system_recovery
```

### Run Full Workload Benchmark

```bash
# Run YCSB workload A (balanced 50/50 read/write)
cargo stress tier4_ycsb_workload_a

# Run all YCSB workloads
cargo stress tier4_ycsb

# Run with verbose output
cargo stress tier4_ycsb -v
```

### Performance Regression Detection

```bash
# Before changes (using Criterion baseline)
cargo bench --bench tier1_hotpath_api -- --save-baseline baseline

# Make changes (optimization or refactoring)
# ...

# After changes
cargo bench --bench tier1_hotpath_api -- --baseline baseline

# Criterion will show % change and statistical significance
```

### Profile a Benchmark

```bash
# Generate flame graph (Linux with flamegraph support)
cargo flamegraph --bench tier1_hotpath_api

# Profile with perf
perf record -g ./target/release/deps/tier1_hotpath_api-*
perf report

# macOS with Instruments
xcrun xctrace record --template "System Trace" ./target/release/deps/tier1_hotpath_api-*
```

## Notes for contributors

- If you introduce a new hotpath, consider adding a Tier 1 benchmark.
- Keep benchmark names descriptive and stable; they become part of long-term performance tracking.
- Run `cargo stress -v` before submitting PRs with performance changes to catch regressions.
