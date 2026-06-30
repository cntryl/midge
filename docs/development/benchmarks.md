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

**Run all stress tests:**

```bash
cargo bench --bench 'tier*'
```

**Run a specific benchmark suite:**

```bash
cargo bench --bench tier3_system_engine
cargo bench --bench tier4_ycsb_workload_a
```

**Run with configuration:**

```bash
# Specify number of runs and warmup
cargo bench --bench tier3_system_engine -- --runs 3 --warmup 1

# Filter benchmarks by name pattern
cargo bench --bench tier4_ycsb_workload_a -- --workload "*read*"

# List available benchmarks without running
cargo bench --bench tier4_system_compaction -- --list

# Verbose output
cargo bench --bench tier3_system_engine -- --verbose
```

**Configuration via environment variables:**

```bash
BENCH_RUNS=3 BENCH_WARMUP=1 cargo bench --bench tier3_system_engine
```

**Command-line arguments** (pass with `--` separator):

```bash
cargo bench --bench tier3_system_engine -- --runs 5 --warmup 2
```

Supported flags:

- `--runs <N>` — Number of measurement runs (reports median)
- `--warmup <N>` — Warmup runs to discard before measuring
- `--workload <PATTERN>` — Filter tests by glob pattern
- `--verbose`, `-v` — Verbose output
- `--list` — List benchmarks without running
- `--include-ignored` — Include `#[stress_test(ignore)]` tests
- `--baseline <PATH>` — Compare against baseline JSON
- `--threshold <FLOAT>` — Regression threshold (default: 0.05)
- `--output-dir <PATH>` — Custom output directory

Important: The `--` separator is required to pass arguments to the stress harness.

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

Stress tests use `#[stress_test]` macros and measure behavior under load with different rules than microbenchmarks:

1. **Macro-based test definition**
   - Mark tests with `#[stress_test]`
   - Use `StressContext` for explicit timing: `ctx.measure(|| { ... })`
   - Setup/teardown outside `ctx.measure()` to exclude from timing
   - Call `stress_main!()` once per benchmark suite for auto-discovery

2. **Realistic workload patterns**
   - Use YCSB or actual operation distributions
   - Include proper transaction boundaries
   - Measure end-to-end latency including all overhead

3. **Deterministic scenarios**
   - Fixed operation counts (not time-based)
   - Reproducible data patterns
   - Same initial state across runs

4. **Validate correctness during benchmark**
   - Verify data consistency
   - Check recovery works
   - Validate durability guarantees

5. **Throughput tracking**
   - Call `ctx.set_bytes(n)` for bytes/sec reporting
   - Call `ctx.set_elements(n)` for ops/sec reporting
   - Add metadata with `ctx.tag(key, val)`

6. **Measure realistic metrics**
   - Throughput (ops/sec or bytes/sec)
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

Stress tests save results to `target/stress/{suite}/{timestamp}.{json,txt}`:

**Console output:**
- **name**: Test name and duration
- **throughput**: Bytes/sec (if `ctx.set_bytes()` called) or ops/sec (if `ctx.set_elements()` called)
- **total time**: Time to run entire suite

**JSON output** (`target/stress/{suite}/latest.json`):
- Suite metadata (git SHA, timestamp, run count)
- Per-test results with duration and throughput
- Total suite duration

**Text output** (`target/stress/{suite}/latest.txt`):
- Human-readable format with all metrics
- Git SHA and timestamp for tracking

**Additional metrics to measure:**
- **resource usage**: Memory, CPU utilization
- **compaction stats**: Files created, levels touched (add with `ctx.tag()`)
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
cntryl-tools validate-tests
```

### Benchmark Comparisons

When comparing benchmark results:

1. **Same machine**: Comparisons should be run on the same hardware. Avoid comparing results from different machines or cloud instances.

2. **Control environment**: Close other applications, disable frequency scaling, minimize background load.

3. **Multiple runs**: Use `--runs` flag to run benchmarks at least 3 times, report median + std. dev.
   ```bash
   cargo bench --bench tier1_hotpath_api -- --runs 3
   ```

4. **Look for systematic differences**:
   - Warmup effects (first run slower due to cold caches)
   - Filesystem cache effects (repeat runs cached, first run not)
   - Memory fragmentation (longer running benchmarks)
   - System load variations

5. **Statistical significance**: For Criterion, changes <5% may be noise, not real regression. For stress tests, use `--threshold` flag:
   ```bash
   cargo bench --bench tier3_system_engine -- --baseline baseline.json --threshold 0.05
   ```

### Stress Test Comparisons

For Tier 3-4 stress tests:

- **Compare on identical hardware**: Cloud instance type, disk speed, CPU generation
- **Same background load**: Run benchmarks with similar system state each time
- **Consistent warmup**: Use `--warmup` to discard warmup iterations; cold start vs warm cache can show 2-5x differences
- **Multiple iterations of same workload**: Use `--runs` to get multiple measurements, report median + range across runs
- **Regression detection**: Use `--baseline` and `--threshold` flags:
  ```bash
  # Run and save baseline
  cargo bench --bench tier3_system_engine -- --runs 3 --warmup 1
  
  # Later, compare against it
  cargo bench --bench tier3_system_engine -- --baseline target/stress/tier3_system_engine/latest.json --threshold 0.05
  ```

Example comparison script:

```bash
#!/bin/bash
# Compare before/after for stress test

echo "Before optimization:"
for i in {1..3}; do
    echo "  Run $i:"
    cargo bench --bench tier3_system_engine 2>&1 | grep -E "(throughput|time)"
done

# Make optimization...

echo "After optimization:"
for i in {1..3}; do
    echo "  Run $i:"
    cargo bench --bench tier3_system_engine 2>&1 | grep -E "(throughput|time)"
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
# Run Criterion tier 2 subsystem tests
cargo bench --bench tier2_subsystem_block_cache
cargo bench --bench tier2_subsystem_memtable_rotate

# Run individual tier 3 system stress tests
cargo bench --bench tier3_system_engine
cargo bench --bench tier3_system_compaction
cargo bench --bench tier3_system_recovery

# Run with warmup and multiple runs
cargo bench --bench tier3_system_engine -- --runs 3 --warmup 2
```

### Run Full Workload Benchmark

```bash
# Run YCSB workload A (balanced 50/50 read/write)
cargo bench --bench tier4_ycsb_workload_a

# Run all YCSB workloads (using Cargo glob pattern)
cargo bench --bench 'tier4_ycsb*'

# Run with custom configuration
cargo bench --bench tier4_ycsb_workload_a -- --runs 3 --warmup 2

# Run with baseline comparison for regression detection
cargo bench --bench tier4_ycsb_workload_a -- --baseline /path/to/baseline.json --threshold 0.10
```

### Performance Regression Detection

**For Criterion (Tier 1-2):**

```bash
# Before changes
cargo bench --bench tier1_hotpath_api -- --save-baseline baseline

# Make changes (optimization or refactoring)
# ...

# After changes
cargo bench --bench tier1_hotpath_api -- --baseline baseline

# Criterion will show % change and statistical significance
```

**For Stress Tests (Tier 3-4):**

```bash
# Save baseline before changes
cargo bench --bench tier3_system_engine -- --runs 3 --warmup 1
# Results saved to target/stress/tier3_system_engine/latest.json

# Make changes...

# Compare against baseline
cargo bench --bench tier3_system_engine -- --baseline target/stress/tier3_system_engine/latest.json --threshold 0.05

# cntryl-stress will report % change and flag regressions >5%
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

## Writing Stress Tests (Tier 3-4)

Stress tests use the `#[stress_test]` macro and `StressContext` API:

```rust
use cntryl_stress::{stress_test, stress_main, StressContext};

#[stress_test]
fn compaction_throughput(ctx: &mut StressContext) {
    // Setup outside measurement
    let engine = Engine::new(config).unwrap();
    let test_data = generate_test_data(10_000);
    
    // Record throughput (bytes processed)
    ctx.set_bytes((test_data.len() * test_data[0].len()) as u64);
    
    // Measure the operation
    ctx.measure(|| {
        engine.compact().unwrap();
    });
    
    // Add metadata
    ctx.tag("workload", "compaction");
    ctx.tag("data_size", "100MB");
}

#[stress_test]
fn recovery_time(ctx: &mut StressContext) {
    let engine = Engine::new(config).unwrap();
    
    // Setup: write some data
    for i in 0..1_000_000 {
        engine.put(format!("key{}", i), vec![0u8; 100]).unwrap();
    }
    engine.close().unwrap();
    
    // Measure recovery operation
    ctx.measure(|| {
        let _ = Engine::open(config).unwrap();
    });
    
    ctx.tag("recovery_type", "cold_start");
}

#[stress_test(ignore)]  // Use (ignore) to skip a test
fn expensive_operation(ctx: &mut StressContext) {
    // This won't run by default
    // Run with: cargo bench -- --include-ignored
}

stress_main!();  // Required once per bench file for auto-discovery
```

**In `Cargo.toml`, mark the benchmark as a stress test:**

```toml
[[bench]]
name = "tier3_system_engine"
path = "benches/tier3_system_engine.rs"
harness = false  # Important: use cntryl-stress harness, not Criterion
```

**Key API:**

- `ctx.measure(|| { ... })` — Time one operation, excluding setup/teardown
- `ctx.set_bytes(n)` — Enable bytes/sec throughput reporting
- `ctx.set_elements(n)` — Enable ops/sec throughput reporting
- `ctx.tag(key, val)` — Add metadata (workload name, configuration, etc.)
- `#[stress_test]` — Mark function as a test
- `#[stress_test(ignore)]` — Skip test by default
- `stress_main!()` — Auto-discover and run all `#[stress_test]` functions

## Regression Detection & Signal Discipline

### Criterion Benchmarks (Tier 1-2): Explicit Thresholds

**Problem:** Criterion's default behavior requires manual baseline comparison. Without explicit thresholds, operators can't automatically flag regressions in CI.

**Solution:** Set explicit regression thresholds per tier:

| Tier | Threshold | Rationale |
|------|-----------|-----------|
| **Tier 1** | **±5% latency** | Sub-microsecond ops; high precision needed. A 5% increase is meaningful. |
| **Tier 2** | **±8-10% latency** | Subsystem-level; more variance due to component interaction. |

**Usage:**

```bash
# Save baseline before optimization
cargo bench --bench tier1_hotpath_api -- --save-baseline before

# Make changes...

# Compare (Criterion will show difference vs baseline)
cargo bench --bench tier1_hotpath_api -- --baseline before --verbose
```

**Tip:** If Criterion reports >5% regression, investigate before merging.

### Stress Tests (Tier 3-4): Multi-Run Regression Detection

**Problem:** Stress tests run 1-3 times by default. With high variance (10-15%), single runs can't reliably detect real regressions.

**Solution:**

1. **Increase sample count for regression-critical tests:**
   ```bash
   BENCH_RUNS=5 cargo bench --bench tier4_ycsb_workload_b
   ```

2. **Track std. dev, not just median:**
   - Record all runs in CI artifacts
   - Compute statistical significance (e.g., >2σ change is likely real)

3. **Set thresholds based on metric type:**
   - **Throughput:** Flag if >10-15% drop sustained across ≥2 runs
   - **p99 Latency:** Flag if >20% increase (tail latencies have higher variance)
   - **Correctness:** Flag if any isolation violations detected

4. **For MVCC/concurrent tests:**
   - Measure fairness: writer latency under snapshot hold
   - Flag if writer p99 increases >50% under contention

### Example: CI Regression Thresholds

Add to your CI workflow or benchmark tracking:

```bash
# Pseudo-code for regression detection

BENCH_RUNS=3 cargo bench --bench tier4_ycsb_workload_b > bench_output.txt

throughput=$(grep "throughput:" bench_output.txt | tail -1 | awk '{print $2}')
prev_throughput=<baseline>

percent_change=$(( (throughput - prev_throughput) * 100 / prev_throughput ))

if [ ${percent_change#-} -gt 15 ]; then
  echo "REGRESSION: Throughput dropped by ${percent_change}%"
  exit 1
fi
```

### Latency Distribution Tracking (Future)

Current stress tests report throughput only. To improve signal, future updates should track latency percentiles:

```rust
// Pseudo-code: Future latency tracking in stress tests

let mut latencies = Vec::new();
ctx.measure(|| {
    let start = Instant::now();
    // ... operation ...
    latencies.push(start.elapsed().as_micros());
});

latencies.sort_unstable();
let p50 = latencies[latencies.len() / 2];
let p99 = latencies[latencies.len() * 99 / 100];
let max = latencies[latencies.len() - 1];

ctx.tag("latency_p50_us", &p50.to_string());
ctx.tag("latency_p99_us", &p99.to_string());
ctx.tag("latency_max_us", &max.to_string());
```

This enables detection of:
- Long-tail latency regressions (p99 jumps due to compaction)
- Fairness issues (writer latency under snapshot load)
- Tail amplification (e.g., multi-threaded scenarios where one thread blocks others)

## CI Strategy: Fast Lane vs Extended Lane

### Fast Lane (every push to main)

Runs per-push. Target: <2 hours total (all platforms).

- **Tier 1-2:** All hotpath + subsystem tests
- **Tier 3:** All system tests
- **Tier 4:** Core signal only (B, D, E, F, batch, recovery)

**Rationale:** Fast feedback for regression detection without burning CI hours.

### Extended Lane (nightly / on demand)

Runs on schedule or with `[bench-extended]` commit message.

- **Tier 2:** Compression subsystem (less critical)
- **Tier 4:** Exploratory workloads (A, C, streaming, compaction_throughput, cloud_durability_base)
- **Tier 4:** NEW high-priority tests (cloud durability modes, MVCC isolation under concurrency)

**Rationale:** Deeper validation, exploratory patterns, failure modes.

## Notes for contributors

- If you introduce a new hotpath, consider adding a Tier 1 benchmark.
- Keep benchmark names descriptive and stable; they become part of long-term performance tracking.
- Run `cargo bench` before submitting PRs with performance changes to catch regressions.
- For new Tier 3-4 tests, use the `#[stress_test]` macro and call `stress_main!()` once per benchmark suite.
- Always call `ctx.measure()` only around the operation being timed; put setup/teardown outside.
- **For MVCC or concurrent tests:** Validate isolation correctness and measure writer fairness under contention.
- **For cloud tests:** Test failure modes (transient errors, partial uploads) not just happy-path throughput.
