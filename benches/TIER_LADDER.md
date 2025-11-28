# Benchmark Tier Ladder

This document defines the benchmark tier system for Midge. When writing benchmarks,
test fixtures, or optimization code, **always respect these tier definitions**.

## Tier 1 — Hotpath (ns → µs)

**Purpose:** Microbenchmarks of the absolute critical path.

**Requirements:**

- Zero allocation (unless unavoidable)
- Deterministic inputs and outputs
- No sleeps, timers, async runtimes, or nondeterministic inputs
- Precompute all data outside `b.iter()`
- Use `group.sampling_mode(SamplingMode::Flat)` for fast iterations

**Examples:** Bloom filter probes, TLV encode/decode, skiplist operations,
block index lookups, memtable point lookups, WAL frame parsing, cache lookups.

**Criterion config:** 200ms warmup, 500ms measurement, 20 samples, 1.5% noise threshold.

## Tier 2 — Subsystem (µs → ms)

**Purpose:** Benchmarks of individual components in isolation.

**Requirements:**

- Represent realistic access patterns
- Test component boundaries, not full engine paths
- Use `group.sampling_mode(SamplingMode::Flat)` for fast iterations

**Examples:** Block cache operations, bloom filter construction, manifest parsing,
memtable scans/inserts, WAL append, SST block reads, segment rollover.

**Criterion config:** 300ms warmup, 1s measurement, 15 samples, 2% noise threshold.

## Tier 3 — System (ms → 10ms)

**Purpose:** Full engine operations with real I/O.

**Requirements:**

- Include full write path (memtable → WAL → flush)
- Include full read path (cache → memtable → SST)
- Return engine from timed closures to exclude teardown from timing
- Simulate realistic key/value sizes

**Examples:** Put/get through full stack, flush operations, scan queries,
CRUD operations, concurrent CF scaling, recovery scenarios.

**Criterion config:** 500ms warmup, 2s measurement, 10 samples, 3% noise threshold.

## Tier 4 — Durability (10ms → 100ms)

**Purpose:** Measure the cost of durable writes (fsync-heavy operations).

**Requirements:**

- Enable WAL sync to trigger real fsync calls
- Measure durable write latency, not buffered writes
- Include manifest updates where relevant

**Examples:** WAL sync modes, SST write with sync, manifest persistence,
checkpoint operations, durable batch commits.

**Criterion config:** 500ms warmup, 3s measurement, 8 samples, 5% noise threshold, 90% confidence.

## Tier 5 — Stress (100ms → s)

**Purpose:** Load testing, throughput measurement, and deadlock detection.

**Requirements:**

- Focus on wallclock throughput, not fine timing
- Simulate multi-threaded hot load
- Observe compaction and background worker latencies
- Progress tracking over precision

**Examples:** Concurrent writer scaling, compaction under load, high-contention
scenarios, mixed read/write throughput, background worker pressure.

**Criterion config:** 1s warmup, 10s measurement, 5 samples, 10% noise threshold, 85% confidence.

## Tier 6 — Soak (minutes → hours)

**Purpose:** Long-running stability and resource leak detection.

**Requirements:**

- Run for extended periods (minutes to hours)
- Monitor for memory fragmentation, compaction storms, file descriptor leaks
- Criterion used for harnessing, not statistical precision
- Often run overnight on dedicated hardware

**Examples:** Sustained write load, repeated flush/compaction cycles,
long-running iterators, memory pressure scenarios, chaos testing.

**Criterion config:** 2s warmup, 30s measurement, 3 samples, 20% noise threshold, 80% confidence.

## Quick Reference

| Tier | Name        | Latency Range | Samples | Noise Threshold |
| ---- | ----------- | ------------- | ------- | --------------- |
| 1    | Hotpath     | ns → µs       | 20      | 1.5%            |
| 2    | Subsystem   | µs → ms       | 15      | 2%              |
| 3    | System      | ms → 10ms     | 10      | 3%              |
| 4    | Integration | 10ms → 100ms  | 8       | 5%              |
| 5    | Soak        | 100ms → s     | 5       | 10%             |
| 6    | Capacity    | min → hr      | 3       | 20%             |
