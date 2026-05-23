# Midge Overview

**An embedded LSM-tree key-value storage engine**

## What is Midge?

Midge is a local-first embedded LSM storage engine designed for predictable behavior and explicit control. It runs in-process as a library, with durability guarantees defined by the local-disk contract and explicit write options.

### Key Features

- **Two core storage modes**: InMemory (ephemeral) and Local (disk)
- **Explicit durability control**: Choose between sync, buffered, and best-effort modes
- **Actor-based architecture**: Single-threaded event loop for predictable state transitions
- **Transaction support**: ACID transactions with snapshot-based reads
- **Smart configuration**: Automatically derive tuning parameters from high-level goals

### Design Philosophy

Midge optimizes for:

- **Predictability**: Bounded latency, deterministic behavior, no surprise thread explosions
- **Auditability**: Every state transition is explicit, loggable, and reproducible
- **Local-first**: Clear durable local storage semantics and explicit recovery behavior
- **Embeddability**: Synchronous APIs, explicit control, no hidden background threads

## When to Use Midge

**Ideal use cases:**

- State management in distributed system components
- Embedded in search indexers or stream processors
- Local materialization for edge/serverless workloads
- Applications requiring deterministic testability and replay
- Durable queues or changelogs in message brokers
- Applications that need deterministic embedded storage semantics

**Choose Midge when you need:**

- Predictable behavior over raw throughput
- Explicit durability choices for local disk
- Explicit control over durability and lifecycle
- Deterministic state transitions for testing/debugging
- Synchronous APIs without async/await complexity

## Core Operations

Midge provides standard key-value operations through a transaction API:

- **Point operations**: `get()`, `put()`, `delete()`
- **Range operations**: `scan()` with prefix/bounds/limits
- **Bulk operations**: `Transaction::delete_range()` or `Engine::delete_range()` for range tombstones
- **Transactions**: Multi-operation atomic commits with snapshot-based reads

### Storage Modes

**InMemory**
- No persistence, data lost on shutdown
- Use for: testing, caching, ephemeral workloads

**Local**
- Persists to local filesystem
- Use for: traditional deployments, single-node databases

### Durability Levels

All writes require explicit `WriteOptions`:

| Mode | Guarantee | Latency | Use Case |
|------|-----------|---------|----------|
| `sync()` | Local fsync completed before return | Highest | Critical data, financial transactions |
| `buffered()` | Visible after WAL append; fsync follows later | Lower | General workloads |
| `best_effort()` | WAL skipped; durable only after flush publication | Lowest | Bulk loads, reloadable data |
See [durability.md](durability.md) for detailed guarantees and recovery behavior.

## Performance Characteristics

Midge prioritizes **predictability over raw speed**:

- **Write latency**: strongly depends on durability mode and storage backend
- **Read latency**: dominated by cache warmth, SST layout, and storage mode
- **Throughput**: workload-dependent; measure with the included benches for your target profile
- **Cache overhead**: ~10-20% of cache size for metadata

Midge optimizes for predictable behavior and explicitness over raw throughput maximization.

## Configuration

Midge uses smart defaults with automatic parameter derivation:

```rust
use cntryl_midge::{MidgeEngine, OpenOptions, Goal, MemoryBudget};

// Simple configuration - all tuning parameters derived automatically
let opts = OpenOptions::local("./my_db")
    .goal(Goal::Latency)           // Optimize for low latency
    .memory_budget(MemoryBudget::Auto)  // Use ~50% of available memory
    .build();

let engine = MidgeEngine::open(opts)?;
```

**Configuration levels:**

1. **Storage mode** (required): InMemory or Local
2. **Goal** (optional): Latency, Throughput, or Economy
3. **Memory budget** (optional): Auto or explicit bytes
4. **Workload profile** (optional): Mixed, WriteHeavy, ReadMostly, RangeScan, TtlHeavy

All low-level parameters (block sizes, memtable sizes, compaction triggers) are derived from these high-level knobs.

See [api-guide.md](api-guide.md) for comprehensive API documentation.

## Observability

Midge exposes comprehensive metrics and state:

- **Runtime metrics**: Sequence number rate, memtable/SST counts, flush/compaction frequency
- **Read amplification**: SSTs touched per read, L0 overlap patterns, budget violations
- **WAL metrics**: Upload latency, batch sizes, durability guarantees
- **Cache metrics**: Hit rates, eviction patterns, size distribution

All configuration is explicit - no hidden magic constants or adaptive tuning.

## Next Steps

- **Quick start**: [quick-start.md](quick-start.md) — 5-minute hello-world
- **API reference**: [api-guide.md](api-guide.md) — Complete API documentation
- **Durability**: [durability.md](durability.md) — Recovery guarantees and crash behavior
- **FAQ**: [faq.md](faq.md) — Common questions and troubleshooting
- **Performance tuning**: [../operations/performance-tuning.md](../operations/performance-tuning.md) — Optimization guide
- **Architecture deep-dive**: [../development/the-big-idea.md](../development/the-big-idea.md) — Design philosophy and internals
