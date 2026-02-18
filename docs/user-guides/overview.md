# Midge Overview

**An embedded LSM-tree key-value storage engine**

## What is Midge?

Midge is an actor-sequenced, cloud-native embedded LSM storage engine designed for predictable behavior and explicit control. It runs in-process as a library, providing durability guarantees tailored to modern cloud infrastructure.

### Key Features

- **Three storage modes**: InMemory (ephemeral), Local (disk), and Cloud (object storage)
- **Explicit durability control**: Choose between sync, buffered, best-effort, and cloud-strict modes
- **Actor-based architecture**: Single-threaded event loop for predictable state transitions
- **Transaction support**: ACID transactions with snapshot isolation
- **Smart configuration**: Automatically derive tuning parameters from high-level goals

### Design Philosophy

Midge optimizes for:

- **Predictability**: Bounded latency, deterministic behavior, no surprise thread explosions
- **Auditability**: Every state transition is explicit, loggable, and reproducible
- **Cloud-native**: Object storage (S3, Azure, GCS) as a first-class durability target
- **Embeddability**: Synchronous APIs, explicit control, no hidden background threads

## When to Use Midge

**Ideal use cases:**

- State management in distributed system components
- Embedded in search indexers or stream processors
- Local materialization for edge/serverless workloads
- Applications requiring deterministic testability and replay
- Durable queues or changelogs in message brokers
- Cloud-native applications with ephemeral compute

**Choose Midge when you need:**

- Predictable behavior over raw throughput
- Cloud storage as primary durability target
- Explicit control over durability and lifecycle
- Deterministic state transitions for testing/debugging
- Synchronous APIs without async/await complexity

## Core Operations

Midge provides standard key-value operations through a transaction API:

- **Point operations**: `get()`, `put()`, `delete()`
- **Range operations**: `scan()` with prefix/bounds/limits
- **Bulk operations**: `delete_range()` for range tombstones
- **Transactions**: Multi-operation atomic commits with snapshot isolation

### Storage Modes

**InMemory**
- No persistence, data lost on shutdown
- Use for: testing, caching, ephemeral workloads

**Local**
- Persists to local filesystem
- Use for: traditional deployments, single-node databases

**Cloud**
- Persists to object storage (S3, Azure, GCS, R2, MinIO)
- Local disk is cache only, cloud is source of truth
- Use for: cloud-native deployments, serverless, distributed systems

### Durability Levels

All writes require explicit `WriteOptions`:

| Mode | Guarantee | Latency | Use Case |
|------|-----------|---------|----------|
| `sync()` | Fsynced to disk | ~10ms | Critical data, financial transactions |
| `buffered()` | Group commit batching | ~1-5ms | General production workloads |
| `best_effort()` | No WAL, must flush | ~0.1ms | Bulk loads, reloadable data |
| `cloud_strict()` | Cloud upload confirmed | ~100ms | Explicit cloud durability checkpoints |

See [durability.md](durability.md) for detailed guarantees and recovery behavior.

## Comparison with Alternatives

### vs RocksDB

**RocksDB**: Maximum throughput, multi-threaded, battle-tested for local disk
**Midge**: Predictable behavior, cloud-native, deterministic testing

Choose RocksDB for raw speed on local disk. Choose Midge for cloud-native deployments and predictability.

### vs FoundationDB

**FoundationDB**: Distributed database with consensus, multi-node ACID
**Midge**: Embedded single-process storage engine

Choose FoundationDB for distributed consensus. Choose Midge for embedded local storage.

### vs SQLite

**SQLite**: Relational database with full SQL engine
**Midge**: Key-value store with explicit APIs

Choose SQLite for structured relational data. Choose Midge for high-throughput KV operations.

## Performance Characteristics

Midge prioritizes **predictability over raw speed**:

- **Write latency**: 1-10ms (depends on durability mode)
- **Read latency**: Sub-ms for cached, 10-100ms for cloud (with local cache)
- **Throughput**: ~50-75k ops/sec (limited by WAL I/O and memtable work, not event loop)
- **Cache overhead**: ~10-20% of cache size for metadata

If you need hundreds of thousands or millions of ops/sec, use RocksDB or a sharded design. If you need predictable, auditable behavior, use Midge.

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

1. **Storage mode** (required): InMemory, Local, or Cloud
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
- **Cloud deployment**: [../operations/cloud-setup.md](../operations/cloud-setup.md) — Cloud provider setup
- **Performance tuning**: [../operations/performance-tuning.md](../operations/performance-tuning.md) — Optimization guide
- **Architecture deep-dive**: [../development/the-big-idea.md](../development/the-big-idea.md) — Design philosophy and internals
