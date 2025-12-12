# The Big Idea

**An Actor-Sequenced, Cloud-Native Embedded LSM**

## What We Are Building

We are building an **embedded LSM storage engine** designed around one core principle:

> **All state transitions are explicit, serialized, and reproducible.**

The engine runs **in-process** as a library, uses an **actor-owned core** to sequence all mutations, and treats **cloud object storage as a first-class durability target**, not an afterthought.

This is infrastructure-grade storage, not a convenience cache.

## Embeddability First

- The database runs inside the host application's process.
- All APIs are synchronous, typed, and in-process.
- There is no standalone server, daemon, or RPC boundary.
- Lifecycle, shutdown, memory, and durability tradeoffs are controlled by the host.

The engine integrates like a library, not a service.

## Actor-Owned Core

A single **EngineActor** owns all mutable engine state:

- sequence numbers and visibility
- memtables and immutables
- snapshots
- version sets and manifests
- flush and compaction planning
- WAL segment lifecycle
- cache pinning and eviction
- backpressure and write stalls

No other component mutates state.

Background work (I/O, compression, uploads) is performed by task executors operating on immutable inputs and reporting results back to the actor.

> **Invariant:** If state changes, it happens in the actor, via a message, in a known order.

## Unified Write Path

Every write follows the same straight-line pipeline:

```
operation
→ seqno assignment
→ WAL append
→ memtable apply
→ commit visibility
→ background work scheduled explicitly
```

This applies uniformly to:

- Put / Delete / DeleteRange
- Merge
- WriteBatch
- Transaction commit

There are no side paths, fast paths, or hidden mutations.

## Supported Operations

### Core KV

- Get / Range / Prefix
- Put
- Delete
- DeleteRange (range tombstones)

### Merge

- Append-only merge operands on write
- Deterministic read resolution
- Optional compaction-time collapse via merge operators

### Atomic Batches

- Multi-key atomic visibility
- Single commit boundary
- WAL-backed durability

### Transactions (MVCC, optional)

- Snapshot or read-committed isolation
- Optimistic conflict detection
- Actor-serialized commit
- No long-lived locks

Snapshots are first-class and enforced by seqno visibility.

## Storage Modes & Source of Truth

The engine supports **three explicit storage modes**, each with a single authoritative source of truth.

### Memory Mode

- In-memory WAL and SSTs
- No durability
- Used for tests, benchmarks, and ephemeral workloads

### Local Mode

- Local disk WAL and SSTs
- Filesystem is authoritative
- Classic embedded durability model

### Cloud Mode (Cloud-Native)

- Cloud WAL + cloud SSTs are the source of truth
- Local disk/NVMe is **ephemeral cache only**
- WAL and SSTs are uploaded and tracked by the actor
- Recovery ignores local state unless reused opportunistically

> In cloud mode, local data may disappear at any time without violating correctness.

## Cloud-Native WAL

- WAL appends locally for latency
- Upload to cloud storage is actor-scheduled
- Durability levels are explicit (memory / local / cloud-ack)
- Recovery is driven by:

  - manifest
  - cloud WAL objects
  - compaction intent/log state

Local WAL segments are never trusted in cloud mode.

## Cloud-Native SST Layer

- SSTs live primarily in object storage
- Local storage is a cache
- The actor decides:

  - what to prefetch
  - what to pin
  - what to evict

- Compaction can write directly to cloud

The SST format is designed for:

- few large objects
- sequential reads
- minimal cloud round-trips

## Deterministic Flush & Compaction

Flush and compaction are:

- planned by the actor
- executed as explicit tasks
- committed via manifest transitions

The engine records **intent before action**:

- plan
- execute
- validate
- commit

Given the same workload and timing, the same plans and state transitions occur.

This determinism is intentional and enforced.

## Modern SST Format

- TLV-encoded blocks
- Pluggable metadata:

  - sparse index
  - trie (prefix/range acceleration)
  - bloom filters

- Compression is first-class and tunable
- Designed for both local and cloud access patterns

Indexes exist to reduce I/O, not to look clever.

## Failure & Recovery Model

Failures are expected.

- WAL uploads may fail
- Compactions may abort
- Tasks may be retried or abandoned

The actor:

- tracks in-flight intents
- reconciles partial work
- advances state only when safe

Recovery is a replay of known transitions, not filesystem archaeology.

## Quality Bar

This system targets **infrastructure-grade rigor**:

- deterministic behavior over opportunistic speed
- invariants documented and enforced
- tests that define behavior, not just coverage
- explainable failures
- performance tuning only after correctness is locked

If a behavior cannot be explained as:

> _"this message caused this state transition"_

it is a design failure.

## In One Sentence

This is an **actor-sequenced, cloud-native embedded LSM** designed to be predictable, inspectable, and durable—without hiding complexity behind threads, magic, or luck.

## Use Cases & Target Applications

Midge is designed for applications that:

- **Need predictable behavior** — no surprise latency spikes, no black-box thread pools
- **Control their own lifecycle** — they manage startup, shutdown, and resource budgets
- **Can use synchronous APIs** — no need for async/await abstractions
- **Operate at infrastructure scale** — reliability and debuggability matter more than raw speed
- **May run in cloud or on-prem** — same code, different durability targets
- **Benefit from determinism** — testing, replay, forensics, auditing

Example contexts:

- Embedded in a search indexer
- State management in a distributed system component
- Local materialization in a streaming processor
- Durable queue or changelog in a message broker
- Embedded in a sidecar or agent
- Testing harness for complex systems

The principle: **Midge is the storage layer you can reason about and test.**

## Key Design Decisions & Tradeoffs

### Synchronous APIs (Not Async)

**Decision:** All public APIs are synchronous and blocking.

**Rationale:**

- Simpler mental model for embedders
- Easier to test deterministically
- Clearer control flow and error handling
- No async executor dependency
- Caller controls their own concurrency strategy

**Tradeoff:** Embedders manage their own background thread pools if needed.

### Actor-Sequenced, Not Thread-Safe Partitions

**Decision:** Single actor sequences all state mutations, not partitioned by key or level.

**Rationale:**

- Global seqno visibility is simple and correct
- Ordering is explicit and auditable
- No distributed consensus or compare-and-swap
- Failure recovery is replay, not repair

**Tradeoff:** Throughput ceiling is lower than thread-per-shard designs, but predictability is higher.

### Cloud as First-Class, Not an Afterthought

**Decision:** Cloud durability and SST storage are architectural choices, not plugins.

**Rationale:**

- Explicit modes (memory / local / cloud) prevent confusion
- Ephemeral local cache is simpler than "maybe sync, maybe don't"
- Recovery logic is the same across all modes
- Embedders know exactly where their data lives

**Tradeoff:** Running in-cloud requires explicit planning, but the semantics are clear.

### Deterministic Flush & Compaction

**Decision:** Plans are logged; execution is deterministic.

**Rationale:**

- Reproducible behavior enables testing and debugging
- Forensics: "what compaction happened and why?"
- No surprise performance cliffs from concurrent decisions
- Same input → same state transition, always

**Tradeoff:** Opportunistic speed is sacrificed for predictability.

### Modern SST Format, Not RocksDB-Compatible

**Decision:** Custom TLV blocks with pluggable metadata.

**Rationale:**

- Designed for cloud and large objects, not tiny writes
- Metadata is explicit (trie, sparse index, bloom)
- Compression is first-class, not bolted on
- No legacy baggage

**Tradeoff:** Not drop-in compatible with RocksDB, but cleaner and more intentional.

## Concrete Example: A Write and Its State Transitions

Here's what happens when an application calls `put("user:42", value)`:

```
1. Application calls: engine.put("user:42", value)

2. EngineActor receives Put message
   - Assign seqno (e.g., 1005)
   - Check write stall conditions

3. WAL append
   - Append ["Put", seqno=1005, "user:42", value] to local WAL buffer
   - Return immediately (or block until cloud-ack, depending on durability level)

4. Memtable apply
   - Insert into in-memory memtable keyed by seqno
   - Update internal index

5. Commit visibility
   - Mark seqno 1005 as committed
   - Application sees value on subsequent read

6. Background work scheduled (explicitly)
   - Actor posts message: "WAL segment exceeds threshold, schedule upload"
   - Task executor: compress, upload to cloud storage, report back
   - Actor updates: "segment uploaded, can delete local copy"

7. Memtable flush (when full)
   - Actor creates flush intent: "memtable [seqno 900–1005] → SST"
   - Task executor: sort, build SST, write locally/to cloud
   - Report back: "SST at path X, manifest version Y"
   - Actor updates manifest and seqno visibility

All state changes happen in actor messages. Every decision is logged.
If we replay the same sequence of inputs, we get the same sequence of outputs.
```

## Integration: How an Application Uses Midge

### Initialization

```rust
// Application creates engine with config
let config = EngineConfig {
    storage_mode: StorageMode::Cloud {
        bucket: "my-db-bucket",
        provider: CloudProvider::S3,
    },
    write_buffer_size: 64 * MB,
    cache_size: 512 * MB,
    compaction_levels: 10,
};

let engine = Engine::open(config)?;
```

### Write Loop

```rust
// Application's request handler
loop {
    let req = receive_request();

    // Single, synchronous call
    let result = engine.put(&req.key, &req.value);

    match result {
        Ok(seqno) => {
            // Durability level indicates where data is
            println!("Committed at seqno {}", seqno);
        }
        Err(WriteStall) => {
            // Backpressure: memtable full, compaction lagging
            // Application decides: retry, drop, or wait
        }
        Err(e) => {
            // Permanent failure
        }
    }
}
```

### Snapshots & Iteration

```rust
// Get a snapshot at a known seqno
let snap = engine.snapshot(seqno_1000)?;

// Iterate consistently
for item in snap.iter(prefix)? {
    process(item);
}

// Snapshot is released when dropped
drop(snap);
```

### Shutdown

```rust
// Explicit graceful shutdown
engine.flush()?;  // Flush all memtables
engine.compact()?; // Optional: run compaction
engine.close()?;   // Close all handles, sync manifests
```

The API is **simple, synchronous, and explicit**. No magic.

## Why This Isn't RocksDB

| Aspect           | RocksDB                  | Midge                                |
| ---------------- | ------------------------ | ------------------------------------ |
| **API**          | Sync, but threads hidden | Sync, threads explicit               |
| **Compaction**   | Concurrent, background   | Actor-scheduled, deterministic       |
| **SST Format**   | Legacy RocksDB blocks    | Modern TLV, pluggable metadata       |
| **Cloud**        | Bolted on (experimental) | Native, three modes                  |
| **Durability**   | Local FS or S3 SDK       | Explicit levels (memory/local/cloud) |
| **Recovery**     | Filesystem scan + WAL    | Manifest + cloud WAL + intent log    |
| **Debugging**    | Thread dumps, flamegraph | Message trace, intent log replay     |
| **Transactions** | Optional, complex        | Optional, actor-serialized           |

**Bottom line:** RocksDB is battle-tested and fast. Midge trades some speed for predictability, cloud-nativity, and debuggability.

## Why This Isn't FoundationDB

| Aspect           | FoundationDB              | Midge                        |
| ---------------- | ------------------------- | ---------------------------- |
| **Scope**        | Distributed database      | Embedded storage engine      |
| **Consensus**    | Paxos + leader election   | Single actor, no consensus   |
| **Network**      | Fundamental               | Not fundamental              |
| **Transactions** | ACID across cluster       | Serializable within actor    |
| **Latency**      | Multi-region, managed     | Single-machine, predictable  |
| **Use Case**     | Cluster-wide coordination | Embedded in a single process |

**Bottom line:** FoundationDB solves distributed consensus. Midge solves "I need predictable storage in my process."

## Why This Isn't SQLite

| Aspect           | SQLite                      | Midge                       |
| ---------------- | --------------------------- | --------------------------- |
| **Schema**       | Relational with SQL         | Key-value, untyped          |
| **Queries**      | Full SQL engine             | Simple KV, range, prefix    |
| **Transactions** | SQL-level ACID              | Seqno-based visibility      |
| **Durability**   | Local file                  | Local/Cloud, explicit modes |
| **Concurrency**  | Multiple connections, locks | Single actor, no locks      |
| **Use Case**     | Embedded relational DB      | Embedded KV store           |

**Bottom line:** SQLite is for applications that need SQL. Midge is for applications that need a fast, reliable KV store that doesn't hide its internals.

## Visibility & Observability

Every component exposes its state:

### EngineActor Metrics

- Seqno assignment rate
- Memtable size and count
- SST count per level
- Flush and compaction frequency
- WAL upload latency
- Cache hit rate

### Tracing & Intent Log

- Every state transition is loggable
- Messages can be recorded and replayed
- Failure scenarios can be simulated
- Forensics: "what happened between seqno 1000 and 2000?"

### Configuration & Tuning

All tuning parameters are explicit:

- Write buffer size
- Cache size and eviction policy
- Compaction level count and size ratios
- Cloud upload strategy (batch size, latency target)
- Bloom filter tuning

No magic constants. No adaptive tuning (unless explicitly enabled).

## Quality Bar: Infrastructure Grade

This is what "infrastructure-grade" means:

### Predictability

- Latency variance is bounded
- No surprise thread explosions
- Compaction happens when planned
- Failures are expected and handled

### Debuggability

- Every state change is loggable
- Intent log enables forensics
- Determinism enables reproduction
- Tests can validate behavior, not just coverage

### Reliability

- Recovery is a known process
- Partial failures are isolated
- Manifest is the source of truth
- Cloud durability is explicit

### Testability

- Deterministic execution
- No flaky tests from timing
- Mock cloud storage for testing
- Intent logs for scenario validation

If a behavior cannot be explained as a sequence of actor messages and state transitions, it is a **design failure**.

## Implementation Layers

These layers exist and must maintain their invariants:

### API Layer

User-facing engine interface (get, put, range, snapshot, etc).

### Core Actor

EngineActor: sequences all mutations, owns state.

### Memtable & Immutable Layers

In-memory storage, sorted by seqno.

### SST Layer

Local and cloud SST storage, with cache management.

### WAL Layer

Local buffer → cloud upload, durability levels.

### Compaction Layer

Task-based execution, deterministic planning.

### Manifest Layer

Source of truth for what SSTs exist and which seqnos they cover.

### Cloud Integration

S3, Azure, Wasabi, mock provider interface.

Dependencies must flow **upward** only. See `docs/DEPENDENCY_ANALYSIS.md` for the layer rules.

## Performance Model

Midge is **not a raw-speed benchmark champion**. It is a **predictable, auditable system**.

Expected characteristics:

- **Write latency:** 1–10ms (depends on WAL upload strategy)
- **Read latency:** Sub-ms for in-cache, 10–100ms for cloud (with local cache)
- **Throughput:** Limited by actor serialization (~100k ops/sec), not by disk
- **Cache overhead:** ~10–20% of cache size for metadata

If you need **raw throughput** (millions of ops/sec), use **RocksDB** or a sharded design.

If you need **predictability and correctness**, use **Midge**.

## Path to Production

1. **Correctness first** — all tests pass, all invariants hold
2. **Stability** — deterministic behavior, no flaky tests, reproducible failures
3. **Performance tuning** — only after correctness is locked
4. **Cloud integration** — tested against real providers (optional)
5. **Observability** — metrics, logging, intent traces
6. **Documentation** — invariants, recovery procedures, design decisions

Each phase has a clear quality gate. No phase proceeds without the previous one passing.
