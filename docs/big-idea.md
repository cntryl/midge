# The Big Idea

**Why Midge Exists and How It Works**

## The Problem

Midge is built for modern infrastructure: ephemeral compute, object storage as the source of truth, and predictable behavior under load. It makes different tradeoffs than existing embedded storage engines:

- **Cloud-native storage** — Object storage (S3, Azure, GCS) is a first-class durability target, not an afterthought
- **Predictable behavior** — Single-threaded actor sequences all state changes; no hidden concurrency
- **True embeddability** — Synchronous APIs, explicit control, no background daemon threads
- **Auditable state** — Every mutation is sequenced through explicit messages and reproducible

If you need an embedded LSM that treats cloud storage and determinism as primary design goals, Midge exists for you.

## Our Core Principle

We are building an embedded LSM storage engine around one uncompromising belief:

> **All state transitions are explicit, serialized, and reproducible.**

No hidden threads. No lock contention. No "eventually consistent" internal state. No surprises.

If a behavior cannot be explained as _"this message caused this state transition,"_ it is a design failure.

## What We're Building: Midge

**Midge** is an actor-sequenced, cloud-native embedded LSM storage engine.

Three architectural pillars define everything we build:

### 1. Actor-Sequenced Core

A single **EventLoop** owns all mutable engine state. Every mutation—sequence number assignment, memtable operations, snapshot creation, flush and compaction planning, WAL lifecycle, cache management, backpressure signaling—happens in this actor, via explicit messages, in a known order.

Background work (I/O, compression, uploads) is performed by **task executors** operating on immutable inputs. They report results back to the actor but never mutate shared state.

**Invariant:** If state changes, it happens in the actor, via a message, in a defined sequence. Always.

### 2. Cloud-Native by Design

We support three explicit storage modes, each with a single authoritative source of truth:

- **Memory Mode:** In-memory WAL and SSTs. No durability. For tests and ephemeral workloads.
- **Local Mode:** Local disk is authoritative. Classic embedded durability.
- **Cloud Mode:** Cloud storage (S3, Azure, etc.) is authoritative. Local disk is an **ephemeral cache only** that can disappear without violating correctness.

In cloud mode:

- WAL writes locally for latency, then uploads to object storage
- SSTs live primarily in cloud storage
- Manifest tracks cloud state as source of truth
- Recovery ignores local filesystem except for opportunistic reuse
- Local cache can be blown away at any time

This isn't a plugin. It's architecture.

### 3. Embedded and Explicit

The database runs **in-process** as a library. All APIs are synchronous, typed, and blocking. There is no standalone server, no daemon, no RPC boundary, no hidden async runtime.

The host application controls:

- Lifecycle (startup, shutdown, resource cleanup)
- Concurrency (run in background threads if you want)
- Memory budget (explicit cache and write buffer sizes)
- Durability tradeoffs (explicit policies: strict, batched, best-effort)

We integrate like a library should, not like a service pretending to be a library.

## How It Works

### Unified Write Path

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
- Transaction commit

There are no side paths, fast paths, or hidden mutations.

### Deterministic Flush & Compaction

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

### Modern SST Format

Custom **TLV-encoded blocks** with pluggable metadata (sparse index, trie for prefix/range queries, bloom filters). Compression is first-class and tunable. Designed for cloud object patterns (few large objects, sequential reads, minimal round-trips) while remaining efficient for local access.

Indexes exist to reduce I/O, not to look clever.

### Failure & Recovery Model

Failures are expected.

- WAL uploads may fail
- Compactions may abort
- Tasks may be retried or abandoned

The actor:

- tracks in-flight intents
- reconciles partial work
- advances state only when safe

Recovery is a replay of known transitions, not filesystem archaeology.

### What You Can Do

**Core operations:**

- **Get** (point reads), **Put**, **Delete**, **DeleteRange** (range tombstones)
- **Scan** (range queries with prefix, bounds, limits, direction)

**Transactions:**

- Multi-operation atomic commits
- ReadOnly and ReadWrite modes
- Snapshot isolation at transaction start
- Actor-serialized commits
- No long-lived locks

**Durability policies** (explicit):

- Strict / Batched / CloudMirrored / CloudFirst / BestEffort

Snapshots provide consistent point-in-time views. Iteration is consistent within a snapshot.

## Who Should Use Midge

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

**The principle:** Midge is the storage layer you can reason about and test.

## Choosing the Right Storage Engine

Midge optimizes for different goals than other embedded storage systems. Here's how the tradeoffs compare:

### RocksDB

| Aspect          | RocksDB                   | Midge                             |
| --------------- | ------------------------- | --------------------------------- |
| **Goal**        | Maximum throughput        | Predictable, auditable behavior   |
| **Concurrency** | Multi-threaded            | Single-actor sequencing           |
| **Compaction**  | Concurrent, opportunistic | Deterministic, planned            |
| **SST Format**  | RocksDB blocks            | TLV with pluggable metadata       |
| **Cloud**       | Via experimental plugins  | Native architecture               |
| **Recovery**    | Filesystem scan + WAL     | Manifest + cloud WAL + intent log |
| **Debugging**   | Thread dumps, profiling   | Message trace, intent replay      |

**When to choose RocksDB:** You need maximum throughput and are optimizing for local disk. RocksDB is battle-tested, widely deployed, and extremely fast.

**When to choose Midge:** You need predictable behavior, cloud-native storage, or the ability to replay and debug state transitions deterministically.

### FoundationDB

| Aspect           | FoundationDB              | Midge                        |
| ---------------- | ------------------------- | ---------------------------- |
| **Scope**        | Distributed database      | Embedded storage engine      |
| **Consensus**    | Raft/Paxos across cluster | Single process, no consensus |
| **Network**      | Core architecture         | Optional (cloud I/O only)    |
| **Transactions** | ACID across cluster       | Serializable within process  |
| **Use Case**     | Cluster-wide coordination | Embedded in a single process |

**When to choose FoundationDB:** You're building a distributed system that needs cross-node transactions and strong consistency guarantees.

**When to choose Midge:** You need reliable storage within a single process, not distributed consensus.

### SQLite

| Aspect          | SQLite                     | Midge                      |
| --------------- | -------------------------- | -------------------------- |
| **Data Model**  | Relational with SQL        | Key-value                  |
| **Query Model** | Full SQL engine            | Get/Put/Scan/Range         |
| **Durability**  | Local file                 | Local or cloud             |
| **Concurrency** | Multiple readers, WAL mode | Single-actor sequencing    |
| **Use Case**    | Structured, queryable data | High-throughput KV storage |

**When to choose SQLite:** You need SQL, relational queries, or have structured data with complex access patterns.

**When to choose Midge:** You need a fast key-value store with explicit control over durability and cloud storage.

## The Tradeoffs We Chose

Every architectural choice is a tradeoff. Here's what we chose and why:

### Synchronous APIs → Simple mental model, but caller manages threads

All public APIs are synchronous and blocking. No async/await, no hidden executor. This makes control flow explicit, testing deterministic, and error handling straightforward. The cost: embedders manage their own background thread pools if they want concurrency.

### Single Actor → Predictable but bounded throughput

One actor sequences all state mutations, not partitioned by key or level. This makes ordering explicit, recovery simple (replay, not repair), and visibility trivial. The cost: throughput ceiling (~100k ops/sec) is lower than thread-per-shard designs. We choose **predictability over raw speed**.

### Cloud-First → Explicit semantics, more planning required

Cloud durability and SST storage are architectural pillars, not plugins. Explicit modes (memory/local/cloud) prevent confusion. Ephemeral local cache is simpler than "maybe sync, maybe don't." The cost: running in cloud requires explicit planning. But you always know where your data lives.

### Deterministic Compaction → Reproducible but not opportunistic

Plans are logged; execution is deterministic. Same input → same state transition. This enables testing, debugging, and forensics. The cost: we sacrifice opportunistic speed for reproducibility.

### Custom SST Format → No RocksDB compatibility

TLV blocks with pluggable metadata. Designed for cloud and large objects, not tiny writes. Compression is first-class. The cost: not drop-in compatible with RocksDB. But cleaner and more intentional.

## A Concrete Example: How a Write Flows Through the System

Here's what happens when an application calls `put` within a transaction:

```
1. Application creates transaction and calls: txn.put("user:42", value)

2. EventLoop receives ApplyTransaction message
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

**If we replay the same sequence of inputs, we get the same sequence of outputs. This is not luck. This is design.**

## What Integration Looks Like

Applications use Midge like any embedded library:

**Initialization:** Create an engine with explicit config (storage mode, write buffer size, cache size, compaction levels). Pick your storage backend: memory, local, or cloud.

**Write operations:** All writes are synchronous. Commit via Transaction with explicit WriteOptions. Engine may signal backpressure (write stalls) when memtable queue is full—application decides how to handle it.

**Snapshots & iteration:** Snapshots provide consistent point-in-time views at specific sequence numbers. Iteration is consistent within a snapshot. Snapshots are released when dropped.

**Shutdown:** Flush memtables, optionally run compaction, close handles with manifest synchronization. Simple. Synchronous. Explicit.

No magic.

## Observability: See Everything

Every component exposes its state:

**Runtime metrics:** seqno rate, memtable size/count, SST count per level, flush/compaction frequency, WAL upload latency, cache hit rate.

**Tracing & intent log:** Every state transition is loggable. Messages can be recorded and replayed. Failure scenarios can be simulated. Forensics: "what happened between seqno 1000 and 2000?"

**Configuration:** All tuning parameters are explicit (write buffer size, cache size, compaction ratios, bloom tuning, cloud upload strategy). No magic constants. No adaptive tuning unless explicitly enabled.

If you can't see it, we didn't build it right.

## The Quality Bar: Infrastructure Grade

This system targets infrastructure-grade rigor:

**Predictability:** Latency variance is bounded. No surprise thread explosions. Compaction happens when planned. Failures are expected and handled.

**Debuggability:** Every state change is loggable. Intent log enables forensics. Determinism enables reproduction. Tests validate behavior, not just coverage.

**Reliability:** Recovery is a known process. Partial failures are isolated. Manifest is source of truth. Cloud durability is explicit.

**Testability:** Deterministic execution. No flaky tests from timing. Mock cloud storage for testing. Intent logs for scenario validation.

If a behavior cannot be explained as a sequence of actor messages and state transitions, **it is a design failure**.

## Performance: What to Expect

Midge is **not a raw-speed benchmark champion**. It is a **predictable, auditable system**.

Expected characteristics:
- **Write latency:** 1–10ms (depends on WAL upload strategy)
- **Read latency:** Sub-ms for in-cache, 10–100ms for cloud (with local cache)
- **Throughput:** ~100k ops/sec (limited by actor serialization, not by disk)
- **Cache overhead:** ~10–20% of cache size for metadata

If you need **raw throughput** (millions of ops/sec), use **RocksDB** or a sharded design.

If you need **predictability and correctness**, use **Midge**.

## How We Build

Midge maintains strict discipline:

1. **Correctness before performance** — Invariants are documented, tested, and enforced
2. **Determinism before optimization** — Same inputs produce same state transitions
3. **Explainability before convenience** — Every behavior traces to an actor message
4. **Testing rigor** — No flaky tests, all failures are reproducible
5. **Observable by default** — Metrics, intent logs, and configuration are always visible

This discipline is ongoing, not a phase we graduate from.

---

## In One Sentence

Midge is an **actor-sequenced, cloud-native embedded LSM** designed to be predictable, inspectable, and durable—without hiding complexity behind threads, magic, or luck.

**This is storage infrastructure you can reason about, test, and trust.**
```
