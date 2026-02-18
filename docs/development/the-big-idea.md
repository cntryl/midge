# The Big Idea

**Midge Design Philosophy and Architectural Principles**

> For user-facing overview and use case guidance, see [../user-guides/overview.md](../user-guides/overview.md)

## Core Principle

We are building an embedded LSM storage engine around one uncompromising belief:

> **All state transitions are explicit, serialized, and reproducible.**

No hidden threads. No lock contention. No "eventually consistent" internal state. No surprises.

If a behavior cannot be explained as _"this message caused this state transition,"_ it is a design failure.

## The Problem We're Solving

Midge is built for modern infrastructure: ephemeral compute, object storage as the source of truth, and predictable behavior under load. It makes different tradeoffs than existing embedded storage engines:

- **Cloud-native storage** — Object storage (S3, Azure, GCS) is a first-class durability target, not an afterthought
- **Predictable behavior** — Single-threaded actor sequences all state changes; no hidden concurrency
- **True embeddability** — Synchronous APIs, explicit control, no background daemon threads
- **Auditable state** — Every mutation is sequenced through explicit messages and reproducible

If you need an embedded LSM that treats cloud storage and determinism as primary design goals, Midge exists for you.

## Three Architectural Pillars

### 1. Actor-Sequenced Core

A single **EventLoop** owns all mutable engine state. Every mutation—sequence number assignment, memtable operations, snapshot creation, flush and compaction planning, WAL lifecycle, cache management, backpressure signaling—happens in this actor, via explicit messages, in a known order.

Background work (I/O, compression, uploads) is performed by **task executors** operating on immutable inputs. They report results back to the actor but never mutate shared state.

**Invariant:** If state changes, it happens in the actor, via a message, in a defined sequence. Always.

**Why this matters:**
- Deterministic execution: same inputs → same state transitions
- Simplified reasoning: no concurrent mutations to track
- Testability: message sequences are recordable and replayable
- Debuggability: full state visible at any message boundary

**Cost:**
- Throughput ceiling (~50-75k ops/sec) lower than multi-threaded designs
- Single serialization point can become bottleneck
- We choose predictability over raw speed

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

**This isn't a plugin. It's architecture.**

The cloud-first design influences:
- WAL upload pipeline with acknowledgment flow
- SST block format (larger blocks optimized for object storage)
- Recovery from manifest (no filesystem scan required)
- Ephemeral compute support (serverless-friendly)

**Cost:**
- Running in cloud requires explicit planning and configuration
- Higher latency on cloud writes vs pure-local designs
- We choose explicit semantics over magical fallbacks

### 3. Embedded and Explicit

The database runs **in-process** as a library. All APIs are synchronous, typed, and blocking. There is no standalone server, no daemon, no RPC boundary, no hidden async runtime.

The host application controls:

- Lifecycle (startup, shutdown, resource cleanup)
- Concurrency (run in background threads if you want)
- Memory budget (explicit cache and write buffer sizes)
- Durability tradeoffs (explicit policies: strict, batched, best-effort)

**We integrate like a library should, not like a service pretending to be a library.**

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
- Transaction commit

There are no side paths, fast paths, or hidden mutations.

**Implications:**
- Every write gets a sequence number in order
- Sequence numbers never go backward
- WAL append happens before memtable (no lost writes)
- Background work is explicit (no surprise flushes during writes)

## Deterministic Flush & Compaction

Flush and compaction are:

- planned by the actor
- executed as explicit tasks
- committed via manifest transitions

The engine records **intent before action**:

1. **Plan** — Actor creates flush/compaction intent
2. **Execute** — Task executor builds SST from immutable inputs
3. **Validate** — Actor checks result matches intent
4. **Commit** — Manifest updated, old SST references removed

Given the same workload and timing, the same plans and state transitions occur.

**This determinism is intentional and enforced.**

**Cost:**
- We sacrifice opportunistic compaction speed for reproducibility
- Compaction cannot start until actor plans it
- We choose debuggability over maximum throughput

## Modern SST Format

Custom **TLV-encoded blocks** with pluggable metadata:

- Sparse index for binary search within blocks
- Trie for prefix/range query optimization
- Bloom filters for point lookup false-positive reduction
- Compression first-class (LZ4, Snappy, Zstd)

Designed for cloud object patterns:
- Larger blocks (64 KiB default) reduce object count
- Sequential reads minimize round-trips
- Metadata enables skipping irrelevant blocks

**Not compatible with RocksDB SST format.**

**Why custom format:**
- Cloud-optimized block sizes and metadata
- Clean separation of concerns (no legacy baggage)
- Compression as first-class design element
- Pluggable metadata for future extensions

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

**Recovery process:**

```
1. Acquire exclusive lease (prevents concurrent access)
2. Load manifest (discover SST files and sequence ranges)
3. Replay WAL (restore uncommitted writes from log)
4. Reconcile state (merge WAL writes with SST data)
5. Resume operations (start accepting new writes)
```

**Key invariants:**
- Recovery is deterministic (same input → same output)
- Sequence numbers are monotonic (never reused)
- All committed writes are restored (per their WriteOptions)

See [recovery-internals.md](recovery-internals.md) for detailed recovery algorithm.

## Concrete Example: Write Flow Through the System

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
```

**If we replay the same sequence of inputs, we get the same sequence of outputs. This is not luck. This is design.**

## Integration Model

Applications use Midge like any embedded library:

**Initialization**
Create an engine with explicit config (storage mode, write buffer size, cache size, compaction levels). Pick your storage backend: memory, local, or cloud.

**Write operations**
All writes are synchronous. Commit via Transaction with explicit WriteOptions. Engine may signal backpressure (write stalls) when memtable queue is full—application decides how to handle it.

**Snapshots & iteration**
Snapshots provide consistent point-in-time views at specific sequence numbers. Iteration is consistent within a snapshot. Snapshots are released when dropped.

**Shutdown**
Flush memtables, optionally run compaction, close handles with manifest synchronization. Simple. Synchronous. Explicit.

No magic.

## Observability: See Everything

Every component exposes its state:

**Runtime metrics**
Seqno rate, memtable size/count, SST count per level, flush/compaction frequency, WAL upload latency, cache hit rate.

**Tracing & intent log**
Every state transition is loggable. Messages can be recorded and replayed. Failure scenarios can be simulated. Forensics: "what happened between seqno 1000 and 2000?"

**Configuration**
All tuning parameters are explicit (write buffer size, cache size, compaction ratios, bloom tuning, cloud upload strategy). No magic constants. No adaptive tuning unless explicitly enabled.

**If you can't see it, we didn't build it right.**

## The Quality Bar: Infrastructure Grade

This system targets infrastructure-grade rigor:

**Predictability**
Latency variance is bounded. No surprise thread explosions. Compaction happens when planned. Failures are expected and handled.

**Debuggability**
Every state change is loggable. Intent log enables forensics. Determinism enables reproduction. Tests validate behavior, not just coverage.

**Reliability**
Recovery is a known process. Partial failures are isolated. Manifest is source of truth. Cloud durability is explicit.

**Testability**
Deterministic execution. No flaky tests from timing. Mock cloud storage for testing. Intent logs for scenario validation.

**If a behavior cannot be explained as a sequence of actor messages and state transitions, it is a design failure.**

## The Tradeoffs We Chose

Every architectural choice is a tradeoff. Here's what we chose and why:

### Synchronous APIs → Simple mental model, but caller manages threads

All public APIs are synchronous and blocking. No async/await, no hidden executor. This makes control flow explicit, testing deterministic, and error handling straightforward.

**Cost:** Embedders manage their own background thread pools if they want concurrency.

### Single Actor → Predictable but bounded throughput

One actor sequences all state mutations, not partitioned by key or level. This makes ordering explicit, recovery simple (replay, not repair), and visibility trivial.

**Cost:** Throughput ceiling (~50-75k ops/sec) is lower than thread-per-shard designs (RocksDB: 500k-2M+ ops/sec). We choose **predictability over raw speed**.

### Cloud-First → Explicit semantics, more planning required

Cloud durability and SST storage are architectural pillars, not plugins. Explicit modes (memory/local/cloud) prevent confusion. Ephemeral local cache is simpler than "maybe sync, maybe don't."

**Cost:** Running in cloud requires explicit planning. But you always know where your data lives.

### Deterministic Compaction → Reproducible but not opportunistic

Plans are logged; execution is deterministic. Same input → same state transition. This enables testing, debugging, and forensics.

**Cost:** We sacrifice opportunistic speed for reproducibility.

### Custom SST Format → No RocksDB compatibility

TLV blocks with pluggable metadata. Designed for cloud and large objects, not tiny writes. Compression is first-class.

**Cost:** Not drop-in compatible with RocksDB. But cleaner and more intentional.

## Performance: What to Expect

Midge is **not a raw-speed benchmark champion**. It is a **predictable, auditable system**.

Expected characteristics:

- **Write latency:** 1–10ms (depends on WAL upload strategy)
- **Read latency:** Sub-ms for in-cache, 10–100ms for cloud (with local cache)
- **Throughput:** ~50-75k ops/sec (limited by WAL I/O and per-operation work; event loop itself handles 67M msgs/sec)
- **Cache overhead:** ~10–20% of cache size for metadata

If you need **raw throughput** (500k+ ops/sec), use **RocksDB** or a sharded design.

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

---

## Related Documentation

- **User guide**: [../user-guides/overview.md](../user-guides/overview.md) — When to use Midge, comparison with alternatives
- **Architecture details**: [architecture.md](architecture.md) — Module structure, threading model, layer dependencies
- **Recovery internals**: [recovery-internals.md](recovery-internals.md) — WAL replay, manifest reconciliation, crash scenarios
- **Testing philosophy**: [testing.md](testing.md) — Test structure, naming conventions, deterministic testing
