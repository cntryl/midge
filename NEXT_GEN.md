Perfect.
Here is the **end-state specification** for Midge’s engine architecture after the full “surgery” is complete.
This is _not_ code — it’s a clean, conceptual specification describing what the system _is_.

---

# **Midge Engine Architecture — End-State Specification (2025 World-Class Design)**

_A cohesive, deterministic, actor-driven LSM engine optimized for embedded OLTP + real-time streaming workloads._

---

# **1. Architectural Overview**

The Midge engine operates as a **single-coordinated system** built around a central **Engine Runtime** that governs _all_ background activity and state transitions. The runtime enforces deterministic ordering, eliminates cross-thread state mutation, and provides a unified mechanism for scheduling and executing internal operations.

At its core, Midge is:

- **Actor-driven**, not thread-driven
- **Deterministic**, not opportunistic
- **Unified**, not organically layered
- **Composable**, not rigid

All engine components — WAL, memtable, flush, compaction, SST lifecycle, hybrid storage — communicate exclusively through the runtime using explicit tasks.

---

# **2. Engine Runtime (Central Actor)**

The Engine Runtime is the sole authority over all engine background operations. It exposes a task interface and processes each task sequentially or in controlled parallel lanes.

### Responsibilities:

1. **Task orchestration**

   - All flushes, compactions, WAL uploads, manifest syncs occur via runtime-managed tasks.
   - Tasks are strictly ordered, logged, and replayable.

2. **State machine ownership**

   - The runtime owns the mutable engine state:

     - active memtables
     - immutable memtables pending flush
     - SST set per column family
     - compaction queues
     - WAL sequence progress
     - hybrid storage metadata

3. **Deterministic execution**

   - The same workload produces the same flush/compaction sequence.
   - No hidden cross-thread coordination.
   - Nondeterminism is removed except where explicitly allowed.

4. **Crash resilience**

   - Before executing each major task, the runtime records an entry in a task log.
   - Recovery reconstructs the exact state of in-flight operations.

5. **Zero internal panics**

   - All tasks catch errors and escalate through well-defined error channels.
   - Panics never escape the runtime boundary.

### Design principles:

- **Single brain, multiple hands**
  Background workers act as workers for the runtime, but only the runtime decides _what_ happens.

- **Explicit transitions**
  Every internal state change is modelled as a bounded transition with invariants checked.

- **Replayability**
  Every task represents a durable intent, enabling deterministic recovery.

---

# **3. Unified Write Path**

All write operations through the KV API enter a **Unified Write Pipeline**:

1. **Sequence allocation**

   - Centralized; strictly monotonic; runtime-owned.

2. **WAL append**

   - Zero-copy encoding where possible.
   - Group commit opportunities handled by the runtime.
   - WAL writes are non-blocking from the user’s perspective.

3. **Memtable application**

   - Applied to the active memtable.
   - Optional prewarm signals emitted to block cache.

4. **Flush signaling**

   - Threshold-based flush requests submitted as runtime tasks — _never directly invoked by worker threads_.

The write path is isolated from all background processes except via scheduling messages posted to the runtime.

---

# **4. Memtable + Block Cache Unification**

The memtable, block cache, and WAL buffering are treated as a unified in-memory hot data layer.

### Properties:

- Hot keys may live in WAL-buffer segments, memtable structures, or cached SST blocks.
- The unified design eliminates redundant memory copies and synchronizations.
- Cache warming becomes a first-class event driven by the runtime.

### Benefits:

- Lower read and write latencies.
- Reduced memory fragmentation.
- Better predictability under load.

---

# **5. SST Format (Dual-Index Design)**

Midge uses a flexible, modern SST format:

### Data encoding:

- **TLV-encoded entries** to maximize prefix compression, SIMD-friendliness, and structured decoding.

### Indexes:

Each SST optionally contains two index structures:

1. **Legacy index**

   - Simple block-index maintained for compatibility.

2. **Prefix Trie Index (primary)**

   - Hierarchical, prefix-aware search index enabling:

     - near O(prefix-length) key lookups
     - precise block skip decisions
     - extremely fast range scans for clustered keys

### Backward compatibility:

- Readers auto-detect index availability.
- Old files remain readable indefinitely.

---

# **6. Flush Lifecycle**

Flushes are executed exclusively by the runtime:

1. Memtable reaches threshold → write path submits `FlushRequested` task.
2. Runtime evaluates flush conditions in context of:

   - concurrent flushes
   - compaction backlog
   - WAL size

3. If accepted, runtime:

   - freezes memtable
   - schedules flush worker to produce a new SST (with dual index support)
   - updates manifest atomically via runtime state transitions

Flush ordering is fully deterministic.

---

# **7. Deterministic Compaction Engine**

Compaction is no longer a swarm of independent threads; it is a planned, logged, deterministic subsystem.

### Key components:

1. **Planner**

   - Pure function: manifest → list of compaction plans
   - Considers level scores, overlap, write pressure, hybrid storage state.

2. **Compaction Log (intent log)**

   - Before executing, runtime records:

     - input SSTs
     - output level
     - target files

   - Enables replay after crash.

3. **Executor**

   - Background worker executes compaction tasks but never independently decides to run.
   - Resulting SSTs are validated, then atomically swapped into the manifest.

4. **Determinism guarantees**

   - Same workload produces same compaction plan sequence.
   - No interleaving compactions unless allowed by the runtime.

---

# **8. Hybrid Storage Mode (Cloud + Local)**

Hybrid mode treats cloud and local storage as layers in a unified system.

### Concepts:

- **Cloud WAL** — durable record of all operations.
- **Local ephemeral cache** — stores memtables, hot SSTs, index segments.
- **Upload tasks** — runtime-scheduled and deterministic.
- **Eviction tasks** — triggered based on policies and scheduled by runtime.

Hybrid storage workers do not modify state directly; they report availability and bandwidth signals to the runtime.

---

# **9. Manifest Management**

The manifest is treated as an authoritative snapshot of engine state.

### End-state manifest behaviors:

- Updated by the runtime only — no out-of-band writers.
- All updates correspond to:

  - flush result transition
  - compaction result transition
  - WAL advancement
  - SST lifecycle change

- Manifest sync tasks are scheduled deterministically.
- Recovery reconstructs engine state using:

  - manifest
  - compaction log
  - WAL history
  - SST directory

---

# **10. Concurrency Model**

### Principle: **No shared mutable state across threads.**

- The runtime is the sole writer of engine-wide mutable state.
- Worker threads perform only isolated I/O tasks and return results.
- All state transitions occur inside the runtime actor thread(s).

### Effect:

This eliminates an entire class of:

- race conditions
- missing memory fences
- double-application issues
- inconsistent flush/compaction interleavings

---

# **11. Error and Panic Handling**

### Rules:

- No Midge subsystem is permitted to panic.
- All worker tasks must catch and return errors.
- The runtime consolidates and escalates failures through structured error channels.
- If a fatal error is detected (disk corruption, IO failures), the runtime:

  - freezes writes
  - enters safe mode
  - provides a structured recovery path to the embedding application

---

# **12. Testability & Deterministic Debugging**

The new architecture is built for introspection.

### Capabilities:

- **Task injection**
  Test suite can enqueue tasks manually.
- **Gating**
  Memtable and compaction gates allow pausing/resuming precise phases.
- **Replay**
  System state can be reconstructed by replaying task metadata.
- **Fuzzing hooks**
  Randomized message ordering inside allowed constraints.

This makes Midge one of the most testable storage engines in the industry.

---

# **13. Performance Characteristics (Target)**

### Write Path:

- Sub-microsecond key updates in memory.
- Write throughput gated by WAL fsync latency only.
- Flushes never block writes.

### Read Path:

- Prefix-trie index + block cache unification yields:

  - p50 < 2µs for cached reads
  - p99 under sustained load < 10µs

- Hybrid storage prewarming makes cloud-backed reads comparable to local NVMe for hot data.

### Compaction:

- Deterministic scheduling reduces tail latency.
- Background compaction nearly invisible to foreground workloads.

---

# **14. Extensibility**

The engine architecture supports:

- alternative memtable implementations
- new SST variants
- pluggable compaction strategies
- custom index structures per CF
- new hybrid storage backends
- integration with Fitz/Portia as a drop-in embedded KV layer

Without changing the core runtime model.

---

# **15. Summary (One Sentence)**

> **Midge becomes a deterministic, actor-driven, unified LSM engine where all background operations flow through a central runtime that owns state transitions, enabling correctness, predictability, and world-class performance.**

---
