Nice, let’s turn this into an actual battle plan instead of vibes.

I’ll give you a **phased blueprint** you can execute over weeks/months without stalling development. No scrapping, just organ swaps.

---

## Phase 0 – Baseline & Guardrails (1–2 days)

**Goal:** Make it safe to do surgery.

**Keep API frozen**

* Freeze the public `MidgeEngine` + `kv::KV` behavior.
* Only allow *additive* options; no behavioral changes without a feature flag.

**Add guardrails**

* Add a `DebugMode` / `EngineFlags` struct:

  * `deterministic_compaction`
  * `single_executor_runtime`
  * `new_sst_index`
  * `unified_write_path`
* Wire them from `MidgeOptions`.

**Tests & benches**

* Lock in a baseline:

  * Full test suite pass.
  * Capture YCSB Tier 3 read/write latencies.
  * Capture compaction behavior logs for a couple workloads.

> After this: you have a known-good baseline and feature flags to flip new behaviors on/off.

---

## Phase 1 – Introduce the Internal Executor (Engine Runtime)

**Goal:** Centralize “engine background work” without changing semantics (yet).

**What to build**

* New module: `core::runtime` or `core::executor`:

  * Single-threaded executor struct, e.g. `EngineRuntime`.
  * Owns queues for:

    * flush requests
    * compaction requests
    * maintenance jobs (stats, manifest fsync, etc.)
* `MidgeEngine` holds an `Arc<EngineRuntime>`.

**What to route through it (initially)**

* Anything that was already “background-ish”:

  * Flush triggers
  * Compaction scheduling requests
  * Periodic tasks / maintenance timers (if any)

**Behavior**

* For now, the runtime may still spawn threads under the hood, but:

  * All decisions funnel through it.
  * All scheduling decisions are logged (for debug/determinism).

**Success criteria**

* All tests pass with `single_executor_runtime = true`.
* No change in external behavior, but logs clearly show runtime decisions.
* You can set `MIDGE_TRACE_RUNTIME=1` and get a text trace of engine tasks.

---

## Phase 2 – Deterministic Compaction Engine

**Goal:** Replace “organic” compaction with a deterministic task model.

**New concepts**

* `CompactionPlan`:

  * Input tables, output level, key range, file sizes.
* `CompactionTask`:

  * Executes a `CompactionPlan` and emits new SSTs + obsolete SST list.
* `CompactionLog`:

  * Append-only log of compaction plans (and outcomes).

**Implementation steps**

1. **Planner**

   * Move compaction decisions into a pure function:

     * Inputs: current manifest state, scores, CF configs.
     * Outputs: list of `CompactionPlan`s.
   * Makes it easy to test with snapshot manifest JSON fixtures.

2. **Executor**

   * `EngineRuntime` owns a queue of `CompactionTask`s.
   * Executes tasks one at a time (per-CF or globally depending on config).
   * Writes a durable compaction log entry *before* starting.

3. **Manifest update**

   * Apply compaction results as a state transition:

     * Remove input files, add output files.
   * Ideally also modeled as a pure function for testing.

**Testing**

* Unit tests:

  * `should_plan_l0_to_l1_given_overlapping_tables_when_threshold_exceeded`
  * `should_replay_compaction_log_given_crash_midway_when_restarting_engine`
* Integration:

  * Run the durability suite with `deterministic_compaction = true`.
  * Compare compaction traces between runs; they should match.

**Success criteria**

* Engine compaction decisions are replayable from logs.
* No nondeterministic deadlocks or surprising stalls.
* Benchmarks show similar or better perf, but predictably shaped.

---

## Phase 3 – New SST Index (Trie) With Dual Read Path

**Goal:** Upgrade read path without breaking old files.

**Writer changes**

* Extend your `sst_format`:

  * Keep existing block index.
  * Add *optional* trie-based index block to the file footer.
* Controlled by `new_sst_index` flag:

  * When enabled, new SSTs write both:

    * Legacy block index.
    * Trie index.

**Reader changes**

* Reader detects presence of trie index:

  * If enabled and present → use trie path.
  * If not → fall back to legacy index.
* This lets you:

  * Use new index for new files.
  * Continue reading old files without upgrade tooling.

**Data structures**

* Simple first version:

  * Compact prefix trie where leaves store block offsets.
  * Node structure optimized for cache-line hits, not genericity.

**Testing & benches**

* Microbenches:

  * Key lookup p50/p99 with old vs new index.
* Compatibility tests:

  * `should_read_legacy_sst_given_new_index_disabled`
  * `should_read_new_sst_given_legacy_reader_when_trie_missing`

**Success criteria**

* No behavioral changes visible at the KV API.
* Measurable reduction in read latency for prefix-heavy workloads.

---

## Phase 4 – Unified WAL + Memtable + Cache Write Path

**Goal:** Make the write path straight-line and cache-friendly.

**New component**

* `WritePathCoordinator` (or similar) that:

  * Accepts logical write ops.
  * Appends to WAL.
  * Applies to memtable.
  * Optionally informs block cache about hot keys/blocks.

**Refactor steps**

1. Extract whatever logic currently:

   * Appends to WAL.
   * Updates memtable.
   * Nudges flush/compaction.

2. Consolidate into a single function:

   ```rust
   fn apply_write(&self, batch: &WriteBatch) -> Result<SequenceNumber>
   ```

3. Give the coordinator ownership of:

   * Sequence allocation.
   * Write grouping / batching (later).
   * Hooks to prewarm cache entries.

**Concurrency**

* With the executor in place, you can:

  * Keep the write path mostly lock-free.
  * Hand off flush/compaction triggers to the runtime instead of doing any heavy work inline.

**Tests**

* `should_not_lose_merge_operands_under_concurrency_given_same_key_when_merging`
* Write-stress tests with high concurrency using the new coordinator.

**Success criteria**

* Lower write path variance.
* Cleaner stack traces and far fewer places that touch WAL/memtable/cache directly.

---

## Phase 5 – Mutable SST Segments (Optional but Spicy)

**Goal:** Reduce L0 thrash and write-amp for super-hot data.

**Concept**

* New on-disk type: “segment SST”.

  * Appendable while “hot”.
  * Eventually sealed into a normal immutable SST.
* Lookups:

  * Check memtable → mutable segments → sealed SSTs.

**Implementation sketch**

* Segment files:

  * Structured like small WAL-backed SSTs.
  * Tracked in manifest separately from normal SSTs.
* Runtime:

  * `EngineRuntime` is responsible for:

    * Deciding when to seal segments.
    * Promoting sealed segments into regular levels.

**Success criteria**

* Hot key workloads show lower write amp and lower L0 compaction pressure.
* No user-visible API change.

