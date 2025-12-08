# Midge Architectural Roadmap (2025–2026)

## 0. Goals

Midge's next phases aim to:

1. **Make the architecture explicit**
   Actor-driven runtime, clear coordinators, single source of truth for metadata.

2. **Lock in determinism**
   Flush + compaction ordering is reproducible and test-validated.

3. **Be truly cloud-native**
   WAL and SST persistence correctly wired to cloud backends with clear semantics.

4. **Reduce friction for future features**
   Mutable SSTs, advanced indexing, and hybrid deployment become easy to add.

---

## Phase 8 — Consolidation & Determinism

### 8.1 Unify Background Coordinators

**Objective:** One mental model for "background work".

* Create `core/coordinator/` module.
* Introduce a `BackgroundCoordinator` trait that covers:

  * flush
  * compaction
  * WAL upload
  * cloud SST upload/eviction
* Move `FlushCoordinator`, `CompactionController`, `WalUploadCoordinator`, `CloudCoordinator` behind this trait.
* Deprecate direct thread spawning from coordinators; all use a common pattern.

**Acceptance criteria:**

* All background components implement `BackgroundCoordinator`.
* No new coordinator types outside `core/coordinator/`.
* One place to look to understand "what background work exists".

---

### 8.2 Extract Engine State & Dependencies

**Objective:** Make `MidgeEngine` small, obvious, and testable.

* Add `core/engine/state.rs`:

  * versions, active/pending memtables
  * open SST lists per CF
  * snapshot tracking
* Add `core/engine/deps.rs`:

  * storage backend handles
  * clock, sequence allocators
  * runtime/coordinator references
* Refactor `MidgeEngine` to delegate to these two types instead of holding everything.

**Acceptance criteria:**

* `MidgeEngine` struct becomes thin: mostly entrypoints and a few handles.
* Tests can construct `EngineState` in isolation.

---

### 8.3 Wire Cloud Uploads into Hot Paths

**Objective:** Cloud-backed mode is real, not theoretical.

* When a flush completes:

  * New SST is:

    * registered in manifest
    * submitted to the cloud upload path via `BackgroundCoordinator`.
* When compaction completes:

  * New SSTs are submitted for cloud upload.
  * Obsolete cloud objects are scheduled for deletion/GC.
* WAL rotation:

  * Closed WAL segments are queued for upload when using cloud WAL mode.

**Acceptance criteria:**

* In cloud mode, a durability test can:

  * kill the process
  * delete local disk
  * recover fully from cloud WAL + SST.
* Cloud code paths are covered by tests (even if using a mock backend).

---

### 8.4 Background Error Surface

**Objective:** Engine never silently degrades.

* Introduce an internal `EngineHealth` / `BackgroundError` state on `MidgeEngine`.
* Any unrecoverable background error:

  * recorded and exposed via `MidgeEngine::last_background_error()` (or similar).
  * causes new writes to either:

    * fail fast, or
    * continue in a documented "degraded" mode.
* Add logging for first occurrence of each unique background error.

**Acceptance criteria:**

* A failing flush/compaction/cloud upload is visible to user code.
* No background error path is "log-only".

---

### 8.5 Determinism Validation Suite

**Objective:** Prove deterministic behavior, don't just claim it.

* Add a `tests/determinism/` group that:

  * Spins up two engines with identical options.
  * Runs the same sequence of operations (YCSB-ish, compaction-inducing).
  * Flushes / compactions allowed to run to quiescence.
  * Compares:

    * manifests
    * set of SST files
    * compaction logs (if present).
* Include variations:

  * with and without cloud mode
  * single vs multi-CF

**Acceptance criteria:**

* Determinism tests pass consistently in CI.
* A non-deterministic change in compaction/flush shows up as a test failure.

---

### 8.6 Snapshot Isolation & Invariants

**Objective:** Keep snapshots correct during refactors.

* Enumerate snapshot invariants (e.g., `docs/INVARIANTS.md`):

  * every snapshot pins a version
  * compaction never drops keys visible to active snapshots
  * flush/compaction cannot advance a snapshot's view invisibly
* Add explicit assertions and tests:

  * tests that open a snapshot, hammer writes/compactions, and verify view stability.

**Acceptance criteria:**

* Snapshot invariants documented.
* At least one test per invariant.
* No snapshot tests fail after refactors.

---

### 8.7 Dead Code & Metadata Cache Cleanup (Safe Prune)

**Objective:** Remove noise before heavier surgery.

* Remove confirmed-unused:

  * `planner_controller.rs`
  * `db_lock` and `mem_mode` on `MidgeEngine` (if truly unused)
* Begin metadata simplification:

  * Identify all metadata caches (manifest cache, bloom cache, sparse index cache).
  * Add a `Metadata`/`MetadataView` abstraction that becomes the future aggregation point, even if not fully wired yet.

**Acceptance criteria:**

* Unused types and fields removed or clearly marked as deprecated.
* No tests break from the removal.
* There is a clear, single "front door" for reading metadata, even if it delegates internally.

---

## Phase 9 — RuntimeV2 (True Actor Executor)

This is the big architectural step: making the runtime the single brain.

### 9.1 RuntimeV2 Design & Skeleton

**Objective:** Have a clear, testable actor-runtime without touching current behavior yet.

* Write `docs/runtime/RUNTIME_V2.md` describing:

  * task model (`EngineTask` enum)
  * ownership rules (what state lives where)
  * concurrency model (single-threaded actor vs limited pool)
  * error propagation model.
* Implement `RuntimeV2` in `core/runtime/v2.rs`:

  * internal task queue
  * main event loop
  * hooks for posting tasks from `MidgeEngine`.

**Acceptance criteria:**

* `RuntimeV2` compiles, but may not yet be wired to production paths.
* Tests can enqueue synthetic tasks and observe ordering.

---

### 9.2 Feature-Flagged Parallel Runtime

**Objective:** Run old and new runtimes side-by-side for a while.

* Introduce a config flag:

  * `runtime_mode = Legacy | V2`
* Wire `MidgeEngine` to:

  * construct either the old runtime or `RuntimeV2` based on options.
* Initially, most work still goes through the legacy runtime; `RuntimeV2` can mirror or log without driving real actions.

**Acceptance criteria:**

* Both runtime modes compile and can be selected at engine creation.
* Basic tests run under both modes (even if V2 is still "shadowing").

---

### 9.3 Move Coordinators onto RuntimeV2 Tasks

**Objective:** Coordinators stop owning threads and start owning messages.

* For each coordinator type:

  * Convert work from "spawn thread / loop" to "post `EngineTask`s to RuntimeV2":

    * `FlushRequested(cf_id)`
    * `ScheduleCompaction`
    * `ExecuteCompaction(plan)`
    * `UploadWal(segment_id)`
    * `UploadSst(file_id)`
* Coordinators become lightweight facades over task submission + callbacks.

**Acceptance criteria:**

* No coordinator directly spawns long-lived threads.
* All background work flows through `RuntimeV2` when `runtime_mode = V2`.

---

### 9.4 Make Engine Runtime the Sole Owner of Engine State Transitions

**Objective:** Centralize all state mutations.

* Identify all places that currently:

  * mutate versions
  * update manifest
  * adjust SST sets
  * flip memtable states
* Move these transitions into:

  * `RuntimeV2` methods, or
  * "state transition" helpers called only by `RuntimeV2`.
* Prohibit direct state mutation from outside the runtime in V2 mode.

**Acceptance criteria:**

* In V2 mode, any change to:

  * manifest
  * active/immutable memtables
  * SST lists
    happens only from the runtime event loop.

---

### 9.5 Determinism & Performance Validation Under RuntimeV2

**Objective:** Ensure V2 is at least as correct and fast.

* Run:

  * determinism suite
  * YCSB Tier 1–4
  * durability and crash/restart tests
* Compare:

  * manifests
  * compaction logs
  * p50/p95/p99 latencies
  * throughput.

**Acceptance criteria:**

* No correctness regressions in tests.
* Tail latency and throughput are comparable or better in V2 for core workloads.

---

### 9.6 Cutover & Cleanup

**Objective:** Make V2 the default.

* Flip default `runtime_mode` to `V2`.
* Keep legacy runtime behind a non-default flag temporarily.
* After sufficient confidence:

  * remove legacy runtime
  * simplify code paths accordingly.

**Acceptance criteria:**

* Codebase has a single runtime implementation: `RuntimeV2`.
* Coordinator and state code no longer reference legacy runtime concepts.

---

## Phase 10 — Pipeline, Versioning & Mutable SSTs

### 10.1 Shared Entry Pipeline Helpers

**Objective:** Reduce duplication between flush and compaction data paths.

* Identify shared steps:

  * record creation
  * SST building
  * index generation
  * manifest update patterns
* Extract these into shared helpers under `core/pipeline/`.

**Acceptance criteria:**

* Flush and compaction reuse the same core building blocks.
* No duplicated encode/merge logic scattered across modules.

---

### 10.2 Version Management Simplification

**Objective:** Lower the complexity of version tracking.

* Inventory all "version" types.
* Design a simpler model with two core concepts:

  * `VersionSet` (persistent snapshots of the world)
  * `VersionManager` (live, mutable controller inside runtime).
* Migrate callers in small steps, with tests after each step.

**Acceptance criteria:**

* Version-related types reduced to a small, cohesive set.
* All tests involving snapshots, iterators, and compaction still pass.

---

### 10.3 Mutable SST Segments

**Objective:** Solve L0 explosion and write bursts cleanly.

* Add a `MutableSegment` concept:

  * local-only appendable SST
  * tracked in manifest as "mutable".
* Define lifecycle:

  * memtable → mutable segment → frozen SST → (optional) cloud upload.
* Update read path:

  * memtable → mutable segments → immutable SSTs → cloud.

**Acceptance criteria:**

* Under write-heavy workloads, L0 behaves more predictably (fewer tiny SSTs).
* YCSB A/F show smoother throughput and lower p99/p999.

---

### 10.4 Cloud-Tuned Policies

**Objective:** Align mutable segments and compaction with cloud economics.

* Tune:

  * segment size
  * freeze thresholds
  * upload batching
* Add configuration for:

  * local cache size vs. cloud reliance
  * upload concurrency
  * eviction policies.

**Acceptance criteria:**

* Hybrid mode benchmarks show:

  * fewer cloud PUTs
  * acceptable read latency
  * stable write throughput.

---

## Phase 11 — SLOs, Benchmarks & Docs

**Objective:** Turn Midge from "correct and fast" into a polished product.

* Define target SLOs (p99 read/write, recovery times).
* Finalize benchmark suite:

  * Tiered benches (hotpath → YCSB → soak).
  * Cloud vs local runs.
* Tighten docs:

  * `ARCHITECTURE.md`
  * `RUNTIME_V2.md`
  * `CLOUD_STORAGE.md`
  * `MUTABLE_SST.md`
* Produce a short "Adoption Guide" for Fitz/Uno.

---

## Guiding Principles Across All Phases

* **No panics on hot paths.**
* **Determinism first, micro-optimizations second.**
* **Runtime is the brain; everything else is a worker.**
* **Cloud is not a bolt-on — it's a primary durability path.**
* **Every architectural change gets at least one new invariant test.**
