# Midge Next-Gen Roadmap

This document complements `PLAN.md`, `ACTOR_MODEL.md`, and `NEXT_GEN.md`. It translates the phased blueprint into a clear roadmap and a prioritized TODO list so the team can make progress incrementally while minimizing risk.

High-level goals
- Centralize engine background work under `EngineRuntime` and enable deterministic compaction.
- Ship innovations behind conservative feature flags and avoid breaking public API.
- Maintain compatibility: old readers read new files; old engines read new data when possible.

Milestones & Timeline (high-level estimate)
- Phase 0 – Baseline & Guardrails: 1–2 days (complete)
- Phase 1 – Engine Runtime: 1–2 weeks
- Phase 2 – Deterministic Compaction: 2–4 weeks
- Phase 3 – Trie Index SST Format: 2–3 weeks
- Phase 4 – Unified Write Path: 3–6 weeks
- Phase 5 – Segment SSTs (Optional): 2–4 weeks

Developer workflows
- All new paths appear behind `EngineFlags` and default to `false`.
- Implement in small, testable steps: add types → add runtime plumbing → add tests → update docs/benchmarks.
- Use feature-gates in master on green tests, then run a targeted bench suite to capture the impact.

Phase-based TODOs

Phase 0: (Done)
- Implement `EngineFlags` (in `MidgeOptions`) and wire through `MidgeEngine`.
- Add `MIDGE_TRACE_RUNTIME` toggle for instrumentation.
- Validate via `cargo test` and `cargo run --bin validate_tests -- --summary`.

Phase 1: Engine Runtime (current work)
- Define `RuntimeTask`, `RuntimeTaskKind`, and `EngineRuntime` API (`submit`, `submit_and_wait`, optional trace logging).
- Ensure `MidgeEngine` owns `Arc<EngineRuntime>`.
- Route flush trigger through runtime `RuntimeTask` when `single_executor_runtime` is enabled.
- Route compaction triggers through runtime `RuntimeTask` (manual and scheduled compaction).
- Add `MIDGE_TRACE_RUNTIME` support to log queueing and execution decisions.
- Add test hooks for task injection and gating.

Phase 1 – Validation checklist
- [ ] `cargo test` with `single_executor_runtime = true`.
- [ ] Integration tests exercising flush/compaction behave identically when the flag is on/off.
- [ ] Bench micro-traces showing scheduling events when runtime tracing is enabled.

Phase 2: Deterministic Compaction
- Build `CompactionPlan`, `CompactionTask`, `CompactionLog` types.
- Implement a pure `planner(manifest)` that yields deterministic plans.
- Add runtime task to accept `CompactionTask`s, write a log entry, and execute tasks.
- Add manifest transition function for compaction results.
- Add replay tests and validation that compaction logs re-play and reproduce the same plan.

Phase 2 – Validation checklist
- [ ] Planner determinism unit tests.
- [ ] Compaction log replay tests.
- [ ] Durability: `deterministic_compaction = true` passes durability/recovery tests.

Phase 3: Trie SST Index
- Add writer support for optional trie index block, controlled by `new_sst_index`.
- Extend `SstReader` to detect the trie block and use it when present.
- Add fans-out test coverage and micro benchmarks for prefix-heavy reads.

Phase 4: Unified Write Path
- Introduce `WritePathCoordinator` with a single `.apply_write()` API.
- Refactor WAL + memtable + cache signaling to use the coordinator.
- Wire `WritePathCoordinator` to runtime tasks for flush/compaction triggers.

Phase 5: Mutable SST Segments
- Design `segment SST` and track segments in manifest separately.
- Add runtime logic to seal segments and promote them to normal SSTs.
- Add read path that checks memtable → segments → sealed SSTs.

Cross-cutting tasks
- Add observability: metrics and runtime trace logs (queue depth, latencies).
- Add gating and test-hooks for deterministic test cases (pause at gates and resume).
- Ensure graceful error handling and safe-mode when background tasks fail.
- Add docs to `docs/` and link to `NEXT_GEN.md` and `ACTOR_MODEL.md`.

Suggested immediate actions
- Formalize `RuntimeTask` types and `EngineRuntime` queue with a small PoC (done).
- Route a single path (flush) through runtime and add trace logs to validate behavior. This gives confidence for broader refactor work.
- Run unit / integration tests with `single_executor_runtime = true` and collect execution traces.

Contributing guide
- For each PR, include:
  - The flag(s) you added or modified.
  - Tests demonstrating both legacy and new behavior.
  - A brief benchmark or performance sample if the change affects the write or read path.

Contact / ownership
- Core engine team: overall
- Compaction & manifest: det. compaction owner
- SST format & index: sst/format owner

---

This `ROADMAP.md` aims to transform the plan into a tactical checklist and to give the team a practical sequence of deliverables. We can refine further per subcomponent and add issue tracker links once work begins. 
