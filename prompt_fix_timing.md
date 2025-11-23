You are helping me audit and fix flaky tests across the entire Midge LSM storage engine.

Your job is to rewrite ANY test in this file to be fully deterministic and free of timing issues or race conditions, using Midge’s testing primitives and gatepoints.

General goal:
Every test should rely on logical events, test hooks, and deterministic progress—not wall-clock timing, sleeps, or scheduler luck.

Apply these rules to ALL tests you modify:

1. Do NOT use std::thread::sleep or any fixed timing assumptions.
2. For flush behavior, use MemtableGatePoints or force_flush() to block until flush visibility is deterministic.
3. For compaction, use CompactionGatePoints (BeforeExecution, AfterManifestUpdate, AfterFsync, etc.) and wait for those gates instead of timing.
4. For WAL sequencing or durability, use WAL test hooks such as force_rotate() and force_sync() rather than waiting.
5. For manifest visibility, wait for the specific manifest write gate instead of assuming ordering.
6. Prefer single-threaded tests with EphemeralKV or MockKV when a real backend is unnecessary.
7. When concurrency is required, synchronize with channels, barriers, or Midge test hooks—not on OS scheduling or sleeps.
8. Assert on logical events (“compaction complete”, “flush finished”, “manifest updated”, “SST sequence increased”, etc.) instead of elapsed time.
9. Ensure background workers cannot race the test: install a gate, block the worker, inspect state, then release deterministically.
10. Use deterministic sequence allocators (SST/WAL) if a test compares filenames or expects numeric ordering.
11. For tests involving multiple column families, ensure CF creation and registration is fully completed before performing any operations.
12. Rewrite the test so that it produces identical results regardless of core count, machine speed, or slow CI environments.
13. Remove flakiness and nondeterminism wherever present.

For each test you rewrite, apply the above rules and return a clean, idiomatic, stable version. Also provide a brief explanation of how the rewrite eliminates the previous race or timing issues.
