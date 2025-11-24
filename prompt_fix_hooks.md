You are helping me fix the bugs in the Midge LSM engine where the test suite stops early because background worker threads panic.

Goal:
Ensure that any panic inside a worker thread NEVER aborts the test process. Every worker thread must be wrapped in a panic guard that converts the panic into a TestHooks signal instead of unwinding into the runner.

Rules:
1. Wrap all background thread entrypoints in std::panic::catch_unwind.
2. On panic, do NOT rethrow. Convert the panic into a TestHooks notification:
      hooks.record_worker_panic(kind)
   or use the appropriate existing failure counters (compaction_failed_count, etc.).
3. The worker thread must log the panic (eprintln or slog) but must not kill the test runner.
4. Never use unwrap(), expect(), or assume() inside worker threads unless it is inside the catch_unwind guard.
5. Keep normal worker behavior unchanged; only isolate and capture panics.
6. If a compaction failure is *intended* (e.g. FailMidway, CrashBeforeFsync), trigger the same test hook for deterministic tests, not a real thread panic.
7. The panic guard should be placed around the entire worker loop, not just individual operations.
8. Return a clean patch to the worker code implementing this behavior, and ensure tests that intentionally inject failures now get deterministic hook events instead of aborting the entire test suite.

Produce the code changes needed inside the worker file to wrap the spawned thread and the worker loop in a panic guard.
