You are helping me audit the Midge LSM storage engine for ANY code paths that can panic.

Goal:
Midge must never panic in production or background workers. All errors must be returned as Result<T, MidgeError> or surfaced through TestHooks. Only test code may use panic, and even then it must be wrapped with catch_unwind.

Sweep this file (and any related modules it references) for:

1. unwrap(), expect(), or indexing operations that can panic.
2. assert!(), debug_assert!(), panic!(), todo!(), unimplemented!(), or unreachable!() that will panic at runtime.
3. Any code in workers that could cause thread panics (compaction, WAL, flush, manifest, CF registration, etc.).
4. Any error paths that implicitly panic via FromResidual, Try, or Option/Result combinators.
5. Any constructors or initialization paths that can panic via unwrap or expect.
6. Any concurrency operations that could panic due to poisoning or misuse.
7. Any path where test failure-injection uses panic instead of returning a structured error through hooks.

For every panic hazard you find:

- Replace the panic with a Result<T, MidgeError>.
- If inside a background worker, keep the error inside the worker loop and propagate it through hooks, never unwind the thread.
- If inside test code, isolate the panic behind catch_unwind.
- Convert expect()/unwrap() to proper error handling.
- Replace assert!() with explicit error returns or hook notifications unless it is test-only debug logic.
- Remove todo!/unimplemented! and convert them into meaningful error variants.

Return a cleaned, panic-free version of the code, preserving behavior while eliminating runtime panics.

Also give a short summary of what panic risks were removed and what the safe replacements are.
