# Locking Module Refactoring - Deduplication Summary

## Problem

The `LocalFileLock` and `CloudLeaseLock` implementations had significant code duplication in their renewal thread logic:

- **Identical patterns**: Both spawned background threads with stop signals
- **Duplicate boilerplate**: Thread spawning, stop signal coordination, join handling
- **Inconsistent error handling**: Small variations between implementations
- **Maintenance burden**: Changes had to be applied twice

### Before: 130+ lines of duplicated renewal logic

**LocalFileLock:**
```rust
renewal_handle: Option<JoinHandle<()>>,
stop_renewal: Arc<Mutex<bool>>,

fn start_renewal_thread(&mut self) {
    let stop_signal = Arc::clone(&self.stop_renewal);
    let renewal_interval = Duration::from_millis((self.ttl_ms as u64) / 2);
    
    let handle = thread::spawn(move || {
        loop {
            {
                let stop = stop_signal.lock();
                if *stop { break; }
            }
            thread::sleep(renewal_interval);
            // ... renewal logic ...
        }
    });
    self.renewal_handle = Some(handle);
}

fn release(&mut self) -> MidgeResult<()> {
    {
        let mut stop = self.stop_renewal.lock();
        *stop = true;
    }
    if let Some(handle) = self.renewal_handle.take() {
        let _ = handle.join();
    }
    // ... cleanup ...
}
```

**CloudLeaseLock:** Nearly identical with different renewal callback.

## Solution

Created a new `renewal.rs` module with a reusable `RenewalThread` abstraction:

### New Architecture

```
src/core/locking/
├── traits.rs     - DbLock trait
├── meta.rs       - LockMeta serialization
├── renewal.rs    - NEW: Common renewal infrastructure
├── local.rs      - Uses RenewalThread
├── cloud.rs      - Uses RenewalThread
└── mod.rs        - Public API
```

### RenewalThread API

```rust
pub(super) struct RenewalThread {
    handle: Option<JoinHandle<()>>,
    stop_signal: Arc<Mutex<bool>>,
}

impl RenewalThread {
    /// Create a new renewal thread infrastructure (not yet started).
    pub(super) fn new() -> Self;

    /// Start the renewal thread with the given interval and renewal callback.
    pub(super) fn start<F>(&mut self, renewal_interval: Duration, renewal_fn: F)
    where
        F: FnMut() + Send + 'static;

    /// Signal the renewal thread to stop and wait for it to finish.
    pub(super) fn stop(&mut self);

    /// Check if the renewal thread is currently running.
    pub(super) fn is_running(&self) -> bool;
}

/// Helper to compute renewal interval from TTL (TTL / 2).
pub(super) fn renewal_interval_from_ttl(ttl_ms: u32) -> Duration;
```

### After: Simplified implementations

**LocalFileLock:**
```rust
renewal: RenewalThread,

fn start_renewal_thread(&mut self) {
    let lock_path = self.lock_path.clone();
    let renewal_interval = renewal_interval_from_ttl(self.ttl_ms);

    self.renewal.start(renewal_interval, move || {
        // Just the renewal logic - no boilerplate
        if let Ok(data) = fs::read(&lock_path) {
            if let Ok(mut meta) = LockMeta::decode(&data) {
                meta.renew();
                // ... atomic write ...
            }
        }
    });
}

fn release(&mut self) -> MidgeResult<()> {
    self.renewal.stop();  // One line!
    // ... cleanup ...
}
```

**CloudLeaseLock:** Same pattern, different callback.

## Benefits

### 1. **Reduced Code Duplication**
- Eliminated ~130 lines of duplicated thread management code
- Single source of truth for renewal thread lifecycle

### 2. **Improved Maintainability**
- Thread management bugs fixed once, benefit both implementations
- Consistent behavior across local and cloud locks

### 3. **Better Testability**
- `RenewalThread` has isolated unit tests
- Lock implementations test only their specific renewal logic
- Clear separation of concerns

### 4. **Cleaner API**
- Lock implementations focus on *what* to renew, not *how* to renew it
- Renewal interval calculation centralized (`renewal_interval_from_ttl`)

### 5. **Encapsulation**
- `RenewalThread` is `pub(super)` - not exposed outside locking module
- Implementation detail properly hidden from consumers

## Key Design Decisions

### 1. **Callback-based design**
Renewal logic is passed as a closure, allowing each lock type to customize the renewal behavior while reusing the threading infrastructure.

### 2. **Automatic cleanup on Drop**
`RenewalThread` implements `Drop` to ensure the thread is always stopped, preventing resource leaks even if `release()` isn't called explicitly.

### 3. **Module-private visibility**
`RenewalThread` is only visible within the `locking` module (`pub(super)`), keeping it as an implementation detail.

### 4. **Test coverage**
Added comprehensive tests for `RenewalThread`:
- Periodic execution verification
- Stop signal handling
- TTL interval calculation

## Code Metrics

| Metric | Before | After | Improvement |
|--------|--------|-------|-------------|
| **Total LOC (locking module)** | 951 | ~950 | Similar |
| **Duplicated code** | ~130 lines | 0 lines | -100% |
| **Files in module** | 4 | 5 | +1 (new abstraction) |
| **Lines per file (avg)** | 237 | 190 | -20% |
| **Test coverage** | 10 tests | 12 tests | +20% |

## Testing

All 1094 tests pass, including:
- Existing local and cloud lock tests
- New `RenewalThread` unit tests
- Integration tests remain unchanged

## Pattern Established

This refactoring demonstrates a clean pattern for extracting common functionality:

1. ✅ Identify duplicated code
2. ✅ Create focused abstraction module
3. ✅ Use callback/closure for customization points
4. ✅ Keep abstraction module-private
5. ✅ Add comprehensive tests
6. ✅ Verify all existing tests still pass

This pattern can be applied to other areas of the codebase (e.g., compaction executors, flush workers).

## Future Improvements

Potential enhancements to consider:

1. **Configurable backoff strategy**: Allow exponential backoff for renewals
2. **Renewal failure callbacks**: Notify locks when renewal fails (for metrics/logging)
3. **Renewal statistics**: Track renewal success/failure rates
4. **Graceful degradation**: Fallback behavior when renewals consistently fail

## Related Work

This refactoring complements the broader `core` module restructuring outlined in `docs/dev/core_refactoring_plan.md`. It demonstrates that not all refactorings require splitting files - sometimes extracting shared abstractions is more valuable.
