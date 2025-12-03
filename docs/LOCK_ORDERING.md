# Lock Ordering Protocol

This document defines the lock hierarchy for Midge to prevent deadlocks.

## Core Principle

**Always acquire locks in the order specified below. Never acquire a higher-numbered lock while holding a lower-numbered lock.**

## Lock Hierarchy (Low to High)

### Level 0: Fine-Grained State (Lock-Free or Very Short-Lived)
- AtomicU64 sequence numbers
- Arc-swap for read-heavy structures
- DashMap internal shards (never acquired explicitly)

### Level 1: Column Family Locks
- `ColumnFamilySet::create_lock` - Protects CF creation/deletion
  - Only held during check-then-insert across name_to_id and cfs maps
  - **Never acquire Level 2+ locks while holding this**

### Level 2: Per-CF Data Structures
- `ColumnFamily::immutable_memtables` - Protects immutable memtable list
- `ColumnFamily::compaction_filter` - RwLock for compaction filter
  - Read locks are short-lived
  - **Never block or acquire other locks while holding read lock**

### Level 3: Background Worker Coordination
- `FlushCoordinator::tx` - No explicit lock (channel-based)
- `CompactionController::tx` - No explicit lock (channel-based)
- `VersionManager::tx` - Mutex protecting channel sender
  - **Must drop lock before calling recv() on response channel**

### Level 4: Version & Manifest State
- `VersionManager` actor loop - Single-threaded, no explicit locks
- Manifest file I/O - No locks held during write operations

### Level 5: Engine-Wide State
- `MidgeEngine::flush_mutex` - Protects flush operations
- `MidgeEngine::merge_operators` - RwLock for merge operator registry
- `MidgeEngine::background_error` - RwLock for error state
  - **Read locks must be released immediately after check**
  - Never block while holding read lock

### Level 6: Test Hooks (Test Code Only)
- `TestHooks::compaction_gate`
- `TestHooks::flush_gate`
- `TestHooks::manifest_update_notifiers`

## Special Cases

### BatchedSyncCoordinator
- Uses single Mutex + Condvar (mutex-based design)
- Leader releases lock before performing fsync
- Followers wait on condvar, not spin loops
- **Pattern**: Lock → Check state → Either lead or wait → Release lock before I/O

### Channel Operations
- **Always release locks before blocking recv()**
- Use `try_send()` in Drop implementations (never block in Drop)
- Bounded channels: Consider backpressure implications

### RwLock Guidelines
- Prefer read locks for read-heavy operations
- Release read lock immediately after check
- Never call blocking operations while holding read lock
- Never acquire write lock while holding read lock (self-deadlock)

## Drop Implementation Rules

**CRITICAL**: Never block indefinitely in Drop implementations.

1. Use `try_send()` instead of `send()` for shutdown signals
2. Use timeouts when waiting for threads: max 100ms
3. Log warning if thread doesn't finish cleanly
4. Let OS clean up hung threads on process exit

## Validation

### Debug Builds
Enable lock order checking with:
```rust
#[cfg(debug_assertions)]
fn validate_lock_order() { ... }
```

### Testing
Run deadlock detection:
```bash
cargo run --bin detect_deadlocks -- --summary
```

## Common Pitfalls

### ❌ DON'T: Hold lock during blocking I/O
```rust
let guard = mutex.lock();
std::fs::read(&path)?;  // WRONG: I/O while holding lock
```

### ✅ DO: Release lock before I/O
```rust
let value = {
    let guard = mutex.lock();
    guard.clone()
};
std::fs::read(&path)?;  // OK: Lock released
```

### ❌ DON'T: Block in Drop
```rust
impl Drop for Worker {
    fn drop(&mut self) {
        self.handle.join();  // WRONG: Can block forever
    }
}
```

### ✅ DO: Timeout in Drop
```rust
impl Drop for Worker {
    fn drop(&mut self) {
        let timeout = Duration::from_millis(100);
        if let Some(handle) = self.handle.take() {
            let start = Instant::now();
            while !handle.is_finished() && start.elapsed() < timeout {
                thread::sleep(Duration::from_millis(10));
            }
            if handle.is_finished() {
                let _ = handle.join();
            }
        }
    }
}
```

### ❌ DON'T: Nested lock acquisition without order
```rust
let guard_a = mutex_a.lock();
let guard_b = mutex_b.lock();  // WRONG: No defined order
```

### ✅ DO: Follow hierarchy
```rust
// Always acquire lower-numbered locks first
let guard_b = mutex_b.lock();  // Level 2
let guard_a = mutex_a.lock();  // Level 1
```

## Evolution

When adding new locks:
1. Assign a level in the hierarchy
2. Document in this file
3. Add validation tests
4. Review all acquisition sites for order compliance

## References

- Original batched_sync redesign: [PR #XXX]
- Deadlock detection tool: `testutils/detect_deadlocks.rs`
- Test timeout guide: `docs/TEST_TIMEOUT_GUIDE.md`
