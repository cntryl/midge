# engine_ttl.rs - Spec Card

## Philosophy

Tests define the **correct future behavior**, not document current limitations. Always implement tests fully; they may fail until features exist.

- ✅ Write ALL tests (never `#[ignore]`)
- ✅ Tests **MAY FAIL** if features aren't implemented yet
- ✅ Once features are built, failing tests become passing tests
- ✅ Tests act as a specification for what code needs to do
- ❌ Never stub behavior; always assert desired semantics
- ❌ Never skip tests on certain storage modes; use conditional logic instead

---

## PROMPT (Self-Driving Implementation Guide)

**Create a test file that validates TTL (Time-To-Live) expiration semantics.**

**Key Requirements**:
- All 12 tests parametrized across all storage modes (Memory, LocalDisk, CloudBacked)
- Pattern: `for_each_storage_mode(&all_storage_modes_new(), |mode, opts| { ... })`
- TTL semantics: keys expire after specified duration
- Expiration at read time: expired keys return None, not stored value
- Zero TTL: means no expiration (key lives forever)
- TTL in write batches: batch operations can have TTL
- TTL in snapshots: snapshots respect TTL at snapshot time
- Persistence: TTL metadata persisted, honored after restart
- Compaction cleanup: expired entries removed during compaction
- Mixed TTL: some keys expire, others don't

**Testing Approach**:
1. Write key with TTL, read before expiry → returns value
2. Write key with TTL, wait until expiry → returns None
3. Zero TTL → key never expires
4. Batch with TTL values → TTL applied to batch operations
5. Snapshot before/after expiry → snapshot respects TTL state at time
6. Restart with expired TTL → keys still expired
7. Compaction removes expired entries
8. Mix TTL and non-TTL keys
9. TTL metadata persisted
10. Update TTL on overwrite → new TTL applied
11. Snapshot hides expired keys
12. Range queries skip expired keys

---

**File Location**: `tests/engine_ttl.rs`
**Test Count**: 12 tests
**Storage Modes**: ALL (Memory, LocalDisk, CloudBacked)
**Pattern**: `for_each_storage_mode(&all_storage_modes_new(), |mode, opts| { ... })`
**Status**: 🚧 7/12 passing (5 failing - compaction cleanup pending)

---

## Purpose

Test TTL (Time-To-Live) expiration: keys can have associated expiration times, after which they return None on read. TTL is essential for cache-like workloads and automatic data cleanup.

---

## Tests

1. **should_return_value_given_ttl_not_elapsed_when_reading**
   - Write key with TTL=1 hour, read immediately, returns value

2. **should_return_none_given_ttl_elapsed_when_reading**
   - Write key with TTL=0 (immediate expiry), read, returns None

3. **should_not_expire_key_given_zero_ttl_means_no_expiration_when_reading**
   - Zero TTL → key never expires (lives forever)

4. **should_persist_ttl_metadata_given_restart_when_reopening**
   - Write key with TTL, flush, restart, TTL metadata preserved

5. **should_expire_after_restart_given_ttl_elapsed_during_shutdown_when_reopening**
   - Shutdown with TTL active, wait, restart, key expired

6. **should_remove_expired_entries_given_compaction_when_ttl_exceeded**
    - Expired keys removed during compaction

7. **should_preserve_non_expired_entries_given_compaction_when_ttl_not_exceeded**
   - Non-expired keys preserved during compaction

8. **should_hide_expired_key_given_snapshot_after_expiry_when_reading_at_snapshot**
   - Snapshot after expiry doesn't see key

9. **should_show_key_given_snapshot_before_expiry_when_reading_at_snapshot**
   - Snapshot before expiry sees key

10. **should_apply_ttl_given_write_batch_with_ttl_when_committed**
    - Batch operation with TTL, TTL applied

11. **should_handle_mixed_ttl_keys_given_some_expire_when_reading**
    - Mix of TTL and non-TTL keys, each behaves correctly

12. **should_update_ttl_given_overwrite_with_new_ttl_when_writing**
    - Overwrite key with new TTL, new TTL applied

---

## Key APIs

- `engine.put_with_ttl(cf, key, value, ttl_duration)` → Result
- `engine.get(cf, key)` → Result<Option<Bytes>> (returns None if expired)
- `WriteBatch::put_with_ttl(cf, key, value, ttl)` → WriteBatch
- TTL metadata internal (not directly queryable)

---

## Implementation Notes

✅ All tests use `all_storage_modes_new()` (TTL semantics are mode-invariant)
✅ Expiration checked at read time (lazy deletion)
✅ Compaction removes expired entries (eager cleanup)
✅ TTL persisted as metadata alongside value
✅ Zero/None TTL means never expire
✅ TTL metadata preserved across restart

---

## Test Pattern Example

```rust
#[test]
fn should_return_value_given_ttl_not_elapsed_when_reading() {
    for_each_storage_mode(&all_storage_modes_new(), |mode, opts| {
        // Arrange
        let engine = open_with_mode(opts, mode);
        let cf = engine.default_column_family();
        let ttl = std::time::Duration::from_secs(3600); // 1 hour
        
        // Act
        engine.put_with_ttl(cf, b"key", b"value", ttl).unwrap();
        
        // Assert
        assert_eq!(engine.get(cf, b"key").unwrap(), Some(Bytes::from_static(b"value")), "key should be readable in mode: {}", mode);
    });
}
```

---

## Status

**Current**: 🚧 7/12 passing (5 failing - compaction cleanup and expiry timing pending)
**Notes**: Basic TTL working; expiry cleanup and compaction integration pending

---

## References
- See INTEGRATION_TESTS_FINAL.md lines ~730 for full TTL spec
- TTL API in `src/engine/api.rs`
