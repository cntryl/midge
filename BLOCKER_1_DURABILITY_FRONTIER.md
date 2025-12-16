# Blocker #1: Durability Frontier Enforcement on Reads

## Status: STEP 1 COMPLETE ✅

### What we did

Added structural support for durability-aware reads. The runtime now carries durability requests through to the event loop.

#### Changes made:

1. **Modified `RuntimeMsg` enum** (`src/runtime/mod.rs`):
   - Added `requested_durability: crate::engine::api::Durability` field to `Read` message
   - Added `requested_durability: crate::engine::api::Durability` field to `RangeScan` message
   - Updated `request_id()` extraction to include both Read and RangeScan

2. **Updated engine API** (`src/engine/mod.rs`):
   - `get()` now passes `Durability::Steady` when calling `Read`
   - `get_cf()` (alias) inherits change
   - `get_transactional()` passes `Durability::Steady`
   - `range()` and `range_with_sequence()` pass `Durability::Steady`
   - `range_cf()` (alias) inherits change

3. **Updated snapshot API** (`src/engine/api/snapshot.rs`):
   - Snapshot `get()` passes `Durability::Steady`
   - Snapshot range scan passes `Durability::Steady`

4. **Updated event loop handlers** (`src/runtime/event_loop.rs`):
   - `Read` handler now extracts `requested_durability` field
   - `RangeScan` handler now extracts `requested_durability` field

5. **Updated tests** (`src/runtime/dispatch.rs`):
   - Dispatcher tests now include `requested_durability` field
   - Added ignored test `should_not_return_unsynced_data_on_read_with_strict_durability` to `tests/smoke.rs`

### Test results

```
running 12 tests
test should_read_written_value_when_in_memory ... ok
test should_hide_value_when_deleted ... ok
test should_preserve_latest_version_when_compacting ... ok
test should_preserve_tombstone_when_flushed ... ok
test should_read_written_value_after_flush ... ok
test should_maintain_isolation_given_snapshot_when_concurrent_writes ... ok
test should_respect_visibility_rules_when_range_scanning ... ok
test should_maintain_monotonic_sequence_numbers_when_writing ... ok
test should_persist_data_given_write_when_restarted ... ok
test should_not_corrupt_state_given_unclean_shutdown_when_recovering ... ok
test should_persist_tombstone_given_delete_when_restarted ... ok
test should_not_return_unsynced_data_on_read_with_strict_durability ... ignored

test result: ok. 11 passed; 0 failed; 1 ignored
```

### Next: STEP 2 - Event Loop Enforcement

The event loop now receives `requested_durability` but doesn't yet enforce it. The next step is:

1. In event loop's `Read` handler, check: if `requested_durability != CloudFirst` AND `sequence > local_durable_seq`:
   - Queue read in `durability_waiters`
   - Wait for frontier to advance before responding

2. Same for `RangeScan` handler

3. Add integration test: write, crash before fsync, verify read doesn't return the data on restart

---

## Commit message (for reference)

```
BLOCKER #1: Add durability field to Read/RangeScan messages

- Add `requested_durability: Durability` to RuntimeMsg::Read and RangeScan
- Engine passes Durability::Steady by default (balanced durability/performance)
- Event loop extracts durability but doesn't yet enforce it
- All tests pass; structure ready for enforcement logic

Enforcement (checking durable_seq before responding) is Step 2.
```

---

## Next steps

**STEP 2:** Implement enforcement in event loop (enforce_durability_frontier logic)
- Check: `if sequence > self.state.wal.local_durable_seq` and durability is Strict/Steady
- Queue read in `durability_waiters`
- Respond only after frontier advances

**STEP 3:** Add enforcement for CloudFirst mode
- Check: `if sequence > self.state.wal.cloud_durable_seq` when CloudFirst
- Queue read in `durability_waiters`

**STEP 4:** Add crash-recovery test
- Verify data is never visible after crash if not synced
