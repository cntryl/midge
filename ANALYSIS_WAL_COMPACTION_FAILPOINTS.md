# Analysis: Midge WAL & Compaction Failpoints and Durability

**Date**: March 8, 2026  
**Scope**: WAL interaction with compaction, existing failpoints, durability gaps

---

## Executive Summary

Midge implements a **Slice 6** compaction completion safety model with three critical failpoints injected at key durability boundaries. The system uses two complementary durability mechanisms:

1. **Intent Log** (`intent_log.yaml`): Soft-state recovery log recording state transitions
2. **Manifest Persistence** (`manifest.yaml`): Hard durability for SST file membership and compaction results

**Key Finding**: WAL and compaction are well-synchronized at manifest persistence boundaries, but several WAL operations **lack failpoints** that should exist for complete durability testing and crash-safety verification.

---

## Part 1: Current WAL Failpoints

### 1.1 Failpoint: `midge::wal::after_append_batch_before_sync`

**Location**: [src/runtime/actors/wal.rs](src/runtime/actors/wal.rs#L793)

**Context**: Early in the write path during batch append
```rust
writer.append_batch(&wal_records)?;
fail::fail_point!("midge::wal::after_append_batch_before_sync");
```

**Purpose**:
- Test crash **after** writing records to WAL file buffer
- **Before** fsync / durability guarantee is established
- Models: Process crash, storage layer crash while data in OS buffer cache
- **Risk window**: Data written to OSfilbuffer but not flushed; vulnerable to power loss

**Data State After Failpoint**:
- WAL records may or may not be on stable storage
- Memtable updates NOT yet applied (happens only after sync)
- Sequence counter advanced but durability not confirmed
- Write operation still in-flight (no response to client)

**Recovery Behavior**:
- WAL recovery replays unfsynced data from `wal.log`
- Memtables rebuilt with all records up to last complete fsync
- Stale writer epoch records skipped (see `max_epoch_seen` in recovery)

---

### 1.2 Failpoint: `midge::wal::after_fsync_before_durable_frontier`

**Location**: [src/runtime/actors/wal.rs](src/runtime/actors/wal.rs#L1240)

**Context**: After fsync, before updating durable frontiers
```rust
f.sync(Durability::Durable).map_err(...)?;
fail::fail_point!("midge::wal::after_fsync_before_durable_frontier");
state.wal.last_synced_seq = state.sequence;
state.wal.local_durable_seq = state.sequence;
```

**Purpose**:
- Test crash **after** fsync but **before** in-memory frontier is updated
- Models: Crash immediately after I/O completes, before bookkeeping
- **Risk window**: Very narrow — fsync guaranteed on disk, but process state desynchronized

**Data State After Failpoint**:
- WAL records **definitely on disk** (fsync completed)
- In-memory `local_durable_seq` still points to old sequence
- Recovery will replay entire unsynced range again (redundant but safe)
- Group commit waiters may not be advanced until restart

**Recovery Behavior**:
- WAL recovery starts from `last_synced_seq` on disk  
- May replay some records twice, but idempotency + epoch semantics catch stale writes
- `local_durable_seq` restored from WAL file inspection during recovery

---

## Part 2: Current Compaction-Related Failpoints

### 2.1 Failpoint: `slice6::after_compaction_update_before_manifest_persist`

**Location**: [src/runtime/event_loop/mod.rs](src/runtime/event_loop/mod.rs#L1266)

**Context**: After in-memory manifest update, before persistence
```rust
// Apply compaction changes to in-memory manifest (manifest_complete())
if let Err(e) = self.manifest_actor.compaction_complete(&mut self.state, ...) {
    // Error response
} else {
    fail::fail_point!("slice6::after_compaction_update_before_manifest_persist");
    
    // Persist manifest to disk
    if let Err(e) = self.manifest_actor.persist(&self.state) {
        // Error response
    } else {
        // Proceed to GC
    }
}
```

**Purpose**:
- Test crash **after** in-memory manifest updated with compaction results
- **Before** manifest written and synced to disk
- Models: Crash between manifest publication and durability
- **Risk window**: In-memory state shows new SST structure, but it's not persistent

**Historical Context** (from Slice 6 implementation):
- This failpoint was added to enable **Slice 6 chaos testing**
- Proves that manifest persistence is the **durability boundary** for compaction
- Without it, system cannot distinguish between "compaction recorded" and "compaction durable"

**Data State After Failpoint**:
- **In-memory manifest**: Updated with:
  - Old compaction input SSTs removed
  - New output SSTs added at correct levels
  - Level ranges and file counts correct
- **On-disk manifest**: Still reflects pre-compaction state
- **Input SSTs on disk**: Still exist (not yet deleted)
- **Output SSTs on disk**: Exist but not yet "owned" by manifest
- **Intent log**: `CompactionApplied {removed: [...], added: [...]}` persisted

**Recovery Behavior**:
- Manifest reload from `manifest.yaml` (pre-compaction state)
- Intent log shows `CompactionApplied` entry
  - System can detect incomplete compaction during recovery
  - **Action**: Re-scan SST directory and GC output SSTs that aren't in manifest
- Memtables unaffected (compaction doesn't touch them)
- Reads may miss newly-compacted data until restart

**Critical Distinction**:
- Compaction completion in memory ≠ compaction durable
- This failpoint explicitly tests this separation

---

### 2.2 Failpoint: `slice6::after_manifest_persist_before_sst_gc`

**Location**: [src/runtime/event_loop/mod.rs](src/runtime/event_loop/mod.rs#L1283)

**Context**: After manifest persistence, before SST deletion
```rust
if let Err(e) = self.manifest_actor.persist(&self.state) {
    // Error response
} else {
    fail::fail_point!("slice6::after_manifest_persist_before_sst_gc");
    
    // Queue GC deletion of input SSTs
    if let Err(e) = self.gc_actor.delete_ssts(&mut self.state, &input_ssts) {
        // Warn (non-fatal)
    }
    self.respond(request_id, RuntimeResponse::Ok { request_id });
}
```

**Purpose**:
- Test crash **after** manifest is durable, **before** old SSTs are deleted
- Models: Incomplete garbage collection
- **Risk window**: Orphaned files on disk but not dangerous integrity-wise

**Data State After Failpoint**:
- **On-disk manifest**: Shows post-compaction state
  - Input SSTs **removed** from manifest
  - Output SSTs **added** to manifest
- **Input SSTs on disk**: Still physically exist (not yet deleted)
- **Output SSTs on disk**: Referenced by manifest
- **Response to client**: Already sent (compaction success)
- **Intent log**: Already persisted

**Recovery Behavior**:
- Manifest reloaded (post-compaction state)
- GC actor checks: files on disk not in manifest
  - Input SSTs are **orphaned** (can be safely deleted)
  - Output SSTs are **live** (referenced by manifest)
- GC cleanup scheduled opportunistically (low priority)
- Reads work correctly (manifest is source of truth)
- **Space reclamation deferred**, but data integrity is preserved

**Why This Failpoint Matters**:
- Proves that **manifest is source of truth** for file ownership
- Proves garbage collection can be asynchronous
- Proves crash after manifest persist has bounded impact on durability

---

### 2.3 Failpoint: `midge::manifest::after_temp_sync_before_rename`

**Location**: [src/metadata/persistence.rs](src/metadata/persistence.rs#L179)

**Context**: During manifest file persistence
```rust
// Sync temp manifest to disk
f.sync(Durability::Durable)?;

fail::fail_point!("midge::manifest::after_temp_sync_before_rename");

// Atomic rename temp → manifest.yaml
fs.rename_atomic(&temp_path, &FsPath::new(Self::MANIFEST_FILE))?;
```

**Purpose**:
- Test crash **after** temp manifest synced, **before** atomic rename
- Models: Incomplete atomic operation or filesystem crash during rename
- **Risk window**: Temp file on disk but not yet published as the manifest

**Data State After Failpoint**:
- **`manifest.yaml.tmp`**: On disk, synced, with new state
- **`manifest.yaml`**: Unchanged (old state)
- **In-memory manifest**: May reflect new state (if process was running)
- **Active readers**: Use old manifest (point-in-time copy)

**Recovery Behavior**:
- On restart, load `manifest.yaml` (old state) — temp file ignored
- Temp file is detected but not part of recovery
- System operates with pre-compaction manifest
- Orphaned output SSTs are cleaned up on next GC check
- **Idempotent**: Safe to retry manifest persistence

**Deferred Cleanup**:
- Temp file may remain on disk indefinitely (not cleaned up)
- Low-impact garbage (ignored on reload)
- Could add `manifest.yaml.tmp` cleanup to startup

---

### 2.4 Failpoint: `midge::flush::after_sst_write_before_publish`

**Location**: [src/runtime/actors/flush.rs](src/runtime/actors/flush.rs#L166)

**Context**: After SST file written, before manifest publication
```rust
self.write_memtable_to_sst(&frozen, &sst_path)?;
fail::fail_point!("midge::flush::after_sst_write_before_publish");

// Signal flush completion (cloud upload if needed)
if let Some(hybrid) = sba { ... }
// Queue SST for manifest addition (implicit)
```

**Purpose**:
- Test crash **after** SST written to disk **before** manifest knows about it
- Models: Flush completes but event loop doesn't acknowledge
- **Risk window**: Orphaned SST file not yet referenced by manifest

**Data State After Failpoint**:
- **SST file on disk**: Complete, synced, valid
- **Manifest**: Doesn't yet reference this SST
- **In-memory state**: Flush actor marked as in-progress
- **Memtable**: Still frozen, occupies memory

**Recovery Behavior**:
- SST file exists but manifest doesn't reference it
- GC identifies as orphaned during cleanup check
- Can be safely deleted
- Memtable is recovered from WAL
- Flush must be retried or detected as incomplete

---

## Part 3: Data Flow: Write → Compaction → Manifest → GC

```
┌─────────────────────────────────────────────────────────────────┐
│ CLIENT REQUEST: Write Transaction                              │
├─────────────────────────────────────────────────────────────────┤
│                                                                 │
│ 1. RUNTIME: Allocate global sequence number                    │
│    └─> state.next_sequence()                                   │
│                                                                 │
│ 2. WAL ACTOR: Append to write-ahead log                        │
│    └─> writer.append_batch(&wal_records)                       │
│        ❌ FAILPOINT: midge::wal::after_append_batch_before_sync│
│        │ (records in buffer, not yet durable)                  │
│                                                                 │
│ 3. WAL ACTOR: Sync to disk (group commit)                      │
│    └─> writer.flush() → fsync(wal.log)                        │
│        ❌ FAILPOINT: midge::wal::after_fsync_before_...        │
│        │  (records on disk, frontiers not updated)             │
│        └─> UPDATE: local_durable_seq ← sequence                │
│            UPDATE: pending_writes = 0                          │
│                                                                 │
│ 4. MEMTABLE: Apply transaction (if durability satisfied)       │
│    └─> memtable.put(...) → visible to reads                   │
│                                                                 │
│ 5. INTENT LOG: Record SstAdded intent                          │
│    └─> state.append_intent(SstAdded) → persist to disk        │
│                                                                 │
│ 6. CLIENT: Receive response (sequence/durability guarantees)   │
│                                                                 │
└─────────────────────────────────────────────────────────────────┘
         ↓
         ↓ (Memtable reaches size threshold)
         ↓
┌─────────────────────────────────────────────────────────────────┐
│ FLUSH: Memtable → SST                                           │
├─────────────────────────────────────────────────────────────────┤
│                                                                 │
│ 1. FLUSH ACTOR: Freeze memtable (stop new writes)             │
│    └─> Create new empty memtable for incoming writes           │
│                                                                 │
│ 2. FLUSH ACTOR: Write frozen memtable to SST file             │
│    └─> sst_writer.write_all(records) → sync to disk           │
│        ❌ FAILPOINT: midge::flush::after_sst_write_before_...  │
│        │ (SST on disk, not yet in manifest)                    │
│                                                                 │
│ 3. MANIFEST ACTOR: Add SST to manifest                        │
│    └─> Append intent: SstAdded {file_meta}                    │
│    └─> Append mangicest edit: AddSst(...)                      │
│    └─> Update in-memory manifest                               │
│                                                                 │
│ 4. FLUSH COMPLETE: Signal ready for cloud upload              │
│    └─> cloud_actor.queue_for_upload(...) (if CloudFirst)      │
│                                                                 │
└─────────────────────────────────────────────────────────────────┘
         ↓
         ↓ (L0 compaction triggered or scheduled)
         ↓
┌─────────────────────────────────────────────────────────────────┐
│ COMPACTION: Multi-SST → Output SST                             │
├─────────────────────────────────────────────────────────────────┤
│                                                                 │
│ 1. COMPACTION ACTOR: Plan compaction (pick input SSTs)        │
│    └─> Read manifests, check size/overlap                      │
│    └─> Allocate output session ID                              │
│                                                                 │
│ 2. COMPACTION ACTOR: Execute compaction (async/separate thread)│
│    └─> Collect versions from input SSTs                        │
│    └─> Merge, deduplicate, apply tombstones                    │
│    └─> Write output SST(s) to disk → sync                     │
│    └─> Signal CompactionComplete via event loop               │
│                                                                 │
│ 3. EVENT LOOP: Handle CompactionComplete message               │
│    ├─> Decrement active_compactions counter                    │
│    │                                                            │
│    ├─> Clean up local state (compaction_actor.handle_complete) │
│    │   └─> Mark input SSTs no longer being compacted           │
│    │                                                            │
│    └─> CRITICAL SECTION: Manifest update & persistence        │
│        ├─> manifest_actor.compaction_complete(...)            │
│        │   ├─> Append intent: CompactionApplied               │
│        │   ├─> Append manifest edits: RemoveSst, AddSst       │
│        │   └─> Update in-memory manifest (input removed, output added)
│        │                                                        │
│        ├─> ❌ FAILPOINT: slice6::after_compaction_update_...  │
│        │   │ (in-memory manifest updated, NOT YET PERSISTENT)  │
│        │   │ RISK: Crash here → need manifest persistence     │
│        │   │       to recover state                            │
│        │   │                                                    │
│        ├─> manifest_actor.persist(...) [DURABILITY BOUNDARY] │
│        │   ├─> Write temp: manifest.yaml.tmp                  │
│        │   ├─> Sync: fsync(manifest.yaml.tmp)                 │
│        │   ├─> ❌ FAILPOINT: midge::manifest::after_temp_...  │
│        │   │   (temp synced, atomic rename not yet done)       │
│        │   │ RISK: Low — temp ignored, old manifest used      │
│        │   │                                                    │
│        │   └─> Atomic rename: manifest.yaml.tmp → manifest.yaml
│        │       SAFE: Manifest now durable with compaction results
│        │                                                        │
│        ├─> ❌ FAILPOINT: slice6::after_manifest_persist_...  │
│        │   │ (manifest persisted, GC not yet started)          │
│        │   │ RISK: Input SSTs still on disk but orphaned      │
│        │   │       Safe — manifest is source of truth         │
│        │   │                                                    │
│        └─> gc_actor.delete_ssts(&input_ssts) [ASYNC/GC]      │
│            ├─> Check: File in manifest? No → safe to delete   │
│            ├─> Check: In active compaction? No → safe         │
│            ├─> Check: Pinned by snapshot? No → safe           │
│            └─> DELETE input SSTs from filesystem              │
│                                                                 │
│ 4. RESPOND: Send CompactionComplete response to client         │
│                                                                 │
└─────────────────────────────────────────────────────────────────┘
         ↓
         ↓ (Time passing, more writes/flushes)
         ↓
┌─────────────────────────────────────────────────────────────────┐
│ CRASH & RECOVERY                                                │
├─────────────────────────────────────────────────────────────────┤
│                                                                 │
│ 1. Load manifest.yaml (restored pre-compaction state if        │
│    crashed between steps 3.2 and 3.4)                          │
│                                                                 │
│ 2. Load intent_log.yaml (shows state transitions)             │
│    └─> If CompactionApplied exists but manifest not updated    │
│        └─> Orphaned output SSTs detected                       │
│        └─> Scheduled for GC                                    │
│                                                                 │
│ 3. Replay WAL files (reconstruct memtables)                   │
│    └─> [Covered in Part 1]                                    │
│                                                                 │
│ 4. Normal operation resumes                                    │
│                                                                 │
└─────────────────────────────────────────────────────────────────┘
```

---

## Part 4: Identified Gaps — WAL Operations Lacking Failpoints

### 4.1 GAP: WAL Segment Rotation (`midge::wal::rotate_segment`)

**Issue**: No failpoint during WAL segment rotation

**Location**: [src/runtime/actors/wal.rs](src/runtime/actors/wal.rs) — rotate not explicitly visible in snippet, but inferred from CloudFirst WAL handling

**Risk Scenario**:
1. WAL segment `{segment_id}.wal` full
2. Rotate: Rename `wal.log` → `{segment_id}.wal`
3. **CRASH POINT**: After rename but before next write begins in new segment
4. Recovery: Which segment is active? Where to resume appending?

**Missing Failpoint Needed**:
```rust
// After segment rotation complete
wal_fs.rename(&wal_path, &segment_path)?;
fail::fail_point!("midge::wal::after_segment_rotate_before_new_segment");
// Create new wal.log and transition current_segment_id
```

**Recovery Concern**: If crash occurs mid-rotation, is the next segment ID correct? Is state consistent?

---

### 4.2 GAP: Cloud WAL Flush in CloudFirst Mode

**Issue**: No failpoint between CloudActor completion and `cloud_durable_seq` update

**Location**: [src/runtime/event_loop/cloud_integration.rs](src/runtime/event_loop/cloud_integration.rs) — CloudAck handling

**Risk Scenario** (CloudFirst durability mode):
1. WAL segment uploaded to cloud
2. CloudActor receives cloud acknowledgment
3. **CRASH POINT**: Before `cloud_durable_seq` frontier advanced
4. Impact: In-flight transactions may be lost or visible to reads despite cloud loss

**Missing Failpoint Needed**:
```rust
// In handle_cloud_upload_complete (WAL actor)
// After cloud confirms data is durable
wal_actor.handle_cloud_upload_complete(segment_id, max_sequence)?;
fail::fail_point!("midge::wal::after_cloud_ack_before_frontier_update");
// Update cloud_durable_seq
```

**What This Would Test**:
- Idempotency of cloud writes (retries after crash)
- In-flight transaction cleanup (`pending_cloud_writes`)
- Durability frontier semantics in CloudFirst mode

---

### 4.3 GAP: Manifest Journal Append (Before Snapshot)

**Issue**: No failpoint between manifest edit appended to journal and in-memory manifest applied

**Location**: [src/runtime/actors/manifest.rs](src/runtime/actors/manifest.rs) — `compaction_complete()` method

**Risk Scenario**:
1. Append compaction edits to manifest journal (`manifest.log` or `.journal` file)
2. **CRASH POINT**: After journal write, before in-memory manifest updated
3. Impact: On recovery, edit is in journal but not in-memory (replayed on load)

**Code Path**:
```rust
// In manifest_actor.compaction_complete()
if let Err(e) = crate::metadata::append_edit_batch(&state.db_path, &edits) {
    tracing::warn!(...);
    // CURRENT: Error is logged but edits may be partially applied
}
// Missing failpoint here:
// fail::fail_point!("midge::manifest::after_journal_append_before_apply");

// Now apply to in-memory manifest
state.manifest.files.retain(...);
state.manifest.files.push(...);
```

**Missing Failpoint**:
```rust
crate::metadata::append_edit_batch(&state.db_path, &edits)?;
fail::fail_point!("midge::manifest::after_journal_append_before_app");
// Apply in-memory mutations
```

**Why It Matters**:
- Tests idempotent replay of partially-written journal entries
- Validates crash recovery correctly restores manifest from journal

---

### 4.4 GAP: Intent Log Append (Various Points)

**Issue**: No failpoint after intent log write but before state change is visible

**Locations**:
- After appending `CompactionApplied` intent
- After appending `SstAdded` intent  
- After appending `FlushPlanned` intent

**Risk Scenario**:
1. Append intent: `CompactionApplied {removed, added}`
2. **CRASH POINT**: After intent persisted, before manifest updated
3. Recovery: Intent log shows compaction happened; manifest should reflect it

**Missing Failpoint**:
```rust
// In manifest_actor.compaction_complete()
let intent = IntentLogEntry::CompactionApplied { ... };
state.append_intent(intent)?;
fail::fail_point!("midge::manifest::after_intent_append_before_manifest_update");
// Update manifest
```

**Why It Matters**:
- Tests that intent log persistence is **independent** from manifest persistence
- Validates recovery correctly interprets intent log without manifest
- Allows recreating manifest state from intent log

---

### 4.5 GAP: GC Before Snapshot Pinning Check

**Issue**: No failpoint to test race between GC deletion and snapshot acquisition

**Location**: [src/runtime/actors/gc.rs](src/runtime/actors/gc.rs) — `delete_ssts()` method

**Current Check**:
```rust
let pinned_ssts = state.get_pinned_sst_names();
if pinned_ssts.contains(sst_name) {
    // Skip deletion
}
// Then delete
```

**Risk Scenario**:
1. Snapshot acquired after GC check but before deletion
2. Snapshot reads from file being deleted
3. **Gap**: No failpoint to test this exact race condition

**Missing Failpoint**:
```rust
// In GC delete_ssts, after pinning check but before unlink
fail::fail_point!("midge::gc::before_file_delete");
match std::fs::remove_file(&sst_path) { ... }
```

**Why It Matters**:
- Tests that snapshot pinning is **race-free** against GC
- Validates `get_pinned_sst_names()` is correctly implemented

---

## Part 5: Crash Scenarios & Durability Boundaries

### Scenario A: Crash During WAL Write (Before Fsync)

**Crash Point**: Between `append_batch()` and `fsync()`

**System State**:
- **On-disk WAL**: May or may not contain the records (unflushed)
- **In-memory memtable**: Records NOT applied (happens only after durability confirmed)
- **Client response**: Not yet sent (write still in progress)

**On Recovery**:
1. Replay WAL up to last complete fsync
2. Fsync boundary is the replay boundary
3. Partial writes in OS buffer are lost (OK — client never saw response)

**Durability Guarantee**: **Intact** — client sees either full success or full failure

---

### Scenario B: Crash Between Manifest Update & Persist

**Crash Point**: Between `manifest_actor.compaction_complete()` and `manifest_actor.persist()`

**System State**:
- **In-memory manifest**: Updated (input removed, output added)
- **On-disk manifest**: Old state (pre-compaction)
- **Input SSTs on disk**: Still exist
- **Output SSTs on disk**: Exist but not referenced

**On Recovery**:
1. Load `manifest.yaml` (old state) ← **Source of truth**
2. Load intent log: see `CompactionApplied`
3. GC scan: finds orphaned output SSTs (not in manifest)
4. Schedule orphaned files for deletion
5. Compact the compaction: no permanent data loss

**Durability Guarantee**: **Intact** — manifest on disk is authoritative; orphaned files detected

**This is why `slice6::after_compaction_update_before_manifest_persist` is critical**: It tests exactly this recovery path.

---

### Scenario C: Crash After Manifest Persist, Before GC

**Crash Point**: Between `manifest_actor.persist()` and `gc_actor.delete_ssts()`

**System State**:
- **On-disk manifest**: Updated (compaction reflected)
- **Input SSTs on disk**: Still exist (not yet deleted)
- **Output SSTs on disk**: Referenced by manifest
- **Response to client**: Already sent

**On Recovery**:
1. Load `manifest.yaml` (new state) ← **Already correct**
2. Load intent log: see `CompactionApplied`
3. GC scan: finds orphaned input SSTs
4. Actually delete them (GC resumes)
5. Space reclaimed on next GC cycle

**Durability Guarantee**: **Intact** — Manifest correct, GC deferred but safe

**This is why `slice6::after_manifest_persist_before_sst_gc` is important**: It tests GC as asynchronous cleanup, not critical path.

---

### Scenario D: Crash During Cloud WAL Upload (CloudFirst)

**Crash Point**: Between CloudActor request and cloud confirmation

**System State**:
- **Local WAL**: Segment fsynced locally
- **Cloud WAL**: May or may not be present (upload in-flight)
- **Memtable**: Updates made visible (durability policy: CloudFirst)
- **Pending cloud writes**: Still in `pending_cloud_writes` queue

**On Recovery**:
1. Reload from local WAL (not cloud):
   - Segment already fsynced locally
2. Memtable reconstructed from WAL (all previously-visible writes safe)
3. Either:
   - Cloud has segment → skip re-upload (idempotent)
   - Cloud lacks segment → re-upload (retry semantics)
4. No data loss, but possible redundant uploads

**Durability Guarantee**: **Depends on policy**:
- **CloudFirst**: Strong (cloud is source of truth when available)
- **Batched**: Strong (local WAL fsync is source of truth)

**Hidden Failpoint Need**: Test cloud upload retry/idempotency

---

### Scenario E: Crash During Manifest Journal Append

**Crash Point**: Partial write to `manifest.log`

**System State**:
- **Journal on disk**: Partial entry (truncated)
- **In-memory manifest**: Not updated yet
- **Output SSTs on disk**: On disk

**On Recovery**:
1. Load `manifest.yaml` snapshot (if exists)
   - If snapshot up-to-date: use it (recovery done)
   - If snapshot old: replay journal edits
2. Journal replay:
   - Truncated entry detected (read fails partway)
   - Recovery stops at last complete edit
   - Can be retried/resumed

**Durability Guarantee**: **Partially at-risk** — Journal corruption can delay recovery, but full snapshot backup provides fallback

---

## Part 6: Summary Table: Failpoint Coverage

| **Operation** | **Failpoint Exists** | **Tests** | **Gap** |
|---|---|---|---|
| WAL append_batch | ✅ `after_append_batch_before_sync` | Buffer crash | — |
| WAL fsync | ✅ `after_fsync_before_durable_frontier` | Frontier desync | — |
| WAL segment rotate | ❌ None | — | Segment ID consistency |
| Manifest temp sync | ✅ `after_temp_sync_before_rename` | Temp file stale | — |
| Manifest atomic rename | ❌ None | — | Rename atomicity |
| Manifest journal append | ❌ None | — | Journal corruption recovery |
| Intent log append | ❌ None | — | Intent recovery |
| Compaction apply | ✅ `after_compaction_update_before_persist` | In-mem ↔ disk sync | — |
| Manifest persist | ✅ `after_manifest_persist_before_sst_gc` | GC ordering | — |
| SST write | ✅ `after_sst_write_before_publish` | Orphaned SST | — |
| Cloud WAL upload | ❌ None | — | Upload retry semantics |
| GC deletion | ❌ None | — | Snapshot race condition |

---

## Part 7: Recommendations

### High Priority (Affects Durability)

1. **Add failpoint: `midge::wal::after_segment_rotate_before_new_segment`**
   - Tests WAL segment rotation consistency
   - Required for full durability testing in grow-only WAL patterns

2. **Add failpoint: `midge::manifest::after_journal_append_before_app`**
   - Tests manifest journal replay idempotency
   - Critical for large deployments with many compactions

3. **Add failpoint: `midge::gc::before_file_delete`**
   - Tests snapshot pinning race condition
   - Validates concurrent safety of snapshot + GC

### Medium Priority (Improves Test Coverage)

4. **Add failpoint: `midge::intent::after_append_before_apply`**
   - Tests intent log recovery paths
   - Validates crash between intent and state change

5. **Add failpoint: `midge::wal::after_cloud_ack_before_frontier_update` (CloudFirst)**
   - Tests cloud durability frontier skew
   - Critical for CloudFirst mode verification

### Low Priority (Operational)

6. **Add cleanup for manifest temp files at startup**
   - Detect `manifest.yaml.tmp` and remove on load
   - Prevents indefinite accumulation

7. **Add GC recovery test for orphaned output SSTs**
   - Verify GC correctly identifies orphaned files from intent log
   - Ensures recovery can complete without manual cleanup

---

## Part 8: Key Architectural Insights

### 1. **Manifest Persistence is the Compaction Durability Boundary**

The two Slice 6 failpoints prove that:
- In-memory manifest update ≠ compaction durable
- Manifest file on disk (`manifest.yaml`) is the **source of truth**
- GC is asynchronous; must never race ahead of manifest

### 2. **Intent Log is Recovery Metadata, Not Durability Critical**

The intent log serves recovery, not forward durability:
- Records what **should** happen (CompactionApplied)
- Compared against actual resources (manifest, files on disk)
- Allows detecting incomplete operations on restart

### 3. **Two Durability Frontiers in CloudFirst Mode**

- **`local_durable_seq`**: Last sequence durable to local disk
- **`cloud_durable_seq`**: Last sequence durable to cloud
- Clients in CloudFirst mode wait for cloud; gap between them is in-flight risk window

### 4. **Snapshot Pinning Prevents GC Races**

The GC actor checks `state.get_pinned_sst_names()` before deletion:
- Snapshots acquire pin (reference count)
- GC skips pinned files
- No failpoint yet to test this race exhaustively

---

## Testing Implications

**Chaos Testing Strategy**:
1. Run compaction races with manifests persistence failpoints active
2. Crash recovery: Verify manifest reload, intent log playback
3. Cloud mode: Test drift between `local_durable_seq` and `cloud_durable_seq`
4. Snapshot + compaction: Run snapshot reads concurrently with GC
5. Journal corruption: Corrupt `manifest.log`, verify recovery from snapshot

**Minimum Viable Durability Tests**:
- ✅ `after_append_batch_before_sync` (WAL sync boundary)
- ✅ `after_fsync_before_durable_frontier` (frontier update)
- ✅ `after_compaction_update_before_persist` (manifest durability)
- ✅ `after_manifest_persist_before_sst_gc` (GC ordering)
- ❌ Missing: WAL rotation, manifest journal, cloudack frontier

---

## Conclusion

Midge's Slice 6 compaction model is well-designed around the principle that **manifest persistence is the durability boundary**. The failpoints strategically test the critical path from in-memory update through disk persistence to asynchronous cleanup.

**Key Gap**: WAL operations (rotation, cloud flush) and manifest journal operations lack failpoints. These would strengthen durability verification for large-scale deployments and cloud-backed workloads.

The two-layer approach (intent log + manifest) provides good crash recovery semantics, but tests should explicitly verify that intent log entries correctly drive recovery actions without manifest assistance.
