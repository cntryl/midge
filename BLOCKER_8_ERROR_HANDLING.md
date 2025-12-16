# BLOCKER #8: Error Handling + Design Debt Refactor

**Status:** Implementation Plan (2-3 weeks, lower priority)  
**Impact:** Medium (maintainability + production debugging)  
**Risk if Skipped:** Hard to debug production failures; refactoring becomes expensive later

---

## Problem Statement

Midge's error handling loses context through two mechanisms:

1. **Error Context Loss** — Errors mapped to generic `MidgeError::Internal` without preserving root cause
   ```rust
   // Current: loses stack context
   Err(some_io_error).map_err(|_| MidgeError::Internal("disk write failed"))
   
   // Should be: preserve chain
   Err(some_io_error).with_context("disk_write_during_compaction", file!(), line!())
   ```

2. **Non-Typesafe Actor Routing** — `RuntimeMsg` giant enum (50+ variants), message dispatch fragile
   ```rust
   // Current: all variants in one enum, pattern matching fragile
   pub enum RuntimeMsg {
       Read { ... },
       Write { ... },
       Compact { ... },
       // 47 more variants...
   }
   
   // Current: dispatcher must handle all cases
   pub fn dispatch(msg: RuntimeMsg) {
       match msg {
           RuntimeMsg::Read => ...,  // Easy to forget
           RuntimeMsg::Write => ...,
           ...
       }
   }
   ```

3. **No Recovery Telemetry** — Can't easily observe WAL replay, intent log recovery, or error frequency
   ```rust
   // Current: no metrics for production incident debugging
   pub fn replay_wal(...) -> MidgeResult<RecoveryStats> {
       // No insight into: corruption frequency, replay duration, error patterns
   }
   ```

---

## Failure Modes This Addresses

### Scenario 1: Production Corruption Debug Nightmare
```
1. Customer reports "corruption error" in production
2. Error logged: "MidgeError::Internal("SST corruption")"
3. What happened? Where did it come from? Need to check:
   - WAL recovery? (file:line?) 
   - Compaction? (which merge operator?)
   - Cloud write retry? (network vs. local?)
4. Current: Can't tell from logs
5. After fix: Error context chain shows exact call path
```

### Scenario 2: Silent Message Drop
```
1. New actor added (e.g., StatsCollector)
2. Developer forgets to add pattern match in dispatcher
3. Messages silently disappear, stats don't update
4. Users report metrics "stuck", investigate takes hours
5. After fix: Compile-time error if message not routed
```

### Scenario 3: Production Incident Diagnosis Impossible
```
1. Engine takes 5 minutes to recover
2. Need to know: How many WAL files? How many corrupted entries?
3. Current: No metrics, only timestamps in logs
4. After fix: gauge midge_wal_recovery_duration_ms, counter midge_wal_corruption_errors
```

---

## Implementation Plan (2-3 weeks)

### STEP 8A: Error Context Chain (1 week)

**Goal:** Preserve error context without breaking API

**Implementation:**

1. **Add context methods to `MidgeError`:**
   ```rust
   #[derive(Debug)]
   pub enum MidgeError {
       Io(std::io::Error),
       NotFound,
       Corruption(String),
       // ... rest unchanged
   }
   
   impl MidgeError {
       /// Attach context to error for debugging
       /// Usage: err.context("operation_name", file!(), line!())
       pub fn context(self, operation: &str, file: &'static str, line: u32) -> Self {
           // Wrap self in ErrorWithContext newtype that carries stack
           ErrorWithContext {
               inner: self,
               breadcrumbs: vec![(operation, file, line)],
           }
       }
   }
   ```

2. **Update critical paths to attach context:**
   ```rust
   // File: src/wal/recovery.rs
   pub fn replay_wal(...) -> MidgeResult<RecoveryStats> {
       ...
       match result {
           Err(e) => Err(e.context("wal_replay_failed", file!(), line!())),
           Ok(...) => ...
       }
   }
   
   // File: src/compaction/executor.rs
   pub fn execute_compaction(...) -> MidgeResult<...> {
       ...
       apply_merge_operator()
           .map_err(|e| e.context("merge_operator_apply", file!(), line!()))?
   }
   ```

3. **Update Display impl to show breadcrumbs:**
   ```
   Corruption: SST footer invalid
     context chain:
       - sst_read_footer (src/sst/fs/reader_io.rs:189)
       - compaction_merge (src/compaction/executor.rs:156)
       - execute_compaction (src/compaction/executor.rs:42)
   ```

**Timeline:** 1 week (3-4 files affected: recovery, compaction, validation)

**Testing:**
- Add test `should_preserve_error_context_through_stack`
- Verify Display output includes all breadcrumbs
- Benchmark: no perf impact (Context stored only in error, not hot path)

---

### STEP 8B: Typed Actor Routing (1.5 weeks)

**Goal:** Make message dispatch compile-time safe

**Current:**
```rust
// All messages in one giant enum - hard to extend safely
pub enum RuntimeMsg {
    Read { cf_id: u32, key: Vec<u8>, request_id: u64, ... },
    Write { ... },
    RangeStart { ... },
    Compact { ... },
    // 46 more...
}

// Pattern matching required in all 3 places: dispatch, event_loop, response routing
impl EventLoop {
    fn handle_msg(&mut self, msg: RuntimeMsg) {
        match msg {
            RuntimeMsg::Read(...) => self.handle_read(...),
            RuntimeMsg::Write(...) => self.handle_write(...),
            // ... must match all 50 variants
            _ => panic!("unhandled message"),
        }
    }
}
```

**Proposed:**
```rust
// Define per-actor message types
pub trait RuntimeActor: Send {
    type Msg;
    fn handle(&mut self, msg: Self::Msg) -> MidgeResult<()>;
}

pub struct WalActor { ... }
impl RuntimeActor for WalActor {
    type Msg = WalMessage; // Only WalActor variants
    fn handle(&mut self, msg: WalMessage) -> MidgeResult<()> { ... }
}

pub struct CompactionActor { ... }
impl RuntimeActor for CompactionActor {
    type Msg = CompactionMessage; // Only Compaction variants
    fn handle(&mut self, msg: CompactionMessage) -> MidgeResult<()> { ... }
}

// Router becomes: HashMap<ActorId, Box<dyn Any>>
pub fn route_message(msg: Box<dyn Any>, actors: &HashMap<ActorId, Box<dyn Any>>) {
    // Downcast and dispatch - any missing route is compile error (via type system)
}
```

**Benefits:**
- New actor: just `impl RuntimeActor` + register in router map
- Compiler catches missing routes (downcast panics caught at startup)
- Each actor owns its message type definition
- No giant enum to maintain

**Timeline:** 1.5 weeks
- Week 1: Define per-actor message types, implement 2-3 actors as proof
- Days 3-4: Convert remaining actors
- Days 5-7: Test, benchmark, profiling

**Backward Compatibility:** Internal only (no API change)

---

### STEP 8C: Recovery Telemetry (1 week)

**Goal:** Observable recovery and error handling

**Metrics to Add:**

```rust
// File: src/telemetry/recovery.rs (new)

lazy_static! {
    static ref RECOVERY_METRICS = RecoveryMetrics {
        /// Duration of WAL replay in milliseconds
        replay_duration_ms: Histogram::new(...),
        
        /// Number of WAL records successfully replayed
        records_replayed: Counter::new(...),
        
        /// Number of corruption errors encountered during recovery
        corruption_errors: Counter::new(...),
        
        /// Number of entries filtered by TTL during compaction
        ttl_entries_filtered: Counter::new(...),
        
        /// Number of intent log entries replayed on recovery
        intent_log_entries_replayed: Counter::new(...),
        
        /// Time to recover snapshot registry after unclean shutdown
        snapshot_registry_rebuild_ms: Gauge::new(...),
    };
}
```

**Integration Points:**

```rust
// File: src/wal/recovery.rs
pub fn replay_wal(...) -> MidgeResult<RecoveryStats> {
    let start = Instant::now();
    // ... replay logic ...
    let elapsed = start.elapsed();
    
    RECOVERY_METRICS.replay_duration_ms.record(elapsed.as_millis() as u64);
    RECOVERY_METRICS.records_replayed.add(stats.record_count);
    if stats.had_corruption {
        RECOVERY_METRICS.corruption_errors.increment();
    }
    
    Ok(stats)
}

// File: src/manifest.rs
pub fn replay_intent_log(...) -> MidgeResult<u64> {
    let mut count = 0;
    while let Some(intent) = next_intent()? {
        apply_intent(&intent)?;
        count += 1;
    }
    
    RECOVERY_METRICS.intent_log_entries_replayed.add(count);
    Ok(count)
}
```

**Dashboard Queries:**

```promql
# Recovery duration SLA monitoring
histogram_quantile(0.99, midge_wal_replay_duration_ms) < 5000  # p99 < 5s

# Corruption frequency (early warning)
rate(midge_wal_corruption_errors[5m]) > 0  # Any corruption = investigate

# Intent log replay (compaction atomicity verification)
increase(midge_intent_log_entries_replayed[5m]) > 0  # Being exercised during recovery
```

**Timeline:** 1 week (localized changes: just add metrics calls)

---

## Work Breakdown

| Phase | Task | Est. Time | Files | Priority |
|-------|------|-----------|-------|----------|
| **8A** | Add error context methods | 2 days | error.rs, recovery.rs, compaction/* | HIGH |
| **8A** | Add context to critical paths | 3 days | 5-10 files in wal/, sst/, compaction/ | HIGH |
| **8B** | Define per-actor message types | 3 days | Define new WalMessage, CompactionMessage, etc. | MEDIUM |
| **8B** | Implement typesafe router | 4 days | New router/dispatch.rs | MEDIUM |
| **8B** | Migrate existing actors (2-3) | 3 days | Port actors incrementally | MEDIUM |
| **8B** | Full migration + tests | 3 days | Final actors + validation | MEDIUM |
| **8C** | Add recovery metrics | 3 days | telemetry/recovery.rs + integration points | MEDIUM |
| **8C** | Add observability tests | 2 days | Verify metrics are recorded | MEDIUM |
| | **TOTAL** | **~3 weeks** | ~20-30 files | |

---

## Recommendation

### For Production Deployment NOW (7 of 8 blockers done):

**Deploy with current error handling** — Risk level is LOW:
- Error logs are still readable (just less context on chain)
- No bugs introduced by STEP 8 skipping
- Actor routing works (just manual pattern matching)
- Recovery telemetry can be added post-launch

### For Post-Launch (Parallel Track):

Schedule STEP 8 as **Week 2-3 after launch**:
- Enables smoother debugging of edge cases users discover
- Typed actor routing prevents deployment mistakes
- Recovery telemetry needed only when scaling to 10k+ concurrent sessions

---

## Success Criteria

- [ ] Error Display output includes 3+ levels of context chain
- [ ] Zero new errors introduced by refactoring
- [ ] 11/11 smoke tests passing after migration
- [ ] Typed router compiles with all actors migrated
- [ ] Recovery metrics populated during normal startup
- [ ] Deployment script validates STEP 8C metrics are present before releasing

---

## Risk Mitigation

| Risk | Mitigation |
|------|-----------|
| Refactoring introduces regressions | - Each step tested independently with smoke tests<br>- Feature branch tested 48h before merge |
| Typed router harder to debug | - Add debug_assertions that log dispatch tree at startup |
| Telemetry overhead | - All metrics are lazy_static (zero alloc)<br>- Histograms use bounded buckets |
| Backward incompatibility | - Error enum stays public + unchanged<br>- New context methods are additive only |

---

## Post-Implementation Checklist

- [ ] All 8 blockers resolved
- [ ] Production deployment SLA confirmed
- [ ] Monitoring dashboard created (recovery metrics visible)
- [ ] Runbooks updated (new error context in logs)
- [ ] Team training on new error context format
- [ ] Benchmark run (should show no regression)
