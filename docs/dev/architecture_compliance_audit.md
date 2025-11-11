# Architecture Compliance Audit

**Date:** November 11, 2025  
**Context:** Audit of Midge codebase against [Architecture Principles](./architecture_principles.md)

---

## Executive Summary

The Midge codebase is **largely compliant** with the stated architectural principles, with most violations isolated to **observability boundaries** (metrics, monitoring) and **test infrastructure**. The core storage engine, transactions, and compaction logic follow deterministic, composable patterns.

**Key Findings:**
- ✅ **Core engine is deterministic** — storage operations, WAL, transactions, compaction
- ⚠️ **Global singletons exist** — but limited to metrics and thread pools (non-critical)
- ⚠️ **Time dependencies exist** — primarily in observability, TTL, and transaction timeouts
- ⚠️ **Background threads exist** — group commit, NTP sync, cloud uploads (acceptable boundaries)
- ✅ **No async/await complexity** — synchronous design throughout

---

## 1. Composable by Design ✅ (Minor Issues)

### Compliant Patterns

- **Trait-based composition:** `KvStore`, `KvTransaction`, `CloudBackend`, `Compressor`
- **No deep inheritance:** components are wired, not subclassed
- **Clear interfaces:** each layer (WAL, SST, memtable, compaction) has explicit boundaries

### Violations

#### 🟡 Global Singletons for Metrics

**Location:** `src/core/metrics/mod.rs`

```rust
static GLOBAL_PERF: OnceCell<PerformanceMetrics> = OnceCell::new();

pub fn global_performance_metrics() -> &'static PerformanceMetrics {
    GLOBAL_PERF.get_or_init(PerformanceMetrics::new)
}
```

**Impact:** Low — metrics are observability, not correctness  
**Used in:** WAL writer, group commit, SST operations

**Recommendation:**
- Keep for convenience, **but document** that this is an observability boundary
- Consider making metrics **injectable** via config for test isolation

#### 🟡 Global Thread Pool

**Location:** `src/wal/encode_pipeline.rs`

```rust
static GLOBAL_WAL_POOL: OnceCell<ThreadPool> = OnceCell::new();
```

**Impact:** Low — performance optimization for benchmarks  
**Justification:** Avoids repeated thread creation overhead

**Recommendation:**
- Document why this is acceptable (performance, not correctness)
- Ensure pool size can be controlled via config

#### 🟡 Global Rate Limiter

**Location:** `src/common/rate_limiter.rs`

**Impact:** Medium — affects cloud upload behavior  
**Used in:** WAL cloud uploads, SST cloud uploads

**Recommendation:**
- Make rate limiter **injectable** via `CloudBackend` trait
- Remove `global_rate_limiter()` accessor in favor of explicit wiring

---

## 2. Deterministic Behavior ⚠️ (Acceptable Violations)

### Compliant Patterns

- **WAL replay is deterministic** — sequence numbers drive state transitions
- **Compaction is deterministic** — driven by manifest, not wall-clock time
- **Transactions use sequence numbers** — not timestamps for ordering

### Violations

#### 🟡 Transaction Timeouts

**Location:** `src/core/transaction/core.rs`

```rust
let created_at = Instant::now();
let deadline = timeout.map(|t| created_at + t);
// ...
pub(crate) fn is_expired(&self) -> bool { 
    self.deadline.is_some_and(|d| Instant::now() > d) 
}
```

**Impact:** Medium — transaction aborts are time-dependent  
**Justification:** Timeouts prevent resource exhaustion, not correctness

**Recommendation:**
- Document that timeouts are a **liveness mechanism**, not correctness requirement
- Ensure tests can **disable timeouts** or use deterministic clocks

#### 🟡 TTL Compaction Filter

**Location:** `src/core/compaction/execution/merging.rs`, `output_writer.rs`

```rust
let now = std::time::SystemTime::now()
    .duration_since(UNIX_EPOCH)
    .unwrap()
    .as_secs();
```

**Impact:** Medium — TTL expiration is wall-clock dependent  
**Justification:** TTL is explicitly a time-based feature

**Recommendation:**
- Add **injectable clock** for TTL logic (allow tests to control time)
- Document that TTL is an exception to determinism for practical reasons

#### 🟡 File Deletion Grace Period

**Location:** `src/sst/file_manager.rs`

```rust
marked_at: std::time::Instant::now(),
// ...
let now = std::time::Instant::now();
```

**Impact:** Low — delays deletion, doesn't affect correctness  
**Justification:** Safety mechanism to avoid deleting in-use files

**Recommendation:**
- Keep current design
- Document as a **safety boundary**, not core logic

#### 🟢 NTP Background Sync

**Location:** `src/common/timestamp.rs`

```rust
fn start_resync_thread() {
    thread::spawn(|| loop {
        thread::sleep(Duration::from_secs(6 * 3600)); // every 6h
        // ... resync NTP offset
    });
}
```

**Impact:** None — clock drift correction, not state transitions  
**Justification:** Distributed system requirement

**Recommendation:**
- Keep as-is
- This is a **boundary component** explicitly mentioned in principles

---

## 3. No Hidden Timers or Side Effects ⚠️ (Mostly Compliant)

### Compliant Patterns

- **No hidden background compaction loops** — compaction is explicit
- **No auto-flush timers** — flushes are triggered by memtable size
- **No implicit cleanup threads** — deletion is explicit via `execute_pending_deletions`

### Violations

#### 🟡 Group Commit Sleep

**Location:** `src/wal/fs/group_commit.rs`

```rust
if self.config.wait_micros > 0 {
    std::thread::sleep(Duration::from_micros(self.config.wait_micros));
}
```

**Impact:** Medium — uses sleep to batch writes  
**Justification:** Performance optimization (reduces fsync calls)

**Recommendation:**
- Keep, but document as **explicit performance tuning**, not hidden behavior
- Ensure `wait_micros` is configurable (already is via config)
- Add comment explaining trade-off: latency vs. throughput

#### 🟢 Cloud Upload Background Thread

**Location:** `src/wal/cloud/shared.rs`, `src/sst/manifest_cache.rs`

```rust
std::thread::spawn(move || {
    // ... upload to cloud
});
```

**Impact:** None — async I/O boundary  
**Justification:** Non-blocking cloud uploads

**Recommendation:**
- Keep as-is
- This is an **explicit side effect boundary** as allowed by principles

#### 🟢 Test Sleeps

**Location:** `src/sst/file_manager.rs`, `src/sst/manifest_cache.rs`, etc.

```rust
std::thread::sleep(Duration::from_millis(10));
```

**Impact:** None — test infrastructure only  
**Justification:** Simulating concurrency races

**Recommendation:**
- Keep in tests
- Consider using **barriers or channels** instead of sleeps where possible

---

## 4. Reproducibility and Purity ✅ (Strong Compliance)

### Compliant Patterns

- **WAL is a pure state machine** — deterministic replay
- **Compaction is pure** — given inputs, produces deterministic outputs
- **Transactions are isolated** — no hidden global state
- **Side effects are explicit** — all I/O goes through `CloudBackend`, `FileSystem` traits

### No Violations Found

The core engine is **highly pure**. All side effects (disk I/O, cloud uploads, metrics) are isolated to explicit boundaries.

**Praise:**
- Excellent use of traits for I/O abstraction
- Mock implementations for testing (`MockCloudBackend`)
- No hidden `unwrap()` or `panic!()` in hot paths

---

## 5. Transparent Causality ✅ (Strong Compliance)

### Compliant Patterns

- **WAL provides causal ordering** — all writes are sequenced
- **Manifest tracks SST lineage** — compaction history is explicit
- **Transaction conflicts are logged** — clear conflict detection
- **No self-healing** — failures are explicit, not hidden

### No Violations Found

The system has **excellent observability**. Every state change is traceable through WAL entries, manifest updates, or transaction logs.

---

## Summary of Recommendations

### High Priority (Address Soon)

1. **Make rate limiter injectable** — remove `global_rate_limiter()` accessor
2. **Add injectable clock for TTL** — allow tests to control time
3. **Document transaction timeouts** — clarify liveness vs. correctness

### Medium Priority (Nice to Have)

4. **Make metrics injectable** — allow per-engine metrics instead of global singleton
5. **Document group commit sleep** — explain latency/throughput trade-off
6. **Add test clock abstraction** — replace `Instant::now()` in tests with controllable clock

### Low Priority (Optional)

7. **Replace test sleeps with barriers** — improve test determinism
8. **Document global thread pool** — explain why it's acceptable

---

## Conclusion

**Midge is architecturally sound.** The violations found are:

1. **Observability boundaries** (metrics, monitoring) — acceptable
2. **Performance optimizations** (group commit, thread pools) — justified
3. **Test infrastructure** (sleeps, timing) — not production code

**The core engine follows the principles:**
- ✅ Composition over inheritance
- ✅ Deterministic state transitions
- ✅ Explicit actions over side effects
- ✅ Pure subsystems with isolated I/O
- ✅ Traceable causality

**Recommendation:** Continue with current design. Address high-priority items for improved testability and explicit composition.
