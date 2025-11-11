# Acceptable Global State

This document explains the **intentional** use of global state in Midge and why it doesn't violate our [Architecture Principles](./architecture_principles.md).

---

## Principle: Composition Over Globals

Our architecture favors **explicit composition** — components should be wired together through traits, not coupled via hidden globals.

However, certain **cross-cutting concerns** benefit from global accessors when they meet these criteria:

1. **Observability, not correctness** — doesn't affect state transitions
2. **Boundary components** — metrics, rate limiting, resource management
3. **Default to safe** — unlimited/no-op if not configured
4. **Set once at startup** — not mutated during operation

---

## Acceptable Globals in Midge

### 1. Performance Metrics (`GLOBAL_PERF`)

**Location:** `src/core/metrics/mod.rs`

```rust
static GLOBAL_PERF: OnceCell<PerformanceMetrics> = OnceCell::new();

pub fn global_performance_metrics() -> &'static PerformanceMetrics {
    GLOBAL_PERF.get_or_init(PerformanceMetrics::new)
}
```

**Why it's acceptable:**
- Pure **observability** — metrics don't affect correctness
- Avoids threading `Arc<PerformanceMetrics>` through every hot path
- Tests can construct their own `PerformanceMetrics` instances if needed
- Zero cost if never accessed (lazy initialization)

**Used in:**
- WAL writer (write/fsync latency)
- Group commit (batch sizes)
- SST operations (read/write throughput)

---

### 2. Rate Limiter (`GLOBAL_RATE_LIMITER`)

**Location:** `src/common/rate_limiter.rs`

```rust
static GLOBAL_RATE_LIMITER: OnceLock<Arc<RateLimiter>> = OnceLock::new();

pub fn set_global_rate_limiter(limiter: Arc<RateLimiter>) {
    let _ = GLOBAL_RATE_LIMITER.set(limiter);
}

pub fn global_rate_limiter() -> Arc<RateLimiter> {
    GLOBAL_RATE_LIMITER
        .get()
        .cloned()
        .unwrap_or_else(|| Arc::new(RateLimiter::unlimited()))
}
```

**Why it's acceptable:**
- **Defaults to unlimited** — no hidden throttling unless explicitly configured
- **Set once at engine initialization** — `set_global_rate_limiter()` called during startup
- **Resource management boundary** — prevents cloud upload saturation
- **Doesn't affect correctness** — only delays uploads, doesn't change behavior
- **Alternative is trait pollution** — threading `RateLimiter` through `CloudBackend` trait would complicate interface

**Used in:**
- `wal/cloud/shared.rs` — WAL segment uploads
- `sst/cloud/writer.rs` — SST block uploads
- `sst/cloud/lifecycle.rs` — SST finalization uploads

**Configuration:**
```rust
let engine = Engine::open(EngineOptions {
    cloud_upload_rate_limit_bytes_per_sec: 100_000_000, // 100 MB/s
    cloud_upload_rate_limit_burst_bytes: 50_000_000,    // 50 MB burst
    // ...
})?;
```

---

### 3. WAL Encode Thread Pool (`GLOBAL_WAL_POOL`)

**Location:** `src/wal/encode_pipeline.rs`

```rust
static GLOBAL_WAL_POOL: OnceCell<ThreadPool> = OnceCell::new();

fn get_global_pool(num_threads: usize) -> &'static ThreadPool {
    GLOBAL_WAL_POOL.get_or_init(|| {
        ThreadPoolBuilder::new()
            .thread_name(|i| format!("wal-enc-{}", i))
            .num_threads(num_threads)
            .build()
            .expect("failed to build global wal encode thread pool")
    })
}
```

**Why it's acceptable:**
- **Performance optimization** — avoids repeated thread creation in benchmarks
- **Lazy initialization** — only created if parallel encoding is enabled
- **Size is configurable** — respects `EncodePipelineConfig::parallelism`
- **Pure CPU work** — encoding is deterministic, parallelism is just optimization

**Trade-off:**
- ✅ Faster benchmark setup (no thread churn)
- ⚠️ Shared across multiple `Engine` instances in same process
- Alternative: pass `ThreadPool` via config (could be added if needed)

---

## What's NOT Acceptable

These patterns would violate our principles:

### ❌ Hidden Background Loops

```rust
// WRONG - hidden timer affecting correctness
static AUTO_COMPACT: OnceCell<()> = OnceCell::new();

fn ensure_background_compaction() {
    AUTO_COMPACT.get_or_init(|| {
        std::thread::spawn(|| loop {
            std::thread::sleep(Duration::from_secs(60));
            trigger_compaction(); // ❌ Hidden side effect
        });
    });
}
```

**Why it's wrong:**
- Affects correctness (compaction timing matters)
- Not deterministic (depends on wall-clock time)
- Can't be controlled or tested

### ❌ Mutable Global State

```rust
// WRONG - shared mutable state
static mut GLOBAL_CACHE: Option<LruCache> = None;

fn get_cache() -> &'static mut LruCache {
    unsafe { GLOBAL_CACHE.as_mut().unwrap() }
}
```

**Why it's wrong:**
- Race conditions (requires unsafe)
- Implicit coordination between components
- Breaks composition (can't have multiple engines)

### ❌ Globals for Core Logic

```rust
// WRONG - global affects state transitions
static GLOBAL_SEQUENCE: AtomicU64 = AtomicU64::new(0);

fn next_sequence() -> u64 {
    GLOBAL_SEQUENCE.fetch_add(1, Ordering::SeqCst)
}
```

**Why it's wrong:**
- Correctness depends on global (breaks composability)
- Can't have isolated engines in tests
- Implicit coupling across components

---

## Guidelines for New Globals

Before adding global state, ask:

1. **Is it observability/resource management?** (metrics, rate limiting)
   - ✅ Acceptable with default safe behavior

2. **Does it affect correctness?** (sequence numbers, locks, caches)
   - ❌ Must be explicit component, not global

3. **Can it be injected via config?**
   - ✅ Prefer injection, but global is acceptable if injection adds complexity

4. **Does it have hidden side effects?** (background threads, timers)
   - ❌ All actions must be explicit

5. **Is it set once at startup?**
   - ✅ Acceptable if immutable after init
   - ❌ Not acceptable if mutated during operation

---

## Conclusion

Midge uses **minimal, justified global state** for:
- Metrics collection (observability)
- Rate limiting (resource management)
- Thread pool (performance optimization)

All globals:
- Default to safe/no-op behavior
- Are set once at initialization
- Don't affect correctness
- Are documented and justified

This balances **pragmatism** (avoiding trait pollution) with **principles** (explicit composition for core logic).
