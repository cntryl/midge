# Durability Modes Explained (Simple Version)

## The Three Modes in Plain English

### 🔒 Strict = "Wait for Cloud Confirmation"

**What happens on write:**
```
You: PUT key=value
├─ Write to local disk (fsync)
├─ Upload to cloud (S3/Azure/GCS)
├─ ⏳ WAIT for cloud to confirm
└─ Return: "OK, it's saved!"

Latency: 50-200ms (cloud round-trip)
Safety: ZERO data loss (even if node explodes)
```

**When to use:**
- Money transfers
- Audit logs
- Anything where losing data = disaster

---

### ⚖️ Steady = "Save Locally, Sync to Cloud in Background"

**What happens on write:**
```
You: PUT key=value
├─ Write to local disk (fsync every ~20ms)
└─ Return: "OK, it's saved locally!"

Meanwhile, in background:
└─ Background thread uploads to cloud every 20ms

Latency: <1ms (local disk only)
Safety: Lose up to 20ms of writes if node crashes
```

**When to use:**
- Most applications (this is the default!)
- High-throughput OLTP
- General purpose databases
- Anything needing good performance + reasonable safety

**Why it's called "Steady":**
- Steady stream of background uploads
- Steady balance between performance and safety
- Steady state operation (predictable behavior)

---

### ☁️ Cloud-Replicated = "Cloud is Truth, Local is Just Cache"

**What happens on write:**
```
You: PUT key=value
├─ Write to local disk (NO fsync!)
└─ Return: "OK!" (instant)

Meanwhile, in background:
└─ Background thread uploads to cloud every ~100ms

Latency: <0.5ms (no disk sync!)
Safety: Lose up to 100ms of writes if node crashes
Cache: Only 256MB (hot data only)
```

**When to use:**
- Docker/Kubernetes (ephemeral nodes)
- AWS Lambda / serverless
- Spot instances (can be killed anytime)
- Multi-region replication
- Anywhere local disk is temporary

---

## Visual Comparison

### Write Path Comparison

**Strict (Synchronous):**
```
Client → Local WAL (fsync) → Cloud Upload → Wait ⏳ → Return OK ✅
                              (50-200ms)
```

**Steady (Async with fsync):**
```
Client → Local WAL (fsync every 20ms) → Return OK ✅
                                         (<1ms)
         
         Background: → Cloud Upload (async)
                      (batched every 20ms)
```

**Cloud-Replicated (Async without fsync):**
```
Client → Local WAL (NO fsync) → Return OK ✅
                                 (<0.5ms)
         
         Background: → Cloud Upload (async)
                      (batched every 100ms)
```

---

## The Key Differences

| Question | Strict | Steady | Cloud-Replicated |
|----------|--------|--------|------------------|
| **Does it wait for cloud before returning?** | ✅ Yes (slow) | ❌ No (fast) | ❌ No (fastest) |
| **Does it fsync local disk?** | ✅ Yes (every write) | ✅ Yes (every 20ms) | ❌ No |
| **How much data can you lose?** | **0 bytes** | ~20ms of writes | ~100ms of writes |
| **Write latency** | 50-200ms | <1ms | <0.5ms |
| **Local cache size** | 1024 MB (all data) | 2048 MB (all data) | 256 MB (hot data only) |
| **Best for** | Mission-critical | General purpose | Ephemeral compute |

---

## Real-World Analogy

Think of it like saving a document:

**Strict** = "Save to Dropbox and wait for 'Synced ✅'"
- Slow, but you KNOW it's backed up
- Like waiting for the bank to confirm your deposit

**Steady** = "Save to local disk, Dropbox syncs in background"
- Fast local save, cloud catches up soon
- Like saving to your laptop with iCloud sync enabled

**Cloud-Replicated** = "Save to memory, Dropbox syncs periodically"
- Fastest, but might lose work if laptop dies
- Like Google Docs auto-save (saves to cloud, not local disk)

---

## How to Choose

```
START: What kind of application?

├─ Financial system / Audit logs / Legal docs
│  └─ Use: Strict
│     (Cannot afford ANY data loss)
│
├─ Normal database / Web app / API server
│  └─ Use: Steady (recommended default)
│     (Good balance of speed + safety)
│
├─ Container / Lambda / Spot instance / Temporary node
│  └─ Use: Cloud-Replicated
│     (Local disk is temporary anyway)
│
└─ Not sure?
   └─ Start with Steady
      (Works great for 90% of use cases)
```

---

## Code Examples

### Strict (Zero Data Loss)

```rust
let backend = Arc::new(AwsS3Backend::new("us-east-1", "bucket", None)?);

let storage = CloudConfigBuilder::strict_durability(backend, "./cache")
    .build();

// Every write waits for cloud confirmation (slow but safe)
engine.put(b"transaction_123", b"$1000 transfer")?; // ⏳ waits for S3
```

### Steady (Recommended Default)

```rust
let backend = Arc::new(AwsS3Backend::new("us-east-1", "bucket", None)?);

let storage = CloudConfigBuilder::balanced_durability(backend, "./cache")
    .build();

// Writes return immediately, cloud syncs in background
engine.put(b"user_profile", b"{...}")?; // ⚡ instant
// Background: uploads to S3 every 20ms
```

### Cloud-Replicated (Ephemeral Nodes)

```rust
let backend = Arc::new(AwsS3Backend::new("us-east-1", "bucket", None)?);

let storage = CloudConfigBuilder::replicated_durability(backend, "./cache")
    .build();

// Writes are fastest, cloud is source of truth
engine.put(b"session_data", b"{...}")?; // ⚡⚡ fastest
// Background: uploads to S3 every 100ms
// If container dies, latest data is in S3
```

---

## Why "Steady" and Not "Async" or "Balanced"?

The name "Steady" emphasizes:
1. **Steady stream** of background uploads (every 20ms)
2. **Steady state** operation (predictable, consistent)
3. **Steady balance** between durability and performance

Alternative names considered:
- ~~"Async"~~ - Too technical, doesn't convey the balanced nature
- ~~"Balanced"~~ - True, but doesn't hint at the interval-based mechanism
- ~~"Default"~~ - Boring, doesn't describe characteristics
- ✅ **"Steady"** - Evokes reliability, consistency, and the heartbeat-like sync interval

---

## Common Questions

**Q: Can I change the sync interval in Steady mode?**
```rust
CloudConfigBuilder::balanced_durability(backend, "./cache")
    .with_sync_interval_ms(50)  // 50ms instead of 20ms
    .build()
```

**Q: Can I make Cloud-Replicated safer?**
```rust
CloudConfigBuilder::replicated_durability(backend, "./cache")
    .with_sync_interval_ms(10)  // Sync every 10ms (RPO = 10ms)
    .build()
```

**Q: Can I use Strict but with a larger cache?**
```rust
CloudConfigBuilder::strict_durability(backend, "./cache")
    .with_max_cache_size_mb(4096)  // 4GB cache
    .build()
```

**Q: Which mode should I use for my web app?**
→ **Steady** (it's the default recommendation for a reason!)

**Q: I'm running in Kubernetes, which mode?**
→ **Cloud-Replicated** (pods are ephemeral, cloud is permanent)

**Q: I need ACID transactions, which mode?**
→ **All modes support ACID!** Choose based on durability needs:
- Mission-critical → Strict
- Normal app → Steady  
- Ephemeral compute → Cloud-Replicated

