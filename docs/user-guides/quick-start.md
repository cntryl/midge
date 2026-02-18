# Quick Start Guide

**Get started with Midge in 5 minutes**

## Installation

Add Midge to your `Cargo.toml`:

```toml
[dependencies]
cntryl-midge = "0.1"  # Check latest version
```

## Basic Example

Here's a complete example showing basic operations:

```rust
use cntryl_midge::{MidgeEngine, OpenOptions, TransactionMode, WriteOptions};

fn main() -> Result<(), Box<dyn std::error::Error>> {
    // 1. Open an in-memory database (no persistence)
    let opts = OpenOptions::in_memory().build();
    let engine = MidgeEngine::open(opts)?;
    
    // 2. Create a column family
    let cf = engine.create_column_family("default")?;
    
    // 3. Write some data
    let mut tx = engine.begin_tx(cf.id(), TransactionMode::ReadWrite)?;
    tx.put(b"user:1".to_vec(), b"alice".to_vec(), None)?;
    tx.put(b"user:2".to_vec(), b"bob".to_vec(), None)?;
    engine.commit(tx, WriteOptions::sync())?;
    
    // 4. Read data
    let tx = engine.begin_tx(cf.id(), TransactionMode::ReadOnly)?;
    if let Some(value) = tx.get(b"user:1")? {
        println!("user:1 = {}", String::from_utf8_lossy(&value));
    }
    
    // 5. Scan a range
    let query = tx.scan()
        .start(b"user:".to_vec())
        .prefix(b"user:".to_vec())
        .build()?;
    
    for entry in query {
        let (key, value) = entry?;
        println!("{} = {}", 
            String::from_utf8_lossy(&key), 
            String::from_utf8_lossy(&value)
        );
    }
    
    // 6. Delete data
    let mut tx = engine.begin_tx(cf.id(), TransactionMode::ReadWrite)?;
    tx.delete(b"user:2".to_vec())?;
    engine.commit(tx, WriteOptions::sync())?;
    
    // 7. Clean shutdown
    drop(engine);
    
    Ok(())
}
```

## Storage Modes

### In-Memory (Testing)

```rust
let opts = OpenOptions::in_memory().build();
let engine = MidgeEngine::open(opts)?;
```

No persistence. Data lost when engine drops. Use for testing and caching.

### Local Filesystem (Single-Node)

```rust
let opts = OpenOptions::local("./my_database").build();
let engine = MidgeEngine::open(opts)?;
```

Data persists to local disk. Use for traditional single-node deployments.

### Cloud Storage (Cloud-Native)

```rust
let opts = OpenOptions::cloud(
    "./cache",           // Local cache directory
    "my-bucket",         // S3/GCS/Azure bucket name
    "databases/prod/"    // Object key prefix
).build();

let engine = MidgeEngine::open(opts)?;
```

Cloud is source of truth, local disk is ephemeral cache. Use for serverless and distributed deployments.

**Note:** Cloud mode currently development/testing only. See [../operations/cloud-setup.md](../operations/cloud-setup.md) for configuration.

## Write Durability

All commits require explicit `WriteOptions`:

```rust
// Full durability (fsync to disk)
engine.commit(tx, WriteOptions::sync())?;

// Group commit batching (fast, <500ms loss window)
engine.commit(tx, WriteOptions::buffered())?;

// No durability until flush (bulk loads only)
engine.commit(tx, WriteOptions::best_effort())?;
engine.flush_cf(&cf)?;  // Make durable
```

See [durability.md](durability.md) for detailed guarantees.

## Transactions

### Read-Only Transaction

```rust
let tx = engine.begin_tx(cf.id(), TransactionMode::ReadOnly)?;
let value = tx.get(b"key")?;
// No commit needed for read-only
drop(tx);
```

### Read-Write Transaction

```rust
let mut tx = engine.begin_tx(cf.id(), TransactionMode::ReadWrite)?;
tx.put(b"key1".to_vec(), b"value1".to_vec(), None)?;
tx.put(b"key2".to_vec(), b"value2".to_vec(), None)?;
tx.delete(b"old_key".to_vec())?;

// Atomic commit - all writes visible together
engine.commit(tx, WriteOptions::buffered())?;
```

### Transaction with TTL

```rust
let mut tx = engine.begin_tx(cf.id(), TransactionMode::ReadWrite)?;

// Value expires after 3600 seconds
tx.put(b"session:123".to_vec(), b"data".to_vec(), Some(3600))?;

engine.commit(tx, WriteOptions::buffered())?;
```

## Range Scans

### Prefix Scan

```rust
let tx = engine.begin_tx(cf.id(), TransactionMode::ReadOnly)?;
let query = tx.scan()
    .prefix(b"user:".to_vec())
    .build()?;

for entry in query {
    let (key, value) = entry?;
    // Process entries with "user:" prefix
}
```

### Bounded Range Scan

```rust
let query = tx.scan()
    .start(b"user:100".to_vec())
    .end(b"user:200".to_vec())
    .build()?;

for entry in query {
    let (key, value) = entry?;
    // Process entries between user:100 and user:200
}
```

### Reverse Scan

```rust
let query = tx.scan()
    .prefix(b"user:".to_vec())
    .direction(Direction::Reverse)
    .build()?;

for entry in query {
    let (key, value) = entry?;
    // Process entries in reverse order
}
```

### Limited Scan

```rust
let query = tx.scan()
    .prefix(b"user:".to_vec())
    .limit(10)
    .build()?;

for entry in query {
    let (key, value) = entry?;
    // Process at most 10 entries
}
```

## Column Families

Use column families to logically separate data:

```rust
let engine = MidgeEngine::open(opts)?;

// Create multiple column families
let users_cf = engine.create_column_family("users")?;
let posts_cf = engine.create_column_family("posts")?;
let comments_cf = engine.create_column_family("comments")?;

// Each CF is independent
let mut tx = engine.begin_tx(users_cf.id(), TransactionMode::ReadWrite)?;
tx.put(b"user:1".to_vec(), b"alice".to_vec(), None)?;
engine.commit(tx, WriteOptions::buffered())?;

let mut tx = engine.begin_tx(posts_cf.id(), TransactionMode::ReadWrite)?;
tx.put(b"post:1".to_vec(), b"Hello world".to_vec(), None)?;
engine.commit(tx, WriteOptions::buffered())?;
```

## Configuration

### Smart Defaults

```rust
let opts = OpenOptions::local("./db")
    .goal(Goal::Latency)              // Optimize for low latency
    .memory_budget(MemoryBudget::Auto) // Use ~50% of available memory
    .workload(WorkloadProfile::Mixed)  // Balanced read/write
    .build();
```

### Explicit Memory Budget

```rust
use cntryl_midge::{Goal, MemoryBudget};

let opts = OpenOptions::local("./db")
    .goal(Goal::Throughput)                    // Optimize for throughput
    .memory_budget(MemoryBudget::Bytes(1 << 30))  // 1 GiB total memory
    .build();
```

### Workload Profiles

```rust
use cntryl_midge::WorkloadProfile;

// Write-heavy workload
let opts = OpenOptions::local("./db")
    .workload(WorkloadProfile::WriteHeavy)
    .build();

// Read-mostly workload
let opts = OpenOptions::local("./db")
    .workload(WorkloadProfile::ReadMostly)
    .build();

// Range scan workload
let opts = OpenOptions::local("./db")
    .workload(WorkloadProfile::RangeScan)
    .build();
```

All low-level parameters are derived automatically from these high-level settings.

## Error Handling

```rust
use cntryl_midge::MidgeError;

match engine.commit(tx, WriteOptions::sync()) {
    Ok(_) => println!("Committed successfully"),
    Err(MidgeError::WriteStall) => {
        // Memtable queue full, backpressure
        std::thread::sleep(std::time::Duration::from_millis(100));
        // Retry...
    }
    Err(MidgeError::KeyNotFound) => println!("Key does not exist"),
    Err(e) => eprintln!("Error: {:?}", e),
}
```

## Common Patterns

### Bulk Load

```rust
// Fast bulk load with best_effort, then flush
let mut tx = engine.begin_tx(cf.id(), TransactionMode::ReadWrite)?;

for i in 0..1_000_000 {
    tx.put(
        format!("key:{}", i).into_bytes(),
        b"value".to_vec(),
        None
    )?;
    
    // Commit every 10k writes
    if i % 10_000 == 0 {
        engine.commit(tx, WriteOptions::best_effort())?;
        tx = engine.begin_tx(cf.id(), TransactionMode::ReadWrite)?;
    }
}

engine.commit(tx, WriteOptions::best_effort())?;

// Make all writes durable
engine.flush_cf(&cf)?;
```

### Read-Modify-Write

```rust
let mut tx = engine.begin_tx(cf.id(), TransactionMode::ReadWrite)?;

// Read current value
let current = tx.get(b"counter")?
    .unwrap_or_else(|| vec![0]);

// Modify
let count = u64::from_le_bytes(current.try_into().unwrap());
let new_value = (count + 1).to_le_bytes().to_vec();

// Write back
tx.put(b"counter".to_vec(), new_value, None)?;
engine.commit(tx, WriteOptions::buffered())?;
```

### Graceful Shutdown

```rust
// Flush all column families before shutdown
for cf_name in engine.list_column_families() {
    if let Some(cf) = engine.get_column_family(&cf_name) {
        engine.flush_cf(&cf)?;
    }
}

// Drop engine (releases locks, closes files)
drop(engine);
```

## Next Steps

- **Complete API reference**: [api-guide.md](api-guide.md)
- **Durability guarantees**: [durability.md](durability.md)
- **Cloud deployment**: [../operations/cloud-setup.md](../operations/cloud-setup.md)
- **Performance tuning**: [../operations/performance-tuning.md](../operations/performance-tuning.md)
- **FAQ**: [faq.md](faq.md)
- **Troubleshooting**: [troubleshooting.md](troubleshooting.md)
