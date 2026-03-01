# Troubleshooting Guide

**Common issues and how to resolve them**

## Performance Issues

### Slow Write Performance

**Symptoms:**
- High commit latency (>100ms)
- Low write throughput (<1k ops/sec)

**Possible causes:**

1. **Using `sync()` WriteOptions**
   - Every commit fsyncs to disk (~10ms per commit)
   - **Solution**: Use `buffered()` for general workloads

2. **Small transactions**
   - Transaction overhead dominates
   - **Solution**: Batch multiple writes per transaction

3. **Write stalls**
   - Memtable queue full (backpressure)
   - **Solution**: Increase memory budget or reduce write rate

4. **Disk I/O saturation**
   - Check disk IOPS and throughput
   - **Solution**: Use faster disk (SSD) or reduce write rate

**Diagnostic:**

```rust
// Check for write stalls
match engine.commit(tx, WriteOptions::buffered()) {
    Err(MidgeError::WriteStall) => {
        println!("Write stall detected - memtable queue full");
    }
    Ok(_) => { /* success */ }
    Err(e) => eprintln!("Error: {:?}", e),
}
```

**Fixes:**

```rust
// Option 1: Increase memory budget
let opts = OpenOptions::local("./db")
    .memory_budget(MemoryBudget::Bytes(2 << 30))  // 2 GiB
    .build();

// Option 2: Use buffered() instead of sync()
engine.commit(tx, WriteOptions::buffered())?;

// Option 3: Batch writes
let mut tx = engine.begin_tx(cf.id(), TransactionMode::ReadWrite)?;
for i in 0..1000 {
    tx.put(format!("key:{}", i).into_bytes(), b"value".to_vec(), None)?;
}
engine.commit(tx, WriteOptions::buffered())?;  // Commit batch
```

---

### Slow Read Performance

**Symptoms:**
- High get() latency (>100ms)
- Low scan throughput

**Possible causes:**

1. **High read amplification**
   - Too many L0 SSTs (each must be scanned)
   - **Solution**: Trigger manual compaction or increase memory budget

2. **Cache misses**
   - Block not in cache, requires disk I/O
   - **Solution**: Increase cache size via memory budget

3. **Cloud mode latency**
   - Downloading blocks from cloud storage (~50-100ms)
   - **Solution**: Use Local mode or ensure local cache is warm

**Diagnostic:**

```rust
// Check read amplification
let metrics = engine.read_amplification_metrics(&cf)?;
println!("Avg SSTs per read: {}", metrics.avg_ssts_per_read);
println!("L0 overlap rate: {}", metrics.l0_overlap_rate);

if metrics.avg_ssts_per_read > 10.0 {
    println!("High read amplification - consider compaction");
}
```

**Fixes:**

```rust
// Option 1: Manual flush/compaction
engine.flush_cf(&cf)?;

// Option 2: Increase memory budget (larger cache)
let opts = OpenOptions::local("./db")
    .memory_budget(MemoryBudget::Bytes(4 << 30))  // 4 GiB
    .build();

// Option 3: Optimize for read-heavy workload
let opts = OpenOptions::local("./db")
    .workload(WorkloadProfile::ReadMostly)
    .build();
```

---

### High Memory Usage

**Symptoms:**
- Memory usage exceeds expected budget
- OOM errors

**Possible causes:**

1. **Memory budget too high**
   - Auto mode allocated 50% of system memory
   - **Solution**: Set explicit budget

2. **Large memtables**
   - Multiple memtables in queue waiting for flush
   - **Solution**: Trigger manual flush or reduce write rate

3. **Many open snapshots**
   - Snapshots hold references to old SSTs
   - **Solution**: Drop unused snapshots

**Diagnostic:**

```rust
// Check configured memory budget
let opts = OpenOptions::local("./db")
    .memory_budget(MemoryBudget::Bytes(1 << 30))  // Explicit 1 GiB
    .build();

// Monitor actual usage (system tools)
// Linux: cat /proc/<pid>/status | grep VmRSS
// macOS: ps -o rss= -p <pid>
```

**Fixes:**

```rust
// Set explicit memory budget
let opts = OpenOptions::local("./db")
    .memory_budget(MemoryBudget::Bytes(512 << 20))  // 512 MiB
    .goal(Goal::Economy)  // Minimize memory usage
    .build();

// Flush to free memtables
engine.flush_cf(&cf)?;

// Drop unused transactions/snapshots
drop(tx);
```

---

## Recovery Issues

### Slow Recovery After Crash

**Symptoms:**
- Engine::open() takes >10 seconds
- Recovery time increasing over time

**Possible causes:**

1. **Large WAL**
   - Many uncommitted writes need replay
   - **Solution**: Flush before shutdown in production

2. **Cloud download latency**
   - Downloading manifest + WAL from cloud
   - **Solution**: Expected for cloud mode, use Local for faster recovery

**Diagnostic:**

```rust
let start = std::time::Instant::now();
let engine = MidgeEngine::open(opts)?;
let recovery_time = start.elapsed();
println!("Recovery took: {:?}", recovery_time);
```

**Fixes:**

```rust
// Flush before clean shutdown
for cf_name in engine.list_column_families() {
    if let Some(cf) = engine.get_column_family(&cf_name) {
        engine.flush_cf(&cf)?;
    }
}
drop(engine);

// Use smaller memtables (more frequent flushes = smaller WAL)
let opts = OpenOptions::local("./db")
    .goal(Goal::Latency)  // Uses smaller memtables
    .build();
```

---

### Recovery Fails with Corruption Error

**Symptoms:**
- `Engine::open()` returns error
- Logs show "checksum mismatch" or "corrupted data"

**Possible causes:**

1. **Disk corruption**
   - Filesystem or disk hardware error
   - **Solution**: Restore from backup

2. **Incomplete write**
   - Power loss during write
   - **Solution**: Expected for buffered/best_effort writes, restore from backup

3. **Manual file modification**
   - Someone edited files directly
   - **Solution**: Never edit Midge files manually

**Diagnostic:**

Check error message for specific corruption:

```
Error: WAL corruption at offset 1234: checksum mismatch
Error: SST block corruption: expected checksum XXXX, got YYYY
Error: Manifest corruption: invalid JSON
```

**Fixes:**

```bash
# Restore from backup
rm -rf ./db
tar -xzf backup-YYYYMMDD.tar.gz

# For cloud mode: redownload from cloud
rm -rf ./cache
# Engine will redownload on next open
```

---

### Data Loss After Crash

**Symptoms:**
- Recent writes missing after recovery
- Data from before crash present

**Expected behavior based on WriteOptions:**

| WriteOptions | Expected Result |
|--------------|-----------------|
| `sync()` | No data loss (if disk intact) |
| `buffered()` | Up to 500ms of writes may be lost |
| `best_effort()` | All writes lost until flush |

**If unexpected data loss:**

1. **Check WriteOptions used**
   - Verify you used `sync()` for critical data
   - Check commit didn't return error

2. **Check flush status**
   - If using `best_effort()`, did you call `flush_cf()`?

3. **Check disk integrity**
   - Disk write cache enabled? (Data may be lost)
   - Filesystem corruption?

**Prevention:**

```rust
// Use sync() for critical data
engine.commit(tx, WriteOptions::sync())?;

// Verify commit succeeded
match engine.commit(tx, WriteOptions::buffered()) {
    Ok(_) => {
        // Write acknowledged
    }
    Err(e) => {
        // Write failed - not durable!
        eprintln!("Commit failed: {:?}", e);
    }
}

// Disable disk write cache (Linux)
// hdparm -W 0 /dev/sdX
```

---

## Error Messages

### "WriteStall" Error

**Error:**
```
MidgeError::WriteStall
```

**Cause:**
Memtable queue is full. Engine applying backpressure to prevent memory exhaustion.

**Solution:**

```rust
// Wait and retry
loop {
    match engine.commit(tx, WriteOptions::buffered()) {
        Ok(_) => break,
        Err(MidgeError::WriteStall) => {
            std::thread::sleep(Duration::from_millis(100));
            // Retry...
        }
        Err(e) => return Err(e),
    }
}

// Or reduce write rate
// Or increase memory budget
// Or manually flush
engine.flush_cf(&cf)?;
```

---

### "KeyNotFound" Error

**Error:**
```
MidgeError::KeyNotFound
```

**Cause:**
Key does not exist in database.

**Possible reasons:**
1. Key was never written
2. Key was deleted
3. Reading from wrong column family
4. TTL expired
5. Transaction sees snapshot before key was written

**Solution:**

```rust
// Option 1: Handle gracefully
match tx.get(b"key")? {
    Some(value) => {
        // Key exists
    }
    None => {
        // Key does not exist - this is OK
    }
}

// Option 2: Check column family
let cf = engine.get_column_family("correct_cf_name")
    .ok_or("Column family not found")?;
let tx = engine.begin_tx(cf.id(), TransactionMode::ReadOnly)?;
```

---

### "IoError" During Open

**Error:**
```
MidgeError::IoError("Permission denied") 
MidgeError::IoError("File not found")
```

**Cause:**
Filesystem access error.

**Solutions:**

```bash
# Check permissions
ls -l ./db/
chmod 755 ./db/

# Check path exists
mkdir -p ./db/

# Check disk space
df -h

# Check for stale lock file
rm ./db/LOCK  # Only if no other instance running!
```

---

## Cloud Mode Issues

### Cloud Upload Failures

**Symptoms:**
- Slow commits with `cloud_strict()`
- Logs show upload errors

**Possible causes:**

1. **Network connectivity**
   - Check internet connection
   - Check cloud provider status

2. **Invalid credentials**
   - Check environment variables
   - Check IAM permissions

3. **Bucket doesn't exist**
   - Verify bucket name
   - Check region

**Diagnostic:**

Check logs for specific error:

```
Error: Cloud upload failed: 403 Forbidden
Error: Cloud upload failed: Network timeout
Error: Cloud upload failed: NoSuchBucket
```

**Fixes:**

```bash
# Verify credentials
aws s3 ls s3://my-bucket/  # For AWS
az storage blob list -c my-container  # For Azure
gsutil ls gs://my-bucket/  # For GCS

# Check environment variables
echo $AWS_ACCESS_KEY_ID
echo $AWS_SECRET_ACCESS_KEY

# Verify bucket exists and region is correct
aws s3api head-bucket --bucket my-bucket --region us-east-1
```

**Note:** Cloud mode is production-ready. See [../operations/cloud-setup.md](../operations/cloud-setup.md) for configuration details.

---

## Monitoring and Debugging

### Enable Logging

```rust
// Enable debug logs (if using env_logger or similar)
RUST_LOG=midge=debug cargo run

// Or in code:
env_logger::Builder::from_env(Env::default().default_filter_or("debug")).init();
```

### Monitor Key Metrics

```rust
// Read amplification
let metrics = engine.read_amplification_metrics(&cf)?;
println!("SSTs per read: {}", metrics.avg_ssts_per_read);
println!("L0 overlap: {}", metrics.l0_overlap_rate);

// Check if exceeding budgets
if metrics.sst_budget_violation_rate > 0.01 {
    println!("WARNING: 1% of reads exceed SST budget");
}
```

### Performance Profiling

```bash
# Profile with perf (Linux)
perf record -g ./my_app
perf report

# Profile with Instruments (macOS)
# Use Xcode Instruments

# Memory profiling with valgrind
valgrind --tool=massif ./my_app
```

---

## Getting Help

If you're experiencing an issue not covered here:

1. Check [overview.md](overview.md) for design context
2. Check [api-guide.md](api-guide.md) for API details
3. Check [faq.md](faq.md) for common questions
4. Check [durability.md](durability.md) for recovery behavior
5. File a detailed bug report on GitHub with:
   - Midge version
   - Rust version
   - Operating system
   - Minimal reproduction case
   - Relevant logs

## Related Documentation

- **FAQ**: [faq.md](faq.md)
- **Durability**: [durability.md](durability.md)
- **API reference**: [api-guide.md](api-guide.md)
- **Cloud setup**: [../operations/cloud-setup.md](../operations/cloud-setup.md)
- **Performance tuning**: [../operations/performance-tuning.md](../operations/performance-tuning.md)
