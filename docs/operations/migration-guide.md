# Migration and Upgrade Guide

**Version compatibility, breaking changes, and upgrade procedures**

## Version Compatibility

### Current Version

Midge is in early development (0.1.x). APIs and data formats may change between minor versions until 1.0 release.

### Compatibility Policy

**Pre-1.0 (current):**
- Minor version updates (0.1 → 0.2) may include breaking changes
- Patch updates (0.1.0 → 0.1.1) maintain compatibility
- Data format changes announced in CHANGELOG

**Post-1.0 (planned):**
- Minor updates (1.0 → 1.1) maintain API compatibility
- Major updates (1.x → 2.0) may include breaking changes
- Data format stability within major version

## Upgrade Procedures

### Between Patch Versions (0.1.0 → 0.1.1)

**Safe upgrade path:**

1. Update Cargo.toml:
   ```toml
   [dependencies]
   cntryl-midge = "0.1.1"
   ```

2. Rebuild:
   ```bash
   cargo build
   ```

3. No data migration needed (format compatible)

### Between Minor Versions (0.1 → 0.2)

**Check CHANGELOG first** for breaking changes.

**Recommended upgrade path:**

1. **Backup existing data:**
   ```bash
   # Local mode
   tar -czf backup-$(date +%Y%m%d).tar.gz ./db/
   
   # Cloud mode
   aws s3 sync s3://my-bucket/db1/ ./backups/$(date +%Y%m%d)/
   ```

2. **Update dependency:**
   ```toml
   [dependencies]
   cntryl-midge = "0.2"
   ```

3. **Check for API changes:**
   - Review CHANGELOG for breaking API changes
   - Update code for new APIs
   - Run `cargo test` to catch compilation errors

4. **Test with copy of data:**
   ```bash
   cp -r ./db ./db-test
   # Run application with test data
   # Verify functionality
   ```

5. **Deploy to production**

### Data Format Migration

If data format changes between versions, follow this procedure:

**Export from old version:**

```rust
// Using old Midge version
let old_engine = MidgeEngine::open(opts)?;
let cf = old_engine.get_column_family("default")
    .ok_or("CF not found")?;

// Export all data
let tx = old_engine.begin_tx(cf.id(), TransactionMode::ReadOnly)?;
let query = tx.scan().build()?;

let mut export = std::fs::File::create("export.bin")?;
for entry in query {
    let (key, value) = entry?;
    // Write key length, key, value length, value
    export.write_u32::<LittleEndian>(key.len() as u32)?;
    export.write_all(&key)?;
    export.write_u32::<LittleEndian>(value.len() as u32)?;
    export.write_all(&value)?;
}
```

**Import into new version:**

```rust
// Using new Midge version
let new_engine = MidgeEngine::open(opts)?;
let cf = new_engine.create_column_family("default")?;

let mut import = std::fs::File::open("export.bin")?;
loop {
    // Read key
    let key_len = match import.read_u32::<LittleEndian>() {
        Ok(len) => len,
        Err(ref e) if e.kind() == std::io::ErrorKind::UnexpectedEof => break,
        Err(e) => return Err(e.into()),
    };
    let mut key = vec![0u8; key_len as usize];
    import.read_exact(&mut key)?;
    
    // Read value
    let value_len = import.read_u32::<LittleEndian>()?;
    let mut value = vec![0u8; value_len as usize];
    import.read_exact(&mut value)?;
    
    // Import
    let mut tx = new_engine.begin_tx(cf.id(), TransactionMode::ReadWrite)?;
    tx.put(key, value, None)?;
    new_engine.commit(tx, WriteOptions::buffered())?;
}

// Flush to make durable
new_engine.flush_cf(&cf)?;
```

## Breaking Changes by Version

### 0.1.0 → Current

No breaking changes yet (initial release).

**Future breaking changes will be documented here.**

---

## Backup Strategies

### Local Mode Backup

**Full backup:**

```bash
# Stop writes, flush, then backup
./my_app shutdown  # Your application shutdown procedure

# Backup database files
tar -czf backup-$(date +%Y%m%d-%H%M%S).tar.gz ./db/

# Verify backup
tar -tzf backup-*.tar.gz | head
```

**Incremental backup:**

```bash
# Backup only new SST files since last backup
find ./db/sst/ -newer ./db/last_backup.marker -type f | \
    tar -czf incremental-$(date +%Y%m%d-%H%M%S).tar.gz -T -

# Update marker
touch ./db/last_backup.marker
```

### Cloud Mode Backup

**Backup from cloud storage:**

```bash
# AWS S3
aws s3 sync s3://my-bucket/db1/ ./backups/$(date +%Y%m%d)/

# Azure Blob
az storage blob download-batch \
    --source container-name \
    --destination ./backups/$(date +%Y%m%d)/ \
    --pattern "db1/*"

# Google Cloud Storage
gsutil -m rsync -r gs://my-bucket/db1/ ./backups/$(date +%Y%m%d)/
```

**Backup to secondary bucket:**

```bash
# AWS S3 to S3
aws s3 sync s3://primary-bucket/db1/ s3://backup-bucket/db1-$(date +%Y%m%d)/

# Cross-region replication (configure via S3 console)
aws s3api put-bucket-replication --bucket primary-bucket --replication-configuration file://replication.json
```

### Backup Verification

```rust
#[test]
fn should_restore_from_backup() {
    // 1. Create backup
    let original = MidgeEngine::open(OpenOptions::local("./db").build())?;
    // ... populate data ...
    original.flush_cf(&cf)?;
    drop(original);
    
    // 2. Backup files
    std::fs::rename("./db", "./db.backup")?;
    
    // 3. Restore
    std::fs::rename("./db.backup", "./db")?;
    
    // 4. Verify
    let restored = MidgeEngine::open(OpenOptions::local("./db").build())?;
    // ... verify data ...
}
```

---

## Cloud Provider Migration

### From Local to Cloud

```rust
// 1. Export from Local mode
let local_engine = MidgeEngine::open(OpenOptions::local("./db").build())?;
let cf = local_engine.get_column_family("default")?;

// Export all key-value pairs
let tx = local_engine.begin_tx(cf.id(), TransactionMode::ReadOnly)?;
let query = tx.scan().build()?;
let mut data = Vec::new();
for entry in query {
    let (key, value) = entry?;
    data.push((key, value));
}
drop(local_engine);

// 2. Import into Cloud mode
let cloud_engine = MidgeEngine::open(
    OpenOptions::cloud("./cache", "my-bucket", "db/").build()
)?;
let cf = cloud_engine.create_column_family("default")?;

for (key, value) in data {
    let mut tx = cloud_engine.begin_tx(cf.id(), TransactionMode::ReadWrite)?;
    tx.put(key, value, None)?;
    cloud_engine.commit(tx, WriteOptions::buffered())?;
}

cloud_engine.flush_cf(&cf)?;
```

### Between Cloud Providers

```bash
# AWS S3 to Google Cloud Storage
gsutil -m rsync -r s3://aws-bucket/db1/ gs://gcs-bucket/db1/

# AWS S3 to Azure Blob
# Use azcopy or rclone
rclone sync s3:aws-bucket/db1/ azure:container-name/db1/
```

Update configuration:

```rust
// Old: AWS S3
let opts = OpenOptions::cloud("./cache", "aws-bucket", "db1/")
    .endpoint(Some("https://s3.us-east-1.amazonaws.com".to_string()))
    .build();

// New: Google Cloud Storage
let opts = OpenOptions::cloud("./cache", "gcs-bucket", "db1/")
    .endpoint(Some("https://storage.googleapis.com".to_string()))
    .build();
```

---

## Rollback Procedures

### Rollback Application Version

If new Midge version causes issues:

1. **Stop application**

2. **Restore code to previous version:**
   ```toml
   [dependencies]
   cntryl-midge = "0.1.0"  # Previous version
   ```

3. **Rebuild and deploy:**
   ```bash
   cargo build --release
   ./deploy.sh
   ```

4. **If data format changed, restore from backup:**
   ```bash
   rm -rf ./db
   tar -xzf backup-YYYYMMDD.tar.gz
   ```

### Rollback Data from Backup

```bash
# Stop application
./my_app shutdown

# Restore from backup
rm -rf ./db
tar -xzf backup-YYYYMMDD.tar.gz

# Restart application
./my_app start

# Verify
./my_app verify  # Your verification procedure
```

---

## Monitoring Upgrades

### Pre-Upgrade Checklist

- [ ] Backup complete and verified
- [ ] CHANGELOG reviewed for breaking changes
- [ ] Test environment validated with new version
- [ ] Rollback procedure documented

### Post-Upgrade Verification

```rust
// Check version
println!("Midge version: {}", env!("CARGO_PKG_VERSION"));

// Verify data integrity
let tx = engine.begin_tx(cf.id(), TransactionMode::ReadOnly)?;
let count = tx.scan().build()?.count();
println!("Total keys: {}", count);

// Check recovery time
let start = Instant::now();
let engine = MidgeEngine::open(opts)?;
println!("Recovery: {:?}", start.elapsed());

// Monitor performance
let metrics = engine.read_amplification_metrics(&cf)?;
println!("Read amp: {}", metrics.avg_ssts_per_read);
```

---

## Long-Term Maintenance

### Retention Policy

**Backups:**
- Daily backups: Retain 7 days
- Weekly backups: Retain 4 weeks
- Monthly backups: Retain 12 months

**Automation:**

```bash
#!/bin/bash
# backup.sh

DATE=$(date +%Y%m%d)
BACKUP_DIR="./backups/$DATE"

# Create backup
tar -czf "$BACKUP_DIR/db.tar.gz" ./db/

# Delete backups older than 7 days
find ./backups/ -name "*.tar.gz" -mtime +7 -delete
```

### Upgrade Schedule

**Recommended:**
- Patch updates: Apply monthly or as needed for security
- Minor updates: Test in staging, apply quarterly
- Major updates: Plan migration, test thoroughly, apply annually

---

## Related Documentation

- **Overview**: [../user-guides/overview.md](../user-guides/overview.md)
- **Cloud setup**: [cloud-setup.md](cloud-setup.md )
- **Troubleshooting**: [../user-guides/troubleshooting.md](../user-guides/troubleshooting.md)
- **CHANGELOG**: [../../CHANGELOG.md](../../CHANGELOG.md)
