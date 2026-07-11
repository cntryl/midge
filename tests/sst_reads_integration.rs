//! Integration test for SST reads with read amplification metrics

mod common;
use cntryl_midge::WriteOptions;
use cntryl_midge::{MidgeResult, Query};
use common::{open_with_mode, opts_for_mode, StorageMode};

fn local_db_path(opts: &common::MidgeOptions) -> std::path::PathBuf {
    match &opts.storage_mode {
        StorageMode::LocalDisk { db_path } => db_path.clone(),
        _ => panic!("expected local disk options"),
    }
}

fn corrupt_first_sst_data_byte(db_path: &std::path::Path) -> MidgeResult<()> {
    use std::io::{Read, Seek, Write};

    let sst_path = std::fs::read_dir(db_path.join("sst"))?
        .filter_map(Result::ok)
        .map(|entry| entry.path())
        .find(|path| path.extension().is_some_and(|extension| extension == "sst"))
        .expect("flushed SST file");
    let mut file = std::fs::OpenOptions::new()
        .read(true)
        .write(true)
        .open(sst_path)?;
    file.seek(std::io::SeekFrom::Start(4))?;
    let mut byte = [0_u8; 1];
    file.read_exact(&mut byte)?;
    file.seek(std::io::SeekFrom::Start(4))?;
    file.write_all(&[byte[0] ^ 0x01])?;
    file.sync_all()?;
    Ok(())
}

#[test]
fn should_read_from_sst_after_flush() -> MidgeResult<()> {
    // Arrange: Create engine
    let engine = open_with_mode(&opts_for_mode("memory"), "memory");
    let cf = engine.create_column_family("test").expect("create cf");

    // Write keys that will be flushed to SST
    for i in 0..10 {
        let key = format!("key_{i:03}");
        let mut tx = engine.begin_tx(cf.id(), cntryl_midge::TransactionMode::ReadWrite)?;
        tx.put(key.as_bytes().to_vec(), b"value_from_sst".to_vec(), None)?;
        tx.commit(WriteOptions::buffered())?;
    }

    // Force flush to SST
    engine.flush_cf(&cf)?;
    std::thread::sleep(std::time::Duration::from_millis(100)); // Give flush time to complete

    // Act: Read keys that should be in SST now
    let read_tx = engine.begin_tx(cf.id(), cntryl_midge::TransactionMode::ReadOnly)?;
    let value1 = read_tx.get(b"key_000")?;
    let value2 = read_tx.get(b"key_005")?;
    let value3 = read_tx.get(b"missing_key")?;

    // Assert: Verify values
    assert_eq!(value1, Some(b"value_from_sst".to_vec().into()));
    assert_eq!(value2, Some(b"value_from_sst".to_vec().into()));
    assert_eq!(value3, None);

    println!("SST reads completed successfully");
    Ok(())
}

#[test]
fn should_track_l0_sst_reads() -> MidgeResult<()> {
    // Arrange: Create engine and write multiple batches to create L0 files
    let engine = open_with_mode(&opts_for_mode("memory"), "memory");
    let cf = engine.create_column_family("test").expect("create cf");

    // Write and flush multiple times to create multiple L0 SSTs
    for batch in 0..3 {
        for i in 0..5 {
            let key = format!("batch{batch}_key{i}");
            let mut tx = engine.begin_tx(cf.id(), cntryl_midge::TransactionMode::ReadWrite)?;
            tx.put(key.as_bytes().to_vec(), b"value".to_vec(), None)?;
            tx.commit(WriteOptions::buffered())?;
        }
        engine.flush_cf(&cf)?;
        std::thread::sleep(std::time::Duration::from_millis(50));
    }

    // Act: Read keys that require checking multiple L0 files
    // Assert: Reads succeed across multiple L0 files
    for batch in 0..3 {
        let key = format!("batch{batch}_key0");
        let read_tx = engine.begin_tx(cf.id(), cntryl_midge::TransactionMode::ReadOnly)?;
        let value = read_tx.get(key.as_bytes())?;
        assert_eq!(value, Some(b"value".to_vec().into()));
    }

    // Note: Metrics are tracked in RuntimeState but not yet exposed to Engine API
    // This test verifies the read path works correctly with multiple L0 files
    println!("Multi-L0 reads completed successfully");
    Ok(())
}

#[test]
fn should_use_key_ranges_for_higher_levels() -> MidgeResult<()> {
    // Arrange: Create engine
    let engine = open_with_mode(&opts_for_mode("memory"), "memory");
    let cf = engine.create_column_family("test").expect("create cf");

    // Write sorted keys
    for i in 0..20 {
        let key = format!("key_{i:03}");
        let mut tx = engine.begin_tx(cf.id(), cntryl_midge::TransactionMode::ReadWrite)?;
        tx.put(key.as_bytes().to_vec(), b"test_value".to_vec(), None)?;
        tx.commit(WriteOptions::buffered())?;
    }

    engine.flush_cf(&cf)?;
    std::thread::sleep(std::time::Duration::from_millis(100));

    // Act: Read various keys
    let read_tx = engine.begin_tx(cf.id(), cntryl_midge::TransactionMode::ReadOnly)?;
    let first = read_tx.get(b"key_000")?;
    let middle = read_tx.get(b"key_010")?;
    let last = read_tx.get(b"key_019")?;
    let missing = read_tx.get(b"key_999")?;

    // Assert
    assert_eq!(first, Some(b"test_value".to_vec().into()));
    assert_eq!(middle, Some(b"test_value".to_vec().into()));
    assert_eq!(last, Some(b"test_value".to_vec().into()));
    assert_eq!(missing, None);

    println!("Range-aware SST reads completed successfully");
    Ok(())
}

#[test]
fn should_handle_memtable_and_sst_reads() -> MidgeResult<()> {
    // Arrange: Mix of memtable and SST data
    let engine = open_with_mode(&opts_for_mode("memory"), "memory");
    let cf = engine.create_column_family("test").expect("create cf");

    // Write to SST
    let mut tx1 = engine.begin_tx(cf.id(), cntryl_midge::TransactionMode::ReadWrite)?;
    tx1.put(b"sst_key".to_vec(), b"sst_value".to_vec(), None)?;
    tx1.commit(WriteOptions::buffered())?;
    engine.flush_cf(&cf)?;
    std::thread::sleep(std::time::Duration::from_millis(100));

    // Write to memtable
    let mut tx2 = engine.begin_tx(cf.id(), cntryl_midge::TransactionMode::ReadWrite)?;
    tx2.put(b"mem_key".to_vec(), b"mem_value".to_vec(), None)?;
    tx2.commit(WriteOptions::buffered())?;

    // Act: Read from both
    let read_tx = engine.begin_tx(cf.id(), cntryl_midge::TransactionMode::ReadOnly)?;
    let from_sst = read_tx.get(b"sst_key")?;
    let from_mem = read_tx.get(b"mem_key")?;

    // Assert
    assert_eq!(from_sst, Some(b"sst_value".to_vec().into()));
    assert_eq!(from_mem, Some(b"mem_value".to_vec().into()));

    // Update SST key in memtable (newer version should win)
    let mut tx3 = engine.begin_tx(cf.id(), cntryl_midge::TransactionMode::ReadWrite)?;
    tx3.put(b"sst_key".to_vec(), b"updated_value".to_vec(), None)?;
    tx3.commit(WriteOptions::buffered())?;
    let read_tx2 = engine.begin_tx(cf.id(), cntryl_midge::TransactionMode::ReadOnly)?;
    let updated = read_tx2.get(b"sst_key")?;
    assert_eq!(updated, Some(b"updated_value".to_vec().into()));

    println!("Mixed memtable/SST reads completed successfully");
    Ok(())
}

#[test]
fn should_error_given_corrupt_sst_when_transaction_get_reads_flushed_key() -> MidgeResult<()> {
    // Arrange
    let opts = opts_for_mode("local");
    let db_path = local_db_path(&opts);
    let engine = open_with_mode(&opts, "local");
    let cf = engine.create_column_family("test").expect("create cf");
    let mut tx = engine.begin_tx(cf.id(), cntryl_midge::TransactionMode::ReadWrite)?;
    tx.put(b"corrupt-key".to_vec(), b"corrupt-value".to_vec(), None)?;
    tx.commit(WriteOptions::buffered())?;
    engine.flush_cf(&cf)?;
    corrupt_first_sst_data_byte(&db_path)?;

    // Act
    let read_tx = engine.begin_tx(cf.id(), cntryl_midge::TransactionMode::ReadOnly)?;
    let error = read_tx
        .get(b"corrupt-key")
        .expect_err("corrupt SST point read must surface an error");

    // Assert
    assert!(
        error.to_string().to_ascii_lowercase().contains("corrupt")
            || error.to_string().contains("CRC32C"),
        "expected corruption-oriented error, got {error}"
    );
    Ok(())
}

#[test]
fn should_error_given_corrupt_sst_when_transaction_scan_reads_flushed_range() -> MidgeResult<()> {
    // Arrange
    let opts = opts_for_mode("local");
    let db_path = local_db_path(&opts);
    let engine = open_with_mode(&opts, "local");
    let cf = engine.create_column_family("test").expect("create cf");
    for i in 0..3 {
        let mut tx = engine.begin_tx(cf.id(), cntryl_midge::TransactionMode::ReadWrite)?;
        tx.put(
            format!("corrupt-range-{i}").into_bytes(),
            b"corrupt-value".to_vec(),
            None,
        )?;
        tx.commit(WriteOptions::buffered())?;
    }
    engine.flush_cf(&cf)?;
    corrupt_first_sst_data_byte(&db_path)?;

    // Act
    let read_tx = engine.begin_tx(cf.id(), cntryl_midge::TransactionMode::ReadOnly)?;
    let mut scan = read_tx.scan(&Query::new())?;
    let error = scan
        .next()
        .transpose()
        .expect_err("corrupt SST range scan must surface an error while advancing");

    // Assert
    assert!(
        error.to_string().to_ascii_lowercase().contains("corrupt")
            || error.to_string().contains("CRC32C"),
        "expected corruption-oriented error, got {error}"
    );
    Ok(())
}
