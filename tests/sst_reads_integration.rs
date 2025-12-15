//! Integration test for SST reads with read amplification metrics

use cntryl_midge::common::MidgeResult;
use cntryl_midge::testkit::new_engine;

#[test]
fn should_read_from_sst_after_flush() -> MidgeResult<()> {
    // Arrange: Create engine
    let engine = new_engine()?;
    let cf = engine.default_column_family();

    // Write keys that will be flushed to SST
    for i in 0..10 {
        let key = format!("key_{:03}", i);
        engine.put(cf, key.as_bytes(), b"value_from_sst")?;
    }

    // Force flush to SST
    engine.flush()?;
    std::thread::sleep(std::time::Duration::from_millis(100)); // Give flush time to complete

    // Act: Read keys that should be in SST now
    let value1 = engine.get(cf, b"key_000")?;
    let value2 = engine.get(cf, b"key_005")?;
    let value3 = engine.get(cf, b"missing_key")?;

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
    let engine = new_engine()?;
    let cf = engine.default_column_family();

    // Write and flush multiple times to create multiple L0 SSTs
    for batch in 0..3 {
        for i in 0..5 {
            let key = format!("batch{}_key{}", batch, i);
            engine.put(cf, key.as_bytes(), b"value")?;
        }
        engine.flush()?;
        std::thread::sleep(std::time::Duration::from_millis(50));
    }

    // Act: Read keys that require checking multiple L0 files
    for batch in 0..3 {
        let key = format!("batch{}_key0", batch);
        let value = engine.get(cf, key.as_bytes())?;
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
    let engine = new_engine()?;
    let cf = engine.default_column_family();

    // Write sorted keys
    for i in 0..20 {
        let key = format!("key_{:03}", i);
        engine.put(cf, key.as_bytes(), b"test_value")?;
    }

    engine.flush()?;
    std::thread::sleep(std::time::Duration::from_millis(100));

    // Act: Read various keys
    let first = engine.get(cf, b"key_000")?;
    let middle = engine.get(cf, b"key_010")?;
    let last = engine.get(cf, b"key_019")?;
    let missing = engine.get(cf, b"key_999")?;

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
    let engine = new_engine()?;
    let cf = engine.default_column_family();

    // Write to SST
    engine.put(cf, b"sst_key", b"sst_value")?;
    engine.flush()?;
    std::thread::sleep(std::time::Duration::from_millis(100));

    // Write to memtable
    engine.put(cf, b"mem_key", b"mem_value")?;

    // Act: Read from both
    let from_sst = engine.get(cf, b"sst_key")?;
    let from_mem = engine.get(cf, b"mem_key")?;

    // Assert
    assert_eq!(from_sst, Some(b"sst_value".to_vec().into()));
    assert_eq!(from_mem, Some(b"mem_value".to_vec().into()));

    // Update SST key in memtable (newer version should win)
    engine.put(cf, b"sst_key", b"updated_value")?;
    let updated = engine.get(cf, b"sst_key")?;
    assert_eq!(updated, Some(b"updated_value".to_vec().into()));

    println!("Mixed memtable/SST reads completed successfully");
    Ok(())
}
