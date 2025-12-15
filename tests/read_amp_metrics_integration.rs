//! Integration test for read amplification metrics

use cntryl_midge::common::MidgeResult;
use cntryl_midge::sst::{
    fs::{FsSstWriter, SstFile},
    traits::{DynSstWriter, SstReader},
};
use tempfile::TempDir;

#[test]
fn should_track_blocks_read_per_operation() -> MidgeResult<()> {
    // Arrange: Create SST with multiple blocks
    let temp_dir = TempDir::new().unwrap();
    let mut writer = FsSstWriter::new(temp_dir.path(), 256)?; // Small blocks

    // Write enough keys to span multiple blocks
    for i in 0..50 {
        let key = format!("key_{:03}", i);
        let value = vec![b'x'; 100]; // Large values to force block splits
        writer.add(key.as_bytes(), &value)?;
    }

    let bytes = Box::new(writer).finish_bytes()?;
    let sst_path = temp_dir.path().join("test.sst");
    std::fs::write(&sst_path, &bytes)?;

    // Act: Open reader and perform reads
    let mut reader = SstFile::open(&sst_path)?;
    reader.load_block_bloom()?;

    let metrics_before = reader.read_amp_metrics();
    assert_eq!(metrics_before.reads_total(), 0);

    // Perform several reads
    let _ = reader.get(b"key_000")?;
    let _ = reader.get(b"key_025")?;
    let _ = reader.get(b"missing_key")?;

    // Assert: Verify metrics were recorded
    let metrics_after = reader.read_amp_metrics();

    println!("Read Amp Metrics after 3 reads:");
    println!("  Reads Total: {}", metrics_after.reads_total());
    println!(
        "  SSTs Touched Total: {}",
        metrics_after.ssts_touched_total()
    );
    println!("  Blocks Read Total: {}", metrics_after.blocks_read_total());
    println!(
        "  Avg SSTs per Read: {:.2}",
        metrics_after.avg_ssts_per_read()
    );
    println!(
        "  Avg Blocks per Read: {:.2}",
        metrics_after.avg_blocks_per_read()
    );

    assert_eq!(metrics_after.reads_total(), 3);
    assert_eq!(metrics_after.ssts_touched_total(), 3); // 1 SST per read
    assert!(
        metrics_after.blocks_read_total() > 0,
        "Should have read some blocks"
    );
    assert_eq!(metrics_after.avg_ssts_per_read(), 1.0); // Always 1 SST for single file

    Ok(())
}

#[test]
fn should_track_bloom_rejections_correctly() -> MidgeResult<()> {
    // Arrange: Create SST with bloom filter
    let temp_dir = TempDir::new().unwrap();
    let mut writer = FsSstWriter::new(temp_dir.path(), 1024)?;

    for i in 0..100 {
        let key = format!("key_{:03}", i);
        writer.add(key.as_bytes(), b"value")?;
    }

    let bytes = Box::new(writer).finish_bytes()?;
    let sst_path = temp_dir.path().join("test.sst");
    std::fs::write(&sst_path, &bytes)?;

    // Act: Open reader and query absent keys (bloom should reject)
    let mut reader = SstFile::open(&sst_path)?;
    reader.load_block_bloom()?;

    // Query keys that don't exist - bloom should reject most
    for i in 0..10 {
        let key = format!("missing_{:03}", i);
        let _ = reader.get(key.as_bytes())?;
    }

    // Assert: Reads happened but blocks may be skipped by bloom
    let metrics = reader.read_amp_metrics();

    println!("Read Amp Metrics with bloom rejections:");
    println!("  Reads Total: {}", metrics.reads_total());
    println!("  Blocks Read Total: {}", metrics.blocks_read_total());
    println!(
        "  Avg Blocks per Read: {:.2}",
        metrics.avg_blocks_per_read()
    );

    assert_eq!(metrics.reads_total(), 10);

    // Bloom rejections should reduce average blocks per read
    // (Some reads may hit block bloom, others SST bloom)
    assert!(
        metrics.avg_blocks_per_read() < 2.0,
        "Bloom should keep avg blocks low, got {}",
        metrics.avg_blocks_per_read()
    );

    Ok(())
}

#[test]
fn should_calculate_averages_correctly() -> MidgeResult<()> {
    // Arrange: Create SST
    let temp_dir = TempDir::new().unwrap();
    let mut writer = FsSstWriter::new(temp_dir.path(), 512)?;

    for i in 0..20 {
        let key = format!("key_{:02}", i);
        writer.add(key.as_bytes(), b"test_value")?;
    }

    let bytes = Box::new(writer).finish_bytes()?;
    let sst_path = temp_dir.path().join("test.sst");
    std::fs::write(&sst_path, &bytes)?;

    // Act: Perform multiple reads
    let reader = SstFile::open(&sst_path)?;

    for i in 0..5 {
        let key = format!("key_{:02}", i);
        let _ = reader.get(key.as_bytes())?;
    }

    // Assert: Check calculated averages
    let metrics = reader.read_amp_metrics();

    assert_eq!(metrics.reads_total(), 5);
    assert_eq!(metrics.ssts_touched_total(), 5); // 1 SST * 5 reads
    assert_eq!(metrics.avg_ssts_per_read(), 1.0);

    // Each read should touch at least 1 block (index) + 1 block (data) = 2
    assert!(
        metrics.avg_blocks_per_read() >= 2.0,
        "Should read at least index + data block per read, got {}",
        metrics.avg_blocks_per_read()
    );

    Ok(())
}

#[test]
fn should_handle_zero_reads_without_panic() -> MidgeResult<()> {
    // Arrange: Create SST but don't read from it
    let temp_dir = TempDir::new().unwrap();
    let mut writer = FsSstWriter::new(temp_dir.path(), 1024)?;
    writer.add(b"key", b"value")?;

    let bytes = Box::new(writer).finish_bytes()?;
    let sst_path = temp_dir.path().join("test.sst");
    std::fs::write(&sst_path, &bytes)?;

    // Act: Open reader but perform no reads
    let reader = SstFile::open(&sst_path)?;

    // Assert: Metrics should handle zero reads gracefully
    let metrics = reader.read_amp_metrics();

    assert_eq!(metrics.reads_total(), 0);
    assert_eq!(metrics.avg_ssts_per_read(), 0.0);
    assert_eq!(metrics.avg_blocks_per_read(), 0.0);
    assert_eq!(metrics.l0_overlap_rate(), 0.0);

    Ok(())
}
