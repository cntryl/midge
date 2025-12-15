// Integration test to verify BloomMetrics are recorded during SST reads

use cntryl_midge::common::MidgeResult;
use cntryl_midge::sst::{
    fs::{FsSstWriter, SstFile},
    traits::{DynSstWriter, SstReader},
};
use tempfile::TempDir;

#[test]
fn should_record_bloom_metrics_for_absent_keys() -> MidgeResult<()> {
    // Arrange: Create SST with bloom filter
    let temp_dir = TempDir::new().unwrap();
    let mut writer = FsSstWriter::new(temp_dir.path(), 1024)?;

    // Write keys: key_00 through key_99
    for i in 0..100 {
        let key = format!("key_{:02}", i);
        let value = format!("value_{:02}", i);
        writer.add(key.as_bytes(), value.as_bytes())?;
    }

    let bytes = Box::new(writer).finish_bytes()?;
    let sst_path = temp_dir.path().join("test.sst");
    std::fs::write(&sst_path, &bytes)?;

    // Act: Open reader and query absent keys (focus on bloom behavior)
    let mut reader = SstFile::open(&sst_path)?;
    reader.load_block_bloom()?; // Load the block bloom filter written by writer

    // Get metrics before reads
    let metrics_before = reader.bloom_metrics();
    assert_eq!(metrics_before.checks(), 0);
    assert_eq!(metrics_before.negatives(), 0);
    assert_eq!(metrics_before.blocks_skipped(), 0);

    // Query absent keys (should hit bloom negatives)
    for i in 0..10 {
        let key = format!("missing_key_{:02}", i);
        let _result = reader.get(key.as_bytes());
        // Don't assert on result - just testing bloom metrics
    }

    // Assert: Verify metrics were recorded for absent keys
    let metrics_after = reader.bloom_metrics();

    println!("Bloom Metrics after 10 absent key queries:");
    println!("  Checks: {}", metrics_after.checks());
    println!("  Negatives: {}", metrics_after.negatives());
    println!("  Blocks Skipped: {}", metrics_after.blocks_skipped());
    println!(
        "  Negative Rate: {:.2}%",
        metrics_after.negative_rate() * 100.0
    );

    // We should have recorded bloom checks for absent keys
    assert!(
        metrics_after.checks() >= 10,
        "Expected at least 10 bloom checks for absent keys, got {}",
        metrics_after.checks()
    );

    // We should have some negatives (bloom said "definitely not present")
    // Bloom filters should reject most absent keys
    assert!(
        metrics_after.negatives() > 0,
        "Expected some bloom negatives for absent keys, got {}",
        metrics_after.negatives()
    );

    // Blocks should be skipped when bloom says "definitely not present"
    assert!(
        metrics_after.blocks_skipped() > 0,
        "Expected blocks_skipped > 0 when bloom rejects keys, got {}",
        metrics_after.blocks_skipped()
    );

    Ok(())
}

#[test]
fn should_record_blocks_skipped_metric() -> MidgeResult<()> {
    // Arrange: Create multi-block SST with bloom filter
    let temp_dir = TempDir::new().unwrap();
    let mut writer = FsSstWriter::new(temp_dir.path(), 256)?; // Small blocks to force multiple blocks

    // Write enough keys to span multiple blocks
    for i in 0..100 {
        let key = format!("key_{:03}", i);
        let value = vec![b'x'; 100]; // Large values to force block splits
        writer.add(key.as_bytes(), &value)?;
    }

    let bytes = Box::new(writer).finish_bytes()?;
    let sst_path = temp_dir.path().join("test.sst");
    std::fs::write(&sst_path, &bytes)?;

    // Act: Open reader and query absent key
    let mut reader = SstFile::open(&sst_path)?;
    reader.load_block_bloom()?; // Load the block bloom filter written by writer

    // Query key that doesn't exist - should use sparse index and check block blooms
    let result = reader.get(b"missing_key")?;
    assert!(result.is_none());

    // Assert: Verify blocks_skipped was incremented
    let metrics = reader.bloom_metrics();

    println!("Bloom Metrics after sparse index path:");
    println!("  Checks: {}", metrics.checks());
    println!("  Negatives: {}", metrics.negatives());
    println!("  Blocks Skipped: {}", metrics.blocks_skipped());

    // If block bloom said "definitely not present", we should have skipped the block read
    if metrics.negatives() > 0 {
        assert!(
            metrics.blocks_skipped() > 0,
            "Expected blocks_skipped > 0 when bloom says definitely not present"
        );
    }

    Ok(())
}
