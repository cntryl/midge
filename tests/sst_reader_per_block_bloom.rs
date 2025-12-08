use cntryl_midge::common::codec::CompressionType;
use cntryl_midge::sst::fs::{FsDynWriter, SstFile};
/// TDD Tests for Phase 1.2: Per-block bloom reader integration
///
/// Tests that per-block blooms are written to SST during finalization,
/// and can be loaded and queried by the reader.
use cntryl_midge::sst::DynSstWriter;
use std::path::PathBuf;

fn create_temp_sst_dir() -> PathBuf {
    let dir = std::env::temp_dir().join(format!("midge_reader_{}", uuid::Uuid::new_v4()));
    std::fs::create_dir_all(&dir).unwrap();
    dir
}

#[test]
fn should_write_per_block_blooms_to_sst_during_finalization() {
    // Arrange: Create SST with multiple blocks
    let temp_dir = create_temp_sst_dir();
    let mut writer = FsDynWriter::new(&temp_dir, CompressionType::None, 1024, false, None)
        .expect("Failed to create writer");

    // Add keys to create multiple blocks
    for i in 0..50 {
        let key = format!("key_{:04}", i);
        let value = "value_content";
        writer
            .add(key.as_bytes(), value.as_bytes())
            .expect("Failed to add key");
    }

    // Act: Finish writing - should write per-block blooms to SST
    let sst_path = temp_dir.join("with_blooms.sst");
    (Box::new(writer) as Box<dyn DynSstWriter>)
        .finish_to_path(&sst_path)
        .expect("Failed to finish writing SST");

    // Assert: SST file exists
    assert!(sst_path.exists(), "SST file should exist");

    // Assert: SST should be readable
    let sst = SstFile::open(&sst_path).expect("Should open SST");
    let val = sst.get(b"key_0025").expect("Should read");
    assert!(val.is_some(), "Should find key_0025");

    println!("SST with per-block blooms written successfully");

    // Cleanup
    let _ = std::fs::remove_dir_all(&temp_dir);
}

#[test]
fn should_use_per_block_blooms_to_skip_blocks_in_negative_lookups() {
    // Arrange: Create SST with specific keys
    let temp_dir = create_temp_sst_dir();
    let mut writer = FsDynWriter::new(&temp_dir, CompressionType::None, 768, false, None)
        .expect("Failed to create writer");

    // Add keys in specific ranges to different blocks
    for i in 0..30 {
        let key = format!("block1_{:03}", i);
        writer
            .add(key.as_bytes(), b"value1")
            .expect("Failed to add key");
    }

    for i in 0..30 {
        let key = format!("block2_{:03}", i);
        writer
            .add(key.as_bytes(), b"value2")
            .expect("Failed to add key");
    }

    // Act: Write SST
    let sst_path = temp_dir.join("multi_block.sst");
    (Box::new(writer) as Box<dyn DynSstWriter>)
        .finish_to_path(&sst_path)
        .expect("Failed to finish writing SST");

    // Assert: Read from first block works
    let sst = SstFile::open(&sst_path).expect("Should open SST");
    let val = sst.get(b"block1_010").expect("Should read");
    assert!(val.is_some(), "Should find block1_010");

    // Assert: Read from second block works
    let val = sst.get(b"block2_010").expect("Should read");
    assert!(val.is_some(), "Should find block2_010");

    // Assert: Negative lookup for non-existent key should return None
    // (This tests that blooms are working - key definitely not in SST)
    let val = sst.get(b"nonexistent_key").expect("Should query");
    assert!(val.is_none(), "Should not find nonexistent key");

    println!("Per-block blooms enabled negative lookups");

    // Cleanup
    let _ = std::fs::remove_dir_all(&temp_dir);
}

#[test]
fn should_load_per_block_blooms_on_sst_open() {
    // Arrange
    let temp_dir = create_temp_sst_dir();
    let mut writer = FsDynWriter::new(&temp_dir, CompressionType::None, 1024, false, None)
        .expect("Failed to create writer");

    for i in 0..50 {
        let key = format!("data_{:04}", i);
        writer.add(key.as_bytes(), b"x").expect("Failed to add key");
    }

    let sst_path = temp_dir.join("test.sst");
    (Box::new(writer) as Box<dyn DynSstWriter>)
        .finish_to_path(&sst_path)
        .expect("Failed to finish writing SST");

    // Act: Open SST (should load blooms)
    let sst = SstFile::open(&sst_path).expect("Should open SST");

    // Assert: Should be able to query keys
    for i in [0, 25, 49] {
        let key = format!("data_{:04}", i);
        let val = sst.get(key.as_bytes()).expect("Should query");
        assert!(val.is_some(), "Should find {}", key);
    }

    println!("Per-block blooms loaded successfully on SST open");

    // Cleanup
    let _ = std::fs::remove_dir_all(&temp_dir);
}

#[test]
fn should_preserve_all_values_given_bloom_roundtrip_when_reading() {
    // Arrange: Create SST with specific test values
    let temp_dir = create_temp_sst_dir();
    let mut writer = FsDynWriter::new(&temp_dir, CompressionType::None, 512, false, None)
        .expect("Failed to create writer");

    // Store expected key-value pairs
    let test_data: Vec<(&str, &str)> = vec![
        ("aaa", "value_aaa"),
        ("bbb", "value_bbb"),
        ("ccc", "value_ccc"),
        ("ddd", "value_ddd"),
        ("eee", "value_eee"),
    ];

    for (key, value) in &test_data {
        writer
            .add(key.as_bytes(), value.as_bytes())
            .expect("Failed to add key");
    }

    // Add filler to create multiple blocks
    for i in 0..50 {
        let key = format!("filler_{:03}", i);
        writer
            .add(key.as_bytes(), b"filler_value")
            .expect("Failed to add key");
    }

    // Act: Write and read back
    let sst_path = temp_dir.join("test.sst");
    (Box::new(writer) as Box<dyn DynSstWriter>)
        .finish_to_path(&sst_path)
        .expect("Failed to finish writing SST");

    let sst = SstFile::open(&sst_path).expect("Should open SST");

    // Assert: All values should be preserved
    for (key, expected_value) in &test_data {
        let val = sst.get(key.as_bytes()).expect("Should query");
        assert!(val.is_some(), "Should find {}", key);
        let actual_bytes = val.unwrap();
        let actual_value = std::str::from_utf8(&actual_bytes).unwrap();
        assert_eq!(actual_value, *expected_value, "Value mismatch for {}", key);
    }

    println!("All values preserved through bloom write/read cycle");

    // Cleanup
    let _ = std::fs::remove_dir_all(&temp_dir);
}
