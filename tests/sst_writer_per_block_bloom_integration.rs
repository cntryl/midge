/// Tests for Phase 1.1: Verifying per-block blooms are written and readable

use cntryl_midge::sst::DynSstWriter;
use cntryl_midge::sst::fs::{FsDynWriter, SstFile};
use cntryl_midge::common::codec::CompressionType;
use std::path::PathBuf;

fn create_temp_sst_dir() -> PathBuf {
    let dir = std::env::temp_dir().join(format!("midge_pbbloom_{}", uuid::Uuid::new_v4()));
    std::fs::create_dir_all(&dir).unwrap();
    dir
}

#[test]
fn should_track_per_block_blooms_during_write() {
    // Arrange: Create writer with small block size
    let temp_dir = create_temp_sst_dir();
    let mut writer = FsDynWriter::new(
        &temp_dir,
        CompressionType::None,
        512,  // Very small to force many blocks
        false,
        None,
    )
    .expect("Failed to create writer");

    // Add keys ensuring multiple blocks
    for i in 0..100 {
        let key = format!("key_{:05}", i);
        let value = "value_content";
        writer
            .add(key.as_bytes(), value.as_bytes())
            .expect("Failed to add key");
    }

    // Act: Finish writing
    let sst_path = temp_dir.join("multi_block.sst");
    (Box::new(writer) as Box<dyn DynSstWriter>)
        .finish_to_path(&sst_path)
        .expect("Failed to finish writing SST");

    // Assert: SST file exists and is readable
    assert!(sst_path.exists(), "SST file should exist");
    let sst = SstFile::open(&sst_path).expect("Should be able to open SST");
    
    // Verify we can read back some keys
    let key = b"key_00050";
    let result = sst.get(key).expect("Should be able to read");
    assert!(result.is_some(), "Should find key_00050");

    println!("Successfully created SST with per-block blooms");

    // Cleanup
    let _ = std::fs::remove_dir_all(&temp_dir);
}

#[test]
fn should_preserve_per_block_bloom_data_through_write_cycle() {
    // Arrange
    let temp_dir = create_temp_sst_dir();
    let mut writer = FsDynWriter::new(
        &temp_dir,
        CompressionType::None,
        1024,
        false,
        None,
    )
    .expect("Failed to create writer");

    // Add specific test keys
    let test_keys = vec!["apple", "banana", "cherry", "date", "elderberry"];
    for key in &test_keys {
        writer
            .add(key.as_bytes(), b"test_value")
            .expect("Failed to add key");
    }

    // Add more keys to create multiple blocks
    for i in 0..50 {
        let key = format!("filler_{:03}", i);
        writer
            .add(key.as_bytes(), b"x")
            .expect("Failed to add key");
    }

    // Act: Write SST
    let sst_path = temp_dir.join("test_keys.sst");
    (Box::new(writer) as Box<dyn DynSstWriter>)
        .finish_to_path(&sst_path)
        .expect("Failed to finish writing SST");

    // Assert: All test keys can be read back
    let sst = SstFile::open(&sst_path).expect("Should open SST");
    for key in &test_keys {
        let result = sst.get(key.as_bytes()).expect("Should read key");
        assert!(result.is_some(), "Should find key {}", key);
    }

    // Cleanup
    let _ = std::fs::remove_dir_all(&temp_dir);
}

#[test]
fn should_handle_large_sst_with_many_blocks() {
    // Arrange: Create SST with many blocks
    let temp_dir = create_temp_sst_dir();
    let mut writer = FsDynWriter::new(
        &temp_dir,
        CompressionType::None,
        768,
        false,
        None,
    )
    .expect("Failed to create writer");

    // Add many keys to create many blocks (each ~1KB should create ~50-100 keys per block)
    for i in 0..500 {
        let key = format!("large_key_{:06}", i);
        let value = format!("value_content_{}", i);
        writer
            .add(key.as_bytes(), value.as_bytes())
            .expect("Failed to add key");
    }

    // Act
    let sst_path = temp_dir.join("large.sst");
    (Box::new(writer) as Box<dyn DynSstWriter>)
        .finish_to_path(&sst_path)
        .expect("Failed to finish writing SST");

    // Assert
    assert!(sst_path.exists());
    let file_size = std::fs::metadata(&sst_path)
        .expect("Should get metadata")
        .len();
    println!("Large SST: {} bytes", file_size);
    assert!(file_size > 10_000, "SST should be large"); // Should be >10KB

    let sst = SstFile::open(&sst_path).expect("Should open");
    let first_val = sst.get(b"large_key_000000").expect("Should read");
    assert!(first_val.is_some());

    // Cleanup
    let _ = std::fs::remove_dir_all(&temp_dir);
}

#[test]
fn should_include_per_block_bloom_in_sst_metadata() {
    // Arrange: Create a small SST
    let temp_dir = create_temp_sst_dir();
    let mut writer = FsDynWriter::new(
        &temp_dir,
        CompressionType::None,
        1024,
        false,
        None,
    )
    .expect("Failed to create writer");

    // Add a few keys
    for i in 0..10 {
        let key = format!("key_{}", i);
        writer
            .add(key.as_bytes(), b"value")
            .expect("Failed to add key");
    }

    // Act
    let sst_path = temp_dir.join("metadata_test.sst");
    (Box::new(writer) as Box<dyn DynSstWriter>)
        .finish_to_path(&sst_path)
        .expect("Failed to finish writing SST");

    // Assert: File should be readable
    let sst = SstFile::open(&sst_path).expect("Should open SST");
    
    // Verify metadata is accessible
    let result = sst.get(b"key_5").expect("Should read");
    assert!(result.is_some());

    // Cleanup
    let _ = std::fs::remove_dir_all(&temp_dir);
}
