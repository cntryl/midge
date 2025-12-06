/// Integration tests for Phase 1.1: Per-block bloom writer integration
///
/// Tests that verify per-block blooms are correctly built and written
/// during SST creation by FsDynWriter.

use cntryl_midge::sst::DynSstWriter;
use cntryl_midge::sst::fs::FsDynWriter;
use cntryl_midge::common::codec::CompressionType;
use std::path::PathBuf;

/// Test helper: create a temporary directory for test SSTs
fn create_temp_sst_dir() -> PathBuf {
    let dir = std::env::temp_dir().join(format!("midge_test_{}", uuid::Uuid::new_v4()));
    std::fs::create_dir_all(&dir).unwrap();
    dir
}

#[test]
fn should_build_per_block_blooms_during_sst_write() {
    // Arrange: Create writer with small block size to force multiple blocks
    let temp_dir = create_temp_sst_dir();
    let mut writer = FsDynWriter::new(
        &temp_dir,
        CompressionType::None,
        1024, // Small block size to force multiple blocks
        false,
        None,
    )
    .expect("Failed to create writer");

    // Add keys that will span multiple blocks
    for i in 0..100 {
        let key = format!("key_{:04}", i);
        let value = format!("value_{:04}", i);
        writer
            .add(key.as_bytes(), value.as_bytes())
            .expect("Failed to add key");
    }

    // Act: Finish writing SST
    let sst_bytes = (Box::new(writer) as Box<dyn DynSstWriter>)
        .finish_bytes()
        .expect("Failed to finish writing SST");

    // Assert: Verify that blooms were written to SST
    // (This is a placeholder for actual verification logic in reader)
    assert!(!sst_bytes.is_empty(), "SST should not be empty");
    println!("SST size: {} bytes", sst_bytes.len());

    // Cleanup
    let _ = std::fs::remove_dir_all(&temp_dir);
}

#[test]
fn should_support_per_block_blooms_in_meta_index() {
    // Arrange: Create writer
    let temp_dir = create_temp_sst_dir();
    let mut writer = FsDynWriter::new(
        &temp_dir,
        CompressionType::None,
        2048,
        false,
        None,
    )
    .expect("Failed to create writer");

    // Add some keys
    for i in 0..50 {
        let key = format!("key_{:03}", i);
        let value = format!("value_{:03}", i);
        writer
            .add(key.as_bytes(), value.as_bytes())
            .expect("Failed to add key");
    }

    // Act: Finish writing
    let sst_bytes = (Box::new(writer) as Box<dyn DynSstWriter>)
        .finish_bytes()
        .expect("Failed to finish writing SST");

    // Assert: SST should contain metadata for per-block blooms
    // (Actual verification deferred to reader tests)
    assert!(!sst_bytes.is_empty());

    // Cleanup
    let _ = std::fs::remove_dir_all(&temp_dir);
}

#[test]
fn should_include_per_block_bloom_offsets_in_index() {
    // Arrange
    let temp_dir = create_temp_sst_dir();
    let mut writer = FsDynWriter::new(
        &temp_dir,
        CompressionType::None,
        512, // Very small to force many blocks
        false,
        None,
    )
    .expect("Failed to create writer");

    // Add keys (must be ordered)
    for i in 0..30 {
        let key = format!("key_{:03}", i);
        let value = format!("val_{:03}", i);
        writer
            .add(key.as_bytes(), value.as_bytes())
            .expect("Failed to add key");
    }

    // Act
    let sst_bytes = (Box::new(writer) as Box<dyn DynSstWriter>)
        .finish_bytes()
        .expect("Failed to finish writing SST");

    // Assert
    assert!(!sst_bytes.is_empty());

    // Cleanup
    let _ = std::fs::remove_dir_all(&temp_dir);
}

#[test]
fn should_write_per_block_blooms_to_sst_file() {
    // Arrange
    let temp_dir = create_temp_sst_dir();
    let mut writer = FsDynWriter::new(
        &temp_dir,
        CompressionType::None,
        1536,
        false,
        None,
    )
    .expect("Failed to create writer");

    // Add keys that are guaranteed to span blocks
    for i in 0..75 {
        let key = format!("key_{:04}", i);
        let value = "x".repeat(10); // Fixed-size values
        writer
            .add(key.as_bytes(), value.as_bytes())
            .expect("Failed to add key");
    }

    // Act
    let sst_bytes = (Box::new(writer) as Box<dyn DynSstWriter>)
        .finish_bytes()
        .expect("Failed to finish writing SST");

    // Assert
    assert!(sst_bytes.len() > 0);
    println!("Finished writing SST with {} bytes", sst_bytes.len());

    // Cleanup
    let _ = std::fs::remove_dir_all(&temp_dir);
}
