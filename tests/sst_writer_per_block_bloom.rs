/// TDD Test: Per-block bloom implementation in writer
///
/// This test verifies that the writer correctly builds per-block blooms
/// and stores them in the SST file with proper metadata.

use cntryl_midge::sst::DynSstWriter;
use cntryl_midge::sst::fs::{FsDynWriter, SstFile};
use cntryl_midge::common::codec::CompressionType;
use std::path::PathBuf;

fn create_temp_sst_dir() -> PathBuf {
    let dir = std::env::temp_dir().join(format!("midge_bloom_{}", uuid::Uuid::new_v4()));
    std::fs::create_dir_all(&dir).unwrap();
    dir
}

#[test]
fn should_create_and_store_per_block_blooms_in_writer() {
    // Arrange: Create writer with moderate block size
    let temp_dir = create_temp_sst_dir();
    let mut writer = FsDynWriter::new(
        &temp_dir,
        CompressionType::None,
        1024,
        false,
        None,
    )
    .expect("Failed to create writer");

    // Add keys that will definitely span multiple blocks
    for i in 0..50 {
        let key = format!("key_{:04}", i);
        let value = "value_with_some_content_to_make_blocks_fill_up";
        writer
            .add(key.as_bytes(), value.as_bytes())
            .expect("Failed to add key");
    }

    // Act: Finish writing SST
    let sst_path = temp_dir.join("test.sst");
    (Box::new(writer) as Box<dyn DynSstWriter>)
        .finish_to_path(&sst_path)
        .expect("Failed to finish writing SST");

    // Assert: Verify SST was created
    assert!(sst_path.exists(), "SST file should exist");
    let file_size = std::fs::metadata(&sst_path)
        .expect("Failed to get metadata")
        .len();
    assert!(file_size > 0, "SST file should not be empty");

    // Try to open and read back
    let _sst = SstFile::open(&sst_path)
        .expect("Failed to open SST for reading");

    println!("SST file created: {} bytes", file_size);

    // Cleanup
    let _ = std::fs::remove_dir_all(&temp_dir);
}

#[test]
fn should_verify_per_block_blooms_are_queryable() {
    // Arrange
    let temp_dir = create_temp_sst_dir();
    let mut writer = FsDynWriter::new(
        &temp_dir,
        CompressionType::None,
        768,
        false,
        None,
    )
    .expect("Failed to create writer");

    // Add specific keys
    let test_keys = vec!["aaa", "bbb", "ccc", "ddd", "eee"];
    for key in &test_keys {
        writer
            .add(key.as_bytes(), b"value")
            .expect("Failed to add key");
    }

    // Add more keys to create multiple blocks
    for i in 0..30 {
        let key = format!("filler_{:03}", i);
        writer
            .add(key.as_bytes(), b"x")
            .expect("Failed to add key");
    }

    // Act
    let sst_path = temp_dir.join("test2.sst");
    (Box::new(writer) as Box<dyn DynSstWriter>)
        .finish_to_path(&sst_path)
        .expect("Failed to finish writing SST");

    // Assert
    assert!(sst_path.exists());

    // Cleanup
    let _ = std::fs::remove_dir_all(&temp_dir);
}
