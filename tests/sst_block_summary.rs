use cntryl_midge::common::codec::CompressionType;
use cntryl_midge::sst::fs::FsDynWriter;
use cntryl_midge::sst::fs::SstFile;
use cntryl_midge::sst::DynSstWriter;
use std::path::PathBuf;

fn create_temp_sst_dir() -> PathBuf {
    let dir = std::env::temp_dir().join(format!("midge_block_summary_{}", uuid::Uuid::new_v4()));
    std::fs::create_dir_all(&dir).unwrap();
    dir
}

#[test]
fn should_persist_block_summary_in_meta_index() {
    // Arrange
    let temp_dir = create_temp_sst_dir();
    let mut writer = FsDynWriter::new(&temp_dir, CompressionType::None, 512, false, None)
        .expect("Failed to create writer");

    // Add a bunch of keys to ensure multiple blocks
    for i in 0..80 {
        let key = format!("k{:03}", i);
        writer.add(key.as_bytes(), b"v").unwrap();
    }

    // Act
    let path = temp_dir.join("summary_test.sst");
    (Box::new(writer) as Box<dyn DynSstWriter>)
        .finish_to_path(&path)
        .unwrap();
    let sst = SstFile::open(&path).unwrap();
    let metas = sst
        .persisted_block_metadata()
        .expect("Should produce block metadata");

    // Assert: each block has non-empty min_key and min_key <= max_key
    assert!(!metas.is_empty());
    for m in metas {
        assert!(m.min_key.len() > 0, "min_key must be non-empty");
        assert!(m.max_key.len() > 0, "max_key must be non-empty");
        assert!(
            m.min_key.as_ref() <= m.max_key.as_ref(),
            "min_key <= max_key"
        );
    }

    // Cleanup
    let _ = std::fs::remove_dir_all(&temp_dir);
}
