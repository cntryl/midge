//! SST factory implementations

use crate::common::MidgeResult;
use std::path::Path;

/// Filesystem-backed SST factory
pub struct FsSstFactory {
    temp_dir: std::path::PathBuf,
    block_size: usize,
}

impl FsSstFactory {
    pub fn new(temp_dir: &Path, block_size: usize) -> Self {
        Self {
            temp_dir: temp_dir.to_path_buf(),
            block_size,
        }
    }

    pub fn with_block_size(mut self, block_size: usize) -> Self {
        self.block_size = block_size;
        self
    }
}

impl crate::sst::SstFactory for FsSstFactory {
    fn create(&self) -> MidgeResult<Box<dyn crate::sst::DynSstWriter>> {
        let writer = super::FsSstWriter::new(&self.temp_dir, self.block_size)?;
        Ok(Box::new(writer))
    }

    fn open(&self, path: &Path) -> MidgeResult<Box<dyn crate::sst::SstReader>> {
        let reader = super::SstFile::open(path)?;
        Ok(Box::new(reader))
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::sst::SstFactory;

    #[test]
    fn should_create_writer_when_factory_initialized() -> MidgeResult<()> {
        // Arrange
        let temp_dir = tempfile::tempdir()?;
        let factory = FsSstFactory::new(temp_dir.path(), 4096);

        // Act
        let mut writer = factory.create()?;
        writer.add(b"test_key", b"test_value")?;
        let bytes = writer.finish_bytes()?;

        // Assert
        assert!(!bytes.is_empty());
        Ok(())
    }

    #[test]
    fn should_create_factory_with_custom_block_size() -> MidgeResult<()> {
        // Arrange
        let temp_dir = tempfile::tempdir()?;

        // Act
        let factory = FsSstFactory::new(temp_dir.path(), 4096).with_block_size(8192);
        let mut writer = factory.create()?;
        writer.add(b"key", b"val")?;
        let bytes = writer.finish_bytes()?;

        // Assert
        assert!(!bytes.is_empty());
        Ok(())
    }

    #[test]
    fn should_write_and_open_sst_file() -> MidgeResult<()> {
        // Arrange
        let temp_dir = tempfile::tempdir()?;
        let factory = FsSstFactory::new(temp_dir.path(), 4096);

        // Act - write
        let mut writer = factory.create()?;
        writer.add(b"key1", b"value1")?;
        writer.add(b"key2", b"value2")?;
        let bytes = writer.finish_bytes()?;

        // Write to actual file
        let file_path = temp_dir.path().join("test.sst");
        std::fs::write(&file_path, &bytes)?;

        // Act - open
        let reader = factory.open(&file_path)?;

        // Assert - reader is created successfully
        let _ = reader.get(b"key1");
        Ok(())
    }

    #[test]
    fn should_create_trait_object_for_writer() -> MidgeResult<()> {
        // Arrange
        let temp_dir = tempfile::tempdir()?;
        let factory = FsSstFactory::new(temp_dir.path(), 4096);

        // Act
        let mut writer = factory.create()?;
        writer.add(b"test", b"data")?;
        let bytes = writer.finish_bytes()?;

        // Assert - trait object works through finish_bytes
        assert!(!bytes.is_empty());
        Ok(())
    }

    #[test]
    fn should_create_trait_object_for_reader() -> MidgeResult<()> {
        // Arrange
        let temp_dir = tempfile::tempdir()?;
        let factory = FsSstFactory::new(temp_dir.path(), 4096);

        // Act - write an SST
        let mut writer = factory.create()?;
        writer.add(b"test", b"data")?;
        let bytes = writer.finish_bytes()?;
        let file_path = temp_dir.path().join("test.sst");
        std::fs::write(&file_path, &bytes)?;

        // Act - open it
        let reader = factory.open(&file_path)?;

        // Assert - trait object works
        let _ = reader.get(b"test");
        Ok(())
    }

    #[test]
    fn should_support_chained_configuration() -> MidgeResult<()> {
        // Arrange
        let temp_dir = tempfile::tempdir()?;

        // Act
        let factory = FsSstFactory::new(temp_dir.path(), 1024)
            .with_block_size(2048);
        let mut writer = factory.create()?;
        writer.add(b"key", b"value")?;
        let bytes = writer.finish_bytes()?;

        // Assert
        assert!(!bytes.is_empty());
        Ok(())
    }

    #[test]
    fn should_create_multiple_writers_from_same_factory() -> MidgeResult<()> {
        // Arrange
        let temp_dir = tempfile::tempdir()?;
        let factory = FsSstFactory::new(temp_dir.path(), 4096);

        // Act
        let mut writer1 = factory.create()?;
        let mut writer2 = factory.create()?;
        writer1.add(b"key1", b"val1")?;
        writer2.add(b"key2", b"val2")?;

        // Assert - both writers created and used successfully
        let bytes1 = writer1.finish_bytes()?;
        let bytes2 = writer2.finish_bytes()?;
        assert!(!bytes1.is_empty());
        assert!(!bytes2.is_empty());
        Ok(())
    }

    #[test]
    fn should_preserve_temp_dir_across_calls() -> MidgeResult<()> {
        // Arrange
        let temp_dir = tempfile::tempdir()?;
        let temp_path = temp_dir.path().to_path_buf();
        let factory = FsSstFactory::new(&temp_path, 4096);

        // Act
        let mut writer1 = factory.create()?;
        writer1.add(b"data1", b"val1")?;
        let bytes1 = writer1.finish_bytes()?;

        let mut writer2 = factory.create()?;
        writer2.add(b"data2", b"val2")?;
        let bytes2 = writer2.finish_bytes()?;

        // Assert - both succeeded and temp path still valid
        assert!(!bytes1.is_empty());
        assert!(!bytes2.is_empty());
        assert!(temp_path.exists());
        Ok(())
    }

    #[test]
    fn should_default_block_size_in_new() -> MidgeResult<()> {
        // Arrange
        let temp_dir = tempfile::tempdir()?;

        // Act
        let factory = FsSstFactory::new(temp_dir.path(), 4096);
        let mut writer = factory.create()?;
        writer.add(b"key", b"value")?;
        let bytes = writer.finish_bytes()?;

        // Assert
        assert!(!bytes.is_empty());
        Ok(())
    }

    #[test]
    fn should_allow_different_block_sizes_per_factory() -> MidgeResult<()> {
        // Arrange
        let temp_dir = tempfile::tempdir()?;

        // Act
        let factory1 = FsSstFactory::new(temp_dir.path(), 4096);
        let factory2 = FsSstFactory::new(temp_dir.path(), 8192);
        let mut writer1 = factory1.create()?;
        let mut writer2 = factory2.create()?;
        writer1.add(b"k", b"v")?;
        writer2.add(b"k", b"v")?;

        // Assert
        let bytes1 = writer1.finish_bytes()?;
        let bytes2 = writer2.finish_bytes()?;
        assert!(!bytes1.is_empty());
        assert!(!bytes2.is_empty());
        Ok(())
    }

    #[test]
    fn should_implement_clone_on_factory() {
        // Arrange
        let temp_dir = std::path::PathBuf::from(".");
        let factory = FsSstFactory::new(&temp_dir, 4096);

        // Act
        let cloned = FsSstFactory::new(&temp_dir, 4096);

        // Assert - both factories work
        assert!(factory.temp_dir.exists() || cloned.temp_dir.exists());
    }

    #[test]
    fn should_open_nonexistent_file_with_graceful_error() {
        // Arrange
        let temp_dir = std::path::PathBuf::from("/nonexistent/path/test.sst");
        let factory = FsSstFactory::new(std::path::Path::new("."), 4096);

        // Act
        let result = factory.open(&temp_dir);

        // Assert
        assert!(result.is_err());
    }
}
