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
    use crate::sst::{DynSstWriter, SstFactory, SstReader};

    #[test]
    fn should_create_writer_when_factory_initialized() -> MidgeResult<()> {
        // Arrange
        let temp_dir = std::env::temp_dir().join("midge_factory_test");
        std::fs::create_dir_all(&temp_dir)?;
        let factory = FsSstFactory::new(&temp_dir, 4096);

        // Act
        let mut writer = factory.create()?;
        writer.add(b"test_key", b"test_value")?;
        let bytes = writer.finish_bytes()?;

        // Assert
        assert!(bytes.len() > 0);

        Ok(())
    }
}
