//! Factory for creating io::Fs-backed SST readers and writers

use crate::common::MidgeResult;
use crate::sst::traits::{DynSstWriter, SstFactory};
use std::path::Path;
use std::sync::Arc;

use crate::io::Fs;

/// SST factory that uses io::Fs abstraction
/// Allows using different filesystem implementations (Real, Mock, Chaos) for testing
pub struct FsSstFactoryIo {
    fs: Arc<dyn Fs>,
    block_size: usize,
}

impl FsSstFactoryIo {
    /// Create a new factory with a custom filesystem implementation
    pub fn new(fs: Arc<dyn Fs>, block_size: usize) -> Self {
        Self { fs, block_size }
    }

    /// Create with custom block size
    pub fn with_block_size(mut self, block_size: usize) -> Self {
        self.block_size = block_size;
        self
    }

    /// Open an SST file using the io::Fs backend
    pub fn open(&self, path: &Path) -> MidgeResult<Box<dyn crate::sst::SstReader>> {
        let path_str = path.to_str().unwrap_or("").to_string();
        let start = std::time::Instant::now();
        let reader = super::SstFileIo::open(&path_str, Arc::clone(&self.fs))?;
        let elapsed = start.elapsed();
        // Try to gather file size for diagnostics (best-effort)
        let size = self
            .fs
            .metadata(&crate::io::FsPath::new(path_str.as_str()))
            .ok()
            .map(|m| m.len)
            .unwrap_or(0);
        tracing::info!(path = ?path, size_bytes = size, open_ms = elapsed.as_secs_f64() * 1000.0, "sst reader opened");
        Ok(Box::new(reader))
    }
}

/// Simple in-memory SST writer for now
/// Used when the factory needs to create a writer but SST writing is still in progress
struct InMemorySstWriter {
    entries: Vec<(Vec<u8>, Vec<u8>)>,
}

impl InMemorySstWriter {
    fn new() -> Self {
        Self {
            entries: Vec::new(),
        }
    }
}

impl DynSstWriter for InMemorySstWriter {
    fn add(&mut self, key: &[u8], value: &[u8]) -> MidgeResult<()> {
        self.entries.push((key.to_vec(), value.to_vec()));
        Ok(())
    }

    fn finish_bytes(self: Box<Self>) -> MidgeResult<Vec<u8>> {
        // Simple serialization: concatenate all entries
        let mut result = Vec::new();
        for (k, v) in self.entries {
            result.extend_from_slice(&[k.len() as u8]);
            result.extend_from_slice(&k);
            result.extend_from_slice(&[v.len() as u8]);
            result.extend_from_slice(&v);
        }
        Ok(result)
    }
}

impl SstFactory for FsSstFactoryIo {
    /// Create a new SST writer
    fn create(&self) -> MidgeResult<Box<dyn DynSstWriter>> {
        // Use in-memory writer for now - SST writing is in progress
        Ok(Box::new(InMemorySstWriter::new()))
    }

    /// Open an existing SST file
    fn open(&self, path: &Path) -> MidgeResult<Box<dyn crate::sst::SstReader>> {
        FsSstFactoryIo::open(self, path)
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn should_create_factory_with_mock_fs() {
        // Arrange
        let fs = Arc::new(crate::io::MockFs::new());

        // Act
        let factory = FsSstFactoryIo::new(fs, 4096);

        // Assert
        assert_eq!(factory.block_size, 4096);
    }

    #[test]
    fn should_create_factory_with_real_fs() -> MidgeResult<()> {
        // Arrange
        let temp_dir = tempfile::tempdir()?;
        let fs = Arc::new(crate::io::RealFs::new(temp_dir.path())?);

        // Act
        let factory = FsSstFactoryIo::new(fs, 4096);

        // Assert
        assert_eq!(factory.block_size, 4096);
        Ok(())
    }

    #[test]
    fn should_support_method_chaining() {
        // Arrange
        let fs = Arc::new(crate::io::MockFs::new());

        // Act
        let factory = FsSstFactoryIo::new(fs, 4096).with_block_size(8192);

        // Assert
        assert_eq!(factory.block_size, 8192);
    }
}
