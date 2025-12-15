//! Factory for creating io::Fs-backed WAL readers and writers

use crate::common::MidgeResult;
use crate::io::Fs;
use std::sync::Arc;

/// WAL factory that uses io::Fs abstraction
/// Allows using different filesystem implementations (Real, Mock, Chaos) for testing
pub struct FsWalFactoryIo {
    fs: Arc<dyn Fs>,
}

impl FsWalFactoryIo {
    /// Create a new factory with a custom filesystem implementation
    pub fn new(fs: Arc<dyn Fs>) -> Self {
        Self { fs }
    }

    /// Create a new WAL writer using the io::Fs backend
    pub fn create_writer(&self, path_str: &str) -> MidgeResult<Box<dyn crate::wal::WalWriter>> {
        let writer = super::FsWalWriterIo::new(path_str, Arc::clone(&self.fs))?;
        Ok(Box::new(writer))
    }

    /// Create a new WAL reader using the io::Fs backend
    pub fn create_reader(&self, path_str: &str) -> MidgeResult<Box<dyn crate::wal::WalReaderDyn>> {
        let reader = super::FsWalReaderIo::open(path_str, Arc::clone(&self.fs))?;
        Ok(Box::new(reader))
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
        let factory = FsWalFactoryIo::new(fs);

        // Assert
        assert!(factory.fs.metadata(&crate::io::FsPath::new("test")).is_err());
    }

    #[test]
    fn should_create_factory_with_real_fs() -> MidgeResult<()> {
        // Arrange
        let temp_dir = tempfile::tempdir()?;
        let fs = Arc::new(crate::io::RealFs::new(temp_dir.path())?);

        // Act
        let factory = FsWalFactoryIo::new(fs);

        // Assert
        let result = factory.fs.metadata(&crate::io::FsPath::new("wal.log"));
        assert!(result.is_err()); // File doesn't exist yet
        Ok(())
    }

    #[test]
    fn should_create_writer() -> MidgeResult<()> {
        // Arrange
        let fs = Arc::new(crate::io::MockFs::new());
        let factory = FsWalFactoryIo::new(fs);

        // Act
        let writer = factory.create_writer("wal.log")?;

        // Assert
        assert_eq!(writer.current_pos(), 0);
        Ok(())
    }

    #[test]
    fn should_create_reader() -> MidgeResult<()> {
        // Arrange
        let fs = Arc::new(crate::io::MockFs::new());
        let factory = FsWalFactoryIo::new(fs);

        // Act & Assert - reader creation may fail if file doesn't exist, which is expected
        let result = factory.create_reader("wal.log");
        assert!(result.is_err()); // File doesn't exist in mock
        Ok(())
    }
}
