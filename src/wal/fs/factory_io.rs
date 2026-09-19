//! Factory for creating `io::Fs-backed` WAL readers and writers

use crate::common::MidgeResult;
use crate::io::Fs;
use std::sync::Arc;
use std::time::Duration;

/// WAL factory that uses `io::Fs` abstraction
/// Allows using real and mock filesystem implementations for testing.
pub struct FsWalFactoryIo {
    fs: Arc<dyn Fs>,
    io_timeout: Duration,
}

impl FsWalFactoryIo {
    /// Create a new factory with a custom filesystem implementation
    pub fn new(fs: Arc<dyn Fs>) -> Self {
        Self {
            fs,
            io_timeout: crate::config::DEFAULT_STORAGE_IO_TIMEOUT,
        }
    }

    /// Configure the maximum acknowledgement wait for writers from this factory.
    #[must_use]
    pub fn with_io_timeout(mut self, timeout: Duration) -> Self {
        self.io_timeout = timeout;
        self
    }

    /// Create a new WAL writer using the `io::Fs` backend
    ///
    /// # Errors
    ///
    /// Returns an error if the writer cannot be created.
    pub fn create_writer(&self, path_str: &str) -> MidgeResult<Box<dyn crate::wal::WalWriter>> {
        let writer = super::FsWalWriterIo::new_with_timeout(
            path_str,
            Arc::clone(&self.fs),
            self.io_timeout,
        )?;
        Ok(Box::new(writer))
    }

    /// Create a new WAL reader using the `io::Fs` backend
    ///
    /// # Errors
    ///
    /// Returns an error if the reader cannot be opened.
    pub fn create_reader(&self, path_str: &str) -> MidgeResult<Box<dyn crate::wal::WalReaderDyn>> {
        let reader = super::FsWalReaderIo::open(path_str, Arc::clone(&self.fs))?;
        Ok(Box::new(reader))
    }
}

#[cfg(test)]
mod tests {

    #[test]
    fn should_sync_wal_directory_when_creating_initial_active_wal() {
        // Arrange
        let mock = std::sync::Arc::new(crate::io::MockFs::new());
        let factory = FsWalFactoryIo::new(mock.clone());

        // Act
        let _writer = factory
            .create_writer(crate::wal::ACTIVE_FILE_NAME)
            .expect("create active WAL");

        // Assert
        assert!(
            mock.sync_dir_calls()
                .iter()
                .any(|(path, durability)| path.0 == "."
                    && *durability == crate::io::Durability::Durable),
            "creating wal.log must make its directory entry durable"
        );
    }

    use super::*;

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
    fn should_create_reader() {
        // Arrange
        let fs = Arc::new(crate::io::MockFs::new());
        let factory = FsWalFactoryIo::new(fs);

        // Act
        let result = factory.create_reader("wal.log");

        // Assert
        assert!(result.is_err()); // File doesn't exist in mock
    }
}
