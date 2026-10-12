//! Factory for creating `io::Fs-backed` WAL readers and writers

use crate::common::MidgeResult;
use crate::io::Fs;
use std::sync::Arc;
use std::time::Duration;

/// WAL factory that uses `io::Fs` abstraction
/// Allows using real and mock filesystem implementations for testing.
pub struct FsWalFactoryIo {
    fs: Arc<dyn Fs>,
    write_authority: Option<crate::wal::traits::WriteAuthority>,
    io_timeout: Duration,
    counters: crate::telemetry::CounterSink,
}

impl FsWalFactoryIo {
    /// Create a new factory with a custom filesystem implementation
    pub fn new(fs: Arc<dyn Fs>) -> Self {
        Self {
            fs,
            write_authority: None,
            io_timeout: crate::config::DEFAULT_STORAGE_IO_TIMEOUT,
            counters: crate::telemetry::CounterSink::default(),
        }
    }

    /// Configure the maximum acknowledgement wait for writers from this factory.
    #[must_use]
    pub fn with_io_timeout(mut self, timeout: Duration) -> Self {
        self.io_timeout = timeout;
        self
    }

    /// Record writer activity into an engine's counters.
    #[must_use]
    pub(crate) fn with_counters(mut self, counters: crate::telemetry::CounterSink) -> Self {
        self.counters = counters;
        self
    }

    pub(crate) fn with_write_authority(
        mut self,
        authority: Option<crate::wal::traits::WriteAuthority>,
    ) -> Self {
        self.write_authority = authority;
        self
    }

    /// A writer appending to segment `id`, which the segment ledger already
    /// records: a missing file is corruption, never recreated empty.
    ///
    /// # Errors
    ///
    /// Returns an error if the segment is missing or the writer cannot be
    /// created.
    pub(crate) fn open_segment(
        &self,
        id: crate::wal::ledger::SegmentId,
    ) -> MidgeResult<Box<dyn crate::wal::WalWriter>> {
        let name = crate::wal::segment_file_name(id);
        if !self
            .fs
            .exists(&crate::io::FsPath::new(&name))
            .map_err(crate::io::FsError::into_midge)?
        {
            return Err(crate::common::MidgeError::Corruption(format!(
                "WAL segment {name} is in the WAL ledger but missing"
            )));
        }
        self.create_writer(&name)
    }

    /// Create a new WAL writer using the `io::Fs` backend
    ///
    /// # Errors
    ///
    /// Returns an error if the writer cannot be created.
    pub fn create_writer(&self, path_str: &str) -> MidgeResult<Box<dyn crate::wal::WalWriter>> {
        let writer = super::FsWalWriterIo::new_with_authority(
            path_str,
            Arc::clone(&self.fs),
            self.io_timeout,
            self.counters.clone(),
            self.write_authority.clone(),
        )?;
        Ok(Box::new(writer))
    }
}

#[cfg(test)]
mod tests {

    #[test]
    fn should_sync_wal_directory_when_creating_a_wal_segment_writer() {
        // Arrange
        let mock = std::sync::Arc::new(crate::io::MockFs::new());
        let factory = FsWalFactoryIo::new(mock.clone());

        // Act
        let _writer = factory
            .create_writer(&crate::wal::segment_file_name(1))
            .expect("create WAL segment writer");

        // Assert
        assert!(
            mock.sync_dir_calls()
                .iter()
                .any(|(path, durability)| path.0 == "."
                    && *durability == crate::io::Durability::Durable),
            "creating a WAL segment must make its directory entry durable"
        );
    }

    use super::*;

    #[test]
    fn should_refuse_writer_when_ledger_segment_is_missing() {
        // Arrange: the ledger names segment 4, but no file holds it.
        let factory = FsWalFactoryIo::new(std::sync::Arc::new(crate::io::MockFs::new()));

        // Act
        let result = factory.open_segment(4);

        // Assert: a missing member is lost history, never recreated empty.
        assert!(matches!(
            result,
            Err(crate::common::MidgeError::Corruption(_))
        ));
    }

    #[test]
    fn should_append_to_existing_ledger_segment_when_opening_it() {
        // Arrange
        let mock = std::sync::Arc::new(crate::io::MockFs::new());
        let name = crate::wal::segment_file_name(4);
        drop(
            mock.open(
                &crate::io::FsPath::new(&name),
                crate::io::OpenOptions {
                    mode: crate::io::OpenMode::ReadWrite,
                    create: true,
                    create_new: true,
                    truncate: false,
                },
            )
            .expect("create segment"),
        );
        let factory = FsWalFactoryIo::new(mock.clone());

        // Act
        let writer = factory.open_segment(4);

        // Assert
        assert!(writer.is_ok());
        assert!(mock.get_file(&name).is_some());
    }

    #[test]
    fn should_create_writer() -> MidgeResult<()> {
        // Arrange
        let fs = Arc::new(crate::io::MockFs::new());
        let factory = FsWalFactoryIo::new(fs);

        // Act
        let writer = factory.create_writer(&crate::wal::segment_file_name(1))?;

        // Assert
        assert_eq!(writer.current_pos(), 0);
        Ok(())
    }
}
