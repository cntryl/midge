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
    /// records and which holds only its header: a missing file is
    /// corruption, never recreated, and a writer never appends behind
    /// records or a footer.
    ///
    /// # Errors
    ///
    /// Returns an error if the segment is missing, holds more than its
    /// header, or the writer cannot be created.
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
        self.writer(&name)
    }

    /// A writer over a new segment `path_str`, created with a header for a
    /// history of its own, for tests that write one segment directly.
    ///
    /// # Errors
    ///
    /// Returns an error if the segment cannot be created or opened.
    #[cfg(test)]
    pub fn create_writer(&self, path_str: &str) -> MidgeResult<Box<dyn crate::wal::WalWriter>> {
        super::writer_io::create_test_segment(self.fs.as_ref(), path_str)?;
        self.writer(path_str)
    }

    fn writer(&self, path_str: &str) -> MidgeResult<Box<dyn crate::wal::WalWriter>> {
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
        crate::wal::fs::writer_io::create_test_segment(mock.as_ref(), &name)
            .expect("create segment");
        let factory = FsWalFactoryIo::new(mock.clone());

        // Act
        let writer = factory.open_segment(4);

        // Assert
        assert!(writer.is_ok());
        assert_eq!(
            mock.get_file(&name).map(|bytes| bytes.len()),
            Some(crate::wal::format::HEADER_LEN)
        );
    }

    #[test]
    fn should_refuse_writer_on_segment_with_records() {
        // Arrange: segment 4 already holds a record past its header.
        let mock = std::sync::Arc::new(crate::io::MockFs::new());
        let name = crate::wal::segment_file_name(4);
        crate::wal::fs::writer_io::create_test_segment(mock.as_ref(), &name)
            .expect("create segment");
        let factory = FsWalFactoryIo::new(mock.clone());
        factory
            .open_segment(4)
            .expect("open the empty segment")
            .append_record(&crate::wal::WalRecord::new(
                crate::wal::WalOpKind::Put,
                bytes::Bytes::from_static(b"key"),
                Some(bytes::Bytes::from_static(b"value")),
                1,
            ))
            .expect("append");

        // Act
        let result = factory.open_segment(4);

        // Assert
        assert!(
            matches!(result, Err(crate::common::MidgeError::Corruption(_))),
            "{:?}",
            result.err()
        );
    }

    #[test]
    fn should_report_sealed_last_sequence_from_writer_tail() -> MidgeResult<()> {
        // Arrange: segment 4 receives records at sequences 5 and 9.
        let mock = std::sync::Arc::new(crate::io::MockFs::new());
        let name = crate::wal::segment_file_name(4);
        crate::wal::fs::writer_io::create_test_segment(mock.as_ref(), &name)?;
        let writer = FsWalFactoryIo::new(mock).open_segment(4)?;
        for sequence in [5, 9] {
            writer.append_record(&crate::wal::WalRecord::new(
                crate::wal::WalOpKind::Put,
                bytes::Bytes::from_static(b"key"),
                Some(bytes::Bytes::from_static(b"value")),
                sequence,
            ))?;
        }

        // Act
        let tail = writer.tail();
        let facts = writer.seal_facts(std::time::Duration::from_secs(5))?;

        // Assert
        assert_eq!((tail.record_count, tail.last_seq), (2, Some(9)));
        assert_eq!((facts.link.id, facts.link.last_seq), (4, 9));
        assert_eq!(facts.record_count, 2);
        Ok(())
    }

    #[test]
    fn should_create_writer() -> MidgeResult<()> {
        // Arrange
        let fs = Arc::new(crate::io::MockFs::new());
        let factory = FsWalFactoryIo::new(fs);

        // Act
        let writer = factory.create_writer(&crate::wal::segment_file_name(1))?;

        // Assert
        assert_eq!(writer.current_pos(), crate::wal::format::HEADER_LEN as u64);
        Ok(())
    }
}
