//! Filesystem WAL reader using `io::Fs` abstraction
//!
//! This reader uses the base `io::Fs` trait instead of storage abstractions directly,
//! allowing for swappable real and mock implementations in tests.
//!
//! Architectural invariants (Maintainer: DO NOT VIOLATE):
//! --------------------------------------------------
//! • `FsWalReaderIo` reads **only** from the active WAL file `wal.log`.
//! • It must treat EOF mid-record as **corruption**, not success.
//! • It must not assume the file ends cleanly.
//! • It must use the canonical format:
//!       `<u32 length prefix><u32 crc32c><encoded record bytes>`
//! • It must NOT attempt to fix, truncate, or adjust the file.
//! • It must update `current_pos` monotonically.

use crate::common::MidgeResult;
use crate::io::{Fs, FsPath};
use crate::wal::encoding;
use crate::wal::traits::{WalReader, WalReaderDyn};
use crate::wal::types::{WalPos, WalRecord};
use std::sync::Arc;

/// Filesystem-backed WAL reader using `io::Fs`.
///
/// This struct provides low-level, corruption-aware reading semantics
/// with swappable filesystem backends.
pub struct FsWalReaderIo {
    path: FsPath,
    fs: Arc<dyn Fs>,
    current_pos: WalPos,
}

impl FsWalReaderIo {
    /// Open `wal.log` in read-only mode using the provided filesystem.
    ///
    /// # Errors
    ///
    /// Returns an error if the WAL reader cannot be initialized.
    pub fn new(path_str: &str, fs: Arc<dyn Fs>) -> MidgeResult<Self> {
        Ok(Self {
            path: FsPath::new(path_str),
            fs,
            current_pos: 0,
        })
    }

    /// Open and verify the WAL file exists.
    ///
    /// # Errors
    ///
    /// Returns an error if the WAL file is missing or unreadable.
    pub fn open(path_str: &str, fs: Arc<dyn Fs>) -> MidgeResult<Self> {
        let path = FsPath::new(path_str);
        fs.metadata(&path)?;
        Ok(Self {
            path,
            fs,
            current_pos: 0,
        })
    }
}

impl WalReader for FsWalReaderIo {
    /// Read a single WAL record at an explicit offset.
    ///
    /// Returns:
    /// - Ok(Some(record)) if a valid record is found
    /// - Ok(None) if clean EOF at `pos`
    /// - Err(Corruption) if EOF occurs mid-record
    fn read_at(&mut self, pos: WalPos) -> MidgeResult<Option<WalRecord>> {
        // Open file in read-only mode
        let file = self.fs.open(
            &self.path,
            crate::io::OpenOptions {
                mode: crate::io::OpenMode::ReadOnly,
                create: false,
                create_new: false,
                truncate: false,
            },
        )?;

        // The shared cursor implements the WalReader contract: Ok(None) on a
        // clean EOF at `pos`, and an error for any torn or damaged frame.
        // This reader has no tolerance for a torn tail: its callers ask for
        // one specific record.
        let source = crate::wal::frame::FileFrames::new(&*file, &self.path);
        match crate::wal::frame::next_frame(
            &source,
            &self.path,
            pos,
            crate::wal::frame::FrameLimits::default(),
        ) {
            Ok(crate::wal::frame::FrameStep::Eof) => Ok(None),
            Ok(crate::wal::frame::FrameStep::Frame { payload, next_pos }) => {
                let record = encoding::decode(payload.as_ref())?;
                self.current_pos = next_pos;
                Ok(Some(record))
            }
            Err(error) => Err(error.into_error()),
        }
    }

    fn replay<F>(&mut self, start: WalPos, mut cb: F) -> MidgeResult<()>
    where
        F: FnMut(&WalRecord) -> MidgeResult<()>,
    {
        let mut pos = start;
        while let Some(record) = <Self as WalReader>::read_at(self, pos)? {
            cb(&record)?;
            pos = self.current_pos;
        }
        Ok(())
    }

    fn close(&mut self) -> MidgeResult<()> {
        // io::Fs doesn't require explicit close, but reset position
        self.current_pos = 0;
        Ok(())
    }
}

impl WalReaderDyn for FsWalReaderIo {
    fn read_at(&mut self, pos: WalPos) -> MidgeResult<Option<WalRecord>> {
        WalReader::read_at(self, pos)
    }

    fn replay_boxed(
        &mut self,
        start: WalPos,
        cb: &mut dyn FnMut(&WalRecord) -> MidgeResult<()>,
    ) -> MidgeResult<()> {
        let mut pos = start;
        while let Some(record) = WalReader::read_at(self, pos)? {
            cb(&record)?;
            pos = self.current_pos;
        }
        Ok(())
    }

    fn close(&mut self) -> MidgeResult<()> {
        WalReader::close(self)
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn should_create_wal_reader_io_without_requiring_file_to_exist() -> MidgeResult<()> {
        // Arrange
        // `new` is the lazy constructor used before a WAL file has been
        // created; unlike `open`, it must not eagerly check the filesystem.
        let fs = Arc::new(crate::io::MockFs::new());
        assert!(fs.metadata(&FsPath::new("wal.log")).is_err());

        // Act
        let reader = FsWalReaderIo::new("wal.log", Arc::clone(&fs) as Arc<dyn crate::io::Fs>)?;

        // Assert: construction succeeded despite the file not existing yet,
        // and it starts at position 0.
        assert_eq!(reader.current_pos, 0);
        assert!(
            FsWalReaderIo::open("wal.log", fs).is_err(),
            "open() should still reject a missing file, unlike new()"
        );
        Ok(())
    }

    #[test]
    fn should_reset_current_pos_when_closed() -> MidgeResult<()> {
        // Arrange: write one record to the backing file and read it, so the
        // reader has advanced past position 0.
        let fs = Arc::new(crate::io::MockFs::new());
        let writer = crate::wal::fs::FsWalWriterIo::new(
            "wal.log",
            Arc::clone(&fs) as Arc<dyn crate::io::Fs>,
        )?;
        let record = crate::wal::types::WalRecord::new(
            crate::wal::types::WalOpKind::Put,
            bytes::Bytes::from_static(b"key"),
            Some(bytes::Bytes::from_static(b"value")),
            1,
            1,
        );
        crate::wal::WalWriter::append_record(&writer, &record)?;
        crate::wal::WalWriter::close(&writer)?;

        let mut reader = FsWalReaderIo::new("wal.log", fs)?;
        WalReader::read_at(&mut reader, 0)?;
        assert!(
            reader.current_pos > 0,
            "read_at should have advanced current_pos"
        );

        // Act
        WalReader::close(&mut reader)?;

        // Assert
        assert_eq!(reader.current_pos, 0);
        Ok(())
    }

    #[test]
    fn should_return_none_on_eof() -> MidgeResult<()> {
        // Arrange: Create reader on newly created empty file
        let fs = Arc::new(crate::io::MockFs::new());
        crate::io::Fs::open(
            fs.as_ref(),
            &FsPath::new("wal.log"),
            crate::io::OpenOptions {
                mode: crate::io::OpenMode::ReadWrite,
                create: true,
                create_new: true,
                truncate: false,
            },
        )?;
        let mut reader = FsWalReaderIo::new("wal.log", fs)?;

        // Act: Try to read at position 0 from empty file
        let result = WalReader::read_at(&mut reader, 0);

        // Assert: Should return None for clean EOF at position 0
        match result {
            Ok(None) => Ok(()),
            Ok(Some(_)) => panic!("Expected None for empty file"),
            Err(error) => panic!("Expected None for empty file, not error: {error}"),
        }
    }
}
