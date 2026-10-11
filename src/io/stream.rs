//! Sequential `std::io` views of an `Fs` file handle, so encoders written
//! against `Read`, `Write` and `Seek` reach storage only through the injected
//! filesystem.

use super::{File, FsError};
use bytes::Bytes;
use std::io::{self, Read, Seek, SeekFrom, Write};
use std::ops::Deref;

/// Bytes a writer gathers before issuing one positional write.
const WRITE_BUFFER_BYTES: usize = 64 * 1024;

/// Report a filesystem error through `std::io`, keeping the kinds callers
/// classify on (a full device stays `StorageFull`, so it becomes `NoSpace`).
impl From<FsError> for io::Error {
    fn from(error: FsError) -> Self {
        let kind = match &error {
            FsError::NotFound(_) => io::ErrorKind::NotFound,
            FsError::AlreadyExists(_) => io::ErrorKind::AlreadyExists,
            FsError::Corruption(_) => io::ErrorKind::InvalidData,
            FsError::NoSpace(_) => io::ErrorKind::StorageFull,
            FsError::Timeout(_) => io::ErrorKind::TimedOut,
            FsError::Unsupported(_) => io::ErrorKind::Unsupported,
            FsError::Io(_) | FsError::Unavailable(_) => io::ErrorKind::Other,
        };
        io::Error::new(kind, error.to_string())
    }
}

/// Positioned reader over a file whose length does not change while it is
/// read. `F` is an owned or borrowed handle.
pub(crate) struct FileReader<F> {
    file: F,
    len: u64,
    position: u64,
}

impl<F> std::fmt::Debug for FileReader<F> {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("FileReader")
            .field("len", &self.len)
            .field("position", &self.position)
            .finish_non_exhaustive()
    }
}

impl<F, T> FileReader<F>
where
    F: Deref<Target = T>,
    T: File + ?Sized,
{
    /// Read `file` from its first byte; its current length bounds the read.
    pub(crate) fn new(file: F) -> io::Result<Self> {
        let len = file.len().map_err(io::Error::from)?;
        Ok(Self::with_len(file, len))
    }

    /// Read the first `len` bytes of `file`.
    pub(crate) fn with_len(file: F, len: u64) -> Self {
        Self {
            file,
            len,
            position: 0,
        }
    }

    pub(crate) fn len(&self) -> u64 {
        self.len
    }
}

impl<F, T> Read for FileReader<F>
where
    F: Deref<Target = T>,
    T: File + ?Sized,
{
    fn read(&mut self, buffer: &mut [u8]) -> io::Result<usize> {
        let count = self
            .len
            .saturating_sub(self.position)
            .min(buffer.len() as u64);
        if count == 0 {
            return Ok(0);
        }
        let bytes = self
            .file
            .read_at(self.position, count)
            .map_err(io::Error::from)?;
        if bytes.len() as u64 != count {
            return Err(io::Error::new(
                io::ErrorKind::UnexpectedEof,
                "file shrank while it was read",
            ));
        }
        buffer[..bytes.len()].copy_from_slice(&bytes);
        self.position += count;
        Ok(bytes.len())
    }
}

impl<F> Seek for FileReader<F> {
    fn seek(&mut self, target: SeekFrom) -> io::Result<u64> {
        let position = match target {
            SeekFrom::Start(offset) => Some(offset),
            SeekFrom::Current(delta) => self.position.checked_add_signed(delta),
            SeekFrom::End(delta) => self.len.checked_add_signed(delta),
        }
        .ok_or_else(|| io::Error::new(io::ErrorKind::InvalidInput, "seek before byte 0"))?;
        self.position = position;
        Ok(position)
    }

    fn stream_position(&mut self) -> io::Result<u64> {
        Ok(self.position)
    }
}

/// Buffered positioned writer. Callers must `flush` before dropping it:
/// unflushed bytes are discarded, never written implicitly.
pub(crate) struct FileWriter {
    file: Box<dyn File>,
    /// File offset of the first buffered byte.
    position: u64,
    buffer: Vec<u8>,
}

impl FileWriter {
    pub(crate) fn new(file: Box<dyn File>) -> Self {
        Self {
            file,
            position: 0,
            buffer: Vec::with_capacity(WRITE_BUFFER_BYTES),
        }
    }

    fn write_buffer(&mut self) -> io::Result<()> {
        if self.buffer.is_empty() {
            return Ok(());
        }
        let bytes = Bytes::from(std::mem::replace(
            &mut self.buffer,
            Vec::with_capacity(WRITE_BUFFER_BYTES),
        ));
        let len = bytes.len() as u64;
        self.file
            .write_at(self.position, bytes)
            .map_err(io::Error::from)?;
        self.position += len;
        Ok(())
    }
}

impl Write for FileWriter {
    fn write(&mut self, bytes: &[u8]) -> io::Result<usize> {
        self.buffer.extend_from_slice(bytes);
        if self.buffer.len() >= WRITE_BUFFER_BYTES {
            self.write_buffer()?;
        }
        Ok(bytes.len())
    }

    fn flush(&mut self) -> io::Result<()> {
        self.write_buffer()
    }
}

impl Seek for FileWriter {
    fn seek(&mut self, target: SeekFrom) -> io::Result<u64> {
        self.write_buffer()?;
        let position = match target {
            SeekFrom::Start(offset) => Some(offset),
            SeekFrom::Current(delta) => self.position.checked_add_signed(delta),
            SeekFrom::End(_) => {
                return Err(io::Error::new(
                    io::ErrorKind::Unsupported,
                    "a positioned writer does not seek from its end",
                ))
            }
        }
        .ok_or_else(|| io::Error::new(io::ErrorKind::InvalidInput, "seek before byte 0"))?;
        self.position = position;
        Ok(position)
    }

    fn stream_position(&mut self) -> io::Result<u64> {
        Ok(self.position + self.buffer.len() as u64)
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::io::{Fs, FsPath, MockFs, OpenMode, OpenOptions};

    fn create(fs: &MockFs, path: &str) -> Box<dyn File> {
        fs.open_persistent_handle(
            &FsPath::new(path),
            OpenOptions {
                mode: OpenMode::ReadWrite,
                create: true,
                create_new: true,
                truncate: false,
            },
        )
        .expect("create file")
    }

    #[test]
    fn should_patch_header_after_streaming_body_when_writer_seeks_back() -> io::Result<()> {
        // Arrange
        let fs = MockFs::new();
        let mut writer = FileWriter::new(create(&fs, "run"));

        // Act
        writer.write_all(&[0; 4])?;
        writer.write_all(b"body")?;
        let end = writer.stream_position()?;
        writer.seek(SeekFrom::Start(0))?;
        writer.write_all(b"head")?;
        writer.flush()?;

        // Assert
        assert_eq!(end, 8);
        assert_eq!(fs.get_file("run").as_deref(), Some(&b"headbody"[..]));
        Ok(())
    }

    #[test]
    fn should_read_from_sought_offset_when_reader_moves() -> io::Result<()> {
        // Arrange
        let fs = MockFs::new();
        let mut writer = FileWriter::new(create(&fs, "run"));
        writer.write_all(b"0123456789")?;
        writer.flush()?;
        let mut reader = FileReader::new(create_existing(&fs, "run"))?;

        // Act
        reader.seek(SeekFrom::Start(6))?;
        let mut tail = Vec::new();
        reader.read_to_end(&mut tail)?;
        reader.seek(SeekFrom::End(-10))?;
        let mut head = [0; 2];
        reader.read_exact(&mut head)?;

        // Assert
        assert_eq!(tail, b"6789");
        assert_eq!(&head, b"01");
        Ok(())
    }

    #[test]
    fn should_report_full_device_as_storage_full_when_converting_error() {
        // Arrange
        let error = FsError::NoSpace("device full".into());

        // Act
        let converted = crate::common::MidgeError::from(io::Error::from(error));

        // Assert
        assert!(matches!(converted, crate::common::MidgeError::NoSpace(_)));
    }

    fn create_existing(fs: &MockFs, path: &str) -> Box<dyn File> {
        fs.open_persistent_handle(
            &FsPath::new(path),
            OpenOptions {
                mode: OpenMode::ReadOnly,
                create: false,
                create_new: false,
                truncate: false,
            },
        )
        .expect("open file")
    }
}
