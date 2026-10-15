//! Sequential `std::io` views of an `Fs` file handle, so encoders written
//! against `Read`, `Write` and `Seek` reach storage only through the injected
//! filesystem.

use super::{File, FsError};
use bytes::Bytes;
use std::io::{self, Read, Seek, SeekFrom, Write};
use std::ops::Deref;

/// Largest positional read or write one call issues, so a large frame never
/// needs a second whole-frame buffer beside the caller's.
const CHUNK_BYTES: usize = 64 * 1024;

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
        let len = file.len().map_err(FsError::into_io)?;
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
            .min(buffer.len() as u64)
            .min(CHUNK_BYTES as u64);
        if count == 0 {
            return Ok(0);
        }
        let bytes = self
            .file
            .read_at(self.position, count)
            .map_err(FsError::into_io)?;
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
            buffer: Vec::with_capacity(CHUNK_BYTES),
        }
    }

    fn write_buffer(&mut self) -> io::Result<()> {
        if self.buffer.is_empty() {
            return Ok(());
        }
        let bytes = Bytes::from(std::mem::replace(
            &mut self.buffer,
            Vec::with_capacity(CHUNK_BYTES),
        ));
        let len = bytes.len() as u64;
        self.file
            .write_at(self.position, bytes)
            .map_err(FsError::into_io)?;
        self.position += len;
        Ok(())
    }
}

impl Write for FileWriter {
    fn write(&mut self, bytes: &[u8]) -> io::Result<usize> {
        // Accept at most one chunk's room per call; `write_all` loops.
        let accepted = bytes.len().min(CHUNK_BYTES - self.buffer.len());
        self.buffer.extend_from_slice(&bytes[..accepted]);
        if self.buffer.len() == CHUNK_BYTES {
            self.write_buffer()?;
        }
        Ok(accepted)
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
    use crate::common::MidgeError;
    use crate::io::{Durability, FsPath, FsResult, MockFs, OpenMode, OpenOptions};

    fn open(fs: &MockFs, path: &str, create: bool) -> Box<dyn File> {
        crate::io::open_persistent_handle(
            fs,
            &FsPath::new(path),
            OpenOptions {
                mode: if create {
                    OpenMode::ReadWrite
                } else {
                    OpenMode::ReadOnly
                },
                create: false,
                create_new: create,
                truncate: false,
            },
        )
        .expect("open file")
    }

    /// Fails the test if any single positional call exceeds one chunk.
    struct ChunkBoundFile(Box<dyn File>);

    impl crate::io::ReadFile for ChunkBoundFile {
        fn read_at(&self, offset: u64, len: u64) -> FsResult<Bytes> {
            assert!(len <= CHUNK_BYTES as u64, "read of {len} bytes");
            self.0.read_at(offset, len)
        }

        fn len(&self) -> FsResult<u64> {
            self.0.len()
        }
    }

    impl File for ChunkBoundFile {
        fn write_at(&mut self, offset: u64, data: Bytes) -> FsResult<()> {
            assert!(data.len() <= CHUNK_BYTES, "write of {} bytes", data.len());
            self.0.write_at(offset, data)
        }

        fn append(&mut self, data: Bytes) -> FsResult<u64> {
            self.0.append(data)
        }

        fn sync(&mut self, durability: Durability) -> FsResult<()> {
            self.0.sync(durability)
        }
    }

    #[test]
    fn should_patch_header_after_streaming_body_when_writer_seeks_back() -> io::Result<()> {
        // Arrange
        let fs = MockFs::new();
        let mut writer = FileWriter::new(open(&fs, "run", true));

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
    fn should_bound_every_positional_call_when_frame_spans_many_chunks() -> io::Result<()> {
        // Arrange: one frame several chunks long, as a large spilled value is.
        let fs = MockFs::new();
        let frame: Vec<u8> = (0..=u8::MAX).cycle().take(3 * CHUNK_BYTES + 17).collect();
        let mut writer = FileWriter::new(Box::new(ChunkBoundFile(open(&fs, "run", true))));

        // Act
        writer.write_all(&[0; 8])?;
        writer.write_all(&frame)?;
        writer.seek(SeekFrom::Start(0))?;
        writer.write_all(b"patched!")?;
        writer.flush()?;
        let mut reader = FileReader::new(ChunkBoundFile(open(&fs, "run", false)).into_box())?;
        let mut read_back = vec![0; 8 + frame.len()];
        reader.read_exact(&mut read_back)?;

        // Assert
        assert_eq!(&read_back[..8], b"patched!");
        assert_eq!(&read_back[8..], frame.as_slice());
        Ok(())
    }

    impl ChunkBoundFile {
        fn into_box(self) -> Box<dyn File> {
            Box::new(self)
        }
    }

    #[test]
    fn should_read_from_sought_offset_when_reader_moves() -> io::Result<()> {
        // Arrange
        let fs = MockFs::new();
        let mut writer = FileWriter::new(open(&fs, "run", true));
        writer.write_all(b"0123456789")?;
        writer.flush()?;
        let mut reader = FileReader::new(open(&fs, "run", false))?;

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
    fn should_reject_seek_when_target_is_unaddressable() -> io::Result<()> {
        // Arrange
        let fs = MockFs::new();
        let mut writer = FileWriter::new(open(&fs, "run", true));
        writer.write_all(b"bytes")?;
        writer.flush()?;
        let mut reader = FileReader::new(open(&fs, "run", false))?;

        // Act
        let from_end = writer.seek(SeekFrom::End(0));
        let before_start = reader.seek(SeekFrom::Current(-1));

        // Assert
        assert_eq!(
            from_end.map_err(|error| error.kind()),
            Err(io::ErrorKind::Unsupported)
        );
        assert_eq!(
            before_start.map_err(|error| error.kind()),
            Err(io::ErrorKind::InvalidInput)
        );
        Ok(())
    }

    #[test]
    fn should_keep_filesystem_error_class_when_carried_through_io() {
        // Arrange
        let cases: [fn() -> FsError; 4] = [
            || FsError::NoSpace("full".into()),
            || FsError::Timeout("slow".into()),
            || FsError::Corruption("torn".into()),
            || FsError::Unavailable("down".into()),
        ];

        for error in cases {
            let expected = std::mem::discriminant(&error().into_midge());

            // Act
            let carried = MidgeError::from(error().into_io());

            // Assert
            assert_eq!(std::mem::discriminant(&carried), expected, "{carried:?}");
        }
    }
}
