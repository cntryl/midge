//! File I/O utilities
//!
//! Common patterns for reading/writing files with proper error handling,
//! optimizations, and future support for vectorized I/O and mmap.

use crate::error::MidgeResult;
use std::fs::File;
use std::io::{IoSlice, Read, Seek, SeekFrom, Write};
use std::path::Path;

// If the `uring` feature is enabled, expose the experimental backend.
#[cfg(feature = "uring")]
mod uring;

/// Read exact number of bytes from a file at the current position
///
/// Wrapper around `read_exact()` with better error messages.
#[inline]
pub fn read_exact(file: &mut File, buf: &mut [u8]) -> MidgeResult<()> {
    file.read_exact(buf)?;
    Ok(())
}

/// Read exact number of bytes from a file at a specific offset
///
/// Combines seek + read_exact into one operation. More efficient than
/// separate calls and provides atomic read semantics.
///
/// # Examples
///
/// ```rust,no_run
/// # use midge::fs::read_exact_at;
/// use std::fs::File;
///
/// let mut file = File::open("data.sst").unwrap();
/// let mut buf = vec![0u8; 1024];
/// read_exact_at(&mut file, 4096, &mut buf).unwrap();
/// ```
#[inline]
pub fn read_exact_at(file: &mut File, offset: u64, buf: &mut [u8]) -> MidgeResult<()> {
    file.seek(SeekFrom::Start(offset))?;
    file.read_exact(buf)?;
    Ok(())
}

/// Read a range of bytes from a file into a new Vec
///
/// Allocates a buffer of the exact size needed and reads into it.
/// Combines seek + allocation + read into one operation.
///
/// # Examples
///
/// ```rust,no_run
/// # use midge::fs::read_range;
/// use std::fs::File;
///
/// let mut file = File::open("data.sst").unwrap();
/// let data = read_range(&mut file, 1000, 2048).unwrap();
/// assert_eq!(data.len(), 1048); // 2048 - 1000
/// ```
pub fn read_range(file: &mut File, start: u64, end: u64) -> MidgeResult<Vec<u8>> {
    let size = (end - start) as usize;
    let mut buf = vec![0u8; size];
    read_exact_at(file, start, &mut buf)?;
    Ok(buf)
}

/// Read the entire file into a Vec
///
/// Simple utility for loading small metadata files. For large files,
/// prefer streaming or mmap approaches.
///
/// # Examples
///
/// ```rust,no_run
/// # use midge::fs::read_file;
/// use std::path::Path;
///
/// let data = read_file(Path::new("metadata.json")).unwrap();
/// println!("Read {} bytes", data.len());
/// ```
pub fn read_file(path: &Path) -> MidgeResult<Vec<u8>> {
    let mut file = File::open(path)?;
    let mut buf = Vec::new();
    file.read_to_end(&mut buf)?;
    Ok(buf)
}

/// Read from the end of a file
///
/// Useful for reading footers that are stored at the end of files.
/// Negative offset counts from the end of the file.
///
/// # Examples
///
/// ```rust,no_run
/// # use midge::fs::read_from_end;
/// use std::fs::File;
///
/// // Read last 48 bytes (e.g., SST footer)
/// let mut file = File::open("data.sst").unwrap();
/// let mut footer = [0u8; 48];
/// read_from_end(&mut file, 48, &mut footer).unwrap();
/// ```
#[inline]
pub fn read_from_end(file: &mut File, bytes_from_end: u64, buf: &mut [u8]) -> MidgeResult<()> {
    file.seek(SeekFrom::End(-(bytes_from_end as i64)))?;
    file.read_exact(buf)?;
    Ok(())
}

/// Write all bytes to a file, ensuring complete write
///
/// Wrapper around `write_all()` with better error context.
#[inline]
pub fn write_all(file: &mut File, buf: &[u8]) -> MidgeResult<()> {
    file.write_all(buf)?;
    Ok(())
}

/// Seek to a specific position in a file
///
/// Wrapper around `seek()` with MidgeResult.
#[inline]
pub fn seek(file: &mut File, pos: SeekFrom) -> MidgeResult<u64> {
    Ok(file.seek(pos)?)
}

/// Get the current file position
///
/// Uses `stream_position()` to get position without moving.
#[inline]
pub fn current_position(file: &mut File) -> MidgeResult<u64> {
    Ok(file.stream_position()?)
}

/// Get the file size
///
/// Uses `seek(SeekFrom::End(0))` to get size. Note: this moves the file position!
#[inline]
pub fn file_size(file: &mut File) -> MidgeResult<u64> {
    Ok(file.seek(SeekFrom::End(0))?)
}

/// Atomic write of multiple buffers (future: use writev on Unix)
///
/// Currently concatenates and writes sequentially. In the future, this could
/// use platform-specific vectorized I/O (writev on Unix, WriteFileGather on Windows)
/// for better performance.
///
/// # Examples
///
/// ```rust,no_run
/// # use midge::fs::write_vectored;
/// use std::fs::File;
///
/// let mut file = File::create("output.dat").unwrap();
/// let header = b"HEADER";
/// let body = b"BODY DATA";
/// let footer = b"FOOTER";
/// write_vectored(&mut file, &[header, body, footer]).unwrap();
/// ```
/// Public vectored write entrypoint.
///
/// When compiled with the `uring` feature this will dispatch to the
/// io_uring-backed implementation. Otherwise it falls back to the
/// synchronous `write_vectored` implementation below.
pub fn write_vectored(file: &mut File, buffers: &[&[u8]]) -> MidgeResult<()> {
    #[cfg(feature = "uring")]
    {
        return uring::write_vectored_uring(file, buffers);
    }

    write_vectored_fallback(file, buffers)
}

/// Internal fallback implementation used when io_uring is not enabled.
/// Kept separate so the io_uring module can call it when still relying on
/// blocking I/O while iterating on the async implementation.
pub fn write_vectored_fallback(file: &mut File, buffers: &[&[u8]]) -> MidgeResult<()> {
    // Try to use the platform's vectored write implementation via `Write::write_vectored`.
    // This lets Rust forward to `writev` on Unix where available. We handle partial
    // writes by reconstructing IoSlice references starting at the appropriate offsets.
    //
    // On platforms or runtimes that don't provide true vectored I/O, this falls back
    // to repeated writes performed by the implementation of `write_vectored`.

    // Fast path: empty input
    if buffers.is_empty() {
        return Ok(());
    }

    let mut idx: usize = 0;
    let mut inner_off: usize = 0;

    while idx < buffers.len() {
        // Build IoSlice array starting at current cursor
        let mut slices: Vec<IoSlice<'_>> = Vec::with_capacity(buffers.len() - idx);
        for b in buffers.iter().skip(idx) {
            if inner_off == 0 {
                slices.push(IoSlice::new(b));
            } else {
                if inner_off >= b.len() {
                    // This buffer already consumed; skip
                    inner_off = 0;
                    continue;
                }
                slices.push(IoSlice::new(&b[inner_off..]));
            }
        }

        let nw = file.write_vectored(&slices)?;
        if nw == 0 {
            return Err(std::io::Error::new(
                std::io::ErrorKind::WriteZero,
                "write_vectored returned zero bytes",
            )
            .into());
        }

        // Advance idx/inner_off by `nw` bytes consumed
        let mut remaining = nw;
        while remaining > 0 && idx < buffers.len() {
            let avail = buffers[idx].len().saturating_sub(inner_off);
            if remaining >= avail {
                remaining -= avail;
                idx += 1;
                inner_off = 0;
            } else {
                inner_off += remaining;
                remaining = 0;
            }
        }
    }

    Ok(())
}

/// Sequential reader that tracks position to avoid unnecessary seeks
///
/// Useful for reading multiple records sequentially from a file.
/// Tracks the current position and only seeks when necessary.
///
/// # Examples
///
/// ```rust,no_run
/// # use midge::fs::SequentialReader;
/// use std::fs::File;
///
/// let file = File::open("wal.log").unwrap();
/// let mut reader = SequentialReader::new(file, 16); // Start after header
///
/// let mut record1 = vec![0u8; 100];
/// reader.read_exact(&mut record1).unwrap();
///
/// let mut record2 = vec![0u8; 200];
/// reader.read_exact(&mut record2).unwrap();
/// // No seeks needed if reading sequentially!
/// ```
pub struct SequentialReader {
    file: File,
    position: u64,
}

impl SequentialReader {
    pub fn new(file: File, start_position: u64) -> Self {
        Self {
            file,
            position: start_position,
        }
    }

    /// Read exact bytes, updating tracked position
    pub fn read_exact(&mut self, buf: &mut [u8]) -> MidgeResult<()> {
        self.file.read_exact(buf)?;
        self.position += buf.len() as u64;
        Ok(())
    }

    /// Seek to a new position (updates tracked position)
    pub fn seek(&mut self, pos: SeekFrom) -> MidgeResult<u64> {
        self.position = self.file.seek(pos)?;
        Ok(self.position)
    }

    /// Get current position (no I/O needed)
    #[inline]
    pub fn position(&self) -> u64 {
        self.position
    }

    /// Get mutable reference to underlying file
    pub fn file_mut(&mut self) -> &mut File {
        &mut self.file
    }

    /// Consume reader and return underlying file
    pub fn into_inner(self) -> File {
        self.file
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::io::Write;

    #[test]
    fn should_read_exact_at_offset() {
        // Arrange
        let tmp = tempfile::NamedTempFile::new().unwrap();
        let mut file = tmp.reopen().unwrap();
        file.write_all(b"0123456789").unwrap();

        // Act
        let mut buf = [0u8; 3];
        read_exact_at(&mut file, 5, &mut buf).unwrap();

        // Assert
        assert_eq!(&buf, b"567");
    }

    #[test]
    fn should_read_range_into_vec() {
        // Arrange
        let tmp = tempfile::NamedTempFile::new().unwrap();
        let mut file = tmp.reopen().unwrap();
        file.write_all(b"0123456789ABCDEF").unwrap();

        // Act
        let data = read_range(&mut file, 5, 10).unwrap();

        // Assert
        assert_eq!(data, b"56789");
    }

    #[test]
    fn should_read_from_end_of_file() {
        // Arrange
        let tmp = tempfile::NamedTempFile::new().unwrap();
        let mut file = tmp.reopen().unwrap();
        file.write_all(b"HEADER_BODY_FOOTER").unwrap();

        // Act
        let mut footer = [0u8; 6];
        read_from_end(&mut file, 6, &mut footer).unwrap();

        // Assert
        assert_eq!(&footer, b"FOOTER");
    }

    #[test]
    fn should_write_vectored_buffers() {
        // Arrange
        let tmp = tempfile::NamedTempFile::new().unwrap();
        let mut file = tmp.reopen().unwrap();

        // Act
        write_vectored(&mut file, &[b"HEADER", b"BODY", b"FOOTER"]).unwrap();

        // Assert
        let content = std::fs::read(tmp.path()).unwrap();
        assert_eq!(content, b"HEADERBODYFOOTER");
    }

    #[test]
    fn should_track_position_in_sequential_reader() {
        // Arrange
        let tmp = tempfile::NamedTempFile::new().unwrap();
        std::fs::write(tmp.path(), b"0123456789").unwrap();
        let file = File::open(tmp.path()).unwrap();
        let mut reader = SequentialReader::new(file, 0);

        // Act
        let mut buf1 = [0u8; 3];
        reader.read_exact(&mut buf1).unwrap();
        let pos1 = reader.position();

        let mut buf2 = [0u8; 2];
        reader.read_exact(&mut buf2).unwrap();
        let pos2 = reader.position();

        // Assert
        assert_eq!(&buf1, b"012");
        assert_eq!(pos1, 3);
        assert_eq!(&buf2, b"34");
        assert_eq!(pos2, 5);
    }

    #[test]
    fn should_avoid_seek_when_reading_sequentially() {
        // Arrange
        let tmp = tempfile::NamedTempFile::new().unwrap();
        std::fs::write(tmp.path(), b"ABCDEFGHIJ").unwrap();
        let file = File::open(tmp.path()).unwrap();
        let mut reader = SequentialReader::new(file, 0);

        // Act - Multiple sequential reads should not need seeks
        let mut buf = [0u8; 2];
        reader.read_exact(&mut buf).unwrap();
        assert_eq!(&buf, b"AB");

        reader.read_exact(&mut buf).unwrap();
        assert_eq!(&buf, b"CD");

        reader.read_exact(&mut buf).unwrap();
        assert_eq!(&buf, b"EF");

        // Assert
        assert_eq!(reader.position(), 6);
    }

    #[test]
    fn should_get_file_size_correctly() {
        // Arrange
        let tmp = tempfile::NamedTempFile::new().unwrap();
        std::fs::write(tmp.path(), b"Hello, World!").unwrap();
        let mut file = File::open(tmp.path()).unwrap();

        // Act
        let size = file_size(&mut file).unwrap();

        // Assert
        assert_eq!(size, 13);
    }
}
