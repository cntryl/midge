//! Whole-file reads for small control files (markers, catalogs, leases),
//! and durable copies and cuts of files that salvage keeps.

use super::{Durability, Fs, FsError, FsPath, FsResult, OpenMode, OpenOptions, ReadFs};
use bytes::Bytes;

/// Bytes one copy step reads, so a large file is never held whole.
const COPY_CHUNK: u64 = 4 << 20;

/// Read every byte of the file at `path`.
///
/// # Errors
///
/// Returns the backend's error when the file cannot be opened, measured or read;
/// a missing file is `FsError::NotFound`.
pub(crate) fn read_all(fs: &dyn ReadFs, path: &FsPath) -> FsResult<Bytes> {
    let file = fs.open_read(path)?;
    let length = file.len()?;
    file.read_at(0, length)
}

/// Copy the file at `from` to a new file at `to`, durably: the copy is
/// synced before this returns. Returns the bytes copied.
///
/// # Errors
///
/// Returns the backend's error, an `Io` error on a short read, and
/// `AlreadyExists` (from the backend) when `to` exists.
pub(crate) fn copy_durably(fs: &dyn Fs, from: &FsPath, to: &FsPath) -> FsResult<u64> {
    let source = fs.open(
        from,
        OpenOptions {
            mode: OpenMode::ReadOnly,
            create: false,
            create_new: false,
            truncate: false,
        },
    )?;
    let mut copy = fs.open(
        to,
        OpenOptions {
            mode: OpenMode::ReadWrite,
            create: true,
            create_new: true,
            truncate: false,
        },
    )?;
    let length = source.len()?;
    let mut offset = 0;
    while offset < length {
        let chunk = source.read_at(offset, COPY_CHUNK.min(length - offset))?;
        if chunk.is_empty() {
            return Err(FsError::Io(format!(
                "short read copying '{}' at byte {offset}",
                from.0
            )));
        }
        let read = chunk.len() as u64;
        copy.write_at(offset, chunk)?;
        offset += read;
    }
    copy.sync(Durability::Durable)?;
    Ok(length)
}

/// Cut the file at `path` to `len` bytes, durably. A file already that short
/// is left alone. Returns the bytes cut off.
///
/// # Errors
///
/// Returns the backend's error.
pub(crate) fn truncate_durably(fs: &dyn Fs, path: &FsPath, len: u64) -> FsResult<u64> {
    let mut file = fs.open(
        path,
        OpenOptions {
            mode: OpenMode::ReadWrite,
            create: false,
            create_new: false,
            truncate: false,
        },
    )?;
    let length = file.len()?;
    if length <= len {
        return Ok(0);
    }
    file.truncate(len)?;
    file.sync(Durability::Durable)?;
    Ok(length - len)
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::io::MockFs;

    #[test]
    fn should_read_every_byte_when_file_exists() {
        // Arrange
        let fs = MockFs::new();
        let path = FsPath::new("FORMAT");
        fs.open(
            &path,
            OpenOptions {
                mode: OpenMode::ReadWrite,
                create: true,
                create_new: false,
                truncate: false,
            },
        )
        .unwrap()
        .append(Bytes::from_static(b"marker\n"))
        .unwrap();

        // Act
        let bytes = read_all(&fs, &path).unwrap();

        // Assert
        assert_eq!(&bytes[..], b"marker\n");
    }

    #[test]
    fn should_report_not_found_when_file_is_missing() {
        // Arrange
        let fs = MockFs::new();

        // Act
        let result = read_all(&fs, &FsPath::new("missing"));

        // Assert
        assert!(matches!(result, Err(FsError::NotFound(_))));
    }
}
