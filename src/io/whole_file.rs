//! Whole-file reads for small control files (markers, catalogs, leases).

use super::{Fs, FsPath, FsResult, OpenMode, OpenOptions};
use bytes::Bytes;

/// Read every byte of the file at `path`.
///
/// # Errors
///
/// Returns the backend's error when the file cannot be opened, measured or read;
/// a missing file is `FsError::NotFound`.
pub(crate) fn read_all(fs: &dyn Fs, path: &FsPath) -> FsResult<Bytes> {
    let file = fs.open(
        path,
        OpenOptions {
            mode: OpenMode::ReadOnly,
            create: false,
            create_new: false,
            truncate: false,
        },
    )?;
    let length = file.len()?;
    file.read_at(0, length)
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::io::{FsError, MockFs};

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
