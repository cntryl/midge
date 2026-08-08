//! Write-Ahead Log (WAL) subsystem
//!
//! Provides durable write-ahead logging with filesystem implementations.
//!
//! ## Implementation
//!
//! Uses modern `io::Fs-based` implementations for production code:
//! - [`fs::FsWalWriterIo`] - Append records to WAL
//! - [`fs::FsWalReaderIo`] - Read records from WAL
//! - [`fs::FsWalFactoryIo`] - Factory for creating readers/writers
//!
//! The `io::Fs` abstraction enables better testability with swappable implementations
//! (Real, Mock, Chaos). All production code has been migrated to this interface.
//!
//! Cloud WAL durability uses epoch-scoped immutable segment objects plus a
//! lease-fenced publication catalog. Recovery replays only catalog entries, so
//! an uploader that finishes after losing its lease can create only an ignored
//! orphan. The catalog is advanced to every newly acquired lease epoch before
//! recovery and is conditionally updated before a cloud durability
//! acknowledgement can advance the runtime frontier.

pub(crate) mod cloud_catalog;
pub(crate) mod cloud_segment;
pub mod encoding;
pub mod frame;
pub mod fs;
pub mod policy;
pub mod recovery;
pub mod traits;
pub mod types;

/// Active WAL filename. This file is mutable and is never uploaded as-is.
pub const ACTIVE_FILE_NAME: &str = "wal.log";

/// Pad sealed WAL segment names to the full `u64` width so filesystem and
/// object-store listings sort in the same order as segment ids.
pub const WAL_SEGMENT_ID_WIDTH: usize = 20;

/// Format a canonical sealed WAL segment filename for the given segment id.
#[must_use]
pub fn segment_file_name(segment_id: u64) -> String {
    format!("{segment_id:0WAL_SEGMENT_ID_WIDTH$}.wal")
}

/// Format the full cloud object key for an epoch-scoped sealed WAL segment.
#[must_use]
pub fn segment_object_key(segment_id: u64, writer_epoch: u64) -> String {
    format!(
        "{}epochs/{writer_epoch:0WAL_SEGMENT_ID_WIDTH$}/{}",
        crate::cloud_layout::CloudObjectLayout::WAL_PREFIX,
        segment_file_name(segment_id)
    )
}

/// Alias for callers that operate specifically on cloud WAL objects.
#[must_use]
pub fn cloud_segment_file_name(segment_id: u64) -> String {
    segment_file_name(segment_id)
}

/// Backward-compatible alias for callers that operate specifically on cloud WAL
/// objects.
#[must_use]
pub fn cloud_segment_object_key(segment_id: u64, writer_epoch: u64) -> String {
    segment_object_key(segment_id, writer_epoch)
}

/// Parse a WAL segment id from a cloud key, staged filename, or legacy name.
#[must_use]
pub fn parse_segment_id(name: &str) -> Option<u64> {
    let file_name = name.strip_prefix("wal/").unwrap_or(name);

    if let Some((epoch, segment)) = file_name
        .strip_prefix("epochs/")
        .and_then(|rest| rest.split_once('/'))
    {
        epoch.parse::<u64>().ok()?;
        return segment
            .strip_suffix(".wal")
            .and_then(|segment_id| segment_id.parse::<u64>().ok());
    }

    if let Some(segment_str) = file_name.strip_suffix(".wal") {
        return segment_str.parse::<u64>().ok();
    }

    file_name
        .strip_prefix("wal_")
        .and_then(|segment_str| segment_str.strip_suffix(".log"))
        .and_then(|segment_str| segment_str.parse::<u64>().ok())
}

// Re-export main WAL types
pub use types::{WalOpKind, WalRecord};

// Re-export traits
pub use traits::{WalReaderDyn, WalWriter};

// Re-export encoding functions

// Re-export io::Fs-based implementations
pub use fs::FsWalFactoryIo;

// Re-export recovery

// Re-export policy types
pub use policy::DurabilityPolicy;

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn should_format_epoch_scoped_cloud_wal_object_key() {
        // Arrange
        let segment_id = 42;
        let writer_epoch = 7;

        // Act
        let object_key = segment_object_key(segment_id, writer_epoch);

        // Assert
        assert_eq!(
            object_key,
            "wal/epochs/00000000000000000007/00000000000000000042.wal"
        );
        assert_eq!(
            cloud_segment_object_key(segment_id, writer_epoch),
            object_key
        );
    }

    #[test]
    fn should_format_wal_segment_names_in_lexicographic_sequence_order() {
        // Arrange
        let names = [1, 2, 10, u64::MAX]
            .into_iter()
            .map(segment_file_name)
            .collect::<Vec<_>>();
        let mut sorted = names.clone();
        sorted.sort();

        // Act
        // Assert
        assert_eq!(names, sorted);
        assert_eq!(names[0], "00000000000000000001.wal");
        assert_eq!(names[3], "18446744073709551615.wal");
    }

    #[test]
    fn should_parse_segment_ids_from_supported_wal_names() {
        // Arrange
        // Act
        // Assert
        assert_eq!(parse_segment_id("1.wal"), Some(1));
        assert_eq!(parse_segment_id("00000000000000000042.wal"), Some(42));
        assert_eq!(parse_segment_id("wal/00000000000000000099.wal"), Some(99));
        assert_eq!(
            parse_segment_id("wal/epochs/00000000000000000007/00000000000000000099.wal"),
            Some(99)
        );
        assert_eq!(parse_segment_id("wal_000123.log"), Some(123));
    }
}
