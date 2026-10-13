//! Write-Ahead Log (WAL) subsystem
//!
//! Provides durable write-ahead logging with filesystem implementations.
//!
//! ## Implementation
//!
//! Uses modern `io::Fs-based` implementations for production code:
//! - `fs::FsWalWriterIo` - Append records to WAL
//! - `fs::FsWalFactoryIo` - Factory for creating writers
//!
//! Reading a WAL back is recovery's job: `recovery` owns frame scanning,
//! torn-tail tolerance, and writer-epoch fencing, so no standalone reader type
//! is published.
//!
//! The `io::Fs` abstraction enables better testability with swappable implementations
//! (real and mock). All production code has been migrated to this interface.
//!
//! Cloud WAL durability uses epoch-scoped immutable segment objects plus a
//! lease-fenced publication catalog with a conditionally updated recovery
//! mirror. The catalog is a chain like the segment ledger (`cloud_catalog`,
//! #844): an anchor, entries that each link the one before, and a next id
//! that never falls. Recovery replays only validated catalog entries, so an
//! uploader that finishes after losing its lease can create only an ignored
//! orphan. A verified upload only marks its segment uploaded; `publish`
//! selects the contiguous run of uploads the catalog may publish next, which
//! must chain onto its head. Both catalog copies advance to every newly
//! acquired lease epoch before recovery and converge before a cloud
//! durability acknowledgement can advance the runtime frontier.
//!
//! `planner` decides recovery: it places a cache's ledger against the
//! catalog, admits the probed segments in chain order, and says what replays,
//! what salvage sets aside and how the local history continues. It and the
//! format, ledger, sequence, publication and catalog modules are pure; the
//! runtime adapters do their I/O.

pub(crate) mod cloud_catalog;
pub(crate) mod cloud_segment;
mod config;
// `encoding`, `frame` and `policy` stay `pub` inside this private module so the
// crate's own tests and benches can reach them through `crate::__internal`;
// `wal` itself is private, so nothing downstream can name them.
pub mod encoding;
pub mod format;
pub mod frame;
pub(crate) mod fs;
pub(crate) mod ledger;
pub(crate) mod ledger_store;
pub(crate) mod planner;
pub mod policy;
pub(crate) mod publish;
pub(crate) mod recovery;
pub(crate) mod replay;
pub(crate) mod sequence;
pub(crate) mod traits;
pub mod types;

/// Pad sealed WAL segment names to the full `u64` width so filesystem and
/// object-store listings sort in the same order as segment ids.
pub const WAL_SEGMENT_ID_WIDTH: usize = 20;

/// Format a canonical sealed WAL segment filename for the given segment id.
#[must_use]
pub fn segment_file_name(segment_id: u64) -> String {
    format!("{segment_id:0WAL_SEGMENT_ID_WIDTH$}.wal")
}

/// The WAL directory below the database root.
pub(crate) const WAL_DIR: &str = "wal";

/// Root filesystem key of the sealed WAL segment `segment_id`.
#[must_use]
pub(crate) fn segment_key(segment_id: u64) -> crate::io::FsPath {
    crate::io::FsPath::new(format!("{WAL_DIR}/{}", segment_file_name(segment_id)))
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

/// The id of the local WAL segment file `name`, which must be exactly
/// [`segment_file_name`]'s form: twenty digits, then `.wal`. Anything else,
/// an unpadded id or a path included, is not a segment file.
#[must_use]
pub fn parse_segment_file_name(name: &str) -> Option<u64> {
    parse_padded_id(name.strip_suffix(".wal")?)
}

/// The writer epoch and segment id of the cloud WAL object `key`, which must
/// be exactly [`segment_object_key`]'s form,
/// `wal/epochs/{epoch:020}/{id:020}.wal`. A local file name or any other
/// key is not a segment object.
#[must_use]
pub fn parse_segment_object_key(key: &str) -> Option<(u64, u64)> {
    let rest = key
        .strip_prefix(crate::cloud_layout::CloudObjectLayout::WAL_PREFIX)?
        .strip_prefix("epochs/")?;
    let (epoch, file_name) = rest.split_once('/')?;
    Some((parse_padded_id(epoch)?, parse_segment_file_name(file_name)?))
}

/// A `u64` written with exactly [`WAL_SEGMENT_ID_WIDTH`] decimal digits.
fn parse_padded_id(digits: &str) -> Option<u64> {
    if digits.len() != WAL_SEGMENT_ID_WIDTH || !digits.bytes().all(|byte| byte.is_ascii_digit()) {
        return None;
    }
    digits.parse().ok()
}

// Re-export main WAL types
pub(crate) use config::WalBatchingConfig;
pub use types::{WalOpKind, WalRecord};

// Re-export traits
pub use traits::WalWriter;

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
        assert_eq!(segment_object_key(segment_id, writer_epoch), object_key);
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
    fn should_reject_foreign_forms_in_each_segment_parser() {
        // Arrange
        let file = segment_file_name(42);
        let object = segment_object_key(42, 7);

        // Act
        let files = [
            parse_segment_file_name(&file),
            parse_segment_file_name("42.wal"),
            parse_segment_file_name(&format!("wal/{file}")),
            parse_segment_file_name(&object),
            parse_segment_file_name("99999999999999999999.wal"),
            parse_segment_file_name("0000000000000000004a.wal"),
        ];
        let objects = [
            parse_segment_object_key(&object),
            parse_segment_object_key(&file),
            parse_segment_object_key(&format!("wal/{file}")),
            parse_segment_object_key("wal/epochs/7/00000000000000000042.wal"),
            parse_segment_object_key("wal/epochs/00000000000000000007/42.wal"),
            parse_segment_object_key(&format!("wal/epochs/00000000000000000007/x/{file}")),
        ];

        // Assert
        assert_eq!(files, [Some(42), None, None, None, None, None]);
        assert_eq!(objects, [Some((7, 42)), None, None, None, None, None]);
    }
}
