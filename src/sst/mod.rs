//! SST (Sorted String Table) module
//!
//! Provides on-disk SST file implementations.
//!
//! ## Key Design: SST uses `std::fs` directly, NOT `storage/` layer
//!
//! SSTs intentionally use synchronous, direct filesystem I/O via `std::fs` rather than
//! the callback-driven `StorageBackend` trait. This is correct because:
//!
//! - **Immutable after `finalize()`**: SST files never change once written, only read or deleted
//! - **Blocking I/O required**: SST access patterns (seek + read at offset) need synchronous I/O
//! - **Local files first**: SSTs are written locally, then persisted to cloud via `HybridStorage`
//! - **Hot path on read side**: Reader needs fast, direct access without callback overhead
//!
//! ### Integration with Storage Layer
//!
//! - **Write path**: Compaction creates SSTs via `FsSstFactoryIo` (using `io::Fs` abstraction)
//!   → Files stored in local directory
//!   → `HybridStorage` persists to cloud (via `StorageBackend` callbacks)
//!
//! - **Read path**: Queries use `SstFileIo` to read local SSTs
//!   → Uses `io::Fs` for flexible real and mock filesystem backends
//!   → Block cache + bloom filters for optimization
//!   → No cloud access on read (reads hit local cache or cloud-synced local file)
//!
//! ## Module Overview
//!
//! - **encoding**: TLV-based entry encoding for SST files
//! - **types**: SST file format types (blocks, footers, handles)
//! - **traits**: Reader/Writer/Factory contracts for SST implementations
//! - **fs**: Filesystem-backed SST implementation (uses `io::Fs` abstraction)

pub mod bloom;
pub mod cache;
pub mod compression;
pub mod encoding;
pub mod fs;
pub(crate) mod identity;
pub mod index;
mod name;
pub mod read_amp_metrics;
pub(crate) mod read_path_metrics;
pub mod traits;
pub mod trie;
pub mod types;

pub use crate::types::KvPair;
pub use fs::FsSstFactoryIo;

pub(crate) use name::PersistedSstName;
pub use read_amp_metrics::ReadAmpMetrics;

pub use traits::{SstFactory, SstReader, SstStateReader};

/// Pad generated SST sequence names to the full `u64` width so filesystem and
/// object-store listings sort in the same order as creation sequence.
pub const SST_SEQUENCE_WIDTH: usize = 20;

/// Format a canonical SST filename. Storage roots already encode the object
/// type via the `sst/` directory or cloud prefix, so the file name only carries
/// ordering identity.
#[must_use]
pub fn file_name(cf_id: u32, level: u32, sequence: u64) -> String {
    format!("{cf_id:06}_{level:02}_{sequence:0SST_SEQUENCE_WIDTH$}.sst")
}

/// Format a deterministic compaction partition filename. One compaction
/// generation owns every zero-based partition ordinal in its replacement set.
#[must_use]
pub fn compaction_file_name(cf_id: u32, level: u32, generation: u64, partition: u32) -> String {
    format!("{cf_id:06}_{level:02}_{generation:0SST_SEQUENCE_WIDTH$}_{partition:010}.sst")
}

/// Parse a canonical compaction partition filename.
///
/// This remains internal because partition identity is an implementation detail,
/// not part of the stable SST naming API.
pub(crate) fn parse_compaction_file_name(name: &str) -> Option<(u32, u32, u64, u32)> {
    let stem = name.strip_suffix(".sst")?;
    let mut parts = stem.split('_');
    let cf = parts.next()?;
    let level = parts.next()?;
    let generation = parts.next()?;
    let partition = parts.next()?;
    if parts.next().is_some()
        || [cf, level, generation, partition]
            .iter()
            .any(|part| part.is_empty() || !part.bytes().all(|byte| byte.is_ascii_digit()))
    {
        return None;
    }

    let parsed = (
        cf.parse().ok()?,
        level.parse().ok()?,
        generation.parse().ok()?,
        partition.parse().ok()?,
    );
    (compaction_file_name(parsed.0, parsed.1, parsed.2, parsed.3) == name).then_some(parsed)
}

/// Format the cloud object key for an SST file.
#[must_use]
pub fn object_key(file_name: &str) -> String {
    format!(
        "{}{file_name}",
        crate::cloud_layout::CloudObjectLayout::SST_PREFIX
    )
}

/// Format the temporary staging path for an SST file inside the local SST root.
#[must_use]
pub fn temp_object_key(file_name: &str) -> String {
    format!(
        "{}{file_name}.tmp",
        crate::cloud_layout::CloudObjectLayout::SST_PREFIX
    )
}

#[cfg(test)]
mod tests {
    #[test]
    fn should_format_sst_names_in_lexicographic_sequence_order() {
        // Arrange
        let names = [1, 2, 10, u64::MAX]
            .into_iter()
            .map(|seq| super::file_name(7, 2, seq))
            .collect::<Vec<_>>();

        // Act
        let mut sorted = names.clone();
        sorted.sort();

        // Assert
        assert_eq!(names, sorted);
        assert_eq!(names[0], "000007_02_00000000000000000001.sst");
        assert_eq!(names[3], "000007_02_18446744073709551615.sst");
    }

    #[test]
    fn should_round_trip_canonical_compaction_partition_names() {
        // Arrange
        let name = super::compaction_file_name(7, 2, 41, 3);

        // Act
        let parsed = super::parse_compaction_file_name(&name);

        // Assert
        assert_eq!(name, "000007_02_00000000000000000041_0000000003.sst");
        assert_eq!(parsed, Some((7, 2, 41, 3)));
    }

    #[test]
    fn should_reject_non_canonical_compaction_partition_names() {
        // Arrange
        let invalid = [
            "000007_02_00000000000000000041.sst",
            "7_2_41_3.sst",
            "000007_02_00000000000000000041_0000000003.tmp",
            "000007_02_00000000000000000041_0000000003_extra.sst",
        ];

        // Act
        let all_rejected = invalid
            .iter()
            .all(|name| super::parse_compaction_file_name(name).is_none());

        // Assert
        assert!(all_rejected);
    }

    #[test]
    fn should_format_sst_object_keys_without_repeating_sst_prefix_in_file_name() {
        // Arrange
        let file_name = super::file_name(0, 0, 1);

        // Act
        let object_key = super::object_key(&file_name);
        let temp_object_key = super::temp_object_key(&file_name);

        // Assert
        assert_eq!(file_name, "000000_00_00000000000000000001.sst");
        assert_eq!(object_key, "sst/000000_00_00000000000000000001.sst");
        assert_eq!(
            temp_object_key,
            "sst/000000_00_00000000000000000001.sst.tmp"
        );
    }
}
