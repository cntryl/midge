//! Stable cloud object classes used for provider lifecycle configuration.

use crate::common::{MidgeError, MidgeResult};
use std::path::{Component, Path, PathBuf};

/// Key layout for independently managed cloud object classes.
///
/// WAL, SST, and control namespaces live in distinct provider locations. The
/// control namespace contains [`Self::METADATA_PREFIX`] and
/// [`Self::LEASE_OBJECT_KEY`]. Provider lifecycle rules must never age-expire
/// current WAL, SST, or metadata objects.
pub struct CloudObjectLayout;

impl CloudObjectLayout {
    /// Sealed write-ahead log segments in the data store.
    pub const WAL_PREFIX: &'static str = "wal/";
    /// Lease-fenced authority document for remotely recoverable WAL segments.
    pub const WAL_CATALOG_OBJECT_KEY: &'static str = "wal/publication-catalog.v1.json";
    /// Crash-recovery mirror of the WAL publication authority document.
    pub const WAL_CATALOG_MIRROR_OBJECT_KEY: &'static str =
        "wal/publication-catalog.v1.mirror.json";
    /// Immutable sorted-string tables in the data store.
    pub const SST_PREFIX: &'static str = "sst/";
    /// Mutable recovery metadata in the control store.
    pub const METADATA_PREFIX: &'static str = "metadata/";
    /// Mutable primary-lease object in the control store.
    pub const LEASE_OBJECT_KEY: &'static str = "midge_primary_lease.json";

    /// Object key for a mutable recovery-metadata file in the control store.
    #[must_use]
    pub(crate) fn metadata_key(file_name: &str) -> String {
        format!("{}{file_name}", Self::METADATA_PREFIX)
    }
}

/// Validated, engine-private filename for an authoritative SST object.
///
/// Persisted metadata stores names rather than paths. Parsing once at the
/// persistence boundary prevents absolute paths, parent traversal, Windows
/// drive/ADS syntax, and nested paths from ever reaching filesystem joins.
#[derive(Debug, Clone, PartialEq, Eq, Hash)]
pub(crate) struct PersistedSstName(String);

impl PersistedSstName {
    pub(crate) fn parse(name: &str) -> MidgeResult<Self> {
        let path = Path::new(name);
        let mut components = path.components();
        let is_single_normal_component =
            matches!(components.next(), Some(Component::Normal(_))) && components.next().is_none();
        let has_unsafe_portable_syntax = name.contains(['/', '\\', ':', '\0']);
        let has_sst_extension = path.extension().is_some_and(|extension| extension == "sst");

        if name.is_empty()
            || path.is_absolute()
            || !is_single_normal_component
            || has_unsafe_portable_syntax
            || !has_sst_extension
        {
            return Err(MidgeError::Corruption(format!(
                "invalid persisted SST name '{name}': expected one relative .sst filename"
            )));
        }

        Ok(Self(name.to_owned()))
    }

    pub(crate) fn as_str(&self) -> &str {
        &self.0
    }

    pub(crate) fn join_under(&self, root: &Path) -> PathBuf {
        root.join(self.as_str())
    }
}

/// Pad generated SST sequence names to the full `u64` width so filesystem and
/// object-store listings sort in the same order as creation sequence.
pub(crate) const SST_SEQUENCE_WIDTH: usize = 20;

/// Format a canonical SST filename. Storage roots already encode the object
/// type via the `sst/` directory or cloud prefix, so the file name only carries
/// ordering identity.
#[must_use]
pub(crate) fn file_name(cf_id: u32, level: u32, sequence: u64) -> String {
    format!("{cf_id:06}_{level:02}_{sequence:0SST_SEQUENCE_WIDTH$}.sst")
}

/// Format a deterministic compaction partition filename. One compaction
/// generation owns every zero-based partition ordinal in its replacement set.
#[must_use]
pub(crate) fn compaction_file_name(
    cf_id: u32,
    level: u32,
    generation: u64,
    partition: u32,
) -> String {
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
pub(crate) fn object_key(file_name: &str) -> String {
    format!("{}{file_name}", CloudObjectLayout::SST_PREFIX)
}

/// Format the temporary staging path for an SST file inside the local SST root.
#[must_use]
pub(crate) fn temp_object_key(file_name: &str) -> String {
    format!("{}{file_name}.tmp", CloudObjectLayout::SST_PREFIX)
}

#[cfg(test)]
mod tests {
    use super::{
        compaction_file_name, file_name, object_key, parse_compaction_file_name, temp_object_key,
        CloudObjectLayout, PersistedSstName,
    };

    #[test]
    fn should_place_metadata_files_under_the_metadata_prefix_when_building_a_key() {
        // Arrange
        let file_name = "manifest.json";

        // Act
        let key = CloudObjectLayout::metadata_key(file_name);

        // Assert
        assert_eq!(key, "metadata/manifest.json");
        assert!(key.starts_with(CloudObjectLayout::METADATA_PREFIX));
    }

    #[test]
    fn should_keep_lifecycle_object_classes_disjoint() {
        // Arrange
        let prefixes = [
            CloudObjectLayout::WAL_PREFIX,
            CloudObjectLayout::SST_PREFIX,
            CloudObjectLayout::METADATA_PREFIX,
        ];

        // Act
        let distinct_prefixes = prefixes.iter().enumerate().all(|(index, prefix)| {
            prefixes
                .iter()
                .skip(index + 1)
                .all(|other| !prefix.starts_with(other) && !other.starts_with(prefix))
        });

        // Assert
        assert!(distinct_prefixes);
        assert!(!CloudObjectLayout::LEASE_OBJECT_KEY.contains('/'));
        assert!(
            CloudObjectLayout::WAL_CATALOG_OBJECT_KEY.starts_with(CloudObjectLayout::WAL_PREFIX)
        );
        assert!(CloudObjectLayout::WAL_CATALOG_MIRROR_OBJECT_KEY
            .starts_with(CloudObjectLayout::WAL_PREFIX));
        assert_ne!(
            CloudObjectLayout::WAL_CATALOG_OBJECT_KEY,
            CloudObjectLayout::WAL_CATALOG_MIRROR_OBJECT_KEY
        );
    }

    #[test]
    fn should_accept_single_relative_sst_name_when_parsing_persisted_metadata() {
        // Arrange
        let name = "000001_00_00000000000000000042.sst";

        // Act
        let parsed = PersistedSstName::parse(name).expect("parse canonical SST name");

        // Assert
        assert_eq!(parsed.as_str(), name);
    }

    #[test]
    fn should_reject_unsafe_path_forms_when_parsing_persisted_sst_name() {
        // Arrange
        let unsafe_names = [
            "",
            ".",
            "..",
            "../escape.sst",
            "nested/escape.sst",
            "nested\\escape.sst",
            "/absolute.sst",
            "C:\\absolute.sst",
            "alternate:stream.sst",
            "not-an-sst.tmp",
        ];

        // Act

        // Assert
        for name in unsafe_names {
            assert!(
                PersistedSstName::parse(name).is_err(),
                "unsafe persisted name must be rejected: {name}"
            );
        }
    }

    #[test]
    fn should_format_sst_names_in_lexicographic_sequence_order() {
        // Arrange
        let names = [1, 2, 10, u64::MAX]
            .into_iter()
            .map(|sequence| file_name(7, 2, sequence))
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
        let name = compaction_file_name(7, 2, 41, 3);

        // Act
        let parsed = parse_compaction_file_name(&name);

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
            .all(|name| parse_compaction_file_name(name).is_none());

        // Assert
        assert!(all_rejected);
    }

    #[test]
    fn should_format_sst_object_keys_without_repeating_sst_prefix_in_file_name() {
        // Arrange
        let name = file_name(0, 0, 1);

        // Act
        let permanent = object_key(&name);
        let temporary = temp_object_key(&name);

        // Assert
        assert_eq!(name, "000000_00_00000000000000000001.sst");
        assert_eq!(permanent, "sst/000000_00_00000000000000000001.sst");
        assert_eq!(temporary, "sst/000000_00_00000000000000000001.sst.tmp");
    }
}
