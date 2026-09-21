//! Stable cloud object classes used for provider lifecycle configuration.

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

#[cfg(test)]
mod tests {
    use super::CloudObjectLayout;

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
}
