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
    /// Immutable sorted-string tables in the data store.
    pub const SST_PREFIX: &'static str = "sst/";
    /// Mutable recovery metadata in the control store.
    pub const METADATA_PREFIX: &'static str = "metadata/";
    /// Mutable primary-lease object in the control store.
    pub const LEASE_OBJECT_KEY: &'static str = "midge_primary_lease.json";
}

#[cfg(test)]
mod tests {
    use super::CloudObjectLayout;

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
    }
}
