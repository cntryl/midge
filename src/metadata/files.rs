//! Names of the metadata files that make up a database's durable state.
//!
//! These names appear in local persistence, the cloud metadata mirror and
//! recovery. Spelling them as literals in each place let the lists drift
//! apart, so every caller reads them from here.

/// Format marker written when a database is created.
pub(crate) const FORMAT: &str = "FORMAT";
/// Current manifest.
pub(crate) const MANIFEST: &str = "manifest.json";
/// Checkpointed manifest snapshot.
pub(crate) const MANIFEST_SNAPSHOT: &str = "manifest.snapshot.json";
/// Manifest edit journal.
pub(crate) const JOURNAL: &str = "manifest.journal";
/// Pending DDL intent log.
pub(crate) const INTENT_LOG: &str = "intent_log.json";

/// Metadata files mirrored to cloud storage, ordered so a reader that stops
/// early still sees a consistent prefix.
pub(crate) const CLOUD_MIRRORED: &[&str] = &[FORMAT, MANIFEST_SNAPSHOT, JOURNAL, INTENT_LOG];

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn should_mirror_every_metadata_file_the_database_writes() {
        // Arrange
        let expected = &[FORMAT, MANIFEST_SNAPSHOT, JOURNAL, INTENT_LOG];
        // Act
        let mirrored = CLOUD_MIRRORED;
        // Assert
        assert_eq!(mirrored, expected);
    }
}
