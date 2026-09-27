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

/// Manifest bodies, newest authority first. A reader compares its local
/// state against these to decide whether another writer moved ahead.
pub(crate) const MANIFEST_BODIES: &[&str] = &[MANIFEST_SNAPSHOT];

/// Whether `file_name` carries a manifest body, and so a sequence to compare.
#[must_use]
pub(crate) fn is_manifest_body(file_name: &str) -> bool {
    MANIFEST_BODIES.contains(&file_name)
}

/// Sequence a manifest body claims, or `None` for any other metadata file.
///
/// # Errors
///
/// Returns `Corruption` when a manifest body cannot be parsed: the bytes are
/// durable state that should decode, wherever they were read from.
pub(crate) fn manifest_sequence(
    file_name: &str,
    data: &[u8],
) -> crate::common::MidgeResult<Option<u64>> {
    if !is_manifest_body(file_name) {
        return Ok(None);
    }
    let manifest: super::Manifest = serde_json::from_slice(data).map_err(|error| {
        crate::common::MidgeError::Corruption(format!(
            "cloud metadata '{file_name}' is invalid: {error}"
        ))
    })?;
    Ok(Some(manifest.last_persisted_sequence))
}

/// Reject a metadata mirror write when the remote body is ahead of local
/// state.
///
/// A higher remote sequence means another writer published newer metadata,
/// so this writer no longer owns it: that is lost authority, not a transient
/// fault, and every caller must treat it the same way.
///
/// # Errors
///
/// Returns `Fenced` when the remote body is ahead, or `Corruption` when a
/// manifest body cannot be parsed.
pub(crate) fn ensure_remote_not_ahead(
    file_name: &str,
    data: &[u8],
    local_sequence: u64,
) -> crate::common::MidgeResult<()> {
    let Some(remote_sequence) = manifest_sequence(file_name, data)? else {
        return Ok(());
    };
    if remote_sequence > local_sequence {
        return Err(crate::common::MidgeError::Fenced(format!(
            "stale cloud metadata mirror rejected: remote {file_name} is ahead of local manifest ({remote_sequence} > {local_sequence})"
        )));
    }
    Ok(())
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn should_fence_when_the_remote_manifest_is_ahead_of_local_state() {
        // Arrange: another writer published a newer manifest.
        let manifest = super::super::Manifest {
            last_persisted_sequence: 9,
            ..Default::default()
        };
        let body = serde_json::to_vec(&manifest).expect("encode manifest");

        // Act
        let error = ensure_remote_not_ahead(MANIFEST_SNAPSHOT, &body, 4)
            .expect_err("a newer remote manifest means lost authority");

        // Assert
        assert!(matches!(error, crate::common::MidgeError::Fenced(_)));
    }

    #[test]
    fn should_accept_a_remote_manifest_at_or_behind_local_state() {
        // Arrange
        let manifest = super::super::Manifest {
            last_persisted_sequence: 4,
            ..Default::default()
        };
        let body = serde_json::to_vec(&manifest).expect("encode manifest");

        // Act
        let accepted = ensure_remote_not_ahead(MANIFEST_SNAPSHOT, &body, 4);

        // Assert
        assert!(accepted.is_ok());
    }

    #[test]
    fn should_report_no_sequence_for_metadata_files_without_a_manifest_body() {
        // Arrange
        let journal = b"not a manifest";

        // Act
        let sequence = manifest_sequence(JOURNAL, journal).expect("non-manifest files parse");

        // Assert
        assert_eq!(sequence, None);
    }

    #[test]
    fn should_reject_a_manifest_body_that_cannot_be_parsed() {
        // Arrange
        let corrupt = b"not-json";

        // Act
        let error =
            manifest_sequence(MANIFEST_SNAPSHOT, corrupt).expect_err("a manifest body must parse");

        // Assert
        assert!(matches!(error, crate::common::MidgeError::Corruption(_)));
    }

    #[test]
    fn should_mirror_every_metadata_file_the_database_writes() {
        // Arrange: a file missing from the mirror list is silently not
        // published, so the set is pinned here.

        // Act
        let mirrored = CLOUD_MIRRORED;

        // Assert
        assert_eq!(mirrored, &[FORMAT, MANIFEST_SNAPSHOT, JOURNAL, INTENT_LOG]);
    }
}
