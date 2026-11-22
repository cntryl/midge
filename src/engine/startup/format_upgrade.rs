//! Exact FORMAT 9 migration inputs captured under the primary writer lease
//! and manifest writer lock.

use crate::common::{DeadlineScope, MidgeError, MidgeResult};
use crate::io::{Fs, FsError, FsPath, OpenOptions};
use crate::metadata::{files, Manifest, ManifestPersistence};
use crate::runtime::{IntentLogEntry, IntentPersistence};
use bytes::Bytes;
use sha2::{Digest, Sha256};

pub(crate) struct Format9UpgradeSource {
    pub(crate) manifest: Manifest,
    pub(crate) intents: Vec<IntentLogEntry>,
    pub(crate) intent_sequence: u64,
    pub(crate) intent_checksum: u32,
    digest: [u8; 32],
}

impl Format9UpgradeSource {
    pub(crate) fn capture(
        fs: &dyn Fs,
        source_budget: u64,
        scope: &DeadlineScope,
    ) -> MidgeResult<Self> {
        let bodies = Format9Bodies::read(fs, source_budget, scope)?;
        let manifest = ManifestPersistence::decode_for_upgrade(
            bodies.snapshot.as_ref().map(|(_, bytes)| bytes.as_ref()),
            bodies.journal.as_deref().unwrap_or_default(),
            scope,
        )?;
        let digest = bodies.digest();
        let intents =
            IntentPersistence::decode_for_upgrade(bodies.intents.unwrap_or_default(), scope)?;
        Ok(Self {
            manifest,
            intents: intents.entries,
            intent_sequence: intents.sequence,
            intent_checksum: intents.checksum,
            digest,
        })
    }

    /// Recheck the legacy inputs before committing the new format marker.
    pub(crate) fn revalidate(
        &self,
        fs: &dyn Fs,
        source_budget: u64,
        scope: &DeadlineScope,
    ) -> MidgeResult<()> {
        if Format9Bodies::read(fs, source_budget, scope)?.digest() == self.digest {
            return Ok(());
        }
        Err(MidgeError::Fenced(
            "FORMAT 9 metadata changed during upgrade staging".into(),
        ))
    }
}

struct Format9Bodies {
    snapshot: Option<(&'static str, Bytes)>,
    journal: Option<Bytes>,
    intents: Option<Bytes>,
}

impl Format9Bodies {
    fn read(fs: &dyn Fs, mut remaining: u64, scope: &DeadlineScope) -> MidgeResult<Self> {
        let mut marker_budget = 64;
        let marker = read_body(fs, files::FORMAT, &mut marker_budget, scope)?;
        if marker.as_deref() != Some(b"midge-format-version=9\n") {
            return Err(MidgeError::CompatibilityError(
                "explicit FORMAT 9 upgrade requires a FORMAT 9 source".into(),
            ));
        }
        let snapshot = match read_body(fs, files::MANIFEST_SNAPSHOT, &mut remaining, scope)? {
            Some(bytes) => Some((files::MANIFEST_SNAPSHOT, bytes)),
            None => read_body(fs, files::MANIFEST, &mut remaining, scope)?
                .map(|bytes| (files::MANIFEST, bytes)),
        };
        let journal = read_body(fs, files::JOURNAL, &mut remaining, scope)?;
        let intents = read_body(fs, files::INTENT_LOG, &mut remaining, scope)?;
        Ok(Self {
            snapshot,
            journal,
            intents,
        })
    }

    fn digest(&self) -> [u8; 32] {
        let mut digest = Sha256::new();
        digest.update(b"Midge FORMAT 9 migration source\0");
        let (snapshot_name, snapshot) = self
            .snapshot
            .as_ref()
            .map_or((files::MANIFEST_SNAPSHOT, None), |(name, bytes)| {
                (*name, Some(bytes.as_ref()))
            });
        digest_body(&mut digest, snapshot_name, snapshot);
        digest_body(&mut digest, files::JOURNAL, self.journal.as_deref());
        digest_body(&mut digest, files::INTENT_LOG, self.intents.as_deref());
        digest.finalize().into()
    }
}

fn read_body(
    fs: &dyn Fs,
    name: &'static str,
    remaining: &mut u64,
    scope: &DeadlineScope,
) -> MidgeResult<Option<Bytes>> {
    scope.check("capture FORMAT 9 metadata")?;
    let file = match fs.open(&FsPath::new(name), OpenOptions::READ_ONLY) {
        Ok(file) => file,
        Err(FsError::NotFound(_)) => return Ok(None),
        Err(error) => return Err(error.into_midge()),
    };
    let size = file.len().map_err(FsError::into_midge)?;
    *remaining = remaining.checked_sub(size).ok_or_else(|| {
        MidgeError::ResourceLimit(format!("FORMAT 9 upgrade source budget exceeded by {name}"))
    })?;
    let bytes = file.read_at(0, size).map_err(FsError::into_midge)?;
    if bytes.len() as u64 != size || file.len().map_err(FsError::into_midge)? != size {
        return Err(MidgeError::Fenced(format!(
            "FORMAT 9 source file {name} changed during capture"
        )));
    }
    scope.check("capture FORMAT 9 metadata")?;
    Ok(Some(bytes))
}

fn digest_body(digest: &mut Sha256, name: &str, bytes: Option<&[u8]>) {
    digest.update(name.as_bytes());
    digest.update([0]);
    if let Some(bytes) = bytes {
        digest.update([1]);
        digest.update((bytes.len() as u64).to_be_bytes());
        digest.update(bytes);
    } else {
        digest.update([0]);
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::common::OperationDeadline;
    use crate::metadata::ManifestEdit;
    use std::sync::Arc;
    use std::time::Duration;

    const SOURCE_BUDGET: u64 = 1024 * 1024;

    #[test]
    fn should_reject_upgrade_when_verified_legacy_metadata_advances_after_capture() {
        // Arrange
        let directory = tempfile::tempdir().expect("database directory");
        let fs: Arc<dyn Fs> =
            Arc::new(crate::io::RealFs::new(directory.path()).expect("open source directory"));
        crate::metadata::ensure_or_create_format_marker(&fs, directory.path())
            .expect("create legacy marker");
        crate::metadata::append_edit(
            directory.path(),
            &ManifestEdit::BumpNextSstSeq {
                cf_id: 0,
                next_seq: 1,
            },
        )
        .expect("append first durable edit");
        IntentPersistence::save_with_fs_typed(
            &fs,
            &[IntentLogEntry::SeqnoAllocated { seqno: 7, cf_id: 0 }],
        )
        .expect("write legacy intent");
        let scope = DeadlineScope::new(OperationDeadline::from_budget(Duration::from_secs(5)));
        let source = Format9UpgradeSource::capture(fs.as_ref(), SOURCE_BUDGET, &scope)
            .expect("capture verified legacy metadata");
        crate::metadata::append_edit(
            directory.path(),
            &ManifestEdit::BumpNextSstSeq {
                cf_id: 0,
                next_seq: 2,
            },
        )
        .expect("append second durable edit");

        // Act
        let result = source.revalidate(fs.as_ref(), SOURCE_BUDGET, &scope);

        // Assert
        assert_eq!(source.manifest.edit_checkpoint_id, 1);
        assert_eq!(source.intent_sequence, 1);
        assert_ne!(source.intent_checksum, 0);
        assert_eq!(source.intents.len(), 1);
        assert!(matches!(result, Err(MidgeError::Fenced(_))));
    }
}
