//! Exact FORMAT 9 migration inputs captured under the primary writer lease
//! and manifest writer lock.

use super::{epoch_floor::StartupEpochFloor, StartupLease, StartupStoragePath};
use crate::common::{DeadlineScope, MidgeError, MidgeResult, OperationDeadline};
use crate::config::Storage;
use crate::io::{Durability, Fs, FsError, FsPath, OpenOptions};
use crate::metadata::local_authority::LocalMetadataAuthority;
use crate::metadata::{codec, files, Manifest, ManifestPersistence};
use crate::runtime::{IntentLogEntry, IntentPersistence};
use bytes::Bytes;
use sha2::{Digest, Sha256};
use std::{sync::Arc, time::Duration};

const FORMAT_9: &[u8] = b"midge-format-version=9\n";
const FORMAT_10: &[u8] = b"midge-format-version=10\n";

pub(in crate::engine) fn upgrade_local(
    opts: &crate::engine::OpenOptions,
    timeout: Duration,
) -> MidgeResult<()> {
    let Storage::Local { path } = opts.storage() else {
        return Err(MidgeError::CompatibilityError(
            "this FORMAT 9 upgrade entry point requires a local database".into(),
        ));
    };
    let scope = DeadlineScope::new(OperationDeadline::from_budget(timeout));
    scope.check("FORMAT 9 upgrade startup")?;
    let root = match opts.injected_filesystem() {
        Some(root) => root,
        None => super::existing_host_root(path)?,
    };
    crate::metadata::validate_format_marker(root.as_ref(), path)?;
    let storage_path = StartupStoragePath {
        db_path: path.clone(),
        memory_mode: false,
        root,
    };
    let floor = StartupEpochFloor::discover_within(opts, &storage_path, &scope)?;
    let lease = StartupLease::acquire_within(opts, &storage_path, floor, &scope)?;
    let store = lease.leader_store.as_deref().ok_or_else(|| {
        MidgeError::Fenced("FORMAT 9 upgrade requires a fenced local writer effect".into())
    })?;
    let root = crate::io::scope_fs(Arc::clone(&storage_path.root), scope.clone());
    let mut result = None;
    store
        .with_writer_epoch(
            &lease.lease.holder_id(),
            lease.writer_epoch,
            scope.deadline().remaining(),
            &mut || {
                result = Some(crate::metadata::journal::with_manifest_writer_lock_within(
                    &root,
                    &scope,
                    || select_upgrade(&root, &lease, &scope),
                ));
            },
        )
        .map_err(|error| error.into_validation_error("FORMAT 9 upgrade writer effect"))?;
    result.ok_or_else(|| MidgeError::Internal("FORMAT 9 writer effect did not execute".into()))?
}

fn select_upgrade(
    root: &Arc<dyn Fs>,
    lease: &StartupLease,
    scope: &DeadlineScope,
) -> MidgeResult<()> {
    // Establish a durable current marker before discarding any abandoned
    // candidate, including one left after a failed FORMAT directory sync.
    sync_root(root.as_ref())?;
    if marker_bytes(root.as_ref(), scope)?.as_deref() == Some(FORMAT_10) {
        LocalMetadataAuthority::open(Arc::clone(root))?;
        return Ok(());
    }
    let source_budget = crate::metadata::cloud_manifest::MAX_CHAIN_BYTES;
    let source = Format9UpgradeSource::capture(root.as_ref(), source_budget, scope)?;
    let manifest = codec::encode(codec::MANIFEST, &source.manifest)?;
    let intents = serde_json::to_vec(&source.intents)
        .map_err(|error| MidgeError::Internal(format!("encode FORMAT 10 intent base: {error}")))?;
    let candidate = LocalMetadataAuthority::stage_upgrade_candidate(
        Arc::clone(root),
        source.digest,
        &manifest,
        source.manifest.edit_checkpoint_id,
        &intents,
        source.intent_sequence,
        source.intent_checksum,
    )?;
    source.revalidate(root.as_ref(), source_budget, scope)?;
    let candidate_digest = candidate.head_digest();
    let result = crate::io::staging::stage_bytes_typed(
        root,
        &FsPath::new("FORMAT.tmp"),
        &FsPath::new(files::FORMAT),
        FORMAT_10,
        || {
            scope.check("publish FORMAT 10 marker")?;
            lease.ensure_healthy("before FORMAT 10 marker publication")?;
            crate::failpoints::fail_point!("midge::format10::before_marker_switch", |_| Err(
                MidgeError::Internal("injected failure before FORMAT 10 marker switch".into())
            ));
            Ok(())
        },
    );
    match result {
        Ok(()) => Ok(()),
        Err(error) => reconcile_marker_switch(root, scope, candidate_digest, error),
    }
}

fn reconcile_marker_switch(
    root: &Arc<dyn Fs>,
    scope: &DeadlineScope,
    candidate_digest: [u8; 32],
    write_error: MidgeError,
) -> MidgeResult<()> {
    let selection: MidgeResult<Option<bool>> = (|| {
        let before = marker_bytes(root.as_ref(), scope)?;
        sync_root(root.as_ref())?;
        let after = marker_bytes(root.as_ref(), scope)?;
        if before != after {
            return Ok(None);
        }
        match after.as_deref() {
            Some(FORMAT_9) => Ok(Some(false)),
            Some(FORMAT_10) => {
                let selected = LocalMetadataAuthority::open(Arc::clone(root))?;
                Ok((selected.head_digest() == candidate_digest).then_some(true))
            }
            _ => Ok(None),
        }
    })();
    match selection {
        Ok(Some(true)) => Ok(()),
        Ok(Some(false)) => Err(write_error),
        Ok(None) | Err(_) => Err(MidgeError::OutcomeUnknown(format!(
            "could not prove the durable FORMAT marker after publication failed: {write_error}"
        ))),
    }
}

fn sync_root(fs: &dyn Fs) -> MidgeResult<()> {
    fs.sync_dir(&FsPath::new("."), Durability::Durable)
        .map_err(FsError::into_midge)
}

fn marker_bytes(fs: &dyn Fs, scope: &DeadlineScope) -> MidgeResult<Option<Bytes>> {
    read_body(fs, files::FORMAT, &mut 64, scope)
}

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
        if marker_bytes(fs, scope)?.as_deref() != Some(FORMAT_9) {
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

    #[cfg(feature = "failpoints")]
    #[test]
    fn should_resume_local_upgrade_when_marker_switch_was_interrupted() -> MidgeResult<()> {
        use crate::{Engine, TransactionMode, WriteOptions};

        // Arrange: close a populated FORMAT 9 database and fail after
        // staging the new closure but before selecting its FORMAT marker.
        let directory = tempfile::tempdir()?;
        let opts = crate::OpenOptions::local(directory.path()).build()?;
        let timeout = Duration::from_secs(5);
        let write = |key: &[u8]| -> MidgeResult<()> {
            let mut engine = Engine::open(opts.clone())?;
            let cf = engine.get_column_family("default").expect("default family");
            let mut transaction = engine.begin_tx(cf.id(), TransactionMode::ReadWrite)?;
            transaction.put(key.to_vec(), b"value".to_vec(), None)?;
            transaction.commit(WriteOptions::sync())?;
            engine.flush_cf(&cf)?;
            engine.shutdown(timeout)
        };
        write(b"before-first-attempt")?;
        let failpoint_guard = crate::failpoints::test_failpoint_guard();
        let failpoint = fail::FailScenario::setup();
        fail::cfg("midge::format10::before_marker_switch", "return")
            .expect("inject pre-switch interruption");
        let interrupted = Engine::upgrade_format_9(opts.clone(), timeout);
        failpoint.teardown();
        drop(failpoint_guard);
        assert!(interrupted.is_err());
        assert_eq!(
            std::fs::read(directory.path().join(files::FORMAT))?,
            FORMAT_9
        );
        write(b"after-interruption")?;
        let original = std::fs::read(directory.path().join(files::MANIFEST_SNAPSHOT))?;

        // Act: retry against the new source and open through normal startup.
        Engine::upgrade_format_9(opts.clone(), timeout)?;
        let mut reopened = Engine::open(opts)?;

        // Assert
        assert_eq!(
            std::fs::read(directory.path().join(files::FORMAT))?,
            FORMAT_10
        );
        assert_eq!(
            std::fs::read(directory.path().join(files::MANIFEST_SNAPSHOT))?,
            original
        );
        let cf = reopened
            .get_column_family("default")
            .expect("default family");
        let read = reopened.begin_tx(cf.id(), TransactionMode::ReadOnly)?;
        for key in [b"before-first-attempt".as_slice(), b"after-interruption"] {
            assert_eq!(read.get(key)?.as_deref(), Some(b"value".as_slice()));
        }
        drop(read);
        reopened.shutdown(timeout)
    }

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
