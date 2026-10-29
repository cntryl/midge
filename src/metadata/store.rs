//! The single runtime writer of the manifest journal and snapshot (#494).
//!
//! The manifest is `manifest.snapshot.json` plus an append-only journal of
//! edits since that snapshot. The free functions in `journal` and
//! `persistence` rebuild the journal position from disk on every call: each
//! append parsed the whole snapshot just to pick the next edit id. A store
//! remembers the position instead, so an append writes one record and a
//! snapshot from a current caller reads nothing.
//!
//! Remembering is only safe if nothing else writes. Every runtime journal
//! append and snapshot save goes through the store: the free functions that
//! write directly are compiled only for tests, so a production bypass does
//! not build. As a second line of defence the store stats the journal and the
//! snapshot before trusting its position, and any failed write forgets it, so
//! the
//! next operation re-reads the position from disk and repairs a torn tail.

use crate::common::{MidgeError, MidgeResult};
use crate::io::traits::{Fs, FsError, FsPath};
use crate::metadata::accounted_fs::account_fs;
#[cfg(any(test, feature = "internal-testing"))]
use crate::metadata::accounting::MetricsHandle;
use crate::metadata::accounting::{Medium, OperationKind, Origin, Owner};
use crate::metadata::journal::{self, ManifestEdit};
use crate::metadata::persistence::{JournalPosition, WrittenCheckpoint};
use crate::metadata::{Manifest, ManifestPersistence};
use std::sync::atomic::{AtomicBool, Ordering};
use std::sync::Arc;

/// The journal position the store last wrote, and the file lengths that
/// write left behind. A length that moved means another writer touched the
/// files, so the position is re-read from disk.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
struct KnownPosition {
    position: JournalPosition,
    lengths: FileLengths,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
struct FileLengths {
    journal: u64,
    snapshot: u64,
}

pub(crate) struct DdlAppendFailure {
    pub(crate) error: MidgeError,
    pub(crate) may_have_written: bool,
}

// Candidate bounds for #752. One bounded flush record may cross the byte
// trigger; the next append must first checkpoint successfully. Forced
// publications continue to checkpoint on every call.
pub(crate) const LOCAL_CHECKPOINT_EDITS: u64 = 16;
pub(crate) const LOCAL_CHECKPOINT_BYTES: u64 = 16_384;
pub(crate) const LOCAL_FLUSH_RECORD_BYTES: usize = 4_096;

/// Owns the manifest journal and snapshot of one open database.
pub(crate) struct ManifestStore {
    fs: Arc<dyn Fs>,
    known: parking_lot::Mutex<Option<KnownPosition>>,
    // A frontier fallback may refresh `known` after a failed snapshot. Only
    // a successful checkpoint can release publication backpressure.
    checkpoint_retry_required: AtomicBool,
    accounting: Owner,
    medium: Medium,
}

impl std::fmt::Debug for ManifestStore {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("ManifestStore")
            .field("known", &*self.known.lock())
            .finish_non_exhaustive()
    }
}

impl ManifestStore {
    /// Cheap, already checkpointed flushes can retain the original forced
    /// path. Append still checks file lengths under the writer lock and the
    /// following snapshot reconciles any intervening edits. Unknown, stale
    /// or deferred cached authority must take the full checkpoint preflight.
    pub(crate) fn can_use_forced_flush_path(&self, applied_edit_id: u64, sst_bytes: u64) -> bool {
        self.known.lock().is_some_and(|cached| {
            !self.checkpoint_retry_required.load(Ordering::Acquire)
                && cached.lengths.snapshot > 0
                && cached.lengths.journal == 0
                && cached.position.checkpoint_edit_id == cached.position.highest_edit_id
                && cached.position.highest_edit_id == applied_edit_id
                && u128::from(cached.lengths.snapshot) * 20 < u128::from(sst_bytes)
        })
    }

    /// Use the last successful checkpoint's payload, not an uncharged JSON
    /// serialization, to apply #715's predeclared five-percent byte-cost gate.
    /// Unknown authority cannot establish a cost justification for deferral.
    pub(crate) fn local_checkpoint_is_costly(&self, sst_bytes: u64) -> bool {
        self.known.lock().is_some_and(|cached| {
            sst_bytes > 0 && u128::from(cached.lengths.snapshot) * 20 >= u128::from(sst_bytes)
        })
    }

    /// Unknown, externally changed or stale authority must never defer a
    /// checkpoint. This query only stats files; it does not repair/replay them.
    pub(crate) fn local_checkpoint_due(&self, applied_edit_id: u64) -> bool {
        journal::with_manifest_writer_lock(&self.fs, || {
            if self.checkpoint_retry_required.load(Ordering::Acquire) {
                return true;
            }
            let known = self.known.lock();
            let Some(cached) = *known else {
                return true;
            };
            if Self::lengths_with_fs(&self.fs).ok() != Some(cached.lengths)
                || applied_edit_id != cached.position.highest_edit_id
            {
                return true;
            }
            cached
                .position
                .highest_edit_id
                .saturating_sub(cached.position.checkpoint_edit_id)
                >= LOCAL_CHECKPOINT_EDITS
                || cached.lengths.journal >= LOCAL_CHECKPOINT_BYTES
        })
    }

    #[cfg(any(test, feature = "internal-testing"))]
    pub(crate) fn new(fs: Arc<dyn Fs>) -> Self {
        Self::new_with_accounting(fs, Owner::new(), Medium::MemoryOnly)
    }

    #[cfg(any(test, feature = "internal-testing"))]
    pub(crate) fn append(&self, edit: &ManifestEdit) -> MidgeResult<u64> {
        self.append_for(Origin::Unclassified, edit)
    }

    #[cfg(test)]
    pub(crate) fn append_batch(&self, edits: &[ManifestEdit]) -> MidgeResult<u64> {
        self.append_batch_for(Origin::Unclassified, edits)
    }

    #[cfg(test)]
    pub(crate) fn save_snapshot(&self, manifest: &Manifest) -> MidgeResult<WrittenCheckpoint> {
        self.save_snapshot_for(Origin::Unclassified, manifest)
    }

    pub(crate) fn new_with_accounting(fs: Arc<dyn Fs>, accounting: Owner, medium: Medium) -> Self {
        Self {
            fs,
            accounting,
            medium,
            known: parking_lot::Mutex::new(None),
            checkpoint_retry_required: AtomicBool::new(false),
        }
    }

    #[cfg(any(test, feature = "internal-testing"))]
    pub(crate) fn accounting_handle(&self) -> MetricsHandle {
        self.accounting.handle()
    }
    pub(crate) fn accounting_owner(&self) -> &Owner {
        &self.accounting
    }
    pub(crate) const fn accounting_medium(&self) -> Medium {
        self.medium
    }

    pub(crate) fn append_for(&self, origin: Origin, edit: &ManifestEdit) -> MidgeResult<u64> {
        self.append_for_with_progress(origin, edit, None)
    }

    pub(crate) fn append_ddl_for(
        &self,
        origin: Origin,
        edit: &ManifestEdit,
    ) -> Result<u64, DdlAppendFailure> {
        let write_started = std::cell::Cell::new(false);
        self.append_for_with_progress(origin, edit, Some(&write_started))
            .map_err(|error| DdlAppendFailure {
                error,
                may_have_written: write_started.get(),
            })
    }

    fn append_for_with_progress(
        &self,
        origin: Origin,
        edit: &ManifestEdit,
        write_started: Option<&std::cell::Cell<bool>>,
    ) -> MidgeResult<u64> {
        let operation = self
            .accounting
            .begin(OperationKind::JournalAppend, origin, self.medium);
        let fs = account_fs(Arc::clone(&self.fs), operation.ledger());
        let result = edit.validate_for_append().and_then(|()| {
            self.write_next_with_fs(&fs, |fs, edit_id| {
                journal::append_validated_edit_with_id_observed(
                    fs,
                    edit,
                    edit_id,
                    Some(&operation),
                    write_started,
                )
            })
        });
        operation.finish(result.is_ok());
        result
    }

    pub(crate) fn append_batch_for(
        &self,
        origin: Origin,
        edits: &[ManifestEdit],
    ) -> MidgeResult<u64> {
        let operation = self
            .accounting
            .begin(OperationKind::JournalAppend, origin, self.medium);
        let fs = account_fs(Arc::clone(&self.fs), operation.ledger());
        let result = ManifestEdit::Batch(edits.to_vec())
            .validate_for_append()
            .and_then(|()| {
                self.write_next_with_fs(&fs, |fs, edit_id| {
                    journal::append_validated_edit_batch_with_id_observed(
                        fs,
                        edits,
                        edit_id,
                        Some(&operation),
                        None,
                    )
                })
            });
        operation.finish(result.is_ok());
        result
    }

    pub(crate) fn save_snapshot_for(
        &self,
        origin: Origin,
        manifest: &Manifest,
    ) -> MidgeResult<WrittenCheckpoint> {
        let operation = self
            .accounting
            .begin(OperationKind::Checkpoint, origin, self.medium);
        let fs = account_fs(Arc::clone(&self.fs), operation.ledger());
        let result = journal::with_manifest_writer_lock(&fs, || {
            let mut known = self.known.lock();
            let result = Self::position_with_fs(&fs, &mut known).and_then(|position| {
                ManifestPersistence::save_snapshot_unlocked_observed(
                    &fs,
                    manifest,
                    Some(position),
                    Some(&operation),
                )
            });
            *known = match &result {
                Ok(written) => Self::remember_with_fs(
                    &fs,
                    JournalPosition {
                        checkpoint_edit_id: written.edit_checkpoint_id,
                        highest_edit_id: written.edit_checkpoint_id,
                    },
                ),
                Err(_) => None,
            };
            self.checkpoint_retry_required
                .store(result.is_err(), Ordering::Release);
            result
        });
        operation.finish(result.is_ok());
        result
    }

    /// Encode an immutable, already applied view off the runtime thread, then
    /// install exactly its cut while retaining every later durable edit.
    pub(crate) fn save_captured_snapshot_for(
        &self,
        origin: Origin,
        manifest: &Manifest,
    ) -> MidgeResult<WrittenCheckpoint> {
        manifest.validate_persisted()?;
        let bytes = super::codec::encode(super::codec::MANIFEST, manifest)?;
        crate::failpoints::fail_point!("midge::manifest::after_captured_checkpoint_encoded");
        let operation = self
            .accounting
            .begin(OperationKind::Checkpoint, origin, self.medium);
        let fs = account_fs(Arc::clone(&self.fs), operation.ledger());
        let result = journal::with_manifest_writer_lock(&fs, || {
            self.install_captured_snapshot(&fs, manifest.edit_checkpoint_id, &bytes, &operation)
        });
        self.checkpoint_retry_required
            .store(result.is_err(), Ordering::Release);
        operation.finish(result.is_ok());
        result
    }

    fn install_captured_snapshot(
        &self,
        fs: &Arc<dyn Fs>,
        cut: u64,
        bytes: &[u8],
        operation: &crate::metadata::accounting::Operation,
    ) -> MidgeResult<WrittenCheckpoint> {
        let mut known = self.known.lock();
        let position = Self::position_with_fs(fs, &mut known)?;
        if position.checkpoint_edit_id > cut {
            return Ok(WrittenCheckpoint {
                edit_checkpoint_id: position.checkpoint_edit_id,
                caller_was_current: false,
            });
        }
        if cut > position.highest_edit_id {
            return Err(crate::common::MidgeError::Corruption(
                "captured manifest cut exceeds durable journal authority".into(),
            ));
        }
        let tail = journal::replay_identified_edits_after_with_fs_unlocked(fs, cut)?;
        let result = (|| {
            crate::io::staging::stage_bytes_typed(
                fs,
                &FsPath::new("manifest.snapshot.json.tmp"),
                &FsPath::new(crate::metadata::files::MANIFEST_SNAPSHOT),
                bytes,
                || {
                    crate::failpoints::fail_point!(
                        "midge::manifest::inject_no_space_on_checkpoint_save",
                        |_| Err(crate::common::MidgeError::NoSpace(
                            "failpoint: no space while saving manifest checkpoint".into()
                        ))
                    );
                    Ok(())
                },
            )?;
            operation.snapshot_durable(u64::try_from(bytes.len()).unwrap_or(u64::MAX));
            crate::failpoints::fail_point!(
                "midge::manifest::after_snapshot_rename_before_journal_truncate",
                |_| Err(crate::common::MidgeError::Internal(
                    "failpoint: crash after manifest snapshot rename".into()
                ))
            );
            journal::retain_edits_after_with_fs_unlocked(fs, &tail)?;
            operation.checkpoint_complete();
            Ok(WrittenCheckpoint {
                edit_checkpoint_id: cut,
                caller_was_current: true,
            })
        })();
        *known = result.as_ref().ok().and_then(|_| {
            Self::remember_with_fs(
                fs,
                JournalPosition {
                    checkpoint_edit_id: cut,
                    ..position
                },
            )
        });
        result
    }

    fn write_next_with_fs(
        &self,
        fs: &Arc<dyn Fs>,
        write: impl FnOnce(&Arc<dyn Fs>, u64) -> MidgeResult<u64>,
    ) -> MidgeResult<u64> {
        journal::with_manifest_writer_lock(fs, || {
            let mut known = self.known.lock();
            let position = Self::position_with_fs(fs, &mut known)?;
            let edit_id = journal::next_edit_id_after(position.highest_edit_id)?;
            let result = write(fs, edit_id);
            *known = match result {
                Ok(_) => Self::remember_with_fs(
                    fs,
                    JournalPosition {
                        highest_edit_id: edit_id,
                        ..position
                    },
                ),
                Err(_) => None,
            };
            result
        })
    }

    // Position replay can rewrite a torn journal's valid prefix. Use the same
    // observed filesystem for positioning and writes so repair payload remains
    // attributable to this operation.
    fn position_with_fs(
        fs: &Arc<dyn Fs>,
        known: &mut Option<KnownPosition>,
    ) -> MidgeResult<JournalPosition> {
        let lengths = Self::lengths_with_fs(fs)?;
        if let Some(cached) = *known {
            if cached.lengths == lengths {
                return Ok(cached.position);
            }
            tracing::warn!(expected = ?cached.lengths, actual = ?lengths,
                "manifest files changed outside the store; re-reading the journal position");
        }
        // Positioning also stages a genuine partial-EOF repair. Reading the
        // highest identity does not allocate a successor, so a saturated
        // journal remains readable and checkpointable.
        let highest_edit_id = journal::highest_edit_id_with_repair_with_fs_unlocked(fs)?;
        let position = JournalPosition {
            checkpoint_edit_id: journal::checkpoint_edit_id_with_fs(fs)?,
            highest_edit_id,
        };
        *known = Some(KnownPosition {
            position,
            lengths: Self::lengths_with_fs(fs)?,
        });
        Ok(position)
    }

    fn remember_with_fs(fs: &Arc<dyn Fs>, position: JournalPosition) -> Option<KnownPosition> {
        match Self::lengths_with_fs(fs) {
            Ok(lengths) => Some(KnownPosition { position, lengths }),
            Err(error) => {
                // Preserve the existing successful durable-write result: a failed
                // stat only forgets cache state, never changes it into an error.
                tracing::warn!(%error, "cannot stat manifest files after a write; forgetting position");
                None
            }
        }
    }

    fn lengths_with_fs(fs: &Arc<dyn Fs>) -> MidgeResult<FileLengths> {
        Ok(FileLengths {
            journal: Self::file_len_with_fs(fs, crate::metadata::files::JOURNAL)?,
            snapshot: Self::file_len_with_fs(fs, crate::metadata::files::MANIFEST_SNAPSHOT)?,
        })
    }

    fn file_len_with_fs(fs: &Arc<dyn Fs>, name: &str) -> MidgeResult<u64> {
        match fs.metadata(&FsPath::new(name)) {
            Ok(metadata) => Ok(metadata.len),
            Err(FsError::NotFound(_)) => Ok(0),
            Err(error) => Err(error.into_midge()),
        }
    }
}

#[cfg(test)]
mod tests;
