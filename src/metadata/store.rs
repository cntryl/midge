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

use crate::common::MidgeResult;
use crate::io::traits::{Fs, FsError, FsPath};
use crate::metadata::journal::{self, ManifestEdit};
use crate::metadata::persistence::{JournalPosition, WrittenCheckpoint};
use crate::metadata::{Manifest, ManifestPersistence};
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

/// Owns the manifest journal and snapshot of one open database.
pub(crate) struct ManifestStore {
    fs: Arc<dyn Fs>,
    known: parking_lot::Mutex<Option<KnownPosition>>,
}

impl std::fmt::Debug for ManifestStore {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("ManifestStore")
            .field("known", &*self.known.lock())
            .finish_non_exhaustive()
    }
}

impl ManifestStore {
    pub(crate) fn new(fs: Arc<dyn Fs>) -> Self {
        Self {
            fs,
            known: parking_lot::Mutex::new(None),
        }
    }

    /// Journals one edit and returns its edit id.
    pub(crate) fn append(&self, edit: &ManifestEdit) -> MidgeResult<u64> {
        edit.validate_for_append()?;
        self.write_next(|fs, edit_id| journal::append_validated_edit_with_id(fs, edit, edit_id))
    }

    /// Journals `edits` as one record and returns its edit id.
    pub(crate) fn append_batch(&self, edits: &[ManifestEdit]) -> MidgeResult<u64> {
        for edit in edits {
            edit.validate_for_append()?;
        }
        self.write_next(|fs, edit_id| {
            journal::append_validated_edit_batch_with_id(fs, edits, edit_id)
        })
    }

    /// Snapshots `manifest` plus any journaled edit it lacks, then truncates
    /// the journal.
    pub(crate) fn save_snapshot(&self, manifest: &Manifest) -> MidgeResult<WrittenCheckpoint> {
        journal::with_manifest_writer_lock(&self.fs, || {
            let mut known = self.known.lock();
            let position = self.position(&mut known)?;
            let result =
                ManifestPersistence::save_snapshot_unlocked(&self.fs, manifest, Some(position));
            *known = match &result {
                Ok(written) => Some(KnownPosition {
                    position: JournalPosition {
                        checkpoint_edit_id: written.edit_checkpoint_id,
                        highest_edit_id: written.edit_checkpoint_id,
                    },
                    lengths: self.lengths()?,
                }),
                Err(_) => None,
            };
            result
        })
    }

    fn write_next(
        &self,
        write: impl FnOnce(&Arc<dyn Fs>, u64) -> MidgeResult<u64>,
    ) -> MidgeResult<u64> {
        journal::with_manifest_writer_lock(&self.fs, || {
            let mut known = self.known.lock();
            let position = self.position(&mut known)?;
            let edit_id = position.highest_edit_id.saturating_add(1).max(1);
            let result = write(&self.fs, edit_id);
            *known = match result {
                Ok(_) => Some(KnownPosition {
                    position: JournalPosition {
                        highest_edit_id: edit_id,
                        ..position
                    },
                    lengths: self.lengths()?,
                }),
                // The write may have left a torn tail; re-read from disk,
                // which repairs it, before the next write.
                Err(_) => None,
            };
            result
        })
    }

    /// The journal position, from memory when the journal is as the store
    /// left it, otherwise from disk.
    fn position(&self, known: &mut Option<KnownPosition>) -> MidgeResult<JournalPosition> {
        let lengths = self.lengths()?;
        if let Some(cached) = *known {
            if cached.lengths == lengths {
                return Ok(cached.position);
            }
            tracing::warn!(
                expected = ?cached.lengths,
                actual = ?lengths,
                "manifest files changed outside the store; re-reading the journal position"
            );
        }
        // `next_edit_id_with_fs` also repairs a torn journal tail.
        let highest_edit_id = journal::next_edit_id_with_fs(&self.fs)?.saturating_sub(1);
        let position = JournalPosition {
            checkpoint_edit_id: journal::checkpoint_edit_id_with_fs(&self.fs)?,
            highest_edit_id,
        };
        *known = Some(KnownPosition {
            position,
            lengths: self.lengths()?,
        });
        Ok(position)
    }

    fn lengths(&self) -> MidgeResult<FileLengths> {
        Ok(FileLengths {
            journal: self.file_len(crate::metadata::files::JOURNAL)?,
            snapshot: self.file_len(crate::metadata::files::MANIFEST_SNAPSHOT)?,
        })
    }

    fn file_len(&self, name: &str) -> MidgeResult<u64> {
        match self.fs.metadata(&FsPath::new(name)) {
            Ok(metadata) => Ok(metadata.len),
            Err(FsError::NotFound(_)) => Ok(0),
            Err(error) => Err(error.into()),
        }
    }
}

#[cfg(test)]
mod tests;
