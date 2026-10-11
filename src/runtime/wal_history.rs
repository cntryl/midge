//! Which WAL files are history (#808, #842).
//!
//! The segment ledger (`wal/LEDGER`) is the only authority: its members, in
//! order, are the files recovery replays, its open segment is the one the
//! writer appends to, and it hands out segment ids. Files the ledger does not
//! name are orphans: a segment created by a rotation that crashed before
//! recording it, or one retired below `log_start` whose unlink failed or
//! was undone. Nothing reads them; [`WalHistory::sweep_orphans`] removes them
//! and is the only place that lists `wal/`.

use crate::common::{MidgeError, MidgeResult};
use crate::io::{Durability, Fs, FsError, FsPath, OpenMode, OpenOptions};
use crate::wal::ledger::{LedgerEdit, SegmentId, WalLedger};
use crate::wal::ledger_store::{LedgerStore, LEDGER_FILE};
use crate::wal::recovery::ReplaySegments;
use std::sync::Arc;

/// The ledger this process writes through, with the WAL directory it
/// governs.
pub(crate) struct WalHistory {
    wal: Arc<dyn Fs>,
    store: LedgerStore,
}

impl std::fmt::Debug for WalHistory {
    fn fmt(&self, formatter: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        formatter
            .debug_struct("WalHistory")
            .field("ledger", self.store.ledger())
            .finish_non_exhaustive()
    }
}

impl WalHistory {
    /// The ledger as stored in `wal`, without changing anything: for epoch
    /// discovery and verification, which do not hold the lease. `None`
    /// before the first open.
    pub(crate) fn read(wal: &Arc<dyn Fs>) -> MidgeResult<Option<WalLedger>> {
        Ok(LedgerStore::load(wal)?.map(|(ledger, _tail)| ledger))
    }

    /// Open the ledger for writing, under the lease. A database without one
    /// gets one whose first open segment is `first_open`.
    pub(crate) fn open(wal: Arc<dyn Fs>, first_open: SegmentId) -> MidgeResult<Self> {
        match LedgerStore::open(Arc::clone(&wal))? {
            Some(store) => Ok(Self { wal, store }),
            None => Self::genesis(wal, first_open),
        }
    }

    /// Start the history of a new database: create its first segment, then
    /// record it. A crash between the two leaves an empty segment and no
    /// ledger, and the next open starts again.
    fn genesis(wal: Arc<dyn Fs>, first_open: SegmentId) -> MidgeResult<Self> {
        wal.create_dir_all(&FsPath::new(""))
            .map_err(FsError::into_midge)?;
        if let Some(name) = segment_files(wal.as_ref())?
            .into_iter()
            .find(|(id, _)| *id != first_open)
            .map(|(_, name)| name)
        {
            return Err(MidgeError::RecoveryFailed(format!(
                "WAL segment {name} exists without a WAL ledger; refusing to start a new history over it"
            )));
        }
        create_segment(wal.as_ref(), first_open)?;
        let mut ledger = WalLedger::empty();
        ledger.apply(&LedgerEdit::Genesis {
            holder: uuid::Uuid::new_v4().to_string(),
            open: first_open,
        })?;
        let store = LedgerStore::create(Arc::clone(&wal), &ledger)?;
        Ok(Self { wal, store })
    }

    pub(crate) fn ledger(&self) -> &WalLedger {
        self.store.ledger()
    }

    /// Make `edit` durable, then apply it. Every edit pairs with its file
    /// effects in the methods below, in the order that survives a crash.
    ///
    /// # Errors
    ///
    /// An invalid edit is refused unchanged. A failed write may or may not
    /// have made the edit durable; the next edit republishes the ledger
    /// first.
    fn commit(&mut self, edit: &LedgerEdit) -> MidgeResult<()> {
        self.store.append(edit)
    }

    /// The reversible half of a rotation: republish the ledger if an earlier
    /// write failed, check that the open segment still holds its history,
    /// then create the next segment, empty and durable. Nothing becomes
    /// history: a crash leaves at most an empty file that the next attempt
    /// adopts.
    ///
    /// # Errors
    ///
    /// `RecoveryFailed` when the open segment's file is gone, `Corruption`
    /// when the next segment already holds data, and the I/O error otherwise.
    pub(crate) fn prepare_rotation(&mut self) -> MidgeResult<PreparedRotation> {
        self.store.heal()?;
        let ledger = self.ledger();
        let (seal, open) = (ledger.open_segment(), ledger.next_segment_id());
        let open_file = FsPath::new(crate::wal::segment_file_name(seal));
        if !self.wal.exists(&open_file).map_err(FsError::into_midge)? {
            return Err(MidgeError::RecoveryFailed(format!(
                "open WAL segment {seal} disappeared; the records appended to it are lost"
            )));
        }
        create_segment(self.wal.as_ref(), open)?;
        Ok(PreparedRotation { seal, open })
    }

    /// The irreversible half of a rotation: record `prepared`'s segment
    /// sealed and the next one open.
    ///
    /// # Errors
    ///
    /// The ledger write error; the rotation may or may not have become
    /// durable.
    pub(crate) fn record_rotation(&mut self, prepared: PreparedRotation) -> MidgeResult<()> {
        self.commit(&LedgerEdit::Rotate {
            seal: prepared.seal,
            open: prepared.open,
        })
    }

    /// Remove segment files the ledger does not name, and staging residue.
    /// Members are never touched.
    pub(crate) fn sweep_orphans(&self) -> MidgeResult<usize> {
        let ledger = self.ledger();
        let mut removed = 0;
        for (id, name) in segment_files(self.wal.as_ref())? {
            if ledger.is_member(id) {
                continue;
            }
            if id >= ledger.next_segment_id() && !is_empty(self.wal.as_ref(), &name)? {
                // A non-empty file above every id the ledger handed out was
                // written by a writer this ledger never knew: keep it for an
                // operator rather than delete what may be data.
                return Err(MidgeError::Corruption(format!(
                    "WAL segment {name} holds data the WAL ledger never recorded"
                )));
            }
            self.wal
                .remove_file(&FsPath::new(&name))
                .map_err(FsError::into_midge)?;
            removed += 1;
        }
        Ok(removed)
    }

    /// Retire every member below `to`: make the new log start durable, then
    /// remove their files. A file that cannot be removed is an orphan
    /// nothing reads, which the next open sweeps.
    ///
    /// # Errors
    ///
    /// Returns an error, removing nothing, if the ledger refuses or cannot
    /// record the new log start.
    pub(crate) fn retire_below(&mut self, to: SegmentId) -> MidgeResult<RetiredWal> {
        let members: Vec<(SegmentId, u64)> = self
            .ledger()
            .members()
            .take_while(|id| *id < to)
            .map(|id| {
                let name = FsPath::new(crate::wal::segment_file_name(id));
                let bytes = self.wal.metadata(&name).map_or(0, |metadata| metadata.len);
                (id, bytes)
            })
            .collect();
        self.commit(&LedgerEdit::AdvanceLogStart { to })?;
        let mut retired = RetiredWal::default();
        for (id, bytes) in members {
            match self
                .wal
                .remove_file(&FsPath::new(crate::wal::segment_file_name(id)))
            {
                Ok(()) | Err(FsError::NotFound(_)) => retired.removed.push((id, bytes)),
                Err(error) => {
                    tracing::warn!(
                        segment_id = id,
                        %error,
                        "left a retired WAL segment for the next open to sweep"
                    );
                    retired.left.push(id);
                }
            }
        }
        Ok(retired)
    }

    /// Salvage: continue history in a new segment, at least `open_at_least`,
    /// without the members `set_aside` names.
    ///
    /// Each affected file is first copied into `salvaged-<millis>/`, which
    /// nothing reads, so a crash at any step loses nothing: until the ledger
    /// records the set-aside every member is still history, and afterwards
    /// the dropped members are orphans that the sweep removes. A cut segment
    /// is truncated only once the ledger no longer replays past it.
    pub(crate) fn set_aside(
        &mut self,
        set_aside: SetAside,
        open_at_least: SegmentId,
    ) -> MidgeResult<()> {
        let ledger = self.ledger();
        let (from, cut) = match set_aside {
            SetAside::From(from) => (from.max(ledger.log_start()), None),
            SetAside::After(cut) => (
                cut.segment.checked_add(1).ok_or_else(|| {
                    MidgeError::ResourceLimit("WAL segment identity space exhausted".into())
                })?,
                Some(cut),
            ),
        };
        let affected: Vec<SegmentId> = cut
            .map(|cut| cut.segment)
            .into_iter()
            .chain(ledger.members().filter(|id| *id >= from))
            .collect();
        let open = ledger.next_segment_id().max(open_at_least);
        let aside = copy_aside(self.wal.as_ref(), &affected)?;
        create_segment(self.wal.as_ref(), open)?;
        self.commit(&LedgerEdit::SetAside { from, open })?;
        if let Some(cut) = cut {
            truncate(self.wal.as_ref(), cut)?;
        }
        self.sweep_orphans()?;
        tracing::warn!(
            aside = %aside.0,
            from,
            open,
            cut = ?cut,
            "set aside WAL that salvage recovery did not replay"
        );
        Ok(())
    }
}

/// What one retirement did. Every segment in it is out of history.
#[derive(Debug, Default, Clone, PartialEq, Eq)]
pub(crate) struct RetiredWal {
    /// Segments whose files are gone, with the bytes each held.
    pub(crate) removed: Vec<(SegmentId, u64)>,
    /// Segments whose files could not be removed; the next open sweeps them.
    pub(crate) left: Vec<SegmentId>,
}

impl RetiredWal {
    /// Bytes the removed files held.
    pub(crate) fn removed_bytes(&self) -> u64 {
        self.removed
            .iter()
            .fold(0, |total, (_, bytes)| total.saturating_add(*bytes))
    }
}

/// A next segment a rotation created, which the ledger has not recorded.
#[must_use]
#[derive(Debug)]
pub(crate) struct PreparedRotation {
    seal: SegmentId,
    open: SegmentId,
}

/// What salvage drops from history.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(crate) enum SetAside {
    /// Every member from this id on; ids below the log start are already
    /// retired, so it starts there at the earliest.
    From(SegmentId),
    /// Every member after the cut's segment, which keeps only the prefix
    /// replay applied.
    After(SalvageCut),
}

/// Where salvage cuts history: the segment holding the first frame replay
/// could not use, and the verified prefix of it that replay applied.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(crate) struct SalvageCut {
    pub(crate) segment: SegmentId,
    pub(crate) valid_bytes: u64,
}

/// Copy `segments` durably into a new `salvaged-<millis>/` directory.
fn copy_aside(wal: &dyn Fs, segments: &[SegmentId]) -> MidgeResult<FsPath> {
    const CHUNK: u64 = 4 << 20;
    let millis = std::time::SystemTime::now()
        .duration_since(std::time::UNIX_EPOCH)
        .map_or(0, |elapsed| elapsed.as_millis());
    let aside = FsPath::new(format!("salvaged-{millis}"));
    wal.create_dir_all(&aside).map_err(FsError::into_midge)?;
    for id in segments {
        let name = crate::wal::segment_file_name(*id);
        let source = wal
            .open(&FsPath::new(&name), read_only())
            .map_err(FsError::into_midge)?;
        let length = source.len().map_err(FsError::into_midge)?;
        let mut copy = wal
            .open(
                &join(&aside, &name),
                OpenOptions {
                    mode: OpenMode::ReadWrite,
                    create: true,
                    create_new: true,
                    truncate: false,
                },
            )
            .map_err(FsError::into_midge)?;
        let mut offset = 0;
        while offset < length {
            let bytes = source
                .read_at(offset, CHUNK.min(length - offset))
                .map_err(FsError::into_midge)?;
            copy.write_at(offset, bytes).map_err(FsError::into_midge)?;
            offset += CHUNK.min(length - offset);
        }
        copy.sync(Durability::Durable)
            .map_err(FsError::into_midge)?;
    }
    wal.sync_dir(&aside, Durability::Durable)
        .map_err(FsError::into_midge)?;
    wal.sync_dir(&FsPath::new(""), Durability::Durable)
        .map_err(FsError::into_midge)?;
    Ok(aside)
}

/// Cut segment `cut.segment` back to its verified prefix, durably.
fn truncate(wal: &dyn Fs, cut: SalvageCut) -> MidgeResult<()> {
    let mut file = wal
        .open(
            &FsPath::new(crate::wal::segment_file_name(cut.segment)),
            OpenOptions {
                mode: OpenMode::ReadWrite,
                create: false,
                create_new: false,
                truncate: false,
            },
        )
        .map_err(FsError::into_midge)?;
    file.truncate(cut.valid_bytes)
        .map_err(FsError::into_midge)?;
    file.sync(Durability::Durable).map_err(FsError::into_midge)
}

fn read_only() -> OpenOptions {
    OpenOptions {
        mode: OpenMode::ReadOnly,
        create: false,
        create_new: false,
        truncate: false,
    }
}

/// The files `ledger` replays, relative to `wal_dir`: sealed members in
/// order, then the open segment.
pub(crate) fn replay_segments(ledger: &WalLedger, wal_dir: &FsPath) -> ReplaySegments {
    let path = |id| join(wal_dir, &crate::wal::segment_file_name(id));
    ReplaySegments::new(
        ledger.sealed().map(path),
        ledger.is_initialized().then(|| path(ledger.open_segment())),
    )
}

/// Create segment `id` empty and durable: the file, then its directory
/// entry. An empty file left by an earlier crash is adopted.
fn create_segment(wal: &dyn Fs, id: SegmentId) -> MidgeResult<()> {
    let name = crate::wal::segment_file_name(id);
    let path = FsPath::new(&name);
    if wal.exists(&path).map_err(FsError::into_midge)? {
        if !is_empty(wal, &name)? {
            return Err(MidgeError::Corruption(format!(
                "WAL segment {name} already holds data the WAL ledger never recorded"
            )));
        }
    } else {
        let mut file = wal
            .open(
                &path,
                OpenOptions {
                    mode: OpenMode::ReadWrite,
                    create: true,
                    create_new: true,
                    truncate: false,
                },
            )
            .map_err(FsError::into_midge)?;
        file.sync(Durability::Durable)
            .map_err(FsError::into_midge)?;
    }
    wal.sync_dir(&FsPath::new(""), Durability::Durable)
        .map_err(FsError::into_midge)
}

fn is_empty(wal: &dyn Fs, name: &str) -> MidgeResult<bool> {
    Ok(wal
        .metadata(&FsPath::new(name))
        .map_err(FsError::into_midge)?
        .len
        == 0)
}

/// Every segment file in `wal`, by id. The only listing of `wal/`.
fn segment_files(wal: &dyn Fs) -> MidgeResult<Vec<(SegmentId, String)>> {
    let entries = match wal.list_dir(&FsPath::new("")) {
        Ok(entries) => entries,
        Err(FsError::NotFound(_)) => return Ok(Vec::new()),
        Err(error) => return Err(error.into_midge()),
    };
    let mut files: Vec<(SegmentId, String)> = entries
        .into_iter()
        .filter(|entry| !entry.is_dir && entry.name != LEDGER_FILE)
        .filter_map(|entry| Some((crate::wal::parse_segment_id(&entry.name)?, entry.name)))
        .collect();
    files.sort();
    Ok(files)
}

fn join(dir: &FsPath, leaf: &str) -> FsPath {
    let base = dir.0.trim_end_matches('/');
    if base.is_empty() {
        FsPath::new(leaf)
    } else {
        FsPath::new(format!("{base}/{leaf}"))
    }
}

#[cfg(test)]
pub(crate) mod fixture;
#[cfg(test)]
mod tests;
