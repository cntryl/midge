//! Which WAL files are history (#808, #842).
//!
//! The segment ledger (`wal/LEDGER`) is the only authority: its members, in
//! order, are the files recovery replays, its open segment is the one the
//! writer appends to, and it hands out segment ids. Files the ledger does not
//! name are orphans: a segment created by a rotation that crashed before
//! recording it, or one retired below `log_start` whose unlink failed or
//! was undone. Nothing reads them; [`WalHistory::sweep_orphans`] removes them
//! and is the only place that lists `wal/`.
//!
//! Memory mode keeps the same ledger in memory (#851), with no segment files
//! behind it, so the ledger is the one open-segment authority in every mode.

use crate::common::{MidgeError, MidgeResult};
use crate::io::{Durability, Fs, FsError, FsPath, OpenMode, OpenOptions};
use crate::wal::ledger::{LedgerEdit, SegmentId, WalLedger};
use crate::wal::ledger_store::{LedgerStore, LEDGER_FILE};
use crate::wal::recovery::ReplaySegments;
use std::sync::Arc;

/// Where a [`WalHistory`] persists its ledger edits.
pub(crate) trait LedgerJournal: Send {
    /// The ledger as persisted.
    fn ledger(&self) -> &WalLedger;

    /// Make `edit` durable, then apply it; an edit that does not apply is
    /// refused unchanged.
    fn append(&mut self, edit: &LedgerEdit) -> MidgeResult<()>;

    /// Make the journal describe the ledger again after a failed write.
    fn heal(&mut self) -> MidgeResult<()>;
}

impl LedgerJournal for LedgerStore {
    fn ledger(&self) -> &WalLedger {
        LedgerStore::ledger(self)
    }

    fn append(&mut self, edit: &LedgerEdit) -> MidgeResult<()> {
        LedgerStore::append(self, edit)
    }

    fn heal(&mut self) -> MidgeResult<()> {
        LedgerStore::heal(self)
    }
}

/// The ledger of a memory-mode WAL, which persists nothing.
struct MemoryJournal {
    ledger: WalLedger,
}

impl LedgerJournal for MemoryJournal {
    fn ledger(&self) -> &WalLedger {
        &self.ledger
    }

    fn append(&mut self, edit: &LedgerEdit) -> MidgeResult<()> {
        let mut next = self.ledger.clone();
        next.apply(edit)?;
        self.ledger = next;
        Ok(())
    }

    fn heal(&mut self) -> MidgeResult<()> {
        Ok(())
    }
}

/// The ledger this process writes through, with the WAL directory it
/// governs; a memory-mode history has no directory.
pub(crate) struct WalHistory {
    segments: Option<Arc<dyn Fs>>,
    journal: Box<dyn LedgerJournal>,
}

impl std::fmt::Debug for WalHistory {
    fn fmt(&self, formatter: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        formatter
            .debug_struct("WalHistory")
            .field("ledger", self.journal.ledger())
            .field("files", &self.segments.is_some())
            .finish_non_exhaustive()
    }
}

impl WalHistory {
    /// The ledger as stored in `wal`, without changing anything: for epoch
    /// discovery and verification, which do not hold the lease. `None`
    /// before the first open.
    ///
    /// # Errors
    ///
    /// Fails, as the writable open would, when segment files hold data but
    /// no ledger names them.
    pub(crate) fn read(wal: &Arc<dyn Fs>) -> MidgeResult<Option<WalLedger>> {
        if let Some((ledger, _tail)) = LedgerStore::load(wal)? {
            return Ok(Some(ledger));
        }
        for (_, name) in segment_files(wal.as_ref())? {
            if !is_empty(wal.as_ref(), &name)? {
                return Err(unrecorded_history(&name));
            }
        }
        Ok(None)
    }

    /// Open the ledger for writing, under the lease, and sweep the files it
    /// does not name. A database without one gets one whose first open
    /// segment is `first_open`.
    pub(crate) fn open(wal: Arc<dyn Fs>, first_open: SegmentId) -> MidgeResult<Self> {
        let history = match LedgerStore::open(Arc::clone(&wal))? {
            Some(store) => Self {
                segments: Some(wal),
                journal: Box::new(store),
            },
            None => Self::genesis(wal, first_open)?,
        };
        history.sweep_orphans()?;
        Ok(history)
    }

    /// The history of a memory-mode WAL: a ledger with no files, whose first
    /// open segment is 1.
    pub(crate) fn in_memory() -> Self {
        let ledger = WalLedger::genesis(uuid::Uuid::new_v4().to_string(), 1)
            .expect("a new holder identity and segment 1 make a valid ledger");
        Self {
            segments: None,
            journal: Box::new(MemoryJournal { ledger }),
        }
    }

    /// Start the history of a new database: create its first segment, then
    /// record it. A crash between the two leaves an empty segment and no
    /// ledger, and the next open starts again.
    fn genesis(wal: Arc<dyn Fs>, first_open: SegmentId) -> MidgeResult<Self> {
        wal.create_dir_all(&FsPath::new(""))
            .map_err(FsError::into_midge)?;
        // Empty files are what an interrupted genesis or rotation leaves;
        // the sweep after genesis removes them. Data is history this ledger
        // never knew.
        for (_, name) in segment_files(wal.as_ref())? {
            if !is_empty(wal.as_ref(), &name)? {
                return Err(unrecorded_history(&name));
            }
        }
        create_segment(wal.as_ref(), first_open)?;
        let ledger = WalLedger::genesis(uuid::Uuid::new_v4().to_string(), first_open)?;
        let store = LedgerStore::create(Arc::clone(&wal), &ledger)?;
        Ok(Self {
            segments: Some(wal),
            journal: Box::new(store),
        })
    }

    pub(crate) fn ledger(&self) -> &WalLedger {
        self.journal.ledger()
    }

    /// The WAL directory this history governs; `None` in memory mode.
    pub(crate) fn segment_files(&self) -> Option<Arc<dyn Fs>> {
        self.segments.clone()
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
        self.journal.append(edit)
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
        self.journal.heal()?;
        let ledger = self.ledger();
        let (seal, open) = (ledger.open_segment(), ledger.next_segment_id());
        if let Some(wal) = &self.segments {
            let open_file = FsPath::new(crate::wal::segment_file_name(seal));
            if !wal.exists(&open_file).map_err(FsError::into_midge)? {
                return Err(MidgeError::RecoveryFailed(format!(
                    "open WAL segment {seal} disappeared; the records appended to it are lost"
                )));
            }
            create_segment(wal.as_ref(), open)?;
        }
        Ok(PreparedRotation { seal, open })
    }

    /// The irreversible half of a rotation: record `prepared`'s segment
    /// sealed and the next one open.
    ///
    /// # Errors
    ///
    /// The ledger write error; the rotation may or may not have become
    /// durable.
    #[allow(clippy::needless_pass_by_value)] // Consuming it records each prepared rotation once.
    pub(crate) fn record_rotation(&mut self, prepared: PreparedRotation) -> MidgeResult<()> {
        self.commit(&LedgerEdit::Rotate {
            seal: prepared.seal,
            open: prepared.open,
        })
    }

    /// Make the sequence reservation reach `through` (#843): every sequence
    /// up to it may be handed out once this returns.
    ///
    /// # Errors
    ///
    /// The ledger's refusal or write error; the reservation may or may not
    /// have become durable, so the caller hands out nothing past the old one.
    pub(crate) fn reserve(&mut self, through: u64) -> MidgeResult<()> {
        self.commit(&LedgerEdit::Reserve { through })
    }

    /// Remove segment files the ledger does not name. Members are never
    /// touched.
    fn sweep_orphans(&self) -> MidgeResult<usize> {
        let Some(wal) = &self.segments else {
            return Ok(0);
        };
        let ledger = self.ledger();
        let mut removed = 0;
        for (id, name) in segment_files(wal.as_ref())? {
            if ledger.is_member(id) {
                continue;
            }
            if id >= ledger.next_segment_id() && !is_empty(wal.as_ref(), &name)? {
                // A non-empty file above every id the ledger handed out was
                // written by a writer this ledger never knew: keep it for an
                // operator rather than delete what may be data.
                return Err(MidgeError::Corruption(format!(
                    "WAL segment {name} holds data the WAL ledger never recorded"
                )));
            }
            wal.remove_file(&FsPath::new(&name))
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
        let Some(wal) = self.segments.clone() else {
            self.commit(&LedgerEdit::AdvanceLogStart { to })?;
            return Ok(RetiredWal::default());
        };
        let members: Vec<(SegmentId, u64)> = self
            .ledger()
            .members()
            .take_while(|id| *id < to)
            .map(|id| {
                let name = FsPath::new(crate::wal::segment_file_name(id));
                let bytes = wal.metadata(&name).map_or(0, |metadata| metadata.len);
                (id, bytes)
            })
            .collect();
        self.commit(&LedgerEdit::AdvanceLogStart { to })?;
        let mut retired = RetiredWal::default();
        for (id, bytes) in members {
            match wal.remove_file(&FsPath::new(crate::wal::segment_file_name(id))) {
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
    /// What salvage drops is kept first, in a new `salvaged-…/` directory
    /// nothing reads: a copy of the cut segment, which stays history, and the
    /// dropped members themselves, moved there in ascending order. Then the
    /// cut segment is cut back to the prefix replay applied, the next segment
    /// is created, and the ledger records the set-aside. A crash before the
    /// ledger records it leaves the ledger naming members whose files moved,
    /// which the next salvage sets aside from the same point; after it, only
    /// the orphan sweep remains, and any open repeats it.
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
        let dropped: Vec<SegmentId> = ledger.members().filter(|id| *id >= from).collect();
        let open = ledger.next_segment_id().max(open_at_least);
        let wal = self.segments.clone().ok_or_else(|| {
            MidgeError::Internal("a memory-mode WAL has nothing to set aside".to_string())
        })?;
        let aside = keep_aside(wal.as_ref(), cut.map(|cut| cut.segment), &dropped)?;
        if let Some(cut) = cut {
            crate::io::truncate_durably(
                wal.as_ref(),
                &FsPath::new(crate::wal::segment_file_name(cut.segment)),
                cut.valid_bytes,
            )
            .map_err(FsError::into_midge)?;
        }
        create_segment(wal.as_ref(), open)?;
        self.commit(&LedgerEdit::SetAside { from, open })?;
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

/// Keep what salvage drops in a new directory of `wal`: a durable copy of
/// `cut`, then each of `dropped` moved there. A dropped member whose file is
/// already gone has nothing to keep.
fn keep_aside(wal: &dyn Fs, cut: Option<SegmentId>, dropped: &[SegmentId]) -> MidgeResult<FsPath> {
    let aside = unused_aside_directory(wal)?;
    wal.create_dir_all(&aside).map_err(FsError::into_midge)?;
    if let Some(id) = cut {
        let name = crate::wal::segment_file_name(id);
        crate::io::copy_durably(wal, &FsPath::new(&name), &join(&aside, &name))
            .map_err(FsError::into_midge)?;
    }
    for id in dropped {
        let name = FsPath::new(crate::wal::segment_file_name(*id));
        if wal.exists(&name).map_err(FsError::into_midge)? {
            wal.rename_atomic(&name, &join(&aside, &name.0))
                .map_err(FsError::into_midge)?;
            crate::failpoints::fail_point!("midge::recovery::after_salvage_member_moved");
        }
    }
    wal.sync_dir(&aside, Durability::Durable)
        .map_err(FsError::into_midge)?;
    wal.sync_dir(&FsPath::new(""), Durability::Durable)
        .map_err(FsError::into_midge)?;
    Ok(aside)
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

fn unrecorded_history(name: &str) -> MidgeError {
    MidgeError::RecoveryFailed(format!(
        "WAL segment {name} exists without a WAL ledger; refusing to start a new history over it"
    ))
}

/// A `salvaged-<millis>[-n]/` directory name that does not exist yet.
fn unused_aside_directory(wal: &dyn Fs) -> MidgeResult<FsPath> {
    let millis = std::time::SystemTime::now()
        .duration_since(std::time::UNIX_EPOCH)
        .map_or(0, |elapsed| elapsed.as_millis());
    for attempt in 0_u32.. {
        let name = if attempt == 0 {
            format!("salvaged-{millis}")
        } else {
            format!("salvaged-{millis}-{attempt}")
        };
        let path = FsPath::new(name);
        if !wal.exists(&path).map_err(FsError::into_midge)? {
            return Ok(path);
        }
    }
    unreachable!("some salvage directory name below u32::MAX attempts is free")
}

/// The files `ledger` replays, relative to `wal_dir`: sealed members in
/// order, then the open segment.
pub(crate) fn replay_segments(ledger: &WalLedger, wal_dir: &FsPath) -> ReplaySegments {
    let path = |id| join(wal_dir, &crate::wal::segment_file_name(id));
    ReplaySegments::new(ledger.sealed().map(path), Some(path(ledger.open_segment())))
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
        // Only canonical names are segments; anything else is left alone.
        .filter_map(|entry| {
            Some((
                crate::wal::parse_segment_file_name(&entry.name)?,
                entry.name,
            ))
        })
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
