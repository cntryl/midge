//! Which WAL files are history (#808, #842, #843).
//!
//! The segment ledger (`wal/LEDGER`) is the only authority: its members, in
//! order, are the files recovery replays, its open segment is the one the
//! writer appends to, and it hands out segment ids and sequence
//! reservations. Files the ledger does not name are orphans: a segment
//! created by a rotation that crashed before recording it, one a `Reopen`
//! replaced, or one retired below `log_start` whose unlink failed or was
//! undone. Nothing reads them; [`WalHistory::sweep_orphans`] removes them and
//! is the only place that lists `wal/`.
//!
//! Every segment starts with a header that links the segment before it
//! (`wal::format`), written and synced before any ledger edit names the
//! segment, and ends, once sealed, with a footer the ledger records.
//!
//! Memory mode keeps the same ledger in memory (#851), with no segment files
//! behind it, so the ledger is the one open-segment authority in every mode.

use crate::common::{MidgeError, MidgeResult};
use crate::io::{Durability, Fs, FsError, FsPath, OpenMode, OpenOptions, ReadFs};
use crate::lease::LeaderStore;
use crate::wal::format::{holds_no_records, ChainLink, SealedFacts, SegmentHeader, SegmentScan};
use crate::wal::ledger::{HolderId, LedgerEdit, OpenFacts, SegmentId, WalLedger};
use crate::wal::ledger_store::{LedgerStore, LEDGER_FILE};
use std::sync::Arc;
use std::time::Duration;

/// Current ownership required for one durable WAL history mutation.
#[derive(Clone)]
pub(crate) struct WalHistoryFence {
    store: Arc<dyn LeaderStore>,
    holder_id: String,
    writer_epoch: u64,
    timeout: Duration,
}

impl WalHistoryFence {
    pub(crate) fn new(
        store: Arc<dyn LeaderStore>,
        holder_id: String,
        writer_epoch: u64,
        timeout: Duration,
    ) -> Self {
        Self {
            store,
            holder_id,
            writer_epoch,
            timeout,
        }
    }
}

fn with_writer_fence<T>(
    fence: Option<&WalHistoryFence>,
    writer_epoch: u64,
    operation: impl FnOnce() -> MidgeResult<T>,
) -> MidgeResult<T> {
    let Some(fence) = fence else {
        return operation();
    };
    if fence.writer_epoch != writer_epoch {
        return Err(MidgeError::Fenced(format!(
            "WAL history epoch changed from fenced epoch {} to {writer_epoch}",
            fence.writer_epoch
        )));
    }

    let mut operation = Some(operation);
    let mut result = None;
    let mut run = || {
        if let Some(operation) = operation.take() {
            result = Some(operation());
        }
    };
    fence
        .store
        .with_writer_epoch(
            &fence.holder_id,
            fence.writer_epoch,
            fence.timeout,
            &mut run,
        )
        .map_err(MidgeError::from)?;
    result.unwrap_or_else(|| {
        Err(MidgeError::Internal(
            "writer authority completed without running its storage effect".into(),
        ))
    })
}

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
/// governs and the writer epoch new segments carry; a memory-mode history
/// has no directory.
pub(crate) struct WalHistory {
    segments: Option<Arc<dyn Fs>>,
    journal: Box<dyn LedgerJournal>,
    epoch: u64,
    fence: Option<WalHistoryFence>,
}

impl std::fmt::Debug for WalHistory {
    fn fmt(&self, formatter: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        formatter
            .debug_struct("WalHistory")
            .field("ledger", self.journal.ledger())
            .field("files", &self.segments.is_some())
            .field("epoch", &self.epoch)
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
    /// Fails, as the writable open would, when segment files hold records
    /// but no ledger names them.
    pub(crate) fn read(wal: &Arc<dyn ReadFs>) -> MidgeResult<Option<WalLedger>> {
        if let Some((ledger, _tail)) = LedgerStore::load(wal.as_ref())? {
            return Ok(Some(ledger));
        }
        for (_, name) in segment_files(wal.as_ref())? {
            if !holds_no_records_named(wal.as_ref(), &name)? {
                return Err(unrecorded_history(&name));
            }
        }
        Ok(None)
    }

    /// Open the ledger for writing, under the lease of `epoch`, and sweep
    /// the files it does not name. A database without one starts a new
    /// history in segment `first_open`.
    pub(crate) fn open(wal: Arc<dyn Fs>, first_open: SegmentId, epoch: u64) -> MidgeResult<Self> {
        Self::open_inner(wal, first_open, epoch, None)
    }

    /// [`Self::open`] bound to the acquired writer lease for all ledger and
    /// segment effects performed during recovery initialization.
    pub(crate) fn open_with_fence(
        wal: Arc<dyn Fs>,
        first_open: SegmentId,
        epoch: u64,
        fence: WalHistoryFence,
    ) -> MidgeResult<Self> {
        with_writer_fence(Some(&fence), epoch, || {
            Self::open_inner(wal, first_open, epoch, Some(fence.clone()))
        })
    }

    fn open_inner(
        wal: Arc<dyn Fs>,
        first_open: SegmentId,
        epoch: u64,
        fence: Option<WalHistoryFence>,
    ) -> MidgeResult<Self> {
        match Self::load_inner(Arc::clone(&wal), epoch, fence.clone())? {
            Some(history) => Ok(history),
            None => Self::genesis_inner(
                wal,
                epoch,
                ChainLink::GENESIS,
                first_open,
                HolderId::new(),
                fence,
            ),
        }
    }

    /// Open the existing ledger in `wal` for writing and sweep the files it
    /// does not name; `None` when there is none.
    pub(crate) fn load(wal: Arc<dyn Fs>, epoch: u64) -> MidgeResult<Option<Self>> {
        Self::load_inner(wal, epoch, None)
    }

    /// [`Self::load`] bound to the acquired writer lease while it republishes
    /// the ledger and sweeps orphan files.
    pub(crate) fn load_with_fence(
        wal: Arc<dyn Fs>,
        epoch: u64,
        fence: WalHistoryFence,
    ) -> MidgeResult<Option<Self>> {
        with_writer_fence(Some(&fence), epoch, || {
            Self::load_inner(wal, epoch, Some(fence.clone()))
        })
    }

    fn load_inner(
        wal: Arc<dyn Fs>,
        epoch: u64,
        fence: Option<WalHistoryFence>,
    ) -> MidgeResult<Option<Self>> {
        let Some(store) = LedgerStore::open(Arc::clone(&wal))? else {
            return Ok(None);
        };
        let history = Self {
            segments: Some(wal),
            journal: Box::new(store),
            epoch,
            fence,
        };
        history.sweep_orphans()?;
        Ok(Some(history))
    }

    /// Start a new history in `wal`, continuing `anchor` (the genesis link,
    /// or a cloud catalog's head for a fresh cache) in segment `open`:
    /// create the segment, then record it. A crash between the two leaves a
    /// segment holding no record and no ledger, and the next open starts
    /// again.
    ///
    /// # Errors
    ///
    /// `RecoveryFailed` when a segment file already holds records no ledger
    /// names, and the I/O or ledger error otherwise.
    pub(crate) fn genesis(
        wal: Arc<dyn Fs>,
        epoch: u64,
        anchor: ChainLink,
        open: SegmentId,
    ) -> MidgeResult<Self> {
        Self::genesis_inner(wal, epoch, anchor, open, HolderId::new(), None)
    }

    /// [`Self::genesis`] bound to the acquired writer lease across creation
    /// of the segment and its authoritative ledger entry.
    pub(crate) fn genesis_with_fence(
        wal: Arc<dyn Fs>,
        epoch: u64,
        anchor: ChainLink,
        open: SegmentId,
        fence: WalHistoryFence,
    ) -> MidgeResult<Self> {
        with_writer_fence(Some(&fence), epoch, || {
            Self::genesis_inner(
                wal,
                epoch,
                anchor,
                open,
                HolderId::new(),
                Some(fence.clone()),
            )
        })
    }

    /// [`Self::genesis`] for a fixed `holder`, so fixtures can build
    /// segments that belong to the ledger.
    #[cfg(test)]
    pub(crate) fn genesis_held_by(
        wal: Arc<dyn Fs>,
        epoch: u64,
        anchor: ChainLink,
        open: SegmentId,
        holder: HolderId,
    ) -> MidgeResult<Self> {
        Self::genesis_inner(wal, epoch, anchor, open, holder, None)
    }

    fn genesis_inner(
        wal: Arc<dyn Fs>,
        epoch: u64,
        anchor: ChainLink,
        open: SegmentId,
        holder: HolderId,
        fence: Option<WalHistoryFence>,
    ) -> MidgeResult<Self> {
        wal.create_dir_all(&FsPath::new(""))
            .map_err(FsError::into_midge)?;
        // Files holding no record are what an interrupted genesis or
        // rotation leaves; the sweep after genesis removes them. Records are
        // history this ledger never knew.
        for (_, name) in segment_files(wal.as_ref())? {
            if !holds_no_records_named(wal.as_ref(), &name)? {
                return Err(unrecorded_history(&name));
            }
        }
        let open = OpenFacts {
            id: open,
            epoch,
            first_seq: anchor.last_seq.saturating_add(1),
            prev: anchor,
        };
        let ledger = WalLedger::genesis(holder, anchor, open)?;
        create_segment(wal.as_ref(), &open.header(holder.bytes()))?;
        let store = LedgerStore::create(Arc::clone(&wal), &ledger)?;
        let history = Self {
            segments: Some(wal),
            journal: Box::new(store),
            epoch,
            fence,
        };
        history.sweep_orphans()?;
        Ok(history)
    }

    /// The history of a memory-mode WAL under `epoch`: a ledger with no
    /// files, whose first open segment is 1.
    pub(crate) fn in_memory(epoch: u64) -> Self {
        let open = OpenFacts {
            id: 1,
            epoch,
            first_seq: 1,
            prev: ChainLink::GENESIS,
        };
        let ledger = WalLedger::genesis(HolderId::new(), ChainLink::GENESIS, open)
            .expect("segment 1 after the genesis link makes a valid ledger");
        Self {
            segments: None,
            journal: Box::new(MemoryJournal { ledger }),
            epoch,
            fence: None,
        }
    }

    pub(crate) fn ledger(&self) -> &WalLedger {
        self.journal.ledger()
    }

    /// The WAL directory this history governs; `None` in memory mode.
    pub(crate) fn segment_files(&self) -> Option<Arc<dyn Fs>> {
        self.segments.clone()
    }

    fn under_writer_fence<T>(
        &mut self,
        operation: impl FnOnce(&mut Self) -> MidgeResult<T>,
    ) -> MidgeResult<T> {
        let fence = self.fence.clone();
        let epoch = self.epoch;
        with_writer_fence(fence.as_ref(), epoch, || operation(self))
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

    /// The header facts of a new segment `id` this process writes.
    fn opening(&self, id: SegmentId, first_seq: u64, prev: ChainLink) -> OpenFacts {
        OpenFacts {
            id,
            epoch: self.epoch,
            first_seq,
            prev,
        }
    }

    /// Create segment `open` with its header, synced, when segment files
    /// back this history.
    fn create(&self, open: &OpenFacts) -> MidgeResult<()> {
        match &self.segments {
            Some(wal) => create_segment(wal.as_ref(), &open.header(self.ledger().holder().bytes())),
            None => Ok(()),
        }
    }

    /// The reversible half of a rotation: republish the ledger if an earlier
    /// write failed, check that the open segment still holds its history,
    /// then create the next segment, linked to `sealed` and starting at
    /// `first_seq`, with its header synced. Nothing becomes history: a crash
    /// leaves at most a segment holding no record, an orphan the next open
    /// sweeps.
    ///
    /// # Errors
    ///
    /// `RecoveryFailed` when the open segment's file is gone, `Corruption`
    /// when the next segment already holds records, `Internal` when `sealed`
    /// does not seal the open segment or `first_seq` does not follow it, and
    /// the I/O error otherwise.
    pub(crate) fn prepare_rotation(
        &mut self,
        sealed: SealedFacts,
        first_seq: u64,
    ) -> MidgeResult<PreparedRotation> {
        self.under_writer_fence(|history| history.prepare_rotation_inner(sealed, first_seq))
    }

    fn prepare_rotation_inner(
        &mut self,
        sealed: SealedFacts,
        first_seq: u64,
    ) -> MidgeResult<PreparedRotation> {
        self.journal.heal()?;
        let ledger = self.ledger();
        let seal = ledger.open_segment();
        let open = self.opening(ledger.next_segment_id(), first_seq, sealed.link);
        let rotation = LedgerEdit::Rotate {
            sealed: sealed.clone(),
            open,
        };
        ledger.check(&rotation).map_err(|error| {
            MidgeError::Internal(format!("cannot rotate WAL segment {seal}: {error}"))
        })?;
        if let Some(wal) = &self.segments {
            let open_file = FsPath::new(crate::wal::segment_file_name(seal));
            if !wal.exists(&open_file).map_err(FsError::into_midge)? {
                return Err(MidgeError::RecoveryFailed(format!(
                    "open WAL segment {seal} disappeared; the records appended to it are lost"
                )));
            }
        }
        self.create(&open)?;
        Ok(PreparedRotation { sealed, open })
    }

    /// The irreversible half of a rotation: record `prepared`'s segment
    /// sealed and the next one open.
    ///
    /// # Errors
    ///
    /// The ledger write error; the rotation may or may not have become
    /// durable.
    pub(crate) fn record_rotation(&mut self, prepared: PreparedRotation) -> MidgeResult<()> {
        self.under_writer_fence(|history| history.record_rotation_inner(prepared))
    }

    fn record_rotation_inner(&mut self, prepared: PreparedRotation) -> MidgeResult<()> {
        let PreparedRotation { sealed, open } = prepared;
        self.commit(&LedgerEdit::Rotate { sealed, open })
    }

    /// Start this process's WAL epoch over the open segment recovery left
    /// (#843), before the writer appends to it. Each step is synced before
    /// the next, and a crash between any two leaves a state the next open
    /// handles the same way:
    ///
    /// - an open segment holding records is sealed: its footer is written
    ///   and synced (only synced when a rotation that crashed already wrote
    ///   it), segment `next` is created linked to it, and `Rotate` records
    ///   both, so the recovered tail is durable before anything is served;
    /// - an open segment holding no record is kept when this epoch created
    ///   it, and otherwise replaced (`Reopen`) by segment `next` under this
    ///   epoch: a segment with no record is never sealed, so it is never
    ///   owed to the cloud.
    ///
    /// The new open segment starts at `first_seq`. Returns the segment
    /// sealed, if any.
    ///
    /// # Errors
    ///
    /// The scan, I/O or ledger error; the open segment's records stay where
    /// they are.
    pub(crate) fn start_epoch(
        &mut self,
        epoch: u64,
        first_seq: u64,
    ) -> MidgeResult<Option<SealedSegment>> {
        self.under_writer_fence(|history| history.start_epoch_inner(epoch, first_seq))
    }

    fn start_epoch_inner(
        &mut self,
        epoch: u64,
        first_seq: u64,
    ) -> MidgeResult<Option<SealedSegment>> {
        self.journal.heal()?;
        self.epoch = epoch;
        let member = self.ledger().open_member().open;
        let next = self.ledger().next_segment_id();
        let scan = match &self.segments {
            Some(wal) => Some(scan_member(wal.as_ref(), member.id)?),
            None => None,
        };
        let records = scan.as_ref().map_or(0, |scan| scan.tail().record_count);
        if records == 0 {
            let has_footer = scan.as_ref().is_some_and(|scan| scan.footer().is_some());
            if member.epoch == epoch && !has_footer {
                return Ok(None);
            }
            let open = self.opening(next, first_seq.max(member.first_seq), member.prev);
            self.create(&open)?;
            self.commit(&LedgerEdit::Reopen { open })?;
            self.sweep_orphans()?;
            return Ok(None);
        }
        let (Some(wal), Some(scan)) = (self.segments.clone(), scan) else {
            unreachable!("only a file-backed history scans records")
        };
        let facts = seal_scanned(wal.as_ref(), member.id, &scan)?;
        let open = self.opening(next, first_seq, facts.link);
        self.create(&open)?;
        self.commit(&LedgerEdit::Rotate {
            sealed: facts.clone(),
            open,
        })?;
        Ok(Some(SealedSegment {
            id: member.id,
            epoch: member.epoch,
            facts,
        }))
    }

    /// Make the sequence reservation reach `through` (#843): every sequence
    /// up to it may be handed out once this returns.
    ///
    /// # Errors
    ///
    /// The ledger's refusal or write error; the reservation may or may not
    /// have become durable, so the caller hands out nothing past the old one.
    pub(crate) fn reserve(&mut self, through: u64) -> MidgeResult<()> {
        self.under_writer_fence(|history| history.commit(&LedgerEdit::Reserve { through }))
    }

    /// Persist evidence of a completed WAL fsync before acknowledging it.
    pub(crate) fn record_sync(&mut self, through_bytes: u64) -> MidgeResult<()> {
        self.under_writer_fence(|history| history.record_sync_inner(through_bytes))
    }

    fn record_sync_inner(&mut self, through_bytes: u64) -> MidgeResult<()> {
        if self
            .ledger()
            .open_member()
            .durable_bytes
            .is_some_and(|bytes| through_bytes <= bytes)
        {
            return Ok(());
        }
        self.commit(&LedgerEdit::SyncOpen {
            id: self.ledger().open_segment(),
            through_bytes,
        })
    }

    /// Record a salvage cut only after shared authority committed its floor.
    pub(crate) fn prepare_open_cut(
        &mut self,
        cut: crate::wal::planner::OpenCut,
        floor: u64,
    ) -> MidgeResult<()> {
        self.under_writer_fence(|history| {
            history.commit(&LedgerEdit::SalvageOpen {
                id: cut.id,
                valid_bytes: cut.valid_bytes,
                floor,
            })
        })
    }

    /// Continue from `anchor`, a history another writer published past this
    /// one (cloud failback): every member is kept aside, a new segment at
    /// least `open_at_least` is created linked to `anchor`, and `Rebase`
    /// records it. A crash before `Rebase` leaves the ledger naming members
    /// whose files moved; the next open finds the same failback.
    ///
    /// # Errors
    ///
    /// The I/O or ledger error.
    pub(crate) fn rebase(
        &mut self,
        anchor: ChainLink,
        open_at_least: SegmentId,
    ) -> MidgeResult<()> {
        self.under_writer_fence(|history| history.rebase_inner(anchor, open_at_least))
    }

    fn rebase_inner(&mut self, anchor: ChainLink, open_at_least: SegmentId) -> MidgeResult<()> {
        let wal = self.files_for("rebase")?;
        let ledger = self.ledger();
        let dropped: Vec<SegmentId> = ledger.members().collect();
        let open = self.opening(
            ledger.next_segment_id().max(open_at_least),
            anchor.last_seq.saturating_add(1),
            anchor,
        );
        let aside = keep_aside(wal.as_ref(), None, &dropped)?;
        self.create(&open)?;
        self.commit(&LedgerEdit::Rebase { anchor, open })?;
        self.sweep_orphans()?;
        tracing::warn!(
            aside = %aside.0,
            anchor = anchor.id,
            open = open.id,
            "kept the local WAL aside and rebased onto the published history"
        );
        Ok(())
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
            if id >= ledger.next_segment_id() && !holds_no_records_named(wal.as_ref(), &name)? {
                // Records above every id the ledger handed out were written
                // by a writer this ledger never knew: keep them for an
                // operator rather than delete what may be data.
                return Err(MidgeError::Corruption(format!(
                    "WAL segment {name} holds records the WAL ledger never recorded"
                )));
            }
            wal.remove_file(&FsPath::new(&name))
                .map_err(FsError::into_midge)?;
            removed += 1;
        }
        Ok(removed)
    }

    /// Durably retire every member below `to`, then return its physical cleanup.
    /// Only the GC actor unlinks retired files; recovery ignores leftovers.
    ///
    /// # Errors
    ///
    /// Returns an error, removing nothing, if the ledger refuses or cannot
    /// record the new log start.
    pub(crate) fn retire_below(&mut self, to: SegmentId) -> MidgeResult<RetiredWal> {
        self.under_writer_fence(|history| history.retire_below_inner(to))
    }

    fn retire_below_inner(&mut self, to: SegmentId) -> MidgeResult<RetiredWal> {
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
        Ok(RetiredWal { pending: members })
    }

    /// Salvage: continue history in a new segment, at least `open_at_least`,
    /// without the members `set_aside` names.
    ///
    /// What salvage drops is kept first, in a new `salvaged-…/` directory
    /// nothing reads: a copy of the cut segment, which stays history, and the
    /// dropped members themselves, moved there in ascending order. Then the
    /// cut segment is cut back to the prefix replay applied and sealed with
    /// a footer for it, the next segment is created linked to the last kept
    /// segment, and the ledger records the set-aside. A crash before the
    /// ledger records it leaves the ledger naming members whose files moved,
    /// or a cut whose footer disagrees with the facts the ledger holds; the
    /// next salvage sets aside from the same point. After it, only the
    /// orphan sweep remains, and any open repeats it.
    pub(crate) fn set_aside(
        &mut self,
        set_aside: SetAside,
        open_at_least: SegmentId,
    ) -> MidgeResult<()> {
        self.under_writer_fence(|history| history.set_aside_inner(set_aside, open_at_least))
    }

    fn set_aside_inner(
        &mut self,
        set_aside: SetAside,
        open_at_least: SegmentId,
    ) -> MidgeResult<()> {
        let wal = self.files_for("set aside")?;
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
        let kept_link = ledger
            .member_facts()
            .filter(|member| member.id() < from)
            .last()
            .and_then(|member| member.sealed.as_ref())
            .map_or(ledger.anchor(), |sealed| sealed.link);
        let open_id = ledger.next_segment_id().max(open_at_least);
        let aside = keep_aside(wal.as_ref(), cut.map(|cut| cut.segment), &dropped)?;
        let cut_facts = match cut {
            Some(cut) => Some(cut_and_seal(wal.as_ref(), cut)?),
            None => None,
        };
        let prev = cut_facts.as_ref().map_or(kept_link, |facts| facts.link);
        let open = self.opening(open_id, prev.last_seq.saturating_add(1), prev);
        self.create(&open)?;
        self.commit(&LedgerEdit::SetAside {
            from,
            cut: cut_facts,
            open,
        })?;
        self.sweep_orphans()?;
        tracing::warn!(
            aside = %aside.0,
            from,
            open = open.id,
            cut = ?cut,
            "set aside WAL that salvage recovery did not replay"
        );
        Ok(())
    }

    fn files_for(&self, action: &str) -> MidgeResult<Arc<dyn Fs>> {
        self.segments.clone().ok_or_else(|| {
            MidgeError::Internal(format!("a memory-mode WAL has nothing to {action}"))
        })
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

/// Cut segment `cut.segment` back to the `cut.valid_bytes` replay applied
/// and seal that prefix with a footer, synced.
fn cut_and_seal(wal: &dyn Fs, cut: SalvageCut) -> MidgeResult<SealedFacts> {
    let path = FsPath::new(crate::wal::segment_file_name(cut.segment));
    let scan = {
        let file = wal.open(&path, read_only()).map_err(FsError::into_midge)?;
        crate::wal::recovery::streaming::scan_segment_file(
            file.as_ref(),
            &path,
            Some(cut.valid_bytes),
        )?
    };
    crate::io::truncate_durably(wal, &path, cut.valid_bytes).map_err(FsError::into_midge)?;
    write_footer(wal, &path, &scan)
}

/// Seal open segment `id`, which `scan` read whole: write the footer its
/// records need and sync it, or only sync the footer a rotation that
/// crashed before recording it already wrote.
fn seal_scanned(wal: &dyn Fs, id: SegmentId, scan: &SegmentScan) -> MidgeResult<SealedFacts> {
    let path = FsPath::new(crate::wal::segment_file_name(id));
    if let Some(footer) = scan.footer() {
        let mut file = wal.open(&path, read_write()).map_err(FsError::into_midge)?;
        file.sync(Durability::Durable)
            .map_err(FsError::into_midge)?;
        return Ok(footer.sealed_facts());
    }
    write_footer(wal, &path, scan)
}

/// Append the footer sealing what `scan` read at its end, and sync.
fn write_footer(wal: &dyn Fs, path: &FsPath, scan: &SegmentScan) -> MidgeResult<SealedFacts> {
    let footer = scan.sealing();
    let frame = footer.encode_frame()?;
    let mut file = wal.open(path, read_write()).map_err(FsError::into_midge)?;
    // The records the footer seals reach the device first: otherwise a
    // power loss could keep the footer while an earlier record page
    // reverts, and the segment would no longer replay.
    file.sync(Durability::Durable)
        .map_err(FsError::into_midge)?;
    file.write_at(footer.data_len, bytes::Bytes::from(frame))
        .map_err(FsError::into_midge)?;
    file.sync(Durability::Durable)
        .map_err(FsError::into_midge)?;
    Ok(footer.sealed_facts())
}

/// Read open member `id` whole.
fn scan_member(wal: &dyn Fs, id: SegmentId) -> MidgeResult<SegmentScan> {
    let path = FsPath::new(crate::wal::segment_file_name(id));
    let file = wal.open(&path, read_only()).map_err(|error| match error {
        FsError::NotFound(_) => MidgeError::RecoveryFailed(format!(
            "open WAL segment {id} disappeared; the records appended to it are lost"
        )),
        error => error.into_midge(),
    })?;
    crate::wal::recovery::streaming::scan_segment_file(file.as_ref(), &path, None)
}

/// Physical cleanup authorized by a durable ledger retirement.
#[derive(Debug, Default, Clone, PartialEq, Eq)]
pub(crate) struct RetiredWal {
    pub(crate) pending: Vec<(SegmentId, u64)>,
}

/// A next segment a rotation created, which the ledger has not recorded.
#[must_use]
#[derive(Debug)]
pub(crate) struct PreparedRotation {
    sealed: SealedFacts,
    open: OpenFacts,
}

/// A segment [`WalHistory::start_epoch`] sealed: the recovered tail.
#[derive(Debug, Clone, PartialEq, Eq)]
pub(crate) struct SealedSegment {
    pub(crate) id: SegmentId,
    /// The epoch of the writer that wrote it.
    pub(crate) epoch: u64,
    pub(crate) facts: SealedFacts,
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
/// could not use, and the verified prefix of it that replay applied, which
/// holds at least one record.
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

/// Create the segment `header` names, holding only its header, durably:
/// the header synced, then its directory entry. A file holding no record
/// that an earlier crash left is rewritten.
fn create_segment(wal: &dyn Fs, header: &SegmentHeader) -> MidgeResult<()> {
    let name = crate::wal::segment_file_name(header.id);
    let path = FsPath::new(&name);
    if wal.exists(&path).map_err(FsError::into_midge)? && !holds_no_records_named(wal, &name)? {
        return Err(MidgeError::Corruption(format!(
            "WAL segment {name} already holds records the WAL ledger never recorded"
        )));
    }
    let mut file = wal
        .open(
            &path,
            OpenOptions {
                mode: OpenMode::ReadWrite,
                create: true,
                create_new: false,
                truncate: true,
            },
        )
        .map_err(FsError::into_midge)?;
    file.write_at(0, bytes::Bytes::copy_from_slice(&header.encode()))
        .map_err(FsError::into_midge)?;
    file.sync(Durability::Durable)
        .map_err(FsError::into_midge)?;
    drop(file);
    wal.sync_dir(&FsPath::new(""), Durability::Durable)
        .map_err(FsError::into_midge)
}

fn holds_no_records_named(wal: &dyn ReadFs, name: &str) -> MidgeResult<bool> {
    Ok(holds_no_records(
        wal.metadata(&FsPath::new(name))
            .map_err(FsError::into_midge)?
            .len,
    ))
}

fn read_only() -> OpenOptions {
    OpenOptions {
        mode: OpenMode::ReadOnly,
        create: false,
        create_new: false,
        truncate: false,
    }
}

fn read_write() -> OpenOptions {
    OpenOptions {
        mode: OpenMode::ReadWrite,
        create: false,
        create_new: false,
        truncate: false,
    }
}

/// Every segment file in `wal`, by id. The only listing of `wal/`.
fn segment_files(wal: &dyn ReadFs) -> MidgeResult<Vec<(SegmentId, String)>> {
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
