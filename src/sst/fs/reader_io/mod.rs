//! Filesystem-backed SST reader using `io::Fs` abstraction (new approach)
//!
//! This reader uses the base `io::Fs` trait instead of `std::fs` directly,
//! allowing for swappable real and mock implementations in tests.

use arc_swap::ArcSwapOption;
use bytes::Bytes;
use std::sync::Arc;

use crate::common::{MidgeError, MidgeResult};
use crate::io::{File, Fs, FsError, FsPath, OpenMode, OpenOptions};
use crate::sst::bloom::BlockBloomFilter;
use crate::sst::cache::{BlockCache, CacheFill};
use crate::sst::index::tuner::IndexKind;
use crate::sst::trie::TrieReader;
use crate::sst::types::{BlockHandle, Footer, SST_FORMAT_V5};
use crate::types::{EntryType, KeyState, RangeTombstone};

type IndexEntries = Arc<Vec<(Vec<u8>, BlockHandle)>>;

/// Exercise the same restart/entry decoder used by point and streaming reads.
///
/// # Errors
/// Returns the production decoder error for malformed data or restart entries.
#[cfg(feature = "internal-testing")]
#[doc(hidden)]
pub fn validate_data_block(data: &[u8]) -> MidgeResult<()> {
    let layout = crate::sst::data_block::DataBlockLayout::parse(data)?;
    let block = Bytes::copy_from_slice(data);
    let mut decoder = BlockEntryDecoder::default();
    while decoder
        .next(&block, SST_FORMAT_V5, None, "fuzz data key")?
        .is_some()
    {}
    for key in [
        b"".as_slice(),
        &data[..data.len().min(32)],
        &data[data.len().saturating_sub(32)..],
    ] {
        let _ = layout.seek_interval(key)?;
    }
    for reverse in [false, true] {
        let mut cursor = BlockEntryCursor::new(block.clone(), SST_FORMAT_V5, reverse)?;
        while cursor.next()?.is_some() {}
    }
    Ok(())
}

/// Decode sparse index bytes through the production extent/order validator.
///
/// # Errors
/// Returns corruption for malformed entries, handles or key ordering.
#[cfg(feature = "internal-testing")]
#[doc(hidden)]
pub fn validate_index_block(data: &[u8]) -> MidgeResult<()> {
    let mut reader = SstFileIo::new("fuzz.sst", Arc::new(crate::io::MockFs::new()));
    reader.block_region_end = u64::MAX - crate::sst::types::SST_FOOTER_SIZE as u64;
    reader.decode_index_entries(data).map(|_| ())
}

/// Stable summary of the physical contents of a single SST file.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct SstFileSummary {
    pub earliest_expiration_millis: Option<u64>,
    pub size_bytes: u64,
    pub smallest_key: Vec<u8>,
    pub largest_key: Vec<u8>,
    pub smallest_seq: u64,
    pub largest_seq: u64,
}

/// Counts produced by a complete checksummed SST verification pass.
#[derive(Debug, Clone, PartialEq, Eq)]
pub(crate) struct SstVerificationStats {
    pub size_bytes: u64,
    pub data_blocks: u64,
    pub summary: SstFileSummary,
}

/// Physical work performed by one point lookup against one SST.
#[derive(Debug, Clone, Copy, Default, PartialEq, Eq)]
pub(crate) struct SstPointReadStats {
    pub sst_touched: bool,
    pub blocks_read: u64,
    pub entries_decoded: u64,
}

/// SST file reader using `io::Fs` abstraction
/// Identical to `SstFile` but accepts `Arc<dyn Fs>` for the filesystem backend
pub struct SstFileIo {
    path: FsPath,
    fs: Arc<dyn Fs>,
    /// The file handle held for the reader's lifetime when the backend offers
    /// owned handles (#919): block reads and scans share it, so the file is
    /// opened and its path checked once per reader instead of once per block.
    /// Backends without owned handles leave it `None` and open per read.
    handle: Option<Box<dyn File>>,
    footer: Option<Footer>,
    /// Exclusive end of the block region (the footer starts here).
    block_region_end: u64,
    sst_id: u64,
    block_bloom_filter: Option<BlockBloomFilter>,
    trie_reader: Option<Arc<TrieReader>>,
    block_cache: Option<Arc<BlockCache>>,
    /// Where read-path activity is reported. Standalone readers count into a
    /// detached observer until a runtime attaches its own.
    diagnostics: Arc<dyn crate::sst::read_path_metrics::SstReadObserver>,
    /// Immutable index publication. The common lookup path loads this
    /// atomically without taking a reader-wide mutex.
    index_entries: ArcSwapOption<Vec<(Vec<u8>, BlockHandle)>>,
    format_version: u32,
    index_kind: IndexKind,
    smallest_key: Option<Vec<u8>>,
    largest_key: Option<Vec<u8>>,
    range_tombstone_handle: Option<BlockHandle>,
    range_tombstones: Vec<RangeTombstone>,
    /// Fragmented view of `range_tombstones` for point lookups, built once at
    /// open; `None` when they nest too deeply for its memory bound (#921).
    range_tombstone_index:
        Option<crate::memtable::range_tombstone_index::FragmentedRangeTombstones>,
    metadata_budget: Option<crate::common::resource_budget::ResourceBudget>,
    metadata_reservations: Vec<crate::common::resource_budget::ResourceReservation>,
}

/// Walks one decoded block's entries in either direction without copying.
struct BlockEntryCursor {
    encoded: Bytes,
    format_version: u32,
    block: Option<state::DecodedScanBlock>,
    /// Entries handed out so far, counted from the walk's starting end.
    taken: usize,
    reverse: bool,
    next_interval: usize,
    remaining_intervals: usize,
}

impl BlockEntryCursor {
    fn new(encoded: Bytes, format_version: u32, reverse: bool) -> MidgeResult<Self> {
        let count = crate::sst::data_block::DataBlockLayout::read(&encoded)?.restart_count;
        Ok(Self {
            encoded,
            format_version,
            block: None,
            taken: 0,
            reverse,
            next_interval: if reverse { count - 1 } else { 0 },
            remaining_intervals: count,
        })
    }

    fn next(&mut self) -> MidgeResult<Option<state::ScanEntry>> {
        let remaining = self
            .block
            .as_ref()
            .map_or(0, |block| block.len().saturating_sub(self.taken));
        if remaining == 0 {
            self.block = None;
            if self.remaining_intervals == 0 {
                return Ok(None);
            }
            self.block = Some(SstFileIo::decode_scan_interval(
                &self.encoded,
                self.format_version,
                self.next_interval,
            )?);
            self.remaining_intervals -= 1;
            self.next_interval = if self.reverse {
                self.next_interval.saturating_sub(1)
            } else {
                self.next_interval + 1
            };
            self.taken = 0;
        }
        let block = self.block.as_ref().expect("decoded restart interval");
        let remaining = block.len() - self.taken;
        let index = if self.reverse {
            remaining - 1
        } else {
            self.taken
        };
        self.taken += 1;
        Ok(Some(block.entry(index)))
    }
}

/// Reconstructs prefix-compressed keys once for point, range, and raw scans.
#[derive(Default)]
struct BlockEntryDecoder {
    offset: usize,
    previous_key: Vec<u8>,
    key_reservation: Option<crate::common::resource_budget::ResourceReservation>,
    data_end: Option<usize>,
    restart_count: usize,
    next_restart: usize,
    entries_in_interval: usize,
    skip_initial_continuation: bool,
}

impl BlockEntryDecoder {
    fn at_restart(block: &Bytes, restart: usize) -> MidgeResult<Self> {
        let layout = crate::sst::data_block::DataBlockLayout::read(block)?;
        let (offset, _) = layout.restart(restart)?;
        Ok(Self {
            offset,
            data_end: Some(layout.data_end),
            restart_count: layout.restart_count,
            next_restart: restart,
            skip_initial_continuation: restart > 0,
            ..Self::default()
        })
    }

    fn exhausted(&self) -> bool {
        self.data_end.is_some_and(|end| self.offset >= end)
    }

    fn restart_at(&self, block: &Bytes, index: usize) -> MidgeResult<(usize, bool)> {
        let start = self.data_end.expect("initialized layout") + index * 4;
        let bytes = block
            .get(start..start + 4)
            .ok_or_else(|| MidgeError::Corruption("truncated restart offset".into()))?;
        let raw = u32::from_le_bytes(bytes.try_into().expect("restart word"));
        Ok((
            (raw & !crate::sst::data_block::CONTINUED_KEY) as usize,
            raw & crate::sst::data_block::CONTINUED_KEY != 0,
        ))
    }

    fn next<'a>(
        &mut self,
        block: &'a Bytes,
        format_version: u32,
        budget: Option<&crate::common::resource_budget::ResourceBudget>,
        resource: &'static str,
    ) -> MidgeResult<Option<crate::sst::encoding::EntryView<'a>>> {
        if self.data_end.is_none() {
            *self = Self::at_restart(block, 0)?;
        }
        let data_end = self.data_end.expect("initialized layout");
        if self.offset >= data_end {
            return Ok(None);
        }
        let restart = if self.next_restart < self.restart_count {
            let (offset, continued) = self.restart_at(block, self.next_restart)?;
            if self.offset > offset {
                return Err(MidgeError::Corruption(
                    "restart offset is inside an SST entry".into(),
                ));
            }
            (self.offset == offset).then_some(continued)
        } else {
            None
        };
        if restart.is_some() {
            if self.entries_in_interval != 0
                && self.entries_in_interval != crate::sst::data_block::RESTART_INTERVAL
            {
                return Err(MidgeError::Corruption(
                    "incorrect SST restart interval".into(),
                ));
            }
            self.entries_in_interval = 0;
            self.next_restart += 1;
        } else if self.entries_in_interval == crate::sst::data_block::RESTART_INTERVAL {
            return Err(MidgeError::Corruption("missing SST restart entry".into()));
        }
        let (entry, next_offset) = crate::sst::encoding::decode_with_format(
            &block[..data_end],
            self.offset,
            format_version,
        )?;
        if let Some(continued) = restart {
            if entry.shared_len != 0
                || (!self.skip_initial_continuation
                    && continued != (self.offset > 0 && self.previous_key == entry.key_delta))
            {
                return Err(MidgeError::Corruption(
                    "invalid independent SST restart key".into(),
                ));
            }
            self.skip_initial_continuation = false;
        }
        if self.next_restart < self.restart_count
            && next_offset > self.restart_at(block, self.next_restart)?.0
        {
            return Err(MidgeError::Corruption(
                "SST entry crosses a restart offset".into(),
            ));
        }
        let shared_len =
            SstFileIo::shared_prefix_len(usize::from(entry.shared_len), self.previous_key.len())?;
        let key_len = shared_len
            .checked_add(entry.key_delta.len())
            .ok_or_else(|| {
                MidgeError::Corruption("SST reconstructed key length overflow".into())
            })?;
        if let Some(budget) = budget {
            let reservation = budget.reserve(key_len, resource)?;
            let mut key = Vec::with_capacity(key_len);
            key.extend_from_slice(&self.previous_key[..shared_len]);
            key.extend_from_slice(entry.key_delta);
            self.previous_key = key;
            self.key_reservation = Some(reservation);
        } else {
            self.previous_key.truncate(shared_len);
            self.previous_key.extend_from_slice(entry.key_delta);
        }
        self.offset = next_offset;
        self.entries_in_interval += 1;
        Ok(Some(entry))
    }

    fn key(&self) -> &[u8] {
        &self.previous_key
    }
}

/// Fallible, block-at-a-time cursor over the visible key states in one SST.
///
/// Index metadata is loaded on the first call to `next`; data blocks are read
/// only as the caller advances the cursor. This keeps a small query limit from
/// turning into a full-table read and lets corruption discovered in a later
/// block surface from the corresponding iterator item.
pub(crate) struct SstStateScan {
    reader: Arc<SstFileIo>,
    start: Option<Vec<u8>>,
    end: Option<Vec<u8>>,
    reverse: bool,
    snapshot_seq: u64,
    now_millis: u64,
    raw_state: bool,
    /// Whether blocks this scan fetches are inserted into the block cache.
    cache_fill: CacheFill,
    initialized: bool,
    lifecycle: SstScanLifecycle,
    index: Option<IndexEntries>,
    first_block: usize,
    last_block: usize,
    next_block: usize,
    block_entries: Option<BlockEntryCursor>,
    pending_entry: Option<state::ScanEntry>,
    /// Bytes read ahead of the current block for large scans (#945).
    window: Option<index::ReadWindow>,
}

/// Fallible, block-at-a-time cursor over every persisted logical version.
///
/// Unlike [`SstStateScan`], this cursor does not collapse equal keys or apply a
/// snapshot/TTL view. Compaction needs the exact persisted ordering and decides
/// which versions remain authoritative itself.
struct SstRawVersionScan {
    reader: Arc<SstFileIo>,
    start: Option<Vec<u8>>,
    end: Option<Vec<u8>>,
    initialized: bool,
    lifecycle: SstScanLifecycle,
    index: Option<IndexEntries>,
    first_block: usize,
    last_block: usize,
    next_block: usize,
    block: Option<Bytes>,
    decoder: BlockEntryDecoder,
    budget: Option<crate::common::resource_budget::ResourceBudget>,
    _bounds_reservation: Option<crate::common::resource_budget::ResourceReservation>,
    block_reservation: Option<crate::common::resource_budget::ResourceReservation>,
    yield_reservation: Option<crate::common::resource_budget::ResourceReservation>,
}

impl SstRawVersionScan {
    fn new(
        reader: Arc<SstFileIo>,
        start: Option<Vec<u8>>,
        end: Option<Vec<u8>>,
        budget: Option<crate::common::resource_budget::ResourceBudget>,
    ) -> MidgeResult<Self> {
        let bounds_bytes = start
            .as_ref()
            .map_or(0, Vec::capacity)
            .saturating_add(end.as_ref().map_or(0, Vec::capacity));
        let bounds_reservation = budget
            .as_ref()
            .map(|budget| budget.reserve(bounds_bytes, "raw cursor bounds"))
            .transpose()?;
        Ok(Self {
            reader,
            start,
            end,
            initialized: false,
            lifecycle: SstScanLifecycle::Active,
            index: None,
            first_block: 0,
            last_block: 0,
            next_block: 0,
            block: None,
            decoder: BlockEntryDecoder::default(),
            budget,
            _bounds_reservation: bounds_reservation,
            block_reservation: None,
            yield_reservation: None,
        })
    }

    fn initialize(&mut self) -> MidgeResult<()> {
        if self.initialized {
            return Ok(());
        }
        self.initialized = true;
        if self
            .reader
            .range_outside_persisted_bounds(self.start.as_deref(), self.end.as_deref())
        {
            self.lifecycle = SstScanLifecycle::Exhausted;
            return Ok(());
        }

        let index = self
            .reader
            .index_entries_from(self.reader.handle.as_deref())?;
        if index.is_empty() {
            self.lifecycle = SstScanLifecycle::Exhausted;
            return Ok(());
        }
        let Some(span) =
            self.reader
                .block_span(index.as_ref(), self.start.as_deref(), self.end.as_deref())
        else {
            self.lifecycle = SstScanLifecycle::Exhausted;
            return Ok(());
        };
        self.first_block = *span.start();
        self.last_block = *span.end();
        self.next_block = self.first_block;
        self.index = Some(index);
        Ok(())
    }

    fn load_next_block(&mut self) -> MidgeResult<bool> {
        self.initialize()?;
        if matches!(self.lifecycle, SstScanLifecycle::Exhausted) {
            return Ok(false);
        }
        let block_index = self.next_block;
        let handle = self
            .index
            .as_ref()
            .and_then(|index| index.get(block_index))
            .map(|(_, handle)| *handle)
            .ok_or_else(|| {
                MidgeError::Corruption("SST raw cursor block index is invalid".into())
            })?;
        if block_index == self.last_block {
            self.lifecycle = SstScanLifecycle::Exhausted;
        } else {
            self.next_block = block_index + 1;
        }

        let opened_file = if self.reader.handle.is_none() {
            Some(
                self.reader
                    .fs
                    .open(
                        &self.reader.path,
                        OpenOptions {
                            mode: OpenMode::ReadOnly,
                            create: false,
                            create_new: false,
                            truncate: false,
                        },
                    )
                    .map_err(FsError::into_midge)?,
            )
        } else {
            None
        };
        let file = self
            .reader
            .handle
            .as_deref()
            .or(opened_file.as_deref())
            .expect("SST file opened");
        let (block, decompressed_reservation) = self.reader.read_framed_block(
            file,
            &handle,
            self.budget
                .as_ref()
                .map(|budget| (budget, "compressed SST block")),
            |decoded_size| {
                self.budget
                    .as_ref()
                    .map(|budget| budget.reserve(decoded_size, "decompressed SST block"))
                    .transpose()
            },
        )?;

        self.reader
            .diagnostics
            .sst_metrics()
            .record_candidate_blocks_checked(1);
        self.reader
            .diagnostics
            .sst_metrics()
            .record_data_block_read();
        self.block = Some(block);
        self.block_reservation = decompressed_reservation;
        self.decoder = BlockEntryDecoder::default();
        Ok(true)
    }

    fn next_version(&mut self) -> MidgeResult<Option<crate::sst::traits::RawSstVersion>> {
        self.yield_reservation = None;
        loop {
            let needs_block = self.block.as_ref().is_none_or(|_| self.decoder.exhausted());
            if needs_block {
                self.block = None;
                self.block_reservation = None;
                self.decoder = BlockEntryDecoder::default();
                if !self.load_next_block()? {
                    return Ok(None);
                }
            }

            let block = self.block.as_ref().expect("raw cursor loaded a block");
            let entry = self
                .decoder
                .next(
                    block,
                    self.reader.format_version,
                    self.budget.as_ref(),
                    "raw cursor decoder key",
                )?
                .ok_or_else(|| MidgeError::Corruption("raw cursor lost its block entry".into()))?;
            let key_len = self.decoder.key().len();
            let value_len = entry.value.map_or(0, <[u8]>::len);
            let yield_bytes = std::mem::size_of::<crate::sst::traits::RawSstVersion>()
                .saturating_add(key_len)
                .saturating_add(value_len);
            let yield_reservation = self
                .budget
                .as_ref()
                .map(|budget| budget.reserve(yield_bytes, "raw cursor yielded version"))
                .transpose()?;

            let key = self.decoder.key().to_vec();
            if self
                .start
                .as_deref()
                .is_some_and(|start| key.as_slice() < start)
                || self.end.as_deref().is_some_and(|end| key.as_slice() >= end)
            {
                continue;
            }

            let value = entry.value_offset.map(|offset| {
                let end = offset.saturating_add(value_len);
                block[offset..end].to_vec()
            });
            self.yield_reservation = yield_reservation;
            return Ok(Some(crate::sst::traits::RawSstVersion {
                key,
                seq: entry.sequence,
                is_tombstone: matches!(entry.entry_type, EntryType::Delete),
                value,
                expiration: entry.expiration,
            }));
        }
    }
}

impl Iterator for SstRawVersionScan {
    type Item = MidgeResult<crate::sst::traits::RawSstVersion>;

    fn next(&mut self) -> Option<Self::Item> {
        match &self.lifecycle {
            SstScanLifecycle::Failed(error) => return Some(Err(error.replay())),
            SstScanLifecycle::Exhausted if self.block.is_none() => return None,
            SstScanLifecycle::Active | SstScanLifecycle::Exhausted => {}
        }

        match self.next_version() {
            Ok(Some(version)) => Some(Ok(version)),
            Ok(None) => {
                self.lifecycle = SstScanLifecycle::Exhausted;
                None
            }
            Err(error) => {
                self.block = None;
                self.lifecycle = SstScanLifecycle::Failed(error);
                match &self.lifecycle {
                    SstScanLifecycle::Failed(error) => Some(Err(error.replay())),
                    SstScanLifecycle::Active | SstScanLifecycle::Exhausted => unreachable!(),
                }
            }
        }
    }
}

enum SstScanLifecycle {
    Active,
    Exhausted,
    Failed(MidgeError),
}

impl SstStateScan {
    fn new(
        reader: Arc<SstFileIo>,
        start: Option<Vec<u8>>,
        end: Option<Vec<u8>>,
        reverse: bool,
        snapshot_seq: u64,
        now_millis: u64,
    ) -> Self {
        Self {
            reader,
            start,
            end,
            reverse,
            snapshot_seq,
            now_millis,
            raw_state: false,
            cache_fill: CacheFill::Normal,
            initialized: false,
            lifecycle: SstScanLifecycle::Active,
            index: None,
            first_block: 0,
            last_block: 0,
            next_block: 0,
            block_entries: None,
            pending_entry: None,
            window: None,
        }
    }

    fn initialize(&mut self) -> MidgeResult<()> {
        if self.initialized {
            return Ok(());
        }
        self.initialized = true;

        if self
            .reader
            .range_outside_persisted_bounds(self.start.as_deref(), self.end.as_deref())
        {
            self.lifecycle = SstScanLifecycle::Exhausted;
            return Ok(());
        }

        let index = self
            .reader
            .index_entries_from(self.reader.handle.as_deref())?;
        if index.is_empty() {
            self.lifecycle = SstScanLifecycle::Exhausted;
            return Ok(());
        }

        let Some(span) =
            self.reader
                .block_span(index.as_ref(), self.start.as_deref(), self.end.as_deref())
        else {
            self.lifecycle = SstScanLifecycle::Exhausted;
            return Ok(());
        };
        self.first_block = *span.start();
        self.last_block = *span.end();
        self.next_block = if self.reverse {
            self.last_block
        } else {
            self.first_block
        };
        self.index = Some(index);
        Ok(())
    }

    fn load_next_block(&mut self) -> MidgeResult<bool> {
        self.initialize()?;
        if matches!(self.lifecycle, SstScanLifecycle::Exhausted) {
            return Ok(false);
        }

        let block_index = self.next_block;
        let handle = self
            .index
            .as_ref()
            .and_then(|index| index.get(block_index))
            .map(|(_, handle)| *handle)
            .ok_or_else(|| MidgeError::Corruption("SST scan block index is invalid".into()))?;

        if self.reverse {
            if block_index == self.first_block {
                self.lifecycle = SstScanLifecycle::Exhausted;
            } else {
                self.next_block = block_index - 1;
            }
        } else if block_index == self.last_block {
            self.lifecycle = SstScanLifecycle::Exhausted;
        } else {
            self.next_block = block_index + 1;
        }

        self.reader
            .diagnostics
            .sst_metrics()
            .record_candidate_blocks_checked(1);
        // The bytes this scan can still visit: readahead never leaves them.
        let span = self
            .index
            .as_ref()
            .and_then(|index| {
                let (_, first) = index.get(self.first_block)?;
                let (_, last) = index.get(self.last_block)?;
                Some(first.offset..last.offset.saturating_add(last.size))
            })
            .unwrap_or(handle.offset..handle.offset.saturating_add(handle.size));
        let block = self.reader.read_cached_data_block_windowed(
            &handle,
            self.reader.handle.as_deref(),
            self.cache_fill,
            self.reverse,
            &span,
            &mut self.window,
        )?;
        self.block_entries = Some(BlockEntryCursor::new(
            block,
            self.reader.format_version,
            self.reverse,
        )?);
        Ok(true)
    }

    fn next_raw_entry(&mut self) -> MidgeResult<Option<state::ScanEntry>> {
        loop {
            let next = self
                .block_entries
                .as_mut()
                .map(BlockEntryCursor::next)
                .transpose()?
                .flatten();
            if let Some(entry) = next {
                if self
                    .start
                    .as_deref()
                    .is_some_and(|start| entry.key.as_ref() < start)
                    || self
                        .end
                        .as_deref()
                        .is_some_and(|end| entry.key.as_ref() >= end)
                {
                    continue;
                }
                return Ok(Some(entry));
            }
            self.block_entries = None;
            if !self.load_next_block()? {
                return Ok(None);
            }
        }
    }

    fn consider_entry(&self, best: &mut KeyState, entry: state::ScanEntry) -> MidgeResult<()> {
        if self.snapshot_seq != u64::MAX && entry.sequence > self.snapshot_seq {
            return Ok(());
        }
        SstFileIo::merge_newer_state(best, SstFileIo::state_from_scan_entry(entry))
    }

    fn next_state(&mut self) -> MidgeResult<Option<(Bytes, KeyState)>> {
        loop {
            let Some(first) = self
                .pending_entry
                .take()
                .map_or_else(|| self.next_raw_entry(), |entry| Ok(Some(entry)))?
            else {
                return Ok(None);
            };

            // A reference count, not a copy: the key is a slice of its
            // block's key arena (#945).
            let key = first.key.clone();
            let mut best = KeyState::Absent;
            self.consider_entry(&mut best, first)?;

            while let Some(entry) = self.next_raw_entry()? {
                if entry.key == key {
                    self.consider_entry(&mut best, entry)?;
                } else {
                    self.pending_entry = Some(entry);
                    break;
                }
            }

            if !matches!(best, KeyState::Absent) {
                let state = if self.raw_state {
                    best
                } else {
                    match best {
                        KeyState::Value(_, seq, expiration, _)
                            if crate::common::time::is_expired_at(expiration, self.now_millis) =>
                        {
                            KeyState::Tombstone(seq)
                        }
                        state => state,
                    }
                };
                return Ok(Some((key, state)));
            }
        }
    }
}

impl std::iter::Iterator for SstStateScan {
    type Item = MidgeResult<(Bytes, KeyState)>;

    fn next(&mut self) -> Option<Self::Item> {
        match &self.lifecycle {
            SstScanLifecycle::Failed(error) => return Some(Err(error.replay())),
            SstScanLifecycle::Exhausted
                if self.block_entries.is_none() && self.pending_entry.is_none() =>
            {
                return None;
            }
            SstScanLifecycle::Active | SstScanLifecycle::Exhausted => {}
        }

        match self.next_state() {
            Ok(Some(state)) => Some(Ok(state)),
            Ok(None) => {
                self.lifecycle = SstScanLifecycle::Exhausted;
                None
            }
            Err(error) => {
                self.block_entries = None;
                self.pending_entry = None;
                self.lifecycle = SstScanLifecycle::Failed(error);
                match &self.lifecycle {
                    SstScanLifecycle::Failed(error) => Some(Err(error.replay())),
                    SstScanLifecycle::Active | SstScanLifecycle::Exhausted => unreachable!(),
                }
            }
        }
    }
}

mod progress;
pub(crate) use progress::SstSummaryProgress;

mod index;
mod io;
mod scan;
mod state;

impl SstFileIo {
    /// Create a new SST reader using the provided filesystem
    #[must_use]
    pub fn new(path_str: &str, fs: Arc<dyn Fs>) -> Self {
        Self {
            path: FsPath::new(path_str),
            fs,
            handle: None,
            footer: None,
            block_region_end: 0,
            // Readers without a cache never consume this placeholder. Cache
            // attachment requires a generation-scoped identity explicitly.
            sst_id: 0,
            block_bloom_filter: None,
            trie_reader: None,
            block_cache: None,
            diagnostics: Arc::new(
                crate::sst::read_path_metrics::DetachedSstReadObserver::default(),
            ),
            index_entries: ArcSwapOption::empty(),
            format_version: SST_FORMAT_V5,
            index_kind: IndexKind::Sparse,
            smallest_key: None,
            largest_key: None,
            range_tombstone_handle: None,
            range_tombstones: Vec::new(),
            range_tombstone_index: None,
            metadata_budget: None,
            metadata_reservations: Vec::new(),
        }
    }

    /// Open and load metadata from an SST file
    ///
    /// # Errors
    ///
    /// Returns an error when the SST footer, metadata, or backing file cannot be read.
    pub fn open(path_str: &str, fs: Arc<dyn Fs>) -> MidgeResult<Self> {
        let fs = fs
            .immutable_read_view(&FsPath::new(path_str))
            .map_err(FsError::into_midge)?
            .unwrap_or(fs);
        let mut reader = Self::new(path_str, fs);
        reader.load_metadata()?;
        Ok(reader)
    }

    /// Open an SST while charging all eagerly retained metadata to `budget`.
    pub(crate) fn open_for_compaction(
        path_str: &str,
        fs: Arc<dyn Fs>,
        budget: crate::common::resource_budget::ResourceBudget,
    ) -> MidgeResult<Self> {
        let fs = fs
            .immutable_read_view(&FsPath::new(path_str))
            .map_err(FsError::into_midge)?
            .unwrap_or(fs);
        let mut reader = Self::new(path_str, fs);
        reader.metadata_reservations.push(budget.reserve(
            std::mem::size_of::<Self>().saturating_add(path_str.len()),
            "SST reader",
        )?);
        reader.metadata_budget = Some(budget);
        reader.load_metadata()?;
        Ok(reader)
    }

    pub(crate) fn file_size(&self) -> u64 {
        self.block_region_end
            .saturating_add(crate::sst::types::SST_FOOTER_SIZE as u64)
    }

    /// Stream a summary through the caller's filesystem abstraction.
    #[cfg(test)]
    pub(crate) fn summarize_with_fs(path: &str, fs: Arc<dyn Fs>) -> MidgeResult<SstFileSummary> {
        Self::open(path, fs)?.into_streaming_summary()
    }

    pub(crate) fn summarize_with_fs_for_compaction(
        path: &str,
        fs: Arc<dyn Fs>,
        budget: crate::common::resource_budget::ResourceBudget,
    ) -> MidgeResult<SstFileSummary> {
        Self::open_for_compaction(path, fs, budget)?.into_streaming_summary()
    }

    /// Load block bloom filter from footer (if present)
    ///
    /// # Errors
    ///
    /// Returns an error when the block bloom handle cannot be read or decoded.
    pub fn load_block_bloom(&mut self) -> MidgeResult<()> {
        if let Some(block_bloom_handle) = self
            .footer
            .as_ref()
            .and_then(|footer| footer.block_bloom_handle)
        {
            let bloom_data = self.read_metadata_block(&block_bloom_handle, "SST block bloom")?;
            let block_bloom = BlockBloomFilter::deserialize(&bloom_data)?;
            self.block_bloom_filter = Some(block_bloom);
        }
        Ok(())
    }

    /// Enable the block cache with a generation-scoped immutable SST identity.
    ///
    /// The identity must change when a logical path is replaced with different
    /// contents. Keeping it in the same operation makes unsafe path-derived
    /// cache identities unrepresentable.
    #[must_use]
    pub fn with_block_cache(mut self, cache: Arc<BlockCache>, sst_id: u64) -> Self {
        self.sst_id = sst_id;
        self.block_cache = Some(cache);
        self
    }

    /// The identity this reader's blocks are cached under.
    #[cfg(test)]
    pub(crate) fn block_cache_sst_id(&self) -> u64 {
        self.sst_id
    }

    /// Attach the runtime that owns this reader's diagnostics.
    #[must_use]
    pub(crate) fn with_read_path_diagnostics(
        mut self,
        diagnostics: Arc<dyn crate::sst::read_path_metrics::SstReadObserver>,
    ) -> Self {
        self.diagnostics = diagnostics;
        self
    }

    pub(crate) fn state_scan(
        self: &Arc<Self>,
        start: Option<Vec<u8>>,
        end: Option<Vec<u8>>,
        reverse: bool,
        snapshot_seq: u64,
        now_millis: u64,
    ) -> SstStateScan {
        SstStateScan::new(
            Arc::clone(self),
            start,
            end,
            reverse,
            snapshot_seq,
            now_millis,
        )
    }

    pub(crate) fn raw_state_scan(
        self: &Arc<Self>,
        start: Option<Vec<u8>>,
        end: Option<Vec<u8>>,
        reverse: bool,
        snapshot_seq: u64,
        cache_fill: CacheFill,
    ) -> SstStateScan {
        let mut scan = self.state_scan(start, end, reverse, snapshot_seq, 0);
        scan.raw_state = true;
        scan.cache_fill = cache_fill;
        scan
    }

    pub(crate) fn newer_overlapping_range_tombstone_seq(
        &self,
        start: Option<&[u8]>,
        end: Option<&[u8]>,
        threshold: u64,
    ) -> Option<u64> {
        self.range_tombstones
            .iter()
            .find(|range| {
                range.seq > threshold
                    && end.is_none_or(|end| range.start.as_slice() < end)
                    && start.is_none_or(|start| range.end.as_slice() > start)
            })
            .map(|range| range.seq)
    }

    fn into_streaming_summary(self) -> MidgeResult<SstFileSummary> {
        use crate::sst::traits::SstStateReader;

        let size_bytes = self
            .fs
            .metadata(&self.path)
            .map_err(FsError::into_midge)?
            .len;
        let budget = self.metadata_budget.clone();
        let mut accumulator = SstSummaryProgress::default();
        for range in &self.range_tombstones {
            accumulator.observe(size_bytes, &range.start, range.seq, budget.as_ref())?;
            accumulator.observe(size_bytes, &range.end, range.seq, budget.as_ref())?;
        }
        let mut cursor =
            Box::new(self).raw_version_cursor_with_budget(None, None, budget.clone())?;
        for version in &mut cursor {
            let version = version?;
            accumulator.observe(size_bytes, &version.key, version.seq, budget.as_ref())?;
            accumulator.observe_expiration(version.expiration);
        }
        accumulator
            .summary
            .ok_or_else(|| MidgeError::Corruption("SST contains no publishable entries".into()))
    }
}

#[cfg(test)]
mod tests;
