//! Filesystem-backed SST reader using `io::Fs` abstraction (new approach)
//!
//! This reader uses the base `io::Fs` trait instead of `std::fs` directly,
//! allowing for swappable real and mock implementations in tests.

use arc_swap::ArcSwapOption;
use bytes::Bytes;
use std::sync::Arc;

use crate::common::{MidgeError, MidgeResult};
use crate::io::{File, Fs, FsError, FsPath, OpenMode, OpenOptions};
use crate::sst::bloom::{BlockBloomFilter, BloomMetrics};
use crate::sst::cache::BlockCache;
use crate::sst::index::tuner::IndexKind;
use crate::sst::read_amp_metrics::ReadAmpMetrics;
use crate::sst::trie::TrieReader;
use crate::sst::types::{BlockHandle, Footer, KeyState, RangeTombstone, SstEntry, SST_FORMAT_V4};

type IndexEntries = Arc<Vec<(Vec<u8>, BlockHandle)>>;

/// Stable summary of the physical contents of a single SST file.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct SstFileSummary {
    pub size_bytes: u64,
    pub smallest_key: Vec<u8>,
    pub largest_key: Vec<u8>,
    pub smallest_seq: u64,
    pub largest_seq: u64,
}

/// Counts produced by a complete checksummed SST verification pass.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(crate) struct SstVerificationStats {
    pub size_bytes: u64,
    pub data_blocks: u64,
}

/// Physical work performed by one point lookup against one SST.
#[derive(Debug, Clone, Copy, Default, PartialEq, Eq)]
pub(crate) struct SstPointReadStats {
    pub sst_touched: bool,
    pub blocks_read: u64,
}

/// SST file reader using `io::Fs` abstraction
/// Identical to `SstFile` but accepts `Arc<dyn Fs>` for the filesystem backend
pub struct SstFileIo {
    path: FsPath,
    fs: Arc<dyn Fs>,
    footer: Option<Footer>,
    /// Exclusive end of the block region (the footer starts here).
    block_region_end: u64,
    sst_id: u64,
    block_bloom_filter: Option<BlockBloomFilter>,
    bloom_metrics: BloomMetrics,
    read_amp_metrics: ReadAmpMetrics,
    trie_reader: Option<Arc<TrieReader>>,
    block_cache: Option<Arc<BlockCache>>,
    /// Runtime-owned read-path counters. Standalone readers use the legacy
    /// compatibility bucket until a runtime supplies its own state.
    diagnostics: Arc<crate::diagnostics::RuntimeDiagnostics>,
    /// Immutable index publication. The common lookup path loads this
    /// atomically without taking a reader-wide mutex.
    index_entries: ArcSwapOption<Vec<(Vec<u8>, BlockHandle)>>,
    format_version: u32,
    index_kind: IndexKind,
    smallest_key: Option<Vec<u8>>,
    largest_key: Option<Vec<u8>>,
    range_tombstone_handle: Option<BlockHandle>,
    range_tombstones: Vec<RangeTombstone>,
    metadata_budget: Option<crate::common::resource_budget::ResourceBudget>,
    metadata_reservations: Vec<crate::common::resource_budget::ResourceReservation>,
}

enum BlockEntryCursor {
    Forward(std::vec::IntoIter<SstEntry>),
    Reverse(std::iter::Rev<std::vec::IntoIter<SstEntry>>),
}

impl BlockEntryCursor {
    fn next(&mut self) -> Option<SstEntry> {
        match self {
            Self::Forward(entries) => entries.next(),
            Self::Reverse(entries) => entries.next(),
        }
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
    initialized: bool,
    lifecycle: SstScanLifecycle,
    file: Option<Box<dyn File>>,
    index: Option<IndexEntries>,
    first_block: usize,
    last_block: usize,
    next_block: usize,
    block_entries: Option<BlockEntryCursor>,
    pending_entry: Option<SstEntry>,
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
    file: Option<Box<dyn File>>,
    index: Option<IndexEntries>,
    first_block: usize,
    last_block: usize,
    next_block: usize,
    block: Option<Bytes>,
    block_offset: usize,
    previous_key: Vec<u8>,
    budget: Option<crate::common::resource_budget::ResourceBudget>,
    _bounds_reservation: Option<crate::common::resource_budget::ResourceReservation>,
    block_reservation: Option<crate::common::resource_budget::ResourceReservation>,
    decoder_reservation: Option<crate::common::resource_budget::ResourceReservation>,
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
        let (file, lifecycle) = match reader.fs.open_persistent_handle(
            &reader.path,
            OpenOptions {
                mode: OpenMode::ReadOnly,
                create: false,
                create_new: false,
                truncate: false,
            },
        ) {
            Ok(file) => (Some(file), SstScanLifecycle::Active),
            Err(FsError::Unsupported(_)) => (None, SstScanLifecycle::Active),
            Err(error) => (None, SstScanLifecycle::Failed(error.into())),
        };
        Ok(Self {
            reader,
            start,
            end,
            initialized: false,
            lifecycle,
            file,
            index: None,
            first_block: 0,
            last_block: 0,
            next_block: 0,
            block: None,
            block_offset: 0,
            previous_key: Vec::new(),
            budget,
            _bounds_reservation: bounds_reservation,
            block_reservation: None,
            decoder_reservation: None,
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

        let index = self.reader.index_entries_from(self.file.as_deref())?;
        if index.is_empty() {
            self.lifecycle = SstScanLifecycle::Exhausted;
            return Ok(());
        }
        self.first_block = self
            .start
            .as_deref()
            .and_then(|start| self.reader.candidate_block_indices(index.as_ref(), start))
            .map_or(0, |range| *range.start());
        self.last_block = self
            .end
            .as_deref()
            .and_then(|end| self.reader.candidate_block_indices(index.as_ref(), end))
            .map_or_else(|| index.len().saturating_sub(1), |range| *range.end());
        if self.first_block >= index.len() || self.first_block > self.last_block {
            self.lifecycle = SstScanLifecycle::Exhausted;
            return Ok(());
        }
        self.last_block = self.last_block.min(index.len() - 1);
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

        SstFileIo::validate_block_handle(handle, self.reader.block_region_end, "compaction data")?;
        let compressed_size = usize::try_from(handle.size).map_err(|_| {
            MidgeError::ResourceLimit("compressed SST block exceeds addressable memory".into())
        })?;
        let compressed_reservation = self
            .budget
            .as_ref()
            .map(|budget| budget.reserve(compressed_size, "compressed SST block"))
            .transpose()?;
        let buffer = if let Some(file) = self.file.as_deref() {
            file.read_at(handle.offset, handle.size)?
        } else {
            let file = self.reader.fs.open(
                &self.reader.path,
                OpenOptions {
                    mode: OpenMode::ReadOnly,
                    create: false,
                    create_new: false,
                    truncate: false,
                },
            )?;
            file.read_at(handle.offset, handle.size)?
        };
        if buffer.len() < 4 {
            return Err(MidgeError::Corruption("Block too short".into()));
        }
        let payload_len = u32::from_le_bytes([buffer[0], buffer[1], buffer[2], buffer[3]]) as usize;
        if payload_len.checked_add(4) != Some(buffer.len()) {
            return Err(MidgeError::Corruption(
                "SST block length prefix does not exactly match its handle".into(),
            ));
        }
        let raw = &buffer[4..];
        let decompressed_size = crate::sst::compression::decompressed_size_with_trailer(raw)?;
        let decompressed_reservation = self
            .budget
            .as_ref()
            .map(|budget| budget.reserve(decompressed_size, "decompressed SST block"))
            .transpose()?;
        let block = crate::sst::compression::decompress_block_with_trailer(raw)?;
        if block.len() > decompressed_size {
            return Err(MidgeError::Corruption(
                "decoded SST block exceeded its declared size".into(),
            ));
        }
        drop(compressed_reservation);

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
        self.block_offset = 0;
        self.previous_key.clear();
        self.decoder_reservation = None;
        Ok(true)
    }

    fn next_version(&mut self) -> MidgeResult<Option<crate::sst::traits::RawSstVersion>> {
        self.yield_reservation = None;
        loop {
            let needs_block = self
                .block
                .as_ref()
                .is_none_or(|block| self.block_offset >= block.len());
            if needs_block {
                self.block = None;
                self.block_reservation = None;
                self.decoder_reservation = None;
                self.previous_key.clear();
                if !self.load_next_block()? {
                    return Ok(None);
                }
            }

            let block = self.block.as_ref().expect("raw cursor loaded a block");
            let (entry, next_offset) = crate::sst::encoding::decode_with_format(
                block.as_ref(),
                self.block_offset,
                self.reader.format_version,
            )?;
            let shared_len = usize::from(entry.shared_len);
            if shared_len > self.previous_key.len() {
                return Err(MidgeError::Corruption(
                    "Invalid shared prefix length in SST entry".into(),
                ));
            }
            let key_len = shared_len.saturating_add(entry.key_delta.len());
            let value_len = entry.value.map_or(0, <[u8]>::len);
            let decoder_reservation = self
                .budget
                .as_ref()
                .map(|budget| budget.reserve(key_len, "raw cursor decoder key"))
                .transpose()?;
            let yield_bytes = std::mem::size_of::<crate::sst::traits::RawSstVersion>()
                .saturating_add(key_len)
                .saturating_add(value_len);
            let yield_reservation = self
                .budget
                .as_ref()
                .map(|budget| budget.reserve(yield_bytes, "raw cursor yielded version"))
                .transpose()?;

            let mut key = Vec::with_capacity(key_len);
            key.extend_from_slice(&self.previous_key[..shared_len]);
            key.extend_from_slice(entry.key_delta);
            self.previous_key = key.clone();
            self.decoder_reservation = decoder_reservation;
            self.block_offset = next_offset;
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
                is_tombstone: matches!(entry.entry_type, crate::sst::encoding::EntryType::Delete),
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
        let (file, lifecycle) = match reader.fs.open_persistent_handle(
            &reader.path,
            OpenOptions {
                mode: OpenMode::ReadOnly,
                create: false,
                create_new: false,
                truncate: false,
            },
        ) {
            Ok(file) => (Some(file), SstScanLifecycle::Active),
            Err(FsError::Unsupported(_)) => (None, SstScanLifecycle::Active),
            Err(error) => (None, SstScanLifecycle::Failed(error.into())),
        };
        Self {
            reader,
            start,
            end,
            reverse,
            snapshot_seq,
            now_millis,
            initialized: false,
            lifecycle,
            file,
            index: None,
            first_block: 0,
            last_block: 0,
            next_block: 0,
            block_entries: None,
            pending_entry: None,
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

        let index = self.reader.index_entries_from(self.file.as_deref())?;
        if index.is_empty() {
            self.lifecycle = SstScanLifecycle::Exhausted;
            return Ok(());
        }

        self.first_block = self
            .start
            .as_deref()
            .and_then(|start| self.reader.candidate_block_indices(index.as_ref(), start))
            .map_or(0, |range| *range.start());
        self.last_block = self
            .end
            .as_deref()
            .and_then(|end| self.reader.candidate_block_indices(index.as_ref(), end))
            .map_or_else(|| index.len().saturating_sub(1), |range| *range.end());

        if self.first_block >= index.len() || self.first_block > self.last_block {
            self.lifecycle = SstScanLifecycle::Exhausted;
            return Ok(());
        }
        self.last_block = self.last_block.min(index.len() - 1);
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
        let block = self
            .reader
            .read_cached_data_block_from(&handle, self.file.as_deref())?;
        let entries = self.reader.scan_block_entries_from_bytes(&block)?;
        self.block_entries = Some(if self.reverse {
            BlockEntryCursor::Reverse(entries.into_iter().rev())
        } else {
            BlockEntryCursor::Forward(entries.into_iter())
        });
        Ok(true)
    }

    fn next_raw_entry(&mut self) -> MidgeResult<Option<SstEntry>> {
        loop {
            if let Some(entry) = self.block_entries.as_mut().and_then(BlockEntryCursor::next) {
                if self
                    .start
                    .as_deref()
                    .is_some_and(|start| entry.key.as_slice() < start)
                    || self
                        .end
                        .as_deref()
                        .is_some_and(|end| entry.key.as_slice() >= end)
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

    fn consider_entry(&self, best: &mut KeyState, entry: SstEntry) {
        if self.snapshot_seq != u64::MAX && entry.sequence > self.snapshot_seq {
            return;
        }

        let candidate = if entry.is_tombstone() || entry.is_expired(self.now_millis) {
            KeyState::Tombstone(entry.sequence)
        } else {
            SstFileIo::state_from_entry(entry)
        };
        SstFileIo::merge_newer_state(best, candidate);
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

            let key = first.key.clone();
            let mut best = KeyState::Absent;
            self.consider_entry(&mut best, first);

            while let Some(entry) = self.next_raw_entry()? {
                if entry.key == key {
                    self.consider_entry(&mut best, entry);
                } else {
                    self.pending_entry = Some(entry);
                    break;
                }
            }

            if !matches!(best, KeyState::Absent) {
                return Ok(Some((Bytes::from(key), best)));
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
            footer: None,
            block_region_end: 0,
            // Readers without a cache never consume this placeholder. Cache
            // attachment requires a generation-scoped identity explicitly.
            sst_id: 0,
            block_bloom_filter: None,
            bloom_metrics: BloomMetrics::new(),
            read_amp_metrics: ReadAmpMetrics::new(),
            trie_reader: None,
            block_cache: None,
            diagnostics: crate::diagnostics::legacy_runtime_diagnostics(),
            index_entries: ArcSwapOption::empty(),
            format_version: SST_FORMAT_V4,
            index_kind: IndexKind::Sparse,
            smallest_key: None,
            largest_key: None,
            range_tombstone_handle: None,
            range_tombstones: Vec::new(),
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
        let mut reader = Self::new(path_str, fs);
        reader.metadata_budget = Some(budget);
        reader.load_metadata()?;
        Ok(reader)
    }

    /// Open an SST file using `RealFs` (convenience method for single-file access)
    /// This creates a new `RealFs` instance for the parent directory of the SST file.
    ///
    /// # Errors
    ///
    /// Returns an error when the filesystem cannot be opened or the SST metadata cannot be read.
    pub fn open_with_real_fs(path: &std::path::Path) -> MidgeResult<Self> {
        let parent = path.parent().unwrap_or_else(|| std::path::Path::new("."));
        let fs = Arc::new(crate::io::RealFs::new(parent)?);
        // Use filename relative to parent dir so RealFs (rooted at parent) resolves it correctly
        let path_str = path
            .file_name()
            .and_then(|n| n.to_str())
            .unwrap_or("")
            .to_string();
        Self::open(&path_str, fs)
    }

    /// Summarize an SST file opened via `RealFs`.
    ///
    /// # Errors
    ///
    /// Returns an error when the SST cannot be opened or summarized.
    pub fn summarize_with_real_fs(path: &std::path::Path) -> MidgeResult<SstFileSummary> {
        Self::open_with_real_fs(path)?.into_streaming_summary()
    }

    /// Stream a summary through the caller's filesystem abstraction.
    pub(crate) fn summarize_with_fs(path: &str, fs: Arc<dyn Fs>) -> MidgeResult<SstFileSummary> {
        Self::open(path, fs)?.into_streaming_summary()
    }

    /// Stream a summary while charging reader metadata and decoded blocks to
    /// the compaction publication budget.
    pub(crate) fn summarize_with_real_fs_for_compaction(
        path: &std::path::Path,
        budget: crate::common::resource_budget::ResourceBudget,
    ) -> MidgeResult<SstFileSummary> {
        let parent = path.parent().unwrap_or_else(|| std::path::Path::new("."));
        let fs = Arc::new(crate::io::RealFs::new(parent)?);
        let path_str = path
            .file_name()
            .and_then(|name| name.to_str())
            .unwrap_or("")
            .to_string();
        Self::open_for_compaction(&path_str, fs, budget)?.into_streaming_summary()
    }

    /// Enable block bloom filter for this reader
    #[must_use]
    pub fn with_block_bloom(mut self, block_bloom: BlockBloomFilter) -> Self {
        self.block_bloom_filter = Some(block_bloom);
        self
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

    /// Attach the runtime that owns this reader's diagnostics.
    #[must_use]
    pub(crate) fn with_read_path_diagnostics(
        mut self,
        diagnostics: Arc<crate::diagnostics::RuntimeDiagnostics>,
    ) -> Self {
        self.diagnostics = diagnostics;
        self
    }

    pub(crate) fn sst_id(&self) -> u64 {
        self.sst_id
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

    /// Get reference to bloom metrics for this reader
    pub fn bloom_metrics(&self) -> &BloomMetrics {
        &self.bloom_metrics
    }

    /// Get reference to read amplification metrics for this reader
    pub fn read_amp_metrics(&self) -> &ReadAmpMetrics {
        &self.read_amp_metrics
    }

    /// Derive key-range and sequence metadata from the actual SST contents.
    ///
    /// # Errors
    ///
    /// Returns an error when the SST cannot be scanned or has no publishable entries.
    pub fn summary(&self) -> MidgeResult<SstFileSummary> {
        use crate::sst::traits::SstStateReader;

        let size_bytes = self.fs.metadata(&self.path)?.len;
        let entries = self.scan_range_state(None, None)?;

        let mut smallest_key: Option<Vec<u8>> = None;
        let mut largest_key: Option<Vec<u8>> = None;
        let mut smallest_seq: Option<u64> = None;
        let mut largest_seq: Option<u64> = None;

        for (key, state) in entries {
            let key_vec = key.to_vec();
            if smallest_key
                .as_ref()
                .is_none_or(|current| key_vec.as_slice() < current.as_slice())
            {
                smallest_key = Some(key_vec.clone());
            }
            if largest_key
                .as_ref()
                .is_none_or(|current| key_vec.as_slice() > current.as_slice())
            {
                largest_key = Some(key_vec);
            }

            let seq = match state {
                KeyState::Value(_, seq, _, _) | KeyState::Tombstone(seq) => seq,
                KeyState::Absent => continue,
            };
            smallest_seq = Some(smallest_seq.map_or(seq, |current| current.min(seq)));
            largest_seq = Some(largest_seq.map_or(seq, |current| current.max(seq)));
        }

        for tombstone in self.range_tombstones() {
            if smallest_key
                .as_ref()
                .is_none_or(|current| tombstone.start.as_slice() < current.as_slice())
            {
                smallest_key = Some(tombstone.start.clone());
            }
            if largest_key
                .as_ref()
                .is_none_or(|current| tombstone.end.as_slice() > current.as_slice())
            {
                largest_key = Some(tombstone.end.clone());
            }
            smallest_seq =
                Some(smallest_seq.map_or(tombstone.seq, |current| current.min(tombstone.seq)));
            largest_seq =
                Some(largest_seq.map_or(tombstone.seq, |current| current.max(tombstone.seq)));
        }

        Ok(SstFileSummary {
            size_bytes,
            smallest_key: smallest_key.ok_or_else(|| {
                MidgeError::Corruption(format!(
                    "SST '{}' contains no publishable entries",
                    self.path.0.as_str()
                ))
            })?,
            largest_key: largest_key.ok_or_else(|| {
                MidgeError::Corruption(format!(
                    "SST '{}' contains no publishable entries",
                    self.path.0.as_str()
                ))
            })?,
            smallest_seq: smallest_seq.ok_or_else(|| {
                MidgeError::Corruption(format!(
                    "SST '{}' contains no publishable sequence bounds",
                    self.path.0.as_str()
                ))
            })?,
            largest_seq: largest_seq.ok_or_else(|| {
                MidgeError::Corruption(format!(
                    "SST '{}' contains no publishable sequence bounds",
                    self.path.0.as_str()
                ))
            })?,
        })
    }

    fn into_streaming_summary(self) -> MidgeResult<SstFileSummary> {
        use crate::sst::traits::SstStateReader;

        let size_bytes = self.fs.metadata(&self.path)?.len;
        let mut smallest_key: Option<Vec<u8>> = None;
        let mut largest_key: Option<Vec<u8>> = None;
        let mut smallest_seq: Option<u64> = None;
        let mut largest_seq: Option<u64> = None;

        for tombstone in &self.range_tombstones {
            if smallest_key
                .as_ref()
                .is_none_or(|current| tombstone.start.as_slice() < current.as_slice())
            {
                smallest_key = Some(tombstone.start.clone());
            }
            if largest_key
                .as_ref()
                .is_none_or(|current| tombstone.end.as_slice() > current.as_slice())
            {
                largest_key = Some(tombstone.end.clone());
            }
            smallest_seq =
                Some(smallest_seq.map_or(tombstone.seq, |current| current.min(tombstone.seq)));
            largest_seq =
                Some(largest_seq.map_or(tombstone.seq, |current| current.max(tombstone.seq)));
        }

        let cursor_budget = self.metadata_budget.clone();
        let mut cursor =
            Box::new(self).raw_version_cursor_with_budget(None, None, cursor_budget)?;
        for version in &mut cursor {
            let version = version?;
            if smallest_key
                .as_ref()
                .is_none_or(|current| version.key.as_slice() < current.as_slice())
            {
                smallest_key = Some(version.key.clone());
            }
            if largest_key
                .as_ref()
                .is_none_or(|current| version.key.as_slice() > current.as_slice())
            {
                largest_key = Some(version.key);
            }
            smallest_seq =
                Some(smallest_seq.map_or(version.seq, |current| current.min(version.seq)));
            largest_seq = Some(largest_seq.map_or(version.seq, |current| current.max(version.seq)));
        }

        Ok(SstFileSummary {
            size_bytes,
            smallest_key: smallest_key.ok_or_else(|| {
                MidgeError::Corruption("SST contains no publishable entries".into())
            })?,
            largest_key: largest_key.ok_or_else(|| {
                MidgeError::Corruption("SST contains no publishable entries".into())
            })?,
            smallest_seq: smallest_seq.ok_or_else(|| {
                MidgeError::Corruption("SST contains no publishable sequence bounds".into())
            })?,
            largest_seq: largest_seq.ok_or_else(|| {
                MidgeError::Corruption("SST contains no publishable sequence bounds".into())
            })?,
        })
    }

    /// Readahead window size: read up to this many blocks in a single IO operation
    /// for cold-cache range scans. Tuned for typical SSD latency/throughput tradeoffs.
    pub(super) const READAHEAD_WINDOW_BLOCKS: usize = 32;
}

#[cfg(test)]
mod tests;
