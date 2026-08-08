//! Filesystem-backed SST reader using `io::Fs` abstraction (new approach)
//!
//! This reader uses the base `io::Fs` trait instead of `std::fs` directly,
//! allowing for swappable implementations (Real, Mock, Chaos) for testing.

use arc_swap::ArcSwapOption;
use bytes::Bytes;
use std::sync::Arc;

use crate::common::{MidgeError, MidgeResult};
use crate::io::{Fs, FsPath};
use crate::sst::bloom::{BlockBloomFilter, BloomMetrics};
use crate::sst::cache::BlockCache;
use crate::sst::index::tuner::IndexKind;
use crate::sst::read_amp_metrics::ReadAmpMetrics;
use crate::sst::trie::TrieReader;
use crate::sst::types::{BlockHandle, Footer, KeyState, RangeTombstone, SstEntry, SST_FORMAT_V1};

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
    range_tombstones: Vec<RangeTombstone>,
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
    exhausted: bool,
    index: Option<IndexEntries>,
    first_block: usize,
    last_block: usize,
    next_block: usize,
    block_entries: Option<BlockEntryCursor>,
    pending_entry: Option<SstEntry>,
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
            initialized: false,
            exhausted: false,
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
            self.exhausted = true;
            return Ok(());
        }

        let index = self.reader.index_entries()?;
        if index.is_empty() {
            self.exhausted = true;
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
            self.exhausted = true;
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
        if self.exhausted {
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
                self.exhausted = true;
            } else {
                self.next_block = block_index - 1;
            }
        } else if block_index == self.last_block {
            self.exhausted = true;
        } else {
            self.next_block = block_index + 1;
        }

        self.reader
            .diagnostics
            .sst_metrics()
            .record_candidate_blocks_checked(1);
        let block = self.reader.read_cached_data_block(&handle)?;
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
        if self.exhausted && self.block_entries.is_none() && self.pending_entry.is_none() {
            return None;
        }

        match self.next_state() {
            Ok(Some(state)) => Some(Ok(state)),
            Ok(None) => None,
            Err(error) => {
                self.exhausted = true;
                self.block_entries = None;
                self.pending_entry = None;
                Some(Err(error))
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
            format_version: SST_FORMAT_V1,
            index_kind: IndexKind::Sparse,
            smallest_key: None,
            largest_key: None,
            range_tombstones: Vec::new(),
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
        Self::open_with_real_fs(path)?.summary()
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
        if let Some(ref footer) = self.footer {
            if let Some(block_bloom_handle) = footer.block_bloom_handle {
                let bloom_data = self.read_block(&block_bloom_handle)?;
                let block_bloom = BlockBloomFilter::deserialize(&bloom_data)?;
                self.block_bloom_filter = Some(block_bloom);
            }
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

    /// Readahead window size: read up to this many blocks in a single IO operation
    /// for cold-cache range scans. Tuned for typical SSD latency/throughput tradeoffs.
    pub(super) const READAHEAD_WINDOW_BLOCKS: usize = 32;
}

#[cfg(test)]
mod tests;
