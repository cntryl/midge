//! Factory for creating `io::Fs-backed` SST readers and writers

use crate::common::MidgeResult;
use crate::sst::traits::{DynSstWriter, SstFactory};
use std::io::{Read, Write};
use std::path::Path;
use std::sync::Arc;

use crate::io::Fs;

use crate::sst::bloom::{BlockBloomFilter, BloomWriter};
use crate::sst::compression::CompressionPolicy;
use crate::sst::encoding::EntryType;
use crate::sst::index::profiler::{KeyStructureProfile, KeyStructureProfiler};
use crate::sst::index::tuner::{IndexKind, IndexTuner};
use crate::sst::trie::writer::TrieWriter;
use crate::sst::types::{
    encode_range_tombstones, BlockHandle, Footer, KeyRangeMetadata, RangeTombstone, SstMetadata,
    SST_FORMAT_V4,
};

/// SST factory that uses the `io::Fs` abstraction.
///
/// Readers and writers both address `fs`: a writer carries it from creation
/// and publishes through [`crate::io::staging`], so mock and fault-injecting
/// filesystems observe the staging write, fsync, rename, and directory sync
/// that publish an SST. Writers map their target onto `fs` and refuse a target
/// outside its root rather than publishing somewhere the caller did not name.
pub struct FsSstFactoryIo {
    fs: Arc<dyn Fs>,
    block_size: usize,
    compression_policy: CompressionPolicy,
    scratch_outstanding: Arc<std::sync::atomic::AtomicUsize>,
    compaction_scratch_directory: Option<std::path::PathBuf>,
}

impl FsSstFactoryIo {
    pub(crate) fn create_for_flush(
        &self,
        budget: crate::common::resource_budget::ResourceBudget,
    ) -> MidgeResult<Box<dyn DynSstWriter>> {
        let mut writer = FsSstWriter::new_with_budget(
            Arc::clone(&self.fs),
            self.compression_policy.clone(),
            self.block_size,
            Some(budget.clone()),
        );
        writer.streaming = Some(StreamingState::new_tracked(
            Some(budget),
            Arc::clone(&self.scratch_outstanding),
            self.compaction_scratch_directory.as_deref(),
        )?);
        Ok(Box::new(writer))
    }
    /// Create a new factory with a custom filesystem implementation
    pub fn new(fs: Arc<dyn Fs>, block_size: usize) -> Self {
        Self {
            fs,
            block_size,
            compression_policy: CompressionPolicy::default(),
            scratch_outstanding: Arc::default(),
            compaction_scratch_directory: None,
        }
    }

    pub(crate) fn with_compaction_scratch_directory(
        mut self,
        directory: std::path::PathBuf,
    ) -> Self {
        self.compaction_scratch_directory = Some(directory);
        self
    }

    /// Create with custom block size
    #[must_use]
    pub fn with_block_size(mut self, block_size: usize) -> Self {
        self.block_size = block_size;
        self
    }

    /// Set the compression policy for SST blocks produced by this factory.
    #[must_use]
    pub fn with_compression_policy(mut self, policy: CompressionPolicy) -> Self {
        self.compression_policy = policy;
        self
    }

    /// Open an SST file using the `io::Fs` backend
    ///
    /// # Errors
    ///
    /// Returns an error if the file cannot be opened or parsed as an SST reader.
    pub fn open(&self, path: &Path) -> MidgeResult<Box<dyn crate::sst::traits::SstReaderExt>> {
        self.open_internal(path, None)
    }

    fn open_internal(
        &self,
        path: &Path,
        budget: Option<crate::common::resource_budget::ResourceBudget>,
    ) -> MidgeResult<Box<dyn crate::sst::traits::SstReaderExt>> {
        let path_str = path.to_str().unwrap_or("").to_string();
        let start = std::time::Instant::now();
        let reader = if let Some(budget) = budget {
            super::SstFileIo::open_for_compaction(&path_str, Arc::clone(&self.fs), budget)?
        } else {
            super::SstFileIo::open(&path_str, Arc::clone(&self.fs))?
        };
        let elapsed = start.elapsed();
        // Try to gather file size for diagnostics (best-effort)
        let size = self
            .fs
            .metadata(&crate::io::FsPath::new(path_str.as_str()))
            .ok()
            .map_or(0, |m| m.len);
        tracing::info!(path = ?path, size_bytes = size, open_ms = elapsed.as_secs_f64() * 1000.0, "sst reader opened");
        Ok(Box::new(reader))
    }
}

/// Write an SST whose single entry holds an uncompressed value larger than the
/// decompressed-block ceiling, exactly as the writer produced before that
/// admission limit existed.
///
/// Compaction owns the round-trip assertion for these files, so the fixture is
/// built here — where the writer's pending-entry representation lives — and the
/// behaviour is asserted in `crate::compaction`.
#[cfg(test)]
pub(crate) fn write_legacy_oversized_uncompressed_sst(
    fs: Arc<dyn Fs>,
    path: &Path,
    key: &[u8],
    value: Vec<u8>,
    sequence: u64,
) -> MidgeResult<()> {
    let mut legacy = FsSstWriter::new(
        fs,
        CompressionPolicy::Fixed(crate::sst::compression::CompressionAlgo::None),
        4096,
    );
    legacy.entries.push(PendingEntry {
        key: key.to_vec(),
        value: Some(value),
        sequence,
        op_type: EntryType::Put,
        expiration: Some(u64::MAX),
    });
    super::finish_writer_to_path(Box::new(legacy), path)
}

/// Simple in-memory SST writer that applies block-level compression.
struct FsSstWriter {
    /// Filesystem this writer publishes through, injected by the factory so
    /// staging writes, fsyncs, the rename, and the directory sync all reach
    /// the same backend the factory reads from.
    fs: Arc<dyn Fs>,
    entries: Vec<PendingEntry>,
    range_tombstones: Vec<RangeTombstone>,
    block_size: usize,
    compression_policy: CompressionPolicy,
    /// Activated only by `add_sorted_with_meta`, which is the compaction
    /// contract. Complete encoded blocks are spilled to a scratch file rather
    /// than retained as logical entries until finalization.
    streaming: Option<StreamingState>,
    budget: Option<crate::common::resource_budget::ResourceBudget>,
    range_tombstone_reservations: Vec<crate::common::resource_budget::ResourceReservation>,
    /// Only budgeted compaction writers may rewrite existing entries beyond
    /// new-write admission limits. Oversized output blocks stay uncompressed.
    preserve_legacy_entries: bool,
}

#[derive(Debug, Clone)]
struct PendingEntry {
    key: Vec<u8>,
    value: Option<Vec<u8>>,
    sequence: u64,
    op_type: EntryType,
    expiration: Option<u64>,
}

struct FinalizedDataBlocks {
    file_bytes: Vec<u8>,
    block_index_entries: Vec<(Vec<u8>, BlockHandle)>,
    block_bloom: BlockBloomFilter,
    key_profile: KeyStructureProfile,
    smallest_key: Option<Vec<u8>>,
    largest_key: Option<Vec<u8>>,
}

struct StreamingState {
    scratch: super::scratch::TrackedScratch,
    offset: u64,
    block_index_entries: Vec<(Vec<u8>, BlockHandle)>,
    key_profiler: KeyStructureProfiler,
    current_block: Vec<u8>,
    current_block_keys: Vec<Vec<u8>>,
    current_first_key: Option<Vec<u8>>,
    block_bloom: BlockBloomFilter,
    previous_key: Vec<u8>,
    last_key: Option<Vec<u8>>,
    last_sequence: u64,
    smallest_key: Option<Vec<u8>>,
    largest_key: Option<Vec<u8>>,
    budget: Option<crate::common::resource_budget::ResourceBudget>,
    current_reservations: Vec<crate::common::resource_budget::ResourceReservation>,
    persistent_reservations: Vec<crate::common::resource_budget::ResourceReservation>,
}

impl StreamingState {
    fn new(budget: Option<crate::common::resource_budget::ResourceBudget>) -> MidgeResult<Self> {
        Self::new_tracked(budget, Arc::default(), None)
    }

    fn new_tracked(
        budget: Option<crate::common::resource_budget::ResourceBudget>,
        outstanding: Arc<std::sync::atomic::AtomicUsize>,
        directory: Option<&Path>,
    ) -> MidgeResult<Self> {
        Ok(Self {
            scratch: super::scratch::TrackedScratch::new(outstanding, directory)
                .map_err(crate::common::MidgeError::Io)?,
            offset: 0,
            block_index_entries: Vec::new(),
            key_profiler: KeyStructureProfiler::new(),
            current_block: Vec::new(),
            current_block_keys: Vec::new(),
            current_first_key: None,
            block_bloom: BlockBloomFilter::new(),
            previous_key: Vec::new(),
            last_key: None,
            last_sequence: 0,
            smallest_key: None,
            largest_key: None,
            budget,
            current_reservations: Vec::new(),
            persistent_reservations: Vec::new(),
        })
    }
}

impl DynSstWriter for FsSstWriter {
    fn encoded_size_upper_bound_after_sorted_entry(
        &self,
        key: &[u8],
        value: Option<&[u8]>,
    ) -> Option<usize> {
        // A new block may add one index/trie boundary, filter framing and two
        // key bounds. Its uncompressed data includes at most 32 framing bytes;
        // doubling the payload also covers fixed-compressor expansion.
        self.encoded_size_upper_bound().map(|bound| {
            bound
                .saturating_add(512)
                .saturating_add(key.len().saturating_mul(12))
                .saturating_add(value.map_or(0, <[u8]>::len).saturating_mul(2))
        })
    }

    fn additional_range_tombstone_size_upper_bound(
        &self,
        start: &[u8],
        end: &[u8],
    ) -> Option<usize> {
        Some(crate::sst::size_bound::range_bytes(start.len(), end.len()))
    }

    fn encoded_size_upper_bound(&self) -> Option<usize> {
        // A trie has at most two nodes per boundary key; each node and edge
        // uses fewer than 64 bytes of integer framing. Key payloads also
        // appear in the block index and the two metadata bounds. The factors
        // below allow their copies plus worst-case fixed-compressor growth.
        let ranges = self.range_tombstones.iter().fold(0usize, |total, range| {
            total.saturating_add(
                self.additional_range_tombstone_size_upper_bound(&range.start, &range.end)
                    .unwrap_or(usize::MAX),
            )
        });
        let fixed = ranges.saturating_add(crate::sst::size_bound::FIXED_SST_BYTES);
        let Some(streaming) = &self.streaming else {
            return Some(self.entries.iter().fold(fixed, |total, entry| {
                total.saturating_add(crate::sst::size_bound::point_bytes(
                    entry.key.len(),
                    entry.value.as_ref().map_or(0, Vec::len),
                ))
            }));
        };
        let index = streaming
            .block_index_entries
            .iter()
            .fold(0usize, |total, (key, _)| {
                total
                    .saturating_add(256)
                    .saturating_add(key.len().saturating_mul(8))
            });
        let current_index = streaming.current_first_key.as_ref().map_or(0, |key| {
            256usize.saturating_add(key.len().saturating_mul(8))
        });
        let current_bloom =
            BloomWriter::with_defaults(streaming.current_block_keys.len()).size_bytes();
        let key_bounds = streaming
            .smallest_key
            .as_ref()
            .map_or(0, Vec::len)
            .saturating_add(streaming.largest_key.as_ref().map_or(0, Vec::len))
            .saturating_mul(2);
        Some(
            fixed
                .saturating_add(usize::try_from(streaming.offset).unwrap_or(usize::MAX))
                .saturating_add(streaming.current_block.len().saturating_mul(2))
                .saturating_add(index)
                .saturating_add(current_index)
                .saturating_add(key_bounds)
                .saturating_add(
                    streaming
                        .block_bloom
                        .size_bytes()
                        .saturating_add(current_bloom)
                        .saturating_mul(2),
                ),
        )
    }

    fn estimated_size_bytes(&self) -> usize {
        if let Some(streaming) = &self.streaming {
            let persisted = usize::try_from(streaming.offset).unwrap_or(usize::MAX);
            let index = streaming.block_index_entries.iter().fold(
                streaming
                    .block_index_entries
                    .len()
                    .saturating_mul(std::mem::size_of::<(Vec<u8>, BlockHandle)>()),
                |total, (key, _)| total.saturating_add(key.len()),
            );
            let current_bloom = if streaming.current_block_keys.is_empty() {
                0
            } else {
                BloomWriter::with_defaults(streaming.current_block_keys.len())
                    .size_bytes()
                    .saturating_add(13)
            };
            let bloom = streaming
                .block_bloom
                .size_bytes()
                .saturating_add(current_bloom);
            return persisted
                .saturating_add(streaming.current_block.len())
                .saturating_add(index.saturating_mul(2))
                .saturating_add(bloom)
                .saturating_add(16 * 1024);
        }
        self.entries.iter().fold(0usize, |total, entry| {
            total
                .saturating_add(entry.key.len())
                .saturating_add(entry.value.as_ref().map_or(0, Vec::len))
                .saturating_add(32)
        })
    }

    fn add_with_meta(
        &mut self,
        key: &[u8],
        value: Option<&[u8]>,
        seq: u64,
        op_type: EntryType,
        expiration: Option<u64>,
    ) -> MidgeResult<()> {
        Self::writable_entry_type(op_type)?;
        if !self.preserve_legacy_entries {
            crate::sst::encoding::validate_entry_size(key.len(), value.map_or(0, <[u8]>::len))?;
        }
        if self.streaming.is_some() {
            return Err(crate::common::MidgeError::InvalidArgument(
                "cannot append unordered entries after sorted SST streaming has started"
                    .to_string(),
            ));
        }
        self.entries.push(PendingEntry {
            key: key.to_vec(),
            value: value.map(<[u8]>::to_vec),
            sequence: seq,
            op_type,
            expiration,
        });
        Ok(())
    }

    fn add_sorted_with_meta(
        &mut self,
        key: &[u8],
        value: Option<&[u8]>,
        seq: u64,
        op_type: EntryType,
        expiration: Option<u64>,
    ) -> MidgeResult<()> {
        Self::writable_entry_type(op_type)?;
        if !self.preserve_legacy_entries {
            crate::sst::encoding::validate_entry_size(key.len(), value.map_or(0, <[u8]>::len))?;
        }
        if !self.entries.is_empty() {
            return Err(crate::common::MidgeError::InvalidArgument(
                "cannot start sorted SST streaming after unordered entries were added".to_string(),
            ));
        }
        if self.streaming.is_none() {
            self.streaming = Some(StreamingState::new(self.budget.clone())?);
        }

        let block_size = self.block_size;
        let compression_policy = self.compression_policy.clone();
        let retained_bytes = std::mem::size_of::<PendingEntry>()
            .saturating_add(key.len().saturating_mul(4))
            .saturating_add(value.map_or(0, <[u8]>::len))
            .saturating_add(64);
        let entry_reservation = self
            .budget
            .as_ref()
            .map(|budget| budget.reserve(retained_bytes, "SST current block entry"))
            .transpose()?;
        let entry = PendingEntry {
            key: key.to_vec(),
            value: value.map(<[u8]>::to_vec),
            sequence: seq,
            op_type,
            expiration,
        };
        Self::append_sorted_entry(
            self.streaming
                .as_mut()
                .expect("streaming state is initialized above"),
            entry,
            block_size,
            &compression_policy,
            entry_reservation,
        )
    }

    fn add_range_tombstone(&mut self, start: &[u8], end: &[u8], seq: u64) -> MidgeResult<()> {
        if !self.preserve_legacy_entries {
            crate::sst::types::validate_range_tombstone_size(start.len(), end.len())?;
        }
        let retained_bytes = std::mem::size_of::<RangeTombstone>()
            .saturating_add(start.len())
            .saturating_add(end.len());
        let reservation = self
            .budget
            .as_ref()
            .map(|budget| budget.reserve(retained_bytes, "SST range tombstone metadata"))
            .transpose()?;
        self.range_tombstones
            .push(RangeTombstone::new(start.to_vec(), end.to_vec(), seq));
        if let Some(reservation) = reservation {
            self.range_tombstone_reservations.push(reservation);
        }
        Ok(())
    }

    fn finish_to_path(self: Box<Self>, path: &Path) -> MidgeResult<()> {
        if self.streaming.is_none() {
            let fs = Arc::clone(&self.fs);
            let bytes = self.finish_bytes()?;
            return crate::sst::fs::persist_sst_bytes_to_path(&fs, &bytes, path);
        }

        let FsSstWriter {
            fs,
            entries,
            range_tombstones,
            block_size: _,
            compression_policy,
            streaming,
            budget: _,
            range_tombstone_reservations: _range_tombstone_reservations,
            preserve_legacy_entries: _,
        } = *self;
        debug_assert!(entries.is_empty());
        let scratch = Self::finish_streaming(
            streaming.expect("streaming writer checked above"),
            &range_tombstones,
            &compression_policy,
        )?;
        let mut source = scratch.reopen().map_err(crate::common::MidgeError::Io)?;
        let result = crate::sst::fs::persist_sst_stream_to_path(&fs, &mut source, path);
        drop(source);
        let cleanup = scratch.close().map_err(crate::common::MidgeError::Io);
        result.and(cleanup)
    }

    fn finish_bytes(self: Box<Self>) -> MidgeResult<Vec<u8>> {
        let FsSstWriter {
            fs,
            entries,
            range_tombstones,
            block_size,
            compression_policy,
            streaming,
            budget,
            range_tombstone_reservations: _range_tombstone_reservations,
            preserve_legacy_entries,
        } = *self;

        if let Some(streaming) = streaming {
            let scratch =
                Self::finish_streaming(streaming, &range_tombstones, &compression_policy)?;
            let mut source = scratch.reopen().map_err(crate::common::MidgeError::Io)?;
            let mut bytes = Vec::new();
            source
                .read_to_end(&mut bytes)
                .map_err(crate::common::MidgeError::Io)?;
            drop(source);
            scratch.close().map_err(crate::common::MidgeError::Io)?;
            return Ok(bytes);
        }

        let writer = Self {
            fs,
            entries: Vec::new(),
            range_tombstones,
            block_size,
            compression_policy,
            streaming: None,
            budget,
            range_tombstone_reservations: Vec::new(),
            preserve_legacy_entries,
        };

        let entries = Self::sort_entries(entries);
        let mut finalized = writer.finalize_data_blocks(entries)?;
        let range_tombstone_handle =
            writer.append_range_tombstone_block(&mut finalized.file_bytes)?;
        let chosen = IndexTuner::decide(&finalized.key_profile);
        let trie_bytes = Self::build_trie_index(chosen, &finalized.block_index_entries);
        let index_kind = Self::effective_index_kind(chosen, trie_bytes.as_ref());
        let trie_handle = Self::append_trie_block(
            &mut finalized.file_bytes,
            &writer.compression_policy,
            trie_bytes.as_ref(),
        )?;
        let metadata = SstMetadata {
            format_version: SST_FORMAT_V4,
            index_kind,
            range_tombstone_handle,
            key_range: finalized.smallest_key.zip(finalized.largest_key).map(
                |(smallest_key, largest_key)| KeyRangeMetadata {
                    smallest_key,
                    largest_key,
                },
            ),
        };
        Self::append_metadata_index_and_footer(
            &mut finalized.file_bytes,
            &writer.compression_policy,
            &metadata,
            &finalized.block_index_entries,
            trie_handle,
            &finalized.block_bloom,
        )?;
        Ok(finalized.file_bytes)
    }
}

impl SstFactory for FsSstFactoryIo {
    fn compaction_scratch_cleanup_verified(&self) -> bool {
        self.scratch_outstanding
            .load(std::sync::atomic::Ordering::Acquire)
            == 0
    }
    /// Create a new SST writer
    fn create(&self) -> MidgeResult<Box<dyn DynSstWriter>> {
        Ok(Box::new(FsSstWriter::new(
            Arc::clone(&self.fs),
            self.compression_policy.clone(),
            self.block_size,
        )))
    }

    fn create_for_compaction(
        &self,
        budget: crate::common::resource_budget::ResourceBudget,
    ) -> MidgeResult<Box<dyn DynSstWriter>> {
        let mut writer = FsSstWriter::new_with_budget(
            Arc::clone(&self.fs),
            self.compression_policy.clone(),
            self.block_size,
            Some(budget.clone()),
        );
        writer.streaming = Some(StreamingState::new_tracked(
            Some(budget),
            Arc::clone(&self.scratch_outstanding),
            self.compaction_scratch_directory.as_deref(),
        )?);
        writer.preserve_legacy_entries = true;
        Ok(Box::new(writer))
    }

    /// Open an existing SST file
    fn open(&self, path: &Path) -> MidgeResult<Box<dyn crate::sst::traits::SstReaderExt>> {
        FsSstFactoryIo::open(self, path)
    }

    fn open_for_compaction(
        &self,
        path: &Path,
        budget: crate::common::resource_budget::ResourceBudget,
    ) -> MidgeResult<Box<dyn crate::sst::traits::SstReaderExt>> {
        self.open_internal(path, Some(budget))
    }
}

mod pipeline;

#[cfg(test)]
mod tests;
