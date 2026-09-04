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

/// SST factory that uses `io::Fs` abstraction
/// Allows using real and mock filesystem implementations for testing.
pub struct FsSstFactoryIo {
    fs: Arc<dyn Fs>,
    block_size: usize,
    compression_policy: CompressionPolicy,
}

impl FsSstFactoryIo {
    /// Create a new factory with a custom filesystem implementation
    pub fn new(fs: Arc<dyn Fs>, block_size: usize) -> Self {
        Self {
            fs,
            block_size,
            compression_policy: CompressionPolicy::default(),
        }
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

/// Simple in-memory SST writer that applies block-level compression.
struct InMemorySstWriter {
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
}

#[derive(Debug, Clone)]
struct PendingEntry {
    key: Vec<u8>,
    value: Option<Vec<u8>>,
    sequence: u64,
    op_type: u8,
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
    scratch: tempfile::NamedTempFile,
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
        Ok(Self {
            scratch: tempfile::NamedTempFile::new().map_err(crate::common::MidgeError::Io)?,
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

impl InMemorySstWriter {
    fn new(compression_policy: CompressionPolicy, block_size: usize) -> Self {
        Self::new_with_budget(compression_policy, block_size, None)
    }

    fn new_with_budget(
        compression_policy: CompressionPolicy,
        block_size: usize,
        budget: Option<crate::common::resource_budget::ResourceBudget>,
    ) -> Self {
        Self {
            entries: Vec::new(),
            range_tombstones: Vec::new(),
            block_size,
            compression_policy,
            streaming: None,
            budget,
            range_tombstone_reservations: Vec::new(),
        }
    }

    fn append_block(
        file_bytes: &mut Vec<u8>,
        block_bytes: &[u8],
        compression_policy: &CompressionPolicy,
    ) -> MidgeResult<BlockHandle> {
        use crate::sst::compression;

        let compressed = compression::compress_block_with_trailer(block_bytes, compression_policy)?;
        let offset = u64::try_from(file_bytes.len()).map_err(|_| {
            crate::common::MidgeError::ResourceLimit(
                "SST output offset exceeds the supported range".to_string(),
            )
        })?;
        let (payload_len, size) = Self::checked_block_payload_len(compressed.len())?;
        file_bytes.extend_from_slice(&payload_len.to_le_bytes());
        file_bytes.extend_from_slice(&compressed);
        Ok(BlockHandle::new(offset, size))
    }

    fn append_block_to_stream(
        file: &mut std::fs::File,
        offset: &mut u64,
        block_bytes: &[u8],
        compression_policy: &CompressionPolicy,
    ) -> MidgeResult<BlockHandle> {
        use crate::sst::compression;

        let compressed = compression::compress_block_with_trailer(block_bytes, compression_policy)?;
        let (payload_len, size) = Self::checked_block_payload_len(compressed.len())?;
        let handle = BlockHandle::new(*offset, size);
        file.write_all(&payload_len.to_le_bytes())
            .map_err(crate::common::MidgeError::Io)?;
        file.write_all(&compressed)
            .map_err(crate::common::MidgeError::Io)?;
        *offset = offset.checked_add(size).ok_or_else(|| {
            crate::common::MidgeError::ResourceLimit(
                "SST stream offset exceeds the supported range".to_string(),
            )
        })?;
        Ok(handle)
    }

    fn checked_block_payload_len(payload_len: usize) -> MidgeResult<(u32, u64)> {
        let encoded_len = u32::try_from(payload_len).map_err(|_| {
            crate::common::MidgeError::ResourceLimit(
                "compressed SST block exceeds the 4 GiB format limit".to_string(),
            )
        })?;
        let total_len = 4u64.checked_add(u64::from(encoded_len)).ok_or_else(|| {
            crate::common::MidgeError::ResourceLimit(
                "encoded SST block length exceeds the supported range".to_string(),
            )
        })?;
        Ok((encoded_len, total_len))
    }

    fn serialize_index(index_entries: &[(Vec<u8>, BlockHandle)]) -> MidgeResult<Vec<u8>> {
        let mut index_bytes = Vec::new();
        for (key, handle) in index_entries {
            let key_len = u32::try_from(key.len()).map_err(|_| {
                crate::common::MidgeError::ResourceLimit(
                    "SST index key exceeds the 4 GiB format limit".to_string(),
                )
            })?;
            index_bytes.extend_from_slice(&key_len.to_le_bytes());
            index_bytes.extend_from_slice(key);
            index_bytes.extend_from_slice(&handle.offset.to_le_bytes());
            index_bytes.extend_from_slice(&handle.size.to_le_bytes());
        }
        Ok(index_bytes)
    }

    fn shared_prefix_len(previous_key: &[u8], key: &[u8]) -> u16 {
        let shared = previous_key
            .iter()
            .zip(key.iter())
            .take_while(|(left, right)| left == right)
            .count();
        u16::try_from(shared.min(u16::MAX as usize)).unwrap_or(u16::MAX)
    }

    fn sort_entries(mut entries: Vec<PendingEntry>) -> Vec<PendingEntry> {
        entries.sort_by(|left, right| {
            left.key
                .cmp(&right.key)
                .then_with(|| right.sequence.cmp(&left.sequence))
        });
        entries
    }

    fn encode_pending_entry(previous_key: &[u8], entry: &PendingEntry) -> MidgeResult<Vec<u8>> {
        let shared_len = Self::shared_prefix_len(previous_key, &entry.key);
        let key_delta = &entry.key[shared_len as usize..];
        crate::sst::encoding::encode_v4(
            key_delta,
            shared_len,
            entry.value.as_deref(),
            entry.sequence,
            match entry.op_type {
                1 => EntryType::Insert,
                2 => EntryType::Delete,
                3 => EntryType::Merge,
                _ => EntryType::Put,
            },
            entry.expiration,
        )
    }

    fn update_key_bounds(
        smallest_key: &mut Option<Vec<u8>>,
        largest_key: &mut Option<Vec<u8>>,
        key: &[u8],
    ) {
        if smallest_key
            .as_ref()
            .is_none_or(|current| key < current.as_slice())
        {
            *smallest_key = Some(key.to_vec());
        }
        if largest_key
            .as_ref()
            .is_none_or(|current| key > current.as_slice())
        {
            *largest_key = Some(key.to_vec());
        }
    }

    fn flush_current_block(
        file_bytes: &mut Vec<u8>,
        current_block: &mut Vec<u8>,
        current_first_key: &mut Option<Vec<u8>>,
        block_index_entries: &mut Vec<(Vec<u8>, BlockHandle)>,
        current_block_keys: &mut Vec<Vec<u8>>,
        block_bloom: &mut BlockBloomFilter,
        compression_policy: &CompressionPolicy,
    ) -> MidgeResult<()> {
        if current_block.is_empty() {
            return Ok(());
        }

        let handle = Self::append_block(file_bytes, current_block, compression_policy)?;
        if let Some(first_key) = current_first_key.take() {
            block_index_entries.push((first_key, handle));
        }
        let mut bloom = BloomWriter::with_defaults(current_block_keys.len().max(1));
        for key in current_block_keys.drain(..) {
            bloom.insert(&key);
        }
        block_bloom.add_block_bloom(&bloom);
        current_block.clear();
        Ok(())
    }

    fn flush_streaming_current_block(
        state: &mut StreamingState,
        compression_policy: &CompressionPolicy,
    ) -> MidgeResult<()> {
        if state.current_block.is_empty() {
            return Ok(());
        }

        let compression_workspace_bytes = state
            .current_block
            .len()
            .saturating_mul(2)
            .saturating_add(4096);
        let _compression_workspace = state
            .budget
            .as_ref()
            .map(|budget| budget.reserve(compression_workspace_bytes, "SST compression workspace"))
            .transpose()?;
        let persistent_bytes = state
            .current_first_key
            .as_ref()
            .map_or(0, |key| {
                key.len()
                    .saturating_add(std::mem::size_of::<(Vec<u8>, BlockHandle)>())
            })
            .saturating_add(state.current_block_keys.len().saturating_mul(16));
        let persistent_reservation = state
            .budget
            .as_ref()
            .map(|budget| budget.reserve(persistent_bytes, "SST index and bloom metadata"))
            .transpose()?;

        let handle = Self::append_block_to_stream(
            state.scratch.as_file_mut(),
            &mut state.offset,
            &state.current_block,
            compression_policy,
        )?;
        if let Some(first_key) = state.current_first_key.take() {
            state.block_index_entries.push((first_key, handle));
        }
        let mut bloom = BloomWriter::with_defaults(state.current_block_keys.len().max(1));
        for key in state.current_block_keys.drain(..) {
            bloom.insert(&key);
        }
        state.block_bloom.add_block_bloom(&bloom);
        state.current_block.clear();
        state.current_reservations.clear();
        if let Some(reservation) = persistent_reservation {
            state.persistent_reservations.push(reservation);
        }
        Ok(())
    }

    fn append_sorted_entry(
        state: &mut StreamingState,
        entry: PendingEntry,
        block_size: usize,
        compression_policy: &CompressionPolicy,
        entry_reservation: Option<crate::common::resource_budget::ResourceReservation>,
    ) -> MidgeResult<()> {
        if let Some(last_key) = &state.last_key {
            match entry.key.cmp(last_key) {
                std::cmp::Ordering::Less => {
                    return Err(crate::common::MidgeError::InvalidArgument(
                        "sorted SST writer received keys out of order".to_string(),
                    ));
                }
                std::cmp::Ordering::Equal if entry.sequence > state.last_sequence => {
                    return Err(crate::common::MidgeError::InvalidArgument(
                        "sorted SST writer received sequences out of descending order".to_string(),
                    ));
                }
                _ => {}
            }
        }

        state.key_profiler.add_key(&entry.key);
        Self::update_key_bounds(&mut state.smallest_key, &mut state.largest_key, &entry.key);

        let target_block_size = block_size.clamp(
            4 * 1024,
            crate::sst::compression::MAX_DECOMPRESSED_BLOCK_SIZE,
        );
        let mut encoded = Self::encode_pending_entry(&state.previous_key, &entry)?;
        if !state.current_block.is_empty()
            && state.current_block.len().saturating_add(encoded.len()) > target_block_size
        {
            Self::flush_streaming_current_block(state, compression_policy)?;
            state.previous_key.clear();
            encoded = Self::encode_pending_entry(&state.previous_key, &entry)?;
        }

        if state.current_first_key.is_none() {
            state.current_first_key = Some(entry.key.clone());
        }
        state.current_block.extend_from_slice(&encoded);
        state.current_block_keys.push(entry.key.clone());
        state.previous_key.clone_from(&entry.key);
        state.last_sequence = entry.sequence;
        state.last_key = Some(entry.key);
        if let Some(reservation) = entry_reservation {
            state.current_reservations.push(reservation);
        }
        Ok(())
    }

    fn finalize_data_blocks(&self, entries: Vec<PendingEntry>) -> MidgeResult<FinalizedDataBlocks> {
        let target_block_size = self.block_size.clamp(
            4 * 1024,
            crate::sst::compression::MAX_DECOMPRESSED_BLOCK_SIZE,
        );
        let mut file_bytes = Vec::new();
        let mut block_index_entries = Vec::new();
        let mut key_profiler = KeyStructureProfiler::new();
        let mut current_block = Vec::new();
        let mut current_block_keys = Vec::new();
        let mut block_bloom = BlockBloomFilter::new();
        let mut current_first_key = None;
        let mut previous_key = Vec::new();
        let mut smallest_key = self
            .range_tombstones
            .iter()
            .map(|tombstone| tombstone.start.clone())
            .min();
        let mut largest_key = self
            .range_tombstones
            .iter()
            .map(|tombstone| tombstone.end.clone())
            .max();

        for entry in entries {
            key_profiler.add_key(&entry.key);
            Self::update_key_bounds(&mut smallest_key, &mut largest_key, &entry.key);

            let mut encoded = Self::encode_pending_entry(&previous_key, &entry)?;
            if !current_block.is_empty()
                && current_block.len().saturating_add(encoded.len()) > target_block_size
            {
                Self::flush_current_block(
                    &mut file_bytes,
                    &mut current_block,
                    &mut current_first_key,
                    &mut block_index_entries,
                    &mut current_block_keys,
                    &mut block_bloom,
                    &self.compression_policy,
                )?;
                previous_key.clear();
                encoded = Self::encode_pending_entry(&previous_key, &entry)?;
            }

            if current_first_key.is_none() {
                current_first_key = Some(entry.key.clone());
            }

            current_block.extend_from_slice(&encoded);
            current_block_keys.push(entry.key.clone());
            previous_key = entry.key;
        }

        Self::flush_current_block(
            &mut file_bytes,
            &mut current_block,
            &mut current_first_key,
            &mut block_index_entries,
            &mut current_block_keys,
            &mut block_bloom,
            &self.compression_policy,
        )?;

        Ok(FinalizedDataBlocks {
            file_bytes,
            block_index_entries,
            block_bloom,
            key_profile: key_profiler.finish(),
            smallest_key,
            largest_key,
        })
    }

    fn append_range_tombstone_block(
        &self,
        file_bytes: &mut Vec<u8>,
    ) -> MidgeResult<Option<BlockHandle>> {
        if self.range_tombstones.is_empty() {
            return Ok(None);
        }

        let block_bytes = encode_range_tombstones(&self.range_tombstones);
        Self::append_block(file_bytes, &block_bytes, &self.compression_policy).map(Some)
    }

    fn append_trie_block(
        file_bytes: &mut Vec<u8>,
        compression_policy: &CompressionPolicy,
        index_kind: IndexKind,
        block_index_entries: &[(Vec<u8>, BlockHandle)],
    ) -> MidgeResult<Option<BlockHandle>> {
        if !matches!(index_kind, IndexKind::Trie) {
            return Ok(None);
        }

        let mut trie_writer = TrieWriter::new(true);
        for (block_index, (first_key, _handle)) in block_index_entries.iter().enumerate() {
            trie_writer.add_block_key(first_key, u32::try_from(block_index).unwrap_or(u32::MAX))?;
        }

        match trie_writer.finish() {
            Some(trie_bytes) => {
                Self::append_block(file_bytes, &trie_bytes, compression_policy).map(Some)
            }
            None => Ok(None),
        }
    }

    fn append_metadata_index_and_footer(
        file_bytes: &mut Vec<u8>,
        compression_policy: &CompressionPolicy,
        metadata: &SstMetadata,
        block_index_entries: &[(Vec<u8>, BlockHandle)],
        trie_handle: Option<BlockHandle>,
        block_bloom: &BlockBloomFilter,
    ) -> MidgeResult<()> {
        let block_bloom_handle =
            Self::append_block(file_bytes, &block_bloom.serialize(), compression_policy)?;
        let meta_handle = Self::append_block(file_bytes, &metadata.encode(), compression_policy)?;
        let index_bytes = Self::serialize_index(block_index_entries)?;
        let index_handle = Self::append_block(file_bytes, &index_bytes, compression_policy)?;
        let footer = trie_handle
            .map_or_else(
                || Footer::new(meta_handle, index_handle),
                |handle| Footer::new(meta_handle, index_handle).with_trie(handle),
            )
            .with_block_bloom(block_bloom_handle);
        file_bytes.extend_from_slice(&footer.encode());
        Ok(())
    }

    fn append_range_tombstone_block_to_stream(
        state: &mut StreamingState,
        range_tombstones: &[RangeTombstone],
        compression_policy: &CompressionPolicy,
    ) -> MidgeResult<Option<BlockHandle>> {
        if range_tombstones.is_empty() {
            return Ok(None);
        }

        let bytes = encode_range_tombstones(range_tombstones);
        Self::append_block_to_stream(
            state.scratch.as_file_mut(),
            &mut state.offset,
            &bytes,
            compression_policy,
        )
        .map(Some)
    }

    fn append_trie_block_to_stream(
        state: &mut StreamingState,
        compression_policy: &CompressionPolicy,
        index_kind: IndexKind,
    ) -> MidgeResult<Option<BlockHandle>> {
        if !matches!(index_kind, IndexKind::Trie) {
            return Ok(None);
        }

        let mut trie_writer = TrieWriter::new(true);
        for (block_index, (first_key, _)) in state.block_index_entries.iter().enumerate() {
            trie_writer.add_block_key(first_key, u32::try_from(block_index).unwrap_or(u32::MAX))?;
        }

        match trie_writer.finish() {
            Some(bytes) => Self::append_block_to_stream(
                state.scratch.as_file_mut(),
                &mut state.offset,
                &bytes,
                compression_policy,
            )
            .map(Some),
            None => Ok(None),
        }
    }

    fn append_metadata_index_and_footer_to_stream(
        state: &mut StreamingState,
        compression_policy: &CompressionPolicy,
        metadata: &SstMetadata,
        block_index_entries: &[(Vec<u8>, BlockHandle)],
        trie_handle: Option<BlockHandle>,
    ) -> MidgeResult<()> {
        let block_bloom_handle = Self::append_block_to_stream(
            state.scratch.as_file_mut(),
            &mut state.offset,
            &state.block_bloom.serialize(),
            compression_policy,
        )?;
        let meta_handle = Self::append_block_to_stream(
            state.scratch.as_file_mut(),
            &mut state.offset,
            &metadata.encode(),
            compression_policy,
        )?;
        let index_bytes = Self::serialize_index(block_index_entries)?;
        let index_handle = Self::append_block_to_stream(
            state.scratch.as_file_mut(),
            &mut state.offset,
            &index_bytes,
            compression_policy,
        )?;
        let footer = trie_handle
            .map_or_else(
                || Footer::new(meta_handle, index_handle),
                |handle| Footer::new(meta_handle, index_handle).with_trie(handle),
            )
            .with_block_bloom(block_bloom_handle);
        let footer = footer.encode();
        state
            .scratch
            .as_file_mut()
            .write_all(&footer)
            .map_err(crate::common::MidgeError::Io)?;
        state.offset = state
            .offset
            .saturating_add(u64::try_from(footer.len()).unwrap_or(u64::MAX));
        Ok(())
    }

    fn finish_streaming(
        mut state: StreamingState,
        range_tombstones: &[RangeTombstone],
        compression_policy: &CompressionPolicy,
    ) -> MidgeResult<tempfile::NamedTempFile> {
        Self::flush_streaming_current_block(&mut state, compression_policy)?;

        let index_bytes = state.block_index_entries.iter().fold(
            state
                .block_index_entries
                .len()
                .saturating_mul(std::mem::size_of::<(Vec<u8>, BlockHandle)>()),
            |total, (key, _)| total.saturating_add(key.len()),
        );
        let tombstone_bytes = range_tombstones.iter().fold(
            range_tombstones
                .len()
                .saturating_mul(std::mem::size_of::<RangeTombstone>()),
            |total, tombstone| {
                total
                    .saturating_add(tombstone.start.len())
                    .saturating_add(tombstone.end.len())
            },
        );
        let finalization_bytes = index_bytes
            .saturating_mul(4)
            .saturating_add(tombstone_bytes.saturating_mul(3))
            .saturating_add(16 * 1024);
        let _finalization_reservation = state
            .budget
            .as_ref()
            .map(|budget| budget.reserve(finalization_bytes, "SST finalization buffers"))
            .transpose()?;

        for tombstone in range_tombstones {
            Self::update_key_bounds(
                &mut state.smallest_key,
                &mut state.largest_key,
                &tombstone.start,
            );
            Self::update_key_bounds(
                &mut state.smallest_key,
                &mut state.largest_key,
                &tombstone.end,
            );
        }

        let range_tombstone_handle = Self::append_range_tombstone_block_to_stream(
            &mut state,
            range_tombstones,
            compression_policy,
        )?;
        let index_kind = IndexTuner::decide(&std::mem::take(&mut state.key_profiler).finish());
        let trie_handle =
            Self::append_trie_block_to_stream(&mut state, compression_policy, index_kind)?;
        let metadata = SstMetadata {
            format_version: SST_FORMAT_V4,
            index_kind,
            range_tombstone_handle,
            key_range: state
                .smallest_key
                .clone()
                .zip(state.largest_key.clone())
                .map(|(smallest_key, largest_key)| KeyRangeMetadata {
                    smallest_key,
                    largest_key,
                }),
        };
        let block_index_entries = state.block_index_entries.clone();
        Self::append_metadata_index_and_footer_to_stream(
            &mut state,
            compression_policy,
            &metadata,
            &block_index_entries,
            trie_handle,
        )?;
        state
            .scratch
            .as_file_mut()
            .sync_all()
            .map_err(crate::common::MidgeError::Io)?;
        Ok(state.scratch)
    }
}

impl DynSstWriter for InMemorySstWriter {
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

    fn add(&mut self, key: &[u8], value: &[u8]) -> MidgeResult<()> {
        self.add_with_meta(key, Some(value), 0, 0, None)
    }

    fn add_with_meta(
        &mut self,
        key: &[u8],
        value: Option<&[u8]>,
        seq: u64,
        op_type: u8,
        expiration: Option<u64>,
    ) -> MidgeResult<()> {
        crate::sst::encoding::validate_entry_size(key.len(), value.map_or(0, <[u8]>::len))?;
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
        op_type: u8,
        expiration: Option<u64>,
    ) -> MidgeResult<()> {
        crate::sst::encoding::validate_entry_size(key.len(), value.map_or(0, <[u8]>::len))?;
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
            let bytes = self.finish_bytes()?;
            return crate::sst::fs::persist_sst_bytes_to_path(&bytes, path);
        }

        let InMemorySstWriter {
            entries,
            range_tombstones,
            block_size: _,
            compression_policy,
            streaming,
            budget: _,
            range_tombstone_reservations: _range_tombstone_reservations,
        } = *self;
        debug_assert!(entries.is_empty());
        let scratch = Self::finish_streaming(
            streaming.expect("streaming writer checked above"),
            &range_tombstones,
            &compression_policy,
        )?;
        let mut source = scratch.reopen().map_err(crate::common::MidgeError::Io)?;
        crate::sst::fs::persist_sst_stream_to_path(&mut source, path)
    }

    fn finish_bytes(self: Box<Self>) -> MidgeResult<Vec<u8>> {
        let InMemorySstWriter {
            entries,
            range_tombstones,
            block_size,
            compression_policy,
            streaming,
            budget,
            range_tombstone_reservations: _range_tombstone_reservations,
        } = *self;

        if let Some(streaming) = streaming {
            let scratch =
                Self::finish_streaming(streaming, &range_tombstones, &compression_policy)?;
            let mut source = scratch.reopen().map_err(crate::common::MidgeError::Io)?;
            let mut bytes = Vec::new();
            source
                .read_to_end(&mut bytes)
                .map_err(crate::common::MidgeError::Io)?;
            return Ok(bytes);
        }

        let writer = Self {
            entries: Vec::new(),
            range_tombstones,
            block_size,
            compression_policy,
            streaming: None,
            budget,
            range_tombstone_reservations: Vec::new(),
        };

        let entries = Self::sort_entries(entries);
        let mut finalized = writer.finalize_data_blocks(entries)?;
        let range_tombstone_handle =
            writer.append_range_tombstone_block(&mut finalized.file_bytes)?;
        let index_kind = IndexTuner::decide(&finalized.key_profile);
        let trie_handle = Self::append_trie_block(
            &mut finalized.file_bytes,
            &writer.compression_policy,
            index_kind,
            &finalized.block_index_entries,
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
    /// Create a new SST writer
    fn create(&self) -> MidgeResult<Box<dyn DynSstWriter>> {
        Ok(Box::new(InMemorySstWriter::new(
            self.compression_policy.clone(),
            self.block_size,
        )))
    }

    fn create_for_compaction(
        &self,
        budget: crate::common::resource_budget::ResourceBudget,
    ) -> MidgeResult<Box<dyn DynSstWriter>> {
        let mut writer = InMemorySstWriter::new_with_budget(
            self.compression_policy.clone(),
            self.block_size,
            Some(budget.clone()),
        );
        writer.streaming = Some(StreamingState::new(Some(budget))?);
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

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn should_reject_unrepresentable_compressed_block_length_before_prefix_encoding() {
        // Arrange
        let too_large = usize::try_from(u64::from(u32::MAX) + 1).unwrap_or(usize::MAX);

        // Act
        let result = InMemorySstWriter::checked_block_payload_len(too_large);

        // Assert
        assert!(matches!(
            result,
            Err(crate::common::MidgeError::ResourceLimit(_))
        ));
    }

    #[test]
    fn should_create_factory_with_mock_fs() {
        // Arrange
        let fs = Arc::new(crate::io::MockFs::new());

        // Act
        let factory = FsSstFactoryIo::new(fs, 4096);

        // Assert
        assert_eq!(factory.block_size, 4096);
    }

    #[test]
    fn should_create_factory_with_real_fs() -> MidgeResult<()> {
        // Arrange
        let temp_dir = tempfile::tempdir()?;
        let fs = Arc::new(crate::io::RealFs::new(temp_dir.path())?);

        // Act
        let factory = FsSstFactoryIo::new(fs, 4096);

        // Assert
        assert_eq!(factory.block_size, 4096);
        Ok(())
    }

    #[test]
    fn should_support_method_chaining() {
        // Arrange
        let fs = Arc::new(crate::io::MockFs::new());

        // Act
        let factory = FsSstFactoryIo::new(fs, 4096).with_block_size(8192);

        // Assert
        assert_eq!(factory.block_size, 8192);
    }

    #[test]
    fn should_roundtrip_stateful_entries_when_sst_contains_range_tombstones() -> MidgeResult<()> {
        // Arrange
        let temp_dir = tempfile::tempdir()?;
        let fs = Arc::new(crate::io::RealFs::new(temp_dir.path())?);
        let factory = FsSstFactoryIo::new(fs, 4096);
        let path = temp_dir.path().join("stateful.sst");

        let mut writer = factory.create()?;
        writer.add_with_meta(b"alpha", Some(b"value-a"), 10, 0, Some(4_000_000_000_000))?;
        writer.add_with_meta(b"alpha", None, 9, 2, None)?;
        writer.add_with_meta(b"beta", Some(b"value-b"), 8, 1, None)?;
        writer.add_range_tombstone(b"cat", b"cow", 7)?;
        crate::sst::fs::finish_writer_to_path(writer, &path)?;

        // Act
        let reader = factory.open(std::path::Path::new("stateful.sst"))?;
        let states = reader.scan_range_state(None, None)?;

        // Assert
        assert_eq!(states.len(), 3);
        match &states[0].1 {
            crate::sst::types::KeyState::Value(value, seq, expiration, op_type) => {
                assert_eq!(states[0].0.as_ref(), b"alpha");
                assert_eq!(value.as_ref(), b"value-a");
                assert_eq!(*seq, 10);
                assert_eq!(*expiration, Some(4_000_000_000_000));
                assert_eq!(*op_type, 0);
            }
            other => panic!("expected value state, got {other:?}"),
        }

        match &states[1].1 {
            crate::sst::types::KeyState::Tombstone(seq) => {
                assert_eq!(states[1].0.as_ref(), b"alpha");
                assert_eq!(*seq, 9);
            }
            other => panic!("expected tombstone state, got {other:?}"),
        }

        assert_eq!(reader.range_tombstones().len(), 1);
        assert_eq!(reader.range_tombstones()[0].start, b"cat".to_vec());
        assert_eq!(reader.range_tombstones()[0].end, b"cow".to_vec());

        Ok(())
    }

    #[test]
    fn should_roundtrip_large_key_when_sst_entry_key_delta_exceeds_inline_limit() -> MidgeResult<()>
    {
        // Arrange
        let temp_dir = tempfile::tempdir()?;
        let fs = Arc::new(crate::io::RealFs::new(temp_dir.path())?);
        let factory = FsSstFactoryIo::new(fs, 4096);
        let path = temp_dir.path().join("large-key.sst");
        let oversized_key = vec![b'k'; 65_536];

        // Act
        let mut writer = factory.create()?;
        writer.add_with_meta(&oversized_key, Some(b"value"), 1, 0, None)?;
        crate::sst::fs::finish_writer_to_path(writer, &path)?;
        let reader = factory.open(std::path::Path::new("large-key.sst"))?;
        let states = reader.scan_range_state(None, None)?;

        // Assert
        assert_eq!(states.len(), 1);
        assert_eq!(states[0].0.as_ref(), oversized_key.as_slice());
        match &states[0].1 {
            crate::sst::types::KeyState::Value(value, sequence, expiration, op_type) => {
                assert_eq!(value.as_ref(), b"value");
                assert_eq!(*sequence, 1);
                assert_eq!(*expiration, None);
                assert_eq!(*op_type, 0);
            }
            other => panic!("expected value state, got {other:?}"),
        }
        Ok(())
    }

    #[test]
    fn should_roundtrip_empty_value_when_sst_entry_is_put() -> MidgeResult<()> {
        // Arrange
        let temp_dir = tempfile::tempdir()?;
        let fs = Arc::new(crate::io::RealFs::new(temp_dir.path())?);
        let factory = FsSstFactoryIo::new(fs, 4096);
        let path = temp_dir.path().join("empty-value.sst");

        // Act
        let mut writer = factory.create()?;
        writer.add_with_meta(b"empty", Some(b""), 1, 0, None)?;
        crate::sst::fs::finish_writer_to_path(writer, &path)?;
        let reader = factory.open(std::path::Path::new("empty-value.sst"))?;
        let states = reader.scan_range_state(None, None)?;

        // Assert
        assert_eq!(states.len(), 1);
        match &states[0].1 {
            crate::sst::types::KeyState::Value(value, sequence, expiration, op_type) => {
                assert_eq!(states[0].0.as_ref(), b"empty");
                assert_eq!(value.as_ref(), b"");
                assert_eq!(*sequence, 1);
                assert_eq!(*expiration, None);
                assert_eq!(*op_type, 0);
            }
            other => panic!("expected empty value state, got {other:?}"),
        }
        Ok(())
    }

    #[test]
    fn should_roundtrip_multiple_blocks_when_sorted_compaction_spills() -> MidgeResult<()> {
        // Arrange
        let temp_dir = tempfile::tempdir()?;
        let fs = Arc::new(crate::io::RealFs::new(temp_dir.path())?);
        let factory = FsSstFactoryIo::new(fs, 4096);
        let path = temp_dir.path().join("streamed.sst");
        let mut writer = factory.create()?;

        // Act
        for index in 0..2_000 {
            let key = format!("key-{index:06}");
            writer.add_sorted_with_meta(key.as_bytes(), Some(b"value"), index, 0, None)?;
        }
        crate::sst::fs::finish_writer_to_path(writer, &path)?;
        let reader = factory.open(std::path::Path::new("streamed.sst"))?;
        let states = reader.scan_range_state(None, None)?;

        // Assert
        assert_eq!(states.len(), 2_000);
        assert_eq!(
            states.first().map(|(key, _)| key.as_ref()),
            Some(&b"key-000000"[..])
        );
        assert_eq!(
            states.last().map(|(key, _)| key.as_ref()),
            Some(&b"key-001999"[..])
        );
        Ok(())
    }

    #[test]
    fn should_reject_out_of_order_entries_on_sorted_writer_path() -> MidgeResult<()> {
        // Arrange
        let fs = Arc::new(crate::io::MockFs::new());
        let factory = FsSstFactoryIo::new(fs, 4096);
        let mut writer = factory.create()?;
        writer.add_sorted_with_meta(b"b", Some(b"value"), 2, 0, None)?;

        // Act
        let result = writer.add_sorted_with_meta(b"a", Some(b"value"), 1, 0, None);

        // Assert
        assert!(matches!(
            result,
            Err(crate::common::MidgeError::InvalidArgument(_))
        ));
        Ok(())
    }
}
