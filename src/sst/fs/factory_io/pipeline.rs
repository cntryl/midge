//! Shared block, index, trie, bloom and footer assembly for [`FsSstWriter`].
//!
//! Both buffered compatibility writes and scratch-backed flush/compaction
//! writes feed this one pipeline. Output destinations only implement byte
//! persistence and resource lifetime in `sink`.

use super::sink::{BlockSink, VecBlockSink};

use super::{CompressionPolicy, FsSstWriter, PendingEntry, StreamingState};
use crate::common::MidgeResult;
use crate::io::Fs;
use crate::sst::bloom::{BlockBloomFilter, BloomWriter};
use crate::sst::index::profiler::KeyStructureProfiler;
use crate::sst::index::tuner::{IndexKind, IndexTuner};
use crate::sst::trie::writer::TrieWriter;
use crate::sst::types::{
    encode_range_tombstones, BlockHandle, Footer, KeyRangeMetadata, SstMetadata, SST_FORMAT_V4,
};
use crate::types::{EntryType, RangeTombstone};
use std::sync::Arc;

/// Mutable state shared by both writer entry paths before blocks are emitted.
pub(super) struct BlockPipeline {
    pub(super) block_index_entries: Vec<(Vec<u8>, BlockHandle)>,
    key_profiler: KeyStructureProfiler,
    pub(super) current_block: Vec<u8>,
    pub(super) current_block_keys: Vec<Vec<u8>>,
    pub(super) current_first_key: Option<Vec<u8>>,
    pub(super) block_bloom: BlockBloomFilter,
    previous_key: Vec<u8>,
    last_key: Option<Vec<u8>>,
    last_sequence: u64,
    pub(super) smallest_key: Option<Vec<u8>>,
    pub(super) largest_key: Option<Vec<u8>>,
}

impl BlockPipeline {
    pub(super) fn new() -> Self {
        Self {
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
        }
    }

    fn append_block<S: BlockSink>(
        sink: &mut S,
        block_bytes: &[u8],
        compression_policy: &CompressionPolicy,
    ) -> MidgeResult<BlockHandle> {
        let compressed = FsSstWriter::encode_readable_block(block_bytes, compression_policy)?;
        let offset = sink.offset()?;
        let (payload_len, size) = FsSstWriter::checked_block_payload_len(compressed.len())?;
        sink.append(&payload_len.to_le_bytes())?;
        sink.append(&compressed)?;
        sink.advance_after_append(size)?;
        Ok(BlockHandle::new(offset, size))
    }

    fn append_range_tombstone_block<S: BlockSink>(
        sink: &mut S,
        range_tombstones: &[RangeTombstone],
        compression_policy: &CompressionPolicy,
    ) -> MidgeResult<Option<BlockHandle>> {
        if range_tombstones.is_empty() {
            return Ok(None);
        }

        let bytes = encode_range_tombstones(range_tombstones);
        Self::append_block(sink, &bytes, compression_policy).map(Some)
    }

    fn append_trie_block<S: BlockSink>(
        sink: &mut S,
        compression_policy: &CompressionPolicy,
        trie_bytes: Option<&Vec<u8>>,
    ) -> MidgeResult<Option<BlockHandle>> {
        trie_bytes
            .map(|bytes| Self::append_block(sink, bytes, compression_policy))
            .transpose()
    }

    fn append_metadata_index_and_footer<S: BlockSink>(
        &mut self,
        sink: &mut S,
        compression_policy: &CompressionPolicy,
        metadata: &SstMetadata,
        trie_handle: Option<BlockHandle>,
    ) -> MidgeResult<()> {
        let block_bloom_handle =
            Self::append_block(sink, &self.block_bloom.serialize(), compression_policy)?;
        let meta_handle = Self::append_block(sink, &metadata.encode(), compression_policy)?;
        let index_bytes = FsSstWriter::serialize_index(&self.block_index_entries)?;
        let index_handle = Self::append_block(sink, &index_bytes, compression_policy)?;
        let footer = trie_handle
            .map_or_else(
                || Footer::new(meta_handle, index_handle),
                |handle| Footer::new(meta_handle, index_handle).with_trie(handle),
            )
            .with_block_bloom(block_bloom_handle)
            .encode();
        sink.append(&footer)?;
        sink.advance_after_append(u64::try_from(footer.len()).map_err(|_| {
            crate::common::MidgeError::ResourceLimit(
                "SST footer length exceeds the supported range".to_string(),
            )
        })?)?;
        Ok(())
    }

    fn flush_current_block<S: BlockSink>(
        &mut self,
        sink: &mut S,
        compression_policy: &CompressionPolicy,
    ) -> MidgeResult<()> {
        if self.current_block.is_empty() {
            return Ok(());
        }

        let reservation = sink.prepare_block(
            self.current_block.len(),
            self.current_first_key.as_deref(),
            self.current_block_keys.len(),
        )?;
        let handle = Self::append_block(sink, &self.current_block, compression_policy)?;
        if let Some(first_key) = self.current_first_key.take() {
            self.block_index_entries.push((first_key, handle));
        }
        let mut bloom = BloomWriter::with_defaults(self.current_block_keys.len().max(1));
        for key in self.current_block_keys.drain(..) {
            bloom.insert(&key);
        }
        self.block_bloom.add_block_bloom(&bloom);
        self.current_block.clear();
        sink.finish_block(reservation);
        Ok(())
    }

    pub(super) fn append_sorted_entry<S: BlockSink>(
        &mut self,
        sink: &mut S,
        entry: PendingEntry,
        block_size: usize,
        compression_policy: &CompressionPolicy,
        entry_reservation: S::EntryReservation,
    ) -> MidgeResult<()> {
        if let Some(last_key) = &self.last_key {
            match entry.key.cmp(last_key) {
                std::cmp::Ordering::Less => {
                    return Err(crate::common::MidgeError::InvalidArgument(
                        "sorted SST writer received keys out of order".to_string(),
                    ));
                }
                std::cmp::Ordering::Equal if entry.sequence > self.last_sequence => {
                    return Err(crate::common::MidgeError::InvalidArgument(
                        "sorted SST writer received sequences out of descending order".to_string(),
                    ));
                }
                _ => {}
            }
        }

        self.key_profiler.add_key(&entry.key);
        FsSstWriter::update_key_bounds(&mut self.smallest_key, &mut self.largest_key, &entry.key);

        let target_block_size = FsSstWriter::clamp_block_size(block_size);
        let mut encoded = FsSstWriter::encode_pending_entry(&self.previous_key, &entry)?;
        if !self.current_block.is_empty()
            && self.current_block.len().saturating_add(encoded.len()) > target_block_size
        {
            self.flush_current_block(sink, compression_policy)?;
            self.previous_key.clear();
            encoded = FsSstWriter::encode_pending_entry(&self.previous_key, &entry)?;
        }

        if self.current_first_key.is_none() {
            self.current_first_key = Some(entry.key.clone());
        }
        self.current_block.extend_from_slice(&encoded);
        self.current_block_keys.push(entry.key.clone());
        self.previous_key.clone_from(&entry.key);
        self.last_sequence = entry.sequence;
        self.last_key = Some(entry.key);
        sink.retain_entry(entry_reservation);
        Ok(())
    }

    pub(super) fn finish<S: BlockSink>(
        &mut self,
        sink: &mut S,
        range_tombstones: &[RangeTombstone],
        compression_policy: &CompressionPolicy,
    ) -> MidgeResult<()> {
        self.flush_current_block(sink, compression_policy)?;

        for tombstone in range_tombstones {
            FsSstWriter::update_key_bounds(
                &mut self.smallest_key,
                &mut self.largest_key,
                &tombstone.start,
            );
            FsSstWriter::update_key_bounds(
                &mut self.smallest_key,
                &mut self.largest_key,
                &tombstone.end,
            );
        }

        let range_tombstone_handle =
            Self::append_range_tombstone_block(sink, range_tombstones, compression_policy)?;
        let chosen = IndexTuner::decide(&std::mem::take(&mut self.key_profiler).finish());
        let trie_bytes = FsSstWriter::build_trie_index(chosen, &self.block_index_entries);
        let index_kind = FsSstWriter::effective_index_kind(chosen, trie_bytes.as_ref());
        let trie_handle = Self::append_trie_block(sink, compression_policy, trie_bytes.as_ref())?;
        let metadata = SstMetadata {
            format_version: SST_FORMAT_V4,
            index_kind,
            range_tombstone_handle,
            key_range: self.smallest_key.clone().zip(self.largest_key.clone()).map(
                |(smallest_key, largest_key)| KeyRangeMetadata {
                    smallest_key,
                    largest_key,
                },
            ),
        };
        self.append_metadata_index_and_footer(sink, compression_policy, &metadata, trie_handle)
    }
}

impl FsSstWriter {
    pub(super) fn new(
        fs: Arc<dyn Fs>,
        compression_policy: CompressionPolicy,
        block_size: usize,
    ) -> Self {
        Self::new_with_budget(fs, compression_policy, block_size, None)
    }

    pub(super) fn new_with_budget(
        fs: Arc<dyn Fs>,
        compression_policy: CompressionPolicy,
        block_size: usize,
        budget: Option<crate::common::resource_budget::ResourceBudget>,
    ) -> Self {
        Self {
            fs,
            entries: Vec::new(),
            range_tombstones: Vec::new(),
            block_size,
            compression_policy,
            streaming: None,
            budget,
            range_tombstone_reservations: Vec::new(),
            preserve_legacy_entries: false,
        }
    }

    pub(super) fn clamp_block_size(block_size: usize) -> usize {
        block_size.clamp(
            4 * 1024,
            crate::sst::compression::MAX_DECOMPRESSED_BLOCK_SIZE,
        )
    }

    pub(super) fn encode_readable_block(
        bytes: &[u8],
        policy: &CompressionPolicy,
    ) -> MidgeResult<bytes::Bytes> {
        use crate::sst::compression::{
            compress_block_with_trailer, CompressionAlgo, MAX_DECOMPRESSED_BLOCK_SIZE,
        };
        // Legacy raw blocks can exceed the compressed decoder's ceiling. Keep
        // rewrites and large metadata blocks raw so every emitted block remains
        // readable without relaxing compressed-input validation.
        let policy = if bytes.len() > MAX_DECOMPRESSED_BLOCK_SIZE {
            &CompressionPolicy::Fixed(CompressionAlgo::None)
        } else {
            policy
        };
        compress_block_with_trailer(bytes, policy)
    }

    pub(super) fn checked_block_payload_len(payload_len: usize) -> MidgeResult<(u32, u64)> {
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

    pub(super) fn serialize_index(
        index_entries: &[(Vec<u8>, BlockHandle)],
    ) -> MidgeResult<Vec<u8>> {
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

    pub(super) fn shared_prefix_len(previous_key: &[u8], key: &[u8]) -> u16 {
        let shared = previous_key
            .iter()
            .zip(key.iter())
            .take_while(|(left, right)| left == right)
            .count();
        u16::try_from(shared.min(u16::MAX as usize)).unwrap_or(u16::MAX)
    }

    pub(super) fn sort_entries(mut entries: Vec<PendingEntry>) -> Vec<PendingEntry> {
        entries.sort_by(|left, right| {
            left.key
                .cmp(&right.key)
                .then_with(|| right.sequence.cmp(&left.sequence))
        });
        entries
    }

    /// Reject entry types SST writers must never emit.
    ///
    /// lsm-spec sst.md §3.2 forbids writers from emitting `EntryType::Merge`.
    pub(super) fn writable_entry_type(entry_type: EntryType) -> MidgeResult<EntryType> {
        match entry_type {
            EntryType::Merge => Err(crate::common::MidgeError::InvalidArgument(format!(
                "SST writers must not emit entry type {entry_type:?}; only Put, Insert, and Delete are writable"
            ))),
            other => Ok(other),
        }
    }

    pub(super) fn encode_pending_entry(
        previous_key: &[u8],
        entry: &PendingEntry,
    ) -> MidgeResult<Vec<u8>> {
        let entry_type = Self::writable_entry_type(entry.op_type)?;
        let shared_len = Self::shared_prefix_len(previous_key, &entry.key);
        let key_delta = &entry.key[shared_len as usize..];
        crate::sst::encoding::encode_v4(
            key_delta,
            shared_len,
            entry.value.as_deref(),
            entry.sequence,
            entry_type,
            entry.expiration,
        )
    }

    pub(super) fn update_key_bounds(
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

    pub(super) fn build_trie_index(
        index_kind: IndexKind,
        block_index_entries: &[(Vec<u8>, BlockHandle)],
    ) -> Option<Vec<u8>> {
        if !matches!(index_kind, IndexKind::Trie) {
            return None;
        }
        let mut trie_writer = TrieWriter::new(true);
        for (block_index, (first_key, _handle)) in block_index_entries.iter().enumerate() {
            let block_id = u32::try_from(block_index).unwrap_or(u32::MAX);
            if let Err(error) = trie_writer.add_block_key(first_key, block_id) {
                tracing::warn!(%error, "trie index cannot represent SST keys; using sparse index");
                return None;
            }
        }
        trie_writer.finish()
    }

    pub(super) fn effective_index_kind(
        chosen: IndexKind,
        trie_bytes: Option<&Vec<u8>>,
    ) -> IndexKind {
        if trie_bytes.is_some() {
            chosen
        } else {
            IndexKind::Sparse
        }
    }

    pub(super) fn finish_buffered(
        entries: Vec<PendingEntry>,
        range_tombstones: &[RangeTombstone],
        block_size: usize,
        compression_policy: &CompressionPolicy,
    ) -> MidgeResult<Vec<u8>> {
        let mut sink = VecBlockSink::new();
        let mut pipeline = BlockPipeline::new();
        for entry in Self::sort_entries(entries) {
            sink.reserve_entry(0)?;
            pipeline.append_sorted_entry(&mut sink, entry, block_size, compression_policy, ())?;
        }
        pipeline.finish(&mut sink, range_tombstones, compression_policy)?;
        sink.finish()
    }

    pub(super) fn finish_streaming(
        mut state: StreamingState,
        range_tombstones: &[RangeTombstone],
        compression_policy: &CompressionPolicy,
    ) -> MidgeResult<super::super::scratch::TrackedScratch> {
        // Release the final block's transient entry reservations before asking
        // the same budget to hold the final index/footer workspace. This is
        // the ordering used by the former streaming path; reserving first
        // would reject a valid marginal-budget writer solely because the two
        // temporary charges overlapped.
        state
            .pipeline
            .flush_current_block(&mut state.sink, compression_policy)?;

        let index_bytes = state.pipeline.block_index_entries.iter().fold(
            state
                .pipeline
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
        let _finalization_reservation = state.sink.reserve_finalization(finalization_bytes)?;

        state
            .pipeline
            .finish(&mut state.sink, range_tombstones, compression_policy)?;
        // The scratch file carries no durability: `finish_to_path` copies it
        // into the staging file, and that copy is what is fsynced and renamed
        // into place. Syncing here would write every output byte to the device
        // twice. A crash simply orphans the scratch, which cleanup handles.
        state.sink.finish()
    }
}
