//! Block, index, trie, bloom and footer assembly for [`FsSstWriter`].
//!
//! The in-memory and streaming pipelines share one on-disk layout; they live
//! together so a change to the layout is made in one place.

#[allow(clippy::wildcard_imports)]
use super::*;

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

    pub(super) fn append_block(
        file_bytes: &mut Vec<u8>,
        block_bytes: &[u8],
        compression_policy: &CompressionPolicy,
    ) -> MidgeResult<BlockHandle> {
        let compressed = Self::encode_readable_block(block_bytes, compression_policy)?;
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

    pub(super) fn append_block_to_stream(
        file: &mut std::fs::File,
        offset: &mut u64,
        block_bytes: &[u8],
        compression_policy: &CompressionPolicy,
    ) -> MidgeResult<BlockHandle> {
        let compressed = Self::encode_readable_block(block_bytes, compression_policy)?;
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

    pub(super) fn flush_current_block(
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

    pub(super) fn flush_streaming_current_block(
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

    pub(super) fn append_sorted_entry(
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

        let target_block_size = Self::clamp_block_size(block_size);
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

    pub(super) fn finalize_data_blocks(
        &self,
        entries: Vec<PendingEntry>,
    ) -> MidgeResult<FinalizedDataBlocks> {
        let target_block_size = Self::clamp_block_size(self.block_size);
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

    pub(super) fn append_range_tombstone_block(
        &self,
        file_bytes: &mut Vec<u8>,
    ) -> MidgeResult<Option<BlockHandle>> {
        if self.range_tombstones.is_empty() {
            return Ok(None);
        }

        let block_bytes = encode_range_tombstones(&self.range_tombstones);
        Self::append_block(file_bytes, &block_bytes, &self.compression_policy).map(Some)
    }

    /// Build the trie index bytes in memory, or `None` when the tuner did not
    /// choose a trie or the trie cannot represent these keys (for example a
    /// shared prefix longer than its `u16` prefix length). The caller then
    /// records the sparse index kind: a key the write path accepted must
    /// never make every flush or compaction of it fail.
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

    pub(super) fn append_trie_block(
        file_bytes: &mut Vec<u8>,
        compression_policy: &CompressionPolicy,
        trie_bytes: Option<&Vec<u8>>,
    ) -> MidgeResult<Option<BlockHandle>> {
        trie_bytes
            .map(|bytes| Self::append_block(file_bytes, bytes, compression_policy))
            .transpose()
    }

    pub(super) fn append_metadata_index_and_footer(
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

    pub(super) fn append_range_tombstone_block_to_stream(
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

    pub(super) fn append_trie_block_to_stream(
        state: &mut StreamingState,
        compression_policy: &CompressionPolicy,
        trie_bytes: Option<&Vec<u8>>,
    ) -> MidgeResult<Option<BlockHandle>> {
        trie_bytes
            .map(|bytes| {
                Self::append_block_to_stream(
                    state.scratch.as_file_mut(),
                    &mut state.offset,
                    bytes,
                    compression_policy,
                )
            })
            .transpose()
    }

    pub(super) fn append_metadata_index_and_footer_to_stream(
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

    pub(super) fn finish_streaming(
        mut state: StreamingState,
        range_tombstones: &[RangeTombstone],
        compression_policy: &CompressionPolicy,
    ) -> MidgeResult<super::super::scratch::TrackedScratch> {
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
        let chosen = IndexTuner::decide(&std::mem::take(&mut state.key_profiler).finish());
        let trie_bytes = Self::build_trie_index(chosen, &state.block_index_entries);
        let index_kind = Self::effective_index_kind(chosen, trie_bytes.as_ref());
        let trie_handle =
            Self::append_trie_block_to_stream(&mut state, compression_policy, trie_bytes.as_ref())?;
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
        // The scratch file carries no durability: `finish_to_path` copies it
        // into the staging file, and that copy is what is fsynced and renamed
        // into place. Syncing here would write every output byte to the device
        // twice. A crash simply orphans the scratch, which cleanup handles.
        Ok(state.scratch)
    }
}
