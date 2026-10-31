use super::{BlockHandle, SstFileIo, SstSummaryProgress, SstVerificationStats};
use crate::common::{MidgeError, MidgeResult};
use crate::io::File;
use crate::io::FsError;
use crate::sst::bloom::BlockBloomFilter;
use crate::sst::index::tuner::IndexKind;
use crate::sst::trie::TrieReader;
use crate::sst::types::{
    decode_range_tombstones, Footer, SstMetadata, SST_FOOTER_MAGIC, SST_FOOTER_SIZE,
};
use std::convert::TryFrom;
use std::sync::Arc;

impl SstFileIo {
    /// Read, checksum, and decode every block referenced by this SST.
    ///
    /// Opening an SST validates only its footer and metadata. Authoritative
    /// verification must also validate the index, optional accelerators, and
    /// every data block so late corruption cannot hide behind intact metadata.
    pub(crate) fn verify_all_blocks(&self) -> MidgeResult<SstVerificationStats> {
        self.verify_all_blocks_until(None)
    }

    pub(crate) fn verify_all_blocks_until(
        &self,
        deadline: Option<&crate::common::OperationDeadline>,
    ) -> MidgeResult<SstVerificationStats> {
        let file_size = self
            .fs
            .metadata(&self.path)
            .map_err(FsError::into_midge)?
            .len;
        let footer = self
            .footer
            .as_ref()
            .ok_or_else(|| MidgeError::Corruption("SST footer is missing".into()))?;
        let index = self.parse_index_entries()?;

        Self::validate_block_handle(footer.meta_index_handle, self.block_region_end, "metadata")?;
        Self::validate_block_handle(footer.index_handle, self.block_region_end, "index")?;
        let _ = self.read_block(&footer.meta_index_handle)?;
        let _ = self.read_block(&footer.index_handle)?;

        if let Some(handle) = footer.trie_handle {
            Self::validate_block_handle(handle, self.block_region_end, "trie")?;
            let trie = self.read_block(&handle)?;
            let _ = TrieReader::new(&trie)?;
        }
        if let Some(handle) = footer.block_bloom_handle {
            Self::validate_block_handle(handle, self.block_region_end, "block bloom")?;
            let bloom = self.read_block(&handle)?;
            let _ = BlockBloomFilter::deserialize(&bloom)?;
        }

        let mut summary = SstSummaryProgress::default();
        for tombstone in &self.range_tombstones {
            if deadline.is_some_and(crate::common::OperationDeadline::is_expired) {
                return Err(MidgeError::Timeout(
                    "SST verification deadline expired".into(),
                ));
            }
            summary.observe(file_size, &tombstone.start, tombstone.seq, None)?;
            summary.observe(file_size, &tombstone.end, tombstone.seq, None)?;
        }
        for (_, handle) in index.iter() {
            if deadline.is_some_and(crate::common::OperationDeadline::is_expired) {
                return Err(MidgeError::Timeout(
                    "SST verification deadline expired".into(),
                ));
            }
            Self::validate_block_handle(handle, self.block_region_end, "data")?;
            let block = self.read_block(&handle)?;
            let decoded = self.decode_scan_block(&block)?;
            for index in 0..decoded.len() {
                if deadline.is_some_and(crate::common::OperationDeadline::is_expired) {
                    return Err(MidgeError::Timeout(
                        "SST verification deadline expired".into(),
                    ));
                }
                let entry = decoded.entry(index);
                summary.observe(file_size, &entry.key, entry.sequence, None)?;
                summary.observe_expiration(entry.expiration);
            }
        }

        if deadline.is_some_and(crate::common::OperationDeadline::is_expired) {
            return Err(MidgeError::Timeout(
                "SST verification deadline expired".into(),
            ));
        }
        let summary = summary
            .summary
            .ok_or_else(|| MidgeError::Corruption("SST contains no publishable entries".into()))?;
        if self
            .smallest_key
            .as_ref()
            .is_some_and(|key| summary.smallest_key.as_slice() < key.as_slice())
            || self
                .largest_key
                .as_ref()
                .is_some_and(|key| summary.largest_key.as_slice() > key.as_slice())
        {
            return Err(MidgeError::Corruption(
                "SST persisted key range hides decoded entries".into(),
            ));
        }

        Ok(SstVerificationStats {
            size_bytes: file_size,
            data_blocks: u64::try_from(index.len()).unwrap_or(u64::MAX),
            summary,
        })
    }

    pub(super) fn validate_block_handle(
        handle: BlockHandle,
        block_region_end: u64,
        kind: &str,
    ) -> MidgeResult<()> {
        let end = handle.offset.checked_add(handle.size).ok_or_else(|| {
            MidgeError::Corruption(format!("SST {kind} block handle overflows file offsets"))
        })?;
        let minimum_size = 4_u64
            .saturating_add(u64::try_from(crate::codec::BLOCK_TRAILER_SIZE).unwrap_or(u64::MAX));
        if handle.size < minimum_size || end > block_region_end {
            return Err(MidgeError::Corruption(format!(
                "SST {kind} block [{}, {}) exceeds block region ending at {block_region_end}",
                handle.offset, end
            )));
        }
        Ok(())
    }

    pub(super) fn load_metadata(&mut self) -> MidgeResult<()> {
        // Open file in read-only mode
        // Get file size
        let metadata = self.fs.metadata(&self.path).map_err(FsError::into_midge)?;
        let file_size = metadata.len;

        let footer_size = u64::try_from(SST_FOOTER_SIZE).expect("V5 footer size fits u64");
        let read_only = crate::io::OpenOptions {
            mode: crate::io::OpenMode::ReadOnly,
            create: false,
            create_new: false,
            truncate: false,
        };
        // One owned handle serves this reader for its lifetime (#919); a
        // backend without owned handles falls back to opening per read.
        self.handle =
            match crate::io::open_persistent_handle(self.fs.as_ref(), &self.path, read_only) {
                Ok(handle) => Some(handle),
                Err(FsError::Unsupported(_)) => None,
                Err(error) => return Err(error.into_midge()),
            };
        let footer = {
            let transient;
            let file: &dyn File = if let Some(handle) = &self.handle {
                handle.as_ref()
            } else {
                transient = self
                    .fs
                    .open(&self.path, read_only)
                    .map_err(FsError::into_midge)?;
                transient.as_ref()
            };
            Self::read_footer(file, file_size, footer_size)?
        };
        let footer_offset = file_size - footer_size;
        self.block_region_end = footer_offset;
        Self::validate_block_handle(footer.meta_index_handle, footer_offset, "metadata")?;
        Self::validate_block_handle(footer.index_handle, footer_offset, "index")?;
        if let Some(handle) = footer.trie_handle {
            Self::validate_block_handle(handle, footer_offset, "trie")?;
        }
        if let Some(handle) = footer.block_bloom_handle {
            Self::validate_block_handle(handle, footer_offset, "block bloom")?;
        }
        self.footer = Some(footer);
        self.load_sst_metadata()?;
        self.load_block_bloom()?;
        let index_handle = self
            .footer
            .as_ref()
            .ok_or_else(|| MidgeError::Corruption("SST footer is missing".into()))?
            .index_handle;
        let index_data = self.read_metadata_block(&index_handle, "SST index")?;
        let index = self.decode_index_entries(&index_data.bytes)?;
        self.validate_nonoverlapping_references(&index)?;
        self.retain_metadata(index_data, index.retained_bytes(), "SST index")?;
        self.index_entries.store(Some(Arc::new(index)));

        Ok(())
    }

    /// Decode the V5 footer at the end of `file`, naming legacy formats.
    fn read_footer(file: &dyn File, file_size: u64, footer_size: u64) -> MidgeResult<Footer> {
        if file_size < footer_size {
            if file_size >= 8
                && file
                    .read_at(file_size - 8, 8)
                    .map_err(FsError::into_midge)?
                    .as_ref()
                    .eq(&SST_FOOTER_MAGIC.to_le_bytes())
            {
                return Err(MidgeError::CompatibilityError(
                    "legacy SST V1-V4 is unsupported; this build requires V5".into(),
                ));
            }
            return Err(MidgeError::Corruption(format!(
                "SST is too short for the {SST_FOOTER_SIZE}-byte V5 footer"
            )));
        }
        let footer_data = file
            .read_at(file_size - footer_size, footer_size)
            .map_err(FsError::into_midge)?;
        match Footer::decode(&footer_data) {
            Ok(footer) => Ok(footer),
            Err(error) => {
                let trailing_magic = file
                    .read_at(file_size.saturating_sub(8), 8)
                    .map_err(FsError::into_midge)?;
                if trailing_magic.as_ref() == SST_FOOTER_MAGIC.to_le_bytes() {
                    return Err(MidgeError::CompatibilityError(
                        "legacy SST V1-V4 is unsupported; this build requires V5".into(),
                    ));
                }
                Err(error)
            }
        }
    }

    fn load_sst_metadata(&mut self) -> MidgeResult<()> {
        let Some(footer) = self.footer.clone() else {
            return Ok(());
        };

        let metadata_block = self.read_metadata_block(&footer.meta_index_handle, "SST metadata")?;
        if metadata_block.bytes.is_empty() {
            return Err(MidgeError::Corruption(
                "SST V5 metadata block is empty".into(),
            ));
        }

        let metadata = SstMetadata::decode(&metadata_block.bytes)?;
        self.format_version = metadata.format_version;
        self.index_kind = metadata.index_kind;
        self.smallest_key = metadata
            .key_range
            .as_ref()
            .map(|range| range.smallest_key.clone());
        self.largest_key = metadata
            .key_range
            .as_ref()
            .map(|range| range.largest_key.clone());
        self.range_tombstone_handle = metadata.range_tombstone_handle;
        let key_bytes = [&self.smallest_key, &self.largest_key]
            .into_iter()
            .flatten()
            .map(Vec::capacity)
            .sum();
        self.retain_metadata(metadata_block, key_bytes, "SST metadata")?;
        self.load_range_tombstones(metadata.range_tombstone_handle)?;
        self.trie_reader = match (self.index_kind, footer.trie_handle) {
            (IndexKind::Trie, Some(handle)) => {
                let trie_block = self.read_metadata_block(&handle, "SST trie")?;
                let trie = TrieReader::new(&trie_block.bytes)?;
                self.retain_metadata(trie_block, trie.memory_usage(), "SST trie")?;
                Some(Arc::new(trie))
            }
            (IndexKind::Trie, None) => {
                return Err(MidgeError::Corruption(
                    "Trie-selected SST metadata is missing trie footer handle".into(),
                ));
            }
            (IndexKind::Sparse, Some(_handle)) => {
                return Err(MidgeError::Corruption(
                    "Sparse-selected SST metadata should not carry trie footer handle".into(),
                ));
            }
            (IndexKind::Sparse, None) => None,
        };

        Ok(())
    }

    fn load_range_tombstones(&mut self, handle: Option<BlockHandle>) -> MidgeResult<()> {
        self.range_tombstones = Vec::new();
        let block = if let Some(handle) = handle {
            Self::validate_block_handle(handle, self.block_region_end, "range tombstone")?;
            let block = self.read_metadata_block(&handle, "SST range tombstones")?;
            self.range_tombstones = decode_range_tombstones(&block.bytes)?;
            Some(block)
        } else {
            None
        };
        self.range_tombstone_index =
            crate::memtable::range_tombstone_index::FragmentedRangeTombstones::build(
                &self.range_tombstones,
            );
        if let Some(block) = block {
            let retained = self.range_tombstones.iter().fold(
                self.range_tombstones
                    .capacity()
                    .saturating_mul(std::mem::size_of::<crate::types::RangeTombstone>()),
                |total, range| {
                    total
                        .saturating_add(range.start.capacity())
                        .saturating_add(range.end.capacity())
                },
            );
            let retained = retained.saturating_add(self.range_tombstone_index.as_ref().map_or(
                0,
                crate::memtable::range_tombstone_index::FragmentedRangeTombstones::memory_usage,
            ));
            self.retain_metadata(block, retained, "SST range tombstones")?;
        }
        Ok(())
    }

    pub(super) fn validate_nonoverlapping_references(
        &self,
        index: &super::block_index::BlockIndex,
    ) -> MidgeResult<()> {
        let footer = self
            .footer
            .as_ref()
            .ok_or_else(|| MidgeError::Corruption("SST footer is missing".into()))?;
        let mut handles = vec![
            ("metadata", footer.meta_index_handle),
            ("index", footer.index_handle),
        ];
        if let Some(handle) = footer.trie_handle {
            handles.push(("trie", handle));
        }
        if let Some(handle) = footer.block_bloom_handle {
            handles.push(("block bloom", handle));
        }
        if let Some(handle) = self.range_tombstone_handle {
            handles.push(("range tombstone", handle));
        }
        handles.extend(index.iter().map(|(_, handle)| ("data", handle)));
        handles.sort_unstable_by_key(|(_, handle)| handle.offset);

        if handles
            .first()
            .is_some_and(|(_, handle)| handle.offset != 0)
        {
            return Err(MidgeError::Corruption(
                "SST block references leave unreferenced bytes at the start of the file".into(),
            ));
        }

        for pair in handles.windows(2) {
            let (left_kind, left) = pair[0];
            let (right_kind, right) = pair[1];
            let left_end = left.offset.checked_add(left.size).ok_or_else(|| {
                MidgeError::Corruption(format!(
                    "SST {left_kind} block handle overflows file offsets"
                ))
            })?;
            if left_end > right.offset {
                return Err(MidgeError::Corruption(format!(
                    "SST {left_kind} block overlaps {right_kind} block"
                )));
            }
            if left_end < right.offset {
                return Err(MidgeError::Corruption(format!(
                    "SST {left_kind} and {right_kind} blocks leave unreferenced bytes"
                )));
            }
        }
        if handles.last().is_some_and(|(_, handle)| {
            handle.offset.checked_add(handle.size) != Some(self.block_region_end)
        }) {
            return Err(MidgeError::Corruption(
                "SST block references do not exactly reach the V5 footer".into(),
            ));
        }
        Ok(())
    }

    pub(super) fn read_block(&self, handle: &BlockHandle) -> MidgeResult<bytes::Bytes> {
        Self::validate_block_handle(*handle, self.block_region_end, "referenced")?;
        if let Some(file) = &self.handle {
            return self.read_block_from(file.as_ref(), handle);
        }
        let file = self
            .fs
            .open(
                &self.path,
                crate::io::OpenOptions {
                    mode: crate::io::OpenMode::ReadOnly,
                    create: false,
                    create_new: false,
                    truncate: false,
                },
            )
            .map_err(FsError::into_midge)?;

        self.read_block_from(file.as_ref(), handle)
    }

    pub(super) fn read_metadata_block(
        &mut self,
        handle: &BlockHandle,
        resource: &'static str,
    ) -> MidgeResult<MetadataBlock> {
        let Some(budget) = self.metadata_budget.clone() else {
            return Ok(MetadataBlock {
                bytes: self.read_block(handle)?,
                charge: None,
            });
        };

        let (decoded, retained_reservation) = {
            let transient;
            let file: &dyn File = if let Some(owned) = &self.handle {
                owned.as_ref()
            } else {
                transient = self
                    .fs
                    .open(
                        &self.path,
                        crate::io::OpenOptions {
                            mode: crate::io::OpenMode::ReadOnly,
                            create: false,
                            create_new: false,
                            truncate: false,
                        },
                    )
                    .map_err(FsError::into_midge)?;
                transient.as_ref()
            };
            self.read_framed_block(file, handle, Some((&budget, resource)), |decoded_size| {
                budget.reserve(decoded_size.saturating_mul(4).saturating_add(256), resource)
            })?
        };
        // The decoded bytes are transient, but the reader structure parsed
        // from them stays live. This conservative charge bounds the parse;
        // `retain_metadata` then holds exactly what the structure keeps.
        Ok(MetadataBlock {
            bytes: decoded,
            charge: Some(retained_reservation),
        })
    }

    /// Keep `retained` bytes of `block`'s charge with the reader for the
    /// structure parsed from it, releasing the rest of its parse charge.
    pub(super) fn retain_metadata(
        &mut self,
        block: MetadataBlock,
        retained: usize,
        resource: &'static str,
    ) -> MidgeResult<()> {
        let MetadataBlock { bytes, charge } = block;
        drop(bytes);
        if let Some(mut charge) = charge {
            charge.resize(retained, resource)?;
            self.metadata_reservations.push(charge);
        }
        Ok(())
    }

    pub(super) fn read_block_from(
        &self,
        file: &dyn File,
        handle: &BlockHandle,
    ) -> MidgeResult<bytes::Bytes> {
        self.read_framed_block(file, handle, None, |_| Ok(()))
            .map(|(block, ())| block)
    }

    /// Read and decode one handle through the same checked framing as all
    /// other SST readers. The caller reserves its retained decoded form before
    /// decompression so metadata and recovery can keep different lifetimes.
    pub(super) fn read_framed_block<R>(
        &self,
        file: &dyn File,
        handle: &BlockHandle,
        compressed_budget: Option<(
            &crate::common::resource_budget::ResourceBudget,
            &'static str,
        )>,
        reserve_decoded: impl FnOnce(usize) -> MidgeResult<R>,
    ) -> MidgeResult<(bytes::Bytes, R)> {
        Self::validate_block_handle(*handle, self.block_region_end, "referenced")?;
        let compressed_reservation = compressed_budget
            .map(|(budget, resource)| {
                let size = usize::try_from(handle.size).map_err(|_| {
                    MidgeError::ResourceLimit(format!("{resource} exceeds addressable memory"))
                })?;
                budget.reserve(size, resource)
            })
            .transpose()?;
        let buffer = file
            .read_at(handle.offset, handle.size)
            .map_err(FsError::into_midge)?;
        let result = Self::decode_framed_block(&buffer, reserve_decoded);
        drop(compressed_reservation);
        result
    }

    pub(super) fn decode_framed_block<R>(
        buffer: &[u8],
        reserve_decoded: impl FnOnce(usize) -> MidgeResult<R>,
    ) -> MidgeResult<(bytes::Bytes, R)> {
        let raw = Self::split_block_frame(buffer)?;
        let declared = crate::codec::decompressed_size_with_trailer(raw)?;
        let reservation = reserve_decoded(declared)?;
        let decoded = crate::codec::decompress_block_with_trailer(raw)?;
        if decoded.len() > declared {
            return Err(MidgeError::Corruption(
                "decoded SST block exceeded its declared size".into(),
            ));
        }
        Ok((decoded, reservation))
    }

    /// Split a block frame read from its handle: a `u32` length prefix that
    /// covers the rest exactly, then the payload with its trailer. Every
    /// block reader goes through this (#510).
    pub(super) fn split_block_frame(buffer: &[u8]) -> MidgeResult<&[u8]> {
        let prefix: [u8; 4] = buffer
            .get(..4)
            .and_then(|bytes| bytes.try_into().ok())
            .ok_or_else(|| MidgeError::Corruption("Block too short".into()))?;
        let payload_len = u32::from_le_bytes(prefix) as usize;
        if payload_len.checked_add(4) != Some(buffer.len()) {
            return Err(MidgeError::Corruption(
                "SST block length prefix does not exactly match its handle".into(),
            ));
        }
        let payload = &buffer[4..];
        if payload.len() < crate::codec::BLOCK_TRAILER_SIZE {
            return Err(MidgeError::Corruption(
                "SST V5 block is too short for its mandatory trailer".into(),
            ));
        }
        Ok(payload)
    }

    /// The number of leading bytes an entry shares with the previous key,
    /// checked against that key. Every entry decoder goes through this.
    pub(super) fn shared_prefix_len(
        shared_len: usize,
        previous_key_len: usize,
    ) -> MidgeResult<usize> {
        if shared_len > previous_key_len {
            return Err(MidgeError::Corruption(
                "Invalid shared prefix length in SST entry".into(),
            ));
        }
        Ok(shared_len)
    }
}

/// A decoded metadata block and the parse charge covering it.
pub(super) struct MetadataBlock {
    pub(super) bytes: bytes::Bytes,
    charge: Option<crate::common::resource_budget::ResourceReservation>,
}
