use super::{BlockHandle, SstFileIo, SstVerificationStats};
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

        for (_, handle) in &index {
            if deadline.is_some_and(crate::common::OperationDeadline::is_expired) {
                return Err(MidgeError::Timeout(
                    "SST verification deadline expired".into(),
                ));
            }
            Self::validate_block_handle(*handle, self.block_region_end, "data")?;
            let block = self.read_block(handle)?;
            let _ = self.scan_block_entries_from_bytes(&block)?;
        }

        Ok(SstVerificationStats {
            size_bytes: file_size,
            data_blocks: u64::try_from(index.len()).unwrap_or(u64::MAX),
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

        let footer_size = u64::try_from(SST_FOOTER_SIZE).expect("V4 footer size fits u64");
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
        if file_size < footer_size {
            if file_size >= 8
                && file
                    .read_at(file_size - 8, 8)
                    .map_err(FsError::into_midge)?
                    .as_ref()
                    .eq(&SST_FOOTER_MAGIC.to_le_bytes())
            {
                return Err(MidgeError::CompatibilityError(
                    "legacy SST V1-V3 is unsupported; this build requires V4".into(),
                ));
            }
            return Err(MidgeError::Corruption(format!(
                "SST is too short for the {SST_FOOTER_SIZE}-byte V4 footer"
            )));
        }
        let footer_offset = file_size - footer_size;
        let footer_data = file
            .read_at(footer_offset, footer_size)
            .map_err(FsError::into_midge)?;
        let footer = match Footer::decode(&footer_data) {
            Ok(footer) => footer,
            Err(error) => {
                let trailing_magic = file
                    .read_at(file_size.saturating_sub(8), 8)
                    .map_err(FsError::into_midge)?;
                if trailing_magic.as_ref() == SST_FOOTER_MAGIC.to_le_bytes() {
                    return Err(MidgeError::CompatibilityError(
                        "legacy SST V1-V3 is unsupported; this build requires V4".into(),
                    ));
                }
                return Err(error);
            }
        };
        self.block_region_end = footer_offset;
        Self::validate_block_handle(footer.meta_index_handle, footer_offset, "metadata")?;
        Self::validate_block_handle(footer.index_handle, footer_offset, "index")?;
        if let Some(handle) = footer.trie_handle {
            Self::validate_block_handle(handle, footer_offset, "trie")?;
        }
        if let Some(handle) = footer.block_bloom_handle {
            Self::validate_block_handle(handle, footer_offset, "block bloom")?;
        }
        drop(file);
        self.footer = Some(footer);
        self.load_sst_metadata()?;
        self.load_block_bloom()?;
        let index_handle = self
            .footer
            .as_ref()
            .ok_or_else(|| MidgeError::Corruption("SST footer is missing".into()))?
            .index_handle;
        let index_data = self.read_metadata_block(&index_handle, "SST index")?;
        let index = self.decode_index_entries(&index_data)?;
        self.validate_nonoverlapping_references(&index)?;
        self.index_entries.store(Some(Arc::new(index)));

        Ok(())
    }

    fn load_sst_metadata(&mut self) -> MidgeResult<()> {
        let Some(footer) = self.footer.clone() else {
            return Ok(());
        };

        let metadata_bytes = self.read_metadata_block(&footer.meta_index_handle, "SST metadata")?;
        if metadata_bytes.is_empty() {
            return Err(MidgeError::Corruption(
                "SST V4 metadata block is empty".into(),
            ));
        }

        let metadata = SstMetadata::decode(&metadata_bytes)?;
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
        self.range_tombstones = match metadata.range_tombstone_handle {
            Some(handle) => {
                Self::validate_block_handle(handle, self.block_region_end, "range tombstone")?;
                let tombstone_bytes = self.read_metadata_block(&handle, "SST range tombstones")?;
                decode_range_tombstones(&tombstone_bytes)?
            }
            _ => Vec::new(),
        };
        self.trie_reader = match (self.index_kind, footer.trie_handle) {
            (IndexKind::Trie, Some(handle)) => {
                let trie_bytes = self.read_metadata_block(&handle, "SST trie")?;
                Some(Arc::new(TrieReader::new(&trie_bytes)?))
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

    pub(super) fn validate_nonoverlapping_references(
        &self,
        index: &[(Vec<u8>, BlockHandle)],
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
        handles.extend(index.iter().map(|(_, handle)| ("data", *handle)));
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
                "SST block references do not exactly reach the V4 footer".into(),
            ));
        }
        Ok(())
    }

    pub(super) fn read_block(&self, handle: &BlockHandle) -> MidgeResult<bytes::Bytes> {
        Self::validate_block_handle(*handle, self.block_region_end, "referenced")?;
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
    ) -> MidgeResult<bytes::Bytes> {
        let Some(budget) = self.metadata_budget.clone() else {
            return self.read_block(handle);
        };

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
        let (decoded, retained_reservation) = self.read_framed_block(
            file.as_ref(),
            handle,
            Some((&budget, resource)),
            |decoded_size| {
                budget.reserve(decoded_size.saturating_mul(4).saturating_add(256), resource)
            },
        )?;
        // The decoded bytes are transient, but their decoded reader structure
        // remains live (index, filter, trie, or range tombstones). Keep this
        // conservative reservation with the reader for that parsed structure.
        self.metadata_reservations.push(retained_reservation);
        Ok(decoded)
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

    fn decode_framed_block<R>(
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
                "SST V4 block is too short for its mandatory trailer".into(),
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
