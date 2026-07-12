use super::{BlockHandle, SstFileIo, SstVerificationStats};
use crate::common::{MidgeError, MidgeResult};
use crate::sst::bloom::BlockBloomFilter;
use crate::sst::index::tuner::IndexKind;
use crate::sst::trie::TrieReader;
use crate::sst::types::{decode_range_tombstones, Footer, SstMetadata, SST_FORMAT_V1};
use std::convert::TryFrom;
use std::sync::Arc;

impl SstFileIo {
    /// Read, checksum, and decode every block referenced by this SST.
    ///
    /// Opening an SST validates only its footer and metadata. Authoritative
    /// verification must also validate the index, optional accelerators, and
    /// every data block so late corruption cannot hide behind intact metadata.
    pub(crate) fn verify_all_blocks(&self) -> MidgeResult<SstVerificationStats> {
        let file_size = self.fs.metadata(&self.path)?.len;
        if !self.uses_block_trailers() {
            return Err(MidgeError::Corruption(format!(
                "SST '{}' does not use checksummed block trailers",
                self.path.0.as_str()
            )));
        }

        let footer = self
            .footer
            .as_ref()
            .ok_or_else(|| MidgeError::Corruption("SST footer is missing".into()))?;
        let index = self.parse_index_entries()?;

        Self::validate_block_handle(footer.meta_index_handle, file_size, "metadata")?;
        Self::validate_block_handle(footer.index_handle, file_size, "index")?;
        let _ = self.read_block(&footer.meta_index_handle)?;
        let _ = self.read_block(&footer.index_handle)?;

        if let Some(handle) = footer.trie_handle {
            Self::validate_block_handle(handle, file_size, "trie")?;
            let trie = self.read_block(&handle)?;
            let _ = TrieReader::new(&trie)?;
        }
        if let Some(handle) = footer.block_bloom_handle {
            Self::validate_block_handle(handle, file_size, "block bloom")?;
            let bloom = self.read_block(&handle)?;
            let _ = BlockBloomFilter::deserialize(&bloom)?;
        }

        for (_, handle) in &index {
            Self::validate_block_handle(*handle, file_size, "data")?;
            let block = self.read_block(handle)?;
            let _ = self.scan_block_entries_from_bytes(&block)?;
        }

        Ok(SstVerificationStats {
            size_bytes: file_size,
            data_blocks: u64::try_from(index.len()).unwrap_or(u64::MAX),
        })
    }

    fn validate_block_handle(handle: BlockHandle, file_size: u64, kind: &str) -> MidgeResult<()> {
        let end = handle.offset.checked_add(handle.size).ok_or_else(|| {
            MidgeError::Corruption(format!("SST {kind} block handle overflows file offsets"))
        })?;
        if handle.size < 4 || end > file_size {
            return Err(MidgeError::Corruption(format!(
                "SST {kind} block [{}, {}) exceeds file length {file_size}",
                handle.offset, end
            )));
        }
        Ok(())
    }

    pub(super) fn load_metadata(&mut self) -> MidgeResult<()> {
        // Open file in read-only mode
        // Get file size
        let metadata = self.fs.metadata(&self.path)?;
        let file_size = metadata.len;

        if file_size < 48 {
            return Err(MidgeError::Corruption("SST file too small".into()));
        }

        // Determine footer size (72 bytes new, fall back to 56 or 48)
        let footer_size = if file_size >= 72 {
            72u64
        } else if file_size >= 56 {
            56u64
        } else {
            48u64
        };

        // Read footer from end of file
        let footer_offset = file_size - footer_size;
        let footer_data = {
            let file = self.fs.open(
                &self.path,
                crate::io::OpenOptions {
                    mode: crate::io::OpenMode::ReadOnly,
                    create: false,
                    create_new: false,
                    truncate: false,
                },
            )?;
            file.read_at(footer_offset, footer_size)?
        };

        self.footer = Some(Footer::decode(&footer_data)?);
        self.load_sst_metadata()?;

        Ok(())
    }

    fn load_sst_metadata(&mut self) -> MidgeResult<()> {
        let Some(footer) = self.footer.clone() else {
            return Ok(());
        };

        if footer.meta_index_handle.size == 0 {
            self.format_version = SST_FORMAT_V1;
            self.index_kind = IndexKind::Sparse;
            self.smallest_key = None;
            self.largest_key = None;
            self.trie_reader = None;
            self.range_tombstones.clear();
            return Ok(());
        }

        let metadata_bytes = self.read_block(&footer.meta_index_handle)?;
        if metadata_bytes.is_empty() {
            self.format_version = SST_FORMAT_V1;
            self.index_kind = IndexKind::Sparse;
            self.smallest_key = None;
            self.largest_key = None;
            self.trie_reader = None;
            self.range_tombstones.clear();
            return Ok(());
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
        self.range_tombstones = match metadata.range_tombstone_handle {
            Some(handle) if handle.size > 0 => {
                let tombstone_bytes = self.read_block(&handle)?;
                decode_range_tombstones(&tombstone_bytes)?
            }
            _ => Vec::new(),
        };
        self.trie_reader = match (self.index_kind, footer.trie_handle) {
            (IndexKind::Trie, Some(handle)) => {
                let trie_bytes = self.read_block(&handle)?;
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

    pub(super) fn read_block(&self, handle: &BlockHandle) -> MidgeResult<bytes::Bytes> {
        let file = self.fs.open(
            &self.path,
            crate::io::OpenOptions {
                mode: crate::io::OpenMode::ReadOnly,
                create: false,
                create_new: false,
                truncate: false,
            },
        )?;

        // Read block with size prefix
        let buffer = file.read_at(handle.offset, handle.size)?;

        // First 4 bytes are length prefix
        if buffer.len() < 4 {
            return Err(MidgeError::Corruption("Block too short".into()));
        }

        let len = u32::from_le_bytes([buffer[0], buffer[1], buffer[2], buffer[3]]) as usize;
        if len + 4 > buffer.len() {
            return Err(MidgeError::Corruption("Block data truncated".into()));
        }

        let raw = &buffer[4..4 + len];
        Self::decompress_raw_block(raw, self.uses_block_trailers())
    }

    /// Decompress a raw block payload, stripping the block trailer if present.
    ///
    /// Blocks with a valid trailer (`[data][algo:u8][crc32c:u32]`) are verified
    /// and decompressed.  Legacy blocks without a trailer (pre-v1.0.0) are
    /// returned as-is for backward compatibility.
    fn uses_block_trailers(&self) -> bool {
        self.footer
            .as_ref()
            .is_some_and(|footer| footer.meta_index_handle.size > 0)
    }

    fn decompress_raw_block(raw: &[u8], strict_trailer: bool) -> MidgeResult<bytes::Bytes> {
        use crate::sst::compression;

        // A block must be at least BLOCK_TRAILER_SIZE bytes to contain a
        // trailer.  Shorter payloads are legacy / uncompressed.
        if raw.len() < compression::BLOCK_TRAILER_SIZE {
            if strict_trailer {
                return Err(MidgeError::Corruption(
                    "current-format SST block too short for trailer".into(),
                ));
            }
            return Ok(bytes::Bytes::copy_from_slice(raw));
        }

        match compression::decompress_block_with_trailer(raw) {
            Ok(decompressed) => Ok(decompressed),
            Err(error) if strict_trailer => Err(error),
            Err(_) => Ok(bytes::Bytes::copy_from_slice(raw)),
        }
    }

    /// Read multiple contiguous blocks in a single IO operation for cold-cache scans.
    ///
    /// This is the core optimization for range scan readahead:
    /// - Reads from `handles[0].offset` to `handles[last].offset + handles[last].size`
    /// - Slices the buffer to extract individual block data
    /// - Preserves existing error handling and alignment rules
    ///
    /// Returns a Vec of decoded block data (Bytes), one per handle.
    pub(super) fn read_blocks_contiguous(
        &self,
        handles: &[BlockHandle],
    ) -> MidgeResult<Vec<bytes::Bytes>> {
        if handles.is_empty() {
            return Ok(Vec::new());
        }

        // Single block: use existing path
        if handles.len() == 1 {
            let block_data = self.read_block(&handles[0])?;
            self.diagnostics.sst_metrics().record_data_block_read();
            return Ok(vec![block_data]);
        }

        // Compute contiguous read range
        let first = &handles[0];
        let last = &handles[handles.len() - 1];
        let read_start = first.offset;
        let read_end = last.offset + last.size;
        let total_len = read_end - read_start;

        // Open file once for the entire window
        let file = self.fs.open(
            &self.path,
            crate::io::OpenOptions {
                mode: crate::io::OpenMode::ReadOnly,
                create: false,
                create_new: false,
                truncate: false,
            },
        )?;

        // Single contiguous read for all blocks in window
        let buffer = file.read_at(read_start, total_len)?;
        let read_metrics = self.diagnostics.sst_metrics();
        for _ in handles {
            read_metrics.record_data_block_read();
        }

        // Extract individual blocks from the buffer
        let mut result = Vec::with_capacity(handles.len());
        let strict_trailer = self.uses_block_trailers();
        for handle in handles {
            // Compute offset within the buffer
            let buf_offset = usize::try_from(handle.offset - read_start).map_err(|_| {
                MidgeError::Corruption("Block offset exceeds addressable memory".into())
            })?;
            let handle_size = usize::try_from(handle.size).map_err(|_| {
                MidgeError::Corruption("Block size exceeds addressable memory".into())
            })?;
            let buf_end = buf_offset + handle_size;

            if buf_end > buffer.len() {
                return Err(MidgeError::Corruption(
                    "Block extends past read buffer".into(),
                ));
            }

            let block_slice = &buffer[buf_offset..buf_end];

            // Parse length prefix (same logic as read_block)
            if block_slice.len() < 4 {
                return Err(MidgeError::Corruption("Block too short".into()));
            }

            let len = u32::from_le_bytes([
                block_slice[0],
                block_slice[1],
                block_slice[2],
                block_slice[3],
            ]) as usize;

            if len + 4 > block_slice.len() {
                return Err(MidgeError::Corruption("Block data truncated".into()));
            }

            let raw = &block_slice[4..4 + len];
            result.push(Self::decompress_raw_block(raw, strict_trailer)?);
        }

        Ok(result)
    }
}
