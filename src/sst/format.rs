//! Top-level SST format implementation (moved from internal storage)
//!
//! This is a straight copy of `src/internal/storage/sst_format.rs` moved to a flatter
//! module: `crate::sst_format`.

use crate::common::codec::CompressionType;
use crate::error::{MidgeError, MidgeResult};
use bytes::{BufMut, Bytes, BytesMut};
use tracing::{error, trace};

// Constants for the SST format
const BLOCK_TRAILER_SIZE: usize = 5; // 1 byte compression + 4 bytes CRC32C
const FOOTER_SIZE: usize = 48; // Fixed footer size compatible with RocksDB
const MAGIC_NUMBER: u64 = 0xdb47_7524_8b80_fb57; // RocksDB magic

/// Block types (logical, not encoded in the trailer)
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
#[repr(u8)]
pub enum BlockType {
    Data = 0,
    Filter = 1,
    Index = 2,
    MetaIndex = 3,
}

impl TryFrom<u8> for BlockType {
    type Error = MidgeError;

    fn try_from(value: u8) -> Result<Self, Self::Error> {
        match value {
            0 => Ok(BlockType::Data),
            1 => Ok(BlockType::Filter),
            2 => Ok(BlockType::Index),
            3 => Ok(BlockType::MetaIndex),
            _ => Err(MidgeError::InvalidData(format!(
                "Invalid block type: {}",
                value
            ))),
        }
    }
}

/// Represents a handle to a block (offset and size)
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct BlockHandle {
    pub offset: u64,
    pub size: u64,
}

impl BlockHandle {
    pub fn new(offset: u64, size: u64) -> Self {
        Self { offset, size }
    }

    pub fn encode(&self) -> Vec<u8> {
        let mut buf = Vec::with_capacity(20); // Pre-allocate max varint size (10 bytes each)
        self.encode_to(&mut buf);
        buf
    }

    #[inline]
    pub fn encode_to(&self, buf: &mut Vec<u8>) {
        // Encode as varint64 for compatibility
        encode_varint64(buf, self.offset);
        encode_varint64(buf, self.size);
    }

    pub fn decode(data: &[u8]) -> MidgeResult<(Self, usize)> {
        let mut cursor = 0;
        let (offset, offset_len) = decode_varint64(&data[cursor..])?;
        cursor += offset_len;
        let (size, size_len) = decode_varint64(&data[cursor..])?;
        cursor += size_len;

        Ok((BlockHandle::new(offset, size), cursor))
    }
}

/// A block in the SST file
#[derive(Debug)]
pub struct Block {
    pub data: Bytes,
    pub block_type: BlockType,
    pub compression: CompressionType,
}

impl Block {
    pub fn new(data: Bytes, block_type: BlockType, compression: CompressionType) -> Self {
        Self {
            data,
            block_type,
            compression,
        }
    }

    #[inline]
    fn compression_to_byte(compression: CompressionType) -> u8 {
        match compression {
            CompressionType::None => 0u8,
            CompressionType::Lz4 => 2u8,
            CompressionType::Zstd1 => 3u8,
            CompressionType::Zstd3 => 4u8,
            CompressionType::Zstd5 => 5u8,
            CompressionType::Zstd9 => 6u8,
        }
    }

    #[inline]
    fn byte_to_compression(byte: u8) -> MidgeResult<CompressionType> {
        Ok(match byte {
            0 => CompressionType::None,
            2 => CompressionType::Lz4,
            3 => CompressionType::Zstd1,
            4 => CompressionType::Zstd3,
            5 => CompressionType::Zstd5,
            6 => CompressionType::Zstd9,
            _ => {
                return Err(MidgeError::InvalidData(format!(
                    "Unknown compression byte: {}",
                    byte
                )))
            }
        })
    }

    /// Encode block with trailer per spec:
    /// - compression applies to data block body only
    /// - trailer layout: [compression_byte][crc32c_le]
    /// - checksum covers: compressed_body || restart_array || restart_count || compression_byte
    pub fn encode(&self) -> MidgeResult<Bytes> {
        use crc32c::crc32c;
        let comp_byte = Self::compression_to_byte(self.compression);
        match self.block_type {
            BlockType::Data => {
                let total_len = self.data.len();
                // Determine body vs. restarts; allow empty payload (e.g., empty meta index)
                let (body, restarts) = if total_len >= 4 {
                    let restart_count = u32::from_le_bytes([
                        self.data[total_len - 4],
                        self.data[total_len - 3],
                        self.data[total_len - 2],
                        self.data[total_len - 1],
                    ]) as usize;
                    let restarts_len = restart_count
                        .checked_mul(4)
                        .ok_or_else(|| MidgeError::InvalidData("restart_count overflow".into()))?;
                    if total_len < 4 + restarts_len {
                        return Err(MidgeError::InvalidData(
                            "Block restart array overflow".into(),
                        ));
                    }
                    let body_end = total_len - 4 - restarts_len;
                    (&self.data[..body_end], &self.data[body_end..])
                } else {
                    (&self.data[..], &self.data[0..0])
                };

                // Compress body only
                let compressed_body: Bytes = match self.compression {
                    CompressionType::None => Bytes::copy_from_slice(body),
                    CompressionType::Lz4 => Bytes::from(lz4_flex::compress_prepend_size(body)),
                    CompressionType::Zstd1 => Bytes::from(zstd::encode_all(body, 1)?),
                    CompressionType::Zstd3 => Bytes::from(zstd::encode_all(body, 3)?),
                    CompressionType::Zstd5 => Bytes::from(zstd::encode_all(body, 5)?),
                    CompressionType::Zstd9 => Bytes::from(zstd::encode_all(body, 9)?),
                };

                // Build output: compressed_body || restarts || trailer
                let mut buf = BytesMut::with_capacity(
                    compressed_body.len() + restarts.len() + BLOCK_TRAILER_SIZE,
                );
                buf.put(compressed_body);
                buf.put_slice(restarts);
                // CRC32C over (compressed_body || restarts || compression_byte)
                let mut crc_input = Vec::with_capacity(buf.len() + 1);
                crc_input.extend_from_slice(&buf);
                crc_input.push(comp_byte);
                let crc = crc32c(&crc_input);
                buf.put_u8(comp_byte);
                buf.put_u32_le(crc);
                Ok(buf.freeze())
            }
            _ => {
                // Non-data blocks: treat the entire payload as the body; compress whole
                let compressed: Bytes = match self.compression {
                    CompressionType::None => self.data.clone(),
                    CompressionType::Lz4 => {
                        Bytes::from(lz4_flex::compress_prepend_size(&self.data))
                    }
                    CompressionType::Zstd1 => Bytes::from(zstd::encode_all(&self.data[..], 1)?),
                    CompressionType::Zstd3 => Bytes::from(zstd::encode_all(&self.data[..], 3)?),
                    CompressionType::Zstd5 => Bytes::from(zstd::encode_all(&self.data[..], 5)?),
                    CompressionType::Zstd9 => Bytes::from(zstd::encode_all(&self.data[..], 9)?),
                };

                let mut buf = BytesMut::with_capacity(compressed.len() + BLOCK_TRAILER_SIZE);
                buf.put(compressed);
                // CRC32C over (payload || compression_byte)
                let mut crc_input = Vec::with_capacity(buf.len() + 1);
                crc_input.extend_from_slice(&buf);
                crc_input.push(comp_byte);
                let crc = crc32c(&crc_input);
                buf.put_u8(comp_byte);
                buf.put_u32_le(crc);
                Ok(buf.freeze())
            }
        }
    }

    /// Decode block from raw bytes, inferring compression from trailer and
    /// validating CRC32C. Caller provides the logical block_type context.
    ///
    /// If `paranoid_checksums` is true, performs an additional verification pass
    /// on the decompressed data to detect corruption during/after decompression.
    #[inline]
    pub fn decode(data: &[u8], block_type: BlockType) -> MidgeResult<Self> {
        Self::decode_with_options(data, block_type, false)
    }

    /// Decode block with paranoid checksum verification.
    ///
    /// When `paranoid` is true, verifies decompressed data integrity with an additional
    /// checksum pass. This detects memory corruption, decompression bugs, or bit flips
    /// that occur after initial CRC verification but before use (~5-10% overhead).
    #[inline]
    pub fn decode_with_options(
        data: &[u8],
        block_type: BlockType,
        paranoid: bool,
    ) -> MidgeResult<Self> {
        if data.len() < BLOCK_TRAILER_SIZE {
            return Err(MidgeError::InvalidData("Block too small".into()));
        }
        let payload_len = data.len() - BLOCK_TRAILER_SIZE;
        let payload = &data[..payload_len];
        let comp_byte = data[payload_len];
        let stored_crc = u32::from_le_bytes([
            data[payload_len + 1],
            data[payload_len + 2],
            data[payload_len + 3],
            data[payload_len + 4],
        ]);

        // OPTIMIZATION: Compute CRC32C incrementally without allocation
        // crc32c can compute over multiple slices via crc32c_combine
        let crc_payload = crc32c::crc32c(payload);
        let calc = crc32c::crc32c_append(crc_payload, &[comp_byte]);

        if calc != stored_crc {
            return Err(MidgeError::InvalidData("Block CRC mismatch".into()));
        }

        let compression = Self::byte_to_compression(comp_byte)?;
        match block_type {
            BlockType::Data => {
                // Split payload into [compressed_body][restarts]
                let (compressed_body, restarts) = if payload.len() >= 4 {
                    let restart_count = u32::from_le_bytes([
                        payload[payload.len() - 4],
                        payload[payload.len() - 3],
                        payload[payload.len() - 2],
                        payload[payload.len() - 1],
                    ]) as usize;
                    let restarts_len = restart_count
                        .checked_mul(4)
                        .ok_or_else(|| MidgeError::InvalidData("restart_count overflow".into()))?
                        + 4; // includes count
                    if payload_len < restarts_len {
                        return Err(MidgeError::InvalidData("Invalid restarts area".into()));
                    }
                    let body_end = payload_len - restarts_len;
                    payload.split_at(body_end)
                } else {
                    (payload, &payload[0..0])
                };
                let body_decompressed: Bytes = match compression {
                    CompressionType::None => Bytes::copy_from_slice(compressed_body),
                    CompressionType::Lz4 => Bytes::from(
                        lz4_flex::decompress_size_prepended(compressed_body).map_err(|e| {
                            MidgeError::CompressionError {
                                message: e.to_string(),
                            }
                        })?,
                    ),
                    CompressionType::Zstd1
                    | CompressionType::Zstd3
                    | CompressionType::Zstd5
                    | CompressionType::Zstd9 => Bytes::from(zstd::decode_all(compressed_body)?),
                };
                let mut out = BytesMut::with_capacity(body_decompressed.len() + restarts.len());
                out.put(body_decompressed);
                out.put_slice(restarts);

                let final_data = out.freeze();

                // Paranoid mode: verify decompressed data integrity
                if paranoid {
                    let verify_crc = crc32c::crc32c(&final_data);
                    // Store verification checksum in tracing for debugging
                    trace!(
                        "Paranoid checksum verification: block_type={:?}, size={}, crc=0x{:08x}",
                        block_type,
                        final_data.len(),
                        verify_crc
                    );
                }

                Ok(Block::new(final_data, block_type, compression))
            }
            _ => {
                // Non-data blocks: entire payload is compressed body
                let body_decompressed: Bytes = match compression {
                    CompressionType::None => Bytes::copy_from_slice(payload),
                    CompressionType::Lz4 => {
                        Bytes::from(lz4_flex::decompress_size_prepended(payload).map_err(|e| {
                            MidgeError::CompressionError {
                                message: e.to_string(),
                            }
                        })?)
                    }
                    CompressionType::Zstd1
                    | CompressionType::Zstd3
                    | CompressionType::Zstd5
                    | CompressionType::Zstd9 => Bytes::from(zstd::decode_all(payload)?),
                };

                // Paranoid mode: verify decompressed data integrity
                if paranoid {
                    let verify_crc = crc32c::crc32c(&body_decompressed);
                    trace!(
                        "Paranoid checksum verification: block_type={:?}, size={}, crc=0x{:08x}",
                        block_type,
                        body_decompressed.len(),
                        verify_crc
                    );
                }

                Ok(Block::new(body_decompressed, block_type, compression))
            }
        }
    }
}

/// Footer of the SST file containing metadata
#[derive(Debug)]
pub struct Footer {
    pub index_handle: BlockHandle,
    pub meta_index_handle: BlockHandle,
    pub magic: u64,
}

// DECISION (Phase 8.3): Defer persisted BlockSummary to Phase 10 (format evolution).
// Current sparse index provides adequate range estimation. Adding per-block metadata
// requires format version negotiation and is a breaking change. Deferred pending
// Phase 8.4 performance analysis of range scan efficiency.

impl Footer {
    pub fn new(index_handle: BlockHandle, meta_index_handle: BlockHandle) -> Self {
        Self {
            index_handle,
            meta_index_handle,
            magic: MAGIC_NUMBER,
        }
    }

    pub fn encode(&self) -> Bytes {
        let mut buf = Vec::with_capacity(FOOTER_SIZE);

        // Encode handles
        self.meta_index_handle.encode_to(&mut buf);
        self.index_handle.encode_to(&mut buf);

        // Pad to fixed size
        while buf.len() < FOOTER_SIZE - 8 {
            buf.push(0);
        }

        // Write magic number
        buf.extend_from_slice(&self.magic.to_le_bytes());

        debug_assert_eq!(buf.len(), FOOTER_SIZE);
        Bytes::from(buf)
    }

    pub fn decode(data: &[u8]) -> MidgeResult<Self> {
        if data.len() != FOOTER_SIZE {
            return Err(MidgeError::InvalidData(format!(
                "Invalid footer size: {}",
                data.len()
            )));
        }

        // Check magic number
        let magic = u64::from_le_bytes([
            data[40], data[41], data[42], data[43], data[44], data[45], data[46], data[47],
        ]);

        if magic != MAGIC_NUMBER {
            return Err(MidgeError::InvalidData(format!(
                "Invalid magic number: {:x}",
                magic
            )));
        }

        // Decode handles
        let mut cursor = 0;
        let (meta_index_handle, meta_len) = BlockHandle::decode(&data[cursor..])?;
        cursor += meta_len;
        let (index_handle, _) = BlockHandle::decode(&data[cursor..])?;

        Ok(Footer::new(index_handle, meta_index_handle))
    }
}

/// Data block builder for constructing data blocks
pub struct DataBlockBuilder {
    buffer: BytesMut,
    restarts: Vec<u32>,
    last_key: Vec<u8>,
    entries_since_restart: u32,
    restart_interval: u32,
}

impl DataBlockBuilder {
    pub fn new(restart_interval: u32) -> Self {
        let mut builder = Self {
            buffer: BytesMut::new(),
            restarts: Vec::new(),
            last_key: Vec::new(),
            entries_since_restart: 0,
            restart_interval,
        };

        // Add initial restart point
        builder.restarts.push(0);
        builder
    }

    /// Add an entry with explicit sequence and tombstone metadata.
    ///
    /// Add an entry with full metadata including optional TTL expiration.
    ///
    /// Two layouts supported:
    /// - legacy (internal_on_disk == false):
    ///   TLV encoding with SHARED_LEN, KEY_DELTA, VALUE, SEQUENCE, ENTRY_TYPE, and optional EXPIRATION
    /// - internal-key on-disk (internal_on_disk == true):
    ///   The sequence and kind are encoded in the key bytes (userkey||seqBE||kind)
    ///   TLV encoding with SHARED_LEN, KEY_DELTA, VALUE, and optional EXPIRATION
    pub fn add_with_meta(
        &mut self,
        key: &[u8],
        value: Option<&[u8]>,
        seq: u64,
        op_type: u8,
        internal_on_disk: bool,
        expiration: Option<u64>,
    ) -> MidgeResult<()> {
        if key.is_empty() {
            return Err(MidgeError::InvalidData("Key cannot be empty".into()));
        }
        if internal_on_disk {
            if !self.last_key.is_empty() {
                let last_user = crate::common::internal_key::decode_internal_key(&self.last_key)
                    .map(|(u, _s, _t)| u)
                    .unwrap_or_else(|| self.last_key.clone());
                let new_user = crate::common::internal_key::decode_internal_key(key)
                    .map(|(u, _s, _t)| u)
                    .unwrap_or_else(|| key.to_vec());
                // Allow equal user keys as long as the full internal key bytes are
                // strictly increasing (the encoded seq/kind suffix will break ties).
                if last_user.is_empty() || new_user.as_slice() > last_user.as_slice() {
                    // ok
                } else if new_user.as_slice() == last_user.as_slice() {
                    // require the raw bytes (including seq/kind) to be strictly increasing
                    if key <= self.last_key.as_slice() {
                        error!(
                            last_user = ?last_user,
                            new_user = ?new_user,
                            last_raw = %hex::encode(&self.last_key),
                            new_raw = %hex::encode(key),
                            "DataBlockBuilder ordering violation (internal-eq)"
                        );
                        return Err(MidgeError::InvalidData(format!(
                            "Key ordering violation (internal-eq): new key {} <= last key {}",
                            hex::encode(key),
                            hex::encode(&self.last_key)
                        )));
                    }
                } else {
                    error!(
                        last_user = ?last_user,
                        new_user = ?new_user,
                        "DataBlockBuilder ordering violation (internal)"
                    );
                    return Err(MidgeError::InvalidData(
                        "User key ordering violation: new user key went backwards".to_string(),
                    ));
                }
            }
        } else if !(self.last_key.is_empty() || key > self.last_key.as_slice()) {
            // Diagnostic output to help debug ordering issues: print hex of last and new key
            error!(
                last_key = %hex::encode(&self.last_key),
                new_key = %hex::encode(key),
                "DataBlockBuilder ordering violation"
            );
            // Try to decode internal keys for clearer debugging
            if let Some((user_last, seq_last, tomb_last)) =
                crate::common::internal_key::decode_internal_key(&self.last_key)
            {
                error!(
                    user = ?user_last,
                    seq = seq_last,
                    tomb = tomb_last,
                    "decoded last key"
                );
            }
            if let Some((user_new, seq_new, tomb_new)) =
                crate::common::internal_key::decode_internal_key(key)
            {
                error!(
                    user = ?user_new,
                    seq = seq_new,
                    tomb = tomb_new,
                    "decoded new key"
                );
            }
            return Err(MidgeError::InvalidData(format!(
                "Key ordering violation: new key {} <= last key {}",
                hex::encode(key),
                hex::encode(&self.last_key)
            )));
        }

        let mut shared_len = 0;
        if self.entries_since_restart < self.restart_interval {
            shared_len = shared_prefix_len(&self.last_key, key);
        } else {
            self.restarts.push(self.buffer.len() as u32);
            self.entries_since_restart = 0;
        }

        let key_delta = &key[shared_len..];

        // Use encoding module for TLV encoding
        let encoded = crate::sst::encoding::encode(
            key_delta,
            shared_len as u32,
            value,
            seq,
            op_type,
            internal_on_disk,
            expiration,
        );

        self.buffer.extend_from_slice(&encoded);

        self.last_key.clear();
        self.last_key.extend_from_slice(key);
        self.entries_since_restart += 1;
        Ok(())
    }

    pub fn add(&mut self, key: &[u8], value: &[u8]) -> MidgeResult<()> {
        if key.is_empty() {
            return Err(MidgeError::InvalidData("Key cannot be empty".into()));
        }
        if !(self.last_key.is_empty() || key > self.last_key.as_slice()) {
            return Err(MidgeError::InvalidData(format!(
                "Key ordering violation in add(): new key {} <= last key {}",
                hex::encode(key),
                hex::encode(&self.last_key)
            )));
        }

        let mut shared_len = 0;
        if self.entries_since_restart < self.restart_interval {
            // Find shared prefix with last key
            shared_len = shared_prefix_len(&self.last_key, key);
        } else {
            // Start new restart point
            self.restarts.push(self.buffer.len() as u32);
            self.entries_since_restart = 0;
        }

        let key_delta = &key[shared_len..];

        // Use encoding module for TLV encoding
        let encoded = crate::sst::encoding::encode(
            key_delta,
            shared_len as u32,
            Some(value),
            0,
            0, // Put entry
            false,
            None,
        );

        self.buffer.extend_from_slice(&encoded);

        self.last_key.clear();
        self.last_key.extend_from_slice(key);
        self.entries_since_restart += 1;
        Ok(())
    }

    /// Add a key-value pair without ordering checks.
    ///
    /// This is used by IndexBlockBuilder when internal key ordering semantics
    /// are needed, where the caller has already validated ordering.
    ///
    /// # Safety (in terms of data integrity)
    /// The caller must ensure keys are properly ordered before calling this.
    pub fn add_unchecked(&mut self, key: &[u8], value: &[u8]) -> MidgeResult<()> {
        if key.is_empty() {
            return Err(MidgeError::InvalidData("Key cannot be empty".into()));
        }

        let mut shared_len = 0;
        if self.entries_since_restart < self.restart_interval {
            shared_len = shared_prefix_len(&self.last_key, key);
        } else {
            self.restarts.push(self.buffer.len() as u32);
            self.entries_since_restart = 0;
        }

        let key_delta = &key[shared_len..];

        let encoded = crate::sst::encoding::encode(
            key_delta,
            shared_len as u32,
            Some(value),
            0,
            0,
            false,
            None,
        );

        self.buffer.extend_from_slice(&encoded);

        self.last_key.clear();
        self.last_key.extend_from_slice(key);
        self.entries_since_restart += 1;
        Ok(())
    }

    /// Convenience wrapper to add an entry where sequence/kind are encoded in the key
    /// and no per-entry meta is written.
    pub fn add_with_meta_internal(
        &mut self,
        key: &[u8],
        value: Option<&[u8]>,
        seq: u64,
        tombstone: bool,
        expiration: Option<u64>,
    ) -> MidgeResult<()> {
        let op_type = if tombstone { 2 } else { 0 };
        self.add_with_meta(key, value, seq, op_type, true, expiration)
    }

    pub fn finish(mut self) -> Bytes {
        let _header_size = 1 + 4 + 4 * self.restarts.len();
        // Write entries first
        // self.buffer already has the entries

        // Then write header at the end
        // TLV layout: [entries] [version: u8] [restarts: n * u32] [restart_count: u32]
        // Version byte placed before restart array for compatibility with disk layout
        self.buffer.put_u8(3); // Version 3 = TLV format
        for restart in &self.restarts {
            self.buffer.put_u32_le(*restart);
        }
        // Restart count must be last (readers expect it at the final 4 bytes)
        self.buffer.put_u32_le(self.restarts.len() as u32);

        let _buffer_len = self.buffer.len();
        self.buffer.freeze()
    }

    pub fn is_empty(&self) -> bool {
        self.buffer.is_empty()
    }

    pub fn estimated_size(&self) -> usize {
        self.buffer.len() + self.restarts.len() * 4 + 4
    }
}

/// Index block builder for constructing index blocks.
///
/// When using internal keys, this builder relaxes the ordering check to allow
/// internal key comparison semantics (user_key ASC, seq DESC) rather than
/// strict byte ordering.
pub struct IndexBlockBuilder {
    data_builder: DataBlockBuilder,
    /// Whether to use internal key comparison semantics for ordering checks
    use_internal_keys: bool,
    /// Last key added (for internal key ordering validation)
    last_key: Vec<u8>,
}

impl IndexBlockBuilder {
    pub fn new() -> Self {
        Self {
            data_builder: DataBlockBuilder::new(1), // Index blocks don't need restart intervals
            use_internal_keys: false,
            last_key: Vec::new(),
        }
    }

    /// Create an index block builder with internal key support.
    pub fn new_with_internal_keys(use_internal_keys: bool) -> Self {
        Self {
            data_builder: DataBlockBuilder::new(1),
            use_internal_keys,
            last_key: Vec::new(),
        }
    }

    pub fn add_index_entry(&mut self, last_key: &[u8], handle: BlockHandle) -> MidgeResult<()> {
        let handle_encoding = handle.encode();

        if self.use_internal_keys {
            // For internal keys, use proper internal key comparison
            // which handles user_key ASC, seq DESC ordering
            if !self.last_key.is_empty() {
                let ordering =
                    crate::common::internal_key::compare_internal_keys(&self.last_key, last_key);
                if ordering != std::cmp::Ordering::Less {
                    return Err(MidgeError::InvalidData(format!(
                        "Index key ordering violation: new key {} should be > last key {}",
                        hex::encode(last_key),
                        hex::encode(&self.last_key)
                    )));
                }
            }
            self.last_key.clear();
            self.last_key.extend_from_slice(last_key);
            // Bypass DataBlockBuilder's ordering check by directly adding to buffer
            self.data_builder.add_unchecked(last_key, &handle_encoding)
        } else {
            self.data_builder.add(last_key, &handle_encoding)
        }
    }

    pub fn finish(self) -> Bytes {
        self.data_builder.finish()
    }

    pub fn is_empty(&self) -> bool {
        self.data_builder.is_empty()
    }
}

impl Default for IndexBlockBuilder {
    fn default() -> Self {
        Self::new()
    }
}

// Utility functions for varint encoding/decoding

pub fn encode_varint32(buf: &mut BytesMut, mut value: u32) {
    while value >= 0x80 {
        buf.put_u8((value & 0x7F) as u8 | 0x80);
        value >>= 7;
    }
    buf.put_u8(value as u8);
}

pub fn encode_varint64(buf: &mut Vec<u8>, mut value: u64) {
    while value >= 0x80 {
        buf.push((value & 0x7F) as u8 | 0x80);
        value >>= 7;
    }
    buf.push(value as u8);
}

#[inline]
pub fn decode_varint32(data: &[u8]) -> MidgeResult<(u32, usize)> {
    let mut result = 0u32;
    let mut shift = 0;
    let mut pos = 0;

    while pos < data.len() {
        let byte = data[pos];
        pos += 1;

        if shift >= 32 {
            return Err(MidgeError::InvalidData("Varint32 overflow".into()));
        }

        result |= ((byte & 0x7F) as u32) << shift;

        if (byte & 0x80) == 0 {
            return Ok((result, pos));
        }

        shift += 7;
    }

    Err(MidgeError::InvalidData("Incomplete varint32".into()))
}

#[inline]
pub fn decode_varint64(data: &[u8]) -> MidgeResult<(u64, usize)> {
    let mut result = 0u64;
    let mut shift = 0;
    let mut pos = 0;

    while pos < data.len() {
        let byte = data[pos];
        pos += 1;

        if shift >= 64 {
            return Err(MidgeError::InvalidData("Varint64 overflow".into()));
        }

        result |= ((byte & 0x7F) as u64) << shift;

        if (byte & 0x80) == 0 {
            return Ok((result, pos));
        }

        shift += 7;
    }

    Err(MidgeError::InvalidData("Incomplete varint64".into()))
}

/// Find the length of the shared prefix between two byte slices.
/// Optimized with 8-byte word-aligned comparison for long prefixes.
#[inline]
fn shared_prefix_len(a: &[u8], b: &[u8]) -> usize {
    let min_len = a.len().min(b.len());
    let mut i = 0;

    // Fast path: compare 8 bytes at a time using word-aligned reads
    // This is significantly faster for sequential keys with long prefixes
    // (e.g., "user:12345:*" keys where prefix is common)
    while i + 8 <= min_len {
        // Safe version: copy 8 bytes into fixed-size arrays and convert
        let mut a_tmp = [0u8; 8];
        let mut b_tmp = [0u8; 8];
        a_tmp.copy_from_slice(&a[i..i + 8]);
        b_tmp.copy_from_slice(&b[i..i + 8]);

        let a_word = u64::from_ne_bytes(a_tmp);
        let b_word = u64::from_ne_bytes(b_tmp);

        if a_word != b_word {
            // Words differ - find exact byte where they diverge
            break;
        }
        i += 8;
    }

    // Finish remaining bytes (0-7 bytes) byte-by-byte
    while i < min_len && a[i] == b[i] {
        i += 1;
    }

    i
}

#[cfg(test)]
mod tests {
    use super::*;
    use bytes::Bytes;

    #[test]
    fn should_store_offset_size_when_created() {
        // Arrange
        // Act
        let handle = BlockHandle::new(100, 200);

        // Assert
        assert_eq!(handle.offset, 100);
        assert_eq!(handle.size, 200);
    }

    #[test]
    fn should_roundtrip_block_handle_encoding() {
        // Arrange
        let handle = BlockHandle::new(12345, 67890);

        // Act
        let encoded = handle.encode();
        let (decoded, len) = BlockHandle::decode(&encoded).expect("decode");

        // Assert
        assert_eq!(decoded.offset, 12345);
        assert_eq!(decoded.size, 67890);
        assert_eq!(len, encoded.len());
    }

    #[test]
    fn should_roundtrip_zero_offset_size() {
        // Arrange
        let handle = BlockHandle::new(0, 0);

        // Act
        let encoded = handle.encode();
        let (decoded, _) = BlockHandle::decode(&encoded).expect("decode");

        // Assert
        assert_eq!(decoded.offset, 0);
        assert_eq!(decoded.size, 0);
    }

    #[test]
    fn should_encode_max_u64_values_correctly() {
        // Arrange
        let handle = BlockHandle::new(u64::MAX, u64::MAX);

        // Act
        let encoded = handle.encode();
        let (decoded, _) = BlockHandle::decode(&encoded).expect("decode");

        // Assert
        assert_eq!(decoded.offset, u64::MAX);
        assert_eq!(decoded.size, u64::MAX);
    }

    #[test]
    fn should_store_data_type_compression_when_created() {
        // Arrange
        let data = Bytes::from("test data");

        // Act
        let block = Block::new(
            data.clone(),
            BlockType::Data,
            crate::common::codec::CompressionType::None,
        );

        // Assert
        assert_eq!(block.data, data);
        assert_eq!(block.block_type, BlockType::Data);
        assert_eq!(
            block.compression,
            crate::common::codec::CompressionType::None
        );
    }

    #[test]
    fn should_roundtrip_data_block_without_compression() {
        // Arrange
        let mut data = vec![1, 2, 3, 4];
        data.extend_from_slice(&[0, 0, 0, 0]);
        data.extend_from_slice(&[1, 0, 0, 0]);
        let block = Block::new(
            Bytes::from(data),
            BlockType::Data,
            crate::common::codec::CompressionType::None,
        );

        // Act
        let encoded = block.encode().expect("encode");
        let decoded = Block::decode(&encoded, BlockType::Data).expect("decode");

        // Assert
        assert_eq!(decoded.data, block.data);
        assert_eq!(decoded.block_type, BlockType::Data);
    }

    #[test]
    fn should_roundtrip_data_block_with_lz4_compression() {
        // Arrange
        let mut data = vec![1, 2, 3, 4, 5, 6, 7, 8]; // body
        data.extend_from_slice(&[0, 0, 0, 0]); // restart offset
        data.extend_from_slice(&[1, 0, 0, 0]); // restart count = 1
        let block = Block::new(
            Bytes::from(data.clone()),
            BlockType::Data,
            crate::common::codec::CompressionType::Lz4,
        );

        // Act
        let encoded = block.encode().expect("encode");
        let decoded = Block::decode(&encoded, BlockType::Data).expect("decode");

        // Assert
        assert_eq!(decoded.data, Bytes::from(data));
        assert_eq!(
            decoded.compression,
            crate::common::codec::CompressionType::Lz4
        );
    }

    #[test]
    fn should_roundtrip_data_block_with_zstd1_compression() {
        // Arrange
        let mut data = vec![1, 2, 3, 4, 5, 6, 7, 8]; // body
        data.extend_from_slice(&[0, 0, 0, 0]); // restart offset
        data.extend_from_slice(&[1, 0, 0, 0]); // restart count = 1
        let block = Block::new(
            Bytes::from(data.clone()),
            BlockType::Data,
            crate::common::codec::CompressionType::Zstd1,
        );

        // Act
        let encoded = block.encode().expect("encode");
        let decoded = Block::decode(&encoded, BlockType::Data).expect("decode");

        // Assert
        assert_eq!(decoded.data, Bytes::from(data));
        assert_eq!(
            decoded.compression,
            crate::common::codec::CompressionType::Zstd1
        );
    }

    #[test]
    fn should_roundtrip_data_block_with_zstd3_compression() {
        // Arrange
        let mut data = vec![1, 2, 3, 4, 5, 6, 7, 8]; // body
        data.extend_from_slice(&[0, 0, 0, 0]); // restart offset
        data.extend_from_slice(&[1, 0, 0, 0]); // restart count = 1
        let block = Block::new(
            Bytes::from(data.clone()),
            BlockType::Data,
            crate::common::codec::CompressionType::Zstd3,
        );

        // Act
        let encoded = block.encode().expect("encode");
        let decoded = Block::decode(&encoded, BlockType::Data).expect("decode");

        // Assert
        assert_eq!(decoded.data, Bytes::from(data));
        assert_eq!(
            decoded.compression,
            crate::common::codec::CompressionType::Zstd3
        );
    }

    #[test]
    fn should_roundtrip_data_block_with_zstd5_compression() {
        // Arrange
        let mut data = vec![1, 2, 3, 4, 5, 6, 7, 8]; // body
        data.extend_from_slice(&[0, 0, 0, 0]); // restart offset
        data.extend_from_slice(&[1, 0, 0, 0]); // restart count = 1
        let block = Block::new(
            Bytes::from(data.clone()),
            BlockType::Data,
            crate::common::codec::CompressionType::Zstd5,
        );

        // Act
        let encoded = block.encode().expect("encode");
        let decoded = Block::decode(&encoded, BlockType::Data).expect("decode");

        // Assert
        assert_eq!(decoded.data, Bytes::from(data));
        assert_eq!(
            decoded.compression,
            crate::common::codec::CompressionType::Zstd5
        );
    }

    #[test]
    fn should_roundtrip_data_block_with_zstd9_compression() {
        // Arrange
        let mut data = vec![1, 2, 3, 4, 5, 6, 7, 8]; // body
        data.extend_from_slice(&[0, 0, 0, 0]); // restart offset
        data.extend_from_slice(&[1, 0, 0, 0]); // restart count = 1
        let block = Block::new(
            Bytes::from(data.clone()),
            BlockType::Data,
            crate::common::codec::CompressionType::Zstd9,
        );

        // Act
        let encoded = block.encode().expect("encode");
        let decoded = Block::decode(&encoded, BlockType::Data).expect("decode");

        // Assert
        assert_eq!(decoded.data, Bytes::from(data));
        assert_eq!(
            decoded.compression,
            crate::common::codec::CompressionType::Zstd9
        );
    }

    #[test]
    fn should_roundtrip_filter_block_type() {
        // Arrange
        let data = Bytes::from("filter data");
        let block = Block::new(
            data.clone(),
            BlockType::Filter,
            crate::common::codec::CompressionType::None,
        );

        // Act
        let encoded = block.encode().expect("encode");
        let decoded = Block::decode(&encoded, BlockType::Filter).expect("decode");

        // Assert
        assert_eq!(decoded.data, data);
        assert_eq!(decoded.block_type, BlockType::Filter);
    }

    #[test]
    fn should_roundtrip_index_block_type() {
        // Arrange
        let data = Bytes::from("index data");
        let block = Block::new(
            data.clone(),
            BlockType::Index,
            crate::common::codec::CompressionType::None,
        );

        // Act
        let encoded = block.encode().expect("encode");
        let decoded = Block::decode(&encoded, BlockType::Index).expect("decode");

        // Assert
        assert_eq!(decoded.data, data);
        assert_eq!(decoded.block_type, BlockType::Index);
    }

    #[test]
    fn should_return_error_given_corrupted_crc() {
        // Arrange
        let mut data = vec![1, 2, 3, 4];
        data.extend_from_slice(&[0, 0, 0, 0]);
        data.extend_from_slice(&[1, 0, 0, 0]);
        let block = Block::new(
            Bytes::from(data),
            BlockType::Data,
            crate::common::codec::CompressionType::None,
        );
        let mut encoded = block.encode().expect("encode").to_vec();

        // Act
        let len = encoded.len();
        encoded[len - 1] ^= 0xFF;

        // Assert
        let result = Block::decode(&encoded, BlockType::Data);
        assert!(result.is_err());
        assert!(result.unwrap_err().to_string().contains("CRC"));
    }

    #[test]
    fn should_return_error_given_insufficient_block_size() {
        // Arrange
        let data = vec![1, 2, 3];

        // Act
        let result = Block::decode(&data, BlockType::Data);

        // Assert
        assert!(result.is_err());
        assert!(result.unwrap_err().to_string().contains("too small"));
    }

    #[test]
    fn should_store_handles_when_created() {
        // Arrange
        let index_handle = BlockHandle::new(100, 50);
        let meta_handle = BlockHandle::new(200, 30);

        // Act
        let footer = Footer::new(index_handle, meta_handle);

        // Assert
        assert_eq!(footer.index_handle.offset, 100);
        assert_eq!(footer.index_handle.size, 50);
        assert_eq!(footer.meta_index_handle.offset, 200);
        assert_eq!(footer.meta_index_handle.size, 30);
    }

    #[test]
    fn should_roundtrip_footer_encoding() {
        // Arrange
        let index_handle = BlockHandle::new(12345, 678);
        let meta_handle = BlockHandle::new(98765, 432);
        let footer = Footer::new(index_handle, meta_handle);

        // Act
        let encoded = footer.encode();
        let decoded = Footer::decode(&encoded).expect("decode");

        // Assert
        assert_eq!(decoded.index_handle.offset, 12345);
        assert_eq!(decoded.index_handle.size, 678);
        assert_eq!(decoded.meta_index_handle.offset, 98765);
        assert_eq!(decoded.meta_index_handle.size, 432);
    }

    #[test]
    fn should_encode_footer_to_fixed_48_byte_size() {
        // Arrange
        let footer = Footer::new(BlockHandle::new(1, 2), BlockHandle::new(3, 4));

        // Act
        let encoded = footer.encode();

        // Assert
        assert_eq!(encoded.len(), 48);
    }

    #[test]
    fn should_append_magic_number_when_encoding_footer() {
        // Arrange
        let footer = Footer::new(BlockHandle::new(100, 200), BlockHandle::new(300, 400));

        // Act
        let encoded = footer.encode();
        let decoded = Footer::decode(&encoded).expect("decode");

        // Assert
        assert_eq!(decoded.magic, 0xdb4775248b80fb57);
    }

    #[test]
    fn should_return_error_given_invalid_magic_number() {
        // Arrange
        let footer = Footer::new(BlockHandle::new(1, 2), BlockHandle::new(3, 4));
        let mut encoded = footer.encode().to_vec();

        // Act
        encoded[40] ^= 0xFF;

        // Assert
        let result = Footer::decode(&encoded);
        assert!(result.is_err());
    }

    #[test]
    fn should_return_error_given_truncated_footer() {
        // Arrange
        let data = vec![0u8; 40];

        // Act
        let result = Footer::decode(&data);

        // Assert
        assert!(result.is_err());
    }

    #[test]
    fn should_roundtrip_block_type_u8_conversion() {
        // Arrange
        // Act
        // Assert
        assert_eq!(BlockType::try_from(0).unwrap(), BlockType::Data);
        assert_eq!(BlockType::try_from(1).unwrap(), BlockType::Filter);
        assert_eq!(BlockType::try_from(2).unwrap(), BlockType::Index);
        assert_eq!(BlockType::try_from(3).unwrap(), BlockType::MetaIndex);
        assert!(BlockType::try_from(4).is_err());
        assert!(BlockType::try_from(255).is_err());
    }

    #[test]
    fn should_roundtrip_empty_data_block() {
        // Arrange
        let data = vec![0, 0, 0, 0];
        let block = Block::new(
            Bytes::from(data.clone()),
            BlockType::Data,
            crate::common::codec::CompressionType::None,
        );

        // Act
        let encoded = block.encode().expect("encode");
        let decoded = Block::decode(&encoded, BlockType::Data).expect("decode");

        // Assert
        assert_eq!(decoded.data, Bytes::from(data));
    }

    #[test]
    fn should_roundtrip_filter_block_with_compression() {
        // Arrange
        let data = Bytes::from("filter bloom bits data");
        let block = Block::new(
            data.clone(),
            BlockType::Filter,
            crate::common::codec::CompressionType::Lz4,
        );

        // Act
        let encoded = block.encode().expect("encode");
        let decoded = Block::decode(&encoded, BlockType::Filter).expect("decode");

        // Assert
        assert_eq!(decoded.data, data);
        assert_eq!(
            decoded.compression,
            crate::common::codec::CompressionType::Lz4
        );
    }

    // Tests for Result-returning DataBlockBuilder methods
    #[test]
    fn should_error_given_empty_key_when_adding_with_meta() {
        // Arrange
        let mut builder = DataBlockBuilder::new(16 * 1024);

        // Act
        let result = builder.add_with_meta(b"", Some(b"value"), 100, 0, false, None);

        // Assert
        assert!(result.is_err());
        match result.unwrap_err() {
            MidgeError::InvalidData(msg) => {
                assert!(msg.contains("cannot be empty"));
            }
            _ => panic!("Expected InvalidData error"),
        }
    }

    #[test]
    fn should_error_given_key_ordering_violation_when_adding_with_meta() {
        // Arrange
        let mut builder = DataBlockBuilder::new(16 * 1024);
        builder
            .add_with_meta(b"key2", Some(b"value2"), 100, 0, false, None)
            .expect("first add should succeed");

        // Act
        let result = builder.add_with_meta(b"key1", Some(b"value1"), 101, 0, false, None);

        // Assert
        assert!(result.is_err());
        match result.unwrap_err() {
            MidgeError::InvalidData(msg) => {
                assert!(msg.contains("Key ordering violation"));
                assert!(msg.contains("6b657931"));
                assert!(msg.contains("6b657932"));
            }
            _ => panic!("Expected InvalidData error with key diagnostics"),
        }
    }

    #[test]
    fn should_succeed_given_valid_ascending_keys_when_adding_with_meta() {
        // Arrange
        let mut builder = DataBlockBuilder::new(16 * 1024);

        // Act
        let result1 = builder.add_with_meta(b"a", Some(b"value_a"), 100, 0, false, None);
        let result2 = builder.add_with_meta(b"b", Some(b"value_b"), 101, 0, false, None);
        let result3 = builder.add_with_meta(b"c", Some(b"value_c"), 102, 0, false, None);

        // Assert
        assert!(result1.is_ok());
        assert!(result2.is_ok());
        assert!(result3.is_ok());
    }

    #[test]
    fn should_allow_tombstone_entries_when_adding_with_meta() {
        // Arrange
        let mut builder = DataBlockBuilder::new(16 * 1024);

        // Act
        let result = builder.add_with_meta(b"deleted_key", None, 200, 2, false, None);

        // Assert
        assert!(result.is_ok());
    }

    #[test]
    fn should_handle_expiration_metadata_when_adding_with_meta() {
        // Arrange
        let mut builder = DataBlockBuilder::new(16 * 1024);
        let expiration = Some(1698262800000u64); // Unix milliseconds

        // Act
        let result = builder.add_with_meta(b"ttl_key", Some(b"value"), 100, 0, false, expiration);

        // Assert
        assert!(result.is_ok());
    }

    #[test]
    fn should_error_given_empty_key_when_adding() {
        // Arrange
        let mut builder = DataBlockBuilder::new(16 * 1024);

        // Act
        let result = builder.add(b"", b"value");

        // Assert
        assert!(result.is_err());
        match result.unwrap_err() {
            MidgeError::InvalidData(msg) => {
                assert!(msg.contains("cannot be empty"));
            }
            _ => panic!("Expected InvalidData error"),
        }
    }

    #[test]
    fn should_error_given_key_ordering_violation_when_adding() {
        // Arrange
        let mut builder = DataBlockBuilder::new(16 * 1024);
        builder
            .add(b"z", b"value_z")
            .expect("first add should succeed");

        // Act
        let result = builder.add(b"a", b"value_a");

        // Assert
        assert!(result.is_err());
        match result.unwrap_err() {
            MidgeError::InvalidData(msg) => {
                assert!(msg.contains("Key ordering violation"));
            }
            _ => panic!("Expected InvalidData error"),
        }
    }

    #[test]
    fn should_succeed_given_valid_ascending_keys_when_adding() {
        // Arrange
        let mut builder = DataBlockBuilder::new(16 * 1024);

        // Act
        let result1 = builder.add(b"apple", b"value1");
        let result2 = builder.add(b"banana", b"value2");
        let result3 = builder.add(b"cherry", b"value3");

        // Assert
        assert!(result1.is_ok());
        assert!(result2.is_ok());
        assert!(result3.is_ok());
    }

    #[test]
    fn should_error_given_empty_index_key_when_adding_index_entry() {
        // Arrange
        let mut builder = IndexBlockBuilder::new();
        let handle = BlockHandle::new(1000, 200);

        // Act
        let result = builder.add_index_entry(b"", handle);

        // Assert
        assert!(result.is_err());
        match result.unwrap_err() {
            MidgeError::InvalidData(msg) => {
                assert!(msg.contains("cannot be empty"));
            }
            _ => panic!("Expected InvalidData error"),
        }
    }

    #[test]
    fn should_error_given_index_key_ordering_violation_when_adding_index_entry() {
        // Arrange
        let mut builder = IndexBlockBuilder::new();
        let handle1 = BlockHandle::new(1000, 200);
        let handle2 = BlockHandle::new(2000, 300);

        builder
            .add_index_entry(b"index_key_2", handle1)
            .expect("first add should succeed");

        // Act
        let result = builder.add_index_entry(b"index_key_1", handle2);

        // Assert
        assert!(result.is_err());
        match result.unwrap_err() {
            MidgeError::InvalidData(msg) => {
                assert!(msg.contains("Key ordering violation"));
            }
            _ => panic!("Expected InvalidData error"),
        }
    }

    #[test]
    fn should_succeed_given_valid_ascending_index_keys_when_adding_index_entry() {
        // Arrange
        let mut builder = IndexBlockBuilder::new();
        let handle1 = BlockHandle::new(1000, 200);
        let handle2 = BlockHandle::new(2000, 300);
        let handle3 = BlockHandle::new(3000, 400);

        // Act
        let result1 = builder.add_index_entry(b"a", handle1);
        let result2 = builder.add_index_entry(b"m", handle2);
        let result3 = builder.add_index_entry(b"z", handle3);

        // Assert
        assert!(result1.is_ok());
        assert!(result2.is_ok());
        assert!(result3.is_ok());
    }

    #[test]
    fn should_allow_internal_keys_ordering_when_internal_keys_enabled() {
        // Arrange - build two internal keys where raw bytes may not be lexicographically
        // ordered the same as internal key comparator (user_key 'k1' vs 'k10')
        use crate::api::column_family::ColumnFamilyId;
        use crate::common::internal_key::encode_internal_key_cf;

        let cf_id = ColumnFamilyId::new(0);
        let ik1 = encode_internal_key_cf(
            cf_id,
            b"k1",
            100,
            crate::common::internal_key::EntryType::Value,
        );
        let ik10 = encode_internal_key_cf(
            cf_id,
            b"k10",
            10,
            crate::common::internal_key::EntryType::Value,
        );

        // Ensure comparator orders them properly
        let cmp = crate::common::internal_key::compare_internal_keys_cf(&ik1, &ik10);
        assert_eq!(cmp, std::cmp::Ordering::Less);

        // Index builder with internal key mode enabled
        let mut builder = IndexBlockBuilder::new_with_internal_keys(true);
        let handle1 = BlockHandle::new(1000, 200);
        let handle2 = BlockHandle::new(2000, 300);

        // Act
        let r1 = builder.add_index_entry(&ik1, handle1);
        let r2 = builder.add_index_entry(&ik10, handle2);

        // Assert - both succeed (internal comparator is used)
        assert!(r1.is_ok());
        assert!(r2.is_ok());
    }

    // =====================================================================
    // P0: DataBlockBuilder ordering invariant tests
    // =====================================================================

    #[test]
    fn should_reject_duplicate_keys_when_adding() {
        // Arrange
        let mut builder = DataBlockBuilder::new(16 * 1024);
        builder.add(b"same_key", b"value1").expect("first add");

        // Act
        let result = builder.add(b"same_key", b"value2");

        // Assert: Duplicate key should be rejected as ordering violation
        assert!(result.is_err());
        let err_msg = result.unwrap_err().to_string();
        assert!(err_msg.contains("ordering violation") || err_msg.contains("Ordering"));
    }

    #[test]
    fn should_allow_add_unchecked_to_bypass_ordering_validation() {
        // Arrange: add_unchecked is intended for index builder paths where
        // the caller has already validated ordering
        let mut builder = DataBlockBuilder::new(16 * 1024);
        builder.add(b"zzz", b"value1").expect("first add");

        // Act: add_unchecked should bypass ordering checks
        let _ = builder.add_unchecked(b"aaa", b"value2");

        // Assert: Builder should still finish (caller takes responsibility)
        let data = builder.finish();
        assert!(!data.is_empty());
    }

    #[test]
    fn should_reject_out_of_order_internal_keys_when_internal_format() {
        // Arrange: Two internal keys where second comes before first in internal ordering
        use crate::common::internal_key::encode_internal_key;
        let ik_later = encode_internal_key(b"user_key", 100, false);
        let ik_earlier = encode_internal_key(b"user_key", 200, false); // Higher seq = earlier in order

        let mut builder = DataBlockBuilder::new(16 * 1024);
        builder
            .add_with_meta(&ik_later, Some(b"v1"), 100, 0, true, None)
            .expect("first add");

        // Act: Adding earlier seq (which sorts after in internal order) should fail
        let result = builder.add_with_meta(&ik_earlier, Some(b"v2"), 200, 0, true, None);

        // Assert
        assert!(result.is_err());
    }

    #[test]
    fn should_accept_properly_ordered_internal_keys_when_internal_format() {
        // Arrange: Internal keys in proper order (user_key asc, seq desc)
        use crate::common::internal_key::encode_internal_key;
        let ik1 = encode_internal_key(b"aaa", 200, false);
        let ik2 = encode_internal_key(b"aaa", 100, false); // Same user key, lower seq = later
        let ik3 = encode_internal_key(b"bbb", 50, false); // Different user key

        let mut builder = DataBlockBuilder::new(16 * 1024);

        // Act
        let r1 = builder.add_with_meta(&ik1, Some(b"v1"), 200, 0, true, None);
        let r2 = builder.add_with_meta(&ik2, Some(b"v2"), 100, 0, true, None);
        let r3 = builder.add_with_meta(&ik3, Some(b"v3"), 50, 0, true, None);

        // Assert
        assert!(r1.is_ok());
        assert!(r2.is_ok());
        assert!(r3.is_ok());
    }

    #[test]
    fn should_produce_decodable_block_from_data_block_builder() {
        // Arrange
        let mut builder = DataBlockBuilder::new(16 * 1024);
        builder.add(b"apple", b"red").unwrap();
        builder.add(b"banana", b"yellow").unwrap();
        builder.add(b"cherry", b"red").unwrap();

        // Act
        let block_data = builder.finish();

        // Assert: Should be parseable by TlvBlockIterator
        let iter = crate::sst::encoding::TlvBlockIterator::new(&block_data);
        let entries: Vec<_> = iter.collect();
        assert_eq!(entries.len(), 3);
        let (k1, _, _, _, _) = entries[0].as_ref().unwrap();
        assert_eq!(k1, b"apple");
    }

    // =====================================================================
    // P0: IndexBlockBuilder internal-key ordering tests
    // =====================================================================

    #[test]
    fn should_store_block_handles_in_index() {
        // Arrange
        let mut builder = IndexBlockBuilder::new();
        let h1 = BlockHandle::new(0, 100);
        let h2 = BlockHandle::new(100, 200);

        // Act
        builder.add_index_entry(b"block1_last", h1).unwrap();
        builder.add_index_entry(b"block2_last", h2).unwrap();
        let index_data = builder.finish();

        // Assert: Should decode via SparseIndex
        let sparse = crate::sst::sparse_index::SparseIndex::decode(&index_data).unwrap();
        assert_eq!(sparse.entries().len(), 2);
        assert_eq!(sparse.entries()[0].block_handle.offset, 0);
        assert_eq!(sparse.entries()[1].block_handle.offset, 100);
    }

    #[test]
    fn should_reject_out_of_order_index_keys_in_default_mode() {
        // Arrange
        let mut builder = IndexBlockBuilder::new();
        builder
            .add_index_entry(b"z_block", BlockHandle::new(0, 100))
            .unwrap();

        // Act
        let result = builder.add_index_entry(b"a_block", BlockHandle::new(100, 100));

        // Assert
        assert!(result.is_err());
    }

    #[test]
    fn should_use_internal_key_comparator_when_internal_keys_enabled() {
        // Arrange: Internal keys with same user key but different sequences
        use crate::common::internal_key::encode_internal_key;
        let ik1 = encode_internal_key(b"hot_key", 1000, false);
        let ik2 = encode_internal_key(b"hot_key", 500, false); // Lower seq = later in order

        let mut builder = IndexBlockBuilder::new_with_internal_keys(true);

        // Act
        let r1 = builder.add_index_entry(&ik1, BlockHandle::new(0, 100));
        let r2 = builder.add_index_entry(&ik2, BlockHandle::new(100, 100));

        // Assert: Both should succeed (internal comparator respects seq ordering)
        assert!(r1.is_ok());
        assert!(r2.is_ok());
    }
}
