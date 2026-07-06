//! SST core types and structures

use bytes::Bytes;
use std::fmt;

/// Handle to a block in SST file (offset + size)
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct BlockHandle {
    pub offset: u64,
    pub size: u64,
}

impl BlockHandle {
    #[must_use]
    pub fn new(offset: u64, size: u64) -> Self {
        Self { offset, size }
    }
}

/// Block types in SST file
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
#[repr(u8)]
pub enum BlockType {
    Data = 0,
    Index = 1,
    MetaIndex = 2,
}

/// A block in the SST file
#[derive(Debug, Clone)]
pub struct Block {
    pub data: Bytes,
    pub block_type: BlockType,
}

impl Block {
    pub fn new(data: Bytes, block_type: BlockType) -> Self {
        Self { data, block_type }
    }
}

/// RocksDB-compatible magic number for SST footer validation
pub const SST_FOOTER_MAGIC: u64 = 0xdb47_7524_8b80_fb57;

/// Legacy SST entry format without persisted expiration metadata.
pub const SST_FORMAT_V1: u32 = 1;

/// Stateful SST entry format with persisted expiration and metadata blocks.
pub const SST_FORMAT_V2: u32 = 2;

/// Footer stored at end of SST file
#[derive(Debug, Clone)]
pub struct Footer {
    pub meta_index_handle: BlockHandle,
    pub index_handle: BlockHandle,
    pub trie_handle: Option<BlockHandle>,
    pub block_bloom_handle: Option<BlockHandle>,
}

impl Footer {
    #[must_use]
    pub fn new(meta_index_handle: BlockHandle, index_handle: BlockHandle) -> Self {
        Self {
            meta_index_handle,
            index_handle,
            trie_handle: None,
            block_bloom_handle: None,
        }
    }

    #[must_use]
    pub fn with_trie(mut self, trie_handle: BlockHandle) -> Self {
        self.trie_handle = Some(trie_handle);
        self
    }

    #[must_use]
    pub fn with_block_bloom(mut self, block_bloom_handle: BlockHandle) -> Self {
        self.block_bloom_handle = Some(block_bloom_handle);
        self
    }

    /// Encode footer to exactly 72 bytes
    /// Layout:
    ///   [`meta_index_handle`: 16 bytes]
    ///   [`index_handle`: 16 bytes]
    ///   [`trie_handle`: 16 bytes (optional, 0 if None)]
    ///   [`block_bloom_handle`: 16 bytes (optional, 0 if None)]
    ///   [magic: 8 bytes]
    /// Total: 72 bytes (extended from 56 for block bloom support)
    #[must_use]
    pub fn encode(&self) -> Vec<u8> {
        let mut buf = vec![0u8; 72];
        // Store handles as fixed 16 bytes each
        // meta_index: offset (8) + size (8)
        buf[0..8].copy_from_slice(&self.meta_index_handle.offset.to_le_bytes());
        buf[8..16].copy_from_slice(&self.meta_index_handle.size.to_le_bytes());
        // index: offset (8) + size (8)
        buf[16..24].copy_from_slice(&self.index_handle.offset.to_le_bytes());
        buf[24..32].copy_from_slice(&self.index_handle.size.to_le_bytes());
        // trie: offset (8) + size (8) - zero if None
        if let Some(trie) = self.trie_handle {
            buf[32..40].copy_from_slice(&trie.offset.to_le_bytes());
            buf[40..48].copy_from_slice(&trie.size.to_le_bytes());
        }
        // block_bloom: offset (8) + size (8) - zero if None
        if let Some(block_bloom) = self.block_bloom_handle {
            buf[48..56].copy_from_slice(&block_bloom.offset.to_le_bytes());
            buf[56..64].copy_from_slice(&block_bloom.size.to_le_bytes());
        }
        // Magic number at end [64..72]
        buf[64..72].copy_from_slice(&SST_FOOTER_MAGIC.to_le_bytes());
        buf
    }

    /// Decode footer from 48, 56, or 72 bytes (backward compatible)
    ///
    /// # Errors
    ///
    /// Returns `MidgeError::Corruption` when the footer is truncated or has an invalid magic value.
    pub fn decode(data: &[u8]) -> crate::common::MidgeResult<Self> {
        if data.len() < 48 {
            return Err(crate::common::MidgeError::Corruption(
                "Footer too short".into(),
            ));
        }

        // Determine format: 48 (original), 56 (with trie), or 72 (with block bloom)
        let has_trie = data.len() >= 56;
        let has_block_bloom = data.len() >= 72;

        // Validate magic number
        let magic_offset = if has_block_bloom {
            64
        } else if has_trie {
            48
        } else {
            40
        };
        let magic = u64::from_le_bytes([
            data[magic_offset],
            data[magic_offset + 1],
            data[magic_offset + 2],
            data[magic_offset + 3],
            data[magic_offset + 4],
            data[magic_offset + 5],
            data[magic_offset + 6],
            data[magic_offset + 7],
        ]);
        if magic != SST_FOOTER_MAGIC {
            return Err(crate::common::MidgeError::Corruption(format!(
                "Invalid footer magic: expected 0x{SST_FOOTER_MAGIC:016x}, got 0x{magic:016x}"
            )));
        }

        let meta_offset = u64::from_le_bytes([
            data[0], data[1], data[2], data[3], data[4], data[5], data[6], data[7],
        ]);
        let meta_size = u64::from_le_bytes([
            data[8], data[9], data[10], data[11], data[12], data[13], data[14], data[15],
        ]);
        let idx_offset = u64::from_le_bytes([
            data[16], data[17], data[18], data[19], data[20], data[21], data[22], data[23],
        ]);
        let idx_size = u64::from_le_bytes([
            data[24], data[25], data[26], data[27], data[28], data[29], data[30], data[31],
        ]);

        // Read trie handle if format supports it
        let trie_handle = if has_trie {
            let trie_offset = u64::from_le_bytes([
                data[32], data[33], data[34], data[35], data[36], data[37], data[38], data[39],
            ]);
            let trie_size = u64::from_le_bytes([
                data[40], data[41], data[42], data[43], data[44], data[45], data[46], data[47],
            ]);
            if trie_offset == 0 && trie_size == 0 {
                None
            } else {
                Some(BlockHandle::new(trie_offset, trie_size))
            }
        } else {
            None
        };

        // Read block bloom handle if format supports it
        let block_bloom_handle = if has_block_bloom {
            let bloom_offset = u64::from_le_bytes([
                data[48], data[49], data[50], data[51], data[52], data[53], data[54], data[55],
            ]);
            let bloom_size = u64::from_le_bytes([
                data[56], data[57], data[58], data[59], data[60], data[61], data[62], data[63],
            ]);
            if bloom_offset == 0 && bloom_size == 0 {
                None
            } else {
                Some(BlockHandle::new(bloom_offset, bloom_size))
            }
        } else {
            None
        };

        Ok(Footer {
            meta_index_handle: BlockHandle::new(meta_offset, meta_size),
            index_handle: BlockHandle::new(idx_offset, idx_size),
            trie_handle,
            block_bloom_handle,
        })
    }
}

/// SST-level metadata persisted in the meta block.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct SstMetadata {
    pub format_version: u32,
    pub range_tombstone_handle: Option<BlockHandle>,
}

impl Default for SstMetadata {
    fn default() -> Self {
        Self {
            format_version: SST_FORMAT_V1,
            range_tombstone_handle: None,
        }
    }
}

impl SstMetadata {
    #[must_use]
    pub fn encode(&self) -> Vec<u8> {
        let mut buf = Vec::with_capacity(4 + 8 + 8);
        buf.extend_from_slice(&self.format_version.to_le_bytes());
        if let Some(handle) = self.range_tombstone_handle {
            buf.extend_from_slice(&handle.offset.to_le_bytes());
            buf.extend_from_slice(&handle.size.to_le_bytes());
        } else {
            buf.extend_from_slice(&0u64.to_le_bytes());
            buf.extend_from_slice(&0u64.to_le_bytes());
        }
        buf
    }

    /// Decode SST metadata from the meta block payload.
    ///
    /// # Errors
    ///
    /// Returns `MidgeError::Corruption` when the metadata block is truncated.
    pub fn decode(data: &[u8]) -> crate::common::MidgeResult<Self> {
        if data.len() < 20 {
            return Err(crate::common::MidgeError::Corruption(
                "SST metadata block too short".into(),
            ));
        }

        let format_version = u32::from_le_bytes([data[0], data[1], data[2], data[3]]);
        let offset = u64::from_le_bytes([
            data[4], data[5], data[6], data[7], data[8], data[9], data[10], data[11],
        ]);
        let size = u64::from_le_bytes([
            data[12], data[13], data[14], data[15], data[16], data[17], data[18], data[19],
        ]);

        Ok(Self {
            format_version,
            range_tombstone_handle: if offset == 0 && size == 0 {
                None
            } else {
                Some(BlockHandle::new(offset, size))
            },
        })
    }
}

/// Range tombstone for covering key ranges
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct RangeTombstone {
    pub start: Vec<u8>,
    pub end: Vec<u8>,
    pub seq: u64,
}

impl RangeTombstone {
    #[must_use]
    pub fn new(start: Vec<u8>, end: Vec<u8>, seq: u64) -> Self {
        Self { start, end, seq }
    }

    /// Check if a key is covered by this range tombstone
    #[must_use]
    pub fn covers(&self, key: &[u8]) -> bool {
        key >= self.start.as_slice() && key < self.end.as_slice()
    }
}

#[must_use]
pub fn encode_range_tombstones(tombstones: &[RangeTombstone]) -> Vec<u8> {
    let mut buf = Vec::new();
    buf.extend_from_slice(
        &u32::try_from(tombstones.len())
            .unwrap_or(u32::MAX)
            .to_le_bytes(),
    );

    for tombstone in tombstones {
        buf.extend_from_slice(
            &u32::try_from(tombstone.start.len())
                .unwrap_or(u32::MAX)
                .to_le_bytes(),
        );
        buf.extend_from_slice(&tombstone.start);
        buf.extend_from_slice(
            &u32::try_from(tombstone.end.len())
                .unwrap_or(u32::MAX)
                .to_le_bytes(),
        );
        buf.extend_from_slice(&tombstone.end);
        buf.extend_from_slice(&tombstone.seq.to_le_bytes());
    }

    buf
}

/// Decode serialized range tombstones.
///
/// # Errors
///
/// Returns `MidgeError::Corruption` when the encoded tombstone block is truncated.
pub fn decode_range_tombstones(data: &[u8]) -> crate::common::MidgeResult<Vec<RangeTombstone>> {
    if data.len() < 4 {
        return Err(crate::common::MidgeError::Corruption(
            "Range tombstone block too short".into(),
        ));
    }

    let mut offset = 0;
    let count = usize::try_from(read_range_tombstone_u32(
        data,
        &mut offset,
        "Range tombstone count truncated",
    )?)
    .map_err(|_| crate::common::MidgeError::Corruption("Range tombstone count overflow".into()))?;
    let remaining = data.len().saturating_sub(offset);
    if count > remaining / 16 {
        return Err(crate::common::MidgeError::Corruption(
            "Range tombstone count exceeds remaining bytes".into(),
        ));
    }
    let mut tombstones = Vec::with_capacity(count);

    for _ in 0..count {
        let start_len = usize::try_from(read_range_tombstone_u32(
            data,
            &mut offset,
            "Range tombstone start length truncated",
        )?)
        .map_err(|_| {
            crate::common::MidgeError::Corruption("Range tombstone start length overflow".into())
        })?;
        let start = read_range_tombstone_slice(
            data,
            &mut offset,
            start_len,
            "Range tombstone start bytes truncated",
        )?
        .to_vec();

        let end_len = usize::try_from(read_range_tombstone_u32(
            data,
            &mut offset,
            "Range tombstone end length truncated",
        )?)
        .map_err(|_| {
            crate::common::MidgeError::Corruption("Range tombstone end length overflow".into())
        })?;
        let end = read_range_tombstone_slice(
            data,
            &mut offset,
            end_len,
            "Range tombstone end bytes truncated",
        )?
        .to_vec();

        let seq =
            read_range_tombstone_u64(data, &mut offset, "Range tombstone sequence truncated")?;

        tombstones.push(RangeTombstone::new(start, end, seq));
    }

    Ok(tombstones)
}

fn read_range_tombstone_slice<'a>(
    data: &'a [u8],
    offset: &mut usize,
    len: usize,
    message: &str,
) -> crate::common::MidgeResult<&'a [u8]> {
    let end = offset.checked_add(len).ok_or_else(|| {
        crate::common::MidgeError::Corruption(format!("{message}: offset overflow"))
    })?;
    if end > data.len() {
        return Err(crate::common::MidgeError::Corruption(message.into()));
    }
    let slice = &data[*offset..end];
    *offset = end;
    Ok(slice)
}

fn read_range_tombstone_u32(
    data: &[u8],
    offset: &mut usize,
    message: &str,
) -> crate::common::MidgeResult<u32> {
    let bytes = read_range_tombstone_slice(data, offset, 4, message)?;
    Ok(u32::from_le_bytes([bytes[0], bytes[1], bytes[2], bytes[3]]))
}

fn read_range_tombstone_u64(
    data: &[u8],
    offset: &mut usize,
    message: &str,
) -> crate::common::MidgeResult<u64> {
    let bytes = read_range_tombstone_slice(data, offset, 8, message)?;
    Ok(u64::from_le_bytes([
        bytes[0], bytes[1], bytes[2], bytes[3], bytes[4], bytes[5], bytes[6], bytes[7],
    ]))
}

/// Parsed entry from SST block
#[derive(Debug, Clone)]
pub struct SstEntry {
    pub key: Vec<u8>,
    pub value: Option<Bytes>,
    pub sequence: u64,
    pub op_type: u8, // 0=Put, 1=Insert, 2=Delete
    pub expiration: Option<u64>,
}

impl SstEntry {
    pub fn new(
        key: Vec<u8>,
        value: Option<Bytes>,
        sequence: u64,
        op_type: u8,
        expiration: Option<u64>,
    ) -> Self {
        Self {
            key,
            value,
            sequence,
            op_type,
            expiration,
        }
    }

    pub fn is_tombstone(&self) -> bool {
        self.op_type == 2
    }

    pub fn is_expired(&self, now_millis: u64) -> bool {
        if let Some(exp) = self.expiration {
            now_millis >= exp
        } else {
            false
        }
    }
}

/// Key state in SST (used for tombstone-aware reads)
#[derive(Debug, Clone, PartialEq)]
pub enum KeyState {
    Absent,
    Tombstone(u64),                     // sequence number
    Value(Bytes, u64, Option<u64>, u8), // value, seq, expiration, op_type
}

impl fmt::Display for KeyState {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            KeyState::Absent => write!(f, "Absent"),
            KeyState::Tombstone(seq) => write!(f, "Tombstone(seq={seq})"),
            KeyState::Value(_, seq, exp, op) => {
                write!(f, "Value(seq={seq}, exp={exp:?}, op={op})")
            }
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    // =========== BlockHandle Tests ===========

    #[test]
    fn should_create_block_handle_with_zero_offset() {
        // Arrange
        // (no setup)

        // Act
        let handle = BlockHandle::new(0, 100);

        // Assert
        assert_eq!(handle.offset, 0);
        assert_eq!(handle.size, 100);
    }

    #[test]
    fn should_create_block_handle_with_large_offset() {
        // Arrange
        // (no setup)

        // Act
        let handle = BlockHandle::new(u64::MAX - 1000, 500);

        // Assert
        assert_eq!(handle.offset, u64::MAX - 1000);
        assert_eq!(handle.size, 500);
    }

    #[test]
    fn should_create_block_handle_with_max_size() {
        // Arrange
        // (no setup)

        // Act
        let handle = BlockHandle::new(1000, u64::MAX);

        // Assert
        assert_eq!(handle.size, u64::MAX);
    }

    #[test]
    fn should_block_handle_equality() {
        // Arrange
        let h1 = BlockHandle::new(100, 200);
        let h2 = BlockHandle::new(100, 200);

        // Act
        // (none)

        // Assert
        assert_eq!(h1, h2);
    }

    #[test]
    fn should_block_handle_inequality_on_offset() {
        // Arrange
        let h1 = BlockHandle::new(100, 200);
        let h2 = BlockHandle::new(101, 200);

        // Act
        // (none)

        // Assert
        assert_ne!(h1, h2);
    }

    #[test]
    fn should_block_handle_inequality_on_size() {
        // Arrange
        let h1 = BlockHandle::new(100, 200);
        let h2 = BlockHandle::new(100, 201);

        // Act
        // (none)

        // Assert
        assert_ne!(h1, h2);
    }

    #[test]
    fn should_block_handle_clone() {
        // Arrange
        let h1 = BlockHandle::new(100, 200);

        // Act
        let h2 = h1;

        // Assert
        assert_eq!(h1, h2);
    }

    // =========== BlockType Tests ===========

    #[test]
    fn should_block_type_data_has_correct_value() {
        // Assert
        assert_eq!(BlockType::Data as u8, 0);
    }

    #[test]
    fn should_block_type_index_has_correct_value() {
        // Assert
        assert_eq!(BlockType::Index as u8, 1);
    }

    #[test]
    fn should_block_type_meta_index_has_correct_value() {
        // Assert
        assert_eq!(BlockType::MetaIndex as u8, 2);
    }

    #[test]
    fn should_block_type_equality() {
        // Assert
        assert_eq!(BlockType::Data, BlockType::Data);
        assert_eq!(BlockType::Index, BlockType::Index);
    }

    #[test]
    fn should_block_type_inequality() {
        // Assert
        assert_ne!(BlockType::Data, BlockType::Index);
    }

    // =========== Block Tests ===========

    #[test]
    fn should_create_block_with_data() {
        // Arrange
        let data = Bytes::from("test_data");

        // Act
        let block = Block::new(data.clone(), BlockType::Data);

        // Assert
        assert_eq!(block.data, data);
        assert_eq!(block.block_type, BlockType::Data);
    }

    #[test]
    fn should_create_block_with_empty_data() {
        // Arrange
        // (no setup)

        // Act
        let block = Block::new(Bytes::new(), BlockType::Index);

        // Assert
        assert!(block.data.is_empty());
    }

    // =========== Footer Encoding/Decoding Tests ===========

    #[test]
    fn should_encode_footer_to_72_bytes() {
        // Arrange
        let footer = Footer::new(BlockHandle::new(0, 100), BlockHandle::new(100, 200));

        // Act
        let encoded = footer.encode();

        // Assert
        assert_eq!(encoded.len(), 72);
    }

    #[test]
    fn should_encode_footer_with_zero_offsets() {
        // Arrange
        let footer = Footer::new(BlockHandle::new(0, 0), BlockHandle::new(0, 0));

        // Act
        let encoded = footer.encode();

        // Assert
        assert_eq!(encoded.len(), 72);
        let decoded = Footer::decode(&encoded).unwrap();
        assert_eq!(decoded.meta_index_handle.offset, 0);
        assert_eq!(decoded.index_handle.offset, 0);
    }

    #[test]
    fn should_encode_footer_with_large_offsets() {
        // Arrange
        let footer = Footer::new(
            BlockHandle::new(u64::MAX - 1000, 500),
            BlockHandle::new(u64::MAX - 500, 100),
        );

        // Act
        let encoded = footer.encode();

        // Assert
        assert_eq!(encoded.len(), 72);
        let decoded = Footer::decode(&encoded).unwrap();
        assert_eq!(decoded.meta_index_handle.offset, u64::MAX - 1000);
        assert_eq!(decoded.index_handle.offset, u64::MAX - 500);
    }

    #[test]
    fn should_decode_footer_meta_index_handle() {
        // Arrange
        let footer = Footer::new(BlockHandle::new(0, 100), BlockHandle::new(100, 200));
        let encoded = footer.encode();

        // Act
        let decoded = Footer::decode(&encoded).unwrap();

        // Assert
        assert_eq!(decoded.meta_index_handle.offset, 0);
        assert_eq!(decoded.meta_index_handle.size, 100);
    }

    #[test]
    fn should_decode_footer_index_handle() {
        // Arrange
        let footer = Footer::new(BlockHandle::new(0, 100), BlockHandle::new(100, 200));
        let encoded = footer.encode();

        // Act
        let decoded = Footer::decode(&encoded).unwrap();

        // Assert
        assert_eq!(decoded.index_handle.offset, 100);
        assert_eq!(decoded.index_handle.size, 200);
    }

    #[test]
    fn should_decode_footer_without_trie() {
        // Arrange
        let footer = Footer::new(BlockHandle::new(0, 100), BlockHandle::new(100, 200));
        let encoded = footer.encode();

        // Act
        let decoded = Footer::decode(&encoded).unwrap();

        // Assert
        assert_eq!(decoded.trie_handle, None);
    }

    #[test]
    fn should_add_trie_to_footer() {
        // Arrange
        let footer = Footer::new(BlockHandle::new(0, 100), BlockHandle::new(100, 200))
            .with_trie(BlockHandle::new(300, 50));
        let encoded = footer.encode();

        // Act
        let decoded = Footer::decode(&encoded).unwrap();

        // Assert
        assert_eq!(decoded.trie_handle, Some(BlockHandle::new(300, 50)));
    }

    #[test]
    fn should_footer_roundtrip_with_all_fields() {
        // Arrange
        let original = Footer::new(BlockHandle::new(1000, 2000), BlockHandle::new(3000, 4000))
            .with_trie(BlockHandle::new(5000, 6000));

        // Act
        let encoded = original.encode();
        let decoded = Footer::decode(&encoded).unwrap();

        // Assert
        assert_eq!(decoded.meta_index_handle, original.meta_index_handle);
        assert_eq!(decoded.index_handle, original.index_handle);
        assert_eq!(decoded.trie_handle, original.trie_handle);
    }

    #[test]
    fn should_footer_reject_invalid_magic() {
        // Arrange
        let mut data = vec![0u8; 56];
        // Wrong magic at end
        data[48..56].copy_from_slice(&0xffff_ffff_ffff_ffff_u64.to_le_bytes());

        // Act
        let result = Footer::decode(&data);

        // Assert
        assert!(result.is_err());
    }

    #[test]
    fn should_footer_reject_short_data() {
        // Arrange
        let data = vec![0u8; 40];

        // Act
        let result = Footer::decode(&data);

        // Assert
        assert!(result.is_err());
    }

    #[test]
    fn should_footer_builder_pattern() {
        // Arrange
        // (setup)
        let footer = Footer::new(BlockHandle::new(0, 100), BlockHandle::new(100, 200))
            .with_trie(BlockHandle::new(200, 50));

        // Act
        // (none)

        // Assert
        assert_eq!(footer.meta_index_handle.offset, 0);
        assert_eq!(footer.index_handle.offset, 100);
        assert!(footer.trie_handle.is_some());
    }

    #[test]
    fn should_footer_with_none_trie_handle_encodes_zeros() {
        // Arrange
        let footer = Footer::new(BlockHandle::new(100, 200), BlockHandle::new(300, 400));
        let encoded = footer.encode();

        // Act
        // (none)

        // Assert - Trie handle bytes should be zero (None means 0,0)
        assert_eq!(encoded[32..40], vec![0u8; 8][..]);
        assert_eq!(encoded[40..48], vec![0u8; 8][..]);
    }

    // =========== RangeTombstone Tests ===========

    #[test]
    fn should_create_range_tombstone() {
        // Arrange
        // (no setup)

        // Act
        let rt = RangeTombstone::new(b"start".to_vec(), b"end".to_vec(), 100);

        // Assert
        assert_eq!(rt.start, b"start");
        assert_eq!(rt.end, b"end");
        assert_eq!(rt.seq, 100);
    }

    #[test]
    fn should_range_tombstone_cover_key_within_range() {
        // Arrange
        let rt = RangeTombstone::new(b"a".to_vec(), b"z".to_vec(), 10);

        // Act
        // Assert
        assert!(rt.covers(b"m"));
    }

    #[test]
    fn should_range_tombstone_cover_start_boundary() {
        // Arrange
        let rt = RangeTombstone::new(b"a".to_vec(), b"z".to_vec(), 10);

        // Act
        // (none)

        // Assert - Start is inclusive
        assert!(rt.covers(b"a"));
    }

    #[test]
    fn should_range_tombstone_not_cover_end_boundary() {
        // Arrange
        let rt = RangeTombstone::new(b"a".to_vec(), b"z".to_vec(), 10);

        // Act
        // (none)

        // Assert - End is exclusive
        assert!(!rt.covers(b"z"));
    }

    #[test]
    fn should_range_tombstone_not_cover_key_below_start() {
        // Arrange
        let rt = RangeTombstone::new(b"a".to_vec(), b"z".to_vec(), 10);

        // Act
        // (none)

        // Assert
        assert!(!rt.covers(b"0"));
    }

    #[test]
    fn should_range_tombstone_not_cover_key_above_end() {
        // Arrange
        let rt = RangeTombstone::new(b"a".to_vec(), b"z".to_vec(), 10);

        // Act
        // (none)

        // Assert
        assert!(!rt.covers(b"zz"));
    }

    #[test]
    fn should_range_tombstone_with_empty_range() {
        // Arrange
        let rt = RangeTombstone::new(b"a".to_vec(), b"a".to_vec(), 10);

        // Act
        // (none)

        // Assert - [a, a) is empty
        assert!(!rt.covers(b"a"));
    }

    #[test]
    fn should_range_tombstone_with_binary_keys() {
        // Arrange
        let rt = RangeTombstone::new(vec![0u8, 100u8], vec![255u8], 5);

        // Act
        // (none)

        // Assert
        assert!(rt.covers(&[0u8, 200u8]));
        assert!(!rt.covers(&[255u8]));
    }

    #[test]
    fn should_range_tombstone_single_key_range() {
        // Arrange
        let rt = RangeTombstone::new(b"key".to_vec(), b"kez".to_vec(), 1);

        // Act
        // (none)

        // Assert
        assert!(rt.covers(b"key"));
        assert!(rt.covers(b"key_data"));
        assert!(!rt.covers(b"kez"));
    }

    #[test]
    fn should_range_tombstone_clone() {
        // Arrange
        let rt1 = RangeTombstone::new(b"a".to_vec(), b"z".to_vec(), 10);

        // Act
        let rt2 = rt1.clone();

        // Assert
        assert_eq!(rt1.start, rt2.start);
        assert_eq!(rt1.end, rt2.end);
        assert_eq!(rt1.seq, rt2.seq);
    }

    #[test]
    fn should_reject_declared_range_tombstone_count_larger_than_remaining_bytes() {
        // Arrange
        let encoded = u32::MAX.to_le_bytes();

        // Act
        let result = decode_range_tombstones(&encoded);

        // Assert
        assert!(matches!(
            result,
            Err(crate::common::MidgeError::Corruption(_))
        ));
    }

    // =========== SstEntry Tests ===========

    #[test]
    fn should_create_sst_entry_with_value() {
        // Arrange
        // (no setup)

        // Act
        let entry = SstEntry::new(b"key".to_vec(), Some(Bytes::from("value")), 100, 0, None);

        // Assert
        assert_eq!(entry.key, b"key");
        assert_eq!(entry.value.unwrap(), Bytes::from("value"));
        assert_eq!(entry.sequence, 100);
        assert_eq!(entry.op_type, 0);
        assert_eq!(entry.expiration, None);
    }

    #[test]
    fn should_create_sst_entry_without_value() {
        // Arrange
        // (no setup)

        // Act
        let entry = SstEntry::new(b"key".to_vec(), None, 100, 2, None);

        // Assert
        assert!(entry.value.is_none());
    }

    #[test]
    fn should_identify_tombstone_entry_when_op_type_is_2() {
        // Arrange
        let entry = SstEntry::new(b"key".to_vec(), None, 1, 2, None);

        // Act
        // (none)

        // Assert
        assert!(entry.is_tombstone());
    }

    #[test]
    fn should_identify_non_tombstone_for_op_type_0_put() {
        // Arrange
        let entry = SstEntry::new(b"key".to_vec(), Some(Bytes::from("val")), 1, 0, None);

        // Act
        // (none)

        // Assert
        assert!(!entry.is_tombstone());
    }

    #[test]
    fn should_identify_non_tombstone_for_op_type_1_insert() {
        // Arrange
        let entry = SstEntry::new(b"key".to_vec(), Some(Bytes::from("val")), 1, 1, None);

        // Act
        // (none)

        // Assert
        assert!(!entry.is_tombstone());
    }

    #[test]
    fn should_entry_not_expired_when_no_expiration() {
        // Arrange
        let entry = SstEntry::new(b"key".to_vec(), Some(Bytes::from("val")), 1, 0, None);

        // Act
        // (none)

        // Assert
        assert!(!entry.is_expired(u64::MAX));
    }

    #[test]
    fn should_entry_not_expired_when_current_time_before_expiration() {
        // Arrange
        let entry = SstEntry::new(b"key".to_vec(), Some(Bytes::from("val")), 1, 0, Some(1000));

        // Act
        // (none)

        // Assert
        assert!(!entry.is_expired(999));
    }

    #[test]
    fn should_entry_expired_when_current_time_equals_expiration() {
        // Arrange
        let entry = SstEntry::new(b"key".to_vec(), Some(Bytes::from("val")), 1, 0, Some(1000));

        // Act
        // (none)

        // Assert
        assert!(entry.is_expired(1000));
    }

    #[test]
    fn should_entry_expired_when_current_time_after_expiration() {
        // Arrange
        let entry = SstEntry::new(b"key".to_vec(), Some(Bytes::from("val")), 1, 0, Some(1000));

        // Act
        // (none)

        // Assert
        assert!(entry.is_expired(1001));
    }

    #[test]
    fn should_entry_handle_zero_expiration() {
        // Arrange
        let entry = SstEntry::new(b"key".to_vec(), Some(Bytes::from("val")), 1, 0, Some(0));

        // Act
        // (none)

        // Assert
        assert!(entry.is_expired(0));
    }

    #[test]
    fn should_entry_handle_max_expiration() {
        // Arrange
        let entry = SstEntry::new(
            b"key".to_vec(),
            Some(Bytes::from("val")),
            1,
            0,
            Some(u64::MAX),
        );

        // Act
        // (none)

        // Assert
        assert!(!entry.is_expired(u64::MAX - 1));
    }

    #[test]
    fn should_entry_clone() {
        // Arrange
        let entry1 = SstEntry::new(
            b"key".to_vec(),
            Some(Bytes::from("value")),
            100,
            0,
            Some(500),
        );

        // Act
        let entry2 = entry1.clone();

        // Assert
        assert_eq!(entry1.key, entry2.key);
        assert_eq!(entry1.value, entry2.value);
        assert_eq!(entry1.sequence, entry2.sequence);
    }

    #[test]
    fn should_entry_with_large_sequence() {
        // Arrange
        // (setup)
        let entry = SstEntry::new(b"key".to_vec(), Some(Bytes::from("val")), u64::MAX, 0, None);

        // Act
        // (none)

        // Assert
        assert_eq!(entry.sequence, u64::MAX);
    }

    #[test]
    fn should_entry_with_binary_key() {
        // Arrange
        let binary_key = vec![0u8, 1u8, 255u8];

        // Act
        let entry = SstEntry::new(binary_key.clone(), Some(Bytes::from("val")), 1, 0, None);

        // Assert
        assert_eq!(entry.key, binary_key);
    }

    // =========== KeyState Tests ===========

    #[test]
    fn should_create_key_state_absent() {
        // Arrange
        // (no setup)

        // Act
        let state = KeyState::Absent;

        // Assert
        assert!(matches!(state, KeyState::Absent), "expected Absent state");
    }

    #[test]
    fn should_create_key_state_tombstone() {
        // Arrange
        // (no setup)

        // Act
        let state = KeyState::Tombstone(100);

        // Assert
        assert!(
            matches!(state, KeyState::Tombstone(100)),
            "expected Tombstone(100)"
        );
    }

    #[test]
    fn should_create_key_state_value() {
        // Arrange
        // (no setup)

        // Act
        let state = KeyState::Value(Bytes::from("val"), 100, Some(500), 0);

        // Assert
        assert!(
            matches!(state, KeyState::Value(_, 100, Some(500), 0)),
            "expected Value state with correct fields"
        );
    }

    #[test]
    fn should_format_key_state_absent() {
        // Arrange
        let state = KeyState::Absent;

        // Act
        let formatted = format!("{state}");

        // Assert
        assert_eq!(formatted, "Absent");
    }

    #[test]
    fn should_format_key_state_tombstone() {
        // Arrange
        let state = KeyState::Tombstone(42);

        // Act
        let formatted = format!("{state}");

        // Assert
        assert!(formatted.contains("Tombstone"));
        assert!(formatted.contains("42"));
    }

    #[test]
    fn should_format_key_state_value() {
        // Arrange
        let state = KeyState::Value(Bytes::from("val"), 100, Some(500), 0);

        // Act
        let formatted = format!("{state}");

        // Assert
        assert!(formatted.contains("Value"));
        assert!(formatted.contains("100"));
    }

    #[test]
    fn should_key_state_clone() {
        // Arrange
        let state1 = KeyState::Value(Bytes::from("val"), 100, Some(500), 0);

        // Act
        let state2 = state1.clone();

        // Assert
        assert_eq!(state1.clone(), state2, "cloned state should be equal");
    }

    #[test]
    fn should_key_state_value_with_zero_expiration() {
        // Arrange
        // (no setup)

        // Act
        let state = KeyState::Value(Bytes::from("val"), 1, Some(0), 0);

        // Assert
        assert!(
            matches!(state, KeyState::Value(_, _, Some(0), _)),
            "expected Value state with zero expiration"
        );
    }

    #[test]
    fn should_key_state_value_with_max_sequence() {
        // Arrange
        // (no setup)

        // Act
        let state = KeyState::Value(Bytes::from("val"), u64::MAX, None, 0);

        // Assert
        assert!(
            matches!(state, KeyState::Value(_, u64::MAX, None, _)),
            "expected Value state with max sequence"
        );
    }

    #[test]
    fn should_key_state_tombstone_with_zero_sequence() {
        // Arrange
        // (no setup)

        // Act
        let state = KeyState::Tombstone(0);

        // Assert
        assert!(
            matches!(state, KeyState::Tombstone(0)),
            "expected Tombstone(0)"
        );
    }
}
