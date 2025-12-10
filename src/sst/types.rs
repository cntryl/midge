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
pub const SST_FOOTER_MAGIC: u64 = 0xdb4775248b80fb57;

/// Footer stored at end of SST file
#[derive(Debug, Clone)]
pub struct Footer {
    pub meta_index_handle: BlockHandle,
    pub index_handle: BlockHandle,
    pub trie_handle: Option<BlockHandle>,
}

impl Footer {
    pub fn new(meta_index_handle: BlockHandle, index_handle: BlockHandle) -> Self {
        Self {
            meta_index_handle,
            index_handle,
            trie_handle: None,
        }
    }

    pub fn with_trie(mut self, trie_handle: BlockHandle) -> Self {
        self.trie_handle = Some(trie_handle);
        self
    }

    /// Encode footer to exactly 48 bytes (compatible with RocksDB format)
    /// Layout: 
    ///   [meta_index_handle: 16 bytes] 
    ///   [index_handle: 16 bytes] 
    ///   [trie_handle: 16 bytes (optional, 0 if None)]
    ///   [magic: 8 bytes]
    /// Total: 56 bytes (extended from 48 for trie support)
    pub fn encode(&self) -> Vec<u8> {
        let mut buf = vec![0u8; 56];
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
        // Magic number at end [48..56]
        buf[48..56].copy_from_slice(&SST_FOOTER_MAGIC.to_le_bytes());
        buf
    }

    /// Decode footer from 48 or 56 bytes (backward compatible)
    pub fn decode(data: &[u8]) -> crate::common::MidgeResult<Self> {
        if data.len() < 48 {
            return Err(crate::common::MidgeError::Corruption(
                "Footer too short".into(),
            ));
        }

        // Check if this is old format (48 bytes) or new format (56 bytes)
        let is_extended = data.len() >= 56;
        
        // Validate magic number
        let magic_offset = if is_extended { 48 } else { 40 };
        let magic = u64::from_le_bytes([
            data[magic_offset], data[magic_offset+1], data[magic_offset+2], data[magic_offset+3],
            data[magic_offset+4], data[magic_offset+5], data[magic_offset+6], data[magic_offset+7],
        ]);
        if magic != SST_FOOTER_MAGIC {
            return Err(crate::common::MidgeError::Corruption(
                format!("Invalid footer magic: expected 0x{:016x}, got 0x{:016x}", SST_FOOTER_MAGIC, magic),
            ));
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

        // Read trie handle if extended format
        let trie_handle = if is_extended {
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

        Ok(Footer {
            meta_index_handle: BlockHandle::new(meta_offset, meta_size),
            index_handle: BlockHandle::new(idx_offset, idx_size),
            trie_handle,
        })
    }
}

/// Range tombstone for covering key ranges
#[derive(Debug, Clone)]
pub struct RangeTombstone {
    pub start: Vec<u8>,
    pub end: Vec<u8>,
    pub seq: u64,
}

impl RangeTombstone {
    pub fn new(start: Vec<u8>, end: Vec<u8>, seq: u64) -> Self {
        Self { start, end, seq }
    }

    /// Check if a key is covered by this range tombstone
    pub fn covers(&self, key: &[u8]) -> bool {
        key >= self.start.as_slice() && key < self.end.as_slice()
    }
}

/// Parsed entry from SST block
#[derive(Debug, Clone)]
pub struct SstEntry {
    pub key: Vec<u8>,
    pub value: Option<Bytes>,
    pub sequence: u64,
    pub op_type: u8, // 0=Put, 1=Insert, 2=Delete, 3=Merge
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
#[derive(Debug, Clone)]
pub enum KeyState {
    Absent,
    Tombstone(u64),                     // sequence number
    Value(Bytes, u64, Option<u64>, u8), // value, seq, expiration, op_type
}

impl fmt::Display for KeyState {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            KeyState::Absent => write!(f, "Absent"),
            KeyState::Tombstone(seq) => write!(f, "Tombstone(seq={})", seq),
            KeyState::Value(_, seq, exp, op) => {
                write!(f, "Value(seq={}, exp={:?}, op={})", seq, exp, op)
            }
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn should_roundtrip_footer_when_encoding_and_decoding() {
        // Arrange
        let footer = Footer::new(BlockHandle::new(0, 100), BlockHandle::new(100, 200));

        // Act
        let encoded = footer.encode();
        let decoded = Footer::decode(&encoded).unwrap();

        // Assert
        assert_eq!(encoded.len(), 56); // New format with trie support
        assert_eq!(decoded.meta_index_handle.offset, 0);
        assert_eq!(decoded.meta_index_handle.size, 100);
        assert_eq!(decoded.index_handle.offset, 100);
        assert_eq!(decoded.index_handle.size, 200);
        assert_eq!(decoded.trie_handle, None);
    }

    #[test]
    fn should_roundtrip_footer_with_trie() {
        // Arrange
        let footer = Footer::new(BlockHandle::new(0, 100), BlockHandle::new(100, 200))
            .with_trie(BlockHandle::new(300, 50));

        // Act
        let encoded = footer.encode();
        let decoded = Footer::decode(&encoded).unwrap();

        // Assert
        assert_eq!(encoded.len(), 56);
        assert_eq!(decoded.trie_handle, Some(BlockHandle::new(300, 50)));
    }

    #[test]
    fn should_identify_keys_within_range_when_checking_coverage() {
        // Arrange
        let rt = RangeTombstone::new(b"a".to_vec(), b"z".to_vec(), 10);

        // Act
        let covers_m = rt.covers(b"m");
        let covers_z = rt.covers(b"z");
        let covers_0 = rt.covers(b"0");

        // Assert
        assert!(covers_m);
        assert!(!covers_z);
        assert!(!covers_0);
    }

    #[test]
    fn should_identify_tombstones_when_checking_entry_type() {
        // Arrange
        let entry = SstEntry::new(b"key".to_vec(), None, 1, 2, None);
        let entry2 = SstEntry::new(b"key".to_vec(), Some(Bytes::from("val")), 1, 0, None);

        // Act
        let is_tombstone1 = entry.is_tombstone();
        let is_tombstone2 = entry2.is_tombstone();

        // Assert
        assert!(is_tombstone1);
        assert!(!is_tombstone2);
    }
}
