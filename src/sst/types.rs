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

/// Footer stored at end of SST file
#[derive(Debug, Clone)]
pub struct Footer {
    pub meta_index_handle: BlockHandle,
    pub index_handle: BlockHandle,
}

impl Footer {
    pub fn new(meta_index_handle: BlockHandle, index_handle: BlockHandle) -> Self {
        Self {
            meta_index_handle,
            index_handle,
        }
    }

    /// Encode footer to exactly 48 bytes (compatible with RocksDB format)
    pub fn encode(&self) -> Vec<u8> {
        let mut buf = vec![0u8; 48];
        // Store handles as fixed 16 bytes each
        // meta_index: offset (8) + size (8)
        buf[0..8].copy_from_slice(&self.meta_index_handle.offset.to_le_bytes());
        buf[8..16].copy_from_slice(&self.meta_index_handle.size.to_le_bytes());
        // index: offset (8) + size (8)
        buf[16..24].copy_from_slice(&self.index_handle.offset.to_le_bytes());
        buf[24..32].copy_from_slice(&self.index_handle.size.to_le_bytes());
        // Remaining bytes for future expansion
        buf
    }

    /// Decode footer from 48 bytes
    pub fn decode(data: &[u8]) -> crate::common::MidgeResult<Self> {
        if data.len() < 48 {
            return Err(crate::common::MidgeError::Corruption(
                "Footer too short".into(),
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

        Ok(Footer {
            meta_index_handle: BlockHandle::new(meta_offset, meta_size),
            index_handle: BlockHandle::new(idx_offset, idx_size),
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
        assert_eq!(encoded.len(), 48);
        assert_eq!(decoded.meta_index_handle.offset, 0);
        assert_eq!(decoded.meta_index_handle.size, 100);
        assert_eq!(decoded.index_handle.offset, 100);
        assert_eq!(decoded.index_handle.size, 200);
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
