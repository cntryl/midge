//! Compact, borrowed view of a decoded V5 sparse index (#1031).
//!
//! The index block already stores every entry as `[key_len: u32][key]
//! [offset: u64][size: u64]`. Readers keep one exact copy of that block plus
//! one `u32` entry offset, borrowing keys and decoding handles on access,
//! instead of a heap allocation per block key. Each entry records its key's
//! start and length so a binary-search probe is one slice of that copy.

use super::{BlockHandle, SstFileIo};
use crate::common::{MidgeError, MidgeResult};
use bytes::Bytes;

const KEY_LEN_BYTES: usize = 4;
const HANDLE_BYTES: usize = 16;

/// Validated block index entries, ordered by key and file offset.
#[derive(Debug)]
pub(crate) struct BlockIndex {
    bytes: Bytes,
    /// Key start and key length per entry; the handle follows the key.
    entries: Vec<(u32, u32)>,
}

impl BlockIndex {
    /// Validate `data` as a complete sparse index for a block region ending
    /// at `block_region_end`, rejecting truncation, invalid handles, and
    /// entries that are not key-ordered and non-overlapping.
    pub(super) fn decode(data: &[u8], block_region_end: u64) -> MidgeResult<Self> {
        // Entry offsets are `u32`; the block frame already bounds the length.
        u32::try_from(data.len())
            .map_err(|_| MidgeError::Corruption("SST index exceeds 4 GiB".into()))?;
        let mut entries = Vec::new();
        let mut offset = 0_usize;
        let mut previous: Option<(&[u8], BlockHandle)> = None;
        while offset < data.len() {
            let start = offset;
            if offset
                .checked_add(KEY_LEN_BYTES + HANDLE_BYTES)
                .is_none_or(|end| end > data.len())
            {
                return Err(MidgeError::Corruption(
                    "SST index has a truncated entry header".into(),
                ));
            }
            let key_len = u32::from_le_bytes(
                data[offset..offset + KEY_LEN_BYTES]
                    .try_into()
                    .expect("four-byte key length"),
            ) as usize;
            offset += KEY_LEN_BYTES;
            let key_end = offset.checked_add(key_len).ok_or_else(|| {
                MidgeError::Corruption("SST index key length overflows the block".into())
            })?;
            if key_end > data.len() {
                return Err(MidgeError::Corruption(
                    "SST index has a truncated key".into(),
                ));
            }
            let key = &data[offset..key_end];
            offset = key_end;
            if offset
                .checked_add(HANDLE_BYTES)
                .is_none_or(|end| end > data.len())
            {
                return Err(MidgeError::Corruption(
                    "SST index has a truncated block handle".into(),
                ));
            }
            let handle = decode_handle(&data[offset..offset + HANDLE_BYTES]);
            offset += HANDLE_BYTES;
            SstFileIo::validate_block_handle(handle, block_region_end, "data")?;
            if previous.is_some_and(|(previous_key, previous_handle)| {
                previous_key > key
                    || previous_handle
                        .offset
                        .checked_add(previous_handle.size)
                        .is_none_or(|end| end > handle.offset)
            }) {
                return Err(MidgeError::Corruption(
                    "SST index entries are not key-ordered and non-overlapping".into(),
                ));
            }
            previous = Some((key, handle));
            entries.push((
                u32::try_from(start + KEY_LEN_BYTES).expect("bounded by the index length"),
                u32::try_from(key_len).expect("bounded by the index length"),
            ));
        }
        entries.shrink_to_fit();
        Ok(Self {
            // An exact copy: the decoded block may be a slice of a larger
            // read buffer that the reader must not retain.
            bytes: Bytes::copy_from_slice(data),
            entries,
        })
    }

    pub(crate) fn len(&self) -> usize {
        self.entries.len()
    }

    pub(crate) fn is_empty(&self) -> bool {
        self.entries.is_empty()
    }

    /// The first key of block `index`. Panics when `index` is out of range.
    pub(crate) fn key(&self, index: usize) -> &[u8] {
        let (start, len) = self.entries[index];
        &self.bytes[start as usize..(start + len) as usize]
    }

    /// The handle of block `index`. Panics when `index` is out of range.
    pub(crate) fn handle(&self, index: usize) -> BlockHandle {
        let (start, len) = self.entries[index];
        let start = (start + len) as usize;
        decode_handle(&self.bytes[start..start + HANDLE_BYTES])
    }

    pub(crate) fn get(&self, index: usize) -> Option<(&[u8], BlockHandle)> {
        (index < self.len()).then(|| (self.key(index), self.handle(index)))
    }

    pub(crate) fn iter(&self) -> impl Iterator<Item = (&[u8], BlockHandle)> + '_ {
        (0..self.len()).map(|index| (self.key(index), self.handle(index)))
    }

    /// Bytes this index keeps allocated while a reader retains it.
    pub(crate) fn retained_bytes(&self) -> usize {
        std::mem::size_of::<Self>()
            .saturating_add(self.bytes.len())
            .saturating_add(
                self.entries
                    .capacity()
                    .saturating_mul(std::mem::size_of::<(u32, u32)>()),
            )
    }
}

fn decode_handle(bytes: &[u8]) -> BlockHandle {
    BlockHandle::new(
        u64::from_le_bytes(bytes[..8].try_into().expect("eight-byte offset")),
        u64::from_le_bytes(bytes[8..16].try_into().expect("eight-byte size")),
    )
}
