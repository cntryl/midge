use bytes::Bytes;
use std::fs::OpenOptions;
use std::io::{Read, Seek, SeekFrom};
use std::path::PathBuf;

use crate::error::MidgeResult;
use crate::sst::format::BlockHandle;

use super::utils::decode_data_block;

pub struct SstRangeIter {
    path: PathBuf,
    blocks: Vec<BlockHandle>,
    blk_idx: usize,
    data: Option<Vec<u8>>,
    cursor: usize,
    entries_end: usize,
    last_key: Vec<u8>,
    start: Option<Vec<u8>>,
    end: Option<Vec<u8>>,
    use_internal_keys: bool,
}

impl SstRangeIter {
    pub(super) fn new(
        path: PathBuf,
        blocks: Vec<BlockHandle>,
        start: Option<Vec<u8>>,
        end: Option<Vec<u8>>,
        use_internal_keys: bool,
    ) -> Self {
        Self {
            path,
            blocks,
            blk_idx: 0,
            data: None,
            cursor: 0,
            entries_end: 0,
            last_key: Vec::new(),
            start,
            end,
            use_internal_keys,
        }
    }

    fn load_next_block(&mut self) -> MidgeResult<bool> {
        if self.blk_idx >= self.blocks.len() {
            return Ok(false);
        }
        let bh = self.blocks[self.blk_idx];
        self.blk_idx += 1;
        let mut file = OpenOptions::new().read(true).open(&self.path)?;
        file.seek(SeekFrom::Start(bh.offset))?;
        let mut block_data = vec![0u8; bh.size as usize];
        file.read_exact(&mut block_data)?;
        let block = decode_data_block(&block_data)?;
        let data: Vec<u8> = block.data.to_vec();
        let len = data.len();
        if len < 8 {
            self.data = None;
            return Ok(true);
        }
        let num_restarts =
            u32::from_le_bytes([data[len - 4], data[len - 3], data[len - 2], data[len - 1]])
                as usize;
        let restarts_start = len - 4 - (num_restarts * 4);
        self.entries_end = restarts_start;
        self.cursor = 0;
        self.last_key.clear();
        self.data = Some(data);
        Ok(true)
    }

    fn parse_next_entry(&mut self) -> Option<(Bytes, Bytes)> {
        use crate::common::tlv::{tags, TlvReader};

        loop {
            // Load next block if needed
            if self.data.is_none() {
                if !(self.load_next_block().ok()?) {
                    return None;
                }
                if self.data.is_none() {
                    continue;
                }
            }

            // Clone the data to avoid borrow checker issues
            // SAFETY: checked is_none() above, so data is Some
            let data_clone = self.data.as_ref().expect("data checked above").clone();
            if self.cursor >= self.entries_end {
                self.data = None;
                continue;
            }

            // Use TLV reader to parse the entry
            let mut reader = TlvReader::new(&data_clone[self.cursor..self.entries_end]);
            let mut shared_len: Option<u32> = None;
            let mut key_delta_vec: Option<Vec<u8>> = None;
            let mut value_vec: Option<Vec<u8>> = None;
            let mut entry_type = 0u8;
            let mut entry_complete = false;
            let mut bytes_consumed = 0;
            let mut failed = false;

            while let Some((tag, tag_data)) = reader.next() {
                bytes_consumed = reader.position();
                match tag {
                    tags::SHARED_PREFIX_LEN => {
                        shared_len = Some(match crate::common::tlv::parse_varint32_from_slice(tag_data) {
                            Ok(v) => v,
                            Err(_) => {
                                failed = true;
                                break;
                            }
                        });
                    }
                    tags::KEY_DELTA => {
                        key_delta_vec = Some(tag_data.to_vec());
                        if self.use_internal_keys {
                            entry_complete = true;
                            break;
                        }
                    }
                    tags::VALUE => {
                        value_vec = Some(tag_data.to_vec());
                    }
                    tags::ENTRY_TYPE if tag_data.len() == 1 => {
                        entry_type = tag_data[0];
                        if !self.use_internal_keys {
                            entry_complete = true;
                            break;
                        }
                    }
                    _ => {}
                }
            }

            if failed || !entry_complete || shared_len.is_none() || key_delta_vec.is_none() {
                self.data = None;
                continue;
            }

            // Reconstruct the key
            // SAFETY: checked is_none() above, so both are Some
            let sl = shared_len.expect("shared_len checked above");
            let kd = key_delta_vec.expect("key_delta_vec checked above");
            let mut key = self.last_key.clone();
            key.truncate(sl as usize);
            key.extend_from_slice(&kd);

            // Handle internal-on-disk format vs non-internal format
            let tombstone = if self.use_internal_keys {
                if let Some((_u, _seq, t)) = crate::common::internal_key::decode_internal_key(&key) {
                    t
                } else {
                    false
                }
            } else {
                entry_type == 2
            };

            // Apply range filters
            if let Some(s) = &self.start {
                if key.as_slice() < s.as_slice() {
                    // Update last_key and cursor before skipping
                    self.last_key = key;
                    self.cursor += bytes_consumed;
                    continue;
                }
            }
            if let Some(e) = &self.end {
                if key.as_slice() >= e.as_slice() {
                    return None;
                }
            }

            // Skip tombstones
            if tombstone {
                // Update last_key and cursor before skipping
                self.last_key = key;
                self.cursor += bytes_consumed;
                continue;
            }

            // Update cursor (last_key will be updated below)
            self.cursor += bytes_consumed;

            // Return the entry
            let val = if let Some(v) = value_vec {
                Bytes::from(v)
            } else {
                Bytes::new()
            };

            // Save key for last_key, then move into Bytes
            self.last_key = key.clone();
            return Some((Bytes::from(key), val));
        }
    }
}

impl Iterator for SstRangeIter {
    type Item = (Bytes, Bytes);

    fn next(&mut self) -> Option<Self::Item> {
        self.parse_next_entry()
    }
}
