use crate::error::MidgeResult;
use crate::sst::bloom::BloomFilter;
use crate::sst::encoding::{decode, TlvBlockIterator};
use crate::sst::format::{Block, BlockHandle, Footer};
use crate::sst::range_tombstone::is_covered_by_range_tombstone;
use crate::sst::reader_common::{
    parse_key_at_offset, read_data_block_from_bytes, read_data_block_from_bytes_paranoid,
    search_data_block as common_search_data_block, should_skip_key, SstMetadata,
};
use crate::sst::sparse_index::SparseIndex;
use crate::sst::traits::{KeyState, RangeTombstone, SstStateReader};
use bytes::Bytes;

use super::MemSstData;

/// In-memory SST reader
pub struct SstMemReader {
    data: MemSstData,
    _footer: Footer,
    sparse_index: SparseIndex,
    bloom_filter: Option<BloomFilter>,
    range_tombstones: Vec<RangeTombstone>,
    use_internal_keys: bool,
    paranoid_checksums: bool,
}

impl SstMemReader {
    pub(crate) fn from_bytes(raw: Vec<u8>) -> MidgeResult<Self> {
        Self::from_bytes_with_paranoid(raw, false)
    }

    /// Create reader with paranoid checksum verification enabled
    pub(crate) fn from_bytes_with_paranoid(
        raw: Vec<u8>,
        paranoid_checksums: bool,
    ) -> MidgeResult<Self> {
        // Use common metadata parsing logic
        let metadata = SstMetadata::from_bytes(&raw)?;

        Ok(Self {
            data: MemSstData { raw },
            _footer: metadata.footer,
            sparse_index: metadata.sparse_index,
            bloom_filter: metadata.bloom_filter,
            range_tombstones: metadata.range_tombstones,
            use_internal_keys: metadata.use_internal_keys,
            paranoid_checksums,
        })
    }

    fn read_data_block(&self, handle: BlockHandle) -> MidgeResult<Block> {
        let off = handle.offset as usize;
        let sz = handle.size as usize;
        let raw = &self.data.raw[off..off + sz];
        if self.paranoid_checksums {
            read_data_block_from_bytes_paranoid(raw, true)
        } else {
            read_data_block_from_bytes(raw)
        }
    }

    fn search_data_block(&self, data: &[u8], target_key: &[u8]) -> MidgeResult<Option<Bytes>> {
        common_search_data_block(data, target_key, self.use_internal_keys)
    }

    /// Snapshot-aware point lookup. Returns value only if entry's seq <= snapshot and not tombstone.
    pub fn get_at(&self, key: &[u8], snapshot_seq: u64) -> MidgeResult<Option<Bytes>> {
        // Early-out if bloom filter or range tombstones indicate key is not present
        if should_skip_key(
            &self.bloom_filter,
            &self.range_tombstones,
            key,
            snapshot_seq,
        ) {
            return Ok(None);
        }
        if let Some(bh) = self.sparse_index.find_block(key) {
            let blk = self.read_data_block(*bh)?;
            let iter = TlvBlockIterator::new(&blk.data);

            for result in iter {
                let (raw_key, value_opt, seq, entry_type, _expiration) = result?;

                // Move raw_key into stored key to avoid extra clones. Use a borrowed slice for comparisons.
                let mut actual_key = raw_key;
                let mut _key_slice: &[u8] = &actual_key;
                let mut actual_seq = seq;
                let mut tomb = entry_type == 2;

                if self.use_internal_keys {
                    if let Some((user, s, t)) =
                        crate::common::internal_key::decode_internal_key(&actual_key)
                    {
                        actual_key = user;
                        _key_slice = &actual_key;
                        actual_seq = s;
                        tomb = t;
                    }
                }

                if _key_slice == key {
                    // Snapshot isolation: only see writes with seq < snapshot_seq
                    if actual_seq < snapshot_seq && !tomb {
                        return Ok(value_opt.map(Bytes::copy_from_slice));
                    } else {
                        return Ok(None);
                    }
                }
                if _key_slice > key {
                    break;
                }
            }
        }
        Ok(None)
    }

    /// Get the serialized bloom filter bytes for this SST.
    /// Used for manifest caching to enable bloom pre-checks without opening SST.
    pub fn get_bloom_filter_bytes(&self) -> Option<Vec<u8>> {
        self.bloom_filter.as_ref().map(|bf| bf.encode().to_vec())
    }

    /// Checks if a key is within the specified range bounds.
    /// Range is [start, end) - inclusive start, exclusive end.
    #[inline]
    fn is_in_range(key: &[u8], start: Option<&[u8]>, end: Option<&[u8]>) -> bool {
        let within_start = start.is_none_or(|s| key >= s);
        let within_end = end.is_none_or(|e| key < e);
        within_start && within_end
    }

    /// Checks if an entry should be included in scan results.
    /// Returns true if an entry is visible at a snapshot:
    /// - Sequence number < snapshot sequence (snapshot isolation)
    /// - Not a tombstone
    /// - Not covered by a range tombstone
    #[inline]
    fn is_entry_visible(
        seq: u64,
        tomb: bool,
        key: &[u8],
        snapshot_seq: u64,
        range_tombstones: &[RangeTombstone],
    ) -> bool {
        seq < snapshot_seq
            && !tomb
            && !is_covered_by_range_tombstone(range_tombstones, key, snapshot_seq)
    }

    /// Snapshot-aware range scan. Filters entries with seq > snapshot and tombstones.
    pub fn scan_range_at(
        &self,
        start: Option<&[u8]>,
        end: Option<&[u8]>,
        snapshot_seq: u64,
    ) -> MidgeResult<Vec<(Bytes, Bytes)>> {
        use crate::common::tlv::{tags, TlvReader};

        let mut out = Vec::new();
        let entries = self.sparse_index.entries().to_vec();
        for en in entries {
            let blk = self.read_data_block(en.block_handle)?;
            let data = blk.data.as_ref();
            if data.len() < 8 {
                continue;
            }
            let n = u32::from_le_bytes([
                data[data.len() - 4],
                data[data.len() - 3],
                data[data.len() - 2],
                data[data.len() - 1],
            ]) as usize;
            let restarts_start = data.len() - 4 - n * 4;
            let version_offset = restarts_start.saturating_sub(1);
            let limit = version_offset;

            let reader = TlvReader::new(&data[..limit]);
            let mut current_key = Vec::new();
            let mut shared_len: Option<u32> = None;
            let mut key_delta: Option<&[u8]> = None;
            let mut value: Option<&[u8]> = None;
            let mut sequence: u64 = 0;
            let mut entry_type: u8 = 0;

            for (tag, tag_data) in reader {
                match tag {
                    tags::SHARED_PREFIX_LEN => {
                        // Process previous entry if complete
                        if let (Some(sl), Some(kd)) = (shared_len, key_delta) {
                            current_key.truncate(sl as usize);
                            current_key.extend_from_slice(kd);

                            // Decode key and extract metadata
                            let (key_slice, seq, tomb) = if self.use_internal_keys {
                                if let Some((u_key, sseq, t)) =
                                    crate::common::internal_key::decode_internal_key(&current_key)
                                {
                                    // Store decoded user key back into current_key to avoid another allocation
                                    current_key.clear();
                                    current_key.extend_from_slice(&u_key);
                                    (&current_key[..], sseq, t)
                                } else {
                                    (&current_key[..], sequence, entry_type == 2)
                                }
                            } else {
                                (&current_key[..], sequence, entry_type == 2)
                            };

                            // Check if entry is in range and visible
                            if Self::is_in_range(key_slice, start, end)
                                && Self::is_entry_visible(
                                    seq,
                                    tomb,
                                    key_slice,
                                    snapshot_seq,
                                    &self.range_tombstones,
                                )
                            {
                                let val = if let Some(v) = value {
                                    Bytes::copy_from_slice(v)
                                } else {
                                    Bytes::new()
                                };
                                out.push((Bytes::copy_from_slice(key_slice), val));
                            }
                        }

                        // Start new entry
                        shared_len = Some(crate::common::tlv::parse_varint32_from_slice(tag_data)?);
                        key_delta = None;
                        value = None;
                        sequence = 0;
                        entry_type = 0;
                    }
                    tags::KEY_DELTA => key_delta = Some(tag_data),
                    tags::VALUE => value = Some(tag_data),
                    tags::SEQUENCE => {
                        if tag_data.len() >= 8 {
                            sequence = u64::from_be_bytes([
                                tag_data[0],
                                tag_data[1],
                                tag_data[2],
                                tag_data[3],
                                tag_data[4],
                                tag_data[5],
                                tag_data[6],
                                tag_data[7],
                            ]);
                        }
                    }
                    tags::ENTRY_TYPE => {
                        if !tag_data.is_empty() {
                            entry_type = tag_data[0];
                        }
                    }
                    _ => {}
                }
            }

            // Process last entry
            if let (Some(sl), Some(kd)) = (shared_len, key_delta) {
                current_key.truncate(sl as usize);
                current_key.extend_from_slice(kd);

                // Decode key and extract metadata
                let (key_slice, seq, tomb) = if self.use_internal_keys {
                    if let Some((u_key, sseq, t)) =
                        crate::common::internal_key::decode_internal_key(&current_key)
                    {
                        // Store decoded user key back into current_key to avoid another allocation
                        current_key.clear();
                        current_key.extend_from_slice(&u_key);
                        (&current_key[..], sseq, t)
                    } else {
                        (&current_key[..], sequence, entry_type == 2)
                    }
                } else {
                    (&current_key[..], sequence, entry_type == 2)
                };

                // Check if entry is in range and visible (same logic as loop entries)
                if Self::is_in_range(key_slice, start, end)
                    && Self::is_entry_visible(
                        seq,
                        tomb,
                        key_slice,
                        snapshot_seq,
                        &self.range_tombstones,
                    )
                {
                    let val = if let Some(v) = value {
                        Bytes::copy_from_slice(v)
                    } else {
                        Bytes::new()
                    };
                    out.push((Bytes::copy_from_slice(key_slice), val));
                }
            }
        }
        out.sort_by(|a, b| a.0.cmp(&b.0));
        Ok(out)
    }

    fn get_state_internal(&self, key: &[u8]) -> MidgeResult<KeyState> {
        tracing::debug!(
            "DEBUG SST: Looking for key={} use_internal={} ",
            String::from_utf8_lossy(key),
            self.use_internal_keys
        );
        // Early-out if bloom filter or range tombstones indicate key is not present
        if should_skip_key(&self.bloom_filter, &self.range_tombstones, key, u64::MAX) {
            tracing::debug!("DEBUG SST: Bloom filter or tombstone says key not present");
            return Ok(KeyState::Absent);
        }
        if let Some(bh) = self.sparse_index.find_block(key) {
            tracing::debug!("DEBUG SST: Found block for key");
            let blk = self.read_data_block(*bh)?;
            let data = blk.data.as_ref();
            let len = data.len();
            if len < 8 {
                return Ok(KeyState::Absent);
            }

            // TLV format: version marker (1 byte) before restart array
            let num_restarts =
                u32::from_le_bytes([data[len - 4], data[len - 3], data[len - 2], data[len - 1]])
                    as usize;
            let restarts_start = len - 4 - num_restarts * 4;
            let version_offset = restarts_start.saturating_sub(1);
            let entries_end = version_offset;

            // Binary search restart points
            let mut left = 0usize;
            let mut right = num_restarts;
            while left < right {
                let mid = (left + right) / 2;
                let off = u32::from_le_bytes([
                    data[restarts_start + mid * 4],
                    data[restarts_start + mid * 4 + 1],
                    data[restarts_start + mid * 4 + 2],
                    data[restarts_start + mid * 4 + 3],
                ]) as usize;
                if let Ok(k) = parse_key_at_offset(data, off, entries_end, self.use_internal_keys) {
                    if k.as_slice() <= key {
                        left = mid + 1;
                    } else {
                        right = mid;
                    }
                } else {
                    break;
                }
            }

            let idx = if left > 0 { left - 1 } else { 0 };
            let start_offset = u32::from_le_bytes([
                data[restarts_start + idx * 4],
                data[restarts_start + idx * 4 + 1],
                data[restarts_start + idx * 4 + 2],
                data[restarts_start + idx * 4 + 3],
            ]) as usize;

            // Iterate from restart point using TLV parsing
            let mut cursor = start_offset;
            let mut last_key: Vec<u8> = Vec::new();

            while cursor < entries_end {
                let entry = match decode(data, cursor, entries_end) {
                    Ok(e) => e,
                    Err(_) => break,
                };
                cursor += entry.bytes_consumed;

                // Reconstruct full key
                let mut raw_key =
                    Vec::with_capacity(entry.shared_len as usize + entry.key_delta.len());
                raw_key.extend_from_slice(&last_key[..entry.shared_len as usize]);
                raw_key.extend_from_slice(entry.key_delta);
                last_key = raw_key.clone();

                if self.use_internal_keys {
                    let (k_user, seq, tomb) = if let Some((uk, s, t)) =
                        crate::common::internal_key::decode_internal_key(&raw_key)
                    {
                        (uk, s, t)
                    } else {
                        (raw_key.clone(), 0, false)
                    };
                    if k_user.as_slice() == key {
                        return Ok(if tomb {
                            KeyState::Tombstone(seq)
                        } else if let Some(val) = entry.value {
                            KeyState::Value(
                                Bytes::copy_from_slice(val),
                                seq,
                                entry.expiration,
                                entry.entry_type,
                            )
                        } else {
                            KeyState::Tombstone(seq)
                        });
                    }
                    if k_user.as_slice() > key {
                        break;
                    }
                } else {
                    // External keys with explicit metadata
                    let tomb = entry.entry_type == 2;
                    if raw_key.as_slice() == key {
                        return Ok(if tomb {
                            KeyState::Tombstone(entry.sequence)
                        } else if let Some(val) = entry.value {
                            KeyState::Value(
                                Bytes::copy_from_slice(val),
                                entry.sequence,
                                entry.expiration,
                                entry.entry_type,
                            )
                        } else {
                            KeyState::Tombstone(entry.sequence)
                        });
                    }
                    if raw_key.as_slice() > key {
                        break;
                    }
                }
            }
        }
        Ok(KeyState::Absent)
    }

    fn scan_range_state_internal(
        &self,
        start: Option<&[u8]>,
        end: Option<&[u8]>,
    ) -> MidgeResult<Vec<(Bytes, KeyState)>> {
        let mut out: Vec<(Bytes, KeyState)> = Vec::new();
        for en in self.sparse_index.entries().to_vec() {
            let blk = self.read_data_block(en.block_handle)?;
            let iter = TlvBlockIterator::new(&blk.data);

            for result in iter {
                let (raw_key, value_opt, seq, entry_type, expiration) = result?;

                // Move raw_key into stored_key to avoid an extra allocation
                let mut stored_key = raw_key;
                // Work with a borrowed slice pointing into stored_key for comparisons
                tracing::debug!("DEBUG SST: Found block for key");
                let mut key_slice: &[u8] = &stored_key;
                let mut actual_seq = seq;
                let mut tomb = entry_type == 2;

                if self.use_internal_keys {
                    if let Some((user, s, t)) =
                        crate::common::internal_key::decode_internal_key(&stored_key)
                    {
                        // Replace stored_key with decoded user key to avoid extra copies later
                        stored_key = user;
                        key_slice = &stored_key;
                        actual_seq = s;
                        tomb = t;
                    }
                }

                if let Some(s) = start {
                    if key_slice < s {
                        continue;
                    }
                }
                if let Some(e) = end {
                    if key_slice >= e {
                        continue;
                    }
                }

                if tomb {
                    out.push((Bytes::from(stored_key), KeyState::Tombstone(actual_seq)));
                } else if !is_covered_by_range_tombstone(
                    &self.range_tombstones,
                    key_slice,
                    u64::MAX,
                ) {
                    let val = value_opt.map(Bytes::copy_from_slice).unwrap_or_default();
                    out.push((
                        Bytes::from(stored_key),
                        KeyState::Value(val, actual_seq, expiration, entry_type),
                    ));
                }
            }
        }
        out.sort_by(|a, b| a.0.cmp(&b.0));
        Ok(out)
    }

    /// Snapshot-aware stateful get.
    fn get_state_at_internal(&self, key: &[u8], snapshot_seq: u64) -> MidgeResult<KeyState> {
        if let Some(bf) = &self.bloom_filter {
            if !bf.may_contain(key) {
                return Ok(KeyState::Absent);
            }
        }
        if is_covered_by_range_tombstone(&self.range_tombstones, key, snapshot_seq) {
            return Ok(KeyState::Tombstone(snapshot_seq));
        }
        if let Some(bh) = self.sparse_index.find_block(key) {
            let blk = self.read_data_block(*bh)?;
            let iter = TlvBlockIterator::new(&blk.data);

            for result in iter {
                let (raw_key, value_opt, seq, entry_type, expiration) = result?;

                // Move raw_key into actual_key to avoid clone; use key_slice for comparisons
                let mut actual_key = raw_key;
                let mut _key_slice: &[u8] = &actual_key;
                let mut actual_seq = seq;
                let mut tomb = entry_type == 2;

                if self.use_internal_keys {
                    if let Some((user, s, t)) =
                        crate::common::internal_key::decode_internal_key(&actual_key)
                    {
                        actual_key = user;
                        _key_slice = &actual_key;
                        actual_seq = s;
                        tomb = t;
                    }
                }

                if actual_key.as_slice() == key {
                    // Snapshot isolation: only see writes with seq < snapshot_seq
                    if actual_seq < snapshot_seq {
                        return Ok(if tomb {
                            KeyState::Tombstone(actual_seq)
                        } else {
                            let val = value_opt.map(Bytes::copy_from_slice).unwrap_or_default();
                            KeyState::Value(val, actual_seq, expiration, entry_type)
                        });
                    }
                    // seq >= snapshot_seq: this version is too new, continue searching
                    // for an older version of the same key
                }
                if actual_key.as_slice() > key {
                    break;
                }
            }
        }
        Ok(KeyState::Absent)
    }

    /// Snapshot-aware stateful range scan.
    fn scan_range_state_at_internal(
        &self,
        start: Option<&[u8]>,
        end: Option<&[u8]>,
        snapshot_seq: u64,
    ) -> MidgeResult<Vec<(Bytes, KeyState)>> {
        let mut out: Vec<(Bytes, KeyState)> = Vec::new();
        for en in self.sparse_index.entries().to_vec() {
            let blk = self.read_data_block(en.block_handle)?;
            let iter = TlvBlockIterator::new(&blk.data);

            for result in iter {
                let (raw_key, value_opt, seq, entry_type, expiration) = result?;

                // Move raw_key into actual_key to avoid extra clone; use key_slice for comparisons
                let mut actual_key = raw_key;
                let mut key_slice: &[u8] = &actual_key;
                let mut actual_seq = seq;
                let mut tomb = entry_type == 2;

                if self.use_internal_keys {
                    if let Some((user, s, t)) =
                        crate::common::internal_key::decode_internal_key(&actual_key)
                    {
                        actual_key = user;
                        key_slice = &actual_key;
                        actual_seq = s;
                        tomb = t;
                    }
                }

                if let Some(s) = start {
                    if key_slice < s {
                        continue;
                    }
                }
                if let Some(e) = end {
                    if key_slice >= e {
                        continue;
                    }
                }

                // Snapshot isolation: only see writes with seq < snapshot_seq
                if actual_seq < snapshot_seq {
                    let state = if tomb
                        || is_covered_by_range_tombstone(
                            &self.range_tombstones,
                            key_slice,
                            snapshot_seq,
                        ) {
                        KeyState::Tombstone(actual_seq)
                    } else {
                        let val = value_opt.map(Bytes::copy_from_slice).unwrap_or_default();
                        KeyState::Value(val, actual_seq, expiration, entry_type)
                    };
                    out.push((Bytes::from(actual_key), state));
                }
            }
        }
        out.sort_by(|a, b| a.0.cmp(&b.0));
        Ok(out)
    }
}

impl crate::sst::SstReader for SstMemReader {
    fn get(&self, key: &[u8]) -> MidgeResult<Option<Bytes>> {
        // Use Bloom to fast-fail if key definitely not present in this SST
        if let Some(bf) = &self.bloom_filter {
            if !bf.may_contain(key) {
                return Ok(None);
            }
        }
        if let Some(bh) = self.sparse_index.find_block(key) {
            let blk = self.read_data_block(*bh)?;
            return self.search_data_block(&blk.data, key);
        }
        Ok(None)
    }

    fn scan_range(
        &self,
        start: Option<&[u8]>,
        end: Option<&[u8]>,
    ) -> MidgeResult<Vec<(Bytes, Bytes)>> {
        let mut out = Vec::new();
        let entries = self.sparse_index.entries().to_vec();

        for en in entries {
            let blk = self.read_data_block(en.block_handle)?;
            let iter = TlvBlockIterator::new(&blk.data);

            for result in iter {
                let (key, value_opt, _seq, _entry_type, _expiration) = result?;

                // Apply range filter
                if let Some(s) = start {
                    if key.as_slice() < s {
                        continue;
                    }
                }
                if let Some(e) = end {
                    if key.as_slice() >= e {
                        continue;
                    }
                }

                let val = if let Some(v) = value_opt {
                    Bytes::copy_from_slice(v)
                } else {
                    Bytes::new()
                };

                out.push((Bytes::from(key), val));
            }
        }

        out.sort_by(|a, b| a.0.cmp(&b.0));
        Ok(out)
    }
}

// Implement stateful reader for the in-memory SST
impl SstStateReader for SstMemReader {
    fn get_state(&self, key: &[u8]) -> MidgeResult<KeyState> {
        self.get_state_internal(key)
    }
    fn scan_range_state(
        &self,
        start: Option<&[u8]>,
        end: Option<&[u8]>,
    ) -> MidgeResult<Vec<(Bytes, KeyState)>> {
        self.scan_range_state_internal(start, end)
    }

    fn get_state_at(&self, key: &[u8], snapshot_seq: u64) -> MidgeResult<KeyState> {
        self.get_state_at_internal(key, snapshot_seq)
    }
    fn scan_range_state_at(
        &self,
        start: Option<&[u8]>,
        end: Option<&[u8]>,
        snapshot_seq: u64,
    ) -> MidgeResult<Vec<(Bytes, KeyState)>> {
        self.scan_range_state_at_internal(start, end, snapshot_seq)
    }

    fn range_tombstones(&self) -> Vec<RangeTombstone> {
        self.range_tombstones.clone()
    }
}
