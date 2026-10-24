//! V5 restart directory. Offsets are little endian u32, followed by a u32
//! count. Bit 31 marks a restart whose user key continues the previous run.
//! Offsets and the data extent stay below 2 GiB; normal writes additionally
//! enforce their existing 64 MiB decoded-block admission bound.

use crate::common::{MidgeError, MidgeResult};

pub(crate) const RESTART_INTERVAL: usize = 16;
pub(crate) const CONTINUED_KEY: u32 = 1 << 31;
const OFFSET_MASK: u32 = !CONTINUED_KEY;

#[derive(Clone, Copy)]
pub(crate) struct DataBlockLayout<'a> {
    block: &'a [u8],
    pub(crate) data_end: usize,
    pub(crate) restart_count: usize,
}

impl<'a> DataBlockLayout<'a> {
    pub(crate) fn parse(block: &'a [u8]) -> MidgeResult<Self> {
        let layout = Self::read(block)?;
        let mut previous = None;
        for index in 0..layout.restart_count {
            let (offset, _) = layout.restart(index)?;
            if offset >= layout.data_end || previous.is_some_and(|old| offset <= old) {
                return Err(corrupt("invalid restart offset ordering"));
            }
            previous = Some(offset);
        }
        Ok(layout)
    }

    /// Constant-work extent decode. Sequential consumers validate every
    /// crossed boundary; point search validates the complete directory once.
    pub(crate) fn read(block: &'a [u8]) -> MidgeResult<Self> {
        let count_offset = block
            .len()
            .checked_sub(4)
            .ok_or_else(|| corrupt("missing restart count"))?;
        let count = word(block, count_offset)? as usize;
        let bytes = count
            .checked_mul(4)
            .ok_or_else(|| corrupt("restart count overflow"))?;
        let data_end = count_offset
            .checked_sub(bytes)
            .ok_or_else(|| corrupt("restart directory exceeds block"))?;
        if count == 0 || data_end == 0 || data_end >= CONTINUED_KEY as usize {
            return Err(corrupt("invalid empty or oversized data block"));
        }
        let layout = Self {
            block,
            data_end,
            restart_count: count,
        };
        if layout.restart(0)? != (0, false) {
            return Err(corrupt("first restart must be independent at zero"));
        }
        Ok(layout)
    }

    pub(crate) fn restart(&self, index: usize) -> MidgeResult<(usize, bool)> {
        if index >= self.restart_count {
            return Err(corrupt("restart ordinal exceeds directory"));
        }
        let raw = word(self.block, self.data_end + index * 4)?;
        Ok(((raw & OFFSET_MASK) as usize, raw & CONTINUED_KEY != 0))
    }

    pub(crate) fn interval_end(&self, index: usize) -> MidgeResult<usize> {
        if index + 1 == self.restart_count {
            Ok(self.data_end)
        } else {
            self.restart(index + 1).map(|(offset, _)| offset)
        }
    }

    fn restart_key(&self, index: usize) -> MidgeResult<&'a [u8]> {
        let (offset, _) = self.restart(index)?;
        let (entry, end) = crate::sst::encoding::decode_with_format(
            &self.block[..self.data_end],
            offset,
            crate::sst::types::SST_FORMAT_V5,
        )?;
        if entry.shared_len != 0 || end > self.interval_end(index)? {
            return Err(corrupt("restart is not a complete independent entry"));
        }
        Ok(entry.key_delta)
    }

    /// Seek to the first possible interval of the user-key group. Equal keys
    /// may span arbitrarily many restarts: lower_bound finds the first equal
    /// restart, and its continuation bit decides whether the prior interval
    /// holds the beginning. Unique keys start at their exact restart.
    pub(crate) fn seek_interval(&self, key: &[u8]) -> MidgeResult<(usize, usize)> {
        let mut low = 0;
        let mut high = self.restart_count;
        let mut probes = 0;
        while low < high {
            let middle = low + (high - low) / 2;
            probes += 1;
            if self.restart_key(middle)? < key {
                low = middle + 1;
            } else {
                high = middle;
            }
        }
        if low < self.restart_count {
            probes += 1;
            if self.restart_key(low)? == key {
                let (_, continued) = self.restart(low)?;
                if !continued {
                    return Ok((low, probes));
                }
            }
        }
        Ok((low.saturating_sub(1), probes))
    }
}

pub(crate) fn append_restart_directory(block: &mut Vec<u8>, offsets: &[u32]) -> MidgeResult<()> {
    let count = u32::try_from(offsets.len()).map_err(|_| corrupt("restart count exceeds u32"))?;
    for offset in offsets {
        block.extend_from_slice(&offset.to_le_bytes());
    }
    block.extend_from_slice(&count.to_le_bytes());
    Ok(())
}

fn word(data: &[u8], offset: usize) -> MidgeResult<u32> {
    let bytes = data
        .get(offset..offset.saturating_add(4))
        .ok_or_else(|| corrupt("truncated restart directory"))?;
    Ok(u32::from_le_bytes(
        bytes.try_into().expect("exact restart word"),
    ))
}

fn corrupt(message: &str) -> MidgeError {
    MidgeError::Corruption(format!("SST V5 {message}"))
}
