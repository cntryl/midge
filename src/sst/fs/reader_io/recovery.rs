//! One identity-scoped data block for sequential recovery probes.

use super::{BlockHandle, SstFileIo};
use crate::common::resource_budget::{ResourceBudget, ResourceReservation};
use crate::common::{MidgeError, MidgeResult};
use crate::io::FsError;
#[cfg(test)]
use crate::types::EntryType;
use bytes::Bytes;
use std::sync::{Arc, Mutex};

/// Bounded identity-scoped decoded-block cache for sequential recovery probes.
///
/// Invariant: `blocks` holds at most `max_blocks` entries whose combined
/// decoded length is at most `max_bytes` once a load completes. Every retained
/// block owns its budget reservation, so retained memory is always charged to
/// the shared recovery budget and released on eviction or drop.
pub(super) struct RecoveryBlock {
    /// Least recently used first.
    blocks: Vec<(BlockHandle, Bytes)>,
    max_blocks: usize,
    max_bytes: usize,
    hits: u64,
    misses: u64,
    peak: usize,
}

impl RecoveryBlock {
    fn new(max_blocks: usize, max_bytes: usize) -> Self {
        Self {
            blocks: Vec::new(),
            max_blocks,
            max_bytes,
            hits: 0,
            misses: 0,
            peak: 0,
        }
    }

    fn retained_bytes(&self) -> usize {
        self.blocks.iter().map(|(_, bytes)| bytes.len()).sum()
    }

    fn retain(&mut self, handle: BlockHandle, bytes: &Bytes) {
        if self.max_blocks == 0 || bytes.len() > self.max_bytes {
            return;
        }
        while !self.blocks.is_empty()
            && (self.blocks.len() >= self.max_blocks
                || self.retained_bytes().saturating_add(bytes.len()) > self.max_bytes)
        {
            self.blocks.remove(0);
        }
        self.blocks.push((handle, bytes.clone()));
    }
}

struct ReservedBytes {
    bytes: Bytes,
    _reservation: ResourceReservation,
}

impl AsRef<[u8]> for ReservedBytes {
    fn as_ref(&self) -> &[u8] {
        &self.bytes
    }
}

impl SstFileIo {
    pub(crate) fn open_for_recovery(
        path: &str,
        fs: Arc<dyn crate::io::Fs>,
        budget: ResourceBudget,
        max_retained_blocks: usize,
        max_retained_bytes: usize,
    ) -> MidgeResult<Self> {
        let mut reader = Self::open_for_compaction(path, fs, budget)?;
        reader.recovery_block = Some(Mutex::new(RecoveryBlock::new(
            max_retained_blocks,
            max_retained_bytes,
        )));
        Ok(reader)
    }

    pub(crate) fn recovery_block_stats(&self) -> (u64, u64, usize) {
        self.recovery_block.as_ref().map_or((0, 0, 0), |cache| {
            let cache = cache
                .lock()
                .unwrap_or_else(std::sync::PoisonError::into_inner);
            (cache.hits, cache.misses, cache.peak)
        })
    }

    pub(super) fn read_recovery_block(
        &self,
        cache: &Mutex<RecoveryBlock>,
        handle: &BlockHandle,
    ) -> MidgeResult<Bytes> {
        let mut cache = cache
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner);
        if let Some(position) = cache.blocks.iter().position(|(h, _)| h == handle) {
            let entry = cache.blocks.remove(position);
            let bytes = entry.1.clone();
            cache.blocks.push(entry);
            cache.hits = cache.hits.saturating_add(1);
            return Ok(bytes);
        }
        cache.misses = cache.misses.saturating_add(1);
        // Make room before decoding so the transient peak stays within the
        // retained caps plus exactly one in-flight block.
        while !cache.blocks.is_empty() && cache.blocks.len() >= cache.max_blocks.max(1) {
            cache.blocks.remove(0);
        }
        Self::validate_block_handle(*handle, self.block_region_end, "recovery data")?;
        let budget = self.metadata_budget.as_ref().ok_or_else(|| {
            MidgeError::Internal("recovery reader requires a shared budget".into())
        })?;
        let loaded = self.load_recovery_block(budget, handle);
        let (decoded, reservation) = match loaded {
            Ok(loaded) => loaded,
            Err(err) if !cache.blocks.is_empty() => {
                // Retained locality is an optimization; release it and retry
                // once before reporting that the block cannot be proven.
                cache.blocks.clear();
                self.load_recovery_block(budget, handle).map_err(|_| err)?
            }
            Err(err) => return Err(err),
        };
        // Slices returned as KeyState values retain this owner and its charge.
        let bytes = Bytes::from_owner(ReservedBytes {
            bytes: decoded,
            _reservation: reservation,
        });
        cache.peak = cache
            .peak
            .max(cache.retained_bytes().saturating_add(bytes.len()));
        cache.retain(*handle, &bytes);
        Ok(bytes)
    }

    fn load_recovery_block(
        &self,
        budget: &ResourceBudget,
        handle: &BlockHandle,
    ) -> MidgeResult<(Bytes, ResourceReservation)> {
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
        self.read_framed_block(
            file.as_ref(),
            handle,
            Some((budget, "recovery compressed block")),
            |decoded_size| {
                budget.reserve(
                    decoded_size
                        .saturating_add(std::mem::size_of::<ReservedBytes>())
                        .saturating_add(std::mem::size_of::<usize>()),
                    "recovery decoded block",
                )
            },
        )
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::sst::{SstFactory, SstStateReader};

    #[test]
    fn should_release_replaced_blocks_when_the_last_value_is_dropped() -> MidgeResult<()> {
        // Arrange
        let dir = tempfile::tempdir()?;
        let fs = Arc::new(crate::io::RealFs::new(dir.path()).map_err(FsError::into_midge)?);
        let factory = crate::sst::FsSstFactoryIo::new(fs.clone(), 128);
        let mut writer = factory.create()?;
        for key in [b"a", b"z"] {
            writer.add_with_meta(key, Some(&[7; 4096]), 1, EntryType::Put, None)?;
        }
        std::fs::write(dir.path().join("blocks.sst"), writer.finish_bytes()?)?;
        let budget = ResourceBudget::new(128 * 1024);
        let reader = SstFileIo::open_for_recovery("blocks.sst", fs, budget.clone(), 1, usize::MAX)?;

        // Act
        let first = reader.get_state_at_with_time(b"a", u64::MAX, 0)?;
        let repeated = reader.get_state_at_with_time(b"a", u64::MAX, 0)?;
        let last = reader.get_state_at_with_time(b"z", u64::MAX, 0)?;
        let stats = reader.recovery_block_stats();
        drop(reader);
        let retained_value_charge = budget.used();
        drop((first, repeated, last));

        // Assert
        assert_eq!((stats.0, stats.1), (1, 2));
        assert!(stats.2 > 0);
        assert!(
            retained_value_charge > 0,
            "value slices must own their reservations"
        );
        assert_eq!(budget.used(), 0);
        Ok(())
    }

    #[test]
    fn should_evict_least_recent_block_when_retention_exceeds_block_cap() -> MidgeResult<()> {
        // Arrange
        let dir = tempfile::tempdir()?;
        let fs = Arc::new(crate::io::RealFs::new(dir.path()).map_err(FsError::into_midge)?);
        let factory = crate::sst::FsSstFactoryIo::new(fs.clone(), 128);
        let mut writer = factory.create()?;
        for key in [b"a", b"m", b"z"] {
            writer.add_with_meta(key, Some(&[7; 4096]), 1, EntryType::Put, None)?;
        }
        std::fs::write(dir.path().join("three.sst"), writer.finish_bytes()?)?;
        let budget = ResourceBudget::new(128 * 1024);
        let reader = SstFileIo::open_for_recovery("three.sst", fs, budget, 2, usize::MAX)?;

        // Act
        for key in [b"a", b"m", b"a", b"z", b"a", b"m"] {
            reader.get_state_at_with_time(key, u64::MAX, 0)?;
        }
        let (hits, misses, _) = reader.recovery_block_stats();

        // Assert
        assert_eq!((hits, misses), (2, 4));
        Ok(())
    }

    #[test]
    fn should_release_failed_block_loads_when_decoding_exceeds_recovery_budget() -> MidgeResult<()>
    {
        // Arrange
        let dir = tempfile::tempdir()?;
        let fs = Arc::new(crate::io::RealFs::new(dir.path()).map_err(FsError::into_midge)?);
        let factory = crate::sst::FsSstFactoryIo::new(fs.clone(), 128);
        let mut writer = factory.create()?;
        writer.add_with_meta(b"key", Some(&vec![7; 64 * 1024]), 1, EntryType::Put, None)?;
        std::fs::write(dir.path().join("large.sst"), writer.finish_bytes()?)?;
        let budget = ResourceBudget::new(16 * 1024);
        let reader = SstFileIo::open_for_recovery("large.sst", fs, budget.clone(), 1, usize::MAX)?;
        let metadata_charge = budget.used();

        // Act
        for _ in 0..2 {
            let result = reader.get_state_at_with_time(b"key", u64::MAX, 0);
            assert!(matches!(result, Err(MidgeError::ResourceLimit(_))));
        }

        // Assert
        assert_eq!(reader.recovery_block_stats(), (0, 2, 0));
        assert_eq!(budget.used(), metadata_charge);
        drop(reader);
        assert_eq!(budget.used(), 0);
        Ok(())
    }

    #[test]
    fn should_reject_corrupt_data_before_retaining_a_recovery_block() -> MidgeResult<()> {
        // Arrange
        let dir = tempfile::tempdir()?;
        let fs = Arc::new(crate::io::RealFs::new(dir.path()).map_err(FsError::into_midge)?);
        let factory = crate::sst::FsSstFactoryIo::new(fs.clone(), 128);
        let mut writer = factory.create()?;
        writer.add_with_meta(b"key", Some(b"value"), 1, EntryType::Put, None)?;
        let mut bytes = writer.finish_bytes()?;
        let path = dir.path().join("corrupt.sst");
        std::fs::write(&path, &bytes)?;
        let budget = ResourceBudget::new(128 * 1024);
        let reader =
            SstFileIo::open_for_recovery("corrupt.sst", fs, budget.clone(), 1, usize::MAX)?;
        let handle = reader.index_entries()?.first().expect("data block").1;
        bytes[usize::try_from(handle.offset + handle.size - 1).unwrap()] ^= 1;
        std::fs::write(path, bytes)?;
        let metadata_charge = budget.used();

        // Act
        let result = reader.get_state_at_with_time(b"key", u64::MAX, 0);

        // Assert
        assert!(matches!(result, Err(MidgeError::Corruption(_))));
        assert_eq!(reader.recovery_block_stats(), (0, 1, 0));
        assert_eq!(budget.used(), metadata_charge);
        Ok(())
    }
}
