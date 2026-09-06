//! One identity-scoped data block for sequential recovery probes.

use super::{BlockHandle, SstFileIo};
use crate::common::resource_budget::{ResourceBudget, ResourceReservation};
use crate::common::{MidgeError, MidgeResult};
use bytes::Bytes;
use std::sync::{Arc, Mutex};

#[derive(Default)]
pub(super) struct RecoveryBlock {
    block: Option<(BlockHandle, Bytes)>,
    hits: u64,
    misses: u64,
    peak: usize,
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
    ) -> MidgeResult<Self> {
        let mut reader = Self::open_for_compaction(path, fs, budget)?;
        reader.recovery_block = Some(Mutex::new(RecoveryBlock::default()));
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
        if let Some((cached_handle, bytes)) = &cache.block {
            if cached_handle == handle {
                let bytes = bytes.clone();
                cache.hits = cache.hits.saturating_add(1);
                return Ok(bytes);
            }
        }
        cache.block.take();
        cache.misses = cache.misses.saturating_add(1);
        Self::validate_block_handle(*handle, self.block_region_end, "recovery data")?;
        let budget = self.metadata_budget.as_ref().ok_or_else(|| {
            MidgeError::Internal("recovery reader requires a shared budget".into())
        })?;
        let compressed_size = usize::try_from(handle.size).map_err(|_| {
            MidgeError::ResourceLimit("recovery block exceeds addressable memory".into())
        })?;
        let compressed_reservation =
            budget.reserve(compressed_size, "recovery compressed block")?;
        let file = self.fs.open(
            &self.path,
            crate::io::OpenOptions {
                mode: crate::io::OpenMode::ReadOnly,
                create: false,
                create_new: false,
                truncate: false,
            },
        )?;
        let buffer = file.read_at(handle.offset, handle.size)?;
        let prefix: [u8; 4] = buffer
            .get(..4)
            .and_then(|bytes| bytes.try_into().ok())
            .ok_or_else(|| MidgeError::Corruption("recovery block too short".into()))?;
        if (u32::from_le_bytes(prefix) as usize).checked_add(4) != Some(buffer.len()) {
            return Err(MidgeError::Corruption(
                "recovery block length disagrees with handle".into(),
            ));
        }
        let raw = &buffer[4..];
        let decoded_size = crate::sst::compression::decompressed_size_with_trailer(raw)?;
        let reservation = budget.reserve(
            decoded_size
                .saturating_add(std::mem::size_of::<ReservedBytes>())
                .saturating_add(std::mem::size_of::<usize>()),
            "recovery decoded block",
        )?;
        let decoded = crate::sst::compression::decompress_block_with_trailer(raw)?;
        if decoded.len() > decoded_size {
            return Err(MidgeError::Corruption(
                "recovery block exceeded declared decoded size".into(),
            ));
        }
        drop(buffer);
        drop(compressed_reservation);
        // Slices returned as KeyState values retain this owner and its charge.
        let bytes = Bytes::from_owner(ReservedBytes {
            bytes: decoded,
            _reservation: reservation,
        });
        cache.peak = cache.peak.max(bytes.len());
        cache.block = Some((*handle, bytes.clone()));
        Ok(bytes)
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
        let fs = Arc::new(crate::io::RealFs::new(dir.path())?);
        let factory = crate::sst::FsSstFactoryIo::new(fs.clone(), 128);
        let mut writer = factory.create()?;
        for key in [b"a", b"z"] {
            writer.add_with_meta(key, Some(&[7; 4096]), 1, 0, None)?;
        }
        std::fs::write(dir.path().join("blocks.sst"), writer.finish_bytes()?)?;
        let budget = ResourceBudget::new(128 * 1024);
        let reader = SstFileIo::open_for_recovery("blocks.sst", fs, budget.clone())?;

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
    fn should_release_failed_block_loads_when_decoding_exceeds_recovery_budget() -> MidgeResult<()>
    {
        // Arrange
        let dir = tempfile::tempdir()?;
        let fs = Arc::new(crate::io::RealFs::new(dir.path())?);
        let factory = crate::sst::FsSstFactoryIo::new(fs.clone(), 128);
        let mut writer = factory.create()?;
        writer.add_with_meta(b"key", Some(&vec![7; 64 * 1024]), 1, 0, None)?;
        std::fs::write(dir.path().join("large.sst"), writer.finish_bytes()?)?;
        let budget = ResourceBudget::new(16 * 1024);
        let reader = SstFileIo::open_for_recovery("large.sst", fs, budget.clone())?;
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
        let fs = Arc::new(crate::io::RealFs::new(dir.path())?);
        let factory = crate::sst::FsSstFactoryIo::new(fs.clone(), 128);
        let mut writer = factory.create()?;
        writer.add_with_meta(b"key", Some(b"value"), 1, 0, None)?;
        let mut bytes = writer.finish_bytes()?;
        let path = dir.path().join("corrupt.sst");
        std::fs::write(&path, &bytes)?;
        let budget = ResourceBudget::new(128 * 1024);
        let reader = SstFileIo::open_for_recovery("corrupt.sst", fs, budget.clone())?;
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
