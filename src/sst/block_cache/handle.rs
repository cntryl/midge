//! `BlockHandle` — RAII handle that pins a cached block while in use.
//!
//! When all handles to a block are dropped, the block becomes eligible for
//! eviction. The handle provides cheap access to the underlying `BlockData`.

use std::sync::Arc;

use super::value::BlockData;
use crate::sst::block_cache::shard::BLOCK_CACHE_SHARDS;

/// Opaque reference used internally to release pins on drop.
///
/// This will be filled in once shard internals are implemented; for now it's
/// a placeholder that allows the API shape to stabilize.
#[derive(Debug)]
pub(crate) struct PinToken {
    // Will hold shard id + entry id so drop can decrement pins.
    pub(crate) shard_id: u32,
    pub(crate) entry_id: u32,
}

/// A handle to a cached block.
///
/// While a `BlockHandle` exists, the underlying block is **pinned** and will
/// not be evicted. Cloning a handle increments the pin count; dropping
/// decrements it.
///
/// Access the block data via [`BlockHandle::data()`].
#[derive(Debug)]
pub struct BlockHandle {
    /// The cached block payload (shared via `Arc`).
    data: Arc<BlockData>,
    /// Token used to release the pin on drop (will be wired later).
    _pin: Option<PinToken>,
}

impl BlockHandle {
    /// Create a handle that is **not** backed by the cache (no pinning).
    ///
    /// Useful when returning a freshly-loaded block that was rejected by
    /// admission control—caller still gets a usable handle.
    #[inline]
    pub fn unpinned(data: Arc<BlockData>) -> Self {
        Self { data, _pin: None }
    }

    /// Create a handle backed by a cache entry (pinned).
    #[inline]
    pub(crate) fn pinned(data: Arc<BlockData>, shard_id: u32, entry_id: u32) -> Self {
        Self {
            data,
            _pin: Some(PinToken {
                shard_id,
                entry_id,
            }),
        }
    }

    /// Access the underlying block data.
    #[inline]
    pub fn data(&self) -> &BlockData {
        &self.data
    }

    /// Get a clone of the `Arc<BlockData>` for shared ownership.
    #[inline]
    pub fn data_arc(&self) -> Arc<BlockData> {
        Arc::clone(&self.data)
    }

    /// Returns `true` if this handle is backed by a cache entry.
    #[inline]
    pub fn is_pinned(&self) -> bool {
        self._pin.is_some()
    }
}

impl Clone for BlockHandle {
    fn clone(&self) -> Self {
        let data = Arc::clone(&self.data);
        if let Some(pin) = &self._pin {
            if let Some(shard) = BLOCK_CACHE_SHARDS.get(pin.shard_id as usize) {
                shard.repin(pin.entry_id);
            }
            Self {
                data,
                _pin: self._pin.clone(),
            }
        } else {
            Self { data, _pin: None }
        }
    }
}

impl Drop for BlockHandle {
    fn drop(&mut self) {
        if let Some(pin) = &self._pin {
            if let Some(shard) = BLOCK_CACHE_SHARDS.get(pin.shard_id as usize) {
                shard.unpin(pin.entry_id);
            }
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::sst::block_cache::key::BlockKind;

    fn make_block() -> Arc<BlockData> {
        Arc::new(BlockData::uncompressed(
            vec![0u8; 100].into(),
            BlockKind::Data,
        ))
    }

    #[test]
    fn should_access_data_given_unpinned_handle_when_data_called() {
        let block = make_block();
        let handle = BlockHandle::unpinned(block);

        assert_eq!(handle.data().bytes().len(), 100);
        assert!(!handle.is_pinned());
    }

    #[test]
    fn should_report_pinned_given_pinned_handle_when_is_pinned_called() {
        let block = make_block();
        let handle = BlockHandle::pinned(block, 0, 0);

        assert!(handle.is_pinned());
    }

    #[test]
    fn should_share_data_given_clone_when_data_arc_called() {
        let block = make_block();
        let handle = BlockHandle::unpinned(Arc::clone(&block));
        let arc = handle.data_arc();

        assert!(Arc::ptr_eq(&block, &arc));
    }
}
