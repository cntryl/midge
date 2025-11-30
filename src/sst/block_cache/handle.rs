//! `BlockHandle` — RAII handle that pins a cached block while in use.
//!
//! When all handles to a block are dropped, the block becomes eligible for
//! eviction. The handle provides cheap access to the underlying `BlockData`.

use std::sync::{Arc, Weak};

use super::table::EntryId;
use super::value::BlockData;

/// Trait for releasing pins back to a shard.
///
/// This allows `BlockHandle` to call back into the shard without holding
/// a direct reference to the shard's internal state.
pub(crate) trait Unpinner: Send + Sync {
    /// Decrement the pin count for the given entry.
    fn unpin(&self, entry_id: EntryId);
}

/// Shared unpinner that can be cloned into handles.
pub(crate) type SharedUnpinner = Arc<dyn Unpinner>;

/// Weak reference to the unpinner (used in handles to avoid preventing shard drop).
pub(crate) type WeakUnpinner = Weak<dyn Unpinner>;

/// Token that holds the information needed to unpin on drop.
pub(crate) struct PinToken {
    /// Entry ID to unpin.
    pub(crate) entry_id: EntryId,
    /// Weak reference to the shard's unpinner.
    pub(crate) unpinner: WeakUnpinner,
}

impl std::fmt::Debug for PinToken {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("PinToken")
            .field("entry_id", &self.entry_id)
            .field("unpinner", &"<weak ref>")
            .finish()
    }
}

impl Drop for PinToken {
    fn drop(&mut self) {
        // Try to upgrade the weak reference and unpin
        if let Some(unpinner) = self.unpinner.upgrade() {
            unpinner.unpin(self.entry_id);
        }
        // If the shard was dropped, the cache is gone anyway - nothing to unpin
    }
}

impl Clone for PinToken {
    fn clone(&self) -> Self {
        // Cloning a PinToken does NOT increment the pin count.
        // This is intentional: BlockHandle::clone() produces an unpinned handle.
        // The original handle is the only one that will decrement pins on drop.
        Self {
            entry_id: self.entry_id,
            unpinner: self.unpinner.clone(),
        }
    }
}

/// A handle to a cached block.
///
/// While a `BlockHandle` exists, the underlying block is **pinned** and will
/// not be evicted. Cloning a handle shares the data but the new handle is
/// **unpinned** (does not affect eviction). Only the original handle from
/// `get()` or `insert()` pins the entry.
///
/// Access the block data via [`BlockHandle::data()`].
pub struct BlockHandle {
    /// The cached block payload (shared via `Arc`).
    data: Arc<BlockData>,
    /// Token used to release the pin on drop. When Some, drop will decrement pins.
    pin: Option<PinToken>,
}

impl std::fmt::Debug for BlockHandle {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("BlockHandle")
            .field("data_len", &self.data.bytes().len())
            .field("is_pinned", &self.is_pinned())
            .finish()
    }
}

impl BlockHandle {
    /// Create a handle that is **not** backed by the cache (no pinning).
    ///
    /// Useful when returning a freshly-loaded block that was rejected by
    /// admission control—caller still gets a usable handle.
    #[inline]
    pub fn unpinned(data: Arc<BlockData>) -> Self {
        Self { data, pin: None }
    }

    /// Create a handle backed by a cache entry (pinned).
    ///
    /// When dropped, this handle will decrement the pin count on the entry.
    #[inline]
    pub(crate) fn pinned(data: Arc<BlockData>, entry_id: EntryId, unpinner: WeakUnpinner) -> Self {
        Self {
            data,
            pin: Some(PinToken { entry_id, unpinner }),
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
        self.pin.is_some()
    }
}

impl Clone for BlockHandle {
    fn clone(&self) -> Self {
        // Cloning produces an unpinned handle that shares the data.
        // Only the original handle pins the entry.
        Self {
            data: Arc::clone(&self.data),
            pin: None,
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::sst::block_cache::key::BlockKind;
    use std::sync::atomic::{AtomicU32, Ordering};

    fn make_block() -> Arc<BlockData> {
        Arc::new(BlockData::uncompressed(
            vec![0u8; 100].into(),
            BlockKind::Data,
        ))
    }

    /// A mock unpinner that tracks unpin calls.
    struct MockUnpinner {
        unpin_count: AtomicU32,
        last_entry: AtomicU32,
    }

    impl MockUnpinner {
        fn new() -> Self {
            Self {
                unpin_count: AtomicU32::new(0),
                last_entry: AtomicU32::new(u32::MAX),
            }
        }

        fn unpin_count(&self) -> u32 {
            self.unpin_count.load(Ordering::Relaxed)
        }
    }

    impl Unpinner for MockUnpinner {
        fn unpin(&self, entry_id: EntryId) {
            self.unpin_count.fetch_add(1, Ordering::Relaxed);
            self.last_entry.store(entry_id, Ordering::Relaxed);
        }
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
        let unpinner = Arc::new(MockUnpinner::new());
        let handle = BlockHandle::pinned(block, 42, Arc::downgrade(&unpinner) as WeakUnpinner);

        assert!(handle.is_pinned());
    }

    #[test]
    fn should_unpin_on_drop_given_pinned_handle_when_dropped() {
        let block = make_block();
        let unpinner = Arc::new(MockUnpinner::new());
        
        {
            let _handle = BlockHandle::pinned(block, 42, Arc::downgrade(&unpinner) as WeakUnpinner);
            assert_eq!(unpinner.unpin_count(), 0);
        }
        // Handle dropped - should have called unpin
        assert_eq!(unpinner.unpin_count(), 1);
    }

    #[test]
    fn should_not_unpin_given_unpinned_handle_when_dropped() {
        let block = make_block();
        let unpinner = Arc::new(MockUnpinner::new());
        
        {
            let _handle = BlockHandle::unpinned(block);
        }
        // Handle dropped - should NOT have called unpin
        assert_eq!(unpinner.unpin_count(), 0);
    }

    #[test]
    fn should_produce_unpinned_clone_given_pinned_handle_when_cloned() {
        let block = make_block();
        let unpinner = Arc::new(MockUnpinner::new());
        let handle = BlockHandle::pinned(block, 42, Arc::downgrade(&unpinner) as WeakUnpinner);
        
        let cloned = handle.clone();
        
        assert!(handle.is_pinned());
        assert!(!cloned.is_pinned()); // Clone is unpinned
    }

    #[test]
    fn should_share_data_given_clone_when_data_arc_called() {
        let block = make_block();
        let handle = BlockHandle::unpinned(Arc::clone(&block));
        let arc = handle.data_arc();

        assert!(Arc::ptr_eq(&block, &arc));
    }

    #[test]
    fn should_not_panic_given_dropped_shard_when_handle_dropped() {
        let block = make_block();
        let unpinner = Arc::new(MockUnpinner::new());
        let handle = BlockHandle::pinned(block, 42, Arc::downgrade(&unpinner) as WeakUnpinner);
        
        // Drop the unpinner first (simulates shard being dropped)
        drop(unpinner);
        
        // Now drop handle - should not panic
        drop(handle);
    }
}

