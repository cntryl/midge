//! `BlockHandle` — RAII handle that pins a cached block while in use.
//!
//! While a `BlockHandle` is pinned, the corresponding cache entry cannot be
//! evicted. The pin is reference-counted across clones so the entry remains
//! protected until the *last* pinned handle is dropped.
//!
//! Handles may also be "unpinned" (not backed by the cache) which still
//! provide access to the block data but do not affect eviction.

use std::fmt;
use std::sync::{Arc, Weak};

use super::table::EntryId;
use super::value::BlockData;

/// Trait for releasing pins back to a shard.
///
/// This abstraction lets `BlockHandle` notify the owning shard that a pin
/// has been released without depending on shard internals.
pub(crate) trait Unpinner: Send + Sync {
    /// Decrement the pin count for the given entry.
    fn unpin(&self, entry_id: EntryId);
}

/// Shared unpinner that lives on the shard side.
pub(crate) type SharedUnpinner = Arc<dyn Unpinner>;

/// Weak reference to a shard's unpinner used by handles.
///
/// Weak avoids keeping the shard alive solely because pinned blocks exist.
/// If the shard/cache is dropped, upgrading will fail and unpinning becomes
/// a no-op.
pub(crate) type WeakUnpinner = Weak<dyn Unpinner>;

/// Token that represents a single pinned cache entry.
///
/// This token is reference-counted across all pinned `BlockHandle`s.
/// When the last `Arc<PinToken>` is dropped, we call back into the shard
/// via `Unpinner::unpin`.
pub(crate) struct PinToken {
    /// Entry ID to unpin in the shard.
    pub(crate) entry_id: EntryId,
    /// Weak reference to the shard's unpinner.
    pub(crate) unpinner: WeakUnpinner,
}

impl fmt::Debug for PinToken {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.debug_struct("PinToken")
            .field("entry_id", &self.entry_id)
            .field("unpinner", &"<weak>")
            .finish()
    }
}

impl Drop for PinToken {
    fn drop(&mut self) {
        // This is called exactly once when the last Arc<PinToken> is dropped.
        if let Some(unpinner) = self.unpinner.upgrade() {
            unpinner.unpin(self.entry_id);
        }
        // If the shard / cache has already been dropped, upgrading fails and
        // there is nothing left to unpin.
    }
}

/// A handle to a cached block.
///
/// While a pinned `BlockHandle` exists, the underlying cache entry is protected
/// from eviction. Pinning is shared across clones: cloning a pinned handle
/// keeps the entry pinned until the *last* pinned handle is dropped.
///
/// Unpinned handles (`BlockHandle::unpinned`) give access to `BlockData`
/// without affecting cache eviction (used when admission rejects a block).
pub struct BlockHandle {
    /// The cached block payload (shared via `Arc`).
    data: Arc<BlockData>,
    /// Shared pin token; when `Some`, this handle participates in pinning.
    ///
    /// All clones share this `Arc<PinToken>`. The pin is released when the
    /// last clone drops it.
    pin: Option<Arc<PinToken>>,
}

impl fmt::Debug for BlockHandle {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
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
    /// admission control—caller still gets a usable handle, but the block
    /// is not tracked by the cache and does not affect eviction.
    #[inline]
    pub fn unpinned(data: Arc<BlockData>) -> Self {
        Self { data, pin: None }
    }

    /// Create a handle backed by a cache entry (pinned).
    ///
    /// All clones of this handle will keep the entry pinned until the last
    /// clone is dropped (shared `Arc<PinToken>`).
    #[inline]
    pub(crate) fn pinned(data: Arc<BlockData>, entry_id: EntryId, unpinner: WeakUnpinner) -> Self {
        let pin = Arc::new(PinToken { entry_id, unpinner });
        Self {
            data,
            pin: Some(pin),
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

    /// Returns `true` if this handle participates in pinning a cache entry.
    #[inline]
    pub fn is_pinned(&self) -> bool {
        self.pin.is_some()
    }

    /// Returns a new handle that shares the same data but is explicitly
    /// **unpinned**, even if this handle is pinned.
    ///
    /// This is useful for long-lived consumers that don't need to hold onto
    /// eviction protection but still want to keep a copy of the data.
    #[inline]
    pub fn to_unpinned(&self) -> Self {
        BlockHandle {
            data: Arc::clone(&self.data),
            pin: None,
        }
    }
}

impl Clone for BlockHandle {
    fn clone(&self) -> Self {
        // Cloning preserves pinning semantics: pinned stays pinned, unpinned stays unpinned.
        Self {
            data: Arc::clone(&self.data),
            pin: self.pin.as_ref().map(Arc::clone),
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::sst::block_cache::key::BlockKind;
    use crate::sst::block_cache::value::BlockData;
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

        fn last_entry(&self) -> u32 {
            self.last_entry.load(Ordering::Relaxed)
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
    fn should_unpin_exactly_once_given_pinned_handle_and_clones_when_all_dropped() {
        let block = make_block();
        let unpinner = Arc::new(MockUnpinner::new());

        let handle = BlockHandle::pinned(block, 42, Arc::downgrade(&unpinner) as WeakUnpinner);

        // Clone a few times; all should remain pinned.
        let h2 = handle.clone();
        let h3 = h2.clone();

        assert!(handle.is_pinned());
        assert!(h2.is_pinned());
        assert!(h3.is_pinned());
        assert_eq!(unpinner.unpin_count(), 0);

        drop(handle);
        assert_eq!(unpinner.unpin_count(), 0);
        drop(h2);
        assert_eq!(unpinner.unpin_count(), 0);
        drop(h3);

        // Last pinned handle dropped → exactly one unpin call.
        assert_eq!(unpinner.unpin_count(), 1);
        assert_eq!(unpinner.last_entry(), 42);
    }

    #[test]
    fn should_not_unpin_given_unpinned_handle_when_dropped() {
        let block = make_block();
        let unpinner = Arc::new(MockUnpinner::new());

        {
            let _handle = BlockHandle::unpinned(block);
            assert_eq!(unpinner.unpin_count(), 0);
        }

        // Unpinned handle does not interact with unpinner at all.
        assert_eq!(unpinner.unpin_count(), 0);
    }

    #[test]
    fn should_produce_unpinned_handle_given_to_unpinned_when_called() {
        let block = make_block();
        let unpinner = Arc::new(MockUnpinner::new());
        let pinned = BlockHandle::pinned(block, 7, Arc::downgrade(&unpinner) as WeakUnpinner);

        let unpinned = pinned.to_unpinned();

        assert!(pinned.is_pinned());
        assert!(!unpinned.is_pinned());
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
        let handle = BlockHandle::pinned(block, 99, Arc::downgrade(&unpinner) as WeakUnpinner);

        // Drop the unpinner first (simulates shard being dropped).
        drop(unpinner);

        // Now drop handle - should not panic and should not try to unpin.
        drop(handle);
    }
}
