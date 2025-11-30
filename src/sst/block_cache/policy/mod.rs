//! Eviction policy trait and implementations.
//!
//! The policy decides which entries to evict when the cache is full.
//! Different policies offer different trade-offs between hit rate,
//! scan resistance, and implementation complexity.

pub mod lru;
pub mod wtiny_lfu;

pub use lru::LruPolicy;
pub use wtiny_lfu::WTinyLfuPolicy;

use super::table::EntryId;

/// Eviction policy interface.
///
/// The policy maintains metadata about cached entries and decides
/// which entry to evict when space is needed.
pub trait Policy {
    /// Called when an entry is accessed (hit).
    fn on_access(&mut self, entry_id: EntryId);

    /// Called when a new entry is inserted.
    fn on_insert(&mut self, entry_id: EntryId, size: usize);

    /// Called when an entry is evicted or removed.
    fn on_evict(&mut self, entry_id: EntryId);

    /// Choose a victim for eviction. Returns `None` if no entry can be evicted.
    fn choose_victim(&mut self) -> Option<EntryId>;

    /// Clear all policy state.
    fn clear(&mut self);
}
