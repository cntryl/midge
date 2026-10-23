//! Bounded local disk cache for ranges of remote SST objects (#938).
//!
//! Every block-cache miss on a published SST used to be a provider range read,
//! and every reader open a HEAD plus one read per metadata block. This cache
//! keeps fetched ranges on the ephemeral local disk, keyed by the object's
//! pinned identity and byte range, so a working set larger than memory stops
//! paying provider latency and request cost on every miss.
//!
//! The cache is advisory. Each entry carries a CRC32C of its bytes; a
//! corrupt, truncated or missing entry is dropped and the read falls through
//! to the provider with its identity precondition unchanged. The directory is
//! disposable and is cleared on open. Its bytes come from a fixed share of the
//! local storage budget carved out before WAL, spill, flush and staging are
//! admitted, so it can never consume space those need; within its share it
//! evicts least recently used entries.

use crate::io::{Durability, Fs, FsError, FsPath, FsResult, OpenOptions};
use bytes::Bytes;
use std::collections::HashMap;
use std::sync::atomic::{AtomicU64, Ordering};
use std::sync::{Arc, Mutex};

/// Directory under the local root that holds cache entries.
const CACHE_DIR: &str = "sst-cache";
/// Bytes of CRC32C ahead of each entry's payload.
const HEADER_BYTES: usize = 4;

#[derive(Debug, Clone, PartialEq, Eq, Hash)]
struct EntryKey {
    object: String,
    offset: u64,
    len: u64,
}

struct Entry {
    last_used: u64,
}

#[derive(Default)]
struct CacheState {
    entries: HashMap<EntryKey, Entry>,
    /// Entry keys per object, for containment lookups and object-wide drops.
    by_object: HashMap<String, Vec<EntryKey>>,
    used_bytes: u64,
}

pub(crate) struct RemoteBlockCache {
    fs: Arc<dyn Fs>,
    budget_bytes: u64,
    state: Mutex<CacheState>,
    clock: AtomicU64,
    hits: AtomicU64,
    misses: AtomicU64,
}

impl std::fmt::Debug for RemoteBlockCache {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("RemoteBlockCache")
            .field("budget_bytes", &self.budget_bytes)
            .finish_non_exhaustive()
    }
}

impl RemoteBlockCache {
    /// Open an empty cache under `fs`, discarding whatever an earlier process
    /// left behind: entries are keyed by object identity, but a disposable
    /// directory is simpler to reason about than reconciling one.
    pub(crate) fn open(fs: Arc<dyn Fs>, budget_bytes: u64) -> FsResult<Self> {
        let dir = FsPath::new(CACHE_DIR);
        if fs.exists(&dir)? {
            fs.remove_dir_all(&dir)?;
        }
        fs.create_dir_all(&dir)?;
        Ok(Self {
            fs,
            budget_bytes,
            state: Mutex::new(CacheState::default()),
            clock: AtomicU64::new(0),
            hits: AtomicU64::new(0),
            misses: AtomicU64::new(0),
        })
    }

    /// Cache hits and misses so far.
    #[cfg(test)]
    pub(crate) fn lookups(&self) -> (u64, u64) {
        (
            self.hits.load(Ordering::Relaxed),
            self.misses.load(Ordering::Relaxed),
        )
    }

    /// The bytes `[offset, offset + len)` of `object` if a cached range
    /// contains them and verifies; a failed entry is dropped so the next read
    /// repopulates it.
    pub(crate) fn get(&self, object: &str, offset: u64, len: u64) -> Option<Bytes> {
        let end = offset.checked_add(len)?;
        let key = {
            let mut state = self.lock();
            let found = state.by_object.get(object).and_then(|keys| {
                keys.iter()
                    .find(|key| key.offset <= offset && end <= key.offset + key.len)
                    .cloned()
            });
            let Some(key) = found else {
                self.misses.fetch_add(1, Ordering::Relaxed);
                return None;
            };
            if let Some(entry) = state.entries.get_mut(&key) {
                entry.last_used = self.clock.fetch_add(1, Ordering::Relaxed);
            }
            key
        };
        let Ok(payload) = self.read_entry(&key) else {
            self.drop_entry(&key);
            self.misses.fetch_add(1, Ordering::Relaxed);
            return None;
        };
        self.hits.fetch_add(1, Ordering::Relaxed);
        let start = usize::try_from(offset - key.offset).ok()?;
        let stop = usize::try_from(end - key.offset).ok()?;
        Some(payload.slice(start..stop))
    }

    /// Store `bytes` as the range `[offset, offset + bytes.len())` of `object`,
    /// evicting the least recently used entries to fit the budget. Ranges
    /// larger than the whole budget are not cached. Failures to write are
    /// ignored: the cache is advisory.
    pub(crate) fn insert(&self, object: &str, offset: u64, bytes: &Bytes) {
        let len = u64::try_from(bytes.len()).unwrap_or(u64::MAX);
        let charge = len.saturating_add(HEADER_BYTES as u64);
        if len == 0 || charge > self.budget_bytes {
            return;
        }
        let key = EntryKey {
            object: object.to_string(),
            offset,
            len,
        };
        let evicted = {
            let mut state = self.lock();
            if state.entries.contains_key(&key) {
                return;
            }
            let mut evicted = Vec::new();
            while state.used_bytes.saturating_add(charge) > self.budget_bytes {
                let Some(victim) = state
                    .entries
                    .iter()
                    .min_by_key(|(_, entry)| entry.last_used)
                    .map(|(key, _)| key.clone())
                else {
                    break;
                };
                Self::forget(&mut state, &victim);
                evicted.push(victim);
            }
            state.used_bytes = state.used_bytes.saturating_add(charge);
            state.entries.insert(
                key.clone(),
                Entry {
                    last_used: self.clock.fetch_add(1, Ordering::Relaxed),
                },
            );
            state
                .by_object
                .entry(key.object.clone())
                .or_default()
                .push(key.clone());
            evicted
        };
        for victim in &evicted {
            let _ = self.fs.remove_file(&Self::path(victim));
        }
        if self.write_entry(&key, bytes).is_err() {
            self.drop_entry(&key);
        }
    }

    fn lock(&self) -> std::sync::MutexGuard<'_, CacheState> {
        self.state
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner)
    }

    fn path(key: &EntryKey) -> FsPath {
        // Identities are provider strings; a checksum keeps file names portable.
        let object = crc32c::crc32c(key.object.as_bytes());
        FsPath::new(format!(
            "{CACHE_DIR}/{object:08x}-{:016x}-{:x}.blk",
            key.offset, key.len
        ))
    }

    fn write_entry(&self, key: &EntryKey, bytes: &Bytes) -> FsResult<()> {
        let mut file = self.fs.open(
            &Self::path(key),
            OpenOptions {
                mode: crate::io::OpenMode::ReadWrite,
                create: true,
                create_new: false,
                truncate: true,
            },
        )?;
        let mut framed = Vec::with_capacity(HEADER_BYTES + bytes.len());
        framed.extend_from_slice(&crc32c::crc32c(bytes).to_le_bytes());
        framed.extend_from_slice(bytes);
        file.write_at(0, Bytes::from(framed))?;
        // The cache is disposable; no sync beyond what the OS flushes itself.
        let _ = file.sync(Durability::Unsafe);
        Ok(())
    }

    fn read_entry(&self, key: &EntryKey) -> FsResult<Bytes> {
        let file = self.fs.open(&Self::path(key), OpenOptions::READ_ONLY)?;
        let framed = file.read_at(0, key.len.saturating_add(HEADER_BYTES as u64))?;
        let (header, payload) = framed
            .split_at_checked(HEADER_BYTES)
            .ok_or_else(|| FsError::Corruption("cache entry shorter than its header".into()))?;
        let expected = u32::from_le_bytes(header.try_into().expect("four header bytes"));
        if crc32c::crc32c(payload) != expected || payload.len() as u64 != key.len {
            return Err(FsError::Corruption(
                "cache entry failed its checksum".into(),
            ));
        }
        Ok(framed.slice(HEADER_BYTES..))
    }

    fn drop_entry(&self, key: &EntryKey) {
        {
            let mut state = self.lock();
            Self::forget(&mut state, key);
        }
        let _ = self.fs.remove_file(&Self::path(key));
    }

    fn forget(state: &mut CacheState, key: &EntryKey) {
        if state.entries.remove(key).is_some() {
            state.used_bytes = state
                .used_bytes
                .saturating_sub(key.len.saturating_add(HEADER_BYTES as u64));
        }
        if let Some(keys) = state.by_object.get_mut(&key.object) {
            keys.retain(|candidate| candidate != key);
            if keys.is_empty() {
                state.by_object.remove(&key.object);
            }
        }
    }

    #[cfg(test)]
    pub(crate) fn used_bytes(&self) -> u64 {
        self.lock().used_bytes
    }
}

#[cfg(test)]
mod tests {
    use super::RemoteBlockCache;
    use crate::io::{Fs, FsPath, ReadFs};
    use bytes::Bytes;
    use std::sync::Arc;

    fn cache(budget: u64) -> (Arc<crate::io::MockFs>, RemoteBlockCache) {
        let fs = Arc::new(crate::io::MockFs::new());
        let cache = RemoteBlockCache::open(Arc::clone(&fs) as Arc<dyn Fs>, budget).expect("open");
        (fs, cache)
    }

    #[test]
    fn should_serve_sub_range_given_containing_entry_when_read() {
        // Arrange
        let (_fs, cache) = cache(1024);
        cache.insert("obj", 100, &Bytes::from_static(b"0123456789"));

        // Act
        let inner = cache.get("obj", 103, 4);
        let outside = cache.get("obj", 108, 4);

        // Assert
        assert_eq!(inner.as_deref(), Some(b"3456".as_slice()));
        assert_eq!(outside, None);
        assert_eq!(cache.lookups(), (1, 1));
    }

    #[test]
    fn should_miss_given_corrupted_entry_when_read() {
        // Arrange
        let (fs, cache) = cache(1024);
        cache.insert("obj", 0, &Bytes::from_static(b"payload"));
        let entry = fs
            .list_dir(&FsPath::new("sst-cache"))
            .expect("list")
            .into_iter()
            .next()
            .expect("one entry");
        let path = FsPath::new(format!("sst-cache/{}", entry.name));
        let mut file = fs
            .open(
                &path,
                crate::io::OpenOptions {
                    mode: crate::io::OpenMode::ReadWrite,
                    create: false,
                    create_new: false,
                    truncate: false,
                },
            )
            .expect("open entry");
        file.write_at(5, Bytes::from_static(b"X")).expect("corrupt");

        // Act
        let first = cache.get("obj", 0, 7);
        let second = cache.get("obj", 0, 7);

        // Assert: the corrupt entry is served to nobody and removed.
        assert_eq!(first, None);
        assert_eq!(second, None);
        assert_eq!(cache.used_bytes(), 0);
        assert!(fs
            .list_dir(&FsPath::new("sst-cache"))
            .expect("list")
            .is_empty());
    }

    #[test]
    fn should_evict_least_recently_used_given_budget_exceeded_when_inserting() {
        // Arrange: three 100-byte entries fit a 350-byte budget two at a time
        // (each costs 104 bytes with its header).
        let (_fs, cache) = cache(350);
        let payload = Bytes::from(vec![7_u8; 100]);
        cache.insert("a", 0, &payload);
        cache.insert("b", 0, &payload);
        cache.insert("c", 0, &payload);
        assert!(cache.get("a", 0, 100).is_some(), "a is the most recent");

        // Act
        cache.insert("d", 0, &payload);

        // Assert: b, the least recently used, is gone; a stays.
        assert!(cache.get("b", 0, 100).is_none());
        assert!(cache.get("a", 0, 100).is_some());
        assert!(cache.get("d", 0, 100).is_some());
        assert!(cache.used_bytes() <= 350);
    }

    #[test]
    fn should_skip_entry_given_range_larger_than_budget_when_inserting() {
        // Arrange
        let (_fs, cache) = cache(64);

        // Act
        cache.insert("obj", 0, &Bytes::from(vec![1_u8; 100]));

        // Assert
        assert_eq!(cache.used_bytes(), 0);
        assert!(cache.get("obj", 0, 100).is_none());
    }
}
