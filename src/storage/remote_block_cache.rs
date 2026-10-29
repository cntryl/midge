//! Bounded local disk cache for ranges of remote SST objects (#938).
//!
//! Every block-cache miss on a published SST used to be a provider range read,
//! and every reader open a HEAD plus one read per metadata block. This cache
//! keeps fetched ranges on the ephemeral local disk, keyed by the object's
//! pinned identity and byte range, so a working set larger than memory stops
//! paying provider latency and request cost on every miss.
//!
//! The cache is advisory. Each entry records the identity it was stored under
//! and a CRC32C of its bytes; an entry that fails either check, is truncated
//! or is missing is dropped and the read falls through to the provider with
//! its identity precondition unchanged. The directory is disposable and is
//! cleared on open. Its bytes come from a fixed share of the local storage
//! budget carved out before WAL, spill, flush and staging are admitted, so it
//! can never consume space those need; within its share it evicts least
//! recently used entries.

use crate::io::{Durability, Fs, FsError, FsPath, FsResult, OpenOptions};
use bytes::Bytes;
use std::collections::{BTreeMap, HashMap};
use std::sync::atomic::{AtomicU64, Ordering};
use std::sync::{Arc, Mutex};

/// Directory under the local root that holds cache entries.
const CACHE_DIR: &str = "sst-cache";
/// Fixed header bytes ahead of the identity: the payload CRC32C and the
/// identity length, both little-endian `u32`.
const FIXED_HEADER_BYTES: usize = 8;

#[derive(Debug, Clone, PartialEq, Eq, Hash)]
struct EntryKey {
    object: String,
    offset: u64,
    len: u64,
}

impl EntryKey {
    /// Bytes the entry occupies on disk: header, identity and payload.
    fn charge(&self) -> u64 {
        self.len
            .saturating_add(FIXED_HEADER_BYTES as u64)
            .saturating_add(self.object.len() as u64)
    }
}

struct Entry {
    last_used: u64,
}

#[derive(Default)]
struct CacheState {
    entries: HashMap<EntryKey, Entry>,
    /// Entry keys per object, for containment lookups and object-wide drops.
    by_object: HashMap<String, Vec<EntryKey>>,
    /// Entries by last use, oldest first; ticks are unique, so this is a map.
    lru: BTreeMap<u64, EntryKey>,
    used_bytes: u64,
}

impl CacheState {
    fn touch(&mut self, key: &EntryKey, now: u64) {
        if let Some(entry) = self.entries.get_mut(key) {
            self.lru.remove(&entry.last_used);
            entry.last_used = now;
            self.lru.insert(now, key.clone());
        }
    }

    fn register(&mut self, key: EntryKey, now: u64) {
        self.used_bytes = self.used_bytes.saturating_add(key.charge());
        self.lru.insert(now, key.clone());
        self.by_object
            .entry(key.object.clone())
            .or_default()
            .push(key.clone());
        self.entries.insert(key, Entry { last_used: now });
    }

    fn forget(&mut self, key: &EntryKey) {
        if let Some(entry) = self.entries.remove(key) {
            self.used_bytes = self.used_bytes.saturating_sub(key.charge());
            self.lru.remove(&entry.last_used);
        }
        if let Some(keys) = self.by_object.get_mut(&key.object) {
            keys.retain(|candidate| candidate != key);
            if keys.is_empty() {
                self.by_object.remove(&key.object);
            }
        }
    }

    fn containing(&self, object: &str, offset: u64, end: u64) -> Option<EntryKey> {
        self.by_object.get(object).and_then(|keys| {
            keys.iter()
                .find(|key| key.offset <= offset && end <= key.offset.saturating_add(key.len))
                .cloned()
        })
    }
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
            let Some(key) = state.containing(object, offset, end) else {
                self.misses.fetch_add(1, Ordering::Relaxed);
                return None;
            };
            state.touch(&key, self.tick());
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
        let key = EntryKey {
            object: object.to_string(),
            offset,
            len,
        };
        let charge = key.charge();
        if len == 0 || charge > self.budget_bytes || self.lock().entries.contains_key(&key) {
            return;
        }
        // The file is complete before the entry is registered, so a concurrent
        // lookup never finds an entry whose bytes are still being written,
        // which would make it drop good work and leave an orphan file.
        if self.write_entry(&key, bytes).is_err() {
            return;
        }
        let evicted = {
            let mut state = self.lock();
            if state.entries.contains_key(&key) {
                // Another thread stored the same range meanwhile; the file it
                // registered holds the same bytes.
                return;
            }
            let mut evicted = Vec::new();
            while state.used_bytes.saturating_add(charge) > self.budget_bytes {
                let Some((_, victim)) = state.lru.pop_first() else {
                    break;
                };
                state.forget(&victim);
                evicted.push(victim);
            }
            state.register(key, self.tick());
            evicted
        };
        for victim in &evicted {
            let _ = self.fs.remove_file(&Self::path(victim));
        }
    }

    fn tick(&self) -> u64 {
        self.clock.fetch_add(1, Ordering::Relaxed)
    }

    fn lock(&self) -> std::sync::MutexGuard<'_, CacheState> {
        self.state
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner)
    }

    fn path(key: &EntryKey) -> FsPath {
        // Identities are provider strings; a checksum keeps file names portable.
        // Two identities may share a name, so the entry header repeats the
        // identity and `read_entry` compares it.
        let object = crc32c::crc32c(key.object.as_bytes());
        FsPath::new(format!(
            "{CACHE_DIR}/{object:08x}-{:016x}-{:x}.blk",
            key.offset, key.len
        ))
    }

    /// Write the framed entry to a temporary name and move it into place, so
    /// the final name only ever holds a complete entry.
    fn write_entry(&self, key: &EntryKey, bytes: &Bytes) -> FsResult<()> {
        let path = Self::path(key);
        let staging = FsPath::new(format!("{}.{:x}.tmp", path.0, self.tick()));
        let written = self
            .write_framed(&staging, key, bytes)
            .and_then(|()| self.fs.rename_atomic(&staging, &path));
        if written.is_err() {
            let _ = self.fs.remove_file(&staging);
        }
        written
    }

    fn write_framed(&self, path: &FsPath, key: &EntryKey, bytes: &Bytes) -> FsResult<()> {
        let mut file = self.fs.open(
            path,
            OpenOptions {
                mode: crate::io::OpenMode::ReadWrite,
                create: true,
                create_new: true,
                truncate: false,
            },
        )?;
        let identity = key.object.as_bytes();
        let identity_len = u32::try_from(identity.len())
            .map_err(|_| FsError::Unsupported("cache identity too long".into()))?;
        let mut framed = Vec::with_capacity(FIXED_HEADER_BYTES + identity.len() + bytes.len());
        framed.extend_from_slice(&crc32c::crc32c(bytes).to_le_bytes());
        framed.extend_from_slice(&identity_len.to_le_bytes());
        framed.extend_from_slice(identity);
        framed.extend_from_slice(bytes);
        file.write_at(0, Bytes::from(framed))?;
        // The cache is disposable; no sync beyond what the OS flushes itself.
        let _ = file.sync(Durability::Unsafe);
        Ok(())
    }

    fn read_entry(&self, key: &EntryKey) -> FsResult<Bytes> {
        let corrupt = |what: &str| FsError::Corruption(format!("cache entry {what}"));
        let file = self.fs.open(&Self::path(key), OpenOptions::READ_ONLY)?;
        let framed = file.read_at(0, key.charge())?;
        if framed.len() as u64 != key.charge() {
            return Err(corrupt("has the wrong length"));
        }
        let (header, rest) = framed
            .split_at_checked(FIXED_HEADER_BYTES)
            .ok_or_else(|| corrupt("is shorter than its header"))?;
        let expected = u32::from_le_bytes(header[..4].try_into().expect("four crc bytes"));
        let identity_len = u32::from_le_bytes(header[4..].try_into().expect("four length bytes"));
        let (identity, payload) = rest
            .split_at_checked(usize::try_from(identity_len).unwrap_or(usize::MAX))
            .ok_or_else(|| corrupt("is shorter than its identity"))?;
        if identity != key.object.as_bytes() {
            return Err(corrupt("belongs to another object"));
        }
        if crc32c::crc32c(payload) != expected {
            return Err(corrupt("failed its checksum"));
        }
        Ok(framed.slice(framed.len() - payload.len()..))
    }

    fn drop_entry(&self, key: &EntryKey) {
        {
            let mut state = self.lock();
            state.forget(key);
        }
        let _ = self.fs.remove_file(&Self::path(key));
    }

    #[cfg(test)]
    pub(crate) fn used_bytes(&self) -> u64 {
        self.lock().used_bytes
    }
}

#[cfg(test)]
mod tests {
    use super::{EntryKey, RemoteBlockCache, CACHE_DIR};
    use crate::io::traits::{DirEntry, Metadata};
    use crate::io::{Durability, File, Fs, FsPath, FsResult, OpenOptions, ReadFile, ReadFs};
    use bytes::Bytes;
    use std::sync::{Arc, Mutex};

    fn cache(budget: u64) -> (Arc<crate::io::MockFs>, RemoteBlockCache) {
        let fs = Arc::new(crate::io::MockFs::new());
        let cache = RemoteBlockCache::open(Arc::clone(&fs) as Arc<dyn Fs>, budget).expect("open");
        (fs, cache)
    }

    fn entry_files(fs: &dyn Fs) -> Vec<String> {
        fs.list_dir(&FsPath::new(CACHE_DIR))
            .expect("list")
            .into_iter()
            .map(|entry| entry.name)
            .collect()
    }

    fn read_all(fs: &dyn Fs, path: &FsPath) -> Vec<u8> {
        let len = fs.metadata(path).expect("metadata").len;
        fs.open_read(path)
            .expect("open")
            .read_at(0, len)
            .expect("read")
            .to_vec()
    }

    fn rewrite(fs: &dyn Fs, path: &FsPath, bytes: Vec<u8>) {
        let mut file = fs
            .open(
                path,
                OpenOptions {
                    mode: crate::io::OpenMode::ReadWrite,
                    create: true,
                    create_new: false,
                    truncate: true,
                },
            )
            .expect("open entry");
        file.write_at(0, Bytes::from(bytes)).expect("rewrite entry");
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
        let path = FsPath::new(format!(
            "{CACHE_DIR}/{}",
            entry_files(fs.as_ref()).pop().expect("one entry")
        ));
        let mut bytes = read_all(fs.as_ref(), &path);
        let last = bytes.len() - 1;
        bytes[last] ^= 0xff;
        rewrite(fs.as_ref(), &path, bytes);

        // Act
        let first = cache.get("obj", 0, 7);
        let second = cache.get("obj", 0, 7);

        // Assert: the corrupt entry is served to nobody and removed.
        assert_eq!(first, None);
        assert_eq!(second, None);
        assert_eq!(cache.used_bytes(), 0);
        assert_eq!(entry_files(fs.as_ref()), Vec::<String>::new());
    }

    #[test]
    fn should_miss_given_entry_file_of_another_object_when_names_collide() {
        // Arrange: two objects whose entries would share a file name (as two
        // identities with the same checksum do) and the same range; the
        // second object's file is replaced by the first object's bytes, which
        // carry a valid payload checksum.
        let (fs, cache) = cache(1024);
        cache.insert("object-a", 0, &Bytes::from_static(b"bytes of a"));
        cache.insert("object-b", 0, &Bytes::from_static(b"bytes of b"));
        let key = |object: &str| EntryKey {
            object: object.into(),
            offset: 0,
            len: 10,
        };
        let a = read_all(fs.as_ref(), &RemoteBlockCache::path(&key("object-a")));
        rewrite(fs.as_ref(), &RemoteBlockCache::path(&key("object-b")), a);

        // Act
        let served = cache.get("object-b", 0, 10);

        // Assert: the identity check refuses another object's bytes.
        assert_eq!(served, None);
        assert_eq!(
            cache.get("object-a", 0, 10).as_deref(),
            Some(b"bytes of a".as_slice())
        );
    }

    #[test]
    fn should_evict_least_recently_used_given_budget_exceeded_when_inserting() {
        // Arrange: three 100-byte entries under one-byte identities cost 109
        // bytes each, so a 300-byte budget holds two at a time.
        let (_fs, cache) = cache(300);
        let payload = Bytes::from(vec![7_u8; 100]);
        cache.insert("a", 0, &payload);
        cache.insert("b", 0, &payload);
        cache.insert("c", 0, &payload);
        assert!(cache.get("a", 0, 100).is_none(), "a was the oldest");
        assert!(cache.get("b", 0, 100).is_some(), "b is now the most recent");

        // Act
        cache.insert("d", 0, &payload);

        // Assert: c, the least recently used, is gone; b stays.
        assert!(cache.get("c", 0, 100).is_none());
        assert!(cache.get("b", 0, 100).is_some());
        assert!(cache.get("d", 0, 100).is_some());
        assert!(cache.used_bytes() <= 300);
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

    /// Runs a hook the first time a file is created, so a test can act in the
    /// middle of an insert.
    struct HookedFs {
        inner: Arc<crate::io::MockFs>,
        on_create: Mutex<Option<Box<dyn FnOnce() + Send>>>,
    }

    impl ReadFs for HookedFs {
        fn open_read(&self, path: &FsPath) -> FsResult<Box<dyn ReadFile + '_>> {
            self.inner.open_read(path)
        }
        fn exists(&self, path: &FsPath) -> FsResult<bool> {
            self.inner.exists(path)
        }
        fn metadata(&self, path: &FsPath) -> FsResult<Metadata> {
            self.inner.metadata(path)
        }
        fn list_dir(&self, path: &FsPath) -> FsResult<Vec<DirEntry>> {
            self.inner.list_dir(path)
        }
    }

    impl Fs for HookedFs {
        fn open(&self, path: &FsPath, opts: OpenOptions) -> FsResult<Box<dyn File + '_>> {
            if opts.create {
                if let Some(hook) = self.on_create.lock().expect("hook").take() {
                    hook();
                }
            }
            self.inner.open(path, opts)
        }
        fn remove_file(&self, path: &FsPath) -> FsResult<()> {
            self.inner.remove_file(path)
        }
        fn create_dir_all(&self, path: &FsPath) -> FsResult<()> {
            self.inner.create_dir_all(path)
        }
        fn remove_dir_all(&self, path: &FsPath) -> FsResult<()> {
            self.inner.remove_dir_all(path)
        }
        fn sync_dir(&self, path: &FsPath, dur: Durability) -> FsResult<()> {
            self.inner.sync_dir(path, dur)
        }
        fn rename_atomic(&self, from: &FsPath, to: &FsPath) -> FsResult<()> {
            self.inner.rename_atomic(from, to)
        }
    }

    #[test]
    fn should_leave_no_orphan_given_lookup_during_write_when_inserting() {
        // Arrange: a lookup for the range being inserted runs while its file
        // is still being written.
        let fs = Arc::new(HookedFs {
            inner: Arc::new(crate::io::MockFs::new()),
            on_create: Mutex::new(None),
        });
        let cache =
            Arc::new(RemoteBlockCache::open(Arc::clone(&fs) as Arc<dyn Fs>, 1024).expect("open"));
        let during = Arc::new(Mutex::new(None));
        {
            let cache = Arc::clone(&cache);
            let during = Arc::clone(&during);
            *fs.on_create.lock().expect("hook") = Some(Box::new(move || {
                *during.lock().expect("record") =
                    Some((cache.get("obj", 0, 7), cache.used_bytes()));
            }));
        }

        // Act
        cache.insert("obj", 0, &Bytes::from_static(b"payload"));

        // Assert: the concurrent lookup missed without touching the entry,
        // and afterwards the entry is served and the only file is accounted.
        let during = during.lock().expect("record").take().expect("hook ran");
        assert_eq!(during, (None, 0));
        assert_eq!(
            cache.get("obj", 0, 7).as_deref(),
            Some(b"payload".as_slice())
        );
        let files = entry_files(fs.as_ref());
        assert_eq!(files.len(), 1, "files on disk: {files:?}");
        let on_disk = fs
            .metadata(&FsPath::new(format!("{CACHE_DIR}/{}", files[0])))
            .expect("metadata")
            .len;
        assert_eq!(cache.used_bytes(), on_disk);
    }
}
