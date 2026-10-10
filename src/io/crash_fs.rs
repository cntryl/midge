//! Inode-based filesystem model with independent data and namespace barriers.
//!
//! This adapter implements the same interface as the host filesystem. It performs
//! no host I/O. A crash invalidates open handles, retains completed durability
//! barriers, and optionally persists a deterministic subset of unbarriered work.

use super::traits::{
    DirEntry, Durability, File, Fs, FsError, FsPath, FsResult, Metadata, OpenMode, OpenOptions,
};
use bytes::Bytes;
use std::collections::{BTreeMap, BTreeSet};
use std::path::{Component, Path, PathBuf};
use std::sync::{Arc, Mutex, MutexGuard};

/// Persistence selection at a simulated process or power failure.
#[derive(Debug, Clone, Copy)]
pub enum CrashMode {
    /// Lose all effects that have not passed the appropriate durability barrier.
    DiscardVolatile,
    /// Retain dirty OS pages and names across a process failure without making
    /// them durable against a later power failure.
    PreserveVolatile,
    /// Persist reproducible partial writes and unbarriered directory snapshots.
    Torn { seed: u64 },
}

#[derive(Default)]
struct Directory {
    live: BTreeMap<String, u64>,
    durable: BTreeMap<String, u64>,
}

#[derive(Clone)]
enum Mutation {
    Write { offset: usize, bytes: Bytes },
    Resize(usize),
}

#[derive(Default)]
struct Data {
    live: Vec<u8>,
    durable: Vec<u8>,
    pending: Vec<Mutation>,
}

enum Node {
    Directory(Directory),
    Data(Data),
}

struct State {
    nodes: BTreeMap<u64, Node>,
    handles: BTreeMap<(u64, u64), usize>,
    next_inode: u64,
    generation: u64,
}

impl Default for State {
    fn default() -> Self {
        Self {
            nodes: BTreeMap::from([(0, Node::Directory(Directory::default()))]),
            handles: BTreeMap::new(),
            next_inode: 1,
            generation: 0,
        }
    }
}

impl State {
    fn directory(&self, inode: u64) -> FsResult<&Directory> {
        match self.nodes.get(&inode) {
            Some(Node::Directory(directory)) => Ok(directory),
            _ => Err(FsError::Io("path component is not a directory".into())),
        }
    }

    fn directory_mut(&mut self, inode: u64) -> FsResult<&mut Directory> {
        match self.nodes.get_mut(&inode) {
            Some(Node::Directory(directory)) => Ok(directory),
            _ => Err(FsError::Io("path component is not a directory".into())),
        }
    }

    fn lookup(&self, parts: &[String]) -> FsResult<u64> {
        let mut inode = 0;
        for part in parts {
            inode = *self
                .directory(inode)?
                .live
                .get(part)
                .ok_or_else(|| FsError::NotFound(parts.join("/")))?;
        }
        Ok(inode)
    }

    fn parent(&self, parts: &[String]) -> FsResult<(u64, String)> {
        let (name, parents) = parts
            .split_last()
            .ok_or_else(|| FsError::Io("cannot replace filesystem root".into()))?;
        let parent = self.lookup(parents)?;
        self.directory(parent)?;
        Ok((parent, name.clone()))
    }

    fn allocate(&mut self, node: Node) -> u64 {
        let inode = self.next_inode;
        self.next_inode = self
            .next_inode
            .checked_add(1)
            .expect("simulated inode exhaustion");
        self.nodes.insert(inode, node);
        inode
    }

    // Unlinked inodes still occupy space while either a durable name or an open
    // handle refers to them. Failed/unbarriered deletion cannot reclaim them.
    fn reclaim(&mut self) {
        let mut retained = BTreeSet::new();
        let mut pending = vec![0];
        pending.extend(
            self.handles.keys().filter_map(|&(generation, inode)| {
                (generation == self.generation).then_some(inode)
            }),
        );
        while let Some(inode) = pending.pop() {
            if retained.insert(inode) {
                if let Some(Node::Directory(directory)) = self.nodes.get(&inode) {
                    pending.extend(directory.live.values().copied());
                    pending.extend(directory.durable.values().copied());
                }
            }
        }
        self.nodes.retain(|inode, _| retained.contains(inode));
    }
}

/// A cloneable crash-aware filesystem. Clones address the same device.
#[derive(Clone, Default)]
pub struct CrashFs {
    state: Arc<Mutex<State>>,
    host_root: Option<PathBuf>,
}

impl CrashFs {
    #[must_use]
    pub fn new() -> Self {
        Self::default()
    }

    /// Accept absolute engine paths below `root` without accessing that host path.
    #[must_use]
    pub fn for_root(root: impl Into<PathBuf>) -> Self {
        Self {
            host_root: Some(root.into()),
            ..Self::default()
        }
    }

    fn lock(&self) -> MutexGuard<'_, State> {
        self.state
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner)
    }

    fn parts(&self, path: &FsPath) -> FsResult<Vec<String>> {
        let path = Path::new(&path.0);
        let relative = if path.is_absolute() {
            path.strip_prefix(self.host_root.as_deref().unwrap_or_else(|| Path::new("/")))
                .map_err(|_| FsError::Io("path escapes simulated root".into()))?
        } else {
            path
        };
        relative
            .components()
            .filter_map(|component| match component {
                Component::CurDir => None,
                Component::Normal(name) => Some(Ok(name.to_string_lossy().into_owned())),
                _ => Some(Err(FsError::Io("path escapes simulated root".into()))),
            })
            .collect()
    }

    fn open_handle(&self, path: &FsPath, opts: OpenOptions) -> FsResult<CrashFile> {
        let parts = self.parts(path)?;
        let mut state = self.lock();
        let (parent, name) = state.parent(&parts)?;
        let existing = state.directory(parent)?.live.get(&name).copied();
        let inode = match existing {
            Some(_) if opts.create_new => return Err(FsError::AlreadyExists(path.0.clone())),
            Some(inode) => inode,
            None if opts.create || opts.create_new => {
                let inode = state.allocate(Node::Data(Data::default()));
                state.directory_mut(parent)?.live.insert(name, inode);
                inode
            }
            None => return Err(FsError::NotFound(path.0.clone())),
        };
        let Some(Node::Data(data)) = state.nodes.get_mut(&inode) else {
            return Err(FsError::Io("cannot open directory as file".into()));
        };
        if opts.truncate {
            if opts.mode != OpenMode::ReadWrite {
                return Err(FsError::Io("cannot truncate read-only handle".into()));
            }
            data.live.clear();
            data.pending.push(Mutation::Resize(0));
        }
        let generation = state.generation;
        *state.handles.entry((generation, inode)).or_default() += 1;
        Ok(CrashFile {
            fs: self.clone(),
            inode,
            generation,
            writable: opts.mode == OpenMode::ReadWrite,
        })
    }

    /// Bytes still occupying the modeled device, including unlinked open files.
    #[must_use]
    pub fn physical_bytes(&self) -> u64 {
        self.lock()
            .nodes
            .values()
            .map(|node| match node {
                Node::Data(data) => data.live.len().max(data.durable.len()) as u64,
                Node::Directory(_) => 0,
            })
            .sum()
    }

    /// Apply a failure and start a new process generation on the same device.
    ///
    /// # Panics
    /// Panics after exhausting all `u64` process generations.
    pub fn crash(&self, mode: CrashMode) {
        let mut state = self.lock();
        let mut random = match mode {
            CrashMode::Torn { seed } => seed,
            _ => 0,
        };
        for node in state.nodes.values_mut() {
            match node {
                Node::Directory(directory) => {
                    let keep_live = match mode {
                        CrashMode::DiscardVolatile => false,
                        CrashMode::PreserveVolatile => continue,
                        CrashMode::Torn { .. } => next_random(&mut random) & 1 == 1,
                    };
                    if keep_live {
                        directory.durable.clone_from(&directory.live);
                    }
                    directory.live.clone_from(&directory.durable);
                }
                Node::Data(data) => {
                    match mode {
                        CrashMode::DiscardVolatile => data.live.clone_from(&data.durable),
                        CrashMode::PreserveVolatile => continue,
                        CrashMode::Torn { .. } => {
                            let mut image = data.durable.clone();
                            for mutation in &data.pending {
                                match mutation {
                                    Mutation::Write { offset, bytes } => {
                                        let count = bounded_random(&mut random, bytes.len() + 1);
                                        apply_write(&mut image, *offset, &bytes[..count]);
                                        if count < bytes.len() {
                                            break;
                                        }
                                    }
                                    Mutation::Resize(len) => {
                                        if next_random(&mut random) & 1 == 0 {
                                            break;
                                        }
                                        image.resize(*len, 0);
                                    }
                                }
                            }
                            data.live = image;
                        }
                    }
                    data.durable.clone_from(&data.live);
                    data.pending.clear();
                }
            }
        }
        state.generation = state
            .generation
            .checked_add(1)
            .expect("simulated generation exhaustion");
        state.handles.clear();
        state.reclaim();
    }
}

fn next_random(state: &mut u64) -> u64 {
    // SplitMix64 gives a stable schedule, including for seed zero.
    *state = state.wrapping_add(0x9e37_79b9_7f4a_7c15);
    let mut value = *state;
    value = (value ^ (value >> 30)).wrapping_mul(0xbf58_476d_1ce4_e5b9);
    value = (value ^ (value >> 27)).wrapping_mul(0x94d0_49bb_1331_11eb);
    value ^ (value >> 31)
}

fn bounded_random(state: &mut u64, bound: usize) -> usize {
    usize::try_from(next_random(state) % u64::try_from(bound).expect("bound fits u64"))
        .expect("result fits usize")
}

fn apply_write(image: &mut Vec<u8>, offset: usize, bytes: &[u8]) {
    if bytes.is_empty() {
        return;
    }
    image.resize(image.len().max(offset + bytes.len()), 0);
    image[offset..offset + bytes.len()].copy_from_slice(bytes);
}

impl Fs for CrashFs {
    fn coordination_key(&self) -> u64 {
        Arc::as_ptr(&self.state) as usize as u64
    }

    fn open(&self, path: &FsPath, opts: OpenOptions) -> FsResult<Box<dyn File + '_>> {
        self.open_handle(path, opts)
            .map(|handle| Box::new(handle) as Box<dyn File>)
    }

    fn open_persistent_handle(&self, path: &FsPath, opts: OpenOptions) -> FsResult<Box<dyn File>> {
        self.open_handle(path, opts)
            .map(|handle| Box::new(handle) as Box<dyn File>)
    }

    fn remove_file(&self, path: &FsPath) -> FsResult<()> {
        let mut state = self.lock();
        let parts = self.parts(path)?;
        let inode = state.lookup(&parts)?;
        if matches!(state.nodes.get(&inode), Some(Node::Directory(_))) {
            return Err(FsError::Io("cannot unlink directory".into()));
        }
        let (parent, name) = state.parent(&parts)?;
        state.directory_mut(parent)?.live.remove(&name);
        state.reclaim();
        Ok(())
    }

    fn exists(&self, path: &FsPath) -> FsResult<bool> {
        match self.lock().lookup(&self.parts(path)?) {
            Ok(_) => Ok(true),
            Err(FsError::NotFound(_)) => Ok(false),
            Err(error) => Err(error),
        }
    }

    fn metadata(&self, path: &FsPath) -> FsResult<Metadata> {
        let state = self.lock();
        let inode = state.lookup(&self.parts(path)?)?;
        Ok(Metadata {
            len: match state.nodes.get(&inode) {
                Some(Node::Data(data)) => data.live.len() as u64,
                _ => 0,
            },
        })
    }

    fn create_dir_all(&self, path: &FsPath) -> FsResult<()> {
        let mut state = self.lock();
        let mut inode = 0;
        for part in self.parts(path)? {
            let existing = state.directory(inode)?.live.get(&part).copied();
            inode = if let Some(existing) = existing {
                state.directory(existing)?;
                existing
            } else {
                let next = state.allocate(Node::Directory(Directory::default()));
                state.directory_mut(inode)?.live.insert(part, next);
                next
            };
        }
        Ok(())
    }

    fn list_dir(&self, path: &FsPath) -> FsResult<Vec<DirEntry>> {
        let state = self.lock();
        let inode = state.lookup(&self.parts(path)?)?;
        Ok(state
            .directory(inode)?
            .live
            .iter()
            .map(|(name, inode)| DirEntry {
                name: name.clone(),
                is_dir: matches!(state.nodes.get(inode), Some(Node::Directory(_))),
            })
            .collect())
    }

    fn remove_dir_all(&self, path: &FsPath) -> FsResult<()> {
        let mut state = self.lock();
        let parts = self.parts(path)?;
        let inode = state.lookup(&parts)?;
        state.directory(inode)?;
        let (parent, name) = state.parent(&parts)?;
        state.directory_mut(parent)?.live.remove(&name);
        state.reclaim();
        Ok(())
    }

    fn sync_dir(&self, path: &FsPath, dur: Durability) -> FsResult<()> {
        let mut state = self.lock();
        let inode = state.lookup(&self.parts(path)?)?;
        let directory = state.directory_mut(inode)?;
        if dur == Durability::Durable {
            directory.durable.clone_from(&directory.live);
        }
        state.reclaim();
        Ok(())
    }

    fn rename_atomic(&self, from: &FsPath, to: &FsPath) -> FsResult<()> {
        let from_parts = self.parts(from)?;
        let to_parts = self.parts(to)?;
        if from_parts == to_parts {
            self.lock().lookup(&from_parts)?;
            return Ok(());
        }
        if to_parts.starts_with(&from_parts) {
            return Err(FsError::Io("cannot move directory inside itself".into()));
        }
        let mut state = self.lock();
        let inode = state.lookup(&from_parts)?;
        let (from_parent, from_name) = state.parent(&from_parts)?;
        let (to_parent, to_name) = state.parent(&to_parts)?;
        if let Some(target) = state.directory(to_parent)?.live.get(&to_name) {
            let source_is_dir = matches!(state.nodes.get(&inode), Some(Node::Directory(_)));
            let target_is_dir = matches!(state.nodes.get(target), Some(Node::Directory(_)));
            if source_is_dir != target_is_dir
                || (target_is_dir && !state.directory(*target)?.live.is_empty())
            {
                return Err(FsError::Io("incompatible rename destination".into()));
            }
        }
        state.directory_mut(from_parent)?.live.remove(&from_name);
        state.directory_mut(to_parent)?.live.insert(to_name, inode);
        state.reclaim();
        Ok(())
    }
}

struct CrashFile {
    fs: CrashFs,
    inode: u64,
    generation: u64,
    writable: bool,
}

impl CrashFile {
    fn data<'a>(&self, state: &'a State) -> FsResult<&'a Data> {
        if self.generation != state.generation {
            return Err(FsError::Unavailable(
                "handle belongs to crashed process".into(),
            ));
        }
        match state.nodes.get(&self.inode) {
            Some(Node::Data(data)) => Ok(data),
            _ => Err(FsError::NotFound("file inode reclaimed".into())),
        }
    }

    fn data_mut<'a>(&self, state: &'a mut State) -> FsResult<&'a mut Data> {
        self.data(state)?;
        if !self.writable {
            return Err(FsError::Io("read-only handle".into()));
        }
        match state.nodes.get_mut(&self.inode) {
            Some(Node::Data(data)) => Ok(data),
            _ => Err(FsError::NotFound("file inode reclaimed".into())),
        }
    }
}

fn checked_range(offset: u64, len: u64) -> FsResult<(usize, usize)> {
    let start = usize::try_from(offset).map_err(|_| FsError::Io("offset overflow".into()))?;
    let end = offset
        .checked_add(len)
        .and_then(|end| usize::try_from(end).ok())
        .ok_or_else(|| FsError::Io("length overflow".into()))?;
    Ok((start, end))
}

impl File for CrashFile {
    fn read_at(&self, offset: u64, len: u64) -> FsResult<Bytes> {
        let state = self.fs.lock();
        let data = self.data(&state)?;
        let (start, end) = checked_range(offset, len)?;
        let bytes = data
            .live
            .get(start..end)
            .ok_or_else(|| FsError::Io("short read".into()))?;
        Ok(Bytes::copy_from_slice(bytes))
    }

    fn write_at(&mut self, offset: u64, bytes: Bytes) -> FsResult<()> {
        let (start, _) = checked_range(offset, bytes.len() as u64)?;
        let mut state = self.fs.lock();
        let data = self.data_mut(&mut state)?;
        apply_write(&mut data.live, start, &bytes);
        data.pending.push(Mutation::Write {
            offset: start,
            bytes,
        });
        Ok(())
    }

    fn truncate(&mut self, len: u64) -> FsResult<()> {
        let (_, len) = checked_range(0, len)?;
        let mut state = self.fs.lock();
        let data = self.data_mut(&mut state)?;
        data.live.resize(len, 0);
        data.pending.push(Mutation::Resize(len));
        Ok(())
    }

    fn append(&mut self, bytes: Bytes) -> FsResult<u64> {
        let mut state = self.fs.lock();
        let data = self.data_mut(&mut state)?;
        let offset = data.live.len();
        checked_range(offset as u64, bytes.len() as u64)?;
        data.live.extend_from_slice(&bytes);
        data.pending.push(Mutation::Write { offset, bytes });
        Ok(offset as u64)
    }

    fn len(&self) -> FsResult<u64> {
        self.data(&self.fs.lock())
            .map(|data| data.live.len() as u64)
    }

    fn sync(&mut self, dur: Durability) -> FsResult<()> {
        let mut state = self.fs.lock();
        self.data(&state)?;
        if dur == Durability::Durable {
            // A read-only descriptor can still synchronize its inode.
            if let Some(Node::Data(data)) = state.nodes.get_mut(&self.inode) {
                data.durable.clone_from(&data.live);
                data.pending.clear();
            }
        }
        Ok(())
    }
}

impl Drop for CrashFile {
    fn drop(&mut self) {
        let mut state = self.fs.lock();
        if let Some(count) = state.handles.get_mut(&(self.generation, self.inode)) {
            *count -= 1;
            if *count == 0 {
                state.handles.remove(&(self.generation, self.inode));
            }
        }
        state.reclaim();
    }
}

#[cfg(test)]
mod tests;
