//! A crash-aware filesystem that models a journaling filesystem on one device
//! (#817), qualified against the review of the earlier draft (#810).
//!
//! The running process sees *live* state. A successful file `sync` makes the
//! file's dirty pages and its length durable, and `sync_dir` makes a
//! directory's entries durable. A process crash keeps live state, as the page
//! cache does, without making anything more durable. A power loss settles
//! every unsynced effect independently, at the granularity real filesystems
//! expose:
//!
//! - data: the length is the durable or the live length, and each dirty page
//!   keeps its new or its old content, where old content beyond the durable
//!   length is zero;
//! - namespace: each changed directory entry keeps its durable or its live
//!   target, except that a moved inode (a rename) moves atomically, and
//!   syncing either directory of a move commits both sides.
//!
//! Every choice is a function of the seed and a stable identity (a path and a
//! page), so an unrelated file never shifts a saved seed. Faults return real
//! error classes either before or after their effect. A failing file sync
//! drops its dirty pages, so a later sync can succeed over lost data, as a
//! kernel does after a writeback error.

use crate::io::traits::{DirEntry, Metadata};
use crate::io::{Durability, File, Fs, FsError, FsPath, FsResult, OpenMode, OpenOptions};
use bytes::Bytes;
use std::collections::{BTreeMap, BTreeSet};
use std::sync::{Arc, Mutex, MutexGuard};

/// Granularity at which unsynced file data persists or is lost.
pub(crate) const PAGE_BYTES: usize = 4096;

type Inode = u64;
const ROOT: Inode = 0;

#[derive(Clone, Default)]
struct Directory {
    live: BTreeMap<String, Inode>,
    durable: BTreeMap<String, Inode>,
}

#[derive(Clone, Default)]
struct Data {
    live: Vec<u8>,
    durable: Vec<u8>,
    /// Pages whose live content has not reached the durable image.
    dirty: BTreeSet<usize>,
}

impl Data {
    fn mark_dirty(&mut self, start: usize, end: usize) {
        if start < end {
            self.dirty
                .extend(start / PAGE_BYTES..=(end - 1) / PAGE_BYTES);
        }
    }

    /// Persist every dirty page and the live length.
    fn sync(&mut self) {
        let mut durable = std::mem::take(&mut self.durable);
        durable.resize(self.live.len(), 0);
        for &page in &self.dirty {
            copy_page(&mut durable, &self.live, page);
        }
        self.durable = durable;
        self.dirty.clear();
    }

    /// The image a power loss leaves, given its choices.
    fn settle(&self, keep_length: bool, keep_page: impl Fn(usize) -> bool) -> Vec<u8> {
        let len = if keep_length {
            self.live.len()
        } else {
            self.durable.len()
        };
        let mut image = self.durable.clone();
        image.resize(len, 0);
        for &page in &self.dirty {
            if page * PAGE_BYTES < len && keep_page(page) {
                copy_page(&mut image, &self.live, page);
            }
        }
        image
    }

    /// Dirty pages that can still affect the file after a power loss.
    fn relevant_pages(&self) -> impl Iterator<Item = usize> + '_ {
        let bound = self.live.len().max(self.durable.len());
        self.dirty
            .iter()
            .copied()
            .filter(move |page| page * PAGE_BYTES < bound)
    }
}

/// Copy `page` of `source` into `target`, within both lengths.
fn copy_page(target: &mut [u8], source: &[u8], page: usize) {
    let start = page * PAGE_BYTES;
    let end = (start + PAGE_BYTES).min(target.len()).min(source.len());
    if start < end {
        target[start..end].copy_from_slice(&source[start..end]);
    }
}

#[derive(Clone)]
enum Node {
    Directory(Directory),
    Data(Data),
}

/// Operation a fault rule matches.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum FaultOp {
    Open,
    Read,
    Write,
    Sync,
    SyncDir,
    CreateDirAll,
    ListDir,
    Rename,
    RemoveFile,
    RemoveDirAll,
}

/// Whether the faulted operation took effect before its error was returned.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum FaultEffect {
    /// Nothing was applied. A failing file sync drops its dirty pages.
    Before,
    /// The operation was applied, then the error was returned.
    After,
}

/// Error class a fault returns, as `RealFs` would classify it.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum FaultError {
    Io,
    NoSpace,
}

/// One injected failure: the `nth` call of `op` at or below `path` fails.
#[derive(Debug, Clone)]
pub struct Fault {
    pub op: FaultOp,
    /// Calls on this path or below it match; the empty path matches all.
    pub path: String,
    /// The matching call that fails, counting from 1.
    pub nth: u64,
    pub effect: FaultEffect,
    pub error: FaultError,
}

/// Names an injected fault so a test can prove it fired.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct FaultId(usize);

#[derive(Clone)]
struct FaultRule {
    fault: Fault,
    seen: u64,
    fired: bool,
}

/// One independent choice a power loss makes.
#[derive(Debug, Clone, PartialEq, Eq, PartialOrd, Ord)]
pub enum Decision {
    /// The directory entries take their live targets, together, rather than
    /// their durable ones.
    Entries { entries: Vec<String> },
    /// The file keeps its live length rather than its durable length.
    Length { file: String },
    /// The dirty page keeps its new content rather than its old content.
    Page { file: String, page: usize },
}

#[derive(Clone)]
struct State {
    nodes: BTreeMap<Inode, Node>,
    /// Open handles per (process generation, inode).
    handles: BTreeMap<(u64, Inode), usize>,
    next_inode: Inode,
    generation: u64,
    faults: Vec<FaultRule>,
}

impl Default for State {
    fn default() -> Self {
        Self {
            nodes: BTreeMap::from([(ROOT, Node::Directory(Directory::default()))]),
            handles: BTreeMap::new(),
            next_inode: ROOT + 1,
            generation: 0,
            faults: Vec::new(),
        }
    }
}

/// A changed directory entry: (directory inode, name).
type Entry = (Inode, String);

/// Union-find root of `index`, compressing the path.
fn group_root(group_of: &mut [usize], mut index: usize) -> usize {
    while group_of[index] != index {
        group_of[index] = group_of[group_of[index]];
        index = group_of[index];
    }
    index
}

/// The decision for one group of changed entries, named by stable paths.
fn entries_decision(names: &BTreeMap<Inode, String>, group: &[Entry]) -> Decision {
    let mut entries: Vec<String> = group
        .iter()
        .map(|(directory, name)| match names.get(directory) {
            Some(base) if !base.is_empty() => format!("{base}/{name}"),
            Some(_) => name.clone(),
            None => format!("#{directory}/{name}"),
        })
        .collect();
    entries.sort();
    Decision::Entries { entries }
}

impl State {
    fn directory(&self, inode: Inode) -> FsResult<&Directory> {
        match self.nodes.get(&inode) {
            Some(Node::Directory(directory)) => Ok(directory),
            _ => Err(FsError::Io("path component is not a directory".into())),
        }
    }

    fn directory_mut(&mut self, inode: Inode) -> FsResult<&mut Directory> {
        match self.nodes.get_mut(&inode) {
            Some(Node::Directory(directory)) => Ok(directory),
            _ => Err(FsError::Io("path component is not a directory".into())),
        }
    }

    fn data_mut(&mut self, inode: Inode) -> FsResult<&mut Data> {
        match self.nodes.get_mut(&inode) {
            Some(Node::Data(data)) => Ok(data),
            _ => Err(FsError::NotFound("file inode was reclaimed".into())),
        }
    }

    fn lookup(&self, parts: &[String]) -> FsResult<Inode> {
        let mut inode = ROOT;
        for part in parts {
            inode = *self
                .directory(inode)?
                .live
                .get(part)
                .ok_or_else(|| FsError::NotFound(parts.join("/")))?;
        }
        Ok(inode)
    }

    fn parent(&self, parts: &[String]) -> FsResult<(Inode, String)> {
        let (name, parents) = parts
            .split_last()
            .ok_or_else(|| FsError::Io("the filesystem root has no parent".into()))?;
        let parent = self.lookup(parents)?;
        self.directory(parent)?;
        Ok((parent, name.clone()))
    }

    fn allocate(&mut self, node: Node) -> Inode {
        let inode = self.next_inode;
        self.next_inode += 1;
        self.nodes.insert(inode, node);
        inode
    }

    fn is_directory(&self, inode: Inode) -> bool {
        matches!(self.nodes.get(&inode), Some(Node::Directory(_)))
    }

    /// Inodes reachable from the root through live or durable entries.
    fn reachable(&self) -> BTreeSet<Inode> {
        let mut seen = BTreeSet::new();
        let mut pending = vec![ROOT];
        while let Some(inode) = pending.pop() {
            if !seen.insert(inode) {
                continue;
            }
            if let Some(Node::Directory(directory)) = self.nodes.get(&inode) {
                pending.extend(directory.live.values().chain(directory.durable.values()));
            }
        }
        seen
    }

    /// A stable name for every reachable inode: its first durable path, else
    /// its first live path.
    fn identities(&self) -> BTreeMap<Inode, String> {
        let mut names = BTreeMap::from([(ROOT, String::new())]);
        for durable in [true, false] {
            let mut pending = vec![ROOT];
            while let Some(inode) = pending.pop() {
                let Some(Node::Directory(directory)) = self.nodes.get(&inode) else {
                    continue;
                };
                let base = names.get(&inode).cloned().unwrap_or_default();
                let entries = if durable {
                    &directory.durable
                } else {
                    &directory.live
                };
                for (name, child) in entries.iter().rev() {
                    if !names.contains_key(child) {
                        let path = if base.is_empty() {
                            name.clone()
                        } else {
                            format!("{base}/{name}")
                        };
                        names.insert(*child, path);
                        pending.push(*child);
                    }
                }
            }
        }
        names
    }

    /// Changed entries grouped so that every entry naming the same inode
    /// settles together: a rename is one group, a create or unlink is alone.
    fn changed_groups(&self) -> Vec<Vec<Entry>> {
        let mut changed: Vec<(Entry, Option<Inode>, Option<Inode>)> = Vec::new();
        for inode in self.reachable() {
            let Some(Node::Directory(directory)) = self.nodes.get(&inode) else {
                continue;
            };
            let names: BTreeSet<&String> = directory
                .live
                .keys()
                .chain(directory.durable.keys())
                .collect();
            for name in names {
                let live = directory.live.get(name).copied();
                let durable = directory.durable.get(name).copied();
                if live != durable {
                    changed.push(((inode, name.clone()), live, durable));
                }
            }
        }
        let mut group_of: Vec<usize> = (0..changed.len()).collect();
        let mut owner: BTreeMap<Inode, usize> = BTreeMap::new();
        for (index, (_, live, durable)) in changed.iter().enumerate() {
            for target in live.iter().chain(durable.iter()) {
                if let Some(&other) = owner.get(target) {
                    let (left, right) = (
                        group_root(&mut group_of, index),
                        group_root(&mut group_of, other),
                    );
                    group_of[left] = right;
                } else {
                    owner.insert(*target, index);
                }
            }
        }
        let mut groups: BTreeMap<usize, Vec<Entry>> = BTreeMap::new();
        for (index, (entry, _, _)) in changed.into_iter().enumerate() {
            let group = group_root(&mut group_of, index);
            groups.entry(group).or_default().push(entry);
        }
        groups.into_values().collect()
    }

    /// Make every entry in `group` durable at its live target.
    fn commit_group(&mut self, group: &[Entry]) {
        for (directory, name) in group {
            if let Ok(directory) = self.directory_mut(*directory) {
                match directory.live.get(name).copied() {
                    Some(target) => directory.durable.insert(name.clone(), target),
                    None => directory.durable.remove(name),
                };
            }
        }
    }

    /// A directory sync: commit its entries and the other side of every move
    /// they take part in, as a journal commit does.
    fn commit_directory(&mut self, inode: Inode) {
        for group in self.changed_groups() {
            if group.iter().any(|(directory, _)| *directory == inode) {
                self.commit_group(&group);
            }
        }
    }

    fn decisions(&self) -> Vec<Decision> {
        let names = self.identities();
        let mut decisions: Vec<Decision> = self
            .changed_groups()
            .iter()
            .map(|group| entries_decision(&names, group))
            .collect();
        for (inode, node) in &self.nodes {
            let (Node::Data(data), Some(file)) = (node, names.get(inode)) else {
                continue;
            };
            if data.live.len() != data.durable.len() {
                decisions.push(Decision::Length { file: file.clone() });
            }
            decisions.extend(data.relevant_pages().map(|page| Decision::Page {
                file: file.clone(),
                page,
            }));
        }
        decisions.sort();
        decisions
    }

    fn power_loss(&mut self, choose: &mut dyn FnMut(&Decision) -> bool) {
        let names = self.identities();
        let choices: BTreeMap<Decision, bool> = self
            .decisions()
            .into_iter()
            .map(|decision| {
                let keep = choose(&decision);
                (decision, keep)
            })
            .collect();
        for group in self.changed_groups() {
            if choices
                .get(&entries_decision(&names, &group))
                .copied()
                .unwrap_or(false)
            {
                self.commit_group(&group);
            }
        }
        for (inode, node) in &mut self.nodes {
            match node {
                Node::Directory(directory) => directory.live.clone_from(&directory.durable),
                Node::Data(data) => {
                    let Some(file) = names.get(inode) else {
                        data.durable = data.settle(false, |_| false);
                        data.live.clone_from(&data.durable);
                        data.dirty.clear();
                        continue;
                    };
                    let keep_length = choices
                        .get(&Decision::Length { file: file.clone() })
                        .copied()
                        .unwrap_or(false);
                    let image = data.settle(keep_length, |page| {
                        choices
                            .get(&Decision::Page {
                                file: file.clone(),
                                page,
                            })
                            .copied()
                            .unwrap_or(false)
                    });
                    data.live.clone_from(&image);
                    data.durable = image;
                    data.dirty.clear();
                }
            }
        }
        self.end_process();
    }

    /// The running process ends: its handles close and its views are fenced.
    fn end_process(&mut self) {
        self.generation += 1;
        self.handles.clear();
        self.reclaim();
    }

    /// Free inodes no live or durable name and no open handle refers to.
    fn reclaim(&mut self) {
        let mut retained = self.reachable();
        retained.extend(self.handles.keys().map(|&(_, inode)| inode));
        self.nodes.retain(|inode, _| retained.contains(inode));
    }

    /// Count a call against the fault rules; the first rule it completes
    /// fires.
    fn fault(&mut self, op: FaultOp, path: &str) -> Option<(FaultEffect, FsError)> {
        for rule in &mut self.faults {
            let scope = rule.fault.path.as_str();
            let matches = rule.fault.op == op
                && !rule.fired
                && (scope.is_empty()
                    || path == scope
                    || path
                        .strip_prefix(scope)
                        .is_some_and(|rest| rest.starts_with('/')));
            if !matches {
                continue;
            }
            rule.seen += 1;
            if rule.seen == rule.fault.nth {
                rule.fired = true;
                let message = format!("injected {op:?} fault at '{path}'");
                let error = match rule.fault.error {
                    FaultError::Io => FsError::Io(message),
                    FaultError::NoSpace => FsError::NoSpace(message),
                };
                return Some((rule.fault.effect, error));
            }
        }
        None
    }
}

/// A simulated device holding one filesystem. Each process reaches it
/// through a [`CrashFs`] view bound to the process generation.
#[derive(Clone, Default)]
pub struct CrashDevice {
    state: Arc<Mutex<State>>,
}

impl CrashDevice {
    #[must_use]
    pub fn new() -> Self {
        Self::default()
    }

    fn lock(&self) -> MutexGuard<'_, State> {
        self.state
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner)
    }

    /// The filesystem as the current process sees it.
    #[must_use]
    pub fn fs(&self) -> CrashFs {
        CrashFs {
            device: self.clone(),
            generation: self.lock().generation,
        }
    }

    /// An independent copy of the device, for exploring several crashes from
    /// one point.
    #[must_use]
    pub fn fork(&self) -> Self {
        Self {
            state: Arc::new(Mutex::new(self.lock().clone())),
        }
    }

    /// Fail a future call; see [`Fault`].
    #[must_use = "a test should prove the fault fired"]
    pub fn inject(&self, fault: Fault) -> FaultId {
        let mut state = self.lock();
        state.faults.push(FaultRule {
            fault,
            seen: 0,
            fired: false,
        });
        FaultId(state.faults.len() - 1)
    }

    /// Whether the fault has fired.
    #[must_use]
    pub fn fired(&self, fault: FaultId) -> bool {
        self.lock()
            .faults
            .get(fault.0)
            .is_some_and(|rule| rule.fired)
    }

    /// The running process dies. Its page cache survives, so every live
    /// effect stays visible, but nothing becomes more durable; its views and
    /// handles are fenced.
    pub fn process_crash(&self) {
        self.lock().end_process();
    }

    /// The independent choices the next power loss will make.
    #[must_use]
    pub fn decisions(&self) -> Vec<Decision> {
        self.lock().decisions()
    }

    /// Power is lost; each unsynced effect is kept or lost by `seed`.
    pub fn power_loss(&self, seed: u64) {
        self.power_loss_with(|decision| seeded_choice(seed, decision));
    }

    /// Power is lost; `choose` says whether each unsynced effect is kept.
    pub fn power_loss_with(&self, mut choose: impl FnMut(&Decision) -> bool) {
        self.lock().power_loss(&mut choose);
    }
}

/// A seeded choice that depends only on the decision's own identity.
fn seeded_choice(seed: u64, decision: &Decision) -> bool {
    // FNV-1a over a stable rendering, then SplitMix64 finalization.
    let mut hash: u64 = 0xcbf2_9ce4_8422_2325 ^ seed;
    for byte in format!("{decision:?}").bytes() {
        hash ^= u64::from(byte);
        hash = hash.wrapping_mul(0x0100_0000_01b3);
    }
    hash = (hash ^ (hash >> 30)).wrapping_mul(0xbf58_476d_1ce4_e5b9);
    hash = (hash ^ (hash >> 27)).wrapping_mul(0x94d0_49bb_1331_11eb);
    (hash ^ (hash >> 31)) & 1 == 1
}

/// One process's view of a [`CrashDevice`]. After the process crashes, every
/// call through this view fails and has no effect.
#[derive(Clone)]
pub struct CrashFs {
    device: CrashDevice,
    generation: u64,
}

impl CrashFs {
    fn state(&self) -> FsResult<MutexGuard<'_, State>> {
        let state = self.device.lock();
        if state.generation != self.generation {
            return Err(FsError::Io("the simulated process has crashed".into()));
        }
        Ok(state)
    }

    fn parts(path: &FsPath) -> FsResult<Vec<String>> {
        path.0
            .split('/')
            .filter(|part| !part.is_empty() && *part != ".")
            .map(|part| {
                if part == ".." {
                    Err(FsError::Io(format!("path escapes the root: {}", path.0)))
                } else {
                    Ok(part.to_string())
                }
            })
            .collect()
    }

    fn open_file(&self, path: &FsPath, opts: OpenOptions) -> FsResult<CrashFile> {
        let parts = Self::parts(path)?;
        let mut state = self.state()?;
        let fault = state.fault(FaultOp::Open, &path.0);
        if let Some((FaultEffect::Before, error)) = fault {
            return Err(error);
        }
        let (parent, name) = state.parent(&parts)?;
        let inode = match state.directory(parent)?.live.get(&name).copied() {
            Some(_) if opts.create_new => return Err(FsError::AlreadyExists(path.0.clone())),
            Some(inode) if state.is_directory(inode) => {
                return Err(FsError::Io(format!("is a directory: {}", path.0)))
            }
            Some(inode) => inode,
            None if opts.create || opts.create_new => {
                let inode = state.allocate(Node::Data(Data::default()));
                state.directory_mut(parent)?.live.insert(name, inode);
                inode
            }
            None => return Err(FsError::NotFound(path.0.clone())),
        };
        if opts.truncate {
            if opts.mode != OpenMode::ReadWrite {
                return Err(FsError::Io("cannot truncate a read-only handle".into()));
            }
            let data = state.data_mut(inode)?;
            let old = data.live.len();
            data.live.clear();
            data.mark_dirty(0, old);
        }
        if let Some((_, error)) = fault {
            return Err(error);
        }
        let generation = state.generation;
        *state.handles.entry((generation, inode)).or_default() += 1;
        Ok(CrashFile {
            fs: self.clone(),
            path: path.0.clone(),
            inode,
            writable: opts.mode == OpenMode::ReadWrite,
        })
    }

    fn unlink_tree(state: &mut State, directory: Inode) -> FsResult<()> {
        let children: Vec<(String, Inode)> = state
            .directory(directory)?
            .live
            .iter()
            .map(|(name, inode)| (name.clone(), *inode))
            .collect();
        for (name, child) in children {
            if state.is_directory(child) {
                Self::unlink_tree(state, child)?;
            }
            state.directory_mut(directory)?.live.remove(&name);
        }
        Ok(())
    }

    /// Create every missing directory on `parts`, durably, as `RealFs` does.
    fn create_dirs(state: &mut State, parts: &[String]) -> FsResult<()> {
        let mut inode = ROOT;
        for part in parts {
            let existing = state.directory(inode)?.live.get(part).copied();
            inode = if let Some(existing) = existing {
                state.directory(existing)?;
                existing
            } else {
                let created = state.allocate(Node::Directory(Directory::default()));
                state
                    .directory_mut(inode)?
                    .live
                    .insert(part.clone(), created);
                state.commit_directory(inode);
                created
            };
        }
        Ok(())
    }
}

impl Fs for CrashFs {
    fn coordination_key(&self) -> u64 {
        Arc::as_ptr(&self.device.state) as usize as u64
    }

    fn open(&self, path: &FsPath, opts: OpenOptions) -> FsResult<Box<dyn File + '_>> {
        self.open_file(path, opts)
            .map(|file| Box::new(file) as Box<dyn File>)
    }

    fn open_persistent_handle(&self, path: &FsPath, opts: OpenOptions) -> FsResult<Box<dyn File>> {
        self.open_file(path, opts)
            .map(|file| Box::new(file) as Box<dyn File>)
    }

    fn remove_file(&self, path: &FsPath) -> FsResult<()> {
        let parts = Self::parts(path)?;
        let mut state = self.state()?;
        let fault = state.fault(FaultOp::RemoveFile, &path.0);
        if let Some((FaultEffect::Before, error)) = fault {
            return Err(error);
        }
        let inode = state.lookup(&parts)?;
        if state.is_directory(inode) {
            return Err(FsError::Io(format!("is a directory: {}", path.0)));
        }
        let (parent, name) = state.parent(&parts)?;
        state.directory_mut(parent)?.live.remove(&name);
        state.reclaim();
        fault.map_or(Ok(()), |(_, error)| Err(error))
    }

    fn exists(&self, path: &FsPath) -> FsResult<bool> {
        let parts = Self::parts(path)?;
        match self.state()?.lookup(&parts) {
            Ok(_) => Ok(true),
            Err(FsError::NotFound(_)) => Ok(false),
            Err(error) => Err(error),
        }
    }

    fn metadata(&self, path: &FsPath) -> FsResult<Metadata> {
        let parts = Self::parts(path)?;
        let state = self.state()?;
        let inode = state.lookup(&parts)?;
        Ok(match state.nodes.get(&inode) {
            Some(Node::Data(data)) => Metadata {
                len: data.live.len() as u64,
                is_dir: false,
            },
            _ => Metadata {
                len: 0,
                is_dir: true,
            },
        })
    }

    fn create_dir_all(&self, path: &FsPath) -> FsResult<()> {
        let parts = Self::parts(path)?;
        let mut state = self.state()?;
        let fault = state.fault(FaultOp::CreateDirAll, &path.0);
        if let Some((FaultEffect::Before, error)) = fault {
            return Err(error);
        }
        Self::create_dirs(&mut state, &parts)?;
        fault.map_or(Ok(()), |(_, error)| Err(error))
    }

    fn list_dir(&self, path: &FsPath) -> FsResult<Vec<DirEntry>> {
        let parts = Self::parts(path)?;
        let mut state = self.state()?;
        if let Some((_, error)) = state.fault(FaultOp::ListDir, &path.0) {
            return Err(error);
        }
        let inode = state.lookup(&parts)?;
        let directory = state.directory(inode)?;
        Ok(directory
            .live
            .iter()
            .map(|(name, child)| DirEntry {
                name: name.clone(),
                is_dir: state.is_directory(*child),
            })
            .collect())
    }

    fn remove_dir_all(&self, path: &FsPath) -> FsResult<()> {
        let parts = Self::parts(path)?;
        let mut state = self.state()?;
        let fault = state.fault(FaultOp::RemoveDirAll, &path.0);
        if let Some((FaultEffect::Before, error)) = fault {
            return Err(error);
        }
        let inode = state.lookup(&parts)?;
        state.directory(inode)?;
        let (parent, name) = state.parent(&parts)?;
        // Real deletion unlinks entry by entry, so each removal settles on
        // its own at a power loss.
        Self::unlink_tree(&mut state, inode)?;
        state.directory_mut(parent)?.live.remove(&name);
        state.reclaim();
        fault.map_or(Ok(()), |(_, error)| Err(error))
    }

    fn sync_dir(&self, path: &FsPath, dur: Durability) -> FsResult<()> {
        let parts = Self::parts(path)?;
        let mut state = self.state()?;
        let fault = state.fault(FaultOp::SyncDir, &path.0);
        if let Some((FaultEffect::Before, error)) = fault {
            return Err(error);
        }
        let inode = state.lookup(&parts)?;
        state.directory(inode)?;
        if dur == Durability::Durable {
            state.commit_directory(inode);
            state.reclaim();
        }
        fault.map_or(Ok(()), |(_, error)| Err(error))
    }

    fn rename_atomic(&self, from: &FsPath, to: &FsPath) -> FsResult<()> {
        let from_parts = Self::parts(from)?;
        let to_parts = Self::parts(to)?;
        let mut state = self.state()?;
        let fault = state
            .fault(FaultOp::Rename, &from.0)
            .or_else(|| state.fault(FaultOp::Rename, &to.0));
        if let Some((FaultEffect::Before, error)) = fault {
            return Err(error);
        }
        let inode = state.lookup(&from_parts)?;
        if from_parts == to_parts {
            return fault.map_or(Ok(()), |(_, error)| Err(error));
        }
        if to_parts.starts_with(&from_parts) {
            return Err(FsError::Io("cannot move a directory inside itself".into()));
        }
        // `RealFs` creates the destination's parent durably first.
        if let Some((_, parents)) = to_parts.split_last() {
            Self::create_dirs(&mut state, parents)?;
        }
        let (from_parent, from_name) = state.parent(&from_parts)?;
        let (to_parent, to_name) = state.parent(&to_parts)?;
        if let Some(&target) = state.directory(to_parent)?.live.get(&to_name) {
            let source_is_dir = state.is_directory(inode);
            let target_is_dir = state.is_directory(target);
            if source_is_dir != target_is_dir
                || (target_is_dir && !state.directory(target)?.live.is_empty())
            {
                return Err(FsError::Io(format!(
                    "cannot replace '{}' with '{}'",
                    to.0, from.0
                )));
            }
        }
        state.directory_mut(from_parent)?.live.remove(&from_name);
        state.directory_mut(to_parent)?.live.insert(to_name, inode);
        state.reclaim();
        fault.map_or(Ok(()), |(_, error)| Err(error))
    }
}

struct CrashFile {
    fs: CrashFs,
    path: String,
    inode: Inode,
    writable: bool,
}

impl CrashFile {
    fn write(&self, apply: impl FnOnce(&mut Data) -> FsResult<()>) -> FsResult<()> {
        let mut state = self.fs.state()?;
        let fault = state.fault(FaultOp::Write, &self.path);
        if let Some((FaultEffect::Before, error)) = fault {
            return Err(error);
        }
        if !self.writable {
            return Err(FsError::Io("cannot write a read-only handle".into()));
        }
        apply(state.data_mut(self.inode)?)?;
        fault.map_or(Ok(()), |(_, error)| Err(error))
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
        let (start, end) = checked_range(offset, len)?;
        let mut state = self.fs.state()?;
        if let Some((_, error)) = state.fault(FaultOp::Read, &self.path) {
            return Err(error);
        }
        let data = state.data_mut(self.inode)?;
        data.live
            .get(start..end)
            .map(Bytes::copy_from_slice)
            .ok_or_else(|| FsError::Io(format!("short read of '{}'", self.path)))
    }

    fn write_at(&mut self, offset: u64, bytes: Bytes) -> FsResult<()> {
        let (start, end) = checked_range(offset, bytes.len() as u64)?;
        self.write(|data| {
            if data.live.len() < end {
                data.live.resize(end, 0);
            }
            data.live[start..end].copy_from_slice(&bytes);
            data.mark_dirty(start, end);
            Ok(())
        })
    }

    fn truncate(&mut self, len: u64) -> FsResult<()> {
        let (_, len) = checked_range(0, len)?;
        self.write(|data| {
            let old = data.live.len();
            data.live.resize(len, 0);
            data.mark_dirty(old.min(len), old.max(len));
            Ok(())
        })
    }

    fn append(&mut self, bytes: Bytes) -> FsResult<u64> {
        let mut offset = 0;
        self.write(|data| {
            offset = data.live.len();
            checked_range(offset as u64, bytes.len() as u64)?;
            data.live.extend_from_slice(&bytes);
            data.mark_dirty(offset, offset + bytes.len());
            Ok(())
        })?;
        Ok(offset as u64)
    }

    fn len(&self) -> FsResult<u64> {
        let mut state = self.fs.state()?;
        Ok(state.data_mut(self.inode)?.live.len() as u64)
    }

    fn sync(&mut self, dur: Durability) -> FsResult<()> {
        let mut state = self.fs.state()?;
        if dur == Durability::Unsafe {
            return Ok(());
        }
        let fault = state.fault(FaultOp::Sync, &self.path);
        // A read-only descriptor can still synchronize its inode.
        let data = state.data_mut(self.inode)?;
        match fault {
            Some((FaultEffect::Before, error)) => {
                // After a writeback error the kernel marks the pages clean
                // without writing them; a later sync reports success.
                data.dirty.clear();
                Err(error)
            }
            Some((FaultEffect::After, error)) => {
                data.sync();
                Err(error)
            }
            None => {
                data.sync();
                Ok(())
            }
        }
    }
}

impl Drop for CrashFile {
    fn drop(&mut self) {
        let mut state = self.fs.device.lock();
        let key = (self.fs.generation, self.inode);
        if let Some(count) = state.handles.get_mut(&key) {
            *count -= 1;
            if *count == 0 {
                state.handles.remove(&key);
            }
        }
        state.reclaim();
    }
}

#[cfg(test)]
mod tests;
