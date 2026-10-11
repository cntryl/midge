//! A crash-aware filesystem that models a journaling filesystem (ext4 or XFS
//! with ordered data) on one device (#817), qualified against the reviews of
//! the earlier draft (#810).
//!
//! The running process sees *live* state, as the page cache does. What a
//! power loss leaves is decided from logs of the operations not yet durable:
//!
//! - **Namespace.** Directory operations form one ordered log, as a metadata
//!   journal does, and a power loss keeps a prefix of it. Every surviving
//!   namespace is therefore one the process actually saw. A rename is one
//!   entry. `sync_dir` commits the log through the last operation touching
//!   that directory, and so does creating a directory, as `RealFs` syncs a new
//!   directory's parent. A file `sync` commits no directory entry (POSIX).
//!   Like the journal it models, a directory commit carries every earlier
//!   operation, so a missing `sync_dir` is observable only until a later
//!   directory commit covers it.
//! - **Data.** Each file logs its writes and truncates since its last
//!   successful `sync`. A power loss keeps the length the file had after some
//!   prefix of them, or a page boundary inside a growing write, as partial
//!   writeback leaves. Each dirty page independently keeps any of its
//!   versions, except that a committed truncate's effect on a page is never
//!   undone, and a page it freed is not reallocated before it commits. Bytes
//!   past a version's end are zero. Files settle independently of each other
//!   and of the namespace prefix.
//! - **Faults.** Rules fail the nth matching call with `RealFs`'s error
//!   classes, before or after the effect. A failing file sync drops its
//!   pending writes: they stay visible in the page cache but never reach the
//!   device, and a later sync reports success, as a kernel does after a
//!   writeback error.
//! - **Processes.** A process crash keeps every live effect without making
//!   anything more durable. Each process works through a view bound to its
//!   generation, so a crashed process's late calls fail and never land.
//!
//! Choices are seeded per decision and named by stable identities (the path
//! that created a file, with a count of earlier files that path created), so
//! an unrelated file never shifts another file's choices. The namespace prefix
//! is one global choice, so any new directory operation can move it.

use crate::io::traits::{DirEntry, Metadata};
use crate::io::{Durability, File, Fs, FsError, FsPath, FsResult, OpenMode, OpenOptions};
use bytes::Bytes;
use std::collections::{BTreeMap, BTreeSet};
use std::sync::{Arc, Mutex, MutexGuard};

/// Granularity at which unsynced file data reaches the device.
pub(crate) const PAGE_BYTES: usize = 4096;

type Inode = u64;
const ROOT: Inode = 0;

#[derive(Clone)]
enum DataOp {
    Write { offset: usize, bytes: Bytes },
    Truncate { len: usize },
}

impl DataOp {
    fn apply(&self, image: &mut Vec<u8>) {
        match self {
            Self::Write { offset, bytes } => {
                let end = offset + bytes.len();
                if image.len() < end {
                    image.resize(end, 0);
                }
                image[*offset..end].copy_from_slice(bytes);
            }
            Self::Truncate { len } => image.resize(*len, 0),
        }
    }

    fn apply_len(&self, len: &mut usize) {
        match self {
            Self::Write { offset, bytes } => *len = (*len).max(offset + bytes.len()),
            Self::Truncate { len: target } => *len = *target,
        }
    }

    /// Pages whose content this operation changes, given the length before.
    fn pages(&self, before: usize) -> Vec<usize> {
        match self {
            Self::Write { offset, bytes } => {
                (offset / PAGE_BYTES..=(offset + bytes.len() - 1) / PAGE_BYTES).collect()
            }
            Self::Truncate { len } if *len < before => {
                (len / PAGE_BYTES..=(before - 1) / PAGE_BYTES).collect()
            }
            Self::Truncate { .. } => Vec::new(),
        }
    }

    /// Whether this truncate frees `page` outright, rather than zeroing the
    /// tail of the page its new length lands in.
    fn frees(&self, before: usize, page: usize) -> bool {
        match self {
            Self::Truncate { len } => {
                page >= len.div_ceil(PAGE_BYTES) && page * PAGE_BYTES < before
            }
            Self::Write { .. } => false,
        }
    }

    fn is_truncate(&self) -> bool {
        matches!(self, Self::Truncate { .. })
    }
}

/// A length a power loss can leave: `len`, with the first `applied`
/// pending operations committed.
#[derive(Clone, Copy)]
struct Cut {
    len: usize,
    applied: usize,
}

/// A stable name for an inode: the path that created it, and how many
/// earlier inodes that path created.
#[derive(Clone, Debug, PartialEq, Eq, PartialOrd, Ord)]
struct Identity {
    path: String,
    reuse: u64,
}

#[derive(Clone)]
struct Data {
    identity: Identity,
    /// What reads see.
    live: Vec<u8>,
    /// The device image after the last successful sync.
    durable: Vec<u8>,
    /// Writes and truncates since then, in order.
    ops: Vec<DataOp>,
}

impl Data {
    fn new(identity: Identity) -> Self {
        Self {
            identity,
            live: Vec::new(),
            durable: Vec::new(),
            ops: Vec::new(),
        }
    }

    fn push(&mut self, op: DataOp) {
        op.apply(&mut self.live);
        self.ops.push(op);
    }

    /// Persist every pending operation.
    fn sync(&mut self) {
        for op in self.ops.drain(..) {
            op.apply(&mut self.durable);
        }
    }

    /// A writeback error: pending writes never reach the device, but the
    /// lengths they set and the truncates still can.
    fn drop_writeback(&mut self) {
        let mut image = self.durable.clone();
        for op in &mut self.ops {
            if let DataOp::Write { offset, bytes } = op {
                let end = *offset + bytes.len();
                if image.len() < end {
                    image.resize(end, 0);
                }
                *bytes = Bytes::copy_from_slice(&image[*offset..end]);
            }
            op.apply(&mut image);
        }
    }

    /// Lengths a power loss can leave, in operation order. Cuts that leave
    /// the same length under the same committed truncates are one cut.
    fn cuts(&self) -> Vec<Cut> {
        let mut len = self.durable.len();
        let mut truncates = 0;
        let mut seen = BTreeSet::from([(len, truncates)]);
        let mut cuts = vec![Cut { len, applied: 0 }];
        let mut add = |cut: Cut, truncates: usize, cuts: &mut Vec<Cut>| {
            if seen.insert((cut.len, truncates)) {
                cuts.push(cut);
            }
        };
        for (index, op) in self.ops.iter().enumerate() {
            let mut next = len;
            op.apply_len(&mut next);
            if !op.is_truncate() && next > len {
                // Partial writeback advances the length a page at a time.
                let mut boundary = (len / PAGE_BYTES + 1) * PAGE_BYTES;
                while boundary < next {
                    add(
                        Cut {
                            len: boundary,
                            applied: index,
                        },
                        truncates,
                        &mut cuts,
                    );
                    boundary += PAGE_BYTES;
                }
            }
            if op.is_truncate() {
                truncates += 1;
            }
            add(
                Cut {
                    len: next,
                    applied: index + 1,
                },
                truncates,
                &mut cuts,
            );
            len = next;
        }
        cuts
    }

    /// For every page a pending operation changes: the versions it can hold,
    /// as counts of operations applied, after the durable version 0.
    fn page_versions(&self) -> BTreeMap<usize, Vec<usize>> {
        let mut versions: BTreeMap<usize, Vec<usize>> = BTreeMap::new();
        let mut len = self.durable.len();
        for (index, op) in self.ops.iter().enumerate() {
            for page in op.pages(len) {
                versions
                    .entry(page)
                    .or_insert_with(|| vec![0])
                    .push(index + 1);
            }
            op.apply_len(&mut len);
        }
        versions
    }

    /// Versions of `page` the device can hold under `cut`. A committed
    /// truncate that changed the page cannot be undone: ordered data writes
    /// its zeroed tail before the size change commits. A page an uncommitted
    /// truncate freed is not reallocated before that truncate commits.
    fn eligible(&self, cut: Cut, page: usize, versions: &[usize]) -> Vec<usize> {
        let mut low = 0;
        let mut high = usize::MAX;
        let mut len = self.durable.len();
        for (index, op) in self.ops.iter().enumerate() {
            let version = index + 1;
            if op.is_truncate() && op.pages(len).contains(&page) {
                if version <= cut.applied {
                    low = version;
                } else if op.frees(len, page) && high == usize::MAX {
                    high = version;
                }
            }
            op.apply_len(&mut len);
        }
        versions
            .iter()
            .copied()
            .filter(|version| (low..high).contains(version))
            .collect()
    }

    /// The image a power loss leaves, given its choices. A page choice
    /// outside the eligible versions settles on the nearest eligible one.
    /// One working image is replayed, so memory stays near the file size.
    fn settle(&self, cut: Cut, choose_page: impl Fn(usize) -> usize) -> Vec<u8> {
        let mut by_version: BTreeMap<usize, Vec<usize>> = BTreeMap::new();
        let mut chosen = BTreeSet::new();
        for (page, versions) in self.page_versions() {
            if page * PAGE_BYTES >= cut.len {
                continue;
            }
            let wanted = versions[choose_page(page).min(versions.len() - 1)];
            let eligible = self.eligible(cut, page, &versions);
            let version = eligible
                .iter()
                .copied()
                .filter(|version| *version <= wanted)
                .max()
                .or_else(|| eligible.first().copied())
                .unwrap_or(0);
            by_version.entry(version).or_default().push(page);
            chosen.insert(page);
        }
        by_version
            .entry(0)
            .or_default()
            .extend((0..cut.len.div_ceil(PAGE_BYTES)).filter(|page| !chosen.contains(page)));
        let mut image = vec![0; cut.len];
        let mut working = self.durable.clone();
        let mut applied = 0;
        for (version, pages) in by_version {
            while applied < version {
                self.ops[applied].apply(&mut working);
                applied += 1;
            }
            for page in pages {
                let start = page * PAGE_BYTES;
                let end = (start + PAGE_BYTES).min(cut.len).min(working.len());
                if start < end {
                    image[start..end].copy_from_slice(&working[start..end]);
                }
            }
        }
        image
    }
}

#[derive(Clone)]
struct Directory {
    identity: Identity,
    live: BTreeMap<String, Inode>,
    durable: BTreeMap<String, Inode>,
}

impl Directory {
    fn new(identity: Identity) -> Self {
        Self {
            identity,
            live: BTreeMap::new(),
            durable: BTreeMap::new(),
        }
    }
}

#[derive(Clone)]
enum Node {
    Directory(Directory),
    Data(Data),
}

impl Node {
    fn identity(&self) -> &Identity {
        match self {
            Self::Directory(directory) => &directory.identity,
            Self::Data(data) => &data.identity,
        }
    }
}

/// The live or durable entries of `directory`, if it is one.
fn entries_of(
    nodes: &mut BTreeMap<Inode, Node>,
    directory: Inode,
    durable: bool,
) -> Option<&mut BTreeMap<String, Inode>> {
    match nodes.get_mut(&directory) {
        Some(Node::Directory(directory)) => Some(if durable {
            &mut directory.durable
        } else {
            &mut directory.live
        }),
        _ => None,
    }
}

/// One directory operation not yet durable.
#[derive(Clone)]
enum NamespaceOp {
    /// `name` in `directory` now names `inode`; `replaced` was there before.
    Link {
        directory: Inode,
        name: String,
        inode: Inode,
        replaced: Option<Inode>,
    },
    Unlink {
        directory: Inode,
        name: String,
        inode: Inode,
    },
    /// One atomic move; `replaced` was at the destination before.
    Rename {
        from: (Inode, String),
        to: (Inode, String),
        inode: Inode,
        replaced: Option<Inode>,
    },
}

impl NamespaceOp {
    fn touches(&self, inode: Inode) -> bool {
        match self {
            Self::Link { directory, .. } | Self::Unlink { directory, .. } => *directory == inode,
            Self::Rename { from, to, .. } => from.0 == inode || to.0 == inode,
        }
    }

    fn inodes(&self) -> impl Iterator<Item = Inode> {
        let (inode, replaced) = match self {
            Self::Link {
                inode, replaced, ..
            }
            | Self::Rename {
                inode, replaced, ..
            } => (*inode, *replaced),
            Self::Unlink { inode, .. } => (*inode, None),
        };
        std::iter::once(inode).chain(replaced)
    }

    /// Apply this operation to the live or the durable entries.
    fn apply(&self, nodes: &mut BTreeMap<Inode, Node>, durable: bool) {
        match self {
            Self::Link {
                directory,
                name,
                inode,
                ..
            } => {
                if let Some(entries) = entries_of(nodes, *directory, durable) {
                    entries.insert(name.clone(), *inode);
                }
            }
            Self::Unlink {
                directory, name, ..
            } => {
                if let Some(entries) = entries_of(nodes, *directory, durable) {
                    entries.remove(name);
                }
            }
            Self::Rename {
                from, to, inode, ..
            } => {
                if let Some(entries) = entries_of(nodes, from.0, durable) {
                    entries.remove(&from.1);
                }
                if let Some(entries) = entries_of(nodes, to.0, durable) {
                    entries.insert(to.1.clone(), *inode);
                }
            }
        }
    }
}

/// Operation a fault rule matches.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum FaultOp {
    Open,
    Read,
    /// Writes, appends and truncates through a file handle.
    Write,
    Sync,
    /// Durable directory syncs; `Unsafe` ones do nothing, as in `RealFs`.
    SyncDir,
    CreateDirAll,
    ListDir,
    Rename,
    RemoveFile,
    RemoveDirAll,
    /// `exists`, `metadata` and a handle's `len`.
    Stat,
}

/// Whether the faulted operation took effect before its error was returned.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum FaultEffect {
    /// Nothing was applied. A failing file sync drops its pending writes.
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

/// One injected failure: the `nth` call of `op` naming `path`, or a path
/// below it, fails.
#[derive(Debug, Clone)]
pub struct Fault {
    pub op: FaultOp,
    /// Matched as whole path segments; the empty path matches every call.
    pub path: String,
    /// The matching call that fails, counting from 1. When several rules
    /// complete on one call, the first injected fails it and the others wait
    /// for their next match.
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

/// What one power-loss choice decides. Files are named by the path that
/// created them and how many earlier files that path created.
#[derive(Debug, Clone, PartialEq, Eq, PartialOrd, Ord)]
pub enum Subject {
    /// How many pending directory operations persist, in order.
    Namespace,
    /// Which pending length the file keeps.
    Length { file: String, reuse: u64 },
    /// Which version of the page persists.
    Page {
        file: String,
        reuse: u64,
        page: usize,
    },
}

impl Subject {
    /// A rendering that names the subject, independent of `Debug`.
    fn key(&self) -> String {
        match self {
            Self::Namespace => "namespace".to_string(),
            Self::Length { file, reuse } => format!("length\0{file}\0{reuse}"),
            Self::Page { file, reuse, page } => format!("page\0{file}\0{reuse}\0{page}"),
        }
    }
}

/// One choice a power loss makes, among `options` alternatives.
#[derive(Debug, Clone, PartialEq, Eq, PartialOrd, Ord)]
pub struct Decision {
    pub subject: Subject,
    pub options: usize,
}

#[derive(Clone)]
struct State {
    nodes: BTreeMap<Inode, Node>,
    /// Directory operations not yet durable, in the order they happened.
    pending: Vec<NamespaceOp>,
    /// Open handles per (process generation, inode).
    handles: BTreeMap<(u64, Inode), usize>,
    next_inode: Inode,
    generation: u64,
    faults: Vec<FaultRule>,
    /// How many inodes each path has created.
    created: BTreeMap<String, u64>,
    /// How many times each path has been given to an inode, by creation or
    /// rename, over the device's life.
    named: BTreeMap<String, u64>,
}

impl Default for State {
    fn default() -> Self {
        let root = Identity {
            path: String::new(),
            reuse: 0,
        };
        Self {
            nodes: BTreeMap::from([(ROOT, Node::Directory(Directory::new(root)))]),
            pending: Vec::new(),
            handles: BTreeMap::new(),
            next_inode: ROOT + 1,
            generation: 0,
            faults: Vec::new(),
            created: BTreeMap::new(),
            named: BTreeMap::new(),
        }
    }
}

/// A path as whole segments, without empty or `.` segments.
fn normalized(path: &str) -> String {
    path.split('/')
        .filter(|segment| !segment.is_empty() && *segment != ".")
        .collect::<Vec<_>>()
        .join("/")
}

fn within(path: &str, scope: &str) -> bool {
    scope.is_empty()
        || path == scope
        || path
            .strip_prefix(scope)
            .is_some_and(|rest| rest.starts_with('/'))
}

impl State {
    fn directory(&self, inode: Inode) -> FsResult<&Directory> {
        match self.nodes.get(&inode) {
            Some(Node::Directory(directory)) => Ok(directory),
            _ => Err(FsError::Io("path component is not a directory".into())),
        }
    }

    fn data(&self, inode: Inode) -> FsResult<&Data> {
        match self.nodes.get(&inode) {
            Some(Node::Data(data)) => Ok(data),
            _ => Err(FsError::NotFound("file inode was reclaimed".into())),
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

    fn is_directory(&self, inode: Inode) -> bool {
        matches!(self.nodes.get(&inode), Some(Node::Directory(_)))
    }

    /// The path of `name` in `directory`, by the directory's creating path.
    fn path_in(&self, directory: Inode, name: &str) -> String {
        match self.nodes.get(&directory).map(|node| &node.identity().path) {
            Some(parent) if !parent.is_empty() => format!("{parent}/{name}"),
            _ => name.to_string(),
        }
    }

    /// Every file the live namespace holds, by path, with its live bytes.
    fn files(&self) -> BTreeMap<String, Vec<u8>> {
        let mut files = BTreeMap::new();
        let mut pending = vec![(ROOT, String::new())];
        while let Some((inode, path)) = pending.pop() {
            let Some(node) = self.nodes.get(&inode) else {
                continue;
            };
            match node {
                Node::Directory(directory) => {
                    for (name, child) in &directory.live {
                        let child_path = if path.is_empty() {
                            name.clone()
                        } else {
                            format!("{path}/{name}")
                        };
                        pending.push((*child, child_path));
                    }
                }
                Node::Data(data) => {
                    files.insert(path, data.live.clone());
                }
            }
        }
        files
    }

    /// The identity of a new inode at `name` in `parent`.
    fn identity(&mut self, parent: Inode, name: &str) -> Identity {
        let path = match self.nodes.get(&parent).map(|node| &node.identity().path) {
            Some(parent) if !parent.is_empty() => format!("{parent}/{name}"),
            _ => name.to_string(),
        };
        let count = self.created.entry(path.clone()).or_default();
        let reuse = *count;
        *count += 1;
        Identity { path, reuse }
    }

    fn allocate(&mut self, node: Node) -> Inode {
        let inode = self.next_inode;
        self.next_inode += 1;
        self.nodes.insert(inode, node);
        inode
    }

    /// Apply a directory operation to the live namespace and log it.
    fn record(&mut self, op: NamespaceOp) {
        let named = match &op {
            NamespaceOp::Link {
                directory, name, ..
            }
            | NamespaceOp::Rename {
                to: (directory, name),
                ..
            } => Some(self.path_in(*directory, name)),
            NamespaceOp::Unlink { .. } => None,
        };
        if let Some(path) = named {
            *self.named.entry(path).or_default() += 1;
        }
        op.apply(&mut self.nodes, false);
        self.pending.push(op);
    }

    fn link(&mut self, directory: Inode, name: &str, inode: Inode) -> FsResult<()> {
        let replaced = self.directory(directory)?.live.get(name).copied();
        self.record(NamespaceOp::Link {
            directory,
            name: name.to_string(),
            inode,
            replaced,
        });
        Ok(())
    }

    fn unlink(&mut self, directory: Inode, name: &str) -> FsResult<()> {
        let inode = self
            .directory(directory)?
            .live
            .get(name)
            .copied()
            .ok_or_else(|| FsError::NotFound(name.to_string()))?;
        self.record(NamespaceOp::Unlink {
            directory,
            name: name.to_string(),
            inode,
        });
        Ok(())
    }

    fn rename(&mut self, from: (Inode, String), to: (Inode, String), inode: Inode) -> FsResult<()> {
        let replaced = self.directory(to.0)?.live.get(&to.1).copied();
        self.record(NamespaceOp::Rename {
            from,
            to,
            inode,
            replaced,
        });
        Ok(())
    }

    /// Make the first `count` pending directory operations durable.
    fn commit(&mut self, count: usize) {
        let committed: Vec<NamespaceOp> = self.pending.drain(..count).collect();
        for op in committed {
            op.apply(&mut self.nodes, true);
        }
        self.reclaim();
    }

    /// A directory sync: the log is committed through its last operation on
    /// that directory, as a journal commit is.
    fn commit_directory(&mut self, inode: Inode) {
        if let Some(last) = self.pending.iter().rposition(|op| op.touches(inode)) {
            self.commit(last + 1);
        }
    }

    /// Inodes a live or durable name, a pending directory operation or an
    /// open handle can still reach.
    fn reachable(&self) -> BTreeSet<Inode> {
        let mut retained = BTreeSet::new();
        let mut reaching: Vec<Inode> = std::iter::once(ROOT)
            .chain(self.pending.iter().flat_map(NamespaceOp::inodes))
            .chain(self.handles.keys().map(|&(_, inode)| inode))
            .collect();
        while let Some(inode) = reaching.pop() {
            if !retained.insert(inode) {
                continue;
            }
            if let Some(Node::Directory(directory)) = self.nodes.get(&inode) {
                reaching.extend(directory.live.values().chain(directory.durable.values()));
            }
        }
        retained
    }

    fn decisions(&self) -> Vec<Decision> {
        let mut decisions = Vec::new();
        if !self.pending.is_empty() {
            decisions.push(Decision {
                subject: Subject::Namespace,
                options: self.pending.len() + 1,
            });
        }
        let reachable = self.reachable();
        for (inode, node) in &self.nodes {
            let Node::Data(data) = node else {
                continue;
            };
            if data.ops.is_empty() || !reachable.contains(inode) {
                continue;
            }
            let Identity { path, reuse } = data.identity.clone();
            let cuts = data.cuts();
            if cuts.len() > 1 {
                decisions.push(Decision {
                    subject: Subject::Length {
                        file: path.clone(),
                        reuse,
                    },
                    options: cuts.len(),
                });
            }
            for (page, versions) in data.page_versions() {
                decisions.push(Decision {
                    subject: Subject::Page {
                        file: path.clone(),
                        reuse,
                        page,
                    },
                    options: versions.len(),
                });
            }
        }
        decisions.sort();
        decisions
    }

    fn power_loss(&mut self, choices: &BTreeMap<Subject, usize>) {
        let choice = |subject: &Subject| choices.get(subject).copied().unwrap_or(0);
        let prefix = choice(&Subject::Namespace).min(self.pending.len());
        self.commit(prefix);
        self.pending.clear();
        for node in self.nodes.values_mut() {
            match node {
                Node::Directory(directory) => directory.live.clone_from(&directory.durable),
                Node::Data(data) => {
                    let Identity { path, reuse } = data.identity.clone();
                    let cuts = data.cuts();
                    let length = choice(&Subject::Length {
                        file: path.clone(),
                        reuse,
                    });
                    let cut = cuts[length.min(cuts.len() - 1)];
                    let image = data.settle(cut, |page| {
                        choice(&Subject::Page {
                            file: path.clone(),
                            reuse,
                            page,
                        })
                    });
                    data.live.clone_from(&image);
                    data.durable = image;
                    data.ops.clear();
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

    /// Free inodes nothing can reach any more.
    fn reclaim(&mut self) {
        let retained = self.reachable();
        self.nodes.retain(|inode, _| retained.contains(inode));
    }

    /// Count a call against every matching rule. The first rule the call
    /// completes fails it; any other rule it would complete waits for its
    /// next match.
    fn fault(&mut self, op: FaultOp, paths: &[&str]) -> Option<(usize, FaultEffect, FsError)> {
        let paths: Vec<String> = paths.iter().map(|path| normalized(path)).collect();
        let mut first = None;
        for (index, rule) in self.faults.iter_mut().enumerate() {
            let scope = normalized(&rule.fault.path);
            let matches =
                rule.fault.op == op && !rule.fired && paths.iter().any(|path| within(path, &scope));
            if !matches {
                continue;
            }
            if rule.seen == u64::MAX {
                continue;
            }
            if rule.seen + 1 < rule.fault.nth {
                rule.seen += 1;
            } else if first.is_none() {
                rule.seen += 1;
                rule.fired = true;
                let message = format!("injected {op:?} fault at {paths:?}");
                let error = match rule.fault.error {
                    FaultError::Io => FsError::Io(message),
                    FaultError::NoSpace => FsError::NoSpace(message),
                };
                first = Some((index, rule.fault.effect, error));
            }
        }
        first
    }

    /// Run `apply` under any fault the call triggers. An `After` fault whose
    /// operation fails on its own does not count as fired.
    fn faulted<T>(
        &mut self,
        op: FaultOp,
        paths: &[&str],
        apply: impl FnOnce(&mut Self) -> FsResult<T>,
    ) -> FsResult<T> {
        match self.fault(op, paths) {
            Some((_, FaultEffect::Before, error)) => Err(error),
            Some((rule, FaultEffect::After, error)) => match apply(self) {
                Ok(_) => Err(error),
                Err(own) => {
                    self.faults[rule].fired = false;
                    Err(own)
                }
            },
            None => apply(self),
        }
    }

    /// Create every missing directory on `parts`, durably, as `RealFs` does.
    fn create_dirs(&mut self, parts: &[String]) -> FsResult<()> {
        let mut inode = ROOT;
        for part in parts {
            let existing = self.directory(inode)?.live.get(part).copied();
            inode = if let Some(existing) = existing {
                self.directory(existing)?;
                existing
            } else {
                let identity = self.identity(inode, part);
                let created = self.allocate(Node::Directory(Directory::new(identity)));
                self.link(inode, part, created)?;
                self.commit_directory(inode);
                created
            };
        }
        Ok(())
    }

    fn unlink_tree(&mut self, directory: Inode) -> FsResult<()> {
        let children: Vec<(String, Inode)> = self
            .directory(directory)?
            .live
            .iter()
            .map(|(name, inode)| (name.clone(), *inode))
            .collect();
        for (name, child) in children {
            if self.is_directory(child) {
                self.unlink_tree(child)?;
            }
            self.unlink(directory, &name)?;
        }
        Ok(())
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

    /// Retire every fault that has not fired, as a crashed process's failing
    /// hardware path does not follow the restarted one.
    pub fn disarm_faults(&self) {
        for rule in &mut self.lock().faults {
            rule.seen = u64::MAX;
        }
    }

    /// Whether the fault fired and its call returned the injected error.
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

    /// How many times each path has been given to a file or directory, by
    /// creation or rename, over the device's life.
    #[must_use]
    pub fn names_ever(&self) -> BTreeMap<String, u64> {
        self.lock().named.clone()
    }

    /// Every file the current process sees, by path. After a power loss this
    /// is the device's durable state.
    #[must_use]
    pub fn files(&self) -> BTreeMap<String, Vec<u8>> {
        self.lock().files()
    }

    /// A device whose durable state holds exactly `files`, as a crash image
    /// is replayed.
    ///
    /// # Panics
    ///
    /// Panics if a path cannot be created, as when a file and a directory
    /// share a name.
    #[must_use]
    pub fn with_files(files: &BTreeMap<String, Vec<u8>>) -> Self {
        let device = Self::new();
        let fs = device.fs();
        for (path, bytes) in files {
            let mut file = fs
                .open(
                    &FsPath::new(path.as_str()),
                    OpenOptions {
                        mode: OpenMode::ReadWrite,
                        create: true,
                        create_new: true,
                        truncate: false,
                    },
                )
                .expect("create image file");
            file.write_at(0, Bytes::copy_from_slice(bytes))
                .expect("write image file");
            file.sync(Durability::Durable).expect("sync image file");
        }
        let mut state = device.lock();
        let pending = state.pending.len();
        state.commit(pending);
        drop(state);
        device
    }

    /// The choices the next power loss will make.
    #[must_use]
    pub fn decisions(&self) -> Vec<Decision> {
        self.lock().decisions()
    }

    /// Power is lost; each choice is drawn from `seed` and its subject.
    pub fn power_loss(&self, seed: u64) {
        self.power_loss_with(|decision| seeded_choice(seed, decision));
    }

    /// Power is lost; `choose` picks each decision's alternative, below its
    /// `options`. `choose` runs without the device lock.
    ///
    /// # Panics
    ///
    /// Panics if the device changes while `choose` runs.
    pub fn power_loss_with(&self, mut choose: impl FnMut(&Decision) -> usize) {
        let decisions = self.decisions();
        let choices: BTreeMap<Subject, usize> = decisions
            .iter()
            .map(|decision| (decision.subject.clone(), choose(decision)))
            .collect();
        let mut state = self.lock();
        assert!(
            state.decisions() == decisions,
            "the device changed while power-loss choices were made"
        );
        state.power_loss(&choices);
    }
}

/// A seeded choice that depends only on the decision's subject.
fn seeded_choice(seed: u64, decision: &Decision) -> usize {
    // FNV-1a over the subject's key, then SplitMix64 finalization.
    let mut hash: u64 = 0xcbf2_9ce4_8422_2325 ^ seed;
    for byte in decision.subject.key().bytes() {
        hash ^= u64::from(byte);
        hash = hash.wrapping_mul(0x0100_0000_01b3);
    }
    hash = (hash ^ (hash >> 30)).wrapping_mul(0xbf58_476d_1ce4_e5b9);
    hash = (hash ^ (hash >> 27)).wrapping_mul(0x94d0_49bb_1331_11eb);
    hash ^= hash >> 31;
    usize::try_from(hash % decision.options as u64).expect("a choice fits usize")
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
        let inode = state.faulted(FaultOp::Open, &[&path.0], |state| {
            let creating = opts.create || opts.create_new;
            if creating {
                if let Some((_, parents)) = parts.split_last() {
                    // `RealFs` creates missing parents durably.
                    state.create_dirs(parents)?;
                }
            }
            if (creating || opts.truncate) && opts.mode == OpenMode::ReadOnly {
                return Err(FsError::Io(format!(
                    "open {}: creating or truncating needs write access",
                    path.0
                )));
            }
            let (parent, name) = state.parent(&parts)?;
            let inode = match state.directory(parent)?.live.get(&name).copied() {
                // `RealFs` classifies everything but a missing file as `Io`.
                Some(_) if opts.create_new => {
                    return Err(FsError::Io(format!("open {}: file exists", path.0)))
                }
                Some(inode) if state.is_directory(inode) => {
                    return Err(FsError::Io(format!("open {}: is a directory", path.0)))
                }
                Some(inode) => inode,
                None if creating => {
                    let identity = state.identity(parent, &name);
                    let inode = state.allocate(Node::Data(Data::new(identity)));
                    state.link(parent, &name, inode)?;
                    inode
                }
                None => return Err(FsError::NotFound(path.0.clone())),
            };
            if opts.truncate {
                let data = state.data_mut(inode)?;
                if !data.live.is_empty() {
                    data.push(DataOp::Truncate { len: 0 });
                }
            }
            Ok(inode)
        })?;
        let generation = state.generation;
        *state.handles.entry((generation, inode)).or_default() += 1;
        Ok(CrashFile {
            fs: self.clone(),
            path: path.0.clone(),
            inode,
            writable: opts.mode == OpenMode::ReadWrite,
        })
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
        self.state()?
            .faulted(FaultOp::RemoveFile, &[&path.0], |state| {
                let inode = state.lookup(&parts)?;
                if state.is_directory(inode) {
                    return Err(FsError::Io(format!("is a directory: {}", path.0)));
                }
                let (parent, name) = state.parent(&parts)?;
                state.unlink(parent, &name)
            })
    }

    fn exists(&self, path: &FsPath) -> FsResult<bool> {
        let parts = Self::parts(path)?;
        self.state()?.faulted(FaultOp::Stat, &[&path.0], |state| {
            match state.lookup(&parts) {
                Ok(_) => Ok(true),
                Err(FsError::NotFound(_)) => Ok(false),
                Err(error) => Err(error),
            }
        })
    }

    fn metadata(&self, path: &FsPath) -> FsResult<Metadata> {
        let parts = Self::parts(path)?;
        self.state()?.faulted(FaultOp::Stat, &[&path.0], |state| {
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
        })
    }

    fn create_dir_all(&self, path: &FsPath) -> FsResult<()> {
        let parts = Self::parts(path)?;
        self.state()?
            .faulted(FaultOp::CreateDirAll, &[&path.0], |state| {
                state.create_dirs(&parts)
            })
    }

    fn list_dir(&self, path: &FsPath) -> FsResult<Vec<DirEntry>> {
        let parts = Self::parts(path)?;
        self.state()?
            .faulted(FaultOp::ListDir, &[&path.0], |state| {
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
            })
    }

    fn remove_dir_all(&self, path: &FsPath) -> FsResult<()> {
        let parts = Self::parts(path)?;
        self.state()?
            .faulted(FaultOp::RemoveDirAll, &[&path.0], |state| {
                let inode = state.lookup(&parts)?;
                state.directory(inode)?;
                let (parent, name) = state.parent(&parts)?;
                // Real deletion unlinks entry by entry, in order.
                state.unlink_tree(inode)?;
                state.unlink(parent, &name)
            })
    }

    fn sync_dir(&self, path: &FsPath, dur: Durability) -> FsResult<()> {
        let parts = Self::parts(path)?;
        let mut state = self.state()?;
        if dur == Durability::Unsafe {
            // `RealFs` does nothing for an unsafe directory sync.
            return Ok(());
        }
        state.faulted(FaultOp::SyncDir, &[&path.0], |state| {
            // `RealFs` opens the path first, so a missing one is `Io`, and
            // it fsyncs whatever it opened, a file included.
            let inode = state
                .lookup(&parts)
                .map_err(|error| FsError::Io(format!("open_dir {}: {error}", path.0)))?;
            if state.is_directory(inode) {
                state.commit_directory(inode);
            } else {
                state.data_mut(inode)?.sync();
            }
            Ok(())
        })
    }

    fn rename_atomic(&self, from: &FsPath, to: &FsPath) -> FsResult<()> {
        let from_parts = Self::parts(from)?;
        let to_parts = Self::parts(to)?;
        self.state()?
            .faulted(FaultOp::Rename, &[&from.0, &to.0], |state| {
                // `RealFs` creates the destination's parent durably first.
                if let Some((_, parents)) = to_parts.split_last() {
                    state.create_dirs(parents)?;
                }
                // `RealFs` reports every rename failure as `Io` (#832).
                let failed = |error: FsError| FsError::Io(format!("rename {}: {error}", to.0));
                let inode = state.lookup(&from_parts).map_err(failed)?;
                if from_parts == to_parts {
                    return Ok(());
                }
                if to_parts.starts_with(&from_parts) {
                    return Err(failed(FsError::Io(
                        "cannot move a directory inside itself".into(),
                    )));
                }
                let (from_parent, from_name) = state.parent(&from_parts).map_err(failed)?;
                let (to_parent, to_name) = state.parent(&to_parts).map_err(failed)?;
                if let Some(&target) = state.directory(to_parent)?.live.get(&to_name) {
                    let source_is_dir = state.is_directory(inode);
                    let target_is_dir = state.is_directory(target);
                    if source_is_dir != target_is_dir
                        || (target_is_dir && !state.directory(target)?.live.is_empty())
                    {
                        return Err(failed(FsError::Io(format!(
                            "cannot replace it with {}",
                            from.0
                        ))));
                    }
                }
                state.rename((from_parent, from_name), (to_parent, to_name), inode)
            })
    }
}

struct CrashFile {
    fs: CrashFs,
    path: String,
    inode: Inode,
    writable: bool,
}

impl CrashFile {
    fn write(&self, op: DataOp) -> FsResult<()> {
        let inode = self.inode;
        let writable = self.writable;
        self.fs
            .state()?
            .faulted(FaultOp::Write, &[&self.path], |state| {
                if !writable {
                    return Err(FsError::Io("cannot write a read-only handle".into()));
                }
                state.data_mut(inode)?.push(op);
                Ok(())
            })
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
        let inode = self.inode;
        let path = &self.path;
        self.fs.state()?.faulted(FaultOp::Read, &[path], |state| {
            if start == end {
                // `RealFs` reads nothing successfully, at any offset.
                state.data(inode)?;
                return Ok(Bytes::new());
            }
            state
                .data(inode)?
                .live
                .get(start..end)
                .map(Bytes::copy_from_slice)
                .ok_or_else(|| FsError::Io(format!("short read of '{path}'")))
        })
    }

    fn write_at(&mut self, offset: u64, bytes: Bytes) -> FsResult<()> {
        let (start, _) = checked_range(offset, bytes.len() as u64)?;
        if bytes.is_empty() {
            // `pwrite` of nothing changes nothing, not even the length, but a
            // crashed process still cannot reach the device.
            return self.fs.state().map(drop);
        }
        self.write(DataOp::Write {
            offset: start,
            bytes,
        })
    }

    fn truncate(&mut self, len: u64) -> FsResult<()> {
        let (_, len) = checked_range(0, len)?;
        self.write(DataOp::Truncate { len })
    }

    fn append(&mut self, bytes: Bytes) -> FsResult<u64> {
        let offset = self.fs.state()?.data(self.inode)?.live.len() as u64;
        checked_range(offset, bytes.len() as u64)?;
        self.write_at(offset, bytes)?;
        Ok(offset)
    }

    fn len(&self) -> FsResult<u64> {
        let inode = self.inode;
        self.fs
            .state()?
            .faulted(FaultOp::Stat, &[&self.path], |state| {
                Ok(state.data(inode)?.live.len() as u64)
            })
    }

    fn sync(&mut self, dur: Durability) -> FsResult<()> {
        let mut state = self.fs.state()?;
        if dur == Durability::Unsafe {
            return Ok(());
        }
        // A read-only descriptor can still synchronize its inode.
        match state.fault(FaultOp::Sync, &[&self.path]) {
            Some((_, FaultEffect::Before, error)) => {
                // After a writeback error the kernel marks the pages clean
                // without writing them; a later sync reports success.
                state.data_mut(self.inode)?.drop_writeback();
                Err(error)
            }
            Some((_, FaultEffect::After, error)) => {
                state.data_mut(self.inode)?.sync();
                Err(error)
            }
            None => {
                state.data_mut(self.inode)?.sync();
                Ok(())
            }
        }
    }
}

impl Drop for CrashFile {
    fn drop(&mut self) {
        // Unlinked inodes are freed at the next commit or crash.
        let mut state = self.fs.device.lock();
        let key = (self.fs.generation, self.inode);
        if let Some(count) = state.handles.get_mut(&key) {
            *count -= 1;
            if *count == 0 {
                state.handles.remove(&key);
            }
        }
    }
}

#[cfg(test)]
mod tests;
