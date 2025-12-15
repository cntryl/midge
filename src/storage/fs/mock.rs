use std::collections::HashMap;

use bytes::Bytes;
use parking_lot::Mutex;

use super::{DirEntry, Durability, File, Fs, FsError, FsPath, FsResult, FileCaps, IoSlice, IoSliceMut, Metadata, OpenOptions, ReadRange};

#[derive(Debug, Default)]
pub struct MockFs {
    files: Mutex<HashMap<String, MockFileData>>,
    dirs: Mutex<HashMap<String, Vec<String>>>,
}

#[derive(Debug, Clone)]
struct MockFileData {
    data: Vec<u8>,
    synced: bool,
}

impl MockFs {
    pub fn new() -> Self {
        Self::default()
    }
}

impl Fs for MockFs {
    fn open(&self, path: &FsPath, opts: OpenOptions) -> FsResult<Box<dyn File + '_>> {
        let mut files = self.files.lock();
        let data = files.entry(path.0.clone()).or_insert_with(|| MockFileData {
            data: Vec::new(),
            synced: false,
        });
        if opts.truncate {
            data.data.clear();
        }
        if opts.create_new && !data.data.is_empty() {
            return Err(FsError::AlreadyExists(path.0.clone()));
        }
        Ok(Box::new(MockFile {
            path: path.0.clone(),
            fs: self,
        }))
    }

    fn remove_file(&self, path: &FsPath) -> FsResult<()> {
        let mut files = self.files.lock();
        if files.remove(&path.0).is_none() {
            return Err(FsError::NotFound(path.0.clone()));
        }
        Ok(())
    }

    fn exists(&self, path: &FsPath) -> FsResult<bool> {
        let files = self.files.lock();
        Ok(files.contains_key(&path.0))
    }

    fn metadata(&self, path: &FsPath) -> FsResult<Metadata> {
        let files = self.files.lock();
        if let Some(data) = files.get(&path.0) {
            Ok(Metadata { len: data.data.len() as u64 })
        } else {
            Err(FsError::NotFound(path.0.clone()))
        }
    }

    fn create_dir_all(&self, path: &FsPath) -> FsResult<()> {
        let mut dirs = self.dirs.lock();
        dirs.entry(path.0.clone()).or_default();
        Ok(())
    }

    fn list_dir(&self, path: &FsPath) -> FsResult<Vec<DirEntry>> {
        let dirs = self.dirs.lock();
        if let Some(entries) = dirs.get(&path.0) {
            Ok(entries.iter().map(|name| DirEntry {
                name: name.clone(),
                is_dir: false, // For simplicity, assume all are files
            }).collect())
        } else {
            Err(FsError::NotFound(path.0.clone()))
        }
    }

    fn remove_dir_all(&self, path: &FsPath) -> FsResult<()> {
        let mut dirs = self.dirs.lock();
        if dirs.remove(&path.0).is_none() {
            return Err(FsError::NotFound(path.0.clone()));
        }
        Ok(())
    }

    fn sync_dir(&self, _path: &FsPath, _dur: Durability) -> FsResult<()> {
        Ok(())
    }

    fn rename_atomic(&self, from: &FsPath, to: &FsPath) -> FsResult<()> {
        let mut files = self.files.lock();
        if let Some(data) = files.remove(&from.0) {
            files.insert(to.0.clone(), data);
            Ok(())
        } else {
            Err(FsError::NotFound(from.0.clone()))
        }
    }
}

pub struct MockFile<'a> {
    path: String,
    fs: &'a MockFs,
}

impl<'a> File for MockFile<'a> {
    fn read_at(&self, offset: u64, len: u64) -> FsResult<Bytes> {
        let files = self.fs.files.lock();
        if let Some(data) = files.get(&self.path) {
            let start = offset as usize;
            let end = (offset + len) as usize;
            if start > data.data.len() {
                return Err(FsError::Io("offset beyond file".to_string()));
            }
            let slice = &data.data[start..end.min(data.data.len())];
            Ok(Bytes::from(slice.to_vec()))
        } else {
            Err(FsError::NotFound(self.path.clone()))
        }
    }

    fn write_at(&mut self, offset: u64, data: Bytes) -> FsResult<()> {
        let mut files = self.fs.files.lock();
        if let Some(file_data) = files.get_mut(&self.path) {
            let start = offset as usize;
            let end = start + data.len();
            if end > file_data.data.len() {
                file_data.data.resize(end, 0);
            }
            file_data.data[start..end].copy_from_slice(&data);
            file_data.synced = false;
            Ok(())
        } else {
            Err(FsError::NotFound(self.path.clone()))
        }
    }

    fn append(&mut self, data: Bytes) -> FsResult<u64> {
        let mut files = self.fs.files.lock();
        if let Some(file_data) = files.get_mut(&self.path) {
            let pos = file_data.data.len() as u64;
            file_data.data.extend_from_slice(&data);
            file_data.synced = false;
            Ok(pos)
        } else {
            Err(FsError::NotFound(self.path.clone()))
        }
    }

    fn len(&self) -> FsResult<u64> {
        let files = self.fs.files.lock();
        if let Some(data) = files.get(&self.path) {
            Ok(data.data.len() as u64)
        } else {
            Err(FsError::NotFound(self.path.clone()))
        }
    }

    fn sync(&mut self, dur: Durability) -> FsResult<()> {
        if dur == Durability::Durable {
            let mut files = self.fs.files.lock();
            if let Some(data) = files.get_mut(&self.path) {
                data.synced = true;
            }
        }
        Ok(())
    }

    fn close(self: Box<Self>) -> FsResult<()> {
        Ok(())
    }

    fn writev_at(&mut self, offset: u64, bufs: &[IoSlice<'_>]) -> FsResult<u64> {
        let mut total = 0usize;
        for b in bufs { total += b.len(); }
        let mut tmp = Vec::with_capacity(total);
        for b in bufs { tmp.extend_from_slice(b); }
        self.write_at(offset, bytes::Bytes::from(tmp))?;
        Ok(total as u64)
    }

    fn appendv(&mut self, bufs: &[IoSlice<'_>]) -> FsResult<u64> {
        let mut total = 0usize;
        for b in bufs { total += b.len(); }
        let mut tmp = Vec::with_capacity(total);
        for b in bufs { tmp.extend_from_slice(b); }
        self.append(bytes::Bytes::from(tmp))
    }

    fn readv_at(&self, offset: u64, bufs: &mut [IoSliceMut<'_>]) -> FsResult<u64> {
        let need: usize = bufs.iter().map(|b| b.len()).sum();
        let data = self.read_at(offset, need as u64)?;
        let mut written = 0usize;
        let mut cursor = &data[..];
        for b in bufs {
            let n = b.len().min(cursor.len());
            b[..n].copy_from_slice(&cursor[..n]);
            cursor = &cursor[n..];
            written += n;
        }
        Ok(written as u64)
    }

    fn read_ranges(&self, ranges: &[ReadRange]) -> FsResult<Vec<bytes::Bytes>> {
        let mut out = Vec::with_capacity(ranges.len());
        for r in ranges {
            out.push(self.read_at(r.offset, r.len as u64)?);
        }
        Ok(out)
    }

    fn caps(&self) -> FileCaps { FileCaps::empty() }
}