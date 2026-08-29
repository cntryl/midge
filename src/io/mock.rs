//! In-memory mock filesystem implementation
//!
//! Deterministic, no I/O, suitable for testing.

use super::traits::{
    DirEntry, Durability, File, FileCaps, Fs, FsError, FsPath, FsResult, Metadata, OpenOptions,
};
use bytes::Bytes;
use parking_lot::Mutex;
use std::collections::HashMap;
#[cfg(test)]
use std::sync::atomic::{AtomicBool, Ordering};
use std::sync::Arc;

#[derive(Debug, Clone)]
struct MockFileData {
    data: Vec<u8>,
}

/// In-memory mock filesystem
#[derive(Debug, Clone, Default)]
pub struct MockFs {
    files: Arc<Mutex<HashMap<String, MockFileData>>>,
    #[cfg(test)]
    sync_dir_calls: Arc<Mutex<Vec<(FsPath, Durability)>>>,
    #[cfg(test)]
    fail_sync_dir: Arc<AtomicBool>,
}

impl MockFs {
    /// Create a new empty mock filesystem
    #[must_use]
    pub fn new() -> Self {
        Self::default()
    }

    /// Get file contents for testing
    #[cfg(test)]
    #[must_use]
    pub fn get_file(&self, path: &str) -> Option<Vec<u8>> {
        self.files.lock().get(path).map(|f| f.data.clone())
    }

    /// Clear all files
    #[cfg(test)]
    pub fn clear(&self) {
        self.files.lock().clear();
    }

    #[cfg(test)]
    pub(crate) fn set_sync_dir_failure(&self, fail: bool) {
        self.fail_sync_dir.store(fail, Ordering::Relaxed);
    }

    #[cfg(test)]
    pub(crate) fn sync_dir_calls(&self) -> Vec<(FsPath, Durability)> {
        self.sync_dir_calls.lock().clone()
    }
}

fn checked_range(offset: u64, len: u64, file_len: usize) -> FsResult<(usize, usize)> {
    let start = usize::try_from(offset)
        .map_err(|_| FsError::Io("offset does not fit usize".to_string()))?;
    let requested_end = offset
        .checked_add(len)
        .ok_or_else(|| FsError::Io("offset + len overflow".to_string()))?;
    let end = usize::try_from(requested_end)
        .map_err(|_| FsError::Io("end offset does not fit usize".to_string()))?;

    if start > file_len {
        return Err(FsError::Io("offset beyond file".to_string()));
    }

    Ok((start, end.min(file_len)))
}

fn checked_write_start(offset: u64) -> FsResult<usize> {
    usize::try_from(offset).map_err(|_| FsError::Io("offset does not fit usize".to_string()))
}

impl Fs for MockFs {
    fn open(&self, path: &FsPath, opts: OpenOptions) -> FsResult<Box<dyn File + '_>> {
        let mut files = self.files.lock();

        if opts.create_new && files.contains_key(&path.0) {
            return Err(FsError::AlreadyExists(path.0.clone()));
        }

        let file_data = files
            .entry(path.0.clone())
            .or_insert_with(|| MockFileData { data: Vec::new() });

        if opts.truncate {
            file_data.data.clear();
        }

        Ok(Box::new(MockFile {
            path: path.0.clone(),
            fs: self,
        }))
    }

    fn open_persistent_handle(&self, path: &FsPath, opts: OpenOptions) -> FsResult<Box<dyn File>> {
        let mut files = self.files.lock();

        if opts.create_new && files.contains_key(&path.0) {
            return Err(FsError::AlreadyExists(path.0.clone()));
        }

        let file_data = files
            .entry(path.0.clone())
            .or_insert_with(|| MockFileData { data: Vec::new() });

        if opts.truncate {
            file_data.data.clear();
        }
        let readonly_snapshot =
            (opts.mode == super::traits::OpenMode::ReadOnly).then(|| file_data.data.clone());
        drop(files);

        Ok(Box::new(MockPersistentFile {
            path: path.0.clone(),
            files: Arc::clone(&self.files),
            readonly_snapshot,
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
        Ok(self.files.lock().contains_key(&path.0))
    }

    fn metadata(&self, path: &FsPath) -> FsResult<Metadata> {
        let files = self.files.lock();
        if let Some(data) = files.get(&path.0) {
            Ok(Metadata {
                len: data.data.len() as u64,
            })
        } else {
            Err(FsError::NotFound(path.0.clone()))
        }
    }

    fn create_dir_all(&self, _path: &FsPath) -> FsResult<()> {
        // Mock filesystem doesn't track directories
        Ok(())
    }

    fn list_dir(&self, path: &FsPath) -> FsResult<Vec<DirEntry>> {
        let files = self.files.lock();
        let entries: Vec<_> = files
            .keys()
            .filter(|k| k.starts_with(&path.0))
            .map(|name| DirEntry {
                name: name.clone(),
                is_dir: false,
            })
            .collect();

        if entries.is_empty() && !path.0.is_empty() {
            return Err(FsError::NotFound(path.0.clone()));
        }

        Ok(entries)
    }

    fn remove_dir_all(&self, path: &FsPath) -> FsResult<()> {
        let mut files = self.files.lock();
        let before = files.len();
        files.retain(|k, _| !k.starts_with(&path.0));

        if files.len() == before {
            return Err(FsError::NotFound(path.0.clone()));
        }
        Ok(())
    }

    fn sync_dir(&self, path: &FsPath, dur: Durability) -> FsResult<()> {
        #[cfg(test)]
        {
            self.sync_dir_calls.lock().push((path.clone(), dur));
            if self.fail_sync_dir.load(Ordering::Relaxed) {
                return Err(FsError::Unavailable(
                    "injected directory sync failure".to_string(),
                ));
            }
        }
        #[cfg(not(test))]
        let _ = (path, dur);
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

impl File for MockFile<'_> {
    fn read_at(&self, offset: u64, len: u64) -> FsResult<Bytes> {
        let files = self.fs.files.lock();
        if let Some(data) = files.get(&self.path) {
            let (start, end) = checked_range(offset, len, data.data.len())?;
            let slice = &data.data[start..end];
            Ok(Bytes::from(slice.to_vec()))
        } else {
            Err(FsError::NotFound(self.path.clone()))
        }
    }

    fn write_at(&mut self, offset: u64, data: Bytes) -> FsResult<()> {
        let mut files = self.fs.files.lock();
        if let Some(file_data) = files.get_mut(&self.path) {
            let start = checked_write_start(offset)?;
            let end = start
                .checked_add(data.len())
                .ok_or_else(|| FsError::Io("write length overflow".to_string()))?;

            if end > file_data.data.len() {
                file_data.data.resize(end, 0);
            }

            file_data.data[start..end].copy_from_slice(&data);
            Ok(())
        } else {
            Err(FsError::NotFound(self.path.clone()))
        }
    }

    fn truncate(&mut self, len: u64) -> FsResult<()> {
        let len = usize::try_from(len)
            .map_err(|_| FsError::Io("truncate length does not fit usize".to_string()))?;
        let mut files = self.fs.files.lock();
        let file_data = files
            .get_mut(&self.path)
            .ok_or_else(|| FsError::NotFound(self.path.clone()))?;
        file_data.data.resize(len, 0);
        Ok(())
    }

    fn append(&mut self, data: Bytes) -> FsResult<u64> {
        let mut files = self.fs.files.lock();
        if let Some(file_data) = files.get_mut(&self.path) {
            let pos = file_data.data.len() as u64;
            file_data.data.extend_from_slice(&data);
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

    fn sync(&mut self, _dur: Durability) -> FsResult<()> {
        Ok(())
    }

    fn close(self: Box<Self>) -> FsResult<()> {
        Ok(())
    }

    fn caps(&self) -> FileCaps {
        FileCaps::empty()
    }
}

/// Persistent file handle (no lifetime dependency on Fs)
pub struct MockPersistentFile {
    path: String,
    files: Arc<Mutex<HashMap<String, MockFileData>>>,
    readonly_snapshot: Option<Vec<u8>>,
}

impl File for MockPersistentFile {
    fn read_at(&self, offset: u64, len: u64) -> FsResult<Bytes> {
        if let Some(snapshot) = &self.readonly_snapshot {
            let (start, end) = checked_range(offset, len, snapshot.len())?;
            return Ok(Bytes::copy_from_slice(&snapshot[start..end]));
        }
        let files = self.files.lock();
        if let Some(data) = files.get(&self.path) {
            let (start, end) = checked_range(offset, len, data.data.len())?;
            let slice = &data.data[start..end];
            Ok(Bytes::from(slice.to_vec()))
        } else {
            Err(FsError::NotFound(self.path.clone()))
        }
    }

    fn write_at(&mut self, offset: u64, data: Bytes) -> FsResult<()> {
        if self.readonly_snapshot.is_some() {
            return Err(FsError::Unsupported(
                "cannot write through a read-only persistent mock handle".to_string(),
            ));
        }
        let mut files = self.files.lock();
        if let Some(file_data) = files.get_mut(&self.path) {
            let start = checked_write_start(offset)?;
            let end = start
                .checked_add(data.len())
                .ok_or_else(|| FsError::Io("write length overflow".to_string()))?;

            if end > file_data.data.len() {
                file_data.data.resize(end, 0);
            }

            file_data.data[start..end].copy_from_slice(&data);
            Ok(())
        } else {
            Err(FsError::NotFound(self.path.clone()))
        }
    }

    fn truncate(&mut self, len: u64) -> FsResult<()> {
        if self.readonly_snapshot.is_some() {
            return Err(FsError::Unsupported(
                "cannot truncate through a read-only persistent mock handle".to_string(),
            ));
        }
        let len = usize::try_from(len)
            .map_err(|_| FsError::Io("truncate length does not fit usize".to_string()))?;
        let mut files = self.files.lock();
        let file_data = files
            .get_mut(&self.path)
            .ok_or_else(|| FsError::NotFound(self.path.clone()))?;
        file_data.data.resize(len, 0);
        Ok(())
    }

    fn append(&mut self, data: Bytes) -> FsResult<u64> {
        if self.readonly_snapshot.is_some() {
            return Err(FsError::Unsupported(
                "cannot append through a read-only persistent mock handle".to_string(),
            ));
        }
        let mut files = self.files.lock();
        if let Some(file_data) = files.get_mut(&self.path) {
            let pos = file_data.data.len() as u64;
            file_data.data.extend_from_slice(&data);
            Ok(pos)
        } else {
            Err(FsError::NotFound(self.path.clone()))
        }
    }

    fn len(&self) -> FsResult<u64> {
        if let Some(snapshot) = &self.readonly_snapshot {
            return Ok(snapshot.len() as u64);
        }
        let files = self.files.lock();
        if let Some(data) = files.get(&self.path) {
            Ok(data.data.len() as u64)
        } else {
            Err(FsError::NotFound(self.path.clone()))
        }
    }

    fn sync(&mut self, _dur: Durability) -> FsResult<()> {
        Ok(())
    }

    fn close(self: Box<Self>) -> FsResult<()> {
        Ok(())
    }

    fn caps(&self) -> FileCaps {
        FileCaps::empty()
    }

    fn try_lock_exclusive(&self) -> FsResult<()> {
        // Mock implementation: always succeed
        Ok(())
    }

    fn unlock(&self) -> FsResult<()> {
        // Mock implementation: always succeed
        Ok(())
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::io::OpenMode;

    #[test]
    fn should_read_written_data_when_writing() -> FsResult<()> {
        // Arrange
        let fs = MockFs::new();
        let path = FsPath::new("test.txt");

        // Act
        let mut file = fs.open(
            &path,
            OpenOptions {
                mode: OpenMode::ReadWrite,
                create: true,
                create_new: false,
                truncate: false,
            },
        )?;

        file.append(Bytes::from("hello"))?;
        drop(file);

        let file = fs.open(
            &path,
            OpenOptions {
                mode: OpenMode::ReadOnly,
                create: false,
                create_new: false,
                truncate: false,
            },
        )?;

        let data = file.read_at(0, 5)?;

        // Assert
        assert_eq!(data, Bytes::from("hello"));
        Ok(())
    }

    #[test]
    fn should_delete_file() -> FsResult<()> {
        // Arrange
        let fs = MockFs::new();
        let path = FsPath::new("test.txt");

        // Act
        let mut file = fs.open(
            &path,
            OpenOptions {
                mode: OpenMode::ReadWrite,
                create: true,
                create_new: false,
                truncate: false,
            },
        )?;
        file.append(Bytes::from("data"))?;
        drop(file);

        fs.remove_file(&path)?;

        // Assert
        assert!(!fs.exists(&path)?);
        Ok(())
    }

    #[test]
    fn should_keep_readonly_persistent_handle_valid_after_unlink() -> FsResult<()> {
        // Arrange
        let fs = MockFs::new();
        let path = FsPath::new("stable.txt");
        let mut writer = fs.open(
            &path,
            OpenOptions {
                mode: OpenMode::ReadWrite,
                create: true,
                create_new: false,
                truncate: false,
            },
        )?;
        writer.append(Bytes::from_static(b"stable bytes"))?;
        drop(writer);
        let reader = fs.open_persistent_handle(
            &path,
            OpenOptions {
                mode: OpenMode::ReadOnly,
                create: false,
                create_new: false,
                truncate: false,
            },
        )?;
        fs.remove_file(&path)?;

        // Act
        let bytes = reader.read_at(0, 12)?;

        // Assert
        assert_eq!(bytes.as_ref(), b"stable bytes");
        assert!(!fs.exists(&path)?);
        Ok(())
    }

    #[test]
    fn should_zero_extend_regular_file_when_truncating_to_larger_length() -> FsResult<()> {
        // Arrange
        let fs = MockFs::new();
        let path = FsPath::new("grow-regular.txt");
        let mut file = fs.open(
            &path,
            OpenOptions {
                mode: OpenMode::ReadWrite,
                create: true,
                create_new: false,
                truncate: false,
            },
        )?;
        file.append(Bytes::from_static(b"abc"))?;

        // Act
        file.truncate(5)?;

        // Assert
        assert_eq!(file.len()?, 5);
        assert_eq!(file.read_at(0, 5)?.as_ref(), b"abc\0\0");
        Ok(())
    }

    #[test]
    fn should_zero_extend_persistent_file_when_truncating_to_larger_length() -> FsResult<()> {
        // Arrange
        let fs = MockFs::new();
        let path = FsPath::new("grow-persistent.txt");
        let mut file = fs.open_persistent_handle(
            &path,
            OpenOptions {
                mode: OpenMode::ReadWrite,
                create: true,
                create_new: false,
                truncate: false,
            },
        )?;
        file.append(Bytes::from_static(b"abc"))?;

        // Act
        file.truncate(5)?;

        // Assert
        assert_eq!(file.len()?, 5);
        assert_eq!(file.read_at(0, 5)?.as_ref(), b"abc\0\0");
        Ok(())
    }
}
