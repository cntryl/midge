//! In-memory mock filesystem implementation
//!
//! Deterministic, no I/O, suitable for testing.

use super::traits::*;
use bytes::Bytes;
use parking_lot::Mutex;
use std::collections::HashMap;
use std::sync::Arc;

#[derive(Debug, Clone)]
struct MockFileData {
    data: Vec<u8>,
}

/// In-memory mock filesystem
#[derive(Debug, Clone, Default)]
pub struct MockFs {
    files: Arc<Mutex<HashMap<String, MockFileData>>>,
}

impl MockFs {
    /// Create a new empty mock filesystem
    pub fn new() -> Self {
        Self::default()
    }

    /// Get file contents for testing
    #[allow(dead_code)]
    pub fn get_file(&self, path: &str) -> Option<Vec<u8>> {
        self.files.lock().get(path).map(|f| f.data.clone())
    }

    /// Clear all files
    #[allow(dead_code)]
    pub fn clear(&self) {
        self.files.lock().clear();
    }
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

    fn open_persistent_handle(
        &self,
        path: &FsPath,
        opts: OpenOptions,
    ) -> FsResult<Box<dyn File>> {
        let mut files = self.files.lock();

        if opts.create_new && files.contains_key(&path.0) {
            return Err(FsError::AlreadyExists(path.0.clone()));
        }

        files
            .entry(path.0.clone())
            .or_insert_with(|| MockFileData { data: Vec::new() });

        if opts.truncate {
            if let Some(file_data) = files.get_mut(&path.0) {
                file_data.data.clear();
            }
        }

        Ok(Box::new(MockPersistentFile {
            path: path.0.clone(),
            files: Arc::clone(&self.files),
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
}

impl File for MockPersistentFile {
    fn read_at(&self, offset: u64, len: u64) -> FsResult<Bytes> {
        let files = self.files.lock();
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
        let mut files = self.files.lock();
        if let Some(file_data) = files.get_mut(&self.path) {
            let start = offset as usize;
            let end = start + data.len();

            if end > file_data.data.len() {
                file_data.data.resize(end, 0);
            }

            file_data.data[start..end].copy_from_slice(&data);
            Ok(())
        } else {
            Err(FsError::NotFound(self.path.clone()))
        }
    }

    fn append(&mut self, data: Bytes) -> FsResult<u64> {
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
}
