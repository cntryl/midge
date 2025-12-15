//! Real filesystem implementation
//!
//! Direct mapping to std::fs with proper path sanitization.
//! Suitable for production use.

use super::traits::*;
use std::fs;
use std::path::{Component, Path, PathBuf};

/// Real filesystem backend
pub struct RealFs {
    base_path: PathBuf,
}

impl RealFs {
    /// Create a new real filesystem rooted at `base_path`
    pub fn new(base_path: impl AsRef<Path>) -> FsResult<Self> {
        let path = base_path.as_ref().to_path_buf();
        fs::create_dir_all(&path).map_err(|e| FsError::Io(e.to_string()))?;
        Ok(Self { base_path: path })
    }

    /// Compute sanitized full path, preventing directory traversal
    fn full_path(&self, rel: &FsPath) -> PathBuf {
        let mut out = self.base_path.clone();
        for component in Path::new(&rel.0).components() {
            match component {
                Component::Normal(part) => out.push(part),
                Component::CurDir => {}
                Component::ParentDir => {}
                Component::RootDir => {}
                Component::Prefix(_) => {}
            }
        }
        out
    }
}

impl Fs for RealFs {
    fn open(&self, path: &FsPath, opts: OpenOptions) -> FsResult<Box<dyn File + '_>> {
        let full = self.full_path(path);
        let mut std_opts = fs::OpenOptions::new();

        match opts.mode {
            OpenMode::ReadOnly => {
                std_opts.read(true);
            }
            OpenMode::ReadWrite => {
                std_opts.read(true).write(true);
            }
        }

        if opts.create {
            std_opts.create(true);
        }
        if opts.create_new {
            std_opts.create_new(true);
        }
        if opts.truncate {
            std_opts.truncate(true);
        }

        let file = std_opts
            .open(&full)
            .map_err(|e| FsError::Io(e.to_string()))?;

        Ok(Box::new(RealFile { file }))
    }

    fn remove_file(&self, path: &FsPath) -> FsResult<()> {
        let full = self.full_path(path);
        fs::remove_file(&full).map_err(|e| FsError::Io(e.to_string()))
    }

    fn exists(&self, path: &FsPath) -> FsResult<bool> {
        Ok(self.full_path(path).exists())
    }

    fn metadata(&self, path: &FsPath) -> FsResult<Metadata> {
        let meta = fs::metadata(self.full_path(path)).map_err(|e| FsError::Io(e.to_string()))?;
        Ok(Metadata { len: meta.len() })
    }

    fn create_dir_all(&self, path: &FsPath) -> FsResult<()> {
        fs::create_dir_all(self.full_path(path)).map_err(|e| FsError::Io(e.to_string()))
    }

    fn list_dir(&self, path: &FsPath) -> FsResult<Vec<DirEntry>> {
        let entries = fs::read_dir(self.full_path(path))
            .map_err(|e| FsError::Io(e.to_string()))?
            .map(|entry| {
                let entry = entry.map_err(|e| FsError::Io(e.to_string()))?;
                let name = entry.file_name().to_string_lossy().to_string();
                let is_dir = entry
                    .file_type()
                    .map_err(|e| FsError::Io(e.to_string()))?
                    .is_dir();
                Ok(DirEntry { name, is_dir })
            })
            .collect::<Result<Vec<_>, _>>()?;
        Ok(entries)
    }

    fn remove_dir_all(&self, path: &FsPath) -> FsResult<()> {
        fs::remove_dir_all(self.full_path(path)).map_err(|e| FsError::Io(e.to_string()))
    }

    fn sync_dir(&self, path: &FsPath, _dur: Durability) -> FsResult<()> {
        // On Unix could fsync directory, but limited support on Windows
        // For now, no-op but safe
        let _ = path;
        Ok(())
    }

    fn rename_atomic(&self, from: &FsPath, to: &FsPath) -> FsResult<()> {
        let from_full = self.full_path(from);
        let to_full = self.full_path(to);
        fs::rename(&from_full, &to_full).map_err(|e| FsError::Io(e.to_string()))
    }
}

pub struct RealFile {
    file: fs::File,
}

impl File for RealFile {
    fn read_at(&self, offset: u64, len: u64) -> FsResult<bytes::Bytes> {
        use std::io::{Read, Seek, SeekFrom};

        let mut file = &self.file;
        file.seek(SeekFrom::Start(offset))
            .map_err(|e| FsError::Io(e.to_string()))?;

        let mut buf = vec![0; len as usize];
        file.read_exact(&mut buf)
            .map_err(|e| FsError::Io(e.to_string()))?;

        Ok(bytes::Bytes::from(buf))
    }

    fn write_at(&mut self, offset: u64, data: bytes::Bytes) -> FsResult<()> {
        use std::io::{Seek, SeekFrom, Write};

        self.file
            .seek(SeekFrom::Start(offset))
            .map_err(|e| FsError::Io(e.to_string()))?;
        self.file
            .write_all(&data)
            .map_err(|e| FsError::Io(e.to_string()))?;
        Ok(())
    }

    fn append(&mut self, data: bytes::Bytes) -> FsResult<u64> {
        use std::io::{Seek, SeekFrom, Write};

        let pos = self
            .file
            .seek(SeekFrom::End(0))
            .map_err(|e| FsError::Io(e.to_string()))?;
        self.file
            .write_all(&data)
            .map_err(|e| FsError::Io(e.to_string()))?;
        Ok(pos)
    }

    fn len(&self) -> FsResult<u64> {
        let meta = self
            .file
            .metadata()
            .map_err(|e| FsError::Io(e.to_string()))?;
        Ok(meta.len())
    }

    fn sync(&mut self, dur: Durability) -> FsResult<()> {
        match dur {
            Durability::Unsafe => Ok(()),
            Durability::Durable => self
                .file
                .sync_all()
                .map_err(|e| FsError::Io(e.to_string())),
        }
    }

    fn close(self: Box<Self>) -> FsResult<()> {
        Ok(())
    }

    fn caps(&self) -> FileCaps {
        FileCaps::empty()
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use tempfile::TempDir;

    #[test]
    fn should_create_real_fs() -> FsResult<()> {
        let temp = TempDir::new().map_err(|e| FsError::Io(e.to_string()))?;
        let _fs = RealFs::new(temp.path())?;
        Ok(())
    }

    #[test]
    fn should_write_and_read_file() -> FsResult<()> {
        let temp = TempDir::new().map_err(|e| FsError::Io(e.to_string()))?;
        let fs = RealFs::new(temp.path())?;

        let path = FsPath::new("test.txt");
        let mut file = fs.open(&path, OpenOptions {
            mode: OpenMode::ReadWrite,
            create: true,
            create_new: false,
            truncate: false,
        })?;

        file.append(bytes::Bytes::from("hello"))?;
        drop(file);

        let file = fs.open(&path, OpenOptions {
            mode: OpenMode::ReadOnly,
            create: false,
            create_new: false,
            truncate: false,
        })?;

        let data = file.read_at(0, 5)?;
        assert_eq!(data, bytes::Bytes::from("hello"));
        Ok(())
    }

    #[test]
    fn should_sanitize_path_traversal() -> FsResult<()> {
        let temp = TempDir::new().map_err(|e| FsError::Io(e.to_string()))?;
        let fs = RealFs::new(temp.path())?;

        let path = FsPath::new("../escape.txt");
        let mut file = fs.open(&path, OpenOptions {
            mode: OpenMode::ReadWrite,
            create: true,
            create_new: false,
            truncate: false,
        })?;

        file.append(bytes::Bytes::from("data"))?;
        drop(file);

        // File should be in temp dir, not parent
        assert!(fs.exists(&FsPath::new("escape.txt"))?);
        Ok(())
    }
}
