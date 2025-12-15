use std::fs::{self, File as StdFile, OpenOptions as StdOpenOptions};
use std::io::{Read, Seek, SeekFrom, Write};
use std::path::Path;

use bytes::Bytes;

use super::{DirEntry, Durability, File, Fs, FsError, FsPath, FsResult, FileCaps, Metadata, OpenMode, OpenOptions};

pub struct RealFs;

impl RealFs {
    pub fn new() -> Self {
        Self
    }
}

impl Default for RealFs {
    fn default() -> Self {
        Self::new()
    }
}

impl Fs for RealFs {
    fn open(&self, path: &FsPath, opts: OpenOptions) -> FsResult<Box<dyn File + '_>> {
        let mut std_opts = StdOpenOptions::new();
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
        let file = std_opts.open(&path.0).map_err(|e| FsError::Io(e.to_string()))?;
        Ok(Box::new(RealFile { file }))
    }

    fn remove_file(&self, path: &FsPath) -> FsResult<()> {
        fs::remove_file(&path.0).map_err(|e| FsError::Io(e.to_string()))
    }

    fn exists(&self, path: &FsPath) -> FsResult<bool> {
        Ok(Path::new(&path.0).exists())
    }

    fn metadata(&self, path: &FsPath) -> FsResult<Metadata> {
        let meta = fs::metadata(&path.0).map_err(|e| FsError::Io(e.to_string()))?;
        Ok(Metadata { len: meta.len() })
    }

    fn create_dir_all(&self, path: &FsPath) -> FsResult<()> {
        fs::create_dir_all(&path.0).map_err(|e| FsError::Io(e.to_string()))
    }

    fn list_dir(&self, path: &FsPath) -> FsResult<Vec<DirEntry>> {
        let entries = fs::read_dir(&path.0)
            .map_err(|e| FsError::Io(e.to_string()))?
            .map(|entry| {
                let entry = entry.map_err(|e| FsError::Io(e.to_string()))?;
                let name = entry.file_name().to_string_lossy().to_string();
                let is_dir = entry.file_type().map_err(|e| FsError::Io(e.to_string()))?.is_dir();
                Ok(DirEntry { name, is_dir })
            })
            .collect::<Result<Vec<_>, _>>()?;
        Ok(entries)
    }

    fn remove_dir_all(&self, path: &FsPath) -> FsResult<()> {
        fs::remove_dir_all(&path.0).map_err(|e| FsError::Io(e.to_string()))
    }

    fn sync_dir(&self, _path: &FsPath, _dur: Durability) -> FsResult<()> {
        // For simplicity, no-op. In real implementation, might need to fsync the directory.
        // On Unix, can open dir and fsync, but on Windows, limited support.
        Ok(())
    }

    fn rename_atomic(&self, from: &FsPath, to: &FsPath) -> FsResult<()> {
        fs::rename(&from.0, &to.0).map_err(|e| FsError::Io(e.to_string()))
    }
}

pub struct RealFile {
    file: StdFile,
}

impl File for RealFile {
    fn read_at(&self, offset: u64, len: u64) -> FsResult<Bytes> {
        let mut file = &self.file;
        file.seek(SeekFrom::Start(offset)).map_err(|e| FsError::Io(e.to_string()))?;
        let mut buf = vec![0; len as usize];
        file.read_exact(&mut buf).map_err(|e| FsError::Io(e.to_string()))?;
        Ok(Bytes::from(buf))
    }

    fn write_at(&mut self, offset: u64, data: Bytes) -> FsResult<()> {
        self.file.seek(SeekFrom::Start(offset)).map_err(|e| FsError::Io(e.to_string()))?;
        self.file.write_all(&data).map_err(|e| FsError::Io(e.to_string()))?;
        Ok(())
    }

    fn append(&mut self, data: Bytes) -> FsResult<u64> {
        let pos = self.file.seek(SeekFrom::End(0)).map_err(|e| FsError::Io(e.to_string()))?;
        self.file.write_all(&data).map_err(|e| FsError::Io(e.to_string()))?;
        Ok(pos)
    }

    fn len(&self) -> FsResult<u64> {
        let meta = self.file.metadata().map_err(|e| FsError::Io(e.to_string()))?;
        Ok(meta.len())
    }

    fn sync(&mut self, dur: Durability) -> FsResult<()> {
        match dur {
            Durability::Unsafe => Ok(()),
            Durability::Durable => {
                self.file.sync_all().map_err(|e| FsError::Io(e.to_string()))?;
                Ok(())
            }
        }
    }

    fn close(self: Box<Self>) -> FsResult<()> {
        // File is automatically closed when dropped, but we can explicitly sync if needed.
        Ok(())
    }

    fn caps(&self) -> FileCaps { FileCaps::empty() }
}