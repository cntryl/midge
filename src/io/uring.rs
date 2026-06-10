//! Linux `io_uring` filesystem implementation.
//!
//! This backend keeps the same `Fs` / `File` contract as `RealFs`, but routes
//! the hot file operations through `io_uring` when the feature is enabled.

#[cfg(target_os = "linux")]
mod linux_impl {

    use crate::io::traits::*;
    use std::fs;
    use std::io;
    use std::path::{Component, Path, PathBuf};
    use std::sync::Mutex;

    use io_uring::{opcode, types, IoUring};
    use std::os::unix::fs::FileExt;
    use std::os::unix::io::AsRawFd;

    const RING_ENTRIES: u32 = 8;

    /// Linux filesystem backend using `io_uring` for file I/O.
    pub struct UringFs {
        base_path: PathBuf,
    }

    impl UringFs {
        /// Create a new Linux filesystem rooted at `base_path`.
        pub fn new(base_path: impl AsRef<Path>) -> FsResult<Self> {
            let path = base_path.as_ref().to_path_buf();
            fs::create_dir_all(&path).map_err(|e| io_err("create_dir_all", &path, e))?;
            Ok(Self { base_path: path })
        }

        fn full_path(&self, rel: &FsPath) -> PathBuf {
            let mut out = self.base_path.clone();
            for component in Path::new(&rel.0).components() {
                match component {
                    Component::Normal(part) => out.push(part),
                    Component::CurDir
                    | Component::ParentDir
                    | Component::RootDir
                    | Component::Prefix(_) => {}
                }
            }
            out
        }

        fn parent_dir(full_path: &Path) -> Option<&Path> {
            full_path.parent().filter(|p| !p.as_os_str().is_empty())
        }

        fn open_inner(&self, path: &FsPath, opts: OpenOptions) -> FsResult<Box<dyn File + '_>> {
            let full = self.full_path(path);

            if opts.create || opts.create_new {
                if let Some(parent) = Self::parent_dir(&full) {
                    fs::create_dir_all(parent)
                        .map_err(|e| FsError::Io(format!("create_dir_all: {e}")))?;
                }
            }

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
            let len = file
                .metadata()
                .map_err(|e| FsError::Io(e.to_string()))?
                .len();

            Ok(Box::new(UringFile::new(file, len, full)?))
        }
    }

    impl Fs for UringFs {
        fn open(&self, path: &FsPath, opts: OpenOptions) -> FsResult<Box<dyn File + '_>> {
            self.open_inner(path, opts)
        }

        fn open_persistent_handle(
            &self,
            path: &FsPath,
            opts: OpenOptions,
        ) -> FsResult<Box<dyn File>> {
            let full = self.full_path(path);

            if opts.create || opts.create_new {
                if let Some(parent) = Self::parent_dir(&full) {
                    fs::create_dir_all(parent)
                        .map_err(|e| FsError::Io(format!("create_dir_all: {e}")))?;
                }
            }

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
            let len = file
                .metadata()
                .map_err(|e| FsError::Io(e.to_string()))?
                .len();
            Ok(Box::new(UringFile::new(file, len, full)?))
        }

        fn remove_file(&self, path: &FsPath) -> FsResult<()> {
            let full = self.full_path(path);
            fs::remove_file(&full).map_err(|e| io_err("remove_file", &full, e))
        }

        fn exists(&self, path: &FsPath) -> FsResult<bool> {
            Ok(self.full_path(path).exists())
        }

        fn metadata(&self, path: &FsPath) -> FsResult<Metadata> {
            let full = self.full_path(path);
            let meta = fs::metadata(&full).map_err(|e| io_err("metadata", &full, e))?;
            Ok(Metadata { len: meta.len() })
        }

        fn create_dir_all(&self, path: &FsPath) -> FsResult<()> {
            let full = self.full_path(path);
            fs::create_dir_all(&full).map_err(|e| io_err("create_dir_all", &full, e))
        }

        fn list_dir(&self, path: &FsPath) -> FsResult<Vec<DirEntry>> {
            let full = self.full_path(path);
            let entries = fs::read_dir(&full)
                .map_err(|e| io_err("read_dir", &full, e))?
                .map(|entry| {
                    let entry = entry.map_err(|e| io_err("read_dir_entry", &full, e))?;
                    let name = entry.file_name().to_string_lossy().to_string();
                    let is_dir = entry
                        .file_type()
                        .map_err(|e| io_err("file_type", &full, e))?
                        .is_dir();
                    Ok(DirEntry { name, is_dir })
                })
                .collect::<Result<Vec<_>, FsError>>()?;
            Ok(entries)
        }

        fn remove_dir_all(&self, path: &FsPath) -> FsResult<()> {
            let full = self.full_path(path);
            fs::remove_dir_all(&full).map_err(|e| io_err("remove_dir_all", &full, e))
        }

        fn sync_dir(&self, path: &FsPath, dur: Durability) -> FsResult<()> {
            if dur == Durability::Unsafe {
                return Ok(());
            }

            let full = self.full_path(path);
            let dir = fs::File::open(&full).map_err(|e| io_err("open_dir", &full, e))?;
            dir.sync_all().map_err(|e| io_err("fsync_dir", &full, e))?;
            Ok(())
        }

        fn rename_atomic(&self, from: &FsPath, to: &FsPath) -> FsResult<()> {
            let from_full = self.full_path(from);
            let to_full = self.full_path(to);

            if let Some(parent) = Self::parent_dir(&to_full) {
                fs::create_dir_all(parent).map_err(|e| io_err("create_dir_all", parent, e))?;
            }

            fs::rename(&from_full, &to_full).map_err(|e| io_err("rename", &to_full, e))
        }
    }

    pub struct UringFile {
        file: fs::File,
        ring: Option<Mutex<IoUring>>,
        current_pos: u64,
        path: PathBuf,
    }

    impl UringFile {
        fn new(file: fs::File, current_pos: u64, path: PathBuf) -> FsResult<Self> {
            let ring = match IoUring::new(RING_ENTRIES) {
                Ok(ring) => Some(Mutex::new(ring)),
                Err(err) => {
                    tracing::debug!(error = %err, path = %path.display(), "io_uring unavailable; falling back to std I/O");
                    None
                }
            };

            Ok(Self {
                file,
                ring,
                current_pos,
                path,
            })
        }

        fn len_u64(len: u64) -> FsResult<usize> {
            usize::try_from(len).map_err(|_| FsError::Io(format!("len too large: {len}")))
        }

    fn current_file_len(&self) -> FsResult<u64> {
        Ok(self
            .file
            .metadata()
            .map_err(|e| FsError::Io(format!("metadata(len) {}: {e}", self.path.display())))?
            .len())
        }

        fn io_uring_error(&self, op: &str, result: i32) -> FsError {
            let err = io::Error::from_raw_os_error(-result);
            FsError::Io(format!("{op} {}: {err}", self.path.display()))
        }

        fn io_uring_unexpected_eof(&self, op: &str, expected: usize, got: usize) -> FsError {
            FsError::Io(format!(
                "{op} {}: unexpected EOF (expected {expected} bytes, got {got})",
                self.path.display()
            ))
        }

        fn submit_and_wait_one(&self, entry: io_uring::squeue::Entry) -> FsResult<i32> {
            let Some(ring) = &self.ring else {
                return Err(FsError::Unavailable(
                    "io_uring backend not available".into(),
                ));
            };

            let mut ring = ring.lock().expect("io_uring mutex poisoned");
            unsafe {
                ring.submission()
                    .push(&entry)
                    .map_err(|_| FsError::Unavailable("io_uring submission queue full".into()))?;
            }
            ring.submit_and_wait(1)
                .map_err(|e| FsError::Io(format!("io_uring submit_and_wait: {e}")))?;

            let cqe = ring
                .completion()
                .next()
                .ok_or_else(|| FsError::Io("io_uring completion queue empty".into()))?;
            Ok(cqe.result())
        }

        fn read_exact_with_std(&self, offset: u64, len: usize) -> FsResult<bytes::Bytes> {
            let mut buf = vec![0u8; len];
            let mut read = 0usize;
            while read < len {
                let n = self
                    .file
                    .read_at(&mut buf[read..], offset + read as u64)
                    .map_err(|e| FsError::Io(format!("pread {}: {e}", self.path.display())))?;
                if n == 0 {
                    return Err(self.io_uring_unexpected_eof("read_at", len, read));
                }
                read += n;
            }
            Ok(bytes::Bytes::from(buf))
        }

        fn write_all_with_std(&self, offset: u64, data: &[u8]) -> FsResult<()> {
            let mut written = 0usize;
            while written < data.len() {
                let n = self
                    .file
                    .write_at(&data[written..], offset + written as u64)
                    .map_err(|e| FsError::Io(format!("pwrite {}: {e}", self.path.display())))?;
                if n == 0 {
                    return Err(FsError::Io(format!(
                        "pwrite {}: wrote 0 bytes",
                        self.path.display()
                    )));
                }
                written += n;
            }
            Ok(())
        }

        fn sync_with_std(&self, dur: Durability) -> FsResult<()> {
            match dur {
                Durability::Unsafe => Ok(()),
                Durability::Durable => self
                    .file
                    .sync_all()
                    .map_err(|e| FsError::Io(format!("sync_all {}: {e}", self.path.display()))),
            }
        }
    }

    impl File for UringFile {
        fn read_at(&self, offset: u64, len: u64) -> FsResult<bytes::Bytes> {
            let len = Self::len_u64(len)?;
            if len == 0 {
                return Ok(bytes::Bytes::new());
            }

            if let Some(_) = &self.ring {
                let mut buf = vec![0u8; len];
                let entry =
                    opcode::Read::new(types::Fd(self.file.as_raw_fd()), buf.as_mut_ptr(), len as _)
                        .offset(offset)
                        .build();

                let result = self.submit_and_wait_one(entry)?;
                if result < 0 {
                    return Err(self.io_uring_error("read_at", result));
                }

                let got = result as usize;
                if got != len {
                    return Err(self.io_uring_unexpected_eof("read_at", len, got));
                }
                return Ok(bytes::Bytes::from(buf));
            }

            self.read_exact_with_std(offset, len)
        }

        fn write_at(&mut self, offset: u64, data: bytes::Bytes) -> FsResult<()> {
            if data.is_empty() {
                self.current_pos = self.current_pos.max(offset);
                return Ok(());
            }

            if let Some(_) = &self.ring {
                let entry = opcode::Write::new(
                    types::Fd(self.file.as_raw_fd()),
                    data.as_ptr(),
                    data.len() as _,
                )
                .offset(offset)
                .build();
                let result = self.submit_and_wait_one(entry)?;
                if result < 0 {
                    return Err(self.io_uring_error("write_at", result));
                }
                if result as usize != data.len() {
                    return Err(FsError::Io(format!(
                        "write_at {}: short write (expected {}, got {})",
                        self.path.display(),
                        data.len(),
                        result
                    )));
                }
            } else {
                self.write_all_with_std(offset, &data)?;
            }

            self.current_pos = self
                .current_pos
                .max(offset.saturating_add(data.len() as u64));
            Ok(())
        }

        fn append(&mut self, data: bytes::Bytes) -> FsResult<u64> {
            let pos = self.current_file_len()?;
            self.write_at(pos, data)?;
            Ok(pos)
        }

        fn len(&self) -> FsResult<u64> {
            let meta_len = self
                .file
                .metadata()
                .map_err(|e| FsError::Io(format!("metadata(len) {}: {e}", self.path.display())))?
                .len();
            Ok(meta_len.max(self.current_pos))
        }

        fn sync(&mut self, dur: Durability) -> FsResult<()> {
            match dur {
                Durability::Unsafe => Ok(()),
                Durability::Durable => {
                    if let Some(_) = &self.ring {
                        let entry = opcode::Fsync::new(types::Fd(self.file.as_raw_fd())).build();
                        let result = self.submit_and_wait_one(entry)?;
                        if result < 0 {
                            return Err(self.io_uring_error("sync", result));
                        }
                        Ok(())
                    } else {
                        self.sync_with_std(dur)
                    }
                }
            }
        }

        fn close(self: Box<Self>) -> FsResult<()> {
            Ok(())
        }

        fn readv_at(&self, offset: u64, bufs: &mut [std::io::IoSliceMut<'_>]) -> FsResult<u64> {
            let total: usize = bufs.iter().map(|b| b.len()).sum();
            if total == 0 {
                return Ok(0);
            }

            if let Some(_) = &self.ring {
                let iovecs = build_iovecs_mut(bufs);
                let entry = opcode::Readv::new(
                    types::Fd(self.file.as_raw_fd()),
                    iovecs.as_ptr(),
                    iovecs.len() as _,
                )
                .offset(offset)
                .build();
                let result = self.submit_and_wait_one(entry)?;
                if result < 0 {
                    return Err(self.io_uring_error("readv_at", result));
                }
                if result as usize != total {
                    return Err(self.io_uring_unexpected_eof("readv_at", total, result as usize));
                }
                return Ok(result as u64);
            }

            let data = self.read_exact_with_std(offset, total)?;
            let mut cursor = &data[..];
            let mut written = 0usize;
            for b in bufs {
                let n = b.len().min(cursor.len());
                b[..n].copy_from_slice(&cursor[..n]);
                cursor = &cursor[n..];
                written += n;
            }
            Ok(written as u64)
        }

        fn writev_at(&mut self, offset: u64, bufs: &[std::io::IoSlice<'_>]) -> FsResult<u64> {
            let total: usize = bufs.iter().map(|b| b.len()).sum();
            if total == 0 {
                return Ok(0);
            }

            if let Some(_) = &self.ring {
                let iovecs = build_iovecs(bufs);
                let entry = opcode::Writev::new(
                    types::Fd(self.file.as_raw_fd()),
                    iovecs.as_ptr(),
                    iovecs.len() as _,
                )
                .offset(offset)
                .build();
                let result = self.submit_and_wait_one(entry)?;
                if result < 0 {
                    return Err(self.io_uring_error("writev_at", result));
                }
                if result as usize != total {
                    return Err(FsError::Io(format!(
                        "writev_at {}: short write (expected {}, got {})",
                        self.path.display(),
                        total,
                        result
                    )));
                }
            } else {
                let mut tmp = Vec::with_capacity(total);
                for b in bufs {
                    tmp.extend_from_slice(b);
                }
                self.write_all_with_std(offset, &tmp)?;
            }

            self.current_pos = self.current_pos.max(offset.saturating_add(total as u64));
            Ok(total as u64)
        }

        fn appendv(&mut self, bufs: &[std::io::IoSlice<'_>]) -> FsResult<u64> {
            let pos = self.current_file_len()?;
            let _ = self.writev_at(pos, bufs)?;
            Ok(pos)
        }

        fn caps(&self) -> FileCaps {
            FileCaps::READV_AT | FileCaps::WRITEV_AT | FileCaps::APPENDV
        }

        fn try_lock_exclusive(&self) -> FsResult<()> {
            use std::os::unix::io::AsRawFd;
            let fd = self.file.as_raw_fd();
            let result = unsafe { libc::flock(fd, libc::LOCK_EX | libc::LOCK_NB) };
            if result != 0 {
                let err = std::io::Error::last_os_error();
                if err.kind() == std::io::ErrorKind::WouldBlock {
                    return Err(FsError::AlreadyExists("file is already locked".to_string()));
                }
                return Err(FsError::Io(format!("flock failed: {}", err)));
            }
            Ok(())
        }

        fn unlock(&self) -> FsResult<()> {
            use std::os::unix::io::AsRawFd;
            let fd = self.file.as_raw_fd();
            let result = unsafe { libc::flock(fd, libc::LOCK_UN) };
            if result != 0 {
                return Err(FsError::Io(format!(
                    "unlock failed: {}",
                    std::io::Error::last_os_error()
                )));
            }
            Ok(())
        }
    }

    fn build_iovecs(bufs: &[std::io::IoSlice<'_>]) -> Vec<libc::iovec> {
        bufs.iter()
            .map(|buf| libc::iovec {
                iov_base: buf.as_ptr() as *mut libc::c_void,
                iov_len: buf.len(),
            })
            .collect()
    }

    fn build_iovecs_mut(bufs: &mut [std::io::IoSliceMut<'_>]) -> Vec<libc::iovec> {
        bufs.iter_mut()
            .map(|buf| libc::iovec {
                iov_base: buf.as_mut_ptr() as *mut libc::c_void,
                iov_len: buf.len(),
            })
            .collect()
    }

    fn io_err(op: &str, path: &Path, e: io::Error) -> FsError {
        FsError::Io(format!("{op} {}: {e}", path.display()))
    }

    #[cfg(all(test, target_os = "linux"))]
    mod tests {
        use super::*;
        use std::io::{IoSlice, IoSliceMut};
        use tempfile::TempDir;

        fn contract(fs: &dyn Fs) -> FsResult<()> {
            let path = FsPath::new("contract.bin");
            let mut file = fs.open(
                &path,
                OpenOptions {
                    mode: OpenMode::ReadWrite,
                    create: true,
                    create_new: false,
                    truncate: true,
                },
            )?;

            file.write_at(0, bytes::Bytes::from_static(b"hello"))?;
            file.write_at(5, bytes::Bytes::from_static(b" world"))?;
            file.sync(Durability::Durable)?;

            let mut out = [0u8; 11];
            let mut slices = [
                IoSliceMut::new(&mut out[..5]),
                IoSliceMut::new(&mut out[5..]),
            ];
            let read = file.readv_at(0, &mut slices)?;
            assert_eq!(read, 11);
            assert_eq!(&out, b"hello world");
            assert_eq!(file.len()?, 11);

            let append_off = file.append(bytes::Bytes::from_static(b"!"))?;
            assert_eq!(append_off, 11);

            let bufs = [IoSlice::new(b"ab"), IoSlice::new(b"cd")];
            let wrote = file.appendv(&bufs)?;
            assert_eq!(wrote, 4);
            assert_eq!(file.len()?, 16);

            let data = file.read_at(11, 5)?;
            assert_eq!(&data[..], b"!abcd");

            let err = file.read_at(100, 1).unwrap_err();
            assert!(matches!(err, FsError::Io(_)));

            Ok(())
        }

        #[test]
        fn should_match_realfs_contract() -> FsResult<()> {
            let temp = TempDir::new().map_err(|e| FsError::Io(e.to_string()))?;

            let real_dir = temp.path().join("real");
            let uring_dir = temp.path().join("uring");
            let real = crate::io::RealFs::new(&real_dir)?;
            let uring = UringFs::new(&uring_dir)?;

            contract(&real)?;
            contract(&uring)?;
            Ok(())
        }
    }
}

#[cfg(target_os = "linux")]
pub use linux_impl::UringFs;

#[cfg(not(target_os = "linux"))]
pub type UringFs = super::real::RealFs;
