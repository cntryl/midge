//! An owned reference to one file in a filesystem, for work that outlives
//! the call that named it (queued uploads, deferred readbacks).

use super::{Fs, FsPath, FsResult};
use bytes::Bytes;
use std::sync::Arc;

#[derive(Clone)]
pub struct FsLocation {
    fs: Arc<dyn Fs>,
    path: FsPath,
}

impl FsLocation {
    pub fn new(fs: Arc<dyn Fs>, path: FsPath) -> Self {
        Self { fs, path }
    }

    /// The host file at `path`, through a root at its parent directory.
    #[cfg(any(test, feature = "internal-testing"))]
    #[must_use]
    pub fn host_file(path: &std::path::Path) -> Self {
        let parent = path.parent().expect("host file has a parent directory");
        let name = path.file_name().expect("host file has a name");
        Self::new(
            super::real::test_root(parent),
            FsPath::new(name.to_string_lossy()),
        )
    }

    pub fn read_all(&self) -> FsResult<Bytes> {
        super::read_all(self.fs.as_ref(), &self.path)
    }

    pub fn read_exact_size(&self, expected: u64) -> FsResult<Bytes> {
        let file = self.fs.open(&self.path, super::OpenOptions::READ_ONLY)?;
        if file.len()? != expected {
            return Err(super::FsError::Corruption(
                "upload source size changed".into(),
            ));
        }
        let bytes = file.read_at(0, expected)?;
        if bytes.len() as u64 != expected || file.len()? != expected {
            return Err(super::FsError::Corruption(
                "upload source size changed during read".into(),
            ));
        }
        Ok(bytes)
    }

    pub fn len(&self) -> FsResult<u64> {
        self.fs.metadata(&self.path).map(|metadata| metadata.len)
    }

    pub fn is_empty(&self) -> FsResult<bool> {
        self.len().map(|len| len == 0)
    }
}

impl std::fmt::Debug for FsLocation {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_tuple("FsLocation").field(&self.path.0).finish()
    }
}

/// The host path when the filesystem has one, so messages name the database;
/// otherwise the key.
impl std::fmt::Display for FsLocation {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self.fs.host_addressing() {
            Some(addressing) => write!(f, "{}", addressing.root.join(&self.path.0).display()),
            None => f.write_str(&self.path.0),
        }
    }
}
