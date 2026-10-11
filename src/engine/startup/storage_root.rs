//! The database root every engine component reaches storage through.
//!
//! This is the single production constructor of a host-backed root. Tests may
//! inject their own root, so a fault or crash simulator observes every file
//! effect the engine makes.

use super::super::OpenOptions;
use super::super::IN_MEMORY_OPEN_COUNTER;
use super::StartupStoragePath;
use crate::common::MidgeResult;
use crate::config::Storage;
use crate::io::{Fs, FsError};
use std::path::{Path, PathBuf};
use std::sync::atomic::Ordering;
use std::sync::Arc;

impl StartupStoragePath {
    /// Resolve the database path and its root filesystem. A new host root,
    /// and each missing parent directory, is made durable before startup
    /// writes anything below it.
    pub(super) fn resolve(opts: &OpenOptions) -> MidgeResult<Self> {
        let (db_path, memory_mode) = match opts.storage() {
            Storage::InMemory => {
                let counter = IN_MEMORY_OPEN_COUNTER.fetch_add(1, Ordering::SeqCst);
                let timestamp = std::time::SystemTime::now()
                    .duration_since(std::time::UNIX_EPOCH)
                    .map_or(0, |duration| duration.as_nanos());
                let path = PathBuf::from(format!(
                    "target/tmp/midge_test_memory_{}_{}_{}",
                    std::process::id(),
                    counter,
                    timestamp
                ));
                (path, true)
            }
            Storage::Local { path } => (path.clone(), false),
            Storage::Cloud {
                local_cache_path, ..
            }
            | Storage::CloudSimulated {
                local_cache_path, ..
            } => (local_cache_path.clone(), false),
        };
        let root: Arc<dyn Fs> = if let Some(injected) = opts.injected_filesystem() {
            injected
        } else if memory_mode {
            Arc::new(crate::io::MockFs::new())
        } else {
            // Create durably first so a failure keeps its operating-system
            // error kind; the root then already exists.
            crate::io::durable_dir::create_path_durably(&db_path)?;
            Arc::new(crate::io::RealFs::open_existing(&db_path).map_err(FsError::into_midge)?)
        };
        Ok(Self {
            db_path,
            memory_mode,
            root,
        })
    }
}

/// Open an existing host database directory for offline inspection, without
/// creating anything.
///
/// # Errors
///
/// A missing directory has no FORMAT marker to validate, so it is a
/// compatibility error; any other failure keeps its filesystem class.
pub(in crate::engine) fn existing_host_root(db_path: &Path) -> MidgeResult<Arc<dyn Fs>> {
    match crate::io::RealFs::open_existing(db_path) {
        Ok(fs) => Ok(Arc::new(fs)),
        Err(error) if !db_path.exists() => Err(crate::common::MidgeError::CompatibilityError(
            format!("failed to read FORMAT marker: database path is missing: {error}"),
        )),
        Err(error) => Err(error.into_midge()),
    }
}
