use std::path::PathBuf;

/// A test directory that automatically cleans itself up when dropped.
/// This ensures test directories are always removed, even on panic.
pub struct TempTestDir {
    path: PathBuf,
    cleanup: bool,
}

impl TempTestDir {
    /// Create a new temporary test directory with automatic cleanup
    pub fn new(prefix: &str) -> Self {
        let mut base = std::env::temp_dir();
        use std::time::{SystemTime, UNIX_EPOCH};
        let t = SystemTime::now().duration_since(UNIX_EPOCH).unwrap().as_millis();
        base.push(format!("midge_test_{}_{}", prefix, t));
        std::fs::create_dir_all(&base).expect("create temp dir");
        
        Self {
            path: base,
            cleanup: true,
        }
    }

    /// Get the path to this directory
    pub fn path(&self) -> &std::path::Path {
        &self.path
    }

    /// Disable automatic cleanup (for debugging)
    #[allow(dead_code)]
    pub fn keep(&mut self) {
        self.cleanup = false;
    }
}

impl Drop for TempTestDir {
    fn drop(&mut self) {
        if self.cleanup {
            let _ = std::fs::remove_dir_all(&self.path);
        }
    }
}

/// Create a unique temporary directory under the OS temp dir for tests.
/// Returns a TempTestDir that auto-cleans on drop.
///
/// For backwards compatibility with existing code that expects PathBuf,
/// use `.path().to_path_buf()` on the returned value.
#[deprecated(note = "Use TempTestDir::new() directly for automatic cleanup")]
pub fn create_temp_dir(prefix: &str) -> PathBuf {
    let mut base = std::env::temp_dir();
    use std::time::{SystemTime, UNIX_EPOCH};
    let t = SystemTime::now().duration_since(UNIX_EPOCH).unwrap().as_millis();
    base.push(format!("midge_test_{}_{}", prefix, t));
    std::fs::create_dir_all(&base).expect("create temp dir");
    base
}

/// Append a WAL-like entry to `path`. If `do_fsync` is true the file is
/// sync'd to disk (via sync_all()). Returns a std::io::Result for convenience.
pub fn write_wal_entry(path: &std::path::Path, data: &[u8], do_fsync: bool) -> std::io::Result<()> {
    use std::fs::OpenOptions;
    let mut f = OpenOptions::new().create(true).append(true).open(path)?;
    use std::io::Write;
    f.write_all(data)?;
    if do_fsync {
        f.sync_all()?;
    }
    Ok(())
}
