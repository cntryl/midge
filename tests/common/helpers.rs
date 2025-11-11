use std::path::PathBuf;

/// Create a unique temporary directory under the OS temp dir for tests.
/// Simple, dependency-free helper used by integration test stubs.
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
