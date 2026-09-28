use cntryl_midge::{Engine, OpenOptions, TransactionMode, WriteOptions};
use std::time::Duration;

#[test]
fn should_preserve_published_sst_bytes_given_later_flush_then_restart(
) -> cntryl_midge::MidgeResult<()> {
    // Arrange
    let dir = tempfile::tempdir()?;
    let mut engine = Engine::open(OpenOptions::local(dir.path()).build()?)?;
    let cf = engine
        .get_column_family("default")
        .expect("default column family");
    let mut first = engine.begin_tx(cf.id(), TransactionMode::ReadWrite)?;
    first.put(b"first".to_vec(), b"one".to_vec(), None)?;
    first.commit(WriteOptions::sync())?;
    engine.flush_cf(&cf)?;
    let sst_dir = dir.path().join("sst");
    let first_path = std::fs::read_dir(&sst_dir)?
        .filter_map(Result::ok)
        .map(|entry| entry.path())
        .find(|path| path.extension().is_some_and(|extension| extension == "sst"))
        .expect("published first SST");
    let published_bytes = std::fs::read(&first_path)?;

    // Act
    let mut second = engine.begin_tx(cf.id(), TransactionMode::ReadWrite)?;
    second.put(b"second".to_vec(), b"two".to_vec(), None)?;
    second.commit(WriteOptions::sync())?;
    engine.flush_cf(&cf)?;
    let after_flush = std::fs::read(&first_path)?;
    engine.shutdown(Duration::from_secs(5))?;
    let reopened = Engine::open(OpenOptions::local(dir.path()).build()?)?;
    let after_restart = std::fs::read(&first_path)?;
    let read = reopened.begin_tx(cf.id(), TransactionMode::ReadOnly)?;

    // Assert
    assert_eq!(published_bytes, after_flush);
    assert_eq!(published_bytes, after_restart);
    assert_eq!(read.get(b"first")?.as_deref(), Some(b"one".as_slice()));
    assert_eq!(read.get(b"second")?.as_deref(), Some(b"two".as_slice()));
    Ok(())
}
