use cntryl_midge::{Engine, OpenOptions, TransactionMode, WriteOptions};
use std::sync::Arc;
use std::time::Duration;
use tempfile::TempDir;

#[test]
fn should_apply_all_concurrent_commits_durably_when_many_threads_write_simultaneously() {
    // Arrange
    const THREADS: usize = 4;
    const COMMITS_PER_THREAD: usize = 32;

    let temp_dir = TempDir::new().expect("temp dir");
    let options = OpenOptions::local(temp_dir.path())
        .background_compaction(false)
        .build()
        .expect("build local options");
    let engine = Arc::new(Engine::open(options.clone()).expect("open engine"));
    let cf_id = engine
        .create_column_family("concurrent-commits")
        .expect("create column family")
        .id();

    // Act
    let writers = (0..THREADS)
        .map(|thread_id| {
            let engine = Arc::clone(&engine);
            std::thread::spawn(move || {
                for commit_id in 0..COMMITS_PER_THREAD {
                    let key = format!("thread-{thread_id:02}-key-{commit_id:03}");
                    let value = format!("thread-{thread_id:02}-value-{commit_id:03}");
                    let mut tx = engine
                        .begin_tx(cf_id, TransactionMode::ReadWrite)
                        .expect("begin write transaction");
                    tx.put(key.into_bytes(), value.into_bytes(), None)
                        .expect("put unique value");
                    tx.commit(WriteOptions::sync())
                        .expect("commit unique value durably");
                }
            })
        })
        .collect::<Vec<_>>();

    for writer in writers {
        writer.join().expect("join writer");
    }

    let mut engine = Arc::try_unwrap(engine).ok().expect("unique engine");
    engine
        .shutdown(Duration::from_secs(5))
        .expect("shutdown before recovery check");
    let mut reopened = Engine::open(options).expect("reopen engine");
    let reopened_cf = reopened
        .get_column_family("concurrent-commits")
        .expect("recover column family");
    let read_tx = reopened
        .begin_tx(reopened_cf.id(), TransactionMode::ReadOnly)
        .expect("begin recovery read");

    // Assert
    for thread_id in 0..THREADS {
        for commit_id in 0..COMMITS_PER_THREAD {
            let key = format!("thread-{thread_id:02}-key-{commit_id:03}");
            let expected = format!("thread-{thread_id:02}-value-{commit_id:03}");
            let actual = read_tx
                .get(key.as_bytes())
                .expect("read recovered value")
                .expect("recovered value exists");
            assert_eq!(actual.as_ref(), expected.as_bytes(), "key: {key}");
        }
    }

    drop(read_tx);
    reopened
        .shutdown(Duration::from_secs(5))
        .expect("shutdown recovered engine");
}
