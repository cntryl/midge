//! The engine reaches its metadata, WAL and lease only through the injected
//! database root, so a fault or crash simulator observes every effect (#808).

use cntryl_midge::__internal::io::{Fs, RealFs};
use cntryl_midge::{Engine, OpenOptions, TransactionMode, WriteOptions};
use std::sync::Arc;

fn served_options(named: &std::path::Path, served: &std::path::Path) -> OpenOptions {
    let root: Arc<dyn Fs> = Arc::new(RealFs::new(served).expect("served root"));
    OpenOptions::local(named)
        .background_compaction(false)
        .filesystem_for_testing(root)
        .build()
        .expect("open options")
}

#[test]
fn should_keep_metadata_wal_and_lease_under_injected_root_when_opening_local_database() {
    // Arrange: the configured path names the database; a different host
    // directory serves it.
    let named = tempfile::tempdir().expect("named directory");
    let served = tempfile::tempdir().expect("served directory");
    let options = served_options(named.path(), served.path());

    // Act
    {
        let mut engine = Engine::open(options.clone()).expect("open");
        let default = engine.get_column_family("default").expect("default family");
        let mut tx = engine
            .begin_tx(default.id(), TransactionMode::ReadWrite)
            .expect("begin");
        tx.put(b"key".to_vec(), b"value".to_vec(), None)
            .expect("put");
        tx.commit(WriteOptions::sync()).expect("commit");
        engine
            .shutdown(std::time::Duration::from_secs(5))
            .expect("shutdown");
    }
    let reopened = Engine::open(options).expect("reopen");
    let default = reopened
        .get_column_family("default")
        .expect("default family");
    let value = reopened
        .begin_tx(default.id(), TransactionMode::ReadOnly)
        .expect("read")
        .get(b"key")
        .expect("get");

    // Assert
    assert_eq!(value.as_deref(), Some(&b"value"[..]));
    for file in ["FORMAT", ".midge_leader"] {
        assert!(served.path().join(file).exists(), "{file} must be served");
        assert!(
            !named.path().join(file).exists(),
            "{file} must not be on the named path"
        );
    }
    let served_wal = std::fs::read_dir(served.path().join("wal"))
        .expect("served WAL directory")
        .count();
    assert!(served_wal > 0, "WAL must be served by the injected root");
    assert!(!named.path().join("wal").exists());
}
