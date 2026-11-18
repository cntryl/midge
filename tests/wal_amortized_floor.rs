//! The opt-in amortized WAL sync floor (#904) keeps acknowledged writes and
//! records the floor exactly wherever a segment's history is closed.

use cntryl_midge::__internal::io::{
    Durability, Fs, FsPath, MockFs, OpenMode, OpenOptions as FileOptions, ReadFs,
};
use cntryl_midge::__internal::wal::frame::{decode_frame_header, WAL_FRAME_HEADER_LEN};
use cntryl_midge::__internal::wal::segment::HEADER_LEN;
use cntryl_midge::{
    AmortizedSyncFloor, Engine, MidgeError, OpenOptions, TransactionMode, WriteOptions,
};
use std::sync::Arc;
use std::time::Duration;

/// A window far larger than the test writes, so only an exact edit moves it.
fn wide_window() -> AmortizedSyncFloor {
    AmortizedSyncFloor {
        max_unrecorded_bytes: 64 * 1024 * 1024,
        max_unrecorded_delay: Duration::from_secs(10),
    }
}

struct Store {
    directory: tempfile::TempDir,
    fs: Arc<MockFs>,
}

impl Store {
    fn new() -> Self {
        Self {
            directory: tempfile::tempdir().expect("database name"),
            fs: Arc::new(MockFs::new()),
        }
    }

    fn options(&self, floor: Option<AmortizedSyncFloor>) -> OpenOptions {
        let mut builder = OpenOptions::local(self.directory.path())
            .background_compaction(false)
            .filesystem_for_testing(self.fs.clone());
        if let Some(floor) = floor {
            builder = builder.amortized_sync_floor(floor);
        }
        builder.build().expect("options")
    }

    fn open_wal_path(&self) -> FsPath {
        let id = self
            .fs
            .list_dir(&FsPath::new("wal"))
            .expect("WAL directory")
            .iter()
            .filter_map(|entry| cntryl_midge::__internal::wal::parse_segment_file_name(&entry.name))
            .max()
            .expect("open WAL");
        FsPath::new(format!(
            "wal/{}",
            cntryl_midge::__internal::wal::segment_file_name(id)
        ))
    }

    fn wal_bytes(&self) -> Vec<u8> {
        let file = self.fs.open_read(&self.open_wal_path()).expect("read WAL");
        file.read_at(0, file.len().expect("WAL length"))
            .expect("WAL bytes")
            .to_vec()
    }

    fn shorten(&self, end: usize) {
        let mut file = self
            .fs
            .open(
                &self.open_wal_path(),
                FileOptions {
                    mode: OpenMode::ReadWrite,
                    create: false,
                    create_new: false,
                    truncate: false,
                },
            )
            .expect("open WAL");
        file.truncate(end as u64).expect("lose suffix");
        file.sync(Durability::Durable).expect("persist damage");
    }
}

/// The end of the first frame of the open segment.
fn first_frame_end(bytes: &[u8]) -> usize {
    let (payload, _) = decode_frame_header(&bytes[HEADER_LEN..HEADER_LEN + WAL_FRAME_HEADER_LEN])
        .expect("first frame");
    HEADER_LEN + WAL_FRAME_HEADER_LEN + payload
}

fn commit_sync(engine: &Engine, key: &[u8]) {
    let cf = engine.get_column_family("default").expect("default");
    let mut tx = engine
        .begin_tx(cf.id(), TransactionMode::ReadWrite)
        .expect("transaction");
    tx.put(key.to_vec(), b"acknowledged".to_vec(), None)
        .expect("put");
    tx.commit(WriteOptions::sync()).expect("sync commit");
}

#[test]
fn should_record_exact_floor_when_shutting_down_cleanly_with_an_open_window() {
    // Arrange: the second commit stays inside the unrecorded window.
    let store = Store::new();
    let options = store.options(Some(wide_window()));
    let mut engine = Engine::open(options).expect("open");
    commit_sync(&engine, b"first");
    let before = engine
        .metrics()
        .get_runtime_metrics()
        .expect("metrics")
        .wal_ledger_file_syncs;
    commit_sync(&engine, b"later");
    let after = engine
        .metrics()
        .get_runtime_metrics()
        .expect("metrics")
        .wal_ledger_file_syncs;
    assert_eq!(after, before, "the window must be open before shutdown");

    // Act
    engine.shutdown(Duration::from_secs(5)).expect("shutdown");
    drop(engine);
    let bytes = store.wal_bytes();
    let end = first_frame_end(&bytes) + 3;
    assert!(bytes.len() > end);
    store.shorten(end);

    // Assert: shutdown recorded the exact end, so losing acknowledged bytes
    // below it fails a strict open exactly as without the option.
    let error = Engine::open(store.options(None))
        .err()
        .expect("strict recovery rejects acknowledged loss");
    assert!(matches!(error, MidgeError::Corruption(_)), "{error:?}");
}
