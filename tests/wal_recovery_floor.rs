//! A persisted WAL sync floor must distinguish lost acknowledged bytes from
//! an unsynced tail without discarding the verified prefix during salvage.

use cntryl_midge::__internal::io::{
    Durability, Fs, FsPath, MockFs, OpenMode, OpenOptions as FileOptions, ReadFs,
};
use cntryl_midge::__internal::wal::frame::{decode_frame_header, WAL_FRAME_HEADER_LEN};
use cntryl_midge::__internal::wal::segment::HEADER_LEN;
use cntryl_midge::{
    Engine, MidgeError, OpenOptions, RecoveryPolicy, TransactionMode, WriteOptions,
};
use std::sync::Arc;
use std::time::Duration;

struct SyncedHistory {
    directory: tempfile::TempDir,
    fs: Arc<MockFs>,
    options: OpenOptions,
    path: FsPath,
    bytes: Vec<u8>,
    first_end: usize,
    last_sequence: u64,
}

impl SyncedHistory {
    fn new() -> Self {
        let directory = tempfile::tempdir().expect("database name");
        let fs = Arc::new(MockFs::new());
        let options = OpenOptions::local(directory.path())
            .background_compaction(false)
            .filesystem_for_testing(fs.clone())
            .build()
            .expect("options");
        let mut engine = Engine::open(options.clone()).expect("open");
        let cf = engine.get_column_family("default").expect("default");
        let mut last_sequence = 0;
        for key in [b"first", b"later"] {
            let mut tx = engine
                .begin_tx(cf.id(), TransactionMode::ReadWrite)
                .expect("transaction");
            tx.put(key.to_vec(), b"acknowledged".to_vec(), None)
                .expect("put");
            last_sequence = tx
                .commit_with_sequence(WriteOptions::sync())
                .expect("sync commit");
        }
        engine.shutdown(Duration::from_secs(5)).expect("shutdown");
        let id = fs
            .list_dir(&FsPath::new("wal"))
            .expect("WAL directory")
            .iter()
            .filter_map(|entry| cntryl_midge::__internal::wal::parse_segment_file_name(&entry.name))
            .max()
            .expect("open WAL");
        let path = FsPath::new(format!(
            "wal/{}",
            cntryl_midge::__internal::wal::segment_file_name(id)
        ));
        let file = fs.open_read(&path).expect("read WAL");
        let bytes = file
            .read_at(0, file.len().expect("WAL length"))
            .expect("WAL bytes")
            .to_vec();
        drop(file);
        let (payload, _) =
            decode_frame_header(&bytes[HEADER_LEN..HEADER_LEN + WAL_FRAME_HEADER_LEN])
                .expect("first frame");
        let first_end = HEADER_LEN + WAL_FRAME_HEADER_LEN + payload;
        assert!(bytes.len() > first_end + WAL_FRAME_HEADER_LEN);
        Self {
            directory,
            fs,
            options,
            path,
            bytes,
            first_end,
            last_sequence,
        }
    }

    fn shorten(&self, end: usize) {
        let mut file = self
            .fs
            .open(
                &self.path,
                FileOptions {
                    mode: OpenMode::ReadWrite,
                    create: false,
                    create_new: false,
                    truncate: false,
                },
            )
            .expect("open acknowledged WAL");
        file.truncate(end as u64).expect("lose acknowledged suffix");
        file.sync(Durability::Durable).expect("persist damage");
    }
}

#[test]
fn should_keep_verified_prefix_given_salvage_when_local_file_is_shorter_than_the_floor() {
    // Arrange: acknowledged bytes disappear after a complete first commit.
    let history = SyncedHistory::new();
    history.shorten(history.first_end + 3);
    let salvage = OpenOptions::local(history.directory.path())
        .background_compaction(false)
        .filesystem_for_testing(history.fs.clone())
        .recovery_policy(RecoveryPolicy::Salvage)
        .build()
        .expect("salvage options");

    // Act
    let mut engine = Engine::open(salvage).expect("salvage the verified prefix");
    let cf = engine.get_column_family("default").expect("default");
    let read = engine
        .begin_tx(cf.id(), TransactionMode::ReadOnly)
        .expect("read");

    // Assert
    assert_eq!(
        read.get(b"first").unwrap().as_deref(),
        Some(&b"acknowledged"[..])
    );
    assert_eq!(read.get(b"later").unwrap(), None);
    drop(read);
    let mut tx = engine
        .begin_tx(cf.id(), TransactionMode::ReadWrite)
        .expect("new transaction");
    tx.put(b"next".to_vec(), b"value".to_vec(), None).unwrap();
    assert!(tx.commit_with_sequence(WriteOptions::sync()).unwrap() > history.last_sequence);
    engine.shutdown(Duration::from_secs(5)).unwrap();
    let mut reopened = Engine::open(history.options).expect("strict reopen after salvage");
    let read = reopened
        .begin_tx(cf.id(), TransactionMode::ReadOnly)
        .unwrap();
    assert_eq!(
        read.get(b"first").unwrap().as_deref(),
        Some(&b"acknowledged"[..])
    );
    assert_eq!(read.get(b"later").unwrap(), None);
    assert_eq!(read.get(b"next").unwrap().as_deref(), Some(&b"value"[..]));
    drop(read);
    reopened.shutdown(Duration::from_secs(5)).unwrap();
}

#[test]
fn should_reject_lost_acknowledged_suffix_given_strict_recovery_even_at_a_frame_boundary() {
    // Arrange
    for partial_header in [0, 3] {
        let history = SyncedHistory::new();
        let end = history.first_end + partial_header;
        history.shorten(end);

        // Act
        let result = Engine::open(history.options.clone());

        // Assert: strict open neither accepts loss nor rewrites the evidence.
        let error = result
            .err()
            .expect("strict recovery rejects acknowledged loss");
        assert!(matches!(error, MidgeError::Corruption(_)), "{error:?}");
        let file = history.fs.open_read(&history.path).expect("retained WAL");
        assert_eq!(file.len().unwrap(), end as u64);
        assert_eq!(
            file.read_at(0, end as u64).unwrap().as_ref(),
            &history.bytes[..end]
        );
    }
}
