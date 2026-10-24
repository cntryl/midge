use super::*;
use crate::io::traits::{
    DirEntry, Durability, File, Fs, FsPath, FsResult, Metadata, OpenOptions, ReadFs,
};
use crate::metadata::FileMeta;
use crate::metadata::{Manifest, ManifestPersistence};
use proptest::prelude::*;
use std::cell::Cell;
use std::io::Write;
use std::sync::{Arc, Condvar, Mutex};
use tempfile::tempdir;

thread_local! {
    static OBSERVE_REQUIRED_MANIFEST_SYNC: Cell<bool> = const { Cell::new(false) };
}

#[derive(Default)]
struct JournalOpenGate {
    state: Mutex<(usize, bool)>,
    changed: Condvar,
}

impl JournalOpenGate {
    fn wait_in_open(&self) {
        let mut state = self
            .state
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner);
        state.0 += 1;
        self.changed.notify_all();
        while !state.1 {
            state = self
                .changed
                .wait(state)
                .expect("wait for journal-open release");
        }
    }

    fn wait_for_arrivals(&self, expected: usize, timeout: std::time::Duration) -> bool {
        let state = self
            .state
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner);
        let (state, _) = self
            .changed
            .wait_timeout_while(state, timeout, |state| state.0 < expected)
            .expect("wait for journal-open arrivals");
        state.0 >= expected
    }

    fn release(&self) {
        let mut state = self
            .state
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner);
        state.1 = true;
        self.changed.notify_all();
    }
}

struct GatedJournalFs {
    inner: crate::io::real::RealFs,
    open_gate: Arc<JournalOpenGate>,
}

impl ReadFs for GatedJournalFs {
    fn open_read(&self, path: &FsPath) -> FsResult<Box<dyn crate::io::ReadFile + '_>> {
        self.open(path, OpenOptions::READ_ONLY)
            .map(|file| file as Box<dyn crate::io::ReadFile>)
    }

    fn exists(&self, path: &FsPath) -> FsResult<bool> {
        self.inner.exists(path)
    }

    fn metadata(&self, path: &FsPath) -> FsResult<Metadata> {
        self.inner.metadata(path)
    }

    fn list_dir(&self, path: &FsPath) -> FsResult<Vec<DirEntry>> {
        self.inner.list_dir(path)
    }
}

impl Fs for GatedJournalFs {
    fn try_lock_exclusive(&self, path: &FsPath) -> FsResult<Box<dyn crate::io::FsLock>> {
        self.inner.try_lock_exclusive(path)
    }

    fn host_addressing(&self) -> Option<crate::io::HostAddressing<'_>> {
        self.inner.host_addressing()
    }

    fn coordination_key(&self) -> u64 {
        self.inner.coordination_key()
    }

    fn open(&self, path: &FsPath, opts: OpenOptions) -> FsResult<Box<dyn File + '_>> {
        if path.0 == JOURNAL_FILE && opts.create && !opts.truncate {
            self.open_gate.wait_in_open();
        }
        self.inner.open(path, opts)
    }

    fn remove_file(&self, path: &FsPath) -> FsResult<()> {
        self.inner.remove_file(path)
    }

    fn create_dir_all(&self, path: &FsPath) -> FsResult<()> {
        self.inner.create_dir_all(path)
    }

    fn remove_dir_all(&self, path: &FsPath) -> FsResult<()> {
        self.inner.remove_dir_all(path)
    }

    fn sync_dir(&self, path: &FsPath, durability: Durability) -> FsResult<()> {
        self.inner.sync_dir(path, durability)
    }

    fn rename_atomic(&self, from: &FsPath, to: &FsPath) -> FsResult<()> {
        self.inner.rename_atomic(from, to)
    }
}

/// An edit record `edit_id` whose payload holds `payload_edit` and whose
/// CRC covers `crc_edit`.
fn encode_test_record(
    edit_id: u64,
    payload_edit: &ManifestEdit,
    crc_edit: &ManifestEdit,
) -> Vec<u8> {
    let envelope = |edit: &ManifestEdit| JournalEditEnvelope {
        edit_id,
        edit: edit.clone(),
    };
    let payload =
        journal_serialize(&envelope(payload_edit)).expect("serialize test journal payload");
    let crc_payload = journal_serialize(&envelope(crc_edit)).expect("serialize test CRC payload");
    assert_eq!(
        payload.len(),
        crc_payload.len(),
        "test payloads must have equal lengths"
    );
    let crc = crc32c::crc32c(&crc_payload);

    let mut record = Vec::with_capacity(1 + 4 + payload.len() + 4);
    record.push(payload_edit.record_type());
    record.extend_from_slice(
        &u32::try_from(payload.len())
            .expect("test payload length fits in u32")
            .to_le_bytes(),
    );
    record.extend_from_slice(&payload);
    record.extend_from_slice(&crc.to_le_bytes());
    record
}

fn write_and_sync_test_journal(db: &Path, bytes: &[u8]) {
    let path = db.join(JOURNAL_FILE);
    std::fs::write(&path, bytes).expect("write test manifest journal");
    std::fs::OpenOptions::new()
        .write(true)
        .open(path)
        .expect("open test manifest journal")
        .sync_all()
        .expect("sync test manifest journal");
}

#[test]
fn should_refuse_journal_append_when_manifest_edit_ids_are_exhausted() {
    // Arrange: a valid snapshot has consumed the last available identity.
    let directory = tempdir().unwrap();
    let fs: Arc<dyn Fs> = Arc::new(crate::io::RealFs::new(directory.path()).unwrap());
    let mut manifest = crate::metadata::Manifest::default();
    manifest.edit_checkpoint_id = u64::MAX;
    let snapshot = directory
        .path()
        .join(crate::metadata::files::MANIFEST_SNAPSHOT);
    let journal = directory.path().join(JOURNAL_FILE);
    std::fs::write(
        &snapshot,
        crate::metadata::codec::encode(crate::metadata::codec::MANIFEST, &manifest).unwrap(),
    )
    .unwrap();
    std::fs::write(&journal, []).unwrap();
    let before_snapshot = std::fs::read(&snapshot).unwrap();
    let before_journal = std::fs::read(&journal).unwrap();
    let edits = [
        ManifestEdit::AddSst(FileMeta {
            name: crate::cloud_layout::file_name(0, 0, 7),
            size_bytes: 10,
            largest_seq: Some(7),
            ..Default::default()
        }),
        ManifestEdit::AdvanceFlushFrontier {
            cf_id: 0,
            freeze_sequence: 7,
        },
    ];

    // Act
    let appended = append_edit_batch_with_fs(&fs, &edits);

    // Assert
    assert!(
        matches!(appended, Err(crate::common::MidgeError::ResourceLimit(_))),
        "{appended:?}"
    );
    assert_eq!(std::fs::read(&snapshot).unwrap(), before_snapshot);
    assert_eq!(std::fs::read(&journal).unwrap(), before_journal);
    let recovered = crate::metadata::ManifestPersistence::load(directory.path()).unwrap();
    assert!(recovered.files.is_empty());
    assert_eq!(recovered.edit_checkpoint_id, u64::MAX);
    assert_eq!(recovered.flush_frontiers(), manifest.flush_frontiers());
}

#[test]
fn should_fail_salvage_not_drop_edits_when_journal_read_returns_transient_io_error() {
    // Arrange: two durable edits; reads past the first record fail.
    let td = tempdir().unwrap();
    let db = td.path();
    let edit = |seq| {
        ManifestEdit::AddSst(FileMeta {
            name: crate::cloud_layout::file_name(0, 0, seq),
            size_bytes: 1024,
            ..Default::default()
        })
    };
    append_edit(db, &edit(1)).expect("append first edit");
    let first_len = std::fs::metadata(db.join(JOURNAL_FILE)).unwrap().len();
    append_edit(db, &edit(2)).expect("append second edit");
    let fs: Arc<dyn Fs> = Arc::new(crate::io::transient_read::TransientReadFs {
        inner: crate::io::real::RealFs::new(db).unwrap(),
        fail_from: first_len,
    });

    // Act
    let salvaged = salvage_edits_after_with_fs_unlocked(&fs, 0);

    // Assert
    assert!(
        salvaged.is_err(),
        "a transient read error is not corruption and must not drop later edits"
    );
}

#[test]
fn should_replay_journal_when_valid_records_exist() {
    // Arrange
    let td = tempdir().unwrap();
    let db = td.path();
    let sst_name = crate::cloud_layout::file_name(0, 0, 1);

    let file = FileMeta {
        name: sst_name.clone(),
        level: 0,
        size_bytes: 1024,
        cf_id: 0,
        smallest_key: Some(vec![1, 2]),
        largest_key: Some(vec![9, 9]),
        smallest_seq: Some(1),
        largest_seq: Some(1),
        ..Default::default()
    };

    // Act
    let edit = ManifestEdit::AddSst(file.clone());
    append_edit(db, &edit).expect("append_edit failed");

    // Assert
    let edits = replay_journal(db).expect("replay_journal failed");
    assert_eq!(edits.len(), 1);
    match &edits[0] {
        ManifestEdit::AddSst(m) => {
            assert_eq!(m.name, sst_name);
            assert_eq!(m.size_bytes, 1024);
        }
        _ => panic!("unexpected edit variant"),
    }
}

#[test]
fn should_assign_distinct_edit_ids_given_concurrent_manifest_appends() {
    // Arrange
    let td = tempdir().expect("create manifest directory");
    let open_gate = Arc::new(JournalOpenGate::default());
    let first_fs: Arc<dyn Fs> = Arc::new(GatedJournalFs {
        inner: crate::io::real::RealFs::new(td.path()).expect("open first filesystem handle"),
        open_gate: Arc::clone(&open_gate),
    });
    let second_fs: Arc<dyn Fs> = Arc::new(GatedJournalFs {
        inner: crate::io::real::RealFs::new(td.path()).expect("open second filesystem handle"),
        open_gate: Arc::clone(&open_gate),
    });
    let first = std::thread::spawn(move || {
        append_edit_with_fs(
            &first_fs,
            &ManifestEdit::BumpNextSstSeq {
                cf_id: 0,
                next_seq: 1,
            },
        )
    });
    assert!(
        open_gate.wait_for_arrivals(1, std::time::Duration::from_secs(1)),
        "first writer must reach the journal open"
    );
    let (started_tx, started_rx) = std::sync::mpsc::channel();
    let second = std::thread::spawn(move || {
        started_tx.send(()).expect("signal second writer start");
        append_edit_with_fs(
            &second_fs,
            &ManifestEdit::BumpNextSstSeq {
                cf_id: 0,
                next_seq: 2,
            },
        )
    });
    started_rx.recv().expect("observe second writer start");

    // Act
    let concurrent_open = open_gate.wait_for_arrivals(2, std::time::Duration::from_millis(500));
    open_gate.release();
    first
        .join()
        .expect("join first writer")
        .expect("append first manifest edit");
    second
        .join()
        .expect("join second writer")
        .expect("append second manifest edit");
    let replay_fs: Arc<dyn Fs> = Arc::new(
        crate::io::real::RealFs::open_existing(td.path()).expect("open replay filesystem"),
    );
    let replayed = replay_journal_with_ids_with_fs(&replay_fs).expect("replay concurrent appends");
    let edit_ids: Vec<_> = replayed.iter().map(|edit| edit.edit_id).collect();

    // Assert
    assert!(
        !concurrent_open,
        "a second manifest writer entered the journal append transaction"
    );
    assert_eq!(edit_ids, vec![1, 2]);
}

#[test]
fn should_stop_replay_when_partial_tail() {
    // Arrange
    let td = tempdir().unwrap();
    let db = td.path();

    // create a valid edit to precede the partial record
    let file = FileMeta {
        name: "a.sst".to_string(),
        ..Default::default()
    };
    let edit = ManifestEdit::AddSst(file);
    append_edit(db, &edit).expect("append_edit failed");

    // Act: append a truncated record to simulate crash
    let path = db.join(JOURNAL_FILE);
    let mut f = std::fs::OpenOptions::new()
        .append(true)
        .open(&path)
        .expect("failed to open temp journal file");

    let fake = ManifestEdit::RemoveSst {
        name: "missing.sst".to_string(),
    };
    let payload = journal_serialize(&JournalEditEnvelope {
        edit_id: 2,
        edit: fake.clone(),
    })
    .unwrap();

    let mut buf = Vec::new();
    buf.push(fake.record_type());
    buf.extend_from_slice(
        &u32::try_from(payload.len())
            .expect("payload length fits in u32")
            .to_le_bytes(),
    );
    buf.extend_from_slice(&payload[..10.min(payload.len())]); // partial
                                                              // do NOT write crc

    f.write_all(&buf)
        .expect("failed to write partial journal record");
    f.sync_all().expect("failed to sync partial journal file");

    // Assert: replay should only return the first valid record
    let edits = replay_journal(db).expect("replay_journal failed");
    assert_eq!(edits.len(), 1);
}

#[test]
fn should_reject_journal_record_without_edit_envelope() {
    // Arrange: durable edit and batch records whose payloads have no
    // edit envelope.
    let edit = ManifestEdit::BumpNextSstSeq {
        cf_id: 0,
        next_seq: 7,
    };
    let marker = FsyncMarker {
        last_persisted_sequence: 7,
        ts_millis: 0,
    };
    let bare_records = [
        encode_journal_record(edit.record_type(), &edit).expect("encode bare edit"),
        encode_journal_record(BATCH_RECORD_TYPE, std::slice::from_ref(&edit))
            .expect("encode bare batch"),
    ];
    for bare in bare_records {
        let td = tempdir().expect("create temp directory");
        let mut journal = bare;
        journal.extend_from_slice(
            &encode_journal_record(FSYNC_MARKER_TYPE, &marker).expect("encode marker"),
        );
        write_and_sync_test_journal(td.path(), &journal);

        // Act
        let result = replay_journal(td.path());

        // Assert
        assert!(
            matches!(result, Err(crate::common::MidgeError::Corruption(_))),
            "{result:?}"
        );
    }
}

#[test]
fn should_fail_when_corrupt_record_precedes_valid_record() {
    // Arrange
    let td = tempdir().expect("create temp directory");
    let first = ManifestEdit::BumpNextSstSeq {
        cf_id: 0,
        next_seq: 7,
    };
    append_edit(td.path(), &first).expect("append durable first edit");

    let corrupted_middle = ManifestEdit::RemoveSst {
        name: "avoid.sst".to_string(),
    };
    let stale_crc_source = ManifestEdit::RemoveSst {
        name: "apply.sst".to_string(),
    };
    let valid_later = ManifestEdit::BumpNextSstSeq {
        cf_id: 0,
        next_seq: 9,
    };
    let mut suffix = encode_test_record(2, &corrupted_middle, &stale_crc_source);
    suffix.extend_from_slice(&encode_test_record(3, &valid_later, &valid_later));
    let mut journal = std::fs::OpenOptions::new()
        .append(true)
        .open(td.path().join(JOURNAL_FILE))
        .expect("open journal for corrupt suffix");
    journal
        .write_all(&suffix)
        .expect("append corrupt record followed by valid record");
    journal.sync_all().expect("sync corrupt test journal");

    // Act
    let error =
        replay_journal(td.path()).expect_err("mid-file corruption before a valid record must fail");

    // Assert
    assert!(matches!(error, crate::common::MidgeError::Corruption(_)));
}

fn corrupt_journal_record_length(db: &std::path::Path, record_start: u64, len: u32) {
    use std::io::Seek as _;
    let mut journal = std::fs::OpenOptions::new()
        .write(true)
        .open(db.join(JOURNAL_FILE))
        .expect("open journal for length corruption");
    journal
        .seek(std::io::SeekFrom::Start(record_start + 1))
        .expect("seek to record length");
    journal
        .write_all(&len.to_le_bytes())
        .expect("overwrite record length");
    journal.sync_all().expect("sync corrupted journal");
}

#[test]
fn should_fail_replay_when_corrupt_length_hides_durable_later_records() {
    // Arrange
    let td = tempdir().expect("create temp directory");
    append_edit(
        td.path(),
        &ManifestEdit::BumpNextSstSeq {
            cf_id: 0,
            next_seq: 1,
        },
    )
    .expect("append first");
    let second_start = std::fs::metadata(td.path().join(JOURNAL_FILE))
        .expect("journal metadata")
        .len();
    append_edit(
        td.path(),
        &ManifestEdit::BumpNextSstSeq {
            cf_id: 0,
            next_seq: 2,
        },
    )
    .expect("append second");
    append_edit(
        td.path(),
        &ManifestEdit::BumpNextSstSeq {
            cf_id: 0,
            next_seq: 3,
        },
    )
    .expect("append third");
    corrupt_journal_record_length(td.path(), second_start, u32::MAX);

    // Act
    let error = replay_journal(td.path())
        .expect_err("a length that hides durable later records is corruption, not a torn tail");

    // Assert
    match error {
        crate::common::MidgeError::Corruption(message) => {
            assert!(
                message.contains("hides a verified later record"),
                "{message}"
            );
        }
        other => panic!("expected corruption, got {other:?}"),
    }
}

#[test]
fn should_not_truncate_journal_when_corrupt_length_hides_durable_later_records() {
    // Arrange
    let td = tempdir().expect("create temp directory");
    append_edit(
        td.path(),
        &ManifestEdit::BumpNextSstSeq {
            cf_id: 0,
            next_seq: 1,
        },
    )
    .expect("append first");
    let second_start = std::fs::metadata(td.path().join(JOURNAL_FILE))
        .expect("journal metadata")
        .len();
    append_edit(
        td.path(),
        &ManifestEdit::BumpNextSstSeq {
            cf_id: 0,
            next_seq: 2,
        },
    )
    .expect("append second");
    append_edit(
        td.path(),
        &ManifestEdit::BumpNextSstSeq {
            cf_id: 0,
            next_seq: 3,
        },
    )
    .expect("append third");
    corrupt_journal_record_length(td.path(), second_start, u32::MAX);
    let corrupted_len = std::fs::metadata(td.path().join(JOURNAL_FILE))
        .expect("journal metadata")
        .len();

    // Act
    let result = append_edit(
        td.path(),
        &ManifestEdit::BumpNextSstSeq {
            cf_id: 0,
            next_seq: 4,
        },
    );

    // Assert
    assert!(
        result.is_err(),
        "append must not repair past durable records"
    );
    assert_eq!(
        std::fs::metadata(td.path().join(JOURNAL_FILE))
            .expect("journal metadata")
            .len(),
        corrupted_len,
        "durable later records must stay on disk"
    );
}

#[test]
fn should_treat_all_zero_journal_tail_as_torn_append_when_replaying() {
    // Arrange: file size reached disk but the appended bytes did not, a
    // common shape of an unsynced append after power loss.
    let td = tempdir().expect("create temp directory");
    append_edit(
        td.path(),
        &ManifestEdit::BumpNextSstSeq {
            cf_id: 0,
            next_seq: 1,
        },
    )
    .expect("append first");
    append_edit(
        td.path(),
        &ManifestEdit::BumpNextSstSeq {
            cf_id: 0,
            next_seq: 2,
        },
    )
    .expect("append second");
    let mut journal = std::fs::OpenOptions::new()
        .append(true)
        .open(td.path().join(JOURNAL_FILE))
        .expect("open journal");
    journal.write_all(&[0_u8; 64]).expect("append zero tail");
    journal.sync_all().expect("sync zero tail");
    drop(journal);

    // Act
    let edits = replay_journal(td.path()).expect("a zero-filled tail is a torn append");
    append_edit(
        td.path(),
        &ManifestEdit::BumpNextSstSeq {
            cf_id: 0,
            next_seq: 3,
        },
    )
    .expect("append after repair");
    let repaired = replay_journal(td.path()).expect("replay repaired journal");

    // Assert
    assert_eq!(edits.len(), 2);
    assert_eq!(repaired.len(), 3);
}

#[test]
fn should_reject_zero_region_followed_by_data_as_corruption() {
    // Arrange
    let td = tempdir().expect("create temp directory");
    append_edit(
        td.path(),
        &ManifestEdit::BumpNextSstSeq {
            cf_id: 0,
            next_seq: 1,
        },
    )
    .expect("append first");
    let mut journal = std::fs::OpenOptions::new()
        .append(true)
        .open(td.path().join(JOURNAL_FILE))
        .expect("open journal");
    journal.write_all(&[0_u8; 16]).expect("append zeros");
    journal
        .write_all(&[7_u8; 16])
        .expect("append data after zeros");
    journal.sync_all().expect("sync");
    drop(journal);

    // Act
    let result = replay_journal(td.path());

    // Assert
    assert!(matches!(
        result,
        Err(crate::common::MidgeError::Corruption(_))
    ));
}

#[test]
fn should_sync_database_directory_when_first_manifest_append_creates_journal() {
    // Arrange
    let mock = Arc::new(crate::io::MockFs::new());
    let fs: Arc<dyn Fs> = mock.clone();
    let root_syncs = |mock: &crate::io::MockFs| {
        mock.sync_dir_calls()
            .into_iter()
            .filter(|(path, durability)| path.0 == "." && *durability == Durability::Durable)
            .count()
    };

    // Act
    append_edit_with_fs(
        &fs,
        &ManifestEdit::BumpNextSstSeq {
            cf_id: 0,
            next_seq: 1,
        },
    )
    .expect("create journal");
    let after_create = root_syncs(&mock);
    append_edit_with_fs(
        &fs,
        &ManifestEdit::BumpNextSstSeq {
            cf_id: 0,
            next_seq: 2,
        },
    )
    .expect("append again");

    // Assert
    assert_eq!(
        after_create, 1,
        "creating the journal must make its directory entry durable"
    );
    assert_eq!(root_syncs(&mock), 1, "later appends need no directory sync");
}

#[test]
fn should_fail_when_first_journal_record_crc_is_stale() {
    // Arrange
    let td = tempdir().expect("create temp directory");
    let corrupted_first = ManifestEdit::RemoveSst {
        name: "avoid.sst".to_string(),
    };
    let stale_crc_source = ManifestEdit::RemoveSst {
        name: "apply.sst".to_string(),
    };
    let journal = encode_test_record(1, &corrupted_first, &stale_crc_source);
    write_and_sync_test_journal(td.path(), &journal);

    // Act
    let error = replay_journal(td.path()).expect_err("first CRC mismatch must fail");

    // Assert
    assert!(matches!(error, crate::common::MidgeError::Corruption(_)));
}

#[test]
fn should_truncate_partial_eof_tail_before_next_append() {
    // Arrange
    let td = tempdir().expect("create temp directory");
    let first = ManifestEdit::BumpNextSstSeq {
        cf_id: 0,
        next_seq: 1,
    };
    append_edit(td.path(), &first).expect("append first edit");

    let partial = ManifestEdit::BumpNextSstSeq {
        cf_id: 0,
        next_seq: 2,
    };
    let encoded = encode_test_record(2, &partial, &partial);
    let mut journal = std::fs::OpenOptions::new()
        .append(true)
        .open(td.path().join(JOURNAL_FILE))
        .expect("open journal for partial tail");
    journal
        .write_all(&encoded[..encoded.len() - 3])
        .expect("append partial EOF record");
    journal.sync_all().expect("sync partial EOF record");
    drop(journal);

    let after_repair = ManifestEdit::BumpNextSstSeq {
        cf_id: 0,
        next_seq: 3,
    };

    // Act
    append_edit(td.path(), &after_repair).expect("repair tail and append next edit");
    let edits = replay_journal(td.path()).expect("replay repaired journal");

    // Assert
    assert_eq!(edits.len(), 2);
    assert!(matches!(
        edits[0],
        ManifestEdit::BumpNextSstSeq {
            cf_id: 0,
            next_seq: 1
        }
    ));
    assert!(matches!(
        edits[1],
        ManifestEdit::BumpNextSstSeq {
            cf_id: 0,
            next_seq: 3
        }
    ));
}

#[cfg(feature = "failpoints")]
#[test]
fn should_return_error_when_fsync_marker_write_fails() {
    // Arrange
    let td = tempdir().expect("create temp directory");
    let _test_guard = crate::failpoints::test_failpoint_guard();
    append_edit(
        td.path(),
        &ManifestEdit::BumpNextSstSeq {
            cf_id: 0,
            next_seq: 1,
        },
    )
    .expect("append durable prefix");
    let marker_failure = fail::FailGuard::new(
        "midge::manifest::inject_fsync_marker_write_failure",
        "return",
    )
    .expect("configure marker-write failpoint");

    // Act
    let error = append_edit(
        td.path(),
        &ManifestEdit::BumpNextSstSeq {
            cf_id: 0,
            next_seq: 2,
        },
    )
    .expect_err("marker write failure must be returned");
    drop(marker_failure);
    append_edit(
        td.path(),
        &ManifestEdit::BumpNextSstSeq {
            cf_id: 0,
            next_seq: 3,
        },
    )
    .expect("repair incomplete append and write next edit");
    let replayed = replay_journal(td.path()).expect("replay repaired journal");

    // Assert
    assert!(error.to_string().contains("marker"));
    assert_eq!(replayed.len(), 2);
    assert!(matches!(
        replayed[0],
        ManifestEdit::BumpNextSstSeq {
            cf_id: 0,
            next_seq: 1
        }
    ));
    assert!(matches!(
        replayed[1],
        ManifestEdit::BumpNextSstSeq {
            cf_id: 0,
            next_seq: 3
        }
    ));
}

#[cfg(feature = "failpoints")]
#[test]
fn should_return_error_when_required_manifest_sync_fails() {
    // Arrange
    let td = tempdir().expect("create temp directory");
    let _test_guard = crate::failpoints::test_failpoint_guard();
    let _guard = fail::FailGuard::new("midge::manifest::inject_required_sync_failure", "return")
        .expect("configure sync failpoint");

    // Act
    let error = append_edit(
        td.path(),
        &ManifestEdit::BumpNextSstSeq {
            cf_id: 0,
            next_seq: 1,
        },
    )
    .expect_err("required sync failure must be returned");

    // Assert
    assert!(error.to_string().contains("sync"));
}

#[cfg(feature = "failpoints")]
#[test]
fn should_issue_one_required_sync_for_manifest_append() {
    // Arrange
    let td = tempdir().expect("create temp directory");
    let _test_guard = crate::failpoints::test_failpoint_guard();
    let syncs = std::sync::Arc::new(std::sync::atomic::AtomicUsize::new(0));
    let observed_syncs = std::sync::Arc::clone(&syncs);
    let _guard =
        fail::FailGuard::with_callback("midge::manifest::before_required_sync", move || {
            OBSERVE_REQUIRED_MANIFEST_SYNC.with(|observe| {
                if observe.get() {
                    observed_syncs.fetch_add(1, std::sync::atomic::Ordering::SeqCst);
                }
            });
        })
        .expect("configure sync observer");

    // Act
    OBSERVE_REQUIRED_MANIFEST_SYNC.with(|observe| observe.set(true));
    append_edit(
        td.path(),
        &ManifestEdit::BumpNextSstSeq {
            cf_id: 0,
            next_seq: 1,
        },
    )
    .expect("append manifest edit");
    OBSERVE_REQUIRED_MANIFEST_SYNC.with(|observe| observe.set(false));

    // Assert
    assert_eq!(syncs.load(std::sync::atomic::Ordering::SeqCst), 1);
}

#[test]
fn should_save_snapshot_to_disk_when_saved() {
    // Arrange
    let td = tempdir().unwrap();
    let db = td.path();

    let mut manifest = Manifest::default();
    manifest.files.push(FileMeta {
        name: "one.sst".to_string(),
        level: 0,
        size_bytes: 100,
        ..Default::default()
    });

    // Act
    ManifestPersistence::save_snapshot_and_truncate_journal(db, &manifest)
        .expect("save snapshot failed");

    // Assert
    let snap = ManifestPersistence::manifest_snapshot_path(db);
    assert!(snap.exists(), "snapshot file should exist after saving");
}

#[test]
fn should_truncate_journal_after_snapshot_saved() {
    // Arrange
    let td = tempdir().unwrap();
    let db = td.path();

    let mut manifest = Manifest::default();
    manifest.files.push(FileMeta {
        name: "one.sst".to_string(),
        level: 0,
        size_bytes: 100,
        ..Default::default()
    });

    // Act
    ManifestPersistence::save_snapshot_and_truncate_journal(db, &manifest)
        .expect("save snapshot failed");

    // Assert: journal should be truncated (zero-length) if it exists
    let journal = db.join(JOURNAL_FILE);
    if journal.exists() {
        let meta = std::fs::metadata(&journal).unwrap();
        assert_eq!(
            meta.len(),
            0,
            "journal should be zero-length after snapshot/truncate"
        );
    }
}

#[test]
fn should_load_snapshot_when_snapshot_exists() {
    // Arrange
    let td = tempdir().unwrap();
    let db = td.path();

    // Create snapshot
    let mut manifest = Manifest::default();
    manifest.files.push(FileMeta {
        name: "base.sst".to_string(),
        level: 0,
        size_bytes: 10,
        ..Default::default()
    });
    ManifestPersistence::save_snapshot_and_truncate_journal(db, &manifest)
        .expect("save snapshot failed");

    // Act
    let loaded = ManifestPersistence::load(db).expect("load failed");

    // Assert
    assert!(
        loaded.files.iter().any(|f| f.name == "base.sst"),
        "loaded manifest should include snapshot entries"
    );
}

#[test]
fn should_replay_journal_on_top_of_snapshot_when_present() {
    // Arrange
    let td = tempdir().unwrap();
    let db = td.path();

    // Create snapshot
    let mut manifest = Manifest::default();
    manifest.files.push(FileMeta {
        name: "base.sst".to_string(),
        level: 0,
        size_bytes: 10,
        ..Default::default()
    });
    ManifestPersistence::save_snapshot_and_truncate_journal(db, &manifest)
        .expect("save snapshot failed");

    // Act: append an edit that should be replayed on load
    let edit = ManifestEdit::AddSst(FileMeta {
        name: "new.sst".to_string(),
        level: 1,
        size_bytes: 20,
        ..Default::default()
    });
    append_edit(db, &edit).expect("append_edit failed");

    // Assert
    let loaded = ManifestPersistence::load(db).expect("load failed");
    assert!(
        loaded.files.iter().any(|f| f.name == "base.sst"),
        "snapshot entry missing"
    );
    assert!(
        loaded.files.iter().any(|f| f.name == "new.sst"),
        "journal replay entry missing"
    );
}

#[test]
fn should_replay_batch_record() {
    // Arrange
    let td = tempdir().unwrap();
    let db = td.path();

    let e1 = ManifestEdit::AddSst(FileMeta {
        name: "b1.sst".to_string(),
        level: 0,
        size_bytes: 10,
        ..Default::default()
    });
    let e2 = ManifestEdit::RemoveSst {
        name: "missing.sst".to_string(),
    };

    // Act
    append_edit_batch(db, &[e1.clone(), e2.clone()]).expect("append batch failed");

    // Assert: replay should yield a single Batch record
    let edits = replay_journal(db).expect("replay_journal failed");
    assert_eq!(edits.len(), 1);
    match &edits[0] {
        ManifestEdit::Batch(b) => {
            assert_eq!(b.len(), 2);
        }
        _ => panic!("expected batch record"),
    }
}

#[test]
fn should_emit_fsync_marker_for_batch_append() {
    // Arrange
    let td = tempdir().unwrap();
    let db = td.path();

    // Act
    let e = ManifestEdit::BumpNextSstSeq {
        cf_id: 0,
        next_seq: 1,
    };
    append_edit_batch(db, std::slice::from_ref(&e)).expect("append batch failed");

    // Assert: the journal contains exactly two structurally valid
    // records in order - the batch edit record immediately followed
    // by an FSYNC_MARKER_TYPE record (not just the marker byte
    // appearing somewhere in the raw file bytes) - and the marker's
    // own payload references the edit it is durability-confirming.
    let fs: std::sync::Arc<dyn crate::io::traits::Fs> =
        std::sync::Arc::new(crate::io::real::RealFs::new(db).expect("open RealFs"));
    let file = open_journal_for_replay(&fs)
        .expect("open journal for replay")
        .expect("journal file must exist after append");
    let file_len = std::fs::metadata(db.join(JOURNAL_FILE))
        .expect("stat journal")
        .len();

    let JournalRecordStatus::Record(batch_record) =
        read_journal_record(&*file, 0, file_len).expect("read first journal record")
    else {
        panic!("expected a complete record at offset 0");
    };
    assert_eq!(
        batch_record.typ, BATCH_RECORD_TYPE,
        "first record must be the batch edit record"
    );

    let JournalRecordStatus::Record(marker_record) =
        read_journal_record(&*file, batch_record.next_offset, file_len)
            .expect("read second journal record")
    else {
        panic!("expected a complete record immediately after the batch record");
    };
    assert_eq!(
        marker_record.typ, FSYNC_MARKER_TYPE,
        "second record must be the fsync marker, positioned right after the batch record"
    );
    assert_eq!(
        marker_record.next_offset, file_len,
        "fsync marker must be the last record in the journal"
    );

    let marker: FsyncMarker =
        journal_deserialize(&marker_record.payload).expect("fsync marker payload must deserialize");
    assert_eq!(
        marker.last_persisted_sequence, 1,
        "fsync marker must reference the edit id it confirms durability for"
    );
}

proptest! {
    #[test]
    fn should_preserve_batch_order_when_replaying_allocation_edits(
        seqs in proptest::collection::vec(0u64..10_000, 1..32)
    ) {
        // Arrange
        let td = tempdir().unwrap();
        let db = td.path();
        let batch: Vec<ManifestEdit> = seqs
            .iter()
            .copied()
            .map(|seq| ManifestEdit::BumpNextSstSeq { cf_id: 0, next_seq: seq })
            .collect();

        // Act
        append_edit_batch(db, &batch).expect("append batch failed");

        let replayed = replay_journal(db).expect("replay_journal failed");
        prop_assert_eq!(replayed.len(), 1);

        // Assert
        match &replayed[0] {
            ManifestEdit::Batch(inner) => {
                prop_assert_eq!(inner.len(), batch.len());
                for (observed, expected) in inner.iter().zip(batch.iter()) {
                    match (observed, expected) {
                        (
                            ManifestEdit::BumpNextSstSeq { cf_id: 0, next_seq: observed_seq },
                            ManifestEdit::BumpNextSstSeq { cf_id: 0, next_seq: expected_seq },
                        ) => prop_assert_eq!(observed_seq, expected_seq),
                        other => prop_assert!(false, "unexpected replayed edit pair: {:?}", other),
                    }
                }
            }
            other => prop_assert!(false, "expected batch replay, got {:?}", other),
        }

        let mut direct_manifest = Manifest::default();
        direct_manifest.apply_edit(&ManifestEdit::Batch(batch.clone())).expect("valid manifest fixture edit");
        let mut replayed_manifest = Manifest::default();
        replayed_manifest.apply_edit(&replayed[0]).expect("valid manifest fixture edit");

        prop_assert_eq!(
            replayed_manifest.next_sst_seqs.get(&0).copied().unwrap_or(0),
            direct_manifest.next_sst_seqs.get(&0).copied().unwrap_or(0)
        );
    }
}
