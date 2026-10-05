//! Actual framed metadata and independent delegated-filesystem accounting.
//! API scaffolding must compile before RED; no producer counters are seeded.

use super::fs_oracle::{observe_fs, Fault, Observations};
use crate::config::RecoveryPolicy;
use crate::io::{Fs, RealFs};
use crate::metadata::accounting::{Counters, Medium, Origin, Owner, Snapshot};
use crate::metadata::store::ManifestStore;
use crate::metadata::{journal, Manifest, ManifestEdit, ManifestPersistence};
use std::io::Write;
use std::sync::Arc;

pub(super) const SNAPSHOT_STAGE: &str = "manifest.snapshot.json.tmp";

struct Fixture {
    directory: tempfile::TempDir,
    fs: Arc<dyn Fs>,
    oracle: Arc<Observations>,
    owner: Owner,
    store: ManifestStore,
}

impl Fixture {
    fn new() -> Self {
        let directory = tempfile::tempdir().expect("actual metadata directory");
        let raw: Arc<dyn Fs> = Arc::new(RealFs::new(directory.path()).expect("actual RealFs"));
        let oracle = Arc::new(Observations::default());
        let fs = observe_fs(raw, Arc::clone(&oracle));
        let owner = Owner::new();
        let store =
            ManifestStore::new_with_accounting(Arc::clone(&fs), owner.clone(), Medium::Persistent);
        Self {
            directory,
            fs,
            oracle,
            owner,
            store,
        }
    }

    fn snapshot(&self) -> Snapshot {
        self.owner.handle().snapshot()
    }

    fn bytes(&self, path: &str) -> Vec<u8> {
        std::fs::read(self.directory.path().join(path)).expect("actual metadata bytes")
    }

    fn seed_manifest(&self) -> Manifest {
        let edit = ManifestEdit::BumpWalSeq { seq: 7 };
        let edit_id = self
            .store
            .append_for(Origin::Ddl, &edit)
            .expect("genuine durable baseline journal");
        let mut manifest = Manifest::default();
        manifest.apply_edit(&edit);
        manifest.note_applied_journal_edit(edit_id);
        manifest
    }
}

fn counters(snapshot: &Snapshot, origin: Origin) -> &Counters {
    &snapshot.bucket(origin, Medium::Persistent).counters
}

#[test]
fn should_account_exact_frames_when_actual_single_and_batch_edits_commit() {
    crate::failpoints::with_read_gate(|| {
        // Arrange: the independent oracle sees real appends, not metric fields.
        let fixture = Fixture::new();
        let before = fixture.snapshot();

        // Act: one real single edit and one real batch each commit a marker.
        let first = fixture
            .store
            .append_for(
                Origin::OrdinaryLocalFlush,
                &ManifestEdit::BumpWalSeq { seq: 1 },
            )
            .expect("actual single append");
        let second = fixture
            .store
            .append_batch_for(
                Origin::OrdinaryLocalFlush,
                &[
                    ManifestEdit::BumpWalSeq { seq: 2 },
                    ManifestEdit::BumpWalSeq { seq: 3 },
                ],
            )
            .expect("actual batch append");
        let replayed = journal::replay_journal_with_fs(&fixture.fs).expect("strict actual replay");
        let bytes = fixture.bytes(crate::metadata::files::JOURNAL);
        let offered = fixture
            .oracle
            .bytes_for(crate::metadata::files::JOURNAL, false);
        let returned = fixture
            .oracle
            .bytes_for(crate::metadata::files::JOURNAL, true);
        let after = fixture.snapshot();
        let delta = after.delta(&before).expect("same bounded owner");

        // Assert: prove framing/data first, then compare the producer to the oracle.
        assert_eq!((first, second), (1, 2));
        assert_eq!(replayed.len(), 2, "single edit and one atomic batch record");
        assert!(matches!(replayed[0], ManifestEdit::BumpWalSeq { seq: 1 }));
        let ManifestEdit::Batch(batch) = &replayed[1] else {
            panic!("the second record must retain its atomic batch");
        };
        assert_eq!(batch.len(), 2);
        for (edit, sequence) in batch.iter().zip(2..=3) {
            assert!(matches!(edit, ManifestEdit::BumpWalSeq { seq } if *seq == sequence));
        }
        assert_eq!(
            fixture.oracle.writes().len(),
            4,
            "two records plus two markers"
        );
        assert!(offered > 0);
        assert_eq!(offered, u64::try_from(bytes.len()).unwrap());
        assert_eq!(offered, returned);
        let observed = counters(&delta, Origin::OrdinaryLocalFlush);
        assert_eq!(observed.issued_bytes, [0, offered, 0]);
        assert_eq!(observed.returned_write_bytes, [0, returned, 0]);
        assert_eq!(observed.journal_durable_bytes, returned);
        assert_eq!(observed.journal_append_attempts, 2);
        assert_eq!(observed.operation_attempts, 2);
        assert_eq!(observed.operation_failures, 0);
        assert_eq!(
            delta
                .bucket(Origin::OrdinaryLocalFlush, Medium::Persistent)
                .active_operations,
            0
        );
        assert_eq!(
            delta
                .bucket(Origin::OrdinaryLocalFlush, Medium::MemoryOnly)
                .counters
                .operation_attempts,
            0
        );
    });
}

#[test]
fn should_credit_completed_checkpoint_when_actual_snapshot_and_truncation_commit() {
    crate::failpoints::with_read_gate(|| {
        // Arrange: actual journaled metadata is current before the checkpoint.
        let fixture = Fixture::new();
        let manifest = fixture.seed_manifest();
        let before = fixture.snapshot();
        fixture.oracle.clear_writes();

        // Act: actual stage, rename, barriers and journal truncation execute.
        let written = fixture
            .store
            .save_snapshot_for(Origin::CompactionBeforeGc, &manifest)
            .expect("actual checkpoint");
        let recovered =
            ManifestPersistence::load_with_fs_and_policy(&fixture.fs, RecoveryPolicy::Strict)
                .expect("actual strict checkpoint recovery");
        let bytes = fixture.bytes(crate::metadata::files::MANIFEST_SNAPSHOT);
        let offered = fixture.oracle.bytes_for(SNAPSHOT_STAGE, false);
        let returned = fixture.oracle.bytes_for(SNAPSHOT_STAGE, true);
        let delta = fixture
            .snapshot()
            .delta(&before)
            .expect("same owner checkpoint delta");

        // Assert: exact durable metadata and zero journal precede metric acceptance.
        assert!(written.caller_was_current);
        assert_eq!(recovered.edit_checkpoint_id, written.edit_checkpoint_id);
        assert_eq!(
            serde_json::to_value(recovered).unwrap(),
            serde_json::from_slice::<serde_json::Value>(&bytes).unwrap()
        );
        assert_eq!(
            fixture.bytes(crate::metadata::files::JOURNAL),
            [] as [u8; 0]
        );
        assert!(offered > 0);
        assert_eq!(offered, u64::try_from(bytes.len()).unwrap());
        assert_eq!(offered, returned);
        let observed = counters(&delta, Origin::CompactionBeforeGc);
        assert_eq!(observed.issued_bytes, [offered, 0, 0]);
        assert_eq!(observed.returned_write_bytes, [returned, 0, 0]);
        assert_eq!(observed.snapshot_durable_bytes, returned);
        assert_eq!(observed.snapshot_durable_count, 1);
        assert_eq!(observed.checkpoint_complete_count, 1);
        assert_eq!(observed.checkpoint_attempts, 1);
        assert_eq!(observed.operation_failures, 0);
    });
}

#[test]
fn should_preserve_attempted_payload_when_actual_checkpoint_io_fails() {
    crate::failpoints::with_read_gate(|| {
        // Arrange: each case starts from genuine durable baseline bytes.
        for fault in [
            Fault::OpenSnapshot,
            Fault::WriteSnapshot,
            Fault::RenameSnapshot,
        ] {
            let fixture = Fixture::new();
            let manifest = fixture.seed_manifest();
            fixture
                .store
                .save_snapshot_for(Origin::Ddl, &manifest)
                .expect("baseline checkpoint");
            let retained = fixture.bytes(crate::metadata::files::MANIFEST_SNAPSHOT);
            fixture.oracle.clear_writes();
            let before = fixture.snapshot();
            fixture.oracle.arm(fault);

            // Act: one actual filesystem boundary returns its original typed error.
            let result = fixture
                .store
                .save_snapshot_for(Origin::CompactionBeforeGc, &manifest);
            let offered = fixture.oracle.bytes_for(SNAPSHOT_STAGE, false);
            let returned = fixture.oracle.bytes_for(SNAPSHOT_STAGE, true);
            let delta = fixture
                .snapshot()
                .delta(&before)
                .expect("complete failed-operation observation");

            // Assert: retained bytes/error class remain authoritative, no durable credit.
            assert_eq!(
                fixture.bytes(crate::metadata::files::MANIFEST_SNAPSHOT),
                retained
            );
            match fault {
                Fault::OpenSnapshot | Fault::WriteSnapshot => assert!(
                    matches!(result, Err(crate::common::MidgeError::NoSpace(_))),
                    "original NoSpace: {result:?}"
                ),
                Fault::RenameSnapshot => assert!(
                    matches!(result, Err(crate::common::MidgeError::Io(_))),
                    "original Io: {result:?}"
                ),
                Fault::SyncJournal => unreachable!("not a snapshot boundary"),
            }
            if fault == Fault::OpenSnapshot {
                assert_eq!(offered, 0, "open failure never offers payload");
            } else {
                assert!(offered > 0, "actual offered serialized snapshot");
            }
            assert_eq!(
                returned,
                if fault == Fault::RenameSnapshot {
                    offered
                } else {
                    0
                }
            );
            let observed = counters(&delta, Origin::CompactionBeforeGc);
            assert_eq!(observed.issued_bytes, [offered, 0, 0]);
            assert_eq!(observed.returned_write_bytes, [returned, 0, 0]);
            assert_eq!(observed.snapshot_durable_bytes, 0);
            assert_eq!(observed.checkpoint_complete_count, 0);
            assert_eq!(observed.checkpoint_attempts, 1);
            assert_eq!(observed.operation_failures, 1);
        }
    });
}

#[test]
fn should_preserve_returned_frames_when_actual_journal_sync_fails() {
    crate::failpoints::with_read_gate(|| {
        // Arrange: an actual durable prefix precedes the targeted sync failure.
        let fixture = Fixture::new();
        fixture.seed_manifest();
        let retained = fixture.bytes(crate::metadata::files::JOURNAL);
        let before = fixture.snapshot();
        fixture.oracle.clear_writes();
        fixture.oracle.arm(Fault::SyncJournal);

        // Act: both real frames return successfully, then required sync fails.
        let result = fixture.store.append_for(
            Origin::OrdinaryLocalFlush,
            &ManifestEdit::BumpWalSeq { seq: 9 },
        );
        let offered = fixture
            .oracle
            .bytes_for(crate::metadata::files::JOURNAL, false);
        let returned = fixture
            .oracle
            .bytes_for(crate::metadata::files::JOURNAL, true);
        let disk = fixture.bytes(crate::metadata::files::JOURNAL);
        let delta = fixture
            .snapshot()
            .delta(&before)
            .expect("complete failed sync observation");

        // Assert: offered/returned payload is distinct from durable confirmation.
        assert!(
            matches!(result, Err(crate::common::MidgeError::Io(_))),
            "original Io: {result:?}"
        );
        assert!(disk.starts_with(&retained));
        assert_eq!(fixture.oracle.writes().len(), 2);
        assert!(offered > 0);
        assert_eq!(returned, offered);
        let observed = counters(&delta, Origin::OrdinaryLocalFlush);
        assert_eq!(observed.issued_bytes, [0, offered, 0]);
        assert_eq!(observed.returned_write_bytes, [0, returned, 0]);
        assert_eq!(observed.journal_durable_bytes, 0);
        assert_eq!(observed.operation_failures, 1);
        // Presence after failed sync is not a crash-durability guarantee.
    });
}

fn append_actual_partial_frame(fixture: &Fixture) {
    let source = Fixture::new();
    journal::append_edit_with_fs(&source.fs, &ManifestEdit::BumpWalSeq { seq: 2 })
        .expect("generate a genuine encoded edit and marker");
    let frames = source.oracle.writes();
    assert_eq!(frames.len(), 2);
    let frame_length = usize::try_from(frames[0].bytes).unwrap();
    assert!(frame_length > 3);
    let encoded = source.bytes(crate::metadata::files::JOURNAL);
    let mut file = std::fs::OpenOptions::new()
        .append(true)
        .open(
            fixture
                .directory
                .path()
                .join(crate::metadata::files::JOURNAL),
        )
        .expect("append genuine partial EOF frame");
    file.write_all(&encoded[..frame_length - 3]).unwrap();
    file.sync_all().unwrap();
}

#[test]
fn should_account_position_repair_when_actual_partial_eof_precedes_append() {
    crate::failpoints::with_read_gate(|| {
        // Arrange: real durable prefix plus a real truncated encoded next frame.
        let fixture = Fixture::new();
        journal::append_edit_with_fs(&fixture.fs, &ManifestEdit::BumpWalSeq { seq: 1 })
            .expect("actual durable baseline prefix");
        let retained = fixture.bytes(crate::metadata::files::JOURNAL);
        append_actual_partial_frame(&fixture);
        fixture.oracle.clear_writes();
        let before = fixture.snapshot();

        // Act: the fresh store reconstructs position, repairs, then appends.
        let assigned = fixture
            .store
            .append_for(Origin::Recovery, &ManifestEdit::BumpWalSeq { seq: 3 })
            .expect("actual partial EOF repair and next append");
        let replayed =
            journal::replay_journal_with_fs(&fixture.fs).expect("actual strict repaired replay");
        let journal_bytes = fixture
            .oracle
            .bytes_for(crate::metadata::files::JOURNAL, false);
        let repair_bytes = fixture
            .oracle
            .bytes_for("manifest.journal.repair.tmp", false);
        let delta = fixture
            .snapshot()
            .delta(&before)
            .expect("same owner reconstructs position");

        // Assert: genuine positive rewrite and exact data precede metric checks.
        assert_eq!(assigned, 2);
        assert_eq!(replayed.len(), 2);
        assert!(matches!(replayed[0], ManifestEdit::BumpWalSeq { seq: 1 }));
        assert!(matches!(replayed[1], ManifestEdit::BumpWalSeq { seq: 3 }));
        assert!(fixture
            .bytes(crate::metadata::files::JOURNAL)
            .starts_with(&retained));
        assert_eq!(repair_bytes, u64::try_from(retained.len()).unwrap());
        assert!(repair_bytes > 0);
        assert_eq!(
            fixture.oracle.writes().len(),
            3,
            "one prefix rewrite, next edit and marker"
        );
        let observed = counters(&delta, Origin::Recovery);
        assert_eq!(observed.issued_bytes, [0, journal_bytes, repair_bytes]);
        assert_eq!(
            observed.returned_write_bytes,
            [0, journal_bytes, repair_bytes]
        );
        assert_eq!(observed.journal_durable_bytes, journal_bytes);
        assert_eq!(observed.journal_append_attempts, 1);
        assert_eq!(observed.operation_failures, 0);
    });
}
