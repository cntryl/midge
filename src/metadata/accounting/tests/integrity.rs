//! Actual filesystem controls for accounting integrity and operation lifetime.
//! Insert beside the metadata accounting tests; these do not prove provider/device timing.

use super::*;
use crate::io::{Durability, Fs, FsPath, OpenMode, OpenOptions, RealFs};
use crate::metadata::accounted_fs::account_fs;
use bytes::Bytes;

#[test]
fn should_detect_late_mutation_when_an_accounted_view_changes_namespace() {
    // Arrange: genuine existing bytes and one completed accounting operation.
    let directory = tempfile::tempdir().expect("actual namespace filesystem");
    let raw: Arc<dyn Fs> = Arc::new(RealFs::new(directory.path()).expect("actual Fs"));
    std::fs::write(directory.path().join("seed"), b"retained bytes").unwrap();
    let owner = Owner::new();
    let handle = owner.handle();
    let before = handle.snapshot();
    let operation = owner.begin(
        OperationKind::Checkpoint,
        Origin::Recovery,
        Medium::Persistent,
    );
    let view = account_fs(raw, operation.ledger());
    operation.finish(true);
    assert!(view.exists(&FsPath::new("seed")).unwrap());
    assert_eq!(handle.snapshot().late_operation_writes, 0);

    // Act: real namespace/barrier/open mutations offer no payload through File.
    view.create_dir_all(&FsPath::new("late-directory")).unwrap();
    drop(
        view.open(
            &FsPath::new("seed"),
            OpenOptions {
                mode: OpenMode::ReadWrite,
                create: false,
                create_new: false,
                truncate: true,
            },
        )
        .unwrap(),
    );
    let truncated = std::fs::read(directory.path().join("seed")).unwrap();
    view.rename_atomic(&FsPath::new("seed"), &FsPath::new("renamed"))
        .unwrap();
    view.sync_dir(&FsPath::new("."), Durability::Durable)
        .unwrap();
    view.remove_file(&FsPath::new("renamed")).unwrap();
    view.remove_dir_all(&FsPath::new("late-directory")).unwrap();
    drop(
        view.open_persistent_handle(
            &FsPath::new("created"),
            OpenOptions {
                mode: OpenMode::ReadWrite,
                create: false,
                create_new: true,
                truncate: false,
            },
        )
        .unwrap(),
    );
    let created = view.exists(&FsPath::new("created")).unwrap();
    view.remove_file(&FsPath::new("created")).unwrap();
    drop(view);
    drop(owner);
    let after = handle.snapshot();

    // Assert: actual effects invalidate integrity without inventing byte credit.
    assert_eq!(truncated, [] as [u8; 0]);
    assert!(created);
    assert!(!directory.path().join("seed").exists());
    assert!(!directory.path().join("renamed").exists());
    assert!(!directory.path().join("late-directory").exists());
    assert!(!directory.path().join("created").exists());
    assert_eq!(after.late_operation_writes, 16);
    assert!(after.delta(&before).is_err());
    assert_eq!(
        after
            .bucket(Origin::Recovery, Medium::Persistent)
            .counters
            .issued_bytes,
        [0; 3]
    );
    assert_eq!(
        after
            .bucket(Origin::Recovery, Medium::Persistent)
            .counters
            .returned_write_bytes,
        [0; 3]
    );
}

#[test]
fn should_detect_late_return_when_operation_finishes_during_actual_sync() {
    // Arrange: a real sync completes underneath a finite child-owned return gate.
    let directory = tempfile::tempdir().expect("actual held-sync filesystem");
    let raw: Arc<dyn Fs> = Arc::new(RealFs::new(directory.path()).expect("actual Fs"));
    std::fs::write(directory.path().join("retained"), b"exact retained bytes").unwrap();
    let oracle = Arc::new(super::fs_oracle::Observations::default());
    let raw = super::fs_oracle::observe_fs(raw, Arc::clone(&oracle));
    let owner = Owner::new();
    let handle = owner.handle();
    let before = handle.snapshot();
    let operation = owner.begin(
        OperationKind::Checkpoint,
        Origin::Recovery,
        Medium::Persistent,
    );
    let view = account_fs(raw, operation.ledger());
    let mut file = view
        .open_persistent_handle(
            &FsPath::new("retained"),
            OpenOptions {
                mode: OpenMode::ReadWrite,
                create: false,
                create_new: false,
                truncate: false,
            },
        )
        .unwrap();
    let (entered_tx, entered_rx) = crossbeam::channel::bounded(1);
    let (release_tx, release_rx) = crossbeam::channel::bounded(1);
    oracle.hold_next_successful_sync(entered_tx, release_rx);

    // Act: only a positively completed real sync can trigger operation closure.
    let worker = std::thread::spawn(move || file.sync(Durability::Durable));
    let entered = entered_rx.recv_timeout(Duration::from_secs(3));
    let was_held = !worker.is_finished();
    operation.finish(true);
    let _ = release_tx.send(());
    drop(release_tx);
    let result = worker.join();
    drop(view);
    drop(owner);
    let after = handle.snapshot();

    // Assert: join/release precedes every assertion and original FsResult survives.
    assert!(
        entered.is_ok(),
        "genuine sync completion handshake: {entered:?}"
    );
    assert!(was_held, "successful sync was still held before finish");
    assert!(result.unwrap().is_ok());
    assert_eq!(
        std::fs::read(directory.path().join("retained")).unwrap(),
        b"exact retained bytes"
    );
    assert_eq!(
        after.late_operation_writes, 1,
        "only the completed-return observation is late"
    );
    assert!(after.delta(&before).is_err());
    assert_eq!(
        after
            .bucket(Origin::Recovery, Medium::Persistent)
            .counters
            .issued_bytes,
        [0; 3]
    );
    assert_eq!(
        after
            .bucket(Origin::Recovery, Medium::Persistent)
            .counters
            .returned_write_bytes,
        [0; 3]
    );
}

#[test]
fn should_detect_late_accounting_when_a_persistent_file_outlives_its_operation() {
    // Arrange: acquire a genuine persistent local file through the actual adapter.
    let directory = tempfile::tempdir().expect("create actual local filesystem");
    let raw: Arc<dyn Fs> = Arc::new(RealFs::new(directory.path()).expect("open actual Fs"));
    let owner = Owner::new();
    let handle = owner.handle();
    let before = handle.snapshot();
    let operation = owner.begin(
        OperationKind::JournalAppend,
        Origin::Recovery,
        Medium::Persistent,
    );
    let view = account_fs(raw, operation.ledger());
    let mut file = view
        .open_persistent_handle(
            &FsPath::new(crate::metadata::files::JOURNAL),
            OpenOptions {
                mode: OpenMode::ReadWrite,
                create: true,
                create_new: false,
                truncate: false,
            },
        )
        .expect("acquire actual persistent handle");
    file.append(Bytes::from_static(b"first"))
        .expect("actual append");
    file.sync(Durability::Durable).expect("actual file sync");
    operation.journal_durable(5);
    operation.finish(true);
    let initial = handle.snapshot();
    assert!(
        initial.delta(&before).is_ok(),
        "initial accounting is complete"
    );
    assert_eq!(initial.late_operation_writes, 0);
    drop(view);

    // Act: the real file escapes its sealed accounting operation and writes again.
    // This deliberate misuse is the integrity control, not a known runtime behavior.
    file.append(Bytes::from_static(b"late"))
        .expect("real late append");
    file.sync(Durability::Durable).expect("real late sync");
    drop(file);
    drop(owner);
    let final_owner = handle.snapshot();

    // Assert: end-of-window acceptance cannot hide later incomplete accounting.
    assert_eq!(
        std::fs::read(directory.path().join(crate::metadata::files::JOURNAL)).unwrap(),
        b"firstlate"
    );
    assert_eq!(
        final_owner.late_operation_writes, 4,
        "issued/returned payload and sync admission/completion escaped"
    );
    assert!(final_owner.delta(&before).is_err());
    assert_eq!(
        final_owner
            .bucket(Origin::Recovery, Medium::Persistent)
            .counters
            .journal_durable_bytes,
        5
    );
}

#[test]
fn should_detect_late_mutation_when_a_persistent_file_truncates_without_payload() {
    // Arrange: genuine existing bytes, a persistent file, and a sealed owner.
    let directory = tempfile::tempdir().expect("create actual local filesystem");
    let raw: Arc<dyn Fs> = Arc::new(RealFs::new(directory.path()).expect("open actual Fs"));
    let path = FsPath::new(crate::metadata::files::JOURNAL);
    std::fs::write(directory.path().join(&path.0), b"retained bytes").unwrap();
    let owner = Owner::new();
    let handle = owner.handle();
    let before = handle.snapshot();
    let operation = owner.begin(
        OperationKind::JournalAppend,
        Origin::Recovery,
        Medium::Persistent,
    );
    let view = account_fs(raw, operation.ledger());
    let mut file = view
        .open_persistent_handle(
            &path,
            OpenOptions {
                mode: OpenMode::ReadWrite,
                create: false,
                create_new: false,
                truncate: false,
            },
        )
        .expect("acquire existing actual persistent handle");
    operation.finish(true);
    // Read-only escaped use is healthy and cannot invent a mutation observation.
    assert_eq!(file.read_at(0, 3).unwrap().as_ref(), b"ret");
    assert!(view.exists(&path).unwrap());
    assert_eq!(handle.snapshot().late_operation_writes, 0);
    assert!(handle.snapshot().delta(&before).is_ok());

    // Act: actual truncation/sync mutate storage without offering payload bytes.
    file.truncate(3).expect("real late truncation");
    file.sync(Durability::Durable).expect("real late file sync");
    drop(file);
    drop(view);
    drop(owner);
    let after = handle.snapshot();

    // Assert: retained counters detect the escape, without payload byte credit.
    assert_eq!(
        std::fs::read(directory.path().join(&path.0)).unwrap(),
        b"ret"
    );
    assert_eq!(after.late_operation_writes, 4);
    assert!(after.delta(&before).is_err());
    let counters = &after.bucket(Origin::Recovery, Medium::Persistent).counters;
    assert_eq!(counters.issued_bytes, [0; 3]);
    assert_eq!(counters.returned_write_bytes, [0; 3]);
    assert_eq!(counters.journal_durable_bytes, 0);
}

#[cfg(feature = "failpoints")]
#[test]
fn should_account_forced_checkpoint_failure_when_snapshot_commits_before_truncation() {
    // Arrange: a real journal edit and checkpoint are attributed to the forced origin.
    let directory = tempfile::tempdir().expect("create actual local filesystem");
    let fs: Arc<dyn Fs> = Arc::new(RealFs::new(directory.path()).expect("open actual Fs"));
    let owner = Owner::new();
    let store = crate::metadata::store::ManifestStore::new_with_accounting(
        Arc::clone(&fs),
        owner.clone(),
        Medium::Persistent,
    );
    let edit = crate::metadata::ManifestEdit::CreateColumnFamily {
        id: 7,
        name: "forced-checkpoint".into(),
        created_at: 1,
    };
    let edit_id = store
        .append_for(Origin::CompactionBeforeGc, &edit)
        .expect("durable actual journal");
    let mut manifest = crate::metadata::Manifest::default();
    manifest.apply_edit(&edit);
    manifest.note_applied_journal_edit(edit_id);
    let journal_before =
        std::fs::read(directory.path().join(crate::metadata::files::JOURNAL)).unwrap();
    let baseline = owner.handle().snapshot();
    let _gate = crate::failpoints::test_failpoint_guard();
    let failure = fail::FailGuard::new(
        "midge::manifest::after_snapshot_rename_before_journal_truncate",
        "return",
    )
    .expect("arm existing durability failpoint");

    // Act: the actual forced checkpoint publishes snapshot bytes, then fails.
    let result = store.save_snapshot_for(Origin::CompactionBeforeGc, &manifest);
    drop(failure);
    let after = owner.handle().snapshot();
    let delta = after
        .delta(&baseline)
        .expect("all failed-operation accounting was folded");
    let recovered = crate::metadata::ManifestPersistence::load_with_fs_and_policy(
        &fs,
        crate::config::RecoveryPolicy::Strict,
    )
    .expect("actual durable snapshot plus retained journal recover");

    // Assert: safe data and forced cost are separate from a successful checkpoint.
    assert!(
        matches!(&result, Err(crate::common::MidgeError::Internal(message))
            if message.contains("crash after manifest snapshot rename")),
        "the actual post-rename failure must remain typed: {result:?}"
    );
    assert_eq!(
        std::fs::read(directory.path().join(crate::metadata::files::JOURNAL)).unwrap(),
        journal_before
    );
    let snapshot_bytes = std::fs::read(
        directory
            .path()
            .join(crate::metadata::files::MANIFEST_SNAPSHOT),
    )
    .expect("read actual durable snapshot bytes");
    let snapshot_len = u64::try_from(snapshot_bytes.len()).unwrap();
    assert!(snapshot_len > 0);
    assert_eq!(recovered.edit_checkpoint_id, edit_id);
    assert_eq!(recovered.column_families.len(), 1);
    assert_eq!(recovered.column_families[0].id, 7);
    let forced = &delta
        .bucket(Origin::CompactionBeforeGc, Medium::Persistent)
        .counters;
    assert_eq!(forced.operation_failures, 1);
    assert_eq!(forced.checkpoint_attempts, 1);
    assert_eq!(forced.snapshot_durable_count, 1);
    assert_eq!(forced.checkpoint_complete_count, 0);
    assert_eq!(forced.snapshot_durable_bytes, snapshot_len);
    assert_eq!(forced.issued_bytes[0], snapshot_len);
    assert_eq!(forced.returned_write_bytes[0], snapshot_len);
    assert_eq!(
        delta
            .bucket(Origin::OrdinaryLocalFlush, Medium::Persistent)
            .counters
            .operation_attempts,
        0
    );
}
