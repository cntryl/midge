//! Genuine delegated payload effects followed by a held typed error return.

use super::*;
use crate::io::{Fs, FsError, FsPath, FsResult, OpenMode, OpenOptions, RealFs};
use crate::metadata::accounted_fs::account_fs;
use bytes::Bytes;
use std::sync::Arc;

use super::fs_oracle::PayloadKind;

const PAYLOAD: &[u8] = b"genuine payload before held error return";

struct Evidence {
    entered: bool,
    held: bool,
    result: FsResult<()>,
    bytes: Vec<u8>,
    before: Snapshot,
    after: Snapshot,
}

fn observe_failed_completion(kind: PayloadKind, seal_while_held: bool) -> Evidence {
    let directory = tempfile::tempdir().expect("actual held-payload filesystem");
    let path = FsPath::new(crate::metadata::files::JOURNAL);
    std::fs::write(directory.path().join(&path.0), []).expect("seed actual existing file");
    let raw: Arc<dyn Fs> = Arc::new(RealFs::new(directory.path()).expect("actual Fs"));
    let oracle = Arc::new(super::fs_oracle::Observations::default());
    let observed = super::fs_oracle::observe_fs(raw, Arc::clone(&oracle));
    let owner = Owner::new();
    let handle = owner.handle();
    let before = handle.snapshot();
    let operation = owner.begin(
        OperationKind::JournalAppend,
        Origin::Recovery,
        Medium::Persistent,
    );
    let view = account_fs(observed, operation.ledger());
    let mut operation = Some(operation);
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
        .expect("genuine persistent file");
    let (entered_tx, entered_rx) = crossbeam::channel::bounded(1);
    let (release_tx, release_rx) = crossbeam::channel::bounded(1);
    oracle.hold_next_successful_payload_then_error(kind, entered_tx, release_rx);

    let worker = std::thread::spawn(move || match kind {
        PayloadKind::Append => file.append(Bytes::from_static(PAYLOAD)).map(|_| ()),
        PayloadKind::WriteAt => file.write_at(0, Bytes::from_static(PAYLOAD)),
    });
    // Only a successful delegated real payload call can deliver this handshake.
    let entered = entered_rx.recv_timeout(Duration::from_secs(3)).is_ok();
    let held = !worker.is_finished();
    if seal_while_held {
        operation.take().expect("live held operation").finish(false);
    }
    let _ = release_tx.send(());
    drop(release_tx);
    // Cleanup is unconditional and precedes every assertion in the caller.
    let result = worker.join().expect("payload worker joined");
    if !seal_while_held {
        operation
            .take()
            .expect("live completed operation")
            .finish(false);
    }
    drop(view);
    drop(owner);
    Evidence {
        entered,
        held,
        result,
        bytes: std::fs::read(directory.path().join(&path.0)).expect("actual payload bytes"),
        before,
        after: handle.snapshot(),
    }
}

fn assert_failed_completion(evidence: &Evidence, expected_late: u64) {
    assert!(
        evidence.entered,
        "positive actual payload completion handshake"
    );
    assert!(evidence.held, "typed error return remained genuinely held");
    assert!(
        matches!(&evidence.result, Err(FsError::Io(message)) if message == "actual oracle error after held payload completion"),
        "original typed Fs error survives: {:?}",
        evidence.result
    );
    assert_eq!(evidence.bytes, PAYLOAD);
    let bucket = evidence.after.bucket(Origin::Recovery, Medium::Persistent);
    assert_eq!(
        bucket.counters.issued_bytes,
        [0, u64::try_from(PAYLOAD.len()).expect("bounded payload"), 0]
    );
    assert_eq!(bucket.counters.returned_write_bytes, [0; 3]);
    assert_eq!(bucket.counters.operation_attempts, 1);
    assert_eq!(bucket.counters.operation_failures, 1);
    assert_eq!(bucket.active_operations, 0);
    assert_eq!(evidence.after.late_operation_writes, expected_late);
    assert_eq!(
        evidence.after.delta(&evidence.before).is_err(),
        expected_late > 0
    );
}

#[test]
fn should_detect_late_error_when_actual_append_finishes_after_operation_seals() {
    // Arrange: choose genuine append through the operation-scoped filesystem.
    let kind = PayloadKind::Append;
    // Act: seal only after the delegated positive-byte call is held before Err.
    let evidence = observe_failed_completion(kind, true);
    // Assert: exact bytes and typed failure invalidate the closed owner's integrity.
    assert_failed_completion(&evidence, 1);
}

#[test]
fn should_detect_late_error_when_actual_write_at_finishes_after_operation_seals() {
    // Arrange: choose genuine positional write through the accounted filesystem.
    let kind = PayloadKind::WriteAt;
    // Act: hold a real successful delegate before its injected typed error return.
    let evidence = observe_failed_completion(kind, true);
    // Assert: failure completion observes sealing without crediting returned bytes.
    assert_failed_completion(&evidence, 1);
}

#[test]
fn should_preserve_failure_without_late_flag_when_actual_append_finishes_before_sealing() {
    // Arrange: the same actual payload and typed error return complete while live.
    let kind = PayloadKind::Append;
    // Act: release and join the delegate before finishing its accounting operation.
    let evidence = observe_failed_completion(kind, false);
    // Assert: no returned-byte credit or spurious late observation is introduced.
    assert_failed_completion(&evidence, 0);
}
