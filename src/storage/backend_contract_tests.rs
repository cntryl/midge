//! One behavior contract for every `StorageBackend` (#514).
//!
//! Each case runs the same operation through the local filesystem and
//! through `CloudStorage` over the mock provider, and expects one outcome.

use super::cloud::CloudStorage;
use super::filesystem::FileSystem;
use super::{StorageBackend, StorageEvent, StorageOutcome};
use std::sync::mpsc;

#[test]
fn should_reject_typed_write_when_callback_budget_is_zero() {
    // Arrange
    let root = tempfile::tempdir().expect("temp dir");
    for (name, backend) in backends(root.path()) {
        let key = format!("{name}/expired-write");
        let request = super::StorageRequest::new(
            &key,
            crate::common::OperationDeadline::unbounded(),
            std::time::Duration::ZERO,
        );

        // Act
        let (tx, rx) = mpsc::channel();
        backend.submit_write_request(request, b"value".to_vec(), tx);

        // Assert
        assert!(matches!(
            rx.recv().expect("write callback"),
            StorageEvent::WriteComplete {
                result: StorageOutcome::Err(_),
                ..
            }
        ));
        let (read_tx, read_rx) = mpsc::channel();
        backend.submit_read_with_metadata(&key, std::time::Duration::from_secs(1), read_tx);
        assert!(read_rx.recv().expect("read callback").is_err(), "{name}");
    }
}

#[test]
fn should_preserve_existing_object_when_typed_create_finds_a_match() {
    // Arrange
    let root = tempfile::tempdir().expect("temp dir");
    for (name, backend) in backends(root.path()) {
        let key = format!("{name}/create-guard");
        let (seed_tx, seed_rx) = mpsc::channel();
        backend.submit_write(&key, b"old".to_vec(), seed_tx);
        let _ = seed_rx.recv().expect("seed callback");
        let request = super::StorageRequest::new(
            &key,
            crate::common::OperationDeadline::unbounded(),
            std::time::Duration::from_secs(1),
        )
        .with_precondition(super::StoragePrecondition::IfAbsent);

        // Act
        let (write_tx, write_rx) = mpsc::channel();
        backend.submit_write_request(request, b"new".to_vec(), write_tx);

        // Assert
        assert!(matches!(
            write_rx.recv().expect("write callback"),
            StorageEvent::WriteComplete {
                result: StorageOutcome::Err(_),
                ..
            }
        ));
        let (read_tx, read_rx) = mpsc::channel();
        backend.submit_read_with_metadata(&key, std::time::Duration::from_secs(1), read_tx);
        let (bytes, _) = read_rx.recv().expect("read callback").expect("read object");
        assert_eq!(bytes, b"old", "{name}");
    }
}

#[test]
fn should_replace_matching_object_when_typed_write_uses_current_identity() {
    // Arrange
    let root = tempfile::tempdir().expect("temp dir");
    for (name, backend) in backends(root.path()) {
        let key = format!("{name}/match-guard");
        let (seed_tx, seed_rx) = mpsc::channel();
        backend.submit_write(&key, b"old".to_vec(), seed_tx);
        let _ = seed_rx.recv().expect("seed callback");
        let (read_tx, read_rx) = mpsc::channel();
        backend.submit_read_with_metadata(&key, std::time::Duration::from_secs(1), read_tx);
        let (_, identity) = read_rx.recv().expect("read callback").expect("read object");
        let request = super::StorageRequest::new(
            &key,
            crate::common::OperationDeadline::unbounded(),
            std::time::Duration::from_secs(1),
        )
        .with_precondition(super::StoragePrecondition::IfMatch(identity));

        // Act
        let (write_tx, write_rx) = mpsc::channel();
        backend.submit_write_request(request, b"new".to_vec(), write_tx);

        // Assert
        assert!(
            matches!(
                write_rx.recv().expect("write callback"),
                StorageEvent::WriteComplete {
                    result: StorageOutcome::Ok(()),
                    ..
                }
            ),
            "{name}"
        );
    }
}

#[test]
fn should_preserve_replaced_object_when_typed_delete_uses_stale_identity() {
    // Arrange
    let root = tempfile::tempdir().expect("temp dir");
    for (name, backend) in backends(root.path()) {
        let key = format!("{name}/delete-guard");
        let (seed_tx, seed_rx) = mpsc::channel();
        backend.submit_write(&key, b"old".to_vec(), seed_tx);
        let _ = seed_rx.recv().expect("seed callback");
        let (head_tx, head_rx) = mpsc::channel();
        backend.submit_head(&key, head_tx);
        let StorageEvent::HeadComplete {
            result: StorageOutcome::Ok(identity),
            ..
        } = head_rx.recv().expect("head callback")
        else {
            panic!("{name}: expected a current identity");
        };
        let (replace_tx, replace_rx) = mpsc::channel();
        backend.submit_write(&key, b"new".to_vec(), replace_tx);
        let _ = replace_rx.recv().expect("replace callback");
        let request = super::StorageRequest::new(
            &key,
            crate::common::OperationDeadline::unbounded(),
            std::time::Duration::from_secs(1),
        )
        .with_precondition(super::StoragePrecondition::IfMatch(identity));

        // Act
        let (delete_tx, delete_rx) = mpsc::channel();
        backend.submit_delete_request(request, delete_tx);

        // Assert
        assert!(
            matches!(
                delete_rx.recv().expect("delete callback"),
                StorageEvent::DeleteComplete {
                    result: StorageOutcome::Err(_),
                    ..
                }
            ),
            "{name}"
        );
        let (read_tx, read_rx) = mpsc::channel();
        backend.submit_read_with_metadata(&key, std::time::Duration::from_secs(1), read_tx);
        let (bytes, _) = read_rx.recv().expect("read callback").expect("read object");
        assert_eq!(bytes, b"new", "{name}");
    }
}

#[test]
fn should_reject_typed_delete_when_callback_budget_is_zero() {
    // Arrange
    let root = tempfile::tempdir().expect("temp dir");
    for (name, backend) in backends(root.path()) {
        let key = format!("{name}/expired-delete");
        let (seed_tx, seed_rx) = mpsc::channel();
        backend.submit_write(&key, b"value".to_vec(), seed_tx);
        let _ = seed_rx.recv().expect("seed callback");
        let request = super::StorageRequest::new(
            &key,
            crate::common::OperationDeadline::unbounded(),
            std::time::Duration::ZERO,
        );

        // Act
        let (delete_tx, delete_rx) = mpsc::channel();
        backend.submit_delete_request(request, delete_tx);

        // Assert
        assert!(
            matches!(
                delete_rx.recv().expect("delete callback"),
                StorageEvent::DeleteComplete {
                    result: StorageOutcome::Err(_),
                    ..
                }
            ),
            "{name}"
        );
        let (read_tx, read_rx) = mpsc::channel();
        backend.submit_read_with_metadata(&key, std::time::Duration::from_secs(1), read_tx);
        assert!(read_rx.recv().expect("read callback").is_ok(), "{name}");
    }
}

#[test]
fn should_preserve_object_when_typed_delete_uses_absence_precondition() {
    // Arrange
    let root = tempfile::tempdir().expect("temp dir");
    for (name, backend) in backends(root.path()) {
        let key = format!("{name}/delete-absence-guard");
        let (seed_tx, seed_rx) = mpsc::channel();
        backend.submit_write(&key, b"value".to_vec(), seed_tx);
        let _ = seed_rx.recv().expect("seed callback");
        let request = super::StorageRequest::new(
            &key,
            crate::common::OperationDeadline::unbounded(),
            std::time::Duration::from_secs(1),
        )
        .with_precondition(super::StoragePrecondition::IfAbsent);

        // Act
        let (delete_tx, delete_rx) = mpsc::channel();
        backend.submit_delete_request(request, delete_tx);

        // Assert
        assert!(
            matches!(
                delete_rx.recv().expect("delete callback"),
                StorageEvent::DeleteComplete {
                    result: StorageOutcome::Err(_),
                    ..
                }
            ),
            "{name}"
        );
        let (read_tx, read_rx) = mpsc::channel();
        backend.submit_read_with_metadata(&key, std::time::Duration::from_secs(1), read_tx);
        assert!(read_rx.recv().expect("read callback").is_ok(), "{name}");
    }
}

#[test]
fn should_succeed_when_typed_generation_delete_targets_missing_object() {
    // Arrange
    let root = tempfile::tempdir().expect("temp dir");
    for (name, backend) in backends(root.path()) {
        let key = format!("{name}/missing-generation-delete");
        let request = super::StorageRequest::new(
            &key,
            crate::common::OperationDeadline::unbounded(),
            std::time::Duration::from_secs(1),
        )
        .with_precondition(super::StoragePrecondition::IfMatch(
            super::StorageObjectMetadata {
                size: 1,
                etag: String::new(),
                generation: Some("7".into()),
            },
        ));

        // Act
        let (delete_tx, delete_rx) = mpsc::channel();
        backend.submit_delete_request(request, delete_tx);

        // Assert
        assert!(
            matches!(
                delete_rx.recv().expect("delete callback"),
                StorageEvent::DeleteComplete {
                    result: StorageOutcome::Ok(()),
                    ..
                }
            ),
            "{name}"
        );
    }
}

#[test]
fn should_keep_existing_bytes_when_generation_precondition_is_unsupported_or_stale() {
    // Arrange
    let root = tempfile::tempdir().expect("temp dir");
    for (name, backend) in backends(root.path()) {
        let key = format!("{name}/generation-guard");
        let (seed_tx, seed_rx) = mpsc::channel();
        backend.submit_write(&key, b"old".to_vec(), seed_tx);
        assert!(matches!(
            seed_rx.recv().expect("seed write"),
            StorageEvent::WriteComplete {
                result: StorageOutcome::Ok(()),
                ..
            }
        ));
        let request = super::StorageRequest::new(
            &key,
            crate::common::OperationDeadline::unbounded(),
            std::time::Duration::from_secs(1),
        )
        .with_precondition(super::StoragePrecondition::IfMatch(
            super::StorageObjectMetadata {
                size: 3,
                etag: "ignored-when-generation-is-present".into(),
                generation: Some("42".into()),
            },
        ));

        // Act
        let (write_tx, write_rx) = mpsc::channel();
        backend.submit_write_request(request, b"new".to_vec(), write_tx);
        let (read_tx, read_rx) = mpsc::channel();
        backend.submit_read_with_metadata(&key, std::time::Duration::from_secs(1), read_tx);

        // Assert
        assert!(matches!(
            write_rx.recv().expect("conditional write"),
            StorageEvent::WriteComplete {
                result: StorageOutcome::Err(_),
                ..
            }
        ));
        let (bytes, _) = read_rx.recv().expect("read callback").expect("read object");
        assert_eq!(bytes, b"old", "{name} overwrote a guarded object");
    }
}

/// Every backend under the contract, named for assertion messages.
fn backends(root: &std::path::Path) -> Vec<(&'static str, Box<dyn StorageBackend>)> {
    vec![
        (
            "filesystem",
            Box::new(FileSystem::new(root).expect("filesystem backend")),
        ),
        ("cloud-mock", Box::new(CloudStorage::with_mock())),
    ]
}

#[test]
fn should_reject_expired_or_conditional_typed_head_for_every_storage_backend() {
    // Arrange
    let root = tempfile::tempdir().expect("temp dir");
    for (name, backend) in backends(root.path()) {
        for (case, request, expected_kind) in [
            (
                "expired",
                super::StorageRequest::new(
                    "missing/head",
                    crate::common::OperationDeadline::unbounded(),
                    std::time::Duration::ZERO,
                ),
                super::StorageErrorKind::Timeout,
            ),
            (
                "conditional",
                super::StorageRequest::new(
                    "missing/head",
                    crate::common::OperationDeadline::unbounded(),
                    std::time::Duration::from_secs(1),
                )
                .with_precondition(super::StoragePrecondition::IfAbsent),
                super::StorageErrorKind::Protocol,
            ),
        ] {
            // Act
            let (tx, rx) = mpsc::channel();
            backend.submit_head_request(request, tx);

            // Assert
            match rx.recv().expect("HEAD callback") {
                StorageEvent::HeadComplete {
                    key,
                    result: StorageOutcome::Err(error),
                } => {
                    assert_eq!(key, "missing/head", "{name}/{case}");
                    assert_eq!(error.kind(), expected_kind, "{name}/{case}");
                }
                other => panic!("{name}/{case}: unexpected HEAD event {other:?}"),
            }
        }
    }
}

#[test]
fn should_reject_expired_or_conditional_typed_range_head_for_every_storage_backend() {
    // Arrange
    let root = tempfile::tempdir().expect("temp dir");
    for (name, backend) in backends(root.path()) {
        for (case, request, expected_kind) in [
            (
                "expired",
                super::StorageRequest::new(
                    "missing/range-head",
                    crate::common::OperationDeadline::unbounded(),
                    std::time::Duration::ZERO,
                ),
                super::StorageErrorKind::Timeout,
            ),
            (
                "conditional",
                super::StorageRequest::new(
                    "missing/range-head",
                    crate::common::OperationDeadline::unbounded(),
                    std::time::Duration::from_secs(1),
                )
                .with_precondition(super::StoragePrecondition::IfAbsent),
                super::StorageErrorKind::Protocol,
            ),
        ] {
            // Act
            let (tx, rx) = mpsc::channel();
            backend.submit_range_head_request(request, tx);

            // Assert
            match rx.recv().expect("range HEAD callback") {
                StorageEvent::HeadComplete {
                    key,
                    result: StorageOutcome::Err(error),
                } => {
                    assert_eq!(key, "missing/range-head", "{name}/{case}");
                    assert_eq!(error.kind(), expected_kind, "{name}/{case}");
                }
                other => panic!("{name}/{case}: unexpected range HEAD event {other:?}"),
            }
        }
    }
}

#[test]
fn should_reject_expired_or_conditional_typed_metadata_read_for_every_storage_backend() {
    // Arrange
    let root = tempfile::tempdir().expect("temp dir");
    for (name, backend) in backends(root.path()) {
        for (case, request, expected_kind) in [
            (
                "expired",
                super::StorageRequest::new(
                    "missing/read",
                    crate::common::OperationDeadline::unbounded(),
                    std::time::Duration::ZERO,
                ),
                super::StorageErrorKind::Timeout,
            ),
            (
                "conditional",
                super::StorageRequest::new(
                    "missing/read",
                    crate::common::OperationDeadline::unbounded(),
                    std::time::Duration::from_secs(1),
                )
                .with_precondition(super::StoragePrecondition::IfAbsent),
                super::StorageErrorKind::Protocol,
            ),
        ] {
            // Act
            let (tx, rx) = mpsc::channel();
            backend.submit_metadata_read_request(request, tx);

            // Assert
            let error = rx
                .recv()
                .expect("metadata read callback")
                .expect_err("invalid typed read must fail");
            assert_eq!(error.kind(), expected_kind, "{name}/{case}");
        }
    }
}

fn delete_outcome(
    backend: &dyn StorageBackend,
    key: &str,
    headers: Vec<(String, String)>,
) -> StorageOutcome<()> {
    let (tx, rx) = mpsc::channel();
    if headers.is_empty() {
        backend.submit_delete(key, tx);
    } else {
        backend.submit_delete_with_headers(key, headers, tx);
    }
    match rx.recv().expect("delete callback") {
        StorageEvent::DeleteComplete { result, .. } => result,
        other => panic!("expected DeleteComplete, got {other:?}"),
    }
}

#[test]
fn should_report_success_when_deleting_missing_object_through_every_storage_backend() {
    // Arrange
    let root = tempfile::tempdir().expect("temp dir");

    for (name, backend) in backends(root.path()) {
        // Act
        let outcome = delete_outcome(backend.as_ref(), "absent/object", Vec::new());

        // Assert
        assert!(
            matches!(outcome, StorageOutcome::Ok(())),
            "{name}: deleting a missing object must succeed, got {outcome:?}"
        );
    }
}

#[test]
fn should_report_success_when_conditionally_deleting_missing_object_through_every_storage_backend()
{
    // Arrange: the object a conditional delete targets is already gone, so
    // no other version can be deleted by mistake.
    let root = tempfile::tempdir().expect("temp dir");

    for (name, backend) in backends(root.path()) {
        // Act
        let outcome = delete_outcome(
            backend.as_ref(),
            "absent/object",
            vec![("If-Match".to_string(), "\"stale-etag\"".to_string())],
        );

        // Assert
        assert!(
            matches!(outcome, StorageOutcome::Ok(())),
            "{name}: conditionally deleting a missing object must succeed, got {outcome:?}"
        );
    }
}

/// The key a completion event reports.
fn event_key(event: &StorageEvent) -> String {
    match event {
        StorageEvent::WriteComplete { key, .. }
        | StorageEvent::DeleteComplete { key, .. }
        | StorageEvent::HeadComplete { key, .. } => key.clone(),
        other => panic!("unexpected completion event {other:?}"),
    }
}

#[test]
fn should_echo_caller_key_when_completing_operations_through_every_storage_backend() {
    // Arrange: `CloudStorage` stores keys under its namespace, which callers
    // never see. Read and list are left to their deletion in #516.
    let root = tempfile::tempdir().expect("temp dir");
    let key = "sst/000001.sst";

    for (name, backend) in backends(root.path()) {
        let run = |submit: &dyn Fn(mpsc::Sender<StorageEvent>)| {
            let (tx, rx) = mpsc::channel();
            submit(tx);
            event_key(&rx.recv().expect("completion callback"))
        };

        // Act
        let keys = [
            (
                "write",
                run(&|tx| backend.submit_write(key, b"value".to_vec(), tx)),
            ),
            ("head", run(&|tx| backend.submit_head(key, tx))),
            ("delete", run(&|tx| backend.submit_delete(key, tx))),
        ];

        // Assert
        for (operation, actual) in keys {
            assert_eq!(actual, key, "{name}: {operation} completion key");
        }
    }
}
