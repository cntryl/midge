//! One behavior contract for every `StorageBackend` (#514).
//!
//! Each case runs the same operation through the local filesystem and
//! through `CloudStorage` over the mock provider, and expects one outcome.

use super::cloud::CloudStorage;
use super::filesystem::FileSystem;
use super::{StorageBackend, StorageEvent, StorageOutcome};
use std::sync::mpsc;

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
