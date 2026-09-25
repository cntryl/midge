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

/// Every Rust source under `src/`.
fn rust_sources(dir: &std::path::Path, out: &mut Vec<std::path::PathBuf>) {
    for entry in std::fs::read_dir(dir).expect("read source dir") {
        let path = entry.expect("source entry").path();
        if path.is_dir() {
            rust_sources(&path, out);
        } else if path.extension().is_some_and(|ext| ext == "rs") {
            out.push(path);
        }
    }
}

#[test]
fn should_not_reintroduce_whole_object_read_or_list_on_storage_backends() {
    // Arrange: these verbs had no production caller and diverged between
    // backends (#516). Cloud-level `CloudBackend::submit_list` takes a
    // `CloudCallback` and is unaffected.
    let mut sources = Vec::new();
    rust_sources(
        &std::path::Path::new(env!("CARGO_MANIFEST_DIR")).join("src"),
        &mut sources,
    );

    // Act
    let mut reintroduced = Vec::new();
    for path in sources
        .iter()
        .filter(|path| !path.ends_with("backend_contract_tests.rs"))
    {
        let source = std::fs::read_to_string(path).expect("read source");
        let compact: String = source.chars().filter(|c| !c.is_whitespace()).collect();
        for needle in [
            "fnsubmit_read_with_timeout(",
            "StorageEvent::ReadComplete",
            "StorageEvent::ListComplete",
        ] {
            if compact.contains(needle) {
                reintroduced.push(format!("{}: {needle}", path.display()));
            }
        }
        // A read or list whose signature takes a storage callback is the
        // deleted `StorageBackend` verb.
        for name in ["fnsubmit_read(", "fnsubmit_list("] {
            for (at, _) in compact.match_indices(name) {
                let end = compact[at..]
                    .find(['{', ';'])
                    .map_or(compact.len(), |end| at + end);
                if compact[at..end].contains("StorageCallback") {
                    reintroduced.push(format!("{}: {name}..StorageCallback", path.display()));
                }
            }
        }
    }

    // Assert
    assert!(
        reintroduced.is_empty(),
        "dead storage verbs returned: {reintroduced:?}"
    );
}
