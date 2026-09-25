use super::*;
use std::sync::{
    atomic::{AtomicUsize, Ordering},
    mpsc,
};

#[derive(Default)]
struct ConditionalPutOnlyBackend {
    puts: AtomicUsize,
    gets: AtomicUsize,
    heads: AtomicUsize,
    deletes: AtomicUsize,
    lists: AtomicUsize,
}

struct DelayedMissingGetBackend;

struct DroppedHeadCallbackBackend;

impl CloudBackend for ConditionalPutOnlyBackend {
    crate::storage::cloud::unsupported_cloud_backend!(submit_get_with_metadata);

    fn submit_put(
        &self,
        key: &str,
        _data: Vec<u8>,
        headers: Vec<(String, String)>,
        callback: CloudCallback,
    ) {
        self.puts.fetch_add(1, Ordering::SeqCst);
        let has_condition = headers
            .iter()
            .any(|(name, value)| name.eq_ignore_ascii_case("if-none-match") && value == "*");
        let result = if has_condition {
            CloudOutcome::Ok(())
        } else {
            CloudOutcome::Err(CloudError::Protocol(
                "conditional header was not delegated".to_string(),
            ))
        };
        let _ = callback.send(CloudEvent::Put {
            key: key.to_string(),
            result,
        });
    }

    fn submit_get(&self, key: &str, callback: CloudCallback) {
        self.gets.fetch_add(1, Ordering::SeqCst);
        let _ = callback.send(CloudEvent::Get {
            key: key.to_string(),
            result: CloudOutcome::Err(CloudError::Protocol("unsupported".to_string())),
        });
    }

    fn submit_delete(&self, key: &str, _headers: Vec<(String, String)>, callback: CloudCallback) {
        self.deletes.fetch_add(1, Ordering::SeqCst);
        let _ = callback.send(CloudEvent::Delete {
            key: key.to_string(),
            result: CloudOutcome::Ok(()),
        });
    }

    fn submit_list(&self, prefix: &str, callback: CloudCallback) {
        self.lists.fetch_add(1, Ordering::SeqCst);
        let _ = callback.send(CloudEvent::List {
            prefix: prefix.to_string(),
            result: CloudOutcome::Ok(Vec::new()),
        });
    }

    fn submit_get_range(&self, key: &str, start: u64, end: Option<u64>, callback: CloudCallback) {
        let _ = callback.send(CloudEvent::GetRange {
            key: key.to_string(),
            start,
            end,
            result: CloudOutcome::Err(CloudError::Protocol("unsupported".to_string())),
        });
    }

    fn submit_head(&self, key: &str, callback: CloudCallback) {
        self.heads.fetch_add(1, Ordering::SeqCst);
        let _ = callback.send(CloudEvent::Head {
            key: key.to_string(),
            result: CloudOutcome::Err(CloudError::Unauthorized(
                "HEAD permission intentionally absent".to_string(),
            )),
        });
    }
}

impl CloudBackend for DelayedMissingGetBackend {
    crate::storage::cloud::unsupported_cloud_backend!(submit_delete, submit_list);

    fn submit_put(
        &self,
        key: &str,
        _data: Vec<u8>,
        _headers: Vec<(String, String)>,
        callback: CloudCallback,
    ) {
        let key = key.to_string();
        std::thread::spawn(move || {
            std::thread::sleep(std::time::Duration::from_secs(1));
            let _ = callback.send(CloudEvent::Put {
                key,
                result: CloudOutcome::Err(CloudError::Protocol("unsupported".to_string())),
            });
        });
    }

    fn submit_get(&self, key: &str, callback: CloudCallback) {
        let key = key.to_string();
        std::thread::spawn(move || {
            std::thread::sleep(std::time::Duration::from_secs(1));
            let _ = callback.send(CloudEvent::Get {
                key,
                result: CloudOutcome::Err(CloudError::NotFound("delayed miss".to_string())),
            });
        });
    }

    fn submit_get_with_metadata(&self, key: &str, callback: CloudCallback) {
        let key = key.to_string();
        std::thread::spawn(move || {
            std::thread::sleep(std::time::Duration::from_secs(1));
            let _ = callback.send(CloudEvent::GetWithMetadata {
                key,
                result: Err(CloudError::NotFound("delayed miss".to_string())),
            });
        });
    }

    fn submit_get_range(&self, key: &str, start: u64, end: Option<u64>, callback: CloudCallback) {
        let _ = callback.send(CloudEvent::GetRange {
            key: key.to_string(),
            start,
            end,
            result: CloudOutcome::Err(CloudError::Protocol("unsupported".to_string())),
        });
    }

    fn submit_head(&self, key: &str, callback: CloudCallback) {
        let key = key.to_string();
        std::thread::spawn(move || {
            std::thread::sleep(std::time::Duration::from_secs(1));
            let _ = callback.send(CloudEvent::Head {
                key,
                result: CloudOutcome::Err(CloudError::NotFound("delayed miss".to_string())),
            });
        });
    }
}

impl CloudBackend for DroppedHeadCallbackBackend {
    crate::storage::cloud::unsupported_cloud_backend!(
        submit_get,
        submit_get_with_metadata,
        submit_delete,
        submit_list,
    );

    fn submit_put(
        &self,
        key: &str,
        _data: Vec<u8>,
        _headers: Vec<(String, String)>,
        callback: CloudCallback,
    ) {
        let _ = callback.send(CloudEvent::Put {
            key: key.to_string(),
            result: CloudOutcome::Err(CloudError::Protocol("unsupported".to_string())),
        });
    }

    fn submit_get_range(&self, key: &str, start: u64, end: Option<u64>, callback: CloudCallback) {
        let _ = callback.send(CloudEvent::GetRange {
            key: key.to_string(),
            start,
            end,
            result: CloudOutcome::Err(CloudError::Protocol("unsupported".to_string())),
        });
    }

    fn submit_head(&self, _key: &str, callback: CloudCallback) {
        drop(callback);
    }
}

// =========== CloudOutcome Tests ===========

#[test]
fn should_share_one_metadata_type_across_storage_layers() {
    // Arrange
    let from_backend = ObjectMetadata::new(7, "etag".to_string());

    // Act
    let as_storage: StorageObjectMetadata = from_backend.clone();

    // Assert
    assert_eq!(as_storage.size, 7);
    assert_eq!(as_storage, from_backend);
}

#[test]
fn should_classify_provider_http_statuses() {
    // Arrange
    let statuses = [404, 401, 400, 503];

    // Act
    let errors = [
        CloudError::from_http_status(statuses[0], "missing"),
        CloudError::from_http_status(statuses[1], "credentials"),
        CloudError::from_http_status(statuses[2], "request"),
        CloudError::from_http_status(statuses[3], "unavailable"),
    ];

    // Assert
    assert!(matches!(errors[0], CloudError::NotFound(_)));
    assert!(matches!(errors[1], CloudError::Unauthorized(_)));
    assert!(matches!(errors[2], CloudError::InvalidRequest(_)));
    assert!(matches!(errors[3], CloudError::ServerError(_)));
}

#[test]
fn should_classify_exhausted_retryable_http_statuses_as_server_errors() {
    // Arrange
    let statuses = [408, 425, 429];

    // Act
    let errors = statuses.map(|status| CloudError::from_http_status(status, "retry exhausted"));

    // Assert
    assert!(errors
        .iter()
        .all(|error| matches!(error, CloudError::ServerError(_))));
}

#[test]
fn should_classify_http_408_as_timeout_without_relying_on_provider_detail_text() {
    // Arrange
    let error = CloudError::from_http_status(408, "request rejected");

    // Act
    let is_timeout = error.is_timeout();

    // Assert
    assert!(is_timeout, "HTTP 408 is intrinsically a request timeout");
}

#[test]
fn should_not_classify_transport_diagnostic_text_as_timeout() {
    // Arrange: a host or certificate name can legitimately contain this
    // word without the transport failure being deadline exhaustion.
    let error =
        CloudError::Transport("TLS certificate rejected for https://timeout.example".to_string());

    // Act
    let is_timeout = error.is_timeout();

    // Assert
    assert!(!is_timeout);
}

#[test]
fn should_preserve_timeout_without_reclassifying_list_parser_errors() {
    // Arrange
    let timeout = MidgeError::Timeout("LIST deadline expired".to_string());
    let malformed = MidgeError::Internal("LIST response contained invalid XML".to_string());

    // Act
    let timeout = CloudError::from_protocol_or_timeout_error(timeout);
    let malformed = CloudError::from_protocol_or_timeout_error(malformed);

    // Assert
    assert!(matches!(timeout, CloudError::Timeout(_)));
    assert!(matches!(malformed, CloudError::Protocol(_)));
}

#[test]
fn should_preserve_typed_timeout_when_crossing_storage_callback_bridge() {
    // Arrange
    let error = CloudError::from_http_status(408, "request rejected");

    // Act
    let outcome = cloud_to_storage_outcome::<()>(CloudOutcome::Err(error));

    // Assert
    assert!(matches!(
        outcome,
        StorageOutcome::Err(message)
            if message.is_timeout()
    ));
}

#[test]
fn should_not_classify_disconnected_cloud_callback_as_timeout() {
    // Arrange
    let storage = CloudStorage::new_with_timeout(
        Arc::new(DroppedHeadCallbackBackend),
        "tenant".to_string(),
        std::time::Duration::from_secs(1),
    );
    let (sender, receiver) = mpsc::channel();

    // Act
    StorageBackend::submit_head_with_timeout(
        &storage,
        "metadata/manifest.json",
        std::time::Duration::from_secs(1),
        sender,
    );
    let event = receiver
        .recv()
        .expect("receive disconnected adapter result");

    // Assert
    assert!(matches!(
        event,
        StorageEvent::HeadComplete {
            result: StorageOutcome::Err(message),
            ..
        } if !message.is_timeout()
            && message.to_string().contains("closed")
    ));
}

#[test]
fn should_not_submit_cloud_operations_given_operation_timeout_is_zero() {
    // Arrange
    let backend = Arc::new(ConditionalPutOnlyBackend::default());
    let storage = CloudStorage::new_with_timeout(
        backend.clone(),
        "tenant".to_string(),
        std::time::Duration::from_secs(1),
    );
    let (write_sender, write_receiver) = mpsc::channel();
    let (head_sender, head_receiver) = mpsc::channel();

    // Act
    StorageBackend::submit_write_with_headers_and_timeout(
        &storage,
        "metadata/manifest.json",
        b"manifest".to_vec(),
        vec![("If-None-Match".to_string(), "*".to_string())],
        std::time::Duration::ZERO,
        write_sender,
    );
    StorageBackend::submit_head_with_timeout(
        &storage,
        "metadata/manifest.json",
        std::time::Duration::ZERO,
        head_sender,
    );
    let write = write_receiver
        .recv()
        .expect("receive zero-budget write result");
    let head = head_receiver
        .recv()
        .expect("receive zero-budget head result");

    // Assert
    assert_eq!(backend.gets.load(Ordering::SeqCst), 0);
    assert_eq!(backend.puts.load(Ordering::SeqCst), 0);
    assert_eq!(backend.heads.load(Ordering::SeqCst), 0);
    assert!(matches!(
        write,
        StorageEvent::WriteComplete {
            result: StorageOutcome::Err(message),
            ..
        } if message.is_timeout()
    ));
    assert!(matches!(
        head,
        StorageEvent::HeadComplete {
            result: StorageOutcome::Err(message),
            ..
        } if message.is_timeout()
    ));
}

#[test]
fn should_not_submit_delete_given_configured_callback_timeout_is_zero() {
    // Arrange
    let backend = Arc::new(ConditionalPutOnlyBackend::default());
    let storage = CloudStorage::new_with_timeout(
        backend.clone(),
        "tenant".to_string(),
        std::time::Duration::ZERO,
    );
    let (delete_sender, delete_receiver) = mpsc::channel();
    let (conditional_delete_sender, conditional_delete_receiver) = mpsc::channel();

    // Act
    StorageBackend::submit_delete(&storage, "metadata/manifest.json", delete_sender);
    StorageBackend::submit_delete_with_headers(
        &storage,
        "metadata/manifest.json",
        vec![("If-Match".to_string(), "etag".to_string())],
        conditional_delete_sender,
    );
    let delete = delete_receiver
        .recv()
        .expect("receive zero-budget delete result");
    let conditional_delete = conditional_delete_receiver
        .recv()
        .expect("receive zero-budget conditional delete result");

    // Assert
    assert_eq!(backend.deletes.load(Ordering::SeqCst), 0);
    assert!(matches!(
        delete,
        StorageEvent::DeleteComplete {
            result: StorageOutcome::Err(message),
            ..
        } if message.is_timeout()
    ));
    assert!(matches!(
        conditional_delete,
        StorageEvent::DeleteComplete {
            result: StorageOutcome::Err(message),
            ..
        } if message.is_timeout()
    ));
}

#[test]
fn should_convert_engine_result_to_cloud_outcome() {
    // Arrange
    let ok_result: Result<i32, MidgeError> = Ok(100);
    let err_result: Result<i32, MidgeError> = Err(MidgeError::Corruption("test".into()));

    // Act
    let ok_outcome = cloud_outcome_from_result(ok_result);
    let err_outcome = cloud_outcome_from_result(err_result);

    // Assert: the success payload and the error message must survive the conversion,
    // not just the Ok/Err discriminant.
    match ok_outcome {
        CloudOutcome::Ok(value) => assert_eq!(value, 100),
        CloudOutcome::Err(e) => panic!("expected Ok(100), got Err({e:?})"),
    }
    match err_outcome {
        CloudOutcome::Err(CloudError::Protocol(message)) => {
            assert!(
                message.contains("test"),
                "converted error should preserve source message, got: {message}"
            );
        }
        other => {
            panic!("expected CloudError::Protocol wrapping the source error, got {other:?}")
        }
    }
}

/// Replaces the object immediately after taking the GET response snapshot.
struct ReplacingGetBackend {
    inner: MockCloudBackend,
}

impl CloudBackend for ReplacingGetBackend {
    crate::storage::cloud::unsupported_cloud_backend!(submit_list);

    crate::storage::cloud::forward_cloud_backend!(inner; submit_put);

    fn submit_get(&self, key: &str, callback: CloudCallback) {
        self.inner.submit_get(key, callback);
        let (tx, _rx) = mpsc::channel();
        self.inner.submit_put(key, b"new".to_vec(), Vec::new(), tx);
    }

    fn submit_get_with_metadata(&self, key: &str, callback: CloudCallback) {
        self.inner.submit_get_with_metadata(key, callback);
        let (tx, _rx) = mpsc::channel();
        self.inner.submit_put(key, b"new".to_vec(), Vec::new(), tx);
    }

    crate::storage::cloud::forward_cloud_backend!(inner; submit_head, submit_get_range, submit_get_range_with_identity, submit_delete);
}

fn replacing_get_storage() -> CloudStorage {
    let backend = Arc::new(ReplacingGetBackend {
        inner: MockCloudBackend::new(),
    });
    let storage = CloudStorage::new(backend, "tenant".to_string());
    let (tx, rx) = mpsc::channel();
    storage.submit_put("object", b"old".to_vec(), Vec::new(), tx);
    assert!(matches!(
        rx.recv().unwrap(),
        CloudEvent::Put { result: Ok(()), .. }
    ));
    storage
}

#[test]
fn should_bind_proof_to_get_version_when_same_length_replacement_follows_get() {
    // Arrange
    let storage = replacing_get_storage();
    let (tx, rx) = mpsc::channel();
    storage.submit_head("object", tx);
    let CloudEvent::Head {
        result: Ok(original),
        ..
    } = rx.recv().unwrap()
    else {
        panic!("initial object must exist");
    };

    // Act
    let proof = blocking_cloud_object_proof(&storage, "object")
        .unwrap()
        .unwrap();

    // Assert
    assert_eq!(proof.bytes, b"old");
    assert_eq!(proof.metadata.etag, original.etag);
}

#[test]
fn should_reject_stale_proof_mutations_when_same_length_replacement_follows_get() {
    // Arrange
    let storage = replacing_get_storage();
    let proof = blocking_cloud_object_proof(&storage, "object")
        .unwrap()
        .unwrap();
    let headers = object_match_precondition_headers(
        &proof.metadata.etag,
        proof.metadata.generation.as_deref(),
    )
    .unwrap();

    // Act
    let (tx, rx) = mpsc::channel();
    storage.submit_put("object", b"bad".to_vec(), headers.clone(), tx);
    let write = rx.recv().unwrap();
    let (tx, rx) = mpsc::channel();
    storage.submit_delete_with_headers("object", headers, tx);
    let delete = rx.recv().unwrap();

    // Assert
    assert!(matches!(
        write,
        CloudEvent::Put {
            result: Err(CloudError::PreconditionFailed(_)),
            ..
        }
    ));
    assert!(matches!(
        delete,
        CloudEvent::Delete {
            result: Err(CloudError::PreconditionFailed(_)),
            ..
        }
    ));
}

#[test]
fn should_reject_proof_when_response_has_missing_identity_or_incorrect_length() {
    // Arrange
    let body = b"abc";
    let cases = [
        StorageObjectMetadata {
            size: 3,
            etag: " ".to_string(),
            generation: None,
        },
        StorageObjectMetadata {
            size: 3,
            etag: String::new(),
            generation: Some(" ".to_string()),
        },
        StorageObjectMetadata {
            size: 2,
            etag: "identity".to_string(),
            generation: None,
        },
        StorageObjectMetadata {
            size: 4,
            etag: "identity".to_string(),
            generation: None,
        },
    ];

    // Act
    let results: Vec<_> = cases
        .iter()
        .map(|metadata| validate_object_proof("object", body, metadata))
        .collect();

    // Assert
    assert!(results.iter().all(Result::is_err));
}

#[test]
fn should_fail_closed_when_backend_lacks_metadata_bearing_get() {
    // Arrange
    let backend = Arc::new(ConditionalPutOnlyBackend::default());
    let storage = CloudStorage::new(backend.clone(), "tenant".to_string());

    // Act
    let result = blocking_cloud_object_proof(&storage, "object");

    // Assert
    assert!(result
        .unwrap_err()
        .contains("does not support metadata-bearing GET"));
    assert_eq!(backend.gets.load(Ordering::SeqCst), 0);
    assert_eq!(backend.heads.load(Ordering::SeqCst), 0);
}

// =========== ObjectMetadata Tests ===========

#[test]
fn should_prefer_generation_when_building_object_match_precondition() {
    // Arrange
    let quoted_etag = "  \"etag-value\"  ";

    // Act
    let headers = object_match_precondition_headers(quoted_etag, Some(" 42 "));

    // Assert
    assert_eq!(
        headers,
        Some(vec![(
            "x-goog-if-generation-match".to_string(),
            "42".to_string()
        )])
    );
}

#[test]
fn should_preserve_quoted_etag_when_building_object_match_precondition() {
    // Arrange
    let quoted_etag = "  \"etag-value\"  ";

    // Act
    let headers = object_match_precondition_headers(quoted_etag, None);

    // Assert
    assert_eq!(
        headers,
        Some(vec![("If-Match".to_string(), "\"etag-value\"".to_string())])
    );
}

// =========== CloudStorage Routing Tests ===========

#[test]
fn should_apply_configured_callback_timeout_to_blocking_cloud_proof() {
    // Arrange
    let storage = CloudStorage::new_with_timeout(
        Arc::new(DelayedMissingGetBackend),
        "tenant".to_string(),
        std::time::Duration::from_millis(5),
    );

    // Act
    let error = blocking_cloud_object_proof(&storage, "metadata/manifest.json")
        .expect_err("configured callback timeout must bound proof reads");

    // Assert
    assert!(
        error.contains("timed out") || (error.contains("Timeout") && error.contains("deadline")),
        "unexpected error: {error}"
    );
}

#[test]
fn should_report_storage_callback_timeout_when_cloud_backend_is_slow() {
    // Arrange
    let storage = CloudStorage::new_with_timeout(
        Arc::new(DelayedMissingGetBackend),
        "tenant".to_string(),
        std::time::Duration::from_millis(5),
    );
    let (sender, receiver) = mpsc::channel();

    // Act
    StorageBackend::submit_head(&storage, "metadata/manifest.json", sender);
    let event = receiver.recv().expect("receive bounded adapter result");

    // Assert
    assert!(matches!(
        event,
        StorageEvent::HeadComplete {
            result: StorageOutcome::Err(message),
            ..
        } if message.to_string().contains("timed out")
    ));
}

#[test]
fn should_apply_operation_timeout_to_cloud_head_adapter_when_shorter_than_configured_timeout() {
    // Arrange: keep the same wide separation for the HEAD adapter.
    let storage = CloudStorage::new_with_timeout(
        Arc::new(DelayedMissingGetBackend),
        "tenant".to_string(),
        std::time::Duration::from_secs(1),
    );
    let (sender, receiver) = mpsc::channel();

    // Act
    let started = std::time::Instant::now();
    StorageBackend::submit_head_with_timeout(
        &storage,
        "metadata/manifest.json",
        std::time::Duration::from_millis(5),
        sender,
    );
    let event = receiver.recv().expect("receive bounded adapter result");

    // Assert
    assert!(started.elapsed() < std::time::Duration::from_millis(500));
    assert!(matches!(
        event,
        StorageEvent::HeadComplete {
            result: StorageOutcome::Err(message),
            ..
        } if message.to_string().contains("timed out")
    ));
}

#[test]
fn should_apply_operation_timeout_to_cloud_cas_adapter_when_shorter_than_configured_timeout() {
    // Arrange: keep the same wide separation for the conditional-write adapter.
    let storage = CloudStorage::new_with_timeout(
        Arc::new(DelayedMissingGetBackend),
        "tenant".to_string(),
        std::time::Duration::from_secs(1),
    );
    let (sender, receiver) = mpsc::channel();

    // Act
    let started = std::time::Instant::now();
    StorageBackend::submit_write_with_headers_and_timeout(
        &storage,
        "metadata/manifest.json",
        b"manifest".to_vec(),
        vec![("If-None-Match".to_string(), "*".to_string())],
        std::time::Duration::from_millis(5),
        sender,
    );
    let event = receiver.recv().expect("receive bounded adapter result");

    // Assert
    assert!(started.elapsed() < std::time::Duration::from_millis(500));
    assert!(matches!(
        event,
        StorageEvent::WriteComplete {
            result: StorageOutcome::Err(message),
            ..
        } if message.to_string().contains("timed out")
    ));
}

#[test]
fn should_route_namespace_put_operation() {
    // Arrange
    let storage = CloudStorage::with_mock();
    let (tx, rx) = mpsc::channel();
    let data = vec![1, 2, 3];

    // Act
    storage.submit_put("file", data, vec![], tx);
    let event = rx.recv().unwrap();

    // Assert
    match event {
        CloudEvent::Put { key, result } => {
            assert_eq!(key, "midge/file");
            assert!(result.is_ok());
        }
        _ => panic!("Expected PutComplete"),
    }
}

#[test]
fn should_delegate_conditional_put_without_head_preflight() {
    // Arrange
    let backend = Arc::new(ConditionalPutOnlyBackend::default());
    let storage = CloudStorage::new(backend.clone(), "tenant".to_string());
    let (sender, receiver) = mpsc::channel();

    // Act
    storage.submit_put(
        "lease/primary",
        b"holder".to_vec(),
        vec![("If-None-Match".to_string(), "*".to_string())],
        sender,
    );
    let event = receiver.recv().expect("receive conditional PUT result");

    // Assert
    assert!(matches!(
        event,
        CloudEvent::Put {
            result: CloudOutcome::Ok(()),
            ..
        }
    ));
    assert_eq!(backend.heads.load(Ordering::SeqCst), 0);
}

#[test]
fn should_route_namespace_get_operation() {
    // Arrange
    let storage = CloudStorage::with_mock();

    // First put a file
    let (put_tx, put_rx) = mpsc::channel();
    storage.submit_put("testfile", vec![1, 2, 3], vec![], put_tx);
    let _ = put_rx.recv();

    // Act
    let (tx, rx) = mpsc::channel();
    storage.submit_get("testfile", tx);
    let event = rx.recv().unwrap();

    // Assert
    match event {
        CloudEvent::Get { key, result } => {
            assert!(key.starts_with("midge/"));
            assert!(result.is_ok());
        }
        _ => panic!("Expected GetComplete"),
    }
}

#[test]
fn should_route_delete_with_namespace_applied() {
    // Arrange
    let storage = CloudStorage::with_mock();
    let (tx, rx) = mpsc::channel();

    // Act
    storage.submit_delete("file", tx);
    let event = rx.recv().unwrap();

    // Assert
    match event {
        CloudEvent::Delete { key, result } => {
            assert_eq!(key, "midge/file");
            assert!(result.is_ok());
        }
        _ => panic!("Expected DeleteComplete"),
    }
}

#[test]
fn should_route_head_return_metadata() {
    // Arrange
    let storage = CloudStorage::with_mock();

    // First put a file
    let (put_tx, put_rx) = mpsc::channel();
    storage.submit_put("testfile", vec![1, 2, 3], vec![], put_tx);
    let _ = put_rx.recv();

    // Act
    let (tx, rx) = mpsc::channel();
    storage.submit_head("testfile", tx);
    let event = rx.recv().unwrap();

    // Assert
    match event {
        CloudEvent::Head { key, result } => {
            assert!(key.starts_with("midge/"));
            match result {
                CloudOutcome::Ok(metadata) => {
                    assert_eq!(metadata.size, 3);
                }
                CloudOutcome::Err(_) => panic!("Expected Ok metadata"),
            }
        }
        _ => panic!("Expected HeadComplete"),
    }
}

#[test]
fn should_honor_if_match_header_on_put() {
    // Arrange
    let storage = CloudStorage::with_mock();
    let (put_tx, put_rx) = mpsc::channel();
    storage.submit_put("file1", vec![1], vec![], put_tx);
    let _ = put_rx.recv();

    // Get current etag via HEAD
    let (head_tx, head_rx) = mpsc::channel();
    storage.submit_head("file1", head_tx);
    let head_event = head_rx.recv().unwrap();
    let current_etag = match head_event {
        CloudEvent::Head { result, .. } => match result {
            CloudOutcome::Ok(meta) => meta.etag,
            CloudOutcome::Err(_) => panic!("expected head ok"),
        },
        _ => panic!("expected head event"),
    };

    // Act - conditional update with matching If-Match
    let headers = vec![("If-Match".into(), current_etag.clone())];
    let (put_tx, put_rx) = mpsc::channel();
    storage.submit_put("file1", vec![9, 9, 9], headers, put_tx);
    let put_event = put_rx.recv().unwrap();

    // Assert - success and new etag changed
    match put_event {
        CloudEvent::Put { result, .. } => assert!(result.is_ok()),
        _ => panic!("expected put complete"),
    }

    let (head_tx, head_rx) = mpsc::channel();
    storage.submit_head("file1", head_tx);
    let head_event = head_rx.recv().unwrap();
    let new_etag = match head_event {
        CloudEvent::Head { result, .. } => match result {
            CloudOutcome::Ok(meta) => meta.etag,
            CloudOutcome::Err(_) => panic!("expected head ok"),
        },
        _ => panic!("expected head event"),
    };

    assert_ne!(current_etag, new_etag);
}

#[test]
fn should_fail_put_when_if_match_mismatch() {
    // Arrange
    let storage = CloudStorage::with_mock();
    let (put_tx, put_rx) = mpsc::channel();
    storage.submit_put("file2", vec![1], vec![], put_tx);
    let _ = put_rx.recv();

    // Act - conditional update with non-matching If-Match
    let headers = vec![("If-Match".into(), "mock-gen-999".into())];
    let (put_tx, put_rx) = mpsc::channel();
    storage.submit_put("file2", vec![2], headers, put_tx);
    let put_event = put_rx.recv().unwrap();

    // Assert - precondition failed
    match put_event {
        CloudEvent::Put { result, .. } => assert!(result.is_err()),
        _ => panic!("expected put complete"),
    }
}

#[test]
fn should_fail_if_match_on_missing_object() {
    // Arrange
    let storage = CloudStorage::with_mock();

    // Act - If-Match on non-existent key should fail
    let headers = vec![("If-Match".into(), "mock-gen-1".into())];
    let (put_tx, put_rx) = mpsc::channel();
    storage.submit_put("no-such", vec![1], headers, put_tx);
    let put_event = put_rx.recv().unwrap();

    // Assert - precondition failed
    match put_event {
        CloudEvent::Put { result, .. } => assert!(result.is_err()),
        _ => panic!("expected put complete"),
    }
}

#[test]
fn should_respect_if_none_match_star_on_existing_object() {
    // Arrange
    let storage = CloudStorage::with_mock();
    let (put_tx, put_rx) = mpsc::channel();
    storage.submit_put("file3", vec![1], vec![], put_tx);
    let _ = put_rx.recv();

    // Act - conditional create should fail when object exists
    let headers = vec![("If-None-Match".into(), "*".into())];
    let (put_tx, put_rx) = mpsc::channel();
    storage.submit_put("file3", vec![2], headers, put_tx);
    let put_event = put_rx.recv().unwrap();

    // Assert - precondition failed
    match put_event {
        CloudEvent::Put { result, .. } => assert!(result.is_err()),
        _ => panic!("expected put complete"),
    }
}

#[test]
fn should_enforce_if_match_if_none_match_given_concurrent_remote_writers_when_publishing() {
    fn concurrent_puts(
        storage: &Arc<CloudStorage>,
        values: [Vec<u8>; 2],
        headers: [Vec<(String, String)>; 2],
    ) -> Vec<CloudOutcome<()>> {
        let barrier = Arc::new(std::sync::Barrier::new(3));
        let mut handles = Vec::new();
        for (value, headers) in values.into_iter().zip(headers) {
            let storage = Arc::clone(storage);
            let barrier = Arc::clone(&barrier);
            handles.push(std::thread::spawn(move || {
                let (sender, receiver) = mpsc::channel();
                barrier.wait();
                storage.submit_put("concurrent", value, headers, sender);
                match receiver.recv().expect("receive concurrent PUT result") {
                    CloudEvent::Put { result, .. } => result,
                    event => panic!("expected PUT event, got {event:?}"),
                }
            }));
        }
        barrier.wait();
        handles
            .into_iter()
            .map(|handle| handle.join().expect("concurrent writer panicked"))
            .collect()
    }

    // Arrange
    let storage = Arc::new(CloudStorage::with_mock());

    // Act
    let creates = concurrent_puts(
        &storage,
        [b"create-a".to_vec(), b"create-b".to_vec()],
        [
            vec![("If-None-Match".to_string(), "*".to_string())],
            vec![("If-None-Match".to_string(), "*".to_string())],
        ],
    );
    let (head_sender, head_receiver) = mpsc::channel();
    storage.submit_head("concurrent", head_sender);
    let etag = match head_receiver
        .recv()
        .expect("receive HEAD after create race")
    {
        CloudEvent::Head {
            result: CloudOutcome::Ok(metadata),
            ..
        } => metadata.etag,
        event => panic!("expected successful HEAD event, got {event:?}"),
    };
    let updates = concurrent_puts(
        &storage,
        [b"update-a".to_vec(), b"update-b".to_vec()],
        [
            vec![("If-Match".to_string(), etag.clone())],
            vec![("If-Match".to_string(), etag)],
        ],
    );

    // Assert
    for outcomes in [&creates, &updates] {
        assert_eq!(outcomes.iter().filter(|result| result.is_ok()).count(), 1);
        assert_eq!(
            outcomes
                .iter()
                .filter(|result| matches!(result, Err(CloudError::PreconditionFailed(_))))
                .count(),
            1
        );
    }
}

#[test]
fn should_honor_if_match_header_on_delete() {
    // Arrange
    let storage = CloudStorage::with_mock();
    let (put_tx, put_rx) = mpsc::channel();
    storage.submit_put("delete-file", vec![1], vec![], put_tx);
    let _ = put_rx.recv();

    let (head_tx, head_rx) = mpsc::channel();
    storage.submit_head("delete-file", head_tx);
    let etag = match head_rx.recv().unwrap() {
        CloudEvent::Head {
            result: CloudOutcome::Ok(metadata),
            ..
        } => metadata.etag,
        other => panic!("expected HEAD ok, got {other:?}"),
    };

    // Act
    let (delete_tx, delete_rx) = mpsc::channel();
    storage.submit_delete_with_headers("delete-file", vec![("If-Match".into(), etag)], delete_tx);

    // Assert
    match delete_rx.recv().unwrap() {
        CloudEvent::Delete { result, .. } => assert!(result.is_ok()),
        other => panic!("expected delete complete, got {other:?}"),
    }
    let (head_tx, head_rx) = mpsc::channel();
    storage.submit_head("delete-file", head_tx);
    match head_rx.recv().unwrap() {
        CloudEvent::Head { result, .. } => assert!(result.is_err()),
        other => panic!("expected HEAD complete, got {other:?}"),
    }
}

#[test]
fn should_reject_delete_when_if_match_mismatches() {
    // Arrange
    let storage = CloudStorage::with_mock();
    let (put_tx, put_rx) = mpsc::channel();
    storage.submit_put("stale-delete-file", vec![1], vec![], put_tx);
    let _ = put_rx.recv();

    // Act
    let (delete_tx, delete_rx) = mpsc::channel();
    storage.submit_delete_with_headers(
        "stale-delete-file",
        vec![("If-Match".into(), "mock-gen-999".into())],
        delete_tx,
    );

    // Assert
    match delete_rx.recv().unwrap() {
        CloudEvent::Delete { result, .. } => assert!(result.is_err()),
        other => panic!("expected delete complete, got {other:?}"),
    }
    let (head_tx, head_rx) = mpsc::channel();
    storage.submit_head("stale-delete-file", head_tx);
    match head_rx.recv().unwrap() {
        CloudEvent::Head {
            result: CloudOutcome::Ok(_),
            ..
        } => {}
        other => panic!("expected object to survive stale delete, got {other:?}"),
    }
}

#[test]
fn should_route_get_range_with_bounds() {
    // Arrange
    let storage = CloudStorage::with_mock();

    // First put a file
    let (put_tx, put_rx) = mpsc::channel();
    storage.submit_put("rangefile", vec![1, 2, 3, 4, 5], vec![], put_tx);
    let _ = put_rx.recv();

    // Act
    let (tx, rx) = mpsc::channel();
    storage.submit_get_range("rangefile", 1, Some(4), tx);
    let event = rx.recv().unwrap();

    // Assert
    match event {
        CloudEvent::GetRange {
            key,
            start,
            end,
            result,
        } => {
            assert!(key.starts_with("midge/"));
            assert_eq!(start, 1);
            assert_eq!(end, Some(4));
            assert!(result.is_ok());
        }
        _ => panic!("Expected GetRangeComplete"),
    }
}

#[test]
fn should_handle_get_range_with_none_end_bound() {
    // Arrange
    let storage = CloudStorage::with_mock();

    // Act
    let (tx, rx) = mpsc::channel();
    storage.submit_get_range("file", 0, None, tx);
    let event = rx.recv().unwrap();

    // Assert
    match event {
        CloudEvent::GetRange { end, .. } => {
            assert_eq!(end, None);
        }
        _ => panic!("Expected GetRangeComplete"),
    }
}

// =========== CloudEvent Tests ===========

#[test]
fn should_send_list_complete_event_via_callback() {
    // Arrange
    let storage = CloudStorage::with_mock();
    let (put_tx, put_rx) = mpsc::channel();
    storage.submit_put("prefix/file1", vec![1], vec![], put_tx);
    let _ = put_rx.recv();

    let (put_tx, put_rx) = mpsc::channel();
    storage.submit_put("prefix/file2", vec![2], vec![], put_tx);
    let _ = put_rx.recv();

    // Act
    let (tx, rx) = mpsc::channel();
    storage.submit_list("prefix", tx);
    let event = rx.recv().unwrap();

    // Assert
    match event {
        CloudEvent::List { prefix, result } => {
            assert_eq!(prefix, "midge/prefix");
            match result {
                CloudOutcome::Ok(items) => {
                    assert!(items.len() >= 2);
                    assert!(items.iter().any(|k| k.contains("file1")));
                    assert!(items.iter().any(|k| k.contains("file2")));
                }
                CloudOutcome::Err(_) => panic!("Expected Ok result"),
            }
        }
        _ => panic!("Expected ListComplete"),
    }
}

// =========== Data Handling & Integration Tests ===========

#[test]
fn should_handle_large_file_operations() {
    // Arrange
    let storage = CloudStorage::with_mock();
    let large_data = vec![42u8; 1_000_000]; // 1 MB
    let (tx, rx) = mpsc::channel();

    // Act
    storage.submit_put("largefile", large_data.clone(), vec![], tx);
    let event = rx.recv().unwrap();

    // Assert
    match event {
        CloudEvent::Put { result, .. } => {
            assert!(result.is_ok());
        }
        _ => panic!("Expected PutComplete"),
    }

    // Verify we can retrieve it
    let (tx, rx) = mpsc::channel();
    storage.submit_get("largefile", tx);
    let event = rx.recv().unwrap();

    match event {
        CloudEvent::Get { result, .. } => match result {
            CloudOutcome::Ok(data) => {
                assert_eq!(data.len(), 1_000_000);
            }
            CloudOutcome::Err(_) => panic!("Expected Ok"),
        },
        _ => panic!("Expected GetComplete"),
    }
}

#[test]
fn should_preserve_binary_data_fidelity() {
    // Arrange
    let storage = CloudStorage::with_mock();
    let binary_data = vec![0u8, 1u8, 255u8, 254u8, 127u8, 128u8];

    // Act: put and get binary data round-trip
    let (put_tx, put_rx) = mpsc::channel();
    storage.submit_put("binaryfile", binary_data.clone(), vec![], put_tx);
    let _ = put_rx.recv();

    let (tx, rx) = mpsc::channel();
    storage.submit_get("binaryfile", tx);
    let event = rx.recv().unwrap();

    // Assert
    match event {
        CloudEvent::Get { result, .. } => match result {
            CloudOutcome::Ok(data) => {
                assert_eq!(data, binary_data, "binary data must be preserved exactly");
            }
            CloudOutcome::Err(_) => panic!("Expected Ok result"),
        },
        _ => panic!("Expected GetComplete"),
    }
}

#[test]
fn should_dispatch_all_cloud_operations_successfully() {
    // Arrange
    let storage = CloudStorage::with_mock();

    // Act: dispatch every supported operation through the same production queue.

    let (tx, rx) = mpsc::channel();
    storage.submit_put("f1", vec![1, 2], vec![], tx);

    // Assert: each operation must reach the backend with the namespaced key and
    // report its real outcome, not just "didn't panic".
    match rx.recv().unwrap() {
        CloudEvent::Put { key, result } => {
            assert_eq!(key, "midge/f1");
            assert!(result.is_ok());
        }
        other => panic!("expected Put event, got {other:?}"),
    }

    // f2 was never put, so the get is expected to miss.
    let (tx, rx) = mpsc::channel();
    storage.submit_get("f2", tx);
    match rx.recv().unwrap() {
        CloudEvent::Get { key, result } => {
            assert_eq!(key, "midge/f2");
            assert!(result.is_err());
        }
        other => panic!("expected Get event, got {other:?}"),
    }

    let (tx, rx) = mpsc::channel();
    storage.submit_delete("f3", tx);
    match rx.recv().unwrap() {
        CloudEvent::Delete { key, result } => {
            assert_eq!(key, "midge/f3");
            assert!(result.is_ok());
        }
        other => panic!("expected Delete event, got {other:?}"),
    }

    let (tx, rx) = mpsc::channel();
    storage.submit_put("prefix/f", vec![9], vec![], tx);
    let _ = rx.recv();
    let (tx, rx) = mpsc::channel();
    storage.submit_list("prefix", tx);
    match rx.recv().unwrap() {
        CloudEvent::List { prefix, result } => {
            assert_eq!(prefix, "midge/prefix");
            let items = result.expect("list should succeed");
            assert!(items.iter().any(|k| k.contains("prefix/f")));
        }
        other => panic!("expected List event, got {other:?}"),
    }

    let (tx, rx) = mpsc::channel();
    storage.submit_put("f4", vec![1, 2, 3], vec![], tx);
    let _ = rx.recv();
    let (tx, rx) = mpsc::channel();
    storage.submit_head("f4", tx);
    match rx.recv().unwrap() {
        CloudEvent::Head { key, result } => {
            assert_eq!(key, "midge/f4");
            let metadata = result.expect("head should succeed for an existing object");
            assert_eq!(metadata.size, 3);
        }
        other => panic!("expected Head event, got {other:?}"),
    }

    let (tx, rx) = mpsc::channel();
    storage.submit_get_range("f4", 0, Some(2), tx);
    match rx.recv().unwrap() {
        CloudEvent::GetRange {
            key,
            start,
            end,
            result,
        } => {
            assert_eq!(key, "midge/f4");
            assert_eq!(start, 0);
            assert_eq!(end, Some(2));
            assert!(result.is_ok());
        }
        other => panic!("expected GetRange event, got {other:?}"),
    }
}

#[test]
fn should_handle_get_missing_file_gracefully() {
    // Arrange
    let storage = CloudStorage::with_mock();
    let (tx, rx) = mpsc::channel();

    // Act
    storage.submit_get("nonexistent", tx);
    let event = rx.recv().unwrap();

    // Assert
    match event {
        CloudEvent::Get { result, .. } => {
            assert!(result.is_err());
        }
        _ => panic!("Expected GetComplete"),
    }
}

#[test]
fn should_handle_metadata_for_empty_files() {
    // Arrange
    let storage = CloudStorage::with_mock();

    // Put an empty file
    let (put_tx, put_rx) = mpsc::channel();
    storage.submit_put("emptyfile", vec![], vec![], put_tx);
    let _ = put_rx.recv();

    // Act
    let (tx, rx) = mpsc::channel();
    storage.submit_head("emptyfile", tx);
    let event = rx.recv().unwrap();

    // Assert
    match event {
        CloudEvent::Head { result, .. } => match result {
            CloudOutcome::Ok(metadata) => {
                assert_eq!(metadata.size, 0);
            }
            CloudOutcome::Err(_) => panic!("Expected Ok metadata"),
        },
        _ => panic!("Expected HeadComplete"),
    }
}

#[derive(Default)]
struct HeaderRecordingBackend {
    puts: parking_lot::Mutex<Vec<Vec<(String, String)>>>,
    deletes: parking_lot::Mutex<Vec<Vec<(String, String)>>>,
    heads: parking_lot::Mutex<Vec<Vec<(String, String)>>>,
    lists: parking_lot::Mutex<Vec<Vec<(String, String)>>>,
}

impl HeaderRecordingBackend {
    fn timeout_header(headers: &[(String, String)]) -> Option<String> {
        headers
            .iter()
            .find(|(name, _)| name.eq_ignore_ascii_case(REQUEST_TIMEOUT_HEADER))
            .map(|(_, value)| value.clone())
    }
}

impl CloudBackend for HeaderRecordingBackend {
    crate::storage::cloud::unsupported_cloud_backend!(
        submit_get_with_metadata,
        submit_list,
        submit_head,
    );

    fn submit_put(
        &self,
        key: &str,
        _data: Vec<u8>,
        headers: Vec<(String, String)>,
        callback: CloudCallback,
    ) {
        self.puts.lock().push(headers);
        let _ = callback.send(CloudEvent::Put {
            key: key.to_string(),
            result: CloudOutcome::Ok(()),
        });
    }

    fn submit_get(&self, key: &str, callback: CloudCallback) {
        let _ = callback.send(CloudEvent::Get {
            key: key.to_string(),
            result: CloudOutcome::Err(CloudError::Protocol("unsupported".to_string())),
        });
    }

    fn submit_delete(&self, key: &str, headers: Vec<(String, String)>, callback: CloudCallback) {
        self.deletes.lock().push(headers);
        let _ = callback.send(CloudEvent::Delete {
            key: key.to_string(),
            result: CloudOutcome::Ok(()),
        });
    }

    fn submit_head_with_headers(
        &self,
        key: &str,
        headers: Vec<(String, String)>,
        callback: CloudCallback,
    ) {
        self.heads.lock().push(headers);
        let _ = callback.send(CloudEvent::Head {
            key: key.to_string(),
            result: CloudOutcome::Ok(ObjectMetadata::new(7, "etag".to_string())),
        });
    }

    fn submit_list_with_headers(
        &self,
        prefix: &str,
        headers: Vec<(String, String)>,
        callback: CloudCallback,
    ) {
        self.lists.lock().push(headers);
        let _ = callback.send(CloudEvent::List {
            prefix: prefix.to_string(),
            result: CloudOutcome::Ok(Vec::new()),
        });
    }

    fn submit_get_range(&self, key: &str, start: u64, end: Option<u64>, callback: CloudCallback) {
        let _ = callback.send(CloudEvent::GetRange {
            key: key.to_string(),
            start,
            end,
            result: CloudOutcome::Err(CloudError::Protocol("unsupported".to_string())),
        });
    }
}

#[test]
fn should_bound_provider_put_by_caller_timeout_when_write_is_unreserved() {
    // Arrange
    let backend = Arc::new(HeaderRecordingBackend::default());
    let storage = CloudStorage::new_with_timeout(
        backend.clone(),
        "tenant".to_string(),
        std::time::Duration::from_secs(30),
    );
    let (sender, receiver) = mpsc::channel();

    // Act
    StorageBackend::submit_write_with_headers_and_timeout(
        &storage,
        "metadata/registry.json",
        b"payload".to_vec(),
        vec![("If-Match".to_string(), "\"e1\"".to_string())],
        std::time::Duration::from_millis(200),
        sender,
    );

    // Assert
    assert!(matches!(
        receiver.recv_timeout(std::time::Duration::from_secs(1)),
        Ok(StorageEvent::WriteComplete {
            result: StorageOutcome::Ok(()),
            ..
        })
    ));
    let put_headers = backend.puts.lock();
    assert_eq!(put_headers.len(), 1);
    assert_eq!(
        HeaderRecordingBackend::timeout_header(&put_headers[0]).as_deref(),
        Some("200")
    );
}

#[test]
fn should_bound_provider_delete_by_callback_timeout_when_deleting() {
    // Arrange
    let backend = Arc::new(HeaderRecordingBackend::default());
    let storage = CloudStorage::new_with_timeout(
        backend.clone(),
        "tenant".to_string(),
        std::time::Duration::from_millis(750),
    );
    let (plain_sender, plain_receiver) = mpsc::channel();
    let (conditional_sender, conditional_receiver) = mpsc::channel();

    // Act
    StorageBackend::submit_delete(&storage, "sst/000001.sst", plain_sender);
    StorageBackend::submit_delete_with_headers(
        &storage,
        "sst/000002.sst",
        vec![("If-Match".to_string(), "\"e1\"".to_string())],
        conditional_sender,
    );

    // Assert
    for receiver in [plain_receiver, conditional_receiver] {
        assert!(matches!(
            receiver.recv_timeout(std::time::Duration::from_secs(1)),
            Ok(StorageEvent::DeleteComplete {
                result: StorageOutcome::Ok(()),
                ..
            })
        ));
    }
    let delete_headers = backend.deletes.lock();
    assert_eq!(delete_headers.len(), 2);
    for headers in delete_headers.iter() {
        assert_eq!(
            HeaderRecordingBackend::timeout_header(headers).as_deref(),
            Some("750")
        );
    }
}
#[test]
fn should_bound_provider_read_requests_by_caller_timeout() {
    // Arrange
    let backend = Arc::new(HeaderRecordingBackend::default());
    let storage = CloudStorage::new_with_timeout(
        backend.clone(),
        "tenant".to_string(),
        std::time::Duration::from_millis(900),
    );
    let (head_sender, head_receiver) = mpsc::channel();
    let (list_sender, list_receiver) = mpsc::channel();

    // Act
    StorageBackend::submit_head_with_timeout(
        &storage,
        "sst/000001.sst",
        std::time::Duration::from_millis(250),
        head_sender,
    );
    storage.submit_list("sst/", list_sender);

    // Assert
    assert!(matches!(
        head_receiver.recv_timeout(std::time::Duration::from_secs(1)),
        Ok(StorageEvent::HeadComplete {
            result: StorageOutcome::Ok(_),
            ..
        })
    ));
    assert!(matches!(
        list_receiver.recv_timeout(std::time::Duration::from_secs(1)),
        Ok(CloudEvent::List { result: Ok(_), .. })
    ));
    assert_eq!(
        HeaderRecordingBackend::timeout_header(&backend.heads.lock()[0]).as_deref(),
        Some("250"),
        "HEAD must carry the caller's timeout"
    );
    assert_eq!(
        HeaderRecordingBackend::timeout_header(&backend.lists.lock()[0]).as_deref(),
        Some("900"),
        "LIST must carry the adapter's callback timeout"
    );
}
#[test]
fn should_preserve_precondition_failed_kind_when_cloud_error_crosses_storage_backend() {
    // Arrange: a lost conditional write whose message begins with another
    // class's old prefix. String matching classified it as absent.
    let error = CloudError::PreconditionFailed("not found: x".to_string());

    // Act
    let converted = storage_error_from_cloud(error);

    // Assert
    assert!(converted.is_precondition_failed());
    assert!(!converted.is_not_found());
}

#[test]
fn should_preserve_not_found_kind_when_cloud_error_crosses_storage_backend() {
    // Arrange
    let error = CloudError::NotFound("precondition failed: y".to_string());

    // Act
    let converted = storage_error_from_cloud(error);

    // Assert
    assert!(converted.is_not_found());
    assert!(!converted.is_precondition_failed());
}
