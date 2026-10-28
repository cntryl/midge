//! Conditional mutation outcomes with real provider effects behind lost responses.

use super::receipt_faults::forward;
use super::{
    build_cloud_storage, conditional_head_headers, ensure_sqrzl_namespace, get, list, put,
    require_sqrzl, CloudError, CloudEvent, CloudStorage,
};
use crate::config::CloudProviderConfig;
use crate::storage::cloud::native_http::NativeHttpServer;
use std::sync::atomic::{AtomicUsize, Ordering};
use std::sync::Arc;
use std::time::Duration;

fn providers() -> Vec<CloudProviderConfig> {
    vec![
        CloudProviderConfig::sqrzl_s3("midge-mutation-outcomes-s3"),
        CloudProviderConfig::sqrzl_azure("midge-mutation-outcomes-azure"),
        CloudProviderConfig::sqrzl_gcs("midge-mutation-outcomes-gcs"),
        CloudProviderConfig::sqrzl_gcs_json("midge-mutation-outcomes-gcs-json"),
    ]
}

fn prepare(provider: &CloudProviderConfig) -> Arc<CloudStorage> {
    require_sqrzl("conditional-mutation-outcome");
    ensure_sqrzl_namespace(provider).unwrap();
    build_cloud_storage(provider, "").unwrap()
}

fn proxy(drop_success: bool) -> (NativeHttpServer, Arc<AtomicUsize>) {
    let requests = Arc::new(AtomicUsize::new(0));
    let observed = Arc::clone(&requests);
    let runtime = tokio::runtime::Builder::new_current_thread()
        .enable_all()
        .build()
        .unwrap();
    let server = NativeHttpServer::start(move |_, request| {
        observed.fetch_add(1, Ordering::AcqRel);
        assert!(matches!(request.method.as_str(), "PUT" | "POST" | "DELETE"));
        let response = forward(request, &runtime);
        if drop_success && (200..300).contains(&response.status) {
            None
        } else {
            Some(response)
        }
    });
    (server, requests)
}

fn proxied(provider: &CloudProviderConfig, server: &NativeHttpServer) -> Arc<CloudStorage> {
    let cloud = build_cloud_storage(
        &provider.clone().with_endpoint(&server.endpoint).unwrap(),
        "",
    )
    .unwrap();
    cloud.set_retry_policy(crate::storage::cloud::CloudRetryPolicy {
        max_retries: 3,
        base_delay: Duration::ZERO,
    });
    cloud
}

fn key() -> String {
    format!("qualification/mutation/{}/object", uuid::Uuid::new_v4())
}

#[test]
fn should_preserve_unknown_create_outcome_when_sqrzl_success_response_is_lost() {
    // Arrange
    for provider in providers() {
        let direct = prepare(&provider);
        let (server, requests) = proxy(true);
        let cloud = proxied(&provider, &server);
        let key = key();
        // Act
        let result = put(
            &cloud,
            &key,
            b"committed once".to_vec(),
            vec![("If-None-Match".into(), "*".into())],
        );
        // Assert: replay cannot turn the committed create into a false 412.
        assert!(
            matches!(
                result,
                Err(CloudError::Transport(_) | CloudError::Timeout(_))
            ),
            "{result:?}"
        );
        assert_eq!(requests.load(Ordering::Acquire), 1);
        assert_eq!(get(&direct, &key).unwrap(), b"committed once");
        assert_eq!(list(&direct, &key).unwrap(), vec![key]);
    }
}

#[test]
fn should_preserve_unknown_delete_outcome_when_sqrzl_success_response_is_lost() {
    // Arrange
    for provider in providers() {
        let direct = prepare(&provider);
        let key = key();
        put(&direct, &key, b"delete once".to_vec(), vec![]).unwrap();
        let headers = conditional_head_headers(&direct, &key);
        let (server, requests) = proxy(true);
        let cloud = proxied(&provider, &server);
        let (tx, rx) = std::sync::mpsc::channel();
        // Act
        cloud.submit_delete_with_headers(&key, headers, tx);
        let event = rx.recv_timeout(Duration::from_secs(5)).unwrap();
        // Assert: the effect is real, but its lost response remains ambiguous.
        assert!(
            matches!(
                event,
                CloudEvent::Delete {
                    result: Err(CloudError::Transport(_) | CloudError::Timeout(_)),
                    ..
                }
            ),
            "{event:?}"
        );
        assert_eq!(requests.load(Ordering::Acquire), 1);
        assert!(matches!(get(&direct, &key), Err(CloudError::NotFound(_))));
    }
}

#[test]
fn should_preserve_sqrzl_replacement_when_conditional_delete_uses_stale_identity() {
    // Arrange
    for provider in providers() {
        let direct = prepare(&provider);
        let key = key();
        put(&direct, &key, b"original".to_vec(), vec![]).unwrap();
        let headers = conditional_head_headers(&direct, &key);
        put(&direct, &key, b"replacement".to_vec(), vec![]).unwrap();
        let (server, requests) = proxy(false);
        let cloud = proxied(&provider, &server);
        let (tx, rx) = std::sync::mpsc::channel();
        // Act
        cloud.submit_delete_with_headers(&key, headers, tx);
        let event = rx.recv_timeout(Duration::from_secs(5)).unwrap();
        // Assert
        assert!(
            matches!(
                event,
                CloudEvent::Delete {
                    result: Err(CloudError::PreconditionFailed(_)),
                    ..
                }
            ),
            "{event:?}"
        );
        assert_eq!(requests.load(Ordering::Acquire), 1);
        assert_eq!(get(&direct, &key).unwrap(), b"replacement");
    }
}

#[test]
fn should_report_timeout_when_sqrzl_create_finishes_after_original_deadline() {
    // Arrange: hold the actual received request until its caller timed out.
    for provider in providers() {
        let direct = prepare(&provider);
        let (arrived_tx, arrived) = crossbeam::channel::bounded(1);
        let (release, released) = crossbeam::channel::bounded(1);
        let (finished_tx, finished) = crossbeam::channel::bounded(1);
        let runtime = tokio::runtime::Builder::new_current_thread()
            .enable_all()
            .build()
            .unwrap();
        let server = NativeHttpServer::start(move |_, request| {
            arrived_tx.send(()).unwrap();
            released.recv_timeout(Duration::from_secs(5)).unwrap();
            let response = forward(request, &runtime);
            finished_tx.send(response.status).unwrap();
            None
        });
        let cloud = proxied(&provider, &server);
        let key = key();
        let mut headers = vec![("If-None-Match".into(), "*".into())];
        crate::storage::cloud::set_request_timeout_header(&mut headers, Duration::from_millis(250));
        let (tx, rx) = std::sync::mpsc::channel();
        // Act: the external effect is released only after the timeout callback.
        cloud.submit_put(&key, b"late effect".to_vec(), headers, tx);
        arrived.recv_timeout(Duration::from_secs(5)).unwrap();
        let event = rx.recv_timeout(Duration::from_secs(5)).unwrap();
        let absent_before_release = matches!(get(&direct, &key), Err(CloudError::NotFound(_)));
        release.send(()).unwrap();
        let status = finished.recv_timeout(Duration::from_secs(5)).unwrap();
        // Assert: timeout is not proof that the remote effect never occurred.
        assert!(
            matches!(
                event,
                CloudEvent::Put {
                    result: Err(CloudError::Timeout(_)),
                    ..
                }
            ),
            "{event:?}"
        );
        assert!(absent_before_release);
        assert!((200..300).contains(&status));
        assert_eq!(get(&direct, &key).unwrap(), b"late effect");
    }
}
