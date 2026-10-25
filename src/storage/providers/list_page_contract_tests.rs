use crate::common::OperationDeadline;
use crate::config::{S3CompatibleConfig, S3CredentialSource};
use crate::storage::cloud::native_http::{NativeHttpServer, Response};
use crate::storage::cloud::{BlockingCloud, CloudBackend, CloudListPageRequest, CloudStorage};
use std::sync::{Arc, Mutex};
use std::time::Duration;

#[derive(Clone, Copy, Debug)]
enum Dialect {
    S3,
    Azure,
    GcsJson,
    GcsXml,
}

impl Dialect {
    const ALL: [Self; 4] = [Self::S3, Self::Azure, Self::GcsJson, Self::GcsXml];
    fn backend(self, endpoint: String) -> Arc<dyn CloudBackend> {
        match self {
            Self::S3 => super::s3_resolver::try_resolve(
                &S3CompatibleConfig::new(
                    "bucket",
                    "us-east-1",
                    endpoint,
                    S3CredentialSource::access_key("access", "secret"),
                )
                .into(),
            )
            .unwrap()
            .unwrap(),
            Self::Azure => super::azure::contract_test_backend(endpoint),
            Self::GcsJson => super::gcs::contract_test_json_backend(endpoint),
            Self::GcsXml => super::gcs::contract_test_xml_backend(endpoint),
        }
    }
    fn body(self) -> String {
        match self {
            Self::Azure => "<EnumerationResults><Blobs><Blob><Name>db/sst/a</Name></Blob></Blobs><NextMarker>next</NextMarker></EnumerationResults>".into(),
            Self::GcsJson => r#"{"items":[{"name":"db/sst/a"}],"nextPageToken":"next"}"#.into(),
            Self::S3 => "<ListBucketResult><Contents><Key>db/sst/a</Key></Contents><IsTruncated>true</IsTruncated><NextContinuationToken>next</NextContinuationToken></ListBucketResult>".into(),
            Self::GcsXml => "<ListBucketResult><Contents><Key>db/sst/a</Key></Contents><IsTruncated>true</IsTruncated><NextMarker>next</NextMarker></ListBucketResult>".into(),
        }
    }
    fn limit(self) -> &'static str {
        match self {
            Self::Azure => "maxresults=2",
            Self::GcsJson => "maxResults=2",
            _ => "max-keys=2",
        }
    }
}

#[test]
fn should_stop_after_one_native_list_request_when_page_has_a_continuation() {
    for dialect in Dialect::ALL {
        // Arrange
        let paths = Arc::new(Mutex::new(Vec::new()));
        let observed = Arc::clone(&paths);
        let server = NativeHttpServer::start(move |_, request| {
            observed.lock().unwrap().push(request.path);
            Some(Response::new(200, dialect.body().into_bytes()))
        });
        let cloud = CloudStorage::new(dialect.backend(server.endpoint.clone()), "db".into());
        let deadline = OperationDeadline::from_budget(Duration::from_secs(5));

        // Act
        let page = BlockingCloud::new(&cloud, &deadline)
            .list_page(
                "sst/",
                CloudListPageRequest {
                    continuation: None,
                    max_keys: 2,
                },
            )
            .unwrap()
            .unwrap();

        // Assert
        assert_eq!(page.keys, ["db/sst/a"], "{dialect:?}");
        assert_eq!(page.continuation.as_deref(), Some("next"), "{dialect:?}");
        let paths = paths.lock().unwrap();
        assert_eq!(
            paths.len(),
            1,
            "{dialect:?} implicitly followed another page"
        );
        assert!(
            paths[0].contains(dialect.limit()),
            "{dialect:?}: {}",
            paths[0]
        );
    }
}

#[test]
fn should_not_retry_native_page_when_provider_returns_transient_failure() {
    for dialect in Dialect::ALL {
        // Arrange
        let calls = Arc::new(std::sync::atomic::AtomicUsize::new(0));
        let observed = Arc::clone(&calls);
        let server = NativeHttpServer::start(move |_, _| {
            observed.fetch_add(1, std::sync::atomic::Ordering::Relaxed);
            Some(Response::new(503, b"unavailable".to_vec()))
        });
        let cloud = CloudStorage::new(dialect.backend(server.endpoint.clone()), "db".into());
        let deadline = OperationDeadline::from_budget(Duration::from_secs(5));

        // Act
        let result = BlockingCloud::new(&cloud, &deadline).list_page(
            "sst/",
            CloudListPageRequest {
                continuation: None,
                max_keys: 2,
            },
        );

        // Assert
        assert!(result.is_err(), "{dialect:?}");
        assert_eq!(
            calls.load(std::sync::atomic::Ordering::Relaxed),
            1,
            "{dialect:?}"
        );
    }
}

#[test]
fn should_reject_native_page_when_response_repeats_the_input_continuation() {
    for dialect in Dialect::ALL {
        // Arrange
        let server = NativeHttpServer::start(move |_, _| {
            Some(Response::new(200, dialect.body().into_bytes()))
        });
        let cloud = CloudStorage::new(dialect.backend(server.endpoint.clone()), "db".into());
        let deadline = OperationDeadline::from_budget(Duration::from_secs(5));

        // Act
        let result = BlockingCloud::new(&cloud, &deadline).list_page(
            "sst/",
            CloudListPageRequest {
                continuation: Some("next".into()),
                max_keys: 2,
            },
        );

        // Assert
        assert!(result.is_err(), "{dialect:?} accepted an inventory cycle");
    }
}
