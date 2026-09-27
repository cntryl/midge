//! One error contract for every cloud provider (#373).
//!
//! Callers branch on the `CloudError` class: lease renewal, control and
//! remote compare-and-swap, and startup lookups all treat
//! `PreconditionFailed` as a lost race and `NotFound` as absent. Each case
//! here scripts the response every provider sends for the same situation
//! and asserts that all of them map to one class, so a provider quirk
//! cannot quietly change caller behavior.

use super::test_support::{
    spawn_recording_http_server_with_status, spawn_scripted_http_response_server,
};
use super::{azure, gcs, s3};
use crate::storage::cloud::{CloudBackend, CloudError, CloudEvent, CloudOutcome};
use crate::storage::StorageObjectMetadata;
use std::sync::{mpsc, Arc};
use std::time::Duration;

const RESPONSE_WAIT: Duration = Duration::from_secs(10);

/// One scripted HTTP response: status, headers and body.
type Response = (u16, Vec<(String, String)>, String);

/// The outcome class callers branch on.
#[derive(Debug, PartialEq, Eq)]
enum Class {
    Ok,
    NotFound,
    PreconditionFailed,
    Protocol,
    Other(String),
}

fn class_of<T>(outcome: &CloudOutcome<T>) -> Class {
    match outcome {
        Ok(_) => Class::Ok,
        Err(CloudError::NotFound(_)) => Class::NotFound,
        Err(CloudError::PreconditionFailed(_)) => Class::PreconditionFailed,
        Err(CloudError::Protocol(_)) => Class::Protocol,
        Err(other) => Class::Other(format!("{other:?}")),
    }
}

fn class_of_event(event: &CloudEvent) -> Class {
    match event {
        CloudEvent::Put { result, .. } | CloudEvent::Delete { result, .. } => class_of(result),
        CloudEvent::Get { result, .. } | CloudEvent::GetRange { result, .. } => class_of(result),
        CloudEvent::GetWithMetadata { result, .. } => class_of(result),
        CloudEvent::List { result, .. } => class_of(result),
        CloudEvent::Head { result, .. } => class_of(result),
    }
}

fn xml_error(status: u16, code: &str) -> Response {
    (
        status,
        vec![("Content-Type".into(), "application/xml".into())],
        format!("<Error><Code>{code}</Code><Message>scripted</Message></Error>"),
    )
}

fn azure_error(status: u16, code: &str) -> Response {
    let (status, mut headers, body) = xml_error(status, code);
    headers.push(("x-ms-error-code".into(), code.into()));
    (status, headers, body)
}

fn gcs_json_error(status: u16, reason: &str) -> Response {
    (
        status,
        vec![("Content-Type".into(), "application/json".into())],
        format!(
            r#"{{"error":{{"code":{status},"message":"scripted","errors":[{{"reason":"{reason}"}}]}}}}"#
        ),
    )
}

/// How one provider mode is built and what it sends for each situation.
struct Provider {
    name: &'static str,
    backend: fn(String) -> Arc<dyn CloudBackend>,
    /// The precondition header callers send to update or delete one version.
    match_header: (&'static str, &'static str),
    /// The identity callers pass to a conditional range read.
    identity: StorageObjectMetadata,
    create_conflict: Response,
    stale_version: Response,
    missing_object: Response,
    missing_container: Response,
    empty_put_status: u16,
    list_pages: [(u16, String, String); 2],
}

fn providers() -> Vec<Provider> {
    let etag_identity = StorageObjectMetadata {
        size: 100,
        etag: "v1".into(),
        generation: None,
    };
    let generation_identity = StorageObjectMetadata {
        size: 100,
        etag: "v1".into(),
        generation: Some("7".into()),
    };
    let xml = |body: &str| (200, "application/xml".to_string(), body.to_string());
    let json = |body: &str| (200, "application/json".to_string(), body.to_string());
    vec![
        Provider {
            name: "s3",
            backend: s3::contract_test_backend,
            match_header: ("If-Match", "\"v1\""),
            identity: etag_identity.clone(),
            create_conflict: xml_error(412, "PreconditionFailed"),
            stale_version: xml_error(412, "PreconditionFailed"),
            missing_object: xml_error(404, "NoSuchKey"),
            missing_container: xml_error(404, "NoSuchBucket"),
            empty_put_status: 200,
            list_pages: [
                xml("<ListBucketResult><Contents><Key>p/a</Key></Contents><IsTruncated>true</IsTruncated><NextContinuationToken>t1</NextContinuationToken></ListBucketResult>"),
                xml("<ListBucketResult><Contents><Key>p/b</Key></Contents><IsTruncated>false</IsTruncated></ListBucketResult>"),
            ],
        },
        Provider {
            name: "azure",
            backend: azure::contract_test_backend,
            match_header: ("If-Match", "\"v1\""),
            identity: etag_identity,
            create_conflict: azure_error(409, "BlobAlreadyExists"),
            stale_version: azure_error(412, "ConditionNotMet"),
            missing_object: azure_error(404, "BlobNotFound"),
            missing_container: azure_error(404, "ContainerNotFound"),
            empty_put_status: 201,
            list_pages: [
                xml("<EnumerationResults><Blobs><Blob><Name>p/a</Name></Blob></Blobs><NextMarker>m1</NextMarker></EnumerationResults>"),
                xml("<EnumerationResults><Blobs><Blob><Name>p/b</Name></Blob></Blobs><NextMarker></NextMarker></EnumerationResults>"),
            ],
        },
        Provider {
            name: "gcs-json",
            backend: gcs::contract_test_json_backend,
            match_header: ("x-goog-if-generation-match", "7"),
            identity: generation_identity.clone(),
            create_conflict: gcs_json_error(412, "conditionNotMet"),
            stale_version: gcs_json_error(412, "conditionNotMet"),
            // GCS answers a generation match on a missing object with 412.
            missing_object: gcs_json_error(412, "conditionNotMet"),
            missing_container: gcs_json_error(404, "notFound"),
            empty_put_status: 200,
            list_pages: [
                json(r#"{"items":[{"name":"p/a"}],"nextPageToken":"t1"}"#),
                json(r#"{"items":[{"name":"p/b"}]}"#),
            ],
        },
        Provider {
            name: "gcs-xml",
            backend: gcs::contract_test_xml_backend,
            match_header: ("x-goog-if-generation-match", "7"),
            identity: generation_identity,
            create_conflict: xml_error(412, "PreconditionFailed"),
            stale_version: xml_error(412, "PreconditionFailed"),
            missing_object: xml_error(412, "PreconditionFailed"),
            missing_container: xml_error(404, "NoSuchBucket"),
            empty_put_status: 200,
            list_pages: [
                xml("<ListBucketResult><Contents><Key>p/a</Key></Contents><IsTruncated>true</IsTruncated><NextMarker>p/a</NextMarker></ListBucketResult>"),
                xml("<ListBucketResult><Contents><Key>p/b</Key></Contents><IsTruncated>false</IsTruncated></ListBucketResult>"),
            ],
        },
    ]
}

/// An operation under contract, submitted against a scripted endpoint.
#[derive(Clone, Copy)]
enum Op {
    CreateOnly,
    PutEmpty,
    PutIfMatch,
    Get,
    Head,
    Delete,
    DeleteIfMatch,
    RangeWithIdentity,
}

fn submit(provider: &Provider, op: Op, backend: &dyn CloudBackend) -> mpsc::Receiver<CloudEvent> {
    let (tx, rx) = mpsc::channel();
    let (name, value) = provider.match_header;
    let if_match = vec![(name.to_string(), value.to_string())];
    match op {
        Op::CreateOnly => backend.submit_put(
            "p/object",
            b"value".to_vec(),
            vec![("If-None-Match".into(), "*".into())],
            tx,
        ),
        Op::PutEmpty => backend.submit_put("p/object", Vec::new(), Vec::new(), tx),
        Op::PutIfMatch => backend.submit_put("p/object", b"value".to_vec(), if_match, tx),
        Op::Get => backend.submit_get("p/object", tx),
        Op::Head => backend.submit_head("p/object", tx),
        Op::Delete => backend.submit_delete("p/object", Vec::new(), tx),
        Op::DeleteIfMatch => backend.submit_delete("p/object", if_match, tx),
        Op::RangeWithIdentity => backend.submit_get_range_with_identity(
            "p/object",
            0,
            5,
            provider.identity.clone(),
            RESPONSE_WAIT,
            tx,
        ),
    }
    rx
}

/// Run `op` on `provider` against one scripted response.
fn outcome(provider: &Provider, op: Op, response: &Response) -> Class {
    let (status, headers, body) = response.clone();
    let server = spawn_recording_http_server_with_status(status, headers, body.into_bytes());
    let backend = (provider.backend)(server.endpoint.clone());
    let event = submit(provider, op, backend.as_ref())
        .recv_timeout(RESPONSE_WAIT)
        .expect("provider callback");
    let _ = server.finish();
    class_of_event(&event)
}

/// Run one case on every provider and report each one that disagrees.
fn assert_every_provider(
    case: &str,
    op: Op,
    response: impl Fn(&Provider) -> Response,
    expected: &Class,
) {
    let mismatches: Vec<String> = providers()
        .iter()
        .filter_map(|provider| {
            let actual = outcome(provider, op, &response(provider));
            (actual != *expected).then(|| format!("{}: {actual:?}", provider.name))
        })
        .collect();
    assert!(
        mismatches.is_empty(),
        "{case}: expected {expected:?} from every provider, got {mismatches:?}"
    );
}

#[test]
fn should_report_precondition_failed_when_creating_an_object_that_exists_on_every_provider() {
    // Arrange
    let response = |provider: &Provider| provider.create_conflict.clone();

    // Act
    // Assert
    assert_every_provider(
        "create-only PUT on an existing object",
        Op::CreateOnly,
        response,
        &Class::PreconditionFailed,
    );
}

#[test]
fn should_report_precondition_failed_when_updating_a_stale_version_on_every_provider() {
    // Arrange
    let response = |provider: &Provider| provider.stale_version.clone();

    // Act
    // Assert
    assert_every_provider(
        "If-Match PUT on a newer version",
        Op::PutIfMatch,
        response,
        &Class::PreconditionFailed,
    );
}

#[test]
fn should_report_precondition_failed_when_updating_a_missing_object_on_every_provider() {
    // Arrange: a conditional update of a missing object did not commit,
    // exactly like a stale version.
    let response = |provider: &Provider| provider.missing_object.clone();

    // Act
    // Assert
    assert_every_provider(
        "If-Match PUT on a missing object",
        Op::PutIfMatch,
        response,
        &Class::PreconditionFailed,
    );
}

#[test]
fn should_report_not_found_when_updating_in_a_missing_container_on_every_provider() {
    // Arrange: a missing bucket or container is a configuration fault, not a
    // lost race.
    let response = |provider: &Provider| provider.missing_container.clone();

    // Act
    // Assert
    assert_every_provider(
        "If-Match PUT in a missing container",
        Op::PutIfMatch,
        response,
        &Class::NotFound,
    );
}

#[test]
fn should_report_not_found_when_reading_a_missing_object_on_every_provider() {
    // Arrange
    let response = |_: &Provider| (404, Vec::new(), String::new());

    // Act
    // Assert
    assert_every_provider(
        "GET of a missing object",
        Op::Get,
        response,
        &Class::NotFound,
    );
}

#[test]
fn should_report_not_found_when_heading_a_missing_object_on_every_provider() {
    // Arrange
    let response = |_: &Provider| (404, Vec::new(), String::new());

    // Act
    // Assert
    assert_every_provider(
        "HEAD of a missing object",
        Op::Head,
        response,
        &Class::NotFound,
    );
}

#[test]
fn should_report_success_when_deleting_a_missing_object_on_every_provider() {
    // Arrange
    let response = |_: &Provider| (404, Vec::new(), String::new());

    // Act
    // Assert
    assert_every_provider(
        "DELETE of a missing object",
        Op::Delete,
        response,
        &Class::Ok,
    );
}

#[test]
fn should_report_success_when_conditionally_deleting_a_missing_object_on_every_provider() {
    // Arrange
    let response = |_: &Provider| (404, Vec::new(), String::new());

    // Act
    // Assert
    assert_every_provider(
        "If-Match DELETE of a missing object",
        Op::DeleteIfMatch,
        response,
        &Class::Ok,
    );
}

#[test]
fn should_report_precondition_failed_when_range_reading_a_stale_version_on_every_provider() {
    // Arrange
    let response = |provider: &Provider| provider.stale_version.clone();

    // Act
    // Assert
    assert_every_provider(
        "conditional range GET of a newer version",
        Op::RangeWithIdentity,
        response,
        &Class::PreconditionFailed,
    );
}

#[test]
fn should_accept_exact_range_response_on_every_provider() {
    // Arrange
    let response = |provider: &Provider| {
        let mut headers = vec![
            ("Content-Range".to_string(), "bytes 0-4/100".to_string()),
            ("ETag".to_string(), "v1".to_string()),
        ];
        if let Some(generation) = &provider.identity.generation {
            headers.push(("x-goog-generation".to_string(), generation.clone()));
        }
        (206, headers, "01234".to_string())
    };

    // Act
    // Assert
    assert_every_provider(
        "exact conditional range",
        Op::RangeWithIdentity,
        response,
        &Class::Ok,
    );
}

#[test]
fn should_accept_empty_put_response_on_every_provider() {
    // Arrange
    let providers = providers();

    for provider in &providers {
        // Act
        let result = outcome(
            provider,
            Op::PutEmpty,
            &(provider.empty_put_status, Vec::new(), String::new()),
        );

        // Assert
        assert_eq!(result, Class::Ok, "{}", provider.name);
    }
}

#[test]
fn should_report_protocol_error_when_range_response_names_another_slice_on_every_provider() {
    // Arrange: 206 for bytes 10-14 when 0-4 was asked for.
    let response = |provider: &Provider| {
        let mut headers = vec![
            ("Content-Range".to_string(), "bytes 10-14/100".to_string()),
            ("ETag".to_string(), "v1".to_string()),
        ];
        if let Some(generation) = &provider.identity.generation {
            headers.push(("x-goog-generation".to_string(), generation.clone()));
        }
        (206, headers, "01234".to_string())
    };

    // Act
    // Assert
    assert_every_provider(
        "206 naming a different slice",
        Op::RangeWithIdentity,
        response,
        &Class::Protocol,
    );
}

#[test]
fn should_report_protocol_error_when_range_request_is_answered_with_whole_object_on_every_provider()
{
    // Arrange: a 200 means the provider ignored the Range header.
    let response = |_: &Provider| (200, Vec::new(), "01234".to_string());

    // Act
    // Assert
    assert_every_provider(
        "200 answering a range GET",
        Op::RangeWithIdentity,
        response,
        &Class::Protocol,
    );
}

#[test]
fn should_return_every_page_when_listing_across_two_pages_on_every_provider() {
    // Arrange
    let providers = providers();

    for provider in &providers {
        let server = spawn_scripted_http_response_server(provider.list_pages.to_vec());
        let backend = (provider.backend)(server.endpoint.clone());
        let (tx, rx) = mpsc::channel();

        // Act
        backend.submit_list("p/", tx);
        let event = rx.recv_timeout(RESPONSE_WAIT).expect("list callback");
        let pages_served = server.finish();

        // Assert
        let CloudEvent::List { result, .. } = event else {
            panic!("{}: expected a LIST event, got {event:?}", provider.name);
        };
        let mut keys = result.unwrap_or_else(|error| panic!("{}: {error:?}", provider.name));
        keys.sort();
        assert_eq!(keys, ["p/a", "p/b"], "{}", provider.name);
        assert_eq!(
            pages_served, 2,
            "{}: both pages must be requested",
            provider.name
        );
    }
}
