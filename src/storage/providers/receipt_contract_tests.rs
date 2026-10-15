//! One write-receipt contract for every provider that can prove an upload.
//!
//! A receipt lets callers skip reading an upload back, so each provider must
//! send its integrity check, return the stored identity callers use for the
//! next conditional write, fall back to readback when the response carries no
//! checksum, and fail when the checksum names other bytes.

use super::test_support::{spawn_recording_http_server_with_status, RecordedHttpRequest};
use super::{azure, gcs};
use crate::config::S3CredentialSource;
use crate::config::{CloudProviderConfig, OciObjectStorageConfig, S3CompatibleConfig};
use crate::storage::cloud::{CloudBackend, CloudError, CloudEvent, CloudRequestOptions};
use crate::storage::StorageObjectMetadata;
use base64::Engine as _;
use std::sync::{mpsc, Arc};
use std::time::Duration;

const BODY: &[u8] = b"value";
const RESPONSE_WAIT: Duration = Duration::from_secs(10);
const WRONG_CHECKSUM: &str = "AAAAAA==";

/// One scripted HTTP response: status, headers and body.
type Response = (u16, Vec<(String, String)>, String);

/// Builds one provider backend against a test endpoint.
type BackendFn = fn(String) -> Arc<dyn CloudBackend>;

/// How one provider proves an upload, scripted for `BODY`.
struct ReceiptProvider {
    name: &'static str,
    backend: BackendFn,
    /// The integrity header the upload must carry.
    request_header: (&'static str, String),
    /// The stored identity the receipt must report.
    identity: StorageObjectMetadata,
    /// A response whose checksum is `checksum`, or none when `None`.
    response: fn(Option<&str>) -> Response,
    /// The checksum the provider reports for `BODY`.
    checksum: String,
}

fn crc32c_base64() -> String {
    base64::engine::general_purpose::STANDARD.encode(crc32c::crc32c(BODY).to_be_bytes())
}

fn md5_base64() -> String {
    use md5::Digest as _;
    base64::engine::general_purpose::STANDARD.encode(md5::Md5::digest(BODY))
}

fn credentials() -> S3CredentialSource {
    S3CredentialSource::access_key("access", "secret")
}

fn resolve(config: &CloudProviderConfig) -> Arc<dyn CloudBackend> {
    super::s3_resolver::try_resolve(config)
        .expect("resolve S3-family backend")
        .expect("S3-family provider")
}

fn s3_compatible(endpoint: String) -> Arc<dyn CloudBackend> {
    resolve(
        &S3CompatibleConfig::new("bucket", "us-east-1", endpoint, credentials())
            .with_checksum_receipts(true)
            .into(),
    )
}

fn oci(endpoint: String) -> Arc<dyn CloudBackend> {
    resolve(&CloudProviderConfig::OciObjectStorage(
        OciObjectStorageConfig::new("namespace", "bucket", "us-ashburn-1", credentials())
            .with_endpoint(endpoint)
            .with_checksum_receipts(true),
    ))
}

fn s3_response(checksum: Option<&str>) -> Response {
    let mut headers = vec![("ETag".to_string(), "\"v1\"".to_string())];
    if let Some(checksum) = checksum {
        headers.push(("x-amz-checksum-crc32c".into(), checksum.into()));
    }
    (200, headers, String::new())
}

fn azure_response(checksum: Option<&str>) -> Response {
    let mut headers = vec![("ETag".to_string(), "\"v1\"".to_string())];
    if let Some(checksum) = checksum {
        headers.push(("Content-MD5".into(), checksum.into()));
    }
    (201, headers, String::new())
}

fn gcs_json_response(checksum: Option<&str>) -> Response {
    let mut resource = serde_json::json!({
        "name": "p/object",
        "size": BODY.len().to_string(),
        "etag": "CJ6n",
        "generation": "7",
    });
    if let Some(checksum) = checksum {
        resource["crc32c"] = checksum.into();
    }
    (
        200,
        vec![("Content-Type".into(), "application/json".into())],
        resource.to_string(),
    )
}

fn gcs_xml_response(checksum: Option<&str>) -> Response {
    let mut headers = vec![
        ("ETag".to_string(), "\"v1\"".to_string()),
        ("x-goog-generation".to_string(), "7".to_string()),
    ];
    // GCS may list several hashes in one header; the CRC32C need not lead.
    let md5 = format!("md5={}", md5_base64());
    headers.push((
        "x-goog-hash".into(),
        checksum.map_or(md5.clone(), |checksum| format!("{md5},crc32c={checksum}")),
    ));
    (200, headers, String::new())
}

fn receipt_providers() -> Vec<ReceiptProvider> {
    let etag = StorageObjectMetadata {
        size: BODY.len() as u64,
        etag: "\"v1\"".into(),
        generation: None,
    };
    let generation = |etag: &str| StorageObjectMetadata {
        size: BODY.len() as u64,
        etag: etag.into(),
        generation: Some("7".into()),
    };
    vec![
        ReceiptProvider {
            name: "s3-compatible",
            backend: s3_compatible,
            request_header: ("x-amz-checksum-crc32c", crc32c_base64()),
            identity: etag.clone(),
            response: s3_response,
            checksum: crc32c_base64(),
        },
        ReceiptProvider {
            name: "oci",
            backend: oci,
            request_header: ("x-amz-checksum-crc32c", crc32c_base64()),
            identity: etag.clone(),
            response: s3_response,
            checksum: crc32c_base64(),
        },
        ReceiptProvider {
            name: "azure",
            backend: azure::contract_test_backend,
            request_header: ("Content-MD5", md5_base64()),
            identity: etag,
            response: azure_response,
            checksum: md5_base64(),
        },
        ReceiptProvider {
            name: "gcs-json",
            backend: gcs::contract_test_json_backend,
            request_header: ("x-goog-hash", format!("crc32c={}", crc32c_base64())),
            identity: generation("CJ6n"),
            response: gcs_json_response,
            checksum: crc32c_base64(),
        },
        ReceiptProvider {
            name: "gcs-xml",
            backend: gcs::contract_test_xml_backend,
            request_header: ("x-goog-hash", format!("crc32c={}", crc32c_base64())),
            identity: generation("\"v1\""),
            response: gcs_xml_response,
            checksum: crc32c_base64(),
        },
    ]
}

/// Upload `BODY` with a receipt request against one scripted response.
fn write_with_receipt(backend: BackendFn, response: Response) -> (RecordedHttpRequest, CloudEvent) {
    let (status, headers, body) = response;
    let server = spawn_recording_http_server_with_status(status, headers, body.into_bytes());
    let backend = backend(server.endpoint.clone());
    let (tx, rx) = mpsc::channel();
    backend.write_with_receipt(
        "p/object",
        BODY.to_vec(),
        CloudRequestOptions::default(),
        tx,
    );
    let event = rx.recv_timeout(RESPONSE_WAIT).expect("provider callback");
    (server.finish(), event)
}

#[test]
fn should_return_stored_identity_when_provider_confirms_checksum_on_every_provider() {
    for provider in receipt_providers() {
        // Arrange
        let response = (provider.response)(Some(&provider.checksum));

        // Act
        let (request, event) = write_with_receipt(provider.backend, response);

        // Assert
        let (name, value) = &provider.request_header;
        assert_eq!(
            request.header(name),
            Some(value.as_str()),
            "{}",
            provider.name
        );
        let CloudEvent::PutReceipt { result, .. } = event else {
            panic!("{}: expected a receipt event, got {event:?}", provider.name);
        };
        let receipt = result
            .unwrap_or_else(|error| panic!("{}: {error:?}", provider.name))
            .unwrap_or_else(|| panic!("{}: confirmed upload returned no receipt", provider.name));
        assert!(receipt.matches(BODY), "{}", provider.name);
        assert_eq!(
            receipt.metadata.size, provider.identity.size,
            "{}",
            provider.name
        );
        assert!(
            receipt.metadata.same_version(&provider.identity),
            "{}: receipt identity {:?} differs from {:?}",
            provider.name,
            receipt.metadata,
            provider.identity
        );
    }
}

#[test]
fn should_leave_readback_to_caller_when_provider_omits_checksum_on_every_provider() {
    for provider in receipt_providers() {
        // Arrange
        let response = (provider.response)(None);

        // Act
        let (_, event) = write_with_receipt(provider.backend, response);

        // Assert
        let CloudEvent::PutReceipt { result, .. } = event else {
            panic!("{}: expected a receipt event, got {event:?}", provider.name);
        };
        assert!(
            matches!(result, Ok(None)),
            "{}: an unconfirmed upload must not yield a receipt: {result:?}",
            provider.name
        );
    }
}

#[test]
fn should_reject_receipt_when_provider_checksum_names_other_bytes_on_every_provider() {
    for provider in receipt_providers() {
        // Arrange
        let response = (provider.response)(Some(WRONG_CHECKSUM));

        // Act
        let (_, event) = write_with_receipt(provider.backend, response);

        // Assert
        let CloudEvent::PutReceipt { result, .. } = event else {
            panic!("{}: expected a receipt event, got {event:?}", provider.name);
        };
        assert!(
            matches!(result, Err(CloudError::Protocol(_))),
            "{}: {result:?}",
            provider.name
        );
    }
}

#[test]
fn should_skip_checksum_receipts_when_s3_compatible_endpoint_has_not_opted_in() {
    // Arrange
    let opted_out = |endpoint: String| {
        resolve(&S3CompatibleConfig::new("bucket", "us-east-1", endpoint, credentials()).into())
    };
    let response = s3_response(Some(&crc32c_base64()));

    // Act
    let (request, event) = write_with_receipt(opted_out, response);

    // Assert
    assert_eq!(request.header("x-amz-checksum-crc32c"), None);
    assert!(
        matches!(event, CloudEvent::Put { result: Ok(()), .. }),
        "an endpoint that has not opted in keeps the plain PUT: {event:?}"
    );
}

#[test]
fn should_leave_readback_to_caller_when_confirmed_checksum_lacks_matching_identity() {
    // Arrange: each response confirms BODY's checksum but cannot name the
    // version it created, or names one of another size.
    let checksum = crc32c_base64();
    let mut wrong_size = gcs_json_response(Some(&checksum));
    wrong_size.2 = wrong_size.2.replace("\"size\":\"5\"", "\"size\":\"6\"");
    let mut no_generation = gcs_xml_response(Some(&checksum));
    no_generation
        .1
        .retain(|(name, _)| !name.eq_ignore_ascii_case("x-goog-generation"));
    let mut no_etag = azure_response(Some(&md5_base64()));
    no_etag
        .1
        .retain(|(name, _)| !name.eq_ignore_ascii_case("etag"));
    let cases: [(&str, BackendFn, Response); 3] = [
        ("gcs-json size", gcs::contract_test_json_backend, wrong_size),
        (
            "gcs-xml generation",
            gcs::contract_test_xml_backend,
            no_generation,
        ),
        ("azure etag", azure::contract_test_backend, no_etag),
    ];

    for (case, backend, response) in cases {
        // Act
        let (_, event) = write_with_receipt(backend, response);

        // Assert
        let CloudEvent::PutReceipt { result, .. } = event else {
            panic!("{case}: expected a receipt event, got {event:?}");
        };
        assert!(matches!(result, Ok(None)), "{case}: {result:?}");
    }
}
