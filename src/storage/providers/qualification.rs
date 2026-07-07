//! Peas qualification tests for real cloud provider front doors.
//!
//! These tests intentionally assume Peas is already running. Start it with:
//! `docker compose up -d peas`
//! then run with the feature enabled, for example:
//! `cargo test --lib --features peas-tests storage::providers::qualification -- --test-threads=1`

use super::build_cloud_storage;
use super::{CloudProviderConfig, GcsApiStyle, GcsCredentialSource};
use crate::storage::cloud::{CloudEvent, CloudOutcome, CloudStorage, ObjectMetadata};
use std::fmt::Write as _;
use std::net::{SocketAddr, TcpStream};
use std::time::Duration;

const PEAS_ENDPOINT: &str = "http://127.0.0.1:9000";
const PEAS_SOCKET: &str = "127.0.0.1:9000";
const PEAS_ACCESS_KEY: &str = "admin";
const PEAS_SECRET_KEY: &str = "easy-peasy";
const REQUIRE_PEAS_ENV: &str = "MIDGE_REQUIRE_PEAS";
const REAL_S3_BUCKET_ENV: &str = "MIDGE_REAL_S3_BUCKET";
const REAL_S3_ENDPOINT_ENV: &str = "MIDGE_REAL_S3_ENDPOINT";
const REAL_S3_REGION_ENV: &str = "MIDGE_REAL_S3_REGION";
const REAL_S3_ACCESS_KEY_ENV: &str = "MIDGE_REAL_S3_ACCESS_KEY";
const REAL_S3_SECRET_KEY_ENV: &str = "MIDGE_REAL_S3_SECRET_KEY";
const REAL_S3_PATH_STYLE_ENV: &str = "MIDGE_REAL_S3_PATH_STYLE";

#[test]
fn should_run_s3_compatible_contract_against_peas() {
    let provider = CloudProviderConfig::peas_s3("midge-peas-s3");
    run_provider_contract("s3", &provider);
}

#[test]
fn should_run_azure_blob_contract_against_peas() {
    let provider = CloudProviderConfig::peas_azure("midge-peas-azure");
    run_provider_contract("azure", &provider);
}

#[test]
fn should_run_gcs_xml_contract_against_peas() {
    let provider = CloudProviderConfig::peas_gcs("midge-peas-gcs");
    run_provider_contract("gcs", &provider);
}

#[test]
fn should_reject_gcs_json_bearer_config_given_peas_hmac_contract() {
    // Arrange
    if !peas_available_or_skip("gcs-json-bearer") {
        return;
    }

    let provider = CloudProviderConfig::Gcs {
        bucket: "midge-peas-gcs".to_string(),
        project_id: "peas".to_string(),
        endpoint: Some(PEAS_ENDPOINT.to_string()),
        api: GcsApiStyle::Json,
        credential: GcsCredentialSource::BearerToken {
            token: "not-a-peas-token".to_string(),
        },
    };
    let backend = build_cloud_storage(&provider, "").expect("build GCS JSON provider");
    let prefix = format!("qualification/{}/", uuid::Uuid::new_v4());
    let result = put(
        &backend,
        &format!("{prefix}auth-should-fail"),
        b"body".to_vec(),
        vec![],
    );
    // Act
    // Assert
    assert!(
        result.is_err(),
        "Peas should reject unsupported GCS bearer credentials"
    );
}

#[test]
fn should_run_s3_compatible_contract_against_real_provider_if_configured() {
    // Arrange
    let Some(provider) = configured_real_s3_provider() else {
        return;
    };

    run_provider_contract_without_namespace_setup("real-s3", &provider);
    // Act
    // Assert
}

fn run_provider_contract(label: &str, provider: &CloudProviderConfig) {
    if !peas_available_or_skip(label) {
        return;
    }

    ensure_peas_namespace(provider).unwrap_or_else(|error| {
        panic!("{label}: failed to prepare Peas namespace: {error}");
    });
    run_provider_contract_body(label, provider);
}

fn run_provider_contract_without_namespace_setup(label: &str, provider: &CloudProviderConfig) {
    run_provider_contract_body(label, provider);
}

fn run_provider_contract_body(label: &str, provider: &CloudProviderConfig) {
    let backend = build_cloud_storage(provider, "").unwrap_or_else(|error| {
        panic!("{label}: failed to build provider backend: {error}");
    });
    let prefix = format!("qualification/{label}/{}/", uuid::Uuid::new_v4());
    let key = format!("{prefix}object.bin");
    let overwrite_key = format!("{prefix}overwrite.bin");
    let conditional_key = format!("{prefix}conditional.bin");
    let missing_key = format!("{prefix}missing.bin");

    put(&backend, &key, b"hello-peas".to_vec(), vec![]).expect("PUT");
    assert_eq!(get(&backend, &key).expect("GET"), b"hello-peas");

    let metadata = head(&backend, &key).expect("HEAD");
    assert_eq!(metadata.size, b"hello-peas".len() as u64);

    let listed = list(&backend, &prefix).expect("LIST");
    assert!(
        listed.iter().any(|item| item == &key),
        "LIST did not include {key}; got {listed:?}"
    );

    assert_eq!(
        range(&backend, &key, 0, Some(5)).expect("range read"),
        b"hello"
    );

    put(&backend, &overwrite_key, b"first".to_vec(), vec![]).expect("initial overwrite PUT");
    put(&backend, &overwrite_key, b"second".to_vec(), vec![]).expect("overwrite PUT");
    assert_eq!(
        get(&backend, &overwrite_key).expect("overwrite GET"),
        b"second"
    );

    put(&backend, &conditional_key, b"created".to_vec(), vec![]).expect("conditional seed");
    assert!(
        put(
            &backend,
            &conditional_key,
            b"duplicate".to_vec(),
            vec![("If-None-Match".to_string(), "*".to_string())],
        )
        .is_err(),
        "conditional create should fail when object exists"
    );
    let conditional_head = head(&backend, &conditional_key).expect("conditional HEAD");
    assert!(
        !conditional_head.etag.is_empty(),
        "HEAD should return an ETag for conditional update"
    );
    put(
        &backend,
        &conditional_key,
        b"updated".to_vec(),
        vec![("If-Match".to_string(), conditional_head.etag)],
    )
    .expect("conditional update with matching ETag");
    assert_eq!(
        get(&backend, &conditional_key).expect("conditional GET"),
        b"updated"
    );

    assert!(
        get(&backend, &missing_key).is_err(),
        "missing object GET should fail"
    );

    delete(&backend, &key).expect("DELETE");
    assert!(
        get(&backend, &key).is_err(),
        "deleted object should be missing"
    );
}

fn configured_real_s3_provider() -> Option<CloudProviderConfig> {
    let bucket = std::env::var(REAL_S3_BUCKET_ENV).ok()?;
    let endpoint = std::env::var(REAL_S3_ENDPOINT_ENV).ok()?;
    let access_key = std::env::var(REAL_S3_ACCESS_KEY_ENV).ok()?;
    let secret_key = std::env::var(REAL_S3_SECRET_KEY_ENV).ok()?;
    let region = std::env::var(REAL_S3_REGION_ENV).unwrap_or_else(|_| "us-east-1".to_string());
    let path_style = std::env::var(REAL_S3_PATH_STYLE_ENV)
        .ok()
        .is_none_or(|value| !matches!(value.to_ascii_lowercase().as_str(), "0" | "false" | "off"));

    let provider =
        CloudProviderConfig::s3_compatible(bucket, region, endpoint, access_key, secret_key);
    Some(
        provider
            .with_path_style(path_style)
            .expect("real S3 path-style override"),
    )
}

fn peas_available_or_skip(label: &str) -> bool {
    if peas_is_available() {
        return true;
    }

    assert!(
        !peas_required(),
        "{label}: Peas is required by {REQUIRE_PEAS_ENV}, but {PEAS_ENDPOINT} is unreachable"
    );

    eprintln!("{label}: skipping Peas qualification test because {PEAS_ENDPOINT} is unreachable");
    false
}

fn peas_is_available() -> bool {
    let Ok(addr) = PEAS_SOCKET.parse::<SocketAddr>() else {
        return false;
    };
    TcpStream::connect_timeout(&addr, Duration::from_millis(200)).is_ok()
}

fn peas_required() -> bool {
    peas_required_from_value(std::env::var(REQUIRE_PEAS_ENV).ok().as_deref())
}

fn peas_required_from_value(value: Option<&str>) -> bool {
    value.is_some_and(|value| {
        matches!(
            value.to_ascii_lowercase().as_str(),
            "1" | "true" | "yes" | "on"
        )
    })
}

fn ensure_peas_namespace(provider: &CloudProviderConfig) -> Result<(), String> {
    match provider {
        CloudProviderConfig::AwsS3 { .. } => Ok(()),
        CloudProviderConfig::S3Compatible { bucket, .. } => ensure_peas_s3_bucket(bucket),
        CloudProviderConfig::Gcs { bucket, .. } => ensure_peas_gcs_bucket(bucket),
        CloudProviderConfig::AzureBlob { container, .. } => ensure_peas_azure_container(container),
    }
}

fn ensure_peas_s3_bucket(bucket: &str) -> Result<(), String> {
    signed_s3_request("PUT", &format!("/{bucket}"), b"").map(|_| ())
}

fn hmac_sha256(key: &[u8], data: &str) -> Vec<u8> {
    use hmac::{Hmac, KeyInit, Mac};
    use sha2::Sha256;

    let mut mac = Hmac::<Sha256>::new_from_slice(key).expect("hmac key");
    mac.update(data.as_bytes());
    mac.finalize().into_bytes().to_vec()
}

fn signed_s3_request(method: &str, path: &str, body: &[u8]) -> Result<Vec<u8>, String> {
    use sha2::{Digest, Sha256};

    let host = "127.0.0.1:9000";
    let region = "us-east-1";
    let now = chrono::Utc::now();
    let amz_date = now.format("%Y%m%dT%H%M%SZ").to_string();
    let date = now.format("%Y%m%d").to_string();
    let payload_hash = hex::encode(Sha256::digest(body));
    let mut headers = [
        ("host".to_string(), host.to_string()),
        ("x-amz-content-sha256".to_string(), payload_hash.clone()),
        ("x-amz-date".to_string(), amz_date.clone()),
    ];
    headers.sort_by(|left, right| left.0.cmp(&right.0));
    let canonical_headers = headers
        .iter()
        .fold(String::new(), |mut acc, (name, value)| {
            writeln!(&mut acc, "{name}:{value}").expect("write canonical header");
            acc
        });
    let signed_headers = headers
        .iter()
        .map(|(name, _)| name.clone())
        .collect::<Vec<_>>()
        .join(";");
    let canonical_request =
        format!("{method}\n{path}\n\n{canonical_headers}\n{signed_headers}\n{payload_hash}");
    let scope = format!("{date}/{region}/s3/aws4_request");
    let string_to_sign = format!(
        "AWS4-HMAC-SHA256\n{}\n{}\n{}",
        amz_date,
        scope,
        hex::encode(Sha256::digest(canonical_request.as_bytes()))
    );
    let k_date = hmac_sha256(format!("AWS4{PEAS_SECRET_KEY}").as_bytes(), &date);
    let k_region = hmac_sha256(&k_date, region);
    let k_service = hmac_sha256(&k_region, "s3");
    let k_signing = hmac_sha256(&k_service, "aws4_request");
    let signature = hex::encode(hmac_sha256(&k_signing, &string_to_sign));
    let authorization = format!(
        "AWS4-HMAC-SHA256 Credential={PEAS_ACCESS_KEY}/{scope}, SignedHeaders={signed_headers}, Signature={signature}"
    );
    let client = reqwest::blocking::Client::builder()
        .timeout(Duration::from_secs(10))
        .build()
        .map_err(|error| error.to_string())?;
    let response = client
        .request(
            reqwest::Method::from_bytes(method.as_bytes()).map_err(|error| error.to_string())?,
            format!("{PEAS_ENDPOINT}{path}"),
        )
        .header("host", host)
        .header("x-amz-content-sha256", payload_hash)
        .header("x-amz-date", amz_date)
        .header("authorization", authorization)
        .body(body.to_vec())
        .send()
        .map_err(|error| error.to_string())?;
    let status = response.status();
    let response_body = response
        .bytes()
        .map_err(|error| error.to_string())?
        .to_vec();
    if status.is_success() || status.as_u16() == 409 || status.as_u16() == 500 {
        Ok(response_body)
    } else {
        Err(format!(
            "S3 setup request {} {} failed with status {}: {}",
            method,
            path,
            status,
            String::from_utf8_lossy(&response_body)
        ))
    }
}

fn ensure_peas_gcs_bucket(bucket: &str) -> Result<(), String> {
    signed_gcs_request("PUT", &format!("/{bucket}"), "", b"").map(|_| ())
}

fn signed_gcs_request(
    method: &str,
    path: &str,
    content_type: &str,
    body: &[u8],
) -> Result<Vec<u8>, String> {
    use hmac::{Hmac, KeyInit, Mac};
    use sha1::Sha1;

    let date = chrono::Utc::now()
        .format("%a, %d %b %Y %H:%M:%S GMT")
        .to_string();
    let string_to_sign = format!("{method}\n\n{content_type}\n{date}\n{path}");
    let mut mac = Hmac::<Sha1>::new_from_slice(PEAS_SECRET_KEY.as_bytes())
        .map_err(|error| error.to_string())?;
    mac.update(string_to_sign.as_bytes());
    let signature = base64::Engine::encode(
        &base64::engine::general_purpose::STANDARD,
        mac.finalize().into_bytes(),
    );
    let mut request = reqwest::blocking::Client::builder()
        .timeout(Duration::from_secs(10))
        .build()
        .map_err(|error| error.to_string())?
        .request(
            reqwest::Method::from_bytes(method.as_bytes()).map_err(|error| error.to_string())?,
            format!("{PEAS_ENDPOINT}{path}"),
        )
        .header("date", date)
        .header(
            "authorization",
            format!("GOOG1 {PEAS_ACCESS_KEY}:{signature}"),
        )
        .body(body.to_vec());
    if !content_type.is_empty() {
        request = request.header("content-type", content_type);
    }
    let response = request.send().map_err(|error| error.to_string())?;
    let status = response.status();
    let response_body = response
        .bytes()
        .map_err(|error| error.to_string())?
        .to_vec();
    if status.is_success() || status.as_u16() == 409 || status.as_u16() == 500 {
        Ok(response_body)
    } else {
        Err(format!(
            "GCS setup request {} {} failed with status {}: {}",
            method,
            path,
            status,
            String::from_utf8_lossy(&response_body)
        ))
    }
}

fn ensure_peas_azure_container(container: &str) -> Result<(), String> {
    signed_azure_request(
        "PUT",
        &format!("/{PEAS_ACCESS_KEY}/{container}"),
        "restype=container",
        b"",
        vec![],
    )
    .map(|_| ())
}

fn azure_header_value(headers: &[(String, String)], name: &str) -> String {
    headers
        .iter()
        .find(|(candidate, _)| candidate.eq_ignore_ascii_case(name))
        .map(|(_, value)| value.clone())
        .unwrap_or_default()
}

fn azure_canonical_headers(headers: &[(String, String)]) -> String {
    let mut x_ms = headers
        .iter()
        .filter(|(name, _)| name.to_ascii_lowercase().starts_with("x-ms-"))
        .map(|(name, value)| {
            (
                name.to_ascii_lowercase(),
                value.split_whitespace().collect::<Vec<_>>().join(" "),
            )
        })
        .collect::<Vec<_>>();
    x_ms.sort_by(|left, right| left.0.cmp(&right.0));
    x_ms.into_iter()
        .fold(String::new(), |mut acc, (name, value)| {
            writeln!(&mut acc, "{name}:{value}").expect("write canonical header");
            acc
        })
}

fn azure_canonical_resource(path: &str, query: &str) -> String {
    let mut canonical_resource = format!("/{PEAS_ACCESS_KEY}{path}");
    if !query.is_empty() {
        let mut query_pairs = query
            .split('&')
            .map(|pair| {
                let (key, value) = pair.split_once('=').unwrap_or((pair, ""));
                (key.to_ascii_lowercase(), value.to_string())
            })
            .collect::<Vec<_>>();
        query_pairs.sort();
        for (key, value) in query_pairs {
            write!(&mut canonical_resource, "\n{key}:{value}")
                .expect("write canonical resource query");
        }
    }
    canonical_resource
}

fn azure_string_to_sign(
    method: &str,
    headers: &[(String, String)],
    path: &str,
    query: &str,
    body: &[u8],
) -> String {
    let content_length = if matches!(method, "GET" | "HEAD") || body.is_empty() {
        String::new()
    } else {
        body.len().to_string()
    };
    [
        method.to_string(),
        azure_header_value(headers, "Content-Encoding"),
        azure_header_value(headers, "Content-Language"),
        content_length,
        azure_header_value(headers, "Content-MD5"),
        azure_header_value(headers, "Content-Type"),
        String::new(),
        azure_header_value(headers, "If-Modified-Since"),
        azure_header_value(headers, "If-Match"),
        azure_header_value(headers, "If-None-Match"),
        azure_header_value(headers, "If-Unmodified-Since"),
        azure_header_value(headers, "Range"),
        azure_canonical_headers(headers),
        azure_canonical_resource(path, query),
    ]
    .join("\n")
}

fn azure_shared_key_signature(string_to_sign: &str) -> Result<String, String> {
    use hmac::{Hmac, KeyInit, Mac};
    use sha2::Sha256;

    let key = base64::Engine::decode(&base64::engine::general_purpose::STANDARD, PEAS_SECRET_KEY)
        .unwrap_or_else(|_| PEAS_SECRET_KEY.as_bytes().to_vec());
    let mut mac = Hmac::<Sha256>::new_from_slice(&key).map_err(|error| error.to_string())?;
    mac.update(string_to_sign.as_bytes());
    Ok(base64::Engine::encode(
        &base64::engine::general_purpose::STANDARD,
        mac.finalize().into_bytes(),
    ))
}

fn signed_azure_request(
    method: &str,
    path: &str,
    query: &str,
    body: &[u8],
    extra_headers: Vec<(&str, &str)>,
) -> Result<Vec<u8>, String> {
    let date = chrono::Utc::now()
        .format("%a, %d %b %Y %H:%M:%S GMT")
        .to_string();
    let mut headers = vec![
        ("x-ms-date".to_string(), date),
        ("x-ms-version".to_string(), "2024-11-04".to_string()),
    ];
    headers.extend(
        extra_headers
            .into_iter()
            .map(|(name, value)| (name.to_string(), value.to_string())),
    );
    let string_to_sign = azure_string_to_sign(method, &headers, path, query, body);
    let signature = azure_shared_key_signature(&string_to_sign)?;
    let url = if query.is_empty() {
        format!("{PEAS_ENDPOINT}{path}")
    } else {
        format!("{PEAS_ENDPOINT}{path}?{query}")
    };
    let client = reqwest::blocking::Client::builder()
        .timeout(Duration::from_secs(10))
        .build()
        .map_err(|error| error.to_string())?;
    let mut request = client
        .request(
            reqwest::Method::from_bytes(method.as_bytes()).map_err(|error| error.to_string())?,
            url,
        )
        .header(
            "authorization",
            format!("SharedKey {PEAS_ACCESS_KEY}:{signature}"),
        )
        .body(body.to_vec());
    for (name, value) in headers {
        request = request.header(name, value);
    }
    let response = request.send().map_err(|error| error.to_string())?;
    let status = response.status();
    let response_body = response
        .bytes()
        .map_err(|error| error.to_string())?
        .to_vec();
    if status.is_success() || status.as_u16() == 409 || status.as_u16() == 500 {
        Ok(response_body)
    } else {
        Err(format!(
            "Azure setup request {} {} failed with status {}: {}",
            method,
            path,
            status,
            String::from_utf8_lossy(&response_body)
        ))
    }
}

fn put(
    backend: &CloudStorage,
    key: &str,
    data: Vec<u8>,
    headers: Vec<(String, String)>,
) -> Result<(), String> {
    let (tx, rx) = std::sync::mpsc::channel();
    backend.submit_put(key, data, headers, tx);
    match rx.recv_timeout(Duration::from_secs(30)) {
        Ok(CloudEvent::Put { result, .. }) => match result {
            CloudOutcome::Ok(()) => Ok(()),
            CloudOutcome::Err(error) => Err(error),
        },
        other => Err(format!("unexpected PUT event: {other:?}")),
    }
}

fn get(backend: &CloudStorage, key: &str) -> Result<Vec<u8>, String> {
    let (tx, rx) = std::sync::mpsc::channel();
    backend.submit_get(key, tx);
    match rx.recv_timeout(Duration::from_secs(30)) {
        Ok(CloudEvent::Get { result, .. }) => match result {
            CloudOutcome::Ok(data) => Ok(data),
            CloudOutcome::Err(error) => Err(error),
        },
        other => Err(format!("unexpected GET event: {other:?}")),
    }
}

fn range(
    backend: &CloudStorage,
    key: &str,
    start: u64,
    end: Option<u64>,
) -> Result<Vec<u8>, String> {
    let (tx, rx) = std::sync::mpsc::channel();
    backend.submit_get_range(key, start, end, tx);
    match rx.recv_timeout(Duration::from_secs(30)) {
        Ok(CloudEvent::GetRange { result, .. }) => match result {
            CloudOutcome::Ok(data) => Ok(data),
            CloudOutcome::Err(error) => Err(error),
        },
        other => Err(format!("unexpected range event: {other:?}")),
    }
}

fn head(backend: &CloudStorage, key: &str) -> Result<ObjectMetadata, String> {
    let (tx, rx) = std::sync::mpsc::channel();
    backend.submit_head(key, tx);
    match rx.recv_timeout(Duration::from_secs(30)) {
        Ok(CloudEvent::Head { result, .. }) => match result {
            CloudOutcome::Ok(metadata) => Ok(metadata),
            CloudOutcome::Err(error) => Err(error),
        },
        other => Err(format!("unexpected HEAD event: {other:?}")),
    }
}

fn list(backend: &CloudStorage, prefix: &str) -> Result<Vec<String>, String> {
    let (tx, rx) = std::sync::mpsc::channel();
    backend.submit_list(prefix, tx);
    match rx.recv_timeout(Duration::from_secs(30)) {
        Ok(CloudEvent::List { result, .. }) => match result {
            CloudOutcome::Ok(keys) => Ok(keys),
            CloudOutcome::Err(error) => Err(error),
        },
        other => Err(format!("unexpected LIST event: {other:?}")),
    }
}

fn delete(backend: &CloudStorage, key: &str) -> Result<(), String> {
    let (tx, rx) = std::sync::mpsc::channel();
    backend.submit_delete(key, tx);
    match rx.recv_timeout(Duration::from_secs(30)) {
        Ok(CloudEvent::Delete { result, .. }) => match result {
            CloudOutcome::Ok(()) => Ok(()),
            CloudOutcome::Err(error) => Err(error),
        },
        other => Err(format!("unexpected DELETE event: {other:?}")),
    }
}

#[cfg(test)]
mod tests {
    use super::peas_required_from_value;

    #[test]
    fn should_not_require_peas_when_env_is_unset_or_false() {
        // Arrange
        // Act
        // Assert
        assert!(!peas_required_from_value(None));
        assert!(!peas_required_from_value(Some("0")));
        assert!(!peas_required_from_value(Some("false")));
        assert!(!peas_required_from_value(Some("off")));
    }

    #[test]
    fn should_require_peas_for_truthy_env_values() {
        // Arrange
        // Act
        // Assert
        assert!(peas_required_from_value(Some("1")));
        assert!(peas_required_from_value(Some("true")));
        assert!(peas_required_from_value(Some("YES")));
        assert!(peas_required_from_value(Some("on")));
    }
}
