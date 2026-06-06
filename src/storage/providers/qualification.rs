//! Peas qualification tests for real cloud provider front doors.
//!
//! These tests intentionally assume Peas is already running. Start it with:
//! `docker compose up -d peas`
//! then run `cargo test` or narrow to this module with:
//! `cargo test storage::providers::qualification -- --test-threads=1`

use super::build_cloud_storage;
use crate::engine::api::{
    CloudProviderConfig, GcsApiStyle, GcsCredentialSource, S3CredentialSource,
};
use crate::engine::{Engine, MemoryBudget, OpenOptions, TransactionMode, WriteOptions};
use crate::storage::cloud::{CloudEvent, CloudOutcome, CloudStorage, ObjectMetadata};
use std::path::PathBuf;
use std::time::Duration;

const PEAS_ENDPOINT: &str = "http://127.0.0.1:9000";
const PEAS_ACCESS_KEY: &str = "admin";
const PEAS_SECRET_KEY: &str = "easy-peasy";

#[test]
fn s3_compatible_contract_against_peas() {
    let provider = CloudProviderConfig::peas_s3("midge-peas-s3");
    run_provider_contract("s3", provider);
}

#[test]
fn minio_contract_against_peas() {
    let provider = CloudProviderConfig::Minio {
        bucket: "midge-peas-minio".to_string(),
        endpoint: PEAS_ENDPOINT.to_string(),
        credentials: S3CredentialSource::access_key(PEAS_ACCESS_KEY, PEAS_SECRET_KEY),
    };
    run_provider_contract("minio", provider);
}

#[test]
fn wasabi_contract_against_peas() {
    let provider = CloudProviderConfig::Wasabi {
        bucket: "midge-peas-wasabi".to_string(),
        region: "us-east-1".to_string(),
        endpoint: Some(PEAS_ENDPOINT.to_string()),
        credentials: S3CredentialSource::access_key(PEAS_ACCESS_KEY, PEAS_SECRET_KEY),
    };
    run_provider_contract("wasabi", provider);
}

#[test]
fn oci_s3_compatible_contract_against_peas() {
    let provider = CloudProviderConfig::OciS3Compatible {
        bucket: "midge-peas-oci".to_string(),
        namespace: "peas".to_string(),
        region: "us-east-1".to_string(),
        endpoint: Some(PEAS_ENDPOINT.to_string()),
        path_style: true,
        credentials: S3CredentialSource::access_key(PEAS_ACCESS_KEY, PEAS_SECRET_KEY),
    };
    run_provider_contract("oci", provider);
}

#[test]
fn azure_blob_contract_against_peas() {
    let provider = CloudProviderConfig::peas_azure("midge-peas-azure");
    run_provider_contract("azure", provider);
}

#[test]
fn gcs_xml_contract_against_peas() {
    let provider = CloudProviderConfig::peas_gcs("midge-peas-gcs");
    run_provider_contract("gcs", provider);
}

#[test]
fn gcs_json_bearer_config_rejects_peas_hmac_contract() {
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
    assert!(
        result.is_err(),
        "Peas should reject unsupported GCS bearer credentials"
    );
}

#[test]
fn engine_recovers_from_peas_s3_after_local_cache_loss() {
    let provider = CloudProviderConfig::peas_s3("midge-peas-engine-s3");
    ensure_peas_namespace(&provider).expect("prepare Peas S3 bucket");

    let prefix = format!("engine/{}/", uuid::Uuid::new_v4());
    let cache_path =
        std::env::temp_dir().join(format!("midge-peas-engine-{}", uuid::Uuid::new_v4()));
    let _ = std::fs::remove_dir_all(&cache_path);

    let opts = real_cloud_engine_options(cache_path.clone(), provider.clone(), prefix.clone());
    let engine = Engine::open(opts).expect("open Peas-backed engine");
    let default_handle = default_cf(&engine);

    let mut tx = engine
        .begin_tx(default_handle.id(), TransactionMode::ReadWrite)
        .expect("begin write tx");
    tx.put(
        b"engine-peas-key".to_vec(),
        b"engine-peas-value".to_vec(),
        None,
    )
    .expect("put value");
    tx.commit(WriteOptions::cloud_strict())
        .expect("cloud-strict commit");

    engine.flush_cf(&default_handle).expect("force SST upload");
    drop(engine);

    std::fs::remove_dir_all(&cache_path).expect("delete local cache");

    let reopened = Engine::open(real_cloud_engine_options(
        cache_path.clone(),
        provider,
        prefix,
    ))
    .expect("reopen from Peas");
    let reopened_cf = default_cf(&reopened);
    let read_tx = reopened
        .begin_tx(reopened_cf.id(), TransactionMode::ReadOnly)
        .expect("begin read tx");
    let value = read_tx.get(b"engine-peas-key").expect("read value");
    assert_eq!(value, Some(bytes::Bytes::from_static(b"engine-peas-value")));

    drop(reopened);
    let _ = std::fs::remove_dir_all(&cache_path);
}

fn run_provider_contract(label: &str, provider: CloudProviderConfig) {
    ensure_peas_namespace(&provider).unwrap_or_else(|error| {
        panic!("{label}: failed to prepare Peas namespace: {error}");
    });
    let backend = build_cloud_storage(&provider, "").unwrap_or_else(|error| {
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

fn real_cloud_engine_options(
    cache_path: PathBuf,
    provider: CloudProviderConfig,
    prefix: String,
) -> OpenOptions {
    OpenOptions::cloud(cache_path, provider, prefix)
        .memory_budget(MemoryBudget::Bytes(8 * 1024 * 1024))
        .build()
}

fn default_cf(engine: &Engine) -> crate::engine::ColumnFamilyHandle {
    engine
        .get_column_family("default")
        .expect("default column family")
}

fn ensure_peas_namespace(provider: &CloudProviderConfig) -> Result<(), String> {
    match provider {
        CloudProviderConfig::AwsS3 { .. } => Ok(()),
        CloudProviderConfig::S3Compatible { bucket, .. }
        | CloudProviderConfig::Minio { bucket, .. }
        | CloudProviderConfig::Wasabi { bucket, .. }
        | CloudProviderConfig::OciS3Compatible { bucket, .. } => ensure_peas_s3_bucket(bucket),
        CloudProviderConfig::Gcs { bucket, .. } => ensure_peas_gcs_bucket(bucket),
        CloudProviderConfig::AzureBlob { container, .. } => ensure_peas_azure_container(container),
    }
}

fn ensure_peas_s3_bucket(bucket: &str) -> Result<(), String> {
    signed_s3_request("PUT", &format!("/{bucket}"), b"").map(|_| ())
}

fn signed_s3_request(method: &str, path: &str, body: &[u8]) -> Result<Vec<u8>, String> {
    use hmac::{Hmac, KeyInit, Mac};
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
        .map(|(name, value)| format!("{}:{}\n", name, value))
        .collect::<String>();
    let signed_headers = headers
        .iter()
        .map(|(name, _)| name.clone())
        .collect::<Vec<_>>()
        .join(";");
    let canonical_request = format!(
        "{}\n{}\n\n{}\n{}\n{}",
        method, path, canonical_headers, signed_headers, payload_hash
    );
    let scope = format!("{date}/{region}/s3/aws4_request");
    let string_to_sign = format!(
        "AWS4-HMAC-SHA256\n{}\n{}\n{}",
        amz_date,
        scope,
        hex::encode(Sha256::digest(canonical_request.as_bytes()))
    );
    fn hmac_sha256(key: &[u8], data: &str) -> Vec<u8> {
        let mut mac = Hmac::<Sha256>::new_from_slice(key).expect("hmac key");
        mac.update(data.as_bytes());
        mac.finalize().into_bytes().to_vec()
    }
    let k_date = hmac_sha256(format!("AWS4{}", PEAS_SECRET_KEY).as_bytes(), &date);
    let k_region = hmac_sha256(&k_date, region);
    let k_service = hmac_sha256(&k_region, "s3");
    let k_signing = hmac_sha256(&k_service, "aws4_request");
    let signature = hex::encode(hmac_sha256(&k_signing, &string_to_sign));
    let authorization = format!(
        "AWS4-HMAC-SHA256 Credential={}/{}, SignedHeaders={}, Signature={}",
        PEAS_ACCESS_KEY, scope, signed_headers, signature
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

fn signed_azure_request(
    method: &str,
    path: &str,
    query: &str,
    body: &[u8],
    extra_headers: Vec<(&str, &str)>,
) -> Result<Vec<u8>, String> {
    use hmac::{Hmac, KeyInit, Mac};
    use sha2::Sha256;

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
    let header_value = |name: &str| -> String {
        headers
            .iter()
            .find(|(candidate, _)| candidate.eq_ignore_ascii_case(name))
            .map(|(_, value)| value.clone())
            .unwrap_or_default()
    };
    let content_length = if matches!(method, "GET" | "HEAD") || body.is_empty() {
        String::new()
    } else {
        body.len().to_string()
    };
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
    let canonical_headers = x_ms
        .into_iter()
        .map(|(name, value)| format!("{name}:{value}\n"))
        .collect::<String>();
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
            canonical_resource.push_str(&format!("\n{key}:{value}"));
        }
    }
    let string_to_sign = [
        method.to_string(),
        header_value("Content-Encoding"),
        header_value("Content-Language"),
        content_length,
        header_value("Content-MD5"),
        header_value("Content-Type"),
        String::new(),
        header_value("If-Modified-Since"),
        header_value("If-Match"),
        header_value("If-None-Match"),
        header_value("If-Unmodified-Since"),
        header_value("Range"),
        canonical_headers,
        canonical_resource,
    ]
    .join("\n");
    let key = base64::Engine::decode(&base64::engine::general_purpose::STANDARD, PEAS_SECRET_KEY)
        .unwrap_or_else(|_| PEAS_SECRET_KEY.as_bytes().to_vec());
    let mut mac = Hmac::<Sha256>::new_from_slice(&key).map_err(|error| error.to_string())?;
    mac.update(string_to_sign.as_bytes());
    let signature = base64::Engine::encode(
        &base64::engine::general_purpose::STANDARD,
        mac.finalize().into_bytes(),
    );
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
    backend.submit_put(key.to_string(), data, headers, tx);
    match rx.recv_timeout(Duration::from_secs(30)) {
        Ok(CloudEvent::PutComplete { result, .. }) => match result {
            CloudOutcome::Ok(()) => Ok(()),
            CloudOutcome::Err(error) => Err(error),
        },
        other => Err(format!("unexpected PUT event: {other:?}")),
    }
}

fn get(backend: &CloudStorage, key: &str) -> Result<Vec<u8>, String> {
    let (tx, rx) = std::sync::mpsc::channel();
    backend.submit_get(key.to_string(), tx);
    match rx.recv_timeout(Duration::from_secs(30)) {
        Ok(CloudEvent::GetComplete { result, .. }) => match result {
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
    backend.submit_get_range(key.to_string(), start, end, tx);
    match rx.recv_timeout(Duration::from_secs(30)) {
        Ok(CloudEvent::GetRangeComplete { result, .. }) => match result {
            CloudOutcome::Ok(data) => Ok(data),
            CloudOutcome::Err(error) => Err(error),
        },
        other => Err(format!("unexpected range event: {other:?}")),
    }
}

fn head(backend: &CloudStorage, key: &str) -> Result<ObjectMetadata, String> {
    let (tx, rx) = std::sync::mpsc::channel();
    backend.submit_head(key.to_string(), tx);
    match rx.recv_timeout(Duration::from_secs(30)) {
        Ok(CloudEvent::HeadComplete { result, .. }) => match result {
            CloudOutcome::Ok(metadata) => Ok(metadata),
            CloudOutcome::Err(error) => Err(error),
        },
        other => Err(format!("unexpected HEAD event: {other:?}")),
    }
}

fn list(backend: &CloudStorage, prefix: &str) -> Result<Vec<String>, String> {
    let (tx, rx) = std::sync::mpsc::channel();
    backend.submit_list(prefix.to_string(), tx);
    match rx.recv_timeout(Duration::from_secs(30)) {
        Ok(CloudEvent::ListComplete { result, .. }) => match result {
            CloudOutcome::Ok(keys) => Ok(keys),
            CloudOutcome::Err(error) => Err(error),
        },
        other => Err(format!("unexpected LIST event: {other:?}")),
    }
}

fn delete(backend: &CloudStorage, key: &str) -> Result<(), String> {
    let (tx, rx) = std::sync::mpsc::channel();
    backend.submit_delete(key.to_string(), tx);
    match rx.recv_timeout(Duration::from_secs(30)) {
        Ok(CloudEvent::DeleteComplete { result, .. }) => match result {
            CloudOutcome::Ok(()) => Ok(()),
            CloudOutcome::Err(error) => Err(error),
        },
        other => Err(format!("unexpected DELETE event: {other:?}")),
    }
}
