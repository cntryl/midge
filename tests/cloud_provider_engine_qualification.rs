#![cfg(all(feature = "cloud-all", feature = "sqrzl-tests"))]

use cntryl_midge::{
    Bytes, CloudProviderConfig, CloudStorageLocation, CloudStorageTopology, ColumnFamilyHandle,
    Engine, MemoryBudget, OpenOptions, TransactionMode, WriteOptions,
};
use std::fmt::Write as _;
use std::net::{SocketAddr, TcpStream};
use std::path::PathBuf;
use std::time::Duration;

const SQRZL_ENDPOINT: &str = "http://127.0.0.1:9000";
const SQRZL_SOCKET: &str = "127.0.0.1:9000";
const SQRZL_ACCESS_KEY: &str = "admin";
const SQRZL_SECRET_KEY: &str = "easy-peasy";
const REAL_S3_BUCKET_ENV: &str = "MIDGE_REAL_S3_BUCKET";
const REAL_S3_ENDPOINT_ENV: &str = "MIDGE_REAL_S3_ENDPOINT";
const REAL_S3_REGION_ENV: &str = "MIDGE_REAL_S3_REGION";
const REAL_S3_ACCESS_KEY_ENV: &str = "MIDGE_REAL_S3_ACCESS_KEY";
const REAL_S3_SECRET_KEY_ENV: &str = "MIDGE_REAL_S3_SECRET_KEY";
const REAL_S3_PATH_STYLE_ENV: &str = "MIDGE_REAL_S3_PATH_STYLE";

#[test]
fn should_join_azure_canonical_headers_directly_to_resource() {
    // Arrange
    let headers = vec![
        (
            "x-ms-date".to_string(),
            "Sun, 10 Aug 2026 12:00:00 GMT".to_string(),
        ),
        ("x-ms-version".to_string(), "2024-11-04".to_string()),
    ];

    // Act
    let string_to_sign = azure_string_to_sign("GET", &headers, "/admin/container/blob", "", b"");

    // Assert
    assert!(string_to_sign.contains("x-ms-version:2024-11-04\n/admin/admin/container/blob"));
    assert!(!string_to_sign.contains("x-ms-version:2024-11-04\n\n/"));
}

#[test]
#[ignore = "requires Sqrzl; run the scheduled/manual Cloud Qualification workflow"]
fn should_recover_engine_from_sqrzl_s3_after_local_cache_loss() {
    // Arrange
    let provider = CloudProviderConfig::sqrzl_s3("midge-sqrzl-engine-s3");

    // Act
    engine_recovers_from_provider_after_local_cache_loss("sqrzl-engine", provider, true, true);

    // Assert
    // The helper performs the provider-backed recovery assertions.
}

#[test]
#[ignore = "requires Sqrzl; run the scheduled/manual Cloud Qualification workflow"]
fn should_recover_engine_from_sqrzl_azure_after_local_cache_loss() {
    // Arrange
    let provider = CloudProviderConfig::sqrzl_azure("midge-sqrzl-engine-azure");

    // Act
    engine_recovers_from_provider_after_local_cache_loss(
        "sqrzl-engine-azure",
        provider,
        true,
        true,
    );

    // Assert
    // The helper performs first acquisition and cache-loss recovery assertions.
}

#[test]
#[ignore = "requires Sqrzl; run the scheduled/manual Cloud Qualification workflow"]
fn should_recover_engine_from_sqrzl_gcs_json_after_local_cache_loss() {
    // Arrange
    let provider = CloudProviderConfig::sqrzl_gcs_json("midge-sqrzl-engine-gcs-json");

    // Act
    engine_recovers_from_provider_after_local_cache_loss(
        "sqrzl-engine-gcs-json",
        provider,
        true,
        true,
    );

    // Assert
    // The helper performs first acquisition and cache-loss recovery assertions.
}

#[test]
#[ignore = "requires Sqrzl; run the scheduled/manual Cloud Qualification workflow"]
fn should_route_two_location_topology_through_sqrzl() {
    // Arrange
    require_sqrzl("sqrzl-two-location");
    let shared = CloudProviderConfig::sqrzl_s3("midge-sqrzl-engine-two-data");
    let control = CloudProviderConfig::sqrzl_s3("midge-sqrzl-engine-two-control");
    let prefix = format!("engine/two/{}/", uuid::Uuid::new_v4());

    // Act
    engine_recovers_from_sqrzl_topology_after_local_cache_loss(
        "sqrzl-two-location",
        &CloudStorageTopology::new(CloudStorageLocation::new(shared.clone(), prefix.clone()))
            .with_control(CloudStorageLocation::new(control.clone(), prefix)),
        &[shared, control],
    );

    // Assert
    // The helper exercises WAL/SST data in the shared location and lease plus
    // metadata recovery through the isolated control location.
}

#[test]
#[ignore = "requires Sqrzl; run the scheduled/manual Cloud Qualification workflow"]
fn should_route_three_location_topology_through_sqrzl() {
    // Arrange
    require_sqrzl("sqrzl-three-location");
    let wal = CloudProviderConfig::sqrzl_s3("midge-sqrzl-engine-three-wal");
    let sst = CloudProviderConfig::sqrzl_s3("midge-sqrzl-engine-three-sst");
    let control = CloudProviderConfig::sqrzl_s3("midge-sqrzl-engine-three-control");
    let prefix = format!("engine/three/{}/", uuid::Uuid::new_v4());

    // Act
    engine_recovers_from_sqrzl_topology_after_local_cache_loss(
        "sqrzl-three-location",
        &CloudStorageTopology::new(CloudStorageLocation::new(wal.clone(), prefix.clone()))
            .with_sst(CloudStorageLocation::new(sst.clone(), prefix.clone()))
            .with_control(CloudStorageLocation::new(control.clone(), prefix)),
        &[wal, sst, control],
    );

    // Assert
    // The helper verifies cache-loss recovery with every object class routed
    // through its configured Sqrzl bucket.
}

#[test]
fn should_recover_engine_from_real_s3_after_local_cache_loss_if_configured() {
    // Arrange
    let Some(provider) = configured_real_s3_provider() else {
        return;
    };

    // Act
    engine_recovers_from_provider_after_local_cache_loss("real-s3-engine", provider, false, true);

    // Assert
    // The helper performs the provider-backed recovery assertions.
}

fn real_cloud_engine_options(
    cache_path: PathBuf,
    provider: CloudProviderConfig,
    database_prefix: String,
) -> OpenOptions {
    OpenOptions::cloud(
        cache_path,
        CloudStorageLocation::new(provider, database_prefix),
    )
    .memory_budget(MemoryBudget::Bytes(8 * 1024 * 1024))
    .build()
    .expect("build provider engine options")
}

fn engine_recovers_from_provider_after_local_cache_loss(
    label: &str,
    provider: CloudProviderConfig,
    prepare_namespace: bool,
    exercise_cloud_data: bool,
) {
    if prepare_namespace {
        require_sqrzl(label);
    }

    if prepare_namespace {
        ensure_sqrzl_namespace(&provider).unwrap_or_else(|error| {
            panic!("{label}: failed to prepare provider namespace: {error}");
        });
    }

    let database_prefix = format!("engine/{label}/{}/", uuid::Uuid::new_v4());
    if prepare_namespace {
        // Namespace creation is administrative setup; lease and database
        // objects must start absent so the engine exercises provider-shaped
        // first-create semantics itself.
    }
    let cache_path =
        std::env::temp_dir().join(format!("midge-provider-engine-{}", uuid::Uuid::new_v4()));
    let _ = std::fs::remove_dir_all(&cache_path);

    let opts = real_cloud_engine_options(
        cache_path.clone(),
        provider.clone(),
        database_prefix.clone(),
    );
    let mut engine = Engine::open(opts).expect("open provider-backed engine");
    if exercise_cloud_data {
        let default_handle = default_cf(&engine);
        let mut tx = engine
            .begin_tx(default_handle.id(), TransactionMode::ReadWrite)
            .expect("begin write tx");
        tx.put(
            b"engine-provider-key".to_vec(),
            b"engine-provider-value".to_vec(),
            None,
        )
        .expect("put value");
        tx.commit(WriteOptions::cloud_strict())
            .expect("cloud-strict commit");
        engine.flush_cf(&default_handle).expect("force SST upload");
    }
    engine
        .shutdown(Duration::from_secs(10))
        .expect("shutdown before provider recovery");

    std::fs::remove_dir_all(&cache_path).expect("delete local cache");

    let mut reopened = Engine::open(real_cloud_engine_options(
        cache_path.clone(),
        provider,
        database_prefix,
    ))
    .expect("reopen from provider");
    let reopened_cf = default_cf(&reopened);
    if exercise_cloud_data {
        let read_tx = reopened
            .begin_tx(reopened_cf.id(), TransactionMode::ReadOnly)
            .expect("begin read tx");
        let value = read_tx.get(b"engine-provider-key").expect("read value");
        assert_eq!(value, Some(Bytes::from_static(b"engine-provider-value")));
    }
    reopened
        .shutdown(Duration::from_secs(10))
        .expect("shutdown recovered engine");
    let _ = std::fs::remove_dir_all(&cache_path);
}

fn engine_recovers_from_sqrzl_topology_after_local_cache_loss(
    label: &str,
    topology: &CloudStorageTopology,
    providers: &[CloudProviderConfig],
) {
    for provider in providers {
        ensure_sqrzl_namespace(provider).unwrap_or_else(|error| {
            panic!("{label}: failed to prepare provider namespace: {error}");
        });
    }

    let cache_path =
        std::env::temp_dir().join(format!("midge-provider-engine-{}", uuid::Uuid::new_v4()));
    let _ = std::fs::remove_dir_all(&cache_path);
    let options = || {
        OpenOptions::cloud_multi(cache_path.clone(), (*topology).clone())
            .memory_budget(MemoryBudget::Bytes(8 * 1024 * 1024))
            .build()
            .expect("build provider engine options")
    };
    let mut engine = Engine::open(options()).expect("open provider-backed engine");
    let default_handle = default_cf(&engine);
    let mut tx = engine
        .begin_tx(default_handle.id(), TransactionMode::ReadWrite)
        .expect("begin write tx");
    tx.put(
        b"engine-provider-key".to_vec(),
        b"engine-provider-value".to_vec(),
        None,
    )
    .expect("put value");
    tx.commit(WriteOptions::cloud_strict())
        .expect("cloud-strict commit");
    engine.flush_cf(&default_handle).expect("force SST upload");
    engine
        .shutdown(Duration::from_secs(10))
        .expect("shutdown before provider recovery");

    std::fs::remove_dir_all(&cache_path).expect("delete local cache");
    let mut reopened = Engine::open(options()).expect("reopen from provider");
    let reopened_cf = default_cf(&reopened);
    let read_tx = reopened
        .begin_tx(reopened_cf.id(), TransactionMode::ReadOnly)
        .expect("begin read tx");
    assert_eq!(
        read_tx.get(b"engine-provider-key").expect("read value"),
        Some(Bytes::from_static(b"engine-provider-value"))
    );
    drop(read_tx);
    reopened
        .shutdown(Duration::from_secs(10))
        .expect("shutdown recovered engine");
    let _ = std::fs::remove_dir_all(&cache_path);
}

fn default_cf(engine: &Engine) -> ColumnFamilyHandle {
    engine
        .get_column_family("default")
        .expect("default column family")
}

fn ensure_sqrzl_namespace(provider: &CloudProviderConfig) -> Result<(), String> {
    match provider {
        CloudProviderConfig::AwsS3(_) => Ok(()),
        CloudProviderConfig::S3Compatible(config) => ensure_sqrzl_s3_bucket(config.bucket()),
        CloudProviderConfig::Gcs(config) => ensure_sqrzl_gcs_bucket(config.bucket()),
        CloudProviderConfig::AzureBlob(config) => ensure_sqrzl_azure_container(config.container()),
        CloudProviderConfig::OciObjectStorage(config) => ensure_sqrzl_s3_bucket(config.bucket()),
    }
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

    Some(
        CloudProviderConfig::s3_compatible(bucket, region, endpoint, access_key, secret_key)
            .with_path_style(path_style)
            .expect("real S3 path-style override"),
    )
}

fn require_sqrzl(label: &str) {
    assert!(
        sqrzl_is_available(),
        "{label}: Sqrzl qualification was explicitly selected but {SQRZL_ENDPOINT} is unreachable"
    );
}

fn sqrzl_is_available() -> bool {
    let Ok(addr) = SQRZL_SOCKET.parse::<SocketAddr>() else {
        return false;
    };
    TcpStream::connect_timeout(&addr, Duration::from_millis(200)).is_ok()
}
fn ensure_sqrzl_s3_bucket(bucket: &str) -> Result<(), String> {
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
    let k_date = hmac_sha256(format!("AWS4{SQRZL_SECRET_KEY}").as_bytes(), &date);
    let k_region = hmac_sha256(&k_date, region);
    let k_service = hmac_sha256(&k_region, "s3");
    let k_signing = hmac_sha256(&k_service, "aws4_request");
    let signature = hex::encode(hmac_sha256(&k_signing, &string_to_sign));
    let authorization = format!(
        "AWS4-HMAC-SHA256 Credential={SQRZL_ACCESS_KEY}/{scope}, SignedHeaders={signed_headers}, Signature={signature}"
    );
    let client = reqwest::blocking::Client::builder()
        .timeout(Duration::from_secs(10))
        .build()
        .map_err(|error| error.to_string())?;
    let response = client
        .request(
            reqwest::Method::from_bytes(method.as_bytes()).map_err(|error| error.to_string())?,
            format!("{SQRZL_ENDPOINT}{path}"),
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

fn ensure_sqrzl_gcs_bucket(bucket: &str) -> Result<(), String> {
    match signed_gcs_request("PUT", &format!("/{bucket}"), "", b"") {
        Ok(_) => Ok(()),
        // Sqrzl reports an existing namespace as a generic 500. The strict
        // fixture PUTs immediately after this call verify that it is usable.
        Err(error) if setup_error_has_conflict_status(&error) => Ok(()),
        Err(error) => Err(error),
    }
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
    let mut mac = Hmac::<Sha1>::new_from_slice(SQRZL_SECRET_KEY.as_bytes())
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
            format!("{SQRZL_ENDPOINT}{path}"),
        )
        .header("date", date)
        .header(
            "authorization",
            format!("GOOG1 {SQRZL_ACCESS_KEY}:{signature}"),
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
    if status.is_success() {
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

fn ensure_sqrzl_azure_container(container: &str) -> Result<(), String> {
    match signed_azure_request(
        "PUT",
        &format!("/{SQRZL_ACCESS_KEY}/{container}"),
        "restype=container",
        b"",
        vec![],
    ) {
        Ok(_) => Ok(()),
        // Sqrzl reports an existing namespace as a generic 500. The strict
        // fixture PUTs immediately after this call verify that it is usable.
        Err(error) if setup_error_has_conflict_status(&error) => Ok(()),
        Err(error) => Err(error),
    }
}

fn setup_error_has_conflict_status(error: &str) -> bool {
    let lower = error.to_ascii_lowercase();
    lower.contains("status 409") || lower.contains("status 500")
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
    let mut canonical_resource = format!("/{SQRZL_ACCESS_KEY}{path}");
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
    format!(
        "{}\n{}\n{}\n{}\n{}\n{}\n{}\n{}\n{}\n{}\n{}\n{}\n{}{}",
        method,
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
    )
}

fn azure_shared_key_signature(string_to_sign: &str) -> Result<String, String> {
    use hmac::{Hmac, KeyInit, Mac};
    use sha2::Sha256;

    let key = base64::Engine::decode(&base64::engine::general_purpose::STANDARD, SQRZL_SECRET_KEY)
        .unwrap_or_else(|_| SQRZL_SECRET_KEY.as_bytes().to_vec());
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
        format!("{SQRZL_ENDPOINT}{path}")
    } else {
        format!("{SQRZL_ENDPOINT}{path}?{query}")
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
            format!("SharedKey {SQRZL_ACCESS_KEY}:{signature}"),
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
    if status.is_success() {
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
