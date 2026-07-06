#![cfg(all(feature = "cloud-common", feature = "peas-tests"))]

use cntryl_midge::{
    Bytes, CloudProviderConfig, ColumnFamilyHandle, Engine, MemoryBudget, OpenOptions,
    TransactionMode, WriteOptions,
};
use std::fmt::Write as _;
use std::net::{SocketAddr, TcpStream};
use std::path::PathBuf;
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
fn should_recover_engine_from_peas_s3_after_local_cache_loss() {
    // Arrange
    let provider = CloudProviderConfig::peas_s3("midge-peas-engine-s3");

    // Act
    engine_recovers_from_provider_after_local_cache_loss("peas-engine", provider, true);

    // Assert
    // The helper performs the provider-backed recovery assertions.
}

#[test]
fn should_recover_engine_from_real_s3_after_local_cache_loss_if_configured() {
    // Arrange
    let Some(provider) = configured_real_s3_provider() else {
        return;
    };

    // Act
    engine_recovers_from_provider_after_local_cache_loss("real-s3-engine", provider, false);

    // Assert
    // The helper performs the provider-backed recovery assertions.
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

fn engine_recovers_from_provider_after_local_cache_loss(
    label: &str,
    provider: CloudProviderConfig,
    prepare_namespace: bool,
) {
    if prepare_namespace && !peas_available_or_skip(label) {
        return;
    }

    if prepare_namespace {
        ensure_peas_s3_bucket(provider_bucket(&provider)).unwrap_or_else(|error| {
            panic!("{label}: failed to prepare provider namespace: {error}");
        });
    }

    let prefix = format!("engine/{label}/{}/", uuid::Uuid::new_v4());
    let cache_path =
        std::env::temp_dir().join(format!("midge-provider-engine-{}", uuid::Uuid::new_v4()));
    let _ = std::fs::remove_dir_all(&cache_path);

    let opts = real_cloud_engine_options(cache_path.clone(), provider.clone(), prefix.clone());
    let engine = Engine::open(opts).expect("open provider-backed engine");
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
    drop(engine);

    std::fs::remove_dir_all(&cache_path).expect("delete local cache");

    let reopened = Engine::open(real_cloud_engine_options(
        cache_path.clone(),
        provider,
        prefix,
    ))
    .expect("reopen from provider");
    let reopened_cf = default_cf(&reopened);
    let read_tx = reopened
        .begin_tx(reopened_cf.id(), TransactionMode::ReadOnly)
        .expect("begin read tx");
    let value = read_tx.get(b"engine-provider-key").expect("read value");
    assert_eq!(value, Some(Bytes::from_static(b"engine-provider-value")));

    drop(reopened);
    let _ = std::fs::remove_dir_all(&cache_path);
}

fn default_cf(engine: &Engine) -> ColumnFamilyHandle {
    engine
        .get_column_family("default")
        .expect("default column family")
}

fn provider_bucket(provider: &CloudProviderConfig) -> &str {
    match provider {
        CloudProviderConfig::S3Compatible { bucket, .. } => bucket,
        _ => panic!("engine qualification currently prepares S3-compatible Peas buckets only"),
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
    std::env::var(REQUIRE_PEAS_ENV)
        .ok()
        .as_deref()
        .is_some_and(|value| {
            matches!(
                value.to_ascii_lowercase().as_str(),
                "1" | "true" | "yes" | "on"
            )
        })
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
