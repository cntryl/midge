//! Shared Sqrzl signing and namespace setup for provider qualification tests.

use std::fmt::Write as _;
use std::net::{SocketAddr, TcpStream};
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

pub(super) struct RealS3Settings {
    pub(super) bucket: String,
    pub(super) endpoint: String,
    pub(super) region: String,
    pub(super) access_key: String,
    pub(super) secret_key: String,
    pub(super) path_style: bool,
}

pub(super) fn configured_real_s3_settings() -> Option<RealS3Settings> {
    Some(RealS3Settings {
        bucket: std::env::var(REAL_S3_BUCKET_ENV).ok()?,
        endpoint: std::env::var(REAL_S3_ENDPOINT_ENV).ok()?,
        region: std::env::var(REAL_S3_REGION_ENV).unwrap_or_else(|_| "us-east-1".to_string()),
        access_key: std::env::var(REAL_S3_ACCESS_KEY_ENV).ok()?,
        secret_key: std::env::var(REAL_S3_SECRET_KEY_ENV).ok()?,
        path_style: std::env::var(REAL_S3_PATH_STYLE_ENV)
            .ok()
            .is_none_or(|value| {
                !matches!(value.to_ascii_lowercase().as_str(), "0" | "false" | "off")
            }),
    })
}

pub(super) fn require_sqrzl(label: &str) {
    assert!(
        sqrzl_is_available(),
        "{label}: Sqrzl qualification was explicitly selected but {SQRZL_ENDPOINT} is unreachable"
    );
}

pub(super) fn sqrzl_is_available() -> bool {
    let Ok(addr) = SQRZL_SOCKET.parse::<SocketAddr>() else {
        return false;
    };
    TcpStream::connect_timeout(&addr, Duration::from_millis(200)).is_ok()
}
pub(super) fn ensure_sqrzl_s3_bucket(bucket: &str) -> Result<(), String> {
    signed_s3_request("PUT", &format!("/{bucket}"), b"").map(|_| ())
}

pub(super) fn hmac_sha256(key: &[u8], data: &str) -> Vec<u8> {
    use hmac::{Hmac, KeyInit, Mac};
    use sha2::Sha256;

    let mut mac = Hmac::<Sha256>::new_from_slice(key).expect("hmac key");
    mac.update(data.as_bytes());
    mac.finalize().into_bytes().to_vec()
}

pub(super) fn signed_s3_request(method: &str, path: &str, body: &[u8]) -> Result<Vec<u8>, String> {
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
    // Sqrzl can return a generic 500 when a namespace already exists. This
    // setup exception must not mask a failed object or catalog publication.
    let bucket_creation =
        method == "PUT" && body.is_empty() && path.trim_start_matches('/').split('/').count() == 1;
    if status.is_success() || (bucket_creation && matches!(status.as_u16(), 409 | 500)) {
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

pub(super) fn ensure_sqrzl_gcs_bucket(bucket: &str) -> Result<(), String> {
    match signed_gcs_request("PUT", &format!("/{bucket}"), "", b"") {
        Ok(_) => Ok(()),
        // Sqrzl reports an existing namespace as a generic 500. The strict
        // fixture PUTs immediately after this call verify that it is usable.
        Err(error) if setup_error_has_conflict_status(&error) => Ok(()),
        Err(error) => Err(error),
    }
}

pub(super) fn signed_gcs_request(
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

pub(super) fn ensure_sqrzl_azure_container(container: &str) -> Result<(), String> {
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

pub(super) fn setup_error_has_conflict_status(error: &str) -> bool {
    let lower = error.to_ascii_lowercase();
    lower.contains("status 409") || lower.contains("status 500")
}

pub(super) fn azure_header_value(headers: &[(String, String)], name: &str) -> String {
    headers
        .iter()
        .find(|(candidate, _)| candidate.eq_ignore_ascii_case(name))
        .map(|(_, value)| value.clone())
        .unwrap_or_default()
}

pub(super) fn azure_canonical_headers(headers: &[(String, String)]) -> String {
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

pub(super) fn azure_canonical_resource(path: &str, query: &str) -> String {
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

pub(super) fn azure_string_to_sign(
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

pub(super) fn azure_shared_key_signature(string_to_sign: &str) -> Result<String, String> {
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

pub(super) fn signed_azure_request(
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
