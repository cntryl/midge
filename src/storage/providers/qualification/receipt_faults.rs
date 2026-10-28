//! Completion-response faults over authenticated uploads to the real emulator.

use super::{build_cloud_storage, ensure_sqrzl_namespace, get, require_sqrzl};
use crate::common::resource_budget::ResourceBudget;
use crate::config::CloudProviderConfig;
use crate::io::{FsPath, RealFs};
use crate::storage::cloud::native_http::{NativeHttpServer, Request, Response};
use crate::storage::hybrid::backend::{HybridStorage, ImmutableFileSource};
use crate::storage::StorageBackend;
use std::io::Write;
use std::sync::atomic::{AtomicUsize, Ordering};
use std::sync::Arc;
use std::time::Duration;

#[derive(Clone, Copy, Debug)]
enum Fault {
    WrongChecksum,
    MissingChecksum,
    LostCompletion,
}

#[derive(Default)]
struct Observations {
    completions: AtomicUsize,
    pinned_ranges: AtomicUsize,
    unpinned_ranges: AtomicUsize,
}

fn providers() -> Vec<CloudProviderConfig> {
    let mut s3 = CloudProviderConfig::sqrzl_s3("midge-receipt-fault-s3");
    if let CloudProviderConfig::S3Compatible(config) = &mut s3 {
        *config = config.clone().with_checksum_receipts(true);
    }
    vec![
        s3,
        CloudProviderConfig::sqrzl_azure("midge-receipt-fault-azure"),
        CloudProviderConfig::sqrzl_gcs("midge-receipt-fault-gcs"),
        CloudProviderConfig::sqrzl_gcs_json("midge-receipt-fault-gcs-json"),
    ]
}

fn is_completion(request: &Request) -> bool {
    (request.method == "POST" && request.path.contains("uploadId="))
        || (request.method == "PUT" && request.path.contains("comp=blocklist"))
        || request.headers.iter().any(|(name, value)| {
            name.eq_ignore_ascii_case("content-range")
                && value.split_once('/').is_some_and(|(span, total)| {
                    span.rsplit_once('-')
                        .and_then(|(_, end)| end.parse::<u64>().ok())
                        .zip(total.parse::<u64>().ok())
                        .is_some_and(|(end, total)| end + 1 == total)
                })
        })
}

fn forward(request: Request, runtime: &tokio::runtime::Runtime) -> Response {
    runtime.block_on(async {
        let head = request.method == "HEAD";
        let client = reqwest::Client::builder()
            .redirect(reqwest::redirect::Policy::none())
            .timeout(Duration::from_secs(30))
            .build()
            .unwrap();
        let mut outgoing = client.request(
            request.method.parse::<reqwest::Method>().unwrap(),
            format!("http://127.0.0.1:9000{}", request.path),
        );
        for (name, value) in request.headers {
            if !name.eq_ignore_ascii_case("connection") {
                outgoing = outgoing.header(name, value);
            }
        }
        let incoming = outgoing.body(request.body).send().await.unwrap();
        let status = incoming.status().as_u16();
        let headers = incoming
            .headers()
            .iter()
            .filter(|(name, _)| {
                (head || name.as_str() != "content-length")
                    && !matches!(name.as_str(), "transfer-encoding" | "connection")
            })
            .map(|(name, value)| (name.to_string(), value.to_str().unwrap().to_string()))
            .collect();
        let mut response = Response::new(status, incoming.bytes().await.unwrap().to_vec());
        response.headers = headers;
        response
    })
}

fn change_checksum(response: &mut Response, fault: Fault) {
    let missing = matches!(fault, Fault::MissingChecksum);
    response.headers.retain_mut(|(name, value)| {
        let replacement = match name.as_str() {
            "content-md5" => "AAAAAAAAAAAAAAAAAAAAAA==",
            "x-amz-checksum-crc32c" => "AAAAAA==",
            "x-goog-hash" => "crc32c=AAAAAA==",
            _ => return true,
        };
        *value = replacement.into();
        !missing
    });
    if let Ok(mut body) = serde_json::from_slice::<serde_json::Value>(&response.body) {
        if body.get("crc32c").is_some() {
            if missing {
                body.as_object_mut().unwrap().remove("crc32c");
            } else {
                body["crc32c"] = "AAAAAA==".into();
            }
            response.body = serde_json::to_vec(&body).unwrap();
        }
    } else if let Ok(body) = std::str::from_utf8(&response.body) {
        if let Some((prefix, rest)) = body.split_once("<ChecksumCRC32C>") {
            let (_, suffix) = rest.split_once("</ChecksumCRC32C>").unwrap();
            let checksum = if missing {
                ""
            } else {
                "<ChecksumCRC32C>AAAAAA==</ChecksumCRC32C>"
            };
            response.body = format!("{prefix}{checksum}{suffix}").into_bytes();
        }
    }
}

fn start_proxy(fault: Fault, observed: Arc<Observations>) -> NativeHttpServer {
    let runtime = tokio::runtime::Builder::new_current_thread()
        .enable_all()
        .build()
        .unwrap();
    NativeHttpServer::start(move |stream, request| {
        let head = request.method == "HEAD";
        let completion = is_completion(&request);
        if request.headers.iter().any(|(name, _)| name == "range") {
            let pinned = request.headers.iter().any(|(name, _)| {
                matches!(name.as_str(), "if-match" | "x-goog-if-generation-match")
            }) || reqwest::Url::parse(&format!("http://localhost{}", request.path))
                .unwrap()
                .query_pairs()
                .any(|(name, value)| name == "ifGenerationMatch" && value != "0");
            if pinned {
                observed.pinned_ranges.fetch_add(1, Ordering::AcqRel);
            } else {
                observed.unpinned_ranges.fetch_add(1, Ordering::AcqRel);
            }
        }
        let host = request
            .headers
            .iter()
            .find(|(name, _)| name == "host")
            .unwrap()
            .1
            .clone();
        let mut response = forward(request, &runtime);
        for (name, value) in &mut response.headers {
            if name == "location" {
                let url = reqwest::Url::parse(value).unwrap();
                *value = format!(
                    "http://{host}{}{}",
                    url.path(),
                    url.query()
                        .map_or_else(String::new, |query| format!("?{query}"))
                );
            }
        }
        if completion && (200..300).contains(&response.status) {
            observed.completions.fetch_add(1, Ordering::AcqRel);
            if matches!(fault, Fault::LostCompletion) {
                return None;
            }
            change_checksum(&mut response, fault);
        }
        if head {
            write!(
                stream,
                "HTTP/1.1 {} Test\r\nConnection: close\r\n",
                response.status
            )
            .unwrap();
            for (name, value) in response.headers {
                write!(stream, "{name}: {value}\r\n").unwrap();
            }
            stream.write_all(b"\r\n").unwrap();
            None
        } else {
            Some(response)
        }
    })
}

fn check_publication(provider: &CloudProviderConfig, fault: Fault) {
    require_sqrzl("streamed-receipt-fault");
    ensure_sqrzl_namespace(provider).unwrap();
    let observed = Arc::new(Observations::default());
    let proxy = start_proxy(fault, Arc::clone(&observed));
    let proxied = provider.clone().with_endpoint(&proxy.endpoint).unwrap();
    let cloud = build_cloud_storage(&proxied, "").unwrap();
    let store: Arc<dyn StorageBackend> = cloud.clone();
    let (tx, _rx) = crossbeam::channel::unbounded();
    let storage = HybridStorage::new_with_class_stores_and_event_sender(
        Arc::clone(&store),
        Arc::clone(&store),
        store,
        tx,
        Duration::from_secs(60),
    );
    let bytes = vec![7_u8; 10 * 1024 * 1024 + 1];
    let directory = tempfile::tempdir().unwrap();
    std::fs::write(directory.path().join("source"), &bytes).unwrap();
    let fs = RealFs::new(directory.path()).unwrap();
    let path = FsPath::new("source");
    let key = format!(
        "qualification/receipt-fault/{}/object.sst",
        uuid::Uuid::new_v4()
    );
    let budget = ResourceBudget::new(32 * 1024 * 1024);

    // Act: only the real provider completion response is changed or lost.
    let result = storage.publish_immutable_file(
        ImmutableFileSource {
            key: &key,
            fs: &fs,
            path: &path,
            size: bytes.len() as u64,
            checksum: crc32c::crc32c(&bytes),
        },
        &budget,
    );

    // Assert: no altered receipt can become an unverified publication proof.
    assert_eq!(
        observed.completions.load(Ordering::Acquire),
        1,
        "{fault:?}: publication error {:?}",
        result.as_ref().err()
    );
    match fault {
        Fault::WrongChecksum => assert!(result.is_err(), "wrong checksum was accepted"),
        Fault::LostCompletion => {
            assert!(
                result.is_ok(),
                "lost completion did not reconcile: {:?}",
                result.err()
            );
            assert!(observed.pinned_ranges.load(Ordering::Acquire) > 0);
        }
        Fault::MissingChecksum => {
            assert!(result.is_err() || observed.pinned_ranges.load(Ordering::Acquire) > 0);
        }
    }
    assert_eq!(observed.unpinned_ranges.load(Ordering::Acquire), 0);
    let direct = build_cloud_storage(provider, "").unwrap();
    assert_eq!(get(&direct, &key).unwrap(), bytes);
    assert_eq!(budget.used(), 0);
}

#[test]
fn should_reject_wrong_streamed_receipts_when_sqrzl_committed_exact_bytes() {
    // Arrange
    let cases = providers();
    // Act
    for provider in cases {
        // Assert: the fixture verifies production publication rejects the receipt.
        check_publication(&provider, Fault::WrongChecksum);
    }
}

#[test]
fn should_fail_closed_or_read_back_when_sqrzl_completion_omits_checksum() {
    // Arrange
    let cases = providers();
    // Act
    for provider in cases {
        // Assert: the fixture requires identity-pinned fallback for success.
        check_publication(&provider, Fault::MissingChecksum);
    }
}

#[test]
fn should_reconcile_same_streamed_object_when_sqrzl_completion_response_is_lost() {
    // Arrange
    let cases = providers();
    // Act
    for provider in cases {
        // Assert: the fixture verifies one completion and exact remote bytes.
        check_publication(&provider, Fault::LostCompletion);
    }
}
