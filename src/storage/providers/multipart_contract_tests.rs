//! Native multipart/resumable requests, exact body digests and bounded reads.

use crate::common::resource_budget::ResourceBudget;
use crate::common::{MidgeResult, OperationDeadline};
use crate::io::{File, FsError, FsResult, ReadFile};
use crate::storage::cloud::native_http::{NativeHttpServer, Request, Response};
use crate::storage::cloud::{CloudBackend, CloudStorage};
use crate::storage::{StorageBackend, StorageFileUpload, StoragePrecondition, StorageRequest};
use base64::Engine as _;
use md5::Digest as _;
use std::collections::BTreeMap;
use std::sync::{
    atomic::{AtomicU64, Ordering},
    mpsc, Arc, Mutex, OnceLock,
};
use std::time::Duration;

#[derive(Clone, Copy)]
enum Protocol {
    S3,
    Azure,
    GcsJson,
    GcsXml,
}

impl Protocol {
    fn backend(self, endpoint: String) -> Arc<dyn CloudBackend> {
        match self {
            Self::S3 => {
                let config = crate::config::S3CompatibleConfig::new(
                    "bucket",
                    "us-east-1",
                    endpoint,
                    crate::config::S3CredentialSource::access_key("access", "secret"),
                )
                .with_checksum_receipts(true);
                super::s3_resolver::try_resolve(&config.into())
                    .unwrap()
                    .unwrap()
            }
            Self::Azure => super::azure::contract_test_backend(endpoint),
            Self::GcsJson => super::gcs::contract_test_json_backend(endpoint),
            Self::GcsXml => super::gcs::contract_test_xml_backend(endpoint),
        }
    }
}

#[derive(Default)]
struct Uploaded {
    parts: BTreeMap<String, Vec<u8>>,
    bytes: Vec<u8>,
    requests: usize,
    committed: bool,
    aborted: bool,
}

fn header<'a>(request: &'a Request, name: &str) -> Option<&'a str> {
    request
        .headers
        .iter()
        .find(|(candidate, _)| candidate.eq_ignore_ascii_case(name))
        .map(|(_, value)| value.as_str())
}
fn encoded_crc(bytes: &[u8]) -> String {
    base64::engine::general_purpose::STANDARD.encode(crc32c::crc32c(bytes).to_be_bytes())
}
fn encoded_md5(bytes: &[u8]) -> String {
    base64::engine::general_purpose::STANDARD.encode(md5::Md5::digest(bytes))
}
fn response(status: u16, body: impl Into<Vec<u8>>, headers: Vec<(&str, String)>) -> Response {
    let mut response = Response::new(status, body);
    response.headers = headers
        .into_iter()
        .map(|(name, value)| (name.into(), value))
        .collect();
    response
}

fn serve_s3(state: &mut Uploaded, mut request: Request, corrupt: bool) -> Response {
    let url = reqwest::Url::parse(&format!("http://localhost{}", request.path)).unwrap();
    let query = url.query_pairs().collect::<BTreeMap<_, _>>();
    if query.contains_key("uploads") {
        assert_eq!(header(&request, "x-amz-checksum-algorithm"), Some("CRC32C"));
        assert_eq!(header(&request, "x-amz-checksum-type"), Some("FULL_OBJECT"));
        return response(200, b"<InitiateMultipartUploadResult><UploadId>session-1</UploadId></InitiateMultipartUploadResult>".to_vec(), vec![]);
    }
    if let Some(ordinal) = query.get("partNumber") {
        if corrupt && state.parts.is_empty() {
            request.body[0] ^= 1;
        }
        let checksum = encoded_crc(&request.body);
        if header(&request, "x-amz-checksum-crc32c") != Some(checksum.as_str()) {
            return response(
                400,
                b"<Error><Code>BadDigest</Code></Error>".to_vec(),
                vec![],
            );
        }
        state.parts.insert(ordinal.to_string(), request.body);
        return response(
            200,
            Vec::new(),
            vec![
                ("ETag", format!("\"part-{ordinal}\"")),
                ("x-amz-checksum-crc32c", checksum),
            ],
        );
    }
    assert_eq!(header(&request, "if-none-match"), Some("*"));
    let body = String::from_utf8(request.body.clone()).unwrap();
    let ordinals = super::xml::extract_xml_tag_values(&body, "PartNumber");
    for ordinal in ordinals {
        state
            .bytes
            .extend_from_slice(state.parts.get(&ordinal).unwrap());
    }
    let checksum = encoded_crc(&state.bytes);
    assert_eq!(
        header(&request, "x-amz-checksum-crc32c"),
        Some(checksum.as_str())
    );
    state.committed = true;
    response(200, format!("<CompleteMultipartUploadResult><ETag>\"object\"</ETag><ChecksumCRC32C>{checksum}</ChecksumCRC32C><ChecksumType>FULL_OBJECT</ChecksumType></CompleteMultipartUploadResult>").into_bytes(), vec![])
}

fn serve_azure(state: &mut Uploaded, mut request: Request, corrupt: bool) -> Response {
    let url = reqwest::Url::parse(&format!("http://localhost{}", request.path)).unwrap();
    let query = url.query_pairs().collect::<BTreeMap<_, _>>();
    if let Some(block) = query.get("blockid") {
        if corrupt && state.parts.is_empty() {
            request.body[0] ^= 1;
        }
        let digest = encoded_md5(&request.body);
        if header(&request, "content-md5") != Some(digest.as_str()) {
            return response(
                400,
                b"<Error><Code>Md5Mismatch</Code></Error>".to_vec(),
                vec![],
            );
        }
        state.parts.insert(block.to_string(), request.body);
        return response(201, Vec::new(), vec![("Content-MD5", digest)]);
    }
    assert_eq!(header(&request, "if-none-match"), Some("*"));
    let digest = encoded_md5(&request.body);
    assert_eq!(header(&request, "content-md5"), Some(digest.as_str()));
    let body = String::from_utf8(request.body).unwrap();
    for block in super::xml::extract_xml_tag_values(&body, "Uncommitted") {
        state
            .bytes
            .extend_from_slice(state.parts.get(&block).unwrap());
    }
    state.committed = true;
    response(
        201,
        Vec::new(),
        vec![("ETag", "\"object\"".into()), ("Content-MD5", digest)],
    )
}

fn serve_gcs(
    state: &mut Uploaded,
    request: Request,
    json: bool,
    endpoint: &str,
    corrupt: bool,
) -> Response {
    if request.method == "POST" {
        if json {
            assert!(request.path.contains("uploadType=resumable"));
            assert!(request.path.contains("ifGenerationMatch=0"));
        } else {
            assert_eq!(header(&request, "x-goog-if-generation-match"), Some("0"));
            assert_eq!(header(&request, "x-goog-resumable"), Some("start"));
        }
        return response(
            201,
            Vec::new(),
            vec![(
                "Location",
                format!("{endpoint}/session?upload_id=session-1"),
            )],
        );
    }
    let range = header(&request, "content-range")
        .unwrap()
        .strip_prefix("bytes ")
        .unwrap();
    let (span, total) = range.split_once('/').unwrap();
    let (start, end) = span.split_once('-').unwrap();
    assert_eq!(start.parse::<usize>().unwrap(), state.bytes.len());
    let end = end.parse::<usize>().unwrap();
    let total = total.parse::<usize>().unwrap();
    state.bytes.extend_from_slice(&request.body);
    if end + 1 < total {
        return response(308, Vec::new(), vec![("Range", format!("bytes=0-{end}"))]);
    }
    if corrupt {
        state.bytes[0] ^= 1;
    }
    let checksum = encoded_crc(&state.bytes);
    if header(&request, "x-goog-hash") != Some(format!("crc32c={checksum}").as_str()) {
        return response(
            400,
            b"{\"error\":{\"code\":400,\"message\":\"checksum mismatch\"}}".to_vec(),
            vec![],
        );
    }
    state.committed = true;
    if json {
        response(200, serde_json::json!({"crc32c":checksum,"size":total.to_string(),"etag":"object","generation":"7"}).to_string().into_bytes(), vec![])
    } else {
        response(
            200,
            Vec::new(),
            vec![
                ("ETag", "\"object\"".into()),
                ("x-goog-generation", "7".into()),
                ("x-goog-hash", format!("crc32c={checksum}")),
            ],
        )
    }
}

struct CountedFile {
    inner: Box<dyn File>,
    bytes: Arc<AtomicU64>,
    maximum: Arc<AtomicU64>,
}
impl ReadFile for CountedFile {
    fn read_at(&self, offset: u64, length: u64) -> FsResult<bytes::Bytes> {
        self.maximum.fetch_max(length, Ordering::Relaxed);
        let bytes = self.inner.read_at(offset, length)?;
        self.bytes.fetch_add(bytes.len() as u64, Ordering::Relaxed);
        Ok(bytes)
    }
    fn len(&self) -> FsResult<u64> {
        self.inner.len()
    }
}
impl File for CountedFile {
    fn write_at(&mut self, offset: u64, bytes: bytes::Bytes) -> FsResult<()> {
        self.inner.write_at(offset, bytes)
    }
    fn append(&mut self, bytes: bytes::Bytes) -> FsResult<u64> {
        self.inner.append(bytes)
    }
    fn sync(&mut self, durability: crate::io::Durability) -> FsResult<()> {
        self.inner.sync(durability)
    }
}

fn upload_fixture(
    protocol: Protocol,
    corrupt: bool,
    mutate_source: bool,
    size: usize,
) -> MidgeResult<()> {
    let directory = tempfile::tempdir()?;
    let bytes = vec![7; size];
    std::fs::write(directory.path().join("source"), &bytes)?;
    if mutate_source {
        let mut changed = bytes.clone();
        changed[0] ^= 1;
        std::fs::write(directory.path().join("source"), changed)?;
    }
    let uploaded = Arc::new(Mutex::new(Uploaded::default()));
    let endpoint = Arc::new(OnceLock::<String>::new());
    let observed = Arc::clone(&uploaded);
    let origin = Arc::clone(&endpoint);
    let server = NativeHttpServer::start(move |_, request| {
        let mut state = observed.lock().unwrap();
        state.requests += 1;
        if request.method == "DELETE" {
            state.aborted = true;
            return Some(response(204, Vec::new(), vec![]));
        }
        Some(match protocol {
            Protocol::S3 => serve_s3(&mut state, request, corrupt),
            Protocol::Azure => serve_azure(&mut state, request, corrupt),
            Protocol::GcsJson => {
                serve_gcs(&mut state, request, true, origin.get().unwrap(), corrupt)
            }
            Protocol::GcsXml => {
                serve_gcs(&mut state, request, false, origin.get().unwrap(), corrupt)
            }
        })
    });
    endpoint.set(server.endpoint.clone()).unwrap();
    let storage = CloudStorage::new(protocol.backend(server.endpoint.clone()), String::new());
    let root = crate::io::RealFs::new(directory.path()).map_err(FsError::into_midge)?;
    let read = Arc::new(AtomicU64::new(0));
    let maximum = Arc::new(AtomicU64::new(0));
    let file = crate::io::open_persistent_handle(
        &root,
        &crate::io::FsPath::new("source"),
        crate::io::OpenOptions::READ_ONLY,
    )
    .map_err(FsError::into_midge)?;
    let file: Arc<dyn File> = Arc::new(CountedFile {
        inner: file,
        bytes: Arc::clone(&read),
        maximum: Arc::clone(&maximum),
    });
    let budget = ResourceBudget::new(16 * 1024 * 1024);
    let reservation = Arc::new(budget.reserve(11 * 1024 * 1024, "test upload parts")?);
    let (tx, rx) = mpsc::channel();
    storage.submit_file_write_receipt_request(
        StorageRequest::new(
            "object",
            OperationDeadline::from_budget(Duration::from_secs(30)),
            Duration::from_secs(30),
        )
        .with_precondition(StoragePrecondition::IfAbsent)
        .with_reservation(reservation),
        StorageFileUpload {
            file,
            size: size as u64,
            content_crc32c: crc32c::crc32c(&bytes),
            part_size: crate::storage::FILE_UPLOAD_PART_BYTES,
        },
        tx,
    );
    let result = rx
        .recv_timeout(Duration::from_secs(30))
        .expect("multipart completion");
    let state = uploaded.lock().unwrap();
    if corrupt || mutate_source {
        assert!(result.is_err());
        assert!(!state.committed);
        if !matches!(protocol, Protocol::Azure) {
            assert!(state.aborted);
        }
    } else {
        let receipt = result
            .expect("upload succeeds")
            .expect("checksummed upload receipt");
        assert!(receipt.matches(&bytes));
        assert_eq!(state.bytes, bytes);
        assert_eq!(read.load(Ordering::Relaxed), size as u64);
        assert!(state.requests <= size.div_ceil(crate::storage::FILE_UPLOAD_PART_BYTES) + 2);
    }
    assert!(maximum.load(Ordering::Relaxed) <= crate::storage::FILE_UPLOAD_PART_BYTES as u64);
    assert!(budget.peak() <= 16 * 1024 * 1024);
    Ok(())
}

#[test]
fn should_stream_exact_bytes_with_bounded_parts_when_native_providers_publish_large_files(
) -> MidgeResult<()> {
    // Arrange / Act / Assert: each provider runs the same production file protocol.
    for protocol in [
        Protocol::S3,
        Protocol::Azure,
        Protocol::GcsJson,
        Protocol::GcsXml,
    ] {
        upload_fixture(protocol, false, false, 64 * 1024 * 1024)?;
    }
    Ok(())
}

#[test]
fn should_reject_corrupted_parts_before_commit_when_native_upload_integrity_fails(
) -> MidgeResult<()> {
    // Arrange / Act / Assert
    for protocol in [
        Protocol::S3,
        Protocol::Azure,
        Protocol::GcsJson,
        Protocol::GcsXml,
    ] {
        upload_fixture(protocol, true, false, 12 * 1024 * 1024)?;
    }
    Ok(())
}

#[test]
fn should_reject_changed_finalized_files_before_commit_when_streaming_upload_reads_them(
) -> MidgeResult<()> {
    // Arrange / Act / Assert
    for protocol in [
        Protocol::S3,
        Protocol::Azure,
        Protocol::GcsJson,
        Protocol::GcsXml,
    ] {
        upload_fixture(protocol, false, true, 12 * 1024 * 1024)?;
    }
    Ok(())
}
