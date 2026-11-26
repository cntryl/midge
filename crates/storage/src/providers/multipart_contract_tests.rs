//! Native multipart/resumable requests, exact body digests and bounded reads.

use crate::cloud::native_http::{NativeHttpServer, Request, Response};
use crate::cloud::{CloudBackend, CloudStorage};
use crate::hybrid::HybridStorage;
use crate::{StorageBackend, StorageFileUpload, StoragePrecondition, StorageRequest};
use base64::Engine as _;
use cntryl_midge_core::common::resource_budget::ResourceBudget;
use cntryl_midge_core::common::{MidgeError, MidgeResult, OperationDeadline};
use cntryl_midge_core::io::{File, FsError};
use md5::Digest as _;
use std::collections::BTreeMap;
use std::sync::{mpsc, Arc, Mutex, OnceLock};
use std::time::Duration;

#[derive(Debug, Clone, Copy)]
pub enum Protocol {
    S3,
    Azure,
    GcsJson,
    GcsXml,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum UploadFault {
    None,
    CorruptPart,
    MutatedSource,
}

#[derive(Debug, Clone, Copy)]
pub enum UploadPath {
    Provider,
    Hybrid,
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
    range_gets: usize,
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
    request: &Request,
    json: bool,
    endpoint: &str,
    corrupt: bool,
) -> Response {
    if request.method == "POST" {
        if json {
            assert!(request.path.contains("uploadType=resumable"));
            assert!(request.path.contains("ifGenerationMatch=0"));
        } else {
            assert_eq!(header(request, "x-goog-if-generation-match"), Some("0"));
            assert_eq!(header(request, "x-goog-resumable"), Some("start"));
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
    let range = header(request, "content-range")
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
    if header(request, "x-goog-hash") != Some(format!("crc32c={checksum}").as_str()) {
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

fn perform_upload(
    storage: &Arc<dyn StorageBackend>,
    directory: &std::path::Path,
    size: u64,
    checksum: u32,
    expected_bytes: &[u8],
    path: UploadPath,
    budget: &ResourceBudget,
) -> MidgeResult<(MidgeResult<()>, Option<u64>, Option<u64>)> {
    if matches!(path, UploadPath::Hybrid) {
        let fs = cntryl_midge_core::io::RealFs::new(directory).map_err(FsError::into_midge)?;
        let hybrid = HybridStorage::with_policy(
            Arc::clone(storage),
            crate::hybrid::policy::StorageBudgetPolicy::default(),
        );
        let result = hybrid
            .publish_immutable_file(
                crate::hybrid::backend::ImmutableFileSource {
                    key: "object",
                    fs: &fs,
                    path: &cntryl_midge_core::io::FsPath::new("source"),
                    size,
                    checksum,
                },
                budget,
            )
            .map(drop);
        return Ok((result, None, None));
    }

    let root = cntryl_midge_core::io::RealFs::new(directory).map_err(FsError::into_midge)?;
    let file = cntryl_midge_core::io::open_persistent_handle(
        &root,
        &cntryl_midge_core::io::FsPath::new("source"),
        cntryl_midge_core::io::OpenOptions::READ_ONLY,
    )
    .map_err(FsError::into_midge)?;
    let file: Arc<dyn File> = Arc::from(file);
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
            size,
            content_crc32c: checksum,
            part_size: crate::FILE_UPLOAD_PART_BYTES,
        },
        tx,
    );
    let provider_result = rx
        .recv_timeout(Duration::from_secs(30))
        .expect("multipart completion");
    let result = match provider_result {
        Ok(Some(receipt)) => {
            assert!(receipt.matches(expected_bytes));
            Ok(())
        }
        Ok(None) => Err(MidgeError::Corruption(
            "multipart provider omitted the checksum receipt".into(),
        )),
        Err(error) => Err(MidgeError::Internal(error.to_string())),
    };
    Ok((result, None, None))
}

fn assert_upload_workspace_bounds(
    maximum_read: Option<u64>,
    through_hybrid: bool,
    budget: &ResourceBudget,
    storage: &dyn StorageBackend,
) {
    if let Some(maximum_read) = maximum_read {
        assert!(maximum_read <= crate::FILE_UPLOAD_PART_BYTES as u64);
    }
    if through_hybrid {
        let part_bounded_workspace = crate::FILE_UPLOAD_PART_BYTES
            .saturating_mul(1 + storage.write_payload_copies())
            .saturating_add(1024 * 1024);
        assert!(
            budget.peak() <= part_bounded_workspace,
            "streaming upload peak {} exceeded the part-derived budget {part_bounded_workspace}",
            budget.peak()
        );
    }
    assert!(budget.peak() <= 16 * 1024 * 1024);
}

pub fn upload_fixture(
    protocol: Protocol,
    fault: UploadFault,
    path: UploadPath,
    source: impl FnOnce(&std::path::Path) -> MidgeResult<(Vec<u8>, u64, u32)>,
) -> MidgeResult<()> {
    let directory = std::env::temp_dir().join(format!("midge-multipart-{}", uuid::Uuid::new_v4()));
    std::fs::create_dir_all(&directory)?;
    let (bytes, finalized_size, finalized_checksum) = source(directory.as_path())?;
    if fault == UploadFault::MutatedSource {
        let mut changed = bytes.clone();
        changed[0] ^= 1;
        std::fs::write(directory.as_path().join("source"), changed)?;
    }
    let uploaded = Arc::new(Mutex::new(Uploaded::default()));
    let endpoint = Arc::new(OnceLock::<String>::new());
    let observed = Arc::clone(&uploaded);
    let origin = Arc::clone(&endpoint);
    let server = NativeHttpServer::start(move |_, request| {
        let mut state = observed.lock().unwrap();
        state.requests += 1;
        if request.method == "GET" && header(&request, "range").is_some() {
            state.range_gets += 1;
            return Some(response(500, Vec::new(), vec![]));
        }
        if request.method == "HEAD" || request.method == "GET" {
            // HybridStorage probes the immutable key before upload. These
            // fixtures start with an absent object; successful publication
            // must rely on the returned receipt instead of a range readback.
            return Some(response(404, Vec::new(), vec![]));
        }
        if request.method == "DELETE" {
            state.aborted = true;
            return Some(response(204, Vec::new(), vec![]));
        }
        Some(match protocol {
            Protocol::S3 => serve_s3(&mut state, request, fault == UploadFault::CorruptPart),
            Protocol::Azure => serve_azure(&mut state, request, fault == UploadFault::CorruptPart),
            Protocol::GcsJson => serve_gcs(
                &mut state,
                &request,
                true,
                origin.get().unwrap(),
                fault == UploadFault::CorruptPart,
            ),
            Protocol::GcsXml => serve_gcs(
                &mut state,
                &request,
                false,
                origin.get().unwrap(),
                fault == UploadFault::CorruptPart,
            ),
        })
    });
    endpoint.set(server.endpoint.clone()).unwrap();
    let storage: Arc<dyn StorageBackend> = Arc::new(CloudStorage::new(
        protocol.backend(server.endpoint.clone()),
        String::new(),
    ));
    let budget = ResourceBudget::new(16 * 1024 * 1024);
    let through_hybrid = matches!(path, UploadPath::Hybrid);
    let (result, bytes_read, maximum_read) = perform_upload(
        &storage,
        directory.as_path(),
        finalized_size,
        finalized_checksum,
        &bytes,
        path,
        &budget,
    )?;
    if through_hybrid {
        assert_eq!(budget.used(), 0);
    }
    let state = uploaded.lock().unwrap();
    if fault == UploadFault::None {
        assert!(result.is_ok(), "upload failed: {result:?}");
        assert_eq!(state.bytes, bytes);
        if let Some(bytes_read) = bytes_read {
            assert_eq!(bytes_read, finalized_size);
        }
        assert_eq!(state.range_gets, 0, "publication must not read back ranges");
        let request_overhead = if through_hybrid { 3 } else { 2 };
        assert!(
            state.requests
                <= usize::try_from(finalized_size)
                    .expect("finalized upload size fits in usize")
                    .div_ceil(crate::FILE_UPLOAD_PART_BYTES)
                    + request_overhead,
            "unexpected provider request count: {}",
            state.requests
        );
    } else {
        assert!(result.is_err());
        assert!(!state.committed);
        if !matches!(protocol, Protocol::Azure) {
            assert!(state.aborted);
        }
    }
    assert_upload_workspace_bounds(maximum_read, through_hybrid, &budget, storage.as_ref());
    let _ = std::fs::remove_dir_all(&directory);
    Ok(())
}

#[test]
fn should_stream_exact_bytes_with_bounded_parts_when_native_providers_publish_large_files(
) -> MidgeResult<()> {
    // Arrange: each native dialect uses the same real-file upload proof.
    for protocol in [
        Protocol::S3,
        Protocol::Azure,
        Protocol::GcsJson,
        Protocol::GcsXml,
    ] {
        // Act: run the production upload with bounded native parts.
        // Assert: the fixture checks exact committed bytes and peak admission.
        upload_fixture(
            protocol,
            UploadFault::None,
            UploadPath::Hybrid,
            |directory| {
                let bytes = vec![7; 64 * 1024 * 1024];
                std::fs::write(directory.join("source"), &bytes)?;
                Ok((bytes.clone(), bytes.len() as u64, crc32c::crc32c(&bytes)))
            },
        )?;
    }
    Ok(())
}

#[test]
fn should_reject_corrupted_parts_before_commit_when_native_upload_integrity_fails(
) -> MidgeResult<()> {
    // Arrange: exercise corrupted parts through every native provider dialect.
    for protocol in [
        Protocol::S3,
        Protocol::Azure,
        Protocol::GcsJson,
        Protocol::GcsXml,
    ] {
        // Act: run the production upload after injecting a corrupt part.
        // Assert: the fixture requires rejection before remote commit.
        upload_fixture(
            protocol,
            UploadFault::CorruptPart,
            UploadPath::Provider,
            |directory| {
                let bytes = vec![7; 12 * 1024 * 1024];
                std::fs::write(directory.join("source"), &bytes)?;
                Ok((bytes.clone(), bytes.len() as u64, crc32c::crc32c(&bytes)))
            },
        )?;
    }
    Ok(())
}
