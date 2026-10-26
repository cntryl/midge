//! Native multipart/resumable requests, exact body digests and bounded reads.

use crate::common::resource_budget::ResourceBudget;
use crate::common::{MidgeError, MidgeResult, OperationDeadline};
use crate::io::{File, FsError, FsResult, ReadFile};
use crate::sst::traits::SstFactory;
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

#[derive(Clone, Copy, PartialEq, Eq)]
enum UploadFault {
    None,
    CorruptPart,
    MutatedSource,
}

#[derive(Clone, Copy)]
enum UploadPath {
    Provider,
    Hybrid,
}

#[derive(Clone, Copy)]
enum UploadSource {
    Bytes,
    FinalizedSst,
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

fn prepare_upload_source(
    directory: &std::path::Path,
    size: usize,
    source: UploadSource,
) -> MidgeResult<(Vec<u8>, u64, u32)> {
    let source_path = directory.join("source");
    if matches!(source, UploadSource::FinalizedSst) {
        let fs = Arc::new(crate::io::RealFs::new(directory).map_err(FsError::into_midge)?);
        let factory = crate::sst::FsSstFactoryIo::new(fs, 4096);
        let mut writer = factory.create_for_compaction(ResourceBudget::new(64 * 1024 * 1024))?;
        let mut value = vec![0_u8; 4096];
        let mut random = 0x9e37_79b9_u32;
        for sequence in 0..=3583_u64 {
            for byte in &mut value {
                random ^= random << 13;
                random ^= random >> 17;
                random ^= random << 5;
                *byte = u8::try_from(random & u32::from(u8::MAX))
                    .expect("masked pseudo-random byte fits in u8");
            }
            writer.add_sorted_with_meta(
                &sequence.to_be_bytes(),
                Some(&value),
                sequence + 1,
                crate::types::EntryType::Put,
                None,
            )?;
        }
        let finalized = writer
            .finish_to_path_with_summary(std::path::Path::new("source"))?
            .expect("filesystem writer returns finalized SST identity");
        assert!(finalized.size_bytes >= (crate::storage::FILE_UPLOAD_PART_BYTES * 2) as u64);
        Ok((
            std::fs::read(&source_path)?,
            finalized.size_bytes,
            finalized.content_crc32c,
        ))
    } else {
        let bytes = vec![7; size];
        std::fs::write(&source_path, &bytes)?;
        Ok((
            bytes.clone(),
            u64::try_from(bytes.len()).expect("fixture size fits in u64"),
            crc32c::crc32c(&bytes),
        ))
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
        let fs = crate::io::RealFs::new(directory).map_err(FsError::into_midge)?;
        let hybrid = crate::storage::HybridStorage::with_policy(
            Arc::clone(storage),
            crate::storage::hybrid::policy::StorageBudgetPolicy::default(),
        );
        let result = hybrid
            .publish_immutable_file(
                crate::storage::hybrid::backend::ImmutableFileSource {
                    key: "object",
                    fs: &fs,
                    path: &crate::io::FsPath::new("source"),
                    size,
                    checksum,
                },
                budget,
            )
            .map(drop);
        return Ok((result, None, None));
    }

    let root = crate::io::RealFs::new(directory).map_err(FsError::into_midge)?;
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
            part_size: crate::storage::FILE_UPLOAD_PART_BYTES,
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
    Ok((
        result,
        Some(read.load(Ordering::Relaxed)),
        Some(maximum.load(Ordering::Relaxed)),
    ))
}

fn assert_upload_workspace_bounds(
    maximum_read: Option<u64>,
    through_hybrid: bool,
    budget: &ResourceBudget,
    storage: &dyn StorageBackend,
) {
    if let Some(maximum_read) = maximum_read {
        assert!(maximum_read <= crate::storage::FILE_UPLOAD_PART_BYTES as u64);
    }
    if through_hybrid {
        let part_bounded_workspace = crate::storage::FILE_UPLOAD_PART_BYTES
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

fn upload_fixture(
    protocol: Protocol,
    fault: UploadFault,
    size: usize,
    path: UploadPath,
    source: UploadSource,
) -> MidgeResult<()> {
    let directory = tempfile::tempdir()?;
    let source_path = directory.path().join("source");
    let (bytes, finalized_size, finalized_checksum) =
        prepare_upload_source(directory.path(), size, source)?;
    if fault == UploadFault::MutatedSource {
        let mut changed = bytes.clone();
        changed[0] ^= 1;
        std::fs::write(&source_path, changed)?;
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
        directory.path(),
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
            assert_eq!(
                bytes_read,
                u64::try_from(size).expect("fixture size fits in u64")
            );
        }
        assert_eq!(state.range_gets, 0, "publication must not read back ranges");
        let request_overhead = if through_hybrid { 3 } else { 2 };
        assert!(
            state.requests
                <= size.div_ceil(crate::storage::FILE_UPLOAD_PART_BYTES) + request_overhead,
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
            64 * 1024 * 1024,
            UploadPath::Hybrid,
            UploadSource::Bytes,
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
            12 * 1024 * 1024,
            UploadPath::Provider,
            UploadSource::Bytes,
        )?;
    }
    Ok(())
}

#[test]
fn should_reject_changed_finalized_files_before_commit_when_streaming_upload_reads_them(
) -> MidgeResult<()> {
    // Arrange: finalized source mutation is exercised through every dialect.
    for protocol in [
        Protocol::S3,
        Protocol::Azure,
        Protocol::GcsJson,
        Protocol::GcsXml,
    ] {
        // Act: run the production upload against the changed source bytes.
        // Assert: the fixture rejects committing an unproven immutable identity.
        upload_fixture(
            protocol,
            UploadFault::MutatedSource,
            12 * 1024 * 1024,
            UploadPath::Hybrid,
            UploadSource::FinalizedSst,
        )?;
    }
    Ok(())
}
