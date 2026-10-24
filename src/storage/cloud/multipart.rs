//! Conditional, bounded file uploads. Each operation owns its source, deadline,
//! staging identity and reservation through completion or best-effort abort.

use super::{
    CloudError, CloudExecutor, CloudOutcome, CloudRequest, CloudRequestOptions, CloudResponse,
    CloudRetryPolicy, CloudSigner,
};
use crate::storage::FILE_UPLOAD_PART_BYTES;
use crate::storage::{StorageFileUpload, StorageWriteReceipt};
use reqwest::{Client, Method};
use std::sync::Arc;
use std::time::Duration;

#[cfg(feature = "cloud-azure")]
mod azure;
#[cfg(feature = "cloud-gcp")]
mod gcs;
#[cfg(any(feature = "cloud-aws", feature = "cloud-oci"))]
mod s3;

type ErrorMapper = Arc<dyn Fn(&CloudResponse, bool) -> CloudError + Send + Sync>;
type ReceiptMapper = Arc<
    dyn Fn(&CloudResponse, u64, u32) -> CloudOutcome<Option<StorageWriteReceipt>> + Send + Sync,
>;

pub(crate) enum FileUploadKind {
    #[cfg(any(feature = "cloud-aws", feature = "cloud-oci"))]
    S3,
    #[cfg(feature = "cloud-azure")]
    Azure,
    #[cfg(feature = "cloud-gcp")]
    Gcs { json: bool },
}

pub(crate) struct FileUploadRequest {
    pub(crate) key: String,
    pub(crate) url: String,
    pub(crate) kind: FileUploadKind,
    pub(crate) source: StorageFileUpload,
    pub(crate) options: CloudRequestOptions,
    pub(crate) error_mapper: ErrorMapper,
    pub(crate) receipt_mapper: ReceiptMapper,
}

pub(super) struct UploadHttp {
    client: Client,
    signer: Option<Arc<dyn CloudSigner>>,
    retry: CloudRetryPolicy,
    deadline: tokio::time::Instant,
    timeout: Duration,
    reservation: Option<Arc<crate::common::resource_budget::ResourceReservation>>,
    headers: Vec<(String, String)>,
    error_mapper: ErrorMapper,
    abort_url: Option<String>,
}

impl UploadHttp {
    pub(super) fn new(
        client: Client,
        signer: Option<Arc<dyn CloudSigner>>,
        retry: CloudRetryPolicy,
        timeout: Duration,
        upload: &FileUploadRequest,
    ) -> Self {
        Self {
            client,
            signer,
            retry,
            deadline: tokio::time::Instant::now() + timeout,
            timeout,
            reservation: upload.options.reservation.clone(),
            headers: upload
                .options
                .headers
                .iter()
                .filter(|(name, _)| !name.eq_ignore_ascii_case("if-none-match"))
                .cloned()
                .collect(),
            error_mapper: Arc::clone(&upload.error_mapper),
            abort_url: None,
        }
    }

    fn request(&self, method: Method, url: String, bytes: bytes::Bytes) -> CloudRequest {
        let mut request = CloudRequest::new(method, url)
            .with_header("Content-Length", bytes.len().to_string())
            .with_reservation(self.reservation.clone())
            .with_retry_policy(self.retry)
            .with_response_limit(64 * 1024);
        request.body = Some(bytes);
        request.headers.extend(self.headers.iter().cloned());
        request
    }

    async fn send(&self, request: CloudRequest, conditional: bool) -> CloudOutcome<CloudResponse> {
        let response = CloudExecutor::execute_request_with_signer_at(
            self.client.clone(),
            self.signer.clone(),
            request,
            Some(self.deadline),
        )
        .await
        .map_err(|error| match error {
            crate::common::MidgeError::Timeout(_) => {
                CloudError::Timeout("streaming request deadline expired".into())
            }
            _ => CloudError::Transport("streaming request transport failed".into()),
        })?;
        if !(200..300).contains(&response.status) && response.status != 308 {
            return Err((self.error_mapper)(&response, conditional));
        }
        Ok(response)
    }

    async fn send_expected(
        &self,
        request: CloudRequest,
        conditional: bool,
        statuses: &[u16],
    ) -> CloudOutcome<CloudResponse> {
        let response = self.send(request, conditional).await?;
        if !statuses.contains(&response.status) {
            return Err(CloudError::Protocol(format!(
                "unexpected streaming upload success status {}",
                response.status
            )));
        }
        Ok(response)
    }

    #[cfg(any(feature = "cloud-aws", feature = "cloud-oci", feature = "cloud-azure"))]
    fn retain_metadata(
        &self,
        bytes: usize,
    ) -> CloudOutcome<Option<crate::common::resource_budget::ResourceReservation>> {
        self.reservation
            .as_ref()
            .map(|reservation| {
                reservation
                    .reserve_related(bytes, "multipart receipt metadata")
                    .map_err(CloudError::from_protocol_or_timeout_error)
            })
            .transpose()
    }
}

struct FileChunks {
    source: StorageFileUpload,
    offset: u64,
    checksum: u32,
    ordinal: u32,
}

struct FilePart {
    bytes: bytes::Bytes,
    #[cfg(feature = "cloud-gcp")]
    start: u64,
    #[cfg(feature = "cloud-gcp")]
    end: u64,
    #[cfg(any(feature = "cloud-aws", feature = "cloud-oci", feature = "cloud-azure"))]
    ordinal: u32,
    #[cfg(any(feature = "cloud-aws", feature = "cloud-oci"))]
    checksum: String,
}

impl FileChunks {
    fn new(source: StorageFileUpload) -> CloudOutcome<Self> {
        if source.part_size != FILE_UPLOAD_PART_BYTES || source.size == 0 {
            return Err(CloudError::Protocol(
                "streaming file upload requires a nonempty file and fixed 5 MiB parts".into(),
            ));
        }
        Ok(Self {
            source,
            offset: 0,
            checksum: 0,
            ordinal: 0,
        })
    }

    async fn next(&mut self, http: &UploadHttp) -> CloudOutcome<Option<FilePart>> {
        #[cfg(any(feature = "cloud-aws", feature = "cloud-oci"))]
        use base64::Engine as _;
        if self.offset == self.source.size {
            return Ok(None);
        }
        self.ordinal = self
            .ordinal
            .checked_add(1)
            .filter(|ordinal| *ordinal <= 10_000)
            .ok_or_else(|| {
                CloudError::Protocol("multipart upload exceeds 10,000 bounded parts".into())
            })?;
        let start = self.offset;
        let length = (self.source.size - start).min(self.source.part_size as u64);
        let file = Arc::clone(&self.source.file);
        let expected_size = self.source.size;
        let reservation = http.reservation.clone();
        let bytes = tokio::task::spawn_blocking(move || {
            let _reservation = reservation;
            if file.len()? != expected_size {
                return Err(crate::io::FsError::Corruption(
                    "immutable upload file size changed".into(),
                ));
            }
            let bytes = file.read_at(start, length)?;
            if bytes.len() as u64 != length || file.len()? != expected_size {
                return Err(crate::io::FsError::Corruption(
                    "immutable upload file changed while reading".into(),
                ));
            }
            Ok(bytes)
        })
        .await
        .map_err(|_| CloudError::Transport("immutable upload file worker failed".into()))?
        .map_err(|error| CloudError::Protocol(error.to_string()))?;
        self.checksum = crc32c::crc32c_append(self.checksum, &bytes);
        self.offset += length;
        if self.offset == self.source.size && self.checksum != self.source.content_crc32c {
            return Err(CloudError::Protocol(
                "immutable upload file checksum changed before commit".into(),
            ));
        }
        #[cfg(any(feature = "cloud-aws", feature = "cloud-oci"))]
        let checksum =
            base64::engine::general_purpose::STANDARD.encode(crc32c::crc32c(&bytes).to_be_bytes());
        Ok(Some(FilePart {
            bytes,
            #[cfg(feature = "cloud-gcp")]
            start,
            #[cfg(feature = "cloud-gcp")]
            end: self.offset,
            #[cfg(any(feature = "cloud-aws", feature = "cloud-oci", feature = "cloud-azure"))]
            ordinal: self.ordinal,
            #[cfg(any(feature = "cloud-aws", feature = "cloud-oci"))]
            checksum,
        }))
    }
}

pub(super) async fn run(
    upload: FileUploadRequest,
    mut http: UploadHttp,
) -> CloudOutcome<Option<StorageWriteReceipt>> {
    if !upload
        .options
        .headers
        .iter()
        .any(|(name, value)| name.eq_ignore_ascii_case("if-none-match") && value.trim() == "*")
        || upload
            .options
            .headers
            .iter()
            .any(|(name, _)| name.eq_ignore_ascii_case("if-match"))
    {
        return Err(CloudError::Protocol(
            "streaming file upload requires absence commitment".into(),
        ));
    }
    let timeout = http.timeout;
    let deadline = http.deadline;
    let operation = async {
        let result = match upload.kind {
            #[cfg(any(feature = "cloud-aws", feature = "cloud-oci"))]
            FileUploadKind::S3 => {
                s3::upload(
                    &upload.url,
                    FileChunks::new(upload.source)?,
                    &mut http,
                    &upload.receipt_mapper,
                )
                .await
            }
            #[cfg(feature = "cloud-azure")]
            FileUploadKind::Azure => {
                azure::upload(
                    &upload.url,
                    FileChunks::new(upload.source)?,
                    &mut http,
                    &upload.receipt_mapper,
                )
                .await
            }
            #[cfg(feature = "cloud-gcp")]
            FileUploadKind::Gcs { json } => {
                gcs::upload(
                    &upload.url,
                    json,
                    FileChunks::new(upload.source)?,
                    &mut http,
                    &upload.receipt_mapper,
                )
                .await
            }
        };
        if result.is_err() && tokio::time::Instant::now() < http.deadline {
            if let Some(url) = http.abort_url.take() {
                let _ = http
                    .send(
                        http.request(Method::DELETE, url, bytes::Bytes::new()),
                        false,
                    )
                    .await;
            }
        }
        result
    };
    tokio::time::timeout_at(deadline, operation)
        .await
        .unwrap_or_else(|_| {
            Err(CloudError::Timeout(format!(
                "streaming upload exceeded {} ms; admitted effects may finish later",
                timeout.as_millis()
            )))
        })
}

#[cfg(any(feature = "cloud-aws", feature = "cloud-oci", feature = "cloud-azure"))]
fn query(url: &str, key: &str, value: &str) -> String {
    format!(
        "{url}{}{key}={}",
        if url.contains('?') { '&' } else { '?' },
        urlencoding::encode(value)
    )
}

#[cfg(any(feature = "cloud-aws", feature = "cloud-oci"))]
fn xml_value(body: &[u8], tag: &str) -> CloudOutcome<String> {
    let values =
        crate::storage::providers::xml::extract_xml_tag_values(&String::from_utf8_lossy(body), tag);
    if values.len() != 1 || values[0].is_empty() || values[0].len() > 4096 {
        return Err(CloudError::Protocol(format!(
            "multipart response has no unique bounded {tag}"
        )));
    }
    Ok(values.into_iter().next().expect("one XML value"))
}

#[cfg(any(feature = "cloud-aws", feature = "cloud-oci"))]
fn optional_xml_value(body: &[u8], tag: &str) -> CloudOutcome<Option<String>> {
    if crate::storage::providers::xml::extract_xml_tag_values(&String::from_utf8_lossy(body), tag)
        .is_empty()
    {
        Ok(None)
    } else {
        xml_value(body, tag).map(Some)
    }
}

#[cfg(any(feature = "cloud-aws", feature = "cloud-oci"))]
fn escape_xml(value: &str) -> String {
    value
        .replace('&', "&amp;")
        .replace('<', "&lt;")
        .replace('>', "&gt;")
        .replace('"', "&quot;")
        .replace('\'', "&apos;")
}
