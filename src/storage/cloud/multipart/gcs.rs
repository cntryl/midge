use super::{FileChunks, ReceiptMapper, UploadHttp};
use crate::storage::cloud::{CloudError, CloudOutcome};
use crate::storage::StorageWriteReceipt;
use base64::Engine as _;
use reqwest::Method;

pub(super) async fn upload(
    url: &str,
    json: bool,
    mut chunks: FileChunks,
    http: &mut UploadHttp,
    receipt: &ReceiptMapper,
) -> CloudOutcome<Option<StorageWriteReceipt>> {
    let checksum = base64::engine::general_purpose::STANDARD
        .encode(chunks.source.content_crc32c.to_be_bytes());
    let body = if json {
        bytes::Bytes::from(serde_json::json!({ "crc32c": checksum }).to_string())
    } else {
        bytes::Bytes::new()
    };
    let mut init = http.request(Method::POST, url.to_owned(), body);
    init = if json {
        init.with_header("Content-Type", "application/json")
            .with_header("X-Upload-Content-Length", chunks.source.size.to_string())
    } else {
        init.with_header("x-goog-resumable", "start")
            .with_header("x-goog-if-generation-match", "0")
            .with_header("Content-Type", "application/octet-stream")
    };
    let response = http.send_expected(init, true, &[200, 201]).await?;
    let session = response
        .header("location")
        .filter(|value| value.len() <= 16 * 1024)
        .ok_or_else(|| {
            CloudError::Protocol("GCS resumable initiation has no bounded session URI".into())
        })?
        .to_owned();
    let origin = reqwest::Url::parse(url)
        .map_err(|_| CloudError::Protocol("invalid upload origin".into()))?;
    let location = reqwest::Url::parse(&session)
        .map_err(|_| CloudError::Protocol("invalid resumable session URI".into()))?;
    if location.origin() != origin.origin() {
        return Err(CloudError::Protocol(
            "resumable upload changed the authenticated origin".into(),
        ));
    }
    let sessions = location
        .query_pairs()
        .filter(|(name, _)| name == "upload_id")
        .collect::<Vec<_>>();
    if sessions.len() != 1
        || sessions[0].1.is_empty()
        || !location.username().is_empty()
        || location.password().is_some()
        || location.fragment().is_some()
    {
        return Err(CloudError::Protocol(
            "GCS returned no unique isolated upload session".into(),
        ));
    }
    http.abort_url = Some(session.clone());
    while let Some(part) = chunks.next(http).await? {
        let final_part = part.end == chunks.source.size;
        let mut request = http
            .request(Method::PUT, session.clone(), part.bytes)
            .with_header("Content-Type", "application/octet-stream")
            .with_header(
                "Content-Range",
                format!(
                    "bytes {}-{}/{}",
                    part.start,
                    part.end - 1,
                    chunks.source.size
                ),
            );
        if final_part {
            request = request.with_header("X-Goog-Hash", format!("crc32c={checksum}"));
        }
        let response = http.send(request, true).await?;
        if final_part {
            if !matches!(response.status, 200 | 201) {
                return Err(CloudError::Protocol(
                    "GCS final upload did not commit the complete object".into(),
                ));
            }
            http.abort_url = None;
            return receipt(&response, chunks.source.size, chunks.source.content_crc32c);
        }
        let expected = format!("bytes=0-{}", part.end - 1);
        if response.status != 308 || response.header("range") != Some(expected.as_str()) {
            return Err(CloudError::Protocol(
                "GCS did not retain exactly the submitted upload prefix".into(),
            ));
        }
    }
    Err(CloudError::Protocol(
        "GCS resumable upload ended without a final part".into(),
    ))
}
