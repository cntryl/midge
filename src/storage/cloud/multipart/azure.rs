use super::{query, FileChunks, ReceiptMapper, UploadHttp};
use crate::storage::cloud::{CloudError, CloudOutcome};
use crate::storage::{StorageObjectMetadata, StorageWriteReceipt};
use base64::Engine as _;
use md5::Digest as _;
use reqwest::Method;

pub(super) async fn upload(
    url: &str,
    mut chunks: FileChunks,
    http: &mut UploadHttp,
    _receipt: &ReceiptMapper,
) -> CloudOutcome<Option<StorageWriteReceipt>> {
    let operation = uuid::Uuid::new_v4().simple().to_string();
    let mut blocks = Vec::new();
    let mut checked_parts = true;
    while let Some(part) = chunks.next(http).await? {
        let block = base64::engine::general_purpose::STANDARD
            .encode(format!("{operation}-{:010}", part.ordinal));
        let md5 = base64::engine::general_purpose::STANDARD.encode(md5::Md5::digest(&part.bytes));
        let part_url = query(&query(url, "comp", "block"), "blockid", &block);
        let request = http
            .request(Method::PUT, part_url, part.bytes)
            .with_header("Content-MD5", &md5)
            .with_staged_part_retries();
        let response = http.send_expected(request, false, &[201]).await?;
        if let Some(actual) = response.header("content-md5") {
            if actual != md5 {
                return Err(CloudError::Protocol(
                    "Azure block MD5 differs from sent bytes".into(),
                ));
            }
        } else {
            checked_parts = false;
        }
        let memory = http.retain_metadata(block.len().saturating_mul(2) + 128)?;
        blocks.push((block, memory));
    }
    let _completion_memory = http.retain_metadata(blocks.len().saturating_mul(256) + 64)?;
    let mut body = String::from("<BlockList>");
    for (block, _) in &blocks {
        body.push_str("<Uncommitted>");
        body.push_str(block);
        body.push_str("</Uncommitted>");
    }
    body.push_str("</BlockList>");
    let list_md5 =
        base64::engine::general_purpose::STANDARD.encode(md5::Md5::digest(body.as_bytes()));
    let request = http
        .request(
            Method::PUT,
            query(url, "comp", "blocklist"),
            bytes::Bytes::from(body),
        )
        .with_header("If-None-Match", "*")
        .with_header("Content-MD5", &list_md5)
        .with_header("Content-Type", "application/xml");
    let response = http.send_expected(request, true, &[201]).await?;
    // The final Content-MD5 is over the ordered block list. Together with
    // checked block receipts it proves the exact assembled byte stream; the
    // unvalidated x-ms-blob-content-md5 property alone would not prove this.
    if let Some(actual) = response.header("content-md5") {
        if actual != list_md5 {
            return Err(CloudError::Protocol(
                "Azure block-list MD5 differs from the sent list".into(),
            ));
        }
    } else {
        checked_parts = false;
    }
    let Some(etag) = response.header("etag").filter(|etag| !etag.is_empty()) else {
        return Ok(None);
    };
    Ok(checked_parts.then(|| StorageWriteReceipt {
        metadata: StorageObjectMetadata::new(chunks.source.size, etag.into()),
        content_crc32c: chunks.source.content_crc32c,
    }))
}
