use super::{
    escape_xml, optional_xml_value, query, xml_value, FileChunks, ReceiptMapper, UploadHttp,
};
use crate::storage::cloud::{CloudError, CloudOutcome};
use crate::storage::StorageWriteReceipt;
use base64::Engine as _;
use reqwest::Method;

pub(super) async fn upload(
    url: &str,
    mut chunks: FileChunks,
    http: &mut UploadHttp,
    receipt: &ReceiptMapper,
) -> CloudOutcome<Option<StorageWriteReceipt>> {
    let init = http
        .request(Method::POST, query(url, "uploads", ""), bytes::Bytes::new())
        .with_header("x-amz-checksum-algorithm", "CRC32C")
        .with_header("x-amz-checksum-type", "FULL_OBJECT");
    let response = http.send_expected(init, false, &[200]).await?;
    let upload_id = xml_value(&response.body, "UploadId")?;
    let session = query(url, "uploadId", &upload_id);
    http.abort_url = Some(session.clone());
    let mut parts = Vec::new();
    let mut all_part_receipts = true;
    while let Some(part) = chunks.next(http).await? {
        let part_url = query(&session, "partNumber", &part.ordinal.to_string());
        let request = http
            .request(Method::PUT, part_url, part.bytes)
            .with_header("x-amz-checksum-crc32c", &part.checksum)
            .with_staged_part_retries();
        let response = http.send_expected(request, false, &[200]).await?;
        if let Some(actual) = response.header("x-amz-checksum-crc32c") {
            if actual != part.checksum {
                return Err(CloudError::Protocol(
                    "multipart part CRC32C differs from sent bytes".into(),
                ));
            }
        } else {
            all_part_receipts = false;
        }
        let etag = response
            .header("etag")
            .filter(|value| !value.is_empty() && value.len() <= 1024)
            .ok_or_else(|| CloudError::Protocol("multipart part lacks a bounded ETag".into()))?;
        let memory = http.retain_metadata((etag.len() + part.checksum.len() + 128) * 2)?;
        parts.push((etag.to_owned(), part.checksum, memory));
    }
    let xml_bytes = parts
        .iter()
        .map(|(etag, checksum, _)| etag.len().saturating_mul(6) + checksum.len() + 160)
        .sum::<usize>()
        + 128;
    let _completion_memory = http.retain_metadata(xml_bytes)?;
    let mut body = String::with_capacity(xml_bytes);
    body.push_str("<CompleteMultipartUpload>");
    for (ordinal, (etag, checksum, _)) in parts.iter().enumerate() {
        use std::fmt::Write as _;
        write!(&mut body, "<Part><PartNumber>{}</PartNumber><ETag>{}</ETag><ChecksumCRC32C>{checksum}</ChecksumCRC32C></Part>", ordinal + 1, escape_xml(etag))
            .expect("writing XML to String is infallible");
    }
    body.push_str("</CompleteMultipartUpload>");
    let checksum = base64::engine::general_purpose::STANDARD
        .encode(chunks.source.content_crc32c.to_be_bytes());
    let request = http
        .request(Method::POST, session, bytes::Bytes::from(body))
        .with_header("If-None-Match", "*")
        .with_header("Content-Type", "application/xml")
        .with_header("x-amz-checksum-type", "FULL_OBJECT")
        .with_header("x-amz-checksum-crc32c", &checksum)
        .with_header("x-amz-mp-object-size", chunks.source.size.to_string());
    let mut response = http.send_expected(request, true, &[200]).await?;
    // CompleteMultipartUpload can return a 200 containing an Error document.
    let etag = xml_value(&response.body, "ETag")?;
    if let Some(actual) = optional_xml_value(&response.body, "ChecksumCRC32C")? {
        if actual != checksum
            || response
                .header("x-amz-checksum-crc32c")
                .is_some_and(|header| header != actual)
        {
            return Err(CloudError::Protocol(
                "multipart full-object checksum receipts conflict".into(),
            ));
        }
        response
            .headers
            .push(("x-amz-checksum-crc32c".into(), actual));
    }
    if let Some(kind) = optional_xml_value(&response.body, "ChecksumType")? {
        if kind != "FULL_OBJECT" {
            return Err(CloudError::Protocol(
                "multipart checksum is not a full-object CRC32C".into(),
            ));
        }
    }
    if response
        .header("x-amz-checksum-type")
        .is_some_and(|kind| kind != "FULL_OBJECT")
        || response.header("etag").is_some_and(|header| header != etag)
    {
        return Err(CloudError::Protocol(
            "multipart response has conflicting type or identity".into(),
        ));
    }
    response.headers.push(("etag".into(), etag));
    http.abort_url = None;
    let proof = receipt(&response, chunks.source.size, chunks.source.content_crc32c)?;
    Ok(proof.filter(|_| all_part_receipts))
}
