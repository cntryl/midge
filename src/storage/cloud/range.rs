//! Validation shared by native conditional SST range adapters.

use super::{CloudError, CloudOutcome, CloudResponse};
use crate::storage::StorageObjectMetadata;

pub(crate) fn validate_range_response(
    response: &CloudResponse,
    start: u64,
    end: u64,
    expected: &StorageObjectMetadata,
) -> CloudOutcome<()> {
    if response.status != 206 {
        return Err(CloudError::Protocol(format!(
            "SST range requires HTTP 206, received {}",
            response.status
        )));
    }
    let actual = super::executor::validate_get_response_length(response)?;
    if start >= end || end > expected.size || actual != end - start {
        return Err(CloudError::Protocol(
            "SST range body length differs from request".into(),
        ));
    }
    let header = |name: &str| {
        response
            .headers
            .iter()
            .find(|(key, _)| key.eq_ignore_ascii_case(name))
            .map(|(_, value)| value.trim())
    };
    let content_range = format!("bytes {start}-{}/{}", end - 1, expected.size);
    if header("content-range") != Some(content_range.as_str()) {
        return Err(CloudError::Protocol(
            "SST range Content-Range differs from requested object slice".into(),
        ));
    }
    let actual = StorageObjectMetadata {
        size: expected.size,
        etag: header("etag").unwrap_or_default().to_string(),
        generation: header("x-goog-generation").map(str::to_string),
    };
    if !expected.same_version(&actual) {
        return Err(CloudError::PreconditionFailed(
            "remote SST object version changed".into(),
        ));
    }
    Ok(())
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn should_reject_wrong_identity_or_range_when_body_has_valid_length() {
        // Arrange
        let expected = StorageObjectMetadata {
            size: 100,
            etag: "v1".into(),
            generation: None,
        };
        let mut response = CloudResponse {
            status: 206,
            headers: vec![
                ("Content-Range".into(), "bytes 10-12/100".into()),
                ("ETag".into(), "v1".into()),
            ],
            body: vec![1, 2, 3],
        };
        // Act
        // Assert
        assert!(validate_range_response(&response, 10, 13, &expected).is_ok());
        response.status = 200;
        assert!(validate_range_response(&response, 10, 13, &expected).is_err());
        response.status = 206;
        response.headers[0].1 = "bytes 11-13/100".into();
        assert!(validate_range_response(&response, 10, 13, &expected).is_err());
        response.headers[0].1 = "bytes 10-12/100".into();
        response.headers[1].1 = "v2".into();
        assert!(validate_range_response(&response, 10, 13, &expected).is_err());
    }
}
