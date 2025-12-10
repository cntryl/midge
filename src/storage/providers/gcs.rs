//! Google Cloud Storage Provider
//!
//! TODO: Implement custom lean GCS client with direct REST API + OAuth2
//! For now, this is a stub. MockCloud is used for testing and integration.

use crate::storage::cloud::{CloudCallback, CloudEvent, CloudOutcome};

/// Google Cloud Storage provider stub
///
/// Full implementation will use:
/// - Direct REST API calls (no Google Cloud SDK)
/// - OAuth2 authentication with service account
/// - reqwest for async HTTP client
/// - tokio for async task spawning
pub struct GcsProvider {
    #[allow(dead_code)]
    bucket: String,
    #[allow(dead_code)]
    project_id: String,
    // service_account_key: String,
}

impl GcsProvider {
    /// Create a new GCS provider
    ///
    /// # Arguments
    /// * `bucket` - GCS bucket name
    /// * `project_id` - GCP project ID
    /// * `service_account_key` - Path or JSON string for service account key
    pub fn new(bucket: String, project_id: String) -> Self {
        Self { bucket, project_id }
    }

    /// Submit a PUT operation (stub)
    #[allow(dead_code)]
    pub fn submit_put(&self, key: String, _data: Vec<u8>, callback: CloudCallback) {
        // TODO: Implement async PUT with OAuth2 signing
        // For now, send success
        let event = CloudEvent::PutComplete {
            key,
            result: CloudOutcome::Ok(()),
        };
        let _ = callback.send(event);
    }

    /// Submit a GET operation (stub)
    #[allow(dead_code)]
    pub fn submit_get(&self, key: String, callback: CloudCallback) {
        // TODO: Implement async GET with OAuth2 signing
        // For now, send empty data
        let event = CloudEvent::GetComplete {
            key,
            result: CloudOutcome::Ok(Vec::new()),
        };
        let _ = callback.send(event);
    }

    /// Submit a DELETE operation (stub)
    #[allow(dead_code)]
    pub fn submit_delete(&self, key: String, callback: CloudCallback) {
        // TODO: Implement async DELETE with OAuth2 signing
        // For now, send success
        let event = CloudEvent::DeleteComplete {
            key,
            result: CloudOutcome::Ok(()),
        };
        let _ = callback.send(event);
    }

    /// Submit a LIST operation (stub)
    #[allow(dead_code)]
    pub fn submit_list(&self, prefix: String, callback: CloudCallback) {
        // TODO: Implement async LIST with OAuth2 signing
        // For now, send empty list
        let event = CloudEvent::ListComplete {
            prefix,
            result: CloudOutcome::Ok(Vec::new()),
        };
        let _ = callback.send(event);
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn should_create_gcs_provider() {
        let provider = GcsProvider::new("my-bucket".to_string(), "my-project".to_string());
        assert_eq!(provider.bucket, "my-bucket");
        assert_eq!(provider.project_id, "my-project");
    }
}
