//! Oracle Cloud Infrastructure (OCI) Object Storage Provider
//!
//! TODO: Implement custom lean OCI client with direct REST API + signature-based auth
//! For now, this is a stub. MockCloud is used for testing and integration.

use crate::storage::cloud::{CloudCallback, CloudEvent, CloudOutcome};

/// Oracle Cloud Infrastructure Object Storage provider stub
///
/// Full implementation will use:
/// - Direct REST API calls (no OCI SDK)
/// - Signature-based authentication (OCI auth headers)
/// - reqwest for async HTTP client
/// - tokio for async task spawning
pub struct OciProvider {
    namespace: String,
    bucket: String,
    region: String,
    // tenancy_id, user_id, fingerprint, private_key
}

impl OciProvider {
    /// Create a new OCI Object Storage provider
    ///
    /// # Arguments
    /// * `namespace` - OCI namespace
    /// * `bucket` - Object Storage bucket name
    /// * `region` - OCI region
    /// * `auth` - Tenancy ID, user ID, key fingerprint, private key (for auth)
    pub fn new(namespace: String, bucket: String, region: String) -> Self {
        Self {
            namespace,
            bucket,
            region,
        }
    }

    /// Submit a PUT operation (stub)
    #[allow(dead_code)]
    pub fn submit_put(&self, key: String, _data: Vec<u8>, callback: CloudCallback) {
        // TODO: Implement async PUT with OCI signature-based auth
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
        // TODO: Implement async GET with OCI signature-based auth
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
        // TODO: Implement async DELETE with OCI signature-based auth
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
        // TODO: Implement async LIST with OCI signature-based auth
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
    use std::sync::mpsc;

    #[test]
    fn should_create_oci_provider() {
        let provider = OciProvider::new(
            "mynamespace".to_string(),
            "mybucket".to_string(),
            "us-phoenix-1".to_string(),
        );
        assert_eq!(provider.namespace, "mynamespace");
        assert_eq!(provider.bucket, "mybucket");
        assert_eq!(provider.region, "us-phoenix-1");
    }
}
