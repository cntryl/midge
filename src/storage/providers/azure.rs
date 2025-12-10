//! Azure Blob Storage Provider
//!
//! TODO: Implement custom lean Azure client with direct REST API + SAS tokens
//! For now, this is a stub. MockCloud is used for testing and integration.

use crate::storage::cloud::{CloudCallback, CloudEvent, CloudOutcome};

/// Azure Blob Storage provider stub
///
/// Full implementation will use:
/// - Direct REST API calls (no Azure SDK)
/// - SAS token or shared key authentication
/// - reqwest for async HTTP client
/// - tokio for async task spawning
pub struct AzureProvider {
    #[allow(dead_code)]
    account_name: String,
    #[allow(dead_code)]
    container: String,
    // account_key or sas_token
}

impl AzureProvider {
    /// Create a new Azure Blob Storage provider
    ///
    /// # Arguments
    /// * `account_name` - Azure storage account name
    /// * `container` - Blob container name
    /// * `auth` - Connection string or SAS token
    pub fn new(account_name: String, container: String) -> Self {
        Self {
            account_name,
            container,
        }
    }

    /// Submit a PUT operation (stub)
    #[allow(dead_code)]
    pub fn submit_put(&self, key: String, _data: Vec<u8>, callback: CloudCallback) {
        // TODO: Implement async PUT with SAS or shared key signing
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
        // TODO: Implement async GET with SAS or shared key signing
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
        // TODO: Implement async DELETE with SAS or shared key signing
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
        // TODO: Implement async LIST with SAS or shared key signing
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
    fn should_create_azure_provider() {
        let provider = AzureProvider::new("myaccount".to_string(), "mycontainer".to_string());
        assert_eq!(provider.account_name, "myaccount");
        assert_eq!(provider.container, "mycontainer");
    }
}
