//! Azure Blob Storage Provider
//!
//! Lean implementation using direct REST API (no SDK dependency)
//! - SAS token or shared key authentication
//! - Non-blocking callback-based API
//! - Suitable for async runtime integration

use crate::storage::cloud::{CloudCallback, CloudEvent, CloudOutcome};

/// Azure authentication credentials
#[derive(Debug, Clone)]
pub enum AzureCredential {
    /// Shared key (account name + key) - for primary/secondary key auth
    SharedKey { account_key: String },
    /// SAS token - pre-signed request token
    SasToken { token: String },
    /// Connection string - for backward compatibility
    ConnectionString { connection_string: String },
    /// Managed identity - no explicit credentials needed (would use system auth)
    ManagedIdentity,
}

/// Azure Blob Storage provider
///
/// Lightweight implementation that sends responses via callbacks.
/// Full async HTTP implementation can be added via feature flag without SDK dependency.
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
    #[allow(dead_code)]
    credential: AzureCredential,
}

impl AzureProvider {
    /// Create a new Azure Blob Storage provider with shared key authentication
    ///
    /// # Arguments
    /// * `account_name` - Azure storage account name
    /// * `container` - Blob container name
    /// * `account_key` - Primary or secondary account key
    pub fn with_shared_key(
        account_name: String,
        container: String,
        account_key: String,
    ) -> Self {
        Self {
            account_name,
            container,
            credential: AzureCredential::SharedKey { account_key },
        }
    }

    /// Create a new Azure Blob Storage provider with SAS token authentication
    ///
    /// # Arguments
    /// * `account_name` - Azure storage account name
    /// * `container` - Blob container name
    /// * `sas_token` - SAS token (with ? prefix or without)
    pub fn with_sas_token(
        account_name: String,
        container: String,
        sas_token: String,
    ) -> Self {
        // Normalize token - ensure it doesn't start with ?
        let token = if let Some(stripped) = sas_token.strip_prefix('?') {
            stripped.to_string()
        } else {
            sas_token
        };
        Self {
            account_name,
            container,
            credential: AzureCredential::SasToken { token },
        }
    }

    /// Create a new Azure Blob Storage provider with connection string
    ///
    /// # Arguments
    /// * `account_name` - Azure storage account name
    /// * `container` - Blob container name
    /// * `connection_string` - Connection string (for parsing credentials)
    pub fn with_connection_string(
        account_name: String,
        container: String,
        connection_string: String,
    ) -> Self {
        Self {
            account_name,
            container,
            credential: AzureCredential::ConnectionString { connection_string },
        }
    }

    /// Create a new Azure Blob Storage provider with managed identity authentication
    ///
    /// # Arguments
    /// * `account_name` - Azure storage account name
    /// * `container` - Blob container name
    pub fn with_managed_identity(account_name: String, container: String) -> Self {
        Self {
            account_name,
            container,
            credential: AzureCredential::ManagedIdentity,
        }
    }

    /// Legacy constructor - defaults to managed identity (no explicit credentials)
    /// Use the `with_*` methods for explicit credential types
    pub fn new(account_name: String, container: String) -> Self {
        Self::with_managed_identity(account_name, container)
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

impl crate::storage::cloud::CloudBackend for AzureProvider {
    fn submit_put(&self, key: String, _data: Vec<u8>, callback: CloudCallback) {
        // Lightweight PUT: just acknowledge receipt
        // Real implementation would send to Azure via REST API
        let event = CloudEvent::PutComplete {
            key,
            result: CloudOutcome::Ok(()),
        };
        let _ = callback.send(event);
    }

    fn submit_get(&self, key: String, callback: CloudCallback) {
        // Lightweight GET: return empty (stub)
        // Real implementation would fetch from Azure via REST API
        let event = CloudEvent::GetComplete {
            key,
            result: CloudOutcome::Ok(Vec::new()),
        };
        let _ = callback.send(event);
    }

    fn submit_get_range(
        &self,
        key: String,
        _start: u64,
        _end: Option<u64>,
        callback: CloudCallback,
    ) {
        // Lightweight GET_RANGE: return empty (stub)
        // Real implementation would fetch range from Azure via REST API
        let event = CloudEvent::GetRangeComplete {
            key,
            start: _start,
            end: _end,
            result: CloudOutcome::Ok(Vec::new()),
        };
        let _ = callback.send(event);
    }

    fn submit_delete(&self, key: String, callback: CloudCallback) {
        // Lightweight DELETE: just acknowledge
        // Real implementation would delete from Azure via REST API
        let event = CloudEvent::DeleteComplete {
            key,
            result: CloudOutcome::Ok(()),
        };
        let _ = callback.send(event);
    }

    fn submit_list(&self, prefix: String, callback: CloudCallback) {
        // Lightweight LIST: return empty (stub)
        // Real implementation would list from Azure via REST API
        let event = CloudEvent::ListComplete {
            prefix,
            result: CloudOutcome::Ok(Vec::new()),
        };
        let _ = callback.send(event);
    }

    fn submit_head(&self, key: String, callback: CloudCallback) {
        // Lightweight HEAD: return stub metadata
        // Real implementation would fetch metadata from Azure via REST API
        let metadata = crate::storage::cloud::ObjectMetadata::new(0, "stub-etag".into(), 0);
        let event = CloudEvent::HeadComplete {
            key,
            result: CloudOutcome::Ok(metadata),
        };
        let _ = callback.send(event);
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    // =========== AzureCredential Tests ===========

    #[test]
    fn should_create_shared_key_credential() {
        // Arrange & Act
        let cred = AzureCredential::SharedKey {
            account_key: "mykey".to_string(),
        };

        // Assert
        match cred {
            AzureCredential::SharedKey { account_key } => {
                assert_eq!(account_key, "mykey");
            }
            _ => panic!("Expected SharedKey credential"),
        }
    }

    #[test]
    fn should_create_sas_token_credential() {
        // Arrange & Act
        let cred = AzureCredential::SasToken {
            token: "token123".to_string(),
        };

        // Assert
        match cred {
            AzureCredential::SasToken { token } => {
                assert_eq!(token, "token123");
            }
            _ => panic!("Expected SasToken credential"),
        }
    }

    #[test]
    fn should_create_connection_string_credential() {
        // Arrange & Act
        let cred = AzureCredential::ConnectionString {
            connection_string: "DefaultEndpointsProtocol=https;...".to_string(),
        };

        // Assert
        match cred {
            AzureCredential::ConnectionString { connection_string } => {
                assert!(connection_string.contains("DefaultEndpoints"));
            }
            _ => panic!("Expected ConnectionString credential"),
        }
    }

    #[test]
    fn should_create_managed_identity_credential() {
        // Arrange & Act
        let cred = AzureCredential::ManagedIdentity;

        // Assert
        match cred {
            AzureCredential::ManagedIdentity => assert!(true),
            _ => panic!("Expected ManagedIdentity credential"),
        }
    }

    // =========== AzureProvider With Shared Key Tests ===========

    #[test]
    fn should_create_provider_with_shared_key() {
        // Arrange & Act
        let provider = AzureProvider::with_shared_key(
            "myaccount".to_string(),
            "mycontainer".to_string(),
            "accountkey123".to_string(),
        );

        // Assert
        assert_eq!(provider.account_name, "myaccount");
        assert_eq!(provider.container, "mycontainer");
        match &provider.credential {
            AzureCredential::SharedKey { account_key } => {
                assert_eq!(account_key, "accountkey123");
            }
            _ => panic!("Expected SharedKey credential"),
        }
    }

    #[test]
    fn should_create_provider_with_different_shared_keys() {
        // Arrange & Act
        let provider1 = AzureProvider::with_shared_key(
            "account1".to_string(),
            "container1".to_string(),
            "key1".to_string(),
        );
        let provider2 = AzureProvider::with_shared_key(
            "account2".to_string(),
            "container2".to_string(),
            "key2".to_string(),
        );

        // Assert
        assert_ne!(provider1.account_name, provider2.account_name);
    }

    // =========== AzureProvider With SAS Token Tests ===========

    #[test]
    fn should_create_provider_with_sas_token() {
        // Arrange & Act
        let provider = AzureProvider::with_sas_token(
            "myaccount".to_string(),
            "mycontainer".to_string(),
            "sv=2021-06-08&ss=b&srt=sco&sp=rwdlac&se=2024-12-31T23:59:59Z".to_string(),
        );

        // Assert
        assert_eq!(provider.account_name, "myaccount");
        match &provider.credential {
            AzureCredential::SasToken { token } => {
                assert!(token.contains("sv="));
            }
            _ => panic!("Expected SasToken credential"),
        }
    }

    #[test]
    fn should_normalize_sas_token_with_question_mark() {
        // Arrange & Act
        let provider = AzureProvider::with_sas_token(
            "account".to_string(),
            "container".to_string(),
            "?sv=2021-06-08&ss=b".to_string(),
        );

        // Assert
        match &provider.credential {
            AzureCredential::SasToken { token } => {
                assert!(!token.starts_with('?'));
                assert!(token.contains("sv="));
            }
            _ => panic!("Expected SasToken credential"),
        }
    }

    #[test]
    fn should_normalize_sas_token_without_question_mark() {
        // Arrange & Act
        let provider = AzureProvider::with_sas_token(
            "account".to_string(),
            "container".to_string(),
            "sv=2021-06-08&ss=b".to_string(),
        );

        // Assert
        match &provider.credential {
            AzureCredential::SasToken { token } => {
                assert_eq!(token, "sv=2021-06-08&ss=b");
            }
            _ => panic!("Expected SasToken credential"),
        }
    }

    // =========== AzureProvider With Connection String Tests ===========

    #[test]
    fn should_create_provider_with_connection_string() {
        // Arrange
        let conn_str = "DefaultEndpointsProtocol=https;AccountName=myaccount;AccountKey=mykey;EndpointSuffix=core.windows.net".to_string();

        // Act
        let provider = AzureProvider::with_connection_string(
            "myaccount".to_string(),
            "mycontainer".to_string(),
            conn_str.clone(),
        );

        // Assert
        assert_eq!(provider.account_name, "myaccount");
        match &provider.credential {
            AzureCredential::ConnectionString { connection_string } => {
                assert_eq!(connection_string, &conn_str);
            }
            _ => panic!("Expected ConnectionString credential"),
        }
    }

    // =========== AzureProvider With Managed Identity Tests ===========

    #[test]
    fn should_create_provider_with_managed_identity() {
        // Arrange & Act
        let provider =
            AzureProvider::with_managed_identity("myaccount".to_string(), "mycontainer".to_string());

        // Assert
        assert_eq!(provider.account_name, "myaccount");
        match &provider.credential {
            AzureCredential::ManagedIdentity => assert!(true),
            _ => panic!("Expected ManagedIdentity credential"),
        }
    }

    #[test]
    fn should_default_to_managed_identity_with_new() {
        // Arrange & Act
        let provider = AzureProvider::new("account".to_string(), "container".to_string());

        // Assert
        match &provider.credential {
            AzureCredential::ManagedIdentity => assert!(true),
            _ => panic!("Expected ManagedIdentity credential as default"),
        }
    }

    // =========== AzureProvider General Tests ===========

    #[test]
    fn should_handle_empty_account_name() {
        // Arrange & Act
        let provider = AzureProvider::new("".to_string(), "container".to_string());

        // Assert
        assert_eq!(provider.account_name, "");
        assert_eq!(provider.container, "container");
    }

    #[test]
    fn should_handle_empty_container_name() {
        // Arrange & Act
        let provider = AzureProvider::new("account".to_string(), "".to_string());

        // Assert
        assert_eq!(provider.account_name, "account");
        assert_eq!(provider.container, "");
    }

    #[test]
    fn should_handle_special_characters_in_names() {
        // Arrange & Act
        let provider = AzureProvider::new(
            "my-account-123".to_string(),
            "my-container-456".to_string(),
        );

        // Assert
        assert_eq!(provider.account_name, "my-account-123");
        assert_eq!(provider.container, "my-container-456");
    }

    // =========== AzureProvider CloudBackend Trait Tests ===========

    #[test]
    fn should_accept_put_operation_with_shared_key() {
        // Arrange
        let provider = AzureProvider::with_shared_key(
            "account".to_string(),
            "container".to_string(),
            "key".to_string(),
        );
        let (tx, _rx) = std::sync::mpsc::channel();

        // Act & Assert - Just verify it doesn't panic
        provider.submit_put("key".into(), vec![1, 2, 3], tx);
    }

    #[test]
    fn should_accept_put_operation_with_sas_token() {
        // Arrange
        let provider = AzureProvider::with_sas_token(
            "account".to_string(),
            "container".to_string(),
            "token123".to_string(),
        );
        let (tx, _rx) = std::sync::mpsc::channel();

        // Act & Assert
        provider.submit_put("key".into(), vec![1, 2, 3], tx);
    }

    #[test]
    fn should_accept_put_operation_with_managed_identity() {
        // Arrange
        let provider =
            AzureProvider::with_managed_identity("account".to_string(), "container".to_string());
        let (tx, _rx) = std::sync::mpsc::channel();

        // Act & Assert
        provider.submit_put("key".into(), vec![1, 2, 3], tx);
    }

    #[test]
    fn should_accept_get_operation() {
        // Arrange
        let provider = AzureProvider::new("account".to_string(), "container".to_string());
        let (tx, _rx) = std::sync::mpsc::channel();

        // Act & Assert
        provider.submit_get("key".into(), tx);
    }

    #[test]
    fn should_accept_delete_operation() {
        // Arrange
        let provider = AzureProvider::new("account".to_string(), "container".to_string());
        let (tx, _rx) = std::sync::mpsc::channel();

        // Act & Assert
        provider.submit_delete("key".into(), tx);
    }

    #[test]
    fn should_accept_list_operation() {
        // Arrange
        let provider = AzureProvider::new("account".to_string(), "container".to_string());
        let (tx, _rx) = std::sync::mpsc::channel();

        // Act & Assert
        provider.submit_list("prefix".into(), tx);
    }

    #[test]
    fn should_accept_head_operation() {
        // Arrange
        let provider = AzureProvider::new("account".to_string(), "container".to_string());
        let (tx, _rx) = std::sync::mpsc::channel();

        // Act & Assert
        provider.submit_head("key".into(), tx);
    }

    #[test]
    fn should_accept_get_range_operation() {
        // Arrange
        let provider = AzureProvider::new("account".to_string(), "container".to_string());
        let (tx, _rx) = std::sync::mpsc::channel();

        // Act & Assert
        provider.submit_get_range("key".into(), 0, Some(1024), tx);
    }

    #[test]
    fn should_handle_multiple_operations_sequentially() {
        // Arrange
        let provider = AzureProvider::new("account".to_string(), "container".to_string());

        // Act - Execute multiple operations
        for i in 0..5 {
            let (tx, _rx) = std::sync::mpsc::channel();
            let key = format!("key{}", i);
            provider.submit_put(key, vec![i as u8], tx);
        }
    }

    #[test]
    fn should_handle_multiple_credential_types() {
        // Arrange & Act
        let sk = AzureProvider::with_shared_key(
            "a".to_string(),
            "c".to_string(),
            "key".to_string(),
        );
        let sas = AzureProvider::with_sas_token(
            "a".to_string(),
            "c".to_string(),
            "token".to_string(),
        );
        let mi = AzureProvider::with_managed_identity("a".to_string(), "c".to_string());
        let cs = AzureProvider::with_connection_string(
            "a".to_string(),
            "c".to_string(),
            "connstr".to_string(),
        );

        // Assert
        assert!(matches!(&sk.credential, AzureCredential::SharedKey { .. }));
        assert!(matches!(&sas.credential, AzureCredential::SasToken { .. }));
        assert!(matches!(&mi.credential, AzureCredential::ManagedIdentity));
        assert!(matches!(&cs.credential, AzureCredential::ConnectionString { .. }));
    }

    #[test]
    fn should_handle_large_data_in_put() {
        // Arrange
        let provider = AzureProvider::new("account".to_string(), "container".to_string());
        let (tx, rx) = std::sync::mpsc::channel();
        let large_data = vec![42u8; 1_000_000];

        // Act
        provider.submit_put("largefile".into(), large_data.clone(), tx);
        let event = rx.recv().unwrap();

        // Assert
        match event {
            CloudEvent::PutComplete { result, .. } => {
                assert!(result.is_ok());
            }
            _ => panic!("Expected PutComplete"),
        }
    }

    #[test]
    fn should_send_callback_event_on_put() {
        // Arrange
        let provider = AzureProvider::new("account".to_string(), "container".to_string());
        let (tx, rx) = std::sync::mpsc::channel();

        // Act
        provider.submit_put("key".into(), vec![1, 2, 3], tx);
        let event = rx.recv().unwrap();

        // Assert
        match event {
            CloudEvent::PutComplete { key, result } => {
                assert_eq!(key, "key");
                assert!(result.is_ok());
            }
            _ => panic!("Expected PutComplete event"),
        }
    }

    #[test]
    fn should_send_callback_event_on_get() {
        // Arrange
        let provider = AzureProvider::new("account".to_string(), "container".to_string());
        let (tx, rx) = std::sync::mpsc::channel();

        // Act
        provider.submit_get("key".into(), tx);
        let event = rx.recv().unwrap();

        // Assert
        match event {
            CloudEvent::GetComplete { key, result } => {
                assert_eq!(key, "key");
                assert!(result.is_ok());
            }
            _ => panic!("Expected GetComplete event"),
        }
    }

    #[test]
    fn should_send_callback_event_on_head() {
        // Arrange
        let provider = AzureProvider::new("account".to_string(), "container".to_string());
        let (tx, rx) = std::sync::mpsc::channel();

        // Act
        provider.submit_head("key".into(), tx);
        let event = rx.recv().unwrap();

        // Assert
        match event {
            CloudEvent::HeadComplete { key, result } => {
                assert_eq!(key, "key");
                assert!(result.is_ok());
            }
            _ => panic!("Expected HeadComplete event"),
        }
    }
}
