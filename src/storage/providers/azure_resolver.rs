use std::sync::Arc;

use super::{AzureCredentialSource, CloudProviderConfig};
use crate::common::{MidgeError, MidgeResult};
use crate::storage::cloud::CloudBackend;

pub(super) fn try_resolve(
    provider: &CloudProviderConfig,
) -> MidgeResult<Option<Arc<dyn CloudBackend>>> {
    let CloudProviderConfig::AzureBlob(_) = provider else {
        return Ok(None);
    };

    Ok(Some(resolve_azure(provider)?))
}

fn resolve_azure(provider: &CloudProviderConfig) -> MidgeResult<Arc<dyn CloudBackend>> {
    let CloudProviderConfig::AzureBlob(config) = provider else {
        return Err(MidgeError::InvalidArgument(
            "expected an Azure provider".to_string(),
        ));
    };

    let provider = match &config.credential {
        AzureCredentialSource::SharedKey { account_key } => {
            super::azure::AzureProvider::with_shared_key_and_endpoint(
                config.account.clone(),
                config.container.clone(),
                account_key,
                config.endpoint.clone(),
            )?
        }
        AzureCredentialSource::SasToken { token } => {
            super::azure::AzureProvider::with_sas_token_and_endpoint(
                config.account.clone(),
                config.container.clone(),
                token,
                config.endpoint.clone(),
            )?
        }
        AzureCredentialSource::ConnectionString { connection_string } => {
            super::azure::AzureProvider::from_connection_string_and_endpoint(
                connection_string,
                config.container.clone(),
                config.endpoint.clone(),
            )?
        }
        AzureCredentialSource::StorageEnvironment => {
            super::azure::AzureProvider::from_env_and_endpoint(
                config.account.clone(),
                config.container.clone(),
                config.endpoint.clone(),
            )?
        }
        AzureCredentialSource::ManagedIdentity { client_id } => {
            super::azure::AzureProvider::with_managed_identity_and_endpoint(
                config.account.clone(),
                config.container.clone(),
                client_id.clone(),
                config.endpoint.clone(),
            )?
        }
        AzureCredentialSource::EnvironmentClientSecret
        | AzureCredentialSource::WorkloadIdentity { .. }
        | AzureCredentialSource::LightweightDefaultChain => {
            super::azure::AzureProvider::from_lightweight_credential_source(
                config.account.clone(),
                config.container.clone(),
                config.endpoint.clone(),
                config.credential.clone(),
            )?
        }
    };

    Ok(provider.backend())
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn should_accept_account_scoped_https_endpoint_given_managed_identity() {
        // Arrange
        let secure = CloudProviderConfig::azure_blob("account", "container")
            .with_azure_credentials(AzureCredentialSource::managed_identity())
            .expect("Azure credential override")
            .with_endpoint("https://account.blob.core.usgovcloudapi.net")
            .expect("Azure endpoint override");
        let insecure = CloudProviderConfig::azure_blob("account", "container")
            .with_azure_credentials(AzureCredentialSource::managed_identity())
            .expect("Azure credential override")
            .with_endpoint("http://account.blob.core.usgovcloudapi.net")
            .expect("Azure endpoint override");

        // Act
        let secure_result = resolve_azure(&secure);
        let insecure_result = resolve_azure(&insecure);

        // Assert: only the HTTPS sovereign identity endpoint resolves; the same origin over
        // plain HTTP must be rejected, proving the accept path is actually discriminating on
        // scheme rather than always succeeding.
        assert!(
            secure_result.is_ok(),
            "sovereign identity endpoint must resolve"
        );
        match &insecure_result {
            Err(MidgeError::InvalidArgument(_)) => {}
            Err(other) => panic!("expected InvalidArgument, got {other}"),
            Ok(_) => panic!("insecure identity endpoint must be rejected"),
        }
    }

    #[test]
    fn should_accept_account_scoped_https_endpoint_given_workload_identity() {
        // Arrange
        let credentials = || {
            AzureCredentialSource::workload_identity_with(
                Some("tenant".to_string()),
                Some("client".to_string()),
                Some(std::path::PathBuf::from("unused-federated-token")),
            )
        };
        let secure = CloudProviderConfig::azure_blob("account", "container")
            .with_azure_credentials(credentials())
            .expect("Azure credential override")
            .with_endpoint("https://account.blob.core.chinacloudapi.cn")
            .expect("Azure endpoint override");
        let insecure = CloudProviderConfig::azure_blob("account", "container")
            .with_azure_credentials(credentials())
            .expect("Azure credential override")
            .with_endpoint("http://account.blob.core.chinacloudapi.cn")
            .expect("Azure endpoint override");

        // Act
        let secure_result = resolve_azure(&secure);
        let insecure_result = resolve_azure(&insecure);

        // Assert: same discrimination as the managed-identity case, exercised through the
        // workload-identity branch of resolve_azure.
        assert!(
            secure_result.is_ok(),
            "sovereign OAuth endpoint must resolve"
        );
        match &insecure_result {
            Err(MidgeError::InvalidArgument(_)) => {}
            Err(other) => panic!("expected InvalidArgument, got {other}"),
            Ok(_) => panic!("insecure OAuth endpoint must be rejected"),
        }
    }

    #[test]
    fn should_reject_insecure_endpoint_given_managed_identity() {
        // Arrange
        let provider = CloudProviderConfig::azure_blob("account", "container")
            .with_azure_credentials(AzureCredentialSource::managed_identity())
            .expect("Azure credential override")
            .with_endpoint("http://account.blob.core.windows.net")
            .expect("Azure endpoint override");

        // Act
        let result = resolve_azure(&provider);

        // Assert
        assert!(matches!(
            result,
            Err(MidgeError::InvalidArgument(message)) if message.contains("identity")
        ));
    }

    #[test]
    fn should_preserve_path_style_http_endpoint_given_shared_key() {
        // Arrange
        use crate::storage::providers::test_support::spawn_recording_http_server;
        use crate::AzureBlobConfig;

        let server = spawn_recording_http_server(Vec::new(), Vec::new());
        let provider: CloudProviderConfig = AzureBlobConfig::new("admin", "container")
            .with_endpoint(server.endpoint.clone())
            .with_credentials(AzureCredentialSource::shared_key("easy-peasy"))
            .into();

        // Act
        let backend = resolve_azure(&provider).expect("shared-key HTTP endpoint must resolve");
        let (sender, receiver) = std::sync::mpsc::channel();
        backend.submit_put("blob", b"value".to_vec(), Vec::new(), sender);
        let _ = receiver.recv_timeout(std::time::Duration::from_secs(5));
        let request = server.finish();

        // Assert: shared-key credentials keep path-style emulator addressing, so the request
        // target embeds account and container in the path rather than as a virtual-hosted
        // subdomain.
        assert!(
            request.target.starts_with("/admin/container/"),
            "expected path-style target with account and container, got: {}",
            request.target
        );
    }
}
