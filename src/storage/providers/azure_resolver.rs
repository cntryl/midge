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
        let provider = CloudProviderConfig::azure_blob("account", "container")
            .with_azure_credentials(AzureCredentialSource::managed_identity())
            .expect("Azure credential override")
            .with_endpoint("https://account.blob.core.usgovcloudapi.net")
            .expect("Azure endpoint override");

        // Act
        let result = resolve_azure(&provider);

        // Assert
        assert!(result.is_ok(), "sovereign identity endpoint must resolve");
    }

    #[test]
    fn should_accept_account_scoped_https_endpoint_given_workload_identity() {
        // Arrange
        let provider = CloudProviderConfig::azure_blob("account", "container")
            .with_azure_credentials(AzureCredentialSource::workload_identity_with(
                Some("tenant".to_string()),
                Some("client".to_string()),
                Some(std::path::PathBuf::from("unused-federated-token")),
            ))
            .expect("Azure credential override")
            .with_endpoint("https://account.blob.core.chinacloudapi.cn")
            .expect("Azure endpoint override");

        // Act
        let result = resolve_azure(&provider);

        // Assert
        assert!(result.is_ok(), "sovereign OAuth endpoint must resolve");
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
        let provider = CloudProviderConfig::sqrzl_azure("container");

        // Act
        let result = resolve_azure(&provider);

        // Assert
        assert!(result.is_ok(), "Sqrzl Azure endpoint must resolve");
    }
}
