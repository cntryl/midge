use std::sync::Arc;

use super::factory::CloudProviderResolver;
use crate::common::{MidgeError, MidgeResult};
use crate::config::{AzureCredentialSource, CloudProviderConfig};
use crate::storage::cloud::CloudBackend;

pub(super) struct AzureProviderResolver;

impl CloudProviderResolver for AzureProviderResolver {
    fn resolve(&self, provider: &CloudProviderConfig) -> MidgeResult<Arc<dyn CloudBackend>> {
        let CloudProviderConfig::AzureBlob {
            account,
            container,
            endpoint,
            credential,
        } = provider
        else {
            return Err(MidgeError::InvalidArgument(
                "expected an Azure provider".to_string(),
            ));
        };

        let provider = match credential {
            AzureCredentialSource::SharedKey { account_key } => {
                super::azure::AzureProvider::with_shared_key_and_endpoint(
                    account.clone(),
                    container.clone(),
                    account_key.clone(),
                    endpoint.clone(),
                )?
            }
            AzureCredentialSource::SasToken { token } => {
                super::azure::AzureProvider::with_sas_token_and_endpoint(
                    account.clone(),
                    container.clone(),
                    token.clone(),
                    endpoint.clone(),
                )?
            }
            AzureCredentialSource::ConnectionString { connection_string } => {
                super::azure::AzureProvider::from_connection_string_and_endpoint(
                    connection_string.clone(),
                    container.clone(),
                    endpoint.clone(),
                )?
            }
            AzureCredentialSource::StorageEnvironment => {
                super::azure::AzureProvider::from_env_and_endpoint(
                    account.clone(),
                    container.clone(),
                    endpoint.clone(),
                )?
            }
            AzureCredentialSource::ManagedIdentity { client_id } => {
                if endpoint.is_some() {
                    return Err(MidgeError::InvalidArgument(
                        "managed identity cannot be used with an emulator endpoint".to_string(),
                    ));
                }
                super::azure::AzureProvider::with_managed_identity(
                    account.clone(),
                    container.clone(),
                    client_id.clone(),
                )?
            }
            AzureCredentialSource::EnvironmentClientSecret
            | AzureCredentialSource::WorkloadIdentity { .. }
            | AzureCredentialSource::LightweightDefaultChain => {
                if endpoint.is_some() {
                    return Err(MidgeError::InvalidArgument(
                        "Azure OAuth credential sources cannot be used with an emulator endpoint"
                            .to_string(),
                    ));
                }
                super::azure::AzureProvider::from_lightweight_credential_source(
                    account.clone(),
                    container.clone(),
                    credential.clone(),
                )?
            }
        };

        Ok(provider.backend())
    }
}
