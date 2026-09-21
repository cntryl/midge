use super::{BlockingCloudIo, CloudStartupRecovery, MidgeError, MidgeResult, RecoveryPolicy};
use std::path::Path;
use std::sync::Arc;

impl CloudStartupRecovery {
    pub(super) fn load_local_manifest_for_cloud_metadata_mirror(
        db_path: &Path,
        recovery_policy: RecoveryPolicy,
    ) -> MidgeResult<crate::metadata::Manifest> {
        let fs: Arc<dyn crate::io::traits::Fs> = Arc::new(crate::io::RealFs::new(db_path)?);
        crate::metadata::ManifestPersistence::load_with_fs_and_policy(&fs, recovery_policy)
            .map_err(MidgeError::Internal)
    }

    pub(super) fn ensure_remote_manifest_metadata_not_ahead(
        cloud: &crate::storage::cloud::CloudStorage,
        local_sequence: u64,
    ) -> MidgeResult<()> {
        for file_name in crate::metadata::files::MANIFEST_BODIES {
            let key = crate::cloud_layout::CloudObjectLayout::metadata_key(file_name);
            let Some(data) = BlockingCloudIo::new(cloud).get_optional(&key)? else {
                continue;
            };
            crate::metadata::files::ensure_remote_not_ahead(file_name, &data, local_sequence)?;
        }

        Ok(())
    }

    pub(super) fn blocking_conditional_cloud_metadata_put(
        cloud: &crate::storage::cloud::CloudStorage,
        file_name: &str,
        key: &str,
        data: Vec<u8>,
        local_manifest_sequence: u64,
    ) -> MidgeResult<()> {
        let io = BlockingCloudIo::new(cloud);
        let headers = match io.head_optional(key)? {
            Some(metadata) => {
                let headers = crate::storage::cloud::object_match_precondition_headers(
                    &metadata.etag,
                    metadata.generation.as_deref(),
                )
                .ok_or_else(|| {
                    MidgeError::Internal(format!(
                        "cloud metadata '{key}' cannot be conditionally updated without an identity token"
                    ))
                })?;
                let current = io.get_optional(key)?.ok_or_else(|| {
                    MidgeError::Internal(format!(
                        "cloud metadata '{key}' disappeared after HEAD precondition"
                    ))
                })?;
                crate::metadata::files::ensure_remote_not_ahead(
                    file_name,
                    &current,
                    local_manifest_sequence,
                )?;
                if current == data {
                    return Ok(());
                }
                headers
            }
            None => vec![("If-None-Match".to_string(), "*".to_string())],
        };

        io.put_with_headers(key, data, headers)
    }
}
