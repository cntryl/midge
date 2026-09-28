use super::{BlockingCloudIo, CloudStartupRecovery, MidgeError, MidgeResult, RecoveryPolicy};
use crate::io::FsError;
use std::path::Path;
use std::sync::Arc;

impl CloudStartupRecovery {
    pub(crate) fn load_local_manifest_for_cloud_metadata_mirror(
        db_path: &Path,
    ) -> MidgeResult<crate::metadata::Manifest> {
        let fs: Arc<dyn crate::io::traits::Fs> =
            Arc::new(crate::io::RealFs::new(db_path).map_err(FsError::into_midge)?);
        crate::metadata::ManifestPersistence::load_with_fs_and_policy(&fs, RecoveryPolicy::Strict)
            .map_err(MidgeError::Internal)
    }

    pub(crate) fn ensure_remote_manifest_metadata_not_ahead(
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

    pub(crate) fn blocking_conditional_cloud_metadata_put(
        cloud: &crate::storage::cloud::CloudStorage,
        file_name: &str,
        data: Vec<u8>,
        local_manifest_sequence: u64,
    ) -> MidgeResult<()> {
        crate::runtime::hybrid_persistence::conditional_metadata_mirror_put(
            cloud,
            file_name,
            data,
            local_manifest_sequence,
            &crate::common::OperationDeadline::unbounded(),
        )
    }
}
