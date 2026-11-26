use super::{BlockingCloudIo, CloudStartupRecovery, MidgeError, MidgeResult, RecoveryPolicy};
use cntryl_midge_core::common::DeadlineScope;
use std::collections::BTreeMap;
use std::sync::Arc;

impl CloudStartupRecovery {
    pub(crate) fn reject_legacy_cloud_metadata_without_generation_within(
        cloud: &cntryl_midge_storage::cloud::CloudStorage,
        scope: &DeadlineScope,
    ) -> MidgeResult<()> {
        for file_name in cntryl_midge_durability::metadata::files::CLOUD_MIRRORED
            .iter()
            .copied()
            .chain(std::iter::once(cntryl_midge_durability::metadata::files::MANIFEST))
        {
            scope.check("legacy cloud metadata inventory")?;
            let key = cntryl_midge_core::cloud_layout::CloudObjectLayout::metadata_key(file_name);
            if BlockingCloudIo::within(cloud, scope)
                .head_optional(&key)?
                .is_some()
            {
                return Err(MidgeError::RecoveryFailed(format!(
                    "cloud metadata '{key}' has no committed lease generation; this format is unsupported"
                )));
            }
        }
        Ok(())
    }

    pub(crate) fn read_committed_cloud_metadata_within(
        cloud: &cntryl_midge_storage::cloud::CloudStorage,
        generation: &cntryl_midge_durability::lease::CloudMetadataGeneration,
        scope: &DeadlineScope,
    ) -> MidgeResult<BTreeMap<String, Vec<u8>>> {
        scope.check("committed cloud metadata validation")?;
        let bodies = crate::runtime::hybrid_persistence::read_committed_metadata_bodies_within(
            cloud,
            generation,
            &scope.deadline(),
        )?;
        crate::runtime::hybrid_persistence::decode_generation_manifest(
            generation,
            &bodies,
            &scope.deadline(),
        )?;
        scope.check("committed cloud metadata validation")?;
        Ok(bodies)
    }

    pub(crate) fn load_local_manifest_for_cloud_metadata_mirror_within(
        root: &Arc<dyn cntryl_midge_core::io::traits::Fs>,
        scope: &DeadlineScope,
    ) -> MidgeResult<cntryl_midge_durability::metadata::Manifest> {
        scope.check("local cloud mirror manifest")?;
        let fs = cntryl_midge_core::io::scope_fs(Arc::clone(root), scope.clone());
        cntryl_midge_durability::metadata::ManifestPersistence::load_with_fs_and_policy_within(
            &fs,
            RecoveryPolicy::Strict,
            scope,
        )
    }
}
