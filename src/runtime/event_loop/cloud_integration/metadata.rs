//! Remote metadata convergence and guarded cleanup proofs.

use super::super::EventLoop;
use crate::runtime::hybrid_persistence::{CloudMetadataPruneGuard, CloudMetadataPruneProof};

impl EventLoop {
    pub(super) fn cloud_metadata_prune_guard_for_wal_cleanup_with_convergence(
        &mut self,
    ) -> Result<Option<CloudMetadataPruneGuard>, String> {
        match self.cloud_metadata_prune_guard_for_wal_cleanup() {
            Ok(guard) => Ok(guard),
            Err(error) if Self::is_convergeable_cloud_metadata_mismatch(&error) => {
                tracing::warn!(
                    error = %error,
                    "Cloud metadata mismatch blocked WAL prune; attempting metadata mirror before retry"
                );
                self.cloud_metadata_cleanup_proofs.clear();
                self.mirror_metadata_after_local_commit("cloud WAL prune metadata convergence")
                    .map_err(|mirror_error| {
                        format!("{error}; metadata mirror before WAL prune failed: {mirror_error}")
                    })?;
                self.cloud_metadata_cleanup_proofs.clear();
                self.cloud_metadata_prune_guard_for_wal_cleanup()
                    .map_err(|retry_error| {
                        format!(
                            "{error}; metadata still not verifiable after mirror: {retry_error}"
                        )
                    })
            }
            Err(error) => Err(error),
        }
    }

    fn is_convergeable_cloud_metadata_mismatch(error: &str) -> bool {
        error.contains("does not match committed local metadata")
    }

    #[cfg(test)]
    pub(super) fn verify_cloud_metadata_for_wal_cleanup(&mut self) -> Result<(), String> {
        self.cloud_metadata_prune_guard_for_wal_cleanup()
            .map(|_| ())
    }

    fn cloud_metadata_prune_guard_for_wal_cleanup(
        &mut self,
    ) -> Result<Option<CloudMetadataPruneGuard>, String> {
        let Some(cloud) = self.cloud_metadata_storage.as_ref() else {
            return Ok(None);
        };

        let mut objects = Vec::new();

        for file_name in crate::storage::cloud::CLOUD_METADATA_FILES {
            let file_name = *file_name;
            let local_path = self.state.db_path.join(file_name);
            if !local_path.exists() {
                continue;
            }
            let local_data = std::fs::read(&local_path).map_err(|error| {
                format!(
                    "local metadata '{}' is unreadable: {error}",
                    local_path.display()
                )
            })?;
            let key = crate::storage::cloud::cloud_metadata_key(file_name);
            let local_len = local_data.len() as u64;
            let local_crc32c = crc32c::crc32c(&local_data);
            if let Some(proof) = self.cloud_metadata_cleanup_proofs.get(file_name) {
                if proof.len == local_len && proof.crc32c == local_crc32c {
                    let actual = crate::storage::cloud::blocking_cloud_object_proof(cloud, &key)?
                        .ok_or_else(|| {
                        format!(
                            "cloud metadata '{key}' disappeared during cached proof revalidation"
                        )
                    })?;
                    if actual.bytes == local_data && actual.metadata == proof.remote {
                        objects.push(CloudMetadataPruneProof {
                            key,
                            expected_bytes: local_data,
                            remote: proof.remote.clone(),
                        });
                        continue;
                    }
                    if actual.bytes != local_data {
                        return Err(format!(
                            "cloud metadata '{key}' does not match committed local metadata"
                        ));
                    }
                    return Err(format!(
                        "cloud metadata '{key}' changed since validation: expected {:?}, actual {:?}",
                        proof.remote, actual.metadata
                    ));
                }
            }
            self.cloud_metadata_cleanup_proofs.remove(file_name);

            let cloud_proof = crate::storage::cloud::blocking_cloud_object_proof(cloud, &key)?
                .ok_or_else(|| format!("cloud metadata '{key}' is missing"))?;
            if cloud_proof.bytes != local_data {
                return Err(format!(
                    "cloud metadata '{key}' does not match committed local metadata"
                ));
            }
            self.cloud_metadata_cleanup_proofs.insert(
                file_name.to_string(),
                super::super::MetadataCleanupProof {
                    len: local_len,
                    crc32c: local_crc32c,
                    remote: cloud_proof.metadata.clone(),
                },
            );
            objects.push(CloudMetadataPruneProof {
                key,
                expected_bytes: local_data,
                remote: cloud_proof.metadata,
            });
        }

        Ok(Some(CloudMetadataPruneGuard::new(cloud.clone(), objects)))
    }
}
