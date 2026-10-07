use super::{CloudStartupRecovery, RuntimeRecoveryMaterialization, RuntimeStorageMaterialization};
use crate::common::{DeadlineScope, MidgeError, MidgeResult, OperationDeadline};
use crate::config::RecoveryPolicy;
use crate::runtime::ddl::DdlLeaseAuthority;
use crate::runtime::hybrid_persistence::CloudMetadataMirrorAuthority;
use crate::runtime::RuntimeState;
use std::path::Path;
use std::sync::Arc;

impl RuntimeRecoveryMaterialization {
    fn evict_resident_manifest_ssts(
        materialized: &mut RuntimeStorageMaterialization,
    ) -> MidgeResult<()> {
        let Some(fs) = &materialized.runtime_config.sst_read_fs else {
            return Ok(());
        };
        let fs = crate::telemetry::recovery_progress::observe_reads(Arc::clone(fs));
        let Some(storage) = &materialized.runtime_config.hybrid_storage else {
            return Ok(());
        };
        let mut salvaged = Vec::new();
        for meta in &materialized.state.manifest.files {
            materialized
                .state
                .check_startup_scope("resident SST migration")?;
            if materialized.state.salvaged_local_ssts.contains(&meta.name) {
                continue;
            }
            let path = materialized.state.sst_dir.join(&meta.name);
            let secondary = materialized
                .state
                .db_path
                .join("hybrid_local/sst")
                .join(&meta.name);
            if !path.exists() && !secondary.exists() {
                continue;
            }
            // This migration path reads only objects which already have a
            // resident copy. An empty cache never triggers full SST validation.
            // Retain the local bytes unless the remote publication proof holds.
            let validation = RuntimeState::validate_sst_fs_proof(
                Arc::clone(&fs),
                &crate::runtime::FileMeta::from(meta),
            );
            materialized
                .state
                .check_startup_scope("resident SST migration proof")?;
            if let Err(error) = validation {
                if matches!(error, MidgeError::Timeout(_)) {
                    return Err(error);
                }
                if materialized.state.recovery_policy() == RecoveryPolicy::Salvage
                    && CloudStartupRecovery::retain_verified_local_sst(&materialized.state, meta)?
                {
                    tracing::warn!(
                        %error,
                        sst_name = %meta.name,
                        "retaining verified local SST during salvage after remote migration proof failed"
                    );
                    salvaged.push(meta.name.clone());
                    continue;
                }
                return Err(error);
            }
            if path.exists() {
                materialized
                    .state
                    .check_startup_scope("resident SST eviction")?;
                std::fs::remove_file(path)?;
                materialized
                    .state
                    .check_startup_scope("resident SST eviction")?;
            }
            storage.evict_local_object_cache(&crate::cloud_layout::object_key(&meta.name))?;
        }
        if !salvaged.is_empty() {
            materialized.state.salvaged_local_ssts.extend(salvaged);
            materialized.state.mark_opened_in_salvage_mode();
            materialized.state.mark_persistence_anomaly();
        }
        Ok(())
    }

    pub(super) fn local_directory_bytes_within(
        path: &Path,
        scope: Option<&DeadlineScope>,
    ) -> MidgeResult<u64> {
        check_scope(scope, "startup storage accounting")?;
        let entries = match std::fs::read_dir(path) {
            Ok(entries) => entries,
            Err(error) if error.kind() == std::io::ErrorKind::NotFound => return Ok(0),
            Err(error) => return Err(error.into()),
        };
        let mut total = 0_u64;
        for entry in entries {
            check_scope(scope, "startup storage accounting entry")?;
            let entry = entry?;
            let kind = entry.file_type()?;
            let bytes = if kind.is_dir() {
                Self::local_directory_bytes_within(&entry.path(), scope)?
            } else if kind.is_file() {
                entry.metadata()?.len()
            } else {
                0
            };
            total = total.checked_add(bytes).ok_or_else(|| {
                crate::common::MidgeError::ResourceLimit("local storage accounting overflow".into())
            })?;
        }
        check_scope(scope, "startup storage accounting completion")?;
        Ok(total)
    }

    fn prepare_storage_for_wal_replay(
        materialized: &mut RuntimeStorageMaterialization,
        db_path: &Path,
    ) -> MidgeResult<()> {
        materialized
            .state
            .check_startup_scope("storage residue cleanup")?;
        materialized.state.cleanup_storage_residue_within()?;
        materialized
            .state
            .check_startup_scope("storage residue cleanup")?;
        Self::evict_resident_manifest_ssts(materialized)?;
        if !materialized.state.salvaged_local_ssts.is_empty() {
            if let Some(storage) = &materialized.runtime_config.hybrid_storage {
                let fs: Arc<dyn crate::io::Fs> = Arc::new(
                    crate::storage::remote_sst::RemoteSstFs::new(
                        Arc::clone(&materialized.state.fs),
                        storage.remote_sst_backend(),
                        storage.storage_io_timeout(),
                    )
                    .with_verified_local_overrides(materialized.state.salvaged_local_ssts.clone()),
                );
                materialized.runtime_config.sst_read_fs = Some(Arc::clone(&fs));
                materialized.state.recovery_sst_fs =
                    Some(crate::telemetry::recovery_progress::observe_reads(fs));
            }
        }
        if let Some(storage) = &materialized.runtime_config.hybrid_storage {
            storage.reconcile_local_disk_usage(
                Self::local_directory_bytes_within(
                    &materialized.state.sst_dir,
                    materialized.state.startup_scope.as_ref(),
                )?
                .saturating_add(Self::local_directory_bytes_within(
                    &db_path.join("hybrid_local/sst"),
                    materialized.state.startup_scope.as_ref(),
                )?),
                Self::local_directory_bytes_within(
                    &materialized.state.wal_dir,
                    materialized.state.startup_scope.as_ref(),
                )?
                .saturating_add(Self::local_directory_bytes_within(
                    &db_path.join("hybrid_local/wal"),
                    materialized.state.startup_scope.as_ref(),
                )?),
            );
            storage.reconcile_startup_scratch_residue(
                materialized.state.retained_startup_scratch_bytes()?,
            )?;
        }
        Ok(())
    }

    pub(super) fn replay_and_repair(
        materialized: RuntimeStorageMaterialization,
        db_path: &Path,
        recovery_policy: RecoveryPolicy,
    ) -> MidgeResult<Self> {
        let scope = materialized
            .state
            .startup_scope
            .clone()
            .unwrap_or_else(|| DeadlineScope::new(OperationDeadline::unbounded()));
        Self::replay_and_repair_within(materialized, db_path, recovery_policy, &scope)
    }

    pub(super) fn replay_and_repair_within(
        mut materialized: RuntimeStorageMaterialization,
        db_path: &Path,
        recovery_policy: RecoveryPolicy,
        scope: &DeadlineScope,
    ) -> MidgeResult<Self> {
        scope.check("recovery repair")?;
        materialized.state.recovery_sst_fs = materialized
            .runtime_config
            .sst_read_fs
            .as_ref()
            .map(|fs| crate::telemetry::recovery_progress::observe_reads(Arc::clone(fs)));
        let remote_cleanup_candidates = materialized
            .state
            .non_authoritative_compaction_outputs_for_remote_cleanup()?;
        let remote_cleanup_names =
            if let Some(storage) = materialized.runtime_config.hybrid_storage.as_deref() {
                CloudStartupRecovery::cleanup_non_authoritative_compaction_outputs(
                    &mut materialized.state,
                    storage,
                    &remote_cleanup_candidates,
                )?
            } else {
                std::collections::BTreeSet::new()
            };

        if let Some(cloud_storage) = materialized.cloud_storage_for_restore.as_deref() {
            let sst_proofs = CloudStartupRecovery::cloud_recovery_sst_proofs_for_intent_replay(
                &materialized.state,
            )
            .into_iter()
            .filter(|proof| !remote_cleanup_names.contains(&proof.name));
            CloudStartupRecovery::ensure_named_sst_cache_from_cloud_storage(
                &mut materialized.state,
                cloud_storage,
                sst_proofs,
            )?;
        }

        reconcile_cloud_ddl_within(&mut materialized, scope)?;

        materialized.state.replay_intent_log()?;
        if let Some(root) = materialized.cloud_root.as_deref() {
            CloudStartupRecovery::ensure_local_sst_cache_from_cloud(&mut materialized.state, root)?;
        }
        if let Some(cloud_storage) = materialized.cloud_storage_for_restore.as_deref() {
            CloudStartupRecovery::ensure_local_sst_cache_from_cloud_storage(
                &mut materialized.state,
                cloud_storage,
            )?;
        }
        if let Some(metadata_storage) = materialized.cloud_metadata_storage_for_mirror.as_deref() {
            let config = &materialized.runtime_config;
            CloudStartupRecovery::mirror_cloud_metadata_within(
                metadata_storage,
                db_path,
                recovery_policy,
                CloudMetadataMirrorAuthority {
                    store: config.leader_store.as_deref().ok_or_else(|| {
                        MidgeError::Internal(
                            "cloud metadata publication requires a leader store".into(),
                        )
                    })?,
                    holder_id: config.leader_holder_id.as_deref().ok_or_else(|| {
                        MidgeError::Internal(
                            "cloud metadata publication requires a lease holder".into(),
                        )
                    })?,
                    writer_epoch: config.writer_epoch,
                },
                &config.metadata_publication_lock,
                |_| super::streaming_recovery::validate_lease(config),
                scope,
            )?;
        }

        if materialized.runtime_config.wal_durability_policy
            == crate::wal::DurabilityPolicy::CloudAsync
        {
            materialized
                .state
                .reset_cloud_durable_sequence_for_recovery();
        }

        Self::prepare_storage_for_wal_replay(&mut materialized, db_path)?;
        if let Some(replay) = materialized.streaming_wal.take() {
            super::timing::measure("wal_replay", || replay.replay(&mut materialized))?;
        }
        // Legacy hybrid_local SST copies have been swept above. Replay may
        // publish new SSTs, so retire the secondary backend only afterward.
        if let Some(storage) = &materialized.runtime_config.hybrid_storage {
            storage.retire_legacy_local_store();
        }
        let recovered_sequence = materialized.state.sequence;
        let recovered_cf_metas = materialized.state.manifest.column_families.clone();

        scope.check("recovery repair completion")?;
        Ok(Self {
            state: materialized.state,
            runtime_config: materialized.runtime_config,
            recovered_sequence,
            recovered_cf_metas,
        })
    }
}

fn check_scope(scope: Option<&DeadlineScope>, context: &str) -> MidgeResult<()> {
    scope.map_or(Ok(()), |scope| scope.check(context))
}

fn reconcile_cloud_ddl_within(
    materialized: &mut RuntimeStorageMaterialization,
    scope: &DeadlineScope,
) -> MidgeResult<()> {
    let authority = if materialized.cloud_metadata_storage_for_mirror.is_some() {
        let config = &materialized.runtime_config;
        Some(DdlLeaseAuthority {
            store: config.leader_store.clone().ok_or_else(|| {
                MidgeError::Internal("cloud DDL recovery requires a leader store".into())
            })?,
            holder_id: config.leader_holder_id.clone().ok_or_else(|| {
                MidgeError::Internal("cloud DDL recovery requires a lease holder".into())
            })?,
            writer_epoch: config.writer_epoch,
        })
    } else {
        None
    };
    crate::runtime::ddl::reconcile_startup_within(
        &mut materialized.state,
        materialized.runtime_config.hybrid_storage.as_ref(),
        authority.as_ref(),
        scope,
    )
}
