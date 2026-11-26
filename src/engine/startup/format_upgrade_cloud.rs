//! Explicit cloud FORMAT migration. No runtime or DDL/WAL fencing starts
//! until the new metadata authority and compatibility gate are resolved.

use super::cloud_format_upgrade::{
    reconcile_cloud_format_gate, source_payload_sha256, stage_candidate_descriptor,
    stage_source_descriptor, CloudFormatGateResolution,
};
use super::{epoch_floor::StartupEpochFloor, StartupLease, StartupStoragePath};
use crate::config::Storage;
use crate::engine::OpenOptions;
use crate::runtime::hybrid_persistence::{
    publish_format10_closure, read_committed_metadata_bodies_within,
};
use crate::runtime::{IntentPersistence, MetadataPublicationLock};
use cntryl_midge_core::common::{DeadlineScope, MidgeError, MidgeResult, OperationDeadline};
use cntryl_midge_core::io::{Fs, FsError, FsPath};
use cntryl_midge_durability::lease::{
    CloudFormatMigrationCandidate, CloudFormatMigrationPending, CloudMetadataGeneration,
    CloudMetadataGenerationIdentity, CloudMetadataHead, LeaderStore,
};
use cntryl_midge_durability::metadata::local_authority::LocalMetadataAuthority;
use cntryl_midge_durability::metadata::{codec, files, ManifestPersistence};
use cntryl_midge_storage::cloud::CloudStorage;
use std::{collections::BTreeMap, sync::Arc, time::Duration};

pub(super) fn upgrade_cloud(opts: &OpenOptions, timeout: Duration) -> MidgeResult<()> {
    let scope = DeadlineScope::new(OperationDeadline::from_budget(timeout));
    scope.check("cloud FORMAT upgrade startup")?;
    let path = StartupStoragePath::resolve(opts)?;
    let floor = StartupEpochFloor::discover_within(opts, &path, &scope)?;
    let lease = StartupLease::acquire_within(opts, &path, floor, &scope)?;
    let cloud = metadata_store(opts, &path, &scope)?;
    let store = lease.leader_store.as_deref().ok_or_else(|| {
        MidgeError::Fenced("cloud FORMAT upgrade requires an acquired provider lease".into())
    })?;
    let holder = lease.lease.holder_id();
    let epoch = lease.writer_epoch;
    let deadline = scope.deadline();
    let current = reconcile_cloud_format_gate(&cloud, store, &holder, epoch, &deadline)?;
    if current == CloudFormatGateResolution::Format10Selected {
        return Ok(());
    }
    let source = read_source(store, &holder, epoch, &deadline)?;
    check_legacy_closure_budget(&source)?;
    let bodies = read_committed_metadata_bodies_within(&cloud, &source, &deadline)?;
    let memory = MetadataPublicationLock::default();
    memory.configure_capture_memory(
        cntryl_midge_core::common::resource_budget::ResourceBudget::new(
            opts.compaction_memory_pool_size(),
        ),
    );
    let admission = memory.reserve_metadata_body_memory(encoded_size(&bodies)?)?;
    let source_sha256 = source_payload_sha256(&bodies);
    let candidate_bodies = prepare_candidate(bodies, &source_sha256, &scope)?;
    drop(admission);
    let admission = memory.reserve_metadata_body_memory(encoded_size(&candidate_bodies)?)?;
    let operation = uuid::Uuid::new_v4();
    let (manifest_horizon, objects) = publish_format10_closure(
        &cloud,
        &deadline,
        operation,
        epoch,
        &candidate_bodies,
        None,
        &admission,
    )?;
    let candidate = CloudMetadataGeneration {
        id: source.id.checked_next().ok_or_else(|| {
            MidgeError::ResourceLimit("cloud metadata generation exhausted".into())
        })?,
        manifest_horizon,
        objects,
    };
    let migration_id = operation.to_string();
    let pending = CloudFormatMigrationPending {
        target_format: 10,
        source: CloudMetadataGenerationIdentity::from_generation(&source),
        candidate: CloudFormatMigrationCandidate {
            generation: CloudMetadataGenerationIdentity::from_generation(&candidate),
            descriptor_key: CloudFormatMigrationPending::candidate_descriptor_key(&migration_id),
        },
        migration_id,
        source_sha256,
    };
    stage_source_descriptor(&cloud, &pending, &source, &deadline)?;
    stage_candidate_descriptor(&cloud, &pending, &candidate, &deadline)?;
    scope.check("install cloud FORMAT 10 gate")?;
    lease.ensure_healthy("before cloud FORMAT 10 gate installation")?;
    store
        .install_format_10_pending(&holder, epoch, pending, deadline.remaining())
        .map_err(|error| error.into_validation_error("install pending cloud FORMAT 10 gate"))?;
    cntryl_midge_core::failpoints::fail_point!("midge::format10::after_pending_cloud_gate", |_| {
        Err(MidgeError::Internal(
            "interrupted after durable pending FORMAT 10 gate".into(),
        ))
    });
    if reconcile_cloud_format_gate(&cloud, store, &holder, epoch, &deadline)?
        != CloudFormatGateResolution::Format10Selected
    {
        return Err(MidgeError::Fenced(
            "cloud FORMAT upgrade could not prove selected FORMAT 10".into(),
        ));
    }
    Ok(())
}

fn metadata_store(
    opts: &OpenOptions,
    path: &StartupStoragePath,
    scope: &DeadlineScope,
) -> MidgeResult<CloudStorage> {
    let cloud = match opts.storage() {
        Storage::Cloud { topology, .. } => {
            let location = topology.control();
            cntryl_midge_storage::providers::build_class_store(
                location.provider(),
                location.prefix(),
                opts.storage_io_timeout(),
                opts.injected_object_store(),
            )?
        }
        Storage::CloudSimulated { .. } => {
            let store = cntryl_midge_storage::simulated::SimulatedCloud::locate(
                path.root.as_ref(),
                opts.injected_object_store(),
                opts.storage_io_timeout(),
            )?
            .open_existing()?
            .ok_or_else(|| {
                MidgeError::CompatibilityError("cloud FORMAT upgrade requires existing data".into())
            })?;
            Arc::new(CloudStorage::from_object_store(
                store,
                opts.storage_io_timeout(),
            ))
        }
        Storage::InMemory | Storage::Local { .. } => {
            return Err(MidgeError::CompatibilityError(
                "expected cloud-backed storage for cloud FORMAT migration".into(),
            ));
        }
    };
    Ok(cloud.with_startup_scope(scope.clone()))
}

fn read_source(
    store: &dyn LeaderStore,
    holder: &str,
    epoch: u64,
    deadline: &OperationDeadline,
) -> MidgeResult<CloudMetadataGeneration> {
    let source = store
        .read_committed_metadata_for_writer(holder, epoch, deadline.remaining())
        .map_err(|error| error.into_validation_error("read cloud FORMAT 9 source"))?;
    let CloudMetadataHead::Committed(source) = source else {
        return Err(MidgeError::CompatibilityError(
            "cloud FORMAT upgrade requires a committed FORMAT 9 source".into(),
        ));
    };
    Ok(source)
}

fn prepare_candidate(
    mut bodies: BTreeMap<String, Vec<u8>>,
    source_sha256: &str,
    scope: &DeadlineScope,
) -> MidgeResult<BTreeMap<String, Vec<u8>>> {
    if bodies.get(files::FORMAT).map(Vec::as_slice) != Some(b"midge-format-version=9\n") {
        return Err(MidgeError::CompatibilityError(
            "cloud upgrade source must be FORMAT 9".into(),
        ));
    }
    let manifest = ManifestPersistence::decode_for_upgrade(
        bodies.get(files::MANIFEST_SNAPSHOT).map(Vec::as_slice),
        bodies
            .get(files::JOURNAL)
            .map_or(&[] as &[u8], Vec::as_slice),
        scope,
    )?;
    let intents = IntentPersistence::decode_for_upgrade(
        bodies.remove(files::INTENT_LOG).unwrap_or_default().into(),
        scope,
    )?;
    let mut digest = [0; 32];
    hex::decode_to_slice(source_sha256, &mut digest).map_err(|error| {
        MidgeError::Internal(format!("decode cloud upgrade source digest: {error}"))
    })?;
    let scratch = tempfile::tempdir()?;
    let fs: Arc<dyn Fs> = Arc::new(
        cntryl_midge_core::io::RealFs::open_existing(scratch.path())
            .map_err(FsError::into_midge)?,
    );
    let fs = cntryl_midge_core::io::scope_fs(fs, scope.clone());
    cntryl_midge_durability::metadata::ensure_or_create_format_marker(&fs, scratch.path())?;
    let base = codec::encode(codec::MANIFEST, &manifest)?;
    let intent_base = serde_json::to_vec(&intents.entries).map_err(|error| {
        MidgeError::Internal(format!("encode cloud FORMAT 10 intent base: {error}"))
    })?;
    let staged = LocalMetadataAuthority::stage_upgrade_candidate(
        Arc::clone(&fs),
        digest,
        &base,
        manifest.edit_checkpoint_id,
        &intent_base,
        intents.sequence,
        intents.checksum,
    )?;
    let mut candidate = BTreeMap::from([(
        files::FORMAT.to_owned(),
        b"midge-format-version=10\n".to_vec(),
    )]);
    for name in staged.selected_relative_paths() {
        scope.check("capture staged cloud FORMAT 10 metadata")?;
        let data = cntryl_midge_core::io::read_all(fs.as_ref(), &FsPath::new(&name))
            .map_err(FsError::into_midge)?;
        candidate.insert(name, data.to_vec());
    }
    Ok(candidate)
}

fn encoded_size(bodies: &BTreeMap<String, Vec<u8>>) -> MidgeResult<u64> {
    bodies.values().try_fold(0_u64, |sum, bytes| {
        sum.checked_add(bytes.len() as u64).ok_or_else(|| {
            MidgeError::ResourceLimit("cloud FORMAT upgrade metadata length overflow".into())
        })
    })
}

fn check_legacy_closure_budget(source: &CloudMetadataGeneration) -> MidgeResult<()> {
    use cntryl_midge_durability::metadata::cloud_manifest::{MAX_CHAIN_BYTES, MAX_CHAIN_OBJECTS};
    let encoded = source
        .objects
        .iter()
        .try_fold(0u64, |total, object| total.checked_add(object.len));
    if source.objects.len() > MAX_CHAIN_OBJECTS
        || encoded.is_none_or(|bytes| bytes > MAX_CHAIN_BYTES)
    {
        return Err(MidgeError::ResourceLimit(
            "legacy metadata exceeds its reserved FORMAT 10 migration allowance".into(),
        ));
    }
    Ok(())
}
