//! Durable two-phase cloud DDL coordination.
//!
//! Column-family edits are small, but they change the set of readers and the
//! files that are authoritative.  In hybrid mode the local manifest journal
//! and the remote DDL registry therefore use an explicit prepare/CAS/commit
//! protocol.  A local prepare survives a crash and is reconciled at startup.
//! The remote CAS is authoritative in hybrid mode, so a confirmed remote
//! commit is made visible immediately even when the local journal append must
//! be retried during recovery.

use crate::common::{MidgeError, MidgeResult};
use crate::io::traits::{FsPath, OpenMode, OpenOptions};
use crate::io::FsError;
use crate::metadata::{ColumnFamilyMeta, Manifest, ManifestEdit};
use crate::runtime::RuntimeState;
use crate::storage::hybrid::backend::{HybridStorage, RemoteObjectProof};
use serde::{Deserialize, Serialize};
use std::sync::Arc;

pub(crate) const REMOTE_DDL_REGISTRY_KEY: &str = "metadata/ddl.registry.json";
const LOCAL_DDL_PREPARE_FILE: &str = "ddl.prepare.json";
const LOCAL_DDL_PREPARE_TEMP: &str = "ddl.prepare.json.tmp";
pub(crate) const MAX_COLUMN_FAMILY_NAME_BYTES: usize = 255;

/// Provider cloud DDL shares the writer lease epoch, but commits through its
/// own registry key. Startup changes that key's identity before recovering DDL,
/// so a conditional write prepared by the prior holder cannot land afterward.
#[derive(Clone)]
pub(crate) struct DdlLeaseAuthority {
    pub(crate) store: Arc<dyn crate::lease::LeaderStore>,
    pub(crate) holder_id: String,
    pub(crate) writer_epoch: u64,
}

impl DdlLeaseAuthority {
    pub(crate) fn validate(&self, deadline: &crate::common::OperationDeadline) -> MidgeResult<()> {
        if deadline.is_expired() {
            return Err(MidgeError::Timeout(
                "cloud DDL lease validation exceeded its deadline".to_string(),
            ));
        }
        self.store
            .validate_epoch_with_timeout(&self.holder_id, self.writer_epoch, deadline.remaining())
            .map_err(|error| error.into_validation_error("cloud DDL lease validation"))
    }
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub(crate) struct DdlPrepare {
    pub(crate) op_id: String,
    pub(crate) expected_remote_epoch: u64,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub(crate) writer_epoch: Option<u64>,
    pub(crate) edit: ManifestEdit,
    /// The remote CAS was admitted and may still commit even if its callback
    /// timed out or disconnected. Negative readback is not conclusive while
    /// this marker is present; only the operation id appearing remotely can
    /// settle the authority decision in-process.
    #[serde(default)]
    pub(crate) remote_cas_ambiguous: bool,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub(crate) struct DdlOperation {
    pub(crate) op_id: String,
    pub(crate) edit: ManifestEdit,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub(crate) struct DdlRegistry {
    pub(crate) epoch: u64,
    #[serde(default)]
    pub(crate) column_families: Vec<ColumnFamilyMeta>,
    #[serde(default)]
    pub(crate) operations: Vec<DdlOperation>,
    /// Present only for the provider-backed V2 wire format. V1 serialization
    /// remains unchanged for simulated cloud stores.
    #[serde(skip)]
    writer_epoch: Option<u64>,
}

#[derive(Deserialize)]
#[serde(deny_unknown_fields)]
struct DdlRegistryV2 {
    registry_version: u8,
    ddl_epoch: u64,
    writer_epoch: u64,
    column_families: Vec<ColumnFamilyMeta>,
    operations: Vec<DdlOperation>,
}

#[derive(Serialize)]
struct DdlRegistryV2Ref<'a> {
    registry_version: u8,
    ddl_epoch: u64,
    writer_epoch: u64,
    column_families: &'a [ColumnFamilyMeta],
    operations: &'a [DdlOperation],
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
enum AmbiguousPrepareResolution {
    ObserveOnly,
    RedriveOnceOnStartup,
}

pub(crate) fn validate_column_family_name(name: &str) -> MidgeResult<()> {
    if name.is_empty() {
        return Err(MidgeError::InvalidArgument(
            "column family name must not be empty".to_string(),
        ));
    }
    if name.as_bytes().contains(&0) {
        return Err(MidgeError::InvalidArgument(
            "column family name must not contain NUL bytes".to_string(),
        ));
    }
    if name.len() > MAX_COLUMN_FAMILY_NAME_BYTES {
        return Err(MidgeError::InvalidArgument(format!(
            "column family name exceeds the {MAX_COLUMN_FAMILY_NAME_BYTES}-byte limit"
        )));
    }
    Ok(())
}

pub(crate) fn create_edit(state: &RuntimeState, name: &str) -> MidgeResult<ManifestEdit> {
    validate_column_family_name(name)?;
    if name == "default" {
        return Err(MidgeError::InvalidArgument(
            "column family name 'default' is reserved".to_string(),
        ));
    }
    if state.manifest.get_column_family_by_name(name).is_some() {
        return Err(MidgeError::InvalidArgument(format!(
            "column family '{name}' already exists"
        )));
    }
    let created_at = std::time::SystemTime::now()
        .duration_since(std::time::UNIX_EPOCH)
        .unwrap_or_default()
        .as_millis()
        .try_into()
        .unwrap_or(u64::MAX);
    Ok(ManifestEdit::CreateColumnFamily {
        id: state.manifest.next_cf_id()?,
        name: name.to_string(),
        created_at,
    })
}

pub(crate) fn drop_edit(
    state: &RuntimeState,
    cf_id: u32,
    discard_unflushed: bool,
) -> MidgeResult<ManifestEdit> {
    if cf_id == 0 {
        return Err(MidgeError::InvalidArgument(
            "Cannot drop default column family".to_string(),
        ));
    }
    if state
        .manifest
        .column_families
        .iter()
        .find(|cf| cf.id == cf_id && cf.deleted_at.is_none())
        .is_none()
    {
        return Err(MidgeError::InvalidArgument(format!(
            "Column family {cf_id} not found or already deleted"
        )));
    }
    let cf_state = state.get_cf(cf_id).ok_or_else(|| {
        MidgeError::InvalidArgument(format!(
            "Column family {cf_id} not found or already deleted"
        ))
    })?;
    // Report unflushed data before in-flight publication work. Both block a
    // safe drop, but only this one is actionable by the caller: flush, or
    // explicitly discard. In-flight publication clears on its own, so
    // surfacing it first would tell the caller to retry when what they
    // actually have to do is decide about their data.
    if !discard_unflushed {
        let unflushed_bytes = cf_state.memtable.size_bytes();
        if unflushed_bytes != 0 {
            // The one signal that licenses discarding committed data. Nothing
            // else on this path may construct it.
            return Err(MidgeError::UnflushedDataPresent {
                cf_id,
                bytes: unflushed_bytes,
            });
        }
    }
    if !cf_state.immutable_flushes.is_empty() {
        return Err(MidgeError::Busy(format!(
            "column family {cf_id} still has flush publication work in flight"
        )));
    }
    let dropped_sst_names = state
        .manifest
        .files
        .iter()
        .filter(|file| file.cf_id == cf_id)
        .map(|file| file.name.clone())
        .collect();
    Ok(ManifestEdit::DropColumnFamilyAt {
        id: cf_id,
        drop_sequence: state.sequence,
        dropped_sst_names,
    })
}

impl DdlRegistry {
    pub(crate) fn from_manifest(manifest: &Manifest) -> Self {
        Self {
            epoch: 0,
            column_families: manifest.column_families.clone(),
            operations: Vec::new(),
            writer_epoch: None,
        }
    }

    fn operation(&self, op_id: &str) -> Option<&DdlOperation> {
        self.operations
            .iter()
            .find(|operation| operation.op_id == op_id)
    }

    fn apply_edit(&mut self, edit: &ManifestEdit) -> MidgeResult<()> {
        if !matches!(
            edit,
            ManifestEdit::CreateColumnFamily { .. } | ManifestEdit::DropColumnFamilyAt { .. }
        ) {
            return Err(MidgeError::InvalidArgument(
                "remote DDL registry accepts only column-family edits".to_string(),
            ));
        }
        let mut manifest = Manifest::default();
        manifest.column_families.clone_from(&self.column_families);
        manifest.apply_edit(edit);
        self.column_families = manifest.column_families;
        Ok(())
    }
}

fn append_prepared_operation(registry: &mut DdlRegistry, prepare: &DdlPrepare) -> MidgeResult<()> {
    registry.apply_edit(&prepare.edit)?;
    registry.epoch = registry.epoch.saturating_add(1);
    registry.operations.push(DdlOperation {
        op_id: prepare.op_id.clone(),
        edit: prepare.edit.clone(),
    });
    Ok(())
}

fn serialize<T: Serialize>(value: &T) -> MidgeResult<Vec<u8>> {
    serde_json::to_vec(value).map_err(|error| MidgeError::Internal(error.to_string()))
}

fn deserialize<T: serde::de::DeserializeOwned>(bytes: &[u8]) -> MidgeResult<T> {
    serde_json::from_slice(bytes).map_err(|error| {
        MidgeError::Corruption(format!("invalid cloud DDL registry or prepare: {error}"))
    })
}

fn deserialize_registry(bytes: &[u8]) -> MidgeResult<DdlRegistry> {
    if let Ok(v2) = deserialize::<DdlRegistryV2>(bytes) {
        if v2.registry_version != 2 || v2.writer_epoch == 0 {
            return Err(MidgeError::Corruption(
                "cloud DDL registry has an invalid V2 version or writer epoch".to_string(),
            ));
        }
        return Ok(DdlRegistry {
            epoch: v2.ddl_epoch,
            column_families: v2.column_families,
            operations: v2.operations,
            writer_epoch: Some(v2.writer_epoch),
        });
    }
    deserialize(bytes)
}

fn serialize_registry(registry: &DdlRegistry) -> MidgeResult<Vec<u8>> {
    match registry.writer_epoch {
        Some(writer_epoch) => serialize(&DdlRegistryV2Ref {
            registry_version: 2,
            ddl_epoch: registry.epoch,
            writer_epoch,
            column_families: &registry.column_families,
            operations: &registry.operations,
        }),
        None => serialize(registry),
    }
}

fn require_current_registry(
    registry: Option<&DdlRegistry>,
    authority: &DdlLeaseAuthority,
) -> MidgeResult<()> {
    match registry.and_then(|registry| registry.writer_epoch) {
        Some(epoch) if epoch == authority.writer_epoch => Ok(()),
        Some(epoch) => Err(MidgeError::Fenced(format!(
            "cloud DDL registry writer epoch {epoch} does not match lease epoch {}",
            authority.writer_epoch
        ))),
        None => Err(MidgeError::Fenced(
            "cloud DDL registry is missing its current writer epoch".into(),
        )),
    }
}

fn local_prepare_exists(state: &RuntimeState) -> MidgeResult<bool> {
    state
        .fs
        .exists(&FsPath::new(LOCAL_DDL_PREPARE_FILE))
        .map_err(FsError::into_midge)
}

fn read_local_prepare(state: &RuntimeState) -> MidgeResult<Option<DdlPrepare>> {
    if !local_prepare_exists(state)? {
        return Ok(None);
    }
    let path = FsPath::new(LOCAL_DDL_PREPARE_FILE);
    let metadata = state.fs.metadata(&path).map_err(FsError::into_midge)?;
    let file = state
        .fs
        .open(
            &path,
            OpenOptions {
                mode: OpenMode::ReadOnly,
                create: false,
                create_new: false,
                truncate: false,
            },
        )
        .map_err(FsError::into_midge)?;
    let bytes = file.read_at(0, metadata.len).map_err(FsError::into_midge)?;
    Ok(Some(deserialize(&bytes)?))
}

fn write_local_prepare(state: &RuntimeState, prepare: &DdlPrepare) -> MidgeResult<()> {
    if state.is_memory_mode() {
        return Ok(());
    }
    let bytes = serialize(prepare)?;
    crate::failpoints::fail_point!("midge::ddl::before_prepare", |_| Err(MidgeError::Internal(
        "failpoint: DDL prepare failed".to_string()
    )));
    crate::io::staging::stage_bytes(
        &state.fs,
        &FsPath::new(LOCAL_DDL_PREPARE_TEMP),
        &FsPath::new(LOCAL_DDL_PREPARE_FILE),
        &bytes,
        MidgeError::Internal,
    )?;
    crate::failpoints::fail_point!("midge::ddl::after_prepare", |_| Err(MidgeError::Internal(
        "failpoint: DDL prepare completion failed".to_string()
    )));
    Ok(())
}

fn clear_local_prepare(state: &RuntimeState) -> MidgeResult<()> {
    if state.is_memory_mode() || !local_prepare_exists(state)? {
        return Ok(());
    }
    state
        .fs
        .remove_file(&FsPath::new(LOCAL_DDL_PREPARE_FILE))
        .map_err(FsError::into_midge)?;
    state
        .fs
        .sync_dir(&FsPath::new("."), crate::io::traits::Durability::Durable)
        .map_err(FsError::into_midge)
}

fn read_remote_registry(
    storage: &HybridStorage,
) -> Result<(Option<DdlRegistry>, Option<RemoteObjectProof>), String> {
    read_remote_registry_within(storage, &crate::common::OperationDeadline::unbounded())
        .map_err(|error| error.to_string())
}

fn read_remote_registry_within(
    storage: &HybridStorage,
    deadline: &crate::common::OperationDeadline,
) -> MidgeResult<(Option<DdlRegistry>, Option<RemoteObjectProof>)> {
    let proof = storage.remote_object_proof_optional_within(REMOTE_DDL_REGISTRY_KEY, deadline)?;
    let Some(proof) = proof else {
        return Ok((None, None));
    };
    let registry = deserialize_registry(proof.bytes())?;
    Ok((Some(registry), Some(proof)))
}

fn reread_remote_registry_after_ambiguous_cas_within(
    storage: &HybridStorage,
    deadline: &crate::common::OperationDeadline,
) -> MidgeResult<(Option<DdlRegistry>, Option<RemoteObjectProof>)> {
    crate::failpoints::fail_point!(
        "midge::ddl::before_ambiguous_cas_authority_reread",
        |_| Err(MidgeError::Internal(
            "failpoint: DDL authority re-read failed".to_string()
        ))
    );
    read_remote_registry_within(storage, deadline)
}

fn write_remote_registry(
    storage: &HybridStorage,
    registry: &DdlRegistry,
    expected: Option<&RemoteObjectProof>,
) -> MidgeResult<RemoteObjectProof> {
    write_remote_registry_within(
        storage,
        registry,
        expected,
        &crate::common::OperationDeadline::unbounded(),
    )
    .map_err(|failure| failure.error)
}

fn write_remote_registry_within(
    storage: &HybridStorage,
    registry: &DdlRegistry,
    expected: Option<&RemoteObjectProof>,
    deadline: &crate::common::OperationDeadline,
) -> Result<RemoteObjectProof, crate::storage::hybrid::backend::RemoteCasFailure> {
    crate::failpoints::fail_point!("midge::ddl::before_remote_cas", |_| Err(
        crate::storage::hybrid::backend::RemoteCasFailure::not_committed(MidgeError::Internal(
            "remote DDL CAS failed before submission: injected failure".to_string()
        ))
    ));
    let bytes = serialize_registry(registry)
        .map_err(crate::storage::hybrid::backend::RemoteCasFailure::not_committed)?;
    let result = storage.compare_exchange_remote_object_phased(
        REMOTE_DDL_REGISTRY_KEY,
        expected.map(RemoteObjectProof::metadata),
        bytes,
        deadline,
    )?;
    crate::failpoints::fail_point!("midge::ddl::after_remote_cas", |_| Err(
        crate::storage::hybrid::backend::RemoteCasFailure::may_have_committed(
            MidgeError::Internal("failpoint: DDL remote CAS completion failed".to_string())
        )
    ));
    Ok(result)
}

/// Complete the successor's registry fence before cloud recovery reads any
/// column-family authority. A predecessor's already-submitted conditional PUT
/// may win first; in that case retry from its committed bytes. Once this CAS
/// changes the registry identity, that predecessor's delayed PUT cannot land.
pub(crate) fn fence_remote_registry_on_startup(
    storage: &HybridStorage,
    authority: &DdlLeaseAuthority,
    deadline: &crate::common::OperationDeadline,
) -> MidgeResult<()> {
    loop {
        authority.validate(deadline)?;
        let (remote, proof) = read_remote_registry_within(storage, deadline)?;
        if let Some(remote) = &remote {
            match remote.writer_epoch {
                Some(epoch) if epoch > authority.writer_epoch => {
                    return Err(MidgeError::Fenced(format!(
                        "cloud DDL registry writer epoch {epoch} exceeds current lease epoch {}",
                        authority.writer_epoch
                    )));
                }
                Some(epoch) if epoch == authority.writer_epoch => {
                    authority.validate(deadline)?;
                    return Ok(());
                }
                _ => {}
            }
        } else {
            let head = authority
                .store
                .read_committed_metadata(deadline.remaining())
                .map_err(|error| {
                    MidgeError::RecoveryFailed(format!(
                        "cloud DDL registry is missing and metadata authority could not be read: {error}"
                    ))
                })?;
            match head {
                crate::lease::CloudMetadataHead::Committed(_) => {
                    return Err(MidgeError::RecoveryFailed(
                        "cloud DDL registry is missing while lease metadata is committed".into(),
                    ));
                }
                crate::lease::CloudMetadataHead::MissingLease => {
                    return Err(MidgeError::RecoveryFailed(
                        "cloud DDL registry and acquired lease disappeared".into(),
                    ));
                }
                crate::lease::CloudMetadataHead::Uncommitted => {}
            }
        }

        let mut replacement =
            remote.unwrap_or_else(|| DdlRegistry::from_manifest(&Manifest::default()));
        replacement.writer_epoch = Some(authority.writer_epoch);
        let expected_bytes = serialize_registry(&replacement)?;
        authority.validate(deadline)?;
        match write_remote_registry_within(storage, &replacement, proof.as_ref(), deadline) {
            Ok(_) => {
                authority.validate(deadline)?;
                return Ok(());
            }
            Err(failure) => {
                // A lost callback cannot prove the CAS failed. Only an exact
                // readback of our candidate establishes that this fence won.
                let (_, actual_proof) = read_remote_registry_within(storage, deadline)?;
                if actual_proof
                    .as_ref()
                    .is_some_and(|actual| actual.bytes() == expected_bytes)
                {
                    authority.validate(deadline)?;
                    return Ok(());
                }
                if !failure.may_have_committed && !matches!(failure.error, MidgeError::Busy(_)) {
                    return Err(failure.error);
                }
                if deadline.is_expired() {
                    return Err(MidgeError::RecoveryFailed(
                        "cloud DDL registry fence did not settle before startup deadline".into(),
                    ));
                }
            }
        }
    }
}

/// An uncommitted metadata lease may describe a new database only when the
/// separately fenced DDL registry has no prior column-family decisions.
pub(crate) fn require_empty_registry_for_metadata_bootstrap(
    storage: &HybridStorage,
    authority: &DdlLeaseAuthority,
    deadline: &crate::common::OperationDeadline,
) -> MidgeResult<()> {
    authority.validate(deadline)?;
    let (remote, _) = read_remote_registry_within(storage, deadline)?;
    authority.validate(deadline)?;
    let Some(remote) = remote.as_ref() else {
        return Err(MidgeError::RecoveryFailed(
            "cloud DDL registry is missing during metadata bootstrap".into(),
        ));
    };
    require_current_registry(Some(remote), authority)?;
    if remote.epoch != 0 || !remote.column_families.is_empty() || !remote.operations.is_empty() {
        return Err(MidgeError::RecoveryFailed(
            "cannot bootstrap empty cloud metadata over existing DDL state".into(),
        ));
    }
    Ok(())
}

fn local_edit_matches(state: &RuntimeState, edit: &ManifestEdit) -> bool {
    match edit {
        ManifestEdit::CreateColumnFamily { id, name, .. } => state
            .manifest
            .column_families
            .iter()
            .any(|cf| cf.id == *id && cf.name == *name && cf.deleted_at.is_none()),
        ManifestEdit::DropColumnFamilyAt {
            id, drop_sequence, ..
        } => state.manifest.column_families.iter().any(|cf| {
            cf.id == *id && cf.deleted_at.is_some() && cf.drop_sequence == Some(*drop_sequence)
        }),
        _ => false,
    }
}

fn apply_remote_committed_visibility(state: &mut RuntimeState, edit: &ManifestEdit) {
    if local_edit_matches(state, edit) {
        return;
    }
    state.manifest.apply_edit(edit);
    match edit {
        ManifestEdit::CreateColumnFamily { id, name, .. } => {
            state.column_families.entry(*id).or_insert_with(|| {
                crate::runtime::state::ColumnFamilyState::new(*id, name.clone())
            });
        }
        ManifestEdit::DropColumnFamilyAt { id, .. } => {
            state.column_families.remove(id);
        }
        _ => {}
    }
}

/// Apply a prepared edit to the local journal and runtime state.  The
/// candidate manifest is built first so a failed journal append cannot mutate
/// in-memory visibility.
pub(crate) fn apply_local_edit(state: &mut RuntimeState, edit: &ManifestEdit) -> MidgeResult<()> {
    if local_edit_matches(state, edit) {
        return Ok(());
    }
    let mut candidate = state.manifest.clone();
    candidate.apply_edit(edit);
    crate::failpoints::fail_point!("midge::ddl::before_local_commit", |_| Err(
        MidgeError::Internal("failpoint: DDL local commit failed".to_string())
    ));
    let journaled_id = if state.is_memory_mode() {
        None
    } else {
        Some(state.manifest_store.append(edit)?)
    };
    crate::failpoints::fail_point!("midge::ddl::after_local_journal_before_memory", |_| Err(
        MidgeError::Internal("failpoint: DDL local visibility failed".to_string(),)
    ));
    state.manifest.replace(candidate);
    if let Some(edit_id) = journaled_id {
        state.manifest.note_applied_journal_edit(edit_id);
    }
    match edit {
        ManifestEdit::CreateColumnFamily { id, name, .. } => {
            state.column_families.entry(*id).or_insert_with(|| {
                crate::runtime::state::ColumnFamilyState::new(*id, name.clone())
            });
        }
        ManifestEdit::DropColumnFamilyAt { id, .. } => {
            state.column_families.remove(id);
        }
        _ => {}
    }
    Ok(())
}

/// Reconcile one local prepare with the remote registry. A remote operation is
/// committed locally; a definitely pre-submit operation absent remotely is
/// aborted. Ambiguous live requests remain fenced, while startup can safely
/// re-drive the same durable operation once.
fn reconcile_prepared_on_startup(
    state: &mut RuntimeState,
    storage: Option<&Arc<HybridStorage>>,
    authority: Option<&DdlLeaseAuthority>,
) -> MidgeResult<()> {
    reconcile_prepared_with_resolution(
        state,
        storage,
        authority,
        &crate::common::OperationDeadline::unbounded(),
        AmbiguousPrepareResolution::RedriveOnceOnStartup,
    )
}

pub(crate) fn reconcile_prepared_within(
    state: &mut RuntimeState,
    storage: Option<&Arc<HybridStorage>>,
    authority: Option<&DdlLeaseAuthority>,
    deadline: &crate::common::OperationDeadline,
) -> MidgeResult<()> {
    reconcile_prepared_with_resolution(
        state,
        storage,
        authority,
        deadline,
        AmbiguousPrepareResolution::ObserveOnly,
    )
}

fn reconcile_prepared_with_resolution(
    state: &mut RuntimeState,
    storage: Option<&Arc<HybridStorage>>,
    authority: Option<&DdlLeaseAuthority>,
    deadline: &crate::common::OperationDeadline,
    resolution: AmbiguousPrepareResolution,
) -> MidgeResult<()> {
    let Some(prepare) = read_local_prepare(state)? else {
        return Ok(());
    };
    let Some(storage) = storage else {
        if local_edit_matches(state, &prepare.edit) {
            return Err(MidgeError::RecoveryFailed(
                "cloud DDL prepare exists without a cloud authority".to_string(),
            ));
        }
        clear_local_prepare(state)?;
        return Ok(());
    };
    if let Some(authority) = authority {
        authority.validate(deadline)?;
    }
    let (remote, proof) = read_remote_registry_within(storage, deadline)?;
    if let Some(authority) = authority {
        require_current_registry(remote.as_ref(), authority)?;
    }
    if remote
        .as_ref()
        .is_some_and(|registry| registry.operation(&prepare.op_id).is_some())
    {
        apply_local_edit(state, &prepare.edit)?;
        clear_local_prepare(state)?;
        return Ok(());
    }
    if local_edit_matches(state, &prepare.edit) {
        let message = if remote.is_some() {
            "local DDL commit is present but its operation is absent remotely"
        } else {
            "local DDL commit is present but the remote registry is missing"
        };
        return Err(MidgeError::RecoveryFailed(message.to_string()));
    }
    if authority.is_some_and(|authority| prepare.writer_epoch != Some(authority.writer_epoch)) {
        // The successor changed the registry object's identity before this
        // read. The old holder's delayed CAS can no longer land, and replaying
        // its uncommitted intent under the new holder would invent a DDL edit.
        clear_local_prepare(state)?;
        return Ok(());
    }
    if prepare.remote_cas_ambiguous {
        if resolution == AmbiguousPrepareResolution::RedriveOnceOnStartup {
            return redrive_ambiguous_prepare_within(
                state,
                storage,
                &prepare,
                remote,
                proof.as_ref(),
                authority,
                deadline,
            );
        }
        let message = if remote.is_some() {
            "DDL authority is ambiguous: the admitted remote CAS operation is not yet visible"
        } else {
            "DDL authority is ambiguous: the admitted remote CAS is not yet visible"
        };
        return Err(MidgeError::Fenced(message.to_string()));
    }
    clear_local_prepare(state)?;
    Ok(())
}

fn redrive_ambiguous_prepare_within(
    state: &mut RuntimeState,
    storage: &HybridStorage,
    prepare: &DdlPrepare,
    remote: Option<DdlRegistry>,
    proof: Option<&RemoteObjectProof>,
    authority: Option<&DdlLeaseAuthority>,
    deadline: &crate::common::OperationDeadline,
) -> MidgeResult<()> {
    let mut registry = match remote {
        Some(registry) if registry.epoch == prepare.expected_remote_epoch => registry,
        Some(registry) => {
            state.mark_ddl_authority_ambiguous();
            return Err(MidgeError::Fenced(format!(
                "DDL authority is ambiguous: prepared operation expected remote epoch {}, but the registry advanced to {}",
                prepare.expected_remote_epoch, registry.epoch
            )));
        }
        None if prepare.expected_remote_epoch == 0 => DdlRegistry::from_manifest(&state.manifest),
        None => {
            state.mark_ddl_authority_ambiguous();
            return Err(MidgeError::Fenced(format!(
                "DDL authority is ambiguous: prepared operation expected remote epoch {}, but the registry is missing",
                prepare.expected_remote_epoch
            )));
        }
    };
    append_prepared_operation(&mut registry, prepare)?;

    if let Some(authority) = authority {
        require_current_registry(Some(&registry), authority)?;
        authority.validate(deadline)?;
    }

    if let Err(failure) = write_remote_registry_within(storage, &registry, proof, deadline) {
        let write_error = failure.error;
        return match reread_remote_registry_after_ambiguous_cas_within(storage, deadline) {
            Ok((Some(remote), _))
                if remote.operation(&prepare.op_id).is_some()
                    && authority.is_none_or(|authority| {
                        remote.writer_epoch == Some(authority.writer_epoch)
                            && authority.validate(deadline).is_ok()
                    }) =>
            {
                apply_local_edit(state, &prepare.edit)?;
                clear_local_prepare(state)
            }
            Ok(_) => {
                state.mark_ddl_authority_ambiguous();
                Err(MidgeError::Fenced(format!(
                    "DDL authority is ambiguous after re-driving the prepared remote CAS ({write_error}); the operation id is not visible"
                )))
            }
            Err(read_error) => {
                state.mark_ddl_authority_ambiguous();
                Err(MidgeError::Fenced(format!(
                    "DDL authority is ambiguous after re-driving the prepared remote CAS ({write_error}); authority re-read failed: {read_error}"
                )))
            }
        };
    }

    apply_local_edit(state, &prepare.edit)?;
    clear_local_prepare(state)
}

/// Reconcile the remote CF registry into a freshly recovered local state.
/// This also bootstraps an absent registry from the local manifest on the
/// first cloud open.
pub(crate) fn reconcile_startup(
    state: &mut RuntimeState,
    storage: Option<&Arc<HybridStorage>>,
    authority: Option<&DdlLeaseAuthority>,
) -> MidgeResult<()> {
    let Some(storage) = storage else {
        return Ok(());
    };
    reconcile_prepared_on_startup(state, Some(storage), authority)?;
    let (remote, proof) = read_remote_registry(storage).map_err(MidgeError::Internal)?;
    if let Some(authority) = authority {
        authority.validate(&crate::common::OperationDeadline::unbounded())?;
        require_current_registry(remote.as_ref(), authority)?;
    }
    let Some(remote) = remote else {
        let registry = DdlRegistry::from_manifest(&state.manifest);
        let _ = write_remote_registry(storage, &registry, proof.as_ref())
            .map_err(|error| MidgeError::Internal(error.to_string()))?;
        return Ok(());
    };

    // The first provider startup places an empty V2 fence before local
    // metadata is hydrated. Fill its CF baseline only after recovery has
    // supplied the authoritative manifest.
    if authority.is_some()
        && remote.epoch == 0
        && remote.operations.is_empty()
        && remote.column_families.is_empty()
    {
        let mut baseline = DdlRegistry::from_manifest(&state.manifest);
        baseline.writer_epoch = remote.writer_epoch;
        let _ = write_remote_registry(storage, &baseline, proof.as_ref())?;
        return Ok(());
    }

    for operation in &remote.operations {
        if !local_edit_matches(state, &operation.edit) {
            apply_local_edit(state, &operation.edit)?;
        }
    }
    for remote_cf in &remote.column_families {
        let local_cf = state
            .manifest
            .column_families
            .iter()
            .find(|cf| cf.id == remote_cf.id);
        if let Some(local_cf) = local_cf {
            if local_cf.name != remote_cf.name {
                return Err(MidgeError::RecoveryFailed(format!(
                    "remote DDL registry conflicts for column family {}",
                    remote_cf.id
                )));
            }
        }
    }
    if state
        .manifest
        .column_families
        .iter()
        .any(|local| !remote.column_families.iter().any(|cf| cf.id == local.id))
    {
        return Err(MidgeError::RecoveryFailed(
            "local column-family state is ahead of the remote DDL registry".to_string(),
        ));
    }
    Ok(())
}

pub(crate) fn execute_within(
    state: &mut RuntimeState,
    storage: Option<&Arc<HybridStorage>>,
    edit: &ManifestEdit,
    authority: Option<&DdlLeaseAuthority>,
    deadline: &crate::common::OperationDeadline,
) -> MidgeResult<()> {
    if let Some(authority) = authority {
        authority.validate(deadline)?;
    }
    reconcile_prepared_within(state, storage, authority, deadline)?;
    if local_edit_matches(state, edit) {
        return Ok(());
    }
    let Some(storage) = storage else {
        return apply_local_edit(state, edit);
    };

    let (remote, proof) = read_remote_registry_within(storage, deadline)?;
    if let Some(authority) = authority {
        require_current_registry(remote.as_ref(), authority)?;
    }
    let mut registry = remote.unwrap_or_else(|| DdlRegistry::from_manifest(&state.manifest));
    let expected_epoch = registry.epoch;
    let mut prepare = DdlPrepare {
        op_id: uuid::Uuid::new_v4().to_string(),
        expected_remote_epoch: expected_epoch,
        writer_epoch: authority.map(|authority| authority.writer_epoch),
        edit: edit.clone(),
        // The first durable prepare proves that no provider mutation has been
        // admitted yet. A failure while writing this phase can therefore be
        // aborted safely during the next reconciliation.
        remote_cas_ambiguous: false,
    };
    write_local_prepare(state, &prepare)?;
    append_prepared_operation(&mut registry, &prepare)?;
    // Persist the ambiguous phase before submitting the provider mutation.
    // From this point until positive readback, absence from an immediate GET
    // is not proof that a delayed CAS will never commit.
    prepare.remote_cas_ambiguous = true;
    write_local_prepare(state, &prepare)?;
    crate::failpoints::fail_point!(
        "midge::ddl::after_ambiguous_prepare_before_remote_cas_submission",
        |_| Err(MidgeError::Internal(
            "failpoint: DDL stopped after ambiguous prepare before remote CAS submission"
                .to_string()
        ))
    );
    if let Some(authority) = authority {
        authority.validate(deadline)?;
    }
    if let Err(failure) = write_remote_registry_within(storage, &registry, proof.as_ref(), deadline)
    {
        let definitely_not_committed = remote_cas_definitely_not_committed(&failure);
        let error = failure.error;
        if definitely_not_committed {
            clear_local_prepare(state)?;
            return Err(error);
        }
        // A provider can lose the successful CAS response. Resolve that
        // ambiguity against the operation id before deciding whether the edit
        // committed. A confirmed remote commit must immediately fence the old
        // local view; otherwise later writes could vanish during recovery.
        match reread_remote_registry_after_ambiguous_cas_within(storage, deadline) {
            Ok((Some(remote), _))
                if remote.operation(&prepare.op_id).is_some()
                    && authority.is_none_or(|authority| {
                        remote.writer_epoch == Some(authority.writer_epoch)
                            && authority.validate(deadline).is_ok()
                    }) =>
            {
                apply_remote_committed_visibility(state, edit);
                state.mark_persistence_anomaly();
                tracing::warn!(%error, "remote DDL CAS committed despite a lost response; retaining prepare for local reconciliation");
                return Ok(());
            }
            Ok(_) => {
                state.mark_persistence_anomaly();
                state.mark_ddl_authority_ambiguous();
                return Err(MidgeError::Fenced(format!(
                    "DDL authority is ambiguous after a lost remote CAS response ({error}); the operation id is not yet visible"
                )));
            }
            Err(read_error) => {
                state.mark_persistence_anomaly();
                state.mark_ddl_authority_ambiguous();
                return Err(MidgeError::Fenced(format!(
                    "DDL authority is ambiguous after a lost remote CAS response ({error}); authority re-read failed: {read_error}"
                )));
            }
        }
    }
    if let Err(error) = apply_local_edit(state, edit) {
        // The remote CAS is the authority switch for a hybrid DDL operation.
        // Once it succeeds, continuing to expose the old local CF state could
        // accept writes that recovery will later discard. Fence that state in
        // memory immediately, retain the prepare for restart reconciliation,
        // and report the operation as committed but degraded.
        apply_remote_committed_visibility(state, edit);
        state.mark_persistence_anomaly();
        tracing::warn!(%error, "remote DDL committed; retaining prepare and applying authoritative visibility after local persistence failure");
        return Ok(());
    }
    if let Err(error) = clear_local_prepare(state) {
        tracing::warn!(%error, "cloud DDL committed but prepare cleanup will be retried");
    }
    Ok(())
}

fn remote_cas_definitely_not_committed(
    failure: &crate::storage::hybrid::backend::RemoteCasFailure,
) -> bool {
    !failure.may_have_committed
}

#[cfg(test)]
mod tests {
    use super::*;

    use std::sync::atomic::{AtomicBool, AtomicUsize, Ordering};

    struct LoseFirstRegistryCasCallback {
        inner: Arc<dyn crate::storage::StorageBackend>,
        armed: AtomicBool,
        registry_writes: AtomicUsize,
    }

    impl crate::storage::StorageBackend for LoseFirstRegistryCasCallback {
        crate::storage::forward_storage_backend!(
            inner;
            submit_range_read_request, submit_range_head_request,
            submit_metadata_read_request, submit_head_request, submit_delete_request,
        );

        fn submit_write_request(
            &self,
            request: crate::storage::StorageRequest,
            data: Vec<u8>,
            callback: crate::storage::StorageCallback,
        ) {
            if request.key != REMOTE_DDL_REGISTRY_KEY {
                self.inner.submit_write_request(request, data, callback);
                return;
            }
            self.registry_writes.fetch_add(1, Ordering::SeqCst);
            if !self.armed.swap(false, Ordering::SeqCst) {
                self.inner.submit_write_request(request, data, callback);
                return;
            }
            let key = request.key.clone();
            let (inner_tx, inner_rx) = std::sync::mpsc::channel();
            self.inner.submit_write_request(request, data, inner_tx);
            assert!(matches!(
                inner_rx.recv_timeout(std::time::Duration::from_secs(1)),
                Ok(crate::storage::StorageEvent::WriteComplete {
                    result: crate::storage::StorageOutcome::Ok(()),
                    ..
                })
            ));
            let _ = callback.send(crate::storage::StorageEvent::WriteComplete {
                key,
                result: crate::storage::StorageOutcome::Err(crate::storage::storage_timeout_error(
                    "registry callback lost after commit",
                )),
            });
        }
    }

    fn provider_test_lease(
        cloud: &Arc<crate::storage::cloud::CloudStorage>,
        path: &std::path::Path,
    ) -> Arc<crate::lease::CloudStorageLease> {
        Arc::new(crate::lease::CloudStorageLease::new_provider_backed(
            crate::lease::CloudLeaseConfig {
                bucket: "ddl-fence-test".to_string(),
                prefix: "test/".to_string(),
            },
            path.to_path_buf(),
            Arc::clone(cloud),
        ))
    }

    fn provider_test_authority(lease: &crate::lease::CloudStorageLease) -> DdlLeaseAuthority {
        use crate::lease::PrimaryLease as _;

        DdlLeaseAuthority {
            store: lease.get_leader_store().expect("provider leader store"),
            holder_id: lease.holder_id(),
            writer_epoch: lease.epoch(),
        }
    }

    fn assert_stale_registry_cas_rejected_after_successor_startup(
        drop_existing: bool,
    ) -> MidgeResult<()> {
        use crate::lease::PrimaryLease as _;

        // Arrange: A reads one registry identity and prepares a DDL CAS. The
        // successor uses the same provider objects but an independent cache.
        let temp = tempfile::tempdir()?;
        let cloud = Arc::new(crate::storage::cloud::CloudStorage::new(
            Arc::new(crate::storage::cloud::MockCloudBackend::new()),
            "ddl-fence-test".to_string(),
        ));
        let local = Arc::new(crate::storage::filesystem::FileSystem::new(
            temp.path().join("hybrid-local"),
        )?);
        let storage = Arc::new(crate::storage::HybridStorage::with_policy(
            local,
            cloud.clone(),
            crate::storage::hybrid::policy::StorageBudgetPolicy::default(),
        ));
        let lease_a = provider_test_lease(&cloud, &temp.path().join("lease-a"));
        let _guard_a = Arc::clone(&lease_a)
            .try_acquire()
            .expect("A acquires provider lease");
        let authority_a = provider_test_authority(&lease_a);
        let deadline =
            crate::common::OperationDeadline::from_budget(std::time::Duration::from_secs(5));
        fence_remote_registry_on_startup(&storage, &authority_a, &deadline)?;
        let mut state_a = RuntimeState::new(temp.path().join("state-a"), false);
        if drop_existing {
            let create = create_edit(&state_a, "kept-cf")?;
            execute_within(
                &mut state_a,
                Some(&storage),
                &create,
                Some(&authority_a),
                &deadline,
            )?;
        }
        let edit = if drop_existing {
            let cf = state_a
                .manifest
                .get_column_family_by_name("kept-cf")
                .expect("created CF");
            drop_edit(&state_a, cf.id, false)?
        } else {
            create_edit(&state_a, "late-cf")?
        };
        let (Some(mut candidate), Some(stale_proof)) =
            read_remote_registry_within(&storage, &deadline)?
        else {
            panic!("A must read a fenced registry and its CAS identity");
        };
        let expected_remote_epoch = candidate.epoch;
        append_prepared_operation(
            &mut candidate,
            &DdlPrepare {
                op_id: uuid::Uuid::new_v4().to_string(),
                expected_remote_epoch,
                writer_epoch: Some(authority_a.writer_epoch),
                edit,
                remote_cas_ambiguous: true,
            },
        )?;

        // Act: B takes over, fences the registry key, and reconciles before
        // A's previously prepared provider PUT is allowed to run.
        lease_a.release().expect("A releases provider lease");
        let lease_b = provider_test_lease(&cloud, &temp.path().join("lease-b"));
        let _guard_b = Arc::clone(&lease_b)
            .try_acquire()
            .expect("B acquires higher provider epoch");
        let authority_b = provider_test_authority(&lease_b);
        fence_remote_registry_on_startup(&storage, &authority_b, &deadline)?;
        let mut state_b = RuntimeState::new(temp.path().join("state-b"), false);
        reconcile_startup(&mut state_b, Some(&storage), Some(&authority_b))?;
        let stale_write =
            write_remote_registry_within(&storage, &candidate, Some(&stale_proof), &deadline);

        // Assert: the stale CAS cannot become authority, and another restart
        // recovers the same CF visibility B already serves.
        assert!(matches!(
            stale_write,
            Err(crate::storage::hybrid::backend::RemoteCasFailure {
                may_have_committed: false,
                ..
            })
        ));
        let (_, after) = read_remote_registry_within(&storage, &deadline)?;
        let after = after.expect("committed registry");
        assert_ne!(after.bytes(), serialize_registry(&candidate)?.as_slice());
        lease_b.release().expect("B releases provider lease");
        let lease_c = provider_test_lease(&cloud, &temp.path().join("lease-c"));
        let _guard_c = Arc::clone(&lease_c)
            .try_acquire()
            .expect("C acquires provider lease");
        let authority_c = provider_test_authority(&lease_c);
        fence_remote_registry_on_startup(&storage, &authority_c, &deadline)?;
        let mut state_c = RuntimeState::new(temp.path().join("state-c"), false);
        reconcile_startup(&mut state_c, Some(&storage), Some(&authority_c))?;
        let name = if drop_existing { "kept-cf" } else { "late-cf" };
        assert_eq!(
            state_b.manifest.get_column_family_by_name(name).is_some(),
            state_c.manifest.get_column_family_by_name(name).is_some()
        );
        assert_eq!(
            state_b.manifest.get_column_family_by_name(name).is_some(),
            drop_existing,
            "stale create/drop must not alter successor visibility"
        );
        Ok(())
    }

    #[test]
    fn should_reject_paused_create_cas_after_successor_fences_ddl_registry() -> MidgeResult<()> {
        assert_stale_registry_cas_rejected_after_successor_startup(false)
    }

    #[test]
    fn should_reject_paused_drop_cas_after_successor_fences_ddl_registry() -> MidgeResult<()> {
        assert_stale_registry_cas_rejected_after_successor_startup(true)
    }

    #[test]
    fn should_encode_provider_registry_without_legacy_epoch_field() -> MidgeResult<()> {
        // Arrange
        let mut registry = DdlRegistry::from_manifest(&Manifest::default());
        registry.writer_epoch = Some(7);

        // Act
        let bytes = serialize_registry(&registry)?;

        // Assert: older binaries require `epoch`, so they reject V2 instead
        // of reserializing it as an unfenced V1 registry.
        assert!(serde_json::from_slice::<DdlRegistry>(&bytes).is_err());
        let parsed = deserialize_registry(&bytes)?;
        assert_eq!(parsed.writer_epoch, Some(7));
        assert_eq!(parsed.epoch, 0);
        Ok(())
    }

    #[test]
    fn should_accept_ddl_startup_fence_only_after_exact_ambiguous_cas_readback() -> MidgeResult<()>
    {
        use crate::lease::PrimaryLease as _;

        // Arrange: the provider commits the first registry CAS but reports a
        // timeout. The startup path must read the exact V2 bytes back.
        let temp = tempfile::tempdir()?;
        let cloud = Arc::new(crate::storage::cloud::CloudStorage::with_mock());
        let inner: Arc<dyn crate::storage::StorageBackend> = cloud.clone();
        let lossy = Arc::new(LoseFirstRegistryCasCallback {
            inner,
            armed: AtomicBool::new(true),
            registry_writes: AtomicUsize::new(0),
        });
        let storage = crate::storage::HybridStorage::with_policy(
            Arc::new(crate::storage::filesystem::FileSystem::new(
                temp.path().join("hybrid-local"),
            )?),
            lossy.clone(),
            crate::storage::hybrid::policy::StorageBudgetPolicy::default(),
        );
        let lease = provider_test_lease(&cloud, &temp.path().join("lease"));
        let _guard = Arc::clone(&lease)
            .try_acquire()
            .expect("acquire provider lease");
        let authority = provider_test_authority(&lease);

        // Act
        fence_remote_registry_on_startup(
            &storage,
            &authority,
            &crate::common::OperationDeadline::from_budget(std::time::Duration::from_secs(5)),
        )?;

        // Assert: a second CAS was unnecessary because exact readback proved
        // the ambiguous first CAS was the committed fence.
        assert_eq!(lossy.registry_writes.load(Ordering::SeqCst), 1);
        let (registry, _) = read_remote_registry(&storage).map_err(MidgeError::Internal)?;
        assert_eq!(
            registry.expect("committed registry").writer_epoch,
            Some(authority.writer_epoch)
        );
        Ok(())
    }

    #[test]
    fn should_abort_prior_epoch_ambiguous_prepare_after_successor_fence() -> MidgeResult<()> {
        use crate::lease::PrimaryLease as _;

        // Arrange: A persisted the admitted-CAS phase but did not publish its
        // operation. Its local cache is reused by B after takeover.
        let temp = tempfile::tempdir()?;
        let cloud = Arc::new(crate::storage::cloud::CloudStorage::with_mock());
        let storage = Arc::new(crate::storage::HybridStorage::with_policy(
            Arc::new(crate::storage::filesystem::FileSystem::new(
                temp.path().join("hybrid-local"),
            )?),
            cloud.clone(),
            crate::storage::hybrid::policy::StorageBudgetPolicy::default(),
        ));
        let lease_a = provider_test_lease(&cloud, &temp.path().join("lease-a"));
        let _guard_a = Arc::clone(&lease_a)
            .try_acquire()
            .expect("A acquires provider lease");
        let authority_a = provider_test_authority(&lease_a);
        let deadline =
            crate::common::OperationDeadline::from_budget(std::time::Duration::from_secs(5));
        fence_remote_registry_on_startup(&storage, &authority_a, &deadline)?;
        let state_path = temp.path().join("shared-state");
        let state_a = RuntimeState::new(state_path.clone(), false);
        let edit = create_edit(&state_a, "aborted-prior-epoch")?;
        let prepare = DdlPrepare {
            op_id: uuid::Uuid::new_v4().to_string(),
            expected_remote_epoch: 0,
            writer_epoch: Some(authority_a.writer_epoch),
            edit,
            remote_cas_ambiguous: true,
        };
        write_local_prepare(&state_a, &prepare)?;

        // Act: B's registry fence invalidates any late A CAS, then startup
        // observes the operation is absent and discards the stale prepare.
        lease_a.release().expect("A releases provider lease");
        let lease_b = provider_test_lease(&cloud, &temp.path().join("lease-b"));
        let _guard_b = Arc::clone(&lease_b)
            .try_acquire()
            .expect("B acquires higher provider epoch");
        let authority_b = provider_test_authority(&lease_b);
        fence_remote_registry_on_startup(&storage, &authority_b, &deadline)?;
        let mut state_b = RuntimeState::new(state_path, false);
        reconcile_startup(&mut state_b, Some(&storage), Some(&authority_b))?;

        // Assert
        assert!(!local_prepare_exists(&state_b)?);
        assert!(state_b
            .manifest
            .get_column_family_by_name("aborted-prior-epoch")
            .is_none());
        let (remote, _) = read_remote_registry_within(&storage, &deadline)?;
        assert!(remote
            .expect("current registry")
            .operation(&prepare.op_id)
            .is_none());
        Ok(())
    }

    #[test]
    fn should_reject_missing_ddl_registry_when_lease_metadata_is_committed() -> MidgeResult<()> {
        use crate::lease::PrimaryLease as _;

        // Arrange: a committed provider metadata generation proves this is an
        // existing database, so an absent DDL registry cannot be bootstrapped.
        let temp = tempfile::tempdir()?;
        let cloud = Arc::new(crate::storage::cloud::CloudStorage::with_mock());
        let storage = crate::storage::HybridStorage::with_policy(
            Arc::new(crate::storage::filesystem::FileSystem::new(
                temp.path().join("hybrid-local"),
            )?),
            cloud.clone(),
            crate::storage::hybrid::policy::StorageBudgetPolicy::default(),
        );
        let lease = provider_test_lease(&cloud, &temp.path().join("lease"));
        let _guard = Arc::clone(&lease)
            .try_acquire()
            .expect("acquire provider lease");
        let authority = provider_test_authority(&lease);
        let generation_id = uuid::Uuid::new_v4();
        let objects = [
            crate::metadata::files::FORMAT,
            crate::metadata::files::MANIFEST_SNAPSHOT,
        ]
        .into_iter()
        .map(|file_name| crate::lease::CloudMetadataObject {
            file_name: file_name.to_string(),
            object_key: format!("metadata/generations/{generation_id}/{file_name}"),
            len: 1,
            crc32c: 1,
        })
        .collect();
        authority
            .store
            .publish_committed_metadata(
                &authority.holder_id,
                authority.writer_epoch,
                None,
                crate::lease::CloudMetadataGeneration {
                    manifest_sequence: 0,
                    objects,
                },
                std::time::Duration::from_secs(5),
            )
            .expect("commit metadata pointer");

        // Act
        let error = fence_remote_registry_on_startup(
            &storage,
            &authority,
            &crate::common::OperationDeadline::from_budget(std::time::Duration::from_secs(5)),
        )
        .expect_err("missing registry must fail closed");

        // Assert
        assert!(matches!(error, MidgeError::RecoveryFailed(_)));
        assert!(storage
            .remote_object_proof_optional(REMOTE_DDL_REGISTRY_KEY)?
            .is_none());
        Ok(())
    }

    /// A memory-mode state holding one empty column family, plus its id.
    fn state_with_column_family(label: &str) -> (RuntimeState, crate::types::ColumnFamilyId) {
        let mut state = RuntimeState::new(std::path::PathBuf::from(label), true);
        let edit = create_edit(&state, "drop-me").expect("create edit");
        apply_local_edit(&mut state, &edit).expect("apply create");
        let ManifestEdit::CreateColumnFamily { id, .. } = edit else {
            unreachable!("create_edit returns a CreateColumnFamily edit")
        };
        (state, id)
    }

    #[test]
    fn should_license_unflushed_discard_when_safe_drop_finds_committed_data_in_active_memtable() {
        // Arrange
        let (state, cf_id) = state_with_column_family("/tmp/midge-ddl-licence");
        state
            .get_cf(cf_id)
            .expect("column family")
            .memtable
            .put_with_seq(b"key".to_vec(), b"value".to_vec(), 1, None)
            .expect("write into the active memtable");

        // Act
        let error = drop_edit(&state, cf_id, false).expect_err("safe drop must refuse");

        // Assert
        assert!(
            error.licenses_unflushed_discard(),
            "the refusal must be the discard licence: {error}"
        );
        assert!(matches!(
            error,
            MidgeError::UnflushedDataPresent { cf_id: reported, bytes } if reported == cf_id && bytes > 0
        ));
    }

    #[test]
    fn should_not_license_unflushed_discard_when_flush_publication_is_still_in_flight() {
        // Arrange: an empty active memtable, so the only thing blocking the
        // drop is publication work that will clear on its own.
        let (mut state, cf_id) = state_with_column_family("/tmp/midge-ddl-inflight");
        let memtable = std::sync::Arc::clone(&state.get_cf(cf_id).expect("column family").memtable);
        state
            .track_new_immutable_flush(cf_id, memtable, 1)
            .expect("queue a flush");

        // Act
        let error = drop_edit(&state, cf_id, false).expect_err("safe drop must refuse");

        // Assert
        assert!(
            !error.licenses_unflushed_discard(),
            "in-flight publication must never be read as permission to discard data: {error}"
        );
        assert!(matches!(error, MidgeError::Busy(_)));
    }

    #[test]
    fn should_report_unflushed_data_before_publication_work_when_both_block_a_safe_drop() {
        // Arrange: both conditions hold at once. Only one of them is
        // actionable by the caller, and that is the one they must be told.
        let (mut state, cf_id) = state_with_column_family("/tmp/midge-ddl-both");
        let memtable = std::sync::Arc::clone(&state.get_cf(cf_id).expect("column family").memtable);
        memtable
            .put_with_seq(b"key".to_vec(), b"value".to_vec(), 1, None)
            .expect("write into the active memtable");
        state
            .track_new_immutable_flush(cf_id, memtable, 1)
            .expect("queue a flush");

        // Act
        let safe = drop_edit(&state, cf_id, false).expect_err("safe drop must refuse");
        let destructive = drop_edit(&state, cf_id, true).expect_err("destructive drop must wait");

        // Assert
        assert!(
            safe.licenses_unflushed_discard(),
            "unflushed data outranks publication work on the safe path: {safe}"
        );
        // The destructive path skips the data check but still waits for
        // publication, so it reports the condition that really blocks it.
        assert!(
            !destructive.licenses_unflushed_discard(),
            "the destructive path has no data question left to answer: {destructive}"
        );
        assert!(matches!(destructive, MidgeError::Busy(_)));
    }

    #[test]
    fn should_classify_cas_outcome_from_typed_flag_not_message_text() {
        // Arrange: the old classifier read phrases out of the message, so
        // rewording an error silently changed the recovery path.
        let not_committed = crate::storage::hybrid::backend::RemoteCasFailure {
            may_have_committed: false,
            error: MidgeError::Internal("provider rejected the request".to_string()),
        };
        let ambiguous = crate::storage::hybrid::backend::RemoteCasFailure {
            may_have_committed: true,
            error: MidgeError::Timeout("remote CAS stalled before mutation".to_string()),
        };

        // Act
        let definite = remote_cas_definitely_not_committed(&not_committed);
        let uncertain = remote_cas_definitely_not_committed(&ambiguous);

        // Assert
        assert!(
            definite,
            "a pre-submission failure is definitely not committed"
        );
        assert!(
            !uncertain,
            "an ambiguous failure stays ambiguous even when its text says otherwise"
        );
    }
}
