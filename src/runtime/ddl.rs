//! Durable two-phase cloud DDL coordination.
//!
//! Column-family edits are small, but they change the set of readers and the
//! files that are authoritative.  In hybrid mode the local manifest journal
//! and the remote DDL registry therefore use an explicit prepare/CAS/commit
//! protocol.  A local prepare survives a crash and is reconciled at startup;
//! an operation is visible to the runtime only after the local journal append
//! succeeds.

use crate::common::{MidgeError, MidgeResult};
use crate::io::traits::{FsPath, OpenMode, OpenOptions};
use crate::metadata::{ColumnFamilyMeta, Manifest, ManifestEdit};
use crate::runtime::RuntimeState;
use crate::storage::hybrid::backend::{HybridStorage, RemoteObjectProof};
use serde::{Deserialize, Serialize};
use std::sync::Arc;

pub(crate) const REMOTE_DDL_REGISTRY_KEY: &str = "metadata/ddl.registry.json";
const LOCAL_DDL_PREPARE_FILE: &str = "ddl.prepare.json";
const LOCAL_DDL_PREPARE_TEMP: &str = "ddl.prepare.json.tmp";

#[derive(Debug, Clone, Serialize, Deserialize)]
pub(crate) struct DdlPrepare {
    pub(crate) op_id: String,
    pub(crate) expected_remote_epoch: u64,
    pub(crate) edit: ManifestEdit,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub(crate) struct DdlOperation {
    pub(crate) op_id: String,
    pub(crate) edit: ManifestEdit,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub(crate) struct DdlRegistry {
    pub(crate) epoch: u64,
    #[serde(default)]
    pub(crate) column_families: Vec<ColumnFamilyMeta>,
    #[serde(default)]
    pub(crate) operations: Vec<DdlOperation>,
}

pub(crate) fn create_edit(state: &RuntimeState, name: &str) -> MidgeResult<ManifestEdit> {
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

pub(crate) fn drop_edit(state: &RuntimeState, cf_id: u32) -> MidgeResult<ManifestEdit> {
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
            ManifestEdit::CreateColumnFamily { .. }
                | ManifestEdit::DropColumnFamily { .. }
                | ManifestEdit::DropColumnFamilyAt { .. }
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

fn serialize<T: Serialize>(value: &T) -> MidgeResult<Vec<u8>> {
    serde_json::to_vec(value).map_err(|error| MidgeError::Internal(error.to_string()))
}

fn deserialize<T: serde::de::DeserializeOwned>(bytes: &[u8]) -> MidgeResult<T> {
    serde_json::from_slice(bytes).map_err(|error| {
        MidgeError::Corruption(format!("invalid cloud DDL registry or prepare: {error}"))
    })
}

fn local_prepare_exists(state: &RuntimeState) -> MidgeResult<bool> {
    state
        .fs
        .exists(&FsPath::new(LOCAL_DDL_PREPARE_FILE))
        .map_err(MidgeError::from)
}

fn read_local_prepare(state: &RuntimeState) -> MidgeResult<Option<DdlPrepare>> {
    if !local_prepare_exists(state)? {
        return Ok(None);
    }
    let path = FsPath::new(LOCAL_DDL_PREPARE_FILE);
    let metadata = state.fs.metadata(&path).map_err(MidgeError::from)?;
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
        .map_err(MidgeError::from)?;
    let bytes = file.read_at(0, metadata.len).map_err(MidgeError::from)?;
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
        .map_err(MidgeError::from)?;
    state
        .fs
        .sync_dir(&FsPath::new("."), crate::io::traits::Durability::Durable)
        .map_err(MidgeError::from)
}

fn read_remote_registry(
    storage: &HybridStorage,
) -> Result<(Option<DdlRegistry>, Option<RemoteObjectProof>), String> {
    let proof = storage.remote_object_proof_optional(REMOTE_DDL_REGISTRY_KEY)?;
    let Some(proof) = proof else {
        return Ok((None, None));
    };
    let registry = deserialize(proof.bytes()).map_err(|error: MidgeError| error.to_string())?;
    Ok((Some(registry), Some(proof)))
}

fn write_remote_registry(
    storage: &HybridStorage,
    registry: &DdlRegistry,
    expected: Option<&RemoteObjectProof>,
) -> MidgeResult<RemoteObjectProof> {
    crate::failpoints::fail_point!("midge::ddl::before_remote_cas", |_| Err(
        MidgeError::Internal("failpoint: DDL remote CAS failed".to_string())
    ));
    let bytes = serialize(registry)?;
    let result = storage.compare_exchange_remote_object(
        REMOTE_DDL_REGISTRY_KEY,
        expected.map(RemoteObjectProof::metadata),
        bytes,
    )?;
    crate::failpoints::fail_point!("midge::ddl::after_remote_cas", |_| Err(
        MidgeError::Internal("failpoint: DDL remote CAS completion failed".to_string())
    ));
    Ok(result)
}

fn local_edit_matches(state: &RuntimeState, edit: &ManifestEdit) -> bool {
    match edit {
        ManifestEdit::CreateColumnFamily { id, name, .. } => state
            .manifest
            .column_families
            .iter()
            .any(|cf| cf.id == *id && cf.name == *name && cf.deleted_at.is_none()),
        ManifestEdit::DropColumnFamily { id } => state
            .manifest
            .column_families
            .iter()
            .any(|cf| cf.id == *id && cf.deleted_at.is_some()),
        ManifestEdit::DropColumnFamilyAt {
            id, drop_sequence, ..
        } => state.manifest.column_families.iter().any(|cf| {
            cf.id == *id && cf.deleted_at.is_some() && cf.drop_sequence == Some(*drop_sequence)
        }),
        _ => false,
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
    if !state.is_memory_mode() {
        crate::metadata::append_edit(&state.db_path, edit)?;
    }
    crate::failpoints::fail_point!("midge::ddl::after_local_journal_before_memory", |_| Err(
        MidgeError::Internal("failpoint: DDL local visibility failed".to_string(),)
    ));
    state.manifest = candidate;
    match edit {
        ManifestEdit::CreateColumnFamily { id, name, .. } => {
            state.column_families.entry(*id).or_insert_with(|| {
                crate::runtime::state::ColumnFamilyState::new(*id, name.clone())
            });
        }
        ManifestEdit::DropColumnFamily { id } | ManifestEdit::DropColumnFamilyAt { id, .. } => {
            state.column_families.remove(id);
        }
        _ => {}
    }
    Ok(())
}

/// Reconcile one local prepare with the remote registry.  A remote operation
/// is committed locally; an operation absent remotely is aborted.  Ambiguous
/// local visibility is refused rather than guessed.
pub(crate) fn reconcile_prepared(
    state: &mut RuntimeState,
    storage: Option<&Arc<HybridStorage>>,
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
    let (remote, _proof) = read_remote_registry(storage).map_err(MidgeError::Internal)?;
    let Some(remote) = remote else {
        if local_edit_matches(state, &prepare.edit) {
            return Err(MidgeError::RecoveryFailed(
                "local DDL commit is present but the remote registry is missing".to_string(),
            ));
        }
        clear_local_prepare(state)?;
        return Ok(());
    };
    if remote.operation(&prepare.op_id).is_some() {
        apply_local_edit(state, &prepare.edit)?;
        clear_local_prepare(state)?;
        return Ok(());
    }
    if local_edit_matches(state, &prepare.edit) {
        return Err(MidgeError::RecoveryFailed(
            "local DDL commit is present but its operation is absent remotely".to_string(),
        ));
    }
    clear_local_prepare(state)?;
    Ok(())
}

/// Reconcile the remote CF registry into a freshly recovered local state.
/// This also bootstraps an absent registry from the local manifest on the
/// first cloud open.
pub(crate) fn reconcile_startup(
    state: &mut RuntimeState,
    storage: Option<&Arc<HybridStorage>>,
) -> MidgeResult<()> {
    let Some(storage) = storage else {
        return Ok(());
    };
    reconcile_prepared(state, Some(storage))?;
    let (remote, proof) = read_remote_registry(storage).map_err(MidgeError::Internal)?;
    let Some(remote) = remote else {
        let registry = DdlRegistry::from_manifest(&state.manifest);
        let _ = write_remote_registry(storage, &registry, proof.as_ref())
            .map_err(|error| MidgeError::Internal(error.to_string()))?;
        return Ok(());
    };

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

/// Execute a create/drop edit under the remote CAS protocol.  Local-only
/// engines retain the existing journal-only behavior.
pub(crate) fn execute(
    state: &mut RuntimeState,
    storage: Option<&Arc<HybridStorage>>,
    edit: &ManifestEdit,
) -> MidgeResult<()> {
    reconcile_prepared(state, storage)?;
    if local_edit_matches(state, edit) {
        return Ok(());
    }
    let Some(storage) = storage else {
        return apply_local_edit(state, edit);
    };

    let (remote, proof) = read_remote_registry(storage).map_err(MidgeError::Internal)?;
    let mut registry = remote.unwrap_or_else(|| DdlRegistry::from_manifest(&state.manifest));
    let expected_epoch = registry.epoch;
    let prepare = DdlPrepare {
        op_id: uuid::Uuid::new_v4().to_string(),
        expected_remote_epoch: expected_epoch,
        edit: edit.clone(),
    };
    write_local_prepare(state, &prepare)?;
    registry.apply_edit(edit)?;
    registry.epoch = registry.epoch.saturating_add(1);
    registry.operations.push(DdlOperation {
        op_id: prepare.op_id.clone(),
        edit: edit.clone(),
    });
    write_remote_registry(storage, &registry, proof.as_ref())?;
    apply_local_edit(state, edit)?;
    if let Err(error) = clear_local_prepare(state) {
        tracing::warn!(%error, "cloud DDL committed but prepare cleanup will be retried");
    }
    Ok(())
}
