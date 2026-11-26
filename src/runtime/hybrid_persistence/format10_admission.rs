//! Durable, bounded admission for immutable FORMAT 10 cloud publications.

use cntryl_midge_core::common::{MidgeError, MidgeResult, OperationDeadline};
use cntryl_midge_durability::lease::CloudMetadataObject;
use cntryl_midge_storage::cloud::CloudStorage;
use cntryl_midge_storage::{
    StorageBackend, StorageError, StorageEvent, StorageObjectMetadata, StorageOutcome,
    StoragePrecondition, StorageRequest, StorageWriteReceipt,
};
use serde::{Deserialize, Serialize};
use std::collections::{BTreeSet, HashSet};
use std::sync::mpsc;
use uuid::Uuid;

pub(crate) const FORMAT10_ADMISSION_SLOT_COUNT: usize = 3;
pub(crate) const FORMAT10_ADMISSION_MAX_REFERENCES: usize = 16;
pub(crate) const FORMAT10_ADMISSION_MAX_BYTES: u64 = 64 * 1024 * 1024;
const FORMAT10_ADMISSION_PREFIX: &str = "metadata/admission/v1/";
const MAX_RECORD_BYTES: usize = 64 * 1024;
const MAX_KEY_BYTES: usize = 1024;
const MAX_ATTEMPTS: usize = 3;

enum AdmissionPutError {
    Storage(StorageError),
    Timeout(String),
}

impl AdmissionPutError {
    fn is_precondition_failed(&self) -> bool {
        matches!(self, Self::Storage(error) if error.is_precondition_failed())
    }

    fn into_midge(self) -> MidgeError {
        match self {
            Self::Storage(error) => MidgeError::Io(std::io::Error::other(error.into_message())),
            Self::Timeout(message) => MidgeError::Timeout(message),
        }
    }
}

#[derive(Clone, Copy, Debug, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub(crate) enum AdmissionPhase {
    Pending,
    Uploaded,
    Finished,
}

#[derive(Clone, Debug, PartialEq, Eq)]
pub(crate) struct AdmissionRecord {
    pub(crate) slot: u8,
    pub(crate) publication_id: Uuid,
    pub(crate) writer_epoch: u64,
    pub(crate) phase: AdmissionPhase,
    pub(crate) objects: Vec<CloudMetadataObject>,
    pub(crate) identity: StorageObjectMetadata,
}

#[derive(Clone, Debug, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
struct AdmissionWire {
    format_version: u8,
    publication_id: String,
    writer_epoch: u64,
    phase: AdmissionPhase,
    objects: Vec<CloudMetadataObject>,
}

pub(crate) struct Format10AdmissionStore<'a> {
    cloud: &'a CloudStorage,
    deadline: &'a OperationDeadline,
}

impl<'a> Format10AdmissionStore<'a> {
    pub(crate) fn new(cloud: &'a CloudStorage, deadline: &'a OperationDeadline) -> Self {
        Self { cloud, deadline }
    }

    /// Read every fixed slot from one provider response identity per body.
    pub(crate) fn list_slots(&self) -> MidgeResult<[Option<AdmissionRecord>; 3]> {
        let mut slots: [Option<AdmissionRecord>; FORMAT10_ADMISSION_SLOT_COUNT] =
            std::array::from_fn(|_| None);
        let mut publication_ids = HashSet::new();
        for (slot, result) in slots.iter_mut().enumerate() {
            let slot = u8::try_from(slot).map_err(|_| {
                MidgeError::Internal("FORMAT 10 admission slot index overflowed u8".into())
            })?;
            *result = self.read_slot(slot)?;
            if result
                .as_ref()
                .is_some_and(|record| !publication_ids.insert(record.publication_id))
            {
                return Err(MidgeError::Corruption(
                    "FORMAT 10 publication appears in multiple admission slots".into(),
                ));
            }
        }
        Ok(slots)
    }

    pub(crate) fn read_slot(&self, slot: u8) -> MidgeResult<Option<AdmissionRecord>> {
        validate_slot(slot)?;
        let key = slot_key(slot);
        let Some((bytes, identity)) = self.read_raw(&key)? else {
            return Ok(None);
        };
        decode_record(slot, &bytes, identity).map(Some)
    }

    /// Claim an empty slot before any immutable publication body is uploaded.
    /// Occupied slots are skipped only after a read proves a durable object is
    /// present. Unknown writes and failed readbacks keep the caller fenced.
    pub(crate) fn reserve(
        &self,
        publication_id: Uuid,
        writer_epoch: u64,
        planned: &[CloudMetadataObject],
    ) -> MidgeResult<AdmissionRecord> {
        if writer_epoch == 0 {
            return Err(MidgeError::Fenced(
                "FORMAT 10 admission requires an active writer epoch".into(),
            ));
        }
        let objects = canonical_plan(publication_id, planned)?;
        let wire = AdmissionWire {
            format_version: 1,
            publication_id: publication_id.to_string(),
            writer_epoch,
            phase: AdmissionPhase::Pending,
            objects,
        };
        let bytes = encode_wire(&wire)?;
        let existing = self.list_slots()?;
        if existing.iter().flatten().any(|record| {
            record.publication_id == publication_id
                && (record.writer_epoch != writer_epoch || record.objects != wire.objects)
        }) {
            return Err(MidgeError::Corruption(
                "FORMAT 10 admission UUID is already bound to a different plan".into(),
            ));
        }
        if existing
            .iter()
            .flatten()
            .any(|record| record.publication_id == publication_id)
        {
            return Err(MidgeError::Busy(
                "FORMAT 10 publication already has an admission record".into(),
            ));
        }
        let mut occupied = existing.iter().flatten().count();
        for index in 0..FORMAT10_ADMISSION_SLOT_COUNT {
            let slot = u8::try_from(index).map_err(|_| {
                MidgeError::Internal("FORMAT 10 admission slot index overflowed u8".into())
            })?;
            if existing[usize::from(slot)].is_some() {
                continue;
            }
            let key = slot_key(slot);
            match self.put(&key, &bytes, StoragePrecondition::IfAbsent) {
                Ok(Some(receipt)) if receipt.matches(&bytes) => {
                    return self.confirm_record(slot, &bytes);
                }
                Ok(Some(_)) => {
                    return Err(MidgeError::Corruption(
                        "FORMAT 10 admission receipt differs from the reserved body".into(),
                    ));
                }
                Ok(None) => return self.confirm_record(slot, &bytes),
                Err(error) if error.is_precondition_failed() => match self.read_slot(slot)? {
                    Some(record) if record.publication_id == publication_id => {
                        return Err(MidgeError::Busy(
                            "FORMAT 10 publication already has an admission record".into(),
                        ));
                    }
                    Some(_) => occupied += 1,
                    None => return Err(error.into_midge()),
                },
                // This create might still land. No other slot may be used.
                Err(error) => return Err(error.into_midge()),
            }
        }
        if occupied == FORMAT10_ADMISSION_SLOT_COUNT {
            return Err(MidgeError::ResourceLimit(
                "all FORMAT 10 cloud admission slots are occupied".into(),
            ));
        }
        Err(MidgeError::Corruption(
            "FORMAT 10 cloud admission reservation did not settle".into(),
        ))
    }

    /// Mark all referenced body writes positively settled. This CAS is
    /// idempotent and resolves lost responses through the exact slot body.
    pub(crate) fn mark_uploaded(
        &self,
        publication_id: Uuid,
        planned: &[CloudMetadataObject],
    ) -> MidgeResult<AdmissionRecord> {
        let objects = canonical_plan(publication_id, planned)?;
        self.transition(publication_id, Some(&objects), AdmissionPhase::Uploaded)
    }

    /// Persist that no caller will attempt to select this publication again.
    /// Callers must establish that property before invoking this transition.
    pub(crate) fn finish(&self, publication_id: Uuid) -> MidgeResult<AdmissionRecord> {
        self.transition(publication_id, None, AdmissionPhase::Finished)
    }

    /// Remove only the exact finished slot version and only after the caller
    /// has removed every planned body following its own reference proof.
    pub(crate) fn delete_finished(
        &self,
        record: &AdmissionRecord,
        removed_object_keys: &BTreeSet<String>,
    ) -> MidgeResult<bool> {
        if record.phase != AdmissionPhase::Finished {
            return Err(MidgeError::Fenced(
                "pending FORMAT 10 admission cannot be reclaimed".into(),
            ));
        }
        let expected = record
            .objects
            .iter()
            .map(|object| object.object_key.clone())
            .collect::<BTreeSet<_>>();
        if &expected != removed_object_keys {
            return Err(MidgeError::Fenced(
                "FORMAT 10 admission cleanup does not prove every planned body removed".into(),
            ));
        }
        validate_slot(record.slot)?;
        let key = slot_key(record.slot);
        let Some(current) = self.read_slot(record.slot)? else {
            return Ok(true);
        };
        if !same_record(&current, record) || current.identity != record.identity {
            return Ok(false);
        }
        let request = self
            .request(&key)?
            .with_precondition(StoragePrecondition::IfMatch(record.identity.clone()));
        let (tx, rx) = mpsc::channel();
        self.cloud.submit_delete_request(request.clone(), tx);
        let completion = rx.recv_timeout(request.remaining_timeout());
        if matches!(
            completion,
            Ok(StorageEvent::DeleteComplete {
                key: ref actual,
                result: StorageOutcome::Ok(()),
            }) if actual == &key
        ) {
            return Ok(true);
        }
        match self.read_slot(record.slot)? {
            None => Ok(true),
            Some(after) if same_record(&after, record) && after.identity == record.identity => {
                Ok(false)
            }
            Some(_) => Ok(false),
        }
    }

    fn transition(
        &self,
        publication_id: Uuid,
        planned: Option<&[CloudMetadataObject]>,
        target: AdmissionPhase,
    ) -> MidgeResult<AdmissionRecord> {
        for _ in 0..MAX_ATTEMPTS {
            let current = self.find_by_uuid(publication_id)?.ok_or_else(|| {
                MidgeError::Corruption("FORMAT 10 admission record is missing".into())
            })?;
            if planned.is_some_and(|objects| objects != current.objects) {
                return Err(MidgeError::Corruption(
                    "FORMAT 10 admission refs differ from the reserved plan".into(),
                ));
            }
            if phase_rank(current.phase) >= phase_rank(target) {
                return Ok(current);
            }
            if target == AdmissionPhase::Finished && current.phase != AdmissionPhase::Uploaded {
                return Err(MidgeError::Fenced(
                    "FORMAT 10 admission cannot finish before all bodies are uploaded".into(),
                ));
            }
            let wire = AdmissionWire {
                format_version: 1,
                publication_id: publication_id.to_string(),
                writer_epoch: current.writer_epoch,
                phase: target,
                objects: current.objects.clone(),
            };
            let bytes = encode_wire(&wire)?;
            let key = slot_key(current.slot);
            let result = self.put(
                &key,
                &bytes,
                StoragePrecondition::IfMatch(current.identity.clone()),
            );
            match result {
                Ok(Some(receipt)) if receipt.matches(&bytes) => {
                    if let Some(record) = self.confirm_transition(&current, &wire, &bytes)? {
                        return Ok(record);
                    }
                }
                Ok(Some(_)) => {
                    return Err(MidgeError::Corruption(
                        "FORMAT 10 admission CAS receipt differs".into(),
                    ));
                }
                Ok(None) | Err(_) => {
                    if let Some(record) = self.confirm_transition(&current, &wire, &bytes)? {
                        return Ok(record);
                    }
                }
            }
        }
        Err(MidgeError::Timeout(
            "FORMAT 10 admission state transition did not settle".into(),
        ))
    }

    fn find_by_uuid(&self, publication_id: Uuid) -> MidgeResult<Option<AdmissionRecord>> {
        let mut found = None;
        for record in self.list_slots()?.into_iter().flatten() {
            if record.publication_id == publication_id {
                if found.is_some() {
                    return Err(MidgeError::Corruption(
                        "FORMAT 10 publication appears in multiple admission slots".into(),
                    ));
                }
                found = Some(record);
            }
        }
        Ok(found)
    }

    fn confirm_transition(
        &self,
        previous: &AdmissionRecord,
        desired: &AdmissionWire,
        desired_bytes: &[u8],
    ) -> MidgeResult<Option<AdmissionRecord>> {
        let Some((actual, identity)) = self.read_raw(&slot_key(previous.slot))? else {
            return Ok(None);
        };
        if actual == desired_bytes {
            return decode_record(previous.slot, &actual, identity).map(Some);
        }
        let observed = decode_record(previous.slot, &actual, identity)?;
        if observed.publication_id == previous.publication_id
            && observed.writer_epoch == previous.writer_epoch
            && observed.objects == previous.objects
            && phase_rank(observed.phase) >= phase_rank(desired.phase)
        {
            return Ok(Some(observed));
        }
        Ok(None)
    }

    fn confirm_record(&self, slot: u8, bytes: &[u8]) -> MidgeResult<AdmissionRecord> {
        let Some((actual, identity)) = self.read_raw(&slot_key(slot))? else {
            return Err(MidgeError::Corruption(
                "FORMAT 10 admission reservation vanished before readback".into(),
            ));
        };
        if actual != bytes {
            return Err(MidgeError::Corruption(
                "FORMAT 10 admission reservation changed before readback".into(),
            ));
        }
        let reserved = decode_record(slot, &actual, identity)?;
        if self.find_by_uuid(reserved.publication_id)?.as_ref() != Some(&reserved) {
            return Err(MidgeError::Fenced(
                "FORMAT 10 reservation changed during readback".into(),
            ));
        }
        Ok(reserved)
    }

    fn read_raw(&self, key: &str) -> MidgeResult<Option<(Vec<u8>, StorageObjectMetadata)>> {
        let request = self.request(key)?;
        let (tx, rx) = mpsc::channel();
        self.cloud.submit_range_head_request(request.clone(), tx);
        let identity = match rx.recv_timeout(request.remaining_timeout()) {
            Ok(StorageEvent::HeadComplete {
                key: actual,
                result: StorageOutcome::Ok(identity),
            }) if actual == key => identity,
            Ok(StorageEvent::HeadComplete {
                result: StorageOutcome::Err(error),
                ..
            }) if error.is_not_found() => return Ok(None),
            Ok(StorageEvent::HeadComplete {
                result: StorageOutcome::Err(error),
                ..
            }) => return Err(storage_error(error)),
            Ok(event) => {
                return Err(MidgeError::Corruption(format!(
                    "FORMAT 10 admission HEAD returned an unexpected event: {event:?}"
                )));
            }
            Err(error) => {
                return Err(MidgeError::Timeout(format!(
                    "FORMAT 10 admission HEAD callback failed: {error}"
                )));
            }
        };
        if identity.size == 0 || identity.size > MAX_RECORD_BYTES as u64 {
            return Err(MidgeError::ResourceLimit(
                "FORMAT 10 admission slot exceeds its wire bound".into(),
            ));
        }
        if identity
            .generation
            .as_deref()
            .is_none_or(|value| value.trim().is_empty())
            && identity.etag.trim().is_empty()
        {
            return Err(MidgeError::Corruption(
                "FORMAT 10 admission slot has no conditional object identity".into(),
            ));
        }
        let range_request = self
            .request(key)?
            .with_precondition(StoragePrecondition::IfMatch(identity.clone()));
        let (tx, rx) = mpsc::channel();
        self.cloud
            .submit_range_read_request(range_request.clone(), 0..identity.size, tx);
        let bytes = rx
            .recv_timeout(range_request.remaining_timeout())
            .map_err(|error| {
                MidgeError::Timeout(format!(
                    "FORMAT 10 admission pinned range callback failed: {error}"
                ))
            })?
            .map_err(storage_error)?;
        if u64::try_from(bytes.len()).unwrap_or(u64::MAX) != identity.size {
            return Err(MidgeError::Corruption(
                "FORMAT 10 admission slot length differs from pinned identity".into(),
            ));
        }
        Ok(Some((bytes, identity)))
    }

    fn put(
        &self,
        key: &str,
        bytes: &[u8],
        precondition: StoragePrecondition,
    ) -> Result<Option<StorageWriteReceipt>, AdmissionPutError> {
        let request = self
            .request(key)
            .map_err(|error| AdmissionPutError::Timeout(error.to_string()))?
            .with_precondition(precondition);
        let (tx, rx) = mpsc::channel();
        self.cloud
            .submit_write_receipt_request(request.clone(), bytes.to_vec(), tx);
        match rx.recv_timeout(request.remaining_timeout()) {
            Ok(result) => result.map_err(AdmissionPutError::Storage),
            Err(error) => Err(AdmissionPutError::Timeout(format!(
                "FORMAT 10 admission PUT callback failed: {error}"
            ))),
        }
    }

    fn request(&self, key: &str) -> MidgeResult<StorageRequest> {
        let timeout = self
            .deadline
            .clamp_nonzero(self.cloud.callback_timeout())
            .ok_or_else(|| MidgeError::Timeout("FORMAT 10 admission deadline exhausted".into()))?;
        Ok(StorageRequest::new(key, *self.deadline, timeout))
    }
}

fn storage_error(error: StorageError) -> MidgeError {
    MidgeError::Io(std::io::Error::other(error.into_message()))
}

fn canonical_plan(
    publication_id: Uuid,
    planned: &[CloudMetadataObject],
) -> MidgeResult<Vec<CloudMetadataObject>> {
    if publication_id.is_nil()
        || planned.is_empty()
        || planned.len() > FORMAT10_ADMISSION_MAX_REFERENCES
    {
        return Err(MidgeError::ResourceLimit(
            "FORMAT 10 admission plan exceeds its reference bound".into(),
        ));
    }
    let mut objects = planned.to_vec();
    objects.sort_by(|left, right| left.file_name.cmp(&right.file_name));
    validate_objects(publication_id, &objects, true)?;
    Ok(objects)
}

fn validate_objects(
    publication_id: Uuid,
    objects: &[CloudMetadataObject],
    require_special: bool,
) -> MidgeResult<()> {
    if objects.is_empty() || objects.len() > FORMAT10_ADMISSION_MAX_REFERENCES {
        return Err(MidgeError::ResourceLimit(
            "FORMAT 10 admission reference count exceeds its bound".into(),
        ));
    }
    let mut seen_names = HashSet::new();
    let mut total = 0_u64;
    let mut has_format = false;
    let mut has_head = false;
    let mut has_manifest_base = false;
    let mut has_intent_base = false;
    for object in objects {
        if object.len == 0
            || object.file_name.len() > MAX_KEY_BYTES
            || object.object_key.len() > MAX_KEY_BYTES
            || !seen_names.insert(object.file_name.as_str())
            || object.object_key
                != format!("metadata/generations/{publication_id}/{}", object.file_name)
        {
            return Err(MidgeError::Corruption(
                "FORMAT 10 admission contains a noncanonical or duplicate object ref".into(),
            ));
        }
        let max_object_len = match object.file_name.as_str() {
            cntryl_midge_durability::metadata::files::FORMAT => {
                has_format = true;
                if object.len != b"midge-format-version=10\n".len() as u64 {
                    return Err(MidgeError::Corruption(
                        "FORMAT 10 admission marker length is invalid".into(),
                    ));
                }
                total = total.checked_add(object.len).ok_or_else(|| {
                    MidgeError::ResourceLimit("FORMAT 10 admission length overflow".into())
                })?;
                continue;
            }
            cntryl_midge_durability::metadata::local_authority::HEAD_PATH => {
                has_head = true;
                cntryl_midge_durability::metadata::local_authority::MAX_HEAD_BYTES as u64
            }
            cntryl_midge_durability::metadata::files::CLOUD_CUT_PROOF => {
                cntryl_midge_durability::metadata::files::MAX_CLOUD_CUT_PROOF_BYTES as u64
            }
            name if cntryl_midge_durability::metadata::local_authority::closure_path_parts(
                name,
            )
            .is_ok() =>
            {
                let (_, body) =
                    cntryl_midge_durability::metadata::local_authority::closure_path_parts(name)?;
                has_manifest_base |= body == "manifest/base";
                has_intent_base |= body == "intent/base";
                cntryl_midge_durability::metadata::local_authority::MAX_CLOSURE_BYTES
            }
            _ => {
                return Err(MidgeError::Corruption(
                    "FORMAT 10 admission references an unrecognized body".into(),
                ));
            }
        };
        if object.len > max_object_len {
            return Err(MidgeError::ResourceLimit(
                "FORMAT 10 admission object exceeds its byte bound".into(),
            ));
        }
        total = total.checked_add(object.len).ok_or_else(|| {
            MidgeError::ResourceLimit("FORMAT 10 admission length overflow".into())
        })?;
    }
    if objects
        .windows(2)
        .any(|pair| pair[0].file_name >= pair[1].file_name)
        || total > FORMAT10_ADMISSION_MAX_BYTES
        || (require_special && !(has_format && has_head && has_manifest_base && has_intent_base))
    {
        return Err(MidgeError::Corruption(
            "FORMAT 10 admission inventory is incomplete or noncanonical".into(),
        ));
    }
    Ok(())
}

fn encode_wire(wire: &AdmissionWire) -> MidgeResult<Vec<u8>> {
    let bytes = serde_json::to_vec(wire)
        .map_err(|error| MidgeError::Corruption(format!("admission encoding failed: {error}")))?;
    if bytes.len() > MAX_RECORD_BYTES {
        return Err(MidgeError::ResourceLimit(
            "FORMAT 10 admission record exceeds its wire bound".into(),
        ));
    }
    Ok(bytes)
}

fn decode_record(
    slot: u8,
    bytes: &[u8],
    identity: StorageObjectMetadata,
) -> MidgeResult<AdmissionRecord> {
    validate_slot(slot)?;
    if bytes.len() > MAX_RECORD_BYTES || identity.size != bytes.len() as u64 {
        return Err(MidgeError::ResourceLimit(
            "FORMAT 10 admission record exceeds its wire bound".into(),
        ));
    }
    if identity
        .generation
        .as_deref()
        .is_none_or(|value| value.trim().is_empty())
        && identity.etag.trim().is_empty()
    {
        return Err(MidgeError::Corruption(
            "FORMAT 10 admission slot has no conditional object identity".into(),
        ));
    }
    let wire: AdmissionWire = serde_json::from_slice(bytes).map_err(|error| {
        MidgeError::Corruption(format!("FORMAT 10 admission record is invalid: {error}"))
    })?;
    let publication_id = Uuid::parse_str(&wire.publication_id).map_err(|error| {
        MidgeError::Corruption(format!("FORMAT 10 admission UUID is invalid: {error}"))
    })?;
    if wire.format_version != 1
        || publication_id.is_nil()
        || wire.writer_epoch == 0
        || publication_id.to_string() != wire.publication_id
        || serde_json::to_vec(&wire).ok().as_deref() != Some(bytes)
    {
        return Err(MidgeError::Corruption(
            "FORMAT 10 admission record is not canonical".into(),
        ));
    }
    validate_objects(publication_id, &wire.objects, true)?;
    Ok(AdmissionRecord {
        slot,
        publication_id,
        writer_epoch: wire.writer_epoch,
        phase: wire.phase,
        objects: wire.objects,
        identity,
    })
}

fn phase_rank(phase: AdmissionPhase) -> u8 {
    match phase {
        AdmissionPhase::Pending => 0,
        AdmissionPhase::Uploaded => 1,
        AdmissionPhase::Finished => 2,
    }
}

fn same_record(left: &AdmissionRecord, right: &AdmissionRecord) -> bool {
    left.slot == right.slot
        && left.publication_id == right.publication_id
        && left.writer_epoch == right.writer_epoch
        && left.phase == right.phase
        && left.objects == right.objects
}

fn slot_key(slot: u8) -> String {
    format!("{FORMAT10_ADMISSION_PREFIX}{slot}")
}

fn validate_slot(slot: u8) -> MidgeResult<()> {
    if usize::from(slot) < FORMAT10_ADMISSION_SLOT_COUNT {
        Ok(())
    } else {
        Err(MidgeError::ResourceLimit(
            "FORMAT 10 admission slot index is out of range".into(),
        ))
    }
}

#[cfg(test)]
mod tests;
