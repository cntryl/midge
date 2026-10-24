//! Independent metadata pointer with an epoch claim before writer activation.
//!
//! Claim and publication CAS the same pointer identity. A delayed predecessor
//! either commits before the claim (and its history is retained), or loses its
//! old identity after the claim. Learning a successor identity cannot authorize
//! a stale writer: every publication also matches the pointer's epoch and owner.

use super::*;
use crate::storage::{StorageBackend, StoragePrecondition, StorageRequest};
use serde::{Deserialize, Serialize};

const POINTER_KEY: &str = "metadata/committed-pointer.v1";
const POINTER_KIND: u8 = 4;

#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
struct MetadataPointer {
    writer_epoch: u64,
    holder_id: String,
    owner_token: String,
    committed: Option<CloudMetadataGeneration>,
}

impl MetadataPointer {
    fn owns(&self, store: &ProviderLeaderStore, holder_id: &str, epoch: u64) -> bool {
        self.writer_epoch == epoch
            && self.holder_id == holder_id
            && self.owner_token == store.owner_token
    }
}

impl ProviderLeaderStore {
    pub(super) fn claim_metadata_pointer(
        &self,
        cloud: &CloudStorage,
        document: &LeaseDocument,
        timeout: Duration,
    ) -> Result<(), LeaseError> {
        let deadline = crate::common::OperationDeadline::from_budget(timeout);
        let epoch = document.epoch.ok_or(LeaseError::EpochExhausted)?;
        if document.version != LeaseDocumentVersion::V4 {
            return Err(legacy_metadata_migration_error());
        }
        loop {
            let current = read_pointer(cloud, remaining(cloud, &deadline)?)?;
            if current.is_none()
                && provider_read_authority_sentinel(cloud, remaining(cloud, &deadline)?)?
                    != AuthoritySentinelState::Pending
            {
                return Err(LeaseError::Indeterminate(
                    "initialized metadata pointer disappeared; refusing empty repair".into(),
                ));
            }
            if current
                .as_ref()
                .is_some_and(|(pointer, _)| pointer.owns(self, &document.holder_id, epoch))
            {
                return Ok(());
            }
            if current
                .as_ref()
                .is_some_and(|(pointer, _)| pointer.writer_epoch >= epoch)
            {
                return Err(LeaseError::RenewalFailed(
                    "metadata pointer belongs to a newer or different writer".into(),
                ));
            }
            let replacement = MetadataPointer {
                writer_epoch: epoch,
                holder_id: document.holder_id.clone(),
                owner_token: self.owner_token.clone(),
                committed: current
                    .as_ref()
                    .and_then(|(pointer, _)| pointer.committed.clone()),
            };
            let condition = current
                .as_ref()
                .map_or(StoragePrecondition::IfAbsent, |(_, metadata)| {
                    StoragePrecondition::IfMatch(metadata.clone())
                });
            match write_pointer(cloud, &replacement, condition, &deadline) {
                Err(LeaseError::AcquisitionFailed(_)) => continue,
                outcome => return outcome,
            }
        }
    }

    pub(super) fn read_separate_metadata_head(
        &self,
        timeout: Duration,
    ) -> Result<CloudMetadataHead, LeaseError> {
        let deadline = crate::common::OperationDeadline::from_budget(timeout);
        let pointer = read_pointer(&self.cloud, remaining(&self.cloud, &deadline)?)?;
        let lease =
            provider_read_doc_with_timeout(&self.cloud, remaining(&self.cloud, &deadline)?)?;
        if lease
            .as_ref()
            .is_some_and(|lease| lease.version != LeaseDocumentVersion::V4)
        {
            return Err(legacy_metadata_migration_error());
        }
        let sentinel =
            provider_read_authority_sentinel(&self.cloud, remaining(&self.cloud, &deadline)?)?;
        match (pointer, lease, sentinel) {
            (Some((pointer, _)), Some(_), AuthoritySentinelState::Active) => Ok(pointer
                .committed
                .map_or(CloudMetadataHead::Uncommitted, CloudMetadataHead::Committed)),
            (Some((pointer, _)), Some(_), AuthoritySentinelState::Pending)
                if pointer.committed.is_none() =>
            {
                Ok(CloudMetadataHead::Uncommitted)
            }
            (None, Some(_), AuthoritySentinelState::Pending) => Ok(CloudMetadataHead::Uncommitted),
            (None, None, AuthoritySentinelState::Missing | AuthoritySentinelState::Pending) => {
                Ok(CloudMetadataHead::MissingLease)
            }
            _ => Err(LeaseError::Indeterminate(
                "initialized cloud metadata authority is missing or inconsistent".into(),
            )),
        }
    }

    pub(super) fn publish_separate_metadata(
        &self,
        holder_id: &str,
        epoch: u64,
        previous: Option<&CloudMetadataGeneration>,
        generation: CloudMetadataGeneration,
        timeout: Duration,
    ) -> Result<(), LeaseError> {
        validate_metadata_generation(&generation)?;
        let deadline = crate::common::OperationDeadline::from_budget(timeout);
        let validity = self.validity.remaining(epoch)?;
        let budget = validity
            .checked_sub(RENEWAL_WRITE_DEADLINE_MARGIN)
            .filter(|budget| !budget.is_zero())
            .ok_or_else(|| {
                LeaseError::RenewalFailed("insufficient validity for metadata pointer CAS".into())
            })?;
        let deadline = deadline.earlier(crate::common::OperationDeadline::from_budget(budget));
        let (current, metadata) = read_pointer(&self.cloud, remaining(&self.cloud, &deadline)?)?
            .ok_or_else(|| {
                LeaseError::Indeterminate("initialized metadata pointer disappeared".into())
            })?;
        if !current.owns(self, holder_id, epoch) {
            return Err(LeaseError::RenewalFailed(
                "metadata pointer writer epoch or owner changed".into(),
            ));
        }
        self.validity.remaining(epoch)?;
        if current.committed.as_ref() == Some(&generation) {
            return Ok(());
        }
        if current.committed.as_ref() != previous {
            return Err(LeaseError::RenewalFailed(
                "metadata pointer predecessor changed".into(),
            ));
        }
        generation.validate_successor(current.committed.as_ref())?;
        let replacement = MetadataPointer {
            committed: Some(generation),
            ..current
        };
        write_pointer(
            &self.cloud,
            &replacement,
            StoragePrecondition::IfMatch(metadata),
            &deadline,
        )?;
        self.validity.remaining(epoch).map(|_| ())
    }
}

fn remaining(
    cloud: &CloudStorage,
    deadline: &crate::common::OperationDeadline,
) -> Result<Duration, LeaseError> {
    deadline
        .clamp_nonzero(cloud.callback_timeout())
        .ok_or_else(|| LeaseError::Timeout("metadata pointer deadline exhausted".into()))
}

fn read_pointer(
    cloud: &CloudStorage,
    timeout: Duration,
) -> Result<Option<(MetadataPointer, ObjectMetadata)>, LeaseError> {
    match provider_read_event_with_timeout(timeout, |budget, callback| {
        cloud.submit_get_with_metadata_within(POINTER_KEY, budget, callback);
    }) {
        Ok(CloudEvent::GetWithMetadata {
            result: CloudOutcome::Ok((bytes, metadata)),
            ..
        }) => {
            if mutation_precondition_headers(&metadata).is_none() {
                return Err(LeaseError::Indeterminate(
                    "metadata pointer has no conditional identity".into(),
                ));
            }
            if metadata.size != bytes.len() as u64 {
                return Err(LeaseError::Indeterminate(
                    "metadata pointer length differs from identity".into(),
                ));
            }
            let pointer: MetadataPointer = crate::metadata::codec::decode(POINTER_KIND, &bytes)
                .map_err(|error| {
                    LeaseError::Indeterminate(format!("metadata pointer decode: {error}"))
                })?;
            if pointer.writer_epoch == 0
                || pointer.holder_id.is_empty()
                || pointer.owner_token.is_empty()
            {
                return Err(LeaseError::Indeterminate(
                    "metadata pointer writer identity is invalid".into(),
                ));
            }
            if let Some(generation) = &pointer.committed {
                validate_metadata_generation(generation)?;
            }
            Ok(Some((pointer, metadata)))
        }
        Ok(CloudEvent::GetWithMetadata {
            result: CloudOutcome::Err(error),
            ..
        }) if error.is_not_found() => Ok(None),
        Ok(CloudEvent::GetWithMetadata {
            result: CloudOutcome::Err(error),
            ..
        }) => Err(classify_lease_read_error("metadata pointer GET", &error)),
        Ok(other) => Err(LeaseError::Indeterminate(format!(
            "unexpected metadata pointer response: {other:?}"
        ))),
        Err(error) => Err(LeaseError::Timeout(format!(
            "metadata pointer GET: {error}"
        ))),
    }
}

fn write_pointer(
    cloud: &CloudStorage,
    pointer: &MetadataPointer,
    condition: StoragePrecondition,
    deadline: &crate::common::OperationDeadline,
) -> Result<(), LeaseError> {
    let bytes = crate::metadata::codec::encode(POINTER_KIND, pointer)
        .map_err(|error| LeaseError::Internal(format!("encode metadata pointer: {error}")))?;
    let (tx, rx) = std::sync::mpsc::channel();
    cloud.submit_write_receipt_request(
        StorageRequest::new(POINTER_KEY, *deadline, remaining(cloud, deadline)?)
            .with_precondition(condition),
        bytes.clone(),
        tx,
    );
    let outcome = rx.recv_timeout(remaining(cloud, deadline)?);
    match outcome {
        Ok(Ok(Some(receipt))) if receipt.matches(&bytes) => return Ok(()),
        Ok(Ok(Some(_))) => {
            return Err(LeaseError::Indeterminate(
                "metadata pointer checksum receipt mismatch".into(),
            ))
        }
        Ok(Err(error)) if error.is_precondition_failed() => {
            return Err(LeaseError::AcquisitionFailed(format!(
                "metadata pointer CAS rejected: {error}"
            )))
        }
        _ => {}
    }
    // Missing receipt and unknown mutation outcomes require an exact pinned
    // readback. A successor's epoch claim cannot be mistaken for our success.
    match read_pointer(cloud, remaining(cloud, deadline)?)? {
        Some((actual, _)) if actual == *pointer => Ok(()),
        _ => Err(LeaseError::Indeterminate(
            "metadata pointer CAS was not proven by readback".into(),
        )),
    }
}

#[cfg(test)]
mod tests;
