//! Remote object identity proofs and guarded deletion.

use super::{
    mpsc, thread, Arc, Duration, HybridStorage, JoinHandle, StorageBackend, StorageEvent,
    StorageOutcome,
};
use crate::common::OperationDeadline;
use crate::storage::StorageObjectMetadata;

/// A stable read plus identity observation for one remote object.
///
/// Format-aware validation belongs to the runtime. Storage only establishes
/// that the bytes it returned still match the provider identity observed by
/// `HEAD`.
#[derive(Clone, Debug)]
pub(crate) struct RemoteObjectProof {
    pub(super) key: String,
    pub(super) bytes: Vec<u8>,
    pub(super) metadata: StorageObjectMetadata,
}

impl RemoteObjectProof {
    pub(crate) fn bytes(&self) -> &[u8] {
        &self.bytes
    }

    pub(crate) fn metadata(&self) -> &StorageObjectMetadata {
        &self.metadata
    }
}

/// A format-neutral object identity that must still hold immediately before a
/// conditional delete is issued.
#[derive(Clone)]
pub(crate) struct GuardedObjectProof {
    pub(super) backend: Arc<dyn StorageBackend>,
    key: String,
    pub(super) expected_bytes: Option<Vec<u8>>,
    metadata: StorageObjectMetadata,
}

impl GuardedObjectProof {
    pub(crate) fn metadata_only(
        backend: Arc<dyn StorageBackend>,
        key: String,
        metadata: StorageObjectMetadata,
    ) -> Self {
        Self {
            backend,
            key,
            expected_bytes: None,
            metadata,
        }
    }

    pub(crate) fn exact(
        backend: Arc<dyn StorageBackend>,
        key: String,
        expected_bytes: Vec<u8>,
        metadata: StorageObjectMetadata,
    ) -> Self {
        Self {
            backend,
            key,
            expected_bytes: Some(expected_bytes),
            metadata,
        }
    }
}

pub(super) struct PruneWorkerRegistry {
    shutting_down: bool,
    handles: Vec<JoinHandle<()>>,
    max_workers: usize,
    max_requests: usize,
}

impl PruneWorkerRegistry {
    pub(super) fn new(max_workers: usize, max_requests: usize) -> Self {
        Self {
            shutting_down: false,
            handles: Vec::new(),
            max_workers: max_workers.max(1),
            max_requests: max_requests.max(1),
        }
    }
}

impl HybridStorage {
    pub(super) fn stable_object_proof_from_backend(
        backend: &Arc<dyn StorageBackend>,
        key: &str,
        callback_timeout: Duration,
    ) -> Result<RemoteObjectProof, String> {
        let before = Self::head_object_from_backend_blocking(backend, key, callback_timeout)?;
        let bytes = Self::read_object_from_backend_blocking(backend, key, callback_timeout)?;
        let after = Self::head_object_from_backend_blocking(backend, key, callback_timeout)?;
        if before != after {
            return Err(format!(
                "object '{key}' identity changed during read: before {before:?}, after {after:?}"
            ));
        }
        if after.size != bytes.len() as u64 {
            return Err(format!(
                "object '{key}' size changed during read: bytes={}, metadata={} ",
                bytes.len(),
                after.size
            ));
        }
        Ok(RemoteObjectProof {
            key: key.to_string(),
            bytes,
            metadata: after,
        })
    }

    pub(super) fn stable_object_proof_from_backend_within(
        backend: &Arc<dyn StorageBackend>,
        key: &str,
        callback_timeout: Duration,
        deadline: &OperationDeadline,
    ) -> crate::common::MidgeResult<RemoteObjectProof> {
        let before_timeout = Self::deadline_timeout(
            key,
            "initial HEAD during object proof",
            callback_timeout,
            deadline,
        )?;
        let before = Self::head_object_from_backend_blocking(backend, key, before_timeout)
            .map_err(|error| Self::proof_round_trip_error(key, "initial HEAD", error, deadline))?;

        let read_timeout =
            Self::deadline_timeout(key, "GET during object proof", callback_timeout, deadline)?;
        let bytes = Self::read_object_from_backend_blocking(backend, key, read_timeout)
            .map_err(|error| Self::proof_round_trip_error(key, "GET", error, deadline))?;

        let after_timeout = Self::deadline_timeout(
            key,
            "final HEAD during object proof",
            callback_timeout,
            deadline,
        )?;
        let after = Self::head_object_from_backend_blocking(backend, key, after_timeout)
            .map_err(|error| Self::proof_round_trip_error(key, "final HEAD", error, deadline))?;

        if before != after {
            return Err(crate::common::MidgeError::Internal(format!(
                "object '{key}' identity changed during read: before {before:?}, after {after:?}"
            )));
        }
        if after.size != bytes.len() as u64 {
            return Err(crate::common::MidgeError::Internal(format!(
                "object '{key}' size changed during read: bytes={}, metadata={} ",
                bytes.len(),
                after.size
            )));
        }
        Ok(RemoteObjectProof {
            key: key.to_string(),
            bytes,
            metadata: after,
        })
    }

    /// Read one cloud object together with a stable provider identity. The
    /// runtime may validate the bytes as WAL, SST, or metadata without giving
    /// those formats to the storage layer.
    #[cfg(test)]
    pub(crate) fn remote_object_proof(&self, key: &str) -> Result<RemoteObjectProof, String> {
        Self::stable_object_proof_from_backend(
            self.cloud_backend_for_key(key),
            key,
            self.callback_timeout,
        )
    }

    /// Read a stable object proof, charging each round trip against a shared
    /// budget.
    ///
    /// One proof is three sequential cloud calls (HEAD, GET, HEAD), and a single
    /// runtime request can chain several proofs. Clamping each call to what the
    /// deadline still allows keeps the whole sequence inside the caller's
    /// budget instead of letting every call restart a fresh `callback_timeout`.
    pub(crate) fn remote_object_proof_within(
        &self,
        key: &str,
        deadline: &OperationDeadline,
    ) -> crate::common::MidgeResult<RemoteObjectProof> {
        Self::stable_object_proof_from_backend_within(
            self.cloud_backend_for_key(key),
            key,
            self.callback_timeout,
            deadline,
        )
    }

    /// Refuse to start another cloud round trip once the shared budget is gone.
    ///
    /// Returning here rather than issuing a zero-timeout call keeps the failure
    /// legible: the caller learns which step ran out of budget instead of seeing
    /// a generic transport error.
    pub(super) fn deadline_timeout(
        key: &str,
        step: &str,
        per_operation_timeout: Duration,
        deadline: &OperationDeadline,
    ) -> crate::common::MidgeResult<Duration> {
        deadline
            .clamp_nonzero(per_operation_timeout)
            .ok_or_else(|| {
                crate::common::MidgeError::Timeout(format!(
                    "operation deadline exhausted before '{step}' for '{key}'"
                ))
            })
    }

    fn proof_round_trip_error(
        key: &str,
        step: &str,
        error: String,
        deadline: &OperationDeadline,
    ) -> crate::common::MidgeError {
        if deadline.is_expired() || Self::storage_error_indicates_timeout(&error) {
            crate::common::MidgeError::Timeout(format!(
                "{step} timed out while reading object proof for '{key}': {error}"
            ))
        } else {
            crate::common::MidgeError::Internal(error)
        }
    }

    /// Return a stable proof when the remote key exists, or `None` for a
    /// provider-confirmed missing key.
    pub(crate) fn remote_object_proof_optional(
        &self,
        key: &str,
    ) -> Result<Option<RemoteObjectProof>, String> {
        self.remote_object_proof_optional_within(key, &OperationDeadline::unbounded())
            .map_err(|error| error.to_string())
    }

    pub(crate) fn remote_object_proof_optional_within(
        &self,
        key: &str,
        deadline: &OperationDeadline,
    ) -> crate::common::MidgeResult<Option<RemoteObjectProof>> {
        let timeout =
            Self::deadline_timeout(key, "optional object HEAD", self.callback_timeout, deadline)?;
        let (tx, rx) = std::sync::mpsc::channel();
        self.cloud_backend_for_key(key)
            .submit_head_with_timeout(key, timeout, tx);
        match rx.recv_timeout(timeout) {
            Ok(StorageEvent::HeadComplete {
                result: StorageOutcome::Ok(_),
                ..
            }) => self.remote_object_proof_within(key, deadline).map(Some),
            Ok(StorageEvent::HeadComplete {
                result: StorageOutcome::Err(error),
                ..
            }) if Self::storage_error_indicates_missing(&error) => Ok(None),
            Ok(StorageEvent::HeadComplete {
                result: StorageOutcome::Err(error),
                ..
            }) => Err(Self::proof_round_trip_error(
                key,
                "optional object HEAD",
                error,
                deadline,
            )),
            Ok(other) => Err(crate::common::MidgeError::Internal(format!(
                "unexpected remote object HEAD response for '{key}': {other:?}"
            ))),
            Err(mpsc::RecvTimeoutError::Timeout) => Err(crate::common::MidgeError::Timeout(
                format!("optional object HEAD timed out for '{key}'"),
            )),
            Err(mpsc::RecvTimeoutError::Disconnected) => Err(crate::common::MidgeError::Internal(
                format!("remote object HEAD callback closed for '{key}'"),
            )),
        }
    }

    /// Conditionally replace or create a remote object and return a stable
    /// proof of the exact bytes that won the provider CAS.
    pub(crate) fn compare_exchange_remote_object(
        &self,
        key: &str,
        expected: Option<&StorageObjectMetadata>,
        data: Vec<u8>,
    ) -> crate::common::MidgeResult<RemoteObjectProof> {
        self.compare_exchange_remote_object_within(
            key,
            expected,
            data,
            &OperationDeadline::unbounded(),
        )
    }

    /// Conditionally replace or create a remote object within a shared budget.
    ///
    /// The CAS itself is one round trip, but the readback proof that follows is
    /// three more, so both participate in the deadline.
    pub(crate) fn compare_exchange_remote_object_within(
        &self,
        key: &str,
        expected: Option<&StorageObjectMetadata>,
        data: Vec<u8>,
        deadline: &OperationDeadline,
    ) -> crate::common::MidgeResult<RemoteObjectProof> {
        Self::deadline_timeout(key, "remote CAS", self.callback_timeout, deadline)?;
        let headers = if let Some(expected) = expected {
            crate::storage::cloud::object_match_precondition_headers(
                &expected.etag,
                expected.generation.as_deref(),
            )
            .ok_or_else(|| {
                crate::common::MidgeError::InvalidArgument(format!(
                    "remote CAS for '{key}' requires a non-empty identity token"
                ))
            })?
        } else {
            vec![("If-None-Match".to_string(), "*".to_string())]
        };
        let expected_bytes = data.clone();
        let (tx, rx) = std::sync::mpsc::channel();
        let timeout = Self::deadline_timeout(key, "remote CAS", self.callback_timeout, deadline)?;
        self.cloud_backend_for_key(key)
            .submit_write_with_headers_and_timeout(key, data, headers, timeout, tx);
        match rx.recv_timeout(timeout) {
            Ok(StorageEvent::WriteComplete {
                result: StorageOutcome::Ok(()),
                ..
            }) => {}
            Ok(StorageEvent::WriteComplete {
                result: StorageOutcome::Err(error),
                ..
            }) => {
                if Self::storage_error_indicates_timeout(&error) {
                    return Err(crate::common::MidgeError::Timeout(format!(
                        "remote CAS timed out for '{key}': {error}"
                    )));
                }
                if Self::storage_error_indicates_precondition_failure(&error) {
                    return Err(crate::common::MidgeError::Busy(format!(
                        "remote CAS conflict for '{key}': {error}"
                    )));
                }
                return Err(crate::common::MidgeError::Internal(format!(
                    "remote CAS failed for '{key}': {error}"
                )));
            }
            Ok(other) => {
                return Err(crate::common::MidgeError::Internal(format!(
                    "unexpected remote CAS response for '{key}': {other:?}"
                )));
            }
            Err(mpsc::RecvTimeoutError::Timeout) => {
                return Err(crate::common::MidgeError::Timeout(format!(
                    "remote CAS timed out for '{key}'"
                )));
            }
            Err(mpsc::RecvTimeoutError::Disconnected) => {
                return Err(crate::common::MidgeError::Internal(format!(
                    "remote CAS callback closed for '{key}'"
                )));
            }
        }

        let proof = self.remote_object_proof_within(key, deadline)?;
        if proof.bytes != expected_bytes {
            return Err(crate::common::MidgeError::Corruption(format!(
                "remote CAS for '{key}' read back different bytes"
            )));
        }
        Ok(proof)
    }

    /// Convert a validated cloud read into a metadata-only dependency for a
    /// guarded delete. The delete worker rechecks this identity immediately
    /// before issuing the conditional delete.
    pub(crate) fn remote_identity_guard(&self, proof: &RemoteObjectProof) -> GuardedObjectProof {
        GuardedObjectProof::metadata_only(
            Arc::clone(self.cloud_backend_for_key(&proof.key)),
            proof.key.clone(),
            proof.metadata.clone(),
        )
    }

    fn verify_guarded_object_proof(
        proof: &GuardedObjectProof,
        callback_timeout: Duration,
    ) -> Result<(), String> {
        Self::verify_guarded_object_proof_within(
            proof,
            callback_timeout,
            &OperationDeadline::unbounded(),
        )
        .map_err(|error| error.to_string())
    }

    fn verify_guarded_object_proof_within(
        proof: &GuardedObjectProof,
        callback_timeout: Duration,
        deadline: &OperationDeadline,
    ) -> crate::common::MidgeResult<()> {
        if let Some(expected_bytes) = proof.expected_bytes.as_ref() {
            let actual = Self::stable_object_proof_from_backend_within(
                &proof.backend,
                &proof.key,
                callback_timeout,
                deadline,
            )?;
            if actual.bytes != *expected_bytes {
                return Err(crate::common::MidgeError::Internal(format!(
                    "guarded object '{}' changed before conditional delete",
                    proof.key
                )));
            }
            if actual.metadata != proof.metadata {
                return Err(crate::common::MidgeError::Internal(format!(
                    "guarded object '{}' identity changed before conditional delete: expected {:?}, actual {:?}",
                    proof.key, proof.metadata, actual.metadata
                )));
            }
            return Ok(());
        }

        let timeout = Self::deadline_timeout(
            &proof.key,
            "guarded object HEAD",
            callback_timeout,
            deadline,
        )?;
        let actual = Self::head_object_from_backend_blocking(&proof.backend, &proof.key, timeout)
            .map_err(|error| {
            Self::proof_round_trip_error(&proof.key, "guarded object HEAD", error, deadline)
        })?;
        if actual == proof.metadata {
            return Ok(());
        }
        Err(crate::common::MidgeError::Internal(format!(
            "guarded object '{}' identity changed before conditional delete: expected {:?}, actual {actual:?}",
            proof.key, proof.metadata
        )))
    }

    /// Revalidate a pending delete target and every semantic dependency before
    /// the runtime retires an authority reference to that target.
    pub(crate) fn verify_remote_delete_guards_within(
        &self,
        target: &RemoteObjectProof,
        dependencies: &[GuardedObjectProof],
        deadline: &OperationDeadline,
    ) -> crate::common::MidgeResult<()> {
        let target_guard = self.remote_identity_guard(target);
        Self::verify_guarded_object_proof_within(&target_guard, self.callback_timeout, deadline)?;
        for dependency in dependencies {
            Self::verify_guarded_object_proof_within(dependency, self.callback_timeout, deadline)?;
        }
        Ok(())
    }

    /// Conditionally delete a remote object after revalidating format-neutral
    /// dependency identities. The runtime must first establish all semantic
    /// coverage relationships and supply the resulting proofs.
    pub(crate) fn delete_remote_object_guarded(
        &self,
        request_id: u64,
        target: RemoteObjectProof,
        dependencies: Vec<GuardedObjectProof>,
    ) -> Result<(), String> {
        self.reap_finished_prune_workers();

        let delete_headers = crate::storage::cloud::object_match_precondition_headers(
            &target.metadata.etag,
            target.metadata.generation.as_deref(),
        )
        .ok_or_else(|| {
            format!(
                "cannot conditionally delete remote object '{}' without an identity token",
                target.key
            )
        })?;

        let cloud = Arc::clone(self.cloud_backend_for_key(&target.key));
        let target_guard = self.remote_identity_guard(&target);
        let target_key = target.key;
        let event_queue = Arc::clone(&self.event_queue);
        let external_event_tx = self.external_event_tx.clone();
        let callback_timeout = self.callback_timeout;

        let mut workers = self.prune_workers.lock();
        if workers.shutting_down {
            return Err("hybrid storage is shutting down; guarded delete rejected".to_string());
        }
        let pending_completions = self.event_queue.lock().pending_prune_completions();
        if workers.handles.len() >= workers.max_workers {
            return Err(format!(
                "guarded delete workers at capacity: running={}/{}",
                workers.handles.len(),
                workers.max_workers
            ));
        }
        if workers.handles.len().saturating_add(pending_completions) >= workers.max_requests {
            return Err(format!(
                "guarded delete completion queue at capacity: outstanding={}/{}",
                workers.handles.len().saturating_add(pending_completions),
                workers.max_requests
            ));
        }

        let worker = thread::Builder::new()
            .name(format!("midge-object-pruner-{request_id}"))
            .spawn(move || {
                let result = (|| {
                    Self::verify_guarded_object_proof(&target_guard, callback_timeout)?;
                    for dependency in &dependencies {
                        Self::verify_guarded_object_proof(dependency, callback_timeout)?;
                    }

                    let (tx, rx) = std::sync::mpsc::channel();
                    cloud.submit_delete_with_headers(&target_key, delete_headers, tx);
                    match rx.recv_timeout(callback_timeout) {
                        Ok(StorageEvent::DeleteComplete { result, .. }) => match result {
                            StorageOutcome::Ok(()) => Ok(()),
                            StorageOutcome::Err(error) => Err(error),
                        },
                        Ok(other) => Err(format!(
                            "unexpected guarded delete response for '{target_key}': {other:?}"
                        )),
                        Err(error) => Err(format!(
                            "guarded delete timed out for '{target_key}': {error}"
                        )),
                    }
                })();

                let result = match result {
                    Ok(()) => StorageOutcome::Ok(()),
                    Err(error) => StorageOutcome::Err(error),
                };
                let event = StorageEvent::CloudWalPruneComplete {
                    segment_id: request_id,
                    result,
                };
                Self::queue_storage_event(&event_queue, external_event_tx.as_ref(), event);
            })
            .map_err(|error| format!("failed to spawn guarded delete worker: {error}"))?;
        workers.handles.push(worker);
        Ok(())
    }

    /// Conditionally delete exactly the remote object represented by a stable
    /// proof. Startup recovery uses this blocking form before the runtime
    /// event loop exists; a provider precondition failure is returned so the
    /// caller can retain the replacement object and fail closed.
    pub(crate) fn delete_remote_object_guarded_blocking(
        &self,
        target: RemoteObjectProof,
    ) -> Result<(), String> {
        let delete_headers = crate::storage::cloud::object_match_precondition_headers(
            &target.metadata.etag,
            target.metadata.generation.as_deref(),
        )
        .ok_or_else(|| {
            format!(
                "cannot conditionally delete remote object '{}' without an identity token",
                target.key
            )
        })?;
        let cloud = Arc::clone(self.cloud_backend_for_key(&target.key));
        let target_key = target.key;

        // This deterministic boundary proves the provider condition, rather
        // than a preceding HEAD, closes the proof/delete race.
        crate::failpoints::fail_point!("midge::cloud::before_compaction_orphan_delete");

        let (tx, rx) = std::sync::mpsc::channel();
        cloud.submit_delete_with_headers(&target_key, delete_headers, tx);
        match rx.recv_timeout(self.callback_timeout) {
            Ok(StorageEvent::DeleteComplete { result, .. }) => match result {
                StorageOutcome::Ok(()) => Ok(()),
                StorageOutcome::Err(error) => Err(error),
            },
            Ok(other) => Err(format!(
                "unexpected guarded delete response for '{target_key}': {other:?}"
            )),
            Err(error) => Err(format!(
                "guarded delete timed out for '{target_key}': {error}"
            )),
        }
    }

    /// Stop admitting cloud prune work and join every outstanding worker.
    ///
    /// Each worker may issue a conditional remote delete, so shutdown must
    /// wait for it while the current lease/fencing epoch remains valid.
    pub(crate) fn shutdown_background_workers(&self) {
        let handles = {
            let mut workers = self.prune_workers.lock();
            workers.shutting_down = true;
            std::mem::take(&mut workers.handles)
        };

        for worker in handles {
            if let Ok(()) = worker.join() {
                tracing::debug!("cloud WAL prune worker joined");
            } else {
                tracing::warn!("cloud WAL prune worker panicked during join");
            }
        }
    }

    fn reap_finished_prune_workers(&self) {
        let mut workers = self.prune_workers.lock();
        let mut still_running = Vec::new();
        for worker in std::mem::take(&mut workers.handles) {
            if worker.is_finished() {
                if let Ok(()) = worker.join() {
                    tracing::debug!("cloud WAL prune worker completed");
                } else {
                    tracing::warn!("cloud WAL prune worker panicked");
                }
            } else {
                still_running.push(worker);
            }
        }
        workers.handles = still_running;
    }
}
