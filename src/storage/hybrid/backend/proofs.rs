//! Remote object identity proofs and guarded deletion.

use super::RemoteCasFailure;
use super::{
    mpsc, Arc, Duration, HybridStorage, JoinHandle, StorageBackend, StorageEvent, StorageOutcome,
};
use super::{thread, Instant, VecDeque};
use crate::common::OperationDeadline;
use crate::storage::StorageObjectMetadata;

/// A stable read plus identity observation for one remote object.
///
/// Format-aware validation belongs to the runtime. Storage only establishes
/// that the bytes it returned still match the provider identity observed by
/// the metadata-bearing GET. Subsequent HEAD checks reject observed changes.
#[derive(Clone, Debug)]
pub(crate) struct RemoteObjectProof {
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
    metadata: StorageObjectMetadata,
}

impl GuardedObjectProof {
    pub(crate) fn range_identity(
        backend: Arc<dyn StorageBackend>,
        key: String,
        metadata: StorageObjectMetadata,
    ) -> Self {
        Self {
            backend,
            key,
            metadata,
        }
    }
}

pub(super) struct PruneWorkerRegistry {
    shutting_down: bool,
    handles: Vec<PruneWorker>,
    max_workers: usize,
    max_requests: usize,
    reserved_requests: usize,
    pending: VecDeque<PendingDelete>,
}

struct PruneWorker {
    handle: JoinHandle<()>,
    requests: usize,
}

#[derive(Clone)]
struct PreparedConditionalDelete {
    request_id: u64,
    cloud: Arc<dyn StorageBackend>,
    target_key: String,
    delete_identity: StorageObjectMetadata,
}

struct PendingDelete {
    target: PreparedConditionalDelete,
    retry_at: Instant,
}

/// Bounded cleanup ownership is reserved before the caller retires authority.
/// Dropping an uncommitted admission releases its capacity without deleting.
pub(crate) struct AdmittedObjectDeletes<'a> {
    storage: &'a HybridStorage,
    targets: Option<Vec<PreparedConditionalDelete>>,
}

impl AdmittedObjectDeletes<'_> {
    pub(crate) fn schedule(mut self) {
        let targets = self.targets.take().expect("admission is consumed once");
        {
            let mut workers = self.storage.prune_workers.lock();
            workers.reserved_requests -= targets.len();
            workers
                .pending
                .extend(targets.into_iter().map(|target| PendingDelete {
                    target,
                    retry_at: Instant::now(),
                }));
        }
        self.storage.process_pending_remote_deletes();
    }
}

impl Drop for AdmittedObjectDeletes<'_> {
    fn drop(&mut self) {
        if let Some(targets) = self.targets.as_ref() {
            self.storage.prune_workers.lock().reserved_requests -= targets.len();
        }
    }
}

impl PruneWorkerRegistry {
    pub(super) fn new(max_workers: usize, max_requests: usize) -> Self {
        Self {
            shutting_down: false,
            handles: Vec::new(),
            max_workers: max_workers.max(1),
            max_requests: max_requests.max(1),
            reserved_requests: 0,
            pending: VecDeque::new(),
        }
    }
}

impl HybridStorage {
    pub(crate) fn remote_sst_backend(&self) -> Arc<dyn StorageBackend> {
        Arc::clone(&self.stores.sst)
    }

    pub(crate) fn storage_io_timeout(&self) -> Duration {
        self.callback_timeout
    }

    pub(crate) fn ephemeral_sst_cache_enabled(&self) -> bool {
        self.ephemeral_sst_cache
            .load(std::sync::atomic::Ordering::Acquire)
    }

    pub(crate) fn remote_range_metadata_optional_within(
        &self,
        key: &str,
        deadline: &OperationDeadline,
    ) -> crate::common::MidgeResult<Option<StorageObjectMetadata>> {
        let timeout =
            Self::deadline_timeout(key, "range object HEAD", self.callback_timeout, deadline)?;
        match Self::head_range_object_from_backend(self.cloud_backend_for_key(key), key, timeout) {
            Ok(metadata) => Ok(Some(metadata)),
            Err(error) if Self::storage_error_indicates_missing(&error) => Ok(None),
            Err(error) => Err(Self::proof_round_trip_error(
                key,
                "range object HEAD",
                &error,
                deadline,
            )),
        }
    }

    pub(crate) fn verify_remote_object_guards_within(
        &self,
        guards: &[GuardedObjectProof],
        deadline: &OperationDeadline,
    ) -> crate::common::MidgeResult<()> {
        for guard in guards {
            Self::verify_guarded_object_proof_within(guard, self.callback_timeout, deadline)?;
        }
        Ok(())
    }

    fn head_range_object_from_backend(
        backend: &Arc<dyn StorageBackend>,
        key: &str,
        timeout: Duration,
    ) -> Result<StorageObjectMetadata, crate::storage::StorageError> {
        let (tx, rx) = mpsc::channel();
        backend.submit_range_head_request(
            crate::storage::StorageRequest::new(
                key,
                OperationDeadline::from_budget(timeout),
                timeout,
            ),
            tx,
        );
        match rx.recv_timeout(timeout) {
            Ok(StorageEvent::HeadComplete {
                result: StorageOutcome::Ok(metadata),
                ..
            }) => Ok(metadata),
            Ok(StorageEvent::HeadComplete {
                result: StorageOutcome::Err(error),
                ..
            }) => Err(error),
            Ok(other) => Err(crate::storage::StorageError::protocol(format!(
                "unexpected range HEAD response: {other:?}"
            ))),
            Err(mpsc::RecvTimeoutError::Timeout) => Err(crate::storage::storage_timeout_error(
                "range HEAD timed out",
            )),
            Err(mpsc::RecvTimeoutError::Disconnected) => Err("range HEAD callback closed".into()),
        }
    }

    pub(super) fn stable_object_proof_from_backend(
        backend: &Arc<dyn StorageBackend>,
        key: &str,
        callback_timeout: Duration,
    ) -> crate::common::MidgeResult<RemoteObjectProof> {
        Self::stable_object_proof_from_backend_within(
            backend,
            key,
            callback_timeout,
            &OperationDeadline::unbounded(),
        )
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
            .map_err(|error| Self::proof_round_trip_error(key, "initial HEAD", &error, deadline))?;

        let read_timeout =
            Self::deadline_timeout(key, "GET during object proof", callback_timeout, deadline)?;
        let (tx, rx) = mpsc::channel();
        backend.submit_metadata_read_request(
            crate::storage::StorageRequest::new(key, *deadline, read_timeout),
            tx,
        );
        let (bytes, metadata) = rx
            .recv_timeout(read_timeout)
            .map_err(|error| match error {
                mpsc::RecvTimeoutError::Timeout => crate::common::MidgeError::Timeout(format!(
                    "metadata-bearing GET timed out for '{key}'"
                )),
                mpsc::RecvTimeoutError::Disconnected => crate::common::MidgeError::Internal(
                    format!("metadata-bearing GET callback closed for '{key}'"),
                ),
            })?
            .map_err(|error| Self::proof_round_trip_error(key, "GET", &error, deadline))?;
        crate::storage::cloud::validate_object_proof(key, &bytes, &metadata)?;

        let after_timeout = Self::deadline_timeout(
            key,
            "final HEAD during object proof",
            callback_timeout,
            deadline,
        )?;
        let after = Self::head_object_from_backend_blocking(backend, key, after_timeout)
            .map_err(|error| Self::proof_round_trip_error(key, "final HEAD", &error, deadline))?;

        if !before.same_version(&metadata) || !metadata.same_version(&after) {
            return Err(crate::common::MidgeError::Internal(format!(
                "object '{key}' identity changed during read: before {before:?}, GET {metadata:?}, after {after:?}"
            )));
        }
        Ok(RemoteObjectProof { bytes, metadata })
    }

    /// Read one cloud object together with a stable provider identity. The
    /// runtime may validate the bytes as WAL, SST, or metadata without giving
    /// those formats to the storage layer.
    #[cfg(test)]
    pub(crate) fn remote_object_proof(
        &self,
        key: &str,
    ) -> crate::common::MidgeResult<RemoteObjectProof> {
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
        error: &crate::storage::StorageError,
        deadline: &OperationDeadline,
    ) -> crate::common::MidgeError {
        if deadline.is_expired() || error.is_timeout() {
            crate::common::MidgeError::Timeout(format!(
                "{step} timed out while reading object proof for '{key}': {error}"
            ))
        } else {
            crate::common::MidgeError::Internal(error.to_string())
        }
    }

    /// Return a stable proof when the remote key exists, or `None` for a
    /// provider-confirmed missing key.
    #[cfg(test)]
    pub(crate) fn remote_object_proof_optional(
        &self,
        key: &str,
    ) -> crate::common::MidgeResult<Option<RemoteObjectProof>> {
        self.remote_object_proof_optional_within(key, &OperationDeadline::unbounded())
    }

    pub(crate) fn remote_object_proof_optional_within(
        &self,
        key: &str,
        deadline: &OperationDeadline,
    ) -> crate::common::MidgeResult<Option<RemoteObjectProof>> {
        let timeout =
            Self::deadline_timeout(key, "optional object HEAD", self.callback_timeout, deadline)?;
        let (tx, rx) = std::sync::mpsc::channel();
        self.cloud_backend_for_key(key).submit_head_request(
            crate::storage::StorageRequest::new(key, *deadline, timeout),
            tx,
        );
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
                &error,
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
    /// Conditional-write failure paired with whether the provider may still
    /// apply the mutation. Callers recover from a definite non-commit; an
    /// ambiguous one must be resolved against durable state instead.
    #[cfg(test)]
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
    #[cfg(test)]
    pub(crate) fn compare_exchange_remote_object_within(
        &self,
        key: &str,
        expected: Option<&StorageObjectMetadata>,
        data: Vec<u8>,
        deadline: &OperationDeadline,
    ) -> crate::common::MidgeResult<RemoteObjectProof> {
        self.compare_exchange_remote_object_phased(key, expected, data, deadline)
            .map_err(|failure| failure.error)
    }

    /// Conditional remote write that reports whether a failure may still
    /// commit at the provider. Recovery paths need that distinction, and
    /// deriving it from message text is silently fragile.
    pub(crate) fn compare_exchange_remote_object_phased(
        &self,
        key: &str,
        expected: Option<&StorageObjectMetadata>,
        data: Vec<u8>,
        deadline: &OperationDeadline,
    ) -> Result<RemoteObjectProof, RemoteCasFailure> {
        // Nothing has been submitted yet, so every failure here is definite.
        Self::deadline_timeout(key, "remote CAS", self.callback_timeout, deadline)
            .map_err(RemoteCasFailure::not_committed)?;
        let precondition = if let Some(expected) = expected {
            crate::storage::conditional_object_identity(
                &expected.etag,
                expected.generation.as_deref(),
            )
            .ok_or_else(|| {
                RemoteCasFailure::not_committed(crate::common::MidgeError::InvalidArgument(
                    format!("remote CAS for '{key}' requires a non-empty identity token"),
                ))
            })?;
            crate::storage::StoragePrecondition::IfMatch(expected.clone())
        } else {
            crate::storage::StoragePrecondition::IfAbsent
        };
        let expected_bytes = data.clone();
        let (tx, rx) = std::sync::mpsc::channel();
        let timeout = Self::deadline_timeout(key, "remote CAS", self.callback_timeout, deadline)
            .map_err(RemoteCasFailure::not_committed)?;
        let request = crate::storage::StorageRequest::new(key, *deadline, self.callback_timeout)
            .with_precondition(precondition);
        let backend = self.cloud_backend_for_key(key);
        self.check_write_authority()
            .map_err(RemoteCasFailure::not_committed)?;
        backend.submit_write_request(request, data, tx);
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
                    // The provider may still apply a submitted write.
                    return Err(RemoteCasFailure::may_have_committed(
                        crate::common::MidgeError::Timeout(format!(
                            "remote CAS timed out for '{key}': {error}"
                        )),
                    ));
                }
                if Self::storage_error_indicates_precondition_failure(&error) {
                    // The provider rejected the write outright.
                    return Err(RemoteCasFailure::not_committed(
                        crate::common::MidgeError::Busy(format!(
                            "remote CAS conflict for '{key}': {error}"
                        )),
                    ));
                }
                return Err(RemoteCasFailure::may_have_committed(
                    crate::common::MidgeError::Internal(format!(
                        "remote CAS failed for '{key}': {error}"
                    )),
                ));
            }
            Ok(other) => {
                return Err(RemoteCasFailure::may_have_committed(
                    crate::common::MidgeError::Internal(format!(
                        "unexpected remote CAS response for '{key}': {other:?}"
                    )),
                ));
            }
            Err(mpsc::RecvTimeoutError::Timeout) => {
                return Err(RemoteCasFailure::may_have_committed(
                    crate::common::MidgeError::Timeout(format!("remote CAS timed out for '{key}'")),
                ));
            }
            Err(mpsc::RecvTimeoutError::Disconnected) => {
                return Err(RemoteCasFailure::may_have_committed(
                    crate::common::MidgeError::Internal(format!(
                        "remote CAS callback closed for '{key}'"
                    )),
                ));
            }
        }

        // The write was accepted, so a failed readback leaves it committed.
        let proof = self
            .remote_object_proof_within(key, deadline)
            .map_err(RemoteCasFailure::may_have_committed)?;
        if proof.bytes != expected_bytes {
            return Err(RemoteCasFailure::may_have_committed(
                crate::common::MidgeError::Corruption(format!(
                    "remote CAS for '{key}' read back different bytes"
                )),
            ));
        }
        Ok(proof)
    }

    /// Recheck the identity of an immutable publication before its authority commit.
    fn verify_guarded_object_proof_within(
        proof: &GuardedObjectProof,
        callback_timeout: Duration,
        deadline: &OperationDeadline,
    ) -> crate::common::MidgeResult<()> {
        let timeout = Self::deadline_timeout(
            &proof.key,
            "guarded object HEAD",
            callback_timeout,
            deadline,
        )?;
        let actual = Self::head_range_object_from_backend(&proof.backend, &proof.key, timeout)
            .map_err(|error| {
                Self::proof_round_trip_error(&proof.key, "guarded object HEAD", &error, deadline)
            })?;
        if actual.same_version(&proof.metadata) {
            return Ok(());
        }
        Err(crate::common::MidgeError::Internal(format!(
            "guarded object '{}' identity changed before conditional delete: expected {:?}, actual {actual:?}",
            proof.key, proof.metadata
        )))
    }

    fn ensure_conditional_delete_capacity(
        &self,
        workers: &PruneWorkerRegistry,
        request_count: usize,
    ) -> Result<(), crate::storage::StorageError> {
        if workers.shutting_down {
            return Err(crate::storage::StorageError::io(
                "hybrid storage is shutting down; guarded delete rejected",
            ));
        }
        if workers.handles.len() >= workers.max_workers {
            return Err(crate::storage::StorageError::io(format!(
                "guarded delete workers at capacity: running={}/{}",
                workers.handles.len(),
                workers.max_workers
            )));
        }

        let pending_completions = self.event_queue.lock().pending_prune_completions();
        let active_requests = workers
            .handles
            .iter()
            .map(|worker| worker.requests)
            .sum::<usize>();
        if active_requests
            .saturating_add(pending_completions)
            .saturating_add(workers.pending.len())
            .saturating_add(workers.reserved_requests)
            .saturating_add(request_count)
            > workers.max_requests
        {
            return Err(crate::storage::StorageError::io(format!(
                "guarded delete completion queue at capacity: outstanding={}/{}",
                active_requests.saturating_add(pending_completions),
                workers.max_requests
            )));
        }
        Ok(())
    }

    /// Reserve bounded cleanup ownership before the caller retires authority.
    /// The original observed identity survives worker admission and transport
    /// failures; each later attempt rechecks it before conditional deletion.
    pub(crate) fn admit_remote_deletes_by_identity(
        &self,
        targets: Vec<(u64, String, StorageObjectMetadata)>,
    ) -> Result<AdmittedObjectDeletes<'_>, crate::storage::StorageError> {
        let prepared = targets
            .into_iter()
            .map(|(request_id, key, metadata)| {
                crate::storage::conditional_object_identity(
                    &metadata.etag,
                    metadata.generation.as_deref(),
                )
                .ok_or_else(|| {
                    crate::storage::StorageError::protocol(format!(
                    "cannot conditionally delete remote object '{key}' without an identity token"))
                })?;
                let cloud = Arc::clone(self.cloud_backend_for_key(&key));
                Ok(PreparedConditionalDelete {
                    request_id,
                    cloud,
                    target_key: key,
                    delete_identity: metadata,
                })
            })
            .collect::<Result<Vec<_>, crate::storage::StorageError>>()?;
        self.reap_finished_prune_workers();
        let mut workers = self.prune_workers.lock();
        if workers.shutting_down {
            return Err(crate::storage::StorageError::io("storage is shutting down"));
        }
        let outstanding = workers
            .handles
            .iter()
            .map(|worker| worker.requests)
            .sum::<usize>()
            .saturating_add(workers.pending.len())
            .saturating_add(workers.reserved_requests)
            .saturating_add(self.event_queue.lock().pending_prune_completions());
        if outstanding.saturating_add(prepared.len()) > workers.max_requests {
            return Err(crate::storage::StorageError::new(
                crate::storage::StorageErrorKind::ResourceLimit,
                "conditional delete target queue is full",
            ));
        }
        workers.reserved_requests += prepared.len();
        Ok(AdmittedObjectDeletes {
            storage: self,
            targets: Some(prepared),
        })
    }

    #[cfg(test)]
    pub(crate) fn delete_remote_objects_by_identity(
        &self,
        targets: Vec<(u64, String, StorageObjectMetadata)>,
    ) -> Result<(), crate::storage::StorageError> {
        self.admit_remote_deletes_by_identity(targets)?.schedule();
        Ok(())
    }

    pub(super) fn next_remote_delete_turn(&self, poll: Duration) -> Option<Duration> {
        let workers = self.prune_workers.lock();
        if workers.shutting_down {
            return None;
        }
        let retry = workers
            .pending
            .iter()
            .map(|pending| {
                pending
                    .retry_at
                    .saturating_duration_since(Instant::now())
                    .max(poll)
            })
            .min();
        if workers.handles.is_empty() {
            retry
        } else {
            Some(retry.map_or(poll, |retry| retry.min(poll)))
        }
    }

    /// The normal storage tick retries retained identities without a caller or
    /// another metadata/catalog publication. Shutdown admits no fresh cleanup.
    pub(super) fn process_pending_remote_deletes(&self) {
        self.reap_finished_prune_workers();
        let prepared = {
            let mut workers = self.prune_workers.lock();
            if workers.shutting_down || workers.handles.len() >= workers.max_workers {
                return;
            }
            let now = Instant::now();
            let count = workers.pending.len();
            let mut due = Vec::new();
            for _ in 0..count {
                let pending = workers.pending.pop_front().expect("counted pending target");
                if pending.retry_at <= now && due.len() < 32 {
                    due.push(pending.target);
                } else {
                    workers.pending.push_back(pending);
                }
            }
            workers.reserved_requests += due.len();
            due
        };
        if prepared.is_empty() {
            return;
        }
        if let Err(error) = self.spawn_conditional_deletes(prepared.clone()) {
            tracing::debug!(%error, "retired object cleanup remains queued");
            let retry_at = Instant::now() + Duration::from_millis(200);
            let mut workers = self.prune_workers.lock();
            workers.reserved_requests -= prepared.len();
            if !workers.shutting_down {
                workers.pending.extend(
                    prepared
                        .into_iter()
                        .map(|target| PendingDelete { target, retry_at }),
                );
            }
        }
    }

    fn spawn_conditional_deletes(
        &self,
        prepared: Vec<PreparedConditionalDelete>,
    ) -> Result<(), crate::storage::StorageError> {
        if prepared.is_empty() {
            return Ok(());
        }
        self.reap_finished_prune_workers();
        let request_count = prepared.len();
        let first_request_id = prepared.first().map_or(0, |entry| entry.request_id);
        let last_request_id = prepared.last().map_or(0, |entry| entry.request_id);
        let event_queue = Arc::clone(&self.event_queue);
        let external_event_tx = self.external_event_tx.clone();
        let callback_timeout = self.callback_timeout;
        let write_authority = self.write_authority.get().cloned();
        let registry = Arc::clone(&self.prune_workers);

        let mut workers = self.prune_workers.lock();
        workers.reserved_requests -= request_count;
        if let Err(error) = self.ensure_conditional_delete_capacity(&workers, request_count) {
            workers.reserved_requests += request_count;
            return Err(error);
        }

        let worker = thread::Builder::new()
            .name(format!(
                "midge-object-pruner-{first_request_id}-{last_request_id}"
            ))
            .spawn(move || {
                // Catalog retirement already established and atomically
                // published every semantic coverage dependency. Re-reading
                // mutable manifest metadata here would race the next valid
                // publication without adding safety. Physical cleanup only
                // needs the retired target's conditional provider identity.
                let deadline = OperationDeadline::from_budget(callback_timeout);

                for target in prepared {
                    let request_id = target.request_id;
                    let target_key = &target.target_key;
                    let attempt = std::panic::catch_unwind(std::panic::AssertUnwindSafe(|| {
                        Self::delete_conditional_target(
                            &target,
                            callback_timeout,
                            &deadline,
                            write_authority.as_ref(),
                        )
                    }));
                    let outcome = match attempt {
                        Ok(result) => result,
                        Err(_) => Err(crate::storage::StorageError::io(format!(
                            "conditional delete worker panicked for '{target_key}'"
                        ))),
                    };
                    if outcome.as_ref().is_err_and(|error| {
                        !matches!(
                            error.kind(),
                            crate::storage::StorageErrorKind::PreconditionFailed
                                | crate::storage::StorageErrorKind::Unauthorized
                                | crate::storage::StorageErrorKind::Corruption
                        )
                    }) {
                        let mut workers = registry.lock();
                        if !workers.shutting_down {
                            workers.pending.push_back(PendingDelete {
                                target: target.clone(),
                                retry_at: Instant::now() + Duration::from_millis(200),
                            });
                        }
                    }
                    let result = match outcome {
                        Ok(()) => StorageOutcome::Ok(()),
                        Err(error) => StorageOutcome::Err(error),
                    };
                    let event = StorageEvent::CloudWalPruneComplete {
                        segment_id: request_id,
                        result,
                    };
                    Self::queue_storage_event(&event_queue, external_event_tx.as_ref(), event);
                }
            })
            .map_err(|error| {
                workers.reserved_requests += request_count;
                crate::storage::StorageError::io(format!(
                    "failed to spawn conditional delete worker: {error}"
                ))
            })?;
        workers.handles.push(PruneWorker {
            handle: worker,
            requests: request_count,
        });
        Ok(())
    }

    fn delete_conditional_target(
        target: &PreparedConditionalDelete,
        callback_timeout: Duration,
        deadline: &OperationDeadline,
        write_authority: Option<&super::StorageWriteAuthority>,
    ) -> Result<(), crate::storage::StorageError> {
        let timeout = Self::deadline_timeout(
            &target.target_key,
            "conditional target HEAD",
            callback_timeout,
            deadline,
        )
        .map_err(crate::storage::StorageError::timeout)?;
        let observed =
            Self::head_range_object_from_backend(&target.cloud, &target.target_key, timeout);
        match observed {
            Err(error) if error.is_not_found() => return Ok(()),
            Err(error) => return Err(error),
            Ok(metadata) if !metadata.same_version(&target.delete_identity) => {
                return Err(crate::storage::StorageError::precondition_failed(
                    "retired target identity changed",
                ));
            }
            Ok(_) => {}
        }
        if let Some(validate) = write_authority {
            validate().map_err(|error| {
                let kind = match &error {
                    crate::common::MidgeError::Fenced(_) => {
                        crate::storage::StorageErrorKind::Unauthorized
                    }
                    crate::common::MidgeError::Timeout(_) => {
                        crate::storage::StorageErrorKind::Timeout
                    }
                    _ => crate::storage::StorageErrorKind::Io,
                };
                crate::storage::StorageError::new(kind, error)
            })?;
        }
        let timeout = Self::deadline_timeout(
            &target.target_key,
            "conditional DELETE",
            callback_timeout,
            deadline,
        )
        .map_err(crate::storage::StorageError::timeout)?;
        let (tx, rx) = mpsc::channel();
        target.cloud.submit_delete_request(
            crate::storage::StorageRequest::new(&target.target_key, *deadline, timeout)
                .with_precondition(crate::storage::StoragePrecondition::IfMatch(
                    target.delete_identity.clone(),
                )),
            tx,
        );
        match rx.recv_timeout(timeout) {
            Ok(StorageEvent::DeleteComplete {
                result: StorageOutcome::Ok(()),
                ..
            }) => Ok(()),
            Ok(StorageEvent::DeleteComplete {
                result: StorageOutcome::Err(error),
                ..
            }) if error.is_not_found() => Ok(()),
            Ok(StorageEvent::DeleteComplete {
                result: StorageOutcome::Err(error),
                ..
            }) => Err(error),
            Ok(other) => Err(crate::storage::StorageError::protocol(format!(
                "unexpected conditional delete response: {other:?}"
            ))),
            Err(error) => Err(crate::storage::StorageError::timeout(error)),
        }
    }

    /// Delete only the observed object version. The provider precondition
    /// closes the identity/delete race after the caller retires authority.
    pub(crate) fn delete_remote_object_by_identity_blocking_within(
        &self,
        key: &str,
        metadata: &StorageObjectMetadata,
        deadline: &OperationDeadline,
    ) -> crate::common::MidgeResult<()> {
        crate::storage::conditional_object_identity(&metadata.etag, metadata.generation.as_deref())
            .ok_or_else(|| {
                crate::common::MidgeError::Internal(format!(
                    "cannot conditionally delete remote object '{key}' without an identity token"
                ))
            })?;
        let cloud = Arc::clone(self.cloud_backend_for_key(key));
        let target_key = key.to_string();
        let timeout = Self::deadline_timeout(
            &target_key,
            "conditional DELETE",
            self.callback_timeout,
            deadline,
        )?;

        // This deterministic boundary proves the provider condition, rather
        // than a preceding HEAD, closes the proof/delete race.
        crate::failpoints::fail_point!("midge::cloud::before_compaction_orphan_delete");

        let (tx, rx) = std::sync::mpsc::channel();
        let request = crate::storage::StorageRequest::new(&target_key, *deadline, timeout)
            .with_precondition(crate::storage::StoragePrecondition::IfMatch(
                metadata.clone(),
            ));
        self.check_write_authority()?;
        cloud.submit_delete_request(request, tx);
        match rx.recv_timeout(timeout) {
            Ok(StorageEvent::DeleteComplete { result, .. }) => match result {
                StorageOutcome::Ok(()) => Ok(()),
                StorageOutcome::Err(error) => Err(Self::proof_round_trip_error(
                    &target_key,
                    "conditional DELETE",
                    &error,
                    deadline,
                )),
            },
            Ok(other) => Err(crate::common::MidgeError::Internal(format!(
                "unexpected guarded delete response for '{target_key}': {other:?}"
            ))),
            Err(error) => Err(crate::common::MidgeError::Timeout(format!(
                "guarded delete timed out for '{target_key}': {error}"
            ))),
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
            if let Ok(()) = worker.handle.join() {
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
            if worker.handle.is_finished() {
                if let Ok(()) = worker.handle.join() {
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
