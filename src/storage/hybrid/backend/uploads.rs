//! WAL upload scheduling, worker execution, retries, and acknowledgements.

use super::{
    cb, mpsc, thread, Arc, BoundedEventQueue, Duration, HeldUploads, HybridStorage, Instant,
    JoinHandle, Mutex, StorageBackend, StorageEvent, StorageOutcome, UploadQueue, UploadState,
    UploadStatus, MAX_CLOUD_ERROR_BYTES,
};
use crate::storage::CloudUploadFailureKind;

impl HybridStorage {
    pub(crate) fn queue_cloud_wal_prune_complete(
        &self,
        segment_id: u64,
        result: StorageOutcome<()>,
    ) {
        Self::queue_storage_event(
            &self.event_queue,
            self.external_event_tx.as_ref(),
            StorageEvent::CloudWalPruneComplete { segment_id, result },
        );
    }

    pub(crate) fn queue_cloud_wal_prune_attempt_failed(&self, segment_id: u64, error: String) {
        Self::queue_storage_event(
            &self.event_queue,
            self.external_event_tx.as_ref(),
            StorageEvent::CloudWalPruneAttemptFailed { segment_id, error },
        );
    }

    pub fn ensure_wal_upload_capacity(
        &self,
        additional_bytes: u64,
    ) -> crate::common::MidgeResult<()> {
        let mut queue = self.upload_queue.lock();
        if let Err(error) = queue.ensure_capacity(additional_bytes) {
            self.counters.record(|m| {
                m.record_write_stall_cloud();
            });
            return Err(error);
        }
        Ok(())
    }

    pub(crate) fn ensure_wal_write_admission(
        &self,
        held: HeldUploads,
    ) -> crate::common::MidgeResult<()> {
        let queue = self.upload_queue.lock();
        if !queue.is_stalled(held) {
            return Ok(());
        }
        Err(crate::common::MidgeError::WriteStall(format!(
            "CloudAsync WAL upload queue remains at capacity: entries={}+{} held/{}, bytes={}+{} held/{}",
            queue.entries.len(),
            held.entries,
            queue.max_entries,
            queue.pending_bytes,
            held.bytes,
            queue.max_bytes
        )))
    }

    pub(crate) fn is_wal_upload_stalled(&self, held: HeldUploads) -> bool {
        self.upload_queue.lock().is_stalled(held)
    }

    /// How long until the upload queue next needs
    /// [`Self::process_uploads`]: `poll` while an upload waits to start or
    /// to finish, else until its earliest retry is due, so a segment that
    /// keeps failing does not keep its caller polling. `None` when nothing
    /// is queued.
    pub(crate) fn next_upload_turn(&self, poll: Duration) -> Option<Duration> {
        let upload = self.upload_queue.lock().next_turn(Instant::now(), poll);
        let cleanup = self.next_remote_delete_turn(poll);
        upload.into_iter().chain(cleanup).min()
    }

    /// Queue a local file for bounded remote publication under an explicit
    /// object key. The runtime owns the meaning of the request id and frontier.
    pub(crate) fn enqueue_object_upload(
        &self,
        request_id: u64,
        object_key: String,
        source: crate::io::FsLocation,
        frontier: u64,
    ) -> crate::common::MidgeResult<()> {
        let size_bytes = source
            .len()
            .map_err(|error| crate::common::MidgeError::Io(error.into_io()))?;
        let upload_state = UploadState {
            segment_id: request_id,
            object_key,
            source: Arc::new(source),
            status: UploadStatus::Pending,
            max_sequence: frontier,
            retries: 0,
            size_bytes,
        };

        let mut queue = self.upload_queue.lock();
        if let Err(error) = queue.try_push(upload_state) {
            self.counters.record(|m| {
                m.record_write_stall_cloud();
            });
            return Err(error);
        }

        if queue.entries.len() >= queue.max_entries / 2
            || queue.pending_bytes >= queue.max_bytes / 2
        {
            tracing::warn!(
                queue_size = queue.entries.len(),
                queue_bytes = queue.pending_bytes,
                request_id,
                "CloudAsync WAL upload queue growing; cloud uploads may be slow"
            );
        }

        tracing::debug!(
            request_id,
            frontier,
            queue_size = queue.entries.len(),
            queue_bytes = queue.pending_bytes,
            "object enqueued for cloud upload"
        );
        Ok(())
    }

    /// Process pending uploads (should be called periodically by runtime)
    ///
    /// **WAL DURABILITY PIPELINE**
    ///
    /// Initiates cloud uploads for pending WAL segments.
    /// This is the ONLY place where WAL segments are uploaded to cloud.
    /// - Reads pending uploads from `upload_queue`
    /// - Initiates cloud upload via cloud backend (not `submit_write`)
    /// - Retries a failed upload with backoff until it succeeds; only a
    ///   corrupt remote object or a fenced writer ends it
    /// - Emits CloudAck/CloudFail events to `event_queue`
    ///
    /// Non-blocking - actual uploads happen asynchronously in the dedicated worker thread.
    ///
    /// Returns any drained CloudAck/CloudFail events for the runtime to consume.
    pub fn process_uploads(&self) -> Vec<StorageEvent> {
        // 1) Drain worker completion events first.
        let drained_events = {
            let mut events = self.event_queue.lock();
            events.drain()
        };

        self.process_pending_remote_deletes();

        let mut queue = self.upload_queue.lock();
        let now = Instant::now();

        // 2) Apply drained events to the upload state machine.
        for queued in &drained_events {
            match &queued.event {
                StorageEvent::CloudAck {
                    segment_id,
                    max_sequence: _,
                } => {
                    if let Some(item) = queue
                        .entries
                        .iter_mut()
                        .find(|u| &u.segment_id == segment_id)
                    {
                        item.status = UploadStatus::Completed;
                    }
                }
                StorageEvent::CloudFail {
                    segment_id,
                    error,
                    failure_kind,
                    ..
                } => {
                    if failure_kind.ends_upload() {
                        queue.abandon(*segment_id);
                    } else {
                        let retry = queue.retry;
                        if let Some(item) = queue
                            .entries
                            .iter_mut()
                            .find(|u| &u.segment_id == segment_id)
                        {
                            item.record_failure(error.clone(), now, retry);
                        }
                    }
                }
                _ => {}
            }
        }

        // 3) Schedule any eligible uploads.
        self.schedule_pending_uploads(&mut queue);

        // 4) Garbage-collect completed items. A failure no retry can fix left
        // the queue above; every other failure stays queued for its retry.
        queue.remove_terminal();

        drained_events
            .into_iter()
            .filter(|queued| !queued.externally_delivered)
            .map(|queued| queued.event)
            .collect()
    }

    fn schedule_pending_uploads(&self, queue: &mut UploadQueue) {
        let now = Instant::now();
        for upload in &mut queue.entries {
            if !upload.is_due(now) {
                continue;
            }

            upload.status = UploadStatus::InFlight { started_at: now };
            // A writer that lost its authority stops uploading: its objects
            // could only become ignored orphans.
            if let Err(error @ crate::common::MidgeError::Fenced(_)) = self.check_write_authority()
            {
                Self::emit_wal_upload_failure(
                    upload,
                    &error.to_string(),
                    CloudUploadFailureKind::Fenced,
                    &self.event_queue,
                    self.external_event_tx.as_ref(),
                );
                continue;
            }
            if crate::failpoints::is_active("midge::cloud::inject_fail_wal_upload") {
                self.counters.record(|m| {
                    m.record_cloud_async_wal_upload_failed();
                });
                Self::emit_wal_upload_failure(
                    upload,
                    "failpoint: cloud WAL upload failed",
                    CloudUploadFailureKind::Other,
                    &self.event_queue,
                    self.external_event_tx.as_ref(),
                );
                continue;
            }

            if self.upload_worker_failed {
                Self::emit_wal_upload_failure(
                    upload,
                    "cloud upload worker failed to start",
                    CloudUploadFailureKind::Other,
                    &self.event_queue,
                    self.external_event_tx.as_ref(),
                );
                continue;
            }

            let send_result = self
                .wal_upload_worker
                .lock()
                .tx
                .as_ref()
                .map(|tx| tx.try_send(upload.clone()));
            match send_result {
                Some(Ok(())) => {}
                Some(Err(mpsc::TrySendError::Full(_))) => {
                    upload.status = UploadStatus::Pending;
                    break;
                }
                Some(Err(mpsc::TrySendError::Disconnected(_))) => {
                    tracing::warn!(
                        segment_id = upload.segment_id,
                        "cloud upload worker unavailable"
                    );
                    Self::emit_wal_upload_failure(
                        upload,
                        "cloud upload worker channel disconnected",
                        CloudUploadFailureKind::Other,
                        &self.event_queue,
                        self.external_event_tx.as_ref(),
                    );
                }
                None => Self::emit_wal_upload_failure(
                    upload,
                    "cloud upload worker is shutting down",
                    CloudUploadFailureKind::Other,
                    &self.event_queue,
                    self.external_event_tx.as_ref(),
                ),
            }
        }
    }

    fn duration_micros_to_u64(duration: Duration) -> u64 {
        u64::try_from(duration.as_micros()).unwrap_or(u64::MAX)
    }

    pub(super) fn queue_storage_event(
        event_queue: &Arc<Mutex<BoundedEventQueue>>,
        external_event_tx: Option<&cb::Sender<StorageEvent>>,
        mut event: StorageEvent,
    ) {
        match &mut event {
            StorageEvent::CloudFail { error, .. }
            | StorageEvent::CloudWalPruneAttemptFailed { error, .. } => {
                Self::truncate_storage_error(error);
            }
            StorageEvent::CloudWalPruneComplete {
                result: StorageOutcome::Err(error),
                ..
            } => Self::truncate_typed_storage_error(error),
            _ => {}
        }
        let externally_delivered =
            external_event_tx.is_some_and(|tx| tx.try_send(event.clone()).is_ok());
        if let Err(error) = event_queue.lock().try_push(event, externally_delivered) {
            tracing::error!(%error, "bounded storage event queue rejected completion");
        }
    }

    fn truncate_storage_error(error: &mut String) {
        if error.len() <= MAX_CLOUD_ERROR_BYTES {
            return;
        }
        let mut end = MAX_CLOUD_ERROR_BYTES;
        while !error.is_char_boundary(end) {
            end = end.saturating_sub(1);
        }
        error.truncate(end);
    }

    /// Bound a typed error's message without losing its classification.
    fn truncate_typed_storage_error(error: &mut crate::storage::StorageError) {
        let mut message = error.message().to_string();
        if message.len() <= MAX_CLOUD_ERROR_BYTES {
            return;
        }
        Self::truncate_storage_error(&mut message);
        *error = crate::storage::StorageError::new(error.kind(), message);
    }

    pub(super) fn spawn_wal_upload_worker(
        counters: &crate::telemetry::CounterSink,
        wal_upload_rx: mpsc::Receiver<UploadState>,
        cloud: Arc<dyn StorageBackend>,
        event_queue: Arc<Mutex<BoundedEventQueue>>,
        external_event_tx: Option<cb::Sender<StorageEvent>>,
        callback_timeout: Duration,
    ) -> (Option<JoinHandle<()>>, bool) {
        let worker_counters = counters.clone();
        let spawn_result = thread::Builder::new()
            .name("midge-wal-uploader".to_string())
            .spawn(move || {
                while let Ok(upload) = wal_upload_rx.recv() {
                    Self::process_wal_upload_attempt(
                        &worker_counters,
                        &upload,
                        &cloud,
                        &event_queue,
                        external_event_tx.as_ref(),
                        callback_timeout,
                    );
                }
            });

        match spawn_result {
            Ok(handle) => (Some(handle), false),
            Err(error) => {
                tracing::error!("Failed to spawn WAL upload worker: {error}");
                counters.record(|m| {
                    m.record_thread_spawn_failure();
                });
                (None, true)
            }
        }
    }

    pub(super) fn process_wal_upload_attempt(
        counters: &crate::telemetry::CounterSink,
        upload: &UploadState,
        cloud: &Arc<dyn StorageBackend>,
        event_queue: &Arc<Mutex<BoundedEventQueue>>,
        external_event_tx: Option<&cb::Sender<StorageEvent>>,
        callback_timeout: Duration,
    ) {
        let result = std::panic::catch_unwind(std::panic::AssertUnwindSafe(|| {
            Self::process_wal_upload(counters, upload, cloud, callback_timeout)
        }));
        let terminal_event = if let Ok(event) = result {
            event
        } else {
            counters.record(|m| {
                m.record_cloud_async_wal_upload_failed();
            });
            Self::wal_upload_failure_event(
                upload,
                "cloud WAL upload worker panicked",
                CloudUploadFailureKind::Other,
            )
        };
        // Publication deliberately sits outside the unwind boundary. Once an
        // attempt result is observable, no caught panic may synthesize a second
        // and contradictory terminal event for that same attempt.
        Self::queue_storage_event(event_queue, external_event_tx, terminal_event);
    }

    fn process_wal_upload(
        counters: &crate::telemetry::CounterSink,
        upload: &UploadState,
        cloud: &Arc<dyn StorageBackend>,
        callback_timeout: Duration,
    ) -> StorageEvent {
        let upload_start = Instant::now();
        counters.record(|m| {
            m.record_cloud_async_wal_upload_started();
        });

        Self::log_wal_upload_start(upload, true);
        let data = match Self::read_wal_file(upload) {
            Ok(data) => data,
            Err(error) => {
                return Self::wal_upload_failure_event(
                    upload,
                    &error,
                    CloudUploadFailureKind::Other,
                );
            }
        };

        Self::record_wal_bytes(counters, &data);
        crate::failpoints::fail_point!("midge::cloud::before_wal_upload");
        if crate::failpoints::is_active("midge::cloud::inject_fail_wal_upload") {
            counters.record(|m| {
                m.record_cloud_async_wal_upload_failed();
            });
            return Self::wal_upload_failure_event(
                upload,
                "failpoint: cloud WAL upload failed",
                CloudUploadFailureKind::Other,
            );
        }

        let expected_data = data.clone();
        let object_key = upload.object_key.clone();
        let (tx, rx) = std::sync::mpsc::channel();
        cloud.submit_write_request(
            crate::storage::StorageRequest::new(
                &upload.object_key,
                crate::common::OperationDeadline::from_budget(callback_timeout),
                callback_timeout,
            )
            .with_precondition(crate::storage::StoragePrecondition::IfAbsent),
            data,
            tx,
        );

        Self::handle_wal_upload_result(
            counters,
            upload,
            upload_start,
            &rx,
            callback_timeout,
            |_, _| {
                let proof =
                    Self::stable_object_proof_from_backend(cloud, &object_key, callback_timeout)
                        .map_err(crate::storage::StorageError::from)?;
                // An existing object with the same bytes is this upload. One
                // with other bytes is never overwritten: no retry can fix it.
                if proof.bytes == expected_data {
                    Ok(())
                } else {
                    Err(crate::storage::StorageError::new(
                        crate::storage::StorageErrorKind::Corruption,
                        format!("remote object '{object_key}' differs from uploaded bytes"),
                    ))
                }
            },
        )
    }
}

impl HybridStorage {
    fn log_wal_upload_start(upload: &UploadState, with_worker: bool) {
        if std::env::var_os("MIDGE_TRACE_CLOUD_ASYNC").is_some()
            && upload.segment_id.is_multiple_of(1000)
        {
            if with_worker {
                eprintln!(
                    "[midge] CloudAsync upload start: segment_id={} max_sequence={} path={}",
                    upload.segment_id, upload.max_sequence, upload.source
                );
            } else {
                eprintln!(
                    "[midge] CloudAsync inline upload start: segment_id={} max_sequence={} path={}",
                    upload.segment_id, upload.max_sequence, upload.source
                );
            }
        }
    }

    fn record_wal_bytes(counters: &crate::telemetry::CounterSink, data: &[u8]) {
        let bytes = u64::try_from(data.len()).unwrap_or(u64::MAX);
        counters.record(|m| {
            m.record_cloud_upload(bytes);
        });
    }

    fn read_wal_file(upload: &UploadState) -> Result<Vec<u8>, String> {
        upload
            .source
            .read_all()
            .map(Vec::from)
            .map_err(|error| format!("read {}: {error}", upload.source))
    }

    fn emit_wal_upload_failure(
        upload: &UploadState,
        error: &str,
        failure_kind: CloudUploadFailureKind,
        event_queue: &Arc<Mutex<BoundedEventQueue>>,
        external_event_tx: Option<&cb::Sender<StorageEvent>>,
    ) {
        let fail = Self::wal_upload_failure_event(upload, error, failure_kind);
        Self::queue_storage_event(event_queue, external_event_tx, fail);
    }

    fn wal_upload_failure_event(
        upload: &UploadState,
        error: &str,
        failure_kind: CloudUploadFailureKind,
    ) -> StorageEvent {
        StorageEvent::CloudFail {
            segment_id: upload.segment_id,
            error: error.to_string(),
            failures: upload.retries.saturating_add(1),
            failure_kind,
        }
    }

    fn log_wal_upload_ack(upload: &UploadState) {
        crate::failpoints::fail_point!("midge::cloud::in_wal_upload_ack_log");
        if std::env::var_os("MIDGE_TRACE_CLOUD_ASYNC").is_some()
            && upload.segment_id.is_multiple_of(1000)
        {
            eprintln!(
                "[midge] CloudAsync upload ack: segment_id={} max_sequence={}",
                upload.segment_id, upload.max_sequence
            );
        }
    }

    fn wal_upload_ack_event(upload: &UploadState) -> StorageEvent {
        StorageEvent::CloudAck {
            segment_id: upload.segment_id,
            max_sequence: upload.max_sequence,
        }
    }

    fn handle_wal_upload_result(
        counters: &crate::telemetry::CounterSink,
        upload: &UploadState,
        upload_start: Instant,
        rx: &std::sync::mpsc::Receiver<StorageEvent>,
        callback_timeout: Duration,
        mut verify_remote: impl FnMut(u64, u64) -> Result<(), crate::storage::StorageError>,
    ) -> StorageEvent {
        let (write_reported_success, write_error, write_failure_kind) =
            match rx.recv_timeout(callback_timeout) {
                Ok(StorageEvent::WriteComplete { key, result }) => {
                    let _ = key;
                    match result {
                        StorageOutcome::Ok(()) => (true, None, CloudUploadFailureKind::Other),
                        StorageOutcome::Err(error) => {
                            let failure_kind = if error.is_timeout() {
                                CloudUploadFailureKind::Timeout
                            } else {
                                CloudUploadFailureKind::Other
                            };
                            (false, Some(error.to_string()), failure_kind)
                        }
                    }
                }
                Err(mpsc::RecvTimeoutError::Timeout) => (
                    false,
                    Some("cloud WAL upload callback timed out".to_string()),
                    CloudUploadFailureKind::Timeout,
                ),
                Err(mpsc::RecvTimeoutError::Disconnected) | Ok(_) => (
                    false,
                    Some(
                        "cloud WAL upload callback channel closed or returned an unexpected event"
                            .to_string(),
                    ),
                    CloudUploadFailureKind::Other,
                ),
            };

        match verify_remote(upload.segment_id, upload.max_sequence) {
            Ok(()) => {
                let upload_latency_us = Self::duration_micros_to_u64(upload_start.elapsed());
                Self::log_wal_upload_ack(upload);
                counters.record(|m| {
                    m.record_cloud_async_wal_upload_completed(upload_latency_us);
                });
                Self::wal_upload_ack_event(upload)
            }
            Err(readback_error) => {
                counters.record(|m| {
                    m.record_cloud_async_wal_upload_failed();
                });
                let error = if write_reported_success {
                    format!("remote WAL readback validation failed: {readback_error}")
                } else {
                    format!(
                        "{}; remote WAL readback did not confirm the write: {readback_error}",
                        write_error.unwrap_or_else(|| "cloud WAL upload failed".to_string())
                    )
                };
                let failure_kind =
                    if readback_error.kind() == crate::storage::StorageErrorKind::Corruption {
                        CloudUploadFailureKind::Corruption
                    } else if write_failure_kind == CloudUploadFailureKind::Timeout
                        || readback_error.is_timeout()
                    {
                        CloudUploadFailureKind::Timeout
                    } else {
                        CloudUploadFailureKind::Other
                    };
                Self::wal_upload_failure_event(upload, &error, failure_kind)
            }
        }
    }
}
