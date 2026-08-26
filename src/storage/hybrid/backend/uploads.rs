//! WAL upload scheduling, worker execution, retries, and acknowledgements.

use super::{
    cb, mpsc, thread, Arc, BoundedEventQueue, Duration, HybridStorage, Instant, JoinHandle, Mutex,
    Path, StorageBackend, StorageEvent, StorageOutcome, UploadQueue, UploadState, UploadStatus,
    MAX_CLOUD_ERROR_BYTES,
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
            if let Some(telemetry) = crate::telemetry::Telemetry::global() {
                telemetry.metrics().record_write_stall_cloud();
            }
            return Err(error);
        }
        Ok(())
    }

    pub fn ensure_wal_write_admission(&self) -> crate::common::MidgeResult<()> {
        let queue = self.upload_queue.lock();
        if !queue.is_stalled() {
            return Ok(());
        }
        Err(crate::common::MidgeError::WriteStall(format!(
            "CloudAsync WAL upload queue remains at capacity: entries={}/{}, bytes={}/{}",
            queue.entries.len(),
            queue.max_entries,
            queue.pending_bytes,
            queue.max_bytes
        )))
    }

    pub fn is_wal_upload_stalled(&self) -> bool {
        self.upload_queue.lock().is_stalled()
    }

    /// Queue a local file for bounded remote publication under an explicit
    /// object key. The runtime owns the meaning of the request id and frontier.
    pub(crate) fn enqueue_object_upload(
        &self,
        request_id: u64,
        object_key: String,
        local_path: &Path,
        frontier: u64,
    ) -> crate::common::MidgeResult<()> {
        let size_bytes = std::fs::metadata(local_path)
            .map_err(crate::common::MidgeError::Io)?
            .len();
        let upload_state = UploadState {
            segment_id: request_id,
            object_key,
            local_path: local_path.to_path_buf(),
            status: UploadStatus::Pending,
            max_sequence: frontier,
            retries: 0,
            size_bytes,
        };

        let mut queue = self.upload_queue.lock();
        if let Err(error) = queue.try_push(upload_state) {
            if let Some(telemetry) = crate::telemetry::Telemetry::global() {
                telemetry.metrics().record_write_stall_cloud();
            }
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
            ?local_path,
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
    /// - Handles retries on failure (up to 3 attempts)
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

        let mut queue = self.upload_queue.lock();

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
                    segment_id, error, ..
                } => {
                    if let Some(item) = queue
                        .entries
                        .iter_mut()
                        .find(|u| &u.segment_id == segment_id)
                    {
                        item.retries = item.retries.saturating_add(1);
                        item.status = UploadStatus::Failed {
                            error: error.clone(),
                            retries: item.retries,
                        };
                    }
                }
                _ => {}
            }
        }

        // 3) Schedule any eligible uploads.
        self.schedule_pending_uploads(&mut queue);

        // 4) Garbage-collect finished items (Completed or Failed after 3 attempts).
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
            let eligible = match upload.status {
                UploadStatus::Pending => true,
                UploadStatus::Failed { .. } => upload.retries < 3,
                UploadStatus::InFlight { .. } | UploadStatus::Completed => false,
            };
            if !eligible {
                continue;
            }

            upload.status = UploadStatus::InFlight { started_at: now };
            if crate::failpoints::is_active("midge::cloud::inject_fail_wal_upload") {
                if let Some(telemetry) = crate::telemetry::Telemetry::global() {
                    telemetry.metrics().record_cloud_async_wal_upload_failed();
                }
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

            let Some(tx) = &self.wal_upload_tx else {
                Self::emit_wal_upload_failure(
                    upload,
                    "cloud upload worker is shutting down",
                    CloudUploadFailureKind::Other,
                    &self.event_queue,
                    self.external_event_tx.as_ref(),
                );
                continue;
            };
            match tx.try_send(upload.clone()) {
                Ok(()) => {}
                Err(mpsc::TrySendError::Full(_)) => {
                    upload.status = UploadStatus::Pending;
                    break;
                }
                Err(mpsc::TrySendError::Disconnected(_)) => {
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
            | StorageEvent::CloudWalPruneAttemptFailed { error, .. }
            | StorageEvent::CloudWalPruneComplete {
                result: StorageOutcome::Err(error),
                ..
            } => Self::truncate_storage_error(error),
            _ => {}
        }
        let externally_delivered =
            external_event_tx.is_some_and(|tx| tx.try_send(event.clone()).is_ok());
        if let Err(error) = event_queue.lock().try_push(event, externally_delivered) {
            tracing::error!(error, "bounded storage event queue rejected completion");
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

    pub(super) fn spawn_wal_upload_worker(
        wal_upload_rx: mpsc::Receiver<UploadState>,
        cloud: Arc<dyn StorageBackend>,
        event_queue: Arc<Mutex<BoundedEventQueue>>,
        external_event_tx: Option<cb::Sender<StorageEvent>>,
        callback_timeout: Duration,
    ) -> (Option<JoinHandle<()>>, bool) {
        let spawn_result = thread::Builder::new()
            .name("midge-wal-uploader".to_string())
            .spawn(move || {
                while let Ok(upload) = wal_upload_rx.recv() {
                    let result = std::panic::catch_unwind(std::panic::AssertUnwindSafe(|| {
                        Self::process_wal_upload(
                            &upload,
                            &cloud,
                            &event_queue,
                            external_event_tx.as_ref(),
                            callback_timeout,
                        );
                    }));
                    if result.is_err() {
                        if let Some(telemetry) = crate::telemetry::Telemetry::global() {
                            telemetry.metrics().record_cloud_async_wal_upload_failed();
                        }
                        Self::emit_wal_upload_failure(
                            &upload,
                            "cloud WAL upload worker panicked",
                            CloudUploadFailureKind::Other,
                            &event_queue,
                            external_event_tx.as_ref(),
                        );
                    }
                }
            });

        match spawn_result {
            Ok(handle) => (Some(handle), false),
            Err(error) => {
                tracing::error!("Failed to spawn WAL upload worker: {error}");
                if let Some(t) = crate::telemetry::Telemetry::global() {
                    t.metrics().record_thread_spawn_failure();
                }
                (None, true)
            }
        }
    }

    fn process_wal_upload(
        upload: &UploadState,
        cloud: &Arc<dyn StorageBackend>,
        event_queue: &Arc<Mutex<BoundedEventQueue>>,
        external_event_tx: Option<&cb::Sender<StorageEvent>>,
        callback_timeout: Duration,
    ) {
        let upload_start = Instant::now();
        if let Some(telemetry) = crate::telemetry::Telemetry::global() {
            telemetry.metrics().record_cloud_async_wal_upload_started();
        }

        Self::log_wal_upload_start(upload, true);
        let data = match Self::read_wal_file(upload) {
            Ok(data) => data,
            Err(error) => {
                Self::emit_wal_upload_failure(
                    upload,
                    &error,
                    CloudUploadFailureKind::Other,
                    event_queue,
                    external_event_tx,
                );
                return;
            }
        };

        Self::record_wal_bytes(&data);
        crate::failpoints::fail_point!("midge::cloud::before_wal_upload");
        if crate::failpoints::is_active("midge::cloud::inject_fail_wal_upload") {
            if let Some(telemetry) = crate::telemetry::Telemetry::global() {
                telemetry.metrics().record_cloud_async_wal_upload_failed();
            }
            Self::emit_wal_upload_failure(
                upload,
                "failpoint: cloud WAL upload failed",
                CloudUploadFailureKind::Other,
                event_queue,
                external_event_tx,
            );
            return;
        }

        let expected_data = data.clone();
        let object_key = upload.object_key.clone();
        let (tx, rx) = std::sync::mpsc::channel();
        cloud.submit_write_with_headers(
            &upload.object_key,
            data,
            vec![("If-None-Match".into(), "*".into())],
            tx,
        );

        Self::handle_wal_upload_result(
            upload,
            upload_start,
            event_queue,
            external_event_tx,
            &rx,
            callback_timeout,
            |_, _| {
                let proof =
                    Self::stable_object_proof_from_backend(cloud, &object_key, callback_timeout)?;
                if proof.bytes == expected_data {
                    Ok(())
                } else {
                    Err(format!(
                        "remote object '{object_key}' differs from uploaded bytes"
                    ))
                }
            },
        );
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
                    upload.segment_id,
                    upload.max_sequence,
                    upload.local_path.display()
                );
            } else {
                eprintln!(
                    "[midge] CloudAsync inline upload start: segment_id={} max_sequence={} path={}",
                    upload.segment_id,
                    upload.max_sequence,
                    upload.local_path.display()
                );
            }
        }
    }

    fn record_wal_bytes(data: &[u8]) {
        let bytes = u64::try_from(data.len()).unwrap_or(u64::MAX);
        if let Some(telemetry) = crate::telemetry::Telemetry::global() {
            telemetry.metrics().record_cloud_upload(bytes);
        }
    }

    fn read_wal_file(upload: &UploadState) -> Result<Vec<u8>, String> {
        std::fs::read(&upload.local_path)
            .map_err(|error| format!("read {}: {}", upload.local_path.display(), error))
    }

    fn emit_wal_upload_failure(
        upload: &UploadState,
        error: &str,
        failure_kind: CloudUploadFailureKind,
        event_queue: &Arc<Mutex<BoundedEventQueue>>,
        external_event_tx: Option<&cb::Sender<StorageEvent>>,
    ) {
        let fail = StorageEvent::CloudFail {
            segment_id: upload.segment_id,
            error: error.to_string(),
            terminal: upload.retries.saturating_add(1) >= 3,
            failure_kind,
        };
        Self::queue_storage_event(event_queue, external_event_tx, fail);
    }

    fn log_wal_upload_ack(upload: &UploadState) {
        if std::env::var_os("MIDGE_TRACE_CLOUD_ASYNC").is_some()
            && upload.segment_id.is_multiple_of(1000)
        {
            eprintln!(
                "[midge] CloudAsync upload ack: segment_id={} max_sequence={}",
                upload.segment_id, upload.max_sequence
            );
        }
    }

    fn emit_wal_upload_ack(
        upload: &UploadState,
        event_queue: &Arc<Mutex<BoundedEventQueue>>,
        external_event_tx: Option<&cb::Sender<StorageEvent>>,
    ) {
        let ack = StorageEvent::CloudAck {
            segment_id: upload.segment_id,
            max_sequence: upload.max_sequence,
        };
        Self::queue_storage_event(event_queue, external_event_tx, ack);
    }

    fn handle_wal_upload_result(
        upload: &UploadState,
        upload_start: Instant,
        event_queue: &Arc<Mutex<BoundedEventQueue>>,
        external_event_tx: Option<&cb::Sender<StorageEvent>>,
        rx: &std::sync::mpsc::Receiver<StorageEvent>,
        callback_timeout: Duration,
        mut verify_remote: impl FnMut(u64, u64) -> Result<(), String>,
    ) {
        let (write_reported_success, write_error, write_failure_kind) =
            match rx.recv_timeout(callback_timeout) {
                Ok(StorageEvent::WriteComplete { key, result }) => {
                    let _ = key;
                    match result {
                        StorageOutcome::Ok(()) => (true, None, CloudUploadFailureKind::Other),
                        StorageOutcome::Err(error) => {
                            let failure_kind = if Self::storage_error_indicates_timeout(&error) {
                                CloudUploadFailureKind::Timeout
                            } else {
                                CloudUploadFailureKind::Other
                            };
                            (false, Some(error), failure_kind)
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
                if let Some(telemetry) = crate::telemetry::Telemetry::global() {
                    telemetry.metrics().record_cloud_async_wal_upload_completed(
                        Self::duration_micros_to_u64(upload_start.elapsed()),
                    );
                }
                Self::emit_wal_upload_ack(upload, event_queue, external_event_tx);
                Self::log_wal_upload_ack(upload);
            }
            Err(readback_error) => {
                if let Some(telemetry) = crate::telemetry::Telemetry::global() {
                    telemetry.metrics().record_cloud_async_wal_upload_failed();
                }
                let error = if write_reported_success {
                    format!("remote WAL readback validation failed: {readback_error}")
                } else {
                    format!(
                        "{}; remote WAL readback did not confirm the write: {readback_error}",
                        write_error.unwrap_or_else(|| "cloud WAL upload failed".to_string())
                    )
                };
                let failure_kind = if write_failure_kind == CloudUploadFailureKind::Timeout
                    || Self::storage_error_indicates_timeout(&readback_error)
                {
                    CloudUploadFailureKind::Timeout
                } else {
                    CloudUploadFailureKind::Other
                };
                Self::emit_wal_upload_failure(
                    upload,
                    &error,
                    failure_kind,
                    event_queue,
                    external_event_tx,
                );
            }
        }
    }
}
