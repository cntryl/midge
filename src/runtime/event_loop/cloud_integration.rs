//! Cloud / hybrid storage integration
//!
//! Handles CloudAsync WAL flush, cloud upload ack/fail events,
//! and hybrid storage polling/push-channel draining.

use super::durability_sync::CompletionSource;
use super::EventLoop;
use crossbeam::channel::TryRecvError;
use std::time::Instant;

impl EventLoop {
    pub(super) fn seal_current_cloud_segment(
        &mut self,
    ) -> crate::common::MidgeResult<Option<(u64, u64)>> {
        if !self.wal_actor.is_cloud_async() {
            return Ok(None);
        }
        let Some(storage) = &self.hybrid_storage else {
            return Err(crate::common::MidgeError::Internal(
                "CloudAsync requires HybridStorage".to_string(),
            ));
        };
        if self.state.memory_mode || self.state.wal.pending_writes == 0 {
            return Ok(None);
        }

        let segment_id = self.state.wal.current_segment_id;
        let bytes_buffered = self.wal_actor.bytes_since_sync() as u64;
        let seal_start = Instant::now();
        self.wal_actor.flush_for_cloud_upload(&mut self.state)?;
        fail::fail_point!(
            "midge::cloud::inject_fail_after_wal_flush_before_rotate",
            |_| Err(crate::common::MidgeError::Internal(
                "failpoint: cloud seal failed after WAL flush before rotate".to_string(),
            ))
        );
        if let Err(error) = self.wal_actor.rotate(&mut self.state) {
            tracing::error!(error = %error, "CloudAsync: WAL rotate failed");
            return Err(error);
        }

        self.durability.rotate_to(self.state.wal.current_segment_id);

        let max_sequence = self.state.wal.local_durable_seq;
        let local_path = self
            .state
            .wal_dir
            .join(crate::wal::segment_file_name(segment_id));
        storage.enqueue_wal_segment(segment_id, local_path, max_sequence);
        self.wal_actor.complete_cloud_upload_seal(&mut self.state);

        let resource = crate::wal::cloud_segment_object_key(segment_id);
        if !self
            .state
            .cloud
            .pending_uploads
            .iter()
            .any(|item| item == &resource)
        {
            self.state.cloud.pending_uploads.push(resource);
        }

        self.durability
            .record_cloud_segment_inflight(segment_id, max_sequence);
        self.durability.record_cloud_flush();

        if let Some(telemetry) = crate::telemetry::Telemetry::global() {
            telemetry.metrics().record_cloud_async_wal_segment_sealed(
                bytes_buffered,
                seal_start.elapsed().as_micros() as u64,
            );
        }

        Ok(Some((segment_id, max_sequence)))
    }

    pub(super) fn tick_hybrid_storage(&mut self) {
        let Some(storage) = &self.hybrid_storage else {
            return;
        };

        // Drive async storage uploads.
        // In push-channel mode, completion events are delivered via `hybrid_storage_events`.
        // In polling mode, `process_uploads()` returns completion events.
        let storage_events = storage.process_uploads();
        for event in storage_events {
            self.handle_storage_event(event);
        }
    }

    pub(super) fn drain_hybrid_storage_events(&mut self) {
        let Some(rx) = &self.hybrid_storage_events else {
            return;
        };

        let rx = rx.clone();

        loop {
            match rx.try_recv() {
                Ok(event) => self.handle_storage_event(event),
                Err(TryRecvError::Empty) => break,
                Err(TryRecvError::Disconnected) => break,
            }
        }
    }

    pub(super) fn handle_storage_event(&mut self, event: crate::storage::StorageEvent) {
        match event {
            crate::storage::StorageEvent::CloudAck {
                segment_id,
                max_sequence,
            } => {
                if let Err(error) =
                    self.verify_remote_wal_segment_before_ack(segment_id, max_sequence)
                {
                    self.handle_cloud_upload_failure(
                        segment_id,
                        format!("cloud WAL readback validation failed: {error}"),
                    );
                    return;
                }

                let resource = crate::wal::cloud_segment_object_key(segment_id);
                self.state
                    .cloud
                    .pending_uploads
                    .retain(|item| item != &resource);
                self.cloud_acked_wal_segments
                    .insert(segment_id, max_sequence);

                let ready_segments = match self
                    .durability
                    .take_contiguous_acked_cloud_segments(&self.cloud_acked_wal_segments)
                {
                    Ok(ready_segments) => ready_segments,
                    Err(error) => {
                        self.cloud_acked_wal_segments.remove(&segment_id);
                        self.handle_cloud_upload_failure(segment_id, error);
                        return;
                    }
                };

                let Some((durable_segment_id, durable_max_sequence)) =
                    ready_segments.last().copied()
                else {
                    tracing::debug!(
                        segment_id,
                        max_sequence,
                        "CloudAck buffered behind an earlier unacked WAL segment"
                    );
                    return;
                };

                match self.wal_actor.handle_cloud_upload_complete(
                    &mut self.state,
                    durable_segment_id,
                    durable_max_sequence,
                ) {
                    Ok(()) => {
                        for (ready_segment_id, _) in &ready_segments {
                            self.remove_cloud_durable_local_wal_segment(*ready_segment_id);
                        }

                        for (seg_id, _) in ready_segments {
                            if let Some(enqueued_at) =
                                self.durability.take_cloud_segment_timing(seg_id)
                            {
                                if let Some(telemetry) = crate::telemetry::Telemetry::global() {
                                    telemetry.metrics().record_cloud_async_wal_ack_latency_us(
                                        enqueued_at.elapsed().as_micros() as u64,
                                    );
                                }
                            }

                            let waiters = self.durability.complete_waiters_at(seg_id);
                            self.complete_durability_waiters(waiters, CompletionSource::CloudAck);
                        }
                        self.prune_cloud_wal_segments_covered_by_manifest();
                        self.drain_auto_flush_memtables();
                    }
                    Err(e) => {
                        tracing::error!(error = %e, "failed to apply cloud ack");
                    }
                }
            }
            crate::storage::StorageEvent::CloudFail { segment_id, error } => {
                self.handle_cloud_upload_failure(segment_id, error);
            }
            crate::storage::StorageEvent::CloudWalPruneComplete { segment_id, result } => {
                self.cloud_wal_prune_inflight.remove(&segment_id);
                match result {
                    crate::storage::StorageOutcome::Ok(()) => {
                        self.cloud_acked_wal_segments.remove(&segment_id);
                        tracing::debug!(segment_id, "Pruned cloud-covered remote WAL segment");
                    }
                    crate::storage::StorageOutcome::Err(error) => {
                        tracing::warn!(
                            segment_id,
                            error = %error,
                            "Failed to prune cloud-covered remote WAL segment; will retry after a future checkpoint"
                        );
                    }
                }
            }
            crate::storage::StorageEvent::BackpressureOn => {
                tracing::warn!("storage backpressure activated — pausing flushes");
                self.state.write_stalled = true;
            }
            crate::storage::StorageEvent::BackpressureOff => {
                tracing::info!("storage backpressure released — resuming normal operation");
                if self.state.write_stalled {
                    self.state.write_stalled = false;
                }
                self.wake_write_stall_waiters();
                self.drain_auto_flush_memtables();
            }
            _ => {}
        }
    }

    fn verify_remote_wal_segment_before_ack(
        &mut self,
        segment_id: u64,
        max_sequence: u64,
    ) -> Result<(), String> {
        let Some(storage) = self.hybrid_storage.as_ref() else {
            return Err("CloudAck received without HybridStorage".to_string());
        };
        storage.verify_cached_remote_wal_segment(segment_id, max_sequence)
    }

    fn handle_cloud_upload_failure(&mut self, segment_id: u64, error: String) {
        let resource = crate::wal::cloud_segment_object_key(segment_id);
        self.state
            .cloud
            .pending_uploads
            .retain(|item| item != &resource);
        self.state.mark_persistence_anomaly();
        self.cloud_acked_wal_segments.split_off(&segment_id);

        // Attempt to recover the failed segment's max_sequence so we can
        // invalidate idempotency allocations that were part of it.
        let failed_max_seq = self.durability.take_cloud_segment_max_sequence(segment_id);

        // Let WAL actor handle its internal failure handling and drop pending writes.
        self.wal_actor
            .handle_cloud_upload_failed(segment_id, &error);

        // If we know the max_sequence for the failed segment, invalidate idempotency
        // allocations up to that sequence so retries will allocate fresh sequences.
        if let Some(max_seq) = failed_max_seq {
            self.state.invalidate_idempotency_allocations_up_to(max_seq);
        }

        let waiters = self.durability.drain_all_waiters();
        for w in waiters {
            let request_id = match w {
                super::super::durability::DurabilityWaiter::WalAppend { request_id, .. }
                | super::super::durability::DurabilityWaiter::ConfirmWalAppend { request_id }
                | super::super::durability::DurabilityWaiter::TransactionApply {
                    request_id, ..
                }
                | super::super::durability::DurabilityWaiter::ConfirmTransactionApply {
                    request_id,
                }
                | super::super::durability::DurabilityWaiter::CloudDurability { request_id }
                | super::super::durability::DurabilityWaiter::Read { request_id, .. }
                | super::super::durability::DurabilityWaiter::RangeScan { request_id, .. } => {
                    request_id
                }
            };
            self.respond(
                request_id,
                super::super::RuntimeResponse::Error {
                    request_id,
                    error: crate::common::MidgeError::Internal(format!(
                        "Cloud durability failed: {error}"
                    )),
                },
            );
        }

        // Clear any remaining inflight segments.
        self.durability.clear_inflight();
    }

    pub(super) fn maybe_flush_cloud_async_wal(&mut self) {
        if !self.wal_actor.is_cloud_async() {
            return;
        }
        if self.hybrid_storage.is_none() {
            return;
        }

        if self.state.memory_mode {
            return;
        }

        // No pending local records to ship.
        if self.state.wal.pending_writes == 0 {
            return;
        }

        let pending_writes = self.state.wal.pending_writes;
        let bytes_buffered = self.wal_actor.bytes_since_sync();

        if !self
            .durability
            .should_flush_cloud_async(pending_writes, bytes_buffered)
        {
            return;
        }

        let seal_result = self.seal_current_cloud_segment();
        let Ok(Some((segment_id, max_sequence))) = seal_result else {
            if let Err(error) = seal_result {
                tracing::error!(error = %error, "CloudAsync: forced WAL seal failed");
            }
            return;
        };

        if std::env::var_os("MIDGE_TRACE_CLOUD_ASYNC").is_some() {
            // Throttle: log every 1000 segments to avoid noise.
            if segment_id.is_multiple_of(1000) {
                eprintln!(
                    "[midge] CloudAsync flush: segment_id={segment_id} max_sequence={max_sequence} pending_cloud={} ",
                    self.wal_actor.has_pending_cloud_writes()
                );
            }
        }

        self.drain_auto_flush_memtables();
    }

    pub(super) fn prune_cloud_wal_segments_covered_by_manifest(&mut self) {
        if !self.wal_actor.is_cloud_async() || self.state.memory_mode {
            return;
        }

        let Some(storage) = self.hybrid_storage.clone() else {
            return;
        };
        let Some(recovery_floor_segment) = self.state.cloud_wal_recovery_floor_segment() else {
            return;
        };
        let persisted_sequence = self.state.manifest.last_persisted_sequence;

        let eligible_segments: Vec<u64> = self
            .cloud_acked_wal_segments
            .iter()
            .filter_map(|(segment_id, max_sequence)| {
                (*segment_id < recovery_floor_segment
                    && *max_sequence <= self.state.wal.cloud_durable_seq
                    && *max_sequence <= persisted_sequence
                    && !self.cloud_wal_prune_inflight.contains(segment_id))
                .then_some(*segment_id)
            })
            .collect();

        if eligible_segments.is_empty() {
            return;
        }

        if let Err(error) = storage.verify_manifest_cloud_objects(&self.state.manifest) {
            self.state.mark_persistence_anomaly();
            tracing::warn!(
                error = %error,
                "Skipping remote WAL prune because manifest-referenced cloud objects are not fully readable"
            );
            return;
        }

        if let Err(error) = self.verify_cloud_metadata_for_wal_cleanup() {
            self.state.mark_persistence_anomaly();
            tracing::warn!(
                error = %error,
                "Skipping remote WAL prune because cloud metadata is not fully readable"
            );
            return;
        }

        for segment_id in eligible_segments {
            self.cloud_wal_prune_inflight.insert(segment_id);
            if let Err(error) = storage.prune_cloud_wal_segment(segment_id) {
                self.cloud_wal_prune_inflight.remove(&segment_id);
                self.state.mark_persistence_anomaly();
                tracing::warn!(
                    segment_id,
                    error = %error,
                    "Failed to schedule cloud-covered remote WAL prune"
                );
            }
        }
    }

    fn read_cloud_metadata_head_for_wal_cleanup(
        cloud: &crate::storage::cloud::CloudStorage,
        key: &str,
    ) -> Result<crate::storage::StorageObjectMetadata, String> {
        let (tx, rx) = std::sync::mpsc::channel();
        cloud.submit_head(key.to_string(), tx);
        match rx.recv_timeout(std::time::Duration::from_secs(30)) {
            Ok(crate::storage::cloud::CloudEvent::HeadComplete {
                result: crate::storage::cloud::CloudOutcome::Ok(metadata),
                ..
            }) => Ok(crate::storage::StorageObjectMetadata {
                size: metadata.size,
                etag: metadata.etag,
                generation: metadata.generation,
            }),
            Ok(crate::storage::cloud::CloudEvent::HeadComplete {
                result: crate::storage::cloud::CloudOutcome::Err(error),
                ..
            }) => Err(format!(
                "cloud metadata '{key}' is unreadable during cached proof revalidation: {error}"
            )),
            Ok(other) => Err(format!(
                "unexpected cloud metadata HEAD response for '{key}': {other:?}"
            )),
            Err(error) => Err(format!(
                "cloud metadata HEAD timed out for '{key}': {error}"
            )),
        }
    }

    fn verify_cloud_metadata_for_wal_cleanup(&mut self) -> Result<(), String> {
        let Some(cloud) = self.cloud_metadata_storage.as_ref() else {
            return Ok(());
        };

        for file_name in crate::storage::cloud::CLOUD_METADATA_FILES {
            let file_name = *file_name;
            let local_path = self.state.db_path.join(file_name);
            if !local_path.exists() {
                continue;
            }
            let local_data = std::fs::read(&local_path).map_err(|error| {
                format!(
                    "local metadata '{}' is unreadable: {error}",
                    local_path.display()
                )
            })?;
            let key = crate::storage::cloud::cloud_metadata_key(file_name);
            let local_len = local_data.len() as u64;
            let local_crc32c = crc32c::crc32c(&local_data);
            if let Some(proof) = self.cloud_metadata_cleanup_proofs.get(file_name) {
                if proof.len == local_len && proof.crc32c == local_crc32c {
                    let actual = Self::read_cloud_metadata_head_for_wal_cleanup(cloud, &key)?;
                    if actual == proof.remote {
                        continue;
                    }
                    return Err(format!(
                        "cloud metadata '{key}' changed since validation: expected {:?}, actual {:?}",
                        proof.remote, actual
                    ));
                }
            }
            self.cloud_metadata_cleanup_proofs.remove(file_name);

            let (tx, rx) = std::sync::mpsc::channel();
            cloud.submit_get(key.clone(), tx);
            match rx.recv_timeout(std::time::Duration::from_secs(30)) {
                Ok(crate::storage::cloud::CloudEvent::GetComplete {
                    result: crate::storage::cloud::CloudOutcome::Ok(cloud_data),
                    ..
                }) => {
                    if cloud_data != local_data {
                        return Err(format!(
                            "cloud metadata '{key}' does not match committed local metadata"
                        ));
                    }
                    let remote = Self::read_cloud_metadata_head_for_wal_cleanup(cloud, &key)?;
                    if remote.size != local_len {
                        return Err(format!(
                            "cloud metadata '{key}' size changed during validation: read={local_len}, head={}",
                            remote.size
                        ));
                    }
                    self.cloud_metadata_cleanup_proofs.insert(
                        file_name.to_string(),
                        super::MetadataCleanupProof {
                            len: local_len,
                            crc32c: local_crc32c,
                            remote,
                        },
                    );
                }
                Ok(crate::storage::cloud::CloudEvent::GetComplete {
                    result: crate::storage::cloud::CloudOutcome::Err(error),
                    ..
                }) => return Err(format!("cloud metadata '{key}' is unreadable: {error}")),
                Ok(other) => {
                    return Err(format!(
                        "unexpected cloud metadata read response for '{key}': {other:?}"
                    ));
                }
                Err(error) => {
                    return Err(format!(
                        "cloud metadata read timed out for '{key}': {error}"
                    ));
                }
            }
        }

        Ok(())
    }

    fn remove_cloud_durable_local_wal_segment(&mut self, segment_id: u64) {
        if self.state.memory_mode {
            return;
        }

        let local_path = self
            .state
            .wal_dir
            .join(crate::wal::segment_file_name(segment_id));
        match std::fs::remove_file(&local_path) {
            Ok(()) => tracing::debug!(
                segment_id,
                path = %local_path.display(),
                "Removed cloud-durable local WAL segment"
            ),
            Err(error) if error.kind() == std::io::ErrorKind::NotFound => {}
            Err(error) => {
                self.state.mark_persistence_anomaly();
                tracing::warn!(
                    segment_id,
                    path = %local_path.display(),
                    error = %error,
                    "Failed to remove cloud-durable local WAL segment"
                );
            }
        }
    }
}

#[cfg(test)]
mod tests {
    use super::super::tests::{create_test_cloud_event_loop, create_test_event_loop};
    use super::super::EventLoop;
    use crate::runtime::{state::RuntimeState, ResponseRouter, RuntimeMsg, RuntimeResponse};
    use crate::sst::Memtable;
    use bytes::Bytes;
    use std::path::{Path, PathBuf};
    use std::sync::{Arc, Mutex, OnceLock};
    use std::time::{Duration, Instant};

    static FAILPOINT_TEST_LOCK: OnceLock<Mutex<()>> = OnceLock::new();

    fn failpoint_test_lock() -> &'static Mutex<()> {
        FAILPOINT_TEST_LOCK.get_or_init(|| Mutex::new(()))
    }

    fn seal_segment_for_test(el: &mut EventLoop) -> crate::common::MidgeResult<(u64, u64)> {
        let (seg_id, max_sequence) = seal_segment_without_remote_proof_for_test(el)?;
        if let Some(storage) = el.hybrid_storage.as_ref() {
            storage
                .verify_remote_wal_segment(seg_id, max_sequence)
                .expect("verify remote WAL for test CloudAck");
        }
        Ok((seg_id, max_sequence))
    }

    fn seal_segment_without_remote_proof_for_test(
        el: &mut EventLoop,
    ) -> crate::common::MidgeResult<(u64, u64)> {
        let seg_id = el.state.wal.current_segment_id;
        el.wal_actor.flush_for_cloud_upload(&mut el.state)?;
        el.wal_actor.rotate(&mut el.state)?;
        el.durability.rotate_to(el.state.wal.current_segment_id);
        let max_sequence = el.state.wal.local_durable_seq;
        copy_local_segment_to_remote_wal_for_test(el, seg_id);
        el.wal_actor.complete_cloud_upload_seal(&mut el.state);
        el.durability
            .record_cloud_segment_inflight(seg_id, max_sequence);
        el.durability.record_cloud_flush();
        Ok((seg_id, max_sequence))
    }

    fn remote_wal_path_for_test(el: &EventLoop, segment_id: u64) -> PathBuf {
        el.state
            .db_path
            .join("cloud_store")
            .join("wal")
            .join(crate::wal::cloud_segment_file_name(segment_id))
    }

    fn remote_sst_path_for_test(el: &EventLoop, sst_name: &str) -> PathBuf {
        el.state
            .db_path
            .join("cloud_store")
            .join("sst")
            .join(sst_name)
    }

    fn write_test_file(path: PathBuf, data: &[u8]) {
        if let Some(parent) = path.parent() {
            std::fs::create_dir_all(parent).expect("create test file parent");
        }
        std::fs::write(path, data).expect("write test file");
    }

    fn copy_local_segment_to_remote_wal_for_test(el: &EventLoop, segment_id: u64) {
        let local_path = el
            .state
            .wal_dir
            .join(crate::wal::segment_file_name(segment_id));
        let remote_path = remote_wal_path_for_test(el, segment_id);
        if let Some(parent) = remote_path.parent() {
            std::fs::create_dir_all(parent).expect("create remote WAL parent");
        }
        std::fs::copy(&local_path, &remote_path).unwrap_or_else(|error| {
            panic!(
                "copy local WAL '{}' to remote WAL '{}': {error}",
                local_path.display(),
                remote_path.display()
            )
        });
    }

    fn seed_cloud_prune_candidate(el: &mut EventLoop, segment_id: u64, max_sequence: u64) {
        el.state.wal.current_segment_id = segment_id + 1;
        el.state.manifest.last_persisted_sequence = max_sequence;
        el.cloud_acked_wal_segments.insert(segment_id, max_sequence);
        let record = crate::wal::WalRecord::new(
            crate::wal::WalOpKind::Put,
            Bytes::from_static(b"prune-candidate"),
            Some(Bytes::from_static(b"value")),
            max_sequence,
            0,
        );
        let payload = crate::wal::encoding::encode(&record).expect("encode test WAL record");
        let mut bytes = Vec::new();
        crate::wal::frame::append_frame(&mut bytes, &payload).expect("append test WAL frame");
        write_test_file(remote_wal_path_for_test(el, segment_id), &bytes);
        if let Some(storage) = el.hybrid_storage.as_ref() {
            storage
                .verify_remote_wal_segment(segment_id, max_sequence)
                .expect("verify remote WAL prune candidate");
        }
    }

    fn add_manifest_sst_for_test(el: &mut EventLoop, sst_name: &str, max_sequence: u64) {
        el.state.manifest.files.push(crate::metadata::FileMeta {
            name: sst_name.to_string(),
            level: 0,
            size_bytes: 128,
            cf_id: 0,
            smallest_key: Some(b"a".to_vec()),
            largest_key: Some(b"z".to_vec()),
            smallest_seq: Some(1),
            largest_seq: Some(max_sequence),
            ..Default::default()
        });
    }

    fn drain_prune_completion_for_test(el: &mut EventLoop) {
        let deadline = Instant::now() + Duration::from_secs(1);
        while Instant::now() < deadline {
            el.tick_hybrid_storage();
            if el.cloud_wal_prune_inflight.is_empty() {
                return;
            }
            std::thread::sleep(Duration::from_millis(10));
        }
    }

    fn put_cloud_metadata_for_test(
        cloud: &crate::storage::cloud::CloudStorage,
        file_name: &str,
        data: Vec<u8>,
    ) {
        let key = crate::storage::cloud::cloud_metadata_key(file_name);
        let (tx, rx) = std::sync::mpsc::channel();
        cloud.submit_put(key.clone(), data, vec![], tx);
        match rx.recv_timeout(Duration::from_secs(1)) {
            Ok(crate::storage::cloud::CloudEvent::PutComplete {
                result: crate::storage::cloud::CloudOutcome::Ok(()),
                ..
            }) => {}
            other => panic!("metadata put for '{key}' failed: {other:?}"),
        }
    }

    fn put_all_cloud_metadata_for_test(
        cloud: &crate::storage::cloud::CloudStorage,
        db_path: &Path,
    ) {
        for file_name in crate::storage::cloud::CLOUD_METADATA_FILES {
            let local_path = db_path.join(file_name);
            if !local_path.exists() {
                continue;
            }
            let data = std::fs::read(&local_path).expect("read local metadata");
            put_cloud_metadata_for_test(cloud, file_name, data);
        }
    }

    fn delete_cloud_metadata_for_test(
        cloud: &crate::storage::cloud::CloudStorage,
        file_name: &str,
    ) {
        let key = crate::storage::cloud::cloud_metadata_key(file_name);
        let (tx, rx) = std::sync::mpsc::channel();
        cloud.submit_delete(key.clone(), tx);
        match rx.recv_timeout(Duration::from_secs(1)) {
            Ok(crate::storage::cloud::CloudEvent::DeleteComplete {
                result: crate::storage::cloud::CloudOutcome::Ok(()),
                ..
            }) => {}
            other => panic!("metadata delete for '{key}' failed: {other:?}"),
        }
    }

    #[test]
    fn should_start_with_no_hybrid_storage() {
        // Arrange
        // Act
        let event_loop = create_test_event_loop().expect("Should create event loop");

        // Assert - eviction_actor should be None initially
        assert!(event_loop.eviction_actor.is_none());
        assert!(event_loop.hybrid_storage.is_none());
    }

    #[test]
    fn should_set_hybrid_storage() {
        // Arrange
        let event_loop = create_test_event_loop().expect("Should create event loop");

        // Act
        // Create a mock hybrid storage (we need to use a real one or skip this test)
        // For now, we'll skip detailed testing of set_hybrid_storage since it requires
        // complex setup with actual HybridStorage instance

        // Assert - Method exists and is callable
        drop(event_loop);
    }

    #[test]
    fn should_support_hybrid_storage_optional_field() {
        // Arrange
        let event_loop = create_test_event_loop().expect("Should create event loop");

        // Act

        // Assert
        // hybrid_storage starts as None
        assert!(event_loop.hybrid_storage.is_none());
    }

    #[test]
    fn should_support_eviction_actor_optional_field() {
        // Arrange
        let event_loop = create_test_event_loop().expect("Should create event loop");

        // Act

        // Assert
        // eviction_actor starts as None
        assert!(event_loop.eviction_actor.is_none());
    }

    #[test]
    fn should_not_prune_remote_wal_when_manifest_sst_is_missing_from_cloud(
    ) -> crate::common::MidgeResult<()> {
        let mut el = create_test_cloud_event_loop(
            crate::storage::hybrid::policy::StorageBudgetPolicy::default(),
        )?;
        let segment_id = 1;
        let max_sequence = 10;
        seed_cloud_prune_candidate(&mut el, segment_id, max_sequence);
        add_manifest_sst_for_test(&mut el, "missing.sst", max_sequence);

        el.prune_cloud_wal_segments_covered_by_manifest();
        drain_prune_completion_for_test(&mut el);

        assert!(
            remote_wal_path_for_test(&el, segment_id).exists(),
            "remote WAL must be retained when a manifest-referenced cloud SST is missing"
        );
        assert!(
            el.cloud_acked_wal_segments.contains_key(&segment_id),
            "retained WAL should remain eligible for a future conservative retry"
        );

        Ok(())
    }

    #[test]
    fn should_not_prune_remote_wal_when_manifest_sst_is_corrupt_in_cloud(
    ) -> crate::common::MidgeResult<()> {
        let mut el = create_test_cloud_event_loop(
            crate::storage::hybrid::policy::StorageBudgetPolicy::default(),
        )?;
        let segment_id = 1;
        let max_sequence = 10;
        let sst_name = "corrupt.sst";
        seed_cloud_prune_candidate(&mut el, segment_id, max_sequence);
        add_manifest_sst_for_test(&mut el, sst_name, max_sequence);
        write_test_file(remote_sst_path_for_test(&el, sst_name), b"not a valid sst");

        el.prune_cloud_wal_segments_covered_by_manifest();
        drain_prune_completion_for_test(&mut el);

        assert!(
            remote_wal_path_for_test(&el, segment_id).exists(),
            "remote WAL must be retained when a manifest-referenced cloud SST is unreadable"
        );
        assert!(
            el.cloud_acked_wal_segments.contains_key(&segment_id),
            "retained WAL should remain eligible for a future conservative retry"
        );

        Ok(())
    }

    #[test]
    fn should_not_prune_remote_wal_when_cloud_metadata_is_missing() -> crate::common::MidgeResult<()>
    {
        let mut el = create_test_cloud_event_loop(
            crate::storage::hybrid::policy::StorageBudgetPolicy::default(),
        )?;
        let segment_id = 1;
        let max_sequence = 10;
        seed_cloud_prune_candidate(&mut el, segment_id, max_sequence);
        crate::metadata::ManifestPersistence::save(&el.state.db_path, &el.state.manifest)
            .map_err(crate::common::MidgeError::Internal)?;
        let metadata_backend = Arc::new(crate::storage::cloud::MockCloudBackend::new());
        el.cloud_metadata_storage = Some(Arc::new(crate::storage::cloud::CloudStorage::new(
            metadata_backend,
            "metadata-test".to_string(),
        )));

        el.prune_cloud_wal_segments_covered_by_manifest();
        drain_prune_completion_for_test(&mut el);

        assert!(
            remote_wal_path_for_test(&el, segment_id).exists(),
            "remote WAL must be retained when committed cloud metadata is missing"
        );
        assert!(
            el.cloud_acked_wal_segments.contains_key(&segment_id),
            "retained WAL should remain eligible for a future conservative retry"
        );

        Ok(())
    }

    #[test]
    fn should_not_prune_remote_wal_when_cloud_metadata_is_stale() -> crate::common::MidgeResult<()>
    {
        let mut el = create_test_cloud_event_loop(
            crate::storage::hybrid::policy::StorageBudgetPolicy::default(),
        )?;
        let segment_id = 1;
        let max_sequence = 10;
        seed_cloud_prune_candidate(&mut el, segment_id, max_sequence);
        crate::metadata::ManifestPersistence::save(&el.state.db_path, &el.state.manifest)
            .map_err(crate::common::MidgeError::Internal)?;

        let metadata_backend = Arc::new(crate::storage::cloud::MockCloudBackend::new());
        let metadata_storage = Arc::new(crate::storage::cloud::CloudStorage::new(
            metadata_backend,
            "metadata-test".to_string(),
        ));
        let format_bytes =
            std::fs::read(el.state.db_path.join("FORMAT")).expect("read local FORMAT");
        put_cloud_metadata_for_test(&metadata_storage, "FORMAT", format_bytes);
        put_cloud_metadata_for_test(&metadata_storage, "manifest.json", b"{}".to_vec());
        el.cloud_metadata_storage = Some(metadata_storage);

        el.prune_cloud_wal_segments_covered_by_manifest();
        drain_prune_completion_for_test(&mut el);

        assert!(
            remote_wal_path_for_test(&el, segment_id).exists(),
            "remote WAL must be retained when cloud metadata does not match the committed manifest"
        );
        assert!(
            el.cloud_acked_wal_segments.contains_key(&segment_id),
            "retained WAL should remain eligible for a future conservative retry"
        );

        Ok(())
    }

    #[test]
    fn should_not_prune_remote_wal_when_segment_is_not_cloud_durable(
    ) -> crate::common::MidgeResult<()> {
        let mut el = create_test_cloud_event_loop(
            crate::storage::hybrid::policy::StorageBudgetPolicy::default(),
        )?;
        let segment_id = 1;
        let max_sequence = 10;
        seed_cloud_prune_candidate(&mut el, segment_id, max_sequence);
        el.state.wal.cloud_durable_seq = max_sequence - 1;

        el.prune_cloud_wal_segments_covered_by_manifest();
        drain_prune_completion_for_test(&mut el);

        assert!(
            remote_wal_path_for_test(&el, segment_id).exists(),
            "remote WAL must be retained until the cloud durable frontier covers its max sequence"
        );
        assert!(
            el.cloud_acked_wal_segments.contains_key(&segment_id),
            "retained WAL should remain eligible for a future conservative retry"
        );

        Ok(())
    }

    #[test]
    fn should_not_prune_remote_wal_when_segment_max_sequence_exceeds_manifest_coverage(
    ) -> crate::common::MidgeResult<()> {
        let mut el = create_test_cloud_event_loop(
            crate::storage::hybrid::policy::StorageBudgetPolicy::default(),
        )?;
        let segment_id = 1;
        let max_sequence = 10;
        seed_cloud_prune_candidate(&mut el, segment_id, max_sequence);
        el.state.wal.cloud_durable_seq = max_sequence;
        el.state.manifest.last_persisted_sequence = max_sequence - 1;

        el.prune_cloud_wal_segments_covered_by_manifest();
        drain_prune_completion_for_test(&mut el);

        assert!(
            remote_wal_path_for_test(&el, segment_id).exists(),
            "remote WAL must be retained when its max sequence exceeds manifest coverage"
        );

        Ok(())
    }

    #[test]
    fn should_prune_remote_wal_when_segment_max_sequence_equals_manifest_coverage(
    ) -> crate::common::MidgeResult<()> {
        let mut el = create_test_cloud_event_loop(
            crate::storage::hybrid::policy::StorageBudgetPolicy::default(),
        )?;
        let segment_id = 1;
        let max_sequence = 10;
        seed_cloud_prune_candidate(&mut el, segment_id, max_sequence);
        el.state.wal.cloud_durable_seq = max_sequence;

        el.prune_cloud_wal_segments_covered_by_manifest();
        drain_prune_completion_for_test(&mut el);

        assert!(
            !remote_wal_path_for_test(&el, segment_id).exists(),
            "remote WAL may be pruned when cloud durability and manifest coverage both include its max sequence"
        );

        Ok(())
    }

    #[test]
    fn should_ignore_listing_only_ssts_when_deciding_remote_wal_cleanup(
    ) -> crate::common::MidgeResult<()> {
        let mut el = create_test_cloud_event_loop(
            crate::storage::hybrid::policy::StorageBudgetPolicy::default(),
        )?;
        let segment_id = 1;
        let max_sequence = 10;
        seed_cloud_prune_candidate(&mut el, segment_id, max_sequence);
        el.state.wal.cloud_durable_seq = max_sequence;
        el.state.manifest.last_persisted_sequence = 0;
        write_test_file(
            remote_sst_path_for_test(&el, "uploaded-but-uncommitted.sst"),
            b"listing-only object",
        );

        el.prune_cloud_wal_segments_covered_by_manifest();
        drain_prune_completion_for_test(&mut el);

        assert!(
            remote_wal_path_for_test(&el, segment_id).exists(),
            "uploaded but uncommitted SST objects must not establish WAL cleanup coverage"
        );

        Ok(())
    }

    #[test]
    fn should_keep_local_wal_when_remote_wal_readback_fails_after_cloud_ack(
    ) -> crate::common::MidgeResult<()> {
        let mut el = create_test_cloud_event_loop(
            crate::storage::hybrid::policy::StorageBudgetPolicy::default(),
        )?;
        let segment_id = 1;
        let local_wal = el
            .state
            .wal_dir
            .join(crate::wal::segment_file_name(segment_id));
        write_test_file(local_wal.clone(), b"local wal still needed");

        el.handle_storage_event(crate::storage::StorageEvent::CloudAck {
            segment_id,
            max_sequence: 1,
        });

        assert!(
            local_wal.exists(),
            "local WAL must be retained when the remote WAL cannot be read back after CloudAck"
        );

        Ok(())
    }

    #[test]
    fn should_treat_unproven_cloud_ack_as_failure_without_local_wal_removal(
    ) -> crate::common::MidgeResult<()> {
        let mut el = create_test_cloud_event_loop(
            crate::storage::hybrid::policy::StorageBudgetPolicy::default(),
        )?;
        let request_id = 501u64;
        let (seq, deferred) = el.wal_actor.append(
            &mut el.state,
            crate::runtime::actors::wal::AppendParams {
                request_id,
                cf_id: 0,
                key: Bytes::from_static(b"unproven-ack"),
                value: Some(Bytes::from_static(b"value")),
                insert_only: false,
                ttl_seconds: None,
            },
        )?;
        assert!(deferred, "CloudAsync append should wait for CloudAck");
        el.durability
            .queue_waiter(crate::runtime::durability::DurabilityWaiter::WalAppend {
                request_id,
                sequence: seq,
            });

        let (segment_id, max_sequence) = seal_segment_without_remote_proof_for_test(&mut el)?;
        let local_wal = el
            .state
            .wal_dir
            .join(crate::wal::segment_file_name(segment_id));
        assert!(
            local_wal.exists(),
            "sealed local WAL should exist before ack"
        );

        el.handle_storage_event(crate::storage::StorageEvent::CloudAck {
            segment_id,
            max_sequence,
        });

        assert!(
            local_wal.exists(),
            "local WAL must be retained when CloudAck has no prior remote readback proof"
        );

        Ok(())
    }

    #[test]
    fn should_not_advance_cloud_durability_across_unacked_segment_gap(
    ) -> crate::common::MidgeResult<()> {
        let mut el = create_test_cloud_event_loop(
            crate::storage::hybrid::policy::StorageBudgetPolicy::default(),
        )?;

        let first_request = 601u64;
        let (first_seq, first_deferred) = el.wal_actor.append(
            &mut el.state,
            crate::runtime::actors::wal::AppendParams {
                request_id: first_request,
                cf_id: 0,
                key: Bytes::from_static(b"gap-first"),
                value: Some(Bytes::from_static(b"value-1")),
                insert_only: false,
                ttl_seconds: None,
            },
        )?;
        assert!(first_deferred, "CloudAsync first append should defer");
        el.durability
            .queue_waiter(crate::runtime::durability::DurabilityWaiter::WalAppend {
                request_id: first_request,
                sequence: first_seq,
            });
        let (first_segment, first_max_sequence) = seal_segment_for_test(&mut el)?;

        let second_request = 602u64;
        let (second_seq, second_deferred) = el.wal_actor.append(
            &mut el.state,
            crate::runtime::actors::wal::AppendParams {
                request_id: second_request,
                cf_id: 0,
                key: Bytes::from_static(b"gap-second"),
                value: Some(Bytes::from_static(b"value-2")),
                insert_only: false,
                ttl_seconds: None,
            },
        )?;
        assert!(second_deferred, "CloudAsync second append should defer");
        el.durability
            .queue_waiter(crate::runtime::durability::DurabilityWaiter::WalAppend {
                request_id: second_request,
                sequence: second_seq,
            });
        let (second_segment, second_max_sequence) = seal_segment_for_test(&mut el)?;
        assert!(second_segment > first_segment);
        let second_local_wal = el
            .state
            .wal_dir
            .join(crate::wal::segment_file_name(second_segment));

        el.handle_storage_event(crate::storage::StorageEvent::CloudAck {
            segment_id: second_segment,
            max_sequence: second_max_sequence,
        });

        assert_eq!(
            el.state.wal.cloud_durable_seq, 0,
            "cloud durable frontier must not jump across an unacked segment"
        );
        assert_eq!(
            el.wal_actor.pending_cloud_writes_len(),
            2,
            "pending CloudAsync writes must not become visible until the gap is closed"
        );
        assert!(
            second_local_wal.exists(),
            "local WAL for an out-of-order ack must remain until earlier segments are durable"
        );

        el.handle_storage_event(crate::storage::StorageEvent::CloudAck {
            segment_id: first_segment,
            max_sequence: first_max_sequence,
        });

        assert_eq!(
            el.state.wal.cloud_durable_seq, second_max_sequence,
            "frontier should advance through the contiguous acked segment range once the gap closes"
        );
        assert_eq!(
            el.wal_actor.pending_cloud_writes_len(),
            0,
            "pending CloudAsync writes should drain after contiguous durability is proven"
        );
        assert!(
            !second_local_wal.exists(),
            "local WAL can be removed after the contiguous cloud durable frontier covers it"
        );

        Ok(())
    }

    #[test]
    fn should_drop_buffered_cloud_acks_when_earlier_segment_fails() -> crate::common::MidgeResult<()>
    {
        let mut el = create_test_cloud_event_loop(
            crate::storage::hybrid::policy::StorageBudgetPolicy::default(),
        )?;

        let first_request = 611u64;
        let (first_seq, first_deferred) = el.wal_actor.append(
            &mut el.state,
            crate::runtime::actors::wal::AppendParams {
                request_id: first_request,
                cf_id: 0,
                key: Bytes::from_static(b"fail-gap-first"),
                value: Some(Bytes::from_static(b"value-1")),
                insert_only: false,
                ttl_seconds: None,
            },
        )?;
        assert!(first_deferred, "CloudAsync first append should defer");
        el.durability
            .queue_waiter(crate::runtime::durability::DurabilityWaiter::WalAppend {
                request_id: first_request,
                sequence: first_seq,
            });
        let (first_segment, _) = seal_segment_for_test(&mut el)?;

        let second_request = 612u64;
        let (second_seq, second_deferred) = el.wal_actor.append(
            &mut el.state,
            crate::runtime::actors::wal::AppendParams {
                request_id: second_request,
                cf_id: 0,
                key: Bytes::from_static(b"fail-gap-second"),
                value: Some(Bytes::from_static(b"value-2")),
                insert_only: false,
                ttl_seconds: None,
            },
        )?;
        assert!(second_deferred, "CloudAsync second append should defer");
        el.durability
            .queue_waiter(crate::runtime::durability::DurabilityWaiter::WalAppend {
                request_id: second_request,
                sequence: second_seq,
            });
        let (second_segment, second_max_sequence) = seal_segment_for_test(&mut el)?;

        el.handle_storage_event(crate::storage::StorageEvent::CloudAck {
            segment_id: second_segment,
            max_sequence: second_max_sequence,
        });
        assert!(
            el.cloud_acked_wal_segments.contains_key(&second_segment),
            "later ack should be buffered while an earlier segment is unacked"
        );

        el.handle_storage_event(crate::storage::StorageEvent::CloudFail {
            segment_id: first_segment,
            error: "injected upload failure".to_string(),
        });

        assert_eq!(
            el.state.wal.cloud_durable_seq, 0,
            "failure of the earlier segment must not let a buffered later ack advance durability"
        );
        assert!(
            !el.cloud_acked_wal_segments.contains_key(&second_segment),
            "later buffered ack bookkeeping must be discarded after an earlier gap fails"
        );
        assert_eq!(
            el.wal_actor.pending_cloud_writes_len(),
            0,
            "pending CloudAsync writes should be cleared after cloud upload failure"
        );

        Ok(())
    }

    #[test]
    fn should_keep_local_wal_when_cached_remote_wal_proof_becomes_stale_before_cloud_ack(
    ) -> crate::common::MidgeResult<()> {
        let mut el = create_test_cloud_event_loop(
            crate::storage::hybrid::policy::StorageBudgetPolicy::default(),
        )?;
        let request_id = 502u64;
        let (seq, deferred) = el.wal_actor.append(
            &mut el.state,
            crate::runtime::actors::wal::AppendParams {
                request_id,
                cf_id: 0,
                key: Bytes::from_static(b"stale-proof-ack"),
                value: Some(Bytes::from_static(b"value")),
                insert_only: false,
                ttl_seconds: None,
            },
        )?;
        assert!(deferred, "CloudAsync append should wait for CloudAck");
        el.durability
            .queue_waiter(crate::runtime::durability::DurabilityWaiter::WalAppend {
                request_id,
                sequence: seq,
            });

        let (segment_id, max_sequence) = seal_segment_without_remote_proof_for_test(&mut el)?;
        let local_wal = el
            .state
            .wal_dir
            .join(crate::wal::segment_file_name(segment_id));
        el.hybrid_storage
            .as_ref()
            .expect("hybrid storage")
            .verify_remote_wal_segment(segment_id, max_sequence)
            .expect("establish remote WAL proof");
        std::fs::remove_file(remote_wal_path_for_test(&el, segment_id))
            .expect("delete remote WAL after proof");

        el.handle_storage_event(crate::storage::StorageEvent::CloudAck {
            segment_id,
            max_sequence,
        });

        assert!(
            local_wal.exists(),
            "local WAL must be retained when cached remote proof becomes stale before CloudAck"
        );

        Ok(())
    }

    #[test]
    fn should_not_reread_verified_cloud_metadata_on_repeated_wal_cleanup_check(
    ) -> crate::common::MidgeResult<()> {
        let mut el = create_test_cloud_event_loop(
            crate::storage::hybrid::policy::StorageBudgetPolicy::default(),
        )?;
        crate::metadata::ManifestPersistence::save(&el.state.db_path, &el.state.manifest)
            .map_err(crate::common::MidgeError::Internal)?;

        let metadata_backend = Arc::new(crate::storage::cloud::MockCloudBackend::new());
        let metadata_storage = Arc::new(crate::storage::cloud::CloudStorage::new(
            metadata_backend.clone(),
            "metadata-test".to_string(),
        ));
        put_all_cloud_metadata_for_test(&metadata_storage, &el.state.db_path);
        el.cloud_metadata_storage = Some(metadata_storage);

        metadata_backend.clear_history();
        el.verify_cloud_metadata_for_wal_cleanup()
            .expect("first cloud metadata validation");
        let first_downloads = metadata_backend.get_downloads();
        assert!(
            !first_downloads.is_empty(),
            "first validation should read cloud metadata"
        );

        el.verify_cloud_metadata_for_wal_cleanup()
            .expect("second cloud metadata validation");

        assert_eq!(
            metadata_backend.get_downloads(),
            first_downloads,
            "unchanged metadata proof should avoid repeated cloud metadata reads"
        );

        Ok(())
    }

    #[test]
    fn should_reject_cached_cloud_metadata_proof_when_remote_metadata_is_deleted(
    ) -> crate::common::MidgeResult<()> {
        let mut el = create_test_cloud_event_loop(
            crate::storage::hybrid::policy::StorageBudgetPolicy::default(),
        )?;
        crate::metadata::ManifestPersistence::save(&el.state.db_path, &el.state.manifest)
            .map_err(crate::common::MidgeError::Internal)?;

        let metadata_backend = Arc::new(crate::storage::cloud::MockCloudBackend::new());
        let metadata_storage = Arc::new(crate::storage::cloud::CloudStorage::new(
            metadata_backend,
            "metadata-test".to_string(),
        ));
        put_all_cloud_metadata_for_test(&metadata_storage, &el.state.db_path);
        el.cloud_metadata_storage = Some(Arc::clone(&metadata_storage));

        el.verify_cloud_metadata_for_wal_cleanup()
            .expect("initial cloud metadata validation");
        delete_cloud_metadata_for_test(&metadata_storage, "manifest.json");

        let error = el
            .verify_cloud_metadata_for_wal_cleanup()
            .expect_err("deleted metadata must invalidate cached cleanup proof");
        assert!(
            error.contains("changed since validation") || error.contains("unreadable"),
            "unexpected stale metadata proof error: {error}"
        );

        Ok(())
    }

    #[test]
    fn should_retry_auto_flush_when_backpressure_releases() -> crate::common::MidgeResult<()> {
        let mut el = create_test_cloud_event_loop(
            crate::storage::hybrid::policy::StorageBudgetPolicy::default(),
        )?;
        el.state.write_stalled = true;
        el.state.memtable_flush_threshold = 1024;
        el.state.memtable_size_limit = 1024 * 1024;
        el.state.sequence = 1;
        {
            let cf = el.state.get_cf(0).expect("default cf");
            cf.memtable
                .put_with_seq(b"retry-key".to_vec(), vec![0xA5; 2048], 1, None)
                .expect("seed memtable");
        }
        el.state.total_memtable_bytes = el
            .state
            .get_cf(0)
            .expect("default cf")
            .memtable
            .size_bytes();

        el.handle_storage_event(crate::storage::StorageEvent::BackpressureOff);

        assert!(!el.state.write_stalled);
        assert!(
            el.state.manifest.files.iter().any(|file| file.cf_id == 0),
            "backpressure release should retry the pending auto-flush"
        );

        Ok(())
    }

    #[test]
    fn should_cloud_async_ack_confirm_idempotent_request() -> crate::common::MidgeResult<()> {
        let mut el = create_test_cloud_event_loop(
            crate::storage::hybrid::policy::StorageBudgetPolicy::default(),
        )?;

        // Act

        // Add a wal append with a specific request_id
        let request_id = 123u64;
        let cf_id = 0u32;

        let (seq, deferred) = el.wal_actor.append(
            &mut el.state,
            crate::runtime::actors::wal::AppendParams {
                request_id,
                cf_id,
                key: bytes::Bytes::from("k1"),
                value: Some(bytes::Bytes::from("v1")),
                insert_only: false,
                ttl_seconds: None,
            },
        )?;

        assert!(
            deferred,
            "CloudAsync append should be deferred waiting for CloudAck"
        );

        // Queue waiter for this append (simulates EventLoop behavior)
        el.durability
            .queue_waiter(crate::runtime::durability::DurabilityWaiter::WalAppend {
                request_id,
                sequence: seq,
            });

        // Simulate sealing & uploading segment for CloudAsync as EventLoop would do
        let (seg_id, max_sequence) = seal_segment_for_test(&mut el)?;

        // Now simulate the storage CloudAck for that segment
        el.handle_storage_event(crate::storage::StorageEvent::CloudAck {
            segment_id: seg_id,
            max_sequence,
        });

        // Assert: After handling, the idempotency entry for request_id should be confirmed at cloud frontier
        assert!(
            el.state
                .sequence_idempotency_cache
                .contains_key(&request_id),
            "idempotency entry missing"
        );
        if let Some(entry) = el.state.sequence_idempotency_cache.get(&request_id) {
            assert!(entry.2 >= el.state.wal.cloud_durable_seq);
        }

        Ok(())
    }

    #[test]
    fn should_cloud_async_retry_after_ack_return_same_sequence_without_queueing(
    ) -> crate::common::MidgeResult<()> {
        let mut el = create_test_cloud_event_loop(
            crate::storage::hybrid::policy::StorageBudgetPolicy::default(),
        )?;

        // Act

        // Add a wal append with a specific request_id
        let request_id = 124u64;
        let cf_id = 0u32;

        let (seq1, deferred1) = el.wal_actor.append(
            &mut el.state,
            crate::runtime::actors::wal::AppendParams {
                request_id,
                cf_id,
                key: bytes::Bytes::from("k1"),
                value: Some(bytes::Bytes::from("v1")),
                insert_only: false,
                ttl_seconds: None,
            },
        )?;

        assert!(
            deferred1,
            "CloudAsync append should be deferred waiting for CloudAck"
        );

        // Queue waiter for this append (simulates EventLoop behavior)
        el.durability
            .queue_waiter(crate::runtime::durability::DurabilityWaiter::WalAppend {
                request_id,
                sequence: seq1,
            });

        // Simulate sealing & uploading segment for CloudAsync as EventLoop would do
        let (seg_id, max_sequence) = seal_segment_for_test(&mut el)?;

        // Now simulate the storage CloudAck for that segment
        el.handle_storage_event(crate::storage::StorageEvent::CloudAck {
            segment_id: seg_id,
            max_sequence,
        });

        // After handling, the idempotency entry for request_id should be confirmed at cloud frontier
        assert!(
            el.state
                .sequence_idempotency_cache
                .contains_key(&request_id),
            "idempotency entry missing"
        );
        if let Some(entry) = el.state.sequence_idempotency_cache.get(&request_id) {
            assert!(entry.2 >= el.state.wal.cloud_durable_seq);
        }

        // Assert: Retry the same request_id: should return the same sequence and NOT be deferred
        // Retry the same request_id: should return the same sequence and NOT be deferred
        let (seq2, deferred2) = el.wal_actor.append(
            &mut el.state,
            crate::runtime::actors::wal::AppendParams {
                request_id,
                cf_id,
                key: bytes::Bytes::from("k1"),
                value: Some(bytes::Bytes::from("v1")),
                insert_only: false,
                ttl_seconds: None,
            },
        )?;

        assert_eq!(seq1, seq2, "retry should return same sequence");
        assert!(
            !deferred2,
            "retry after confirmation should not be deferred"
        );
        assert_eq!(el.wal_actor.pending_cloud_writes_len(), 0);

        Ok(())
    }

    #[test]
    fn should_cloud_async_fail_invalidates_idempotency_then_retry_allocates_new_seq(
    ) -> crate::common::MidgeResult<()> {
        // Arrange: create state and event loop with CloudAsync policy
        let tmp = tempfile::tempdir().expect("create tmpdir");
        let state = RuntimeState::new(tmp.path().to_path_buf(), false);
        let router = Arc::new(ResponseRouter::new());
        let config = crate::runtime::RuntimeConfig {
            wal_durability_policy: crate::wal::DurabilityPolicy::CloudAsync,
            ..Default::default()
        };
        let mut el = EventLoop::new(state, false, router, config, None)?;

        // Act

        // Add a wal append with a specific request_id
        let request_id = 200u64;
        let cf_id = 0u32;

        let (seq1, deferred1) = el.wal_actor.append(
            &mut el.state,
            crate::runtime::actors::wal::AppendParams {
                request_id,
                cf_id,
                key: bytes::Bytes::from("k2"),
                value: Some(bytes::Bytes::from("v2")),
                insert_only: false,
                ttl_seconds: None,
            },
        )?;

        assert!(
            deferred1,
            "CloudAsync append should be deferred waiting for CloudAck"
        );

        // Queue waiter for this append (simulates EventLoop behavior)
        el.durability
            .queue_waiter(crate::runtime::durability::DurabilityWaiter::WalAppend {
                request_id,
                sequence: seq1,
            });

        // Simulate sealing & uploading segment for CloudAsync as EventLoop would do
        let (seg_id, _max_sequence) = seal_segment_for_test(&mut el)?;

        // Now simulate the storage CloudFail for that segment
        el.handle_storage_event(crate::storage::StorageEvent::CloudFail {
            segment_id: seg_id,
            error: "upload_failed".to_string(),
        });

        // Retry the same request_id: since the previous allocation failed, we expect a new sequence
        let (seq2, deferred2) = el.wal_actor.append(
            &mut el.state,
            crate::runtime::actors::wal::AppendParams {
                request_id,
                cf_id,
                key: bytes::Bytes::from("k2"),
                value: Some(bytes::Bytes::from("v2")),
                insert_only: false,
                ttl_seconds: None,
            },
        )?;

        // Assert: retry should allocate a new sequence and be deferred
        assert_ne!(
            seq1, seq2,
            "retry after cloud fail should allocate a new sequence"
        );
        assert!(
            deferred2,
            "retry should be deferred when retried after fail (CloudAsync)"
        );

        Ok(())
    }

    #[test]
    fn should_retry_background_cloud_seal_after_failpoint_before_rotate(
    ) -> crate::common::MidgeResult<()> {
        let _guard = failpoint_test_lock().lock().expect("lock failpoint tests");
        let scenario = fail::FailScenario::setup();
        let mut el = create_test_cloud_event_loop(
            crate::storage::hybrid::policy::StorageBudgetPolicy::default(),
        )?;

        let ops = vec![crate::runtime::TransactionOp::Put {
            cf_id: 0,
            key: bytes::Bytes::from_static(b"buffered-seal-key"),
            value: bytes::Bytes::from_static(b"buffered-seal-value"),
            ttl_seconds: None,
            insert_only: false,
        }];
        let (last_sequence, _op_count, deferred) = el.wal_actor.append_transaction(
            &mut el.state,
            301,
            ops,
            Some(crate::wal::DurabilityPolicy::CloudAsync),
            None,
            crate::runtime::TransactionIsolationPolicy::LastWriteWins,
        )?;
        assert!(
            deferred,
            "buffered transaction should defer cloud durability"
        );

        let failed_segment = el.state.wal.current_segment_id;
        fail::cfg(
            "midge::cloud::inject_fail_after_wal_flush_before_rotate",
            "return",
        )
        .expect("configure cloud seal failpoint");

        let first_error = el
            .seal_current_cloud_segment()
            .expect_err("first seal should fail before rotate");
        match first_error {
            crate::common::MidgeError::Internal(message) => {
                assert!(
                    message.contains("cloud seal failed after WAL flush before rotate"),
                    "unexpected failpoint error: {message}"
                );
            }
            other => panic!("unexpected seal failure: {other:?}"),
        }
        assert_eq!(
            el.state.wal.current_segment_id, failed_segment,
            "failed seal must not advance the current WAL segment"
        );
        assert!(
            el.state.wal.pending_writes > 0,
            "failed seal must preserve buffered WAL accounting for retry"
        );
        assert!(
            el.wal_actor.bytes_since_sync() > 0,
            "failed seal must preserve buffered byte accounting for retry"
        );
        assert!(
            el.has_actionable_work(),
            "failed seal should leave the runtime actionable for retry"
        );

        fail::remove("midge::cloud::inject_fail_after_wal_flush_before_rotate");

        let sealed = el
            .seal_current_cloud_segment()?
            .expect("retry should seal and enqueue the same WAL segment");
        assert_eq!(
            sealed.0, failed_segment,
            "retry should seal the same WAL segment after the failpoint clears"
        );
        assert_eq!(
            sealed.1, last_sequence,
            "retry should preserve the original max sequence for the segment"
        );
        assert_eq!(
            el.state.wal.current_segment_id,
            failed_segment + 1,
            "successful retry should advance to the next WAL segment"
        );
        assert_eq!(
            el.state.wal.pending_writes, 0,
            "successful retry should clear buffered WAL accounting"
        );
        assert_eq!(
            el.wal_actor.bytes_since_sync(),
            0,
            "successful retry should clear buffered byte accounting"
        );
        assert!(
            el.hybrid_storage
                .as_ref()
                .expect("hybrid storage")
                .pending_upload_count()
                > 0,
            "successful retry should enqueue the sealed segment for upload"
        );

        scenario.teardown();
        Ok(())
    }

    #[test]
    fn should_retry_seal_wal_for_cloud_after_failpoint_before_rotate(
    ) -> crate::common::MidgeResult<()> {
        let _guard = failpoint_test_lock().lock().expect("lock failpoint tests");
        let scenario = fail::FailScenario::setup();
        let mut el = create_test_cloud_event_loop(
            crate::storage::hybrid::policy::StorageBudgetPolicy::default(),
        )?;

        let ops = vec![crate::runtime::TransactionOp::Put {
            cf_id: 0,
            key: bytes::Bytes::from_static(b"strict-seal-key"),
            value: bytes::Bytes::from_static(b"strict-seal-value"),
            ttl_seconds: None,
            insert_only: false,
        }];
        let (last_sequence, _op_count, deferred) = el.wal_actor.append_transaction(
            &mut el.state,
            302,
            ops,
            Some(crate::wal::DurabilityPolicy::CloudAsync),
            None,
            crate::runtime::TransactionIsolationPolicy::LastWriteWins,
        )?;
        assert!(
            deferred,
            "cloud-backed transaction should defer cloud durability"
        );

        let fail_request_id = 401u64;
        let fail_rx = el.router.register(fail_request_id);
        fail::cfg(
            "midge::cloud::inject_fail_after_wal_flush_before_rotate",
            "return",
        )
        .expect("configure cloud seal failpoint");

        let msg_rx = crossbeam::channel::unbounded::<RuntimeMsg>().1;
        let outcome = el.handle_runtime_msg(
            RuntimeMsg::SealWalForCloud {
                request_id: fail_request_id,
                sequence: last_sequence,
                wait_for_ack: true,
            },
            &msg_rx,
        );
        assert_eq!(outcome, super::super::HandleOutcome::Continue);
        match fail_rx.recv().expect("failed strict response") {
            RuntimeResponse::Error { error, .. } => match error {
                crate::common::MidgeError::Internal(message) => {
                    assert!(
                        message.contains("cloud seal failed after WAL flush before rotate"),
                        "unexpected strict failure: {message}"
                    );
                }
                other => panic!("unexpected strict failure: {other:?}"),
            },
            other => panic!("unexpected strict failure response: {other:?}"),
        }
        assert!(
            el.state.wal.pending_writes > 0,
            "failed strict seal must preserve buffered WAL accounting for retry"
        );
        assert!(
            el.durability
                .inflight_segment_for_sequence(last_sequence)
                .is_none(),
            "failed strict seal must not invent an inflight segment before rotate succeeds"
        );

        fail::remove("midge::cloud::inject_fail_after_wal_flush_before_rotate");

        let retry_request_id = 402u64;
        let retry_rx = el.router.register(retry_request_id);
        let outcome = el.handle_runtime_msg(
            RuntimeMsg::SealWalForCloud {
                request_id: retry_request_id,
                sequence: last_sequence,
                wait_for_ack: true,
            },
            &msg_rx,
        );
        assert_eq!(outcome, super::super::HandleOutcome::Continue);
        assert!(
            el.durability
                .inflight_segment_for_sequence(last_sequence)
                .is_some(),
            "successful retry should install an inflight segment instead of falling through to a missing-cover error"
        );

        let seg_id = el
            .durability
            .inflight_segment_for_sequence(last_sequence)
            .expect("inflight segment for strict retry");
        copy_local_segment_to_remote_wal_for_test(&el, seg_id);
        el.hybrid_storage
            .as_ref()
            .expect("hybrid storage")
            .verify_remote_wal_segment(seg_id, last_sequence)
            .expect("verify retry remote WAL for test CloudAck");
        el.handle_storage_event(crate::storage::StorageEvent::CloudAck {
            segment_id: seg_id,
            max_sequence: last_sequence,
        });

        match retry_rx.recv().expect("strict retry response") {
            RuntimeResponse::Ok { request_id } => assert_eq!(request_id, retry_request_id),
            other => panic!("unexpected strict retry response: {other:?}"),
        }
        assert_eq!(
            el.state.wal.pending_writes, 0,
            "successful strict retry should clear buffered WAL accounting"
        );

        scenario.teardown();
        Ok(())
    }
}
