//! Cloud WAL publication: a verified upload records its segment as uploaded,
//! and the catalog publishes only the contiguous run of uploads its writer
//! owes it (#841).

use super::super::EventLoop;
use crate::common::{MidgeError, OperationDeadline};
use crate::runtime::hybrid_persistence::{CloudPersistence, PublicationError};
use crate::wal::cloud_catalog::CatalogEntry;
use crate::wal::publish::OwedSegment;

impl EventLoop {
    #[tracing::instrument(level = "debug", skip_all, fields(segment_id, max_sequence))]
    pub(super) fn handle_storage_event_cloud_ack(
        &mut self,
        segment_id: u64,
        max_sequence: u64,
        attempt_deadline: Option<&OperationDeadline>,
    ) {
        let acknowledged = OwedSegment {
            segment_id,
            max_sequence,
        };
        let deadline = attempt_deadline
            .copied()
            .unwrap_or_else(|| self.cloud_ack_deadline(segment_id));
        if !self.pipeline.inline_flush_worker {
            self.queue_cloud_wal_ack(acknowledged, deadline);
            return;
        }
        if let Err(error) = self.validate_runtime_writer_lease_within(&deadline) {
            self.requeue_failed_cloud_upload(
                segment_id,
                &Self::cloud_ack_error(
                    "writer lease validation failed before cloud WAL acknowledgement",
                    error,
                ),
            );
            return;
        }
        // A verified readback only records the segment as uploaded. The
        // catalog publishes it once every earlier segment is uploaded too.
        let upload = match self.verify_uploaded_wal_segment(acknowledged, &deadline) {
            Ok(upload) => upload,
            Err(error) => {
                self.route_cloud_publication_error(
                    acknowledged,
                    error,
                    "cloud WAL readback validation failed",
                    &deadline,
                );
                return;
            }
        };

        self.accept_verified_cloud_wal_upload(acknowledged, upload, deadline);
    }

    pub(in crate::runtime::event_loop) fn accept_verified_cloud_wal_upload(
        &mut self,
        acknowledged: OwedSegment,
        upload: CatalogEntry,
        deadline: OperationDeadline,
    ) {
        let OwedSegment {
            segment_id,
            max_sequence,
        } = acknowledged;
        self.state.cloud.forget_pending_upload(segment_id);
        if let Err(error) = self.pipeline.wal_transition.note_acknowledged(segment_id) {
            if self.state.wal.frontiers.cloud_durable() >= max_sequence {
                tracing::debug!(segment_id, "ignored duplicate cloud WAL acknowledgement");
                return;
            }
            self.requeue_failed_cloud_upload(segment_id, &error);
            return;
        }
        self.actors
            .cloud_coordinator
            .cloud_wal
            .record_upload(upload);

        let ready_segments = match self
            .pipeline
            .durability
            .contiguous_acked_cloud_segments(self.actors.cloud_coordinator.cloud_wal.acks())
        {
            Ok(ready_segments) => ready_segments,
            Err(refusal) => {
                self.route_cloud_publication_error(
                    acknowledged,
                    PublicationError::Refused(refusal),
                    "cloud WAL acknowledgement refused",
                    &deadline,
                );
                return;
            }
        };
        let Some(durable) = ready_segments.last().copied() else {
            tracing::debug!(
                segment_id,
                max_sequence,
                "CloudAck buffered behind an earlier unacked WAL segment"
            );
            return;
        };

        if !self.pipeline.inline_flush_worker {
            self.start_cloud_catalog_publication(acknowledged, ready_segments, deadline);
            return;
        }

        if !self.publish_contiguous_cloud_uploads(acknowledged, &ready_segments, &deadline) {
            return;
        }
        self.commit_contiguous_cloud_ack(&ready_segments, durable);
    }

    /// Publish the uploads of `ready_segments`, the contiguous run of
    /// acknowledged segments past the published frontier, in one catalog
    /// compare-and-swap. Returns whether the whole run is published.
    ///
    /// A failure withdraws the run's first upload, or `acknowledged` when the
    /// run needed none, so no later publication can pass it in the meantime.
    fn publish_contiguous_cloud_uploads(
        &mut self,
        acknowledged: OwedSegment,
        ready_segments: &[OwedSegment],
        deadline: &OperationDeadline,
    ) -> bool {
        let run = self
            .actors
            .cloud_coordinator
            .cloud_wal
            .acks()
            .unpublished(ready_segments);
        let first_upload = run.first().map_or(acknowledged, |upload| OwedSegment {
            segment_id: upload.id(),
            max_sequence: upload.max_sequence(),
        });
        if !run.is_empty() {
            if let Err(error) = self.publish_cloud_wal_run(&run, deadline) {
                self.route_cloud_publication_error(
                    first_upload,
                    error,
                    "cloud WAL catalog publication failed",
                    deadline,
                );
                return false;
            }
        }
        if let Err(error) = self.validate_runtime_writer_lease_within(deadline) {
            self.requeue_failed_cloud_upload(
                first_upload.segment_id,
                &Self::cloud_ack_error(
                    "writer lease validation failed after cloud WAL publication",
                    error,
                ),
            );
            return false;
        }
        self.actors
            .cloud_coordinator
            .cloud_wal
            .mark_published(ready_segments);
        true
    }

    /// Settle a readback or publication failure of `segment`'s upload.
    ///
    /// No retry can fix a mismatched object, a refused run or a fenced
    /// catalog, so those fence the writer. Anything else withdraws the upload
    /// and hands it to the runtime backlog, its one retry owner, to upload
    /// again after the retry delay. Waiting out shared memory pressure fails
    /// no waiter.
    pub(in crate::runtime::event_loop) fn route_cloud_publication_error(
        &mut self,
        segment: OwedSegment,
        error: PublicationError,
        context: &str,
        deadline: &OperationDeadline,
    ) {
        match error {
            PublicationError::Mismatch { segment_id, .. } => {
                self.fence_cloud_upload(segment_id, &error.into());
            }
            PublicationError::Refused(refusal) => {
                let segment_id = refusal.segment_id().unwrap_or(segment.segment_id);
                self.fence_cloud_upload(segment_id, &refusal.into());
            }
            PublicationError::Fenced(error) => {
                self.fence_cloud_upload(segment.segment_id, &error);
            }
            PublicationError::Retriable(error) => {
                // Withdraw the upload first, so no other acknowledgement
                // publishes it while its retry is queued.
                self.actors
                    .cloud_coordinator
                    .cloud_wal
                    .withdraw_upload(segment.segment_id);
                if self.defer_cloud_ack_for_memory(segment.segment_id, &error, deadline)
                    || self.settle_failed_cloud_upload(
                        segment.segment_id,
                        &Self::cloud_ack_error(context, error),
                    )
                {
                    self.requeue_cloud_upload(segment.segment_id);
                }
            }
        }
    }

    /// No retry can upload or publish `segment_id`: its object holds other
    /// bytes, the catalog refuses it, or the writer lost its authority.
    /// Objects are immutable and never overwritten, so nothing after it can
    /// be published either: fence the writer.
    pub(super) fn fence_cloud_upload(&mut self, segment_id: u64, error: &MidgeError) {
        self.actors
            .cloud_coordinator
            .cloud_wal
            .withdraw_upload(segment_id);
        self.state.cloud.forget_pending_upload(segment_id);
        self.fence_wal_transition(error, Some(segment_id));
        tracing::error!(segment_id, %error, "cloud WAL upload cannot be published; writer fenced");
    }

    pub(in crate::runtime::event_loop) fn cloud_persistence(
        &self,
    ) -> Result<CloudPersistence, PublicationError> {
        self.actors
            .cloud_coordinator
            .hybrid_storage
            .as_ref()
            .map(|storage| CloudPersistence::new(std::sync::Arc::clone(storage)))
            .ok_or_else(|| {
                PublicationError::Retriable(MidgeError::Internal(
                    "CloudAck received without HybridStorage".to_string(),
                ))
            })
    }

    fn verify_uploaded_wal_segment(
        &self,
        segment: OwedSegment,
        deadline: &OperationDeadline,
    ) -> Result<CatalogEntry, PublicationError> {
        let source = crate::io::FsLocation::new(
            std::sync::Arc::clone(&self.state.fs),
            crate::wal::segment_key(segment.segment_id),
        );
        self.cloud_persistence()?.verify_uploaded_wal_segment(
            segment.segment_id,
            segment.max_sequence,
            &source,
            deadline,
        )
    }

    fn publish_cloud_wal_run(
        &self,
        run: &[CatalogEntry],
        deadline: &OperationDeadline,
    ) -> Result<(), PublicationError> {
        self.cloud_persistence()?
            .publish_wal_segments(run, self.state.writer_epoch, deadline)
    }
}
